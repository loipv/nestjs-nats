import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ForceStopResult, HandlerAdapter } from '../../src/adapters/adapter.js';
import type {
  ConnectionEntry,
  NatsConnectionManager,
  UsedIdempotencyStore,
} from '../../src/connection/connection-manager.js';
import type { IdempotencyStore } from '../../src/idempotency/store.js';
import { NatsLifecycle } from '../../src/module/nats-lifecycle.js';
import type { NatsModuleOptions } from '../../src/module/options.js';

const never = () => new Promise<void>(() => {});

function fakeLogger() {
  return { log: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn(), verbose: vi.fn() };
}

function fakeAdapter(overrides: Partial<HandlerAdapter> = {}) {
  return {
    start: vi.fn(async () => {}),
    stop: vi.fn(async () => {}),
    idle: vi.fn(async () => {}),
    forceStop: vi.fn((): ForceStopResult => ({ nakked: 0, cancelledRetries: 0 })),
    ...overrides,
  };
}

function fakeStore(close?: () => Promise<void>): IdempotencyStore {
  return {
    claim: async () => 'claimed',
    complete: async () => {},
    release: async () => {},
    ...(close ? { close } : {}),
  };
}

type ConnectionOptions = Omit<NatsModuleOptions, 'connection'>;

function fakeManager(connections: Record<string, ConnectionOptions>) {
  const entries = new Map(
    Object.entries(connections).map(([name, options]) => [
      name,
      {
        name,
        options: { connection: {}, ...options },
        logger: fakeLogger(),
      } as unknown as ConnectionEntry & { logger: ReturnType<typeof fakeLogger> },
    ]),
  );
  const stores: UsedIdempotencyStore[] = [];
  const manager = {
    names: () => [...entries.keys()],
    has: (name: string) => entries.has(name),
    get: (name = 'default') => {
      const entry = entries.get(name);
      if (!entry) throw new Error(`unknown ${name}`);
      return entry;
    },
    closeAll: vi.fn(async (_opts?: { timeoutMs?: number }) => {}),
    idempotencyStores: vi.fn(() => stores),
  };
  return { manager, entries, stores };
}

async function startLifecycle(
  manager: ReturnType<typeof fakeManager>['manager'],
  adapters: Array<{ adapter: HandlerAdapter; connection?: string }>,
): Promise<NatsLifecycle> {
  const explorer = { explore: () => [] };
  const lifecycle = new NatsLifecycle(
    manager as unknown as NatsConnectionManager,
    {} as never,
    explorer as never,
  );
  vi.spyOn(
    lifecycle as unknown as { createAdapters: () => unknown },
    'createAdapters',
  ).mockReturnValue(
    adapters.map(({ adapter, connection = 'default' }) => ({
      adapter,
      entry: manager.get(connection),
    })),
  );
  await lifecycle.onApplicationBootstrap();
  return lifecycle;
}

describe('NatsLifecycle shutdown', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('bounds a hanging stop()', async () => {
    const { manager } = fakeManager({ default: { shutdown: { drainTimeout: 1000 } } });
    const adapter = fakeAdapter({ stop: vi.fn(never) });
    const lifecycle = await startLifecycle(manager, [{ adapter }]);

    let done = false;
    const shutdown = lifecycle.beforeApplicationShutdown().then(() => {
      done = true;
    });
    await vi.advanceTimersByTimeAsync(999);
    expect(manager.closeAll).not.toHaveBeenCalled();
    expect(adapter.forceStop).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await shutdown;
    expect(done).toBe(true);
    expect(adapter.forceStop).toHaveBeenCalledTimes(1);
    expect(manager.closeAll).toHaveBeenCalledWith({ timeoutMs: 1000 });
  });

  it('bounds a hanging idle() and force-stops adapters', async () => {
    const { manager, entries } = fakeManager({ default: { shutdown: { drainTimeout: 1000 } } });
    const hanging = fakeAdapter({
      idle: vi.fn(never),
      forceStop: vi.fn(() => ({ nakked: 3, cancelledRetries: 0 })),
    });
    const quiet = fakeAdapter();
    const lifecycle = await startLifecycle(manager, [{ adapter: hanging }, { adapter: quiet }]);

    const shutdown = lifecycle.beforeApplicationShutdown();
    await vi.advanceTimersByTimeAsync(500);
    expect(hanging.stop).toHaveBeenCalledTimes(1);
    expect(quiet.stop).toHaveBeenCalledTimes(1);
    expect(hanging.forceStop).not.toHaveBeenCalled();
    expect(manager.closeAll).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(500);
    await shutdown;
    expect(hanging.forceStop).toHaveBeenCalledTimes(1);
    expect(quiet.forceStop).toHaveBeenCalledTimes(1);
    expect(manager.closeAll).toHaveBeenCalledWith({ timeoutMs: 1000 });
    const warn = entries.get('default')?.logger.warn as ReturnType<typeof vi.fn>;
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(
      'NATS connection "default" shutdown: nak\'d 3 unsettled JetStream messages and cancelled 0 pending retries',
    );
  });

  it('cancels pending retries even when idle in time', async () => {
    const { manager, entries } = fakeManager({ default: { shutdown: { drainTimeout: 1000 } } });
    const adapter = fakeAdapter({
      forceStop: vi.fn(() => ({ nakked: 0, cancelledRetries: 2 })),
    });
    const lifecycle = await startLifecycle(manager, [{ adapter }]);

    await lifecycle.beforeApplicationShutdown();
    expect(adapter.forceStop).toHaveBeenCalledTimes(1);
    expect(manager.closeAll).toHaveBeenCalledTimes(1);
    const warn = entries.get('default')?.logger.warn as ReturnType<typeof vi.fn>;
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(
      'NATS connection "default" shutdown: nak\'d 0 unsettled JetStream messages and cancelled 2 pending retries',
    );
  });

  it('logs nothing when there was nothing to force', async () => {
    const { manager, entries } = fakeManager({ default: {} });
    const lifecycle = await startLifecycle(manager, [{ adapter: fakeAdapter() }]);
    await lifecycle.beforeApplicationShutdown();
    expect(entries.get('default')?.logger.warn).not.toHaveBeenCalled();
  });

  it('sums the forced work of each connection into one line', async () => {
    const { manager, entries } = fakeManager({ a: {}, b: {} });
    const forced = (nakked: number, cancelledRetries: number) =>
      fakeAdapter({ forceStop: vi.fn(() => ({ nakked, cancelledRetries })) });
    const lifecycle = await startLifecycle(manager, [
      { adapter: forced(1, 1), connection: 'a' },
      { adapter: forced(2, 0), connection: 'a' },
      { adapter: forced(0, 0), connection: 'b' },
    ]);
    await lifecycle.beforeApplicationShutdown();
    const warn = entries.get('a')?.logger.warn as ReturnType<typeof vi.fn>;
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(
      'NATS connection "a" shutdown: nak\'d 3 unsettled JetStream messages and cancelled 1 pending retry',
    );
    expect(entries.get('b')?.logger.warn).not.toHaveBeenCalled();
  });

  it('keeps shutting down when forceStop throws', async () => {
    const { manager, entries } = fakeManager({ default: {} });
    const broken = fakeAdapter({
      forceStop: vi.fn(() => {
        throw new Error('boom');
      }),
    });
    const other = fakeAdapter();
    const lifecycle = await startLifecycle(manager, [{ adapter: broken }, { adapter: other }]);
    await lifecycle.beforeApplicationShutdown();
    expect(other.forceStop).toHaveBeenCalledTimes(1);
    expect(manager.closeAll).toHaveBeenCalledTimes(1);
    expect(entries.get('default')?.logger.error).toHaveBeenCalled();
  });

  it('uses the effective max drainTimeout across connections', async () => {
    const unset = fakeManager({ a: { shutdown: { drainTimeout: 50 } }, b: {} }).manager;
    await (await startLifecycle(unset, [])).beforeApplicationShutdown();
    expect(unset.closeAll).toHaveBeenCalledWith({ timeoutMs: 10_000 });

    const larger = fakeManager({
      a: { shutdown: { drainTimeout: 50 } },
      b: { shutdown: { drainTimeout: 20_000 } },
    }).manager;
    await (await startLifecycle(larger, [])).beforeApplicationShutdown();
    expect(larger.closeAll).toHaveBeenCalledWith({ timeoutMs: 20_000 });
  });

  it('closes idempotency stores once, after the connections', async () => {
    const order: string[] = [];
    const shared = fakeStore(async () => {
      order.push('shared');
    });
    const moduleOnly = fakeStore(async () => {
      order.push('module');
    });
    const failing = fakeStore(async () => {
      throw new Error('store close failed');
    });
    const { manager, entries, stores } = fakeManager({
      a: { idempotency: { store: shared } },
      b: { idempotency: { store: shared } },
      c: { idempotency: { store: moduleOnly } },
      d: { idempotency: { store: failing } },
    });
    stores.push(
      { connection: 'a', store: shared, owned: false },
      { connection: 'b', store: shared, owned: false },
      { connection: 'a', store: fakeStore(), owned: true },
      { connection: 'd', store: failing, owned: false },
    );
    manager.closeAll.mockImplementation(async () => {
      order.push('closeAll');
    });
    const lifecycle = await startLifecycle(manager, []);
    await lifecycle.beforeApplicationShutdown();
    expect(order).toEqual(['closeAll', 'shared', 'module']);
    const error = entries.get('d')?.logger.error as ReturnType<typeof vi.fn>;
    expect(error).toHaveBeenCalledTimes(1);
    expect(String(error.mock.calls[0]?.[0])).toMatch(/store close failed/);
  });

  it('bounds a hanging idempotency store close', async () => {
    const { manager, stores } = fakeManager({ default: { shutdown: { drainTimeout: 1000 } } });
    stores.push({ connection: 'default', store: fakeStore(never), owned: false });
    const lifecycle = await startLifecycle(manager, []);
    let done = false;
    const shutdown = lifecycle.beforeApplicationShutdown().then(() => {
      done = true;
    });
    await vi.advanceTimersByTimeAsync(1000);
    await shutdown;
    expect(done).toBe(true);
  });
});
