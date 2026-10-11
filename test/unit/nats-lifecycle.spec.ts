import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ForceStopResult, HandlerAdapter, HandlerState } from '../../src/adapters/adapter.js';
import { CoreSubscriptionAdapter } from '../../src/adapters/core-subscription.adapter.js';
import { JetStreamConsumerAdapter } from '../../src/adapters/jetstream-consumer.adapter.js';
import type {
  ConnectionEntry,
  ConnectionRegistry,
  UsedIdempotencyStore,
} from '../../src/connection/connection-registry.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import type { IdempotencyStore } from '../../src/idempotency/store.js';
import { NatsStreamManager } from '../../src/jetstream/stream-manager.js';
import { NatsLifecycle } from '../../src/module/nats-lifecycle.js';
import type { NatsFeatureOptions, NatsModuleOptions } from '../../src/module/options.js';
import { NATS_FEATURE_TOKEN_PREFIX } from '../../src/module/tokens.js';

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
  const handlers: Array<{ connection: string; label: string; state: HandlerState }> = [];
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
    trackHandler: vi.fn((connection: string, label: string) => {
      const state = { label, state: 'running' as HandlerState };
      handlers.push({ connection, ...state });
      const entry = handlers[handlers.length - 1];
      return (next: HandlerState) => {
        if (entry) entry.state = next;
      };
    }),
    handlerStates: (connection: string) => handlers.filter((h) => h.connection === connection),
    clearHandlers: vi.fn(() => {
      handlers.length = 0;
    }),
  };
  return { manager, entries, stores };
}

async function startLifecycle(
  manager: ReturnType<typeof fakeManager>['manager'],
  adapters: Array<{ adapter: HandlerAdapter; connection?: string }>,
): Promise<NatsLifecycle> {
  const explorer = { explore: () => [] };
  const lifecycle = new NatsLifecycle(
    manager as unknown as ConnectionRegistry,
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

  it('gives store close its own minimum budget when closing connections used it all', async () => {
    const { manager, entries, stores } = fakeManager({
      default: { shutdown: { drainTimeout: 1000 } },
    });
    let closed = false;
    const slow = fakeStore(
      () =>
        new Promise<void>((resolve) =>
          setTimeout(() => {
            closed = true;
            resolve();
          }, 500),
        ),
    );
    stores.push({ connection: 'default', store: slow, owned: false });
    // The server is gone: closing the connections takes the whole bound.
    manager.closeAll.mockImplementation(
      () => new Promise<void>((resolve) => setTimeout(resolve, 1000)),
    );
    const lifecycle = await startLifecycle(manager, []);
    let done = false;
    const shutdown = lifecycle.beforeApplicationShutdown().then(() => {
      done = true;
    });
    await vi.advanceTimersByTimeAsync(1100);
    expect(done).toBe(false);
    await vi.advanceTimersByTimeAsync(400);
    await shutdown;
    expect(closed).toBe(true);
    expect(entries.get('default')?.logger.warn).not.toHaveBeenCalled();
  });

  it('warns when a store close is abandoned', async () => {
    const { manager, entries, stores } = fakeManager({
      default: { shutdown: { drainTimeout: 1000 } },
    });
    stores.push({ connection: 'default', store: fakeStore(never), owned: false });
    const lifecycle = await startLifecycle(manager, []);
    const shutdown = lifecycle.beforeApplicationShutdown();
    await vi.advanceTimersByTimeAsync(1000);
    await shutdown;
    expect(entries.get('default')?.logger.warn).toHaveBeenCalledWith(
      'NATS connection "default" idempotency store did not close within 1000ms; abandoned',
    );
  });

  it('closes a store only once when shutdown runs again', async () => {
    const close = vi.fn(async () => {});
    const { manager, stores } = fakeManager({ default: {} });
    stores.push({ connection: 'default', store: fakeStore(close), owned: false });
    const lifecycle = await startLifecycle(manager, []);
    await lifecycle.beforeApplicationShutdown();
    await lifecycle.beforeApplicationShutdown();
    expect(close).toHaveBeenCalledTimes(1);
  });
});

describe('NatsLifecycle failed bootstrap', () => {
  it('stops, force-stops, closes the connections and closes the stores', async () => {
    const order: string[] = [];
    const { manager, stores } = fakeManager({ default: { shutdown: { drainTimeout: 1000 } } });
    stores.push({
      connection: 'default',
      store: fakeStore(async () => {
        order.push('store');
      }),
      owned: false,
    });
    manager.closeAll.mockImplementation(async () => {
      order.push('closeAll');
    });
    const started = fakeAdapter({
      stop: vi.fn(async () => {
        order.push('stop');
      }),
      forceStop: vi.fn(() => {
        order.push('forceStop');
        return { nakked: 0, cancelledRetries: 0 };
      }),
    });
    const failing = fakeAdapter({
      start: vi.fn(() => Promise.reject(new Error('start failed'))),
    });
    await expect(
      startLifecycle(manager, [{ adapter: started }, { adapter: failing }]),
    ).rejects.toThrow('start failed');
    expect(order).toEqual(['stop', 'forceStop', 'closeAll', 'store']);
    expect(failing.forceStop).toHaveBeenCalledTimes(1);
  });
});

describe('NatsLifecycle restart wiring', () => {
  function lifecycleWith(manager: ReturnType<typeof fakeManager>['manager']) {
    const def = {
      kind: 'subscribe',
      connection: 'default',
      className: 'Svc',
      methodName: 'on',
      subject: 'a.b',
      options: {},
      wrapper: { isDependencyTreeStatic: () => true, isTransient: false },
    };
    return new NatsLifecycle(
      manager as unknown as ConnectionRegistry,
      {} as never,
      { explore: () => [def] } as never,
    );
  }

  it('fails bootstrap on an invalid restart option', async () => {
    const { manager } = fakeManager({ default: { restart: { factor: 0 } } });
    await expect(lifecycleWith(manager).onApplicationBootstrap()).rejects.toThrow(
      /restart\.factor/,
    );
    expect(manager.trackHandler).not.toHaveBeenCalled();
    expect(manager.closeAll).toHaveBeenCalled();
  });

  it('registers each handler as running after bootstrap', async () => {
    const { manager } = fakeManager({ default: {} });
    const lifecycle = lifecycleWith(manager);
    vi.spyOn(CoreSubscriptionAdapter.prototype, 'start').mockResolvedValue();
    await lifecycle.onApplicationBootstrap();
    expect(manager.handlerStates('default')).toEqual([
      { connection: 'default', label: 'Svc.on on "a.b"', state: 'running' },
    ]);
  });

  it('clears handler states at shutdown', async () => {
    const { manager } = fakeManager({ default: {} });
    const lifecycle = lifecycleWith(manager);
    vi.spyOn(CoreSubscriptionAdapter.prototype, 'start').mockResolvedValue();
    vi.spyOn(CoreSubscriptionAdapter.prototype, 'stop').mockResolvedValue();
    await lifecycle.onApplicationBootstrap();
    expect(manager.handlerStates('default')).toHaveLength(1);
    await lifecycle.beforeApplicationShutdown();
    expect(manager.handlerStates('default')).toEqual([]);
  });
});

describe('NatsLifecycle declarations', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  function lifecycleWith(
    manager: ReturnType<typeof fakeManager>['manager'],
    features: NatsFeatureOptions[] = [],
  ) {
    const discovery = {
      getProviders: () =>
        features.map((instance, i) => ({ token: `${NATS_FEATURE_TOKEN_PREFIX}${i}`, instance })),
    };
    return new NatsLifecycle(
      manager as unknown as ConnectionRegistry,
      discovery as never,
      { explore: () => [] } as never,
    );
  }

  it('rejects an invalid jetstream.removedFields before syncing anything', async () => {
    const ensureStream = vi.spyOn(NatsStreamManager.prototype, 'ensureStream');
    const { manager } = fakeManager({
      default: {
        jetstream: { removedFields: 'drop' as never },
        streams: [{ name: 'S', subjects: ['s.>'] }],
      },
    });
    await expect(lifecycleWith(manager).onModuleInit()).rejects.toThrow(
      new NatsConfigError(
        `NATS connection "default" jetstream.removedFields must be 'reset' or 'ignore', got "drop"`,
      ),
    );
    expect(ensureStream).not.toHaveBeenCalled();
    expect(manager.closeAll).toHaveBeenCalled();
  });

  function syncSpies() {
    return {
      ensureStream: vi
        .spyOn(NatsStreamManager.prototype, 'ensureStream')
        .mockResolvedValue(undefined),
      ensureConsumer: vi
        .spyOn(NatsStreamManager.prototype, 'ensureConsumer')
        .mockResolvedValue(undefined),
    };
  }

  const reset = { jetstream: { removedFields: 'reset' as const } };

  it("merges declarations of one stream with removedFields 'reset'", async () => {
    const { ensureStream } = syncSpies();
    const { manager } = fakeManager({
      default: { ...reset, streams: [{ name: 'S', subjects: ['s.>'], metadata: { a: '1' } }] },
    });
    await lifecycleWith(manager, [
      { streams: [{ name: 'S', subjects: ['s.>'], max_age: 5, metadata: { b: '2' } }] },
      { streams: [{ name: 'T' }, { name: 'S', max_msgs: null as never }] },
    ]).onModuleInit();
    expect(ensureStream.mock.calls).toEqual([
      [{ name: 'S', subjects: ['s.>'], max_age: 5, metadata: { a: '1', b: '2' } }],
      [{ name: 'T' }],
    ]);
  });

  it('merges module consumers of one durable, and keeps streams of other connections apart', async () => {
    const { ensureStream, ensureConsumer } = syncSpies();
    const { manager } = fakeManager({
      default: {
        ...reset,
        streams: [{ name: 'S' }],
        consumers: [{ stream: 'S', config: { durable_name: 'D', max_ack_pending: 5 } }],
      },
      other: { ...reset, streams: [{ name: 'S', max_age: 1 }] },
    });
    await lifecycleWith(manager, [
      { consumers: [{ stream: 'S', config: { durable_name: 'D', filter_subject: 's.a' } }] },
      { consumers: [{ stream: 'S', config: { durable_name: 'E' } }] },
    ]).onModuleInit();
    expect(ensureStream.mock.calls).toEqual([[{ name: 'S' }], [{ name: 'S', max_age: 1 }]]);
    expect(ensureConsumer.mock.calls).toEqual([
      ['S', { durable_name: 'D', max_ack_pending: 5, filter_subject: 's.a' }],
      ['S', { durable_name: 'E' }],
    ]);
  });

  it.each([
    [
      'a field',
      { streams: [{ name: 'S', subjects: ['a'] }] },
      { streams: [{ name: 'S', subjects: ['b'] }] },
      'JetStream stream "S" is declared more than once with different subjects',
    ],
    [
      'a metadata key',
      { streams: [{ name: 'S', metadata: { a: '1' } }] },
      { streams: [{ name: 'S', metadata: { a: '2' } }] },
      'JetStream stream "S" is declared more than once with different metadata key "a"',
    ],
    [
      'a consumer field',
      { consumers: [{ stream: 'S', config: { durable_name: 'D', max_deliver: 3 } }] },
      { consumers: [{ stream: 'S', config: { durable_name: 'D', max_deliver: 4 } }] },
      'JetStream consumer "S/D" is declared more than once with different max_deliver',
    ],
  ])('rejects declarations that disagree on %s', async (_, root, feature, message) => {
    const { ensureStream } = syncSpies();
    const { manager } = fakeManager({ default: { ...reset, ...root } });
    await expect(lifecycleWith(manager, [feature]).onModuleInit()).rejects.toThrow(
      new NatsConfigError(message),
    );
    expect(ensureStream).not.toHaveBeenCalled();
  });

  it("syncs every declaration as given with removedFields 'ignore' (the default)", async () => {
    const { ensureStream } = syncSpies();
    const { manager } = fakeManager({ default: { streams: [{ name: 'S', subjects: ['a'] }] } });
    await lifecycleWith(manager, [{ streams: [{ name: 'S', subjects: ['b'] }] }]).onModuleInit();
    expect(ensureStream.mock.calls).toEqual([
      [{ name: 'S', subjects: ['a'] }],
      [{ name: 'S', subjects: ['b'] }],
    ]);
  });

  describe('a durable declared both in the module options and by a handler', () => {
    function consumeDef(config?: object) {
      return {
        kind: 'consume',
        delivery: 'pull',
        connection: 'default',
        className: 'Svc',
        methodName: 'handle',
        options: { stream: 'S', durable: 'D', ...(config ? { config } : {}) },
        wrapper: { isDependencyTreeStatic: () => true, isTransient: false },
      };
    }

    async function boot(options: ConnectionOptions, def: object) {
      syncSpies();
      const start = vi.spyOn(JetStreamConsumerAdapter.prototype, 'start').mockResolvedValue();
      const { manager } = fakeManager({
        default: {
          ...options,
          streams: [{ name: 'S' }],
          consumers: [{ stream: 'S', config: { durable_name: 'D' } }],
        },
      });
      const lifecycle = new NatsLifecycle(
        manager as unknown as ConnectionRegistry,
        { getProviders: () => [] } as never,
        { explore: () => [def] } as never,
      );
      await lifecycle.onModuleInit();
      return { start, bootstrap: lifecycle.onApplicationBootstrap() };
    }

    it("is rejected with removedFields 'reset', before any handler starts", async () => {
      const { start, bootstrap } = await boot(reset, consumeDef({ max_deliver: 3 }));
      await expect(bootstrap).rejects.toThrow(
        new NatsConfigError(
          'JetStream consumer "S/D" is declared both in the module options and by Svc.handle; declare it in one place',
        ),
      );
      expect(start).not.toHaveBeenCalled();
    });

    it('is accepted when the handler declares no config', async () => {
      const { start, bootstrap } = await boot(reset, consumeDef());
      await bootstrap;
      expect(start).toHaveBeenCalledOnce();
    });

    it("is accepted with removedFields 'ignore'", async () => {
      const { start, bootstrap } = await boot({}, consumeDef({ max_deliver: 3 }));
      await bootstrap;
      expect(start).toHaveBeenCalledOnce();
    });
  });
});
