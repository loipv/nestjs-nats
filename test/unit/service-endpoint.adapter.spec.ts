import { describe, expect, it, vi } from 'vitest';
import {
  NatsServiceAdapter,
  type ServiceEndpointDefinition,
} from '../../src/adapters/service-endpoint.adapter.js';
import type { ConnectionEntry } from '../../src/connection/connection-registry.js';
import { NatsConfigError } from '../../src/errors/errors.js';

const svcm = vi.hoisted(() => ({ add: vi.fn() }));
vi.mock('@nats-io/services', () => ({
  Svcm: class {
    add = svcm.add;
  },
}));

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Push-driven async iterable whose `stop()` ends iteration after buffered items. */
function fakeIterator() {
  const queue: unknown[] = [];
  let wake: (() => void) | undefined;
  let stopped = false;
  const iterator = {
    stop() {
      stopped = true;
      wake?.();
    },
    async *[Symbol.asyncIterator]() {
      for (;;) {
        const next = queue.shift();
        if (next) yield next;
        else if (stopped) return;
        else await new Promise<void>((resolve) => (wake = resolve));
      }
    },
  };
  return {
    iterator,
    push(msg: unknown) {
      queue.push(msg);
      wake?.();
    },
    end() {
      stopped = true;
      wake?.();
    },
  };
}

function fakeMsg(payload: unknown) {
  return {
    subject: 'x',
    reply: 'inbox',
    data: new TextEncoder().encode(JSON.stringify(payload)),
    headers: undefined,
    respond: vi.fn(() => true),
    respondError: vi.fn(() => true),
  };
}

function setup(
  invoke: (payload: unknown) => Promise<unknown> = () => Promise.resolve(),
  plugins: unknown[] = [],
) {
  const iterators = new Map<string, ReturnType<typeof fakeIterator>>();
  let resolveStopped!: (err: Error | null) => void;
  const stopped = new Promise<Error | null>((resolve) => (resolveStopped = resolve));
  const service = {
    addEndpoint: vi.fn((name: string) => {
      const it = fakeIterator();
      iterators.set(name, it);
      return it.iterator;
    }),
    stop: vi.fn(() => {
      for (const it of iterators.values()) it.end();
      resolveStopped(null);
      return Promise.resolve(null);
    }),
    stopped,
    /** What `@nats-io/services` does when a subscription fails: the iterators never end. */
    fail(err: Error) {
      resolveStopped(err);
    },
  };
  svcm.add.mockReset();
  svcm.add.mockResolvedValue(service);
  const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
  const entry = {
    name: 'default',
    options: { plugins },
    logger,
    nc: { flush: () => Promise.resolve(), isClosed: () => false },
  } as unknown as ConnectionEntry;
  const endpoint = (name: string, options: object = {}) =>
    ({
      kind: 'service-endpoint',
      connection: 'default',
      className: 'S',
      methodName: name,
      endpoint: name,
      options,
      invoke,
    }) as unknown as ServiceEndpointDefinition;
  return { service, iterators, logger, entry, endpoint };
}

describe('NatsServiceAdapter', () => {
  it('passes only the options that are set', async () => {
    const { service, entry, endpoint } = setup();
    const adapter = new NatsServiceAdapter(
      { name: 'svc', version: '1.0.0' },
      [endpoint('a'), endpoint('b', { subject: 's.b', queue: 'q', metadata: { k: 'v' } })],
      entry,
    );
    await adapter.start();
    expect(svcm.add).toHaveBeenCalledWith({ name: 'svc', version: '1.0.0' });
    expect(service.addEndpoint).toHaveBeenNthCalledWith(1, 'a', {});
    expect(service.addEndpoint).toHaveBeenNthCalledWith(2, 'b', {
      subject: 's.b',
      queue: 'q',
      metadata: { k: 'v' },
    });
    await adapter.stop();
    await adapter.idle();
  });

  it('responds with the encoded result, including an empty payload for undefined', async () => {
    const { iterators, entry, endpoint } = setup((p) =>
      Promise.resolve(p === 'void' ? undefined : { echo: p }),
    );
    const adapter = new NatsServiceAdapter({ name: 'svc', version: '1' }, [endpoint('a')], entry);
    await adapter.start();
    const [ok, empty] = [fakeMsg('hi'), fakeMsg('void')];
    iterators.get('a')?.push(ok);
    iterators.get('a')?.push(empty);
    await tick();
    await tick();
    expect(ok.respond).toHaveBeenCalledWith(new TextEncoder().encode('{"echo":"hi"}'));
    expect(empty.respond).toHaveBeenCalledWith(new Uint8Array(0));
    await adapter.stop();
    await adapter.idle();
  });

  it('responds with a service error and keeps serving after a failure', async () => {
    const { iterators, entry, endpoint } = setup((p) =>
      p === 'bad' ? Promise.reject(new Error('internal')) : Promise.resolve('fine'),
    );
    const adapter = new NatsServiceAdapter({ name: 'svc', version: '1' }, [endpoint('a')], entry);
    await adapter.start();
    const [bad, good] = [fakeMsg('bad'), fakeMsg('good')];
    iterators.get('a')?.push(bad);
    iterators.get('a')?.push(good);
    await tick();
    await tick();
    expect(bad.respondError).toHaveBeenCalledWith(500, 'Internal Server Error');
    expect(bad.respond).not.toHaveBeenCalled();
    expect(good.respond).toHaveBeenCalledTimes(1);
    await adapter.stop();
    await adapter.idle();
  });

  it('warns when it replies 5xx because a plugin failed, as the core adapter does', async () => {
    const plugin = {
      name: 'p',
      inbound: {
        handle() {
          return Promise.reject(new Error('plugin broke'));
        },
      },
    };
    const { iterators, logger, entry, endpoint } = setup(() => Promise.resolve('ok'), [plugin]);
    const adapter = new NatsServiceAdapter({ name: 'svc', version: '1' }, [endpoint('a')], entry);
    await adapter.start();
    const msg = fakeMsg('x');
    iterators.get('a')?.push(msg);
    await tick();
    await tick();
    expect(msg.respondError).toHaveBeenCalledWith(500, 'Internal Server Error');
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringMatching(/Replied 500 to request on "x" for S\.a.*svc.*plugin broke/),
    );
    await adapter.stop();
    await adapter.idle();
  });

  it('logs an error when an endpoint iterator ends while not stopping', async () => {
    const { iterators, logger, entry, endpoint } = setup();
    const adapter = new NatsServiceAdapter({ name: 'svc', version: '1' }, [endpoint('a')], entry);
    await adapter.start();
    iterators.get('a')?.end();
    await adapter.idle();
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringMatching(/S\.a.*svc.*ended unexpectedly/),
    );
  });

  it('logs and ends the endpoints when the service stops because of an error', async () => {
    const { service, logger, entry, endpoint } = setup();
    const adapter = new NatsServiceAdapter(
      { name: 'svc', version: '1' },
      [endpoint('a'), endpoint('b')],
      entry,
    );
    await adapter.start();
    service.fail(new Error('Permissions Violation for Subscription to "svc.a"'));
    // Without the fix the endpoint loops never end and idle() hangs.
    await expect(adapter.idle()).resolves.toBeUndefined();
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringMatching(/service "svc" stopped.*Permissions Violation.*no more requests/i),
      expect.stringContaining('Permissions Violation'),
    );
  });

  it('does not log when iterators end because of stop()', async () => {
    const { service, logger, entry, endpoint } = setup();
    const adapter = new NatsServiceAdapter({ name: 'svc', version: '1' }, [endpoint('a')], entry);
    await adapter.start();
    await adapter.stop();
    await adapter.idle();
    expect(service.stop).toHaveBeenCalledTimes(1);
    expect(logger.error).not.toHaveBeenCalled();
  });

  it('stops the service when stop() ran during start()', async () => {
    const { service, entry, endpoint } = setup();
    let resolveAdd!: () => void;
    svcm.add.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveAdd = () => resolve(service);
        }),
    );
    const adapter = new NatsServiceAdapter({ name: 'svc', version: '1' }, [endpoint('a')], entry);
    const starting = adapter.start();
    await tick();
    await adapter.stop();
    resolveAdd();
    await starting;
    expect(service.stop).toHaveBeenCalledTimes(1);
    expect(service.addEndpoint).not.toHaveBeenCalled();
    await adapter.idle();
  });

  it('stops the service when adding an endpoint fails', async () => {
    const { service, entry, endpoint } = setup();
    service.addEndpoint.mockImplementation(() => {
      throw new Error('invalid endpoint');
    });
    const adapter = new NatsServiceAdapter({ name: 'svc', version: '1' }, [endpoint('a')], entry);
    const err = await adapter.start().then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(NatsConfigError);
    expect((err as Error).message).toMatch(/S\.a.*"a".*svc.*invalid endpoint/);
    expect((err as Error).cause).toBeInstanceOf(Error);
    await adapter.stop();
    expect(service.stop).toHaveBeenCalledTimes(1);
  });

  it('hands plugins a narrow handler description', async () => {
    const seen: unknown[] = [];
    const plugin = {
      name: 'p',
      inbound: {
        async handle(d: { handler: unknown }, next: () => Promise<void>) {
          seen.push(d.handler);
          await next();
        },
      },
    };
    const { iterators, entry, endpoint } = setup(() => Promise.resolve('ok'), [plugin]);
    const adapter = new NatsServiceAdapter(
      { name: 'svc', version: '1.0.0', description: 'd' },
      [endpoint('a', { subject: 'svc.a' })],
      entry,
    );
    await adapter.start();
    const msg = fakeMsg('x');
    iterators.get('a')?.push(msg);
    await tick();
    await tick();
    expect(seen).toStrictEqual([
      {
        kind: 'service-endpoint',
        connection: 'default',
        className: 'S',
        methodName: 'a',
        service: { name: 'svc', version: '1.0.0' },
        endpoint: 'a',
        subject: 'svc.a',
      },
    ]);
    expect(msg.respond).toHaveBeenCalledTimes(1);
    await adapter.stop();
    await adapter.idle();
  });
  it('forceStop has nothing to force', async () => {
    const { entry, endpoint } = setup();
    const adapter = new NatsServiceAdapter({ name: 'svc', version: '1' }, [endpoint('a')], entry);
    await adapter.start();
    await adapter.stop();
    await adapter.idle();
    expect(adapter.forceStop()).toEqual({ nakked: 0, cancelledRetries: 0 });
  });
});
