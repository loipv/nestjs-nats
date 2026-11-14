import { inspect } from 'node:util';
import type { NatsConnection } from '@nats-io/transport-node';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { jsonCodec } from '../../../src/codec/codec.js';
import { ConnectionRegistry } from '../../../src/connection/connection-registry.js';
import { NatsConnectionError, NatsModuleError } from '../../../src/errors/errors.js';
import { FakeNatsConnection } from '../../../src/testing/fake-connection.js';
import { never } from '../../support/async.js';

/** Every own property (enumerable or not) of `err` and of its cause chain, as text. */
function ownProps(err: unknown, depth = 0): string {
  if (!(err instanceof Error) || depth > 10) return String(err);
  const own = Object.getOwnPropertyNames(err).map((key) => {
    const value = (err as unknown as Record<string, unknown>)[key];
    return key === 'cause' ? ownProps(value, depth + 1) : String(value);
  });
  return own.join('\n');
}

describe('ConnectionRegistry connect errors', () => {
  it('never exposes the password of a malformed server URL, even through the cause', async () => {
    const mgr = new ConnectionRegistry();
    const err = await mgr
      .connect('bad', {
        connection: { servers: 'nats://user:s3cretpw@bad host:4222' },
        logger: false,
      })
      .then(
        () => undefined,
        (e: unknown) => e,
      );
    expect(err).toBeInstanceOf(NatsConnectionError);
    const e = err as NatsConnectionError;
    expect(e.cause).toBeInstanceOf(Error);
    expect((e.cause as Error & { code?: string }).code).toBe('ERR_INVALID_URL');
    expect((e.cause as Error).name).toBe('TypeError');
    expect(inspect(e, { depth: 10 })).not.toContain('s3cretpw');
    expect(JSON.stringify(e)).not.toContain('s3cretpw');
    expect(JSON.stringify(e.cause)).not.toContain('s3cretpw');
    expect(ownProps(e)).not.toContain('s3cretpw');
    expect(mgr.has('bad')).toBe(false);
  });

  it('masks a password containing a space, which the URL pattern alone misses', async () => {
    const mgr = new ConnectionRegistry();
    const err = await mgr
      .connect('bad', { connection: { servers: 'nats://user:s3cret pw@h:1' }, logger: false })
      .then(
        () => undefined,
        (e: unknown) => e,
      );
    expect(err).toBeInstanceOf(NatsConnectionError);
    expect(inspect(err, { depth: 10 })).not.toContain('s3cret pw');
    expect(ownProps(err)).not.toContain('s3cret pw');
  });

  it('masks the pass and token options in the cause', async () => {
    const mgr = new ConnectionRegistry();
    const err = (await mgr
      .connect('bad', {
        // The token also appears in a malformed URL, so the raw error would carry it.
        connection: { servers: 'nats://bad host:1/tok-s3cret', token: 'tok-s3cret' },
        logger: false,
      })
      .then(
        () => undefined,
        (e: unknown) => e,
      )) as NatsConnectionError;
    expect(err).toBeInstanceOf(NatsConnectionError);
    expect(ownProps(err)).not.toContain('tok-s3cret');
    expect(inspect(err, { depth: 10 })).not.toContain('tok-s3cret');
  });
});

/** A connection whose status stream yields `events`, then fails with `failure` if given. */
function statusConnection(events: unknown[], failure?: Error) {
  return {
    status: () => ({
      async *[Symbol.asyncIterator]() {
        yield* events;
        if (failure) throw failure;
      },
    }),
    isClosed: () => true,
    isDraining: () => false,
    closed: () => Promise.resolve(),
  };
}

describe('ConnectionRegistry status logging', () => {
  const SECRET = 'tok-s3cret';

  async function logged(events: unknown[], failure?: Error) {
    const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
    const mgr = new FakeConnectionManager(statusConnection(events, failure));
    await mgr.connect('orders', {
      connection: { servers: `nats://user:pw-${SECRET}@h:4222`, token: SECRET },
      logger,
    });
    // Let the status loop run to its end.
    await new Promise((r) => setImmediate(r));
    return logger;
  }

  it('masks credentials in a server error', async () => {
    const logger = await logged([
      {
        type: 'error',
        error: new Error(`'Authorization Violation' for ${SECRET} at nats://u:p@h`),
      },
    ]);
    expect(logger.error).toHaveBeenCalledExactlyOnceWith(
      `NATS connection "orders" server error: 'Authorization Violation' for *** at nats://***@h`,
    );
  });

  it('masks credentials when the status stream fails', async () => {
    const logger = await logged([], new Error(`stream broke: pw-${SECRET}`));
    expect(logger.error).toHaveBeenCalledExactlyOnceWith(
      'NATS connection "orders" status stream failed: stream broke: ***',
    );
  });

  it('masks credentials when the connection fails to close', async () => {
    const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
    const mgr = new FakeConnectionManager({
      ...statusConnection([]),
      isClosed: () => false,
      drain: () => Promise.reject(new Error(`drain failed for ${SECRET}`)),
    });
    await mgr.connect('orders', { connection: { token: SECRET }, logger });
    await mgr.closeAll();
    expect(logger.error).toHaveBeenCalledExactlyOnceWith(
      'NATS connection "orders" failed to close cleanly: drain failed for ***',
    );
  });

  it('warns about a slow consumer, naming its subject and pending messages', async () => {
    const logger = await logged([
      { type: 'slowConsumer', sub: { getSubject: () => 'orders.*' }, pending: 10_001 },
    ]);
    expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
      'NATS connection "orders" subscription "orders.*" is a slow consumer: 10001 messages pending',
    );
  });
});

/** A connection that never finishes draining nor closing (e.g. a dead socket). */
function stuckConnection() {
  return {
    status: () => ({ [Symbol.asyncIterator]: () => ({ next: never }) }),
    isClosed: () => false,
    isDraining: () => false,
    drain: vi.fn(never),
    close: vi.fn(never),
    closed: never,
  };
}

class FakeConnectionManager extends ConnectionRegistry {
  constructor(private readonly fake: unknown) {
    super();
  }

  protected override openConnection(): Promise<NatsConnection> {
    return Promise.resolve(this.fake as NatsConnection);
  }
}

describe('ConnectionRegistry closeAll', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('bounds a forced close that never settles and logs that it was abandoned', async () => {
    const nc = stuckConnection();
    const mgr = new FakeConnectionManager(nc);
    const warn = vi.fn();
    await mgr.connect('stuck', {
      connection: {},
      logger: { log() {}, error() {}, warn },
    });
    let done = false;
    const closing = mgr.closeAll({ timeoutMs: 100 }).then(() => {
      done = true;
    });
    await vi.advanceTimersByTimeAsync(100);
    expect(nc.close).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(expect.stringMatching(/did not drain within 100ms/));
    expect(done).toBe(false);
    await vi.advanceTimersByTimeAsync(1000);
    await closing;
    expect(done).toBe(true);
    expect(warn).toHaveBeenLastCalledWith(
      expect.stringMatching(/"stuck" did not close within 1000ms; abandoned/),
    );
  });
});

describe('ConnectionRegistry abortStartup', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('does not wait for a connection still opening, and closes it once it opens', async () => {
    let open!: (nc: NatsConnection) => void;
    class SlowConnectionManager extends ConnectionRegistry {
      protected override openConnection(): Promise<NatsConnection> {
        // Like `waitOnFirstConnect` against a server that is not up: settles only when it is.
        return new Promise((resolve) => {
          open = resolve;
        });
      }
    }
    const mgr = new SlowConnectionManager();
    let settled = false;
    void mgr.connect('a', { connection: {}, logger: false }).then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    let aborted = false;
    void mgr.abortStartup().then(() => {
      aborted = true;
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(aborted).toBe(true);

    const nc = { ...stuckConnection(), close: vi.fn(() => Promise.resolve()) };
    open(nc as unknown as NatsConnection);
    await vi.advanceTimersByTimeAsync(0);
    expect(nc.close).toHaveBeenCalledTimes(1);
    expect(mgr.has('a')).toBe(false);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(settled).toBe(false);
  });

  it('gives the connections already open 1 second to drain, not the default 10', async () => {
    const nc = stuckConnection();
    const mgr = new FakeConnectionManager(nc);
    const warn = vi.fn();
    await mgr.connect('stuck', { connection: {}, logger: { log() {}, error() {}, warn } });
    let aborted = false;
    void mgr.abortStartup().then(() => {
      aborted = true;
    });
    await vi.advanceTimersByTimeAsync(1000);
    expect(nc.drain).toHaveBeenCalledTimes(1);
    expect(nc.close).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(expect.stringMatching(/did not drain within 1000ms/));
    await vi.advanceTimersByTimeAsync(1000);
    expect(aborted).toBe(true);
  });
});

describe('ConnectionRegistry handler states', () => {
  async function open(...names: string[]) {
    const mgr = new FakeConnectionManager(new FakeNatsConnection(jsonCodec()));
    for (const n of names) await mgr.connect(n, { connection: {}, logger: false });
    return mgr;
  }

  it('tracks handler states per connection, one entry per call', async () => {
    const mgr = await open('default', 'other');
    const setA = mgr.trackHandler('default', 'A');
    mgr.trackHandler('default', 'A');
    mgr.trackHandler('other', 'B');
    expect(mgr.handlerStates('default')).toEqual([
      { handler: 'A', state: 'running' },
      { handler: 'A', state: 'running' },
    ]);
    setA('failed');
    expect(mgr.handlerStates('default')).toEqual([
      { handler: 'A', state: 'failed' },
      { handler: 'A', state: 'running' },
    ]);
    expect(mgr.handlerStates('other')).toEqual([{ handler: 'B', state: 'running' }]);
    expect(mgr.handlerStates('nope')).toEqual([]);
    expect(() => mgr.trackHandler('nope', 'A')).toThrow(NatsModuleError);
    mgr.clearHandlers();
    expect(mgr.handlerStates('default')).toEqual([]);
    expect(mgr.handlerStates('other')).toEqual([]);
  });
});
