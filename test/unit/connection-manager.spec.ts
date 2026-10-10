import { inspect } from 'node:util';
import type { NatsConnection } from '@nats-io/transport-node';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { NatsConnectionManager } from '../../src/connection/connection-manager.js';
import { NatsConnectionError } from '../../src/errors/errors.js';

/** Every own property (enumerable or not) of `err` and of its cause chain, as text. */
function ownProps(err: unknown, depth = 0): string {
  if (!(err instanceof Error) || depth > 10) return String(err);
  const own = Object.getOwnPropertyNames(err).map((key) => {
    const value = (err as unknown as Record<string, unknown>)[key];
    return key === 'cause' ? ownProps(value, depth + 1) : String(value);
  });
  return own.join('\n');
}

describe('NatsConnectionManager connect errors', () => {
  it('never exposes the password of a malformed server URL, even through the cause', async () => {
    const mgr = new NatsConnectionManager();
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
    const mgr = new NatsConnectionManager();
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
    const mgr = new NatsConnectionManager();
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

const never = () => new Promise<never>(() => {});

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

class FakeConnectionManager extends NatsConnectionManager {
  constructor(private readonly fake: unknown) {
    super();
  }

  protected override openConnection(): Promise<NatsConnection> {
    return Promise.resolve(this.fake as NatsConnection);
  }
}

describe('NatsConnectionManager closeAll', () => {
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
