import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { NatsConfigError, NatsHandlerTimeoutError } from '../../../src/errors/errors.js';
import {
  assertTimeout,
  invokeWithin,
  resolveHandlerTimeout,
} from '../../../src/pipeline/handler-timeout.js';
import { mockLogger } from '../../support/logger.js';

describe('resolveHandlerTimeout', () => {
  it.each([
    [undefined, undefined, false],
    [false, undefined, false],
    [5000, undefined, 5000],
    [5000, false, false],
    [5000, 100, 100],
    [false, 100, 100],
    [undefined, 100, 100],
  ] as const)('module %s, handler %s → %s', (module, handler, expected) => {
    expect(resolveHandlerTimeout(module, handler)).toBe(expected);
  });
});

describe('assertTimeout', () => {
  it.each([undefined, false, 1, 2147483647])('accepts %s', (value) => {
    expect(() => assertTimeout(value, 'NATS handler Orders.on', 'timeout')).not.toThrow();
  });

  it.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY, 2147483648, '5', true])(
    'rejects %s',
    (value) => {
      const check = () => assertTimeout(value, 'NATS handler Orders.on', 'timeout');
      expect(check).toThrow(NatsConfigError);
      expect(check).toThrow(
        `NATS handler Orders.on has invalid timeout ${String(value)}; expected a number of ms in (0, 2147483647] or false`,
      );
    },
  );

  it('names the field and the subject it is given', () => {
    expect(() => assertTimeout(0, 'NATS connection "default"', 'handlerTimeout')).toThrow(
      'NATS connection "default" has invalid handlerTimeout 0; expected a number of ms in (0, 2147483647] or false',
    );
  });
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** Runs `call` through `invokeWithin` with a 5000 ms timeout for `Orders.on`. */
function within<T>(call: () => Promise<T>) {
  const logger = mockLogger();
  const abort = vi.fn();
  let lingering: Promise<boolean> | undefined;
  const linger = vi.fn((run: Promise<boolean>) => {
    lingering = run;
  });
  const result = invokeWithin(call, { timeout: 5000, handler: 'Orders.on', logger, abort, linger });
  return { result, logger, abort, linger, lingering: () => lingering as Promise<boolean> };
}

describe('invokeWithin', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('settles with the result of a run that finishes 1 ms before the timeout', async () => {
    const t = within(() => new Promise<string>((resolve) => setTimeout(() => resolve('ok'), 4999)));
    await vi.advanceTimersByTimeAsync(4999);
    await expect(t.result).resolves.toBe('ok');
    expect(t.abort).not.toHaveBeenCalled();
    expect(t.linger).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('settles with the error of a run that fails in time, leaving no timer', async () => {
    const boom = new Error('boom');
    const t = within(() => Promise.reject(boom));
    await expect(t.result).rejects.toBe(boom);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('settles with the error of a call that throws synchronously, leaving no timer', async () => {
    const boom = new Error('boom');
    const t = within(() => {
      throw boom;
    });
    await expect(t.result).rejects.toBe(boom);
    expect(t.abort).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('fails a run still pending at the timeout, aborting it once and handing it over', async () => {
    const run = deferred<string>();
    const t = within(() => run.promise);
    const failed = expect(t.result).rejects.toBeInstanceOf(NatsHandlerTimeoutError);
    await vi.advanceTimersByTimeAsync(4999);
    expect(t.abort).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await failed;
    const err = await t.result.catch((e: unknown) => e);
    expect(err).toMatchObject({ handler: 'Orders.on', timeout: 5000 });
    expect(t.abort).toHaveBeenCalledExactlyOnceWith(err);
    expect(t.linger).toHaveBeenCalledOnce();
  });

  describe('a run that outlived its timeout', () => {
    async function timedOut<T>(run: Promise<T>) {
      const t = within(() => run);
      const failed = t.result.catch((e: unknown) => e);
      await vi.advanceTimersByTimeAsync(5000);
      return { ...t, error: (await failed) as NatsHandlerTimeoutError };
    }

    it('resolves its lingering run true and warns when it finishes', async () => {
      const run = deferred<string>();
      const t = await timedOut(run.promise);
      await vi.advanceTimersByTimeAsync(1234);
      run.resolve('late');
      await expect(t.lingering()).resolves.toBe(true);
      expect(t.logger.warn).toHaveBeenCalledExactlyOnceWith(
        'Handler Orders.on finished 1234 ms after its 5000 ms timeout; its result was ignored',
      );
      expect(t.logger.error).not.toHaveBeenCalled();
    });

    it('warns that it stopped when it rejects with the timeout error', async () => {
      const run = deferred<string>();
      const t = await timedOut(run.promise);
      await vi.advanceTimersByTimeAsync(1234);
      run.reject(t.error);
      await expect(t.lingering()).resolves.toBe(false);
      expect(t.logger.warn).toHaveBeenCalledExactlyOnceWith(
        'Handler Orders.on stopped 1234 ms after its 5000 ms timeout',
      );
      expect(t.logger.error).not.toHaveBeenCalled();
    });

    it('warns that it stopped when it rejects with an AbortError', async () => {
      const run = deferred<string>();
      const t = await timedOut(run.promise);
      await vi.advanceTimersByTimeAsync(1234);
      run.reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
      await expect(t.lingering()).resolves.toBe(false);
      expect(t.logger.warn).toHaveBeenCalledExactlyOnceWith(
        'Handler Orders.on stopped 1234 ms after its 5000 ms timeout',
      );
      expect(t.logger.error).not.toHaveBeenCalled();
    });

    it('logs an error, with the stack, when it fails otherwise', async () => {
      const run = deferred<string>();
      const t = await timedOut(run.promise);
      await vi.advanceTimersByTimeAsync(1234);
      const boom = new Error('boom');
      run.reject(boom);
      await expect(t.lingering()).resolves.toBe(false);
      expect(t.logger.error).toHaveBeenCalledExactlyOnceWith(
        'Handler Orders.on failed 1234 ms after its 5000 ms timeout: boom',
        boom.stack,
      );
    });

    it('logs a rejection that is not an Error', async () => {
      const run = deferred<string>();
      const t = await timedOut(run.promise);
      run.reject('plain');
      await expect(t.lingering()).resolves.toBe(false);
      expect(t.logger.error).toHaveBeenCalledOnce();
      expect(t.logger.error.mock.calls[0]?.[0]).toMatch(/: plain$/);
    });
  });
});
