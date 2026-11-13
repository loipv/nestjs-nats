import { describe, expect, it } from 'vitest';
import { NatsConfigError } from '../../../src/errors/errors.js';
import { resolveRestartPolicy } from '../../../src/settlement/restart.js';

const DEFAULTS = {
  initialDelay: 1000,
  factor: 2,
  maxDelay: 30_000,
  jitter: 0.2,
  maxAttempts: Number.POSITIVE_INFINITY,
};

describe('resolveRestartPolicy', () => {
  it('defaults when undefined', () => {
    expect(resolveRestartPolicy(undefined, 'c')).toEqual(DEFAULTS);
  });

  it('is false when false', () => {
    expect(resolveRestartPolicy(false, 'c')).toBe(false);
  });

  it('merges given fields over defaults', () => {
    expect(resolveRestartPolicy({ maxAttempts: 3 }, 'c')).toEqual({ ...DEFAULTS, maxAttempts: 3 });
    expect(resolveRestartPolicy({ factor: undefined, maxAttempts: 3 }, 'c')).toEqual({
      ...DEFAULTS,
      maxAttempts: 3,
    });
  });

  it.each([
    ['initialDelay', { initialDelay: -1 }],
    ['initialDelay', { initialDelay: Number.NaN }],
    ['maxDelay', { maxDelay: 2 ** 31 }],
    ['initialDelay', { initialDelay: 5000, maxDelay: 1000 }],
    ['factor', { factor: 0.5 }],
    ['factor', { factor: Number.POSITIVE_INFINITY }],
    ['jitter', { jitter: 1.5 }],
    ['maxAttempts', { maxAttempts: 0 }],
    ['maxAttempts', { maxAttempts: 1.5 }],
  ])('rejects %s', (field, opts) => {
    let error: unknown;
    try {
      resolveRestartPolicy(opts, 'c');
    } catch (err) {
      error = err;
    }
    expect(error).toBeInstanceOf(NatsConfigError);
    expect((error as Error).message).toContain('NATS connection "c"');
    expect((error as Error).message).toContain(field);
  });

  it('accepts maxAttempts Infinity and jitter 0', () => {
    expect(resolveRestartPolicy({ maxAttempts: Number.POSITIVE_INFINITY, jitter: 0 }, 'c')).toEqual(
      { ...DEFAULTS, jitter: 0 },
    );
  });
});
