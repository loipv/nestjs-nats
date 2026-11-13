import { describe, expect, it, vi } from 'vitest';
import { resolveStartupValue, type StartupContext } from '../../../src/decorators/startup-value.js';
import { NatsConfigError } from '../../../src/errors/errors.js';

const SUBJECT = 'NATS handler Orders.on';
const WHAT = '@Consume options';

function context(): StartupContext & { get: ReturnType<typeof vi.fn> } {
  return { get: vi.fn() } as StartupContext & { get: ReturnType<typeof vi.fn> };
}

function failure(run: () => unknown): NatsConfigError {
  try {
    run();
  } catch (err) {
    expect(err).toBeInstanceOf(NatsConfigError);
    return err as NatsConfigError;
  }
  throw new Error('expected resolveStartupValue to throw');
}

describe('resolveStartupValue', () => {
  it.each([['a.b'], [{ stream: 'S' }], [undefined]])('returns %j as it is', (value) => {
    const ctx = context();
    expect(resolveStartupValue(value, ctx, SUBJECT, WHAT)).toBe(value);
    expect(ctx.get).not.toHaveBeenCalled();
  });

  it('calls a function with the context and returns what it returns', () => {
    const ctx = context();
    const value = vi.fn(() => ({ stream: 'S' }));
    expect(resolveStartupValue(value, ctx, SUBJECT, WHAT)).toEqual({ stream: 'S' });
    expect(value).toHaveBeenCalledExactlyOnceWith(ctx);
  });

  it('wraps what the function throws in a NatsConfigError naming the handler', () => {
    const cause = new Error('no config');
    const err = failure(() =>
      resolveStartupValue(
        () => {
          throw cause;
        },
        context(),
        SUBJECT,
        WHAT,
      ),
    );
    expect(err.message).toBe(
      'NATS handler Orders.on failed to resolve its @Consume options: no config',
    );
    expect(err.cause).toBe(cause);
  });

  it.each([
    ['a Promise', () => Promise.resolve({})],
    // biome-ignore lint/suspicious/noThenProperty: the thenable is the input under test
    ['a thenable', () => ({ then() {} })],
  ])('rejects a function returning %s', (_, value) => {
    const err = failure(() => resolveStartupValue(value, context(), SUBJECT, WHAT));
    expect(err.message).toBe(
      'NATS handler Orders.on: the @Consume options function returned a Promise; return the value itself (asynchronous configuration belongs in NatsModule.forRootAsync)',
    );
  });
});
