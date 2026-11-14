import { describe, expect, it } from 'vitest';
import { errorMessage, errorTrace, isNoResponders } from '../../../src/util/errors.js';

describe('errorMessage', () => {
  it('uses the message of an Error', () => expect(errorMessage(new Error('boom'))).toBe('boom'));
  it('stringifies anything else', () => {
    expect(errorMessage('plain')).toBe('plain');
    expect(errorMessage(42)).toBe('42');
    expect(errorMessage(undefined)).toBe('undefined');
  });
});

describe('errorTrace', () => {
  it('prefers the stack of an Error', () => {
    const err = new Error('boom');
    expect(errorTrace(err)).toBe(err.stack);
  });
  it('falls back to the message when there is no stack', () => {
    const err = new Error('boom');
    err.stack = undefined;
    expect(errorTrace(err)).toBe('boom');
  });
  it('stringifies anything else', () => expect(errorTrace('plain')).toBe('plain'));
});

describe('isNoResponders', () => {
  const named = () => Object.assign(new Error('no responders'), { name: 'NoRespondersError' });

  it('recognizes a NoRespondersError by class', async () => {
    const { NoRespondersError } = await import('@nats-io/transport-node');
    expect(isNoResponders(new NoRespondersError('x'))).toBe(true);
  });

  it('recognizes a NoRespondersError by name (another copy of the library)', () =>
    expect(isNoResponders(named())).toBe(true));

  it('looks through the cause chain', () =>
    expect(
      isNoResponders(new Error('outer', { cause: new Error('mid', { cause: named() }) })),
    ).toBe(true));

  it('is false for other errors and non-errors', () => {
    expect(isNoResponders(new Error('timeout'))).toBe(false);
    expect(isNoResponders('NoRespondersError')).toBe(false);
    expect(isNoResponders(undefined)).toBe(false);
  });
});
