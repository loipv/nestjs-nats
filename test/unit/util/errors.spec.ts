import { describe, expect, it } from 'vitest';
import { errorMessage, errorTrace } from '../../../src/util/errors.js';

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
