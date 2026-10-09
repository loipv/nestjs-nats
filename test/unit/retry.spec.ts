import {
  BadRequestException,
  ForbiddenException,
  InternalServerErrorException,
} from '@nestjs/common';
import { describe, expect, it } from 'vitest';
import { NatsDecodeError, NonRetryableError } from '../../src/errors/errors.js';
import {
  backoffDelay,
  DEFAULT_RETRY,
  defaultIsRetryable,
  resolveRetryPolicy,
} from '../../src/settlement/retry.js';

describe('resolveRetryPolicy', () => {
  it('uses the spec defaults', () =>
    expect(resolveRetryPolicy()).toMatchObject({
      maxRetries: 5,
      initialDelay: 1000,
      factor: 2,
      maxDelay: 60000,
      jitter: 0.2,
    }));
  it('defaults isRetryable', () =>
    expect((resolveRetryPolicy() as { isRetryable: unknown }).isRetryable).toBe(
      defaultIsRetryable,
    ));
  it('handler false disables', () =>
    expect(resolveRetryPolicy({ maxRetries: 9 }, false)).toBe(false));
  it('module false disables unless handler opts in', () => {
    expect(resolveRetryPolicy(false)).toBe(false);
    expect(resolveRetryPolicy(false, { maxRetries: 2 })).toMatchObject({
      maxRetries: 2,
      initialDelay: 1000,
    });
  });
  it('handler overrides module', () =>
    expect(resolveRetryPolicy({ maxRetries: 3, factor: 3 }, { maxRetries: 1 })).toMatchObject({
      maxRetries: 1,
      factor: 3,
    }));
});

describe('backoffDelay', () => {
  it.each([
    [1, 1000],
    [2, 2000],
    [3, 4000],
    [7, 60000],
    [20, 60000],
  ])('attempt %i = %i without jitter', (n, ms) =>
    expect(backoffDelay({ ...DEFAULT_RETRY, jitter: 0 }, n)).toBe(ms),
  );
  it('applies jitter bounds', () => {
    expect(backoffDelay(DEFAULT_RETRY, 1, () => 0)).toBe(800);
    expect(backoffDelay(DEFAULT_RETRY, 1, () => 1)).toBe(1200);
  });
});

describe('defaultIsRetryable', () => {
  it.each([
    [new NatsDecodeError('x'), false],
    [new NonRetryableError('x'), false],
    [new BadRequestException(), false],
    [new ForbiddenException(), false],
    [new InternalServerErrorException(), true],
    [new Error('x'), true],
    ['str', true],
  ])('defaultIsRetryable(%s) = %s', (err, expected) =>
    expect(defaultIsRetryable(err)).toBe(expected),
  );
});
