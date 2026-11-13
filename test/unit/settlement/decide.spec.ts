import { BadRequestException } from '@nestjs/common';
import { describe, expect, it } from 'vitest';
import { decide } from '../../../src/settlement/decide.js';
import type { DlqPolicy } from '../../../src/settlement/dlq.js';
import { DEFAULT_RETRY, defaultIsRetryable } from '../../../src/settlement/retry.js';

const retry = { ...DEFAULT_RETRY, jitter: 0, isRetryable: defaultIsRetryable };
const dlqOn: DlqPolicy = { subject: () => 'dlq.x', mode: 'core' };

describe('decide', () => {
  it.each([
    ['retryable, attempt 1', new Error('x'), 1, retry, false, { action: 'retry', delayMs: 1000 }],
    [
      'retryable, attempt 5 (= maxRetries)',
      new Error('x'),
      5,
      retry,
      false,
      { action: 'retry', delayMs: 16000 },
    ],
    ['exhausted, dlq on', new Error('x'), 6, retry, dlqOn, { action: 'dlq' }],
    ['exhausted, dlq off', new Error('x'), 6, retry, false, { action: 'drop' }],
    ['non-retryable, dlq on', new BadRequestException(), 1, retry, dlqOn, { action: 'dlq' }],
    ['non-retryable, dlq off', new BadRequestException(), 1, retry, false, { action: 'drop' }],
    ['retry disabled', new Error('x'), 1, false, false, { action: 'drop' }],
    ['retry disabled, dlq on', new Error('x'), 1, false, dlqOn, { action: 'dlq' }],
  ] as const)('%s', (_, err, attempt, r, d, expected) =>
    expect(decide(err, attempt, r, d)).toEqual(expected),
  );

  it('passes random to the backoff', () =>
    expect(decide(new Error('x'), 1, { ...retry, jitter: 0.2 }, false, () => 1)).toEqual({
      action: 'retry',
      delayMs: 1200,
    }));
});
