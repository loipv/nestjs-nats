import { HttpException } from '@nestjs/common';
import { NatsDecodeError, NonRetryableError } from '../errors/errors.js';
import { withoutUndefined } from '../util/objects.js';

export interface RetryOptions {
  maxRetries?: number;
  initialDelay?: number;
  factor?: number;
  maxDelay?: number;
  jitter?: number;
  isRetryable?: (err: unknown) => boolean;
}

export interface RetryPolicy {
  maxRetries: number;
  initialDelay: number;
  factor: number;
  maxDelay: number;
  jitter: number;
  isRetryable: (err: unknown) => boolean;
}

export const DEFAULT_RETRY: Readonly<Omit<RetryPolicy, 'isRetryable'>> = Object.freeze({
  maxRetries: 5,
  initialDelay: 1000,
  factor: 2,
  maxDelay: 60_000,
  jitter: 0.2,
});

/** `false` for decode errors, `NonRetryableError` and HTTP 4xx exceptions; `true` otherwise. */
export function defaultIsRetryable(err: unknown): boolean {
  if (err instanceof NatsDecodeError || err instanceof NonRetryableError) return false;
  if (err instanceof HttpException) {
    const status = err.getStatus();
    return !(status >= 400 && status < 500);
  }
  return true;
}

/**
 * Merges the retry options (defaults, then module, then handler). `false` on the handler
 * disables retries; `false` on the module disables them unless the handler opts in. Keys
 * set to `undefined` are ignored.
 */
export function resolveRetryPolicy(
  module?: RetryOptions | false,
  handler?: RetryOptions | false,
): RetryPolicy | false {
  if (handler === false) return false;
  if (handler === undefined && module === false) return false;
  const merged = {
    ...DEFAULT_RETRY,
    ...withoutUndefined(module || {}),
    ...withoutUndefined(handler ?? {}),
  };
  return { ...merged, isRetryable: merged.isRetryable ?? defaultIsRetryable };
}

/** Delay before redelivery number `attempt` (>= 1): exponential, capped, with symmetric jitter. */
export function backoffDelay(
  policy: Pick<RetryPolicy, 'initialDelay' | 'factor' | 'maxDelay' | 'jitter'>,
  attempt: number,
  random: () => number = Math.random,
): number {
  const base = Math.min(policy.maxDelay, policy.initialDelay * policy.factor ** (attempt - 1));
  return Math.round(base * (1 + policy.jitter * (2 * random() - 1)));
}
