import type { DlqPolicy } from './dlq.js';
import { backoffDelay, type RetryPolicy } from './retry.js';

export type SettleDecision =
  | { action: 'retry'; delayMs: number }
  | { action: 'dlq' }
  | { action: 'drop' };

/**
 * What to do with a failed delivery: retry while the error is retryable and `attempt`
 * (1-based delivery count) has not exceeded `maxRetries`; otherwise dead-letter or drop.
 */
export function decide(
  err: unknown,
  attempt: number,
  retry: RetryPolicy | false,
  dlq: DlqPolicy | false,
  random?: () => number,
): SettleDecision {
  if (retry !== false && retry.isRetryable(err) && attempt <= retry.maxRetries) {
    return { action: 'retry', delayMs: backoffDelay(retry, attempt, random) };
  }
  return dlq ? { action: 'dlq' } : { action: 'drop' };
}
