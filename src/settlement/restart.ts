import { NatsConfigError } from '../errors/errors.js';

export interface RestartOptions {
  /** Delay before the first restart attempt (ms, default 1000). */
  initialDelay?: number;
  /** Growth of the delay between attempts (>= 1, default 2). */
  factor?: number;
  /** Cap of the delay (ms, default 30_000). */
  maxDelay?: number;
  /** Symmetric jitter ratio of the delay, 0 to 1 (default 0.2). */
  jitter?: number;
  /** Attempts before giving up (integer >= 1, default `Infinity`). */
  maxAttempts?: number;
}

export interface RestartPolicy {
  initialDelay: number;
  factor: number;
  maxDelay: number;
  jitter: number;
  maxAttempts: number;
}

export const DEFAULT_RESTART: Readonly<RestartPolicy> = Object.freeze({
  initialDelay: 1000,
  factor: 2,
  maxDelay: 30_000,
  jitter: 0.2,
  maxAttempts: Number.POSITIVE_INFINITY,
});

/** The largest delay a timer accepts (2^31-1 ms). */
const MAX_TIMER_DELAY = 2 ** 31 - 1;

/** `opts` without the keys set to `undefined`, so they keep their default. */
function defined(opts: RestartOptions): RestartOptions {
  return Object.fromEntries(
    Object.entries(opts).filter(([, value]) => value !== undefined),
  ) as RestartOptions;
}

/**
 * Merges the restart options over the defaults and validates them. `false` disables
 * restarts. Keys set to `undefined` are ignored.
 */
export function resolveRestartPolicy(
  opts: RestartOptions | false | undefined,
  connection: string,
): RestartPolicy | false {
  if (opts === false) return false;
  const policy = { ...DEFAULT_RESTART, ...defined(opts ?? {}) };
  const invalid = (field: keyof RestartPolicy, rule: string): never => {
    throw new NatsConfigError(
      `NATS connection "${connection}" restart.${field} must be ${rule}, got ${policy[field]}`,
    );
  };
  for (const field of ['initialDelay', 'maxDelay'] as const) {
    const value = policy[field];
    if (!Number.isFinite(value) || value < 0 || value > MAX_TIMER_DELAY) {
      invalid(field, `a number of ms between 0 and ${MAX_TIMER_DELAY} (2^31-1)`);
    }
  }
  if (policy.initialDelay > policy.maxDelay) {
    invalid('initialDelay', `at most restart.maxDelay (${policy.maxDelay})`);
  }
  if (!Number.isFinite(policy.factor) || policy.factor < 1) {
    invalid('factor', 'a finite number >= 1');
  }
  if (!(policy.jitter >= 0 && policy.jitter <= 1)) {
    invalid('jitter', 'a number between 0 and 1');
  }
  const { maxAttempts } = policy;
  if (
    maxAttempts !== Number.POSITIVE_INFINITY &&
    !(Number.isInteger(maxAttempts) && maxAttempts >= 1)
  ) {
    invalid('maxAttempts', 'an integer >= 1 or Infinity');
  }
  return policy;
}
