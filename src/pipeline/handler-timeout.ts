import type { LoggerService } from '@nestjs/common';
import { MAX_TIMER_DELAY } from '../batch/collector.js';
import { NatsConfigError, NatsHandlerTimeoutError } from '../errors/errors.js';
import { errorMessage, errorTrace } from '../util/errors.js';

/**
 * The timeout of a handler: its own `timeout`, else the module's `handlerTimeout`; `false`
 * (none) when neither is set. `false` on the handler turns off the module's.
 */
export function resolveHandlerTimeout(
  module: number | false | undefined,
  handler: number | false | undefined,
): number | false {
  return handler ?? module ?? false;
}

/**
 * Throws a `NatsConfigError` unless `value` is `undefined`, `false` or a number of ms in
 * (0, 2^31 - 1]. `subject` names its owner (`NATS handler Orders.on`, `NATS connection "x"`).
 */
export function assertTimeout(
  value: unknown,
  subject: string,
  field: 'timeout' | 'handlerTimeout',
): void {
  if (value === undefined || value === false) return;
  if (typeof value === 'number' && value > 0 && value <= MAX_TIMER_DELAY) return;
  throw new NatsConfigError(
    `${subject} has invalid ${field} ${String(value)}; expected a number of ms in (0, ${MAX_TIMER_DELAY}] or false`,
  );
}

export interface InvokeWithinOptions {
  /** In ms. */
  timeout: number;
  /** `Class.method`, for the error and the log lines. */
  handler: string;
  logger: LoggerService;
  /** Called once when the timeout fires, with the error the call rejects with. */
  abort: (reason: NatsHandlerTimeoutError) => void;
  /**
   * Called once when the timeout fires, with the run still going on: it resolves `true` when
   * the run finishes, `false` when it fails, and never rejects.
   */
  linger: (run: Promise<boolean>) => void;
}

/**
 * Runs `call` for at most `timeout` ms. A run that settles in time settles the returned
 * promise. Otherwise the promise rejects with a `NatsHandlerTimeoutError`: `abort` and
 * `linger` are called, and how the run ends later is logged (its result is ignored).
 */
export function invokeWithin<T>(call: () => Promise<T>, o: InvokeWithinOptions): Promise<T> {
  let run: Promise<T>;
  try {
    run = Promise.resolve(call());
  } catch (err) {
    return Promise.reject(err);
  }
  return new Promise<T>((resolve, reject) => {
    let expired = false;
    const timer = setTimeout(() => {
      expired = true;
      const error = new NatsHandlerTimeoutError(o.handler, o.timeout);
      const since = Date.now();
      const late = () => `${Date.now() - since} ms after its ${o.timeout} ms timeout`;
      o.abort(error);
      o.linger(
        run.then(
          () => {
            o.logger.warn(`Handler ${o.handler} finished ${late()}; its result was ignored`);
            return true;
          },
          (err: unknown) => {
            // A handler that stopped on `ctx.signal` did what it should: not an error.
            if (err === error || (err as { name?: unknown } | null)?.name === 'AbortError') {
              o.logger.warn(`Handler ${o.handler} stopped ${late()}`);
            } else {
              o.logger.error(
                `Handler ${o.handler} failed ${late()}: ${errorMessage(err)}`,
                errorTrace(err),
              );
            }
            return false;
          },
        ),
      );
      reject(error);
    }, o.timeout);
    // The run itself keeps the process alive if it has to; the timer must not.
    timer.unref();
    run.then(
      (value) => {
        if (expired) return;
        clearTimeout(timer);
        resolve(value);
      },
      (err: unknown) => {
        if (expired) return;
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}
