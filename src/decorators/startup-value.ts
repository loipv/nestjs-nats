import type { Type } from '@nestjs/common';
import { NatsConfigError } from '../errors/errors.js';
import { errorMessage } from '../util/errors.js';

/**
 * A decorator argument, or a function called once at application startup that returns it.
 * Decorator arguments are evaluated when the class is imported, before configuration such
 * as `.env` is loaded; a function reads it once the application is created.
 */
export type StartupValue<T> = T | ((ctx: StartupContext) => T);

/** What a startup function receives. */
export interface StartupContext {
  /** Any provider of the application, such as `ConfigService` (looked up in every module). */
  get<T>(token: Type<T> | string | symbol): T;
}

/**
 * Internal: `value` itself, or what it returns when it is a function called with `ctx`.
 * Throws a `NatsConfigError` that names `subject` and `what` (e.g. `NATS handler Orders.on`,
 * `@Consume options`) when the function throws or returns a Promise.
 */
export function resolveStartupValue<T>(
  value: StartupValue<T>,
  ctx: StartupContext,
  subject: string,
  what: string,
): T {
  if (typeof value !== 'function') return value;
  let resolved: T;
  try {
    resolved = (value as (ctx: StartupContext) => T)(ctx);
  } catch (err) {
    throw new NatsConfigError(`${subject} failed to resolve its ${what}: ${errorMessage(err)}`, {
      cause: err,
    });
  }
  if (isThenable(resolved)) {
    throw new NatsConfigError(
      `${subject}: the ${what} function returned a Promise; return the value itself (asynchronous configuration belongs in NatsModule.forRootAsync)`,
    );
  }
  return resolved;
}

function isThenable(value: unknown): boolean {
  return (
    (typeof value === 'object' || typeof value === 'function') &&
    value !== null &&
    typeof (value as { then?: unknown }).then === 'function'
  );
}
