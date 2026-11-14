import type { ModuleRef } from '@nestjs/core';
import type { HandlerDisabled } from '../decorators/handler.decorators.js';
import {
  resolveStartupValue,
  type StartupContext,
  type StartupValue,
} from '../decorators/startup-value.js';
import { NatsConfigError } from '../errors/errors.js';
import { errorMessage } from '../util/errors.js';

/** What a startup function returned, or threw. */
type Resolved = { ok: true; value: unknown } | { ok: false; error: unknown };

/**
 * Resolves the startup values of handler decorators (their arguments and `disabled`): a
 * function is called with the application's providers once per resolver, and what it
 * returned or threw is kept, so a function shared by several decorators gives them all one
 * value.
 */
export class StartupResolver {
  /** What startup functions receive: any provider of the application. */
  private readonly ctx: StartupContext;

  /** What each startup function returned or threw, so it runs once per application. */
  private readonly resolved = new WeakMap<object, Resolved>();

  constructor(moduleRef: ModuleRef) {
    this.ctx = { get: (token) => moduleRef.get(token, { strict: false }) };
  }

  /**
   * `value`, or what its function returns: called once per application, its result (or
   * error) kept. `subject` and `what` name it in errors.
   */
  resolve<T>(value: StartupValue<T>, subject: string, what: string): T {
    if (typeof value !== 'function') return value;
    return this.once(value, () => resolveStartupValue(value, this.ctx, subject, what));
  }

  /** What `run` returns or throws, run on the first call for `fn` only. */
  private once<T>(fn: object, run: () => T): T {
    let outcome = this.resolved.get(fn);
    if (!outcome) {
      try {
        outcome = { ok: true, value: run() };
      } catch (error) {
        outcome = { ok: false, error };
      }
      this.resolved.set(fn, outcome);
    }
    if (!outcome.ok) throw outcome.error;
    return outcome.value as T;
  }

  /**
   * The options of a class decorator stored under `key` on `target`, resolved; `undefined`
   * when the class lacks it. `name` (e.g. `'@NatsService'`) names it in errors.
   */
  classOptions<T extends object>(
    key: string,
    target: abstract new (...args: never) => unknown,
    name: string,
  ): T | undefined {
    const value: StartupValue<T> | undefined = Reflect.getMetadata(key, target);
    if (value === undefined) return undefined;
    return this.resolveOptions(value, `${name} ${target.name}`, `${name} options`);
  }

  /**
   * Resolves a `disabled` option: `false` when unset, the boolean, or what the function
   * returns (called once per application, with the providers). Throws a `NatsConfigError`
   * naming `label` when the function throws or the value is not a boolean.
   */
  isDisabled(value: HandlerDisabled | undefined, label: string): boolean {
    if (value === undefined) return false;
    const resolved: unknown =
      typeof value === 'function'
        ? this.once(value, () => {
            try {
              return value(this.ctx);
            } catch (err) {
              throw new NatsConfigError(
                `${label} failed to resolve disabled: ${errorMessage(err)}`,
                { cause: err },
              );
            }
          })
        : value;
    if (typeof resolved !== 'boolean') {
      throw new NatsConfigError(
        `${label} has invalid disabled ${describeValue(resolved)}; expected a boolean or a function returning one`,
      );
    }
    return resolved;
  }

  /** A string argument; what a function returns must be a non-empty string. */
  resolveString(value: StartupValue<string>, subject: string, what: string): string {
    if (typeof value !== 'function') return value;
    const resolved: unknown = this.resolve(value, subject, what);
    if (typeof resolved !== 'string' || resolved === '') {
      throw invalidValue(subject, what, resolved, 'a non-empty string');
    }
    return resolved;
  }

  /**
   * An options argument; what a function returns must be an object, or `undefined` when the
   * options are `optional`.
   */
  resolveOptions<T extends object>(value: StartupValue<T>, subject: string, what: string): T;
  resolveOptions<T extends object>(
    value: StartupValue<T | undefined> | undefined,
    subject: string,
    what: string,
    optional: true,
  ): T | undefined;
  resolveOptions<T extends object>(
    value: StartupValue<T | undefined> | undefined,
    subject: string,
    what: string,
    optional = false,
  ): T | undefined {
    if (typeof value !== 'function') return value;
    const resolved: unknown = this.resolve(value, subject, what);
    if (resolved === undefined && optional) return undefined;
    if (typeof resolved !== 'object' || resolved === null || Array.isArray(resolved)) {
      throw invalidValue(subject, what, resolved, 'an object');
    }
    return resolved as T;
  }
}

/** The error for a startup function that returned `value` instead of `expected`. */
function invalidValue(subject: string, what: string, value: unknown, expected: string) {
  return new NatsConfigError(
    `${subject}: ${what} resolved to ${describeResolved(value)}; expected ${expected}`,
  );
}

/**
 * A value a startup function returned, for an error: a primitive as JSON, an object, array or
 * function by its kind only, so a provider returned by mistake (with its secrets) is not
 * printed.
 */
function describeResolved(value: unknown): string {
  if (typeof value === 'bigint') return `${value}n`;
  if (typeof value === 'function') return 'a function';
  if (Array.isArray(value)) return 'an array';
  if (typeof value === 'object' && value !== null) return 'an object';
  return JSON.stringify(value) ?? String(value);
}

function describeValue(value: unknown): string {
  return typeof value === 'function' ? 'function' : (JSON.stringify(value) ?? String(value));
}
