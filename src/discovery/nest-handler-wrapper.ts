import type { ArgumentsHost } from '@nestjs/common';
import { ROUTE_ARGS_METADATA } from '@nestjs/common/constants.js';
import type { ExternalContextCreator } from '@nestjs/core';
import type { Invoke } from './handler-definition.js';
import { NatsArgumentsHost } from './nats-arguments-host.js';
import { NatsParamsFactory } from './nats-params-factory.js';

/** Wraps a method with Nest guards, pipes, interceptors and exception filters. */
export function wrapHandler(
  ecc: ExternalContextCreator,
  instance: object,
  methodName: string,
): Invoke {
  const callback = (instance as Record<string, (...args: unknown[]) => unknown>)[methodName] as (
    ...args: unknown[]
  ) => unknown;
  const filters = exceptionFilters(ecc, instance, callback);
  const target = ecc.create(
    instance,
    callback,
    methodName,
    ROUTE_ARGS_METADATA,
    new NatsParamsFactory(),
    undefined,
    undefined,
    { guards: true, interceptors: true, filters: !filters },
    'nats',
  ) as Invoke;
  if (!filters) return target;
  return async (payload, ctx) => {
    try {
      return await target(payload, ctx);
    } catch (err) {
      // Like Nest: a filter handles the error by returning a value (or throwing its own).
      const handled = filters.invokeCustomFilters(err, new NatsArgumentsHost([payload, ctx]));
      if (handled) return handled;
      throw err;
    }
  };
}

/**
 * The exception filters of a method, without Nest's `ExceptionsHandler`, which logs every
 * error they do not handle that the library logs as well. They are reached through a private
 * field of `ExternalContextCreator`; `undefined` if a Nest version does not have it, so Nest
 * handles errors itself.
 */
function exceptionFilters(
  ecc: ExternalContextCreator,
  instance: object,
  callback: (...args: unknown[]) => unknown,
): ExceptionFilters | undefined {
  const creator = (ecc as unknown as { filtersContextCreator?: FiltersContextCreator })
    .filtersContextCreator;
  if (typeof creator?.create !== 'function') return undefined;
  const filters = creator.create(instance, callback, ecc.getContextModuleKey(instance.constructor));
  return typeof filters?.invokeCustomFilters === 'function' ? filters : undefined;
}

/** Nest's `ExternalExceptionsHandler`, as far as the library uses it. */
interface ExceptionFilters {
  /** What the first filter catching `err` returns, `null` when none catches it. */
  invokeCustomFilters(err: unknown, host: ArgumentsHost): unknown;
}

/** Nest's `ExternalExceptionFilterContext`, as far as the library uses it. */
interface FiltersContextCreator {
  create(
    instance: object,
    callback: (...args: unknown[]) => unknown,
    moduleKey: string,
  ): ExceptionFilters | undefined;
}
