import { type ArgumentsHost, Injectable } from '@nestjs/common';
import { ROUTE_ARGS_METADATA } from '@nestjs/common/constants.js';
import { DiscoveryService, ExternalContextCreator, MetadataScanner, ModuleRef } from '@nestjs/core';
import type { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import { createTypeRouter } from '../cloudevents/matcher.js';
import type { NatsBatchContext } from '../context/nats-batch-context.js';
import type { NatsContext } from '../context/nats-context.js';
import type { CloudEventRouterOptions } from '../decorators/cloudevent.decorators.js';
import {
  NATS_CLOUD_EVENT_ROUTER_METADATA,
  NATS_CONSUME_METADATA,
  NATS_CONSUME_ORDERED_METADATA,
  NATS_CONSUME_PUSH_METADATA,
  NATS_ENDPOINT_METADATA,
  NATS_ON_CLOUD_EVENT_METADATA,
  NATS_SERVICE_METADATA,
  NATS_SUBSCRIBE_METADATA,
} from '../decorators/constants.js';
import type {
  ConsumeOptions,
  ConsumeOrderedOptions,
  ConsumePushOptions,
  HandlerDisabled,
  NatsServiceOptions,
  ServiceEndpointOptions,
  SubscribeOptions,
} from '../decorators/handler.decorators.js';
import {
  resolveStartupValue,
  type StartupContext,
  type StartupValue,
} from '../decorators/startup-value.js';
import { NatsConfigError, NatsModuleError } from '../errors/errors.js';
import { DEFAULT_CONNECTION } from '../module/tokens.js';
import { errorMessage } from '../util/errors.js';
import { NatsArgumentsHost } from './nats-arguments-host.js';
import { NatsParamsFactory } from './nats-params-factory.js';

export type HandlerKind = 'subscribe' | 'consume' | 'ordered' | 'service-endpoint';

type Invoke = (payload: unknown, ctx: NatsContext | NatsBatchContext) => Promise<unknown>;

/** The `@OnCloudEvent` routes of a `@CloudEventRouter` class. */
export interface CloudEventRouting {
  /** The method handling CloudEvents of `type`, `undefined` when none matches. */
  route: (type: string) => { methodName: string; invoke: Invoke } | undefined;
  unmatched: 'ack' | 'term' | 'dlq';
}

interface HandlerBase {
  connection: string;
  className: string;
  /** The method name; `'@CloudEventRouter'` for a CloudEvent router. */
  methodName: string;
  instance: object;
  wrapper: InstanceWrapper;
  /**
   * Calls the handler through Nest guards, pipes, interceptors and exception filters. A
   * CloudEvent router has none of its own: the invoker calls the route's (`router`).
   */
  invoke: Invoke;
  /** Set on the definition of a `@CloudEventRouter` class. */
  router?: CloudEventRouting;
}

type HandlerSpec =
  | { kind: 'subscribe'; subject: string; options: SubscribeOptions }
  | { kind: 'consume'; options: ConsumeOptions | ConsumePushOptions; delivery: 'pull' | 'push' }
  | { kind: 'ordered'; options: ConsumeOrderedOptions }
  | {
      kind: 'service-endpoint';
      endpoint: string;
      options: ServiceEndpointOptions;
      /** `undefined` when the class lacks `@NatsService`; `validateHandlers` rejects it. */
      service: NatsServiceOptions | undefined;
    };

export type HandlerDefinition = HandlerBase & HandlerSpec;

/** A handler skipped by its `disabled` option (or its `@NatsService`'s). */
export interface DisabledHandler {
  kind: HandlerKind;
  className: string;
  /** The method name; `'@CloudEventRouter'` for a CloudEvent router. */
  methodName: string;
  connection: string;
}

export interface ExploreOptions {
  /** Called for each handler skipped because it is disabled. */
  onDisabled?: (handler: DisabledHandler) => void;
}

export const ROUTER_METHOD_NAME = '@CloudEventRouter';

const UNMATCHED_POLICIES: ReadonlyArray<CloudEventRouting['unmatched']> = ['ack', 'term', 'dlq'];

/** What a startup function returned, or threw. */
type Resolved = { ok: true; value: unknown } | { ok: false; error: unknown };

const ORDERED_KEYS: readonly string[] = [
  'stream',
  'filterSubjects',
  'deliverPolicy',
  'headersOnly',
];

/**
 * Finds `@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered`,
 * `@NatsService`/`@ServiceEndpoint` handlers and `@CloudEventRouter` classes (one
 * subscribe, pull, push or ordered definition per class).
 */
@Injectable()
export class HandlerExplorer {
  constructor(
    private readonly discovery: DiscoveryService,
    private readonly scanner: MetadataScanner,
    private readonly ecc: ExternalContextCreator,
    private readonly moduleRef: ModuleRef,
  ) {}

  /** What startup functions receive: any provider of the application. */
  private readonly ctx: StartupContext = {
    get: (token) => this.moduleRef.get(token, { strict: false }),
  };

  /** What each startup function returned or threw, so it runs once per application. */
  private readonly resolved = new WeakMap<object, Resolved>();

  /**
   * Returns every handler of every provider/controller. Request-scoped and transient
   * providers, and `@ServiceEndpoint` methods of classes without `@NatsService`, are
   * included (with their current instance) so validation can reject them.
   * `useExisting` aliases are skipped and an instance reachable through several wrappers
   * (other modules, other tokens) is explored once. Disabled handlers are left out (and
   * reported to `onDisabled`). Startup functions (decorator arguments and `disabled`) run on
   * the first call only: later calls reuse what they returned or threw. Throws a
   * `NatsConfigError` for an invalid `@CloudEventRouter`/`@OnCloudEvent` setup, decorator
   * argument or `disabled`.
   */
  explore(options: ExploreOptions = {}): HandlerDefinition[] {
    const wrappers = [...this.discovery.getProviders(), ...this.discovery.getControllers()];
    const seen = new Set<object>();
    const out: HandlerDefinition[] = [];
    for (const wrapper of wrappers) {
      const { instance, metatype } = wrapper;
      if (wrapper.isAlias || !instance || typeof instance !== 'object' || !metatype) continue;
      if (seen.has(instance)) continue;
      seen.add(instance);
      const className = instance.constructor.name;
      const service = this.classOptions<NatsServiceOptions>(
        NATS_SERVICE_METADATA,
        instance.constructor,
        '@NatsService',
      );
      const routerOptions = this.classOptions<CloudEventRouterOptions>(
        NATS_CLOUD_EVENT_ROUTER_METADATA,
        instance.constructor,
        '@CloudEventRouter',
      );
      const serviceDisabled =
        service !== undefined && this.isDisabled(service.disabled, `@NatsService ${className}`);
      const routes: Array<{ pattern: string; methodName: string }> = [];
      const prototype = Object.getPrototypeOf(instance) as object;
      for (const methodName of this.scanner.getAllMethodNames(prototype)) {
        const method = instance[methodName as keyof typeof instance];
        const specs = this.specsFor(method, service, `NATS handler ${className}.${methodName}`);
        const patterns = this.patternsFor(method);
        if (patterns.length > 0) {
          const label = `${instance.constructor.name}.${methodName}`;
          if (!routerOptions) {
            throw new NatsConfigError(
              `@OnCloudEvent ${label} is declared in a class without @CloudEventRouter`,
            );
          }
          if (specs.length > 0) {
            throw new NatsConfigError(
              `@OnCloudEvent ${label} cannot also be a @Subscribe/@Consume/@ConsumePush/@ConsumeOrdered/@ServiceEndpoint handler`,
            );
          }
          for (const pattern of patterns) routes.push({ pattern, methodName });
        }
        for (const spec of specs) {
          const disabled =
            this.isDisabled(spec.options.disabled, `NATS handler ${className}.${methodName}`) ||
            (spec.kind === 'service-endpoint' && serviceDisabled);
          if (disabled) {
            options.onDisabled?.({
              kind: spec.kind,
              className,
              methodName,
              connection: connectionOf(spec, service),
            });
            continue;
          }
          out.push(this.define(wrapper, instance, methodName, spec, service));
        }
      }
      if (!routerOptions) continue;
      if (this.isDisabled(routerOptions.disabled, `@CloudEventRouter ${className}`)) {
        options.onDisabled?.({
          kind: routerKind(routerOptions),
          className,
          methodName: ROUTER_METHOD_NAME,
          connection: routerOptions.connection ?? DEFAULT_CONNECTION,
        });
        continue;
      }
      out.push(this.defineRouter(wrapper, instance, routerOptions, routes));
    }
    return out;
  }

  private patternsFor(method: unknown): string[] {
    if (typeof method !== 'function') return [];
    return Reflect.getMetadata(NATS_ON_CLOUD_EVENT_METADATA, method) ?? [];
  }

  /** The single subscribe/consume/ordered definition of a `@CloudEventRouter` class. */
  private defineRouter(
    wrapper: InstanceWrapper,
    instance: object,
    options: CloudEventRouterOptions,
    routes: Array<{ pattern: string; methodName: string }>,
  ): HandlerDefinition {
    const className = instance.constructor.name;
    const label = `@CloudEventRouter ${className}`;
    if (routes.length === 0) throw new NatsConfigError(`${label} has no @OnCloudEvent method`);
    const unmatched = options.unmatched ?? 'ack';
    if (!UNMATCHED_POLICIES.includes(unmatched)) {
      throw new NatsConfigError(
        `${label} has invalid unmatched ${JSON.stringify(unmatched)}; expected 'ack', 'term' or 'dlq'`,
      );
    }
    // Not part of the options type; rejected for untyped callers (spec §5: no batch).
    for (const key of ['batch', 'ack'] as const) {
      if (key in options && (options as Record<string, unknown>)[key] !== undefined) {
        throw new NatsConfigError(
          `${label} does not support ${key}: a router handles one CloudEvent at a time and settles it automatically`,
        );
      }
    }
    const set = (['subscribe', 'consume', 'consumePush', 'consumeOrdered'] as const).filter(
      (key) => key in options && (options as Record<string, unknown>)[key] !== undefined,
    );
    if (set.length !== 1) {
      throw new NatsConfigError(
        `${label} must set exactly one of subscribe, consume, consumePush or consumeOrdered`,
      );
    }
    const { connection, concurrency, codec, retry, dlq, idempotency, timeout } = options;
    if (set[0] !== 'subscribe' && 'queue' in options && options.queue !== undefined) {
      throw new NatsConfigError(`${label} sets queue, which only applies to subscribe`);
    }
    if (set[0] === 'consumeOrdered' && concurrency !== undefined) {
      throw new NatsConfigError(`${label} with consumeOrdered does not support concurrency`);
    }
    const common = defined({ connection, concurrency, codec, retry, dlq, idempotency, timeout });
    let spec: HandlerSpec;
    if (set[0] === 'subscribe') {
      const { subscribe, queue } = options as Extract<
        CloudEventRouterOptions,
        { subscribe: string }
      >;
      spec = {
        kind: 'subscribe',
        subject: subscribe,
        options: { ...common, ...defined({ queue }) },
      };
    } else if (set[0] === 'consume') {
      const { consume } = options as Extract<CloudEventRouterOptions, { consume: unknown }>;
      spec = {
        kind: 'consume',
        delivery: 'pull',
        options: {
          ...common,
          stream: consume.stream,
          durable: consume.durable,
          ...defined({ config: consume.config }),
        },
      };
    } else if (set[0] === 'consumePush') {
      const { consumePush } = options as Extract<CloudEventRouterOptions, { consumePush: unknown }>;
      spec = {
        kind: 'consume',
        delivery: 'push',
        options: {
          ...common,
          stream: consumePush.stream,
          durable: consumePush.durable,
          ...defined({ config: consumePush.config, deliverGroup: consumePush.deliverGroup }),
        },
      };
    } else {
      const { consumeOrdered } = options as Extract<
        CloudEventRouterOptions,
        { consumeOrdered: unknown }
      >;
      // Not part of the options type; rejected for untyped callers.
      for (const key of Object.keys(consumeOrdered)) {
        if (
          !ORDERED_KEYS.includes(key) &&
          (consumeOrdered as Record<string, unknown>)[key] !== undefined
        ) {
          throw new NatsConfigError(`${label}: consumeOrdered does not support ${key}`);
        }
      }
      spec = {
        kind: 'ordered',
        options: {
          ...common,
          stream: consumeOrdered.stream,
          ...defined({
            filterSubjects: consumeOrdered.filterSubjects,
            deliverPolicy: consumeOrdered.deliverPolicy,
            headersOnly: consumeOrdered.headersOnly,
          }),
        },
      };
    }
    // One Nest-wrapped invoker per method, shared by its patterns.
    const targets = new Map<string, { methodName: string; invoke: Invoke }>();
    for (const { methodName } of routes) {
      if (!targets.has(methodName)) {
        targets.set(methodName, { methodName, invoke: this.wrap(instance, methodName) });
      }
    }
    let route: CloudEventRouting['route'];
    try {
      route = createTypeRouter(
        routes.map(({ pattern, methodName }) => ({
          pattern,
          target: targets.get(methodName) as { methodName: string; invoke: Invoke },
        })),
      );
    } catch (err) {
      // Name the router in the invalid/duplicate pattern error.
      if (err instanceof NatsConfigError) {
        throw new NatsConfigError(`${label}: ${err.message}`, { cause: err });
      }
      throw err;
    }
    return {
      ...spec,
      connection: connection ?? DEFAULT_CONNECTION,
      className,
      methodName: ROUTER_METHOD_NAME,
      instance,
      wrapper,
      invoke: () =>
        Promise.reject(new NatsModuleError(`${label} is invoked through its @OnCloudEvent routes`)),
      router: { route, unmatched },
    };
  }

  /**
   * The handler specs of a method's decorators, their startup functions resolved. `subject`
   * (e.g. `NATS handler Orders.on`) names the method in errors.
   */
  private specsFor(
    method: unknown,
    service: NatsServiceOptions | undefined,
    subject: string,
  ): HandlerSpec[] {
    if (typeof method !== 'function') return [];
    const specs: HandlerSpec[] = [];
    const sub:
      | { subject: StartupValue<string>; options?: StartupValue<SubscribeOptions | undefined> }
      | undefined = Reflect.getMetadata(NATS_SUBSCRIBE_METADATA, method);
    if (sub) {
      specs.push({
        kind: 'subscribe',
        subject: this.resolveString(sub.subject, subject, '@Subscribe subject'),
        options: this.resolveOptions(sub.options, subject, '@Subscribe options', true) ?? {},
      });
    }
    const consume: { options: StartupValue<ConsumeOptions> } | undefined = Reflect.getMetadata(
      NATS_CONSUME_METADATA,
      method,
    );
    if (consume) {
      specs.push({
        kind: 'consume',
        delivery: 'pull',
        options: this.resolveOptions(consume.options, subject, '@Consume options'),
      });
    }
    const push: { options: StartupValue<ConsumePushOptions> } | undefined = Reflect.getMetadata(
      NATS_CONSUME_PUSH_METADATA,
      method,
    );
    if (push) {
      specs.push({
        kind: 'consume',
        delivery: 'push',
        options: this.resolveOptions(push.options, subject, '@ConsumePush options'),
      });
    }
    const ordered: { options: StartupValue<ConsumeOrderedOptions> } | undefined =
      Reflect.getMetadata(NATS_CONSUME_ORDERED_METADATA, method);
    if (ordered) {
      specs.push({
        kind: 'ordered',
        options: this.resolveOptions(ordered.options, subject, '@ConsumeOrdered options'),
      });
    }
    const endpoint:
      | {
          name: StartupValue<string>;
          options?: StartupValue<ServiceEndpointOptions | undefined>;
        }
      | undefined = Reflect.getMetadata(NATS_ENDPOINT_METADATA, method);
    if (endpoint) {
      specs.push({
        kind: 'service-endpoint',
        endpoint: this.resolveString(endpoint.name, subject, '@ServiceEndpoint name'),
        options:
          this.resolveOptions(endpoint.options, subject, '@ServiceEndpoint options', true) ?? {},
        service,
      });
    }
    return specs;
  }

  /**
   * `value`, or what its function returns: called once per application, its result (or
   * error) kept. `subject` and `what` name it in errors.
   */
  private resolve<T>(value: StartupValue<T>, subject: string, what: string): T {
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
  private classOptions<T extends object>(
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
  private isDisabled(value: HandlerDisabled | undefined, label: string): boolean {
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
  private resolveString(value: StartupValue<string>, subject: string, what: string): string {
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
  private resolveOptions<T extends object>(
    value: StartupValue<T>,
    subject: string,
    what: string,
  ): T;
  private resolveOptions<T extends object>(
    value: StartupValue<T | undefined> | undefined,
    subject: string,
    what: string,
    optional: true,
  ): T | undefined;
  private resolveOptions<T extends object>(
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

  private define(
    wrapper: InstanceWrapper,
    instance: object,
    methodName: string,
    spec: HandlerSpec,
    service: NatsServiceOptions | undefined,
  ): HandlerDefinition {
    const invoke = this.wrap(instance, methodName);
    return {
      ...spec,
      connection: connectionOf(spec, service),
      className: instance.constructor.name,
      methodName,
      instance,
      wrapper,
      invoke,
    };
  }

  /** Wraps a method with Nest guards, pipes, interceptors and exception filters. */
  private wrap(instance: object, methodName: string): Invoke {
    const callback = (instance as Record<string, (...args: unknown[]) => unknown>)[methodName] as (
      ...args: unknown[]
    ) => unknown;
    const filters = this.exceptionFilters(instance, callback);
    const target = this.ecc.create(
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
  private exceptionFilters(
    instance: object,
    callback: (...args: unknown[]) => unknown,
  ): ExceptionFilters | undefined {
    const creator = (this.ecc as unknown as { filtersContextCreator?: FiltersContextCreator })
      .filtersContextCreator;
    if (typeof creator?.create !== 'function') return undefined;
    const filters = creator.create(
      instance,
      callback,
      this.ecc.getContextModuleKey(instance.constructor),
    );
    return typeof filters?.invokeCustomFilters === 'function' ? filters : undefined;
  }
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

/** The connection a handler receives from. */
function connectionOf(spec: HandlerSpec, service: NatsServiceOptions | undefined): string {
  return (
    ('connection' in spec.options ? spec.options.connection : undefined) ??
    service?.connection ??
    DEFAULT_CONNECTION
  );
}

/** The kind of the definition a router would have; `'subscribe'` when it sets no source. */
function routerKind(options: CloudEventRouterOptions): HandlerKind {
  if ('consume' in options || 'consumePush' in options) return 'consume';
  if ('consumeOrdered' in options) return 'ordered';
  return 'subscribe';
}

/** The error for a startup function that returned `value` instead of `expected`. */
function invalidValue(subject: string, what: string, value: unknown, expected: string) {
  return new NatsConfigError(
    `${subject}: ${what} resolved to ${describeValue(value)}; expected ${expected}`,
  );
}

function describeValue(value: unknown): string {
  return typeof value === 'function' ? 'function' : (JSON.stringify(value) ?? String(value));
}

/** `obj` without the keys whose value is `undefined`. */
function defined<T extends object>(obj: T): Partial<T> {
  return Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== undefined)) as Partial<T>;
}
