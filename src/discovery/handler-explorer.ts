import { Injectable } from '@nestjs/common';
import { ROUTE_ARGS_METADATA } from '@nestjs/common/constants.js';
import { DiscoveryService, ExternalContextCreator, MetadataScanner } from '@nestjs/core';
import type { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import { createTypeRouter } from '../cloudevents/matcher.js';
import type { NatsBatchContext } from '../context/nats-batch-context.js';
import type { NatsContext } from '../context/nats-context.js';
import type { CloudEventRouterOptions } from '../decorators/cloudevent.decorators.js';
import {
  NATS_CLOUD_EVENT_ROUTER_METADATA,
  NATS_CONSUME_METADATA,
  NATS_ENDPOINT_METADATA,
  NATS_ON_CLOUD_EVENT_METADATA,
  NATS_SERVICE_METADATA,
  NATS_SUBSCRIBE_METADATA,
} from '../decorators/constants.js';
import type {
  ConsumeOptions,
  NatsServiceOptions,
  ServiceEndpointOptions,
  SubscribeOptions,
} from '../decorators/handler.decorators.js';
import { NatsConfigError, NatsModuleError } from '../errors/errors.js';
import { DEFAULT_CONNECTION } from '../module/tokens.js';
import { NatsParamsFactory } from './nats-params-factory.js';

export type HandlerKind = 'subscribe' | 'consume' | 'service-endpoint';

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
  | { kind: 'consume'; options: ConsumeOptions }
  | {
      kind: 'service-endpoint';
      endpoint: string;
      options: ServiceEndpointOptions;
      /** `undefined` when the class lacks `@NatsService`; `validateHandlers` rejects it. */
      service: NatsServiceOptions | undefined;
    };

export type HandlerDefinition = HandlerBase & HandlerSpec;

export const ROUTER_METHOD_NAME = '@CloudEventRouter';

const UNMATCHED_POLICIES: ReadonlyArray<CloudEventRouting['unmatched']> = ['ack', 'term', 'dlq'];

/**
 * Finds `@Subscribe`, `@Consume`, `@NatsService`/`@ServiceEndpoint` handlers and
 * `@CloudEventRouter` classes (one subscribe/consume definition per class).
 */
@Injectable()
export class HandlerExplorer {
  constructor(
    private readonly discovery: DiscoveryService,
    private readonly scanner: MetadataScanner,
    private readonly ecc: ExternalContextCreator,
  ) {}

  /**
   * Returns every handler of every provider/controller. Request-scoped and transient
   * providers, and `@ServiceEndpoint` methods of classes without `@NatsService`, are
   * included (with their current instance) so validation can reject them.
   * `useExisting` aliases are skipped and an instance reachable through several wrappers
   * (other modules, other tokens) is explored once. Throws a `NatsConfigError` for an
   * invalid `@CloudEventRouter`/`@OnCloudEvent` setup.
   */
  explore(): HandlerDefinition[] {
    const wrappers = [...this.discovery.getProviders(), ...this.discovery.getControllers()];
    const seen = new Set<object>();
    const out: HandlerDefinition[] = [];
    for (const wrapper of wrappers) {
      const { instance, metatype } = wrapper;
      if (wrapper.isAlias || !instance || typeof instance !== 'object' || !metatype) continue;
      if (seen.has(instance)) continue;
      seen.add(instance);
      const service: NatsServiceOptions | undefined = Reflect.getMetadata(
        NATS_SERVICE_METADATA,
        instance.constructor,
      );
      const routerOptions: CloudEventRouterOptions | undefined = Reflect.getMetadata(
        NATS_CLOUD_EVENT_ROUTER_METADATA,
        instance.constructor,
      );
      const routes: Array<{ pattern: string; methodName: string }> = [];
      const prototype = Object.getPrototypeOf(instance) as object;
      for (const methodName of this.scanner.getAllMethodNames(prototype)) {
        const method = instance[methodName as keyof typeof instance];
        const specs = this.specsFor(method, service);
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
              `@OnCloudEvent ${label} cannot also be a @Subscribe/@Consume/@ServiceEndpoint handler`,
            );
          }
          for (const pattern of patterns) routes.push({ pattern, methodName });
        }
        for (const spec of specs) {
          out.push(this.define(wrapper, instance, methodName, spec, service));
        }
      }
      if (routerOptions) out.push(this.defineRouter(wrapper, instance, routerOptions, routes));
    }
    return out;
  }

  private patternsFor(method: unknown): string[] {
    if (typeof method !== 'function') return [];
    return Reflect.getMetadata(NATS_ON_CLOUD_EVENT_METADATA, method) ?? [];
  }

  /** The single subscribe/consume definition of a `@CloudEventRouter` class. */
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
    const hasSubscribe = 'subscribe' in options && options.subscribe !== undefined;
    const hasConsume = 'consume' in options && options.consume !== undefined;
    if (hasSubscribe === hasConsume) {
      throw new NatsConfigError(`${label} must set exactly one of subscribe or consume`);
    }
    const { connection, concurrency, codec, retry, dlq, idempotency } = options;
    const common = defined({ connection, concurrency, codec, retry, dlq, idempotency });
    let spec: HandlerSpec;
    if (hasSubscribe) {
      const { subscribe, queue } = options as Extract<
        CloudEventRouterOptions,
        { subscribe: string }
      >;
      spec = {
        kind: 'subscribe',
        subject: subscribe,
        options: { ...common, ...defined({ queue }) },
      };
    } else {
      const { consume } = options as Extract<CloudEventRouterOptions, { consume: unknown }>;
      spec = {
        kind: 'consume',
        options: {
          ...common,
          stream: consume.stream,
          durable: consume.durable,
          ...defined({ config: consume.config }),
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

  private specsFor(method: unknown, service: NatsServiceOptions | undefined): HandlerSpec[] {
    if (typeof method !== 'function') return [];
    const specs: HandlerSpec[] = [];
    const sub: { subject: string; options?: SubscribeOptions } | undefined = Reflect.getMetadata(
      NATS_SUBSCRIBE_METADATA,
      method,
    );
    if (sub) specs.push({ kind: 'subscribe', subject: sub.subject, options: sub.options ?? {} });
    const consume: { options: ConsumeOptions } | undefined = Reflect.getMetadata(
      NATS_CONSUME_METADATA,
      method,
    );
    if (consume) specs.push({ kind: 'consume', options: consume.options });
    const endpoint: { name: string; options?: ServiceEndpointOptions } | undefined =
      Reflect.getMetadata(NATS_ENDPOINT_METADATA, method);
    if (endpoint) {
      specs.push({
        kind: 'service-endpoint',
        endpoint: endpoint.name,
        options: endpoint.options ?? {},
        service,
      });
    }
    return specs;
  }

  private define(
    wrapper: InstanceWrapper,
    instance: object,
    methodName: string,
    spec: HandlerSpec,
    service: NatsServiceOptions | undefined,
  ): HandlerDefinition {
    const invoke = this.wrap(instance, methodName);
    const connection =
      ('connection' in spec.options ? spec.options.connection : undefined) ??
      service?.connection ??
      DEFAULT_CONNECTION;
    return {
      ...spec,
      connection,
      className: instance.constructor.name,
      methodName,
      instance,
      wrapper,
      invoke,
    };
  }

  /** Wraps a method with Nest guards, pipes, interceptors and exception filters. */
  private wrap(instance: object, methodName: string): Invoke {
    const callback = (instance as Record<string, (...args: unknown[]) => unknown>)[methodName];
    return this.ecc.create(
      instance,
      callback as (...args: unknown[]) => unknown,
      methodName,
      ROUTE_ARGS_METADATA,
      new NatsParamsFactory(),
      undefined,
      undefined,
      { guards: true, interceptors: true, filters: true },
      'nats',
    ) as Invoke;
  }
}

/** `obj` without the keys whose value is `undefined`. */
function defined<T extends object>(obj: T): Partial<T> {
  return Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== undefined)) as Partial<T>;
}
