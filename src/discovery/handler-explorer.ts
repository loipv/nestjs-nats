import { Injectable } from '@nestjs/common';
import { ROUTE_ARGS_METADATA } from '@nestjs/common/constants.js';
import { DiscoveryService, ExternalContextCreator, MetadataScanner } from '@nestjs/core';
import type { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import type { NatsContext } from '../context/nats-context.js';
import {
  NATS_CONSUME_METADATA,
  NATS_ENDPOINT_METADATA,
  NATS_SERVICE_METADATA,
  NATS_SUBSCRIBE_METADATA,
} from '../decorators/constants.js';
import type {
  ConsumeOptions,
  NatsServiceOptions,
  ServiceEndpointOptions,
  SubscribeOptions,
} from '../decorators/handler.decorators.js';
import { DEFAULT_CONNECTION } from '../module/tokens.js';
import { NatsParamsFactory } from './nats-params-factory.js';

export type HandlerKind = 'subscribe' | 'consume' | 'service-endpoint';

interface HandlerBase {
  connection: string;
  className: string;
  methodName: string;
  instance: object;
  wrapper: InstanceWrapper;
  /** Calls the handler through Nest guards, pipes, interceptors and exception filters. */
  invoke: (payload: unknown, ctx: NatsContext) => Promise<unknown>;
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

/** Finds `@Subscribe`, `@Consume` and `@NatsService`/`@ServiceEndpoint` handlers. */
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
   * (other modules, other tokens) is explored once.
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
      const prototype = Object.getPrototypeOf(instance) as object;
      for (const methodName of this.scanner.getAllMethodNames(prototype)) {
        for (const spec of this.specsFor(instance[methodName as keyof typeof instance], service)) {
          out.push(this.define(wrapper, instance, methodName, spec, service));
        }
      }
    }
    return out;
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
    const callback = (instance as Record<string, (...args: unknown[]) => unknown>)[methodName];
    const invoke = this.ecc.create(
      instance,
      callback as (...args: unknown[]) => unknown,
      methodName,
      ROUTE_ARGS_METADATA,
      new NatsParamsFactory(),
      undefined,
      undefined,
      { guards: true, interceptors: true, filters: true },
      'nats',
    ) as (payload: unknown, ctx: NatsContext) => Promise<unknown>;
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
}
