import { Injectable } from '@nestjs/common';
import { DiscoveryService, ExternalContextCreator, MetadataScanner, ModuleRef } from '@nestjs/core';
import type { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import type { CloudEventRouterOptions } from '../decorators/cloudevent.decorators.js';
import {
  NATS_CLOUD_EVENT_ROUTER_METADATA,
  NATS_CONSUME_METADATA,
  NATS_CONSUME_ORDERED_METADATA,
  NATS_CONSUME_PUSH_METADATA,
  NATS_ENDPOINT_METADATA,
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
import type { StartupValue } from '../decorators/startup-value.js';
import { NatsConfigError } from '../errors/errors.js';
import { DEFAULT_CONNECTION } from '../module/tokens.js';
import { cloudEventPatterns, defineRouter, routerKind } from './cloud-event-router.js';
import {
  type DisabledSpec,
  type ExploreOptions,
  type HandlerDefinition,
  type HandlerKind,
  type HandlerSpec,
  ROUTER_METHOD_NAME,
} from './handler-definition.js';
import { wrapHandler } from './nest-handler-wrapper.js';
import { StartupResolver } from './startup-resolver.js';

/**
 * Finds `@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered`,
 * `@NatsService`/`@ServiceEndpoint` handlers and `@CloudEventRouter` classes (one
 * subscribe, pull, push or ordered definition per class).
 */
@Injectable()
export class HandlerExplorer {
  /** Resolves decorator startup values: each function runs once per application. */
  private readonly startup: StartupResolver;

  constructor(
    private readonly discovery: DiscoveryService,
    private readonly scanner: MetadataScanner,
    private readonly ecc: ExternalContextCreator,
    moduleRef: ModuleRef,
  ) {
    this.startup = new StartupResolver(moduleRef);
  }

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
      const service = this.startup.classOptions<NatsServiceOptions>(
        NATS_SERVICE_METADATA,
        instance.constructor,
        '@NatsService',
      );
      const routerOptions = this.startup.classOptions<CloudEventRouterOptions>(
        NATS_CLOUD_EVENT_ROUTER_METADATA,
        instance.constructor,
        '@CloudEventRouter',
      );
      const serviceDisabled =
        service !== undefined &&
        this.startup.isDisabled(service.disabled, `@NatsService ${className}`);
      const routes: Array<{ pattern: string; methodName: string }> = [];
      const prototype = Object.getPrototypeOf(instance) as object;
      for (const methodName of this.scanner.getAllMethodNames(prototype)) {
        const method = instance[methodName as keyof typeof instance];
        const specs = this.specsFor(
          method,
          service,
          serviceDisabled,
          `NATS handler ${className}.${methodName}`,
        );
        const patterns = cloudEventPatterns(method);
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
          if ('disabled' in spec) {
            options.onDisabled?.({
              kind: spec.kind,
              className,
              methodName,
              connection: spec.connection,
            });
            continue;
          }
          out.push(this.define(wrapper, instance, methodName, spec, service));
        }
      }
      if (!routerOptions) continue;
      if (this.startup.isDisabled(routerOptions.disabled, `@CloudEventRouter ${className}`)) {
        options.onDisabled?.({
          kind: routerKind(routerOptions),
          className,
          methodName: ROUTER_METHOD_NAME,
          connection: routerOptions.connection ?? DEFAULT_CONNECTION,
        });
        continue;
      }
      out.push(
        defineRouter(wrapper, instance, routerOptions, routes, (i, m) =>
          wrapHandler(this.ecc, i, m),
        ),
      );
    }
    return out;
  }

  /**
   * The handler specs of a method's decorators, their startup functions resolved, or a
   * `DisabledSpec` for each disabled one. A handler's options are resolved first, to read its
   * `disabled`; the subject or endpoint name of a disabled handler is not resolved, and no
   * argument of an endpoint of a disabled service (`serviceDisabled`) is. `subject` (e.g.
   * `NATS handler Orders.on`) names the method in errors.
   */
  private specsFor(
    method: unknown,
    service: NatsServiceOptions | undefined,
    serviceDisabled: boolean,
    subject: string,
  ): Array<HandlerSpec | DisabledSpec> {
    if (typeof method !== 'function') return [];
    const specs: Array<HandlerSpec | DisabledSpec> = [];
    /** The disabled entry of a `kind` handler with `options`, if `disabled` is set. */
    const disabledSpec = (
      kind: HandlerKind,
      options: { connection?: string; disabled?: HandlerDisabled },
    ): DisabledSpec | undefined =>
      this.startup.isDisabled(options.disabled, subject)
        ? {
            kind,
            disabled: true,
            connection: options.connection ?? service?.connection ?? DEFAULT_CONNECTION,
          }
        : undefined;
    const sub:
      | { subject: StartupValue<string>; options?: StartupValue<SubscribeOptions | undefined> }
      | undefined = Reflect.getMetadata(NATS_SUBSCRIBE_METADATA, method);
    if (sub) {
      const options =
        this.startup.resolveOptions(sub.options, subject, '@Subscribe options', true) ?? {};
      specs.push(
        disabledSpec('subscribe', options) ?? {
          kind: 'subscribe',
          subject: this.startup.resolveString(sub.subject, subject, '@Subscribe subject'),
          options,
        },
      );
    }
    const consume: { options: StartupValue<ConsumeOptions> } | undefined = Reflect.getMetadata(
      NATS_CONSUME_METADATA,
      method,
    );
    if (consume) {
      const options = this.startup.resolveOptions(consume.options, subject, '@Consume options');
      specs.push(
        disabledSpec('consume', options) ?? { kind: 'consume', delivery: 'pull', options },
      );
    }
    const push: { options: StartupValue<ConsumePushOptions> } | undefined = Reflect.getMetadata(
      NATS_CONSUME_PUSH_METADATA,
      method,
    );
    if (push) {
      const options = this.startup.resolveOptions(push.options, subject, '@ConsumePush options');
      specs.push(
        disabledSpec('consume', options) ?? { kind: 'consume', delivery: 'push', options },
      );
    }
    const ordered: { options: StartupValue<ConsumeOrderedOptions> } | undefined =
      Reflect.getMetadata(NATS_CONSUME_ORDERED_METADATA, method);
    if (ordered) {
      const options = this.startup.resolveOptions(
        ordered.options,
        subject,
        '@ConsumeOrdered options',
      );
      specs.push(disabledSpec('ordered', options) ?? { kind: 'ordered', options });
    }
    const endpoint:
      | {
          name: StartupValue<string>;
          options?: StartupValue<ServiceEndpointOptions | undefined>;
        }
      | undefined = Reflect.getMetadata(NATS_ENDPOINT_METADATA, method);
    if (endpoint) {
      if (serviceDisabled) {
        specs.push({
          kind: 'service-endpoint',
          disabled: true,
          connection: service?.connection ?? DEFAULT_CONNECTION,
        });
      } else {
        const options =
          this.startup.resolveOptions(
            endpoint.options,
            subject,
            '@ServiceEndpoint options',
            true,
          ) ?? {};
        specs.push(
          disabledSpec('service-endpoint', options) ?? {
            kind: 'service-endpoint',
            endpoint: this.startup.resolveString(endpoint.name, subject, '@ServiceEndpoint name'),
            options,
            service,
          },
        );
      }
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
    const invoke = wrapHandler(this.ecc, instance, methodName);
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
}

/** The connection a handler receives from. */
function connectionOf(spec: HandlerSpec, service: NatsServiceOptions | undefined): string {
  return (
    ('connection' in spec.options ? spec.options.connection : undefined) ??
    service?.connection ??
    DEFAULT_CONNECTION
  );
}
