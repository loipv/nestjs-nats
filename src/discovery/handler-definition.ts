import type { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import type { NatsBatchContext } from '../context/nats-batch-context.js';
import type { NatsContext } from '../context/nats-context.js';
import type {
  ConsumeOptions,
  ConsumeOrderedOptions,
  ConsumePushOptions,
  NatsServiceOptions,
  ServiceEndpointOptions,
  SubscribeOptions,
} from '../decorators/handler.decorators.js';

export type HandlerKind = 'subscribe' | 'consume' | 'ordered' | 'service-endpoint';

export type Invoke = (payload: unknown, ctx: NatsContext | NatsBatchContext) => Promise<unknown>;

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

export type HandlerSpec =
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

/** A handler left out by its `disabled` (or its `@NatsService`'s): what `onDisabled` reports. */
export interface DisabledSpec {
  kind: HandlerKind;
  disabled: true;
  connection: string;
}

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
