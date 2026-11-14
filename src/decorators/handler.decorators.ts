import type { ConsumerConfig } from '@nats-io/jetstream';
import type { BatchOptions } from '../batch/collector.js';
import type { Codec } from '../codec/codec.js';
import { NatsConfigError } from '../errors/errors.js';
import type { IdempotencyOptions } from '../idempotency/idempotency.js';
import type { DlqOptions } from '../settlement/dlq.js';
import type { RetryOptions } from '../settlement/retry.js';
import {
  NATS_CONSUME_METADATA,
  NATS_CONSUME_ORDERED_METADATA,
  NATS_CONSUME_PUSH_METADATA,
  NATS_ENDPOINT_METADATA,
  NATS_SERVICE_METADATA,
  NATS_SUBSCRIBE_METADATA,
} from './constants.js';
import type { StartupContext, StartupValue } from './startup-value.js';

/**
 * Whether a handler is skipped: a boolean, or a function called once at application startup
 * (decorator arguments are evaluated at import time, before configuration is loaded). The
 * function receives the application's providers, as a decorator's startup function does.
 */
export type HandlerDisabled = boolean | ((ctx: StartupContext) => boolean);

export interface SubscribeOptions {
  connection?: string;
  queue?: string;
  /**
   * Messages waiting in the subscription past which the connection logs a slow-consumer
   * warning, once each time the backlog crosses it. Only a warning: core NATS cannot hold
   * messages back, so a backlog grows in memory until the handler catches up (use
   * `@Consume` for flow control). `false` turns the warning off. Default `10_000`.
   */
  slowConsumer?: number | false;
  codec?: Codec;
  concurrency?: number;
  retry?: RetryOptions | false;
  dlq?: DlqOptions | boolean;
  idempotency?: boolean | IdempotencyOptions;
  /**
   * Hands the handler messages in batches (`@Payload() items: T[]`,
   * `@Ctx() ctx: NatsBatchContext`); a batch never replies.
   *
   * @experimental On `@Subscribe`, may change or be removed before 1.0 (see Stability in the
   * README).
   */
  batch?: BatchOptions;
  /**
   * Longest run of the handler, in ms; past it the run counts as failed and `ctx.signal` is
   * aborted (see "Handler timeout" in docs/handlers.md). `false` turns off the module's
   * `handlerTimeout`. Default: the module's `handlerTimeout`, else none.
   */
  timeout?: number | false;
  /**
   * Skips this handler: nothing subscribes, consumes or validates it, as if the decorator
   * were absent. A function is called at application startup, after configuration
   * (e.g. `.env`) is loaded. Default `false`.
   */
  disabled?: HandlerDisabled;
}

export interface ConsumeOptions {
  connection?: string;
  stream: string;
  durable: string;
  config?: Partial<ConsumerConfig>;
  ack?: 'auto' | 'manual';
  codec?: Codec;
  concurrency?: number;
  retry?: RetryOptions | false;
  dlq?: DlqOptions | boolean;
  idempotency?: boolean | IdempotencyOptions;
  /**
   * Hands the handler messages in batches (`@Payload() items: T[]`,
   * `@Ctx() ctx: NatsBatchContext`). Not allowed with `ack: 'manual'`.
   */
  batch?: BatchOptions;
  /**
   * Longest run of the handler, in ms; past it the run counts as failed and `ctx.signal` is
   * aborted (see "Handler timeout" in docs/handlers.md). `false` turns off the module's
   * `handlerTimeout`. Default: the module's `handlerTimeout`, else none.
   */
  timeout?: number | false;
  /**
   * Skips this handler: nothing subscribes, consumes or validates it, as if the decorator
   * were absent. A function is called at application startup, after configuration
   * (e.g. `.env`) is loaded. Default `false`.
   */
  disabled?: HandlerDisabled;
}

/**
 * Options of a durable push consumer: the same as a pull consumer's, plus a delivery
 * group, so several instances share one durable and split its messages.
 */
export interface ConsumePushOptions extends ConsumeOptions {
  /**
   * The consumer's `deliver_group`: instances of one group share the durable's messages.
   * Must agree with `config.deliver_group` when both are set.
   */
  deliverGroup?: string;
}

/** Where an ordered consumer starts reading the stream. */
export type OrderedDeliverPolicy =
  | 'all'
  | 'last'
  | 'new'
  | 'last_per_subject'
  | { startSeq: number }
  | { startTime: Date | string };

/**
 * Options of an ordered consumer: an ephemeral, ack-less, in-order consumer that the
 * client recreates on a gap. It has no durable, `ack`, `config`, `concurrency` or `batch`.
 */
export interface ConsumeOrderedOptions {
  connection?: string;
  stream: string;
  filterSubjects?: string | string[];
  deliverPolicy?: OrderedDeliverPolicy;
  headersOnly?: boolean;
  codec?: Codec;
  retry?: RetryOptions | false;
  dlq?: DlqOptions | boolean;
  /**
   * Skips messages already processed (see docs/idempotency.md).
   *
   * @experimental On an ordered consumer, may change or be removed before 1.0 (see Stability
   * in the README).
   */
  idempotency?: boolean | IdempotencyOptions;
  /**
   * Longest run of the handler, in ms; past it the run counts as failed and `ctx.signal` is
   * aborted (see "Handler timeout" in docs/handlers.md). `false` turns off the module's
   * `handlerTimeout`. Default: the module's `handlerTimeout`, else none.
   */
  timeout?: number | false;
  /**
   * Skips this handler: nothing subscribes, consumes or validates it, as if the decorator
   * were absent. A function is called at application startup, after configuration
   * (e.g. `.env`) is loaded. Default `false`.
   */
  disabled?: HandlerDisabled;
}

export interface NatsServiceOptions {
  name: string;
  version: string;
  description?: string;
  queue?: string;
  metadata?: Record<string, string>;
  connection?: string;
  /** Skips every endpoint of the service; see `SubscribeOptions.disabled`. */
  disabled?: HandlerDisabled;
}

export interface ServiceEndpointOptions {
  subject?: string;
  queue?: string;
  metadata?: Record<string, string>;
  codec?: Codec;
  /**
   * Longest run of the handler, in ms; past it the run counts as failed and `ctx.signal` is
   * aborted (see "Handler timeout" in docs/handlers.md). `false` turns off the module's
   * `handlerTimeout`. Default: the module's `handlerTimeout`, else none.
   */
  timeout?: number | false;
  /** Skips this endpoint; see `SubscribeOptions.disabled`. */
  disabled?: HandlerDisabled;
}

/**
 * Internal: a method decorator storing `value` under `key` on the method, as Nest's
 * `SetMetadata` does. The same decorator applied twice to one method throws a
 * `NatsConfigError`: the second would silently replace the first. `name` is the decorator's
 * name in that error, such as `'@Subscribe'`.
 */
export function methodMetadata(key: string, name: string, value: unknown): MethodDecorator {
  return (target, propertyKey, descriptor) => {
    const method = descriptor.value as object;
    if (Reflect.hasOwnMetadata(key, method)) {
      // `target` is the prototype for an instance method, the class for a static one.
      const owner = typeof target === 'function' ? target : target.constructor;
      throw new NatsConfigError(
        `${name} is applied more than once to ${owner.name}.${String(propertyKey)}; each handler decorator declares one handler per method`,
      );
    }
    Reflect.defineMetadata(key, value, method);
    return descriptor;
  };
}

/**
 * Internal: a class decorator storing `value` under `key` on the class. The same decorator
 * applied twice to one class throws a `NatsConfigError`; a subclass may apply it again.
 * `declares` ends the error, such as `'one service'`.
 */
export function classMetadata(
  key: string,
  name: string,
  value: unknown,
  declares: string,
): ClassDecorator {
  return (target) => {
    if (Reflect.hasOwnMetadata(key, target)) {
      throw new NatsConfigError(
        `${name} is applied more than once to ${target.name}; each class declares ${declares}`,
      );
    }
    Reflect.defineMetadata(key, value, target);
  };
}

export function Subscribe(
  subject: StartupValue<string>,
  options?: StartupValue<SubscribeOptions | undefined>,
): MethodDecorator {
  return methodMetadata(NATS_SUBSCRIBE_METADATA, '@Subscribe', { subject, options });
}

export function Consume(options: StartupValue<ConsumeOptions>): MethodDecorator {
  return methodMetadata(NATS_CONSUME_METADATA, '@Consume', { options });
}

/**
 * A durable JetStream push consumer. NATS recommends pull consumers (`@Consume`).
 *
 * @experimental May change or be removed before 1.0 (see Stability in the README).
 */
export function ConsumePush(options: StartupValue<ConsumePushOptions>): MethodDecorator {
  return methodMetadata(NATS_CONSUME_PUSH_METADATA, '@ConsumePush', { options });
}

export function ConsumeOrdered(options: StartupValue<ConsumeOrderedOptions>): MethodDecorator {
  return methodMetadata(NATS_CONSUME_ORDERED_METADATA, '@ConsumeOrdered', { options });
}

export function NatsService(options: StartupValue<NatsServiceOptions>): ClassDecorator {
  return classMetadata(NATS_SERVICE_METADATA, '@NatsService', options, 'one service');
}

export function ServiceEndpoint(
  name: StartupValue<string>,
  options?: StartupValue<ServiceEndpointOptions | undefined>,
): MethodDecorator {
  return methodMetadata(NATS_ENDPOINT_METADATA, '@ServiceEndpoint', { name, options });
}
