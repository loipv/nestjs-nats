import type { ConsumerConfig } from '@nats-io/jetstream';
import { SetMetadata } from '@nestjs/common';
import type { Codec } from '../codec/codec.js';
import type { IdempotencyOptions } from '../idempotency/idempotency.js';
import type { DlqOptions } from '../settlement/dlq.js';
import type { RetryOptions } from '../settlement/retry.js';
import { NATS_CLOUD_EVENT_ROUTER_METADATA, NATS_ON_CLOUD_EVENT_METADATA } from './constants.js';
import type { HandlerDisabled, OrderedDeliverPolicy } from './handler.decorators.js';

/**
 * Options of `@CloudEventRouter`: exactly one source for the whole class, plus what to do
 * with CloudEvents no `@OnCloudEvent` matches (`unmatched`, default `'ack'`). The source is
 * a core subscription (`subscribe`), a durable pull consumer (`consume`), a durable push
 * consumer (`consumePush`, as `@ConsumePush`) or an ordered consumer (`consumeOrdered`, as
 * `@ConsumeOrdered`; it has no `concurrency` and no acks).
 */
export type CloudEventRouterOptions = (
  | { subscribe: string; queue?: string }
  | { consume: { stream: string; durable: string; config?: Partial<ConsumerConfig> } }
  | {
      consumePush: {
        stream: string;
        durable: string;
        config?: Partial<ConsumerConfig>;
        deliverGroup?: string;
      };
    }
  | {
      consumeOrdered: {
        stream: string;
        filterSubjects?: string | string[];
        deliverPolicy?: OrderedDeliverPolicy;
        headersOnly?: boolean;
      };
    }
) & {
  /**
   * `'ack'` (default): acknowledge and skip (logged at debug level). `'term'`: terminate
   * a JetStream message (pull or push), drop a core or ordered one. `'dlq'`: dead-letter;
   * like `'term'`, with a warning, when no DLQ is configured. A request gets a 404 error
   * reply whatever the policy.
   */
  unmatched?: 'ack' | 'term' | 'dlq';
  connection?: string;
  concurrency?: number;
  codec?: Codec;
  retry?: RetryOptions | false;
  dlq?: DlqOptions | boolean;
  idempotency?: boolean | IdempotencyOptions;
  /**
   * Skips the router and all its `@OnCloudEvent` routes: nothing subscribes, consumes or
   * validates it. A function is called at application startup. Default `false`.
   */
  disabled?: HandlerDisabled;
};

/**
 * Receives CloudEvents (binary or structured) on one subscription, pull, push or ordered
 * consumer and hands each to the `@OnCloudEvent` method whose type pattern matches it best.
 */
export function CloudEventRouter(options: CloudEventRouterOptions): ClassDecorator {
  return SetMetadata(NATS_CLOUD_EVENT_ROUTER_METADATA, options) as ClassDecorator;
}

/**
 * Handles the CloudEvents of the enclosing `@CloudEventRouter` whose `type` matches
 * `typePattern` (segments split on `.`; `*` matches one segment, a trailing `>` one or
 * more). An exact match wins over a pattern with more literal segments, which wins over
 * one using `>`. `@Payload()` is the event's `data`, `@CloudEvent()` its attributes.
 * Several `@OnCloudEvent` on one method route each pattern to it.
 */
export function OnCloudEvent(typePattern: string): MethodDecorator {
  return (_target, _key, descriptor) => {
    const method = descriptor.value as object;
    const patterns: string[] = Reflect.getMetadata(NATS_ON_CLOUD_EVENT_METADATA, method) ?? [];
    Reflect.defineMetadata(NATS_ON_CLOUD_EVENT_METADATA, [...patterns, typePattern], method);
    return descriptor;
  };
}
