import type { ConsumerConfig } from '@nats-io/jetstream';
import { SetMetadata } from '@nestjs/common';
import type { BatchOptions } from '../batch/collector.js';
import type { Codec } from '../codec/codec.js';
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

/**
 * Whether a handler is skipped: a boolean, or a function called at application startup
 * (decorator arguments are evaluated at import time, before configuration is loaded).
 */
export type HandlerDisabled = boolean | (() => boolean);

export interface SubscribeOptions {
  connection?: string;
  queue?: string;
  codec?: Codec;
  concurrency?: number;
  retry?: RetryOptions | false;
  dlq?: DlqOptions | boolean;
  idempotency?: boolean | IdempotencyOptions;
  /**
   * Hands the handler messages in batches (`@Payload() items: T[]`,
   * `@Ctx() ctx: NatsBatchContext`); a batch never replies.
   */
  batch?: BatchOptions;
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
  idempotency?: boolean | IdempotencyOptions;
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
  /** Skips this endpoint; see `SubscribeOptions.disabled`. */
  disabled?: HandlerDisabled;
}

export function Subscribe(subject: string, options?: SubscribeOptions): MethodDecorator {
  return SetMetadata(NATS_SUBSCRIBE_METADATA, { subject, options }) as MethodDecorator;
}

export function Consume(options: ConsumeOptions): MethodDecorator {
  return SetMetadata(NATS_CONSUME_METADATA, { options }) as MethodDecorator;
}

export function ConsumePush(options: ConsumePushOptions): MethodDecorator {
  return SetMetadata(NATS_CONSUME_PUSH_METADATA, { options }) as MethodDecorator;
}

export function ConsumeOrdered(options: ConsumeOrderedOptions): MethodDecorator {
  return SetMetadata(NATS_CONSUME_ORDERED_METADATA, { options }) as MethodDecorator;
}

export function NatsService(options: NatsServiceOptions): ClassDecorator {
  return SetMetadata(NATS_SERVICE_METADATA, options) as ClassDecorator;
}

export function ServiceEndpoint(name: string, options?: ServiceEndpointOptions): MethodDecorator {
  return SetMetadata(NATS_ENDPOINT_METADATA, { name, options }) as MethodDecorator;
}
