import type { ConsumerConfig } from '@nats-io/jetstream';
import { SetMetadata } from '@nestjs/common';
import type { Codec } from '../codec/codec.js';
import type { DlqOptions } from '../settlement/dlq.js';
import type { RetryOptions } from '../settlement/retry.js';
import {
  NATS_CONSUME_METADATA,
  NATS_ENDPOINT_METADATA,
  NATS_SERVICE_METADATA,
  NATS_SUBSCRIBE_METADATA,
} from './constants.js';

export interface SubscribeOptions {
  connection?: string;
  queue?: string;
  codec?: Codec;
  concurrency?: number;
  retry?: RetryOptions | false;
  dlq?: DlqOptions | boolean;
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
}

export interface NatsServiceOptions {
  name: string;
  version: string;
  description?: string;
  queue?: string;
  metadata?: Record<string, string>;
  connection?: string;
}

export interface ServiceEndpointOptions {
  subject?: string;
  queue?: string;
  metadata?: Record<string, string>;
  codec?: Codec;
}

export function Subscribe(subject: string, options?: SubscribeOptions): MethodDecorator {
  return SetMetadata(NATS_SUBSCRIBE_METADATA, { subject, options }) as MethodDecorator;
}

export function Consume(options: ConsumeOptions): MethodDecorator {
  return SetMetadata(NATS_CONSUME_METADATA, { options }) as MethodDecorator;
}

export function NatsService(options: NatsServiceOptions): ClassDecorator {
  return SetMetadata(NATS_SERVICE_METADATA, options) as ClassDecorator;
}

export function ServiceEndpoint(name: string, options?: ServiceEndpointOptions): MethodDecorator {
  return SetMetadata(NATS_ENDPOINT_METADATA, { name, options }) as MethodDecorator;
}
