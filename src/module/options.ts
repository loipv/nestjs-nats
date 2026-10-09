import type { ConsumerConfig, JetStreamOptions, StreamConfig } from '@nats-io/jetstream';
import type { NodeConnectionOptions } from '@nats-io/transport-node';
import type { LoggerService } from '@nestjs/common';
import type { Codec } from '../codec/codec.js';
import type { IdempotencyStore } from '../idempotency/store.js';
import type { NatsPlugin } from '../pipeline/types.js';
import type { DlqOptions } from '../settlement/dlq.js';
import type { RetryOptions } from '../settlement/retry.js';

type WithRequired<T, K extends keyof T> = T & { [P in K]-?: T[P] };

export type StreamSyncMode = 'create-or-update' | 'create-only' | 'verify' | 'none';

export type StreamDeclaration = WithRequired<Partial<StreamConfig>, 'name'>;

export interface ConsumerDeclaration {
  stream: string;
  config: Partial<ConsumerConfig> & { durable_name: string };
}

export interface NatsModuleOptions {
  name?: string;
  connection: NodeConnectionOptions;
  jetstream?: { options?: JetStreamOptions; sync?: StreamSyncMode };
  streams?: StreamDeclaration[];
  consumers?: ConsumerDeclaration[];
  codec?: Codec;
  /** Defaults of the CloudEvents this connection publishes. */
  cloudEvents?: { source?: string };
  plugins?: NatsPlugin[];
  retry?: RetryOptions | false;
  dlq?: DlqOptions | boolean;
  /** Defaults for handlers that enable idempotency: the store and the key TTL (ms, default 300_000). */
  idempotency?: { store?: IdempotencyStore; ttl?: number };
  shutdown?: { drainTimeout?: number };
  logger?: false | LoggerService;
}

export interface NatsFeatureOptions {
  connection?: string;
  streams?: StreamDeclaration[];
  consumers?: ConsumerDeclaration[];
}
