import type { ConsumerConfig, JetStreamOptions, StreamConfig } from '@nats-io/jetstream';
import type { NodeConnectionOptions } from '@nats-io/transport-node';
import type { LoggerService } from '@nestjs/common';
import type { Codec } from '../codec/codec.js';
import type { IdempotencyStore } from '../idempotency/store.js';
import type { NatsPlugin } from '../pipeline/types.js';
import type { DlqOptions } from '../settlement/dlq.js';
import type { RestartOptions } from '../settlement/restart.js';
import type { RetryOptions } from '../settlement/retry.js';

type WithRequired<T, K extends keyof T> = T & { [P in K]-?: T[P] };

export type StreamSyncMode = 'create-or-update' | 'create-only' | 'verify' | 'none';

/**
 * What stream/consumer sync does with a field removed from a declaration: `'reset'` it to the
 * server default (and report it in `verify`), or `'ignore'` it and keep the server's value.
 */
export type RemovedFieldsMode = 'reset' | 'ignore';

export type StreamDeclaration = WithRequired<Partial<StreamConfig>, 'name'>;

export interface ConsumerDeclaration {
  stream: string;
  config: Partial<ConsumerConfig> & { durable_name: string };
}

export interface NatsModuleOptions {
  name?: string;
  connection: NodeConnectionOptions;
  jetstream?: {
    options?: JetStreamOptions;
    sync?: StreamSyncMode;
    /**
     * `'reset'` remembers the declared fields of each stream and consumer in its metadata
     * (`nestjs-nats.declared`, NATS server 2.10 or later), so that `create-or-update` resets a
     * field removed from a declaration to the server default and `verify` reports it. Only
     * for an application that is the only one declaring its streams and consumers. Default
     * `'ignore'`: a removed field keeps its value on the server.
     */
    removedFields?: RemovedFieldsMode;
  };
  streams?: StreamDeclaration[];
  consumers?: ConsumerDeclaration[];
  codec?: Codec;
  /** Defaults of the CloudEvents this connection publishes. */
  cloudEvents?: { source?: string };
  plugins?: NatsPlugin[];
  retry?: RetryOptions | false;
  /** Backoff for restarting handler loops that end on their own; `false` disables restarts. */
  restart?: RestartOptions | false;
  dlq?: DlqOptions | boolean;
  /** Defaults for handlers that enable idempotency: the store and the key TTL (ms, default 300_000). */
  idempotency?: { store?: IdempotencyStore; ttl?: number };
  /**
   * Default `timeout` of the connection's handlers, in ms: a run that outlives it counts as
   * failed. A handler's own `timeout` (or `false`) wins. Default: none.
   */
  handlerTimeout?: number | false;
  shutdown?: { drainTimeout?: number };
  logger?: false | LoggerService;
}

export interface NatsFeatureOptions {
  connection?: string;
  streams?: StreamDeclaration[];
  consumers?: ConsumerDeclaration[];
}
