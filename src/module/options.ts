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

/** What `onPublishError` receives: the error, and which publish failed. */
export type PublishErrorHandler = (
  error: Error,
  info: { subject: string; connection: string },
) => void;

export type StreamSyncMode = 'create-or-update' | 'create-only' | 'verify' | 'none';

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
  /**
   * Receives the errors of `NatsClient.publish()` on this connection instead of the log: the
   * connection is closed, an outbound plugin failed, or the plugins did not pass the message
   * on in time. A call's own `onError` takes precedence.
   */
  onPublishError?: PublishErrorHandler;
  shutdown?: { drainTimeout?: number };
  logger?: false | LoggerService;
}

export interface NatsFeatureOptions {
  connection?: string;
  streams?: StreamDeclaration[];
  consumers?: ConsumerDeclaration[];
}
