import {
  AckPolicy,
  type ConsumerConfig,
  type ConsumerInfo,
  type ConsumerUpdateConfig,
  JetStreamApiCodes,
  JetStreamApiError,
  type JetStreamManager,
  type PurgeOpts,
  type PurgeResponse,
  type StreamInfo,
  type StreamUpdateConfig,
} from '@nats-io/jetstream';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsConfigError, StreamConfigConflictError } from '../errors/errors.js';
import type { ConsumerDeclaration, StreamDeclaration, StreamSyncMode } from '../module/options.js';
import {
  CONSUMER_IMMUTABLE_FIELDS,
  type ConfigDiff,
  diffConfig,
  STREAM_IMMUTABLE_FIELDS,
  withConsumerDefaults,
  withoutNulls,
} from './stream-sync.js';

function isApiError(err: unknown, errCode: number): boolean {
  return err instanceof JetStreamApiError && err.code === errCode;
}

const isStreamNotFound = (err: unknown): boolean =>
  isApiError(err, JetStreamApiCodes.StreamNotFound);
const isConsumerNotFound = (err: unknown): boolean =>
  isApiError(err, JetStreamApiCodes.ConsumerNotFound);

function describeDiffs(diffs: ConfigDiff[]): string {
  return diffs
    .map(
      (d) =>
        `${d.field} (current: ${JSON.stringify(d.current)}, declared: ${JSON.stringify(d.declared)})`,
    )
    .join(', ');
}

export class NatsStreamManager {
  /** Instances are provided by `NatsModule`; this constructor is not part of the public API. */
  constructor(private readonly entry: ConnectionEntry) {}

  jsm(): Promise<JetStreamManager> {
    return this.entry.jsm();
  }

  private get defaultMode(): StreamSyncMode {
    return this.entry.options.jetstream?.sync ?? 'create-or-update';
  }

  async ensureStream(
    declared: StreamDeclaration,
    mode: StreamSyncMode = this.defaultMode,
  ): Promise<StreamInfo | undefined> {
    if (mode === 'none') return undefined;
    const cfg = withoutNulls(declared);
    const jsm = await this.jsm();
    let current: StreamInfo;
    try {
      current = await jsm.streams.info(cfg.name);
    } catch (err) {
      if (!isStreamNotFound(err)) throw err;
      if (mode === 'verify') {
        throw new NatsConfigError(`JetStream stream "${cfg.name}" does not exist`, { cause: err });
      }
      return jsm.streams.add(cfg);
    }
    if (mode === 'create-only') return current;

    const diffs = diffConfig(current.config, cfg, STREAM_IMMUTABLE_FIELDS);
    if (mode === 'verify') {
      if (diffs.length > 0) {
        throw new NatsConfigError(
          `JetStream stream "${cfg.name}" differs from its declaration: ${describeDiffs(diffs)}`,
        );
      }
      return current;
    }
    const conflict = diffs.find((d) => d.immutable);
    if (conflict) {
      throw new StreamConfigConflictError(
        `JetStream stream "${cfg.name}" has a conflicting immutable field "${conflict.field}" (current: ${JSON.stringify(conflict.current)}, declared: ${JSON.stringify(conflict.declared)})`,
        {
          stream: cfg.name,
          field: conflict.field,
          current: conflict.current,
          declared: conflict.declared,
        },
      );
    }
    if (diffs.length === 0) return current;
    const { name, ...update } = cfg;
    return jsm.streams.update(name, update as Partial<StreamUpdateConfig>);
  }

  async ensureConsumer(
    stream: string,
    declared: ConsumerDeclaration['config'],
    mode: StreamSyncMode = this.defaultMode,
    /** Merged under `cfg` only when the consumer is created; never compared for drift. */
    createDefaults?: Partial<ConsumerConfig>,
  ): Promise<ConsumerInfo | undefined> {
    if (mode === 'none') return undefined;
    const cfg = withoutNulls(declared);
    const jsm = await this.jsm();
    const durable = cfg.durable_name;
    const label = `${stream}/${durable}`;
    let current: ConsumerInfo;
    try {
      current = await jsm.consumers.info(stream, durable);
    } catch (err) {
      if (isStreamNotFound(err)) {
        throw new NatsConfigError(
          `JetStream stream "${stream}" does not exist (declared for consumer "${durable}")`,
          { cause: err },
        );
      }
      if (!isConsumerNotFound(err)) throw err;
      if (mode === 'verify') {
        throw new NatsConfigError(`JetStream consumer "${label}" does not exist`, { cause: err });
      }
      // Defaults apply only to consumers the library creates (spec §7).
      return jsm.consumers.add(stream, {
        ...createDefaults,
        ...withConsumerDefaults(cfg),
      } as ConsumerConfig);
    }
    this.warnOnNonExplicitAck(label, current);
    if (mode === 'create-only') return current;

    // Existing consumers are compared with the declaration as written: library defaults
    // must not override what the server already has.
    const diffs = diffConfig(current.config, cfg, CONSUMER_IMMUTABLE_FIELDS);
    if (mode === 'verify') {
      if (diffs.length > 0) {
        throw new NatsConfigError(
          `JetStream consumer "${label}" differs from its declaration: ${describeDiffs(diffs)}`,
        );
      }
      return current;
    }
    const conflict = diffs.find((d) => d.immutable);
    if (conflict) {
      throw new StreamConfigConflictError(
        `JetStream consumer "${label}" has a conflicting immutable field "${conflict.field}" (current: ${JSON.stringify(conflict.current)}, declared: ${JSON.stringify(conflict.declared)})`,
        {
          stream,
          consumer: durable,
          field: conflict.field,
          current: conflict.current,
          declared: conflict.declared,
        },
      );
    }
    if (diffs.length === 0) return current;
    return jsm.consumers.update(stream, durable, cfg as ConsumerUpdateConfig);
  }

  private warnOnNonExplicitAck(label: string, current: ConsumerInfo): void {
    const { ack_policy } = current.config;
    if (ack_policy === AckPolicy.Explicit) return;
    this.entry.logger.warn(
      `JetStream consumer "${label}" uses ack_policy "${ack_policy}"; the library settles messages individually and expects "explicit"`,
    );
  }

  async info(stream: string): Promise<StreamInfo> {
    return (await this.jsm()).streams.info(stream);
  }

  async purge(stream: string, opts?: PurgeOpts): Promise<PurgeResponse> {
    return (await this.jsm()).streams.purge(stream, opts);
  }

  async delete(stream: string): Promise<boolean> {
    return (await this.jsm()).streams.delete(stream);
  }
}
