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

/** The drift entries `verify` reports: the declared fields that differ. */
function describeDrift(diffs: ConfigDiff[]): string {
  return diffs
    .map(
      (d) =>
        `${d.field} (current: ${JSON.stringify(d.current)}, declared: ${JSON.stringify(d.declared)})`,
    )
    .join(', ');
}

/** An existing stream or consumer, and how `syncExisting` brings it in line with its declaration. */
interface ExistingSync<I> {
  /** `stream "<name>"` or `consumer "<stream>/<durable>"`. */
  label: string;
  mode: 'create-or-update' | 'verify';
  current: I;
  diffs: ConfigDiff[];
  /** Sends the declaration. */
  update: () => Promise<I>;
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
    const conflict = diffs.find((d) => d.immutable);
    // `verify` reports a conflict as drift.
    if (conflict && mode !== 'verify') {
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
    const { name, ...update } = cfg;
    return this.syncExisting({
      label: `stream "${name}"`,
      mode,
      current,
      diffs,
      update: () => jsm.streams.update(name, update as Partial<StreamUpdateConfig>),
    });
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
    const durable = cfg.durable_name;
    const label = `${stream}/${durable}`;
    const jsm = await this.jsm();
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
      const create = { ...createDefaults, ...withConsumerDefaults(cfg) };
      return jsm.consumers.add(stream, create as ConsumerConfig);
    }
    this.warnOnNonExplicitAck(label, current);
    if (mode === 'create-only') return current;

    // Existing consumers are compared with the declaration as written: library defaults
    // must not override what the server already has.
    const diffs = diffConfig(current.config, cfg, CONSUMER_IMMUTABLE_FIELDS);
    const conflict = diffs.find((d) => d.immutable);
    // `verify` reports a conflict as drift.
    if (conflict && mode !== 'verify') {
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
    return this.syncExisting({
      label: `consumer "${label}"`,
      mode,
      current,
      diffs,
      update: () => jsm.consumers.update(stream, durable, cfg as ConsumerUpdateConfig),
    });
  }

  /**
   * What `create-or-update` and `verify` do with an existing stream or consumer, once a
   * conflict on a declared immutable field is ruled out: `verify` throws on drift, and
   * `create-or-update` sends one update when a declared field differs.
   */
  private async syncExisting<I extends StreamInfo | ConsumerInfo>(s: ExistingSync<I>): Promise<I> {
    const { label, current, diffs } = s;
    if (diffs.length === 0) return current;
    if (s.mode === 'verify') {
      throw new NatsConfigError(
        `JetStream ${label} differs from its declaration: ${describeDrift(diffs)}`,
      );
    }
    return s.update();
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
