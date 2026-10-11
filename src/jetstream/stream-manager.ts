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
  declaredFields,
  diffConfig,
  parseSnapshot,
  removedFields,
  SNAPSHOT_KEY,
  STREAM_IMMUTABLE_FIELDS,
  snapshotOf,
  syncMetadata,
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

/**
 * Connections whose server does not keep stream/consumer metadata (older than 2.10). Shared
 * by every `NatsStreamManager` of a connection, as several are created for one.
 */
const metadataUnsupported = new WeakSet<ConnectionEntry>();

/** Fields removed from a declaration: `reset` to the server default, or kept as `immutable`. */
interface Removed {
  reset: string[];
  immutable: string[];
}

const NOTHING_REMOVED: Removed = { reset: [], immutable: [] };

/** The drift entries `verify` reports: the declared fields that differ, then the removed ones. */
function describeDrift(diffs: ConfigDiff[], removed: Removed, current: object): string {
  const cur = current as Record<string, unknown>;
  return [
    ...diffs.map(
      (d) =>
        `${d.field} (current: ${JSON.stringify(d.current)}, declared: ${JSON.stringify(d.declared)})`,
    ),
    ...[...removed.reset, ...removed.immutable].map(
      (f) => `${f} (removed from the declaration, current: ${JSON.stringify(cur[f])})`,
    ),
  ].join(', ');
}

/** Own `undefined` properties: merged into the current config, they drop it from the JSON. */
const resetToDefaults = (fields: string[]): Record<string, undefined> =>
  Object.fromEntries(fields.map((f) => [f, undefined]));

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
    const tracked = this.tracksMetadata;
    const label = `stream "${cfg.name}"`;
    const fields = declaredFields(cfg, ['name']);
    const snapshot = snapshotOf(fields);
    let current: StreamInfo;
    try {
      current = await jsm.streams.info(cfg.name);
    } catch (err) {
      if (!isStreamNotFound(err)) throw err;
      if (mode === 'verify') {
        throw new NatsConfigError(`JetStream stream "${cfg.name}" does not exist`, { cause: err });
      }
      if (!tracked) return jsm.streams.add(cfg);
      const created = await jsm.streams.add({
        ...cfg,
        metadata: syncMetadata(cfg.metadata, undefined, snapshot),
      });
      this.checkMetadataKept(created.config.metadata, snapshot);
      return created;
    }
    if (mode === 'create-only') return current;

    const diffs = diffConfig(current.config, cfg, STREAM_IMMUTABLE_FIELDS);
    const removed = tracked
      ? removedFields(
          current.config,
          parseSnapshot(current.config.metadata),
          fields,
          STREAM_IMMUTABLE_FIELDS,
        )
      : NOTHING_REMOVED;
    if (mode === 'verify') {
      if (diffs.length > 0 || removed.reset.length > 0 || removed.immutable.length > 0) {
        throw new NatsConfigError(
          `JetStream ${label} differs from its declaration: ${describeDrift(diffs, removed, current.config)}`,
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
    this.warnOnRemovedImmutable(label, current.config, removed);
    const writesSnapshot = tracked && current.config.metadata?.[SNAPSHOT_KEY] !== snapshot;
    if (diffs.length === 0 && removed.reset.length === 0 && !writesSnapshot) return current;
    const { name, ...update } = cfg;
    const updated = await jsm.streams.update(
      name,
      (tracked
        ? {
            ...update,
            metadata: syncMetadata(cfg.metadata, current.config.metadata, snapshot),
            ...resetToDefaults(removed.reset),
          }
        : update) as Partial<StreamUpdateConfig>,
    );
    this.logResets(label, removed.reset);
    if (tracked) this.checkMetadataKept(updated.config.metadata, snapshot);
    return updated;
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
    const tracked = this.tracksMetadata;
    const fields = declaredFields(cfg, ['durable_name']);
    const snapshot = snapshotOf(fields);
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
      // Defaults apply only to consumers the library creates (spec §7), and never count in
      // the snapshot: they were not declared, so they can never be removed from a declaration.
      const create = { ...createDefaults, ...withConsumerDefaults(cfg) };
      if (!tracked) return jsm.consumers.add(stream, create as ConsumerConfig);
      const created = await jsm.consumers.add(stream, {
        ...create,
        metadata: syncMetadata(cfg.metadata, undefined, snapshot),
      } as ConsumerConfig);
      this.checkMetadataKept(created.config.metadata, snapshot);
      return created;
    }
    this.warnOnNonExplicitAck(label, current);
    if (mode === 'create-only') return current;

    // Existing consumers are compared with the declaration as written: library defaults
    // must not override what the server already has.
    const diffs = diffConfig(current.config, cfg, CONSUMER_IMMUTABLE_FIELDS);
    const removed = tracked
      ? removedFields(
          current.config,
          parseSnapshot(current.config.metadata),
          fields,
          CONSUMER_IMMUTABLE_FIELDS,
        )
      : NOTHING_REMOVED;
    if (mode === 'verify') {
      if (diffs.length > 0 || removed.reset.length > 0 || removed.immutable.length > 0) {
        throw new NatsConfigError(
          `JetStream consumer "${label}" differs from its declaration: ${describeDrift(diffs, removed, current.config)}`,
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
    this.warnOnRemovedImmutable(`consumer "${label}"`, current.config, removed);
    const writesSnapshot = tracked && current.config.metadata?.[SNAPSHOT_KEY] !== snapshot;
    if (diffs.length === 0 && removed.reset.length === 0 && !writesSnapshot) return current;
    const updated = await jsm.consumers.update(
      stream,
      durable,
      (tracked
        ? {
            ...cfg,
            metadata: syncMetadata(cfg.metadata, current.config.metadata, snapshot),
            ...resetToDefaults(removed.reset),
          }
        : cfg) as ConsumerUpdateConfig,
    );
    this.logResets(`consumer "${label}"`, removed.reset);
    if (tracked) this.checkMetadataKept(updated.config.metadata, snapshot);
    return updated;
  }

  /** Whether removed fields are detected on this connection (its server keeps metadata). */
  private get tracksMetadata(): boolean {
    return !metadataUnsupported.has(this.entry);
  }

  /**
   * Turns removed-field detection off for the connection, with one warning, when a create or
   * update response lacks the snapshot just sent: the server does not keep metadata.
   */
  private checkMetadataKept(metadata: Record<string, string> | undefined, snapshot: string): void {
    if (metadata?.[SNAPSHOT_KEY] === snapshot || metadataUnsupported.has(this.entry)) return;
    metadataUnsupported.add(this.entry);
    this.entry.logger.warn(
      `JetStream on connection "${this.entry.name}" does not keep stream/consumer metadata (NATS server 2.10 or later is needed); removed fields are not detected`,
    );
  }

  /** `label` is `stream "<name>"` or `consumer "<stream>/<durable>"`. */
  private warnOnRemovedImmutable(label: string, current: object, removed: Removed): void {
    const cur = current as Record<string, unknown>;
    for (const field of removed.immutable) {
      this.entry.logger.warn(
        `JetStream ${label} keeps ${field} ${JSON.stringify(cur[field])}: it cannot be changed (removed from the declaration)`,
      );
    }
  }

  private logResets(label: string, fields: string[]): void {
    for (const field of fields) {
      this.entry.logger.log(
        `JetStream ${label}: reset ${field} to the server default (removed from the declaration)`,
      );
    }
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
