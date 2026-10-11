import { isDeepStrictEqual } from 'node:util';
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
  CONSUMER_KEPT_FIELDS,
  type ConfigDiff,
  declaredFields,
  diffConfig,
  parseSnapshot,
  rejectReservedMetadata,
  removedFields,
  SNAPSHOT_KEY,
  STREAM_IMMUTABLE_FIELDS,
  STREAM_KEPT_FIELDS,
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

/**
 * Whether a server version keeps stream/consumer metadata: 2.10.0 or later, the threshold
 * nats.js uses (it refuses to send `metadata` to an older server). A pre-release suffix is
 * ignored, and an unknown version counts as supported (the response check still applies).
 */
function keepsMetadata(version: string | undefined): boolean {
  const match = /^v?(\d+)\.(\d+)/.exec(version ?? '');
  if (!match) return true;
  const [major, minor] = [Number(match[1]), Number(match[2])];
  return major > 2 || (major === 2 && minor >= 10);
}

/** Fields removed from a declaration, and what happens to each. */
interface Removed {
  /** Reset to the server default. */
  reset: string[];
  /** Reset to the library default (`createDefaults`), which the server does not have. */
  toLibraryDefault: string[];
  /** Kept: the server cannot change them. */
  kept: string[];
}

const NOTHING_REMOVED: Removed = { reset: [], toLibraryDefault: [], kept: [] };

/** The drift entries `verify` reports: the declared fields that differ, then the removed ones. */
function describeDrift(diffs: ConfigDiff[], removed: Removed, current: object): string {
  const cur = current as Record<string, unknown>;
  return [
    ...diffs.map(
      (d) =>
        `${d.field} (current: ${JSON.stringify(d.current)}, declared: ${JSON.stringify(d.declared)})`,
    ),
    ...[...removed.reset, ...removed.toLibraryDefault, ...removed.kept].map(
      (f) => `${f} (removed from the declaration, current: ${JSON.stringify(cur[f])})`,
    ),
  ].join(', ');
}

/** An existing stream or consumer, and how `syncExisting` brings it in line with its declaration. */
interface ExistingSync<I> {
  /** `stream "<name>"` or `consumer "<stream>/<durable>"`. */
  label: string;
  mode: 'create-or-update' | 'verify';
  current: I;
  diffs: ConfigDiff[];
  /** Removed fields are detected (`checkTracksRemovedFields()`). */
  tracked: boolean;
  /** The declared fields, and their `SNAPSHOT_KEY` value. */
  fields: string[];
  snapshot: string;
  /** The declared `metadata`. */
  metadata: Record<string, string> | undefined;
  /** `STREAM_KEPT_FIELDS` or `CONSUMER_KEPT_FIELDS`. */
  kept: readonly string[];
  /** What a removed field goes back to instead of the server default (push `createDefaults`). */
  libraryDefaults?: Record<string, unknown>;
  /** Sends the declaration, with `changes` (metadata, resets) on top of it. */
  update: (changes: Record<string, unknown>) => Promise<I>;
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

  /** `jetstream.removedFields` is `'reset'` (validated at bootstrap). */
  private get resetsRemovedFields(): boolean {
    return this.entry.options.jetstream?.removedFields === 'reset';
  }

  async ensureStream(
    declared: StreamDeclaration,
    mode: StreamSyncMode = this.defaultMode,
  ): Promise<StreamInfo | undefined> {
    if (mode === 'none') return undefined;
    const cfg = withoutNulls(declared);
    if (this.resetsRemovedFields) rejectReservedMetadata(`stream "${cfg.name}"`, cfg.metadata);
    const jsm = await this.jsm();
    const tracked = this.checkTracksRemovedFields();
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

    const diffs = diffConfig(current.config, cfg, STREAM_IMMUTABLE_FIELDS, tracked);
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
      tracked,
      fields,
      snapshot,
      metadata: cfg.metadata,
      kept: STREAM_KEPT_FIELDS,
      update: (changes) =>
        jsm.streams.update(name, { ...update, ...changes } as Partial<StreamUpdateConfig>),
    });
  }

  async ensureConsumer(
    stream: string,
    declared: ConsumerDeclaration['config'],
    mode: StreamSyncMode = this.defaultMode,
    /**
     * Merged under `cfg` only when the consumer is created; never compared for drift. With
     * `removedFields: 'reset'`, also what a field removed from the declaration goes back to.
     */
    createDefaults?: Partial<ConsumerConfig>,
  ): Promise<ConsumerInfo | undefined> {
    if (mode === 'none') return undefined;
    const cfg = withoutNulls(declared);
    const durable = cfg.durable_name;
    const label = `${stream}/${durable}`;
    if (this.resetsRemovedFields) rejectReservedMetadata(`consumer "${label}"`, cfg.metadata);
    const jsm = await this.jsm();
    const tracked = this.checkTracksRemovedFields();
    // `name` is the durable name again, when given.
    const fields = declaredFields(cfg, ['durable_name', 'name']);
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
    const diffs = diffConfig(current.config, cfg, CONSUMER_IMMUTABLE_FIELDS, tracked);
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
      tracked,
      fields,
      snapshot,
      metadata: cfg.metadata,
      kept: CONSUMER_KEPT_FIELDS,
      ...(createDefaults ? { libraryDefaults: createDefaults } : {}),
      update: (changes) =>
        jsm.consumers.update(stream, durable, { ...cfg, ...changes } as ConsumerUpdateConfig),
    });
  }

  /**
   * What `create-or-update` and `verify` do with an existing stream or consumer, once a
   * conflict on a declared immutable field is ruled out: `verify` throws on drift, and
   * `create-or-update` sends one update when a declared field differs, a removed field is to
   * be reset, or the snapshot is stale.
   */
  private async syncExisting<I extends StreamInfo | ConsumerInfo>(s: ExistingSync<I>): Promise<I> {
    const { label, current, diffs, tracked, snapshot } = s;
    const removed = tracked ? this.removedFrom(s) : NOTHING_REMOVED;
    const resets = removed.reset.length + removed.toLibraryDefault.length;
    if (s.mode === 'verify') {
      if (diffs.length > 0 || resets > 0 || removed.kept.length > 0) {
        throw new NatsConfigError(
          `JetStream ${label} differs from its declaration: ${describeDrift(diffs, removed, current.config)}`,
        );
      }
      return current;
    }
    this.warnOnRemovedKept(label, current.config, removed.kept);
    const writesSnapshot = tracked && current.config.metadata?.[SNAPSHOT_KEY] !== snapshot;
    if (diffs.length === 0 && resets === 0 && !writesSnapshot) return current;
    const updated = await s.update(
      tracked
        ? {
            metadata: syncMetadata(s.metadata, current.config.metadata, snapshot),
            // Own `undefined` properties: merged into the current config, they drop it from
            // the JSON, so the server applies its default.
            ...Object.fromEntries(removed.reset.map((f) => [f, undefined])),
            ...Object.fromEntries(removed.toLibraryDefault.map((f) => [f, s.libraryDefaults?.[f]])),
          }
        : {},
    );
    this.logResets(label, removed.reset, 'server');
    this.logResets(label, removed.toLibraryDefault, 'library');
    if (tracked) this.checkMetadataKept(updated.config.metadata, snapshot);
    return updated;
  }

  /**
   * The fields of the previous snapshot no longer declared. One with a library default goes
   * back to it, and is left alone when the server already has it.
   */
  private removedFrom({
    current,
    fields,
    kept,
    libraryDefaults = {},
  }: ExistingSync<StreamInfo | ConsumerInfo>): Removed {
    const cur = current.config as unknown as Record<string, unknown>;
    const removed = removedFields(cur, parseSnapshot(current.config.metadata), fields, kept);
    return {
      reset: removed.reset.filter((f) => libraryDefaults[f] === undefined),
      toLibraryDefault: removed.reset.filter(
        (f) => libraryDefaults[f] !== undefined && !isDeepStrictEqual(cur[f], libraryDefaults[f]),
      ),
      kept: removed.kept,
    };
  }

  /**
   * Whether removed fields are detected on this connection: only with `removedFields: 'reset'`,
   * and then not, with one warning, when its server is older than 2.10 (decided up front) or
   * once a response lost the snapshot.
   */
  private checkTracksRemovedFields(): boolean {
    if (!this.resetsRemovedFields || metadataUnsupported.has(this.entry)) return false;
    if (keepsMetadata(this.entry.nc.info?.version)) return true;
    this.markMetadataUnsupported();
    return false;
  }

  /**
   * Turns removed-field detection off for the connection, with one warning, when a create or
   * update response lacks the snapshot just sent: the server does not keep metadata.
   */
  private checkMetadataKept(metadata: Record<string, string> | undefined, snapshot: string): void {
    if (metadata?.[SNAPSHOT_KEY] === snapshot) return;
    this.markMetadataUnsupported();
  }

  private markMetadataUnsupported(): void {
    if (metadataUnsupported.has(this.entry)) return;
    metadataUnsupported.add(this.entry);
    this.entry.logger.warn(
      `JetStream on connection "${this.entry.name}" does not keep stream/consumer metadata (NATS server 2.10 or later is needed); removed fields are not detected`,
    );
  }

  /** `label` is `stream "<name>"` or `consumer "<stream>/<durable>"`. */
  private warnOnRemovedKept(label: string, current: object, kept: string[]): void {
    const cur = current as Record<string, unknown>;
    for (const field of kept) {
      this.entry.logger.warn(
        `JetStream ${label} keeps ${field} ${JSON.stringify(cur[field])}: it cannot be changed (removed from the declaration)`,
      );
    }
  }

  private logResets(label: string, fields: string[], to: 'server' | 'library'): void {
    for (const field of fields) {
      this.entry.logger.log(
        `JetStream ${label}: reset ${field} to the ${to} default (removed from the declaration)`,
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
