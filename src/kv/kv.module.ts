import { JetStreamApiCodes, JetStreamApiError } from '@nats-io/jetstream';
import { type KV, type KvLimits, Kvm } from '@nats-io/kv';
import { type DynamicModule, Inject, Module } from '@nestjs/common';
import { type ConnectionEntry, ConnectionRegistry } from '../connection/connection-registry.js';
import { NatsConfigError } from '../errors/errors.js';
import { abortOnFailure } from '../module/nats.module.js';
import { DEFAULT_CONNECTION, getConnectionToken } from '../module/tokens.js';

/** A KV bucket to create or check at startup: its name and the limits it is created with. */
export interface KvBucketDeclaration extends Partial<KvLimits> {
  bucket: string;
}

export interface NatsKvFeatureOptions {
  /** The connection the buckets live on (default `'default'`). */
  connection?: string;
  buckets: KvBucketDeclaration[];
}

/** The injection token of a KV bucket. */
export function getKvToken(bucket: string, connection: string = DEFAULT_CONNECTION): string {
  return `nats:kv:${connection}:${bucket}`;
}

/** Injects the `KV` of a bucket declared with `NatsKvModule.forFeature`. */
export const InjectKv = (
  bucket: string,
  connection?: string,
): ParameterDecorator & PropertyDecorator => Inject(getKvToken(bucket, connection));

const BUCKET_NAME = /^[-\w]+$/;

/**
 * Declared limits that can be compared with the bucket's stream: the stream field, and how
 * to bring the stream's value back to the declaration's unit.
 */
const COMPARED: ReadonlyArray<{
  field: keyof KvLimits;
  stream: string;
  fromStream?: (value: number) => number;
}> = [
  { field: 'history', stream: 'max_msgs_per_subject' },
  // `ttl` is in ms; the stream's `max_age` in ns.
  { field: 'ttl', stream: 'max_age', fromStream: (ns) => ns / 1e6 },
  { field: 'max_bytes', stream: 'max_bytes' },
  { field: 'maxValueSize', stream: 'max_msg_size' },
  { field: 'replicas', stream: 'num_replicas' },
  { field: 'storage', stream: 'storage' },
];

/**
 * Declares KV buckets: each is created or checked at startup, following the connection's
 * `jetstream.sync`, and injectable with `@InjectKv(bucket)`.
 */
@Module({})
export class NatsKvModule {
  static forFeature(options: NatsKvFeatureOptions): DynamicModule {
    const connection = options.connection ?? DEFAULT_CONNECTION;
    const { buckets } = options;
    if (!Array.isArray(buckets) || buckets.length === 0) {
      throw new NatsConfigError('NatsKvModule.forFeature needs at least one bucket');
    }
    const seen = new Set<string>();
    for (const { bucket } of buckets) {
      if (typeof bucket !== 'string' || !BUCKET_NAME.test(bucket)) {
        throw new NatsConfigError(
          `NatsKvModule.forFeature has invalid bucket ${JSON.stringify(bucket)}; expected letters, digits, - or _`,
        );
      }
      if (seen.has(bucket)) {
        throw new NatsConfigError(`NatsKvModule.forFeature declares bucket "${bucket}" twice`);
      }
      seen.add(bucket);
    }
    const providers = buckets.map((declaration) => ({
      provide: getKvToken(declaration.bucket, connection),
      // The connection token makes Nest open the connection first.
      useFactory: (_connected: unknown, registry: ConnectionRegistry) =>
        abortOnFailure(registry, () =>
          openKvBucket(registry.get(connection), declaration, connection),
        ),
      inject: [getConnectionToken(connection), ConnectionRegistry],
    }));
    return {
      module: NatsKvModule,
      providers,
      exports: providers.map((p) => p.provide),
    };
  }
}

/**
 * Creates, opens or checks a declared bucket, following the connection's `jetstream.sync`,
 * on the connection's own JetStream client. Internal: the provider of `forFeature`.
 */
export async function openKvBucket(
  entry: ConnectionEntry,
  declaration: KvBucketDeclaration,
  connection: string,
): Promise<KV> {
  const { bucket, ...declared } = declaration;
  const label = `KV bucket "${bucket}" on connection "${connection}"`;
  const kvm = new Kvm(entry.js());
  const sync = entry.options.jetstream?.sync ?? 'create-or-update';
  if (sync === 'none') return kvm.open(bucket);
  if (sync === 'create-only') return kvm.create(bucket, declared);
  if (sync === 'verify') {
    // `open` does not look the bucket up: its stream tells whether it exists.
    let stream: Record<string, unknown>;
    try {
      stream = await streamConfigOf(entry, bucket);
    } catch (err) {
      if (isStreamNotFound(err))
        throw new NatsConfigError(`${label} does not exist`, { cause: err });
      throw err;
    }
    const drift = driftOf(stream, declared);
    if (drift.length > 0) {
      throw new NatsConfigError(
        `${label} differs from its declaration: ${drift
          .map((d) => `${d.field} (current: ${show(d.current)}, declared: ${show(d.declared)})`)
          .join(', ')}`,
      );
    }
    return kvm.open(bucket);
  }
  const kv = await kvm.create(bucket, declared);
  // `create` uses an existing bucket as it is: report what differs, as it cannot be updated.
  const compares = COMPARED.some(({ field }) => declared[field] !== undefined);
  const drift = compares ? driftOf(await streamConfigOf(entry, bucket), declared) : [];
  for (const d of drift) {
    entry.logger.warn(
      `${label} has ${d.field} ${show(d.current)}, declared ${show(d.declared)}; the library does not update KV buckets: change it with \`nats kv\` or recreate the bucket`,
    );
  }
  return kv;
}

/** The config of the bucket's stream (`KV_<bucket>`). */
async function streamConfigOf(
  entry: ConnectionEntry,
  bucket: string,
): Promise<Record<string, unknown>> {
  const jsm = await entry.jsm();
  const { config } = await jsm.streams.info(`KV_${bucket}`);
  return config as unknown as Record<string, unknown>;
}

/** The declared limits the bucket's stream holds differently; undeclared ones are not read. */
function driftOf(
  stream: Record<string, unknown>,
  declared: Partial<KvLimits>,
): Array<{ field: string; current: unknown; declared: unknown }> {
  const drift: Array<{ field: string; current: unknown; declared: unknown }> = [];
  for (const { field, stream: key, fromStream } of COMPARED) {
    if (declared[field] === undefined) continue;
    const raw = stream[key];
    const current = fromStream && typeof raw === 'number' ? fromStream(raw) : raw;
    if (current !== declared[field]) drift.push({ field, current, declared: declared[field] });
  }
  return drift;
}

function isStreamNotFound(err: unknown): boolean {
  if (err instanceof JetStreamApiError) return err.code === JetStreamApiCodes.StreamNotFound;
  return err instanceof Error && /stream not found/i.test(err.message);
}

const show = (value: unknown): string => JSON.stringify(value) ?? String(value);
