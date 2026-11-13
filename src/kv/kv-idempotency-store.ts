import { createHash } from 'node:crypto';
import { JetStreamApiCodes } from '@nats-io/jetstream';
import { type KV, Kvm } from '@nats-io/kv';
import { Logger } from '@nestjs/common';
import { NatsConfigError, NatsModuleError } from '../errors/errors.js';
import type {
  ClaimResult,
  IdempotencyStore,
  IdempotencyStoreContext,
} from '../idempotency/store.js';

export interface KvIdempotencyStoreOptions {
  /** KV bucket name (1 to 64 of [-_A-Za-z0-9]; default 'nestjs_nats_idempotency'). */
  bucket?: string;
  /** How long the bucket keeps an entry after its last write, in ms (garbage collection; default 86_400_000). Must exceed every handler's ttl. */
  maxAge?: number;
  /** Bucket storage when the store creates it (default 'file'). */
  storage?: 'file' | 'memory';
  /** Bucket replicas when the store creates it (default 1). */
  replicas?: number;
  /**
   * Prefixes the keys of `@ConsumeOrdered` handlers (keys starting with `ordered:`), so the
   * instances sharing the bucket each process every message of an ordered consumer.
   *
   * @experimental May change or be removed before 1.0 (see Stability in the README).
   */
  instanceId?: string;
  /** Clock in ms (tests; default Date.now). */
  now?: () => number;
}

const DEFAULT_BUCKET = 'nestjs_nats_idempotency';
const DEFAULT_MAX_AGE = 86_400_000;
/** The largest stream max_age the server accepts, in ms ((2^31 - 1) seconds). */
const MAX_MAX_AGE = 2_147_483_647_000;
const MAX_BUCKET_NAME = 64;
const MAX_CLAIM_ATTEMPTS = 3;
const ORDERED_PREFIX = 'ordered:';
/**
 * The longest key stored as its own encoding. A longer one is stored under its hash: the
 * subject `$KV.<bucket>.<key>` must fit the server's control line (4096 bytes by default),
 * or the server closes the connection.
 */
const MAX_ENCODED_KEY = 256;
const HASHED_PREFIX = 'h.';

const NOT_BOUND =
  'KvIdempotencyStore is not bound to a NATS connection; pass it as idempotency.store of NatsModule.forRoot()';
const NO_INSTANCE_ID =
  'KvIdempotencyStore without instanceId is shared by the instances of @ConsumeOrdered handlers: each instance skips the messages another processed. Set instanceId.';

/** `p`: claimed, in progress; `d`: done. `x`: expiry, in ms since the epoch. */
interface Value {
  s: 'p' | 'd';
  x: number;
}

/**
 * The JetStream API error code of `err`. Matched by shape rather than `instanceof`:
 * `@nats-io/kv` may resolve its own copy of `@nats-io/jetstream`.
 */
function apiCode(err: unknown): number | undefined {
  const code = err instanceof Error ? (err as { code?: unknown }).code : undefined;
  return typeof code === 'number' ? code : undefined;
}

const isStreamNotFound = (err: unknown): boolean =>
  apiCode(err) === JetStreamApiCodes.StreamNotFound;

/** The write expected another last revision of the key (it exists, or was changed meanwhile). */
const isWrongRevision = (err: unknown): boolean => {
  const code = apiCode(err);
  return (
    code === JetStreamApiCodes.StreamWrongLastSequence ||
    code === JetStreamApiCodes.StreamWrongLastSequenceUnknown
  );
};

function parse(raw: string): Value | undefined {
  try {
    const v: unknown = JSON.parse(raw);
    if (typeof v !== 'object' || v === null) return undefined;
    const { s, x } = v as { s?: unknown; x?: unknown };
    if ((s !== 'p' && s !== 'd') || typeof x !== 'number' || !Number.isFinite(x)) {
      return undefined;
    }
    return { s, x };
  } catch {
    return undefined;
  }
}

/**
 * Idempotency store on a JetStream KV bucket, shared by every application instance on the
 * same NATS system. Give it to `idempotency.store` of `NatsModule.forRoot()`: it binds to
 * that connection at module init, opening the bucket (or creating it when missing). One
 * store per connection.
 *
 * Entries expire logically: each value carries its expiry, compared with `now()`, so clock
 * skew between instances shifts the TTL by as much. The bucket's `maxAge` only garbage
 * collects entries; it must exceed every handler's ttl. `release` deletes the key
 * unconditionally, even when it has expired and another instance has claimed it since.
 */
export class KvIdempotencyStore implements IdempotencyStore {
  private readonly bucket: string;
  private readonly maxAge: number;
  private readonly storage: 'file' | 'memory';
  private readonly replicas: number;
  private readonly instanceId?: string;
  private readonly now: () => number;
  private readonly logger = new Logger('KvIdempotencyStore');
  private connection?: string;
  private kv?: KV;
  /** The bound bucket's max age in ms, 0 for none. */
  private bucketMaxAge = 0;
  /** Bumped by every bind and close, so a bind that outlives a close can tell. */
  private generation = 0;
  private warnedOrdered = false;
  private warnedMaxAge = false;

  constructor(options: KvIdempotencyStoreOptions = {}) {
    const bucket = options.bucket ?? DEFAULT_BUCKET;
    if (typeof bucket !== 'string' || !/^[-\w]+$/.test(bucket) || bucket.length > MAX_BUCKET_NAME) {
      throw new NatsConfigError(
        `KvIdempotencyStore bucket ${JSON.stringify(bucket)} is invalid; expected at most ${MAX_BUCKET_NAME} letters, digits, "-" or "_"`,
      );
    }
    const maxAge = options.maxAge ?? DEFAULT_MAX_AGE;
    if (!Number.isInteger(maxAge) || maxAge <= 0 || maxAge > MAX_MAX_AGE) {
      throw new NatsConfigError(
        `KvIdempotencyStore maxAge ${String(maxAge)} is invalid; expected an integer of ms > 0 and <= ${MAX_MAX_AGE}`,
      );
    }
    const storage = options.storage ?? 'file';
    if (storage !== 'file' && storage !== 'memory') {
      throw new NatsConfigError(
        `KvIdempotencyStore storage ${JSON.stringify(storage)} is invalid; expected "file" or "memory"`,
      );
    }
    const replicas = options.replicas ?? 1;
    if (!Number.isInteger(replicas) || replicas < 1) {
      throw new NatsConfigError(
        `KvIdempotencyStore replicas ${String(replicas)} is invalid; expected an integer >= 1`,
      );
    }
    const { instanceId } = options;
    if (instanceId !== undefined && (typeof instanceId !== 'string' || instanceId === '')) {
      throw new NatsConfigError(
        `KvIdempotencyStore instanceId ${JSON.stringify(instanceId)} is invalid; expected a non-empty string`,
      );
    }
    this.bucket = bucket;
    this.maxAge = maxAge;
    this.storage = storage;
    this.replicas = replicas;
    this.instanceId = instanceId;
    this.now = options.now ?? Date.now;
  }

  async bind({ connection, js }: IdempotencyStoreContext): Promise<void> {
    if (this.connection !== undefined) {
      throw new NatsConfigError(
        `KvIdempotencyStore is already bound to NATS connection "${this.connection}"; give each connection its own store`,
      );
    }
    // Taken before the first await, so a concurrent bind is refused too.
    this.connection = connection;
    const generation = ++this.generation;
    try {
      // On the connection's JetStream client, so its jetstream.options (domain, prefix) apply.
      const { kv, maxAge } = await this.openBucket(new Kvm(js));
      // A close during the open released this bind: its result is discarded.
      if (generation !== this.generation) {
        throw new NatsModuleError('KvIdempotencyStore was closed while binding');
      }
      this.kv = kv;
      this.bucketMaxAge = maxAge;
    } catch (err) {
      // A stale bind must not clear a newer one.
      if (generation === this.generation) this.connection = undefined;
      throw err;
    }
  }

  async claim(key: string, ttlMs: number): Promise<ClaimResult> {
    const kv = this.bound();
    this.checkMaxAge(ttlMs);
    const k = this.kvKey(key);
    let lastErr: unknown;
    for (let attempt = 0; attempt < MAX_CLAIM_ATTEMPTS; attempt++) {
      const claim = this.value('p', ttlMs);
      try {
        // Expects no revision of the key at all: a delete or purge marker fails it too.
        await kv.put(k, claim, { previousSeq: 0 });
        return 'claimed';
      } catch (err) {
        if (!isWrongRevision(err)) throw err;
        lastErr = err;
      }
      const entry = await kv.get(k);
      // Gone between the put and the get (deleted and purged): try again.
      if (!entry) continue;
      if (entry.operation === 'PUT') {
        const v = parse(entry.string());
        if (v && v.x > this.now()) return v.s === 'd' ? 'duplicate' : 'in-progress';
      }
      // Deleted, purged, expired or unparsable: take it over, unless someone else wrote it since.
      try {
        await kv.update(k, claim, entry.revision);
        return 'claimed';
      } catch (err) {
        if (!isWrongRevision(err)) throw err;
        lastErr = err;
      }
    }
    throw lastErr;
  }

  async complete(key: string, ttlMs: number): Promise<void> {
    const kv = this.bound();
    await kv.put(this.kvKey(key), this.value('d', ttlMs));
  }

  async release(key: string): Promise<void> {
    const kv = this.bound();
    await kv.delete(this.kvKey(key));
  }

  /**
   * Releases the binding, so the store can be bound again (by another application, say). The
   * connection belongs to the module: it is not closed.
   */
  async close(): Promise<void> {
    this.generation++;
    this.connection = undefined;
    this.kv = undefined;
    this.bucketMaxAge = 0;
  }

  private async openBucket(kvm: Kvm): Promise<{ kv: KV; maxAge: number }> {
    try {
      const kv = await this.openLeaderReads(kvm);
      // open() does not look the bucket up: status() fails when its stream is missing.
      return { kv, maxAge: (await kv.status()).ttl };
    } catch (err) {
      if (!isStreamNotFound(err)) throw err;
    }
    await kvm.create(this.bucket, {
      history: 1,
      ttl: this.maxAge,
      storage: this.storage,
      replicas: this.replicas,
    });
    // create() returns a handle with direct gets when the server has them: open it again.
    const kv = await this.openLeaderReads(kvm);
    // Another instance may have created the bucket first, with another max age.
    return { kv, maxAge: (await kv.status()).ttl };
  }

  /**
   * Opens the bucket with gets answered by the stream leader, not direct gets. A direct get may
   * be answered by a replica that lags behind the leader, and a claim compares and sets on the
   * revision it read: a stale read would report an outdated state, or lose the write to a
   * conflict until the claim fails.
   */
  private openLeaderReads(kvm: Kvm): Promise<KV> {
    return kvm.open(this.bucket, { allow_direct: false });
  }

  /** Warns once when a claim outlives the bucket's max age: the key could vanish early. */
  private checkMaxAge(ttlMs: number): void {
    if (this.warnedMaxAge || this.bucketMaxAge <= 0 || ttlMs <= this.bucketMaxAge) return;
    this.warnedMaxAge = true;
    this.logger.warn(
      `KvIdempotencyStore bucket "${this.bucket}" keeps entries for ${this.bucketMaxAge} ms, less than the idempotency ttl ${ttlMs} ms; raise its max age`,
    );
  }

  private bound(): KV {
    if (!this.kv) throw new NatsModuleError(NOT_BOUND);
    return this.kv;
  }

  private value(s: Value['s'], ttlMs: number): string {
    const v: Value = { s, x: this.now() + ttlMs };
    return JSON.stringify(v);
  }

  /**
   * The KV key of a store key: base64url, so any character is allowed; past 256 characters,
   * `h.` and the base64url of its sha256 (base64url has no `.`, so the two never collide).
   */
  private kvKey(key: string): string {
    let k = key;
    if (key.startsWith(ORDERED_PREFIX)) {
      if (this.instanceId !== undefined) {
        k = `${ORDERED_PREFIX}${this.instanceId}:${key.slice(ORDERED_PREFIX.length)}`;
      } else if (!this.warnedOrdered) {
        this.warnedOrdered = true;
        this.logger.warn(NO_INSTANCE_ID);
      }
    }
    const encoded = Buffer.from(k, 'utf8').toString('base64url');
    if (encoded.length <= MAX_ENCODED_KEY) return encoded;
    return HASHED_PREFIX + createHash('sha256').update(k, 'utf8').digest('base64url');
  }
}
