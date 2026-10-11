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
  /** KV bucket name ([-_A-Za-z0-9]+; default 'nestjs_nats_idempotency'). */
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
   */
  instanceId?: string;
  /** Clock in ms (tests; default Date.now). */
  now?: () => number;
}

const DEFAULT_BUCKET = 'nestjs_nats_idempotency';
const DEFAULT_MAX_AGE = 86_400_000;
/** The largest stream max_age the server accepts, in ms ((2^31 - 1) seconds). */
const MAX_MAX_AGE = 2_147_483_647_000;
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
  private warnedOrdered = false;

  constructor(options: KvIdempotencyStoreOptions = {}) {
    const bucket = options.bucket ?? DEFAULT_BUCKET;
    if (typeof bucket !== 'string' || !/^[-\w]+$/.test(bucket)) {
      throw new NatsConfigError(
        `KvIdempotencyStore bucket ${JSON.stringify(bucket)} is invalid; expected letters, digits, "-" or "_"`,
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

  async bind({ connection, nc }: IdempotencyStoreContext): Promise<void> {
    if (this.connection !== undefined) {
      throw new NatsConfigError(
        `KvIdempotencyStore is already bound to NATS connection "${this.connection}"; give each connection its own store`,
      );
    }
    // Taken before the first await, so a concurrent bind is refused too.
    this.connection = connection;
    try {
      this.kv = await this.openBucket(new Kvm(nc));
    } catch (err) {
      this.connection = undefined;
      throw err;
    }
  }

  async claim(key: string, ttlMs: number): Promise<ClaimResult> {
    const kv = this.bound();
    const k = this.kvKey(key);
    let lastErr: unknown;
    for (let attempt = 0; attempt < MAX_CLAIM_ATTEMPTS; attempt++) {
      const claim = this.value('p', ttlMs);
      try {
        await kv.create(k, claim);
        return 'claimed';
      } catch (err) {
        if (!isWrongRevision(err)) throw err;
        lastErr = err;
      }
      const entry = await kv.get(k);
      // Gone between the create and the get (deleted and purged): create again.
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

  /** Does nothing: the connection belongs to the module. */
  async close(): Promise<void> {}

  private async openBucket(kvm: Kvm): Promise<KV> {
    try {
      const kv = await kvm.open(this.bucket);
      // open() does not look the bucket up: status() fails when its stream is missing.
      await kv.status();
      return kv;
    } catch (err) {
      if (!isStreamNotFound(err)) throw err;
    }
    return kvm.create(this.bucket, {
      history: 1,
      ttl: this.maxAge,
      storage: this.storage,
      replicas: this.replicas,
    });
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
