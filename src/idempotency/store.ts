import { createHash } from 'node:crypto';
import type { JetStreamClient } from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/transport-node';
import { NatsConfigError } from '../errors/errors.js';

export type ClaimResult = 'claimed' | 'duplicate' | 'in-progress';

/** What a store receives from `bind`. */
export interface IdempotencyStoreContext {
  /** The connection name. */
  connection: string;
  /** The connection the store runs on. */
  nc: NatsConnection;
  /** The JetStream client of that connection, with its `jetstream.options` (domain, API prefix, timeout). */
  js: JetStreamClient;
}

/**
 * Remembers which messages were processed (or are being processed) so redeliveries and
 * duplicates are not handled twice. Implement it over a shared database (Redis, SQL, ...)
 * to deduplicate across application instances.
 */
export interface IdempotencyStore {
  /**
   * Tries to claim `key` for `ttlMs`. `'claimed'` means the caller processes the message;
   * `'duplicate'` that it was already processed; `'in-progress'` that another delivery
   * holds a claim that has been neither completed nor released yet.
   */
  claim(key: string, ttlMs: number): Promise<ClaimResult>;
  /** Marks `key` as processed for another `ttlMs`. */
  complete(key: string, ttlMs: number): Promise<void>;
  /** Frees a claimed `key` (processing failed) so the message can be processed again. */
  release(key: string): Promise<void>;
  /**
   * Called once with the connection of `forRoot`, at module init, before any message is handled.
   * A store whose every bind in this application failed is not closed by the module (it may
   * belong to another running application); a bind that allocates resources and then fails
   * must release them itself.
   */
  bind?(ctx: IdempotencyStoreContext): Promise<void> | void;
  close?(): Promise<void>;
}

export interface InMemoryIdempotencyStoreOptions {
  /** Entries kept before the least recently used one is evicted (default 100_000). */
  maxKeys?: number;
  /** Clock in milliseconds (tests). */
  now?: () => number;
}

interface Entry {
  state: 'in-progress' | 'done';
  expiresAt: number;
}

const DEFAULT_MAX_KEYS = 100_000;
/**
 * The longest key held as it is; a longer one is held by its sha256. Keys come from message
 * headers the publisher controls, so without a bound `maxKeys` would not bound memory.
 */
const MAX_HELD_KEY = 256;
const SWEEP_INTERVAL_MS = 60_000;

/**
 * Idempotency store kept in process memory: only effective within one application
 * instance, and forgotten on restart. Entries expire after their TTL (a claim that is
 * never completed expires too, so a crashed run does not block the key forever) and the
 * least recently used entry is evicted beyond `maxKeys`.
 */
export class InMemoryIdempotencyStore implements IdempotencyStore {
  // A Map iterates in insertion order: touching an entry moves it to the end.
  private readonly entries = new Map<string, Entry>();
  private readonly maxKeys: number;
  private readonly now: () => number;
  private sweeper?: NodeJS.Timeout;

  constructor(opts: InMemoryIdempotencyStoreOptions = {}) {
    const maxKeys = opts.maxKeys ?? DEFAULT_MAX_KEYS;
    if (!Number.isInteger(maxKeys) || maxKeys < 1) {
      throw new NatsConfigError(
        `InMemoryIdempotencyStore maxKeys ${String(maxKeys)} is invalid; expected an integer >= 1`,
      );
    }
    this.maxKeys = maxKeys;
    this.now = opts.now ?? Date.now;
    this.sweeper = setInterval(() => this.sweep(), SWEEP_INTERVAL_MS);
    // The sweep must never keep the process alive.
    this.sweeper.unref();
  }

  claim(rawKey: string, ttlMs: number): Promise<ClaimResult> {
    const key = held(rawKey);
    const now = this.now();
    const existing = this.entries.get(key);
    if (existing && existing.expiresAt > now) {
      this.touch(key, existing);
      return Promise.resolve(existing.state === 'done' ? 'duplicate' : 'in-progress');
    }
    this.put(key, { state: 'in-progress', expiresAt: now + ttlMs });
    return Promise.resolve('claimed');
  }

  complete(key: string, ttlMs: number): Promise<void> {
    this.put(held(key), { state: 'done', expiresAt: this.now() + ttlMs });
    return Promise.resolve();
  }

  release(key: string): Promise<void> {
    this.entries.delete(held(key));
    return Promise.resolve();
  }

  /** Number of entries held, expired ones not yet swept included. */
  get size(): number {
    return this.entries.size;
  }

  close(): Promise<void> {
    if (this.sweeper) clearInterval(this.sweeper);
    this.sweeper = undefined;
    this.entries.clear();
    return Promise.resolve();
  }

  private touch(key: string, entry: Entry): void {
    this.entries.delete(key);
    this.entries.set(key, entry);
  }

  private put(key: string, entry: Entry): void {
    this.entries.delete(key);
    this.entries.set(key, entry);
    while (this.entries.size > this.maxKeys) {
      const oldest = this.entries.keys().next();
      if (oldest.done) break;
      this.entries.delete(oldest.value);
    }
  }

  private sweep(): void {
    const now = this.now();
    for (const [key, entry] of this.entries) {
      if (entry.expiresAt <= now) this.entries.delete(key);
    }
  }
}

/**
 * The map key of a store key: `k:` and the key itself, or past 256 characters `h:` and the
 * base64url of its sha256. The prefixes keep the two forms from ever colliding.
 */
function held(key: string): string {
  if (key.length <= MAX_HELD_KEY) return `k:${key}`;
  return `h:${createHash('sha256').update(key, 'utf8').digest('base64url')}`;
}
