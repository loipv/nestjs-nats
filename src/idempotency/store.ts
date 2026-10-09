export type ClaimResult = 'claimed' | 'duplicate' | 'in-progress';

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
    this.maxKeys = opts.maxKeys ?? DEFAULT_MAX_KEYS;
    this.now = opts.now ?? Date.now;
    this.sweeper = setInterval(() => this.sweep(), SWEEP_INTERVAL_MS);
    // The sweep must never keep the process alive.
    this.sweeper.unref();
  }

  claim(key: string, ttlMs: number): Promise<ClaimResult> {
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
    this.put(key, { state: 'done', expiresAt: this.now() + ttlMs });
    return Promise.resolve();
  }

  release(key: string): Promise<void> {
    this.entries.delete(key);
    return Promise.resolve();
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
