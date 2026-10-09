import type { PubAck } from '@nats-io/jetstream';
import { NatsConfigError, NatsModuleError } from '../errors/errors.js';
import type { JetStreamPublishOptions, PublishManyResult } from '../jetstream/jetstream-client.js';
import { MAX_TIMER_DELAY } from './collector.js';

export const DEFAULT_BATCHER_MAX_SIZE = 100;
export const DEFAULT_BATCHER_MAX_WAIT = 50;

export interface BatcherOptions {
  /** Flush once this many messages are collected (an integer >= 1). */
  maxSize: number;
  /** Flush this many ms after the first message of the batch (>= 0). */
  maxWait: number;
  /** In-flight publish cap of each flush, as in `publishMany`. */
  window?: number;
}

/** The `publishMany` a batcher flushes through. It never rejects for an item's failure. */
export type PublishManyFn = (
  items: ReadonlyArray<{ subject: string; data?: unknown; opts?: JetStreamPublishOptions }>,
  opts?: { window?: number },
) => Promise<PublishManyResult[]>;

interface BatchItem {
  subject: string;
  data: unknown;
  opts: JetStreamPublishOptions | undefined;
  resolve: (ack: PubAck) => void;
  reject: (err: Error) => void;
}

/**
 * Collects JetStream publishes and sends them together through `publishMany`, flushing when
 * `maxSize` messages are collected or `maxWait` ms after the first one. Every `add` settles
 * with the outcome of its own message. Flushes run one at a time, in order.
 */
export class Batcher {
  private items: BatchItem[] = [];
  private timer?: ReturnType<typeof setTimeout>;
  /** The running flushes, chained; never rejects. */
  private tail: Promise<void> = Promise.resolve();
  private unsettled = 0;
  private closed = false;
  private closing?: Promise<void>;

  constructor(
    private readonly publishMany: PublishManyFn,
    private readonly opts: BatcherOptions,
    private readonly onClosed?: () => void,
  ) {
    if (!Number.isInteger(opts.maxSize) || opts.maxSize < 1) {
      throw new NatsConfigError('Batcher maxSize must be an integer >= 1');
    }
    if (!(opts.maxWait >= 0)) throw new NatsConfigError('Batcher maxWait must be >= 0');
    if (opts.window !== undefined && !(opts.window >= 1)) {
      throw new NatsConfigError('Batcher window must be >= 1');
    }
  }

  /** Messages added and not yet acknowledged or failed. */
  get pending(): number {
    return this.unsettled;
  }

  /** Resolves with the PubAck of this message, rejects with its own error. */
  add(subject: string, data?: unknown, opts?: JetStreamPublishOptions): Promise<PubAck> {
    if (this.closed) return Promise.reject(new NatsModuleError('Batcher is closed'));
    return new Promise<PubAck>((resolve, reject) => {
      this.unsettled++;
      this.items.push({
        subject,
        data,
        opts,
        resolve: (ack) => {
          this.unsettled--;
          resolve(ack);
        },
        reject: (err) => {
          this.unsettled--;
          reject(err);
        },
      });
      if (this.items.length >= this.opts.maxSize) this.startFlush();
      else if (this.items.length === 1) this.arm();
    });
  }

  /** Sends everything added so far; resolves once every such message has its result. */
  async flush(): Promise<void> {
    // `add` pushes synchronously, so everything added so far is in `items` or already in `tail`.
    this.startFlush();
    await this.tail;
  }

  /** Flushes, then refuses further `add`s. Idempotent. */
  close(): Promise<void> {
    this.closing ??= (async () => {
      this.closed = true;
      try {
        await this.flush();
      } finally {
        this.onClosed?.();
      }
    })();
    return this.closing;
  }

  private startFlush(): void {
    this.disarm();
    if (this.items.length === 0) return;
    const batch = this.items;
    this.items = [];
    this.tail = this.tail.then(() => this.publishBatch(batch));
  }

  private async publishBatch(batch: BatchItem[]): Promise<void> {
    let results: PublishManyResult[] = [];
    let failure: Error | undefined;
    try {
      const window = this.opts.window;
      results = await this.publishMany(
        batch.map(({ subject, data, opts }) => ({ subject, data, opts })),
        window === undefined ? undefined : { window },
      );
    } catch (err) {
      failure = err instanceof Error ? err : new Error(String(err));
    }
    batch.forEach((item, i) => {
      const result = results[i];
      if (failure) item.reject(failure);
      else if (result?.ok) item.resolve(result.ack);
      else if (result) item.reject(result.error);
      else item.reject(new NatsModuleError('Batcher got no result for a message'));
    });
  }

  private arm(): void {
    this.disarm();
    // Kept ref'd: unflushed publishes must not be lost to an idle process exit.
    this.timer = setTimeout(
      () => {
        this.timer = undefined;
        this.startFlush();
      },
      Math.min(this.opts.maxWait, MAX_TIMER_DELAY),
    );
  }

  private disarm(): void {
    if (this.timer === undefined) return;
    clearTimeout(this.timer);
    this.timer = undefined;
  }
}
