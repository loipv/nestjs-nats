import type { LoggerService } from '@nestjs/common';
import { BatchCollector } from '../batch/collector.js';
import { errorTrace } from '../util/errors.js';
import { createLimiter, type Limiter } from './limiter.js';

/** One message handed to `process`, with its 1-based delivery attempt. */
export interface DispatchItem<M> {
  msg: M;
  attempt: number;
}

export interface DispatcherOptions<M> {
  /** Maximum number of `process` calls running at once (an integer >= 1). */
  concurrency: number;
  process: (items: DispatchItem<M>[]) => Promise<void>;
  logger: LoggerService;
  /** Describes the handler in log lines. */
  label: string;
  /** Called with every message pushed after `stop()` (it will never be processed). */
  onRejected?: (msg: M) => void;
  /**
   * Collects pushed messages into batches (flushed at `maxSize` or `maxWait` ms after
   * their first message), each processed by one `process` call holding one slot.
   */
  batch?: { maxSize: number; maxWait: number };
}

/**
 * Runs messages of one handler through `process` with bounded concurrency and
 * backpressure, and schedules in-process retries. With `batch`, messages are collected
 * first and each batch is processed as one call.
 */
export class Dispatcher<M> {
  private readonly limiter: Limiter;
  private readonly retries = new Set<ReturnType<typeof setTimeout>>();
  private readonly collector?: BatchCollector<DispatchItem<M>>;
  /** Flushes of the collector started by or after `stop()`. */
  private draining: Promise<void> = Promise.resolve();
  private stopped = false;

  constructor(private readonly opts: DispatcherOptions<M>) {
    this.limiter = createLimiter(opts.concurrency);
    if (opts.batch) {
      this.collector = new BatchCollector<DispatchItem<M>>({
        ...opts.batch,
        flush: (items) => this.startBatch(items),
        onError: (err) => this.logError('flush batch', err),
      });
    }
  }

  /** Number of `process` calls currently running. */
  get active(): number {
    return this.limiter.active;
  }

  /**
   * Starts processing `msg` once a slot is free; resolves when it has started (not
   * finished), so awaiting it applies backpressure. After `stop()` the message is
   * handed to `onRejected` instead.
   */
  async push(msg: M, attempt = 1): Promise<void> {
    if (this.stopped) {
      this.reject(msg);
      return;
    }
    if (this.collector) {
      // Resolves once collected, or once its batch started when it completed one.
      await this.collector.add({ msg, attempt });
      // Collected after `stop()` flushed (it waited behind a running flush): flush it too.
      if (this.stopped && this.collector.size > 0) await this.drain();
      return;
    }
    await this.limiter.acquire();
    if (this.stopped) {
      // Stopped while waiting for a slot: it was received but never started.
      this.limiter.release();
      this.reject(msg);
      return;
    }
    this.start([{ msg, attempt }]);
  }

  /**
   * Pushes `item.msg` again with `attempt + 1` after `delayMs`. Allowed after `stop()`
   * (for messages still being processed), where it ends in `onRejected` when it fires;
   * `cancelRetries()` cancels it.
   */
  scheduleRetry(item: DispatchItem<M>, delayMs: number): void {
    const timer = setTimeout(() => {
      this.retries.delete(timer);
      void this.push(item.msg, item.attempt + 1).catch((err: unknown) =>
        this.opts.logger.error(`Failed to retry message for ${this.opts.label}`, errorTrace(err)),
      );
    }, delayMs);
    // A pending retry must not keep the process alive.
    timer.unref();
    this.retries.add(timer);
  }

  /**
   * Stops accepting new pushes; messages already being processed continue. With
   * `batch`, the batch being collected is handed to `process` (once a slot is free);
   * resolves when it started.
   */
  stop(): Promise<void> {
    this.stopped = true;
    return this.collector ? this.drain() : Promise.resolve();
  }

  /**
   * Resolves when no `process` call is running (pending retries are not waited for),
   * including the batch flushed by `stop()`.
   */
  async idle(): Promise<void> {
    await this.draining;
    await this.limiter.idle();
  }

  /** Cancels every pending retry; returns how many were cancelled. */
  cancelRetries(): number {
    const count = this.retries.size;
    for (const timer of this.retries) clearTimeout(timer);
    this.retries.clear();
    return count;
  }

  /** Flushes the collector; never rejects. */
  private drain(): Promise<void> {
    const flushed = (this.collector?.flushNow() ?? Promise.resolve()).catch((err: unknown) =>
      this.logError('flush batch', err),
    );
    this.draining = Promise.all([this.draining, flushed]).then(() => undefined);
    return flushed;
  }

  /** Processes a collected batch once a slot is free; resolves when it started. */
  private async startBatch(items: DispatchItem<M>[]): Promise<void> {
    // A batch flushed by `stop()` is still processed: it was collected for the handler.
    await this.limiter.acquire();
    this.start(items);
  }

  /** Runs `process` in the slot already acquired, releasing it when done. */
  private start(items: DispatchItem<M>[]): void {
    void this.run(items)
      .catch((err: unknown) => this.logError('process message', err))
      .finally(() => this.limiter.release());
  }

  private logError(action: string, err: unknown): void {
    this.opts.logger.error(`Failed to ${action} for ${this.opts.label}`, errorTrace(err));
  }

  private run(items: DispatchItem<M>[]): Promise<void> {
    try {
      return this.opts.process(items);
    } catch (err) {
      return Promise.reject(err);
    }
  }

  private reject(msg: M): void {
    try {
      this.opts.onRejected?.(msg);
    } catch (err) {
      this.opts.logger.error(`Failed to reject message for ${this.opts.label}`, errorTrace(err));
    }
  }
}
