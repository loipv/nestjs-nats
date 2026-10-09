import type { LoggerService } from '@nestjs/common';
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
}

/**
 * Runs messages of one handler through `process` with bounded concurrency and
 * backpressure, and schedules in-process retries.
 */
export class Dispatcher<M> {
  private readonly limiter: Limiter;
  private readonly retries = new Set<ReturnType<typeof setTimeout>>();
  private stopped = false;

  constructor(private readonly opts: DispatcherOptions<M>) {
    this.limiter = createLimiter(opts.concurrency);
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
    await this.limiter.acquire();
    if (this.stopped) {
      // Stopped while waiting for a slot: it was received but never started.
      this.limiter.release();
      this.reject(msg);
      return;
    }
    void this.run([{ msg, attempt }])
      .catch((err: unknown) =>
        this.opts.logger.error(`Failed to process message for ${this.opts.label}`, errorTrace(err)),
      )
      .finally(() => this.limiter.release());
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

  /** Stops accepting new pushes; messages already being processed continue. */
  stop(): void {
    this.stopped = true;
  }

  /** Resolves when no `process` call is running (pending retries are not waited for). */
  idle(): Promise<void> {
    return this.limiter.idle();
  }

  /** Cancels every pending retry; returns how many were cancelled. */
  cancelRetries(): number {
    const count = this.retries.size;
    for (const timer of this.retries) clearTimeout(timer);
    this.retries.clear();
    return count;
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
