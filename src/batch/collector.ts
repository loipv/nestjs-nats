/** Batch options of `@Consume` and `@ConsumePush`. */
export interface BatchOptions {
  /** Flush once this many messages are collected (an integer >= 1; default 100). */
  maxSize?: number;
  /** Flush this many ms after the first message of the batch (>= 0; default 1000). */
  maxWait?: number;
}

export interface ResolvedBatchOptions {
  maxSize: number;
  maxWait: number;
}

export const DEFAULT_BATCH: Readonly<ResolvedBatchOptions> = Object.freeze({
  maxSize: 100,
  maxWait: 1000,
});

/** Largest delay `setTimeout` honours; longer ones fire immediately. */
export const MAX_TIMER_DELAY = 2 ** 31 - 1;

/** The batch settings of a handler, or `undefined` when it does not batch. */
export function resolveBatchOptions(
  opts: BatchOptions | undefined,
): ResolvedBatchOptions | undefined {
  if (!opts) return undefined;
  return {
    maxSize: opts.maxSize ?? DEFAULT_BATCH.maxSize,
    maxWait: opts.maxWait ?? DEFAULT_BATCH.maxWait,
  };
}

export interface BatchCollectorOptions<T> {
  maxSize: number;
  maxWait: number;
  /** Receives each batch; `add` callers wait while it runs (backpressure). */
  flush: (items: T[]) => Promise<void>;
  /** Receives the error of a flush started by the `maxWait` timer. */
  onError?: (err: unknown) => void;
}

/**
 * Groups items into batches flushed when `maxSize` items are collected or `maxWait` ms
 * after the first item of the batch, whichever comes first. One flush runs at a time;
 * `add` waits for a running flush before collecting, so awaiting it applies backpressure.
 */
export class BatchCollector<T> {
  private items: T[] = [];
  private timer?: ReturnType<typeof setTimeout>;
  private flushing?: Promise<void>;

  constructor(private readonly opts: BatchCollectorOptions<T>) {}

  /** Number of items collected and not flushed yet. */
  get size(): number {
    return this.items.length;
  }

  /**
   * Collects `item`; resolves once it is collected, or flushed when it completed a batch.
   * `admit` is asked once a running flush is over: when it returns `false`, `item` is not
   * collected.
   */
  async add(item: T, admit?: () => boolean): Promise<void> {
    while (this.flushing) await this.flushing;
    if (admit && !admit()) return;
    this.items.push(item);
    if (this.items.length >= this.opts.maxSize) {
      await this.flushNow();
      return;
    }
    if (this.items.length === 1) this.arm();
  }

  /** Flushes the collected items (after a running flush, if any); resolves when flushed. */
  async flushNow(): Promise<void> {
    while (this.flushing) await this.flushing;
    this.disarm();
    if (this.items.length === 0) return;
    const batch = this.items;
    this.items = [];
    // An async wrapper turns a synchronous throw of `flush` into a rejection.
    const flushing = (async () => this.opts.flush(batch))();
    // What waiters wait on: settles with the flush but never rejects.
    const tracked: Promise<void> = flushing.then(
      () => this.release(tracked),
      () => this.release(tracked),
    );
    this.flushing = tracked;
    await flushing;
  }

  /** Removes and returns the collected items without flushing them (a running flush goes on). */
  take(): T[] {
    this.disarm();
    const items = this.items;
    this.items = [];
    return items;
  }

  private release(flushing: Promise<void>): void {
    if (this.flushing === flushing) this.flushing = undefined;
  }

  private arm(): void {
    this.disarm();
    const timer = setTimeout(() => {
      if (this.timer !== timer) return;
      this.timer = undefined;
      this.flushNow().catch((err: unknown) => this.opts.onError?.(err));
    }, this.opts.maxWait);
    // The source the items come from keeps the process alive, not a pending batch.
    timer.unref?.();
    this.timer = timer;
  }

  private disarm(): void {
    if (this.timer === undefined) return;
    clearTimeout(this.timer);
    this.timer = undefined;
  }
}
