import { afterEach, describe, expect, it, vi } from 'vitest';
import { Dispatcher, type DispatchItem } from '../../src/adapters/dispatcher.js';
import { NatsConfigError } from '../../src/errors/errors.js';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

function deferred() {
  let resolve!: () => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function setup(
  opts: { concurrency?: number; process?: (items: DispatchItem<string>[]) => Promise<void> } = {},
) {
  const seen: DispatchItem<string>[][] = [];
  const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
  const onRejected = vi.fn();
  const d = new Dispatcher<string>({
    concurrency: opts.concurrency ?? 1,
    process: async (items) => {
      seen.push(items);
      await opts.process?.(items);
    },
    logger,
    label: 'H.m on "a.b"',
    onRejected,
  });
  return { d, seen, logger, onRejected };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('Dispatcher', () => {
  it('validates concurrency', () => {
    expect(() => setup({ concurrency: 0 })).toThrow(NatsConfigError);
  });

  it('caps concurrency and applies backpressure', async () => {
    const gates = [deferred(), deferred(), deferred()];
    let n = 0;
    const { d, seen } = setup({
      concurrency: 2,
      process: () => (gates[n++] as ReturnType<typeof deferred>).promise,
    });
    await d.push('a');
    await d.push('b');
    expect(d.active).toBe(2);
    let thirdAccepted = false;
    const third = d.push('c').then(() => {
      thirdAccepted = true;
    });
    await tick();
    expect(thirdAccepted).toBe(false);
    expect(seen.map((items) => items[0]?.msg)).toEqual(['a', 'b']);
    gates[0]?.resolve();
    await third;
    expect(thirdAccepted).toBe(true);
    expect(seen.map((items) => items[0]?.msg)).toEqual(['a', 'b', 'c']);
    expect(d.active).toBe(2);
    gates[1]?.resolve();
    gates[2]?.resolve();
    await d.idle();
    expect(d.active).toBe(0);
  });

  it('passes attempt through', async () => {
    const { d, seen } = setup();
    await d.push('a', 3);
    await d.idle();
    expect(seen).toEqual([[{ msg: 'a', attempt: 3 }]]);
  });

  it('defaults attempt to 1', async () => {
    const { d, seen } = setup();
    await d.push('a');
    await d.idle();
    expect(seen).toEqual([[{ msg: 'a', attempt: 1 }]]);
  });

  it('isolates process failures', async () => {
    let fail = true;
    const { d, seen, logger } = setup({
      process: () => {
        if (fail) {
          fail = false;
          return Promise.reject(new Error('boom'));
        }
        return Promise.resolve();
      },
    });
    await d.push('a');
    await d.idle();
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('H.m on "a.b"'),
      expect.stringContaining('boom'),
    );
    await d.push('b');
    await d.idle();
    expect(seen.map((items) => items[0]?.msg)).toEqual(['a', 'b']);
    expect(d.active).toBe(0);
  });

  it('isolates a process that throws synchronously', async () => {
    const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
    const d = new Dispatcher<string>({
      concurrency: 1,
      process: () => {
        throw new Error('sync boom');
      },
      logger,
      label: 'L',
    });
    await d.push('a');
    await d.idle();
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('L'),
      expect.stringContaining('sync boom'),
    );
    expect(d.active).toBe(0);
  });

  it('schedules retries with attempt + 1', async () => {
    vi.useFakeTimers();
    const { d, seen } = setup();
    d.scheduleRetry({ msg: 'a', attempt: 1 }, 500);
    await vi.advanceTimersByTimeAsync(499);
    expect(seen).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(seen.at(-1)).toEqual([{ msg: 'a', attempt: 2 }]);
    expect(d.cancelRetries()).toBe(0);
  });

  it('cancelRetries cancels pending timers', async () => {
    vi.useFakeTimers();
    const { d, seen } = setup();
    d.scheduleRetry({ msg: 'a', attempt: 1 }, 500);
    expect(d.cancelRetries()).toBe(1);
    await vi.advanceTimersByTimeAsync(1000);
    expect(seen).toEqual([]);
    expect(d.cancelRetries()).toBe(0);
  });

  it('rejects pushes after stop', async () => {
    const { d, seen, onRejected } = setup();
    d.stop();
    await d.push('x');
    expect(onRejected).toHaveBeenCalledWith('x');
    expect(seen).toEqual([]);
  });

  it('rejects a push that was waiting for a slot when stop() ran', async () => {
    const gate = deferred();
    const { d, seen, onRejected } = setup({ process: () => gate.promise });
    await d.push('a');
    const waiting = d.push('b');
    await tick();
    d.stop();
    gate.resolve();
    await waiting;
    await d.idle();
    expect(seen.map((items) => items[0]?.msg)).toEqual(['a']);
    expect(onRejected).toHaveBeenCalledWith('b');
    expect(d.active).toBe(0);
  });

  it('allows scheduling retries after stop, which are rejected when they fire', async () => {
    vi.useFakeTimers();
    const { d, seen, onRejected } = setup();
    d.stop();
    d.scheduleRetry({ msg: 'a', attempt: 1 }, 100);
    await vi.advanceTimersByTimeAsync(100);
    expect(seen).toEqual([]);
    expect(onRejected).toHaveBeenCalledWith('a');
  });

  it('logs and survives an onRejected that throws', async () => {
    const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
    const d = new Dispatcher<string>({
      concurrency: 1,
      process: () => Promise.resolve(),
      logger,
      label: 'L',
      onRejected: () => {
        throw new Error('nak failed');
      },
    });
    d.stop();
    await expect(d.push('a')).resolves.toBeUndefined();
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('L'),
      expect.stringContaining('nak failed'),
    );
  });

  it('idle waits for in-flight work only', async () => {
    vi.useFakeTimers();
    const gate = deferred();
    const { d } = setup({ process: () => gate.promise });
    await d.push('a');
    d.scheduleRetry({ msg: 'b', attempt: 1 }, 10_000);
    let idle = false;
    const idling = d.idle().then(() => {
      idle = true;
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(idle).toBe(false);
    gate.resolve();
    await idling;
    expect(idle).toBe(true);
    expect(d.cancelRetries()).toBe(1);
  });

  it('unrefs retry timers', () => {
    const unref = vi.fn();
    const spy = vi
      .spyOn(globalThis, 'setTimeout')
      .mockReturnValue({ unref } as unknown as ReturnType<typeof setTimeout>);
    try {
      const { d } = setup();
      d.scheduleRetry({ msg: 'a', attempt: 1 }, 100);
      expect(unref).toHaveBeenCalledTimes(1);
    } finally {
      spy.mockRestore();
    }
  });

  it('abandon returns pushes waiting for a slot, which never start nor get rejected', async () => {
    const gate = deferred();
    const { d, seen, onRejected } = setup({ process: () => gate.promise });
    await d.push('a'); // running
    const b = d.push('b');
    const c = d.push('c');
    await tick();
    expect(d.abandon()).toEqual(['b', 'c']);
    gate.resolve();
    await b;
    await c;
    await d.idle();
    expect(seen.map((items) => items.map((i) => i.msg))).toEqual([['a']]);
    expect(onRejected).not.toHaveBeenCalled();
    expect(d.active).toBe(0);
    expect(d.abandon()).toEqual([]);
  });

  it('does not return pushes that already started or were rejected', async () => {
    const gate = deferred();
    const { d, onRejected } = setup({ process: () => gate.promise });
    await d.push('a');
    const b = d.push('b');
    await tick();
    void d.stop();
    gate.resolve();
    await b; // got the slot after stop(): rejected
    expect(onRejected).toHaveBeenCalledWith('b');
    expect(d.abandon()).toEqual([]);
    await d.idle();
  });

  describe('batch', () => {
    function batchSetup(
      batch: { maxSize: number; maxWait: number },
      process?: (items: DispatchItem<string>[]) => Promise<void>,
      concurrency = 1,
    ) {
      const seen: DispatchItem<string>[][] = [];
      const onRejected = vi.fn();
      const d = new Dispatcher<string>({
        concurrency,
        batch,
        process: async (items) => {
          seen.push(items);
          await process?.(items);
        },
        logger: { log: vi.fn(), error: vi.fn(), warn: vi.fn() },
        label: 'L',
        onRejected,
      });
      return { d, seen, onRejected };
    }

    it('processes collected items as one call holding one slot', async () => {
      const gate = deferred();
      const { d, seen } = batchSetup({ maxSize: 2, maxWait: 60_000 }, () => gate.promise);
      await d.push('a');
      expect(seen).toEqual([]);
      await d.push('b', 3);
      expect(seen).toEqual([
        [
          { msg: 'a', attempt: 1 },
          { msg: 'b', attempt: 3 },
        ],
      ]);
      expect(d.active).toBe(1);
      gate.resolve();
      await d.idle();
      expect(d.active).toBe(0);
    });

    it('waits for a slot before processing the next batch (backpressure)', async () => {
      const gate = deferred();
      let n = 0;
      const { d, seen } = batchSetup({ maxSize: 1, maxWait: 60_000 }, () =>
        n++ === 0 ? gate.promise : Promise.resolve(),
      );
      await d.push('a');
      let accepted = false;
      const second = d.push('b').then(() => {
        accepted = true;
      });
      await tick();
      expect(accepted).toBe(false);
      expect(seen).toHaveLength(1);
      gate.resolve();
      await second;
      await d.idle();
      expect(seen.map((items) => items.map((i) => i.msg))).toEqual([['a'], ['b']]);
    });

    it('flushes after maxWait', async () => {
      vi.useFakeTimers();
      const { d, seen } = batchSetup({ maxSize: 10, maxWait: 100 });
      await d.push('a');
      await vi.advanceTimersByTimeAsync(99);
      expect(seen).toEqual([]);
      await vi.advanceTimersByTimeAsync(1);
      expect(seen).toEqual([[{ msg: 'a', attempt: 1 }]]);
    });

    it('stop() hands the pending batch to process and rejects later pushes', async () => {
      const { d, seen, onRejected } = batchSetup({ maxSize: 10, maxWait: 60_000 });
      await d.push('a');
      await d.push('b');
      await d.stop();
      await d.idle();
      expect(seen.map((items) => items.map((i) => i.msg))).toEqual([['a', 'b']]);
      await d.push('c');
      expect(onRejected).toHaveBeenCalledWith('c');
    });

    it('idle() waits for the batch flushed by stop()', async () => {
      const gate = deferred();
      const { d, seen } = batchSetup({ maxSize: 10, maxWait: 60_000 }, () => gate.promise);
      await d.push('a');
      void d.stop();
      let idle = false;
      const idling = d.idle().then(() => {
        idle = true;
      });
      await tick();
      expect(seen).toHaveLength(1);
      expect(idle).toBe(false);
      gate.resolve();
      await idling;
    });

    it('returns a retried item to the collector with attempt + 1', async () => {
      vi.useFakeTimers();
      const { d, seen } = batchSetup({ maxSize: 2, maxWait: 1000 });
      d.scheduleRetry({ msg: 'a', attempt: 1 }, 100);
      await vi.advanceTimersByTimeAsync(100);
      expect(seen).toEqual([]);
      await d.push('b');
      expect(seen).toEqual([
        [
          { msg: 'a', attempt: 2 },
          { msg: 'b', attempt: 1 },
        ],
      ]);
    });

    it('abandon returns collected items and waiting batches, which never start', async () => {
      const gate = deferred();
      let n = 0;
      const { d, seen, onRejected } = batchSetup({ maxSize: 2, maxWait: 60_000 }, () =>
        n++ === 0 ? gate.promise : Promise.resolve(),
      );
      await d.push('a');
      await d.push('b'); // first batch running
      await d.push('c');
      void d.stop(); // flushes ['c'], waiting for the slot
      await tick();
      expect(d.abandon()).toEqual(['c']);
      gate.resolve();
      await d.idle();
      expect(seen.map((items) => items.map((i) => i.msg))).toEqual([['a', 'b']]);
      expect(d.active).toBe(0);
      expect(onRejected).not.toHaveBeenCalled();
      expect(d.abandon()).toEqual([]);
    });

    it('abandon returns items still being collected', async () => {
      vi.useFakeTimers();
      const { d, seen, onRejected } = batchSetup({ maxSize: 10, maxWait: 100 });
      await d.push('a');
      await d.push('b');
      expect(d.abandon()).toEqual(['a', 'b']);
      await vi.advanceTimersByTimeAsync(1000);
      await d.idle();
      expect(seen).toEqual([]);
      await d.push('c');
      expect(onRejected).toHaveBeenCalledWith('c');
    });

    it('rejects a push that was waiting behind a flush when abandon() ran', async () => {
      const gate = deferred();
      let n = 0;
      const { d, seen, onRejected } = batchSetup({ maxSize: 1, maxWait: 60_000 }, () =>
        n++ === 0 ? gate.promise : Promise.resolve(),
      );
      await d.push('a'); // running
      const waiting = d.push('b'); // flush of ['b'] waits for the slot
      await tick();
      const late = d.push('c'); // waits behind that flush
      await tick();
      expect(d.abandon()).toEqual(['b']);
      gate.resolve();
      await waiting;
      await late;
      await d.idle();
      expect(seen.map((items) => items.map((i) => i.msg))).toEqual([['a']]);
      expect(onRejected).toHaveBeenCalledWith('c');
    });
  });
});
