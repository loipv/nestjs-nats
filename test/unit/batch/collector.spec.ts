import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { BatchCollector } from '../../../src/batch/collector.js';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

function setup(maxSize: number, maxWait: number, flush?: (items: string[]) => Promise<void>) {
  const flushed: string[][] = [];
  const collector = new BatchCollector<string>({
    maxSize,
    maxWait,
    flush: async (items) => {
      flushed.push(items);
      await flush?.(items);
    },
  });
  return { collector, flushed };
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('BatchCollector', () => {
  it('flushes at maxSize', async () => {
    const { collector, flushed } = setup(3, 1000);
    await collector.add('a');
    await collector.add('b');
    expect(flushed).toEqual([]);
    expect(collector.size).toBe(2);
    await collector.add('c');
    expect(flushed).toEqual([['a', 'b', 'c']]);
    expect(collector.size).toBe(0);
    // The timer of the flushed batch does not fire an empty flush.
    await vi.advanceTimersByTimeAsync(2000);
    expect(flushed).toEqual([['a', 'b', 'c']]);
  });

  it('flushes after maxWait from the first item', async () => {
    const { collector, flushed } = setup(10, 100);
    await collector.add('a');
    await vi.advanceTimersByTimeAsync(50);
    await collector.add('b');
    await vi.advanceTimersByTimeAsync(49);
    expect(flushed).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(flushed).toEqual([['a', 'b']]);
    // The next batch gets its own timer, started by its first item.
    await collector.add('c');
    await vi.advanceTimersByTimeAsync(99);
    expect(flushed).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(flushed).toEqual([['a', 'b'], ['c']]);
  });

  it('applies backpressure while a flush runs', async () => {
    const gate = deferred();
    const { collector, flushed } = setup(2, 1000, () => gate.promise);
    await collector.add('a');
    let secondDone = false;
    const second = collector.add('b').then(() => {
      secondDone = true;
    });
    let thirdDone = false;
    const third = collector.add('c').then(() => {
      thirdDone = true;
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(flushed).toEqual([['a', 'b']]);
    expect(secondDone).toBe(false);
    expect(thirdDone).toBe(false);
    expect(collector.size).toBe(0);
    gate.resolve();
    await second;
    await third;
    expect(thirdDone).toBe(true);
    expect(collector.size).toBe(1);
  });

  it('flushNow delivers a partial batch', async () => {
    const { collector, flushed } = setup(10, 1000);
    await collector.add('a');
    await collector.add('b');
    await collector.flushNow();
    expect(flushed).toEqual([['a', 'b']]);
    await collector.flushNow();
    await vi.advanceTimersByTimeAsync(2000);
    expect(flushed).toEqual([['a', 'b']]);
  });

  it('flushNow waits for a running flush first', async () => {
    const gate = deferred();
    let n = 0;
    const { collector, flushed } = setup(10, 100, () =>
      n++ === 0 ? gate.promise : Promise.resolve(),
    );
    await collector.add('a');
    await vi.advanceTimersByTimeAsync(100);
    expect(flushed).toEqual([['a']]);
    let done = false;
    const flushing = collector.flushNow().then(() => {
      done = true;
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(done).toBe(false);
    gate.resolve();
    await flushing;
    expect(flushed).toEqual([['a']]);
  });

  it('reports a failing timer flush to onError', async () => {
    const onError = vi.fn();
    const collector = new BatchCollector<string>({
      maxSize: 10,
      maxWait: 10,
      flush: () => Promise.reject(new Error('boom')),
      onError,
    });
    await collector.add('a');
    await vi.advanceTimersByTimeAsync(10);
    expect(onError).toHaveBeenCalledWith(expect.objectContaining({ message: 'boom' }));
  });

  it('asks admit once a running flush is over and skips the item when it refuses', async () => {
    const gate = deferred();
    const { collector, flushed } = setup(2, 1000, () => gate.promise);
    await collector.add('a');
    const running = collector.add('b');
    const admit = vi.fn(() => false);
    const refused = collector.add('c', admit);
    const accepted = collector.add('d', () => true);
    await vi.advanceTimersByTimeAsync(0);
    expect(admit).not.toHaveBeenCalled();
    gate.resolve();
    await running;
    await refused;
    await accepted;
    expect(admit).toHaveBeenCalledTimes(1);
    expect(collector.take()).toEqual(['d']);
    expect(flushed).toEqual([['a', 'b']]);
  });

  it('take removes the collected items without flushing them', async () => {
    const { collector, flushed } = setup(10, 100);
    await collector.add('a');
    await collector.add('b');
    expect(collector.take()).toEqual(['a', 'b']);
    expect(collector.size).toBe(0);
    await vi.advanceTimersByTimeAsync(1000);
    expect(flushed).toEqual([]);
  });
});
