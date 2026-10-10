import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { sleep } from '../../src/util/sleep.js';

describe('sleep', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it('resolves true once the time is up', async () => {
    let result: boolean | undefined;
    void sleep(100).then((r) => {
      result = r;
    });
    await vi.advanceTimersByTimeAsync(99);
    expect(result).toBeUndefined();
    await vi.advanceTimersByTimeAsync(1);
    expect(result).toBe(true);
  });

  it('resolves false as soon as the signal aborts', async () => {
    const controller = new AbortController();
    let result: boolean | undefined;
    void sleep(100, controller.signal).then((r) => {
      result = r;
    });
    await vi.advanceTimersByTimeAsync(50);
    controller.abort();
    await Promise.resolve();
    expect(result).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('resolves false at once when the signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    await expect(sleep(100, controller.signal)).resolves.toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('does not keep the process alive', async () => {
    const unref = vi.fn();
    // Restored in afterEach, also when an expectation fails.
    vi.spyOn(globalThis, 'setTimeout').mockImplementation(((fn: () => void) => ({
      unref,
      fn,
    })) as never);
    void sleep(100);
    expect(unref).toHaveBeenCalledTimes(1);
  });
});
