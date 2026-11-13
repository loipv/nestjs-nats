import { afterEach, describe, expect, it, vi } from 'vitest';
import { settlesWithin } from '../../../src/util/timeout.js';
import { never } from '../../support/async.js';

describe('settlesWithin', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('is true for a promise that resolves in time', async () => {
    await expect(settlesWithin(Promise.resolve('x'), 10)).resolves.toBe(true);
  });

  it('is true for a promise that rejects in time, and does not reject itself', async () => {
    await expect(settlesWithin(Promise.reject(new Error('boom')), 10)).resolves.toBe(true);
  });

  it('is false once the time runs out', async () => {
    vi.useFakeTimers();
    const result = settlesWithin(never(), 1000);
    await vi.advanceTimersByTimeAsync(999);
    let settled = false;
    void result.then(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await expect(result).resolves.toBe(false);
  });

  it('counts an already settled promise even with no time left', async () => {
    await expect(settlesWithin(Promise.resolve(), 0)).resolves.toBe(true);
    await expect(settlesWithin(Promise.resolve(), -5)).resolves.toBe(true);
  });

  it('clears its timer when the promise settles first', async () => {
    vi.useFakeTimers();
    const result = settlesWithin(Promise.resolve(), 60_000);
    await expect(result).resolves.toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
});
