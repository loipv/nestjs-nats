import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { InMemoryIdempotencyStore } from '../../src/idempotency/store.js';

describe('InMemoryIdempotencyStore', () => {
  let now: number;
  let s: InMemoryIdempotencyStore;

  beforeEach(() => {
    now = 1_000_000;
    s = new InMemoryIdempotencyStore({ now: () => now });
  });

  afterEach(() => s.close());

  it('claims, then reports in-progress, then duplicate after complete', async () => {
    expect(await s.claim('k', 1000)).toBe('claimed');
    expect(await s.claim('k', 1000)).toBe('in-progress');
    await s.complete('k', 1000);
    expect(await s.claim('k', 1000)).toBe('duplicate');
  });

  it('expires done keys after ttl', async () => {
    await s.claim('k', 1000);
    await s.complete('k', 1000);
    now += 1001;
    expect(await s.claim('k', 1000)).toBe('claimed');
  });

  it('expires stale in-progress claims', async () => {
    await s.claim('k', 1000);
    now += 1001;
    expect(await s.claim('k', 1000)).toBe('claimed');
  });

  it('gives complete a fresh ttl', async () => {
    await s.claim('k', 1000);
    now += 900;
    await s.complete('k', 1000);
    now += 900;
    expect(await s.claim('k', 1000)).toBe('duplicate');
  });

  it('release frees the key', async () => {
    await s.claim('k', 1000);
    await s.release('k');
    expect(await s.claim('k', 1000)).toBe('claimed');
  });

  it('evicts least recently used beyond maxKeys', async () => {
    const small = new InMemoryIdempotencyStore({ maxKeys: 2, now: () => now });
    await small.claim('a', 1000);
    await small.claim('b', 1000);
    await small.claim('a', 1000); // touches a
    await small.claim('c', 1000); // evicts b
    expect(await small.claim('a', 1000)).toBe('in-progress');
    expect(await small.claim('c', 1000)).toBe('in-progress');
    expect(await small.claim('b', 1000)).toBe('claimed');
    await small.close();
  });

  it('sweeps expired entries periodically and stops on close', async () => {
    vi.useFakeTimers();
    try {
      let t = 0;
      const timed = new InMemoryIdempotencyStore({ now: () => t, maxKeys: 10 });
      await timed.claim('k', 1000);
      t = 5000;
      vi.advanceTimersByTime(60_000);
      // Expired and swept: a new claim starts from scratch.
      expect(await timed.claim('k', 1000)).toBe('claimed');
      await timed.close();
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it('close clears the data', async () => {
    await s.claim('k', 1000);
    await s.close();
    expect(await s.claim('k', 1000)).toBe('claimed');
  });

  it('does not keep the process alive with its sweep timer', () => {
    const spy = vi.spyOn(globalThis, 'setInterval');
    const created = new InMemoryIdempotencyStore();
    const timer = spy.mock.results[0]?.value as NodeJS.Timeout;
    expect(timer.hasRef()).toBe(false);
    spy.mockRestore();
    return created.close();
  });
});
