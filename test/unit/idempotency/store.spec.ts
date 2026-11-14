import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { NatsConfigError } from '../../../src/errors/errors.js';
import { InMemoryIdempotencyStore } from '../../../src/idempotency/store.js';

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

  describe('a key longer than 256 characters', () => {
    const long = (end: string) => `scope|${'x'.repeat(1_000_000)}${end}`;
    const held = () => [...(s as unknown as { entries: Map<string, unknown> }).entries.keys()];

    it('is held by a short digest, not as it is', async () => {
      await s.claim(long('a'), 1000);
      expect(held()).toHaveLength(1);
      expect(held()[0]?.length).toBeLessThan(100);
    });

    it('is claimed, completed and released like any key', async () => {
      expect(await s.claim(long('a'), 1000)).toBe('claimed');
      expect(await s.claim(long('a'), 1000)).toBe('in-progress');
      expect(await s.claim(long('b'), 1000)).toBe('claimed');
      await s.complete(long('a'), 1000);
      expect(await s.claim(long('a'), 1000)).toBe('duplicate');
      await s.release(long('b'));
      expect(await s.claim(long('b'), 1000)).toBe('claimed');
    });

    it('never collides with a short key equal to its stored form', async () => {
      await s.claim(long('a'), 1000);
      const stored = held()[0] as string;
      expect(await s.claim(stored, 1000)).toBe('claimed');
      expect(await s.claim(stored.slice(2), 1000)).toBe('claimed');
    });

    it('keeps a key of exactly 256 characters as it is', async () => {
      const key = 'k'.repeat(256);
      await s.claim(key, 1000);
      expect(held()[0]).toContain(key);
    });
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
      const timed = new InMemoryIdempotencyStore({ now: () => t });
      await timed.claim('old', 1000);
      await timed.claim('fresh', 100_000);
      t = 5000;
      // Nothing touched the keys: only the sweep can remove the expired one.
      expect(timed.size).toBe(2);
      vi.advanceTimersByTime(60_000);
      expect(timed.size).toBe(1);
      await timed.close();
      expect(timed.size).toBe(0);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it('rejects an invalid maxKeys', () => {
    for (const maxKeys of [0, -1, 1.5, Number.NaN]) {
      expect(() => new InMemoryIdempotencyStore({ maxKeys })).toThrow(NatsConfigError);
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
