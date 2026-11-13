import { AsyncLocalStorage } from 'node:async_hooks';
import type { PubAck } from '@nats-io/jetstream';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Batcher } from '../../../src/batch/batcher.js';
import { NatsConfigError } from '../../../src/errors/errors.js';
import type { PublishManyResult } from '../../../src/jetstream/jetstream-client.js';

type Item = { subject: string; data?: unknown };

const ack = (seq: number): PubAck => ({ seq, stream: 'S', duplicate: false }) as PubAck;

/** A fake `publishMany`: acks each item with the next sequence number unless `fail(item)`. */
function fakePublishMany(fail: (item: Item) => Error | undefined = () => undefined) {
  let seq = 0;
  const batches: Item[][] = [];
  const fn = vi.fn(async (items: ReadonlyArray<Item>): Promise<PublishManyResult[]> => {
    batches.push([...items]);
    return items.map((item) => {
      const error = fail(item);
      return error ? { ok: false, error } : { ok: true, ack: ack(++seq) };
    });
  });
  return { fn, batches };
}

describe('Batcher', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('flushes at maxSize and resolves each add with its own ack', async () => {
    const { fn, batches } = fakePublishMany();
    const b = new Batcher(fn, { maxSize: 2, maxWait: 1000 });
    const p1 = b.add('a', 1);
    expect(b.pending).toBe(1);
    expect(fn).not.toHaveBeenCalled();
    const p2 = b.add('b', 2);
    expect((await p1).seq).toBe(1);
    expect((await p2).seq).toBe(2);
    expect(batches).toEqual([
      [
        { subject: 'a', data: 1, opts: undefined, run: expect.any(Function) },
        { subject: 'b', data: 2, opts: undefined, run: expect.any(Function) },
      ],
    ]);
    expect(b.pending).toBe(0);
  });

  it("hands each item a runner bound to its add's async context", async () => {
    const als = new AsyncLocalStorage<string>();
    const seen: Array<string | undefined> = [];
    const b = new Batcher(
      async (items) => {
        for (const item of items) seen.push(item.run(() => als.getStore()));
        return items.map((_, i) => ({ ok: true, ack: ack(i + 1) }) as PublishManyResult);
      },
      { maxSize: 3, maxWait: 1000 },
    );
    const p1 = als.run('a', () => b.add('s', 1));
    const p2 = als.run('b', () => b.add('s', 2));
    const p3 = als.run('c', () => b.add('s', 3));
    await Promise.all([p1, p2, p3]);
    expect(seen).toEqual(['a', 'b', 'c']);
  });

  it('flushes after maxWait from the first item', async () => {
    const { fn } = fakePublishMany();
    const b = new Batcher(fn, { maxSize: 100, maxWait: 50 });
    const p = b.add('a');
    await vi.advanceTimersByTimeAsync(49);
    expect(fn).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(fn).toHaveBeenCalledTimes(1);
    expect((await p).seq).toBe(1);
  });

  it('rejects only the failed item', async () => {
    const boom = new Error('no stream');
    const { fn } = fakePublishMany((i) => (i.subject === 'bad' ? boom : undefined));
    const b = new Batcher(fn, { maxSize: 3, maxWait: 1000 });
    const p1 = b.add('ok1');
    const p2 = b.add('bad');
    const p3 = b.add('ok2');
    await expect(p2).rejects.toBe(boom);
    expect((await p1).seq).toBe(1);
    expect((await p3).seq).toBe(2);
  });

  it('rejects every item when publishMany itself fails', async () => {
    const boom = new Error('down');
    const b = new Batcher(
      vi.fn(async () => {
        throw boom;
      }),
      { maxSize: 2, maxWait: 1000 },
    );
    const results = await Promise.allSettled([b.add('a'), b.add('b')]);
    expect(results.map((r) => r.status)).toEqual(['rejected', 'rejected']);
  });

  it('rejects an item whose result is missing', async () => {
    const b = new Batcher(
      vi.fn(async () => []),
      { maxSize: 1, maxWait: 1000 },
    );
    await expect(b.add('a')).rejects.toThrow(/no result/);
  });

  it('flush() waits for every added item and clears the timer', async () => {
    const { fn } = fakePublishMany();
    const b = new Batcher(fn, { maxSize: 100, maxWait: 60_000 });
    const ps = [b.add('a'), b.add('b'), b.add('c')];
    await b.flush();
    expect(b.pending).toBe(0);
    expect((await Promise.all(ps)).map((a) => a.seq)).toEqual([1, 2, 3]);
    expect(vi.getTimerCount()).toBe(0);
    await b.flush(); // nothing pending: a no-op
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('runs one publishMany at a time and passes window through', async () => {
    let active = 0;
    let maxActive = 0;
    const fn = vi.fn(
      async (
        items: ReadonlyArray<Item>,
        _opts?: { window?: number },
      ): Promise<PublishManyResult[]> => {
        active++;
        maxActive = Math.max(maxActive, active);
        await new Promise((r) => setTimeout(r, 10));
        active--;
        return items.map(() => ({ ok: true, ack: ack(1) }));
      },
    );
    const b = new Batcher(fn, { maxSize: 1, maxWait: 1000, window: 4 });
    const ps = [b.add('a'), b.add('b'), b.add('c')];
    await vi.advanceTimersByTimeAsync(100);
    await Promise.all(ps);
    expect(maxActive).toBe(1);
    expect(fn).toHaveBeenCalledTimes(3);
    expect(fn.mock.calls[0]?.[1]).toEqual({ window: 4 });
  });

  it('close flushes and rejects later adds', async () => {
    const { fn } = fakePublishMany();
    const onClosed = vi.fn();
    const b = new Batcher(fn, { maxSize: 100, maxWait: 60_000 }, onClosed);
    const p = b.add('a');
    await b.close();
    expect((await p).seq).toBe(1);
    expect(onClosed).toHaveBeenCalledTimes(1);
    await expect(b.add('x')).rejects.toThrow('Batcher is closed');
    await b.close(); // idempotent
    expect(onClosed).toHaveBeenCalledTimes(1);
  });

  it('settles an add racing with close', async () => {
    const { fn } = fakePublishMany();
    const b = new Batcher(fn, { maxSize: 100, maxWait: 60_000 });
    const closing = b.close();
    const late = b.add('late');
    await closing;
    await expect(late).rejects.toThrow('Batcher is closed');
    expect(b.pending).toBe(0);
  });

  it('close waits for items of a flush already running', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    const fn = vi.fn(async (items: ReadonlyArray<Item>): Promise<PublishManyResult[]> => {
      await gate;
      return items.map(() => ({ ok: true, ack: ack(1) }));
    });
    const b = new Batcher(fn, { maxSize: 1, maxWait: 1000 });
    const first = b.add('a'); // flush starts and blocks
    const second = b.add('b'); // queued behind it
    let closed = false;
    const closing = b.close().then(() => {
      closed = true;
    });
    await vi.advanceTimersByTimeAsync(10);
    expect(closed).toBe(false);
    release();
    await closing;
    await Promise.all([first, second]);
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it('validates its options', () => {
    const { fn } = fakePublishMany();
    expect(() => new Batcher(fn, { maxSize: 0, maxWait: 1 })).toThrow(NatsConfigError);
    expect(() => new Batcher(fn, { maxSize: 1.5, maxWait: 1 })).toThrow(NatsConfigError);
    expect(() => new Batcher(fn, { maxSize: 1, maxWait: -1 })).toThrow(NatsConfigError);
    expect(() => new Batcher(fn, { maxSize: 1, maxWait: 1, window: 0 })).toThrow(NatsConfigError);
  });
});
