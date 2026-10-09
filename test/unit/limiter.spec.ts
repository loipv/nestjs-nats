import { describe, expect, it } from 'vitest';
import { createLimiter } from '../../src/adapters/limiter.js';
import { NatsConfigError } from '../../src/errors/errors.js';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

describe('createLimiter', () => {
  it('caps concurrency', async () => {
    const l = createLimiter(2);
    await l.acquire();
    await l.acquire();
    let third = false;
    void l.acquire().then(() => {
      third = true;
    });
    await tick();
    expect(third).toBe(false);
    expect(l.active).toBe(2);
    l.release();
    await tick();
    expect(third).toBe(true);
    expect(l.active).toBe(2);
  });

  it('idle resolves when all released', async () => {
    const l = createLimiter(2);
    await l.acquire();
    await l.acquire();
    let idle = false;
    void l.idle().then(() => {
      idle = true;
    });
    l.release();
    await tick();
    expect(idle).toBe(false);
    l.release();
    await tick();
    expect(idle).toBe(true);
    expect(l.active).toBe(0);
  });

  it('idle resolves immediately when nothing is active', async () => {
    await expect(createLimiter(1).idle()).resolves.toBeUndefined();
  });

  it('idle waits for queued waiters handed a slot', async () => {
    const l = createLimiter(1);
    await l.acquire();
    const queued = l.acquire();
    let idle = false;
    void l.idle().then(() => {
      idle = true;
    });
    l.release();
    await queued;
    await tick();
    expect(idle).toBe(false);
    l.release();
    await tick();
    expect(idle).toBe(true);
  });

  it('grants slots in FIFO order', async () => {
    const l = createLimiter(1);
    await l.acquire();
    const order: number[] = [];
    void l.acquire().then(() => order.push(1));
    void l.acquire().then(() => order.push(2));
    l.release();
    await tick();
    l.release();
    await tick();
    expect(order).toEqual([1, 2]);
  });

  it.each([0, -1, 1.5, Number.NaN])('rejects concurrency %s', (c) =>
    expect(() => createLimiter(c)).toThrow(NatsConfigError),
  );
});
