import { describe, expect, it } from 'vitest';
import { compose, type Next } from '../../src/pipeline/compose.js';

describe('compose', () => {
  it('runs middlewares in onion order', async () => {
    const log: string[] = [];
    const mw = (n: string) => async (_: unknown, next: Next) => {
      log.push(`>${n}`);
      await next();
      log.push(`<${n}`);
    };
    await compose([mw('a'), mw('b')], async () => {
      log.push('t');
    })({});
    expect(log).toEqual(['>a', '>b', 't', '<b', '<a']);
  });

  it('rejects when next is called twice', async () => {
    const run = compose(
      [
        async (_: unknown, n: Next) => {
          await n();
          await n();
        },
      ],
      async () => {},
    );
    await expect(run({})).rejects.toThrow('next() called multiple times');
  });

  it('runs only the terminal when there are no middlewares', async () => {
    let called = false;
    await compose([], async () => {
      called = true;
    })({});
    expect(called).toBe(true);
  });

  it('lets a middleware short-circuit by not calling next', async () => {
    let called = false;
    await compose([async () => {}], async () => {
      called = true;
    })({});
    expect(called).toBe(false);
  });

  it('propagates errors from the terminal', async () => {
    const run = compose([async (_: unknown, n: Next) => n()], async () => {
      throw new Error('boom');
    });
    await expect(run({})).rejects.toThrow('boom');
  });
});
