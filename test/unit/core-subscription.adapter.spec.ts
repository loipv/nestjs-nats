import { describe, expect, it, vi } from 'vitest';
import { CoreSubscriptionAdapter } from '../../src/adapters/core-subscription.adapter.js';
import type { ConnectionEntry } from '../../src/connection/connection-registry.js';
import type { HandlerDefinition } from '../../src/discovery/handler-explorer.js';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Push-driven subscription whose `drain()` ends iteration after buffered messages. */
function fakeSubscription() {
  const queue: unknown[] = [];
  let wake: (() => void) | undefined;
  let closed = false;
  const sub = {
    async *[Symbol.asyncIterator]() {
      for (;;) {
        const next = queue.shift();
        if (next) yield next;
        else if (closed) return;
        else await new Promise<void>((resolve) => (wake = resolve));
      }
    },
    isClosed: () => closed,
    drain: vi.fn(() => {
      closed = true;
      wake?.();
      return Promise.resolve();
    }),
  };
  return {
    sub,
    push(msg: unknown) {
      queue.push(msg);
      wake?.();
    },
  };
}

function fakeMsg(payload: unknown) {
  return {
    subject: 'a.b',
    reply: '',
    data: new TextEncoder().encode(JSON.stringify(payload)),
    headers: undefined,
    respond: vi.fn(() => true),
  };
}

function setup(
  invoke: (payload: unknown, ctx?: unknown) => Promise<unknown>,
  plugins: unknown[] = [],
  moduleOptions: Record<string, unknown> = {},
  handlerOptions: Record<string, unknown> = {},
) {
  const fake = fakeSubscription();
  const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
  const entry = {
    name: 'default',
    options: { plugins, ...moduleOptions },
    logger,
    nc: {
      subscribe: vi.fn(() => fake.sub),
      flush: () => Promise.resolve(),
      isClosed: () => false,
    },
  } as unknown as ConnectionEntry;
  const def = {
    kind: 'subscribe',
    connection: 'default',
    className: 'C',
    methodName: 'm',
    subject: 'a.b',
    options: handlerOptions,
    invoke,
  } as unknown as HandlerDefinition & { kind: 'subscribe' };
  return { adapter: new CoreSubscriptionAdapter(def, entry), fake, logger };
}

describe('CoreSubscriptionAdapter', () => {
  it('delivers messages with attempt 1', async () => {
    const attempts: unknown[] = [];
    const plugin = {
      inbound: {
        async handle(d: { messages: Array<{ attempt: number }> }, next: () => Promise<void>) {
          attempts.push(d.messages.map((m) => m.attempt));
          await next();
        },
      },
    };
    const invoke = vi.fn(() => Promise.resolve());
    const { adapter, fake } = setup(invoke, [plugin]);
    await adapter.start();
    fake.push(fakeMsg(1));
    await tick();
    await tick();
    expect(invoke).toHaveBeenCalledTimes(1);
    expect(attempts).toEqual([[1]]);
    await adapter.stop();
    await adapter.idle();
  });

  it('still processes messages buffered before the drain', async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const handled: unknown[] = [];
    const { adapter, fake } = setup(async (payload) => {
      handled.push(payload);
      await gate;
    });
    await adapter.start();
    fake.push(fakeMsg(1));
    fake.push(fakeMsg(2));
    await tick();
    expect(handled).toEqual([1]);
    await adapter.stop();
    release();
    await adapter.idle();
    expect(handled).toEqual([1, 2]);
  });

  it('logs handler failures without retries and keeps going', async () => {
    const { adapter, fake, logger } = setup(
      (p) => (p === 1 ? Promise.reject(new Error('boom')) : Promise.resolve()),
      [],
      { retry: false },
    );
    await adapter.start();
    fake.push(fakeMsg(1));
    fake.push(fakeMsg(2));
    await adapter.stop();
    await adapter.idle();
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith('C.m on "a.b" failed, gave up after 1 attempt: boom');
  });

  it('retries a failing message in-process with the next attempt', async () => {
    const attempts: number[] = [];
    const plugin = {
      inbound: {
        async handle(d: { messages: Array<{ attempt: number }> }, next: () => Promise<void>) {
          attempts.push(...d.messages.map((m) => m.attempt));
          await next();
        },
      },
    };
    const invoke = vi.fn(() => Promise.reject(new Error('boom')));
    const { adapter, fake, logger } = setup(invoke, [plugin], {
      retry: { maxRetries: 2, initialDelay: 10, jitter: 0 },
    });
    await adapter.start();
    fake.push(fakeMsg(1));
    await vi.waitFor(() => expect(attempts).toEqual([1, 2, 3]));
    await vi.waitFor(() =>
      expect(logger.error).toHaveBeenCalledWith(
        'C.m on "a.b" failed, gave up after 3 attempts: boom',
      ),
    );
    expect(logger.warn).toHaveBeenCalledTimes(2);
    await adapter.stop();
    await adapter.idle();
  });

  it('warns when a retry fires after the subscription stopped', async () => {
    const { adapter, fake, logger } = setup(() => Promise.reject(new Error('boom')), [], {
      retry: { initialDelay: 30, jitter: 0 },
    });
    await adapter.start();
    fake.push(fakeMsg(1));
    await tick();
    await tick();
    await adapter.stop();
    await adapter.idle();
    await vi.waitFor(() =>
      expect(logger.warn).toHaveBeenCalledWith(
        'Dropped message on "a.b" for C.m on "a.b": the subscription is stopping',
      ),
    );
  });

  it('cancels pending retries on forceStop', async () => {
    const { adapter, fake } = setup(() => Promise.reject(new Error('boom')), [], {
      retry: { initialDelay: 10_000, jitter: 0 },
    });
    await adapter.start();
    fake.push(fakeMsg(1));
    await tick();
    await tick();
    await adapter.stop();
    await adapter.idle();
    expect(adapter.forceStop()).toEqual({ nakked: 0, cancelledRetries: 1 });
  });

  it('replies with an error and warns once for 5xx', async () => {
    const { adapter, fake, logger } = setup(() => Promise.reject(new Error('internals')));
    await adapter.start();
    const msg = { ...fakeMsg(1), reply: '_INBOX.1' };
    fake.push(msg);
    await tick();
    await tick();
    expect(msg.respond).toHaveBeenCalledTimes(1);
    expect(logger.warn).toHaveBeenCalledTimes(1);
    expect(logger.warn).toHaveBeenCalledWith('Replied 500 to request on "a.b" for C.m: internals');
    expect(logger.error).not.toHaveBeenCalled();
    await adapter.stop();
    await adapter.idle();
  });

  it('does not warn for 4xx replies', async () => {
    const { BadRequestException } = await import('@nestjs/common');
    const { adapter, fake, logger } = setup(() => Promise.reject(new BadRequestException('bad')));
    await adapter.start();
    fake.push({ ...fakeMsg(1), reply: '_INBOX.1' });
    await tick();
    await tick();
    expect(logger.warn).not.toHaveBeenCalled();
    await adapter.stop();
    await adapter.idle();
  });

  it('forceStop has no messages to nak', async () => {
    const { adapter } = setup(() => Promise.resolve());
    await adapter.start();
    await adapter.stop();
    await adapter.idle();
    expect(adapter.forceStop()).toEqual({ nakked: 0, cancelledRetries: 0 });
  });

  describe('batch', () => {
    it('delivers batches of maxSize and never replies', async () => {
      const batches: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          batches.push(payload);
          return 'reply';
        },
        [],
        {},
        { batch: { maxSize: 2, maxWait: 60_000 } },
      );
      await adapter.start();
      const msgs = [1, 2, 3, 4].map((n) => ({ ...fakeMsg(n), reply: `_INBOX.${n}` }));
      for (const m of msgs) fake.push(m);
      await tick();
      await tick();
      expect(batches).toEqual([
        [1, 2],
        [3, 4],
      ]);
      for (const m of msgs) expect(m.respond).not.toHaveBeenCalled();
      await adapter.stop();
      await adapter.idle();
    });

    it('flushes the pending batch when the subscription drains', async () => {
      const batches: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          batches.push(payload);
        },
        [],
        {},
        { batch: { maxSize: 10, maxWait: 60_000 } },
      );
      await adapter.start();
      fake.push(fakeMsg(1));
      fake.push(fakeMsg(2));
      await tick();
      expect(batches).toEqual([]);
      await adapter.stop();
      await adapter.idle();
      expect(batches).toEqual([[1, 2]]);
    });

    it('retries a failed item in a later batch with the next attempt', async () => {
      const seen: Array<Array<[unknown, number]>> = [];
      const plugin = {
        inbound: {
          async handle(
            d: { messages: Array<{ attempt: number; data: Uint8Array }> },
            next: () => Promise<void>,
          ) {
            seen.push(
              d.messages.map((m) => [JSON.parse(new TextDecoder().decode(m.data)), m.attempt]),
            );
            await next();
          },
        },
      };
      let first = true;
      const { adapter, fake } = setup(
        async (_payload, ctx) => {
          if (first) {
            first = false;
            (ctx as { fail(i: number, e: unknown): void }).fail(1, new Error('x'));
          }
        },
        [plugin],
        { retry: { initialDelay: 10, jitter: 0 } },
        { batch: { maxSize: 2, maxWait: 30 } },
      );
      await adapter.start();
      fake.push(fakeMsg(1));
      fake.push(fakeMsg(2));
      await vi.waitFor(() =>
        expect(seen).toEqual([
          [
            [1, 1],
            [2, 1],
          ],
          [[2, 2]],
        ]),
      );
      await adapter.stop();
      await adapter.idle();
    });

    it('forceStop drops collected messages and warns with their count', async () => {
      const invoke = vi.fn(() => Promise.resolve());
      const { adapter, fake, logger } = setup(
        invoke,
        [],
        {},
        {
          batch: { maxSize: 10, maxWait: 60_000 },
        },
      );
      await adapter.start();
      fake.push(fakeMsg(1));
      fake.push(fakeMsg(2));
      await tick();
      expect(adapter.forceStop()).toEqual({ nakked: 0, cancelledRetries: 0 });
      expect(logger.warn).toHaveBeenCalledWith(
        'Dropped 2 unstarted messages (collected or queued) for C.m on "a.b" at forced stop',
      );
      await adapter.stop();
      await adapter.idle();
      expect(invoke).not.toHaveBeenCalled();
    });
  });
});
