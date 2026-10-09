import { describe, expect, it, vi } from 'vitest';
import { CoreSubscriptionAdapter } from '../../src/adapters/core-subscription.adapter.js';
import type { ConnectionEntry } from '../../src/connection/connection-manager.js';
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

function setup(invoke: (payload: unknown) => Promise<unknown>, plugins: unknown[] = []) {
  const fake = fakeSubscription();
  const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
  const entry = {
    name: 'default',
    options: { plugins },
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
    options: {},
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

  it('logs handler failures and keeps going', async () => {
    const { adapter, fake, logger } = setup((p) =>
      p === 1 ? Promise.reject(new Error('boom')) : Promise.resolve(),
    );
    await adapter.start();
    fake.push(fakeMsg(1));
    fake.push(fakeMsg(2));
    await adapter.stop();
    await adapter.idle();
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('C.m'),
      expect.stringContaining('boom'),
    );
  });

  it('forceStop has no messages to nak', async () => {
    const { adapter } = setup(() => Promise.resolve());
    await adapter.start();
    await adapter.stop();
    await adapter.idle();
    expect(adapter.forceStop()).toEqual({ nakked: 0, cancelledRetries: 0 });
  });
});
