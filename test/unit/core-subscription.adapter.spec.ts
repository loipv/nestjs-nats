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

function setup(
  invoke: (payload: unknown) => Promise<unknown>,
  plugins: unknown[] = [],
  moduleOptions: Record<string, unknown> = {},
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
});
