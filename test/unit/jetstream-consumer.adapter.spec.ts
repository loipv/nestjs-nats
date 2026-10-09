import type { ConsumerMessages, JsMsg } from '@nats-io/jetstream';
import { describe, expect, it, vi } from 'vitest';
import { JetStreamConsumerAdapter } from '../../src/adapters/jetstream-consumer.adapter.js';
import type { ConnectionEntry } from '../../src/connection/connection-manager.js';
import type { HandlerDefinition } from '../../src/discovery/handler-explorer.js';
import type { NatsStreamManager } from '../../src/jetstream/stream-manager.js';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Push-driven async iterable whose `close()` ends iteration after buffered items (like nats.js). */
function fakeMessages() {
  const queue: JsMsg[] = [];
  let wake: (() => void) | undefined;
  let closed = false;
  const statusQueue: unknown[] = [];
  let statusWake: (() => void) | undefined;
  const messages = {
    async *status() {
      for (;;) {
        const next = statusQueue.shift();
        if (next) yield next;
        else if (closed) return;
        else await new Promise<void>((resolve) => (statusWake = resolve));
      }
    },
    async *[Symbol.asyncIterator]() {
      for (;;) {
        const next = queue.shift();
        if (next) yield next;
        else if (closed) return;
        else await new Promise<void>((resolve) => (wake = resolve));
      }
    },
    close: () => {
      closed = true;
      wake?.();
      statusWake?.();
      return Promise.resolve();
    },
  };
  return {
    messages: messages as unknown as ConsumerMessages,
    push(msg: JsMsg) {
      queue.push(msg);
      wake?.();
    },
    notify(n: unknown) {
      statusQueue.push(n);
      statusWake?.();
    },
    /** Ends the iterator without anybody calling stop() on the adapter. */
    end() {
      void messages.close();
    },
  };
}

function fakeMsg(id: number, deliveryCount = 1) {
  const msg = {
    subject: 'ord.x',
    data: new TextEncoder().encode(JSON.stringify(id)),
    headers: undefined,
    info: { stream: 'ORD', streamSequence: id, deliveryCount },
    ack: vi.fn(),
    nak: vi.fn(),
    term: vi.fn(),
    working: vi.fn(),
  };
  return msg;
}

function setup(
  invoke: (payload: unknown) => Promise<unknown>,
  opts: {
    concurrency?: number;
    plugins?: unknown[];
    consume?: () => Promise<ConsumerMessages>;
    /** The consumer's config as reported by `consumer.info()`. */
    consumerConfig?: { ack_wait?: number; max_deliver?: number };
    retry?: unknown;
  } = {},
) {
  const fake = fakeMessages();
  const consume = vi.fn(opts.consume ?? (() => Promise.resolve(fake.messages)));
  const info = vi.fn(() =>
    Promise.resolve({
      config: opts.consumerConfig ?? { ack_wait: 30_000_000_000, max_deliver: -1 },
    }),
  );
  const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
  const entry = {
    name: 'default',
    options: { plugins: opts.plugins },
    logger,
    js: () => ({ consumers: { get: () => Promise.resolve({ consume, info }) } }),
  } as unknown as ConnectionEntry;
  const def = {
    kind: 'consume',
    connection: 'default',
    className: 'C',
    methodName: 'm',
    options: { stream: 'ORD', durable: 'd', concurrency: opts.concurrency, retry: opts.retry },
    invoke,
  } as unknown as HandlerDefinition & { kind: 'consume' };
  const adapter = new JetStreamConsumerAdapter(def, entry, {} as NatsStreamManager);
  return { adapter, fake, consume, info, logger };
}

describe('JetStreamConsumerAdapter', () => {
  it('requests max(2 * concurrency, 2) messages', async () => {
    const { adapter, consume } = setup(() => Promise.resolve());
    await adapter.start();
    expect(consume).toHaveBeenCalledWith({ max_messages: 2 });
    await adapter.stop();
    await adapter.idle();
  });

  it('requests 8 messages for concurrency 4', async () => {
    const { adapter, consume } = setup(() => Promise.resolve(), { concurrency: 4 });
    await adapter.start();
    expect(consume).toHaveBeenCalledWith({ max_messages: 8 });
    await adapter.stop();
    await adapter.idle();
  });

  it('logs an error when the consume loop ends while not stopping', async () => {
    const { adapter, fake, logger } = setup(() => Promise.resolve());
    await adapter.start();
    fake.end();
    await adapter.idle();
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringMatching(/C\.m.*ORD\/d.*ended unexpectedly/),
    );
  });

  it('does not log when the loop ends because of stop()', async () => {
    const { adapter, logger } = setup(() => Promise.resolve());
    await adapter.start();
    await adapter.stop();
    await adapter.idle();
    expect(logger.error).not.toHaveBeenCalled();
  });

  it('logs consumer status notifications', async () => {
    const { adapter, fake, logger } = setup(() => Promise.resolve());
    await adapter.start();
    fake.notify({ type: 'consumer_deleted', stream: 'ORD', consumer: 'd' });
    fake.notify({ type: 'consumer_not_found', name: 'd', stream: 'ORD', count: 1 });
    await tick();
    expect(logger.error).toHaveBeenCalledWith(expect.stringMatching(/ORD\/d.*was deleted/));
    expect(logger.warn).toHaveBeenCalledWith(expect.stringMatching(/ORD\/d.*consumer_not_found/));
    await adapter.stop();
    await adapter.idle();
  });

  it('naks the message when the pipeline fails outside settlement', async () => {
    const plugin = {
      inbound: {
        stage: 'outer',
        handle: () => Promise.reject(new Error('plugin boom')),
      },
    };
    const { adapter, fake, logger } = setup(() => Promise.resolve(), { plugins: [plugin] });
    await adapter.start();
    const m = fakeMsg(1);
    fake.push(m as unknown as JsMsg);
    await tick();
    await tick();
    expect(m.nak).toHaveBeenCalledTimes(1);
    expect(m.ack).not.toHaveBeenCalled();
    expect(logger.error).toHaveBeenCalled();
    await adapter.stop();
    await adapter.idle();
  });

  it('closes freshly created messages when stop() ran during start()', async () => {
    const fake = fakeMessages();
    const closeSpy = vi.spyOn(fake.messages, 'close');
    let resolveConsume!: () => void;
    const { adapter } = setup(() => Promise.resolve(), {
      consume: () =>
        new Promise<ConsumerMessages>((resolve) => {
          resolveConsume = () => resolve(fake.messages);
        }),
    });
    const starting = adapter.start();
    await tick();
    await adapter.stop();
    resolveConsume();
    await starting;
    expect(closeSpy).toHaveBeenCalledTimes(1);
    await adapter.idle();
  });

  it('naks received-but-unstarted messages on stop and lets in-flight ones finish', async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const handled: unknown[] = [];
    const { adapter, fake } = setup(async (payload) => {
      handled.push(payload);
      await gate;
    });
    await adapter.start();
    const [a, b, c] = [fakeMsg(1), fakeMsg(2), fakeMsg(3)];
    for (const m of [a, b, c]) fake.push(m as unknown as JsMsg);
    await tick();
    expect(handled).toEqual([1]);

    await adapter.stop();
    release();
    await adapter.idle();

    expect(handled).toEqual([1]);
    expect(a.ack).toHaveBeenCalledTimes(1);
    expect(b.nak).toHaveBeenCalledTimes(1);
    expect(c.nak).toHaveBeenCalledTimes(1);
    expect(b.ack).not.toHaveBeenCalled();
  });

  it('does not wait for in-flight handlers in stop()', async () => {
    const { adapter, fake } = setup(() => new Promise<void>(() => {}));
    await adapter.start();
    fake.push(fakeMsg(1) as unknown as JsMsg);
    await tick();
    await expect(adapter.stop()).resolves.toBeUndefined();
  });

  it('hands plugins a narrow handler description', async () => {
    const seen: unknown[] = [];
    const plugin = {
      name: 'p',
      inbound: {
        async handle(d: { handler: unknown }, next: () => Promise<void>) {
          seen.push(d.handler);
          await next();
        },
      },
    };
    const invoke = vi.fn(() => Promise.resolve());
    const { adapter, fake } = setup(invoke, { plugins: [plugin] });
    await adapter.start();
    const msg = fakeMsg(1);
    fake.push(msg as unknown as JsMsg);
    await tick();
    await tick();
    expect(seen).toStrictEqual([
      {
        kind: 'consume',
        connection: 'default',
        className: 'C',
        methodName: 'm',
        stream: 'ORD',
        durable: 'd',
      },
    ]);
    expect(invoke).toHaveBeenCalledTimes(1);
    expect(msg.ack).toHaveBeenCalledTimes(1);
    await adapter.stop();
    await adapter.idle();
  });
  it('passes the delivery count as the message attempt', async () => {
    const attempts: unknown[] = [];
    const plugin = {
      inbound: {
        async handle(d: { messages: Array<{ attempt: number }> }, next: () => Promise<void>) {
          attempts.push(d.messages.map((m) => m.attempt));
          await next();
        },
      },
    };
    const { adapter, fake } = setup(() => Promise.resolve(), { plugins: [plugin] });
    await adapter.start();
    fake.push(fakeMsg(1, 4) as unknown as JsMsg);
    await tick();
    await tick();
    expect(attempts).toEqual([[4]]);
    await adapter.stop();
    await adapter.idle();
  });

  it('forceStop naks in-flight unsettled messages only', async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const { adapter, fake } = setup(
      async (payload) => {
        if (payload === 2) await gate;
      },
      { concurrency: 2 },
    );
    await adapter.start();
    const [a, b] = [fakeMsg(1), fakeMsg(2)];
    fake.push(a as unknown as JsMsg);
    fake.push(b as unknown as JsMsg);
    await tick();
    await tick();
    expect(a.ack).toHaveBeenCalledTimes(1);

    await adapter.stop();
    expect(adapter.forceStop()).toEqual({ nakked: 1, cancelledRetries: 0 });
    expect(b.nak).toHaveBeenCalledTimes(1);
    expect(a.nak).not.toHaveBeenCalled();

    release();
    await adapter.idle();
    // Already settled by forceStop: the handler finishing does not ack it.
    expect(b.ack).not.toHaveBeenCalled();
    expect(adapter.forceStop()).toEqual({ nakked: 0, cancelledRetries: 0 });
  });

  it('forceStop survives a nak that throws and does not count it', async () => {
    const { adapter, fake, logger } = setup(() => new Promise<void>(() => {}));
    await adapter.start();
    const m = fakeMsg(1);
    m.nak.mockImplementation(() => {
      throw new Error('closed');
    });
    fake.push(m as unknown as JsMsg);
    await tick();
    await adapter.stop();
    expect(adapter.forceStop()).toEqual({ nakked: 0, cancelledRetries: 0 });
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringMatching(/Failed to nak/),
      expect.stringContaining('closed'),
    );
  });

  it('reads the consumer info (cached) on start', async () => {
    const { adapter, info } = setup(() => Promise.resolve());
    await adapter.start();
    expect(info).toHaveBeenCalledWith(true);
    await adapter.stop();
    await adapter.idle();
  });

  it('naks a failing message with the retry backoff', async () => {
    const { adapter, fake } = setup(() => Promise.reject(new Error('boom')), {
      retry: { initialDelay: 50, jitter: 0 },
    });
    await adapter.start();
    const m = fakeMsg(1, 2);
    fake.push(m as unknown as JsMsg);
    await tick();
    await tick();
    expect(m.nak).toHaveBeenCalledWith(100);
    await adapter.stop();
    await adapter.idle();
  });

  it('heartbeats every ack_wait / 2 while the handler runs', async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const { adapter, fake } = setup(() => gate, {
      // 100 ms ack_wait (in nanoseconds) → working() every 50 ms.
      consumerConfig: { ack_wait: 100_000_000, max_deliver: -1 },
    });
    await adapter.start();
    const m = fakeMsg(1);
    fake.push(m as unknown as JsMsg);
    await new Promise((resolve) => setTimeout(resolve, 230));
    release();
    await tick();
    await tick();
    expect(m.working.mock.calls.length).toBeGreaterThanOrEqual(3);
    expect(m.working.mock.calls.length).toBeLessThanOrEqual(5);
    expect(m.ack).toHaveBeenCalledTimes(1);
    await adapter.stop();
    await adapter.idle();
  });

  it.each([
    [2, { maxRetries: 5 }, true],
    [5, { maxRetries: 5 }, true],
    [6, { maxRetries: 5 }, false],
    [-1, { maxRetries: 5 }, false],
    [2, false, false],
  ])('max_deliver %s with retry %j → warns: %s', async (maxDeliver, retry, warns) => {
    const { adapter, logger } = setup(() => Promise.resolve(), {
      consumerConfig: { ack_wait: 30_000_000_000, max_deliver: maxDeliver },
      retry,
    });
    await adapter.start();
    const expected = expect.stringMatching(
      new RegExp(
        `Consumer ORD/d for C\\.m has max_deliver ${maxDeliver} ≤ maxRetries 5; the server will stop redelivering before the DLQ is reached`,
      ),
    );
    if (warns) expect(logger.warn).toHaveBeenCalledWith(expected);
    else expect(logger.warn).not.toHaveBeenCalled();
    await adapter.stop();
    await adapter.idle();
  });
});
