import type { ConsumerMessages, JsMsg } from '@nats-io/jetstream';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { HandlerState, RestartBinding } from '../../src/adapters/adapter.js';
import { JetStreamConsumerAdapter } from '../../src/adapters/jetstream-consumer.adapter.js';
import type { ConnectionEntry } from '../../src/connection/connection-registry.js';
import type { NatsJetStreamContext } from '../../src/context/nats-context.js';
import type { HandlerDefinition } from '../../src/discovery/handler-explorer.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import type { NatsStreamManager } from '../../src/jetstream/stream-manager.js';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * Push-driven async iterable whose `close()` ends iteration after buffered items, and
 * resolves only once the iterator has been read to the end (like nats.js).
 */
function fakeMessages() {
  const queue: JsMsg[] = [];
  let wake: (() => void) | undefined;
  let closed = false;
  let failure: Error | undefined;
  const statusQueue: unknown[] = [];
  let statusWake: (() => void) | undefined;
  let markEnded!: () => void;
  /** Resolves once the iterator has been read to its end. */
  const ended = new Promise<void>((resolve) => (markEnded = resolve));
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
      try {
        for (;;) {
          const next = queue.shift();
          if (next) yield next;
          else if (failure) throw failure;
          else if (closed) return;
          else await new Promise<void>((resolve) => (wake = resolve));
        }
      } finally {
        markEnded();
      }
    },
    close: () => {
      closed = true;
      wake?.();
      statusWake?.();
      return ended;
    },
  };
  return {
    messages: messages as unknown as ConsumerMessages,
    ended,
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
    /** Makes the iterator throw once its buffered messages are yielded. */
    fail(err: Error) {
      failure = err;
      wake?.();
    },
  };
}

function fakeMsg(id: number, deliveryCount = 1) {
  const msg = {
    subject: 'ord.x',
    data: new TextEncoder().encode(JSON.stringify(id)),
    headers: undefined,
    info: { stream: 'ORD', consumer: 'd', streamSequence: id, deliveryCount },
    ack: vi.fn(),
    nak: vi.fn(),
    term: vi.fn(),
    working: vi.fn(),
  };
  return msg;
}

function setup(
  invoke: (payload: unknown, ctx: unknown) => Promise<unknown>,
  opts: {
    concurrency?: number;
    plugins?: unknown[];
    consume?: () => Promise<ConsumerMessages>;
    /** The consumer's config as reported by `consumer.info()`. */
    consumerConfig?: {
      ack_wait?: number;
      max_deliver?: number;
      deliver_subject?: string;
      deliver_group?: string;
      max_ack_pending?: number;
    };
    delivery?: 'pull' | 'push';
    /** `js().consumers.getPushConsumer()`, by default resolving the fake consumer. */
    getPushConsumer?: () => Promise<unknown>;
    /** `jsm().consumers.info()`, by default reporting the same consumer as `consumer.info()`. */
    jsmInfo?: () => Promise<unknown>;
    deliverGroup?: string;
    config?: Record<string, unknown>;
    retry?: unknown;
    batch?: unknown;
    ack?: 'auto' | 'manual';
    dlq?: unknown;
    /** `js().publish` (DLQ publishes). */
    publish?: () => Promise<unknown>;
    restart?: RestartBinding;
  } = {},
) {
  const fake = fakeMessages();
  const consume = vi.fn(opts.consume ?? (() => Promise.resolve(fake.messages)));
  const push = opts.delivery === 'push';
  const defaultConfig = push
    ? {
        ack_wait: 30_000_000_000,
        max_deliver: -1,
        deliver_subject: '_nats.push.ORD.d',
        max_ack_pending: 1,
      }
    : { ack_wait: 30_000_000_000, max_deliver: -1 };
  const info = vi.fn(() => Promise.resolve({ config: opts.consumerConfig ?? defaultConfig }));
  const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
  const entry = {
    name: 'default',
    options: { plugins: opts.plugins },
    logger,
    nc: { isClosed: () => false },
    js: () => ({
      consumers: {
        get: () => Promise.resolve({ consume, info }),
        getPushConsumer: opts.getPushConsumer ?? (() => Promise.resolve({ consume, info })),
      },
      publish: opts.publish ?? (() => Promise.resolve({ stream: 'DLQ', seq: 1 })),
    }),
  } as unknown as ConnectionEntry;
  const def = {
    kind: 'consume',
    delivery: opts.delivery ?? 'pull',
    connection: 'default',
    className: 'C',
    methodName: 'm',
    options: {
      stream: 'ORD',
      durable: 'd',
      concurrency: opts.concurrency,
      retry: opts.retry,
      batch: opts.batch,
      ack: opts.ack,
      dlq: opts.dlq,
      deliverGroup: opts.deliverGroup,
      config: opts.config,
    },
    invoke,
  } as unknown as HandlerDefinition & { kind: 'consume' };
  const ensureConsumer = vi.fn(() => Promise.resolve(undefined));
  // `jsm().consumers.info()` reports the same consumer as `consumer.info()`.
  const streams = {
    ensureConsumer,
    jsm: () => Promise.resolve({ consumers: { info: opts.jsmInfo ?? info } }),
  } as unknown as NatsStreamManager;
  const adapter = new JetStreamConsumerAdapter(def, entry, streams, opts.restart);
  return { adapter, fake, consume, info, logger, ensureConsumer };
}

describe('JetStreamConsumerAdapter', () => {
  it.each([
    [undefined, 1],
    [4, 4],
  ])('requests concurrency (%s) messages: %s', async (concurrency, expected) => {
    const { adapter, consume } = setup(() => Promise.resolve(), { concurrency });
    await adapter.start();
    expect(consume).toHaveBeenCalledWith({ max_messages: expected });
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

  it('naks with the retry backoff when the pipeline fails outside settlement', async () => {
    const plugin = {
      inbound: {
        stage: 'outer',
        handle: () => Promise.reject(new Error('plugin boom')),
      },
    };
    const { adapter, fake, logger } = setup(() => Promise.resolve(), {
      plugins: [plugin],
      retry: { initialDelay: 50, jitter: 0 },
    });
    await adapter.start();
    const m = fakeMsg(1, 2);
    fake.push(m as unknown as JsMsg);
    await tick();
    await tick();
    expect(m.nak).toHaveBeenCalledExactlyOnceWith(100);
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
    // Read to its end, so nats.js releases it.
    await fake.ended;
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
        delivery: 'pull',
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

  describe('heartbeat', () => {
    /** 100 ms ack_wait (in nanoseconds) → working() every 50 ms. */
    const consumerConfig = { ack_wait: 100_000_000, max_deliver: -1 };
    const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

    it('heartbeats every ack_wait / 2 while the handler runs', async () => {
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const { adapter, fake } = setup(() => gate, { consumerConfig });
      await adapter.start();
      const m = fakeMsg(1);
      fake.push(m as unknown as JsMsg);
      await sleep(230);
      release();
      await tick();
      await tick();
      expect(m.working.mock.calls.length).toBeGreaterThanOrEqual(3);
      expect(m.working.mock.calls.length).toBeLessThanOrEqual(5);
      expect(m.ack).toHaveBeenCalledTimes(1);
      const calls = m.working.mock.calls.length;
      await sleep(150);
      expect(m.working).toHaveBeenCalledTimes(calls);
      await adapter.stop();
      await adapter.idle();
    });

    it('heartbeats a received message waiting for a slot', async () => {
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const handled: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          handled.push(payload);
          await gate;
        },
        { consumerConfig },
      );
      await adapter.start();
      const [a, b] = [fakeMsg(1), fakeMsg(2)];
      fake.push(a as unknown as JsMsg);
      fake.push(b as unknown as JsMsg);
      await sleep(230);
      expect(handled).toEqual([1]);
      expect(b.working.mock.calls.length).toBeGreaterThanOrEqual(3);
      release();
      await adapter.stop();
      await adapter.idle();
    });

    it('heartbeats messages being collected into a batch', async () => {
      const invoke = vi.fn(() => Promise.resolve());
      const { adapter, fake } = setup(invoke, {
        consumerConfig,
        batch: { maxSize: 10, maxWait: 60_000 },
      });
      await adapter.start();
      const msgs = [fakeMsg(1), fakeMsg(2)];
      for (const m of msgs) fake.push(m as unknown as JsMsg);
      await sleep(130);
      expect(invoke).not.toHaveBeenCalled();
      for (const m of msgs) expect(m.working.mock.calls.length).toBeGreaterThanOrEqual(2);
      await adapter.stop();
      await adapter.idle();
    });

    it('heartbeats a running batch and a batch waiting for a slot', async () => {
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const batches: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          batches.push(payload);
          await gate;
        },
        { consumerConfig, batch: { maxSize: 2, maxWait: 60_000 } },
      );
      await adapter.start();
      // [1, 2] runs, [3, 4] waits for the only slot.
      const msgs = [1, 2, 3, 4].map((id) => fakeMsg(id));
      for (const m of msgs) fake.push(m as unknown as JsMsg);
      await sleep(130);
      expect(batches).toEqual([[1, 2]]);
      for (const m of msgs) expect(m.working.mock.calls.length).toBeGreaterThanOrEqual(2);
      release();
      await adapter.stop();
      await adapter.idle();
    });

    it('stops heartbeating a message the handler settled itself', async () => {
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const { adapter, fake } = setup(
        async (_payload, ctx) => {
          await sleep(80);
          (ctx as NatsJetStreamContext).ack();
          await gate;
        },
        { consumerConfig, ack: 'manual' },
      );
      await adapter.start();
      const m = fakeMsg(1);
      fake.push(m as unknown as JsMsg);
      await sleep(100);
      expect(m.ack).toHaveBeenCalledTimes(1);
      const calls = m.working.mock.calls.length;
      expect(calls).toBeGreaterThanOrEqual(1);
      await sleep(150);
      expect(m.working).toHaveBeenCalledTimes(calls);
      release();
      await adapter.stop();
      await adapter.idle();
    });

    it('keeps heartbeating while a dead letter is being published', async () => {
      let ackPub!: () => void;
      const { adapter, fake } = setup(() => Promise.reject(new Error('poison')), {
        consumerConfig,
        retry: false,
        dlq: true,
        publish: () =>
          new Promise((resolve) => {
            ackPub = () => resolve({ stream: 'DLQ', seq: 1 });
          }),
      });
      await adapter.start();
      const m = fakeMsg(1);
      fake.push(m as unknown as JsMsg);
      await sleep(130);
      expect(m.working.mock.calls.length).toBeGreaterThanOrEqual(2);
      expect(m.term).not.toHaveBeenCalled();
      ackPub();
      await tick();
      await tick();
      expect(m.term).toHaveBeenCalledTimes(1);
      const calls = m.working.mock.calls.length;
      await sleep(130);
      expect(m.working).toHaveBeenCalledTimes(calls);
      await adapter.stop();
      await adapter.idle();
    });

    it('starts an unref-ed interval and clears it at forceStop', async () => {
      const setSpy = vi.spyOn(globalThis, 'setInterval');
      const clearSpy = vi.spyOn(globalThis, 'clearInterval');
      try {
        const { adapter } = setup(() => Promise.resolve(), { consumerConfig });
        await adapter.start();
        const timer = setSpy.mock.results.find((r) => r.type === 'return')?.value as
          | NodeJS.Timeout
          | undefined;
        expect(setSpy).toHaveBeenCalledWith(expect.any(Function), 50);
        expect(timer?.hasRef()).toBe(false);
        await adapter.stop();
        adapter.forceStop();
        expect(clearSpy).toHaveBeenCalledWith(timer);
      } finally {
        setSpy.mockRestore();
        clearSpy.mockRestore();
      }
    });

    it('keeps heartbeating in-flight messages after stop() until they finish', async () => {
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const clearSpy = vi.spyOn(globalThis, 'clearInterval');
      try {
        const { adapter, fake } = setup(() => gate, { consumerConfig });
        await adapter.start();
        const m = fakeMsg(1);
        fake.push(m as unknown as JsMsg);
        await tick();
        await adapter.stop();
        const before = m.working.mock.calls.length;
        await sleep(130);
        expect(m.working.mock.calls.length).toBeGreaterThanOrEqual(before + 2);
        release();
        await adapter.idle();
        expect(clearSpy).toHaveBeenCalled();
        const after = m.working.mock.calls.length;
        await sleep(130);
        expect(m.working).toHaveBeenCalledTimes(after);
      } finally {
        clearSpy.mockRestore();
      }
    });

    it('does not heartbeat without an ack_wait', async () => {
      const setSpy = vi.spyOn(globalThis, 'setInterval');
      try {
        const { adapter } = setup(() => Promise.resolve(), {
          consumerConfig: { ack_wait: 0, max_deliver: -1 },
        });
        await adapter.start();
        expect(setSpy).not.toHaveBeenCalled();
        await adapter.stop();
        await adapter.idle();
      } finally {
        setSpy.mockRestore();
      }
    });

    it('logs a failing working() and keeps heartbeating the others', async () => {
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const { adapter, fake, logger } = setup(() => gate, {
        consumerConfig,
        concurrency: 2,
      });
      await adapter.start();
      const [a, b] = [fakeMsg(1), fakeMsg(2)];
      a.working.mockImplementation(() => {
        throw new Error('wip failed');
      });
      fake.push(a as unknown as JsMsg);
      fake.push(b as unknown as JsMsg);
      await sleep(80);
      expect(b.working).toHaveBeenCalled();
      expect(logger.error).toHaveBeenCalledWith(
        expect.stringMatching(/Failed to extend ack deadline .*C\.m/),
        expect.stringContaining('wip failed'),
      );
      release();
      await adapter.stop();
      await adapter.idle();
    });
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

  it('naks with the default backoff when retries are disabled and the pipeline fails', async () => {
    const plugin = { inbound: { stage: 'outer', handle: () => Promise.reject(new Error('p')) } };
    const { adapter, fake } = setup(() => Promise.resolve(), { plugins: [plugin], retry: false });
    await adapter.start();
    const m = fakeMsg(1, 1);
    fake.push(m as unknown as JsMsg);
    await tick();
    await tick();
    const delay = m.nak.mock.calls[0]?.[0] as number;
    // DEFAULT_RETRY: 1000 ms ± 20 % jitter.
    expect(delay).toBeGreaterThanOrEqual(800);
    expect(delay).toBeLessThanOrEqual(1200);
    await adapter.stop();
    await adapter.idle();
  });

  describe('batch', () => {
    it.each([
      [10, 2, 10],
      [2, 4, 4],
    ])(
      'requests max(maxSize %s, concurrency %s) = %s messages',
      async (maxSize, concurrency, expected) => {
        const { adapter, consume } = setup(() => Promise.resolve(), {
          batch: { maxSize },
          concurrency,
        });
        await adapter.start();
        expect(consume).toHaveBeenCalledWith({ max_messages: expected });
        await adapter.stop();
        await adapter.idle();
      },
    );

    it('delivers batches of maxSize and acks each message', async () => {
      const batches: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          batches.push(payload);
        },
        { batch: { maxSize: 2, maxWait: 60_000 } },
      );
      await adapter.start();
      const msgs = [fakeMsg(1), fakeMsg(2), fakeMsg(3), fakeMsg(4)];
      for (const m of msgs) fake.push(m as unknown as JsMsg);
      await tick();
      await tick();
      await tick();
      expect(batches).toEqual([
        [1, 2],
        [3, 4],
      ]);
      for (const m of msgs) expect(m.ack).toHaveBeenCalledTimes(1);
      await adapter.stop();
      await adapter.idle();
    });

    it('flushes the pending batch to the handler on stop', async () => {
      const batches: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          batches.push(payload);
        },
        { batch: { maxSize: 10, maxWait: 60_000 } },
      );
      await adapter.start();
      const msgs = [fakeMsg(1), fakeMsg(2)];
      for (const m of msgs) fake.push(m as unknown as JsMsg);
      await tick();
      expect(batches).toEqual([]);
      await adapter.stop();
      await adapter.idle();
      expect(batches).toEqual([[1, 2]]);
      for (const m of msgs) expect(m.ack).toHaveBeenCalledTimes(1);
    });

    it('forceStop naks every unsettled message of an in-flight batch', async () => {
      const { adapter, fake } = setup(() => new Promise<void>(() => {}), {
        batch: { maxSize: 2, maxWait: 60_000 },
      });
      await adapter.start();
      const msgs = [fakeMsg(1), fakeMsg(2)];
      for (const m of msgs) fake.push(m as unknown as JsMsg);
      await tick();
      await tick();
      await adapter.stop();
      expect(adapter.forceStop()).toEqual({ nakked: 2, cancelledRetries: 0 });
      for (const m of msgs) expect(m.nak).toHaveBeenCalledTimes(1);
    });

    it('naks each message with its own backoff when the pipeline fails outside settlement', async () => {
      const plugin = { inbound: { stage: 'outer', handle: () => Promise.reject(new Error('p')) } };
      const { adapter, fake } = setup(() => Promise.resolve(), {
        plugins: [plugin],
        retry: { initialDelay: 50, jitter: 0 },
        batch: { maxSize: 2 },
      });
      await adapter.start();
      const [a, b] = [fakeMsg(1, 1), fakeMsg(2, 3)];
      fake.push(a as unknown as JsMsg);
      fake.push(b as unknown as JsMsg);
      await tick();
      await tick();
      expect(a.nak).toHaveBeenCalledExactlyOnceWith(50);
      expect(b.nak).toHaveBeenCalledExactlyOnceWith(200);
      await adapter.stop();
      await adapter.idle();
    });

    it('forceStop naks a flushed batch waiting for a slot, which then never runs', async () => {
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const batches: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          batches.push(payload);
          await gate;
        },
        { batch: { maxSize: 2, maxWait: 60_000 } },
      );
      await adapter.start();
      const [a, b, c] = [fakeMsg(1), fakeMsg(2), fakeMsg(3)];
      for (const m of [a, b, c]) fake.push(m as unknown as JsMsg);
      await tick();
      await tick();
      expect(batches).toEqual([[1, 2]]);
      await adapter.stop(); // flushes [3], which waits for the only slot
      await tick();
      expect(adapter.forceStop()).toEqual({ nakked: 3, cancelledRetries: 0 });
      for (const m of [a, b, c]) expect(m.nak).toHaveBeenCalledTimes(1);
      release();
      await adapter.idle();
      expect(batches).toEqual([[1, 2]]);
      for (const m of [a, b, c]) expect(m.ack).not.toHaveBeenCalled();
    });

    it('forceStop naks messages still in the collector', async () => {
      const invoke = vi.fn(() => Promise.resolve());
      const { adapter, fake } = setup(invoke, { batch: { maxSize: 10, maxWait: 60_000 } });
      await adapter.start();
      const [a, b] = [fakeMsg(1), fakeMsg(2)];
      fake.push(a as unknown as JsMsg);
      fake.push(b as unknown as JsMsg);
      await tick();
      expect(adapter.forceStop()).toEqual({ nakked: 2, cancelledRetries: 0 });
      expect(a.nak).toHaveBeenCalledTimes(1);
      expect(b.nak).toHaveBeenCalledTimes(1);
      await adapter.stop();
      await adapter.idle();
      expect(invoke).not.toHaveBeenCalled();
    });

    it.each([
      [30_000, true],
      [10_000, false],
    ])('maxWait %s with a 30 s ack_wait → warns: %s', async (maxWait, warns) => {
      const { adapter, logger } = setup(() => Promise.resolve(), {
        batch: { maxWait },
        consumerConfig: { ack_wait: 30_000_000_000, max_deliver: -1 },
      });
      await adapter.start();
      const expected = expect.stringMatching(
        new RegExp(
          `C\\.m on "ORD/d" has batch\\.maxWait ${maxWait} ms >= the consumer's ack_wait 30000 ms`,
        ),
      );
      if (warns) expect(logger.warn).toHaveBeenCalledWith(expected);
      else expect(logger.warn).not.toHaveBeenCalled();
      await adapter.stop();
      await adapter.idle();
    });
  });

  describe('push', () => {
    afterEach(() => {
      vi.useRealTimers();
    });

    it('consumes the push consumer without options and labels its logs (push)', async () => {
      const { adapter, fake, consume, logger } = setup(() => Promise.resolve(), {
        delivery: 'push',
      });
      await adapter.start();
      expect(consume).toHaveBeenCalledWith();
      fake.end();
      await adapter.idle();
      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining('C.m on "ORD/d" (push) ended unexpectedly'),
      );
    });

    it('creates a declared consumer with the push defaults', async () => {
      const { adapter, ensureConsumer } = setup(() => Promise.resolve(), {
        delivery: 'push',
        concurrency: 2,
        batch: { maxSize: 5 },
        config: { ack_wait: 10_000_000_000 },
        consumerConfig: {
          ack_wait: 10_000_000_000,
          deliver_subject: '_nats.push.ORD.d',
          max_ack_pending: 5,
        },
      });
      await adapter.start();
      expect(ensureConsumer).toHaveBeenCalledWith(
        'ORD',
        { ack_wait: 10_000_000_000, durable_name: 'd' },
        undefined,
        {
          deliver_subject: '_nats.push.ORD.d',
          flow_control: true,
          idle_heartbeat: 5_000_000_000,
          max_ack_pending: 5,
        },
      );
      await adapter.stop();
      await adapter.idle();
    });

    it('push intake reads every message without waiting for a slot', async () => {
      vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const handled: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          handled.push(payload);
          await gate;
        },
        {
          delivery: 'push',
          concurrency: 1,
          consumerConfig: {
            ack_wait: 100_000_000,
            max_deliver: -1,
            deliver_subject: '_nats.push.ORD.d',
            max_ack_pending: 5,
          },
        },
      );
      await adapter.start();
      const msgs = [1, 2, 3, 4, 5].map((id) => fakeMsg(id));
      for (const m of msgs) fake.push(m as unknown as JsMsg);
      await tick();
      expect(handled).toEqual([1]);
      // Every received message is heartbeated, including the four waiting for the slot.
      vi.advanceTimersByTime(50);
      for (const m of msgs) expect(m.working).toHaveBeenCalledTimes(1);
      release();
      await adapter.stop();
      await adapter.idle();
    });

    it('push start rejects a pull consumer', async () => {
      const { adapter, consume } = setup(() => Promise.resolve(), {
        delivery: 'push',
        consumerConfig: { ack_wait: 30_000_000_000, max_deliver: -1, max_ack_pending: 1 },
      });
      const err = await adapter.start().catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NatsConfigError);
      expect((err as Error).message).toContain('is not a push consumer');
      expect((err as Error).message).toMatch(/"ORD\/d".*C\.m/);
      expect((err as Error).message).toContain(
        'use a pull consumer (@Consume, or consume on a router)',
      );
      expect(consume).not.toHaveBeenCalled();
    });

    it.each([
      ['a', 'b'],
      [undefined, 'b'],
    ])('push start rejects deliverGroup %s against deliver_group %s', async (group, actual) => {
      const { adapter } = setup(() => Promise.resolve(), {
        delivery: 'push',
        deliverGroup: group,
        consumerConfig: {
          ack_wait: 30_000_000_000,
          deliver_subject: '_nats.push.ORD.d',
          deliver_group: actual,
          max_ack_pending: 1,
        },
      });
      const err = await adapter.start().catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NatsConfigError);
      expect((err as Error).message).toContain(`deliver_group "${actual}"`);
      expect((err as Error).message).toMatch(/"ORD\/d".*C\.m/);
    });

    it('push start rejects a declared deliverGroup the consumer does not have', async () => {
      const { adapter } = setup(() => Promise.resolve(), {
        delivery: 'push',
        deliverGroup: 'a',
      });
      await expect(adapter.start()).rejects.toThrow(/deliverGroup "a"/);
    });

    it('accepts a matching deliver_group declared in config', async () => {
      const { adapter, ensureConsumer } = setup(() => Promise.resolve(), {
        delivery: 'push',
        config: { deliver_group: 'g' },
        consumerConfig: {
          ack_wait: 30_000_000_000,
          deliver_subject: '_nats.push.ORD.d',
          deliver_group: 'g',
          max_ack_pending: 1,
        },
      });
      await adapter.start();
      // The group decides the defaults even when declared in config only.
      expect(ensureConsumer).toHaveBeenCalledWith(
        'ORD',
        { deliver_group: 'g', durable_name: 'd' },
        undefined,
        { deliver_subject: '_nats.push.ORD.d', deliver_group: 'g' },
      );
      await adapter.stop();
      await adapter.idle();
    });

    it('reads the consumer info when nats.js refuses it for another reason', async () => {
      const refused = new Error('some other wording');
      const { adapter, consume } = setup(() => Promise.resolve(), {
        delivery: 'push',
        getPushConsumer: () => Promise.reject(refused),
        consumerConfig: { ack_wait: 30_000_000_000, max_ack_pending: 1 },
      });
      const err = await adapter.start().catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NatsConfigError);
      expect((err as Error).message).toMatch(/"ORD\/d" for C\.m is not a push consumer/);
      expect((err as Error).cause).toBe(refused);
      expect(consume).not.toHaveBeenCalled();
    });

    it('rethrows that error when the consumer info looks fine', async () => {
      const refused = new Error('boom');
      const { adapter } = setup(() => Promise.resolve(), {
        delivery: 'push',
        getPushConsumer: () => Promise.reject(refused),
      });
      await expect(adapter.start()).rejects.toBe(refused);
    });

    it('rethrows that error when the consumer info cannot be read either', async () => {
      const refused = new Error('boom');
      const { adapter } = setup(() => Promise.resolve(), {
        delivery: 'push',
        getPushConsumer: () => Promise.reject(refused),
        jsmInfo: () => Promise.reject(new Error('info timed out')),
      });
      await expect(adapter.start()).rejects.toBe(refused);
    });

    it('does not take another InvalidOperationError of consume() for an already bound consumer', async () => {
      const other = Object.assign(new Error('consumer already started'), {
        name: 'InvalidOperationError',
      });
      const { adapter } = setup(() => Promise.resolve(), {
        delivery: 'push',
        consume: () => Promise.reject(other),
      });
      await expect(adapter.start()).rejects.toBe(other);
    });

    it('explains a consumer already bound by another subscriber', async () => {
      const bound = Object.assign(new Error('consumer is already bound'), {
        name: 'InvalidOperationError',
      });
      const { adapter } = setup(() => Promise.resolve(), {
        delivery: 'push',
        consume: () => Promise.reject(bound),
      });
      const err = await adapter.start().catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NatsConfigError);
      expect((err as Error).message).toMatch(/"ORD\/d".*C\.m.*already bound.*deliverGroup/);
      expect((err as Error).cause).toBe(bound);
    });

    it('forceStop naks messages waiting for a slot, which then never run', async () => {
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const handled: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          handled.push(payload);
          await gate;
        },
        {
          delivery: 'push',
          consumerConfig: {
            ack_wait: 30_000_000_000,
            deliver_subject: '_nats.push.ORD.d',
            max_ack_pending: 1000,
          },
        },
      );
      await adapter.start();
      const msgs = [1, 2, 3].map((id) => fakeMsg(id));
      for (const m of msgs) fake.push(m as unknown as JsMsg);
      await tick();
      expect(handled).toEqual([1]);
      await adapter.stop();
      // The hung one, and the two waiting for its slot.
      expect(adapter.forceStop()).toEqual({ nakked: 3, cancelledRetries: 0 });
      for (const m of msgs) expect(m.nak).toHaveBeenCalledTimes(1);
      release();
      await adapter.idle();
      expect(handled).toEqual([1]);
      for (const m of msgs) {
        expect(m.nak).toHaveBeenCalledTimes(1);
        expect(m.ack).not.toHaveBeenCalled();
      }
    });

    it('forceStop with batch naks the messages queued behind a batch waiting for a slot', async () => {
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const batches: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          batches.push(payload);
          await gate;
        },
        {
          delivery: 'push',
          batch: { maxSize: 2, maxWait: 60_000 },
          consumerConfig: {
            ack_wait: 30_000_000_000,
            deliver_subject: '_nats.push.ORD.d',
            max_ack_pending: 1000,
          },
        },
      );
      await adapter.start();
      const msgs = [1, 2, 3, 4, 5, 6, 7, 8].map((id) => fakeMsg(id));
      for (const m of msgs) fake.push(m as unknown as JsMsg);
      await tick();
      await tick();
      // [1, 2] hangs, [3, 4] waits for its slot, 5..8 wait behind that batch.
      expect(batches).toEqual([[1, 2]]);
      await adapter.stop();
      expect(adapter.forceStop()).toEqual({ nakked: 8, cancelledRetries: 0 });
      for (const m of msgs) expect(m.nak).toHaveBeenCalledTimes(1);
      release();
      await adapter.idle();
      await tick();
      expect(batches).toEqual([[1, 2]]);
      for (const m of msgs) {
        expect(m.nak).toHaveBeenCalledTimes(1);
        expect(m.ack).not.toHaveBeenCalled();
      }
    });

    it('push start rejects unlimited max_ack_pending', async () => {
      const { adapter } = setup(() => Promise.resolve(), {
        delivery: 'push',
        consumerConfig: {
          ack_wait: 30_000_000_000,
          deliver_subject: '_nats.push.ORD.d',
          max_ack_pending: -1,
        },
      });
      const err = await adapter.start().catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NatsConfigError);
      expect((err as Error).message).toContain('max_ack_pending');
    });

    it('processes every message when concurrency exceeds max_ack_pending', async () => {
      const { adapter, fake } = setup(() => Promise.resolve(), {
        delivery: 'push',
        concurrency: 4,
        consumerConfig: {
          ack_wait: 30_000_000_000,
          deliver_subject: '_nats.push.ORD.d',
          max_ack_pending: 1,
        },
      });
      await adapter.start();
      // The server delivers the next message only once the previous one is acked.
      const msgs = [1, 2, 3].map((id) => fakeMsg(id));
      for (const m of msgs) {
        fake.push(m as unknown as JsMsg);
        await vi.waitFor(() => expect(m.ack).toHaveBeenCalledTimes(1));
      }
      await adapter.stop();
      await expect(adapter.idle()).resolves.toBeUndefined();
      for (const m of msgs) expect(m.nak).not.toHaveBeenCalled();
    });

    it('naks queued messages on stop and lets the in-flight one finish', async () => {
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const handled: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          handled.push(payload);
          await gate;
        },
        { delivery: 'push' },
      );
      await adapter.start();
      const [a, b, c] = [fakeMsg(1), fakeMsg(2), fakeMsg(3)];
      for (const m of [a, b, c]) fake.push(m as unknown as JsMsg);
      await tick();
      await adapter.stop();
      release();
      await adapter.idle();
      expect(handled).toEqual([1]);
      expect(a.ack).toHaveBeenCalledTimes(1);
      for (const m of [b, c]) {
        expect(m.nak).toHaveBeenCalledTimes(1);
        expect(m.ack).not.toHaveBeenCalled();
      }
    });

    it('keeps the order of queued messages', async () => {
      const handled: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          await tick();
          handled.push(payload);
        },
        { delivery: 'push' },
      );
      await adapter.start();
      for (let i = 1; i <= 10; i++) fake.push(fakeMsg(i) as unknown as JsMsg);
      await vi.waitFor(() => expect(handled).toHaveLength(10));
      expect(handled).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
      await adapter.stop();
      await adapter.idle();
    });
  });

  describe('restart', () => {
    const policy = { initialDelay: 10, factor: 1, maxDelay: 10, jitter: 0, maxAttempts: Infinity };
    const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

    /** `consume()` resolves a new fake `messages` on each call, unless `override` handles it. */
    function setupRestart(
      invoke: (payload: unknown, ctx: unknown) => Promise<unknown>,
      opts: Parameters<typeof setup>[1] & {
        /** Handles the n-th `consume()` call (0-based) when it returns something. */
        override?: (call: number) => Promise<ConsumerMessages> | undefined;
      } = {},
    ) {
      const fakes: ReturnType<typeof fakeMessages>[] = [];
      const states: HandlerState[] = [];
      let calls = 0;
      const ctx = setup(invoke, {
        ...opts,
        consume: () => {
          const overridden = opts.override?.(calls++);
          if (overridden) return overridden;
          const fake = fakeMessages();
          fakes.push(fake);
          return Promise.resolve(fake.messages);
        },
        restart: { policy, track: () => (state) => states.push(state) },
      });
      return { ...ctx, fakes, states };
    }

    it.each([
      ['pull', policy, { max_messages: 1, abort_on_missing_resource: true }],
      ['pull', false, { max_messages: 1 }],
      ['push', policy, { abort_on_missing_resource: true }],
      ['push', false, undefined],
    ] as const)(
      '%s consumer with restart %o consumes with %o',
      async (delivery, restartPolicy, expected) => {
        const { adapter, consume } = setup(() => Promise.resolve(), {
          delivery,
          restart: { policy: restartPolicy, track: () => () => {} },
        });
        await adapter.start();
        if (expected === undefined) expect(consume).toHaveBeenCalledExactlyOnceWith();
        else expect(consume).toHaveBeenCalledExactlyOnceWith(expected);
        await adapter.stop();
        await adapter.idle();
      },
    );

    it('reopens a pull consumer whose messages iterator fails, and keeps heartbeating held messages', async () => {
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const handled: unknown[] = [];
      const { adapter, fakes, states, consume, logger } = setupRestart(
        async (payload) => {
          handled.push(payload);
          if (payload === 1) await gate;
        },
        { consumerConfig: { ack_wait: 100_000_000, max_deliver: -1 } },
      );
      await adapter.start();
      const held = fakeMsg(1);
      fakes[0]?.push(held as unknown as JsMsg);
      await vi.waitFor(() => expect(handled).toEqual([1]));

      fakes[0]?.fail(new Error('iterator boom'));
      await vi.waitFor(() => expect(consume).toHaveBeenCalledTimes(2));
      await vi.waitFor(() => expect(states).toEqual(['running', 'restarting', 'running']));
      expect(logger.error).toHaveBeenCalledWith(
        'Consumer of C.m on "ORD/d" failed',
        expect.stringContaining('iterator boom'),
      );
      expect(logger.log).toHaveBeenCalledWith('Restarted C.m on "ORD/d" (attempt 1)');

      // The held message is still heartbeated after the reopen.
      const beats = held.working.mock.calls.length;
      await sleep(120);
      expect(held.working.mock.calls.length).toBeGreaterThan(beats);

      const next = fakeMsg(2);
      fakes[1]?.push(next as unknown as JsMsg);
      release();
      await vi.waitFor(() => expect(handled).toEqual([1, 2]));
      await vi.waitFor(() => expect(next.ack).toHaveBeenCalledTimes(1));
      expect(held.ack).toHaveBeenCalledTimes(1);
      await adapter.stop();
      await adapter.idle();
    });

    it('warns about max_deliver only on the first open', async () => {
      const { adapter, fakes, states, consume, logger } = setupRestart(() => Promise.resolve(), {
        consumerConfig: { ack_wait: 30_000_000_000, max_deliver: 3 },
        retry: { maxRetries: 5 },
      });
      await adapter.start();
      fakes[0]?.end();
      await vi.waitFor(() => expect(consume).toHaveBeenCalledTimes(2));
      await vi.waitFor(() => expect(states).toEqual(['running', 'restarting', 'running']));
      const warnings = logger.warn.mock.calls.filter(([m]) => String(m).includes('max_deliver'));
      expect(warnings).toHaveLength(1);
      await adapter.stop();
      await adapter.idle();
    });

    it('counts an already-bound push consumer on reopen as a failed attempt', async () => {
      const bound = Object.assign(new Error('consumer is already bound'), {
        name: 'InvalidOperationError',
      });
      const { adapter, fakes, states, consume, logger } = setupRestart(() => Promise.resolve(), {
        delivery: 'push',
        override: (call) => (call === 1 ? Promise.reject(bound) : undefined),
      });
      await adapter.start();
      fakes[0]?.end();
      await vi.waitFor(() => expect(consume).toHaveBeenCalledTimes(3));
      await vi.waitFor(() => expect(states.at(-1)).toBe('running'));
      expect(states).toEqual(['running', 'restarting', 'restarting', 'running']);
      expect(logger.error).toHaveBeenCalledWith(
        'Consumer of C.m on "ORD/d" (push) ended unexpectedly',
      );
      const failure = logger.warn.mock.calls.find(([m]) =>
        String(m).startsWith('Restart of C.m on "ORD/d" (push) failed (attempt 1)'),
      );
      expect(failure?.[0]).toMatch(/already bound/);
      expect(logger.log).toHaveBeenCalledWith('Restarted C.m on "ORD/d" (push) (attempt 2)');
      await adapter.stop();
      await adapter.idle();
    });

    it('closes and drains messages opened by a reopen that finishes after stop()', async () => {
      const late = fakeMessages();
      const closeSpy = vi.spyOn(late.messages, 'close');
      const invoke = vi.fn(() => Promise.resolve());
      let resolveConsume!: () => void;
      const { adapter, fakes, states, consume } = setupRestart(invoke, {
        override: (call) =>
          call === 1
            ? new Promise<ConsumerMessages>((resolve) => {
                resolveConsume = () => resolve(late.messages);
              })
            : undefined,
      });
      await adapter.start();
      fakes[0]?.end();
      await vi.waitFor(() => expect(consume).toHaveBeenCalledTimes(2));
      await adapter.stop();
      // idle() waits on the reopen in flight.
      let idled = false;
      const idle = adapter.idle().then(() => {
        idled = true;
      });
      await tick();
      expect(idled).toBe(false);
      // Already buffered when the late messages arrive: handed back, not handled.
      const buffered = fakeMsg(1);
      late.push(buffered as unknown as JsMsg);
      resolveConsume();
      await idle;
      expect(closeSpy).toHaveBeenCalledTimes(1);
      await late.ended;
      expect(buffered.nak).toHaveBeenCalledExactlyOnceWith();
      expect(invoke).not.toHaveBeenCalled();
      expect(states).toEqual(['running', 'restarting']);
      expect(consume).toHaveBeenCalledTimes(2);
    });

    it('stays down and logs the tail when restart is disabled', async () => {
      const states: HandlerState[] = [];
      const { adapter, fake, logger } = setup(() => Promise.resolve(), {
        restart: { policy: false, track: () => (state) => states.push(state) },
      });
      await adapter.start();
      fake.fail(new Error('boom'));
      await vi.waitFor(() => expect(states).toEqual(['running', 'failed']));
      expect(logger.error).toHaveBeenCalledWith(
        'Consumer of C.m on "ORD/d" failed; no more messages will be received',
        expect.stringContaining('boom'),
      );
      await adapter.stop();
      await adapter.idle();
    });
  });
});
