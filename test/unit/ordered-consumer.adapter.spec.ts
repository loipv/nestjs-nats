import type { ConsumerMessages, JsMsg } from '@nats-io/jetstream';
import { JetStreamApiCodes, JetStreamApiError } from '@nats-io/jetstream';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { RestartBinding } from '../../src/adapters/adapter.js';
import { OrderedConsumerAdapter } from '../../src/adapters/ordered-consumer.adapter.js';
import type { ConnectionEntry } from '../../src/connection/connection-registry.js';
import { NatsOrderedContext } from '../../src/context/nats-ordered-context.js';
import type { HandlerDefinition } from '../../src/discovery/handler-explorer.js';
import { NatsConfigError } from '../../src/errors/errors.js';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Push-driven async iterable whose `close()` ends iteration after buffered items (like nats.js). */
function fakeMessages() {
  const queue: JsMsg[] = [];
  let wake: (() => void) | undefined;
  let closed = false;
  let failure: Error | undefined;
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
        else if (failure) throw failure;
        else if (closed) return;
        else await new Promise<void>((resolve) => (wake = resolve));
      }
    },
    close: vi.fn(() => {
      closed = true;
      wake?.();
      statusWake?.();
      return Promise.resolve();
    }),
  };
  return {
    messages: messages as unknown as ConsumerMessages,
    close: messages.close,
    push(msg: unknown) {
      queue.push(msg as JsMsg);
      wake?.();
    },
    fail(err: Error) {
      failure = err;
      wake?.();
    },
    notify(n: unknown) {
      statusQueue.push(n);
      statusWake?.();
    },
  };
}

function fakeMsg(seq: number) {
  return {
    subject: 'ord.x',
    data: new TextEncoder().encode(JSON.stringify(seq)),
    headers: undefined,
    info: {
      stream: 'ORD',
      consumer: 'oc_1',
      streamSequence: seq,
      deliverySequence: seq,
      pending: 0,
      deliveryCount: 1,
    },
  };
}

function setup(
  invoke: (payload: unknown, ctx: unknown) => Promise<unknown>,
  opts: {
    retry?: unknown;
    dlq?: unknown;
    filterSubjects?: string | string[];
    deliverPolicy?: unknown;
    headersOnly?: boolean;
    plugins?: unknown[];
    get?: () => Promise<unknown>;
    /** `js().publish` (DLQ publishes). */
    publish?: () => Promise<unknown>;
    restart?: RestartBinding;
  } = {},
) {
  const fake = fakeMessages();
  const fakes = [fake];
  let calls = 0;
  const consume = vi.fn(() => {
    if (calls++ === 0) return Promise.resolve(fake.messages);
    const next = fakeMessages();
    fakes.push(next);
    return Promise.resolve(next.messages);
  });
  const get = vi.fn(opts.get ?? (() => Promise.resolve({ consume })));
  const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn(), debug: vi.fn() };
  const entry = {
    name: 'default',
    options: { plugins: opts.plugins },
    logger,
    nc: { isClosed: () => false },
    js: () => ({
      consumers: { get },
      publish: opts.publish ?? (() => Promise.resolve({ stream: 'DLQ', seq: 1 })),
    }),
  } as unknown as ConnectionEntry;
  const def = {
    kind: 'ordered',
    connection: 'default',
    className: 'C',
    methodName: 'm',
    options: {
      stream: 'ORD',
      filterSubjects: opts.filterSubjects,
      deliverPolicy: opts.deliverPolicy,
      headersOnly: opts.headersOnly,
      retry: opts.retry,
      dlq: opts.dlq ?? false,
    },
    invoke,
  } as unknown as HandlerDefinition & { kind: 'ordered' };
  const adapter = new OrderedConsumerAdapter(def, entry, opts.restart);
  return { adapter, fake, fakes, get, consume, logger };
}

describe('OrderedConsumerAdapter', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('processes messages in stream order with inline retries', async () => {
    vi.useFakeTimers();
    const calls: number[] = [];
    const attempts: number[] = [];
    let failures = 0;
    const { adapter, fake } = setup(
      async (payload, ctx) => {
        const n = payload as number;
        calls.push(n);
        if (n === 2) {
          attempts.push((ctx as NatsOrderedContext).attempt);
          expect(ctx).toBeInstanceOf(NatsOrderedContext);
          if (failures++ < 2) throw new Error('boom');
        }
      },
      { retry: { maxRetries: 3, initialDelay: 10 } },
    );
    await adapter.start();
    fake.push(fakeMsg(1));
    fake.push(fakeMsg(2));
    fake.push(fakeMsg(3));
    await vi.advanceTimersByTimeAsync(1000);
    expect(calls).toEqual([1, 2, 2, 2, 3]);
    expect(attempts).toEqual([1, 2, 3]);
    await adapter.stop();
    await adapter.idle();
  });

  it('cancels the retry wait at stop() and abandons the message', async () => {
    const invoke = vi.fn(() => Promise.reject(new Error('boom')));
    const { adapter, fake, logger } = setup(invoke, {
      retry: { maxRetries: 3, initialDelay: 60_000 },
    });
    await adapter.start();
    fake.push(fakeMsg(7));
    await vi.waitFor(() => expect(invoke).toHaveBeenCalledTimes(1));
    await tick();
    const startedAt = Date.now();
    await adapter.stop();
    await adapter.idle();
    expect(Date.now() - startedAt).toBeLessThan(200);
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('abandoned at shutdown'));
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('stream seq 7'));
    expect(invoke).toHaveBeenCalledTimes(1);
    expect(adapter.forceStop()).toEqual({ nakked: 0, cancelledRetries: 0 });
  });

  it('does not count a retry wait that stop() already cancelled', async () => {
    const invoke = vi.fn(() => Promise.reject(new Error('boom')));
    const { adapter, fake } = setup(invoke, { retry: { maxRetries: 3, initialDelay: 60_000 } });
    await adapter.start();
    fake.push(fakeMsg(1));
    await vi.waitFor(() => expect(invoke).toHaveBeenCalledTimes(1));
    await tick();
    // forceStop() right after stop(), before the cancelled wait has returned.
    void adapter.stop();
    expect(adapter.forceStop()).toEqual({ nakked: 0, cancelledRetries: 0 });
    await adapter.idle();
  });

  it('ends a failing DLQ publish retry loop at stop()', async () => {
    const invoke = vi.fn(() => Promise.reject(new Error('boom')));
    const publish = vi.fn(() => Promise.reject(new Error('no DLQ stream')));
    const { adapter, fake, logger } = setup(invoke, {
      retry: false,
      dlq: true,
      publish,
    });
    await adapter.start();
    fake.push(fakeMsg(3));
    await vi.waitFor(() => expect(publish).toHaveBeenCalledTimes(1));
    await tick();
    const startedAt = Date.now();
    await adapter.stop();
    await adapter.idle();
    expect(Date.now() - startedAt).toBeLessThan(200);
    expect(publish).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('could not be sent to DLQ'),
      expect.anything(),
    );
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('retry abandoned at shutdown (stream sequence 3)'),
    );
  });

  it('counts the retry wait cancelled by forceStop() without stop()', async () => {
    const invoke = vi.fn(() => Promise.reject(new Error('boom')));
    const { adapter, fake } = setup(invoke, { retry: { maxRetries: 3, initialDelay: 60_000 } });
    await adapter.start();
    fake.push(fakeMsg(1));
    await vi.waitFor(() => expect(invoke).toHaveBeenCalledTimes(1));
    await tick();
    expect(adapter.forceStop()).toEqual({ nakked: 0, cancelledRetries: 1 });
    await adapter.stop();
    await adapter.idle();
  });

  it('does not process messages buffered once stopped', async () => {
    const invoke = vi.fn(() => Promise.resolve());
    const { adapter, fake } = setup(invoke);
    await adapter.start();
    await adapter.stop();
    fake.push(fakeMsg(1));
    await adapter.idle();
    expect(invoke).not.toHaveBeenCalled();
  });

  it('closes the messages at stop() and logs a failing close', async () => {
    const { adapter, fake, logger } = setup(() => Promise.resolve());
    await adapter.start();
    fake.close.mockImplementationOnce(() => Promise.reject(new Error('close failed')));
    await adapter.stop();
    await tick();
    expect(fake.close).toHaveBeenCalled();
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('Failed to close consumer of C.m'),
      expect.anything(),
    );
  });

  it.each([
    [undefined, {}],
    ['all', { deliver_policy: 'all' }],
    ['last', { deliver_policy: 'last' }],
    ['new', { deliver_policy: 'new' }],
    ['last_per_subject', { deliver_policy: 'last_per_subject' }],
    [{ startSeq: 5 }, { deliver_policy: 'by_start_sequence', opt_start_seq: 5 }],
    [
      { startTime: new Date(0) },
      { deliver_policy: 'by_start_time', opt_start_time: '1970-01-01T00:00:00.000Z' },
    ],
    [
      { startTime: '2026-01-01T00:00:00Z' },
      { deliver_policy: 'by_start_time', opt_start_time: '2026-01-01T00:00:00Z' },
    ],
  ])('start maps deliverPolicy %j', async (deliverPolicy, expected) => {
    const { adapter, get } = setup(() => Promise.resolve(), { deliverPolicy });
    await adapter.start();
    expect(get).toHaveBeenCalledWith('ORD', expected);
    await adapter.stop();
    await adapter.idle();
  });

  it('start maps filterSubjects and headersOnly', async () => {
    const { adapter, get } = setup(() => Promise.resolve(), {
      filterSubjects: 'a.>',
      headersOnly: true,
    });
    await adapter.start();
    expect(get).toHaveBeenCalledWith('ORD', { filter_subjects: ['a.>'], headers_only: true });
    await adapter.stop();

    const many = setup(() => Promise.resolve(), { filterSubjects: ['a.>', 'b.>'] });
    await many.adapter.start();
    expect(many.get).toHaveBeenCalledWith('ORD', { filter_subjects: ['a.>', 'b.>'] });
    await many.adapter.stop();
  });

  it('rejects start() with a NatsConfigError when the stream is missing', async () => {
    const notFound = new JetStreamApiError({
      code: 404,
      err_code: JetStreamApiCodes.StreamNotFound,
      description: 'stream not found',
    });
    const { adapter } = setup(() => Promise.resolve(), { get: () => Promise.reject(notFound) });
    const err = await adapter.start().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsConfigError);
    expect((err as Error).message).toContain('"ORD"');
    expect((err as Error).message).toContain('C.m');
    expect((err as Error).cause).toBe(notFound);
  });

  it('logs a recreated consumer at debug level', async () => {
    const { adapter, fake, logger } = setup(() => Promise.resolve());
    await adapter.start();
    fake.notify({ type: 'ordered_consumer_recreated', name: 'oc_2' });
    await tick();
    expect(logger.debug).toHaveBeenCalledWith(expect.stringContaining('oc_2'));
    expect(logger.warn).not.toHaveBeenCalled();
    await adapter.stop();
    await adapter.idle();
  });

  it('warns about missed heartbeats on exponentially spaced counts', async () => {
    const { adapter, fake, logger } = setup(() => Promise.resolve());
    await adapter.start();
    for (const count of [1, 2, 3, 4]) fake.notify({ type: 'heartbeats_missed', count });
    await tick();
    const warned = logger.warn.mock.calls.map(([line]) => String(line));
    expect(warned).toHaveLength(3);
    expect(warned[0]).toMatch(/C\.m.*heartbeats_missed \(attempt 1\)/);
    expect(warned[1]).toContain('(attempt 2)');
    expect(warned[2]).toContain('(attempt 4)');
    await adapter.stop();
    await adapter.idle();
  });

  it('warns about a missing consumer on exponentially spaced counts', async () => {
    const { adapter, fake, logger } = setup(() => Promise.resolve());
    await adapter.start();
    for (const count of [1, 2, 3, 4, 5]) fake.notify({ type: 'consumer_not_found', count });
    await tick();
    const warned = logger.warn.mock.calls.map(([line]) => String(line));
    expect(warned).toEqual([
      expect.stringMatching(/C\.m.*consumer_not_found \(attempt 1\)/),
      expect.stringContaining('consumer_not_found (attempt 2)'),
      expect.stringContaining('consumer_not_found (attempt 4)'),
    ]);
    await adapter.stop();
    await adapter.idle();
  });

  it('warns about a missing stream on exponentially spaced failures', async () => {
    const { adapter, fake, logger } = setup(() => Promise.resolve());
    await adapter.start();
    for (const consumerCreateFails of [1, 2, 3]) {
      fake.notify({ type: 'stream_not_found', name: 'ORD', consumerCreateFails });
    }
    await tick();
    expect(logger.warn).toHaveBeenCalledTimes(2);
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('stream_not_found'));
    await adapter.stop();
    await adapter.idle();
  });

  it('retries with the retry backoff when the pipeline fails outside settlement', async () => {
    vi.useFakeTimers();
    const invoke = vi.fn(() => Promise.resolve());
    let failures = 0;
    const plugin = {
      inbound: {
        stage: 'outer',
        handle: (_d: unknown, next: () => Promise<void>) =>
          failures++ === 0 ? Promise.reject(new Error('plugin boom')) : next(),
      },
    };
    const { adapter, fake, logger } = setup(invoke, {
      plugins: [plugin],
      retry: { initialDelay: 50, jitter: 0 },
    });
    await adapter.start();
    fake.push(fakeMsg(1));
    await vi.advanceTimersByTimeAsync(0);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('Handler C.m'),
      expect.anything(),
    );
    expect(invoke).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(49);
    expect(invoke).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(invoke).toHaveBeenCalledOnce();
    await adapter.stop();
    await adapter.idle();
  });

  it('logs an error when the consume loop ends while not stopping', async () => {
    const { adapter, fake, logger } = setup(() => Promise.resolve());
    await adapter.start();
    void fake.messages.close();
    await adapter.idle();
    expect(logger.error).toHaveBeenCalledWith(expect.stringMatching(/C\.m.*ended unexpectedly/));
  });

  describe('restart', () => {
    const policy = {
      initialDelay: 10,
      factor: 1,
      maxDelay: 10,
      jitter: 0,
      maxAttempts: Infinity,
    };
    const restart: RestartBinding = { policy, track: () => () => {} };

    it('reopens after the last handled message', async () => {
      const handled: unknown[] = [];
      const { adapter, fake, fakes, get } = setup(
        async (payload) => {
          handled.push(payload);
        },
        { deliverPolicy: 'all', filterSubjects: 'a.>', restart },
      );
      await adapter.start();
      fake.push(fakeMsg(1));
      fake.push(fakeMsg(2));
      await vi.waitFor(() => expect(handled).toEqual([1, 2]));
      fake.fail(new Error('boom'));
      await vi.waitFor(() => expect(get).toHaveBeenCalledTimes(2));
      expect(get.mock.calls[1]).toEqual([
        'ORD',
        {
          filter_subjects: ['a.>'],
          deliver_policy: 'by_start_sequence',
          opt_start_seq: 3,
        },
      ]);
      await vi.waitFor(() => expect(fakes).toHaveLength(2));
      await adapter.stop();
      await adapter.idle();
    });

    it('reopens with the declared options when nothing was handled', async () => {
      const { adapter, fake, fakes, get } = setup(() => Promise.resolve(), {
        deliverPolicy: { startTime: '2026-01-01T00:00:00Z' },
        restart,
      });
      await adapter.start();
      fake.fail(new Error('boom'));
      await vi.waitFor(() => expect(get).toHaveBeenCalledTimes(2));
      expect(get.mock.calls[1]).toEqual([
        'ORD',
        {
          deliver_policy: 'by_start_time',
          opt_start_time: '2026-01-01T00:00:00Z',
        },
      ]);
      await vi.waitFor(() => expect(fakes).toHaveLength(2));
      await adapter.stop();
      await adapter.idle();
    });

    it('does not restart after stop()', async () => {
      const { adapter, fake, get, logger } = setup(() => Promise.resolve(), {
        restart,
      });
      await adapter.start();
      await adapter.stop();
      fake.fail(new Error('boom'));
      await adapter.idle();
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(get).toHaveBeenCalledTimes(1);
      expect(logger.error).not.toHaveBeenCalled();
    });
  });
});
