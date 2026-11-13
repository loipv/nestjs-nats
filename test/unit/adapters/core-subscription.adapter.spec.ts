import { describe, expect, it, vi } from 'vitest';
import type { HandlerState, RestartBinding } from '../../../src/adapters/adapter.js';
import { CoreSubscriptionAdapter } from '../../../src/adapters/core-subscription.adapter.js';
import type { ConnectionEntry } from '../../../src/connection/connection-registry.js';
import type { HandlerDefinition } from '../../../src/discovery/handler-explorer.js';
import { tick } from '../../support/async.js';

/** Push-driven subscription whose `drain()` ends iteration after buffered messages. */
function fakeSubscription() {
  const queue: unknown[] = [];
  let wake: (() => void) | undefined;
  let closed = false;
  let failure: Error | undefined;
  const sub = {
    async *[Symbol.asyncIterator]() {
      for (;;) {
        if (failure) throw failure;
        const next = queue.shift();
        if (next) yield next;
        else if (closed) return;
        else await new Promise<void>((resolve) => (wake = resolve));
      }
    },
    isClosed: () => closed,
    unsubscribe: vi.fn(() => {
      closed = true;
      wake?.();
    }),
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
    fail(err: Error) {
      failure = err;
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
    it('delivers batches of maxSize', async () => {
      const batches: unknown[] = [];
      const { adapter, fake } = setup(
        async (payload) => {
          batches.push(payload);
        },
        [],
        {},
        { batch: { maxSize: 2, maxWait: 60_000 } },
      );
      await adapter.start();
      for (const n of [1, 2, 3, 4]) fake.push(fakeMsg(n));
      await tick();
      await tick();
      expect(batches).toEqual([
        [1, 2],
        [3, 4],
      ]);
      await adapter.stop();
      await adapter.idle();
    });

    it('rejects requests with a 400 reply without handling them, warning once', async () => {
      const batches: unknown[] = [];
      const { adapter, fake, logger } = setup(
        async (payload) => {
          batches.push(payload);
          return 'reply';
        },
        [],
        {},
        { batch: { maxSize: 2, maxWait: 60_000 } },
      );
      await adapter.start();
      const requests = [1, 2].map((n) => ({ ...fakeMsg(n), reply: `_INBOX.${n}` }));
      fake.push(requests[0]);
      fake.push(fakeMsg(3));
      fake.push(requests[1]);
      fake.push(fakeMsg(4));
      await tick();
      await tick();
      expect(batches).toEqual([[3, 4]]);
      for (const request of requests) {
        expect(request.respond).toHaveBeenCalledTimes(1);
        const [data, opts] = request.respond.mock.calls[0] as unknown as [
          Uint8Array,
          { headers: { get(k: string): string } },
        ];
        expect(data).toHaveLength(0);
        expect(opts.headers.get('Nats-Service-Error-Code')).toBe('400');
        expect(opts.headers.get('Nats-Service-Error')).toBe(
          'NATS handler C.m handles batches and does not reply to requests',
        );
      }
      expect(logger.warn).toHaveBeenCalledTimes(1);
      expect(logger.warn).toHaveBeenCalledWith(
        'Rejected a request on "a.b" for C.m on "a.b" with 400: batch handlers do not reply to requests',
      );
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

describe('CoreSubscriptionAdapter restart', () => {
  const policy = { initialDelay: 10, factor: 1, maxDelay: 10, jitter: 0, maxAttempts: Infinity };

  function setupRestart(
    invoke: (payload: unknown) => Promise<unknown> = () => Promise.resolve(),
    handlerRetry: Record<string, unknown> = {},
    restartPolicy: RestartBinding['policy'] = policy,
  ) {
    const fakes: ReturnType<typeof fakeSubscription>[] = [];
    const states: HandlerState[] = [];
    const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
    const conn = { closed: false };
    const entry = {
      name: 'default',
      options: { plugins: [], ...handlerRetry },
      logger,
      nc: {
        subscribe: vi.fn(() => {
          const fake = fakeSubscription();
          fakes.push(fake);
          return fake.sub;
        }),
        flush: vi.fn(() => Promise.resolve()),
        isClosed: () => conn.closed,
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
    const restart: RestartBinding = {
      policy: restartPolicy,
      track: () => (state) => states.push(state),
    };
    const adapter = new CoreSubscriptionAdapter(def, entry, restart);
    return { adapter, fakes, states, logger, conn, entry };
  }

  it('resubscribes after the subscription fails and handles new messages', async () => {
    const invoke = vi.fn(() => Promise.resolve());
    const { adapter, fakes, states, entry } = setupRestart(invoke);
    await adapter.start();
    fakes[0]?.fail(new Error('boom'));
    await vi.waitFor(() => expect(entry.nc.subscribe).toHaveBeenCalledTimes(2));
    await vi.waitFor(() => expect(states).toEqual(['running', 'restarting', 'running']));
    fakes[1]?.push(fakeMsg(1));
    await vi.waitFor(() => expect(invoke).toHaveBeenCalledTimes(1));
    await adapter.stop();
    await adapter.idle();
  });

  it('keeps a pending retry across the restart', async () => {
    const invoke = vi
      .fn<(p: unknown) => Promise<unknown>>()
      .mockRejectedValueOnce(new Error('boom'))
      .mockResolvedValue(undefined);
    const { adapter, fakes, entry } = setupRestart(invoke, {
      retry: { initialDelay: 50, jitter: 0 },
    });
    await adapter.start();
    fakes[0]?.push(fakeMsg(1));
    await vi.waitFor(() => expect(invoke).toHaveBeenCalledTimes(1));
    fakes[0]?.fail(new Error('gone'));
    await vi.waitFor(() => expect(entry.nc.subscribe).toHaveBeenCalledTimes(2));
    await vi.waitFor(() => expect(invoke).toHaveBeenCalledTimes(2));
    await adapter.stop();
    await adapter.idle();
  });

  it('does not restart when the connection is closed', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    const { adapter, fakes, states, logger, conn, entry } = setupRestart();
    await adapter.start();
    const timers = vi.getTimerCount();
    conn.closed = true;
    fakes[0]?.fail(new Error('boom'));
    await vi.waitFor(() => expect(states).toEqual(['running', 'failed']));
    expect(entry.nc.subscribe).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(timers);
    vi.useRealTimers();
    expect(logger.error).toHaveBeenCalledWith(
      'Subscription of C.m on "a.b" failed; no more messages will be received',
      expect.anything(),
    );
    await adapter.stop();
    await adapter.idle();
  });

  it('logs as before and stays down without a restart binding', async () => {
    const { adapter, fake, logger } = setup(() => Promise.resolve());
    await adapter.start();
    fake.fail(new Error('boom'));
    await vi.waitFor(() =>
      expect(logger.error).toHaveBeenCalledWith(
        'Subscription of C.m on "a.b" failed; no more messages will be received',
        expect.anything(),
      ),
    );
    await adapter.stop();
    await adapter.idle();
  });

  it('stop() during the restart delay prevents the resubscribe', async () => {
    const { adapter, fakes, states, entry } = setupRestart(
      undefined,
      {},
      {
        ...policy,
        initialDelay: 10_000,
        maxDelay: 10_000,
      },
    );
    await adapter.start();
    fakes[0]?.fail(new Error('boom'));
    await vi.waitFor(() => expect(states).toEqual(['running', 'restarting']));
    await adapter.stop();
    await adapter.idle();
    expect(entry.nc.subscribe).toHaveBeenCalledTimes(1);
  });

  it('drops a pending retry into onRejected when stop() lands during the restart delay', async () => {
    const invoke = vi.fn(() => Promise.reject(new Error('boom')));
    const { adapter, fakes, states, logger } = setupRestart(
      invoke,
      { retry: { initialDelay: 200, jitter: 0 } },
      { ...policy, initialDelay: 10_000, maxDelay: 10_000 },
    );
    await adapter.start();
    fakes[0]?.push(fakeMsg(1));
    await vi.waitFor(() => expect(invoke).toHaveBeenCalledTimes(1));
    fakes[0]?.fail(new Error('gone'));
    await vi.waitFor(() => expect(states).toEqual(['running', 'restarting']));
    await adapter.stop();
    await adapter.idle();
    await vi.waitFor(() =>
      expect(logger.warn).toHaveBeenCalledWith(
        'Dropped message on "a.b" for C.m on "a.b": the subscription is stopping',
      ),
    );
    expect(invoke).toHaveBeenCalledTimes(1);
  });

  it('stops the dispatcher when the supervisor gives up on its own', async () => {
    const invoke = vi.fn(() => Promise.reject(new Error('boom')));
    const { adapter, fakes, states, logger, conn } = setupRestart(
      invoke,
      { retry: { initialDelay: 400, jitter: 0 } },
      { ...policy, initialDelay: 150, maxDelay: 150 },
    );
    await adapter.start();
    fakes[0]?.push(fakeMsg(1));
    await vi.waitFor(() => expect(invoke).toHaveBeenCalledTimes(1));
    fakes[0]?.fail(new Error('gone'));
    await vi.waitFor(() => expect(states).toEqual(['running', 'restarting']));
    conn.closed = true;
    await vi.waitFor(() => expect(states).toEqual(['running', 'restarting', 'failed']));
    await adapter.idle();
    await vi.waitFor(() =>
      expect(logger.warn).toHaveBeenCalledWith(
        'Dropped message on "a.b" for C.m on "a.b": the subscription is stopping',
      ),
    );
    expect(invoke).toHaveBeenCalledTimes(1);
  });

  it('unsubscribes a reopened subscription whose flush fails and ignores its loop', async () => {
    const { adapter, fakes, states, logger, entry } = setupRestart();
    await adapter.start();
    vi.mocked(entry.nc.flush).mockRejectedValueOnce(new Error('flush failed'));
    fakes[0]?.fail(new Error('gone'));
    await vi.waitFor(() => expect(entry.nc.subscribe).toHaveBeenCalledTimes(3));
    await vi.waitFor(() => expect(states.at(-1)).toBe('running'));
    expect(fakes[1]?.sub.unsubscribe).toHaveBeenCalledTimes(1);
    expect(logger.warn).toHaveBeenCalledWith(
      'Restart of C.m on "a.b" failed (attempt 1): flush failed',
    );
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(states).toEqual(['running', 'restarting', 'restarting', 'running']);
    await adapter.stop();
    await adapter.idle();
  });

  it('counts a reopen whose loop ended before the flush as a failed attempt', async () => {
    const { adapter, fakes, states, logger, entry } = setupRestart();
    await adapter.start();
    vi.mocked(entry.nc.flush).mockImplementationOnce(async () => {
      fakes[1]?.fail(new Error('gone again'));
      await tick();
      await tick();
    });
    fakes[0]?.fail(new Error('gone'));
    await vi.waitFor(() => expect(entry.nc.subscribe).toHaveBeenCalledTimes(3));
    await vi.waitFor(() => expect(states.at(-1)).toBe('running'));
    expect(logger.warn).toHaveBeenCalledWith(
      'Restart of C.m on "a.b" failed (attempt 1): the intake ended while restarting',
    );
    fakes[2]?.push(fakeMsg(1));
    await adapter.stop();
    await adapter.idle();
  });

  it('processes messages a draining in-flight reopen yields when stop() lands', async () => {
    const invoke = vi.fn(() => Promise.resolve());
    const { adapter, fakes, logger, entry } = setupRestart(invoke);
    await adapter.start();
    let release!: () => void;
    vi.mocked(entry.nc.flush).mockImplementationOnce(
      () =>
        new Promise<void>((r) => {
          release = r;
        }),
    );
    fakes[0]?.fail(new Error('gone'));
    await vi.waitFor(() => expect(entry.nc.subscribe).toHaveBeenCalledTimes(2));
    fakes[1]?.push(fakeMsg(1));
    await adapter.stop();
    release();
    await adapter.idle();
    expect(invoke).toHaveBeenCalledTimes(1);
    expect(logger.warn).not.toHaveBeenCalledWith(expect.stringContaining('Dropped message'));
  });

  it('stops the dispatcher when stop() lands during a reopen whose flush then fails', async () => {
    const invoke = vi.fn(() => Promise.reject(new Error('boom')));
    const { adapter, fakes, logger, entry } = setupRestart(invoke, {
      retry: { initialDelay: 200, jitter: 0 },
    });
    await adapter.start();
    fakes[0]?.push(fakeMsg(1));
    await vi.waitFor(() => expect(invoke).toHaveBeenCalledTimes(1));
    let reject!: (e: Error) => void;
    vi.mocked(entry.nc.flush).mockImplementationOnce(
      () =>
        new Promise<void>((_, rej) => {
          reject = rej;
        }),
    );
    fakes[0]?.fail(new Error('gone'));
    await vi.waitFor(() => expect(entry.nc.subscribe).toHaveBeenCalledTimes(2));
    const stopped = adapter.stop();
    reject(new Error('flush failed'));
    await stopped;
    await adapter.idle();
    await vi.waitFor(() =>
      expect(logger.warn).toHaveBeenCalledWith(
        'Dropped message on "a.b" for C.m on "a.b": the subscription is stopping',
      ),
    );
    expect(invoke).toHaveBeenCalledTimes(1);
  });

  it('restarts a subscription whose loop ends during the first flush', async () => {
    const { adapter, fakes, states, entry } = setupRestart();
    let release!: () => void;
    vi.mocked(entry.nc.flush).mockImplementationOnce(
      () =>
        new Promise<void>((r) => {
          release = r;
        }),
    );
    const started = adapter.start();
    fakes[0]?.fail(new Error('denied'));
    await tick();
    await tick();
    release();
    await started;
    await vi.waitFor(() => expect(entry.nc.subscribe).toHaveBeenCalledTimes(2));
    expect(states.slice(0, 2)).toEqual(['running', 'restarting']);
    await adapter.stop();
    await adapter.idle();
  });
});
