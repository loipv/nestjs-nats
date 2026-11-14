import type { JetStreamClient as NatsJetStreamClient } from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/transport-node';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { NatsClient } from '../../../src/client/nats-client.js';
import { jsonCodec } from '../../../src/codec/codec.js';
import type { ConnectionEntry } from '../../../src/connection/connection-registry.js';
import {
  NatsAtomicPublishError,
  NatsModuleError,
  NatsRequestError,
} from '../../../src/errors/errors.js';
import { InMemoryIdempotencyStore } from '../../../src/idempotency/store.js';
import { JetStreamClient } from '../../../src/jetstream/jetstream-client.js';
import { createNatsLogger } from '../../../src/module/logger.js';
import type { PublishErrorHandler } from '../../../src/module/options.js';
import type { NatsPlugin, OutgoingMessage } from '../../../src/pipeline/types.js';
import { FakeNatsConnection } from '../../../src/testing/fake-connection.js';
import { never } from '../../support/async.js';

type FakeJs = ReturnType<FakeNatsConnection['jetstream']>;

/** Clients over a fake connection whose outbound plugins are `plugins`. */
function setup(
  plugins: NatsPlugin[] = [],
  jsTimeout?: number,
  onPublishError?: PublishErrorHandler,
) {
  const fake = new FakeNatsConnection(jsonCodec());
  const fakeJs: FakeJs = fake.jetstream();
  if (jsTimeout !== undefined)
    Object.assign(fakeJs, { getOptions: () => ({ timeout: jsTimeout }) });
  const store = new InMemoryIdempotencyStore();
  const logger = createNatsLogger(false, 'test');
  const entry: ConnectionEntry = {
    name: 'default',
    options: { name: 'default', connection: {}, plugins, onPublishError },
    nc: fake as unknown as NatsConnection,
    logger,
    js: () => fakeJs as unknown as NatsJetStreamClient,
    jsm: () => Promise.reject(new Error('no jsm')),
    idempotencyStore: () => store,
    onClose: () => () => {},
  };
  const error = vi.spyOn(logger, 'error');
  return { nats: new NatsClient(entry), js: new JetStreamClient(entry), fake, fakeJs, error };
}

function plugin(handle: (m: OutgoingMessage, next: () => Promise<void>) => Promise<void>) {
  return { name: 'test', outbound: { handle } } satisfies NatsPlugin;
}

/** Never passes the message on, never settles. */
const stuck = plugin(() => never());
/** Passes the message on, then never settles. */
const hanging = plugin(async (_m, next) => {
  await next();
  await never();
});
const boom = new Error('audit failed');
/** Fails after the message was sent. */
const failingAfter = plugin(async (_m, next) => {
  await next();
  throw boom;
});
/** Fails before passing the message on. */
const failingBefore = plugin(async () => {
  throw boom;
});
/** Rethrows the failure of the send. */
const rethrowing = plugin(async (_m, next) => {
  try {
    await next();
  } catch (err) {
    await Promise.resolve();
    throw err;
  }
});

/** Settles `result` after `ms` of fake time, checking it is pending until then. */
async function settlesAfter<T>(result: Promise<T>, ms: number): Promise<T> {
  let settled = false;
  void result.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  await vi.advanceTimersByTimeAsync(ms - 1);
  expect(settled).toBe(false);
  await vi.advanceTimersByTimeAsync(1);
  expect(settled).toBe(true);
  return result;
}

/** Waits for pending callbacks, so a late plugin failure would have been logged. */
const drain = () => new Promise<void>((resolve) => setTimeout(resolve, 10));

afterEach(() => {
  vi.useRealTimers();
});

describe('JetStreamClient.publish outbound bound', () => {
  it('rejects when the plugins do not pass the message on within the timeout', async () => {
    vi.useFakeTimers();
    const { js, fake, fakeJs } = setup([stuck]);
    const publish = vi.spyOn(fakeJs, 'publish');
    const result = js.publish('o', 1, { timeout: 300 }).catch((e: unknown) => e);
    const err = await settlesAfter(result, 300);
    expect(err).toBeInstanceOf(NatsModuleError);
    expect((err as Error).message).toBe(
      'JetStream publish to "o": the outbound plugins did not pass the message on within 300 ms',
    );
    expect(publish).not.toHaveBeenCalled();
    expect(fake.jsPublished).toEqual([]);
  });

  it("defaults the bound to the JetStream client's timeout", async () => {
    vi.useFakeTimers();
    const { js } = setup([stuck], 700);
    const err = await settlesAfter(
      js.publish('o', 1).catch((e: unknown) => e),
      700,
    );
    expect((err as Error).message).toContain('within 700 ms');
  });

  it('defaults the bound to 5000 ms without a JetStream client timeout', async () => {
    vi.useFakeTimers();
    const { js } = setup([stuck]);
    const err = await settlesAfter(
      js.publish('o', 1).catch((e: unknown) => e),
      5000,
    );
    expect((err as Error).message).toContain('within 5000 ms');
  });

  it('clamps a JetStream client timeout that overflows a timer', async () => {
    vi.useFakeTimers();
    const { js } = setup([stuck], 2 ** 40);
    const result = js.publish('o', 1).catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(10);
    let settled = false;
    void result.then(() => {
      settled = true;
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(2 ** 31 - 1);
    expect(((await result) as Error).message).toContain(`within ${2 ** 31 - 1} ms`);
  });

  it('sends nothing when the plugin passes the message on after the timeout', async () => {
    vi.useFakeTimers();
    let late!: () => Promise<void>;
    const { js, fake, error } = setup([
      plugin(async (_m, next) => {
        late = next;
        await never();
      }),
    ]);
    const result = js.publish('o', 1, { timeout: 100 }).catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(100);
    const err = await result;
    await expect(late()).rejects.toBe(err);
    expect(fake.jsPublished).toEqual([]);
    expect(error).not.toHaveBeenCalled();
  });

  it('logs a plugin failure after the timeout, unless it rethrows the timeout', async () => {
    vi.useFakeTimers();
    // Passes the messages on too late: the timeout of 1 propagates, 2 fails on its own instead.
    const slow = plugin(async (m, next) => {
      await new Promise((resolve) => setTimeout(resolve, 200));
      if (m.data === 1) return next();
      await next().catch(() => {});
      throw boom;
    });
    const { js, fake, error } = setup([slow]);
    const results = [
      js.publish('o', 1, { timeout: 100 }).catch((e: unknown) => e),
      js.publish('o', 2, { timeout: 100 }).catch((e: unknown) => e),
    ];
    await vi.advanceTimersByTimeAsync(200);
    for (const err of await Promise.all(results)) expect(err).toBeInstanceOf(NatsModuleError);
    expect(error).toHaveBeenCalledExactlyOnceWith(
      'JetStream publish to "o": an outbound plugin failed after the call timed out',
      boom.stack,
    );
    expect(fake.jsPublished).toEqual([]);
  });

  it('settles with the ack without waiting for a plugin that never settles after next()', async () => {
    const { js, fake } = setup([hanging]);
    await expect(js.publish('o', 1)).resolves.toMatchObject({ seq: 1 });
    expect(fake.jsPublished).toHaveLength(1);
  });

  it('settles with the ack when a plugin fails after next(), logging the failure', async () => {
    const { js, error } = setup([failingAfter]);
    await expect(js.publish('o', 1)).resolves.toMatchObject({ seq: 1 });
    await vi.waitFor(() => expect(error).toHaveBeenCalled());
    expect(error).toHaveBeenCalledExactlyOnceWith(
      'JetStream publish to "o": an outbound plugin failed after the message was sent',
      boom.stack,
    );
  });

  it('rejects with the failure of the send that a plugin rethrows, without logging it', async () => {
    const { js, fakeJs, error } = setup([rethrowing]);
    const refused = new Error('no stream');
    vi.spyOn(fakeJs, 'publish').mockRejectedValue(refused);
    await expect(js.publish('o', 1)).rejects.toBe(refused);
    await drain();
    expect(error).not.toHaveBeenCalled();
  });

  it("rejects with a plugin's failure before next()", async () => {
    const { js, fake } = setup([failingBefore]);
    await expect(js.publish('o', 1)).rejects.toBe(boom);
    expect(fake.jsPublished).toEqual([]);
  });

  it('applies to publishMany', async () => {
    vi.useFakeTimers();
    const { js } = setup([stuck]);
    const results = await settlesAfter(
      js.publishMany([{ subject: 'o', opts: { timeout: 50 } }]),
      50,
    );
    expect(results[0]).toMatchObject({ ok: false, error: expect.any(NatsModuleError) });
  });
});

describe('single-message publishAtomic outbound bound', () => {
  it('rejects when the plugins do not pass the message on within the timeout', async () => {
    vi.useFakeTimers();
    const { js, fake } = setup([stuck]);
    const result = js.publishAtomic([{ subject: 'o', data: 1 }], { timeout: 400 });
    const err = await settlesAfter(
      result.catch((e: unknown) => e),
      400,
    );
    expect(err).toBeInstanceOf(NatsAtomicPublishError);
    expect(err).toMatchObject({ subject: 'o', count: 1 });
    expect((err as Error).message).toBe(
      'Atomic publish of 1 message to "o" aborted: the outbound plugins did not pass the message on within 400 ms',
    );
    expect(fake.jsPublished).toEqual([]);
  });

  it('rejects with NatsAtomicPublishError when a plugin does not pass the message on', async () => {
    const { js, fake } = setup([plugin(async () => {})]);
    const err = await js.publishAtomic([{ subject: 'o', data: 1 }]).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsAtomicPublishError);
    expect((err as Error).message).toBe(
      'Atomic publish of 1 message to "o" aborted: an outbound plugin did not pass message 0 on',
    );
    expect(fake.jsPublished).toEqual([]);
  });

  it('clamps the fallback JetStream client timeout to the largest timer delay', async () => {
    vi.useFakeTimers();
    const { js, fakeJs } = setup([], 2 ** 40);
    const publish = vi.spyOn(fakeJs, 'publish');
    await js.publishAtomic([{ subject: 'o', data: 1 }]);
    expect(publish.mock.calls[0]?.[2]).toMatchObject({ timeout: 2 ** 31 - 1 });
  });

  it('settles with the ack without waiting for a plugin that never settles after next()', async () => {
    const { js } = setup([hanging]);
    await expect(js.publishAtomic([{ subject: 'o', data: 1 }])).resolves.toMatchObject({
      count: 1,
    });
  });

  it('settles with the ack when a plugin fails after next(), logging the failure', async () => {
    const { js, error } = setup([failingAfter]);
    await expect(js.publishAtomic([{ subject: 'o', data: 1 }])).resolves.toMatchObject({
      count: 1,
    });
    await vi.waitFor(() => expect(error).toHaveBeenCalled());
    expect(error).toHaveBeenCalledExactlyOnceWith(
      'Atomic publish of 1 message to "o": an outbound plugin failed after the message was sent',
      boom.stack,
    );
  });

  it('rejects with the failure of the send that a plugin rethrows, without logging it', async () => {
    const { js, fakeJs, error } = setup([rethrowing]);
    vi.spyOn(fakeJs, 'publish').mockRejectedValue(new Error('no stream'));
    const err = await js.publishAtomic([{ subject: 'o', data: 1 }]).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsAtomicPublishError);
    expect((err as Error).message).toContain('no stream');
    await drain();
    expect(error).not.toHaveBeenCalled();
  });

  it("rejects with a plugin's failure before next()", async () => {
    const { js } = setup([failingBefore]);
    await expect(js.publishAtomic([{ subject: 'o', data: 1 }])).rejects.toBe(boom);
  });
});

describe('NatsClient.request outbound bound', () => {
  it('rejects when the plugins do not pass the message on within the timeout', async () => {
    vi.useFakeTimers();
    const { nats, fake } = setup([stuck]);
    fake.whenRequest('svc').reply('pong');
    const result = nats.request('svc', 'ping', { timeout: 250 }).catch((e: unknown) => e);
    const err = await settlesAfter(result, 250);
    expect(err).toBeInstanceOf(NatsModuleError);
    expect((err as Error).message).toBe(
      'Request to "svc": the outbound plugins did not pass the message on within 250 ms',
    );
    expect(fake.published).toEqual([]);
  });

  it('defaults the bound to 5000 ms', async () => {
    vi.useFakeTimers();
    const { nats } = setup([stuck]);
    const err = await settlesAfter(
      nats.request('svc').catch((e: unknown) => e),
      5000,
    );
    expect((err as Error).message).toContain('within 5000 ms');
  });

  it('settles with the reply without waiting for a plugin that never settles after next()', async () => {
    const { nats, fake } = setup([hanging]);
    fake.whenRequest('svc').reply('pong');
    await expect(nats.request('svc', 'ping')).resolves.toBe('pong');
  });

  it('settles with the reply when a plugin fails after next(), logging the failure', async () => {
    const { nats, fake, error } = setup([failingAfter]);
    fake.whenRequest('svc').reply('pong');
    await expect(nats.request('svc', 'ping')).resolves.toBe('pong');
    await vi.waitFor(() => expect(error).toHaveBeenCalled());
    expect(error).toHaveBeenCalledExactlyOnceWith(
      'Request to "svc": an outbound plugin failed after the message was sent',
      boom.stack,
    );
  });

  it('rejects with the NatsRequestError that a plugin rethrows, without logging it', async () => {
    const { nats, error } = setup([rethrowing]);
    const err = await nats.request('svc', 'ping').catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsRequestError);
    expect(err).toMatchObject({ code: 'no_responders' });
    await drain();
    expect(error).not.toHaveBeenCalled();
  });

  it('rejects with the NatsRequestError when a plugin fails on its own after it', async () => {
    const { nats, error } = setup([
      plugin(async (_m, next) => {
        await next().catch(() => {});
        throw boom;
      }),
    ]);
    await expect(nats.request('svc', 'ping')).rejects.toBeInstanceOf(NatsRequestError);
    await vi.waitFor(() => expect(error).toHaveBeenCalled());
    expect(error).toHaveBeenCalledExactlyOnceWith(expect.any(String), boom.stack);
  });

  it("rejects with a plugin's failure before next()", async () => {
    const { nats, fake } = setup([failingBefore]);
    await expect(nats.request('svc', 'ping')).rejects.toBe(boom);
    expect(fake.published).toEqual([]);
  });
});

describe('NatsClient.publish error hook with plugins', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('hands a plugin failure before next() to the hook instead of logging it', async () => {
    const hook = vi.fn();
    const { nats, fake, error } = setup([failingBefore], undefined, hook);
    nats.publish('o', 1);
    await nats.flush();
    expect(fake.published).toEqual([]);
    expect(hook).toHaveBeenCalledExactlyOnceWith(boom, { subject: 'o', connection: 'default' });
    expect(error).not.toHaveBeenCalled();
  });

  it('hands a message the plugins did not pass on in time to the hook', async () => {
    vi.useFakeTimers();
    const hook = vi.fn();
    const { nats, error } = setup([stuck], undefined, hook);
    nats.publish('o', 1);
    await settlesAfter(nats.flush(), 5000);
    expect(hook).toHaveBeenCalledOnce();
    const [err] = hook.mock.calls[0] as [Error];
    expect(err).toBeInstanceOf(NatsModuleError);
    expect(err.message).toBe(
      'Publish to "o": the outbound plugins did not pass the message on within 5000 ms; dropped',
    );
    expect(error).not.toHaveBeenCalled();
  });

  it('does not call the hook for a message a plugin dropped on purpose', async () => {
    const hook = vi.fn();
    const { nats, fake } = setup([plugin(async () => {})], undefined, hook);
    nats.publish('o', 1);
    await nats.flush();
    expect(fake.published).toEqual([]);
    expect(hook).not.toHaveBeenCalled();
  });

  it('does not call the hook for a plugin failing after the message was sent', async () => {
    const hook = vi.fn();
    const { nats, fake, error } = setup([failingAfter], undefined, hook);
    nats.publish('o', 1);
    await nats.flush();
    expect(fake.published).toHaveLength(1);
    await vi.waitFor(() => expect(error).toHaveBeenCalled());
    expect(hook).not.toHaveBeenCalled();
  });

  it('logs a plugin that rejects with undefined, like any failure', async () => {
    const { nats, error } = setup([
      plugin(async () => {
        throw undefined;
      }),
    ]);
    nats.publish('o', 1);
    await nats.flush();
    expect(error).toHaveBeenCalledExactlyOnceWith('Failed to publish to "o": undefined');
  });

  it('logs both errors when the hook throws on a plugin failure', async () => {
    const broken = new Error('hook broke');
    const { nats, error } = setup([failingBefore], undefined, () => {
      throw broken;
    });
    nats.publish('o', 1);
    await nats.flush();
    expect(error.mock.calls).toEqual([
      ['Publish to "o": the publish error handler failed', broken.stack],
      ['Failed to publish to "o": audit failed'],
    ]);
  });

  it("prefers the call's onError on a plugin failure", async () => {
    const hook = vi.fn();
    const onError = vi.fn();
    const { nats } = setup([failingBefore], undefined, hook);
    nats.publish('o', 1, { onError });
    await nats.flush();
    expect(onError).toHaveBeenCalledExactlyOnceWith(boom);
    expect(hook).not.toHaveBeenCalled();
  });

  it('lets flush() wait until the hook ran', async () => {
    const hook = vi.fn();
    const { nats } = setup([failingBefore], undefined, hook);
    nats.publish('o', 1);
    await nats.flush();
    expect(hook).toHaveBeenCalledOnce();
  });
});

describe('NatsClient.publish outbound bound', () => {
  it('drops the message and logs when the plugins do not pass it on within 5000 ms', async () => {
    vi.useFakeTimers();
    const { nats, fake, error } = setup([stuck]);
    nats.publish('o', 1);
    const flushed = nats.flush();
    await settlesAfter(flushed, 5000);
    expect(error).toHaveBeenCalledExactlyOnceWith(
      'Publish to "o": the outbound plugins did not pass the message on within 5000 ms; dropped',
    );
    expect(fake.published).toEqual([]);
  });

  it('does not wait for a plugin that never settles after next()', async () => {
    const { nats, fake } = setup([hanging]);
    nats.publish('o', 1);
    await nats.flush();
    expect(fake.published).toHaveLength(1);
  });

  it('logs a plugin failure after next()', async () => {
    const { nats, fake, error } = setup([failingAfter]);
    nats.publish('o', 1);
    await nats.flush();
    expect(fake.published).toHaveLength(1);
    await vi.waitFor(() => expect(error).toHaveBeenCalled());
    expect(error).toHaveBeenCalledExactlyOnceWith(
      'Publish to "o": an outbound plugin failed after the message was sent',
      boom.stack,
    );
  });

  it("logs a plugin's failure before next() once", async () => {
    const { nats, fake, error } = setup([failingBefore]);
    nats.publish('o', 1);
    await nats.flush();
    expect(fake.published).toEqual([]);
    expect(error).toHaveBeenCalledExactlyOnceWith('Failed to publish to "o": audit failed');
  });

  it('logs the failure of the send that a plugin rethrows once', async () => {
    const { nats, fake, error } = setup([rethrowing]);
    vi.spyOn(fake, 'publish').mockImplementation(() => {
      throw new Error('closed');
    });
    nats.publish('o', 1);
    await nats.flush();
    await drain();
    expect(error).toHaveBeenCalledExactlyOnceWith('Failed to publish to "o": closed');
  });
});

describe('outbound calls without plugins', () => {
  it('send at once, without a timer', async () => {
    vi.useFakeTimers();
    const { js, nats, fake } = setup();
    // The timers of the setup (the idempotency store's sweep).
    const baseline = vi.getTimerCount();
    const setTimer = vi.spyOn(globalThis, 'setTimeout');
    fake.whenRequest('svc').reply('pong');
    nats.publish('o', 1);
    // Published synchronously.
    expect(fake.published).toHaveLength(1);

    const results = [
      js.publish('o', 1),
      js.publishAtomic([{ subject: 'o', data: 2 }]),
      nats.request('svc', 'ping'),
    ];
    expect(vi.getTimerCount()).toBe(baseline);
    await expect(Promise.all(results)).resolves.toMatchObject([{ seq: 1 }, { count: 1 }, 'pong']);
    await nats.flush();
    expect(vi.getTimerCount()).toBe(baseline);
    expect(setTimer).not.toHaveBeenCalled();
    setTimer.mockRestore();
  });

  it('keep the errors of the send', async () => {
    const { js, nats, fake, fakeJs, error } = setup();
    const refused = new Error('no stream');
    vi.spyOn(fakeJs, 'publish').mockRejectedValue(refused);
    await expect(js.publish('o', 1)).rejects.toBe(refused);
    await expect(js.publishAtomic([{ subject: 'o', data: 1 }])).rejects.toBeInstanceOf(
      NatsAtomicPublishError,
    );
    await expect(nats.request('svc')).rejects.toMatchObject({ code: 'no_responders' });
    vi.spyOn(fake, 'publish').mockImplementation(() => {
      throw new Error('closed');
    });
    nats.publish('o', 1);
    await nats.flush();
    expect(error).toHaveBeenCalledExactlyOnceWith('Failed to publish to "o": closed');
  });
});

describe('a next() called after the call failed', () => {
  it('sends nothing after the plugin threw', async () => {
    let late!: () => Promise<void>;
    const { js, fake } = setup([
      plugin(async (_m, next) => {
        late = next;
        throw boom;
      }),
    ]);
    await expect(js.publish('o', 1)).rejects.toBe(boom);
    await expect(late()).rejects.toBe(boom);
    expect(fake.jsPublished).toEqual([]);
  });

  it('sends nothing after the plugin returned without calling it', async () => {
    let late!: () => Promise<void>;
    const { js, nats, fake } = setup([
      plugin(async (_m, next) => {
        late = next;
      }),
    ]);
    const err = await js.publish('o', 1).catch((e: unknown) => e);
    expect((err as Error).message).toBe('JetStream publish to "o" produced no acknowledgement');
    await expect(late()).rejects.toBe(err);
    expect(fake.jsPublished).toEqual([]);
    // NatsClient.publish resolves when the plugin drops the message; a late next() is refused too.
    nats.publish('o', 1);
    await nats.flush();
    await expect(late()).rejects.toBeInstanceOf(NatsModuleError);
    expect(fake.published).toEqual([]);
  });
});
