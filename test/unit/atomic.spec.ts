import type { JetStreamClient as NatsJetStreamClient } from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';
import { jsonCodec } from '../../src/codec/codec.js';
import type { ConnectionEntry } from '../../src/connection/connection-registry.js';
import { NatsAtomicPublishError, NatsConfigError } from '../../src/errors/errors.js';
import { InMemoryIdempotencyStore } from '../../src/idempotency/store.js';
import { JetStreamClient } from '../../src/jetstream/jetstream-client.js';
import { createNatsLogger } from '../../src/module/logger.js';
import type { NatsPlugin, OutgoingMessage } from '../../src/pipeline/types.js';
import { FakeNatsConnection } from '../../src/testing/fake-connection.js';
import { createMockJetStreamClient } from '../../src/testing/index.js';

type FakeJs = ReturnType<FakeNatsConnection['jetstream']>;
type FakeBatch = Awaited<ReturnType<FakeJs['startBatch']>>;

/** A JetStreamClient over a fake connection whose JetStream client the test can spy on. */
function setup(opts: { plugins?: NatsPlugin[]; servers?: string; jsTimeout?: number } = {}): {
  client: JetStreamClient;
  fake: FakeNatsConnection;
  fakeJs: FakeJs;
  logger: ConnectionEntry['logger'];
} {
  const fake = new FakeNatsConnection(jsonCodec());
  const fakeJs = fake.jetstream();
  if (opts.jsTimeout !== undefined) {
    const timeout = opts.jsTimeout;
    Object.assign(fakeJs, { getOptions: () => ({ timeout }) });
  }
  const store = new InMemoryIdempotencyStore();
  const logger = createNatsLogger(false, 'test');
  const entry: ConnectionEntry = {
    name: 'default',
    options: {
      name: 'default',
      connection: opts.servers ? { servers: opts.servers } : {},
      ...(opts.plugins ? { plugins: opts.plugins } : {}),
    },
    nc: fake as unknown as NatsConnection,
    logger,
    js: () => fakeJs as unknown as NatsJetStreamClient,
    jsm: () => Promise.reject(new Error('no jsm')),
    idempotencyStore: () => store,
    onClose: () => () => {},
  };
  return { client: new JetStreamClient(entry), fake, fakeJs, logger };
}

/**
 * Spies on the batches the fake starts; their `commit` runs `commit` (by default the real one).
 */
function spyBatch(fakeJs: FakeJs, commit?: FakeBatch['commit']) {
  const startBatch = fakeJs.startBatch.bind(fakeJs);
  const commitSpy = vi.fn<FakeBatch['commit']>();
  const startBatchSpy = vi.spyOn(fakeJs, 'startBatch').mockImplementation(async (...args) => {
    const batch = await startBatch(...args);
    commitSpy.mockImplementation(commit ?? batch.commit);
    return { ...batch, commit: commitSpy };
  });
  return { startBatch: startBatchSpy, commit: commitSpy };
}

/** A plugin running `handle` for every outbound message. */
function plugin(handle: (m: OutgoingMessage, next: () => Promise<void>) => Promise<void>) {
  return { name: 'test', outbound: { handle } } satisfies NatsPlugin;
}

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const three = [
  { subject: 'o.a', data: 0 },
  { subject: 'o.b', data: 1 },
  { subject: 'o.c', data: 2 },
];

describe('publishAtomic', () => {
  it('rejects empty and oversized batches without publishing', async () => {
    const handle = vi.fn((_m: OutgoingMessage, next: () => Promise<void>) => next());
    const js = createMockJetStreamClient({ plugins: [plugin(handle)] });
    const many = Array.from({ length: 1001 }, (_, i) => ({ subject: 'o', data: i }));
    for (const messages of [[], many]) {
      const err = await js.publishAtomic(messages).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NatsConfigError);
      expect((err as Error).message).toBe('publishAtomic needs 1 to 1000 messages');
    }
    expect(handle).not.toHaveBeenCalled();
    expect(js.published).toEqual([]);
  });

  it('rejects a Nats-Msg-Id header in any case without publishing', async () => {
    const js = createMockJetStreamClient();
    const err = await js
      .publishAtomic([
        { subject: 'o', data: 1 },
        { subject: 'o', data: 2, headers: { 'nats-msg-id': 'x' } },
      ])
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsConfigError);
    expect((err as Error).message).toContain('Nats-Msg-Id');
    expect(js.published).toEqual([]);
  });

  it('rejects a timeout that is not positive', async () => {
    const js = createMockJetStreamClient();
    await expect(js.publishAtomic(three, { timeout: 0 })).rejects.toBeInstanceOf(NatsConfigError);
    expect(js.published).toEqual([]);
  });

  it('commits in array order', async () => {
    const js = createMockJetStreamClient();
    const ack = await js.publishAtomic(three);
    expect(js.published.map((m) => [m.subject, m.data])).toEqual([
      ['o.a', 0],
      ['o.b', 1],
      ['o.c', 2],
    ]);
    expect(js.published.map((m) => m.headers['Nats-Batch-Sequence'])).toEqual(['1', '2', '3']);
    expect(js.published.map((m) => m.headers['Nats-Batch-Commit'])).toEqual([
      undefined,
      undefined,
      '1',
    ]);
    expect(ack).toEqual({
      stream: 'mock',
      seq: 3,
      count: 3,
      batch: expect.any(String),
      duplicate: false,
    });
    expect(js.published.map((m) => m.headers['Nats-Batch-Id'])).toEqual([
      ack.batch,
      ack.batch,
      ack.batch,
    ]);
    expect(js.published.map((m) => m.atomic)).toEqual([
      { batch: ack.batch, index: 0, count: 3 },
      { batch: ack.batch, index: 1, count: 3 },
      { batch: ack.batch, index: 2, count: 3 },
    ]);
  });

  it('keeps the array order when a plugin is slower on an earlier message', async () => {
    const slow = plugin(async (m, next) => {
      await delay(m.atomic?.index === 0 ? 20 : 0);
      await next();
    });
    const js = createMockJetStreamClient({ plugins: [slow] });
    await js.publishAtomic([
      { subject: 'o', data: 'm0' },
      { subject: 'o', data: 'm1' },
    ]);
    expect(js.published.map((m) => m.data)).toEqual(['m0', 'm1']);
  });

  it('shows plugins the position of each message in the batch', async () => {
    const seen: Array<{ kind: string; atomic?: { index: number; count: number } }> = [];
    const js = createMockJetStreamClient({
      plugins: [
        plugin(async (m, next) => {
          seen.push({ kind: m.kind, atomic: m.atomic });
          await next();
        }),
      ],
    });
    await js.publishAtomic(three);
    expect(seen).toEqual(
      [0, 1, 2].map((index) => ({ kind: 'jetstream', atomic: { index, count: 3 } })),
    );
  });

  it('aborts when a plugin throws', async () => {
    const boom = new Error('plugin failed');
    let firstNext: unknown;
    const failing = plugin(async (m, next) => {
      if (m.atomic?.index === 1) throw boom;
      try {
        await next();
      } catch (err) {
        firstNext = err;
        throw err;
      }
    });
    const { client, fake, fakeJs } = setup({ plugins: [failing] });
    const startBatch = vi.spyOn(fakeJs, 'startBatch');
    await expect(client.publishAtomic(three)).rejects.toBe(boom);
    await vi.waitFor(() => expect(firstNext).toBeInstanceOf(NatsAtomicPublishError));
    expect((firstNext as Error).message).toContain('aborted');
    expect((firstNext as Error).cause).toBe(boom);
    expect(startBatch).not.toHaveBeenCalled();
    expect(fake.jsPublished).toEqual([]);
  });

  it('sends nothing once aborted, even when the remaining messages stage later', async () => {
    // Index 0 is staged, then its plugin gives up on next(); index 1 stages after that.
    const racing = plugin(async (m, next) => {
      if (m.atomic?.index === 0) {
        await Promise.race([
          next(),
          delay(30).then(() => {
            throw new Error('plugin timeout');
          }),
        ]);
      } else {
        await delay(100);
        await next();
      }
    });
    const { client, fake, fakeJs } = setup({ plugins: [racing] });
    const startBatch = vi.spyOn(fakeJs, 'startBatch');
    await expect(
      client.publishAtomic([
        { subject: 'o', data: 0 },
        { subject: 'o', data: 1 },
      ]),
    ).rejects.toThrow('plugin timeout');
    await delay(150);
    expect(startBatch).not.toHaveBeenCalled();
    expect(fake.jsPublished).toEqual([]);
  });

  it('aborts when a plugin does not pass a message on', async () => {
    const dropping = plugin(async (m, next) => {
      if (m.atomic?.index !== 1) await next();
    });
    const { client, fake } = setup({ plugins: [dropping] });
    const err = await client.publishAtomic(three).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsAtomicPublishError);
    expect((err as Error).message).toContain('aborted');
    expect(fake.jsPublished).toEqual([]);
  });

  it('shows plugins the commit failure, with credentials redacted', async () => {
    const seen: unknown[] = [];
    const watching = plugin(async (_m, next) => {
      try {
        await next();
      } catch (err) {
        seen.push(err);
        throw err;
      }
    });
    const { client, fake, fakeJs } = setup({
      plugins: [watching],
      servers: 'nats://u:s3cretpw@h:4222',
    });
    spyBatch(fakeJs, () => Promise.reject(new Error('lost nats://u:s3cretpw@h:4222 (s3cretpw)')));
    const err = await client.publishAtomic(three).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsAtomicPublishError);
    expect(err).toMatchObject({ subject: 'o.a', count: 3 });
    expect((err as Error).message).toContain('Atomic publish of 3 messages to "o.a" failed');
    expect(seen).toEqual([err, err, err]);
    const cause = (err as Error).cause as Error;
    expect(cause).toBeInstanceOf(Error);
    expect(`${(err as Error).message} ${cause.message} ${cause.stack}`).not.toContain('s3cretpw');
    expect(fake.jsPublished).toEqual([]);
  });

  it('settles with the commit when a plugin fails after its message was sent', async () => {
    const boom = new Error('audit failed');
    const auditing = plugin(async (m, next) => {
      await next();
      if (m.atomic?.index === 1) throw boom;
    });
    const { client, fake, logger } = setup({ plugins: [auditing] });
    const error = vi.spyOn(logger, 'error');
    const ack = await client.publishAtomic(three);
    expect(ack).toMatchObject({ count: 3, duplicate: false });
    expect(fake.jsPublished.map((m) => m.subject)).toEqual(['o.a', 'o.b', 'o.c']);
    // Logged when the plugin fails, which may be after publishAtomic settled.
    await vi.waitFor(() => expect(error).toHaveBeenCalled());
    expect(error).toHaveBeenCalledExactlyOnceWith(
      'Atomic publish of 3 messages to "o.a": an outbound plugin failed on message 1 after the batch was sent',
      boom.stack,
    );
  });

  it('settles with the commit when a plugin gives up while the commit is in flight', async () => {
    const racing = plugin(async (m, next) => {
      if (m.atomic?.index !== 0) return next();
      await Promise.race([
        next(),
        delay(10).then(() => {
          throw new Error('plugin timeout');
        }),
      ]);
    });
    const { client, fake, fakeJs, logger } = setup({ plugins: [racing] });
    const error = vi.spyOn(logger, 'error');
    // The commit takes longer than the plugin is willing to wait.
    const startBatch = fakeJs.startBatch.bind(fakeJs);
    vi.spyOn(fakeJs, 'startBatch').mockImplementation(async (...args) => {
      const batch = await startBatch(...args);
      const commit: FakeBatch['commit'] = async (...commitArgs) => {
        await delay(50);
        return batch.commit(...commitArgs);
      };
      return { ...batch, commit };
    });
    await expect(
      client.publishAtomic([
        { subject: 'o', data: 0 },
        { subject: 'o', data: 1 },
      ]),
    ).resolves.toMatchObject({ count: 2 });
    expect(fake.jsPublished).toHaveLength(2);
    await vi.waitFor(() => expect(error).toHaveBeenCalled());
    expect(error).toHaveBeenCalledExactlyOnceWith(
      expect.stringContaining('an outbound plugin failed on message 0 after the batch was sent'),
      expect.stringContaining('plugin timeout'),
    );
  });

  it('settles with the commit without waiting for a plugin that never settles after next()', async () => {
    const hanging = plugin(async (m, next) => {
      await next();
      if (m.atomic?.index === 1) await new Promise<never>(() => {});
    });
    const { client, fake } = setup({ plugins: [hanging] });
    await expect(client.publishAtomic(three)).resolves.toMatchObject({ count: 3 });
    expect(fake.jsPublished).toHaveLength(3);
  });

  it('aborts when a message is not staged within the timeout', async () => {
    vi.useFakeTimers();
    try {
      // The plugin neither passes message 1 on nor settles.
      const stuck = plugin(async (m, next) => {
        if (m.atomic?.index === 1) return new Promise<never>(() => {});
        await next();
      });
      const { client, fake, fakeJs } = setup({ plugins: [stuck] });
      const startBatch = vi.spyOn(fakeJs, 'startBatch');
      const result = client.publishAtomic(three, { timeout: 500 }).catch((e: unknown) => e);
      await vi.advanceTimersByTimeAsync(499);
      let settled = false;
      void result.then(() => {
        settled = true;
      });
      await vi.advanceTimersByTimeAsync(0);
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      const err = await result;
      expect(err).toBeInstanceOf(NatsAtomicPublishError);
      expect((err as Error).message).toBe(
        'Atomic publish of 3 messages to "o.a" aborted: not every message was staged within 500 ms',
      );
      expect(startBatch).not.toHaveBeenCalled();
      expect(fake.jsPublished).toEqual([]);
    } finally {
      vi.useRealTimers();
    }
  });

  it('rejects with the commit failure after sending, whatever the plugins throw', async () => {
    const boom = new Error('audit failed');
    // Plugins 0 and 1 rethrow the commit failure; plugin 2 swallows it and fails on its own.
    const rethrowing = plugin(async (m, next) => {
      if (m.atomic?.index !== 2) return next();
      await next().catch(() => {});
      throw boom;
    });
    const { client, fakeJs, logger } = setup({ plugins: [rethrowing] });
    const error = vi.spyOn(logger, 'error');
    spyBatch(fakeJs, () => Promise.reject(new Error('commit refused')));
    const err = await client.publishAtomic(three).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsAtomicPublishError);
    expect((err as Error).message).toContain('commit refused');
    // The commit's own error, rethrown by plugins 0 and 1, is not logged as a plugin failure.
    await vi.waitFor(() => expect(error).toHaveBeenCalled());
    await delay(10);
    expect(error).toHaveBeenCalledExactlyOnceWith(
      expect.stringContaining('failed on message 2 after the batch was sent'),
      boom.stack,
    );
  });

  it('publishes a single message without a batch', async () => {
    const seen: unknown[] = [];
    const { client, fake, fakeJs } = setup({
      plugins: [
        plugin(async (m, next) => {
          seen.push(m.atomic);
          await next();
        }),
      ],
    });
    const startBatch = vi.spyOn(fakeJs, 'startBatch');
    const ack = await client.publishAtomic([{ subject: 'o', data: 1 }]);
    expect(ack).toEqual({ stream: 'mock', seq: 1, count: 1, batch: undefined, duplicate: false });
    expect(startBatch).not.toHaveBeenCalled();
    expect(seen).toEqual([{ index: 0, count: 1 }]);
    expect(Object.keys(fake.jsPublished[0]?.headers ?? {})).toEqual([]);
  });

  it('wraps the failure of a single message', async () => {
    const { client, fakeJs } = setup();
    vi.spyOn(fakeJs, 'publish').mockRejectedValue(new Error('no stream'));
    const err = await client.publishAtomic([{ subject: 'o', data: 1 }]).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsAtomicPublishError);
    expect(err).toMatchObject({ subject: 'o', count: 1 });
    expect((err as Error).message).toContain('no stream');
  });

  it('does not use the CloudEvent id as Nats-Msg-Id', async () => {
    const js = createMockJetStreamClient({ cloudEvents: { source: 's' } });
    await js.publishAtomic([{ subject: 'o', data: 1, cloudEvent: { type: 't', id: 'e0' } }]);
    await js.publishAtomic([
      { subject: 'o', data: 2, cloudEvent: { type: 't', id: 'e1' } },
      { subject: 'o', data: 3, cloudEvent: { type: 't', id: 'e2' } },
    ]);
    expect(js.published.map((m) => m.cloudEvent?.id)).toEqual(['e0', 'e1', 'e2']);
    for (const m of js.published) expect(m.headers).not.toHaveProperty('Nats-Msg-Id');
  });

  it('passes expectations to the first message only', async () => {
    const { client, fakeJs } = setup();
    const startBatch = vi.spyOn(fakeJs, 'startBatch');
    await client.publishAtomic(three, { expect: { lastSequence: 0 } });
    expect(startBatch.mock.calls[0]?.[2]).toMatchObject({ expect: { lastSequence: 0 } });
  });

  it('has a fake batch that rejects a second commit, as nats.js does', async () => {
    const batch = await new FakeNatsConnection(jsonCodec()).jetstream().startBatch('o');
    await batch.commit('o');
    await expect(batch.commit('o')).rejects.toThrow('batch publisher is done');
  });

  describe('timeout', () => {
    it('defaults to 5000 ms without a JetStream client timeout', async () => {
      const { client, fakeJs } = setup();
      const { startBatch, commit } = spyBatch(fakeJs);
      await client.publishAtomic(three);
      expect(startBatch.mock.calls[0]?.[2]).toMatchObject({ timeout: 5000 });
      expect(commit.mock.calls[0]?.[2]).toMatchObject({ timeout: 5000 });
    });

    it("defaults to the JetStream client's timeout", async () => {
      const { client, fakeJs } = setup({ jsTimeout: 777 });
      const { startBatch, commit } = spyBatch(fakeJs);
      await client.publishAtomic(three);
      expect(startBatch.mock.calls[0]?.[2]).toMatchObject({ timeout: 777 });
      expect(commit.mock.calls[0]?.[2]).toMatchObject({ timeout: 777 });
    });

    it('uses opts.timeout', async () => {
      const { client, fakeJs } = setup({ jsTimeout: 777 });
      const { startBatch, commit } = spyBatch(fakeJs);
      const publish = vi.spyOn(fakeJs, 'publish');
      await client.publishAtomic(three, { timeout: 1234 });
      await client.publishAtomic([{ subject: 'o', data: 1 }], { timeout: 1234 });
      expect(startBatch.mock.calls[0]?.[2]).toMatchObject({ timeout: 1234 });
      expect(commit.mock.calls[0]?.[2]).toMatchObject({ timeout: 1234 });
      expect(publish.mock.calls[0]?.[2]).toMatchObject({ timeout: 1234 });
    });
  });
});
