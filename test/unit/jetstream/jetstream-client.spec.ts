import type { MsgHdrs } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';
import type { ConnectionEntry } from '../../../src/connection/connection-registry.js';
import { NatsConfigError, NatsModuleError } from '../../../src/errors/errors.js';
import { JetStreamClient } from '../../../src/jetstream/jetstream-client.js';
import { tick } from '../../support/async.js';
import { mockLogger } from '../../support/logger.js';

const ack = (seq: number) => ({ stream: 'ORDERS', seq, duplicate: false });

function setup(publishImpl?: (subject: string) => Promise<unknown>) {
  let seq = 0;
  const publish = vi.fn(
    (subject: string, _payload?: Uint8Array, _opts?: unknown): Promise<unknown> =>
      publishImpl ? publishImpl(subject) : Promise.resolve(ack(++seq)),
  );
  const hooks: Array<() => Promise<void>> = [];
  const off = vi.fn();
  const entry = {
    name: 'default',
    options: { connection: {}, cloudEvents: { source: 'tests' } },
    logger: mockLogger(),
    js: () => ({ publish }),
    onClose: vi.fn((hook: () => Promise<void>) => {
      hooks.push(hook);
      return off;
    }),
  } as unknown as ConnectionEntry;
  return { client: new JetStreamClient(entry), publish, hooks, off };
}

/** The headers of the `n`th publish, `undefined` when it had none. */
const headersOf = (publish: ReturnType<typeof setup>['publish'], n = 0) =>
  (publish.mock.calls[n]?.[2] as { headers?: MsgHdrs } | undefined)?.headers;

describe('JetStreamClient.publish Nats-Msg-Id', () => {
  it('sets it from msgId, replacing a header of any case', async () => {
    const { client, publish } = setup();
    await client.publish('orders.created', {}, { msgId: 'new', headers: { 'nats-msg-id': 'old' } });
    const h = headersOf(publish);
    expect(h?.keys()).toEqual(['Nats-Msg-Id']);
    expect(h?.get('Nats-Msg-Id')).toBe('new');
  });

  it("re-sets a caller's header of another case under the canonical name", async () => {
    const { client, publish } = setup();
    await client.publish('orders.created', {}, { headers: { 'NATS-MSG-ID': 'h1' } });
    const h = headersOf(publish);
    expect(h?.keys()).toEqual(['Nats-Msg-Id']);
    expect(h?.get('Nats-Msg-Id')).toBe('h1');
  });

  it("is the CloudEvent's id when neither msgId nor a header sets it", async () => {
    const { client, publish } = setup();
    await client.publish(
      'orders.created',
      {},
      { cloudEvent: { type: 'order.created', id: 'ce-1' } },
    );
    expect(headersOf(publish)?.get('Nats-Msg-Id')).toBe('ce-1');
  });

  it("prefers msgId, then the caller's header, over the CloudEvent's id", async () => {
    const { client, publish } = setup();
    const cloudEvent = { type: 'order.created', id: 'ce-1' };
    await client.publish('a', {}, { cloudEvent, msgId: 'm' });
    await client.publish('a', {}, { cloudEvent, headers: { 'Nats-Msg-Id': 'h' } });
    expect(headersOf(publish, 0)?.get('Nats-Msg-Id')).toBe('m');
    expect(headersOf(publish, 1)?.get('Nats-Msg-Id')).toBe('h');
  });

  it('sends no headers at all otherwise', async () => {
    const { client, publish } = setup();
    await client.publish('orders.created', { id: 1 });
    expect(headersOf(publish)).toBeUndefined();
  });
});

describe('JetStreamClient.publishMany', () => {
  it('returns one result per item, in input order, a failure being its result', async () => {
    const { client } = setup(async (subject) => {
      if (subject === 'bad') throw new Error('nope');
      if (subject === 'odd') throw 'plain';
      return ack(subject.length);
    });
    const results = await client.publishMany([
      { subject: 'a' },
      { subject: 'bad' },
      { subject: 'odd' },
      { subject: 'bbb' },
    ]);
    expect(results.map((r) => (r.ok ? r.ack.seq : r.error.message))).toEqual([
      1,
      'nope',
      'plain',
      3,
    ]);
    expect(results[2]).toMatchObject({ ok: false, error: expect.any(Error) });
  });

  it('keeps at most `window` publishes awaiting their PubAck (default 256)', async () => {
    let inFlight = 0;
    let most = 0;
    const { client, publish } = setup(async () => {
      most = Math.max(most, ++inFlight);
      await tick();
      inFlight--;
      return ack(1);
    });
    const items = (n: number) => Array.from({ length: n }, (_, i) => ({ subject: `s.${i}` }));
    await client.publishMany(items(5), { window: 2 });
    expect(publish).toHaveBeenCalledTimes(5);
    expect(most).toBe(2);
    most = 0;
    await client.publishMany(items(300));
    expect(most).toBe(256);
  });

  it('publishes nothing for no items', async () => {
    const { client, publish } = setup();
    await expect(client.publishMany([])).resolves.toEqual([]);
    expect(publish).not.toHaveBeenCalled();
  });

  it.each([0, -1, Number.NaN])('rejects a window of %d', async (window) => {
    const { client, publish } = setup();
    await expect(client.publishMany([{ subject: 'a' }], { window })).rejects.toThrow(
      NatsConfigError,
    );
    expect(publish).not.toHaveBeenCalled();
  });
});

describe('JetStreamClient.batcher', () => {
  it('is flushed and closed when the connection closes', async () => {
    const { client, publish, hooks } = setup();
    const batcher = client.batcher({ maxWait: 60_000 });
    const added = batcher.add('orders.created', { id: 1 });
    expect(publish).not.toHaveBeenCalled();
    expect(hooks).toHaveLength(1);
    await hooks[0]?.();
    await expect(added).resolves.toEqual(ack(1));
    await expect(batcher.add('orders.created', { id: 2 })).rejects.toThrow(NatsModuleError);
  });

  it('unregisters its close hook once closed', async () => {
    const { client, off } = setup();
    const batcher = client.batcher();
    expect(off).not.toHaveBeenCalled();
    await batcher.close();
    expect(off).toHaveBeenCalledOnce();
  });
});
