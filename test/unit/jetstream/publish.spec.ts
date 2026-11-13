import type { MsgHdrs } from '@nats-io/transport-node';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { jsonCodec } from '../../../src/codec/codec.js';
import type { ConnectionEntry } from '../../../src/connection/connection-registry.js';
import { NatsModuleError } from '../../../src/errors/errors.js';
import {
  DEFAULT_JETSTREAM_TIMEOUT,
  jetStreamTimeout,
  publishOutgoing,
} from '../../../src/jetstream/publish.js';
import { createOutgoingMessage } from '../../../src/pipeline/outbound.js';
import type { NatsPlugin, Next, OutgoingMessage } from '../../../src/pipeline/types.js';
import { mockLogger } from '../../support/logger.js';

const ACK = { stream: 'ORDERS', seq: 1, duplicate: false };

function fakeEntry(
  opts: {
    plugins?: NatsPlugin[];
    timeout?: number;
    /** `false`: a JetStream client without `getOptions` (the fake one of /testing). */
    getOptions?: false;
    publish?: () => Promise<unknown>;
  } = {},
) {
  const publish = vi.fn(
    (_subject: string, _payload?: Uint8Array, _opts?: unknown): Promise<unknown> =>
      opts.publish ? opts.publish() : Promise.resolve(ACK),
  );
  const js = {
    publish,
    ...(opts.getOptions === false ? {} : { getOptions: () => ({ timeout: opts.timeout }) }),
  };
  const entry = {
    name: 'default',
    options: { connection: {}, plugins: opts.plugins },
    logger: mockLogger(),
    js: () => js,
  } as unknown as ConnectionEntry;
  return { entry, publish };
}

const plugin = (handle: (m: OutgoingMessage, next: Next) => Promise<void>): NatsPlugin[] => [
  { name: 'p', outbound: { handle } },
];

const message = (entry: ConnectionEntry, headers?: Record<string, string>) =>
  createOutgoingMessage(entry, 'jetstream', 'orders.created', { id: 1 }, { headers });

describe('jetStreamTimeout', () => {
  it("is the JetStream client's timeout", () => {
    expect(jetStreamTimeout(fakeEntry({ timeout: 2000 }).entry)).toBe(2000);
  });

  it('defaults to 5000 ms without one', () => {
    expect(DEFAULT_JETSTREAM_TIMEOUT).toBe(5000);
    expect(jetStreamTimeout(fakeEntry().entry)).toBe(5000);
    expect(jetStreamTimeout(fakeEntry({ timeout: 0 }).entry)).toBe(5000);
    expect(jetStreamTimeout(fakeEntry({ getOptions: false }).entry)).toBe(5000);
  });

  it('is capped at the longest timer delay', () => {
    expect(jetStreamTimeout(fakeEntry({ timeout: 2 ** 40 }).entry)).toBe(2 ** 31 - 1);
  });
});

describe('publishOutgoing', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('publishes the encoded message and resolves with its PubAck', async () => {
    const { entry, publish } = fakeEntry();
    await expect(publishOutgoing(entry, message(entry), {})).resolves.toBe(ACK);
    expect(publish).toHaveBeenCalledExactlyOnceWith(
      'orders.created',
      jsonCodec().encode({ id: 1 }),
      {},
    );
  });

  it('passes the headers, expectations and timeout on', async () => {
    const { entry, publish } = fakeEntry();
    const expect_ = { lastSequence: 3 };
    await publishOutgoing(entry, message(entry, { 'x-tenant': 't1' }), {
      expect: expect_,
      timeout: 1500,
    });
    const opts = publish.mock.calls[0]?.[2] as unknown as {
      headers: MsgHdrs;
      expect: unknown;
      timeout: number;
    };
    expect(opts.headers.get('x-tenant')).toBe('t1');
    expect(opts.expect).toBe(expect_);
    expect(opts.timeout).toBe(1500);
  });

  it('maps a publish failure through wrap, and passes it unchanged without', async () => {
    const failure = new Error('timeout');
    const { entry } = fakeEntry({ publish: () => Promise.reject(failure) });
    await expect(publishOutgoing(entry, message(entry), {})).rejects.toBe(failure);
    await expect(
      publishOutgoing(
        entry,
        message(entry),
        {},
        { wrap: (err) => new Error('wrapped', { cause: err }) },
      ),
    ).rejects.toMatchObject({ message: 'wrapped', cause: failure });
  });

  it('publishes what the outbound plugins pass on', async () => {
    const { entry, publish } = fakeEntry({
      plugins: plugin(async (m, next) => {
        m.subject = 'orders.rerouted';
        await next();
      }),
    });
    await expect(publishOutgoing(entry, message(entry), {})).resolves.toBe(ACK);
    expect(publish.mock.calls[0]?.[0]).toBe('orders.rerouted');
  });

  it('rejects, publishing nothing, when a plugin returns without passing the message on', async () => {
    const { entry, publish } = fakeEntry({ plugins: plugin(async () => {}) });
    const p = publishOutgoing(entry, message(entry), {});
    await expect(p).rejects.toBeInstanceOf(NatsModuleError);
    await expect(p).rejects.toThrow(
      'JetStream publish to "orders.created" produced no acknowledgement',
    );
    await expect(
      publishOutgoing(entry, message(entry), {}, { what: 'Dead letter to "dlq.x"' }),
    ).rejects.toThrow('Dead letter to "dlq.x" produced no acknowledgement');
    const own = new Error('dropped');
    await expect(publishOutgoing(entry, message(entry), {}, { skipped: () => own })).rejects.toBe(
      own,
    );
    expect(publish).not.toHaveBeenCalled();
  });

  it('gives the plugins the publish timeout to pass the message on', async () => {
    vi.useFakeTimers();
    const { entry, publish } = fakeEntry({
      timeout: 60_000,
      plugins: plugin(() => new Promise<void>(() => {})),
    });
    const p = publishOutgoing(entry, message(entry), { timeout: 100 });
    const assertion = expect(p).rejects.toThrow(/did not pass the message on within 100 ms/);
    await vi.advanceTimersByTimeAsync(100);
    await assertion;
    const expired = new Error('too slow');
    const q = publishOutgoing(entry, message(entry), {}, { expired: () => expired });
    const ownAssertion = expect(q).rejects.toBe(expired);
    // Without its own timeout, the JetStream client's applies.
    await vi.advanceTimersByTimeAsync(60_000);
    await ownAssertion;
    expect(publish).not.toHaveBeenCalled();
  });
});
