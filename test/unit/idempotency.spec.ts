import { headers } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';
import type { ConnectionEntry } from '../../src/connection/connection-registry.js';
import { CLOUD_EVENT, NatsContext, NatsJetStreamContext } from '../../src/context/nats-context.js';
import {
  createIdempotency,
  IdempotencyStoreError,
  idempotencyKey,
  resolveIdempotency,
} from '../../src/idempotency/idempotency.js';
import {
  type ClaimResult,
  type IdempotencyStore,
  InMemoryIdempotencyStore,
} from '../../src/idempotency/store.js';
import { compose } from '../../src/pipeline/compose.js';
import {
  type Delivery,
  HANDLER_DEFINITION,
  type InboundMessage,
  type InternalDelivery,
  SKIPPED,
} from '../../src/pipeline/delivery.js';
import { resolveRetryPolicy } from '../../src/settlement/retry.js';
import { createSettlement } from '../../src/settlement/settlement.js';

function logger() {
  return { log: vi.fn(), error: vi.fn(), warn: vi.fn(), debug: vi.fn(), verbose: vi.fn() };
}

function hdrs(init: Record<string, string>) {
  const h = headers();
  for (const [k, v] of Object.entries(init)) h.set(k, v);
  return h;
}

function fakeStore(result: ClaimResult | Error = 'claimed') {
  return {
    claim: vi.fn(() =>
      result instanceof Error ? Promise.reject(result) : Promise.resolve(result),
    ),
    complete: vi.fn(() => Promise.resolve()),
    release: vi.fn(() => Promise.resolve()),
  } satisfies IdempotencyStore;
}

function msgWith(init: Record<string, string>): InboundMessage {
  return {
    subject: 'a.b',
    data: new Uint8Array(),
    headers: hdrs(init),
    raw: {} as never,
    attempt: 1,
  };
}

function jsDelivery(
  init: Record<string, string> = { 'Nats-Msg-Id': 'm1' },
  ack?: 'auto' | 'manual',
) {
  const raw = {
    subject: 'ord.created',
    reply: '$JS.ACK.ORD.d.1.1.1.0.0',
    data: new Uint8Array(),
    headers: hdrs(init),
    ack: vi.fn(),
    nak: vi.fn(),
    term: vi.fn(),
    working: vi.fn(),
    info: { stream: 'ORD', consumer: 'd', streamSequence: 7, deliveryCount: 1 },
  };
  const log = logger();
  const context = new NatsJetStreamContext({
    kind: 'jetstream',
    connection: 'default',
    raw: raw as never,
  });
  const handler = {
    kind: 'consume',
    stream: 'ORD',
    durable: 'd',
    className: 'H',
    methodName: 'on',
  };
  const msg: InboundMessage = {
    subject: raw.subject,
    data: raw.data,
    headers: raw.headers,
    raw: raw as never,
    attempt: 1,
  };
  const d = {
    handler,
    [HANDLER_DEFINITION]: { ...handler, options: { stream: 'ORD', durable: 'd', ack } },
    [SKIPPED]: new Set<InboundMessage>(),
    messages: [msg],
    outcomes: new Map(),
    context,
    logger: log,
  } as unknown as InternalDelivery;
  return { d, raw, log, msg, context };
}

function coreDelivery(init: Record<string, string> = { 'Nats-Msg-Id': 'm1' }) {
  const raw = { subject: 'orders.x', reply: '', data: new Uint8Array(), headers: hdrs(init) };
  const log = logger();
  const context = new NatsContext({ kind: 'core', connection: 'default', raw: raw as never });
  const handler = { kind: 'subscribe', subject: 'orders.*', className: 'H', methodName: 'on' };
  const msg: InboundMessage = {
    subject: raw.subject,
    data: raw.data,
    headers: raw.headers,
    raw: raw as never,
    attempt: 1,
  };
  const d = {
    handler,
    [HANDLER_DEFINITION]: { ...handler, options: {} },
    [SKIPPED]: new Set<InboundMessage>(),
    messages: [msg],
    outcomes: new Map(),
    context,
    logger: log,
  } as unknown as InternalDelivery;
  return { d, log, msg };
}

const cfg = (store: IdempotencyStore, extra: object = {}) => ({
  store,
  ttl: 1000,
  inProgressNakMs: 50,
  ...extra,
});

describe('idempotencyKey', () => {
  it('prefers Nats-Msg-Id over the extractor', () =>
    expect(idempotencyKey(msgWith({ 'Nats-Msg-Id': 'm1' }), () => 'x')).toBe('m1'));

  it('falls back to the extractor', () =>
    expect(idempotencyKey(msgWith({ 'x-order': '7' }), (m) => m.headers?.get('x-order'))).toBe(
      '7',
    ));

  it('falls back to the CloudEvent source|id before the extractor', () => {
    const ce = msgWith({
      'ce-specversion': '1.0',
      'ce-id': '7',
      'ce-source': 'svc',
      'ce-type': 't',
    });
    expect(idempotencyKey(ce, () => 'x')).toBe('svc|7');
  });

  it('percent-encodes the CloudEvent source so a "|" in it is unambiguous', () => {
    const ce = (source: string, id: string) =>
      msgWith({ 'ce-specversion': '1.0', 'ce-id': id, 'ce-source': source, 'ce-type': 't' });
    expect(idempotencyKey(ce('a|b', 'c'))).toBe('a%7Cb|c');
    expect(idempotencyKey(ce('a', 'b|c'))).toBe('a|b|c');
  });

  it('prefers Nats-Msg-Id over the CloudEvent key', () => {
    const ce = msgWith({
      'Nats-Msg-Id': 'm1',
      'ce-specversion': '1.0',
      'ce-id': '7',
      'ce-source': 'svc',
      'ce-type': 't',
    });
    expect(idempotencyKey(ce)).toBe('m1');
  });

  it("uses the context's cached CloudEvent attributes when given one", () => {
    const raw = {
      subject: 'a.b',
      reply: '',
      data: new TextEncoder().encode(
        JSON.stringify({ specversion: '1.0', id: 'body', source: 'svc', type: 't' }),
      ),
      headers: hdrs({ 'Content-Type': 'application/cloudevents+json' }),
    };
    const msg: InboundMessage = { ...raw, raw: raw as never, attempt: 1 };
    const ctx = new NatsContext({ kind: 'core', connection: 'default', raw: raw as never });
    expect(idempotencyKey(msg, undefined, ctx)).toBe('svc|body');
    // The context's attributes are used as they are, without reading the body again.
    ctx[CLOUD_EVENT] = {
      id: 'cached',
      source: 'svc',
      type: 't',
      specversion: '1.0',
      extensions: {},
    };
    expect(idempotencyKey(msg, () => 'x', ctx)).toBe('svc|cached');
    // A "|" in the source is percent-encoded, so (a|b, c) and (a, b|c) do not collide.
    ctx[CLOUD_EVENT] = { id: 'c', source: 'a|b', type: 't', specversion: '1.0', extensions: {} };
    expect(idempotencyKey(msg, undefined, ctx)).toBe('a%7Cb|c');
    ctx[CLOUD_EVENT] = { id: 'b|c', source: 'a', type: 't', specversion: '1.0', extensions: {} };
    expect(idempotencyKey(msg, undefined, ctx)).toBe('a|b|c');
    ctx[CLOUD_EVENT] = undefined;
    expect(idempotencyKey(msg, () => 'x', ctx)).toBe('x');
    // A Nats-Msg-Id header still wins.
    raw.headers.set('Nats-Msg-Id', 'm1');
    expect(idempotencyKey(msg, undefined, ctx)).toBe('m1');
  });

  it('is undefined without header and extractor result', () => {
    expect(idempotencyKey(msgWith({}))).toBeUndefined();
    expect(idempotencyKey(msgWith({}), () => undefined)).toBeUndefined();
    expect(idempotencyKey(msgWith({}), () => '')).toBeUndefined();
  });
});

describe('createIdempotency', () => {
  it('acks JetStream duplicates without invoking', async () => {
    const store = fakeStore('duplicate');
    const { d, raw, msg } = jsDelivery();
    const next = vi.fn(async () => {});
    await createIdempotency(cfg(store)).handle(d, next);
    expect(raw.ack).toHaveBeenCalledTimes(1);
    expect(d[SKIPPED].has(msg)).toBe(true);
    expect(next).not.toHaveBeenCalled();
    expect(store.complete).not.toHaveBeenCalled();
    expect(store.release).not.toHaveBeenCalled();
  });

  it('records skipped outcomes for duplicates and in-progress messages', async () => {
    for (const reason of ['duplicate', 'in-progress'] as const) {
      const { d, msg } = jsDelivery();
      await createIdempotency(cfg(fakeStore(reason))).handle(d, async () => {});
      expect(d.outcomes.get(msg)).toEqual({ action: 'skipped', reason });
    }
    const { d, msg } = coreDelivery();
    await createIdempotency(cfg(fakeStore('duplicate'))).handle(d, async () => {});
    expect(d.outcomes.get(msg)).toEqual({ action: 'skipped', reason: 'duplicate' });
  });

  it('naks in-progress JetStream messages', async () => {
    const store = fakeStore('in-progress');
    const { d, raw, msg } = jsDelivery();
    await createIdempotency(cfg(store)).handle(d, async () => {});
    expect(raw.nak).toHaveBeenCalledExactlyOnceWith(50);
    expect(raw.ack).not.toHaveBeenCalled();
    expect(d[SKIPPED].has(msg)).toBe(true);
  });

  it('skips duplicate and in-progress core messages without settling', async () => {
    for (const result of ['duplicate', 'in-progress'] as const) {
      const { d, msg } = coreDelivery();
      const next = vi.fn(async () => {});
      await createIdempotency(cfg(fakeStore(result))).handle(d, next);
      expect(d[SKIPPED].has(msg)).toBe(true);
      expect(next).not.toHaveBeenCalled();
    }
  });

  it('logs and survives an ack/nak that throws', async () => {
    const { d, raw, log } = jsDelivery();
    raw.ack.mockImplementation(() => {
      throw new Error('closed');
    });
    await createIdempotency(cfg(fakeStore('duplicate'))).handle(d, async () => {});
    expect(log.error).toHaveBeenCalledWith(
      expect.stringContaining('Failed to ack'),
      expect.anything(),
    );
  });

  it('completes on success', async () => {
    const store = fakeStore();
    const { d, msg } = coreDelivery();
    const next = vi.fn(async () => {});
    await createIdempotency(cfg(store)).handle(d, next);
    expect(store.claim).toHaveBeenCalledWith('subscribe:H.on|m1', 1000);
    expect(next).toHaveBeenCalledTimes(1);
    expect(store.complete).toHaveBeenCalledWith('subscribe:H.on|m1', 1000);
    expect(d[SKIPPED].has(msg)).toBe(false);
  });

  it("keys a CloudEvent by its delivery context's cached attributes", async () => {
    const store = fakeStore();
    const { d } = coreDelivery({
      'ce-specversion': '1.0',
      'ce-id': 'hdr',
      'ce-source': 'svc',
      'ce-type': 't',
    });
    // Read by an earlier stage (e.g. tracing); idempotency must not parse the message again.
    (d.context as NatsContext)[CLOUD_EVENT] = {
      id: 'cached',
      source: 'svc',
      type: 't',
      specversion: '1.0',
      extensions: {},
    };
    await createIdempotency(cfg(store)).handle(d, async () => {});
    expect(store.claim).toHaveBeenCalledWith('subscribe:H.on|svc|cached', 1000);
  });

  it('completes on success and survives complete failures', async () => {
    const store = fakeStore();
    store.complete.mockRejectedValue(new Error('store down'));
    const { d, log } = coreDelivery();
    await expect(createIdempotency(cfg(store)).handle(d, async () => {})).resolves.toBeUndefined();
    expect(log.warn).toHaveBeenCalledWith(expect.stringContaining('store down'));
  });

  it('releases on failure and rethrows', async () => {
    const store = fakeStore();
    const { d } = coreDelivery();
    const boom = new Error('boom');
    await expect(createIdempotency(cfg(store)).handle(d, () => Promise.reject(boom))).rejects.toBe(
      boom,
    );
    expect(store.release).toHaveBeenCalledWith('subscribe:H.on|m1');
    expect(store.complete).not.toHaveBeenCalled();
  });

  it('still rethrows the handler error when release fails', async () => {
    const store = fakeStore();
    store.release.mockRejectedValue(new Error('store down'));
    const { d, log } = coreDelivery();
    const boom = new Error('boom');
    await expect(createIdempotency(cfg(store)).handle(d, () => Promise.reject(boom))).rejects.toBe(
      boom,
    );
    expect(log.warn).toHaveBeenCalledWith(expect.stringContaining('store down'));
  });

  it('wraps claim failures in an IdempotencyStoreError so settlement retries', async () => {
    const err = new Error('claim failed');
    const store = fakeStore(err);
    const { d } = jsDelivery();
    const next = vi.fn(async () => {});
    const p = createIdempotency(cfg(store)).handle(d, next);
    await expect(p).rejects.toBeInstanceOf(IdempotencyStoreError);
    await expect(p).rejects.toMatchObject({
      cause: err,
      message: expect.stringContaining('claim failed'),
    });
    expect(next).not.toHaveBeenCalled();
    expect(d[SKIPPED].size).toBe(0);
  });

  it('rethrows a failing key extractor as is (not a store failure)', async () => {
    const err = new Error('bad extractor');
    const store = fakeStore();
    const { d } = coreDelivery({});
    const p = createIdempotency(
      cfg(store, {
        keyExtractor: () => {
          throw err;
        },
      }),
    ).handle(d, async () => {});
    await expect(p).rejects.toBe(err);
  });

  it('processes messages without a key and logs at debug', async () => {
    const store = fakeStore();
    const { d, log } = coreDelivery({});
    const next = vi.fn(async () => {});
    await createIdempotency(cfg(store)).handle(d, next);
    expect(next).toHaveBeenCalledTimes(1);
    expect(store.claim).not.toHaveBeenCalled();
    expect(log.debug).toHaveBeenCalled();
  });

  it('uses the key extractor when there is no Nats-Msg-Id', async () => {
    const store = fakeStore();
    const { d } = coreDelivery({ 'x-order-id': 'o-9' });
    await createIdempotency(
      cfg(store, { keyExtractor: (m: InboundMessage) => m.headers?.get('x-order-id') }),
    ).handle(d, async () => {});
    expect(store.claim).toHaveBeenCalledWith('subscribe:H.on|o-9', 1000);
  });

  it('treats a throwing extractor like a claim failure', async () => {
    const { d } = coreDelivery({});
    const err = new Error('bad extractor');
    await expect(
      createIdempotency(
        cfg(fakeStore(), {
          keyExtractor: () => {
            throw err;
          },
        }),
      ).handle(d, async () => {}),
    ).rejects.toBe(err);
  });
});

describe('handler scoping', () => {
  it("keeps handlers sharing a store from swallowing each other's messages", async () => {
    const store = new InMemoryIdempotencyStore();
    const a = coreDelivery();
    const b = coreDelivery();
    (b.d.handler as { methodName: string }).methodName = 'other';
    const nextA = vi.fn(async () => {});
    const nextB = vi.fn(async () => {});
    const mwA = createIdempotency(cfg(store));
    const mwB = createIdempotency(cfg(store));
    await mwA.handle(a.d, nextA);
    await mwB.handle(b.d, nextB);
    expect(nextA).toHaveBeenCalledTimes(1);
    expect(nextB).toHaveBeenCalledTimes(1);
    // Redelivery to the same handler is still a duplicate.
    const again = coreDelivery();
    const nextAgain = vi.fn(async () => {});
    await mwA.handle(again.d, nextAgain);
    expect(nextAgain).not.toHaveBeenCalled();
    await store.close();
  });

  it('scopes consume keys by stream/durable and leaves the connection out', async () => {
    const store = fakeStore();
    const { d } = jsDelivery();
    await createIdempotency(cfg(store)).handle(d, async () => {});
    expect(store.claim).toHaveBeenCalledWith('consume:ORD/d|m1', 1000);
  });
});

describe('ordering with settlement', () => {
  const retry = resolveRetryPolicy({ initialDelay: 50, jitter: 0 });
  const entry = { name: 'default', options: {}, logger: logger() } as unknown as ConnectionEntry;
  const run = (d: Delivery, store: IdempotencyStore, next: () => Promise<void>) =>
    compose<Delivery>(
      [
        (dd, n) => createSettlement({ entry, retry, dlq: false }).handle(dd, n),
        (dd, n) => createIdempotency(cfg(store)).handle(dd, n),
      ],
      next,
    )(d);

  it('does not ack again after idempotency acked a duplicate', async () => {
    const { d, raw } = jsDelivery();
    const next = vi.fn(async () => {});
    await run(d, fakeStore('duplicate'), next);
    expect(raw.ack).toHaveBeenCalledTimes(1);
    expect(raw.nak).not.toHaveBeenCalled();
    expect(next).not.toHaveBeenCalled();
  });

  it("does not ack an in-progress message that idempotency nak'd", async () => {
    const { d, raw } = jsDelivery();
    await run(d, fakeStore('in-progress'), async () => {});
    expect(raw.nak).toHaveBeenCalledExactlyOnceWith(50);
    expect(raw.ack).not.toHaveBeenCalled();
  });

  it('acks once after a claimed message succeeds and retries one that fails', async () => {
    const ok = jsDelivery();
    await run(ok.d, fakeStore(), async () => {});
    expect(ok.raw.ack).toHaveBeenCalledTimes(1);

    const bad = jsDelivery();
    const store = fakeStore();
    await run(bad.d, store, () => Promise.reject(new Error('boom')));
    expect(store.release).toHaveBeenCalledWith('consume:ORD/d|m1');
    expect(bad.raw.nak).toHaveBeenCalledExactlyOnceWith(50);
    expect(bad.raw.ack).not.toHaveBeenCalled();
  });

  it('lets settlement retry a failed claim', async () => {
    const { d, raw } = jsDelivery();
    await run(d, fakeStore(new Error('store down')), async () => {});
    expect(raw.nak).toHaveBeenCalledExactlyOnceWith(50);
    expect(raw.ack).not.toHaveBeenCalled();
    expect(raw.term).not.toHaveBeenCalled();
  });
});

describe('resolveIdempotency', () => {
  const retry = resolveRetryPolicy({ initialDelay: 70, jitter: 0 });

  function entryWith(store: IdempotencyStore, ttl?: number) {
    return {
      options: { idempotency: { ttl } },
      idempotencyStore: () => store,
    } as unknown as ConnectionEntry;
  }

  it('is undefined unless enabled', () => {
    const entry = entryWith(fakeStore());
    expect(resolveIdempotency(entry, undefined, retry)).toBeUndefined();
    expect(resolveIdempotency(entry, false, retry)).toBeUndefined();
    expect(resolveIdempotency(entry, true, retry)).toBeDefined();
  });

  it('takes ttl from the handler, then the module, then 300_000; nak delay from retry', async () => {
    const store = fakeStore();
    const run = async (entry: ConnectionEntry, opt: boolean | { ttl: number }, r: typeof retry) => {
      store.claim.mockClear();
      store.complete.mockClear();
      const { d } = coreDelivery();
      await resolveIdempotency(entry, opt, r)?.handle(d, async () => {});
      return store.complete.mock.calls[0];
    };
    expect(await run(entryWith(store, 5), { ttl: 9 }, retry)).toEqual(['subscribe:H.on|m1', 9]);
    expect(await run(entryWith(store, 5), true, retry)).toEqual(['subscribe:H.on|m1', 5]);
    expect(await run(entryWith(store), true, retry)).toEqual(['subscribe:H.on|m1', 300_000]);

    const { d, raw } = jsDelivery();
    store.claim.mockResolvedValueOnce('in-progress');
    await resolveIdempotency(entryWith(store), true, retry)?.handle(d, async () => {});
    expect(raw.nak).toHaveBeenCalledWith(70);

    const off = jsDelivery();
    store.claim.mockResolvedValueOnce('in-progress');
    await resolveIdempotency(entryWith(store), true, false)?.handle(off.d, async () => {});
    expect(off.raw.nak).toHaveBeenCalledWith(1000);
  });
});
