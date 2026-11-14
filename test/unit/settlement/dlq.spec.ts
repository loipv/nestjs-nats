import type { JsMsg } from '@nats-io/jetstream';
import { headers, type MsgHdrs } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';
import { type Codec, jsonCodec } from '../../../src/codec/codec.js';
import type { ConnectionEntry } from '../../../src/connection/connection-registry.js';
import { NatsModuleError } from '../../../src/errors/errors.js';
import type { HandlerInfo, InboundMessage } from '../../../src/pipeline/delivery.js';
import type { NatsPlugin, OutgoingMessage } from '../../../src/pipeline/types.js';
import {
  buildDlqHeaders,
  DLQ_HEADERS,
  type DlqPolicy,
  publishToDlq,
  resolveDlqPolicy,
} from '../../../src/settlement/dlq.js';
import { mockLogger } from '../../support/logger.js';

const msg = (subject: string): InboundMessage =>
  ({ subject, data: new Uint8Array(), raw: {} }) as InboundMessage;

const coreMsg = (): InboundMessage => msg('orders.created');

function jsMsg(o: {
  stream: string;
  consumer: string;
  streamSequence: number;
  headers?: Record<string, string>;
}): InboundMessage {
  const h = headers();
  for (const [k, v] of Object.entries(o.headers ?? {})) h.append(k, v);
  return {
    subject: 'ord.x',
    data: new Uint8Array(),
    headers: h,
    raw: {
      info: { stream: o.stream, consumer: o.consumer, streamSequence: o.streamSequence },
    } as unknown as JsMsg,
    attempt: 1,
  };
}

describe('resolveDlqPolicy', () => {
  it('is off by default', () => expect(resolveDlqPolicy('consume')).toBe(false));
  it('defaults an ordered consumer to a jetstream dead letter under its class and method', () => {
    const o = resolveDlqPolicy('ordered', true) as DlqPolicy;
    expect(o.mode).toBe('jetstream');
    expect(
      o.subject(
        { kind: 'ordered', stream: 'ORD', className: 'Audit', methodName: 'on' } as HandlerInfo,
        msg('ord.x'),
      ),
    ).toBe('dlq.ORD.ordered.Audit.on');
  });
  it('defaults subject and mode per kind', () => {
    const c = resolveDlqPolicy('consume', true) as DlqPolicy;
    expect(c.mode).toBe('jetstream');
    expect(
      c.subject({ kind: 'consume', stream: 'ORD', durable: 'bill' } as HandlerInfo, msg('ord.x')),
    ).toBe('dlq.ORD.bill');
    const s = resolveDlqPolicy('subscribe', {}) as DlqPolicy;
    expect(s.mode).toBe('core');
    expect(s.subject({ kind: 'subscribe' } as HandlerInfo, msg('orders.created'))).toBe(
      'dlq.orders.created',
    );
  });
  it('handler false wins over module true', () =>
    expect(resolveDlqPolicy('consume', true, false)).toBe(false));
  it('handler true enables when module is unset or false', () => {
    expect(resolveDlqPolicy('consume', undefined, true)).not.toBe(false);
    expect(resolveDlqPolicy('consume', false, true)).not.toBe(false);
  });
  it('module false disables', () => expect(resolveDlqPolicy('consume', false)).toBe(false));
  it('handler object overrides module object', () => {
    const p = resolveDlqPolicy(
      'consume',
      { subject: 'mod.dlq', mode: 'core' },
      { mode: 'jetstream' },
    ) as DlqPolicy;
    expect(p.mode).toBe('jetstream');
    expect(p.subject({} as HandlerInfo, msg('a'))).toBe('mod.dlq');
  });
  it('supports a static and a function subject', () => {
    const a = resolveDlqPolicy('subscribe', { subject: 'fixed' }) as DlqPolicy;
    expect(a.subject({} as HandlerInfo, msg('a'))).toBe('fixed');
    const b = resolveDlqPolicy('subscribe', {
      subject: (_i, m) => `x.${m.subject}`,
    }) as DlqPolicy;
    expect(b.subject({} as HandlerInfo, msg('a'))).toBe('x.a');
  });
});

describe('buildDlqHeaders', () => {
  it('builds headers', () => {
    const h = buildDlqHeaders(
      jsMsg({ stream: 'ORD', consumer: 'bill', streamSequence: 7, headers: { a: '1' } }),
      new Error('boom\r\nline'),
      6,
      new Date('2026-10-09T00:00:00Z'),
    );
    expect(h.get('a')).toBe('1');
    expect(h.get(DLQ_HEADERS.subject)).toBe('ord.x');
    expect(h.get('Nats-DLQ-Stream')).toBe('ORD');
    expect(h.get('Nats-DLQ-Consumer')).toBe('bill');
    expect(h.get('Nats-DLQ-Seq')).toBe('7');
    expect(h.get('Nats-DLQ-Deliveries')).toBe('6');
    expect(h.get('Nats-DLQ-Error')).toBe('boom line');
    expect(h.get('Nats-DLQ-Timestamp')).toBe('2026-10-09T00:00:00.000Z');
  });
  it('truncates the error to 1024 chars', () =>
    expect(
      buildDlqHeaders(coreMsg(), new Error('x'.repeat(5000)), 1, new Date()).get('Nats-DLQ-Error'),
    ).toHaveLength(1024));
  it('omits JetStream headers for core messages', () => {
    const h = buildDlqHeaders(coreMsg(), new Error('x'), 1, new Date());
    expect(h.has('Nats-DLQ-Stream')).toBe(false);
    expect(h.has('Nats-DLQ-Consumer')).toBe(false);
    expect(h.has('Nats-DLQ-Seq')).toBe(false);
    expect(h.get('Nats-DLQ-Subject')).toBe('orders.created');
  });
  it('stringifies non-Error values', () =>
    expect(buildDlqHeaders(coreMsg(), 'oops', 1, new Date()).get('Nats-DLQ-Error')).toBe('oops'));
  it('strips every original Nats-* header, keeping the other user headers', () => {
    const h = buildDlqHeaders(
      jsMsg({
        stream: 'ORD',
        consumer: 'bill',
        streamSequence: 7,
        headers: {
          'Nats-Expected-Stream': 'ORD',
          'nats-expected-last-sequence': '6',
          'NATS-EXPECTED-LAST-MSG-ID': 'm0',
          'Nats-Rollup': 'sub',
          'nats-ttl': '1m',
          'Nats-Incr': '1',
          'Nats-Schedule': '@at 2026-01-01T00:00:00Z',
          'Nats-Schedule-Target': 'x',
          'Nats-Batch-Id': 'b1',
          'Nats-DLQ-Error': 'stale',
          'x-keep': 'yes',
          'Content-Type': 'application/json',
        },
      }),
      new Error('x'),
      1,
      new Date(),
    );
    const original = h
      .keys()
      .map((k) => k.toLowerCase())
      .filter((k) => k.startsWith('nats-') && !k.startsWith('nats-dlq-') && k !== 'nats-msg-id');
    expect(original).toEqual([]);
    expect(h.get('Nats-DLQ-Error')).toBe('x');
    expect(h.values('Nats-DLQ-Error')).toEqual(['x']);
    expect(h.get('x-keep')).toBe('yes');
    expect(h.get('Content-Type')).toBe('application/json');
  });

  it('gives a JetStream dead letter its own Nats-Msg-Id and keeps the original as Nats-DLQ-Msg-Id', () => {
    const h = buildDlqHeaders(
      jsMsg({
        stream: 'ORD',
        consumer: 'bill',
        streamSequence: 7,
        headers: { 'nats-msg-id': 'm1' },
      }),
      new Error('x'),
      1,
      new Date(),
    );
    expect(h.values('Nats-Msg-Id')).toEqual(['dlq:ORD:bill:7']);
    expect(h.keys().filter((k) => k.toLowerCase() === 'nats-msg-id')).toEqual(['Nats-Msg-Id']);
    expect(h.get(DLQ_HEADERS.msgId)).toBe('m1');
    expect(DLQ_HEADERS.msgId).toBe('Nats-DLQ-Msg-Id');
  });

  it('uses the given consumer name instead of the delivery info one', () => {
    const h = buildDlqHeaders(
      jsMsg({ stream: 'ORDERS', consumer: 'ordered_abc123', streamSequence: 42 }),
      new Error('x'),
      2,
      new Date(),
      'ordered:Proj.on',
    );
    expect(h.get('Nats-DLQ-Consumer')).toBe('ordered:Proj.on');
    expect(h.get('Nats-Msg-Id')).toBe('dlq:ORDERS:ordered:Proj.on:42');
    expect(h.get('Nats-DLQ-Seq')).toBe('42');
    expect(h.get('Nats-DLQ-Deliveries')).toBe('2');
  });

  it('sets no Nats-DLQ-Msg-Id when the original has no Nats-Msg-Id', () => {
    const h = buildDlqHeaders(
      jsMsg({ stream: 'ORD', consumer: 'bill', streamSequence: 7 }),
      new Error('x'),
      1,
      new Date(),
    );
    expect(h.get('Nats-Msg-Id')).toBe('dlq:ORD:bill:7');
    expect(h.has(DLQ_HEADERS.msgId)).toBe(false);
  });

  it('sets no Nats-Msg-Id on a dead letter of a core message', () => {
    const m = coreMsg();
    const hs = headers();
    hs.set('Nats-Msg-Id', 'c1');
    const h = buildDlqHeaders({ ...m, headers: hs }, new Error('x'), 1, new Date());
    expect(h.has('Nats-Msg-Id')).toBe(false);
    expect(h.get(DLQ_HEADERS.msgId)).toBe('c1');
  });
});

const consumeDlq = resolveDlqPolicy('consume', true) as DlqPolicy;
const coreDlq = resolveDlqPolicy('subscribe', true) as DlqPolicy;

function fakeEntry(
  opts: { jsPublish?: () => Promise<unknown>; plugins?: NatsPlugin[]; codec?: Codec } = {},
) {
  const jsPublish = vi.fn(opts.jsPublish ?? (() => Promise.resolve({ stream: 'DLQ', seq: 1 })));
  const ncPublish = vi.fn();
  const entry = {
    name: 'default',
    options: { plugins: opts.plugins, codec: opts.codec },
    logger: mockLogger(),
    nc: { publish: ncPublish },
    js: () => ({ publish: jsPublish }),
  } as unknown as ConnectionEntry;
  return { entry, jsPublish, ncPublish };
}

/** A JetStream message of stream ORD, consumer d, stream sequence 7, failed on `attempt`. */
function failedJsMsg(attempt: number): InboundMessage {
  const raw = {
    subject: 'ord.created',
    reply: '$JS.ACK.ORD.d.1.1.1.0.0',
    data: new TextEncoder().encode('{"id":1}'),
    headers: undefined,
    info: { stream: 'ORD', consumer: 'd', streamSequence: 7, deliveryCount: attempt },
  };
  return { subject: raw.subject, data: raw.data, headers: undefined, raw: raw as never, attempt };
}

describe('publishToDlq', () => {
  const info = { kind: 'consume', stream: 'ORD', durable: 'd' } as never;

  it('maps "no responders" to a NatsModuleError naming the subject', async () => {
    const { NoRespondersError, RequestError } = await import('@nats-io/transport-node');
    const cause = new RequestError('no responders', {
      cause: new NoRespondersError('nowhere.x'),
    });
    const { entry } = fakeEntry({
      jsPublish: () => Promise.reject(new Error('jetstream is not enabled', { cause })),
    });
    const msg = failedJsMsg(1);
    const dlq: DlqPolicy = { subject: () => 'nowhere.x', mode: 'jetstream' };
    const p = publishToDlq(entry, dlq, info, msg, new Error('x'), 1);
    await expect(p).rejects.toBeInstanceOf(NatsModuleError);
    await expect(p).rejects.toThrow(/nowhere\.x.*no stream captures this subject/);
  });

  it('passes other publish errors through', async () => {
    const err = new Error('timeout');
    const { entry } = fakeEntry({ jsPublish: () => Promise.reject(err) });
    const msg = failedJsMsg(1);
    await expect(publishToDlq(entry, consumeDlq, info, msg, new Error('x'), 1)).rejects.toBe(err);
  });

  it('keeps the original headers and body', async () => {
    const { entry, jsPublish } = fakeEntry();
    const msg = failedJsMsg(2);
    const h = headers();
    h.set('x-trace', 'abc');
    msg.headers = h;
    await publishToDlq(entry, consumeDlq, info, msg, new Error('x'), 2);
    const call = jsPublish.mock.calls[0] as unknown as [unknown, unknown, { headers: MsgHdrs }];
    const opts = call[2];
    expect(opts.headers.get('x-trace')).toBe('abc');
    expect(opts.headers.get('Nats-DLQ-Stream')).toBe('ORD');
  });

  describe('through the outbound plugins', () => {
    const plugin = (fn: (m: OutgoingMessage, next: () => Promise<void>) => Promise<void>) =>
      [{ name: 'p', outbound: { handle: fn } }] as unknown as NatsPlugin[];
    const handlerInfo = { kind: 'consume', stream: 'ORD', durable: 'd' } as never;
    const coreInfo = { kind: 'subscribe', subject: 'ord.created' } as never;

    it('hands the dead letter to the outbound plugins, marked', async () => {
      const seen: OutgoingMessage[] = [];
      const codec = jsonCodec();
      const { entry } = fakeEntry({
        codec,
        plugins: plugin(async (m, next) => {
          seen.push(m);
          await next();
        }),
      });
      const msg = failedJsMsg(1);
      const error = new Error('x');
      await publishToDlq(entry, consumeDlq, handlerInfo, msg, error, 1);
      expect(seen).toHaveLength(1);
      const m = seen[0] as OutgoingMessage;
      expect(m.kind).toBe('jetstream');
      expect(m.codec).toBe(codec);
      expect(m.subject).toBe('dlq.ORD.d');
      expect(m.data).toEqual(msg.data);
      expect(m.headers.get('Nats-DLQ-Stream')).toBe('ORD');
      expect(m.deadLetter).toEqual({ subject: msg.subject, handler: handlerInfo, error });
    });

    it('marks a core dead letter as a publish', async () => {
      const seen: OutgoingMessage[] = [];
      const { entry } = fakeEntry({
        plugins: plugin(async (m, next) => {
          seen.push(m);
          await next();
        }),
      });
      const msg = failedJsMsg(1);
      await publishToDlq(entry, coreDlq, coreInfo, msg, new Error('x'), 1);
      expect(seen[0]?.kind).toBe('publish');
      expect(seen[0]?.deadLetter?.handler).toBe(coreInfo);
    });

    it('publishes what the plugins pass on', async () => {
      const plugins = plugin(async (m, next) => {
        m.headers.set('x-tenant', 't1');
        await next();
      });
      const js = fakeEntry({ plugins });
      const msg = failedJsMsg(1);
      await expect(
        publishToDlq(js.entry, consumeDlq, handlerInfo, msg, new Error('x'), 1),
      ).resolves.toBe('sent');
      const jsOpts = (
        js.jsPublish.mock.calls[0] as unknown as [unknown, unknown, { headers: MsgHdrs }]
      )[2];
      expect(jsOpts.headers.get('x-tenant')).toBe('t1');
      const core = fakeEntry({ plugins });
      await expect(
        publishToDlq(core.entry, coreDlq, coreInfo, msg, new Error('x'), 1),
      ).resolves.toBe('sent');
      const ncOpts = (
        core.ncPublish.mock.calls[0] as unknown as [unknown, unknown, { headers: MsgHdrs }]
      )[2];
      expect(ncOpts.headers.get('x-tenant')).toBe('t1');
    });

    it('encodes data a plugin replaced', async () => {
      const { entry, jsPublish } = fakeEntry({
        plugins: plugin(async (m, next) => {
          m.data = { a: 1 };
          await next();
        }),
      });
      const msg = failedJsMsg(1);
      await publishToDlq(entry, consumeDlq, handlerInfo, msg, new Error('x'), 1);
      const body = (jsPublish.mock.calls[0] as unknown as [unknown, Uint8Array])[1];
      expect(jsonCodec().decode(body)).toEqual({ a: 1 });
    });

    it('still publishes when a plugin deletes Nats-Msg-Id', async () => {
      const { entry, jsPublish } = fakeEntry({
        plugins: plugin(async (m, next) => {
          m.headers.delete('Nats-Msg-Id');
          await next();
        }),
      });
      const msg = failedJsMsg(1);
      await expect(
        publishToDlq(entry, consumeDlq, handlerInfo, msg, new Error('x'), 1),
      ).resolves.toBe('sent');
      const opts = (
        jsPublish.mock.calls[0] as unknown as [unknown, unknown, { headers: MsgHdrs }]
      )[2];
      expect(opts.headers.has('Nats-Msg-Id')).toBe(false);
    });

    it('reports a dead letter a plugin dropped', async () => {
      const js = fakeEntry({ plugins: plugin(async () => {}) });
      const msg = failedJsMsg(1);
      await expect(
        publishToDlq(js.entry, consumeDlq, handlerInfo, msg, new Error('x'), 1),
      ).resolves.toBe('dropped');
      expect(js.jsPublish).not.toHaveBeenCalled();
      const core = fakeEntry({ plugins: plugin(async () => {}) });
      await expect(
        publishToDlq(core.entry, coreDlq, coreInfo, msg, new Error('x'), 1),
      ).resolves.toBe('dropped');
      expect(core.ncPublish).not.toHaveBeenCalled();
    });

    it('refuses a next() called after the plugins returned, with a fresh error naming the subject', async () => {
      const nexts: Array<() => Promise<void>> = [];
      const { entry, jsPublish } = fakeEntry({
        plugins: plugin(async (_m, next) => {
          nexts.push(next);
        }),
      });
      const msg = failedJsMsg(1);
      await publishToDlq(entry, consumeDlq, handlerInfo, msg, new Error('x'), 1);
      await publishToDlq(entry, consumeDlq, handlerInfo, msg, new Error('x'), 1);
      const first = await (nexts[0] as () => Promise<void>)().catch((e: unknown) => e);
      const second = await (nexts[1] as () => Promise<void>)().catch((e: unknown) => e);
      expect(first).toBeInstanceOf(NatsModuleError);
      expect((first as Error).message).toBe(
        'Dead letter to "dlq.ORD.d": next() was called after the outbound plugins returned',
      );
      expect(second).not.toBe(first);
      expect(jsPublish).not.toHaveBeenCalled();
    });

    it('rejects with the error of a plugin that throws before next()', async () => {
      const boom = new Error('boom');
      const { entry, jsPublish } = fakeEntry({
        plugins: plugin(async () => {
          throw boom;
        }),
      });
      const msg = failedJsMsg(1);
      await expect(
        publishToDlq(entry, consumeDlq, handlerInfo, msg, new Error('x'), 1),
      ).rejects.toBe(boom);
      expect(jsPublish).not.toHaveBeenCalled();
    });

    it('rejects when the plugins do not pass the dead letter on in time', async () => {
      vi.useFakeTimers();
      try {
        const { entry, jsPublish } = fakeEntry({
          plugins: plugin(() => new Promise<void>(() => {})),
        });
        const msg = failedJsMsg(1);
        const p = publishToDlq(entry, consumeDlq, handlerInfo, msg, new Error('x'), 1);
        const assertion = expect(p).rejects.toThrow(/did not pass the message on within/);
        await vi.advanceTimersByTimeAsync(5000);
        await assertion;
        await expect(p).rejects.toBeInstanceOf(NatsModuleError);
        expect(jsPublish).not.toHaveBeenCalled();
      } finally {
        vi.useRealTimers();
      }
    });

    describe('in core mode', () => {
      const published = (ncPublish: { mock: { calls: unknown[][] } }) =>
        ncPublish.mock.calls[0] as unknown as [string, Uint8Array, { headers: MsgHdrs }];

      it('rejects with the error of a plugin that throws before next()', async () => {
        const boom = new Error('boom');
        const { entry, ncPublish } = fakeEntry({
          plugins: plugin(async () => {
            throw boom;
          }),
        });
        const msg = failedJsMsg(1);
        await expect(publishToDlq(entry, coreDlq, coreInfo, msg, new Error('x'), 1)).rejects.toBe(
          boom,
        );
        expect(ncPublish).not.toHaveBeenCalled();
      });

      it('rejects when the plugins do not pass the dead letter on in time', async () => {
        vi.useFakeTimers();
        try {
          const { entry, ncPublish } = fakeEntry({
            plugins: plugin(() => new Promise<void>(() => {})),
          });
          const msg = failedJsMsg(1);
          const p = publishToDlq(entry, coreDlq, coreInfo, msg, new Error('x'), 1);
          const assertion = expect(p).rejects.toThrow(/did not pass the message on within/);
          await vi.advanceTimersByTimeAsync(5000);
          await assertion;
          expect(ncPublish).not.toHaveBeenCalled();
        } finally {
          vi.useRealTimers();
        }
      });

      it('encodes data a plugin replaced', async () => {
        const { entry, ncPublish } = fakeEntry({
          plugins: plugin(async (m, next) => {
            m.data = { a: 1 };
            await next();
          }),
        });
        const msg = failedJsMsg(1);
        await publishToDlq(entry, coreDlq, coreInfo, msg, new Error('x'), 1);
        expect(jsonCodec().decode(published(ncPublish)[1])).toEqual({ a: 1 });
      });

      it('still publishes when a plugin deletes Nats-Msg-Id', async () => {
        const { entry, ncPublish } = fakeEntry({
          plugins: plugin(async (m, next) => {
            m.headers.delete('Nats-Msg-Id');
            await next();
          }),
        });
        const msg = failedJsMsg(1);
        await expect(publishToDlq(entry, coreDlq, coreInfo, msg, new Error('x'), 1)).resolves.toBe(
          'sent',
        );
        expect(published(ncPublish)[2].headers.has('Nats-Msg-Id')).toBe(false);
      });
    });
  });
});
