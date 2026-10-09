import type { JsMsg } from '@nats-io/jetstream';
import { headers } from '@nats-io/transport-node';
import { describe, expect, it } from 'vitest';
import type { HandlerInfo, InboundMessage } from '../../src/pipeline/delivery.js';
import {
  buildDlqHeaders,
  DLQ_HEADERS,
  type DlqPolicy,
  isNoResponders,
  resolveDlqPolicy,
} from '../../src/settlement/dlq.js';

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
  it('strips JetStream publish directives but keeps Nats-Msg-Id', () => {
    const h = buildDlqHeaders(
      jsMsg({
        stream: 'ORD',
        consumer: 'bill',
        streamSequence: 7,
        headers: {
          'Nats-Expected-Stream': 'ORD',
          'nats-expected-last-sequence': '6',
          'NATS-EXPECTED-LAST-MSG-ID': 'm0',
          'Nats-Expected-Last-Subject-Sequence': '3',
          'Nats-Rollup': 'sub',
          'nats-ttl': '1m',
          'Nats-Msg-Id': 'm1',
          'x-keep': 'yes',
        },
      }),
      new Error('x'),
      1,
      new Date(),
    );
    const keys = h.keys().map((k) => k.toLowerCase());
    expect(keys.filter((k) => k.startsWith('nats-expected-'))).toEqual([]);
    expect(keys).not.toContain('nats-rollup');
    expect(keys).not.toContain('nats-ttl');
    expect(h.get('Nats-Msg-Id')).toBe('m1');
    expect(h.get('x-keep')).toBe('yes');
  });
});

describe('isNoResponders', () => {
  const named = () => Object.assign(new Error('no responders'), { name: 'NoRespondersError' });

  it('recognizes a NoRespondersError by class', async () => {
    const { NoRespondersError } = await import('@nats-io/transport-node');
    expect(isNoResponders(new NoRespondersError('x'))).toBe(true);
  });

  it('recognizes a NoRespondersError by name (another copy of the library)', () =>
    expect(isNoResponders(named())).toBe(true));

  it('looks through the cause chain', () =>
    expect(
      isNoResponders(new Error('outer', { cause: new Error('mid', { cause: named() }) })),
    ).toBe(true));

  it('is false for other errors and non-errors', () => {
    expect(isNoResponders(new Error('timeout'))).toBe(false);
    expect(isNoResponders('NoRespondersError')).toBe(false);
    expect(isNoResponders(undefined)).toBe(false);
  });
});
