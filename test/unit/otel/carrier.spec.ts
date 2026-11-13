import { headers, type MsgHdrs } from '@nats-io/transport-node';
import { propagation, trace } from '@opentelemetry/api';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NatsContext } from '../../../src/context/nats-context.js';
import {
  extractContext,
  headerValues,
  msgHdrsGetter,
  msgHdrsSetter,
} from '../../../src/otel/carrier.js';
import type { InboundMessage } from '../../../src/pipeline/delivery.js';

const TRACE_ID = '0af7651916cd43dd8448eb211c80319c';
const OTHER_TRACE_ID = '4bf92f3577b34da6a3ce929d0e0e4736';
const SPAN_ID = 'b7ad6b7169203331';
const traceparent = (traceId = TRACE_ID) => `00-${traceId}-${SPAN_ID}-01`;

function hdrs(entries: Array<[string, string]>): MsgHdrs {
  const h = headers();
  for (const [k, v] of entries) h.append(k, v);
  return h;
}

/** The span context `extractContext` finds in a core message with these headers and body. */
function extracted(h: MsgHdrs | undefined, body = '') {
  const raw = { subject: 'o.c', reply: '', headers: h, data: new TextEncoder().encode(body) };
  const message: InboundMessage = {
    subject: raw.subject,
    data: raw.data,
    headers: h,
    raw: raw as never,
    attempt: 1,
  };
  const ctx = new NatsContext({ kind: 'core', connection: 'default', raw: raw as never });
  return trace.getSpanContext(extractContext(message, ctx));
}

describe('headerValues', () => {
  it('collects the values of a header under any case, in order', () => {
    const h = hdrs([
      ['X-Tenant', 'a'],
      ['x-tenant', 'b'],
      ['other', 'c'],
    ]);
    expect(headerValues(h, 'X-TENANT')).toEqual(['a', 'b']);
  });

  it('is empty for a missing header or no headers', () => {
    expect(headerValues(hdrs([['a', '1']]), 'b')).toEqual([]);
    expect(headerValues(undefined, 'a')).toEqual([]);
  });
});

describe('msgHdrsGetter', () => {
  it('reads one value as a string and several as an array, under any case', () => {
    const h = hdrs([
      ['TraceParent', 'tp'],
      ['multi', 'a'],
      ['multi', 'b'],
    ]);
    expect(msgHdrsGetter.get(h, 'traceparent')).toBe('tp');
    expect(msgHdrsGetter.get(h, 'multi')).toEqual(['a', 'b']);
    expect(msgHdrsGetter.get(h, 'missing')).toBeUndefined();
  });

  it('lists the header names', () => {
    expect(msgHdrsGetter.keys(hdrs([['a', '1']]))).toEqual(['a']);
    expect(msgHdrsGetter.keys(undefined)).toEqual([]);
  });
});

describe('msgHdrsSetter', () => {
  it('replaces a value set under another case', () => {
    const h = hdrs([
      ['TraceParent', 'old'],
      ['other', 'kept'],
    ]);
    msgHdrsSetter.set(h, 'traceparent', 'new');
    expect(h.keys().sort()).toEqual(['other', 'traceparent']);
    expect(h.get('traceparent')).toBe('new');
  });
});

describe('extractContext', () => {
  beforeAll(() => {
    propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  });

  afterAll(() => {
    propagation.disable();
  });

  it('reads the trace context from the traceparent and tracestate headers', () => {
    const span = extracted(
      hdrs([
        ['traceparent', traceparent()],
        ['tracestate', 'k=v'],
      ]),
    );
    expect(span?.traceId).toBe(TRACE_ID);
    expect(span?.spanId).toBe(SPAN_ID);
    expect(span?.traceState?.serialize()).toBe('k=v');
  });

  it('falls back to the ce-traceparent and ce-tracestate headers of a binary CloudEvent', () => {
    const span = extracted(
      hdrs([
        ['ce-traceparent', traceparent()],
        ['ce-tracestate', 'k=v'],
      ]),
    );
    expect(span?.traceId).toBe(TRACE_ID);
    expect(span?.traceState?.serialize()).toBe('k=v');
  });

  it('prefers the traceparent header over the CloudEvent one', () => {
    const span = extracted(
      hdrs([
        ['traceparent', traceparent(TRACE_ID)],
        ['ce-traceparent', traceparent(OTHER_TRACE_ID)],
      ]),
    );
    expect(span?.traceId).toBe(TRACE_ID);
  });

  it('falls back to the traceparent attribute of a structured CloudEvent', () => {
    const envelope = JSON.stringify({
      specversion: '1.0',
      id: '1',
      source: 'src',
      type: 't',
      traceparent: traceparent(),
      tracestate: 'k=v',
    });
    const span = extracted(hdrs([['Content-Type', 'application/cloudevents+json']]), envelope);
    expect(span?.traceId).toBe(TRACE_ID);
    expect(span?.traceState?.serialize()).toBe('k=v');
  });

  it('finds no trace context in a message without one', () => {
    expect(extracted(undefined)).toBeUndefined();
    expect(extracted(hdrs([['x', '1']]))).toBeUndefined();
  });
});
