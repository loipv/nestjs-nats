import { headers, type MsgHdrs } from '@nats-io/transport-node';
import {
  context,
  propagation,
  ROOT_CONTEXT,
  type SpanContext,
  SpanKind,
  SpanStatusCode,
  trace,
} from '@opentelemetry/api';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-base';
import { afterAll, afterEach, beforeAll, describe, expect, expectTypeOf, it } from 'vitest';
import { jsonCodec } from '../../../src/codec/codec.js';
import * as otelApi from '../../../src/otel/index.js';
import { type TracingOptions, tracing } from '../../../src/otel/index.js';
import type {
  Delivery,
  HandlerInfo,
  InboundMessage,
  MessageOutcome,
} from '../../../src/pipeline/delivery.js';
import type { NatsPlugin, OutgoingMessage } from '../../../src/pipeline/types.js';
import {
  base,
  consumeHandler,
  corePart,
  delivery,
  endpointHandler,
  jsPart,
  orderedPart,
  subscribeHandler,
} from '../../support/deliveries.js';
import { setupTracing, type TestTracing } from '../../support/otel.js';

const TRACE_ID = '0af7651916cd43dd8448eb211c80319c';
const PARENT_ID = 'b7ad6b7169203331';
const traceparent = (traceId = TRACE_ID, spanId = PARENT_ID) => `00-${traceId}-${spanId}-01`;

const enc = new TextEncoder();

function hdrs(init: Record<string, string> = {}): MsgHdrs {
  const h = headers();
  for (const [k, v] of Object.entries(init)) h.append(k, v);
  return h;
}

function outgoing(kind: OutgoingMessage['kind'], h: MsgHdrs = headers()): OutgoingMessage {
  return {
    kind,
    connection: 'default',
    subject: 'o.c',
    data: { id: 1 },
    headers: h,
    codec: jsonCodec(),
  };
}

it('exports the otel subpath API', () => {
  expect(Object.keys(otelApi).sort()).toEqual(['metrics', 'tracing']);
  expectTypeOf<TracingOptions>().toEqualTypeOf<{
    tracerName?: string;
    captureHeaders?: string[];
  }>();
  expectTypeOf<otelApi.MetricsOptions>().toEqualTypeOf<{
    meterName?: string;
    subjects?: boolean | ((subject: string) => string | undefined);
  }>();
});

describe('tracing()', () => {
  let t: TestTracing;
  let plugin: NatsPlugin;

  beforeAll(() => {
    t = setupTracing();
    plugin = tracing({ captureHeaders: ['X-Tenant', 'x-tags'] });
  });
  afterEach(() => t.reset());
  afterAll(() => t.shutdown());

  const spans = () => t.exporter.getFinishedSpans();
  const only = (): ReadableSpan => {
    expect(spans()).toHaveLength(1);
    return spans()[0] as ReadableSpan;
  };
  const inbound = (d: Delivery, next: () => Promise<void> = async () => {}) =>
    (plugin.inbound as NonNullable<NatsPlugin['inbound']>).handle(d, next);
  const outbound = (m: OutgoingMessage, next: () => Promise<void> = async () => {}) =>
    (plugin.outbound as NonNullable<NatsPlugin['outbound']>).handle(m, next);

  it('is named otel-tracing and runs in the outer stage', () => {
    expect(plugin.name).toBe('otel-tracing');
    expect(plugin.inbound?.stage).toBe('outer');
  });

  it('creates a PRODUCER span and injects traceparent', async () => {
    const m = outgoing('publish', hdrs({ 'Nats-Msg-Id': 'm-1' }));
    let activeInNext: SpanContext | undefined;
    await outbound(m, async () => {
      activeInNext = trace.getActiveSpan()?.spanContext();
    });
    const span = only();
    expect(span.name).toBe('publish o.c');
    expect(span.kind).toBe(SpanKind.PRODUCER);
    expect(span.attributes).toMatchObject({
      'messaging.system': 'nats',
      'messaging.destination.name': 'o.c',
      'messaging.operation.type': 'send',
      'messaging.operation.name': 'publish',
      'messaging.message.id': 'm-1',
    });
    const tp = m.headers.get('traceparent');
    expect(tp).toContain(span.spanContext().traceId);
    expect(tp).toContain(span.spanContext().spanId);
    expect(activeInNext?.spanId).toBe(span.spanContext().spanId);
  });

  it('uses PRODUCER for JetStream publishes and the CloudEvent id as message id', async () => {
    const m = outgoing('jetstream');
    m.cloudEvent = {
      meta: { id: 'ce-1', source: 's', type: 't', specversion: '1.0', extensions: {} },
      mode: 'binary',
    };
    await outbound(m);
    const span = only();
    expect(span.name).toBe('publish o.c');
    expect(span.kind).toBe(SpanKind.PRODUCER);
    expect(span.attributes['messaging.message.id']).toBe('ce-1');
  });

  it('records the batch size of an atomic publish', async () => {
    const m = outgoing('jetstream');
    m.atomic = { index: 1, count: 3 };
    await outbound(m);
    expect(only().attributes['messaging.batch.message_count']).toBe(3);
    t.reset();
    await outbound(outgoing('jetstream'));
    expect(only().attributes).not.toHaveProperty('messaging.batch.message_count');
  });

  it('makes the publish span a child of the active span', async () => {
    const parent = trace.getTracer('test').startSpan('parent');
    await context.with(trace.setSpan(context.active(), parent), () =>
      outbound(outgoing('publish')),
    );
    parent.end();
    const span = spans().find((s) => s.name === 'publish o.c') as ReadableSpan;
    expect(span.parentSpanContext?.spanId).toBe(parent.spanContext().spanId);
    expect(span.spanContext().traceId).toBe(parent.spanContext().traceId);
  });

  it('gives a dead letter its own publish span, a child of the consume span', async () => {
    const d = delivery(consumeHandler, [jsPart()]);
    const m = outgoing('jetstream');
    m.subject = 'dlq.ORD.billing';
    m.deadLetter = { subject: 'o.c', handler: consumeHandler, error: new Error('dead') };
    await inbound(d, async () => {
      await outbound(m);
    });
    const consume = spans().find((s) => s.kind === SpanKind.CONSUMER) as ReadableSpan;
    const publish = spans().find((s) => s.name === 'publish dlq.ORD.billing') as ReadableSpan;
    expect(publish.kind).toBe(SpanKind.PRODUCER);
    expect(publish.parentSpanContext?.spanId).toBe(consume.spanContext().spanId);
    const tp = m.headers.get('traceparent');
    expect(tp).toContain(publish.spanContext().traceId);
    expect(tp).toContain(publish.spanContext().spanId);
  });

  it('records outbound errors and rethrows', async () => {
    const boom = new Error('down');
    await expect(
      outbound(outgoing('publish'), async () => {
        throw boom;
      }),
    ).rejects.toBe(boom);
    const span = only();
    expect(span.status.code).toBe(SpanStatusCode.ERROR);
    expect(span.events.map((e) => e.name)).toContain('exception');
  });

  it('creates a CONSUMER child span of the extracted context', async () => {
    const d = delivery(consumeHandler, [jsPart({ headers: hdrs({ traceparent: traceparent() }) })]);
    let activeInNext: SpanContext | undefined;
    await inbound(d, async () => {
      activeInNext = trace.getActiveSpan()?.spanContext();
    });
    const span = only();
    expect(span.name).toBe('process o.c');
    expect(span.kind).toBe(SpanKind.CONSUMER);
    expect(span.spanContext().traceId).toBe(TRACE_ID);
    expect(span.parentSpanContext?.spanId).toBe(PARENT_ID);
    expect(span.links).toHaveLength(0);
    expect(activeInNext?.spanId).toBe(span.spanContext().spanId);
  });

  it('reads the traceparent header case-insensitively', async () => {
    const d = delivery(consumeHandler, [jsPart({ headers: hdrs({ TraceParent: traceparent() }) })]);
    await inbound(d);
    expect(only().spanContext().traceId).toBe(TRACE_ID);
  });

  it('starts a new trace when the message carries no context', async () => {
    await inbound(delivery(consumeHandler, [jsPart()]));
    const span = only();
    expect(span.parentSpanContext).toBeUndefined();
    expect(span.spanContext().traceId).not.toBe(TRACE_ID);
  });

  it('uses SERVER for reply deliveries and CLIENT for requests', async () => {
    const tp = hdrs({ traceparent: traceparent() });
    await inbound(delivery(endpointHandler, [corePart({ reply: '_INBOX.1', headers: tp })]));
    await inbound(delivery(subscribeHandler, [corePart({ reply: '_INBOX.2' })]));
    await inbound(delivery(subscribeHandler, [corePart()]));
    await outbound(outgoing('request'));
    const [endpoint, request, event, send] = spans() as ReadableSpan[];
    expect(endpoint?.kind).toBe(SpanKind.SERVER);
    expect(endpoint?.name).toBe('process o.c');
    expect(endpoint?.parentSpanContext?.spanId).toBe(PARENT_ID);
    expect(request?.kind).toBe(SpanKind.SERVER);
    expect(event?.kind).toBe(SpanKind.CONSUMER);
    expect(send?.kind).toBe(SpanKind.CLIENT);
    expect(send?.name).toBe('send o.c');
    expect(send?.attributes).toMatchObject({
      'messaging.operation.type': 'send',
      'messaging.operation.name': 'request',
    });
  });

  it('links batch spans to every message', async () => {
    const ids = ['1'.repeat(32), '2'.repeat(32), '3'.repeat(32)];
    const parts = ids.map((id) => corePart({ headers: hdrs({ traceparent: traceparent(id) }) }));
    parts.push(corePart({ headers: hdrs({ traceparent: 'garbage' }) }));
    parts.push(corePart());
    await inbound(delivery(subscribeHandler, parts, true));
    const span = only();
    expect(span.name).toBe('process o.c');
    expect(span.kind).toBe(SpanKind.CONSUMER);
    expect(span.parentSpanContext).toBeUndefined();
    expect(span.links.map((l) => l.context.traceId)).toEqual(ids);
    expect(span.attributes['messaging.batch.message_count']).toBe(5);
    expect(span.attributes['messaging.message.id']).toBeUndefined();
    expect(span.attributes['messaging.message.body.size']).toBeUndefined();
  });

  it('reads traceparent from a structured CloudEvent envelope', async () => {
    const env = {
      specversion: '1.0',
      id: 'ce-9',
      source: 'test',
      type: 'o.created',
      traceparent: traceparent(),
      data: { id: 1 },
    };
    const part = corePart({
      headers: hdrs({ 'Content-Type': 'application/cloudevents+json' }),
      data: enc.encode(JSON.stringify(env)),
    });
    await inbound(delivery(subscribeHandler, [part]));
    const span = only();
    expect(span.spanContext().traceId).toBe(TRACE_ID);
    expect(span.parentSpanContext?.spanId).toBe(PARENT_ID);
    expect(span.attributes['messaging.message.id']).toBe('ce-9');
  });

  it('prefers the traceparent header over the envelope', async () => {
    const other = 'f'.repeat(32);
    const env = { specversion: '1.0', id: 'x', source: 's', type: 't', traceparent: traceparent() };
    const part = corePart({
      headers: hdrs({
        'Content-Type': 'application/cloudevents+json',
        traceparent: traceparent(other),
      }),
      data: enc.encode(JSON.stringify(env)),
    });
    await inbound(delivery(subscribeHandler, [part]));
    expect(only().spanContext().traceId).toBe(other);
  });

  it('records retry, dlq and skipped as events and errors as status', async () => {
    const parts = [corePart(), corePart(), corePart(), corePart()];
    const d = delivery(subscribeHandler, parts, true);
    const [m0, m1, m2, m3] = d.messages as [
      InboundMessage,
      InboundMessage,
      InboundMessage,
      InboundMessage,
    ];
    m0.attempt = 2;
    const retryErr = new Error('retry me');
    const dlqErr = new Error('dead');
    await inbound(d, async () => {
      const set = (m: InboundMessage, o: MessageOutcome) => d.outcomes.set(m, o);
      set(m0, { action: 'retry', delayMs: 250, error: retryErr });
      set(m1, { action: 'dlq', subject: 'dlq.o', error: dlqErr });
      set(m2, { action: 'skipped', reason: 'duplicate' });
      set(m3, { action: 'ack' });
    });
    const span = only();
    const events = span.events.map((e) => ({ name: e.name, attributes: e.attributes }));
    expect(events).toContainEqual({
      name: 'nats.retry',
      attributes: { 'nats.retry.delay_ms': 250, 'nats.attempt': 2 },
    });
    expect(events).toContainEqual({
      name: 'nats.dlq',
      attributes: { 'nats.dlq.subject': 'dlq.o' },
    });
    expect(events).toContainEqual({
      name: 'nats.skipped',
      attributes: { 'nats.skipped.reason': 'duplicate' },
    });
    const exceptions = span.events.filter((e) => e.name === 'exception');
    expect(exceptions.map((e) => e.attributes?.['exception.message'])).toEqual([
      'retry me',
      'dead',
    ]);
    expect(span.status.code).toBe(SpanStatusCode.ERROR);
  });

  it.each<[string, MessageOutcome]>([
    ['ack', { action: 'ack', error: new Error('x') }],
    ['term', { action: 'term', error: new Error('x') }],
    ['drop', { action: 'drop', error: new Error('x') }],
    ['replied', { action: 'replied', error: new Error('x') }],
  ])('marks %s with an error as ERROR', async (_, o) => {
    const d = delivery(subscribeHandler, [corePart()]);
    await inbound(d, async () => {
      d.outcomes.set(d.messages[0] as InboundMessage, o);
    });
    const span = only();
    expect(span.status.code).toBe(SpanStatusCode.ERROR);
    expect(span.events.filter((e) => e.name === 'exception')).toHaveLength(1);
  });

  it('leaves the status unset on success', async () => {
    const d = delivery(subscribeHandler, [corePart()]);
    await inbound(d, async () => {
      d.outcomes.set(d.messages[0] as InboundMessage, { action: 'ack' });
    });
    expect(only().status.code).toBe(SpanStatusCode.UNSET);
  });

  it('records an error thrown by next() once and rethrows it', async () => {
    const boom = new Error('boom');
    const d = delivery(subscribeHandler, [corePart()]);
    await expect(
      inbound(d, async () => {
        d.outcomes.set(d.messages[0] as InboundMessage, { action: 'drop', error: boom });
        throw boom;
      }),
    ).rejects.toBe(boom);
    const span = only();
    expect(span.status.code).toBe(SpanStatusCode.ERROR);
    expect(span.events.filter((e) => e.name === 'exception')).toHaveLength(1);
    expect(span.ended).toBe(true);
  });

  it('sets messaging attributes', async () => {
    const h = hdrs({ 'Nats-Msg-Id': 'msg-7', 'x-tenant': 'acme', 'X-Tags': 'a' });
    h.append('X-Tags', 'b');
    await inbound(delivery(consumeHandler, [jsPart({ headers: h, deliveryCount: 3 })]));
    expect(only().attributes).toEqual({
      'messaging.system': 'nats',
      'messaging.destination.name': 'o.c',
      'messaging.operation.type': 'process',
      'messaging.operation.name': 'process',
      'messaging.message.id': 'msg-7',
      'messaging.consumer.group.name': 'billing',
      'messaging.message.body.size': 8,
      'messaging.nats.stream': 'ORD',
      'messaging.nats.seq': 42,
      'messaging.nats.delivery_count': 3,
      'messaging.header.x-tenant': 'acme',
      'messaging.header.x-tags': ['a', 'b'],
    });
  });

  it('sets the stream and sequence of an ordered message, without a delivery count', async () => {
    const orderedHandler: HandlerInfo = {
      kind: 'ordered',
      ...base,
      stream: 'ORD',
      filterSubjects: [],
    };
    await inbound(delivery(orderedHandler, [orderedPart()]));
    expect(only().attributes).toEqual({
      'messaging.system': 'nats',
      'messaging.destination.name': 'o.c',
      'messaging.operation.type': 'process',
      'messaging.operation.name': 'process',
      'messaging.message.body.size': 8,
      'messaging.nats.stream': 'ORD',
      'messaging.nats.seq': 42,
    });
  });

  it('uses the queue group as the consumer group of a subscription', async () => {
    await inbound(delivery(subscribeHandler, [corePart()]));
    const attrs = only().attributes;
    expect(attrs['messaging.consumer.group.name']).toBe('workers');
    expect(attrs['messaging.nats.stream']).toBeUndefined();
  });

  it('captures headers on outbound messages', async () => {
    await outbound(outgoing('publish', hdrs({ 'X-Tenant': 'acme' })));
    expect(only().attributes['messaging.header.x-tenant']).toBe('acme');
  });

  it('uses a custom tracer name', async () => {
    const custom = tracing({ tracerName: 'my-tracer' });
    await custom.outbound?.handle(outgoing('publish'), async () => {});
    expect(only().instrumentationScope.name).toBe('my-tracer');
    await plugin.outbound?.handle(outgoing('publish'), async () => {});
    expect(spans()[1]?.instrumentationScope.name).toBe('@loipv/nestjs-nats');
  });

  it('reads binary-mode ce-traceparent / ce-tracestate headers without a traceparent header', async () => {
    const part = corePart({
      headers: hdrs({
        'ce-specversion': '1.0',
        'ce-id': 'b-1',
        'ce-source': 's',
        'ce-type': 't',
        'Ce-TraceParent': traceparent(),
        'ce-tracestate': 'vendor=1',
      }),
    });
    let state: string | undefined;
    await inbound(delivery(subscribeHandler, [part]), async () => {
      state = trace.getActiveSpan()?.spanContext().traceState?.serialize();
    });
    const span = only();
    expect(span.spanContext().traceId).toBe(TRACE_ID);
    expect(span.parentSpanContext?.spanId).toBe(PARENT_ID);
    expect(span.parentSpanContext?.traceState?.get('vendor')).toBe('1');
    expect(state).toBeDefined();
  });

  it('prefers the traceparent header over ce-traceparent', async () => {
    const other = 'e'.repeat(32);
    const part = corePart({
      headers: hdrs({
        'ce-specversion': '1.0',
        'ce-id': 'b-1',
        'ce-source': 's',
        'ce-type': 't',
        'ce-traceparent': traceparent(),
        traceparent: traceparent(other),
      }),
    });
    await inbound(delivery(subscribeHandler, [part]));
    expect(only().spanContext().traceId).toBe(other);
  });

  it('prefers ce-traceparent headers over the structured envelope', async () => {
    const other = 'd'.repeat(32);
    const env = { specversion: '1.0', id: 'x', source: 's', type: 't', traceparent: traceparent() };
    const part = corePart({
      headers: hdrs({
        'Content-Type': 'application/cloudevents+json',
        'ce-traceparent': traceparent(other),
      }),
      data: enc.encode(JSON.stringify(env)),
    });
    await inbound(delivery(subscribeHandler, [part]));
    expect(only().spanContext().traceId).toBe(other);
  });

  it('is a no-op without an SDK', async () => {
    trace.disable();
    propagation.disable();
    try {
      const m = outgoing('publish');
      await outbound(m);
      expect(m.headers.has('traceparent')).toBe(false);
      await inbound(
        delivery(consumeHandler, [jsPart({ headers: hdrs({ traceparent: traceparent() }) })]),
      );
      const err = new Error('x');
      await expect(
        inbound(delivery(subscribeHandler, [corePart()]), async () => {
          throw err;
        }),
      ).rejects.toBe(err);
      expect(spans()).toHaveLength(0);
      expect(propagation.extract(ROOT_CONTEXT, {})).toBe(ROOT_CONTEXT);
    } finally {
      trace.setGlobalTracerProvider(t.provider);
      propagation.setGlobalPropagator(new W3CTraceContextPropagator());
    }
  });
});
