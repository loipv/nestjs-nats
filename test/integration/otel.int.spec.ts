import { Injectable } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { type SpanContext, SpanKind, trace } from '@opentelemetry/api';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-base';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { NatsClient } from '../../src/client/nats-client.js';
import {
  Consume,
  NatsService,
  ServiceEndpoint,
  Subscribe,
} from '../../src/decorators/handler.decorators.js';
import { Payload } from '../../src/decorators/param.decorators.js';
import type { JetStreamClient } from '../../src/jetstream/jetstream-client.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { getJetStreamClientToken, getNatsClientToken } from '../../src/module/tokens.js';
import { tracing } from '../../src/otel/index.js';
import { setupTracing, type TestTracing } from '../support/otel.js';
import { createTestApp } from './support/app.js';
import { eventually } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

@Injectable()
class Handlers {
  readonly consumed: Array<{ payload: unknown; active?: SpanContext }> = [];

  @Consume({ stream: 'TRC', durable: 'trc', config: { filter_subject: 'trc.created' } })
  onCreated(@Payload() payload: unknown) {
    this.consumed.push({ payload, active: trace.getActiveSpan()?.spanContext() });
  }

  @Subscribe('trc.get')
  onGet(@Payload() p: { id: number }) {
    return { id: p.id };
  }
}

@Injectable()
@NatsService({ name: 'trc', version: '1.0.0' })
class TraceService {
  @ServiceEndpoint('trc_echo', { subject: 'trc.echo' })
  echo(@Payload() p: unknown) {
    return p;
  }
}

describe('OpenTelemetry tracing', () => {
  let srv: NatsTestServer;
  let app: TestingModule;
  let otel: TestTracing;
  let js: JetStreamClient;
  let client: NatsClient;
  let handlers: Handlers;

  const spans = () => otel.exporter.getFinishedSpans();
  const span = (name: string, kind: SpanKind): ReadableSpan => {
    const found = spans().find((s) => s.name === name && s.kind === kind);
    if (!found)
      throw new Error(`no ${SpanKind[kind]} span "${name}" in [${spans().map((s) => s.name)}]`);
    return found;
  };

  beforeAll(async () => {
    otel = setupTracing();
    srv = await startNats();
    app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger: false,
          plugins: [tracing()],
          streams: [{ name: 'TRC', subjects: ['trc.created'] }],
        }),
      ],
      providers: [Handlers, TraceService],
    });
    js = app.get(getJetStreamClientToken());
    client = app.get(getNatsClientToken());
    handlers = app.get(Handlers);
  });

  beforeEach(() => otel.reset());

  afterAll(async () => {
    await app?.close();
    await srv?.stop();
    await otel?.shutdown();
  });

  it('keeps one trace id from publish to @Consume', async () => {
    await js.publish('trc.created', { id: 1 }, { msgId: 'trace-1' });
    await eventually(() => expect(handlers.consumed).toHaveLength(1));
    const process = await eventually(() => span('process trc.created', SpanKind.CONSUMER));
    const publish = span('publish trc.created', SpanKind.PRODUCER);
    expect(process.spanContext().traceId).toBe(publish.spanContext().traceId);
    expect(process.parentSpanContext?.spanId).toBe(publish.spanContext().spanId);
    expect(process.attributes).toMatchObject({
      'messaging.system': 'nats',
      'messaging.destination.name': 'trc.created',
      'messaging.consumer.group.name': 'trc',
      'messaging.nats.stream': 'TRC',
      'messaging.nats.delivery_count': 1,
      'messaging.message.id': 'trace-1',
    });
    // The handler runs inside the process span.
    expect(handlers.consumed[0]?.active?.spanId).toBe(process.spanContext().spanId);
  });

  it('traces request/reply as CLIENT → SERVER', async () => {
    expect(await client.request('trc.get', { id: 2 })).toEqual({ id: 2 });
    const server = await eventually(() => span('process trc.get', SpanKind.SERVER));
    const send = span('send trc.get', SpanKind.CLIENT);
    expect(server.spanContext().traceId).toBe(send.spanContext().traceId);
    expect(server.parentSpanContext?.spanId).toBe(send.spanContext().spanId);
  });

  it('traces service endpoints as SERVER spans', async () => {
    expect(await client.request('trc.echo', { ok: true })).toEqual({ ok: true });
    const server = await eventually(() => span('process trc.echo', SpanKind.SERVER));
    const send = span('send trc.echo', SpanKind.CLIENT);
    expect(server.parentSpanContext?.spanId).toBe(send.spanContext().spanId);
  });
});
