import { type JetStreamManager, jetstreamManager } from '@nats-io/jetstream';
import { connect, type NatsConnection } from '@nats-io/transport-node';
import { Injectable } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { SpanKind } from '@opentelemetry/api';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-base';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Consume } from '../../src/decorators/handler.decorators.js';
import { Payload } from '../../src/decorators/param.decorators.js';
import { NatsAtomicPublishError, NatsConfigError } from '../../src/errors/errors.js';
import type { JetStreamClient } from '../../src/jetstream/jetstream-client.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { getConnectionToken, getJetStreamClientToken } from '../../src/module/tokens.js';
import { tracing } from '../../src/otel/index.js';
import type { NatsPlugin, OutgoingMessage } from '../../src/pipeline/types.js';
import { setupTracing, type TestTracing } from '../support/otel.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

@Injectable()
class TracedHandlers {
  readonly consumed: unknown[] = [];

  @Consume({ stream: 'AT_TRACE', durable: 'at-trace', config: {} })
  on(@Payload() payload: unknown) {
    this.consumed.push(payload);
  }
}

/** A plugin running the current `before` hook of the test before passing each message on. */
function hookPlugin(hooks: { before?: (m: OutgoingMessage) => Promise<void> | void }): NatsPlugin {
  return {
    name: 'hook',
    outbound: {
      async handle(m, next) {
        await hooks.before?.(m);
        await next();
      },
    },
  };
}

const messages = (subject: string, n: number) =>
  Array.from({ length: n }, (_, i) => ({ subject, data: { i } }));

describe('JetStreamClient.publishAtomic', () => {
  let srv: NatsTestServer;
  let otel: TestTracing;
  let app: TestingModule;
  let js: JetStreamClient;
  let handlers: TracedHandlers;
  let nc: NatsConnection;
  let jsm: JetStreamManager;
  const hooks: { before?: (m: OutgoingMessage) => Promise<void> | void } = {};

  const stored = async (stream: string) => (await jsm.streams.info(stream)).state.messages;
  const span = (name: string, kind: SpanKind): ReadableSpan => {
    const spans = otel.exporter.getFinishedSpans();
    const found = spans.find((s) => s.name === name && s.kind === kind);
    if (!found)
      throw new Error(`no ${SpanKind[kind]} span "${name}" in [${spans.map((s) => s.name)}]`);
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
          plugins: [tracing(), hookPlugin(hooks)],
        }),
        NatsModule.forFeature({
          streams: [
            { name: 'AT_OK', subjects: ['at.ok.>'], allow_atomic: true },
            { name: 'AT_PLUGIN', subjects: ['at.plugin.>'], allow_atomic: true },
            { name: 'AT_TRACE', subjects: ['at.trace.>'], allow_atomic: true },
            { name: 'AT_PLAIN', subjects: ['at.plain.>'] },
          ],
        }),
      ],
      providers: [TracedHandlers],
    });
    js = app.get(getJetStreamClientToken());
    handlers = app.get(TracedHandlers);
    nc = await connect({ servers: srv.url });
    jsm = await jetstreamManager(nc);
  });

  afterAll(async () => {
    await app?.close();
    await nc?.close();
    await srv?.stop();
    await otel?.shutdown();
  });

  it('stores every message', async () => {
    const ack = await js.publishAtomic([
      { subject: 'at.ok.a', data: 1 },
      { subject: 'at.ok.b', data: 2 },
      { subject: 'at.ok.c', data: 3 },
    ]);
    expect(ack).toMatchObject({ stream: 'AT_OK', count: 3, duplicate: false });
    expect(ack.batch).toEqual(expect.any(String));
    const { state } = await jsm.streams.info('AT_OK');
    expect(state.messages).toBe(3);
    expect(state.last_seq).toBe(ack.seq);
    const last = await jsm.streams.getMessage('AT_OK', { seq: ack.seq });
    expect(last?.subject).toBe('at.ok.c');
  });

  it('rejects on a stream without allow_atomic', async () => {
    const err = await js.publishAtomic(messages('at.plain.x', 3)).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsAtomicPublishError);
    expect(err).toMatchObject({ subject: 'at.plain.x', count: 3 });
    expect(await stored('AT_PLAIN')).toBe(0);
  });

  it('rejects 1001 messages before sending any', async () => {
    await expect(js.publishAtomic(messages('at.ok.many', 1001))).rejects.toBeInstanceOf(
      NatsConfigError,
    );
  });

  it('stores nothing when a plugin throws', async () => {
    const boom = new Error('plugin failed');
    hooks.before = (m) => {
      if (m.atomic?.index === 2) throw boom;
    };
    try {
      await expect(js.publishAtomic(messages('at.plugin.x', 3))).rejects.toBe(boom);
    } finally {
      hooks.before = undefined;
    }
    await sleep(200);
    expect(await stored('AT_PLUGIN')).toBe(0);
  });

  it('rejects in time and stores nothing when the connection closes mid-way', async () => {
    const closing: { before?: (m: OutgoingMessage) => Promise<void> | void } = {};
    const own = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger: false,
          plugins: [hookPlugin(closing)],
        }),
        NatsModule.forFeature({
          streams: [{ name: 'AT_CLOSE', subjects: ['at.close.>'], allow_atomic: true }],
        }),
      ],
    });
    const ownNc = own.get<NatsConnection>(getConnectionToken());
    const n = 500;
    const timeout = 2000;
    // Deterministic: the connection is closed before message n/2 is passed on.
    closing.before = async (m) => {
      if (m.atomic?.index === n / 2) await ownNc.close();
    };
    try {
      const started = Date.now();
      const err = await own
        .get<JetStreamClient>(getJetStreamClientToken())
        .publishAtomic(messages('at.close.x', n), { timeout })
        .catch((e: unknown) => e);
      expect(Date.now() - started).toBeLessThanOrEqual(timeout + 1000);
      expect(err).toBeInstanceOf(NatsAtomicPublishError);
    } finally {
      await own.close();
    }
    expect(await stored('AT_CLOSE')).toBe(0);
  });

  it('traces produce → consume', async () => {
    otel.reset();
    const ack = await js.publishAtomic([
      { subject: 'at.trace.0', data: 0 },
      { subject: 'at.trace.1', data: 1 },
      { subject: 'at.trace.2', data: 2 },
    ]);
    expect(ack.count).toBe(3);
    await eventually(() => expect(handlers.consumed).toHaveLength(3));
    const process = await eventually(() => span('process at.trace.1', SpanKind.CONSUMER));
    const publish = span('publish at.trace.1', SpanKind.PRODUCER);
    expect(process.spanContext().traceId).toBe(publish.spanContext().traceId);
    expect(process.parentSpanContext?.spanId).toBe(publish.spanContext().spanId);
    expect(publish.attributes['messaging.batch.message_count']).toBe(3);
  });
});
