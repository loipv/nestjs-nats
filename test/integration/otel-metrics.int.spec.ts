import { Injectable } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { NatsClient } from '../../src/client/nats-client.js';
import { Consume, Subscribe } from '../../src/decorators/handler.decorators.js';
import { Payload } from '../../src/decorators/param.decorators.js';
import type { JetStreamClient } from '../../src/jetstream/jetstream-client.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { getJetStreamClientToken, getNatsClientToken } from '../../src/module/tokens.js';
import { metrics } from '../../src/otel/index.js';
import { setupMetrics, type TestMetrics } from '../support/otel.js';
import { createTestApp } from './support/app.js';
import { eventually } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

@Injectable()
class Handlers {
  @Consume({
    stream: 'MTR',
    durable: 'mtr',
    config: { filter_subject: 'mtr.fail' },
    retry: { maxRetries: 1, initialDelay: 10 },
    dlq: { subject: 'mtr.dlq' },
  })
  onFail() {
    throw new Error('boom');
  }

  @Subscribe('mtr.get')
  onGet(@Payload() p: { id: number }) {
    return { id: p.id };
  }
}

describe('OpenTelemetry metrics', () => {
  let srv: NatsTestServer;
  let app: TestingModule;
  let otel: TestMetrics;
  let js: JetStreamClient;
  let client: NatsClient;

  const consumed = async (outcome: string) =>
    (await otel.points('messaging.client.consumed.messages')).filter(
      (p) => p.attributes['nats.outcome'] === outcome,
    );
  const sent = async () => otel.points('messaging.client.sent.messages');

  beforeAll(async () => {
    otel = setupMetrics();
    srv = await startNats();
    app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger: false,
          plugins: [metrics()],
          streams: [
            { name: 'MTR', subjects: ['mtr.fail', 'mtr.ok'] },
            { name: 'MTRDLQ', subjects: ['mtr.dlq'] },
          ],
        }),
      ],
      providers: [Handlers],
    });
    js = app.get(getJetStreamClientToken());
    client = app.get(getNatsClientToken());
  });

  afterAll(async () => {
    await app?.close();
    await srv?.stop();
    await otel?.shutdown();
  });

  it('counts retry and dlq outcomes, and the dead letter as sent', async () => {
    await js.publish('mtr.fail', { id: 1 });
    await eventually(async () => {
      const retry = await consumed('retry');
      const dlq = await consumed('dlq');
      expect(retry.map((p) => p.value)).toEqual([1]);
      expect(dlq.map((p) => p.value)).toEqual([1]);
      expect(retry[0]?.attributes['nats.handler']).toBe('Handlers.onFail');
      expect(dlq[0]?.attributes['nats.handler']).toBe('Handlers.onFail');
      expect((await sent()).some((p) => p.attributes['nats.dead_letter'] === true)).toBe(true);
    });
  });

  it('counts a replied request as consumed and sent', async () => {
    expect(await client.request('mtr.get', { id: 2 })).toEqual({ id: 2 });
    await eventually(async () => {
      expect((await consumed('replied')).length).toBeGreaterThan(0);
      expect((await sent()).some((p) => p.attributes['nats.publish.kind'] === 'request')).toBe(
        true,
      );
    });
  });

  it('counts a JetStream publish as sent', async () => {
    await js.publish('mtr.ok', { id: 3 });
    await eventually(async () => {
      expect((await sent()).some((p) => p.attributes['nats.publish.kind'] === 'jetstream')).toBe(
        true,
      );
    });
  });
});
