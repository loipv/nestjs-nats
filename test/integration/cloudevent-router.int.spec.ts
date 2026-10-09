import { Injectable } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { NatsClient } from '../../src/client/nats-client.js';
import type { CloudEventMeta } from '../../src/cloudevents/types.js';
import { CloudEventRouter, OnCloudEvent } from '../../src/decorators/cloudevent.decorators.js';
import { CloudEvent, Payload } from '../../src/decorators/param.decorators.js';
import { NatsRequestError } from '../../src/errors/errors.js';
import type { JetStreamClient } from '../../src/jetstream/jetstream-client.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { getJetStreamClientToken, getNatsClientToken } from '../../src/module/tokens.js';
import type { MessageOutcome } from '../../src/pipeline/delivery.js';
import type { NatsPlugin } from '../../src/pipeline/types.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

type Received = { handler: string; data: unknown; ce: CloudEventMeta };

@CloudEventRouter({ subscribe: 'cer.core.>' })
@Injectable()
class CoreRouter {
  readonly received: Received[] = [];

  @OnCloudEvent('order.created')
  created(@Payload() data: unknown, @CloudEvent() ce: CloudEventMeta) {
    this.received.push({ handler: 'created', data, ce });
    return { ok: 'created' };
  }

  @OnCloudEvent('order.*')
  other(@Payload() data: unknown, @CloudEvent() ce: CloudEventMeta) {
    this.received.push({ handler: 'other', data, ce });
  }
}

@CloudEventRouter({
  consume: { stream: 'CER', durable: 'cer-router', config: { filter_subject: 'cer.js.>' } },
  unmatched: 'term',
})
@Injectable()
class JsRouter {
  readonly received: Received[] = [];

  @OnCloudEvent('order.>')
  on(@Payload() data: unknown, @CloudEvent() ce: CloudEventMeta) {
    this.received.push({ handler: 'order', data, ce });
  }
}

@CloudEventRouter({ subscribe: 'cer.idem.>', idempotency: true })
@Injectable()
class IdemRouter {
  readonly received: Received[] = [];

  @OnCloudEvent('order.created')
  on(@Payload() data: unknown, @CloudEvent() ce: CloudEventMeta) {
    this.received.push({ handler: 'idem', data, ce });
  }
}

@CloudEventRouter({ subscribe: 'cer404.>', unmatched: 'term' })
@Injectable()
class StrictRouter {
  @OnCloudEvent('order.created')
  on() {
    return 'ok';
  }
}

/** Records the outcome of every message, by handler class. */
const outcomes: Array<{ className: string; type?: string; outcome: MessageOutcome }> = [];
const recorder: NatsPlugin = {
  name: 'outcomes',
  inbound: {
    stage: 'outer',
    async handle(d, next) {
      try {
        await next();
      } finally {
        for (const [msg, outcome] of d.outcomes) {
          outcomes.push({
            className: d.handler.className,
            type: msg.headers?.get('ce-type') || undefined,
            outcome,
          });
        }
      }
    },
  },
};

describe('@CloudEventRouter', () => {
  let srv: NatsTestServer;
  let app: TestingModule;
  let client: NatsClient;
  let js: JetStreamClient;

  beforeAll(async () => {
    srv = await startNats();
    app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger: false,
          cloudEvents: { source: 'urn:router-test' },
          streams: [{ name: 'CER', subjects: ['cer.js.>'] }],
          plugins: [recorder],
        }),
      ],
      providers: [CoreRouter, JsRouter, IdemRouter, StrictRouter],
    });
    client = app.get(getNatsClientToken());
    js = app.get(getJetStreamClientToken());
  });

  afterAll(async () => {
    await app?.close();
    await srv?.stop();
  });

  it('routes binary and structured CloudEvents published by NatsClient', async () => {
    const router = app.get(CoreRouter);
    client.publish('cer.core.a', { id: 1 }, { cloudEvent: { type: 'order.created' } });
    client.publish(
      'cer.core.b',
      { id: 2 },
      { cloudEvent: { type: 'order.paid', mode: 'structured' } },
    );
    await eventually(() => expect(router.received).toHaveLength(2));
    const byHandler = Object.fromEntries(router.received.map((r) => [r.handler, r]));
    expect(byHandler.created).toMatchObject({
      data: { id: 1 },
      ce: { type: 'order.created', source: 'urn:router-test' },
    });
    expect(byHandler.other).toMatchObject({ data: { id: 2 }, ce: { type: 'order.paid' } });
  });

  it('replies through the matched route and 404 for an unmatched request', async () => {
    const ce = (type: string) => ({
      'ce-specversion': '1.0',
      'ce-id': `req-${type}`,
      'ce-source': 'urn:req',
      'ce-type': type,
    });
    await expect(
      client.request('cer.core.req', { q: 1 }, { headers: ce('order.created') }),
    ).resolves.toEqual({ ok: 'created' });
    // Policy 'ack' (the default): no reply.
    await expect(
      client.request('cer.core.req', { q: 1 }, { headers: ce('invoice.sent'), timeout: 300 }),
    ).rejects.toMatchObject({ code: 'timeout' });
  });

  it('routes a JetStream router and settles per outcome', async () => {
    const router = app.get(JsRouter);
    await js.publish('cer.js.a', { n: 1 }, { cloudEvent: { type: 'order.created' } });
    await js.publish('cer.js.b', { n: 2 }, { cloudEvent: { type: 'invoice.sent' } });
    await eventually(() => {
      const mine = outcomes.filter((o) => o.className === 'JsRouter');
      expect(mine).toHaveLength(2);
      expect(mine.find((o) => o.type === 'order.created')?.outcome).toEqual({ action: 'ack' });
      expect(mine.find((o) => o.type === 'invoice.sent')?.outcome).toMatchObject({
        action: 'term',
      });
    });
    expect(router.received).toHaveLength(1);
    expect(router.received[0]).toMatchObject({ data: { n: 1 }, ce: { type: 'order.created' } });
    // Terminated, not redelivered.
    await sleep(300);
    expect(outcomes.filter((o) => o.className === 'JsRouter')).toHaveLength(2);
  });

  it('dedupes by CloudEvent source|id with idempotency', async () => {
    const router = app.get(IdemRouter);
    const ev = { type: 'order.created', id: 'same-id' };
    client.publish('cer.idem.a', { n: 1 }, { cloudEvent: ev });
    client.publish('cer.idem.a', { n: 1 }, { cloudEvent: ev });
    await eventually(() =>
      expect(
        outcomes.filter((o) => o.className === 'IdemRouter').map((o) => o.outcome.action),
      ).toEqual(['none', 'skipped']),
    );
    expect(router.received).toHaveLength(1);
  });

  it('replies 404 to an unmatched request when unmatched is not ack', async () => {
    const err = await client
      .request('cer404.x', null, {
        headers: {
          'ce-specversion': '1.0',
          'ce-id': '1',
          'ce-source': 'urn:x',
          'ce-type': 'invoice.sent',
        },
      })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsRequestError);
    expect(err).toMatchObject({
      code: 'service_error',
      serviceCode: 404,
      description: 'No handler for CloudEvent type "invoice.sent"',
    });
    await eventually(() =>
      expect(outcomes.find((o) => o.className === 'StrictRouter')?.outcome).toMatchObject({
        action: 'replied',
        error: expect.anything(),
      }),
    );
  });
});
