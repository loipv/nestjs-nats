import { Injectable } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { NatsClient } from '../../src/client/nats-client.js';
import type { CloudEventMeta } from '../../src/cloudevents/types.js';
import { Subscribe } from '../../src/decorators/handler.decorators.js';
import { CloudEvent, Payload } from '../../src/decorators/param.decorators.js';
import type { JetStreamClient } from '../../src/jetstream/jetstream-client.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { getJetStreamClientToken, getNatsClientToken } from '../../src/module/tokens.js';
import { createTestApp } from './support/app.js';
import { eventually } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

type Received = { payload: unknown; ce: CloudEventMeta | undefined };

@Injectable()
class Handlers {
  readonly received: Received[] = [];

  @Subscribe('ce.pub.>')
  on(@Payload() payload: unknown, @CloudEvent() ce: CloudEventMeta | undefined) {
    this.received.push({ payload, ce });
  }
}

describe('CloudEvents publish', () => {
  let srv: NatsTestServer;
  let app: TestingModule;
  let client: NatsClient;
  let js: JetStreamClient;
  let handlers: Handlers;

  beforeAll(async () => {
    srv = await startNats();
    app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger: false,
          cloudEvents: { source: 'urn:test' },
          streams: [{ name: 'CEPUB', subjects: ['cejs.>'] }],
        }),
      ],
      providers: [Handlers],
    });
    client = app.get(getNatsClientToken());
    js = app.get(getJetStreamClientToken());
    handlers = app.get(Handlers);
  });

  afterAll(async () => {
    await app?.close();
    await srv?.stop();
  });

  it('roundtrips a binary CloudEvent through NATS to @CloudEvent()', async () => {
    client.publish(
      'ce.pub.binary',
      { a: 1 },
      { cloudEvent: { type: 'order.created', subject: 'o1', extensions: { tenant: 't1' } } },
    );
    await eventually(() => expect(handlers.received).toHaveLength(1));
    const got = handlers.received[0] as Received;
    expect(got.payload).toEqual({ a: 1 });
    expect(got.ce).toMatchObject({
      type: 'order.created',
      source: 'urn:test',
      subject: 'o1',
      specversion: '1.0',
      datacontenttype: 'application/json',
      extensions: { tenant: 't1' },
    });
    expect(got.ce?.id).toBeTruthy();
  });

  it('roundtrips a structured CloudEvent through NATS', async () => {
    client.publish(
      'ce.pub.structured',
      { b: 2 },
      { cloudEvent: { type: 'order.updated', id: 'sid-1', mode: 'structured' } },
    );
    await eventually(() => expect(handlers.received).toHaveLength(2));
    const got = handlers.received[1] as Received;
    expect(got.ce).toMatchObject({ type: 'order.updated', id: 'sid-1', source: 'urn:test' });
  });

  it('dedupes JetStream CloudEvent publishes server-side', async () => {
    const ev = { type: 't', id: 'dup-1' };
    const first = await js.publish('cejs.a', { n: 1 }, { cloudEvent: ev });
    const second = await js.publish('cejs.a', { n: 1 }, { cloudEvent: ev });
    expect(first.duplicate).toBe(false);
    expect(second.duplicate).toBe(true);
    expect(second.seq).toBe(first.seq);
  });
});
