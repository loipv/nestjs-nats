import {
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream';
import { connect, headers, type NatsConnection } from '@nats-io/transport-node';
import { Injectable } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NatsConnectionManager } from '../../src/connection/connection-manager.js';
import { Consume, Subscribe } from '../../src/decorators/handler.decorators.js';
import { Msg } from '../../src/decorators/param.decorators.js';
import {
  type ClaimResult,
  type IdempotencyStore,
  InMemoryIdempotencyStore,
} from '../../src/idempotency/store.js';
import { NatsModule } from '../../src/module/nats.module.js';
import type { InboundMessage } from '../../src/pipeline/delivery.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();
const RETRY = { initialDelay: 50, jitter: 0, maxRetries: 3 };

/** Counts handler runs per subject. */
@Injectable()
class Handlers {
  readonly runs: Record<string, number> = {};

  private count(subject: string): void {
    this.runs[subject] = (this.runs[subject] ?? 0) + 1;
  }

  @Subscribe('idem.core', { idempotency: true })
  onCore(@Msg() msg: { subject: string }) {
    this.count(msg.subject);
  }

  @Subscribe('idem.ttl', { idempotency: { ttl: 200 } })
  onTtl(@Msg() msg: { subject: string }) {
    this.count(msg.subject);
  }

  @Subscribe('idem.off')
  onOff(@Msg() msg: { subject: string }) {
    this.count(msg.subject);
  }

  @Consume({
    stream: 'IDEM',
    durable: 'by-order',
    idempotency: { keyExtractor: (m: InboundMessage) => m.headers?.get('x-order-id') || undefined },
    config: { filter_subject: 'idem.js.order' },
  })
  onOrder(@Msg() msg: { subject: string }) {
    this.count(msg.subject);
  }
}

/** Fails the first `claim`, then delegates to an in-memory store. */
class FlakyStore implements IdempotencyStore {
  readonly inner = new InMemoryIdempotencyStore();
  claims = 0;

  claim(key: string, ttlMs: number): Promise<ClaimResult> {
    if (++this.claims === 1) return Promise.reject(new Error('store unavailable'));
    return this.inner.claim(key, ttlMs);
  }

  complete(key: string, ttlMs: number) {
    return this.inner.complete(key, ttlMs);
  }

  release(key: string) {
    return this.inner.release(key);
  }

  close() {
    return this.inner.close();
  }
}

@Injectable()
class FlakyHandlers {
  runs = 0;

  @Subscribe('flaky.core', { idempotency: true })
  on() {
    this.runs++;
  }
}

describe('idempotency', () => {
  let srv: NatsTestServer;
  let nc: NatsConnection;
  let js: JetStreamClient;
  let jsm: JetStreamManager;
  let app: TestingModule;
  let handlers: Handlers;

  const publishCore = (subject: string, msgId?: string) => {
    const h = headers();
    if (msgId) h.set('Nats-Msg-Id', msgId);
    nc.publish(subject, enc.encode('{}'), { headers: h });
  };

  beforeAll(async () => {
    srv = await startNats();
    nc = await connect({ servers: srv.url });
    js = jetstream(nc);
    jsm = await jetstreamManager(nc);
    app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger: false,
          retry: RETRY,
          streams: [{ name: 'IDEM', subjects: ['idem.js.>'] }],
        }),
      ],
      providers: [Handlers],
    });
    handlers = app.get(Handlers);
  });

  afterAll(async () => {
    await app?.close();
    await nc?.close();
    await srv?.stop();
  });

  it('dedupes core messages by Nats-Msg-Id', async () => {
    publishCore('idem.core', 'm-1');
    publishCore('idem.core', 'm-1');
    publishCore('idem.core', 'm-2');
    await eventually(() => expect(handlers.runs['idem.core']).toBe(2));
    await sleep(200);
    expect(handlers.runs['idem.core']).toBe(2);
  });

  it('does not dedupe handlers that did not enable idempotency', async () => {
    publishCore('idem.off', 'same');
    publishCore('idem.off', 'same');
    await eventually(() => expect(handlers.runs['idem.off']).toBe(2));
  });

  it('processes messages without a key every time', async () => {
    publishCore('idem.core');
    publishCore('idem.core');
    await eventually(() => expect(handlers.runs['idem.core']).toBe(4));
  });

  it('dedupes JetStream messages via keyExtractor and acks the duplicates', async () => {
    // No Nats-Msg-Id: it would take precedence over the extractor, and duplicate ones
    // would be dropped by the server before reaching the consumer.
    for (let i = 0; i < 2; i++) {
      const h = headers();
      h.set('x-order-id', 'order-1');
      await js.publish('idem.js.order', enc.encode('{}'), { headers: h });
    }
    await eventually(async () => {
      const info = await jsm.consumers.info('IDEM', 'by-order');
      expect(info.ack_floor.stream_seq).toBe(2);
      expect(info.num_ack_pending).toBe(0);
      expect(info.num_pending).toBe(0);
    });
    await sleep(200);
    expect(handlers.runs['idem.js.order']).toBe(1);
    const info = await jsm.consumers.info('IDEM', 'by-order');
    expect(info.delivered.stream_seq).toBe(2);
    expect(info.num_redelivered).toBe(0);
  });

  it('processes again after ttl', async () => {
    publishCore('idem.ttl', 't-1');
    await eventually(() => expect(handlers.runs['idem.ttl']).toBe(1));
    await sleep(300);
    publishCore('idem.ttl', 't-1');
    await eventually(() => expect(handlers.runs['idem.ttl']).toBe(2));
  });

  it('shares one default in-memory store per connection and reports it', () => {
    const manager = app.get(NatsConnectionManager);
    const used = manager.idempotencyStores();
    expect(used).toHaveLength(1);
    expect(used[0]).toMatchObject({ connection: 'default', owned: true });
    expect(used[0]?.store).toBeInstanceOf(InMemoryIdempotencyStore);
    expect(manager.get().idempotencyStore()).toBe(used[0]?.store);
  });

  it('retries when the store fails to claim', async () => {
    const store = new FlakyStore();
    const flaky = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger: false,
          retry: RETRY,
          idempotency: { store },
        }),
      ],
      providers: [FlakyHandlers],
    });
    try {
      const h = flaky.get(FlakyHandlers);
      publishCore('flaky.core', 'f-1');
      await eventually(() => expect(h.runs).toBe(1));
      await sleep(300);
      expect(h.runs).toBe(1);
      expect(store.claims).toBeGreaterThanOrEqual(2);
      const used = flaky.get(NatsConnectionManager).idempotencyStores();
      expect(used).toEqual([{ connection: 'default', store, owned: false }]);
    } finally {
      await flaky.close();
    }
  });
});
