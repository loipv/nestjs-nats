import {
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream';
import { connect, type NatsConnection } from '@nats-io/transport-node';
import { Injectable } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { NatsBatchContext } from '../../src/context/nats-batch-context.js';
import type { NatsJetStreamContext } from '../../src/context/nats-context.js';
import { Consume } from '../../src/decorators/handler.decorators.js';
import { Ctx, Payload } from '../../src/decorators/param.decorators.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();
const json = (v: unknown) => enc.encode(JSON.stringify(v));

const RETRY = { initialDelay: 50, jitter: 0, maxRetries: 3 };

interface Item {
  n: number;
}

@Injectable()
class Handlers {
  readonly sized: Item[][] = [];
  readonly partial: Array<{ items: Item[]; at: number }> = [];
  readonly retried: Array<Array<[number, number]>> = [];
  readonly mixed: Item[][] = [];
  private failedOnce = false;

  @Consume({
    stream: 'BATCH',
    durable: 'by-size',
    batch: { maxSize: 5, maxWait: 5000 },
    config: { filter_subject: 'batch.size' },
  })
  onSized(@Payload() items: Item[]) {
    this.sized.push(items);
  }

  @Consume({
    stream: 'BATCH',
    durable: 'by-time',
    batch: { maxSize: 100, maxWait: 200 },
    config: { filter_subject: 'batch.time' },
  })
  onPartial(@Payload() items: Item[]) {
    this.partial.push({ items, at: Date.now() });
  }

  @Consume({
    stream: 'BATCH',
    durable: 'partial-fail',
    batch: { maxSize: 3, maxWait: 300 },
    config: { filter_subject: 'batch.fail' },
  })
  onRetried(@Payload() items: Item[], @Ctx() ctx: NatsBatchContext) {
    this.retried.push(
      items.map((item, i) => [item.n, (ctx.messages[i] as NatsJetStreamContext).deliveryCount]),
    );
    if (!this.failedOnce) {
      this.failedOnce = true;
      ctx.fail(1, new Error('item failed'));
    }
  }

  @Consume({
    stream: 'BATCH',
    durable: 'mixed',
    batch: { maxSize: 3, maxWait: 300 },
    config: { filter_subject: 'batch.mixed' },
  })
  onMixed(@Payload() items: Item[]) {
    this.mixed.push(items);
  }
}

@Injectable()
class ShutdownHandlers {
  readonly batches: Item[][] = [];

  @Consume({
    stream: 'BATCH',
    durable: 'shutdown',
    batch: { maxSize: 100, maxWait: 60_000 },
    config: { filter_subject: 'batch.shutdown' },
  })
  on(@Payload() items: Item[]) {
    this.batches.push(items);
  }
}

describe('batch processing', () => {
  let srv: NatsTestServer;
  let rawNc: NatsConnection;
  let js: JetStreamClient;
  let jsm: JetStreamManager;
  let app: TestingModule;
  let handlers: Handlers;

  const nats = () =>
    NatsModule.forRoot({
      connection: { servers: srv.url },
      logger: false,
      retry: RETRY,
      streams: [{ name: 'BATCH', subjects: ['batch.>'] }],
    });

  const consumerInfo = (durable: string) => jsm.consumers.info('BATCH', durable);

  beforeAll(async () => {
    srv = await startNats();
    rawNc = await connect({ servers: srv.url });
    js = jetstream(rawNc);
    jsm = await jetstreamManager(rawNc);
    app = await createTestApp({ imports: [nats()], providers: [Handlers] });
    handlers = app.get(Handlers);
  });

  afterAll(async () => {
    await app?.close();
    await rawNc?.close();
    await srv?.stop();
  });

  it('delivers JetStream batches by size', async () => {
    for (let n = 0; n < 10; n++) await js.publish('batch.size', json({ n }));
    await eventually(() => expect(handlers.sized.map((b) => b.length)).toEqual([5, 5]), {
      timeout: 4000,
    });
    expect(handlers.sized.flat().map((i) => i.n)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    await eventually(async () => expect((await consumerInfo('by-size')).num_ack_pending).toBe(0));
  });

  it('delivers a partial batch after maxWait', async () => {
    const start = Date.now();
    for (let n = 0; n < 3; n++) await js.publish('batch.time', json({ n }));
    await eventually(() => expect(handlers.partial.map((b) => b.items.length)).toEqual([3]));
    const elapsed = (handlers.partial[0]?.at ?? 0) - start;
    expect(elapsed).toBeGreaterThanOrEqual(150);
    expect(elapsed).toBeLessThan(1000);
  });

  it('acks succeeded and retries failed items', async () => {
    for (let n = 0; n < 3; n++) await js.publish('batch.fail', json({ n }));
    await eventually(() =>
      expect(handlers.retried).toEqual([
        [
          [0, 1],
          [1, 1],
          [2, 1],
        ],
        [[1, 2]],
      ]),
    );
    await eventually(async () => {
      const info = await consumerInfo('partial-fail');
      expect(info.num_ack_pending).toBe(0);
      expect(info.num_pending).toBe(0);
    });
    await sleep(500);
    expect(handlers.retried).toHaveLength(2);
  });

  it('processes the rest when one message is not JSON', async () => {
    await js.publish('batch.mixed', json({ n: 0 }));
    await js.publish('batch.mixed', enc.encode('hello'));
    await js.publish('batch.mixed', json({ n: 2 }));
    await eventually(() => expect(handlers.mixed).toEqual([[{ n: 0 }, { n: 2 }]]));
    await eventually(async () => {
      const info = await consumerInfo('mixed');
      expect(info.num_ack_pending).toBe(0);
      expect(info.num_pending).toBe(0);
    });
    // Termed (DLQ off): never redelivered.
    await sleep(500);
    expect(handlers.mixed).toEqual([[{ n: 0 }, { n: 2 }]]);
    expect((await consumerInfo('mixed')).num_redelivered).toBe(0);
  });

  it('flushes the pending batch on shutdown', async () => {
    const other = await createTestApp({ imports: [nats()], providers: [ShutdownHandlers] });
    const h = other.get(ShutdownHandlers);
    await js.publish('batch.shutdown', json({ n: 0 }));
    await js.publish('batch.shutdown', json({ n: 1 }));
    // Both received (and held by the collector) before shutting down.
    await eventually(async () => expect((await consumerInfo('shutdown')).num_ack_pending).toBe(2));
    expect(h.batches).toEqual([]);
    await other.close();
    expect(h.batches).toEqual([[{ n: 0 }, { n: 1 }]]);
    const info = await consumerInfo('shutdown');
    expect(info.num_ack_pending).toBe(0);
    expect(info.num_pending).toBe(0);
  });
});
