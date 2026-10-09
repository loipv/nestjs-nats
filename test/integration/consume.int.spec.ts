import {
  AckPolicy,
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream';
import { connect, type NatsConnection } from '@nats-io/transport-node';
import { Injectable } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { NatsJetStreamContext } from '../../src/context/nats-context.js';
import { Consume } from '../../src/decorators/handler.decorators.js';
import { Ctx, Payload } from '../../src/decorators/param.decorators.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();
const json = (v: unknown) => enc.encode(JSON.stringify(v));

@Injectable()
class Handlers {
  readonly billing: unknown[] = [];
  readonly deliveries: Record<string, number[]> = {};
  readonly manual: Array<{ id: number; ctx: NatsJetStreamContext }> = [];
  readonly seq: number[] = [];
  parallel = { active: 0, max: 0, done: 0 };

  @Consume({
    stream: 'ORD',
    durable: 'billing',
    config: { filter_subject: 'ord.created' },
  })
  onBilling(@Payload() payload: unknown) {
    this.billing.push(payload);
  }

  @Consume({ stream: 'ORD', durable: 'flaky', config: { filter_subject: 'ord.flaky' } })
  onFlaky(@Ctx() ctx: NatsJetStreamContext) {
    const list = this.deliveries[ctx.subject] ?? [];
    this.deliveries[ctx.subject] = list;
    list.push(ctx.deliveryCount);
    if (ctx.deliveryCount === 1) throw new Error('flaky');
  }

  @Consume({
    stream: 'ORD',
    durable: 'manual',
    ack: 'manual',
    config: { filter_subject: 'ord.manual' },
  })
  onManual(@Payload() p: { id: number; ack?: boolean }, @Ctx() ctx: NatsJetStreamContext) {
    this.manual.push({ id: p.id, ctx });
    if (p.ack) ctx.ack();
  }

  @Consume({ stream: 'ORD', durable: 'seq', config: { filter_subject: 'ord.seq' } })
  async onSeq(@Payload() i: number) {
    await sleep(Math.floor(Math.random() * 11));
    this.seq.push(i);
  }

  @Consume({
    stream: 'ORD',
    durable: 'par',
    concurrency: 4,
    config: { filter_subject: 'ord.par' },
  })
  async onPar() {
    this.parallel.active++;
    this.parallel.max = Math.max(this.parallel.max, this.parallel.active);
    await sleep(200);
    this.parallel.active--;
    this.parallel.done++;
  }
}

@Injectable()
class MissingConsumerHandlers {
  @Consume({ stream: 'ORD', durable: 'ghost' })
  on() {}
}

@Injectable()
class DuplicateDurableHandlers {
  @Consume({ stream: 'ORD', durable: 'dup', config: { filter_subject: 'ord.dup' } })
  a() {}

  @Consume({ stream: 'ORD', durable: 'dup' })
  b() {}
}

@Injectable()
class PreCreatedHandlers {
  readonly got: unknown[] = [];

  @Consume({ stream: 'ORD', durable: 'precreated' })
  on(@Payload() p: unknown) {
    this.got.push(p);
  }
}

@Injectable()
class DoomedHandlers {
  @Consume({ stream: 'ORD', durable: 'doomed', config: { filter_subject: 'ord.doomed' } })
  on() {}
}

@Injectable()
class SyncNoneHandlers {
  @Consume({ stream: 'ORD', durable: 'unsynced', config: { filter_subject: 'ord.unsynced' } })
  on() {}
}

@Injectable()
class SlowHandlers {
  started = 0;
  done = 0;

  @Consume({
    stream: 'ORD',
    durable: 'slow',
    config: { filter_subject: 'ord.slow' },
  })
  async on() {
    this.started++;
    await sleep(300);
    this.done++;
  }
}

describe('@Consume', () => {
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
      // Failed messages are nak'd with a backoff: keep it short for the redelivery tests.
      retry: { initialDelay: 50, jitter: 0 },
      streams: [{ name: 'ORD', subjects: ['ord.>'] }],
    });
  const ackPending = async (durable: string) =>
    (await jsm.consumers.info('ORD', durable)).num_ack_pending;

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

  it('creates the consumer from config and acks on success', async () => {
    await js.publish('ord.created', json({ id: 1 }));
    await eventually(() => expect(handlers.billing).toEqual([{ id: 1 }]));
    await eventually(async () => expect(await ackPending('billing')).toBe(0));
    expect((await jsm.consumers.info('ORD', 'billing')).config.max_deliver).toBe(-1);
  });

  it('naks on error with a backoff so the message is redelivered', async () => {
    await js.publish('ord.flaky', json({ id: 2 }));
    await eventually(() => expect(handlers.deliveries['ord.flaky']).toEqual([1, 2]));
    await eventually(async () => expect(await ackPending('flaky')).toBe(0));
  });

  it('terms non-JSON payloads instead of redelivering', async () => {
    const callsBefore = handlers.billing.length;
    const { seq } = await js.publish('ord.created', enc.encode('hello'));
    await eventually(async () => {
      const info = await jsm.consumers.info('ORD', 'billing');
      expect(info.delivered.stream_seq).toBeGreaterThanOrEqual(seq);
      expect(info.num_ack_pending).toBe(0);
    });
    await sleep(1000);
    expect(handlers.billing.length).toBe(callsBefore);
    expect(await ackPending('billing')).toBe(0);
  });

  it('respects manual ack', async () => {
    await js.publish('ord.manual', json({ id: 1 }));
    await eventually(() => expect(handlers.manual).toHaveLength(1));
    await sleep(200);
    expect(await ackPending('manual')).toBe(1);
    await js.publish('ord.manual', json({ id: 2, ack: true }));
    await eventually(() => expect(handlers.manual).toHaveLength(2));
    // Only the second message is acked; the first stays pending until acked explicitly.
    await eventually(async () => expect(await ackPending('manual')).toBe(1));
    handlers.manual[0]?.ctx.ack();
    await eventually(async () => expect(await ackPending('manual')).toBe(0));
  });

  it('keeps order with concurrency 1', async () => {
    for (let i = 0; i < 20; i++) await js.publish('ord.seq', json(i));
    await eventually(() => expect(handlers.seq).toHaveLength(20));
    expect(handlers.seq).toEqual(Array.from({ length: 20 }, (_, i) => i));
  });

  it('processes in parallel with concurrency 4', async () => {
    const start = Date.now();
    await Promise.all([1, 2, 3, 4].map((i) => js.publish('ord.par', json(i))));
    await eventually(() => expect(handlers.parallel.done).toBe(4));
    expect(Date.now() - start).toBeLessThan(600);
    expect(handlers.parallel.max).toBeGreaterThan(1);
    expect(handlers.parallel.max).toBeLessThanOrEqual(4);
  });

  it('consumes a pre-created consumer without config', async () => {
    await jsm.consumers.add('ORD', {
      durable_name: 'precreated',
      ack_policy: AckPolicy.Explicit,
      filter_subject: 'ord.pre',
    });
    const preApp = await createTestApp({
      imports: [NatsModule.forRoot({ connection: { servers: srv.url }, logger: false })],
      providers: [PreCreatedHandlers],
    });
    try {
      await js.publish('ord.pre', json({ pre: true }));
      await eventually(() => expect(preApp.get(PreCreatedHandlers).got).toEqual([{ pre: true }]));
    } finally {
      await preApp.close();
    }
  });

  it('fails bootstrap when consumer is missing and no config', async () => {
    await expect(
      createTestApp({ imports: [nats()], providers: [MissingConsumerHandlers] }),
    ).rejects.toThrow(
      new NatsConfigError(
        'Consumer "ORD/ghost" not found and no config declared for MissingConsumerHandlers.on',
      ),
    );
  });

  it('says config is declared but not synced when sync mode is none', async () => {
    await expect(
      createTestApp({
        imports: [
          NatsModule.forRoot({
            connection: { servers: srv.url },
            logger: false,
            jetstream: { sync: 'none' },
          }),
        ],
        providers: [SyncNoneHandlers],
      }),
    ).rejects.toThrow(
      /Consumer "ORD\/unsynced" not found for SyncNoneHandlers\.on; its config is declared/,
    );
  });

  it('logs when the consumer is deleted at runtime', async () => {
    const logger = { log: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const doomedApp = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger,
          streams: [{ name: 'ORD', subjects: ['ord.>'] }],
        }),
      ],
      providers: [DoomedHandlers],
    });
    try {
      await jsm.consumers.delete('ORD', 'doomed');
      await eventually(() => {
        const logged = [...logger.error.mock.calls, ...logger.warn.mock.calls].flat().join('\n');
        expect(logged).toMatch(/DoomedHandlers\.on.*ORD\/doomed/);
      });
    } finally {
      await doomedApp.close();
    }
  });

  it('fails bootstrap on duplicate durable', async () => {
    await expect(
      createTestApp({ imports: [nats()], providers: [DuplicateDurableHandlers] }),
    ).rejects.toThrow(NatsConfigError);
  });

  it('waits for in-flight handlers on shutdown and does not start queued messages', async () => {
    const slowApp = await createTestApp({ imports: [nats()], providers: [SlowHandlers] });
    const slow = slowApp.get(SlowHandlers);
    for (let i = 0; i < 3; i++) await js.publish('ord.slow', json(i));
    await eventually(() => expect(slow.started).toBe(1));
    await slowApp.close();
    // The in-flight message finished; the others were handed back without being handled.
    expect(slow.done).toBe(slow.started);
    expect(slow.started).toBe(1);
  });
});
