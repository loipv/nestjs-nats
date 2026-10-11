import {
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
  type StoredMsg,
} from '@nats-io/jetstream';
import { connect, type NatsConnection } from '@nats-io/transport-node';
import { BadRequestException, Injectable, type LoggerService } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { NatsClient } from '../../src/client/nats-client.js';
import { ConnectionRegistry } from '../../src/connection/connection-registry.js';
import type { NatsContext, NatsJetStreamContext } from '../../src/context/nats-context.js';
import { Consume, Subscribe } from '../../src/decorators/handler.decorators.js';
import { Ctx } from '../../src/decorators/param.decorators.js';
import { NatsRequestError, NonRetryableError } from '../../src/errors/errors.js';
import { NatsModule } from '../../src/module/nats.module.js';
import type { InboundMessage } from '../../src/pipeline/delivery.js';
import type { NatsPlugin } from '../../src/pipeline/types.js';
import { sendToDlq } from '../../src/settlement/dlq.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();
const json = (v: unknown) => enc.encode(JSON.stringify(v));

const RETRY = { initialDelay: 50, jitter: 0, maxRetries: 2 };

/** Records, per subject, each attempt and when it happened. */
class Attempts {
  readonly log: Record<string, Array<{ attempt: number; at: number }>> = {};

  record(subject: string, attempt: number): void {
    const list = this.log[subject] ?? [];
    this.log[subject] = list;
    list.push({ attempt, at: Date.now() });
  }

  attempts(subject: string): number[] {
    return (this.log[subject] ?? []).map((a) => a.attempt);
  }

  gaps(subject: string): number[] {
    const list = this.log[subject] ?? [];
    return list.slice(1).map((a, i) => a.at - (list[i]?.at ?? 0));
  }
}

@Injectable()
class Handlers extends Attempts {
  @Consume({
    stream: 'ORD',
    durable: 'fail-consumer',
    dlq: true,
    config: { filter_subject: 'ord.fail' },
  })
  onFail(@Ctx() ctx: NatsJetStreamContext) {
    this.record(ctx.subject, ctx.deliveryCount);
    throw new Error('always fails');
  }

  @Consume({
    stream: 'ORD',
    durable: 'bad-consumer',
    dlq: true,
    config: { filter_subject: 'ord.bad' },
  })
  onBad(@Ctx() ctx: NatsJetStreamContext) {
    this.record(ctx.subject, ctx.deliveryCount);
    throw new BadRequestException('invalid order');
  }

  @Consume({ stream: 'ORD', durable: 'nodlq', config: { filter_subject: 'ord.nodlq' } })
  onNoDlq(@Ctx() ctx: NatsJetStreamContext) {
    this.record(ctx.subject, ctx.deliveryCount);
    throw new Error('no dlq');
  }

  @Subscribe('orders.x', { dlq: true })
  onCore(@Ctx() ctx: NatsContext) {
    // Core messages carry no delivery count: count the calls.
    this.record(ctx.subject, (this.log[ctx.subject]?.length ?? 0) + 1);
    throw new Error('core fails');
  }

  @Subscribe('rpc.fail')
  onRpc(): never {
    throw new Error('secret internals');
  }
}

@Injectable()
class NowhereHandlers extends Attempts {
  @Consume({
    stream: 'ORD',
    durable: 'nowhere',
    retry: { maxRetries: 0, initialDelay: 10_000, jitter: 0 },
    dlq: { subject: 'nowhere.x' },
    config: { filter_subject: 'ord.nowhere' },
  })
  on(@Ctx() ctx: NatsJetStreamContext) {
    this.record(ctx.subject, ctx.deliveryCount);
    throw new Error('fails');
  }
}

/** Two consumers of the same stream both failing the same poison message. */
@Injectable()
class SharedDlqHandlers extends Attempts {
  @Consume({
    stream: 'ORD',
    durable: 'billing',
    retry: false,
    dlq: true,
    config: { filter_subject: 'ord.poison' },
  })
  onBilling(@Ctx() ctx: NatsJetStreamContext) {
    this.record(`billing:${ctx.subject}`, ctx.deliveryCount);
    throw new Error('billing fails');
  }

  @Consume({
    stream: 'ORD',
    durable: 'shipping',
    retry: false,
    dlq: true,
    config: { filter_subject: 'ord.poison' },
  })
  onShipping(@Ctx() ctx: NatsJetStreamContext) {
    this.record(`shipping:${ctx.subject}`, ctx.deliveryCount);
    throw new Error('shipping fails');
  }
}

@Injectable()
class SlowHandlers extends Attempts {
  done = 0;

  @Consume({
    stream: 'ORD',
    durable: 'slow-hb',
    config: { filter_subject: 'ord.slow', ack_wait: 2_000_000_000 },
  })
  async on(@Ctx() ctx: NatsJetStreamContext) {
    this.record(ctx.subject, ctx.deliveryCount);
    await sleep(5000);
    this.done++;
  }
}

/** One slot, a 2 s ack_wait and 5 s of work: queued messages outlive several ack_waits. */
@Injectable()
class QueuedSlowHandlers {
  readonly calls: string[] = [];
  done = 0;

  @Consume({
    stream: 'ORD',
    durable: 'slow-queued',
    concurrency: 1,
    retry: false,
    config: { filter_subject: 'ord.queued', ack_wait: 2_000_000_000 },
  })
  async on(@Ctx() ctx: NatsJetStreamContext) {
    this.calls.push(`${ctx.seq}#${ctx.deliveryCount}`);
    await sleep(5000);
    this.done++;
  }
}

@Injectable()
class PluginDlqHandlers {
  @Consume({
    stream: 'ORD',
    durable: 'plugin-dl',
    dlq: true,
    config: { filter_subject: 'ord.1' },
  })
  on(): never {
    throw new NonRetryableError('poison');
  }
}

@Injectable()
class MaxDeliverHandlers {
  @Consume({
    stream: 'ORD',
    durable: 'md',
    retry: { maxRetries: 5 },
    config: { filter_subject: 'ord.md', max_deliver: 2 },
  })
  on() {}
}

function recordingLogger() {
  return { log: vi.fn(), warn: vi.fn(), error: vi.fn() } satisfies LoggerService;
}

const lines = (fn: { mock: { calls: unknown[][] } }) =>
  fn.mock.calls.map((c) => c.map(String).join(' '));

describe('retry, backoff and DLQ', () => {
  let srv: NatsTestServer;
  let rawNc: NatsConnection;
  let js: JetStreamClient;
  let jsm: JetStreamManager;
  let app: TestingModule;
  let handlers: Handlers;
  let logger: ReturnType<typeof recordingLogger>;

  const nats = (log: LoggerService | false = false) =>
    NatsModule.forRoot({
      connection: { servers: srv.url },
      logger: log,
      retry: RETRY,
      streams: [
        { name: 'ORD', subjects: ['ord.>'] },
        { name: 'DLQ', subjects: ['dlq.>'] },
      ],
    });

  const nextFromStream = (stream: string, subject: string, timeout = 10_000) =>
    eventually<StoredMsg>(
      async () => {
        const m = await jsm.streams.getMessage(stream, { last_by_subj: subject });
        if (!m) throw new Error(`no message on ${subject}`);
        return m;
      },
      { timeout, interval: 50 },
    );

  const consumerInfo = (durable: string) => jsm.consumers.info('ORD', durable);

  beforeAll(async () => {
    srv = await startNats();
    rawNc = await connect({ servers: srv.url });
    js = jetstream(rawNc);
    jsm = await jetstreamManager(rawNc);
    logger = recordingLogger();
    app = await createTestApp({ imports: [nats(logger)], providers: [Handlers] });
    handlers = app.get(Handlers);
  });

  afterAll(async () => {
    await app?.close();
    await rawNc?.close();
    await srv?.stop();
  });

  it('retries with backoff then sends to DLQ with headers', async () => {
    await js.publish('ord.fail', json({ id: 1 }));
    const m = await nextFromStream('DLQ', 'dlq.ORD.fail-consumer');
    expect(handlers.attempts('ord.fail')).toEqual([1, 2, 3]);
    expect(m.header.get('Nats-DLQ-Deliveries')).toBe('3');
    expect(m.header.get('Nats-DLQ-Stream')).toBe('ORD');
    expect(m.header.get('Nats-DLQ-Consumer')).toBe('fail-consumer');
    expect(m.header.get('Nats-DLQ-Subject')).toBe('ord.fail');
    expect(m.header.get('Nats-DLQ-Error')).toBe('always fails');
    expect(m.json()).toEqual({ id: 1 });
    // Backoff of 50 ms then 100 ms (wide tolerance for scheduling noise).
    const [first, second] = handlers.gaps('ord.fail');
    expect(first).toBeGreaterThanOrEqual(40);
    expect(first).toBeLessThan(1000);
    expect(second).toBeGreaterThanOrEqual(90);
    expect(second).toBeLessThan(1500);
    await eventually(async () =>
      expect((await consumerInfo('fail-consumer')).num_ack_pending).toBe(0),
    );
    expect(lines(logger.warn).filter((l) => l.includes('ord.fail'))).toEqual([
      expect.stringMatching(/\(attempt 1\/3\), retrying in 50ms: always fails/),
      expect.stringMatching(/\(attempt 2\/3\), retrying in 100ms: always fails/),
    ]);
    expect(lines(logger.error)).toContainEqual(
      expect.stringMatching(/sent to DLQ dlq\.ORD\.fail-consumer after 3 attempts: always fails/),
    );
  });

  it('sends non-retryable errors to DLQ on the first attempt', async () => {
    await js.publish('ord.bad', json({ id: 2 }));
    const m = await nextFromStream('DLQ', 'dlq.ORD.bad-consumer');
    expect(m.json()).toEqual({ id: 2 });
    expect(m.header.get('Nats-DLQ-Deliveries')).toBe('1');
    await sleep(300);
    expect(handlers.attempts('ord.bad')).toEqual([1]);
  });

  it('terms without DLQ when DLQ is off', async () => {
    await js.publish('ord.nodlq', json({ id: 3 }));
    await eventually(() => expect(handlers.attempts('ord.nodlq')).toEqual([1, 2, 3]));
    await eventually(async () => expect((await consumerInfo('nodlq')).num_ack_pending).toBe(0));
    await sleep(500);
    expect(handlers.attempts('ord.nodlq')).toEqual([1, 2, 3]);
    await expect(jsm.streams.getMessage('DLQ', { last_by_subj: 'dlq.ORD.nodlq' })).resolves.toBe(
      null,
    );
    expect(lines(logger.error)).toContainEqual(
      expect.stringMatching(/ord\.nodlq.*gave up after 3 attempts: no dlq/),
    );
  });

  it('keeps one dead letter per consumer on a shared DLQ stream, and dedupes re-sends', async () => {
    const sharedApp = await createTestApp({ imports: [nats()], providers: [SharedDlqHandlers] });
    try {
      const ack = await js.publish('ord.poison', json({ id: 9 }), { msgID: 'poison-1' });
      const billing = await nextFromStream('DLQ', 'dlq.ORD.billing');
      const shipping = await nextFromStream('DLQ', 'dlq.ORD.shipping');
      expect(billing.json()).toEqual({ id: 9 });
      expect(shipping.json()).toEqual({ id: 9 });
      expect(billing.header.get('Nats-Msg-Id')).toBe(`dlq:ORD:billing:${ack.seq}`);
      expect(shipping.header.get('Nats-Msg-Id')).toBe(`dlq:ORD:shipping:${ack.seq}`);
      expect(billing.header.get('Nats-DLQ-Msg-Id')).toBe('poison-1');
      expect(shipping.header.get('Nats-DLQ-Msg-Id')).toBe('poison-1');
      expect(billing.header.get('Nats-DLQ-Error')).toBe('billing fails');
      expect(shipping.header.get('Nats-DLQ-Error')).toBe('shipping fails');
      const counts = async () =>
        (await jsm.streams.info('DLQ', { subjects_filter: 'dlq.ORD.*' })).state.subjects ?? {};
      expect(await counts()).toMatchObject({ 'dlq.ORD.billing': 1, 'dlq.ORD.shipping': 1 });

      // The same dead letter sent again (same stream sequence and consumer) is deduplicated.
      const entry = sharedApp.get(ConnectionRegistry).get();
      const again: InboundMessage = {
        subject: 'ord.poison',
        data: json({ id: 9 }),
        raw: { info: { stream: 'ORD', consumer: 'billing', streamSequence: ack.seq } } as never,
        attempt: 1,
      };
      await sendToDlq(entry, 'dlq.ORD.billing', 'jetstream', again, new Error('again'), 1, {
        kind: 'consume',
        stream: 'ORD',
        durable: 'billing',
      } as never);
      expect(await counts()).toMatchObject({ 'dlq.ORD.billing': 1, 'dlq.ORD.shipping': 1 });
      expect(sharedApp.get(SharedDlqHandlers).attempts('billing:ord.poison')).toEqual([1]);
      expect(sharedApp.get(SharedDlqHandlers).attempts('shipping:ord.poison')).toEqual([1]);
    } finally {
      await sharedApp.close();
    }
  });

  it('naks and logs when no stream captures the DLQ subject', async () => {
    const log = recordingLogger();
    const nowhereApp = await createTestApp({ imports: [nats(log)], providers: [NowhereHandlers] });
    try {
      const h = nowhereApp.get(NowhereHandlers);
      await js.publish('ord.nowhere', json({ id: 4 }));
      await eventually(() =>
        expect(lines(log.error)).toContainEqual(
          expect.stringMatching(/nowhere\.x.*no stream captures this subject/),
        ),
      );
      expect(h.attempts('ord.nowhere')).toEqual([1]);
      // Nak'd with a 10 s backoff: still pending, not lost.
      const info = await consumerInfo('nowhere');
      expect(info.num_ack_pending).toBe(1);
    } finally {
      await nowhereApp.close();
    }
  });

  it('retries core subscriptions in-process then publishes to core DLQ', async () => {
    const sub = rawNc.subscribe('dlq.orders.x', { max: 1 });
    const received = (async () => {
      for await (const m of sub) return m;
      return undefined;
    })();
    await rawNc.flush();
    rawNc.publish('orders.x', json({ id: 5 }));
    const m = await received;
    expect(m?.json()).toEqual({ id: 5 });
    expect(m?.headers?.get('Nats-DLQ-Deliveries')).toBe('3');
    expect(m?.headers?.get('Nats-DLQ-Subject')).toBe('orders.x');
    expect(handlers.attempts('orders.x')).toEqual([1, 2, 3]);
  });

  it('keeps a slow handler alive with heartbeats', async () => {
    const slowApp = await createTestApp({ imports: [nats()], providers: [SlowHandlers] });
    try {
      const slow = slowApp.get(SlowHandlers);
      await js.publish('ord.slow', json({ id: 6 }));
      await eventually(() => expect(slow.done).toBe(1), { timeout: 15_000 });
      await eventually(async () => expect((await consumerInfo('slow-hb')).num_ack_pending).toBe(0));
      expect(slow.attempts('ord.slow')).toEqual([1]);
    } finally {
      await slowApp.close();
    }
  }, 30_000);

  it('keeps messages waiting for a slot alive: each is handled exactly once', async () => {
    const queuedApp = await createTestApp({ imports: [nats()], providers: [QueuedSlowHandlers] });
    try {
      const h = queuedApp.get(QueuedSlowHandlers);
      const seqs: number[] = [];
      for (const id of [1, 2, 3]) seqs.push((await js.publish('ord.queued', json({ id }))).seq);
      await eventually(() => expect(h.done).toBe(3), { timeout: 25_000, interval: 100 });
      await eventually(async () =>
        expect((await consumerInfo('slow-queued')).num_ack_pending).toBe(0),
      );
      // A redelivery would show up as a second call (or a delivery count > 1).
      await sleep(2500);
      expect(h.calls).toEqual(seqs.map((seq) => `${seq}#1`));
      expect((await consumerInfo('slow-queued')).num_redelivered).toBe(0);
    } finally {
      await queuedApp.close();
    }
  }, 45_000);

  it('warns when max_deliver <= maxRetries', async () => {
    const log = recordingLogger();
    const mdApp = await createTestApp({ imports: [nats(log)], providers: [MaxDeliverHandlers] });
    try {
      expect(lines(log.warn)).toContainEqual(
        expect.stringMatching(/ORD\/md.*MaxDeliverHandlers\.on.*max_deliver 2.*maxRetries 5/),
      );
    } finally {
      await mdApp.close();
    }
  });

  it('logs 5xx replies once', async () => {
    const client = app.get(NatsClient);
    await expect(client.request('rpc.fail', {})).rejects.toBeInstanceOf(NatsRequestError);
    await eventually(() =>
      expect(lines(logger.warn).filter((l) => l.includes('rpc.fail'))).toEqual([
        'Replied 500 to request on "rpc.fail" for Handlers.onRpc: secret internals',
      ]),
    );
    expect(lines(logger.error).filter((l) => l.includes('rpc.fail'))).toEqual([]);
  });
});

describe('dead letters and outbound plugins', () => {
  it('dead letters go through the outbound plugins', async () => {
    const srv = await startNats();
    const rawNc = await connect({ servers: srv.url });
    const jsm = await jetstreamManager(rawNc);
    const plugin: NatsPlugin = {
      name: 'mark-dead-letter',
      outbound: {
        async handle(m, next) {
          if (m.deadLetter) m.headers.set('x-dead-letter-by', 'plugin');
          await next();
        },
      },
    };
    const app = await createTestApp(
      {
        imports: [
          NatsModule.forRoot({
            connection: { servers: srv.url },
            logger: false,
            plugins: [plugin],
            streams: [
              { name: 'ORD', subjects: ['ord.>'] },
              { name: 'DLQ', subjects: ['dlq.>'] },
            ],
          }),
        ],
        providers: [PluginDlqHandlers],
      },
      { silent: true },
    );
    try {
      await jetstream(rawNc).publish('ord.1', json({ id: 1 }));
      const m = await eventually<StoredMsg>(
        async () => {
          const got = await jsm.streams.getMessage('DLQ', { last_by_subj: 'dlq.ORD.plugin-dl' });
          if (!got) throw new Error('no dead letter yet');
          return got;
        },
        { timeout: 10_000, interval: 50 },
      );
      expect(m.header.get('x-dead-letter-by')).toBe('plugin');
      expect(m.header.get('Nats-DLQ-Subject')).toBe('ord.1');
    } finally {
      await app.close();
      await rawNc.close();
      await srv.stop();
    }
  });
});
