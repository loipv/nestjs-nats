import {
  AckPolicy,
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
  type StoredMsg,
} from '@nats-io/jetstream';
import { connect, type NatsConnection } from '@nats-io/transport-node';
import { Injectable } from '@nestjs/common';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { NatsJetStreamContext } from '../../src/context/nats-context.js';
import { ConsumePush } from '../../src/decorators/handler.decorators.js';
import { Ctx, Payload } from '../../src/decorators/param.decorators.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();
const json = (v: unknown) => enc.encode(JSON.stringify(v));

@Injectable()
class GroupHandlers {
  readonly ids: number[] = [];

  @ConsumePush({ stream: 'PG', durable: 'p1', deliverGroup: 'g', config: {} })
  async on(@Payload() id: number) {
    await sleep(5);
    this.ids.push(id);
  }
}

@Injectable()
class DefaultsHandlers {
  @ConsumePush({ stream: 'PD', durable: 'p2', concurrency: 2, config: {} })
  on() {}

  @ConsumePush({ stream: 'PD', durable: 'p2g', deliverGroup: 'g', concurrency: 2, config: {} })
  onGroup() {}
}

/**
 * Two slots, four pending messages (two of them queued on the client), a 2 s ack_wait and
 * 5 s of work: only heartbeats keep the messages.
 */
@Injectable()
class SlowHandlers {
  readonly deliveries: number[] = [];
  done = 0;

  @ConsumePush({ stream: 'PH', durable: 'ph', concurrency: 2, retry: false })
  async on(@Ctx() ctx: NatsJetStreamContext) {
    this.deliveries.push(ctx.deliveryCount);
    await sleep(5000);
    this.done++;
  }
}

@Injectable()
class FailingHandlers {
  readonly deliveries: number[] = [];

  @ConsumePush({
    stream: 'PR',
    durable: 'p4',
    retry: { maxRetries: 1, initialDelay: 50 },
    dlq: true,
    config: {},
  })
  on(@Ctx() ctx: NatsJetStreamContext) {
    this.deliveries.push(ctx.deliveryCount);
    throw new Error('always fails');
  }
}

@Injectable()
class PullConsumerHandlers {
  @ConsumePush({ stream: 'PP', durable: 'p3' })
  on() {}
}

@Injectable()
class StuckHandlers {
  started = 0;
  finished = 0;

  @ConsumePush({ stream: 'PS', durable: 'p5', config: {} })
  async on() {
    this.started++;
    await sleep(3000);
    this.finished++;
  }
}

@Injectable()
class RecoveringHandlers {
  readonly deliveries: number[] = [];

  @ConsumePush({ stream: 'PS', durable: 'p5', config: {} })
  on(@Ctx() ctx: NatsJetStreamContext) {
    this.deliveries.push(ctx.deliveryCount);
  }
}

describe('@ConsumePush', () => {
  let srv: NatsTestServer;
  let rawNc: NatsConnection;
  let js: JetStreamClient;
  let jsm: JetStreamManager;
  const nats = (drainTimeout?: number) =>
    NatsModule.forRoot({
      connection: { servers: srv.url },
      logger: false,
      ...(drainTimeout !== undefined ? { shutdown: { drainTimeout } } : {}),
      streams: ['PG', 'PD', 'PH', 'PR', 'PP', 'PS', 'DLQ'].map((name) => ({
        name,
        subjects: [`${name.toLowerCase()}.>`],
      })),
    });

  beforeAll(async () => {
    srv = await startNats();
    rawNc = await connect({ servers: srv.url });
    js = jetstream(rawNc);
    jsm = await jetstreamManager(rawNc);
  });

  afterAll(async () => {
    await rawNc?.close();
    await srv?.stop();
  });

  it('shares messages across a deliver group', async () => {
    const a = await createTestApp({ imports: [nats()], providers: [GroupHandlers] });
    const b = await createTestApp({ imports: [nats()], providers: [GroupHandlers] });
    try {
      const [ha, hb] = [a.get(GroupHandlers), b.get(GroupHandlers)];
      const ids = Array.from({ length: 20 }, (_, i) => i);
      for (const id of ids) await js.publish('pg.x', json(id));
      await eventually(() => expect(ha.ids.length + hb.ids.length).toBe(20), { timeout: 10_000 });
      await sleep(300);
      expect([...ha.ids, ...hb.ids].sort((x, y) => x - y)).toEqual(ids);
      expect(ha.ids.length).toBeGreaterThanOrEqual(1);
      expect(hb.ids.length).toBeGreaterThanOrEqual(1);
    } finally {
      await a.close();
      await b.close();
    }
  });

  it('creates the push consumer with its defaults', async () => {
    const app = await createTestApp({ imports: [nats()], providers: [DefaultsHandlers] });
    try {
      const { config } = await jsm.consumers.info('PD', 'p2');
      expect(config).toMatchObject({
        deliver_subject: '_nats.push.PD.p2',
        flow_control: true,
        idle_heartbeat: 5_000_000_000,
        max_ack_pending: 2,
        ack_policy: AckPolicy.Explicit,
      });
      // A group shares max_ack_pending across its instances: the server default applies.
      const { config: group } = await jsm.consumers.info('PD', 'p2g');
      expect(group).toMatchObject({ deliver_subject: '_nats.push.PD.p2g', deliver_group: 'g' });
      expect(group.max_ack_pending).toBe(1000);
      expect(group.flow_control).toBeFalsy();
      expect(group.idle_heartbeat).toBeFalsy();
    } finally {
      await app.close();
    }
  });

  it('heartbeats queued messages', async () => {
    await jsm.consumers.add('PH', {
      durable_name: 'ph',
      ack_policy: AckPolicy.Explicit,
      ack_wait: 2_000_000_000,
      max_ack_pending: 4,
      deliver_subject: '_nats.push.PH.ph',
    });
    const app = await createTestApp({ imports: [nats()], providers: [SlowHandlers] });
    try {
      const slow = app.get(SlowHandlers);
      for (let i = 0; i < 4; i++) await js.publish('ph.x', json(i));
      await eventually(() => expect(slow.done).toBe(4), { timeout: 20_000 });
      // No redelivery past the 2 s ack_wait: each message was handled exactly once.
      await sleep(500);
      expect(slow.deliveries).toEqual([1, 1, 1, 1]);
    } finally {
      await app.close();
    }
  });

  it('retries then dead-letters', async () => {
    const app = await createTestApp({ imports: [nats()], providers: [FailingHandlers] });
    try {
      const failing = app.get(FailingHandlers);
      await js.publish('pr.x', json({ id: 1 }));
      const dead = await eventually<StoredMsg>(
        async () => {
          const m = await jsm.streams.getMessage('DLQ', { last_by_subj: 'dlq.PR.p4' });
          if (!m) throw new Error('no dead letter');
          return m;
        },
        { timeout: 10_000, interval: 50 },
      );
      expect(dead.header.get('Nats-DLQ-Consumer')).toBe('p4');
      expect((await jsm.streams.info('DLQ')).state.messages).toBe(1);
      expect(failing.deliveries).toEqual([1, 2]);
    } finally {
      await app.close();
    }
  });

  it('rejects a pull consumer', async () => {
    await jsm.consumers.add('PP', { durable_name: 'p3', ack_policy: AckPolicy.Explicit });
    const err = await createTestApp({ imports: [nats()], providers: [PullConsumerHandlers] }).catch(
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(NatsConfigError);
    expect((err as Error).message).toMatch(
      /Consumer "PP\/p3" for PullConsumerHandlers\.on is not a push consumer/,
    );
  });

  it('naks in-flight messages at shutdown', async () => {
    const app = await createTestApp({ imports: [nats(300)], providers: [StuckHandlers] });
    const stuck = app.get(StuckHandlers);
    await js.publish('ps.x', json({}));
    await eventually(() => expect(stuck.started).toBe(1));

    const t0 = Date.now();
    await app.close();
    expect(Date.now() - t0).toBeLessThan(1500);
    expect(stuck.finished).toBe(0);

    const next = await createTestApp({ imports: [nats(300)], providers: [RecoveringHandlers] });
    try {
      const recovering = next.get(RecoveringHandlers);
      // Redelivered at once (nak), long before the consumer's 30 s ack_wait.
      await eventually(() => expect(recovering.deliveries).toEqual([2]), { timeout: 3000 });
      await eventually(async () =>
        expect((await jsm.consumers.info('PS', 'p5')).num_ack_pending).toBe(0),
      );
    } finally {
      await next.close();
    }
    await eventually(() => expect(stuck.finished).toBe(1), { timeout: 5000 });
  });
});
