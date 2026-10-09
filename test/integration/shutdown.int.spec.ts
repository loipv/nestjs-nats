import {
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream';
import { connect, type NatsConnection } from '@nats-io/transport-node';
import { Injectable } from '@nestjs/common';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NatsConnectionManager } from '../../src/connection/connection-manager.js';
import type { NatsJetStreamContext } from '../../src/context/nats-context.js';
import { Consume, Subscribe } from '../../src/decorators/handler.decorators.js';
import { Ctx } from '../../src/decorators/param.decorators.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();

@Injectable()
class GoneHandlers {
  @Subscribe('gone.core')
  onCore() {}

  @Consume({ stream: 'GONE', durable: 'gone', config: { filter_subject: 'gone.js' } })
  onJs() {}
}

@Injectable()
class StuckHandlers {
  started = 0;
  finished = 0;

  @Consume({ stream: 'SHUT', durable: 'stuck', config: { filter_subject: 'shut.stuck' } })
  async on() {
    this.started++;
    await sleep(3000);
    this.finished++;
  }
}

@Injectable()
class RecoveringHandlers {
  readonly deliveries: number[] = [];

  @Consume({ stream: 'SHUT', durable: 'stuck', config: { filter_subject: 'shut.stuck' } })
  on(@Ctx() ctx: NatsJetStreamContext) {
    this.deliveries.push(ctx.deliveryCount);
  }
}

@Injectable()
class BriefHandlers {
  started = 0;
  finished = 0;

  @Consume({ stream: 'SHUT', durable: 'brief', config: { filter_subject: 'shut.brief' } })
  async on() {
    this.started++;
    await sleep(200);
    this.finished++;
  }
}

describe('graceful shutdown when the server is gone', () => {
  let srv: NatsTestServer | undefined;

  afterAll(async () => {
    await srv?.stop().catch(() => {});
  });

  it('does not hang when the server is gone', async () => {
    srv = await startNats();
    const app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger: false,
          shutdown: { drainTimeout: 500 },
          streams: [{ name: 'GONE', subjects: ['gone.>'] }],
        }),
      ],
      providers: [GoneHandlers],
    });
    const manager = app.get(NatsConnectionManager);
    await srv.stop();
    srv = undefined;
    await eventually(() => expect(manager.status().state).toBe('reconnecting'));

    const t0 = Date.now();
    await app.close();
    expect(Date.now() - t0).toBeLessThan(2 * 500 + 1500);
    expect(manager.status().state).toBe('closed');
  });
});

describe('graceful shutdown', () => {
  let srv: NatsTestServer;
  let rawNc: NatsConnection;
  let js: JetStreamClient;
  let jsm: JetStreamManager;
  const nats = (drainTimeout: number) =>
    NatsModule.forRoot({
      connection: { servers: srv.url },
      logger: false,
      shutdown: { drainTimeout },
      streams: [{ name: 'SHUT', subjects: ['shut.>'] }],
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

  it('naks in-flight JetStream messages past drainTimeout', async () => {
    const app = await createTestApp({ imports: [nats(300)], providers: [StuckHandlers] });
    const stuck = app.get(StuckHandlers);
    await js.publish('shut.stuck', enc.encode('{}'));
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
        expect((await jsm.consumers.info('SHUT', 'stuck')).num_ack_pending).toBe(0),
      );
    } finally {
      await next.close();
    }
    // The abandoned handler still finishes; settling its nak'd message must not fail.
    await eventually(() => expect(stuck.finished).toBe(1), { timeout: 5000 });
  });

  it('waits for in-flight handlers within drainTimeout', async () => {
    const app = await createTestApp({ imports: [nats(2000)], providers: [BriefHandlers] });
    const brief = app.get(BriefHandlers);
    await js.publish('shut.brief', enc.encode('{}'));
    await eventually(() => expect(brief.started).toBe(1));

    await app.close();
    expect(brief.finished).toBe(1);
    const info = await jsm.consumers.info('SHUT', 'brief');
    expect(info.num_ack_pending).toBe(0);
    expect(info.num_redelivered).toBe(0);
  });
});
