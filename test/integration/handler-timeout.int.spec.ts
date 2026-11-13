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
import type { NatsJetStreamContext } from '../../src/context/nats-context.js';
import { Consume } from '../../src/decorators/handler.decorators.js';
import { Ctx, Payload } from '../../src/decorators/param.decorators.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();

/** Resolves when `signal` aborts. */
const aborted = (signal: AbortSignal) =>
  new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));

@Injectable()
class Handlers {
  /** What happened, in order: `start <id>#<delivery>`, `done <id>#<delivery>`. */
  readonly events: string[] = [];
  idemRuns = 0;
  idemDone = 0;

  @Consume({
    stream: 'TMO',
    durable: 'hang',
    timeout: 200,
    config: { filter_subject: 'tmo.hang' },
  })
  async onHang(@Payload() body: { id: number }, @Ctx() ctx: NatsJetStreamContext) {
    const at = `${body.id}#${ctx.deliveryCount}`;
    this.events.push(`start ${at}`);
    if (body.id === 1 && ctx.deliveryCount === 1) {
      // Notices the timeout, then keeps running past it.
      await aborted(ctx.signal);
      await sleep(300);
    }
    this.events.push(`done ${at}`);
  }

  @Consume({
    stream: 'TMO',
    durable: 'idem',
    timeout: 200,
    idempotency: true,
    config: { filter_subject: 'tmo.idem' },
  })
  async onIdem() {
    this.idemRuns++;
    // Outlives its timeout, then succeeds.
    await sleep(500);
    this.idemDone++;
  }
}

describe('handler timeout', () => {
  let srv: NatsTestServer;
  let nc: NatsConnection;
  let js: JetStreamClient;
  let jsm: JetStreamManager;
  let app: TestingModule;
  let handlers: Handlers;

  const settled = async (durable: string) => {
    const info = await jsm.consumers.info('TMO', durable);
    expect(info.num_ack_pending).toBe(0);
    expect(info.num_pending).toBe(0);
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
          retry: { initialDelay: 50, jitter: 0, maxRetries: 10 },
          streams: [{ name: 'TMO', subjects: ['tmo.>'] }],
          shutdown: { drainTimeout: 2000 },
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

  it('frees the consumer of a run that outlived its timeout and redelivers its message', async () => {
    await js.publish('tmo.hang', enc.encode(JSON.stringify({ id: 1 })));
    await js.publish('tmo.hang', enc.encode(JSON.stringify({ id: 2 })));
    await eventually(
      () => {
        expect(handlers.events).toContain('done 1#2');
        expect(handlers.events).toContain('done 1#1');
      },
      { timeout: 10_000 },
    );
    const { events } = handlers;
    // Message 2 did not wait for the run of message 1 that outlived its timeout.
    expect(events.indexOf('done 2#1')).toBeGreaterThan(-1);
    expect(events.indexOf('done 2#1')).toBeLessThan(events.indexOf('done 1#1'));
    await eventually(() => settled('hang'));
  });

  it('acks the redelivery of a message whose timed-out run succeeded, without running it again', async () => {
    await js.publish('tmo.idem', enc.encode('{}'), { msgID: 'idem-1' });
    await eventually(() => expect(handlers.idemDone).toBe(1), { timeout: 10_000 });
    await eventually(() => settled('idem'), { timeout: 10_000 });
    await sleep(300);
    expect(handlers.idemRuns).toBe(1);
    expect(handlers.idemDone).toBe(1);
  });
});
