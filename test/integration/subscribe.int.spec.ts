import { connect, type NatsConnection } from '@nats-io/transport-node';
import { Injectable, NotFoundException } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { NatsClient } from '../../src/client/nats-client.js';
import { NatsConnectionManager } from '../../src/connection/connection-manager.js';
import { Subscribe } from '../../src/decorators/handler.decorators.js';
import { Payload, Subject } from '../../src/decorators/param.decorators.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { getNatsClientToken } from '../../src/module/tokens.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();

@Injectable()
class Handlers {
  readonly orders: Array<{ payload: unknown; subject: string }> = [];
  readonly seq: number[] = [];
  readonly boom: unknown[] = [];
  parallel = { active: 0, max: 0, done: 0 };

  @Subscribe('orders.*')
  onOrder(@Payload() payload: unknown, @Subject() subject: string) {
    if (subject === 'orders.created') this.orders.push({ payload, subject });
  }

  @Subscribe('orders.get')
  get(@Payload() payload: unknown) {
    return payload;
  }

  @Subscribe('orders.void')
  void() {}

  @Subscribe('orders.missing')
  missing() {
    throw new NotFoundException('no such order');
  }

  @Subscribe('seq.in')
  async onSeq(@Payload() i: number) {
    await sleep(Math.floor(Math.random() * 11));
    this.seq.push(i);
  }

  @Subscribe('par.in', { concurrency: 3 })
  async onPar() {
    this.parallel.active++;
    this.parallel.max = Math.max(this.parallel.max, this.parallel.active);
    await sleep(100);
    this.parallel.active--;
    this.parallel.done++;
  }

  @Subscribe('boom')
  onBoom(@Payload() p: { fail: boolean; n: number }) {
    if (p.fail) throw new Error('boom');
    this.boom.push(p.n);
  }

  received() {
    return eventually(() => {
      const first = this.orders[0];
      if (!first) throw new Error('nothing received yet');
      return first;
    });
  }

  sequence(n: number) {
    return eventually(() => {
      if (this.seq.length < n) throw new Error(`received ${this.seq.length}/${n}`);
      return [...this.seq];
    });
  }
}

@Injectable()
class QueueHandlers {
  count = 0;

  @Subscribe('work', { queue: 'q' })
  onWork() {
    this.count++;
  }
}

@Injectable()
class SlowHandlers {
  started = false;
  done = false;

  @Subscribe('slow')
  async onSlow() {
    this.started = true;
    await sleep(300);
    this.done = true;
  }
}

@Injectable()
class BadHandlers {
  @Subscribe('bad', { connection: 'nope' })
  on() {}
}

describe('@Subscribe', () => {
  let srv: NatsTestServer;
  let rawNc: NatsConnection;
  let app: TestingModule;
  let client: NatsClient;
  let handlers: Handlers;
  const nats = () => NatsModule.forRoot({ connection: { servers: srv.url }, logger: false });

  beforeAll(async () => {
    srv = await startNats();
    rawNc = await connect({ servers: srv.url });
    app = await createTestApp({ imports: [nats()], providers: [Handlers] });
    client = app.get(getNatsClientToken());
    handlers = app.get(Handlers);
  });

  afterAll(async () => {
    await app?.close();
    await rawNc?.close();
    await srv?.stop();
  });

  it('delivers decoded payload and wildcard subject', async () => {
    client.publish('orders.created', { id: 1 });
    expect(await handlers.received()).toEqual({ payload: { id: 1 }, subject: 'orders.created' });
  });

  it('replies with the return value', async () =>
    expect(await client.request('orders.get', { id: 2 })).toEqual({ id: 2 }));

  it('does not reply when handler returns undefined', async () =>
    await expect(client.request('orders.void', {}, { timeout: 300 })).rejects.toMatchObject({
      code: 'timeout',
    }));

  it('replies 400 for non-JSON payload', async () => {
    const m = await rawNc.request('orders.get', enc.encode('hello'));
    expect(m.headers?.get('Nats-Service-Error-Code')).toBe('400');
    expect(m.headers?.get('Nats-Service-Error')).toBe('Bad Request');
    expect(m.data.length).toBe(0);
  });

  it('replies with HttpException status', async () =>
    await expect(client.request('orders.missing', {})).rejects.toMatchObject({
      code: 'service_error',
      serviceCode: 404,
    }));

  it('keeps order with concurrency 1', async () => {
    for (let i = 0; i < 20; i++) client.publish('seq.in', i);
    expect(await handlers.sequence(20)).toEqual([...Array(20).keys()]);
  });

  it('runs up to `concurrency` handlers in parallel', async () => {
    for (let i = 0; i < 6; i++) client.publish('par.in', i);
    await eventually(() => expect(handlers.parallel.done).toBe(6));
    expect(handlers.parallel.max).toBe(3);
  });

  it('survives a throwing handler', async () => {
    client.publish('boom', { fail: true, n: 1 });
    client.publish('boom', { fail: false, n: 2 });
    await eventually(() => expect(handlers.boom).toEqual([2]));
  });

  it('load-balances a queue group across two apps', async () => {
    const a = await createTestApp({ imports: [nats()], providers: [QueueHandlers] });
    const b = await createTestApp({ imports: [nats()], providers: [QueueHandlers] });
    try {
      const ha = a.get(QueueHandlers);
      const hb = b.get(QueueHandlers);
      for (let i = 0; i < 20; i++) rawNc.publish('work', enc.encode(String(i)));
      await rawNc.flush();
      await eventually(() => expect(ha.count + hb.count).toBe(20));
      expect(ha.count).toBeGreaterThan(0);
      expect(hb.count).toBeGreaterThan(0);
      await sleep(100);
      expect(ha.count + hb.count).toBe(20);
    } finally {
      await a.close();
      await b.close();
    }
  });

  it('fails bootstrap for unknown connection', async () =>
    await expect(createTestApp({ imports: [nats()], providers: [BadHandlers] })).rejects.toThrow(
      NatsConfigError,
    ));

  it('closes connections when bootstrap fails', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [nats()],
      providers: [BadHandlers],
    }).compile();
    const mgr = moduleRef.get(NatsConnectionManager);
    expect(mgr.status().state).toBe('connected');
    await expect(moduleRef.init()).rejects.toThrow(/BadHandlers\.on/);
    expect(mgr.names().map((n) => mgr.status(n).state)).toEqual(['closed']);
  });

  it('closes connections when stream sync fails', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [nats(), NatsModule.forFeature({ connection: 'nope', streams: [{ name: 'X' }] })],
    }).compile();
    const mgr = moduleRef.get(NatsConnectionManager);
    await expect(moduleRef.init()).rejects.toThrow(NatsConfigError);
    expect(mgr.names().map((n) => mgr.status(n).state)).toEqual(['closed']);
  });

  it('waits for in-flight handlers on close', async () => {
    const slowApp = await createTestApp({ imports: [nats()], providers: [SlowHandlers] });
    const slow = slowApp.get(SlowHandlers);
    rawNc.publish('slow', enc.encode('{}'));
    await eventually(() => expect(slow.started).toBe(true));
    expect(slow.done).toBe(false);
    await slowApp.close();
    expect(slow.done).toBe(true);
    expect(slowApp.get(NatsConnectionManager).status().state).toBe('closed');
  });

  it('stops waiting after shutdown.drainTimeout', async () => {
    const slowApp = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger: false,
          shutdown: { drainTimeout: 50 },
        }),
      ],
      providers: [SlowHandlers],
    });
    const slow = slowApp.get(SlowHandlers);
    rawNc.publish('slow', enc.encode('{}'));
    await eventually(() => expect(slow.started).toBe(true));
    await slowApp.close();
    expect(slow.done).toBe(false);
    expect(slowApp.get(NatsConnectionManager).status().state).toBe('closed');
    await eventually(() => expect(slow.done).toBe(true));
  });
});
