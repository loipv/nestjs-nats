import { connect, headers, type NatsConnection, type Subscription } from '@nats-io/transport-node';
import type { TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { NatsClient } from '../../src/client/nats-client.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { getNatsClientToken } from '../../src/module/tokens.js';
import type { NatsPlugin } from '../../src/pipeline/types.js';
import { createTestApp } from './support/app.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

async function first(sub: Subscription) {
  for await (const m of sub) return m;
  throw new Error('subscription ended');
}

describe('NatsClient', () => {
  let srv: NatsTestServer;
  let rawNc: NatsConnection;
  let app: TestingModule;
  let client: NatsClient;
  const order: string[] = [];

  const headerPlugin: NatsPlugin = {
    name: 'header',
    outbound: {
      async handle(m, next) {
        order.push('header');
        m.headers.set('x-plugin', '1');
        await next();
      },
    },
  };
  const secondPlugin: NatsPlugin = {
    name: 'second',
    outbound: {
      async handle(_m, next) {
        order.push('second');
        await next();
      },
    },
  };

  beforeAll(async () => {
    srv = await startNats();
    rawNc = await connect({ servers: srv.url });
    app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          plugins: [headerPlugin, secondPlugin],
          logger: false,
        }),
      ],
    });
    client = app.get(getNatsClientToken());
  });

  afterAll(async () => {
    await app?.close();
    await rawNc?.close();
    await srv?.stop();
  });

  it('publishes JSON with plugin headers', async () => {
    const sub = rawNc.subscribe('t.pub', { max: 1 });
    await rawNc.flush();
    client.publish('t.pub', { a: 1 }, { headers: { 'x-h': 'v' } });
    const m = await first(sub);
    expect(m.json()).toEqual({ a: 1 });
    expect(m.headers?.get('x-h')).toBe('v');
    expect(m.headers?.get('x-plugin')).toBe('1');
    expect(order.slice(0, 2)).toEqual(['header', 'second']);
  });

  it('flush waits for in-flight publishes', async () => {
    const sub = rawNc.subscribe('t.flush', { max: 1 });
    await rawNc.flush();
    client.publish('t.flush', { n: 1 });
    await client.flush();
    await rawNc.flush();
    expect((await first(sub)).json()).toEqual({ n: 1 });
  });

  it('request returns decoded reply', async () => {
    const sub = rawNc.subscribe('t.req');
    await rawNc.flush();
    void (async () => {
      for await (const m of sub) m.respond(JSON.stringify({ ok: true }));
    })();
    expect(await client.request('t.req', {})).toEqual({ ok: true });
  });

  it('maps timeout', async () => {
    rawNc.subscribe('t.silent');
    await rawNc.flush();
    await expect(client.request('t.silent', {}, { timeout: 200 })).rejects.toMatchObject({
      code: 'timeout',
    });
  });

  it('maps no responders', async () => {
    await expect(client.request('t.none', {})).rejects.toMatchObject({ code: 'no_responders' });
  });

  it('maps service errors', async () => {
    const sub = rawNc.subscribe('t.err');
    await rawNc.flush();
    void (async () => {
      for await (const m of sub) {
        const h = headers();
        h.set('Nats-Service-Error-Code', '404');
        h.set('Nats-Service-Error', 'not found');
        m.respond(new Uint8Array(0), { headers: h });
      }
    })();
    await expect(client.request('t.err', {})).rejects.toMatchObject({
      code: 'service_error',
      serviceCode: 404,
      description: 'not found',
    });
  });

  it('logs pipeline errors on publish instead of throwing', async () => {
    const errors: string[] = [];
    const failing = await createTestApp({
      imports: [
        NatsModule.forRoot({
          name: 'failing',
          connection: { servers: srv.url },
          plugins: [
            {
              name: 'boom',
              outbound: {
                async handle() {
                  throw new Error('plugin exploded');
                },
              },
            },
          ],
          logger: {
            log() {},
            warn() {},
            debug() {},
            verbose() {},
            error: (msg: unknown) => {
              errors.push(String(msg));
            },
          },
        }),
      ],
    });
    try {
      const c = failing.get<NatsClient>(getNatsClientToken('failing'));
      expect(() => c.publish('t.fail', { a: 1 })).not.toThrow();
      await c.flush();
      await vi.waitFor(() => expect(errors.join('\n')).toMatch(/plugin exploded/));
    } finally {
      await failing.close();
    }
  });
});
