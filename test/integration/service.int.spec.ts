import { type ServiceInfo, Svcm } from '@nats-io/services';
import { connect, type NatsConnection, NoRespondersError } from '@nats-io/transport-node';
import { ConflictException, Injectable } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { NatsClient } from '../../src/client/nats-client.js';
import { NatsConnectionManager } from '../../src/connection/connection-manager.js';
import { NatsService, ServiceEndpoint } from '../../src/decorators/handler.decorators.js';
import { Payload } from '../../src/decorators/param.decorators.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { getNatsClientToken } from '../../src/module/tokens.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();

@NatsService({
  name: 'inventory',
  version: '1.0.0',
  description: 'stock',
  metadata: { team: 'core' },
})
@Injectable()
class Inv {
  parallel = { active: 0, max: 0 };

  @ServiceEndpoint('reserve', { subject: 'inv.reserve' })
  reserve(@Payload() d: { sku: string }) {
    return { ok: d.sku };
  }

  @ServiceEndpoint('noop', { subject: 'inv.noop' })
  noop() {}

  @ServiceEndpoint('fail', { subject: 'inv.fail' })
  fail() {
    throw new ConflictException('out of stock');
  }

  @ServiceEndpoint('crash', { subject: 'inv.crash' })
  crash() {
    throw new Error('secret internals');
  }

  @ServiceEndpoint('slow', { subject: 'inv.slow' })
  async slow() {
    this.parallel.active++;
    this.parallel.max = Math.max(this.parallel.max, this.parallel.active);
    await sleep(100);
    this.parallel.active--;
    return 'done';
  }

  @ServiceEndpoint('plain')
  plain() {
    return 'plain';
  }
}

@NatsService({ name: 'billing', version: '2.0.0' })
@Injectable()
class Billing {
  @ServiceEndpoint('charge', { subject: 'bill.charge' })
  charge() {
    return 'charged';
  }
}

@Injectable()
class Orphan {
  @ServiceEndpoint('x')
  x() {}
}

@NatsService({ name: 'bad', version: '1.0.0', connection: 'nope' })
@Injectable()
class BadService {
  @ServiceEndpoint('x')
  x() {}
}

@NatsService({ name: 'badep', version: '1.0.0' })
@Injectable()
class InvalidEndpoint {
  @ServiceEndpoint('bad name')
  x() {}
}

@NatsService({ name: 'denied', version: '1.0.0' })
@Injectable()
class Denied {
  @ServiceEndpoint('ok', { subject: 'den.ok' })
  ok() {
    return 'ok';
  }

  @ServiceEndpoint('forbidden', { subject: 'den.forbidden' })
  forbidden() {}
}

async function infos(nc: NatsConnection, name: string): Promise<ServiceInfo[]> {
  const out: ServiceInfo[] = [];
  for await (const info of await new Svcm(nc).client().info(name)) out.push(info);
  return out;
}

describe('@NatsService / @ServiceEndpoint', () => {
  let srv: NatsTestServer;
  let rawNc: NatsConnection;
  let app: TestingModule;
  let client: NatsClient;
  const nats = () => NatsModule.forRoot({ connection: { servers: srv.url }, logger: false });

  beforeAll(async () => {
    srv = await startNats();
    rawNc = await connect({ servers: srv.url });
    app = await createTestApp({ imports: [nats()], providers: [Inv, Billing] });
    client = app.get(getNatsClientToken());
  });

  afterAll(async () => {
    await app?.close();
    await rawNc?.close();
    await srv?.stop();
  });

  it('responds with the return value', async () =>
    expect(await client.request('inv.reserve', { sku: 'a' })).toEqual({ ok: 'a' }));

  it('responds empty when handler returns undefined', async () =>
    expect(await client.request('inv.noop', {})).toBeUndefined());

  it('maps errors to respondError', async () =>
    await expect(client.request('inv.fail', {})).rejects.toMatchObject({
      code: 'service_error',
      serviceCode: 409,
      description: 'out of stock',
    }));

  it('does not leak internal error messages', async () =>
    await expect(client.request('inv.crash', {})).rejects.toMatchObject({
      code: 'service_error',
      serviceCode: 500,
      description: 'Internal Server Error',
    }));

  it('replies 400 for non-JSON payload', async () => {
    const m = await rawNc.request('inv.reserve', enc.encode('hello'));
    expect(m.headers?.get('Nats-Service-Error-Code')).toBe('400');
    expect(m.headers?.get('Nats-Service-Error')).toBe('Bad Request');
  });

  it('uses the endpoint name as subject by default', async () =>
    expect(await client.request('plain', {})).toBe('plain'));

  it('is discoverable via the services API', async () => {
    const found = await infos(rawNc, 'inventory');
    expect(found).toHaveLength(1);
    const [info] = found;
    expect(info?.version).toBe('1.0.0');
    expect(info?.description).toBe('stock');
    expect(info?.metadata).toEqual({ team: 'core' });
    expect(info?.endpoints.map((e) => e.name).sort()).toEqual([
      'crash',
      'fail',
      'noop',
      'plain',
      'reserve',
      'slow',
    ]);
  });

  it('registers one service per @NatsService class', async () => {
    const found = await infos(rawNc, 'billing');
    expect(found).toHaveLength(1);
    expect(found[0]?.endpoints.map((e) => e.name)).toEqual(['charge']);
    expect(await client.request('bill.charge', {})).toBe('charged');
  });

  it('handles one request at a time per endpoint', async () => {
    const inv = app.get(Inv);
    const results = await Promise.all([1, 2, 3].map(() => client.request('inv.slow', {})));
    expect(results).toEqual(['done', 'done', 'done']);
    expect(inv.parallel.max).toBe(1);
  });

  it('stops the service on close', async () => {
    const solo = await startNats();
    const nc = await connect({ servers: solo.url });
    const soloApp = await createTestApp({
      imports: [NatsModule.forRoot({ connection: { servers: solo.url }, logger: false })],
      providers: [Inv],
    });
    try {
      expect((await nc.request('inv.reserve', enc.encode('{"sku":"z"}'))).string()).toBe(
        '{"ok":"z"}',
      );
      await soloApp.close();
      await expect(nc.request('inv.reserve', enc.encode('{"sku":"z"}'))).rejects.toMatchObject({
        cause: expect.any(NoRespondersError),
      });
    } finally {
      await nc.close();
      await solo.stop();
    }
  });

  it('rejects @ServiceEndpoint on a class without @NatsService', async () =>
    await expect(createTestApp({ imports: [nats()], providers: [Orphan] })).rejects.toThrow(
      NatsConfigError,
    ));

  it('closes connections when a service references an unknown connection', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [nats()],
      providers: [BadService],
    }).compile();
    const mgr = moduleRef.get(NatsConnectionManager);
    await expect(moduleRef.init()).rejects.toThrow(NatsConfigError);
    expect(mgr.names().map((n) => mgr.status(n).state)).toEqual(['closed']);
  });

  it('fails bootstrap with NatsConfigError for an invalid endpoint name and cleans up', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [nats()],
      providers: [InvalidEndpoint],
    }).compile();
    const mgr = moduleRef.get(NatsConnectionManager);
    const err = await moduleRef.init().then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(NatsConfigError);
    expect((err as Error).message).toMatch(/InvalidEndpoint\.x/);
    expect(mgr.names().map((n) => mgr.status(n).state)).toEqual(['closed']);
    // The service registered before the failing endpoint was stopped again.
    await expect(infos(rawNc, 'badep')).rejects.toThrow(/no responders/);
  });

  it('answers in-flight requests before closing', async () => {
    const solo = await startNats();
    const nc = await connect({ servers: solo.url });
    const soloApp = await createTestApp({
      imports: [NatsModule.forRoot({ connection: { servers: solo.url }, logger: false })],
      providers: [Inv],
    });
    try {
      const inv = soloApp.get(Inv);
      const pending = nc.request('inv.slow', enc.encode('{}'));
      await eventually(() => expect(inv.parallel.active).toBe(1));
      await soloApp.close();
      expect((await pending).string()).toBe('"done"');
    } finally {
      await nc.close();
      await solo.stop();
    }
  });
});

describe('@NatsService killed by a subscription error', () => {
  let srv: NatsTestServer;

  beforeAll(async () => {
    srv = await startNats([], {
      config: `authorization {
        users = [{ user: "svc", password: "pw", permissions: { subscribe: { deny: ["den.forbidden"] } } }]
      }`,
    });
  });

  afterAll(async () => {
    await srv?.stop();
  });

  it('logs the failure and does not hold up shutdown until drainTimeout', async () => {
    const errors: string[] = [];
    const logger = {
      log() {},
      warn() {},
      error: (m: unknown) => errors.push(String(m)),
    };
    const app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url, user: 'svc', pass: 'pw' },
          logger,
          shutdown: { drainTimeout: 10_000 },
        }),
      ],
      providers: [Denied],
    });
    await eventually(() =>
      expect(errors).toContainEqual(
        expect.stringMatching(/service "denied" stopped: .*den\.forbidden/i),
      ),
    );
    const started = Date.now();
    await app.close();
    expect(Date.now() - started).toBeLessThan(5_000);
  });
});
