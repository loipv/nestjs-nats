import { ServiceUnavailableException } from '@nestjs/common';
import { HealthCheckService, TerminusModule } from '@nestjs/terminus';
import type { TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NatsHealthIndicator, NatsHealthModule } from '../../src/health/index.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { createTestApp } from './support/app.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

describe('NATS health indicator', () => {
  let srv: NatsTestServer;
  let app: TestingModule;
  let health: HealthCheckService;
  let indicator: NatsHealthIndicator;
  let stopped = false;

  beforeAll(async () => {
    srv = await startNats();
    app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url, reconnect: false },
          logger: false,
        }),
        TerminusModule,
        NatsHealthModule,
      ],
    });
    health = app.get(HealthCheckService);
    indicator = app.get(NatsHealthIndicator);
  });

  afterAll(async () => {
    await app?.close();
    if (!stopped) await srv?.stop();
  });

  it('reports up through HealthCheckService', async () => {
    const result = await health.check([() => indicator.isHealthy('nats', { jetstream: true })]);
    expect(result.status).toBe('ok');
    expect(result.info?.nats).toMatchObject({ status: 'up', connection: 'default' });
    expect(result.info?.nats?.server).toEqual(expect.any(String));
    expect(result.info?.nats?.rtt).toEqual(expect.any(Number));
  });

  it('reports down for an unknown connection', async () => {
    const result = await indicator.isHealthy('nats', { connection: 'nope' });
    expect(result.nats).toMatchObject({ status: 'down', reason: 'unknown connection' });
  });

  it('reports down after the server stops', async () => {
    await srv.stop();
    stopped = true;
    await expect(
      health.check([() => indicator.isHealthy('nats', { rttTimeout: 500 })]),
    ).rejects.toBeInstanceOf(ServiceUnavailableException);
    const result = await indicator.isHealthy('nats', { rttTimeout: 500 });
    expect(result.nats?.status).toBe('down');
  });
});
