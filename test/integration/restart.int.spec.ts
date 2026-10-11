import { jetstreamManager } from '@nats-io/jetstream';
import { connect, type NatsConnection } from '@nats-io/transport-node';
import { Injectable, type LoggerService, type Provider } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import type { TestingModule } from '@nestjs/testing';
import { afterEach, describe, expect, it } from 'vitest';
import {
  Consume,
  NatsService,
  ServiceEndpoint,
  Subscribe,
} from '../../src/decorators/handler.decorators.js';
import { Payload } from '../../src/decorators/param.decorators.js';
import { NatsHealthIndicator, NatsHealthModule } from '../../src/health/index.js';
import { NatsModule } from '../../src/module/nats.module.js';
import type { RestartOptions } from '../../src/settlement/restart.js';
import { createTestApp } from './support/app.js';
import { eventually } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();

const ALLOW = 'authorization { users = [{ user: "a", password: "pw" }] }';
const DENY_INBOX = `authorization {
  users = [{ user: "a", password: "pw", permissions: { subscribe: { deny: ["_INBOX.>"] } } }]
}`;
const DENY_DEN = `authorization {
  users = [{ user: "a", password: "pw", permissions: { subscribe: { deny: ["den.core", "den.svc"] } } }]
}`;

function collectingLogger() {
  const lines = { log: [] as string[], warn: [] as string[], error: [] as string[] };
  const logger: LoggerService = {
    log: (m: unknown) => lines.log.push(String(m)),
    warn: (m: unknown) => lines.warn.push(String(m)),
    error: (m: unknown) => lines.error.push(String(m)),
  };
  return { lines, logger };
}

@Injectable()
class Worker {
  readonly got: unknown[] = [];

  @Consume({ stream: 'RST', durable: 'w', config: { filter_subject: 'rst.>' } })
  handle(@Payload() p: unknown) {
    this.got.push(p);
  }
}

@Injectable()
class DeletedWorker {
  readonly got: unknown[] = [];

  @Consume({ stream: 'RST', durable: 'del', config: { filter_subject: 'rst.del.>' } })
  handle(@Payload() p: unknown) {
    this.got.push(p);
  }
}

@Injectable()
class CoreListener {
  @Subscribe('den.core')
  on() {}
}

@NatsService({ name: 'gated', version: '1.0.0' })
@Injectable()
class Gated {
  @ServiceEndpoint('svc', { subject: 'den.svc' })
  svc() {
    return 'ok';
  }
}

describe('restarting handlers', () => {
  let srv: NatsTestServer | undefined;
  let app: TestingModule | undefined;
  let nc: NatsConnection | undefined;

  afterEach(async () => {
    await app?.close();
    await nc?.close();
    await srv?.stop();
    app = undefined;
    nc = undefined;
    srv = undefined;
  });

  const startApp = async (
    server: NatsTestServer,
    restart: RestartOptions,
    logger: LoggerService,
    providers: Provider[],
  ) => {
    app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: server.url, user: 'a', pass: 'pw' },
          logger,
          restart,
          streams: [{ name: 'RST', subjects: ['rst.>'] }],
        }),
        TerminusModule,
        NatsHealthModule,
      ],
      providers,
    });
    return app;
  };

  it('restarts a pull consumer whose subscription was revoked, once permissions are back', async () => {
    srv = await startNats([], { config: ALLOW });
    const { lines, logger } = collectingLogger();
    const started = await startApp(srv, { initialDelay: 100, maxDelay: 500 }, logger, [Worker]);
    const worker = started.get(Worker);
    const indicator = started.get(NatsHealthIndicator);
    nc = await connect({ servers: srv.url, user: 'a', pass: 'pw' });

    // Core publishes: they need no reply inbox, which the deny below would revoke.
    nc.publish('rst.1', enc.encode('1'));
    await eventually(() => expect(worker.got).toEqual([1]));

    await srv.reload(DENY_INBOX);
    await eventually(
      () =>
        expect(lines.warn).toContainEqual(expect.stringMatching(/^Restart of .*RST\/w.* failed/)),
      { timeout: 10_000 },
    );
    await eventually(async () => {
      const result = await indicator.isHealthy('nats', { handlers: true });
      expect(result.nats).toMatchObject({
        status: 'up',
        handlers: { restarting: ['Worker.handle on "RST/w"'] },
      });
    });

    // Only a restart once permissions are back counts.
    const logged = lines.log.length;
    await srv.reload(ALLOW);
    await eventually(
      () =>
        expect(lines.log.slice(logged)).toContainEqual(
          expect.stringMatching(/^Restarted .*RST\/w/),
        ),
      { timeout: 10_000 },
    );

    nc.publish('rst.2', enc.encode('2'));
    await eventually(() => expect(worker.got).toEqual([1, 2]));
  });

  it('recreates a deleted pull consumer declared with config', async () => {
    srv = await startNats([], { config: ALLOW });
    const { lines, logger } = collectingLogger();
    const started = await startApp(srv, { initialDelay: 100, maxDelay: 500 }, logger, [
      DeletedWorker,
    ]);
    const worker = started.get(DeletedWorker);
    nc = await connect({ servers: srv.url, user: 'a', pass: 'pw' });

    nc.publish('rst.del.1', enc.encode('1'));
    await eventually(() => expect(worker.got).toEqual([1]));

    const jsm = await jetstreamManager(nc);
    await jsm.consumers.delete('RST', 'del');
    await eventually(
      () => expect(lines.log).toContainEqual(expect.stringMatching(/^Restarted .*RST\/del/)),
      { timeout: 10_000 },
    );

    // The recreated consumer starts from its deliver policy (all): message 1 comes again.
    nc.publish('rst.del.2', enc.encode('2'));
    await eventually(() => expect(worker.got).toEqual([1, 1, 2]));
  });

  it('gives up on a denied core subscription after maxAttempts and reports it', async () => {
    srv = await startNats([], { config: DENY_DEN });
    const { lines, logger } = collectingLogger();
    const started = await startApp(
      srv,
      { initialDelay: 50, maxDelay: 100, maxAttempts: 2 },
      logger,
      [CoreListener],
    );
    const indicator = started.get(NatsHealthIndicator);

    await eventually(async () => {
      const result = await indicator.isHealthy('nats', { handlers: true });
      expect(result.nats).toMatchObject({
        status: 'down',
        reason: expect.stringMatching(/^handler failed: .*den\.core/),
      });
    });
    expect(lines.error).toContainEqual(
      'Gave up restarting CoreListener.on on "den.core" after 2 attempts; no more messages will be received',
    );
  });

  it('recreates a denied service until it gives up', async () => {
    srv = await startNats([], { config: DENY_DEN });
    const { lines, logger } = collectingLogger();
    const started = await startApp(
      srv,
      { initialDelay: 50, maxDelay: 100, maxAttempts: 2 },
      logger,
      [Gated],
    );
    const indicator = started.get(NatsHealthIndicator);

    await eventually(() =>
      expect(lines.error).toContainEqual(
        'Gave up restarting service "gated" after 2 attempts; no more messages will be received',
      ),
    );
    const result = await indicator.isHealthy('nats', { handlers: true });
    expect(result.nats).toMatchObject({
      status: 'down',
      reason: 'handler failed: service "gated"',
    });

    const closing = Date.now();
    await started.close();
    app = undefined;
    expect(Date.now() - closing).toBeLessThan(5_000);
  });
});
