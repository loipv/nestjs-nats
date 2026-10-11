import { jetstreamManager } from '@nats-io/jetstream';
import { Kvm } from '@nats-io/kv';
import { connect, headers, type NatsConnection } from '@nats-io/transport-node';
import { Injectable } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { Subscribe } from '../../src/decorators/handler.decorators.js';
import { Msg } from '../../src/decorators/param.decorators.js';
import { KvIdempotencyStore } from '../../src/kv/index.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { getConnectionToken } from '../../src/module/tokens.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();

/** The handler runs of every app, as `<app>:<subject>`. */
const calls: string[] = [];

/**
 * The handler of one replica. Keys are scoped by class and method names, so the replicas
 * share them as two deployments of one application would; their queue groups differ, so
 * both receive every message and only the store de-duplicates.
 */
function replicaHandler(queue: 'A' | 'B') {
  @Injectable()
  class Handler {
    @Subscribe('idem.x', { queue, idempotency: true })
    on(@Msg() msg: { subject: string }) {
      calls.push(`${queue}:${msg.subject}`);
    }
  }
  return Handler;
}

@Injectable()
class LongKeyHandler {
  @Subscribe('idem.long', { idempotency: true })
  on(@Msg() msg: { subject: string }) {
    calls.push(`L:${msg.subject}`);
  }
}

describe('KvIdempotencyStore on a real server', () => {
  let srv: NatsTestServer;
  let nc: NatsConnection;
  const apps: TestingModule[] = [];

  const boot = async (store: KvIdempotencyStore, providers: (new () => unknown)[] = []) => {
    const app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger: false,
          idempotency: { store },
        }),
      ],
      providers,
    });
    apps.push(app);
    return app;
  };

  const publish = (subject: string, msgId: string) => {
    const h = headers();
    h.set('Nats-Msg-Id', msgId);
    nc.publish(subject, enc.encode('{}'), { headers: h });
  };

  beforeAll(async () => {
    srv = await startNats();
    nc = await connect({ servers: srv.url });
  });

  afterEach(async () => {
    await Promise.all(apps.splice(0).map((app) => app.close()));
    calls.length = 0;
  });

  afterAll(async () => {
    await nc?.close();
    await srv?.stop();
  });

  it('de-duplicates across two applications sharing a bucket', async () => {
    // One store object per app: a store binds to one connection.
    await boot(new KvIdempotencyStore({ bucket: 'idem' }), [replicaHandler('A')]);
    await boot(new KvIdempotencyStore({ bucket: 'idem' }), [replicaHandler('B')]);

    publish('idem.x', 'shared-1');
    publish('idem.x', 'shared-1');
    await eventually(() => expect(calls).toHaveLength(1));
    // Guards against a late duplicate: neither app may run the handler again.
    await sleep(300);
    expect(calls).toHaveLength(1);
  });

  it('reclaims an expired key on a real server', async () => {
    let clock = 1_000_000;
    const store = new KvIdempotencyStore({ bucket: 'idem-reclaim', now: () => clock });
    await boot(store);

    expect(await store.claim('k', 1)).toBe('claimed');
    clock += 10;
    expect(await store.claim('k', 1_000)).toBe('claimed');
    expect(await store.claim('k', 1_000)).toBe('in-progress');
  });

  it('creates the bucket with history 1 and max age', async () => {
    const jsm = await jetstreamManager(nc);
    await expect(jsm.streams.info('KV_idem2')).rejects.toThrow(/stream not found/);

    await boot(new KvIdempotencyStore({ bucket: 'idem2', maxAge: 60_000 }));

    const status = await (await new Kvm(nc).open('idem2')).status();
    expect(status.history).toBe(1);
    expect(status.ttl).toBe(60_000);
  });

  it('stores a 5 KB Nats-Msg-Id without losing the connection', async () => {
    const app = await boot(new KvIdempotencyStore({ bucket: 'idem-long' }), [LongKeyHandler]);
    const appNc = app.get<NatsConnection>(getConnectionToken());
    const events: string[] = [];
    void (async () => {
      for await (const s of appNc.status()) events.push(s.type);
    })();

    // Unhashed, its key would make a KV subject longer than the server's control line.
    const longId = 'm'.repeat(5 * 1024);
    publish('idem.long', longId);
    publish('idem.long', longId);
    await eventually(() => expect(calls).toEqual(['L:idem.long']));
    await sleep(300);
    expect(calls).toEqual(['L:idem.long']);

    publish('idem.long', 'short-1');
    await eventually(() => expect(calls).toEqual(['L:idem.long', 'L:idem.long']));
    expect(events).not.toContain('disconnect');
    expect(appNc.isClosed()).toBe(false);
  });
});
