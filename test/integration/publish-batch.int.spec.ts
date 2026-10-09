import { jetstreamManager } from '@nats-io/jetstream';
import { connect } from '@nats-io/transport-node';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NatsConfigError } from '../../src/errors/errors.js';
import type { JetStreamClient } from '../../src/jetstream/jetstream-client.js';
import type { NatsStreamManager } from '../../src/jetstream/stream-manager.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { getJetStreamClientToken, getStreamManagerToken } from '../../src/module/tokens.js';
import type { NatsPlugin } from '../../src/pipeline/types.js';
import { createTestApp } from './support/app.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

describe('publishMany / Batcher', () => {
  let srv: NatsTestServer;

  beforeAll(async () => {
    srv = await startNats();
  });

  afterAll(async () => {
    await srv?.stop();
  });

  async function setup(plugins: NatsPlugin[] = []) {
    const app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger: false,
          plugins,
          streams: [{ name: 'PB', subjects: ['pb.>'] }],
        }),
      ],
    });
    // Streams outlive apps on the shared server: start each test from an empty one.
    await (await app.get<NatsStreamManager>(getStreamManagerToken()).jsm()).streams.purge('PB');
    return {
      app,
      js: app.get<JetStreamClient>(getJetStreamClientToken()),
      sm: app.get<NatsStreamManager>(getStreamManagerToken()),
    };
  }

  async function streamMessages(sm: NatsStreamManager): Promise<number> {
    const jsm = await sm.jsm();
    return (await jsm.streams.info('PB')).state.messages;
  }

  it('publishMany keeps input order and reports per-item results', async () => {
    const { app, js } = await setup();
    try {
      const r = await js.publishMany([
        { subject: 'pb.a', data: 1 },
        { subject: 'nostream.x', data: 2 },
        { subject: 'pb.b', data: 3 },
      ]);
      expect(r.map((x) => x.ok)).toEqual([true, false, true]);
      expect((r[0] as { ack: { seq: number } }).ack.seq).toBeLessThan(
        (r[2] as { ack: { seq: number } }).ack.seq,
      );
      expect((r[1] as { error: Error }).error).toBeInstanceOf(Error);
      expect(await js.publishMany([])).toEqual([]);
    } finally {
      await app.close();
    }
  });

  it('rejects a window below 1', async () => {
    const { app, js } = await setup();
    try {
      await expect(js.publishMany([{ subject: 'pb.a' }], { window: 0 })).rejects.toBeInstanceOf(
        NatsConfigError,
      );
      expect(() => js.batcher({ window: 0 })).toThrow(NatsConfigError);
    } finally {
      await app.close();
    }
  });

  it('caps in-flight publishes at window', async () => {
    let active = 0;
    let max = 0;
    const counter: NatsPlugin = {
      name: 'counter',
      outbound: {
        async handle(_m, next) {
          active++;
          max = Math.max(max, active);
          try {
            await next();
          } finally {
            active--;
          }
        },
      },
    };
    const { app, js } = await setup([counter]);
    try {
      const items = Array.from({ length: 50 }, (_, i) => ({ subject: 'pb.w', data: i }));
      const r = await js.publishMany(items, { window: 4 });
      expect(r.every((x) => x.ok)).toBe(true);
      expect(max).toBeGreaterThan(1);
      expect(max).toBeLessThanOrEqual(4);
    } finally {
      await app.close();
    }
  });

  it('batcher publishes and resolves acks', async () => {
    const { app, js, sm } = await setup();
    try {
      const b = js.batcher({ maxSize: 100, maxWait: 20 });
      const acks = await Promise.all(Array.from({ length: 250 }, (_, i) => b.add('pb.b', i)));
      expect(acks).toHaveLength(250);
      expect(new Set(acks.map((a) => a.seq)).size).toBe(250);
      await b.close();
      expect(await streamMessages(sm)).toBe(250);
    } finally {
      await app.close();
    }
  });

  it('flushes batchers on app.close()', async () => {
    const { app, js } = await setup();
    const b = js.batcher({ maxSize: 100, maxWait: 60_000 });
    const pending = [b.add('pb.c', 1), b.add('pb.c', 2), b.add('pb.c', 3)];
    await app.close();
    expect(await Promise.all(pending)).toHaveLength(3);
    // The app's connection is closed: count through a fresh one.
    const nc = await connect({ servers: srv.url });
    try {
      const jsm = await jetstreamManager(nc);
      expect((await jsm.streams.info('PB')).state.messages).toBe(3);
    } finally {
      await nc.close();
    }
  });
});
