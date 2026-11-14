import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { ConnectionRegistry } from '../../src/connection/connection-registry.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import type { JetStreamClient } from '../../src/jetstream/jetstream-client.js';
import type { NatsStreamManager } from '../../src/jetstream/stream-manager.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { getJetStreamClientToken, getStreamManagerToken } from '../../src/module/tokens.js';
import type { NatsPlugin } from '../../src/pipeline/types.js';
import { createTestApp } from './support/app.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

describe('publishMany', () => {
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
    };
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
    } finally {
      await app.close();
    }
  });

  it('caps in-flight publishes at window', async () => {
    const { app, js } = await setup();
    try {
      // Counts the publishes awaiting their PubAck (an outbound plugin's own code after next()
      // is not waited for, so it would not tell).
      const natsJs = app.get(ConnectionRegistry).get().js();
      const publish = natsJs.publish.bind(natsJs);
      let active = 0;
      let max = 0;
      vi.spyOn(natsJs, 'publish').mockImplementation(async (...args) => {
        active++;
        max = Math.max(max, active);
        try {
          return await publish(...args);
        } finally {
          active--;
        }
      });
      const items = Array.from({ length: 50 }, (_, i) => ({ subject: 'pb.w', data: i }));
      const r = await js.publishMany(items, { window: 4 });
      expect(r.every((x) => x.ok)).toBe(true);
      expect(max).toBeGreaterThan(1);
      expect(max).toBeLessThanOrEqual(4);
    } finally {
      await app.close();
    }
  });
});
