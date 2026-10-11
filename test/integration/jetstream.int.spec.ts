import { AckPolicy, StorageType } from '@nats-io/jetstream';
import type { LoggerService } from '@nestjs/common';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { NatsConnectionManager } from '../../src/connection/connection-manager.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import type { JetStreamClient } from '../../src/jetstream/jetstream-client.js';
import type { NatsStreamManager } from '../../src/jetstream/stream-manager.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { getJetStreamClientToken, getStreamManagerToken } from '../../src/module/tokens.js';
import type { NatsPlugin } from '../../src/pipeline/types.js';
import { createTestApp } from './support/app.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

describe('JetStream', () => {
  let srv: NatsTestServer;
  let url: string;

  beforeAll(async () => {
    srv = await startNats();
    url = srv.url;
  });

  afterAll(async () => {
    await srv?.stop();
  });

  async function bareApp(extra: Record<string, unknown> = {}) {
    const app = await createTestApp({
      imports: [NatsModule.forRoot({ connection: { servers: url }, logger: false, ...extra })],
    });
    return {
      app,
      sm: app.get<NatsStreamManager>(getStreamManagerToken()),
      js: app.get<JetStreamClient>(getJetStreamClientToken()),
    };
  }

  it('creates declared streams and consumers on init', async () => {
    const app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: url },
          logger: false,
          streams: [{ name: 'S1', subjects: ['s1.>'] }],
          consumers: [{ stream: 'S1', config: { durable_name: 'c1' } }],
        }),
      ],
    });
    try {
      const jsm = await app.get<NatsStreamManager>(getStreamManagerToken()).jsm();
      expect((await jsm.streams.info('S1')).config.subjects).toEqual(['s1.>']);
      const ci = await jsm.consumers.info('S1', 'c1');
      expect(ci.config.max_deliver).toBe(-1);
      expect(ci.config.ack_policy).toBe('explicit');
    } finally {
      await app.close();
    }
  });

  it('picks up forFeature declarations', async () => {
    const app = await createTestApp({
      imports: [
        NatsModule.forRoot({ connection: { servers: url }, logger: false }),
        NatsModule.forFeature({
          streams: [{ name: 'S2', subjects: ['s2.>'] }],
          consumers: [{ stream: 'S2', config: { durable_name: 'c2' } }],
        }),
      ],
    });
    try {
      const jsm = await app.get<NatsStreamManager>(getStreamManagerToken()).jsm();
      expect((await jsm.streams.info('S2')).config.subjects).toEqual(['s2.>']);
      expect((await jsm.consumers.info('S2', 'c2')).config.durable_name).toBe('c2');
    } finally {
      await app.close();
    }
  });

  it('routes forFeature declarations to the named connection', async () => {
    const app = await createTestApp({
      imports: [
        NatsModule.forRoot({ connection: { servers: url }, logger: false }),
        NatsModule.forRoot({ name: 'b', connection: { servers: url }, logger: false }),
        NatsModule.forFeature({ connection: 'b', streams: [{ name: 'S2B', subjects: ['s2b.>'] }] }),
      ],
    });
    try {
      const jsm = await app.get<NatsStreamManager>(getStreamManagerToken('b')).jsm();
      expect((await jsm.streams.info('S2B')).config.name).toBe('S2B');
    } finally {
      await app.close();
    }
  });

  it('fails bootstrap when forFeature references an unknown connection', async () => {
    await expect(
      createTestApp({
        imports: [
          NatsModule.forRoot({ connection: { servers: url }, logger: false }),
          NatsModule.forFeature({ connection: 'nope', streams: [{ name: 'S2X' }] }),
        ],
      }),
    ).rejects.toThrow(NatsConfigError);
  });

  it('creates streams before consumers regardless of declaration source', async () => {
    const app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: url },
          logger: false,
          consumers: [{ stream: 'S2C', config: { durable_name: 'c' } }],
        }),
        NatsModule.forFeature({ streams: [{ name: 'S2C', subjects: ['s2c.>'] }] }),
      ],
    });
    try {
      const jsm = await app.get<NatsStreamManager>(getStreamManagerToken()).jsm();
      expect((await jsm.consumers.info('S2C', 'c')).config.durable_name).toBe('c');
    } finally {
      await app.close();
    }
  });

  it('updates mutable fields', async () => {
    const { app, sm } = await bareApp();
    try {
      await sm.ensureStream({ name: 'S3', subjects: ['s3.>'], max_msgs: 10 });
      const info = await sm.ensureStream({ name: 'S3', subjects: ['s3.>'], max_msgs: 20 });
      expect(info?.config.max_msgs).toBe(20);
      expect((await sm.info('S3')).config.max_msgs).toBe(20);
    } finally {
      await app.close();
    }
  });

  it('updates mutable consumer fields', async () => {
    const { app, sm } = await bareApp();
    try {
      await sm.ensureStream({ name: 'S3C', subjects: ['s3c.>'] });
      await sm.ensureConsumer('S3C', { durable_name: 'd', max_deliver: 3 });
      const ci = await sm.ensureConsumer('S3C', { durable_name: 'd', max_deliver: 5 });
      expect(ci?.config.max_deliver).toBe(5);
    } finally {
      await app.close();
    }
  });

  it('throws on immutable conflict without deleting', async () => {
    const { app, sm } = await bareApp();
    try {
      await sm.ensureStream({ name: 'S4', subjects: ['s4.>'], storage: StorageType.File });
      const p = sm.ensureStream({ name: 'S4', subjects: ['s4.>'], storage: StorageType.Memory });
      await expect(p).rejects.toMatchObject({
        name: 'StreamConfigConflictError',
        field: 'storage',
        current: 'file',
        declared: 'memory',
      });
      expect((await sm.info('S4')).config.storage).toBe('file');
    } finally {
      await app.close();
    }
  });

  it('throws on immutable consumer conflict', async () => {
    const { app, sm } = await bareApp();
    try {
      await sm.ensureStream({ name: 'S4C', subjects: ['s4c.>'] });
      await sm.ensureConsumer('S4C', { durable_name: 'd', ack_policy: 'none' as never });
      await expect(
        sm.ensureConsumer('S4C', { durable_name: 'd', ack_policy: AckPolicy.Explicit }),
      ).rejects.toMatchObject({
        name: 'StreamConfigConflictError',
        stream: 'S4C',
        consumer: 'd',
        field: 'ack_policy',
      });
    } finally {
      await app.close();
    }
  });

  it('verify mode rejects missing stream and consumer', async () => {
    const { app, sm } = await bareApp();
    try {
      await expect(sm.ensureStream({ name: 'S5', subjects: ['s5'] }, 'verify')).rejects.toThrow(
        NatsConfigError,
      );
      await expect(sm.ensureConsumer('S5', { durable_name: 'd' }, 'verify')).rejects.toThrow(
        NatsConfigError,
      );
      await sm.ensureStream({ name: 'S5B', subjects: ['s5b'] });
      await expect(sm.ensureConsumer('S5B', { durable_name: 'd' }, 'verify')).rejects.toThrow(
        /does not exist/,
      );
    } finally {
      await app.close();
    }
  });

  it('verify mode lists drifted fields and passes when equal', async () => {
    const { app, sm } = await bareApp();
    try {
      await sm.ensureStream({ name: 'S5V', subjects: ['s5v'], max_msgs: 10 });
      await expect(
        sm.ensureStream({ name: 'S5V', subjects: ['s5v'], max_msgs: 20 }, 'verify'),
      ).rejects.toThrow(/max_msgs/);
      const info = await sm.ensureStream(
        { name: 'S5V', subjects: ['s5v'], max_msgs: 10 },
        'verify',
      );
      expect(info?.config.max_msgs).toBe(10);
    } finally {
      await app.close();
    }
  });

  it('verify mode passes for a stream declared with metadata and false/empty fields', async () => {
    const { app, sm } = await bareApp();
    try {
      const cfg = {
        name: 'SMD',
        subjects: ['smd.>'],
        description: '',
        metadata: { team: 'x' },
        discard_new_per_subject: false,
        allow_rollup_hdrs: false,
        deny_delete: false,
        allow_msg_ttl: false,
      };
      await sm.ensureStream(cfg);
      const info = await sm.ensureStream(cfg, 'verify');
      expect(info?.config.metadata?.team).toBe('x');
      const jsm = await sm.jsm();
      const update = vi.spyOn(jsm.streams, 'update');
      await sm.ensureStream(cfg, 'create-or-update');
      expect(update).not.toHaveBeenCalled();
      update.mockRestore();
    } finally {
      await app.close();
    }
  });

  it('verify mode passes for a consumer declared with metadata and false/empty fields', async () => {
    const { app, sm } = await bareApp();
    try {
      await sm.ensureStream({ name: 'SMDC', subjects: ['smdc.>'] });
      const cfg = {
        durable_name: 'd',
        description: '',
        metadata: { team: 'x' },
        headers_only: false,
        mem_storage: false,
        backoff: [],
        filter_subjects: [],
      };
      await sm.ensureConsumer('SMDC', cfg);
      const info = await sm.ensureConsumer('SMDC', cfg, 'verify');
      expect(info?.config.metadata?.team).toBe('x');
      const jsm = await sm.jsm();
      const update = vi.spyOn(jsm.consumers, 'update');
      await sm.ensureConsumer('SMDC', cfg, 'create-or-update');
      expect(update).not.toHaveBeenCalled();
      update.mockRestore();
    } finally {
      await app.close();
    }
  });

  it('verify mode fails on a drifted consumer', async () => {
    const { app, sm } = await bareApp();
    try {
      await sm.ensureStream({ name: 'SDC', subjects: ['sdc.>'] });
      await sm.ensureConsumer('SDC', { durable_name: 'd', max_ack_pending: 10 });
      await expect(
        sm.ensureConsumer('SDC', { durable_name: 'd', max_ack_pending: 20 }, 'verify'),
      ).rejects.toThrow(/max_ack_pending/);
    } finally {
      await app.close();
    }
  });

  it('create-only leaves an existing consumer untouched', async () => {
    const { app, sm } = await bareApp();
    try {
      await sm.ensureStream({ name: 'SCO', subjects: ['sco.>'] });
      await sm.ensureConsumer('SCO', { durable_name: 'd', max_ack_pending: 10 });
      const ci = await sm.ensureConsumer(
        'SCO',
        { durable_name: 'd', max_ack_pending: 20 },
        'create-only',
      );
      expect(ci?.config.max_ack_pending).toBe(10);
      expect((await (await sm.jsm()).consumers.info('SCO', 'd')).config.max_ack_pending).toBe(10);
    } finally {
      await app.close();
    }
  });

  it('does not apply consumer defaults to an existing consumer', async () => {
    const { app, sm } = await bareApp();
    try {
      await sm.ensureStream({ name: 'SXD', subjects: ['sxd.>'] });
      const jsm = await sm.jsm();
      await jsm.consumers.add('SXD', {
        durable_name: 'd',
        ack_policy: AckPolicy.Explicit,
        max_deliver: 5,
      });
      const ci = await sm.ensureConsumer('SXD', { durable_name: 'd', max_ack_pending: 50 });
      expect(ci?.config.max_deliver).toBe(5);
      expect(ci?.config.max_ack_pending).toBe(50);
      expect((await jsm.consumers.info('SXD', 'd')).config.max_deliver).toBe(5);
      await expect(
        sm.ensureConsumer('SXD', { durable_name: 'd', max_ack_pending: 50 }, 'verify'),
      ).resolves.toBeDefined();
    } finally {
      await app.close();
    }
  });

  it('applies consumer defaults to a consumer it creates', async () => {
    const { app, sm } = await bareApp();
    try {
      await sm.ensureStream({ name: 'SND', subjects: ['snd.>'] });
      const ci = await sm.ensureConsumer('SND', { durable_name: 'd' });
      expect(ci?.config.max_deliver).toBe(-1);
      expect(ci?.config.ack_policy).toBe('explicit');
    } finally {
      await app.close();
    }
  });

  it('warns instead of failing when an existing consumer does not use explicit acks', async () => {
    const logger: LoggerService = { log: vi.fn(), error: vi.fn(), warn: vi.fn() };
    const { app, sm } = await bareApp({ logger });
    try {
      await sm.ensureStream({ name: 'SNA', subjects: ['sna.>'] });
      const jsm = await sm.jsm();
      await jsm.consumers.add('SNA', { durable_name: 'd', ack_policy: AckPolicy.None });
      const ci = await sm.ensureConsumer('SNA', { durable_name: 'd' });
      expect(ci?.config.ack_policy).toBe('none');
      expect(logger.warn).toHaveBeenCalledWith(expect.stringMatching(/SNA\/d.*ack_policy.*none/));
    } finally {
      await app.close();
    }
  });

  it('create-only leaves existing stream untouched', async () => {
    const { app, sm } = await bareApp();
    try {
      await sm.ensureStream({ name: 'S6', subjects: ['s6.>'], max_msgs: 10 });
      const info = await sm.ensureStream(
        { name: 'S6', subjects: ['s6.>'], max_msgs: 20 },
        'create-only',
      );
      expect(info?.config.max_msgs).toBe(10);
      expect((await sm.info('S6')).config.max_msgs).toBe(10);
    } finally {
      await app.close();
    }
  });

  it('none does not touch the server', async () => {
    const { app, sm } = await bareApp();
    try {
      expect(await sm.ensureStream({ name: 'S7', subjects: ['s7'] }, 'none')).toBeUndefined();
      expect(await sm.ensureConsumer('S7', { durable_name: 'd' }, 'none')).toBeUndefined();
      await expect(sm.info('S7')).rejects.toThrow();
    } finally {
      await app.close();
    }
  });

  it('uses options.jetstream.sync as the default mode', async () => {
    const { app, sm } = await bareApp({ jetstream: { sync: 'verify' } });
    try {
      await expect(sm.ensureStream({ name: 'S7V', subjects: ['s7v'] })).rejects.toThrow(
        NatsConfigError,
      );
    } finally {
      await app.close();
    }
  });

  it('purge and delete', async () => {
    const { app, sm, js } = await bareApp();
    try {
      await sm.ensureStream({ name: 'S8', subjects: ['s8.>'] });
      await js.publish('s8.x', { a: 1 });
      expect((await sm.info('S8')).state.messages).toBe(1);
      expect((await sm.purge('S8')).purged).toBe(1);
      expect(await sm.delete('S8')).toBe(true);
    } finally {
      await app.close();
    }
  });

  it('publish returns PubAck and dedupes by msgId', async () => {
    const app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: url },
          logger: false,
          streams: [{ name: 'S9', subjects: ['s9.>'] }],
        }),
      ],
    });
    try {
      const js = app.get<JetStreamClient>(getJetStreamClientToken());
      const a = await js.publish('s9.x', { a: 1 }, { msgId: 'm1' });
      const b = await js.publish('s9.x', { a: 1 }, { msgId: 'm1' });
      expect(a.stream).toBe('S9');
      expect(a.duplicate).toBe(false);
      expect(b.duplicate).toBe(true);
    } finally {
      await app.close();
    }
  });

  it('exposes msgId to outbound plugins as the Nats-Msg-Id header', async () => {
    const seen: Array<string | undefined> = [];
    const plugin: NatsPlugin = {
      name: 'p',
      outbound: {
        async handle(m, next) {
          seen.push(m.headers.has('Nats-Msg-Id') ? m.headers.get('Nats-Msg-Id') : undefined);
          // A plugin may set the id itself, e.g. derived from the payload.
          if (!m.headers.has('Nats-Msg-Id')) m.headers.set('Nats-Msg-Id', 'from-plugin');
          await next();
        },
      },
    };
    const { app, sm, js } = await bareApp({ plugins: [plugin] });
    try {
      await sm.ensureStream({ name: 'S9P', subjects: ['s9p.>'] });
      const a = await js.publish('s9p.x', 1, { msgId: 'm1' });
      const b = await js.publish('s9p.x', 1, { msgId: 'm1' });
      const c = await js.publish('s9p.x', 2);
      const d = await js.publish('s9p.x', 3);
      expect(seen).toEqual(['m1', 'm1', undefined, undefined]);
      expect([a.duplicate, b.duplicate, c.duplicate, d.duplicate]).toEqual([
        false,
        true,
        false,
        true,
      ]);
      const jsm = await sm.jsm();
      const msg = await jsm.streams.getMessage('S9P', { seq: 1 });
      expect(msg?.header.get('Nats-Msg-Id')).toBe('m1');
    } finally {
      await app.close();
    }
  });

  it('publish runs the outbound pipeline with kind jetstream and honors expect', async () => {
    const seen: string[] = [];
    const plugin: NatsPlugin = {
      name: 'p',
      outbound: {
        async handle(m, next) {
          seen.push(m.kind);
          m.headers.set('X-Test', '1');
          await next();
        },
      },
    };
    const { app, sm, js } = await bareApp({ plugins: [plugin] });
    try {
      await sm.ensureStream({ name: 'S10', subjects: ['s10.>'] });
      await js.publish('s10.x', 'hi', { headers: { A: 'b' } });
      expect(seen).toEqual(['jetstream']);
      const jsm = await sm.jsm();
      const msg = await jsm.streams.getMessage('S10', { seq: 1 });
      expect(msg?.header.get('X-Test')).toBe('1');
      expect(msg?.header.get('A')).toBe('b');
      await expect(js.publish('s10.x', 'hi', { expect: { lastSequence: 99 } })).rejects.toThrow();
    } finally {
      await app.close();
    }
  });

  it('registers one stream manager per connection', async () => {
    const { app } = await bareApp();
    try {
      expect(app.get(NatsConnectionManager).names()).toEqual(['default']);
    } finally {
      await app.close();
    }
  });
});

describe('JetStream removed fields', () => {
  let srv: NatsTestServer;
  let url: string;

  beforeAll(async () => {
    srv = await startNats();
    url = srv.url;
  });

  afterAll(async () => {
    await srv?.stop();
  });

  async function boot(extra: Record<string, unknown>) {
    return createTestApp({
      imports: [NatsModule.forRoot({ connection: { servers: url }, logger: false, ...extra })],
    });
  }

  it('resets a stream field removed from the declaration', async () => {
    const first = await boot({
      streams: [{ name: 'RM', subjects: ['rm.>'], max_age: 60_000_000_000 }],
    });
    await first.close();
    const app = await boot({ streams: [{ name: 'RM', subjects: ['rm.>'] }] });
    try {
      const jsm = await app.get<NatsStreamManager>(getStreamManagerToken()).jsm();
      const { config } = await jsm.streams.info('RM');
      expect(config.max_age).toBe(0);
      expect(config.metadata?.['nestjs-nats.declared']).toBe('subjects');
    } finally {
      await app.close();
    }
  });

  it('resets a consumer field removed from the declaration', async () => {
    const streams = [{ name: 'RC', subjects: ['rc.>'] }];
    const first = await boot({
      streams,
      consumers: [{ stream: 'RC', config: { durable_name: 'c', max_ack_pending: 5 } }],
    });
    await first.close();
    const app = await boot({
      streams,
      consumers: [{ stream: 'RC', config: { durable_name: 'c' } }],
    });
    try {
      const jsm = await app.get<NatsStreamManager>(getStreamManagerToken()).jsm();
      expect((await jsm.consumers.info('RC', 'c')).config.max_ack_pending).not.toBe(5);
    } finally {
      await app.close();
    }
  });

  it('fails verify for a removed field', async () => {
    const first = await boot({
      streams: [{ name: 'RV', subjects: ['rv.>'], max_age: 60_000_000_000 }],
    });
    await first.close();
    await expect(
      boot({
        jetstream: { sync: 'verify' },
        streams: [{ name: 'RV', subjects: ['rv.>'] }],
      }),
    ).rejects.toThrow(/max_age \(removed from the declaration/);
  });
});
