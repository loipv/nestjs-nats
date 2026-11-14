import { Kvm } from '@nats-io/kv';
import { connect, type NatsConnection } from '@nats-io/transport-node';
import { Injectable, type LoggerService } from '@nestjs/common';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { ConnectionRegistry } from '../../src/connection/connection-registry.js';
import { InjectKv, NatsKvModule } from '../../src/kv/index.js';
import type { KvBucketDeclaration } from '../../src/kv/kv.module.js';
import { NatsModule } from '../../src/module/nats.module.js';
import type { StreamSyncMode } from '../../src/module/options.js';
import { createTestApp } from './support/app.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

describe('NatsKvModule', () => {
  let srv: NatsTestServer;
  let rawNc: NatsConnection;

  beforeAll(async () => {
    srv = await startNats();
    rawNc = await connect({ servers: srv.url });
  });

  afterAll(async () => {
    await rawNc?.close();
    await srv?.stop();
  });

  /** Boots an app declaring `buckets`, with a provider injecting the first one. */
  async function boot(
    buckets: KvBucketDeclaration[],
    sync?: StreamSyncMode,
    logger: false | LoggerService = false,
  ) {
    const bucket = buckets[0]?.bucket as string;
    @Injectable()
    class Store {
      constructor(@InjectKv(bucket) readonly kv: import('@nats-io/kv').KV) {}
    }
    const app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger,
          ...(sync ? { jetstream: { sync } } : {}),
        }),
        NatsKvModule.forFeature({ buckets }),
      ],
      providers: [Store],
    });
    return { app, kv: app.get(Store).kv };
  }

  it('creates the bucket and injects it', async () => {
    const { app, kv } = await boot([{ bucket: 'kvi-new', history: 3 }]);
    try {
      await kv.put('a', 'one');
      expect((await kv.get('a'))?.string()).toBe('one');
      const status = await (await new Kvm(rawNc).open('kvi-new')).status();
      expect(status.history).toBe(3);
    } finally {
      await app.close();
    }
  });

  it('warns about a drifted bucket under create-or-update, and uses it as it is', async () => {
    await new Kvm(rawNc).create('kvi-drift', { history: 1 });
    const warn = vi.fn();
    const { app } = await boot([{ bucket: 'kvi-drift', history: 5 }], 'create-or-update', {
      log() {},
      error() {},
      warn,
    });
    try {
      expect(warn).toHaveBeenCalledWith(
        'KV bucket "kvi-drift" on connection "default" has history 1, declared 5; the library does not update KV buckets: change it with `nats kv` or recreate the bucket',
      );
    } finally {
      await app.close();
    }
  });

  it('fails bootstrap under verify when the bucket is missing, and closes the connection', async () => {
    const abort = vi.spyOn(ConnectionRegistry.prototype, 'abortStartup');
    try {
      await expect(boot([{ bucket: 'kvi-missing' }], 'verify')).rejects.toThrow(
        'KV bucket "kvi-missing" on connection "default" does not exist',
      );
      expect(abort).toHaveBeenCalled();
    } finally {
      abort.mockRestore();
    }
  });

  it('fails bootstrap under verify when the bucket differs from its declaration', async () => {
    await new Kvm(rawNc).create('kvi-verify', { history: 2 });
    await expect(boot([{ bucket: 'kvi-verify', history: 4 }], 'verify')).rejects.toThrow(
      'KV bucket "kvi-verify" on connection "default" differs from its declaration: history (current: 2, declared: 4)',
    );
  });

  it('opens an existing bucket under none', async () => {
    const raw = await new Kvm(rawNc).create('kvi-none');
    await raw.put('k', 'v');
    const { app, kv } = await boot([{ bucket: 'kvi-none', history: 9 }], 'none');
    try {
      expect((await kv.get('k'))?.string()).toBe('v');
    } finally {
      await app.close();
    }
  });
});
