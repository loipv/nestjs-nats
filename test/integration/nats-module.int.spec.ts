import { Injectable, Module } from '@nestjs/common';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NatsClient } from '../../src/client/nats-client.js';
import { NatsConnectionManager } from '../../src/connection/connection-manager.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import { JetStreamClient } from '../../src/jetstream/jetstream-client.js';
import { NatsStreamManager } from '../../src/jetstream/stream-manager.js';
import { InjectNatsClient } from '../../src/module/inject.decorators.js';
import { NatsModule } from '../../src/module/nats.module.js';
import {
  getConnectionToken,
  getJetStreamClientToken,
  getNatsClientToken,
  getStreamManagerToken,
  NATS_FEATURE_TOKEN_PREFIX,
} from '../../src/module/tokens.js';
import { createTestApp } from './support/app.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

describe('NatsModule', () => {
  let srv: NatsTestServer;
  let url: string;

  beforeAll(async () => {
    srv = await startNats();
    url = srv.url;
  });

  afterAll(async () => {
    await srv?.stop();
  });

  it('registers two named connections', async () => {
    const app = await createTestApp({
      imports: [
        NatsModule.forRoot({ connection: { servers: url }, logger: false }),
        NatsModule.forRoot({ name: 'b', connection: { servers: url }, logger: false }),
      ],
    });
    try {
      expect(app.get(getNatsClientToken())).toBeInstanceOf(NatsClient);
      expect(app.get(getNatsClientToken('b'))).not.toBe(app.get(getNatsClientToken()));
      expect(app.get(NatsConnectionManager).names().sort()).toEqual(['b', 'default']);
    } finally {
      await app.close();
    }
  });

  it('forRootAsync uses the factory', async () => {
    const app = await createTestApp({
      imports: [
        NatsModule.forRootAsync({
          useFactory: () => ({ connection: { servers: url }, logger: false }),
        }),
      ],
    });
    try {
      expect(app.get(getNatsClientToken())).toBeInstanceOf(NatsClient);
      expect(app.get(getConnectionToken()).isClosed()).toBe(false);
    } finally {
      await app.close();
    }
  });

  it('forRootAsync supports useClass', async () => {
    class Factory {
      createNatsOptions() {
        return { connection: { servers: url }, logger: false as const };
      }
    }
    const app = await createTestApp({
      imports: [NatsModule.forRootAsync({ name: 'cls', useClass: Factory })],
    });
    try {
      expect(app.get(getNatsClientToken('cls'))).toBeInstanceOf(NatsClient);
    } finally {
      await app.close();
    }
  });

  it('supports @InjectNatsClient in consumers', async () => {
    @Injectable()
    class Svc {
      constructor(@InjectNatsClient() readonly client: NatsClient) {}
    }
    @Module({ providers: [Svc] })
    class FeatureModule {}
    const app = await createTestApp({
      imports: [NatsModule.forRoot({ connection: { servers: url }, logger: false }), FeatureModule],
    });
    try {
      expect(app.get(Svc).client).toBeInstanceOf(NatsClient);
    } finally {
      await app.close();
    }
  });

  it('injects the default connection clients by class', async () => {
    @Injectable()
    class Svc {
      constructor(
        readonly client: NatsClient,
        readonly js: JetStreamClient,
        readonly streams: NatsStreamManager,
      ) {}
    }
    @Module({ providers: [Svc] })
    class FeatureModule {}
    const app = await createTestApp({
      imports: [
        NatsModule.forRoot({ connection: { servers: url }, logger: false }),
        NatsModule.forRoot({ name: 'b', connection: { servers: url }, logger: false }),
        FeatureModule,
      ],
    });
    try {
      const svc = app.get(Svc);
      expect(svc.client).toBe(app.get(getNatsClientToken()));
      expect(svc.js).toBe(app.get(getJetStreamClientToken()));
      expect(svc.streams).toBe(app.get(getStreamManagerToken()));
      expect(app.get(NatsClient)).toBe(app.get(getNatsClientToken()));
    } finally {
      await app.close();
    }
  });

  it('does not register class aliases for named connections only', async () => {
    const app = await createTestApp({
      imports: [NatsModule.forRoot({ name: 'only', connection: { servers: url }, logger: false })],
    });
    try {
      expect(() => app.get(NatsClient)).toThrow();
    } finally {
      await app.close();
    }
  });

  it('forFeature provides a feature token', () => {
    const mod = NatsModule.forFeature({ streams: [{ name: 'S' }] });
    expect(mod.global).toBeFalsy();
    const provider = (mod.providers ?? [])[0] as { provide: string; useValue: unknown };
    expect(provider.provide.startsWith(NATS_FEATURE_TOKEN_PREFIX)).toBe(true);
    expect(provider.useValue).toEqual({ connection: 'default', streams: [{ name: 'S' }] });
  });

  it('fails bootstrap on duplicate name', async () => {
    await expect(
      createTestApp({
        imports: [
          NatsModule.forRoot({ connection: { servers: url }, logger: false }),
          NatsModule.forRoot({ connection: { servers: [url] }, logger: false }),
        ],
      }),
    ).rejects.toThrow(NatsConfigError);
  });

  it('close() drains connections', async () => {
    const app = await createTestApp({
      imports: [NatsModule.forRoot({ connection: { servers: url }, logger: false })],
    });
    const mgr = app.get(NatsConnectionManager);
    expect(mgr.status().state).toBe('connected');
    await app.close();
    expect(mgr.status().state).toBe('closed');
  });
});
