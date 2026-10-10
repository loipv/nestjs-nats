import { Injectable, Module } from '@nestjs/common';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { NatsClient } from '../../src/client/nats-client.js';
import {
  type ConnectionEntry,
  NatsConnectionManager,
} from '../../src/connection/connection-manager.js';
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
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

/** Records every connection the managers open, also when connect() rejects afterwards. */
function recordConnections(): ConnectionEntry[] {
  const opened: ConnectionEntry[] = [];
  const proto = NatsConnectionManager.prototype as unknown as {
    openConnection: (...args: unknown[]) => Promise<ConnectionEntry['nc']>;
  };
  const original = proto.openConnection;
  vi.spyOn(proto, 'openConnection').mockImplementation(async function (
    this: unknown,
    ...args: unknown[]
  ) {
    const nc = await original.apply(this, args);
    opened.push({ name: args[0] as string, nc } as ConnectionEntry);
    return nc;
  });
  return opened;
}

describe('NatsModule', () => {
  let srv: NatsTestServer;
  let url: string;

  afterEach(() => {
    vi.restoreAllMocks();
  });

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

  it('forRootAsync passes only the injected dependencies to the factory', async () => {
    @Module({ providers: [{ provide: 'NATS_URL', useValue: url }], exports: ['NATS_URL'] })
    class ConfigModule {}
    let received: unknown[] = [];
    const app = await createTestApp({
      imports: [
        NatsModule.forRootAsync({
          imports: [ConfigModule],
          inject: ['NATS_URL'],
          useFactory: (...args: unknown[]) => {
            received = args;
            return { connection: { servers: args[0] as string }, logger: false };
          },
        }),
      ],
    });
    try {
      expect(received).toEqual([url]);
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
    const opened = recordConnections();
    await expect(
      createTestApp({
        imports: [
          NatsModule.forRoot({ connection: { servers: url }, logger: false }),
          NatsModule.forRoot({ connection: { servers: [url] }, logger: false }),
        ],
      }),
    ).rejects.toThrow(NatsConfigError);
    // The first connection may still be connecting when the duplicate is rejected.
    await eventually(() => expect(opened).toHaveLength(1));
    await eventually(() => expect(opened.every((e) => e.nc.isClosed())).toBe(true));
  });

  it('closes the connections already open when another options factory fails during DI', async () => {
    const opened = recordConnections();
    await expect(
      createTestApp({
        imports: [
          NatsModule.forRoot({ connection: { servers: url }, logger: false }),
          NatsModule.forRootAsync({
            name: 'b',
            useFactory: async () => {
              // Let the default connection open first.
              await sleep(200);
              throw new Error('missing NATS_B_URL');
            },
          }),
        ],
      }),
    ).rejects.toThrow('missing NATS_B_URL');
    expect(opened.map((e) => e.name)).toEqual(['default']);
    expect(opened.every((e) => e.nc.isClosed())).toBe(true);
  });

  it('does not leave open a connection whose options resolve after another one failed', async () => {
    const opened = recordConnections();
    await expect(
      createTestApp({
        imports: [
          NatsModule.forRootAsync({
            useFactory: async () => {
              // Still connecting when the other connection fails.
              await sleep(200);
              return { connection: { servers: url }, logger: false };
            },
          }),
          NatsModule.forRootAsync({
            name: 'b',
            useFactory: () => {
              throw new Error('missing NATS_B_URL');
            },
          }),
        ],
      }),
    ).rejects.toThrow('missing NATS_B_URL');
    // Without the abort, the default connection would open after its factory returns.
    await sleep(500);
    expect(opened.every((e) => e.nc.isClosed())).toBe(true);
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
