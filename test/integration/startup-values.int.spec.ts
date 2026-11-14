import {
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream';
import { connect, type NatsConnection } from '@nats-io/transport-node';
import { Injectable } from '@nestjs/common';
import type { TestingModule } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Consume, Subscribe } from '../../src/decorators/handler.decorators.js';
import { Payload } from '../../src/decorators/param.decorators.js';
import type { StartupContext } from '../../src/decorators/startup-value.js';
import { NatsModule } from '../../src/module/nats.module.js';
import { createTestApp } from './support/app.js';
import { eventually, sleep } from './support/eventually.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

const enc = new TextEncoder();

interface Config {
  prefix: string;
  stream: string;
  durable: string;
}

const config = ({ get }: StartupContext) => get<Config>('CONFIG');

@Injectable()
class Handlers {
  readonly core: unknown[] = [];
  readonly js: unknown[] = [];

  @Subscribe((ctx) => `${config(ctx).prefix}.core`)
  onCore(@Payload() body: unknown) {
    this.core.push(body);
  }

  @Consume((ctx) => ({
    stream: config(ctx).stream,
    durable: config(ctx).durable,
    config: { filter_subject: `${config(ctx).prefix}.js` },
  }))
  onJs(@Payload() body: unknown) {
    this.js.push(body);
  }
}

describe('startup values', () => {
  let srv: NatsTestServer;
  let nc: NatsConnection;
  let js: JetStreamClient;
  let jsm: JetStreamManager;
  let app: TestingModule;
  let handlers: Handlers;

  beforeAll(async () => {
    srv = await startNats();
    nc = await connect({ servers: srv.url });
    js = jetstream(nc);
    jsm = await jetstreamManager(nc);
    app = await createTestApp({
      imports: [
        NatsModule.forRoot({
          connection: { servers: srv.url },
          logger: false,
          streams: [{ name: 'SV', subjects: ['sv.>'] }],
        }),
      ],
      providers: [
        Handlers,
        { provide: 'CONFIG', useValue: { prefix: 'sv', stream: 'SV', durable: 'sv-d' } },
      ],
    });
    handlers = app.get(Handlers);
  });

  afterAll(async () => {
    await app?.close();
    await nc?.close();
    await srv?.stop();
  });

  it('creates the consumer named by the configuration', async () => {
    const info = await jsm.consumers.info('SV', 'sv-d');
    expect(info.config.filter_subject).toBe('sv.js');
  });

  it('delivers to the subject and the stream the configuration names', async () => {
    nc.publish('sv.core', enc.encode(JSON.stringify({ id: 'core' })));
    await js.publish('sv.js', enc.encode(JSON.stringify({ id: 'js' })));
    await eventually(() => {
      expect(handlers.core).toEqual([{ id: 'core' }]);
      expect(handlers.js).toEqual([{ id: 'js' }]);
    });
    await sleep(200);
    expect(handlers.core).toHaveLength(1);
    expect(handlers.js).toHaveLength(1);
  });
});
