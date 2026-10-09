import type { JetStreamManager, JetStreamClient as NatsJetStreamClient } from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/transport-node';
import {
  type DynamicModule,
  Global,
  Injectable,
  Module,
  type OnApplicationShutdown,
  type Provider,
} from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { NatsClient } from '../client/nats-client.js';
import { jsonCodec } from '../codec/codec.js';
import { NatsConnectionManager } from '../connection/connection-manager.js';
import { HandlerExplorer } from '../discovery/handler-explorer.js';
import { NatsConfigError } from '../errors/errors.js';
import { JetStreamClient } from '../jetstream/jetstream-client.js';
import { NatsStreamManager } from '../jetstream/stream-manager.js';
import type { NatsModuleOptions } from '../module/options.js';
import {
  DEFAULT_CONNECTION,
  getConnectionToken,
  getJetStreamClientToken,
  getModuleOptionsToken,
  getNatsClientToken,
  getStreamManagerToken,
} from '../module/tokens.js';
import { FakeNatsConnection } from './fake-connection.js';
import { NatsTestHarness } from './harness.js';
import { mockJetStreamClientOf, mockNatsClientOf } from './mock-clients.js';

/**
 * Internal: a connection manager whose connections are fake: nothing is sent over the
 * network and JetStream management is unavailable.
 */
@Injectable()
export class TestingNatsConnectionManager
  extends NatsConnectionManager
  implements OnApplicationShutdown
{
  protected override openConnection(
    _name: string,
    options: NatsModuleOptions,
  ): Promise<NatsConnection> {
    const fake = new FakeNatsConnection(options.codec ?? jsonCodec());
    return Promise.resolve(fake as unknown as NatsConnection);
  }

  protected override createJetStream(nc: NatsConnection): NatsJetStreamClient {
    return (nc as unknown as FakeNatsConnection).jetstream() as unknown as NatsJetStreamClient;
  }

  protected override createJetStreamManager(): Promise<JetStreamManager> {
    return Promise.reject(
      new NatsConfigError('JetStream management is not available in NatsTestingModule'),
    );
  }

  /** Runs the close hooks (e.g. flushes batchers) and closes the stores the library created. */
  async onApplicationShutdown(): Promise<void> {
    await this.closeAll();
    for (const { store, owned } of this.idempotencyStores()) {
      if (owned) await store.close?.();
    }
  }
}

@Global()
@Module({
  imports: [DiscoveryModule],
  providers: [
    { provide: NatsConnectionManager, useClass: TestingNatsConnectionManager },
    HandlerExplorer,
    NatsTestHarness,
  ],
  exports: [NatsConnectionManager, HandlerExplorer, NatsTestHarness],
})
class NatsTestingCoreModule {}

@Module({})
// biome-ignore lint/complexity/noStaticOnlyClass: Nest dynamic-module convention
export class NatsTestingModule {
  /**
   * Replaces `NatsModule.forRoot` in tests: registers the same providers for the
   * connection (`NatsClient` and `JetStreamClient` as mocks recording what is published,
   * the fake connection, `NatsStreamManager`; injectable by class for the `'default'`
   * connection), plus `NatsConnectionManager` and `NatsTestHarness`. It never connects,
   * syncs streams nor subscribes: messages reach handlers through `NatsTestHarness.dispatch`.
   */
  static forRoot(options: Omit<NatsModuleOptions, 'connection'> = {}): DynamicModule {
    const name = options.name ?? DEFAULT_CONNECTION;
    const optionsToken = getModuleOptionsToken(name);
    const connectionToken = getConnectionToken(name);
    const clientToken = getNatsClientToken(name);
    const jetStreamToken = getJetStreamClientToken(name);
    const streamManagerToken = getStreamManagerToken(name);
    const moduleOptions: NatsModuleOptions = { ...options, name, connection: {} };
    const providers: Provider[] = [
      { provide: optionsToken, useValue: moduleOptions },
      {
        provide: connectionToken,
        useFactory: async (
          manager: NatsConnectionManager,
          opts: NatsModuleOptions,
        ): Promise<NatsConnection> => (await manager.connect(name, opts)).nc,
        inject: [NatsConnectionManager, optionsToken],
      },
      {
        provide: clientToken,
        useFactory: (manager: NatsConnectionManager) => mockNatsClientOf(manager.get(name)),
        inject: [NatsConnectionManager, connectionToken],
      },
      {
        provide: jetStreamToken,
        useFactory: (manager: NatsConnectionManager) => mockJetStreamClientOf(manager.get(name)),
        inject: [NatsConnectionManager, connectionToken],
      },
      {
        provide: streamManagerToken,
        useFactory: (manager: NatsConnectionManager) => new NatsStreamManager(manager.get(name)),
        inject: [NatsConnectionManager, connectionToken],
      },
    ];
    const exports: NonNullable<DynamicModule['exports']> = [
      optionsToken,
      connectionToken,
      clientToken,
      jetStreamToken,
      streamManagerToken,
    ];
    if (name === DEFAULT_CONNECTION) {
      providers.push(
        { provide: NatsClient, useExisting: clientToken },
        { provide: JetStreamClient, useExisting: jetStreamToken },
        { provide: NatsStreamManager, useExisting: streamManagerToken },
      );
      exports.push(NatsClient, JetStreamClient, NatsStreamManager);
    }
    return {
      module: NatsTestingModule,
      global: true,
      imports: [NatsTestingCoreModule],
      providers,
      exports,
    };
  }
}
