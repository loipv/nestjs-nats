import { randomUUID } from 'node:crypto';
import type { NatsConnection } from '@nats-io/transport-node';
import {
  type DynamicModule,
  type InjectionToken,
  Module,
  type ModuleMetadata,
  type Provider,
  type Type,
} from '@nestjs/common';
import { NatsClient } from '../client/nats-client.js';
import { NatsConnectionManager } from '../connection/connection-manager.js';
import { NatsConfigError } from '../errors/errors.js';
import { JetStreamClient } from '../jetstream/jetstream-client.js';
import { NatsStreamManager } from '../jetstream/stream-manager.js';
import { NatsCoreModule } from './nats-core.module.js';
import type { NatsFeatureOptions, NatsModuleOptions } from './options.js';
import {
  DEFAULT_CONNECTION,
  getConnectionToken,
  getJetStreamClientToken,
  getModuleOptionsToken,
  getNatsClientToken,
  getStreamManagerToken,
  NATS_FEATURE_TOKEN_PREFIX,
} from './tokens.js';

type NatsOptionsValue = Omit<NatsModuleOptions, 'name'>;

export interface NatsOptionsFactory {
  createNatsOptions(): NatsOptionsValue | Promise<NatsOptionsValue>;
}

export interface NatsModuleAsyncOptions {
  name?: string;
  imports?: ModuleMetadata['imports'];
  inject?: InjectionToken[];
  // biome-ignore lint/suspicious/noExplicitAny: factory arguments are user-defined
  useFactory?: (...args: any[]) => NatsOptionsValue | Promise<NatsOptionsValue>;
  useClass?: Type<NatsOptionsFactory>;
  useExisting?: Type<NatsOptionsFactory>;
}

@Module({})
// biome-ignore lint/complexity/noStaticOnlyClass: Nest dynamic-module convention
export class NatsModule {
  static forRoot(options: NatsModuleOptions): DynamicModule {
    const name = options.name ?? DEFAULT_CONNECTION;
    return NatsModule.build(
      name,
      [],
      [{ provide: getModuleOptionsToken(name), useValue: { ...options, name } }],
    );
  }

  static forRootAsync(options: NatsModuleAsyncOptions): DynamicModule {
    const name = options.name ?? DEFAULT_CONNECTION;
    const optionsToken = getModuleOptionsToken(name);
    const providers: Provider[] = [];
    if (options.useFactory) {
      const { useFactory } = options;
      providers.push({
        provide: optionsToken,
        // The manager is injected last, after the user's dependencies.
        useFactory: async (...args: unknown[]) => {
          const manager = args.pop() as NatsConnectionManager;
          return { ...(await abortOnFailure(manager, () => useFactory(...args))), name };
        },
        inject: [...(options.inject ?? []), NatsConnectionManager],
      });
    } else if (options.useClass || options.useExisting) {
      const factoryType = (options.useClass ?? options.useExisting) as Type<NatsOptionsFactory>;
      if (options.useClass) providers.push({ provide: factoryType, useClass: factoryType });
      providers.push({
        provide: optionsToken,
        useFactory: async (factory: NatsOptionsFactory, manager: NatsConnectionManager) => ({
          ...(await abortOnFailure(manager, () => factory.createNatsOptions())),
          name,
        }),
        inject: [factoryType, NatsConnectionManager],
      });
    } else {
      throw new NatsConfigError(
        'NatsModule.forRootAsync requires useFactory, useClass or useExisting',
      );
    }
    return NatsModule.build(name, options.imports ?? [], providers);
  }

  static forFeature(options: NatsFeatureOptions): DynamicModule {
    return {
      module: NatsModule,
      providers: [
        {
          provide: `${NATS_FEATURE_TOKEN_PREFIX}${randomUUID()}`,
          useValue: { connection: DEFAULT_CONNECTION, ...options },
        },
      ],
    };
  }

  private static build(
    name: string,
    imports: NonNullable<ModuleMetadata['imports']>,
    optionProviders: Provider[],
  ): DynamicModule {
    const optionsToken = getModuleOptionsToken(name);
    const connectionToken = getConnectionToken(name);
    const clientToken = getNatsClientToken(name);
    const jetStreamToken = getJetStreamClientToken(name);
    const streamManagerToken = getStreamManagerToken(name);
    const providers: Provider[] = [
      ...optionProviders,
      {
        provide: connectionToken,
        useFactory: async (
          manager: NatsConnectionManager,
          opts: NatsModuleOptions,
        ): Promise<NatsConnection> =>
          (await abortOnFailure(manager, () => manager.connect(name, opts))).nc,
        inject: [NatsConnectionManager, optionsToken],
      },
      {
        provide: clientToken,
        // Injecting the connection token guarantees the connection exists first.
        useFactory: (manager: NatsConnectionManager) => new NatsClient(manager.get(name)),
        inject: [NatsConnectionManager, connectionToken],
      },
      {
        provide: jetStreamToken,
        useFactory: (manager: NatsConnectionManager) => new JetStreamClient(manager.get(name)),
        inject: [NatsConnectionManager, connectionToken],
      },
      {
        provide: streamManagerToken,
        useFactory: (manager: NatsConnectionManager) => new NatsStreamManager(manager.get(name)),
        inject: [NatsConnectionManager, connectionToken],
      },
    ];
    const exports: NonNullable<ModuleMetadata['exports']> = [
      optionsToken,
      connectionToken,
      clientToken,
      jetStreamToken,
      streamManagerToken,
    ];
    if (name === DEFAULT_CONNECTION) {
      // Let the default connection's clients be injected by class as well.
      const classAliases: Provider[] = [
        { provide: NatsClient, useExisting: clientToken },
        { provide: JetStreamClient, useExisting: jetStreamToken },
        { provide: NatsStreamManager, useExisting: streamManagerToken },
      ];
      providers.push(...classAliases);
      exports.push(NatsClient, JetStreamClient, NatsStreamManager);
    }
    return {
      module: NatsModule,
      global: true,
      imports: [NatsCoreModule, ...imports],
      providers,
      exports,
    };
  }
}

/**
 * Runs a provider factory; when it fails, closes the connections already open before
 * rethrowing. A failure during dependency injection runs no lifecycle hook, so nothing
 * else would close them.
 */
async function abortOnFailure<T>(
  manager: NatsConnectionManager,
  factory: () => T | Promise<T>,
): Promise<T> {
  try {
    return await factory();
  } catch (err) {
    await manager.abortStartup();
    throw err;
  }
}
