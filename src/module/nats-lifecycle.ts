import { Injectable, type OnModuleInit } from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';
import { NatsConnectionManager } from '../connection/connection-manager.js';
import { NatsConfigError } from '../errors/errors.js';
import { NatsStreamManager } from '../jetstream/stream-manager.js';
import type { ConsumerDeclaration, NatsFeatureOptions, StreamDeclaration } from './options.js';
import { DEFAULT_CONNECTION, NATS_FEATURE_TOKEN_PREFIX } from './tokens.js';

interface Declarations {
  streams: StreamDeclaration[];
  consumers: ConsumerDeclaration[];
}

@Injectable()
export class NatsLifecycle implements OnModuleInit {
  constructor(
    private readonly manager: NatsConnectionManager,
    private readonly discovery: DiscoveryService,
  ) {}

  async onModuleInit(): Promise<void> {
    const declarations = this.collectDeclarations();
    for (const [name, { streams, consumers }] of declarations) {
      const sm = new NatsStreamManager(this.manager.get(name));
      for (const stream of streams) await sm.ensureStream(stream);
      for (const { stream, config } of consumers) await sm.ensureConsumer(stream, config);
    }
  }

  private collectDeclarations(): Map<string, Declarations> {
    const result = new Map<string, Declarations>();
    for (const name of this.manager.names()) {
      const { streams = [], consumers = [] } = this.manager.get(name).options;
      result.set(name, { streams: [...streams], consumers: [...consumers] });
    }
    for (const wrapper of this.discovery.getProviders()) {
      const { token, instance } = wrapper;
      if (typeof token !== 'string' || !token.startsWith(NATS_FEATURE_TOKEN_PREFIX)) continue;
      const feature = instance as NatsFeatureOptions | undefined;
      if (!feature) continue;
      const connection = feature.connection ?? DEFAULT_CONNECTION;
      const target = result.get(connection);
      if (!target) {
        throw new NatsConfigError(
          `NatsModule.forFeature references unknown NATS connection "${connection}"`,
        );
      }
      target.streams.push(...(feature.streams ?? []));
      target.consumers.push(...(feature.consumers ?? []));
    }
    return result;
  }
}
