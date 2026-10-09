import {
  type BeforeApplicationShutdown,
  Injectable,
  type OnApplicationBootstrap,
  type OnModuleInit,
} from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';
import type { HandlerAdapter } from '../adapters/adapter.js';
import { CoreSubscriptionAdapter } from '../adapters/core-subscription.adapter.js';
import { NatsConnectionManager } from '../connection/connection-manager.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import { HandlerExplorer } from '../discovery/handler-explorer.js';
import { validateHandlers } from '../discovery/handler-validator.js';
import { NatsConfigError } from '../errors/errors.js';
import { NatsStreamManager } from '../jetstream/stream-manager.js';
import type { ConsumerDeclaration, NatsFeatureOptions, StreamDeclaration } from './options.js';
import { DEFAULT_CONNECTION, NATS_FEATURE_TOKEN_PREFIX } from './tokens.js';

interface Declarations {
  streams: StreamDeclaration[];
  consumers: ConsumerDeclaration[];
}

const DEFAULT_DRAIN_TIMEOUT = 10_000;

/**
 * Syncs declared streams/consumers on module init, starts handler adapters on
 * bootstrap and stops them (waiting for in-flight messages) before closing the
 * connections on shutdown. A failed init/bootstrap closes everything before rethrowing.
 */
@Injectable()
export class NatsLifecycle
  implements OnModuleInit, OnApplicationBootstrap, BeforeApplicationShutdown
{
  private adapters: HandlerAdapter[] = [];

  constructor(
    private readonly manager: NatsConnectionManager,
    private readonly discovery: DiscoveryService,
    private readonly explorer: HandlerExplorer,
  ) {}

  async onModuleInit(): Promise<void> {
    try {
      const declarations = this.collectDeclarations();
      for (const [name, { streams, consumers }] of declarations) {
        const sm = new NatsStreamManager(this.manager.get(name));
        for (const stream of streams) await sm.ensureStream(stream);
        for (const { stream, config } of consumers) await sm.ensureConsumer(stream, config);
      }
    } catch (err) {
      await this.abort();
      throw err;
    }
  }

  async onApplicationBootstrap(): Promise<void> {
    try {
      const defs = this.explorer.explore();
      validateHandlers(defs, this.manager);
      for (const def of defs) {
        const adapter = this.createAdapter(def);
        if (!adapter) continue;
        this.adapters.push(adapter);
        await adapter.start();
      }
    } catch (err) {
      await this.abort();
      throw err;
    }
  }

  async beforeApplicationShutdown(): Promise<void> {
    const adapters = this.adapters;
    this.adapters = [];
    await this.stopAdapters(adapters);
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, this.drainTimeout());
    });
    try {
      await Promise.race([Promise.allSettled(adapters.map((a) => a.idle())), timeout]);
    } finally {
      clearTimeout(timer);
    }
    await this.manager.closeAll();
  }

  private createAdapter(def: HandlerDefinition): HandlerAdapter | undefined {
    const entry = this.manager.get(def.connection);
    switch (def.kind) {
      case 'subscribe':
        return new CoreSubscriptionAdapter(def, entry);
      default:
        // `consume` and `service-endpoint` adapters are added by later tasks.
        return undefined;
    }
  }

  /** Releases everything after a failed init/bootstrap so no socket stays open. */
  private async abort(): Promise<void> {
    const adapters = this.adapters;
    this.adapters = [];
    await this.stopAdapters(adapters);
    await this.manager.closeAll();
  }

  private async stopAdapters(adapters: HandlerAdapter[]): Promise<void> {
    // Adapters log their own failures; a failing stop must not block the others.
    await Promise.allSettled(adapters.map((a) => a.stop()));
  }

  private drainTimeout(): number {
    const timeouts = this.manager
      .names()
      .map((name) => this.manager.get(name).options.shutdown?.drainTimeout)
      .filter((t): t is number => t !== undefined);
    return timeouts.length > 0 ? Math.max(...timeouts) : DEFAULT_DRAIN_TIMEOUT;
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
