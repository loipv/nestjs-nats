import {
  type BeforeApplicationShutdown,
  Injectable,
  type OnApplicationBootstrap,
  type OnModuleInit,
} from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';
import type { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper.js';
import type { HandlerAdapter } from '../adapters/adapter.js';
import { CoreSubscriptionAdapter } from '../adapters/core-subscription.adapter.js';
import { JetStreamConsumerAdapter } from '../adapters/jetstream-consumer.adapter.js';
import {
  NatsServiceAdapter,
  type ServiceEndpointDefinition,
} from '../adapters/service-endpoint.adapter.js';
import { type ConnectionEntry, NatsConnectionManager } from '../connection/connection-manager.js';
import { redactText } from '../connection/redact.js';
import type { NatsServiceOptions } from '../decorators/handler.decorators.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import { HandlerExplorer } from '../discovery/handler-explorer.js';
import { validateHandlers } from '../discovery/handler-validator.js';
import { NatsConfigError } from '../errors/errors.js';
import type { IdempotencyStore } from '../idempotency/store.js';
import { NatsStreamManager } from '../jetstream/stream-manager.js';
import { errorMessage, errorTrace } from '../util/errors.js';
import { settlesWithin } from '../util/timeout.js';
import type { ConsumerDeclaration, NatsFeatureOptions, StreamDeclaration } from './options.js';
import { DEFAULT_CONNECTION, NATS_FEATURE_TOKEN_PREFIX } from './tokens.js';

interface Declarations {
  streams: StreamDeclaration[];
  consumers: ConsumerDeclaration[];
}

/** An adapter and the connection it receives from. */
interface ManagedAdapter {
  adapter: HandlerAdapter;
  entry: ConnectionEntry;
}

const DEFAULT_DRAIN_TIMEOUT = 10_000;
/** The least time stores get to close, even when closing the connections used it all. */
const MIN_STORE_CLOSE_TIMEOUT = 1000;

/**
 * Syncs declared streams/consumers on module init, starts handler adapters on
 * bootstrap and shuts them down gracefully, within `shutdown.drainTimeout`, before closing
 * the connections. A failed init/bootstrap shuts down the same way before rethrowing.
 */
@Injectable()
export class NatsLifecycle
  implements OnModuleInit, OnApplicationBootstrap, BeforeApplicationShutdown
{
  private adapters: ManagedAdapter[] = [];
  /** Stores already closed: a failed bootstrap followed by `app.close()` shuts down twice. */
  private readonly closedStores = new WeakSet<IdempotencyStore>();

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
      // Releases everything so no socket or store stays open.
      await this.shutdown();
      throw err;
    }
  }

  async onApplicationBootstrap(): Promise<void> {
    try {
      const defs = this.explorer.explore();
      validateHandlers(defs, this.manager);
      for (const managed of this.createAdapters(defs)) {
        this.adapters.push(managed);
        await managed.adapter.start();
      }
    } catch (err) {
      await this.shutdown();
      throw err;
    }
  }

  async beforeApplicationShutdown(): Promise<void> {
    await this.shutdown();
  }

  /**
   * Shuts down in phases (spec §14), on shutdown and after a failed init/bootstrap. Phases
   * 1–2 share one `drainTimeout`, phases 4–5 another; a forced close may take 1 s more and
   * stores always get at least `min(1 s, drainTimeout)`, so about `2 × drainTimeout + 2 s`
   * at most overall:
   * 1. stop receiving new messages;
   * 2. wait for in-flight handlers;
   * 3. force-stop: nak JetStream messages still not settled, cancel pending retries;
   * 4. close the connections (close hooks, then drain; closed forcefully past the timeout);
   * 5. close the idempotency stores.
   */
  private async shutdown(): Promise<void> {
    const adapters = this.adapters;
    this.adapters = [];
    const drainTimeout = this.drainTimeout();
    const deadline = Date.now() + drainTimeout;
    const remaining = () => deadline - Date.now();

    await settlesWithin(this.stopAdapters(adapters), remaining());
    // Bounded, and before forceStop: a hung handler holds its slot forever.
    await settlesWithin(
      Promise.allSettled(adapters.map(({ adapter }) => adapter.idle())),
      remaining(),
    );
    this.forceStopAdapters(adapters);

    const closeDeadline = Date.now() + drainTimeout;
    await this.manager.closeAll({ timeoutMs: drainTimeout });
    await this.closeIdempotencyStores(
      Math.max(closeDeadline - Date.now(), Math.min(MIN_STORE_CLOSE_TIMEOUT, drainTimeout)),
    );
  }

  private createAdapters(defs: HandlerDefinition[]): ManagedAdapter[] {
    const adapters: ManagedAdapter[] = [];
    // One service adapter per `@NatsService` provider, grouping all of its endpoints.
    const services = new Map<
      InstanceWrapper,
      { service: NatsServiceOptions; endpoints: ServiceEndpointDefinition[] }
    >();
    for (const def of defs) {
      const entry = this.manager.get(def.connection);
      switch (def.kind) {
        case 'subscribe':
          adapters.push({ adapter: new CoreSubscriptionAdapter(def, entry), entry });
          break;
        case 'consume':
          adapters.push({
            adapter: new JetStreamConsumerAdapter(def, entry, new NatsStreamManager(entry)),
            entry,
          });
          break;
        case 'service-endpoint': {
          // `validateHandlers` already rejected endpoints of classes without `@NatsService`.
          if (def.service === undefined) break;
          const group = services.get(def.wrapper) ?? { service: def.service, endpoints: [] };
          group.endpoints.push(def);
          services.set(def.wrapper, group);
          break;
        }
      }
    }
    for (const { service, endpoints } of services.values()) {
      const [first] = endpoints;
      if (!first) continue;
      const entry = this.manager.get(first.connection);
      adapters.push({ adapter: new NatsServiceAdapter(service, endpoints, entry), entry });
    }
    return adapters;
  }

  private async stopAdapters(adapters: ManagedAdapter[]): Promise<void> {
    // Adapters log their own failures; a failing stop must not block the others.
    await Promise.allSettled(adapters.map(({ adapter }) => adapter.stop()));
  }

  /** Force-stops every adapter and logs, per connection, one line summing what was forced. */
  private forceStopAdapters(adapters: ManagedAdapter[]): void {
    const totals = new Map<ConnectionEntry, { nakked: number; cancelledRetries: number }>();
    for (const { adapter, entry } of adapters) {
      const total = totals.get(entry) ?? { nakked: 0, cancelledRetries: 0 };
      totals.set(entry, total);
      try {
        const { nakked, cancelledRetries } = adapter.forceStop();
        total.nakked += nakked;
        total.cancelledRetries += cancelledRetries;
      } catch (err) {
        entry.logger.error(
          `NATS connection "${entry.name}" failed to force-stop a handler`,
          errorTrace(err),
        );
      }
    }
    for (const [entry, { nakked, cancelledRetries }] of totals) {
      if (nakked === 0 && cancelledRetries === 0) continue;
      entry.logger.warn(
        `NATS connection "${entry.name}" shutdown: nak'd ${plural(nakked, 'unsettled JetStream message', 'unsettled JetStream messages')} and cancelled ${plural(cancelledRetries, 'pending retry', 'pending retries')}`,
      );
    }
  }

  /**
   * Closes each idempotency store once: the ones handed to handlers and the ones given in
   * the module options (a store may be shared by several connections). Waits at most `ms`,
   * then warns about each store still closing.
   */
  private async closeIdempotencyStores(ms: number): Promise<void> {
    const stores = new Map<IdempotencyStore, ConnectionEntry>();
    for (const { connection, store } of this.manager.idempotencyStores()) {
      if (!stores.has(store)) stores.set(store, this.manager.get(connection));
    }
    for (const name of this.manager.names()) {
      const entry = this.manager.get(name);
      const store = entry.options.idempotency?.store;
      if (store && !stores.has(store)) stores.set(store, entry);
    }
    const pending = new Map<IdempotencyStore, ConnectionEntry>();
    for (const [store, entry] of stores) {
      if (this.closedStores.has(store)) continue;
      this.closedStores.add(store);
      pending.set(store, entry);
    }
    const closing = [...pending].map(async ([store, entry]) => {
      try {
        await store.close?.();
      } catch (err) {
        entry.logger.error(
          `NATS connection "${entry.name}" failed to close its idempotency store: ${redactText(errorMessage(err))}`,
        );
      } finally {
        pending.delete(store);
      }
    });
    if (await settlesWithin(Promise.all(closing), ms)) return;
    for (const entry of pending.values()) {
      entry.logger.warn(
        `NATS connection "${entry.name}" idempotency store did not close within ${ms}ms; abandoned`,
      );
    }
  }

  /** The effective `shutdown.drainTimeout`: the largest one of every connection. */
  private drainTimeout(): number {
    const timeouts = this.manager
      .names()
      .map(
        (name) => this.manager.get(name).options.shutdown?.drainTimeout ?? DEFAULT_DRAIN_TIMEOUT,
      );
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

function plural(count: number, one: string, many: string): string {
  return `${count} ${count === 1 ? one : many}`;
}
