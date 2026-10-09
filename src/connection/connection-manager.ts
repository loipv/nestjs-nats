import {
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream';
import { connect, type NatsConnection, type Status } from '@nats-io/transport-node';
import { Injectable, type LoggerService } from '@nestjs/common';
import { Observable, Subject } from 'rxjs';
import { NatsConfigError, NatsConnectionError } from '../errors/errors.js';
import { type IdempotencyStore, InMemoryIdempotencyStore } from '../idempotency/store.js';
import { createNatsLogger } from '../module/logger.js';
import type { NatsModuleOptions } from '../module/options.js';
import { DEFAULT_CONNECTION } from '../module/tokens.js';
import { errorMessage } from '../util/errors.js';
import { settlesWithin } from '../util/timeout.js';
import { redactServers, redactText } from './redact.js';

export type NatsConnectionState = 'connected' | 'reconnecting' | 'draining' | 'closed';

export interface NatsStatusEvent {
  connection: string;
  type: Status['type'];
  data?: unknown;
}

export interface ConnectionEntry {
  readonly name: string;
  readonly options: NatsModuleOptions;
  readonly nc: NatsConnection;
  readonly logger: LoggerService;
  js(): JetStreamClient;
  jsm(): Promise<JetStreamManager>;
  /**
   * The idempotency store of this connection: the one given in the module options, else a
   * default in-memory store created on first use and shared by the connection's handlers.
   */
  idempotencyStore(): IdempotencyStore;
  /**
   * Registers a hook run when the connection is being closed, before it drains (to flush
   * what is buffered). Returns a function that unregisters it.
   */
  onClose(hook: () => Promise<void>): () => void;
}

/** An idempotency store handed to a connection's handlers. */
export interface UsedIdempotencyStore {
  connection: string;
  store: IdempotencyStore;
  /** `true` for the default store created by the library, `false` for one the module was given. */
  owned: boolean;
}

interface CloseHookRun {
  entry: ConnectionEntry;
  hook: () => Promise<void>;
}

interface InternalEntry {
  entry: ConnectionEntry;
  reconnecting: boolean;
  usedStores: UsedIdempotencyStore[];
  closeHooks: Set<() => Promise<void>>;
}

const DEFAULT_CLOSE_TIMEOUT = 10_000;

const LOGGED_EVENTS = new Set<Status['type']>([
  'disconnect',
  'reconnect',
  'ldm',
  'error',
  'staleConnection',
]);

@Injectable()
export class NatsConnectionManager {
  private readonly entries = new Map<string, InternalEntry>();
  private readonly pending = new Set<string>();
  private readonly events$ = new Subject<NatsStatusEvent>();

  async connect(name: string, options: NatsModuleOptions): Promise<ConnectionEntry> {
    if (this.entries.has(name) || this.pending.has(name)) {
      throw new NatsConfigError(`NATS connection "${name}" is already registered`);
    }
    this.pending.add(name);
    try {
      const nc = await this.openConnection(name, options);
      const logger = createNatsLogger(options.logger, `NatsConnection:${name}`);
      let js: JetStreamClient | undefined;
      let jsm: Promise<JetStreamManager> | undefined;
      let idempotency: UsedIdempotencyStore | undefined;
      const closeHooks = new Set<() => Promise<void>>();
      const entry: ConnectionEntry = {
        name,
        options,
        nc,
        logger,
        js: () => {
          js ??= this.createJetStream(nc, options);
          return js;
        },
        jsm: () => {
          jsm ??= this.createJetStreamManager(nc, options);
          // Do not cache a failed attempt.
          jsm.catch(() => {
            jsm = undefined;
          });
          return jsm;
        },
        idempotencyStore() {
          if (!idempotency) {
            const provided = options.idempotency?.store;
            idempotency = {
              connection: name,
              store: provided ?? new InMemoryIdempotencyStore(),
              owned: provided === undefined,
            };
            internal.usedStores.push(idempotency);
          }
          return idempotency.store;
        },
        onClose(hook) {
          // Wrapped so registering the same function twice yields two independent hooks.
          const registered = () => hook();
          closeHooks.add(registered);
          return () => {
            closeHooks.delete(registered);
          };
        },
      };
      const internal: InternalEntry = { entry, reconnecting: false, usedStores: [], closeHooks };
      this.entries.set(name, internal);
      void this.watchStatus(internal);
      return entry;
    } finally {
      this.pending.delete(name);
    }
  }

  /** Opens the NATS connection of `name`. Internal: the testing module overrides it. */
  protected async openConnection(
    name: string,
    options: NatsModuleOptions,
  ): Promise<NatsConnection> {
    try {
      return await connect(options.connection);
    } catch (err) {
      const servers = redactServers(options.connection.servers);
      const { pass, token } = options.connection;
      const reason = redactText(err instanceof Error ? err.message : String(err), [pass, token]);
      throw new NatsConnectionError(
        `NATS connection "${name}" failed to connect to ${servers.join(', ')}: ${reason}`,
        { connection: name, servers, cause: err },
      );
    }
  }

  /** The JetStream client of a connection. Internal: the testing module overrides it. */
  protected createJetStream(nc: NatsConnection, options: NatsModuleOptions): JetStreamClient {
    return jetstream(nc, options.jetstream?.options);
  }

  /** The JetStream manager of a connection. Internal: the testing module overrides it. */
  protected createJetStreamManager(
    nc: NatsConnection,
    options: NatsModuleOptions,
  ): Promise<JetStreamManager> {
    return jetstreamManager(nc, options.jetstream?.options);
  }

  get(name: string = DEFAULT_CONNECTION): ConnectionEntry {
    return this.getInternal(name).entry;
  }

  has(name: string): boolean {
    return this.entries.has(name);
  }

  names(): string[] {
    return [...this.entries.keys()];
  }

  /** The idempotency stores handed to handlers so far (for closing them on shutdown). */
  idempotencyStores(): UsedIdempotencyStore[] {
    return [...this.entries.values()].flatMap(({ usedStores }) => usedStores);
  }

  status(name: string = DEFAULT_CONNECTION): {
    name: string;
    state: NatsConnectionState;
    server?: string;
  } {
    const internal = this.getInternal(name);
    const { nc } = internal.entry;
    if (nc.isClosed()) return { name, state: 'closed' };
    if (nc.isDraining()) return { name, state: 'draining' };
    if (internal.reconnecting) return { name, state: 'reconnecting' };
    return { name, state: 'connected', server: nc.getServer() };
  }

  status$(name?: string): Observable<NatsStatusEvent> {
    return new Observable<NatsStatusEvent>((subscriber) =>
      this.events$.subscribe({
        next: (event) => {
          if (name === undefined || event.connection === name) subscriber.next(event);
        },
        complete: () => subscriber.complete(),
      }),
    );
  }

  /**
   * Closes every connection: runs its close hooks, then drains it. Hooks and drain together
   * get at most `timeoutMs` (default 10_000) per connection, after which the connection is
   * closed without draining. Never rejects; failures are logged.
   */
  closeAll({ timeoutMs = DEFAULT_CLOSE_TIMEOUT }: { timeoutMs?: number } = {}): Promise<void> {
    // Each close starts synchronously: a connection without hooks is draining on return.
    return Promise.all(
      [...this.entries.values()].map((internal) => this.close(internal, timeoutMs)),
    ).then(() => undefined);
  }

  private async close(internal: InternalEntry, timeoutMs: number): Promise<void> {
    const { entry } = internal;
    if (entry.nc.isClosed()) return;
    let forced = false;
    const graceful = this.closeGracefully(internal, () => forced);
    if (await settlesWithin(graceful, timeoutMs)) return;
    forced = true;
    entry.logger.warn(
      `NATS connection "${entry.name}" did not drain within ${timeoutMs}ms; closed forcefully`,
    );
    try {
      await entry.nc.close();
    } catch (err) {
      entry.logger.error(
        `NATS connection "${entry.name}" failed to close: ${redactText(errorMessage(err))}`,
      );
    }
  }

  /** Runs the connection's close hooks, then drains it (or waits for a drain in progress). */
  private async closeGracefully(internal: InternalEntry, forced: () => boolean): Promise<void> {
    const { entry } = internal;
    // Awaited only when there are hooks, so a hook-less close starts draining synchronously.
    const hooks = [...internal.closeHooks].map((hook) => ({ entry, hook }));
    if (hooks.length > 0) await this.runCloseHooks(hooks);
    try {
      if (entry.nc.isClosed()) return;
      if (!entry.nc.isDraining()) {
        await entry.nc.drain();
        return;
      }
      const err = await entry.nc.closed();
      if (err instanceof Error) throw err;
    } catch (err) {
      // A drain abandoned for a forced close may fail afterwards: that is expected.
      if (forced()) return;
      entry.logger.error(
        `NATS connection "${entry.name}" failed to close cleanly: ${redactText(errorMessage(err))}`,
      );
    }
  }

  /** Runs the given close hooks together; a failing hook is logged, never thrown. */
  private async runCloseHooks(runs: CloseHookRun[]): Promise<void> {
    const results = await Promise.allSettled(runs.map(({ hook }) => hook()));
    results.forEach((result, i) => {
      if (result.status === 'rejected') {
        const { entry } = runs[i] as (typeof runs)[number];
        entry.logger.error(
          `NATS connection "${entry.name}" close hook failed: ${redactText(errorMessage(result.reason))}`,
        );
      }
    });
  }

  private getInternal(name: string): InternalEntry {
    const internal = this.entries.get(name);
    if (!internal) throw new NatsConfigError(`Unknown NATS connection "${name}"`);
    return internal;
  }

  private async watchStatus(internal: InternalEntry): Promise<void> {
    const { entry } = internal;
    try {
      for await (const s of entry.nc.status()) {
        if (s.type === 'disconnect' || s.type === 'reconnecting') internal.reconnecting = true;
        else if (s.type === 'reconnect') internal.reconnecting = false;

        const { type, ...data } = s as Status & Record<string, unknown>;
        this.events$.next({
          connection: entry.name,
          type,
          ...(Object.keys(data).length > 0 ? { data } : {}),
        });
        if (LOGGED_EVENTS.has(type)) this.logStatus(entry, s);
      }
    } catch (err) {
      entry.logger.error(
        `NATS connection "${entry.name}" status stream failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  private logStatus(entry: ConnectionEntry, s: Status): void {
    const prefix = `NATS connection "${entry.name}"`;
    switch (s.type) {
      case 'disconnect':
        entry.logger.warn(`${prefix} disconnected from ${redactServers(s.server)[0]}`);
        break;
      case 'reconnect':
        entry.logger.log(`${prefix} reconnected to ${redactServers(s.server)[0]}`);
        break;
      case 'ldm':
        entry.logger.warn(
          `${prefix} server entered lame duck mode (${redactServers(s.server)[0]})`,
        );
        break;
      case 'error':
        entry.logger.error(`${prefix} server error: ${s.error.message}`);
        break;
      case 'staleConnection':
        entry.logger.warn(`${prefix} connection is stale`);
        break;
    }
  }
}
