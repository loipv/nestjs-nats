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
import type { NatsConnectionState, NatsStatusEvent } from './connection-manager.js';
import { redactError, redactServers, redactText, serverSecrets } from './redact.js';

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
/** How long a forced close (after the drain timed out) may take before it is abandoned. */
const FORCED_CLOSE_TIMEOUT = 1000;

const LOGGED_EVENTS = new Set<Status['type']>([
  'disconnect',
  'reconnect',
  'ldm',
  'error',
  'staleConnection',
]);

/**
 * Internal: opens, tracks and closes the module's connections. Users see it through the
 * {@link NatsConnectionManager} facade, which exposes only its queries; the module, the
 * lifecycle, the health indicator and the testing module use it directly. It is not
 * exported from the package entry points, so its connection entries and the seams the
 * testing module overrides (`openConnection`, `createJetStream`, `createJetStreamManager`)
 * stay out of the public API.
 */
@Injectable()
export class ConnectionRegistry {
  private readonly entries = new Map<string, InternalEntry>();
  private readonly pending = new Set<string>();
  private readonly events$ = new Subject<NatsStatusEvent>();
  /** The connection attempts in progress. */
  private readonly opening = new Set<Promise<unknown>>();
  private startupAborted = false;

  /**
   * Opens and registers the connection `name`. After `abortStartup()`, the returned promise
   * never settles (and a connection that opens is closed again): startup has failed already,
   * and settling would race the original error.
   */
  async connect(name: string, options: NatsModuleOptions): Promise<ConnectionEntry> {
    if (this.startupAborted) return new Promise<never>(() => {});
    if (this.entries.has(name) || this.pending.has(name)) {
      throw new NatsConfigError(`NATS connection "${name}" is already registered`);
    }
    this.pending.add(name);
    try {
      const opening = this.openConnection(name, options);
      this.opening.add(opening);
      let nc: NatsConnection;
      try {
        nc = await opening;
      } finally {
        this.opening.delete(opening);
      }
      if (this.startupAborted) {
        await settlesWithin(nc.close(), FORCED_CLOSE_TIMEOUT);
        return new Promise<never>(() => {});
      }
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

  /**
   * Internal: called by the module's providers when startup fails during dependency
   * injection, where Nest runs no lifecycle hook and nothing else would close the
   * connections. Closes every connection opened so far, and each one still connecting once
   * it is open. Never rejects.
   */
  async abortStartup(): Promise<void> {
    this.startupAborted = true;
    // Bounded: `connect()` gives up on its own after `connection.timeout`.
    await Promise.allSettled([...this.opening]);
    await this.closeAll();
  }

  /** Opens the NATS connection of `name`. Internal: the testing module overrides it. */
  protected async openConnection(
    name: string,
    options: NatsModuleOptions,
  ): Promise<NatsConnection> {
    try {
      return await connect(options.connection);
    } catch (err) {
      const { pass, token } = options.connection;
      const secrets = [pass, token, ...serverSecrets(options.connection.servers)];
      const servers = redactServers(options.connection.servers).map((s) => redactText(s, secrets));
      // The raw error may carry credentials outside its message (e.g. the `input` URL of
      // ERR_INVALID_URL), so only a sanitized copy is exposed as the cause.
      const cause = redactError(err, secrets);
      throw new NatsConnectionError(
        `NATS connection "${name}" failed to connect to ${servers.join(', ')}: ${cause.message}`,
        { connection: name, servers, cause },
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
   * closed without draining, waiting at most `FORCED_CLOSE_TIMEOUT` more. Never rejects;
   * failures are logged.
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
    const closing = (async () => {
      try {
        await entry.nc.close();
      } catch (err) {
        entry.logger.error(
          `NATS connection "${entry.name}" failed to close: ${redactText(errorMessage(err))}`,
        );
      }
    })();
    if (!(await settlesWithin(closing, FORCED_CLOSE_TIMEOUT))) {
      entry.logger.warn(
        `NATS connection "${entry.name}" did not close within ${FORCED_CLOSE_TIMEOUT}ms; abandoned`,
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
