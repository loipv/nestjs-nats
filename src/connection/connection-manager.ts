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
import { createNatsLogger } from '../module/logger.js';
import type { NatsModuleOptions } from '../module/options.js';
import { DEFAULT_CONNECTION } from '../module/tokens.js';
import { redactServers } from './redact.js';

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
}

interface InternalEntry {
  entry: ConnectionEntry;
  reconnecting: boolean;
}

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
      const servers = redactServers(options.connection.servers);
      let nc: NatsConnection;
      try {
        nc = await connect(options.connection);
      } catch (err) {
        const reason = err instanceof Error ? err.message : String(err);
        throw new NatsConnectionError(
          `NATS connection "${name}" failed to connect to ${servers.join(', ')}: ${reason}`,
          { connection: name, servers, cause: err },
        );
      }

      const logger = createNatsLogger(options.logger, `NatsConnection:${name}`);
      let js: JetStreamClient | undefined;
      let jsm: Promise<JetStreamManager> | undefined;
      const entry: ConnectionEntry = {
        name,
        options,
        nc,
        logger,
        js() {
          js ??= jetstream(nc, options.jetstream?.options);
          return js;
        },
        jsm() {
          jsm ??= jetstreamManager(nc, options.jetstream?.options);
          // Do not cache a failed attempt.
          jsm.catch(() => {
            jsm = undefined;
          });
          return jsm;
        },
      };
      const internal: InternalEntry = { entry, reconnecting: false };
      this.entries.set(name, internal);
      void this.watchStatus(internal);
      return entry;
    } finally {
      this.pending.delete(name);
    }
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

  async closeAll(): Promise<void> {
    const open = [...this.entries.values()].filter(({ entry }) => !entry.nc.isClosed());
    await Promise.all(open.map(({ entry }) => entry.nc.drain()));
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
