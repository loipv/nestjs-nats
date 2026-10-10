import type { Status } from '@nats-io/transport-node';
import { Inject, Injectable } from '@nestjs/common';
import type { Observable } from 'rxjs';
import { DEFAULT_CONNECTION } from '../module/tokens.js';
import { ConnectionRegistry } from './connection-registry.js';

export type NatsConnectionState = 'connected' | 'reconnecting' | 'draining' | 'closed';

export interface NatsStatusEvent {
  connection: string;
  type: Status['type'];
  data?: unknown;
}

/**
 * The state of the module's NATS connections. Opening and closing them is the module's
 * job: this only answers questions about them.
 */
@Injectable()
export class NatsConnectionManager {
  // Injected as a property, so the internal registry stays out of the constructor's type.
  @Inject(ConnectionRegistry) private readonly registry!: ConnectionRegistry;

  /** Whether a connection named `name` is registered. */
  has(name: string): boolean {
    return this.registry.has(name);
  }

  /** The names of the registered connections. */
  names(): string[] {
    return this.registry.names();
  }

  /**
   * The state of the connection `name` (default `'default'`), with the server it is
   * connected to. Throws a `NatsConfigError` for an unknown connection.
   */
  status(name: string = DEFAULT_CONNECTION): {
    name: string;
    state: NatsConnectionState;
    server?: string;
  } {
    return this.registry.status(name);
  }

  /** The status events (disconnect, reconnect, lame duck mode, ...) of one or every connection. */
  status$(name?: string): Observable<NatsStatusEvent> {
    return this.registry.status$(name);
  }
}
