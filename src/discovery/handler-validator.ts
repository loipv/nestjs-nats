import type { NatsConnectionManager } from '../connection/connection-manager.js';
import { NatsConfigError } from '../errors/errors.js';
import type { HandlerDefinition } from './handler-explorer.js';

const label = (def: HandlerDefinition) => `${def.className}.${def.methodName}`;

/**
 * Rejects handler configurations that cannot work at runtime. Throws a
 * `NatsConfigError` naming the offending `Class.method` for the first problem found.
 */
export function validateHandlers(defs: HandlerDefinition[], mgr: NatsConnectionManager): void {
  const durables = new Map<string, HandlerDefinition>();
  for (const def of defs) {
    if (!def.wrapper.isDependencyTreeStatic() || def.wrapper.isTransient) {
      throw new NatsConfigError(
        `NATS handler ${label(def)} is declared on a request-scoped or transient provider; NATS handlers require singleton providers`,
      );
    }
    if (def.kind === 'service-endpoint' && !def.service) {
      throw new NatsConfigError(
        `@ServiceEndpoint ${label(def)} is declared in a class without @NatsService`,
      );
    }
    if (!mgr.has(def.connection)) {
      throw new NatsConfigError(
        `NATS handler ${label(def)} references unknown NATS connection "${def.connection}"`,
      );
    }
    if (def.kind === 'subscribe' || def.kind === 'consume') {
      const { concurrency } = def.options;
      if (concurrency !== undefined && (!Number.isInteger(concurrency) || concurrency < 1)) {
        throw new NatsConfigError(
          `NATS handler ${label(def)} has invalid concurrency ${concurrency}; expected an integer >= 1`,
        );
      }
    }
    if (def.kind === 'consume') {
      const { stream, durable } = def.options;
      const key = JSON.stringify([def.connection, stream, durable]);
      const other = durables.get(key);
      if (other) {
        throw new NatsConfigError(
          `NATS handlers ${label(other)} and ${label(def)} both consume durable "${durable}" of stream "${stream}" on connection "${def.connection}"`,
        );
      }
      durables.set(key, def);
    }
  }
}
