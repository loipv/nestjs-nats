import { MAX_TIMER_DELAY } from '../batch/collector.js';
import type { NatsConnectionManager } from '../connection/connection-manager.js';
import { NatsConfigError } from '../errors/errors.js';
import { DEFAULT_IDEMPOTENCY_TTL } from '../idempotency/idempotency.js';
import { resolveRetryPolicy } from '../settlement/retry.js';
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
      validateRetry(def, mgr);
      validateIdempotency(def, mgr);
      validateBatch(def);
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

function invalidRetry(def: HandlerDefinition, field: string, value: unknown, expected: string) {
  return new NatsConfigError(
    `NATS handler ${label(def)} has invalid retry.${field} ${String(value)}; expected ${expected}`,
  );
}

/** Validates the retry policy a handler resolves to (module options merged with its own). */
function validateRetry(def: HandlerDefinition, mgr: NatsConnectionManager): void {
  if (def.kind === 'service-endpoint') return;
  const policy = resolveRetryPolicy(mgr.get(def.connection).options.retry, def.options.retry);
  if (!policy) return;
  const { maxRetries, initialDelay, factor, maxDelay, jitter } = policy;
  if (!Number.isInteger(maxRetries) || maxRetries < 0) {
    throw invalidRetry(def, 'maxRetries', maxRetries, 'an integer >= 0');
  }
  if (!(initialDelay >= 0)) throw invalidRetry(def, 'initialDelay', initialDelay, 'a number >= 0');
  if (!(factor >= 1)) throw invalidRetry(def, 'factor', factor, 'a number >= 1');
  if (!(maxDelay >= initialDelay)) {
    throw invalidRetry(def, 'maxDelay', maxDelay, `a number >= initialDelay (${initialDelay})`);
  }
  if (!(jitter >= 0 && jitter <= 1))
    throw invalidRetry(def, 'jitter', jitter, 'a number in [0, 1]');
}

/** Validates the TTL an idempotent handler resolves to (its own, else the module's). */
function validateIdempotency(def: HandlerDefinition, mgr: NatsConnectionManager): void {
  if (def.kind === 'service-endpoint') return;
  const option = def.options.idempotency;
  if (!option) return;
  const handlerTtl = option === true ? undefined : option.ttl;
  const ttl =
    handlerTtl ?? mgr.get(def.connection).options.idempotency?.ttl ?? DEFAULT_IDEMPOTENCY_TTL;
  if (!(Number.isFinite(ttl) && ttl > 0)) {
    throw new NatsConfigError(
      `NATS handler ${label(def)} has invalid idempotency.ttl ${String(ttl)}; expected a number > 0`,
    );
  }
}

/** Validates the batch options of a handler; a batch cannot be acked manually. */
function validateBatch(def: HandlerDefinition): void {
  if (def.kind === 'service-endpoint') return;
  const { batch } = def.options;
  if (batch === undefined) return;
  if (def.kind === 'consume' && def.options.ack === 'manual') {
    throw new NatsConfigError(
      `NATS handler ${label(def)} combines batch with ack: 'manual'; batches are settled per message by the library (use ctx.fail() for partial failures)`,
    );
  }
  const { maxSize, maxWait } = batch;
  if (maxSize !== undefined && !(Number.isInteger(maxSize) && maxSize >= 1)) {
    throw new NatsConfigError(
      `NATS handler ${label(def)} has invalid batch.maxSize ${String(maxSize)}; expected an integer >= 1`,
    );
  }
  if (maxWait !== undefined && !(maxWait >= 0 && maxWait <= MAX_TIMER_DELAY)) {
    throw new NatsConfigError(
      `NATS handler ${label(def)} has invalid batch.maxWait ${String(maxWait)}; expected a number in [0, ${MAX_TIMER_DELAY}]`,
    );
  }
}
