import { MAX_TIMER_DELAY } from '../batch/collector.js';
import type { ConnectionRegistry } from '../connection/connection-registry.js';
import type { ConsumePushOptions } from '../decorators/handler.decorators.js';
import { NatsConfigError } from '../errors/errors.js';
import { DEFAULT_IDEMPOTENCY_TTL } from '../idempotency/idempotency.js';
import { assertTimeout } from '../pipeline/handler-timeout.js';
import type { DlqOptions } from '../settlement/dlq.js';
import { resolveRetryPolicy } from '../settlement/retry.js';
import type { HandlerDefinition } from './handler-explorer.js';

const label = (def: HandlerDefinition) => `${def.className}.${def.methodName}`;

/**
 * Rejects handler configurations that cannot work at runtime. Throws a
 * `NatsConfigError` naming the offending `Class.method` for the first problem found.
 */
export function validateHandlers(defs: HandlerDefinition[], mgr: ConnectionRegistry): void {
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
    assertTimeout(def.options.timeout, `NATS handler ${label(def)}`, 'timeout');
    if (def.kind === 'ordered') validateOrdered(def);
    if (def.kind !== 'service-endpoint') {
      validateRetry(def, mgr);
      validateDlq(def, mgr);
      validateIdempotency(def, mgr);
    }
    if (def.kind === 'subscribe') validateSlowConsumer(def);
    if (def.kind === 'subscribe' || def.kind === 'consume') {
      validateBatch(def);
      const { concurrency } = def.options;
      if (concurrency !== undefined && (!Number.isInteger(concurrency) || concurrency < 1)) {
        throw new NatsConfigError(
          `NATS handler ${label(def)} has invalid concurrency ${concurrency}; expected an integer >= 1`,
        );
      }
    }
    if (def.kind === 'consume') {
      validateDeliverGroup(def);
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

/** `slowConsumer` is a number of messages, or `false`. */
function validateSlowConsumer(def: HandlerDefinition & { kind: 'subscribe' }): void {
  const { slowConsumer } = def.options;
  if (slowConsumer === undefined || slowConsumer === false) return;
  if (!Number.isInteger(slowConsumer) || slowConsumer < 1) {
    throw new NatsConfigError(
      `NATS handler ${label(def)} has invalid slowConsumer ${String(slowConsumer)}; expected an integer >= 1 or false`,
    );
  }
}

/** `@ConsumePush`: `deliverGroup` must agree with `config.deliver_group`. */
function validateDeliverGroup(def: HandlerDefinition & { kind: 'consume' }): void {
  if (def.delivery !== 'push') return;
  const { deliverGroup, config } = def.options as ConsumePushOptions;
  const configured = config?.deliver_group;
  if (deliverGroup !== undefined && configured !== undefined && deliverGroup !== configured) {
    throw new NatsConfigError(
      `NATS handler ${label(def)} has invalid options: deliverGroup "${deliverGroup}" conflicts with config.deliver_group "${configured}"`,
    );
  }
}

const DELIVER_POLICIES: readonly string[] = ['all', 'last', 'new', 'last_per_subject'];
const UNSUPPORTED_ORDERED_OPTIONS = ['concurrency', 'durable', 'batch', 'ack', 'config'] as const;

/** `@ConsumeOrdered` has no durable, acks, batches or concurrency: reject what it would ignore. */
function validateOrdered(def: HandlerDefinition & { kind: 'ordered' }): void {
  const options = def.options as unknown as Record<string, unknown>;
  for (const key of UNSUPPORTED_ORDERED_OPTIONS) {
    if (options[key] !== undefined) {
      throw new NatsConfigError(
        `NATS handler ${label(def)}: @ConsumeOrdered does not support ${key}`,
      );
    }
  }
  const { stream, filterSubjects, deliverPolicy } = def.options;
  const nonEmpty = (value: unknown) => typeof value === 'string' && value !== '';
  if (!nonEmpty(stream)) {
    throw new NatsConfigError(
      `NATS handler ${label(def)} has invalid stream ${JSON.stringify(stream)}; expected a non-empty string`,
    );
  }
  if (
    filterSubjects !== undefined &&
    !(Array.isArray(filterSubjects) ? filterSubjects.every(nonEmpty) : nonEmpty(filterSubjects))
  ) {
    throw new NatsConfigError(
      `NATS handler ${label(def)} has invalid filterSubjects ${JSON.stringify(filterSubjects)}; expected a non-empty string or an array of non-empty strings`,
    );
  }
  if (deliverPolicy === undefined) return;
  const invalid = (what: string, expected?: string) =>
    new NatsConfigError(
      `NATS handler ${label(def)} has invalid deliverPolicy${what}${expected ? `; expected ${expected}` : ''}`,
    );
  if (typeof deliverPolicy === 'string') {
    if (!DELIVER_POLICIES.includes(deliverPolicy)) {
      throw invalid(
        ` ${JSON.stringify(deliverPolicy)}`,
        "'all', 'last', 'new' or 'last_per_subject'",
      );
    }
    return;
  }
  if (typeof deliverPolicy !== 'object' || deliverPolicy === null) {
    throw invalid(` ${String(deliverPolicy)}`);
  }
  if ('startSeq' in deliverPolicy && 'startTime' in deliverPolicy) {
    throw invalid('', 'only one of startSeq or startTime');
  }
  if ('startSeq' in deliverPolicy) {
    const { startSeq } = deliverPolicy;
    if (!Number.isInteger(startSeq) || startSeq < 1) {
      throw invalid(`.startSeq ${String(startSeq)}`, 'an integer >= 1');
    }
  } else if ('startTime' in deliverPolicy) {
    const { startTime } = deliverPolicy;
    const valid =
      (typeof startTime === 'string' || startTime instanceof Date) &&
      !Number.isNaN(new Date(startTime).getTime());
    if (!valid) {
      throw invalid(`.startTime ${String(startTime)}`, 'a valid Date or date string');
    }
  } else {
    throw invalid('', 'startSeq or startTime');
  }
}

function invalidRetry(def: HandlerDefinition, field: string, value: unknown, expected: string) {
  return new NatsConfigError(
    `NATS handler ${label(def)} has invalid retry.${field} ${String(value)}; expected ${expected}`,
  );
}

/** Validates the retry policy a handler resolves to (module options merged with its own). */
function validateRetry(def: HandlerDefinition, mgr: ConnectionRegistry): void {
  if (def.kind === 'service-endpoint') return;
  const policy = resolveRetryPolicy(mgr.get(def.connection).options.retry, def.options.retry);
  if (!policy) return;
  const { maxRetries, initialDelay, factor, maxDelay, jitter } = policy;
  if (!Number.isInteger(maxRetries) || maxRetries < 0) {
    throw invalidRetry(def, 'maxRetries', maxRetries, 'an integer >= 0');
  }
  if (!(initialDelay >= 0 && initialDelay <= MAX_TIMER_DELAY)) {
    throw invalidRetry(def, 'initialDelay', initialDelay, `a number in [0, ${MAX_TIMER_DELAY}]`);
  }
  if (!(factor >= 1)) throw invalidRetry(def, 'factor', factor, 'a number >= 1');
  if (!(maxDelay >= initialDelay && maxDelay <= MAX_TIMER_DELAY)) {
    throw invalidRetry(
      def,
      'maxDelay',
      maxDelay,
      `a number in [initialDelay (${initialDelay}), ${MAX_TIMER_DELAY}]`,
    );
  }
  if (!(jitter >= 0 && jitter <= 1))
    throw invalidRetry(def, 'jitter', jitter, 'a number in [0, 1]');
}

/**
 * Validates the DLQ subject a handler resolves to (its own options over the module's,
 * as `resolveDlqPolicy` merges them): a non-empty string or a function.
 */
function validateDlq(def: HandlerDefinition, mgr: ConnectionRegistry): void {
  if (def.kind === 'service-endpoint') return;
  const handler = def.options.dlq;
  const module = mgr.get(def.connection).options.dlq;
  if (handler === false) return;
  if (!(handler !== undefined ? handler : module)) return;
  const opts: DlqOptions = {
    ...(typeof module === 'object' ? module : {}),
    ...(typeof handler === 'object' ? handler : {}),
  };
  const { subject } = opts;
  if (subject === undefined || typeof subject === 'function') return;
  if (typeof subject !== 'string' || subject.trim() === '') {
    throw new NatsConfigError(
      `NATS handler ${label(def)} has invalid dlq.subject ${JSON.stringify(subject)}; expected a non-empty string or a function`,
    );
  }
}

/** Validates the TTL an idempotent handler resolves to (its own, else the module's). */
function validateIdempotency(def: HandlerDefinition, mgr: ConnectionRegistry): void {
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
  if (def.kind === 'service-endpoint' || def.kind === 'ordered') return;
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
