import { cloudEventKey } from '../cloudevents/codec.js';
import type { ConnectionEntry } from '../connection/connection-manager.js';
import { NatsJetStreamContext } from '../context/nats-context.js';
import { NatsModuleError } from '../errors/errors.js';
import type {
  Delivery,
  HandlerInfo,
  InboundMessage,
  InternalDelivery,
} from '../pipeline/delivery.js';
import { FAILURES, messageContexts, recordOutcome, SKIPPED } from '../pipeline/delivery.js';
import type { InboundMiddleware, Next } from '../pipeline/types.js';
import type { RetryPolicy } from '../settlement/retry.js';
import { errorMessage, errorTrace } from '../util/errors.js';
import type { IdempotencyStore } from './store.js';

export interface IdempotencyOptions {
  /** How long a processed message is remembered, in ms (default: the module's, else 300_000). */
  ttl?: number;
  /** Derives the key when the message has neither a `Nats-Msg-Id` header nor CloudEvent attributes. */
  keyExtractor?: (msg: InboundMessage) => string | undefined;
}

export const DEFAULT_IDEMPOTENCY_TTL = 300_000;

/**
 * Internal: the idempotency store failed to claim a key, so the handler did not run.
 * Settlement retries such messages with the backoff, regardless of `isRetryable` and of
 * the attempts left: a store outage delays messages but never drops or dead-letters them.
 */
export class IdempotencyStoreError extends NatsModuleError {
  constructor(cause: unknown) {
    super(`Idempotency store unavailable: ${errorMessage(cause)}`, { cause });
    this.name = 'IdempotencyStoreError';
  }
}
const DEFAULT_IN_PROGRESS_NAK_MS = 1000;

/**
 * The idempotency key of a message: its `Nats-Msg-Id` header, else the CloudEvent
 * `source|id`, else what `extractor` returns. `undefined` when there is none.
 */
export function idempotencyKey(
  msg: InboundMessage,
  extractor?: IdempotencyOptions['keyExtractor'],
): string | undefined {
  const msgId = msg.headers?.get('Nats-Msg-Id');
  if (msgId) return msgId;
  const ceKey = cloudEventKey(msg);
  if (ceKey) return ceKey;
  return extractor?.(msg) || undefined;
}

export interface IdempotencyConfig {
  store: IdempotencyStore;
  ttl: number;
  keyExtractor?: IdempotencyOptions['keyExtractor'];
  /** Redelivery delay of a JetStream message whose key is being processed elsewhere. */
  inProgressNakMs: number;
}

/**
 * Stable identity of the handler a key belongs to, so handlers sharing a store (the
 * connection's default one, or an external one) do not swallow each other's messages.
 * It leaves the connection name out on purpose: replicas of an application sharing an
 * external store must agree on it.
 */
function scopeOf(h: HandlerInfo): string {
  return h.kind === 'consume'
    ? `consume:${h.stream}/${h.durable}`
    : `${h.kind}:${h.className}.${h.methodName}`;
}

const keysOf = (claims: Array<{ key: string }>): string[] => claims.map((c) => c.key);

const label = (d: Delivery, msg: InboundMessage): string =>
  `${d.handler.className}.${d.handler.methodName} on "${msg.subject}"`;

/**
 * Builds the idempotency middleware of a handler, or `undefined` when it did not enable
 * it. The store and TTL default to the connection's; the store defaults to one in-memory
 * store shared by the connection's handlers.
 */
export function resolveIdempotency(
  entry: ConnectionEntry,
  option: boolean | IdempotencyOptions | undefined,
  retry: RetryPolicy | false,
): InboundMiddleware | undefined {
  if (!option) return undefined;
  const opts = option === true ? {} : option;
  return createIdempotency({
    store: entry.idempotencyStore(),
    ttl: opts.ttl ?? entry.options.idempotency?.ttl ?? DEFAULT_IDEMPOTENCY_TTL,
    ...(opts.keyExtractor ? { keyExtractor: opts.keyExtractor } : {}),
    inProgressNakMs: retry ? retry.initialDelay : DEFAULT_IN_PROGRESS_NAK_MS,
  });
}

/**
 * Inbound middleware (placed right after settlement) that claims each message's key
 * before the handler runs: duplicates and messages being processed elsewhere are left out
 * of the delivery (`SKIPPED`) and, for JetStream, acked / nak'd here. Claims of messages
 * that succeed are completed, those of failed ones (the whole delivery when the handler
 * throws, else the batch messages recorded in `FAILURES`) released. A failing `claim` is
 * rethrown as an `IdempotencyStoreError`, which settlement retries.
 */
export function createIdempotency(cfg: IdempotencyConfig): InboundMiddleware {
  const { store, ttl, keyExtractor, inProgressNakMs } = cfg;

  /** Settles a JetStream message through its own context; failures are only logged. */
  function settleJs(d: Delivery, msg: InboundMessage, op: 'ack' | 'nak'): void {
    const context = messageContexts(d)[d.messages.indexOf(msg)];
    if (!(context instanceof NatsJetStreamContext)) return;
    try {
      if (op === 'ack') context.ack();
      else context.nak(inProgressNakMs);
    } catch (err) {
      d.logger.error(`Failed to ${op} message for ${label(d, msg)}`, errorTrace(err));
    }
  }

  async function release(d: Delivery, keys: string[]): Promise<void> {
    await Promise.all(
      keys.map((key) =>
        store.release(key).catch((err: unknown) => {
          d.logger.warn(`Failed to release idempotency key "${key}": ${errorMessage(err)}`);
        }),
      ),
    );
  }

  async function complete(d: Delivery, keys: string[]): Promise<void> {
    await Promise.all(
      keys.map((key) =>
        store.complete(key, ttl).catch((err: unknown) => {
          d.logger.warn(`Failed to complete idempotency key "${key}": ${errorMessage(err)}`);
        }),
      ),
    );
  }

  return {
    async handle(d: Delivery, next: Next): Promise<void> {
      const skipped = (d as InternalDelivery)[SKIPPED];
      const scope = scopeOf(d.handler);
      const claimed: Array<{ key: string; msg: InboundMessage }> = [];
      for (const msg of d.messages) {
        let result: Awaited<ReturnType<IdempotencyStore['claim']>>;
        let key: string | undefined;
        try {
          key = idempotencyKey(msg, keyExtractor);
          if (key === undefined) {
            d.logger.debug?.(`No idempotency key for message on "${msg.subject}"; processing it`);
            continue;
          }
          key = `${scope}|${key}`;
          try {
            result = await store.claim(key, ttl);
          } catch (err) {
            throw new IdempotencyStoreError(err);
          }
        } catch (err) {
          // Not processed: free what this delivery claimed and let settlement retry it.
          await release(d, keysOf(claimed));
          throw err;
        }
        switch (result) {
          case 'claimed':
            claimed.push({ key, msg });
            break;
          case 'duplicate':
            skipped.add(msg);
            d.logger.debug?.(
              `Skipping duplicate message on "${msg.subject}" (key "${key}") for ${label(d, msg)}`,
            );
            settleJs(d, msg, 'ack');
            recordOutcome(d, msg, { action: 'skipped', reason: 'duplicate' });
            break;
          case 'in-progress':
            skipped.add(msg);
            d.logger.debug?.(
              `Message on "${msg.subject}" (key "${key}") is being processed elsewhere for ${label(d, msg)}`,
            );
            settleJs(d, msg, 'nak');
            recordOutcome(d, msg, { action: 'skipped', reason: 'in-progress' });
            break;
        }
      }
      // Nothing left to process: the handler (and the plugins around it) are not run.
      if (d.messages.every((m) => skipped.has(m))) return;
      try {
        await next();
      } catch (err) {
        await release(d, keysOf(claimed));
        throw err;
      }
      // Messages of a batch that failed on their own are retried: free their keys.
      const failures = (d as Partial<InternalDelivery>)[FAILURES];
      const succeeded: string[] = [];
      const failed: string[] = [];
      for (const { key, msg } of claimed) (failures?.has(msg) ? failed : succeeded).push(key);
      await Promise.all([complete(d, succeeded), release(d, failed)]);
    },
  };
}
