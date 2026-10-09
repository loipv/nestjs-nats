import type { ConnectionEntry } from '../connection/connection-manager.js';
import { NatsJetStreamContext } from '../context/nats-context.js';
import type { Delivery, InboundMessage, InternalDelivery } from '../pipeline/delivery.js';
import { SKIPPED } from '../pipeline/delivery.js';
import type { InboundMiddleware, Next } from '../pipeline/types.js';
import type { RetryPolicy } from '../settlement/retry.js';
import { errorMessage, errorTrace } from '../util/errors.js';
import type { IdempotencyStore } from './store.js';

export interface IdempotencyOptions {
  /** How long a processed message is remembered, in ms (default: the module's, else 300_000). */
  ttl?: number;
  /** Derives the key when the message has no `Nats-Msg-Id` header. */
  keyExtractor?: (msg: InboundMessage) => string | undefined;
}

export const DEFAULT_IDEMPOTENCY_TTL = 300_000;
const DEFAULT_IN_PROGRESS_NAK_MS = 1000;

/**
 * The idempotency key of a message: its `Nats-Msg-Id` header, else (Plan 3: the
 * CloudEvent `source|id`), else what `extractor` returns. `undefined` when there is none.
 */
export function idempotencyKey(
  msg: InboundMessage,
  extractor?: IdempotencyOptions['keyExtractor'],
): string | undefined {
  const msgId = msg.headers?.get('Nats-Msg-Id');
  if (msgId) return msgId;
  return extractor?.(msg) || undefined;
}

export interface IdempotencyConfig {
  store: IdempotencyStore;
  ttl: number;
  keyExtractor?: IdempotencyOptions['keyExtractor'];
  /** Redelivery delay of a JetStream message whose key is being processed elsewhere. */
  inProgressNakMs: number;
}

const label = (d: Delivery): string =>
  `${d.handler.className}.${d.handler.methodName} on "${d.context.subject}"`;

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
 * of the delivery (`SKIPPED`) and, for JetStream, acked / nak'd here so settlement sees
 * them as settled. Claims of messages that succeed are completed, those of failed ones
 * released. A failing `claim` is rethrown, so settlement retries the message.
 */
export function createIdempotency(cfg: IdempotencyConfig): InboundMiddleware {
  const { store, ttl, keyExtractor, inProgressNakMs } = cfg;

  /** Settles a JetStream message through its context; failures are only logged. */
  function settleJs(d: Delivery, msg: InboundMessage, op: 'ack' | 'nak'): void {
    const { context } = d;
    // Single-message deliveries only: the context settles the first message.
    if (!(context instanceof NatsJetStreamContext) || msg !== d.messages[0]) return;
    try {
      if (op === 'ack') context.ack();
      else context.nak(inProgressNakMs);
    } catch (err) {
      d.logger.error(`Failed to ${op} message for ${label(d)}`, errorTrace(err));
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
      const claimed: string[] = [];
      for (const msg of d.messages) {
        let result: Awaited<ReturnType<IdempotencyStore['claim']>>;
        let key: string | undefined;
        try {
          key = idempotencyKey(msg, keyExtractor);
          if (key === undefined) {
            d.logger.debug?.(`No idempotency key for message on "${msg.subject}"; processing it`);
            continue;
          }
          result = await store.claim(key, ttl);
        } catch (err) {
          // Not processed: free what this delivery claimed and let settlement retry it.
          await release(d, claimed);
          throw err;
        }
        switch (result) {
          case 'claimed':
            claimed.push(key);
            break;
          case 'duplicate':
            skipped.add(msg);
            d.logger.debug?.(
              `Skipping duplicate message on "${msg.subject}" (key "${key}") for ${label(d)}`,
            );
            settleJs(d, msg, 'ack');
            break;
          case 'in-progress':
            skipped.add(msg);
            d.logger.debug?.(
              `Message on "${msg.subject}" (key "${key}") is being processed elsewhere for ${label(d)}`,
            );
            settleJs(d, msg, 'nak');
            break;
        }
      }
      // Nothing left to process: the handler (and the plugins around it) are not run.
      if (d.messages.every((m) => skipped.has(m))) return;
      try {
        await next();
      } catch (err) {
        await release(d, claimed);
        throw err;
      }
      await complete(d, claimed);
    },
  };
}
