import { cloudEventKey, cloudEventKeyOf } from '../cloudevents/codec.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { type NatsContext, NatsJetStreamContext } from '../context/nats-context.js';
import { NatsModuleError } from '../errors/errors.js';
import type {
  Delivery,
  HandlerInfo,
  InboundMessage,
  InternalDelivery,
} from '../pipeline/delivery.js';
import {
  FAILURES,
  LINGERING,
  messageContexts,
  recordOutcome,
  SKIPPED,
} from '../pipeline/delivery.js';
import type { InboundMiddleware, Next } from '../pipeline/types.js';
import type { RetryPolicy } from '../settlement/retry.js';
import { errorMessage, errorTrace } from '../util/errors.js';
import type { ClaimResult, IdempotencyStore } from './store.js';

/** Claims of one delivery sent to the store at once: a batch is not one round-trip per message. */
const MAX_CONCURRENT_CLAIMS = 16;

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
 * `source|id` (source percent-encoded, see `cloudEventKeyOf`), else what `extractor` returns. `undefined` when there is none. With the
 * message's context, its (cached) CloudEvent attributes are used instead of reading them
 * from the message again.
 */
export function idempotencyKey(
  msg: InboundMessage,
  extractor?: IdempotencyOptions['keyExtractor'],
  ctx?: NatsContext,
): string | undefined {
  const msgId = msg.headers?.get('Nats-Msg-Id');
  if (msgId) return msgId;
  let ceKey: string | undefined;
  if (ctx) {
    const meta = ctx.cloudEvent;
    if (meta) ceKey = cloudEventKeyOf(meta);
  } else {
    ceKey = cloudEventKey(msg);
  }
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
 * store shared by the connection's handlers. `store` (called only when idempotency is
 * enabled) replaces the connection's store.
 */
export function resolveIdempotency(
  entry: ConnectionEntry,
  option: boolean | IdempotencyOptions | undefined,
  retry: RetryPolicy | false,
  store: () => IdempotencyStore = () => entry.idempotencyStore(),
): InboundMiddleware | undefined {
  if (!option) return undefined;
  const opts = option === true ? {} : option;
  return createIdempotency({
    store: store(),
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

  /**
   * Completes the keys of the messages that succeeded and frees those of the batch messages
   * that failed on their own (recorded in `FAILURES`), which are retried.
   */
  async function settle(
    d: Delivery,
    claimed: Array<{ key: string; msg: InboundMessage }>,
  ): Promise<void> {
    const failures = (d as Partial<InternalDelivery>)[FAILURES];
    const succeeded: string[] = [];
    const failed: string[] = [];
    for (const { key, msg } of claimed) (failures?.has(msg) ? failed : succeeded).push(key);
    await Promise.all([complete(d, succeeded), release(d, failed)]);
  }

  /**
   * Claims the key of every message of `d`, at most `MAX_CONCURRENT_CLAIMS` at once, and
   * returns each message's key and claim result (`undefined` for a message without a key). A
   * key repeated in the delivery is claimed once; its repeats are `'in-progress'`, as a second
   * claim of a key being processed would be. When a claim fails, the keys claimed are freed
   * and the failure is thrown as an `IdempotencyStoreError`; a failing key extractor is thrown
   * as it is, before anything is claimed.
   */
  async function claimAll(
    d: Delivery,
    scope: string,
  ): Promise<Array<{ key: string; result: ClaimResult } | undefined>> {
    const contexts = messageContexts(d);
    const keys = d.messages.map((msg, i) => {
      const key = idempotencyKey(msg, keyExtractor, contexts[i]);
      if (key === undefined) {
        d.logger.debug?.(`No idempotency key for message on "${msg.subject}"; processing it`);
        return undefined;
      }
      return `${scope}|${key}`;
    });
    const distinct = [...new Set(keys.filter((k): k is string => k !== undefined))];
    const claims = new Map<string, ClaimResult>();
    let failure: { error: unknown } | undefined;
    let next = 0;
    const worker = async (): Promise<void> => {
      while (next < distinct.length && !failure) {
        const key = distinct[next++] as string;
        try {
          claims.set(key, await store.claim(key, ttl));
        } catch (err) {
          failure ??= { error: err };
        }
      }
    };
    await Promise.all(
      Array.from({ length: Math.min(MAX_CONCURRENT_CLAIMS, distinct.length) }, worker),
    );
    if (failure) {
      // Not processed: free what this delivery claimed and let settlement retry it.
      await release(
        d,
        [...claims].filter(([, r]) => r === 'claimed').map(([key]) => key),
      );
      throw new IdempotencyStoreError(failure.error);
    }
    const seen = new Set<string>();
    return keys.map((key) => {
      if (key === undefined) return undefined;
      const first = !seen.has(key);
      seen.add(key);
      return { key, result: first ? (claims.get(key) as ClaimResult) : 'in-progress' };
    });
  }

  return {
    async handle(d: Delivery, next: Next): Promise<void> {
      const skipped = (d as InternalDelivery)[SKIPPED];
      const scope = scopeOf(d.handler);
      const claimed: Array<{ key: string; msg: InboundMessage }> = [];
      const results = await claimAll(d, scope);
      for (const [i, msg] of d.messages.entries()) {
        const entry = results[i];
        if (!entry) continue;
        const { key, result } = entry;
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
        const lingering = (d as Partial<InternalDelivery>)[LINGERING];
        if (lingering) {
          // The handler outlived its timeout and still runs: a redelivery must find its keys
          // in progress until that run ends, and then done (or free, when it failed). The
          // delivery's run now ends once that is written, so shutdown waits for the store too.
          (d as InternalDelivery)[LINGERING] = lingering.then(async (ok) => {
            await (ok ? settle(d, claimed) : release(d, keysOf(claimed)));
            return ok;
          });
        } else {
          await release(d, keysOf(claimed));
        }
        throw err;
      }
      await settle(d, claimed);
    },
  };
}
