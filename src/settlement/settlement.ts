import type { ConnectionEntry } from '../connection/connection-manager.js';
import { NatsJetStreamContext } from '../context/nats-context.js';
import { IdempotencyStoreError } from '../idempotency/idempotency.js';
import {
  type Delivery,
  definitionOf,
  FAILURES,
  type InboundMessage,
  type InternalDelivery,
  messageContexts,
  RETRY_SCHEDULER,
  SKIPPED,
} from '../pipeline/delivery.js';
import { isReplyDelivery } from '../pipeline/reply.js';
import type { InboundMiddleware, Next } from '../pipeline/types.js';
import { errorMessage, errorTrace } from '../util/errors.js';
import { decide } from './decide.js';
import { type DlqPolicy, sendToDlq } from './dlq.js';
import { backoffDelay, DEFAULT_RETRY, type RetryPolicy } from './retry.js';

export interface SettlementConfig {
  entry: ConnectionEntry;
  retry: RetryPolicy | false;
  dlq: DlqPolicy | false;
  /** Source of randomness for the backoff jitter (tests). */
  random?: () => number;
}

/** A JetStream message of a delivery together with the context that settles it. */
interface JsTarget {
  msg: InboundMessage;
  ctx: NatsJetStreamContext;
}

type SettleOp = 'ack' | 'nak' | 'term';

/** What happened to one message of a delivery: `undefined` when it succeeded. */
type Outcome = { err: unknown } | undefined;

function label(d: Delivery, msg: InboundMessage): string {
  return `${d.handler.className}.${d.handler.methodName} on "${msg.subject}"`;
}

const attempts = (n: number) => `${n} attempt${n === 1 ? '' : 's'}`;

function jsTargets(d: Delivery): JsTarget[] {
  const contexts = messageContexts(d);
  const out: JsTarget[] = [];
  d.messages.forEach((msg, i) => {
    const ctx = contexts[i];
    if (ctx instanceof NatsJetStreamContext) out.push({ msg, ctx });
  });
  return out;
}

/** Settles through the context (which marks it settled only on success); logs failures. */
function settle(d: Delivery, msg: InboundMessage, op: SettleOp, fn: () => void): void {
  try {
    fn();
  } catch (err) {
    d.logger.error(`Failed to ${op} message for ${label(d, msg)}`, errorTrace(err));
  }
}

/**
 * Inbound middleware deciding the fate of each message of a delivery: acks successful
 * JetStream messages (auto ack), retries failures with exponential backoff (`nak(delay)`
 * for JetStream, an in-process timer for core subscriptions), then dead-letters or drops
 * them; rethrows for deliveries expecting a reply. A batch is settled message by message,
 * each with its own attempt: the messages recorded in `FAILURES` fail with their own
 * error; when the handler throws, every other message fails with its error. Messages left out of the delivery
 * (`SKIPPED`) are not settled here. (The JetStream adapter heartbeats its messages until
 * this completes.) Handler errors are already logged by Nest: this logs one line per settlement
 * decision, without the stack.
 */
export function createSettlement(cfg: SettlementConfig): InboundMiddleware {
  const { entry, retry, dlq, random } = cfg;

  /**
   * Delay before handing back a message whose idempotency key could not be claimed: the
   * retry backoff (the default one when retries are off), capped at `maxDelay`. There is
   * no attempt limit and `isRetryable` is not consulted: the handler never ran.
   */
  const storeRetryDelay = (attempt: number): number => {
    const policy = retry || DEFAULT_RETRY;
    return Math.min(policy.maxDelay, backoffDelay(policy, attempt, random));
  };

  const storeLine = (
    d: Delivery,
    msg: InboundMessage,
    delayMs: number,
    err: IdempotencyStoreError,
  ): string =>
    `${label(d, msg)} not processed: idempotency store unavailable (${errorMessage(err.cause)}), retrying in ${delayMs}ms`;

  const retryLine = (d: Delivery, msg: InboundMessage, delayMs: number, err: unknown): string =>
    `${label(d, msg)} failed (attempt ${msg.attempt}/${retry ? retry.maxRetries + 1 : 1}), retrying in ${delayMs}ms: ${errorMessage(err)}`;

  /** Publishes `msg` to its DLQ; reports the subject and the error, if any (never throws). */
  async function deadLetter(
    d: Delivery,
    msg: InboundMessage,
    err: unknown,
  ): Promise<{ subject: string; error?: unknown }> {
    let subject = '(unresolved subject)';
    try {
      // `decide` only returns 'dlq' when a DLQ policy is set.
      if (!dlq) throw new Error('no DLQ configured');
      subject = dlq.subject(d.handler, msg);
      await sendToDlq(entry, subject, dlq.mode, msg, err, msg.attempt);
      return { subject };
    } catch (error) {
      return { subject, error: error ?? new Error(String(error)) };
    }
  }

  async function failConsume(d: Delivery, { msg, ctx }: JsTarget, err: unknown): Promise<void> {
    // The handler settled it itself (e.g. `ctx.nak(delay)` before throwing).
    if (ctx.settled) return;
    if (err instanceof IdempotencyStoreError) {
      const delayMs = storeRetryDelay(msg.attempt);
      d.logger.warn(storeLine(d, msg, delayMs, err));
      settle(d, msg, 'nak', () => ctx.nak(delayMs));
      return;
    }
    const decision = decide(err, msg.attempt, retry, dlq, random);
    switch (decision.action) {
      case 'retry':
        d.logger.warn(retryLine(d, msg, decision.delayMs, err));
        settle(d, msg, 'nak', () => ctx.nak(decision.delayMs));
        return;
      case 'dlq': {
        const { subject, error } = await deadLetter(d, msg, err);
        if (error === undefined) {
          d.logger.error(
            `${label(d, msg)} failed, sent to DLQ ${subject} after ${attempts(msg.attempt)}: ${errorMessage(err)}`,
          );
          settle(d, msg, 'term', () => ctx.term());
          return;
        }
        // Not dead-lettered: hand it back so it is not lost.
        const delayMs = backoffDelay(retry || DEFAULT_RETRY, msg.attempt, random);
        d.logger.error(
          `${label(d, msg)} failed and could not be sent to DLQ ${subject}, retrying in ${delayMs}ms: ${errorMessage(error)}`,
          errorTrace(error),
        );
        settle(d, msg, 'nak', () => ctx.nak(delayMs));
        return;
      }
      case 'drop':
        d.logger.error(
          `${label(d, msg)} failed, gave up after ${attempts(msg.attempt)}: ${errorMessage(err)}`,
        );
        settle(d, msg, 'term', () => ctx.term(errorMessage(err)));
        return;
    }
  }

  async function failSubscribe(d: Delivery, msg: InboundMessage, err: unknown): Promise<void> {
    const schedule = (d as Partial<InternalDelivery>)[RETRY_SCHEDULER];
    if (err instanceof IdempotencyStoreError && schedule) {
      const delayMs = storeRetryDelay(msg.attempt);
      d.logger.warn(storeLine(d, msg, delayMs, err));
      schedule(msg, delayMs);
      return;
    }
    const decision = decide(err, msg.attempt, schedule ? retry : false, dlq, random);
    switch (decision.action) {
      case 'retry':
        d.logger.warn(retryLine(d, msg, decision.delayMs, err));
        schedule?.(msg, decision.delayMs);
        return;
      case 'dlq': {
        const { subject, error } = await deadLetter(d, msg, err);
        if (error === undefined) {
          d.logger.error(
            `${label(d, msg)} failed, sent to DLQ ${subject} after ${attempts(msg.attempt)}: ${errorMessage(err)}`,
          );
        } else {
          d.logger.error(
            `${label(d, msg)} failed after ${attempts(msg.attempt)} and could not be sent to DLQ ${subject}: ${errorMessage(error)}`,
            errorTrace(error),
          );
        }
        return;
      }
      case 'drop':
        d.logger.error(
          `${label(d, msg)} failed, gave up after ${attempts(msg.attempt)}: ${errorMessage(err)}`,
        );
        return;
    }
  }

  /**
   * The outcome of `msg`: its own recorded failure (e.g. undecodable), else the error of
   * the handler, if it threw.
   */
  function outcomeOf(d: Delivery, msg: InboundMessage, failure: Outcome): Outcome {
    const failures = (d as Partial<InternalDelivery>)[FAILURES];
    return failures?.has(msg) ? { err: failures.get(msg) } : failure;
  }

  async function settleConsume(d: Delivery, failure: Outcome): Promise<void> {
    const skipped = (d as Partial<InternalDelivery>)[SKIPPED];
    let autoAck: boolean | undefined;
    for (const target of jsTargets(d)) {
      if (skipped?.has(target.msg)) continue;
      const outcome = outcomeOf(d, target.msg, failure);
      if (outcome) {
        await failConsume(d, target, outcome.err);
        continue;
      }
      if (autoAck === undefined) {
        const def = definitionOf(d);
        autoAck = def.kind === 'consume' && (def.options.ack ?? 'auto') === 'auto';
      }
      const { ctx } = target;
      if (autoAck && !ctx.settled) settle(d, target.msg, 'ack', () => ctx.ack());
    }
  }

  async function settleSubscribe(d: Delivery, failure: Outcome): Promise<void> {
    const skipped = (d as Partial<InternalDelivery>)[SKIPPED];
    for (const msg of d.messages) {
      if (skipped?.has(msg)) continue;
      const outcome = outcomeOf(d, msg, failure);
      if (outcome) await failSubscribe(d, msg, outcome.err);
    }
  }

  return {
    async handle(d: Delivery, next: Next): Promise<void> {
      let failure: Outcome;
      try {
        await next();
      } catch (err) {
        failure = { err };
      }
      if (failure && isReplyDelivery(d)) throw failure.err;
      if (d.handler.kind === 'consume') await settleConsume(d, failure);
      else await settleSubscribe(d, failure);
    },
  };
}
