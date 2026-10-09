import type { ConnectionEntry } from '../connection/connection-manager.js';
import { NatsJetStreamContext } from '../context/nats-context.js';
import {
  type Delivery,
  definitionOf,
  type InboundMessage,
  type InternalDelivery,
  RETRY_SCHEDULER,
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
  /** Interval of `working()` calls on JetStream messages while the handler runs. */
  heartbeatMs?: number;
  /** Source of randomness for the backoff jitter (tests). */
  random?: () => number;
}

/** A JetStream message of a delivery together with the context that settles it. */
interface JsTarget {
  msg: InboundMessage;
  ctx: NatsJetStreamContext;
}

type SettleOp = 'ack' | 'nak' | 'term';

function label(d: Delivery): string {
  return `${d.handler.className}.${d.handler.methodName} on "${d.context.subject}"`;
}

const attempts = (n: number) => `${n} attempt${n === 1 ? '' : 's'}`;

function jsTargets(d: Delivery): JsTarget[] {
  const [msg] = d.messages;
  return msg && d.context instanceof NatsJetStreamContext ? [{ msg, ctx: d.context }] : [];
}

/** Settles through the context (which marks it settled only on success); logs failures. */
function settle(d: Delivery, op: SettleOp, fn: () => void): void {
  try {
    fn();
  } catch (err) {
    d.logger.error(`Failed to ${op} message for ${label(d)}`, errorTrace(err));
  }
}

/**
 * Inbound middleware deciding the fate of each delivery: acks successful JetStream
 * messages (auto ack), retries failures with exponential backoff (`nak(delay)` for
 * JetStream, an in-process timer for core subscriptions), then dead-letters or drops
 * them; rethrows for deliveries expecting a reply. Heartbeats JetStream messages while
 * the handler runs. Handler errors are already logged by Nest: this logs one line per
 * settlement decision, without the stack.
 */
export function createSettlement(cfg: SettlementConfig): InboundMiddleware {
  const { entry, retry, dlq, heartbeatMs, random } = cfg;

  const startHeartbeat = (d: Delivery): (() => void) => {
    if (!heartbeatMs || heartbeatMs <= 0 || d.handler.kind !== 'consume') return () => {};
    const targets = jsTargets(d);
    const timer = setInterval(() => {
      for (const { ctx } of targets) {
        if (ctx.settled) continue;
        try {
          ctx.working();
        } catch (err) {
          d.logger.error(`Failed to extend ack deadline for ${label(d)}`, errorTrace(err));
        }
      }
    }, heartbeatMs);
    return () => clearInterval(timer);
  };

  const retryLine = (d: Delivery, attempt: number, delayMs: number, err: unknown): string =>
    `${label(d)} failed (attempt ${attempt}/${retry ? retry.maxRetries + 1 : 1}), retrying in ${delayMs}ms: ${errorMessage(err)}`;

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

  async function failConsume(d: Delivery, err: unknown): Promise<void> {
    for (const { msg, ctx } of jsTargets(d)) {
      // The handler settled it itself (e.g. `ctx.nak(delay)` before throwing).
      if (ctx.settled) continue;
      const decision = decide(err, msg.attempt, retry, dlq, random);
      switch (decision.action) {
        case 'retry':
          d.logger.warn(retryLine(d, msg.attempt, decision.delayMs, err));
          settle(d, 'nak', () => ctx.nak(decision.delayMs));
          break;
        case 'dlq': {
          const { subject, error } = await deadLetter(d, msg, err);
          if (error === undefined) {
            d.logger.error(
              `${label(d)} failed, sent to DLQ ${subject} after ${attempts(msg.attempt)}: ${errorMessage(err)}`,
            );
            settle(d, 'term', () => ctx.term());
            break;
          }
          // Not dead-lettered: hand it back so it is not lost.
          const delayMs = backoffDelay(retry || DEFAULT_RETRY, msg.attempt, random);
          d.logger.error(
            `${label(d)} failed and could not be sent to DLQ ${subject}, retrying in ${delayMs}ms: ${errorMessage(error)}`,
            errorTrace(error),
          );
          settle(d, 'nak', () => ctx.nak(delayMs));
          break;
        }
        case 'drop':
          d.logger.error(
            `${label(d)} failed, gave up after ${attempts(msg.attempt)}: ${errorMessage(err)}`,
          );
          settle(d, 'term', () => ctx.term(errorMessage(err)));
          break;
      }
    }
  }

  async function failSubscribe(d: Delivery, err: unknown): Promise<void> {
    const schedule = (d as Partial<InternalDelivery>)[RETRY_SCHEDULER];
    for (const msg of d.messages) {
      const decision = decide(err, msg.attempt, schedule ? retry : false, dlq, random);
      switch (decision.action) {
        case 'retry':
          d.logger.warn(retryLine(d, msg.attempt, decision.delayMs, err));
          schedule?.(msg, decision.delayMs);
          break;
        case 'dlq': {
          const { subject, error } = await deadLetter(d, msg, err);
          if (error === undefined) {
            d.logger.error(
              `${label(d)} failed, sent to DLQ ${subject} after ${attempts(msg.attempt)}: ${errorMessage(err)}`,
            );
          } else {
            d.logger.error(
              `${label(d)} failed after ${attempts(msg.attempt)} and could not be sent to DLQ ${subject}: ${errorMessage(error)}`,
              errorTrace(error),
            );
          }
          break;
        }
        case 'drop':
          d.logger.error(
            `${label(d)} failed, gave up after ${attempts(msg.attempt)}: ${errorMessage(err)}`,
          );
          break;
      }
    }
  }

  return {
    async handle(d: Delivery, next: Next): Promise<void> {
      let failure: { err: unknown } | undefined;
      const stopHeartbeat = startHeartbeat(d);
      try {
        await next();
      } catch (err) {
        failure = { err };
      } finally {
        stopHeartbeat();
      }
      if (failure) {
        if (isReplyDelivery(d)) throw failure.err;
        if (d.handler.kind === 'consume') await failConsume(d, failure.err);
        else await failSubscribe(d, failure.err);
        return;
      }
      if (d.handler.kind !== 'consume') return;
      const def = definitionOf(d);
      if (def.kind !== 'consume' || (def.options.ack ?? 'auto') !== 'auto') return;
      for (const { ctx } of jsTargets(d)) {
        if (!ctx.settled) settle(d, 'ack', () => ctx.ack());
      }
    },
  };
}
