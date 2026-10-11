import { UnmatchedCloudEventError } from '../cloudevents/errors.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsJetStreamContext, SETTLED_BY } from '../context/nats-context.js';
import { IdempotencyStoreError } from '../idempotency/idempotency.js';
import {
  ABORT_SIGNAL,
  type Delivery,
  definitionOf,
  FAILURES,
  type InboundMessage,
  type InternalDelivery,
  type MessageOutcome,
  messageContexts,
  RETRY_SCHEDULER,
  recordOutcome,
  SKIPPED,
} from '../pipeline/delivery.js';
import { isReplyDelivery } from '../pipeline/reply.js';
import type { InboundMiddleware, Next } from '../pipeline/types.js';
import { errorMessage, errorTrace } from '../util/errors.js';
import { sleep } from '../util/sleep.js';
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

const MAX_TERM_REASON_LENGTH = 256;

/** The reason sent with `term()`: the error message on one line, bounded. */
const termReason = (err: unknown): string =>
  errorMessage(err)
    .replace(/[\r\n]+/g, ' ')
    .slice(0, MAX_TERM_REASON_LENGTH);

const attempts = (n: number) => `${n} attempt${n === 1 ? '' : 's'}`;

/** The stream sequence of a JetStream message, for logs. */
const streamSeq = (msg: InboundMessage): number | undefined =>
  'info' in msg.raw ? msg.raw.info?.streamSequence : undefined;

function jsTargets(d: Delivery): JsTarget[] {
  const contexts = messageContexts(d);
  const out: JsTarget[] = [];
  d.messages.forEach((msg, i) => {
    const ctx = contexts[i];
    if (ctx instanceof NatsJetStreamContext) out.push({ msg, ctx });
  });
  return out;
}

/**
 * The outcome of a message the handler settled itself (`ctx.ack()`/`nak(delay)`/`term()`);
 * `error` is the one the handler then threw, if any.
 */
function manualOutcome(ctx: NatsJetStreamContext, error?: unknown): MessageOutcome {
  const call = ctx[SETTLED_BY];
  switch (call?.op) {
    case 'nak':
      return { action: 'retry', delayMs: call.delayMs, error };
    case 'term':
      return { action: 'term', ...(error !== undefined ? { error } : {}) };
    default:
      return { action: 'ack', ...(error !== undefined ? { error } : {}) };
  }
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
 * (`SKIPPED`) are not settled here. An `UnmatchedCloudEventError` (CloudEvent router) is
 * not retried: its policy terms/drops or dead-letters it. An ordered consumer's message is
 * retried like a core one; its failing dead letter is retried in place until the delivery's
 * abort signal fires. (The JetStream adapter heartbeats its messages until this completes.)
 * Handler errors are already logged by Nest: this logs one line per settlement decision,
 * without the stack.
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

  /**
   * Publishes `msg` to its DLQ (`consumer` overrides the consumer name of the headers);
   * reports the subject and the error, if any (never throws).
   */
  async function deadLetter(
    d: Delivery,
    msg: InboundMessage,
    err: unknown,
    consumer?: string,
  ): Promise<{ subject: string; error?: unknown; dropped?: boolean }> {
    let subject = '(unresolved subject)';
    try {
      // `decide` only returns 'dlq' when a DLQ policy is set.
      if (!dlq) throw new Error('no DLQ configured');
      subject = dlq.subject(d.handler, msg);
      const result = await sendToDlq(
        entry,
        subject,
        dlq.mode,
        msg,
        err,
        msg.attempt,
        d.handler,
        consumer,
      );
      return { subject, dropped: result === 'dropped' };
    } catch (error) {
      return { subject, error: error ?? new Error(String(error)) };
    }
  }

  /**
   * Logs a successful dead letter: a handler failure, or an unmatched routing decision;
   * a warning when an outbound plugin dropped it (it still counts as dead-lettered).
   */
  function logDeadLettered(
    d: Delivery,
    msg: InboundMessage,
    subject: string,
    err: unknown,
    dropped?: boolean,
  ) {
    if (dropped) {
      d.logger.warn(
        err instanceof UnmatchedCloudEventError
          ? `${label(d, msg)}: ${err.message}; its dead letter to ${subject} was dropped by an outbound plugin (unmatched: 'dlq')`
          : `${label(d, msg)} failed, its dead letter to ${subject} was dropped by an outbound plugin after ${attempts(msg.attempt)}: ${errorMessage(err)}`,
      );
    } else if (err instanceof UnmatchedCloudEventError) {
      d.logger.warn(`${label(d, msg)}: ${err.message}; sent to DLQ ${subject} (unmatched: 'dlq')`);
    } else {
      d.logger.error(
        `${label(d, msg)} failed, sent to DLQ ${subject} after ${attempts(msg.attempt)}: ${errorMessage(err)}`,
      );
    }
  }

  /** Dead-letters then terms a JetStream message; naks it when the DLQ publish fails. */
  async function deadLetterConsume(d: Delivery, { msg, ctx }: JsTarget, err: unknown) {
    const { subject, error, dropped } = await deadLetter(d, msg, err);
    if (error === undefined) {
      logDeadLettered(d, msg, subject, err, dropped);
      settle(d, msg, 'term', () => ctx.term());
      recordOutcome(d, msg, { action: 'dlq', subject, error: err });
      return;
    }
    // Not dead-lettered: hand it back so it is not lost.
    const delayMs = backoffDelay(retry || DEFAULT_RETRY, msg.attempt, random);
    d.logger.error(
      `${label(d, msg)} failed and could not be sent to DLQ ${subject}, retrying in ${delayMs}ms: ${errorMessage(error)}`,
      errorTrace(error),
    );
    settle(d, msg, 'nak', () => ctx.nak(delayMs));
    // The handler's error, not the DLQ's: the message goes back for another attempt.
    recordOutcome(d, msg, { action: 'retry', delayMs, error: err });
  }

  /**
   * Whether an unmatched CloudEvent is dead-lettered (policy `'dlq'` with a DLQ); else it
   * is given up on, with a warning when the policy asked for a DLQ that is not configured.
   */
  function unmatchedToDlq(d: Delivery, msg: InboundMessage, err: UnmatchedCloudEventError) {
    if (err.policy === 'dlq' && dlq) return true;
    if (err.policy === 'dlq') {
      d.logger.warn(
        `${label(d, msg)}: ${err.message}; unmatched is 'dlq' but no DLQ is configured, giving up on it`,
      );
    } else {
      d.logger.debug?.(`${label(d, msg)}: ${err.message}; giving up on it (unmatched: 'term')`);
    }
    return false;
  }

  async function failConsume(d: Delivery, { msg, ctx }: JsTarget, err: unknown): Promise<void> {
    // The handler settled it itself (e.g. `ctx.nak(delay)` before throwing).
    if (ctx.settled) {
      recordOutcome(d, msg, manualOutcome(ctx, err));
      return;
    }
    if (err instanceof IdempotencyStoreError) {
      const delayMs = storeRetryDelay(msg.attempt);
      d.logger.warn(storeLine(d, msg, delayMs, err));
      settle(d, msg, 'nak', () => ctx.nak(delayMs));
      recordOutcome(d, msg, { action: 'retry', delayMs, error: err });
      return;
    }
    // Not retried: the policy of the router decides.
    if (err instanceof UnmatchedCloudEventError) {
      if (unmatchedToDlq(d, msg, err)) {
        await deadLetterConsume(d, { msg, ctx }, err);
        return;
      }
      settle(d, msg, 'term', () => ctx.term(termReason(err)));
      recordOutcome(d, msg, { action: 'term', error: err });
      return;
    }
    const decision = decide(err, msg.attempt, retry, dlq, random);
    switch (decision.action) {
      case 'retry':
        d.logger.warn(retryLine(d, msg, decision.delayMs, err));
        settle(d, msg, 'nak', () => ctx.nak(decision.delayMs));
        recordOutcome(d, msg, { action: 'retry', delayMs: decision.delayMs, error: err });
        return;
      case 'dlq':
        await deadLetterConsume(d, { msg, ctx }, err);
        return;
      case 'drop':
        d.logger.error(
          `${label(d, msg)} failed, gave up after ${attempts(msg.attempt)}: ${errorMessage(err)}`,
        );
        settle(d, msg, 'term', () => ctx.term(termReason(err)));
        recordOutcome(d, msg, { action: 'term', error: err });
        return;
    }
  }

  /** Dead-letters a core message; it is dropped when the DLQ publish fails. */
  async function deadLetterSubscribe(
    d: Delivery,
    msg: InboundMessage,
    err: unknown,
    consumer?: string,
  ) {
    const { subject, error, dropped } = await deadLetter(d, msg, err, consumer);
    if (error === undefined) {
      logDeadLettered(d, msg, subject, err, dropped);
    } else {
      d.logger.error(
        `${label(d, msg)} failed after ${attempts(msg.attempt)} and could not be sent to DLQ ${subject}: ${errorMessage(error)}`,
        errorTrace(error),
      );
    }
    recordOutcome(
      d,
      msg,
      error === undefined ? { action: 'dlq', subject, error: err } : { action: 'drop', error: err },
    );
  }

  /**
   * Dead-letters a message of an ordered consumer, which waits (the next message must not
   * overtake it): a failing DLQ publish is retried with the retry backoff (the default one
   * when retries are off), capped at `maxDelay`, with no attempt limit, until the signal of
   * the delivery aborts (shutdown); the message is then dropped. Without a signal, it is
   * dropped at the first failure, like a core message.
   */
  async function deadLetterOrdered(d: Delivery, msg: InboundMessage, err: unknown) {
    const consumer = `ordered:${d.handler.className}.${d.handler.methodName}`;
    const signal = (d as Partial<InternalDelivery>)[ABORT_SIGNAL];
    if (!signal) return deadLetterSubscribe(d, msg, err, consumer);
    const policy = retry || DEFAULT_RETRY;
    /** Drops the message at shutdown; `error` is a DLQ publish failure not logged yet. */
    const abandon = (subject: string, error?: unknown) => {
      d.logger.warn(
        `${label(d, msg)} not sent to DLQ ${subject}: retry abandoned at shutdown (stream sequence ${streamSeq(msg)})${error === undefined ? '' : `: ${errorMessage(error)}`}`,
      );
      recordOutcome(d, msg, { action: 'drop', error: err });
    };
    for (let n = 1; ; n++) {
      const { subject, error, dropped } = await deadLetter(d, msg, err, consumer);
      if (error === undefined) {
        logDeadLettered(d, msg, subject, err, dropped);
        recordOutcome(d, msg, { action: 'dlq', subject, error: err });
        return;
      }
      // Already shutting down: there is no retry to announce.
      if (signal.aborted) return abandon(subject, error);
      const delayMs = Math.min(policy.maxDelay, backoffDelay(policy, n, random));
      d.logger.error(
        `${label(d, msg)} failed and could not be sent to DLQ ${subject}, retrying in ${delayMs}ms: ${errorMessage(error)}`,
        errorTrace(error),
      );
      if (!(await sleep(delayMs, signal))) return abandon(subject);
    }
  }

  async function failSubscribe(
    d: Delivery,
    msg: InboundMessage,
    err: unknown,
    deadLetterFn: (d: Delivery, msg: InboundMessage, err: unknown) => Promise<void>,
  ): Promise<void> {
    const schedule = (d as Partial<InternalDelivery>)[RETRY_SCHEDULER];
    if (err instanceof IdempotencyStoreError && schedule) {
      const delayMs = storeRetryDelay(msg.attempt);
      d.logger.warn(storeLine(d, msg, delayMs, err));
      schedule(msg, delayMs);
      recordOutcome(d, msg, { action: 'retry', delayMs, error: err });
      return;
    }
    // Not retried: the policy of the router decides ('term' means drop on core).
    if (err instanceof UnmatchedCloudEventError) {
      if (unmatchedToDlq(d, msg, err)) await deadLetterFn(d, msg, err);
      else recordOutcome(d, msg, { action: 'drop', error: err });
      return;
    }
    const decision = decide(err, msg.attempt, schedule ? retry : false, dlq, random);
    switch (decision.action) {
      case 'retry':
        d.logger.warn(retryLine(d, msg, decision.delayMs, err));
        schedule?.(msg, decision.delayMs);
        recordOutcome(d, msg, { action: 'retry', delayMs: decision.delayMs, error: err });
        return;
      case 'dlq':
        await deadLetterFn(d, msg, err);
        return;
      case 'drop':
        d.logger.error(
          `${label(d, msg)} failed, gave up after ${attempts(msg.attempt)}: ${errorMessage(err)}`,
        );
        recordOutcome(d, msg, { action: 'drop', error: err });
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
      const { ctx, msg } = target;
      if (ctx.settled) {
        recordOutcome(d, msg, manualOutcome(ctx));
      } else if (autoAck) {
        settle(d, msg, 'ack', () => ctx.ack());
        recordOutcome(d, msg, { action: 'ack' });
      } else {
        recordOutcome(d, msg, { action: 'none' });
      }
    }
  }

  /**
   * Settles a core subscription delivery, or one of an ordered consumer (retried through
   * the scheduler like a core message; only its dead-lettering differs).
   */
  async function settleSubscribe(d: Delivery, failure: Outcome): Promise<void> {
    const skipped = (d as Partial<InternalDelivery>)[SKIPPED];
    const deadLetterFn = d.handler.kind === 'ordered' ? deadLetterOrdered : deadLetterSubscribe;
    for (const msg of d.messages) {
      if (skipped?.has(msg)) continue;
      const outcome = outcomeOf(d, msg, failure);
      if (outcome) await failSubscribe(d, msg, outcome.err, deadLetterFn);
      else recordOutcome(d, msg, { action: 'none' });
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
      if (isReplyDelivery(d)) {
        const skipped = (d as Partial<InternalDelivery>)[SKIPPED];
        for (const msg of d.messages) {
          if (skipped?.has(msg)) continue;
          recordOutcome(d, msg, { action: 'replied', ...(failure ? { error: failure.err } : {}) });
        }
        if (failure) throw failure.err;
        return;
      }
      if (d.handler.kind === 'consume') await settleConsume(d, failure);
      else await settleSubscribe(d, failure);
    },
  };
}
