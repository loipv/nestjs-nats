import { MAX_TIMER_DELAY } from '../batch/collector.js';
import { resolveCloudEvent, writeCloudEvent } from '../cloudevents/codec.js';
import type { CloudEventInit } from '../cloudevents/types.js';
import { type Codec, encodePayload, jsonCodec } from '../codec/codec.js';
import { type NatsHeadersInit, toMsgHdrs } from '../codec/headers.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsModuleError } from '../errors/errors.js';
import { errorTrace } from '../util/errors.js';
import { compose } from './compose.js';
import type { Next, OutgoingMessage } from './types.js';

export type OutboundHandler = (m: OutgoingMessage, next: Next) => Promise<void>;

/** The outbound middlewares of a connection's plugins, in registration order. */
export function outboundMiddlewares(entry: ConnectionEntry): OutboundHandler[] {
  const out: OutboundHandler[] = [];
  for (const { outbound } of entry.options.plugins ?? []) {
    if (outbound) out.push((m, next) => outbound.handle(m, next));
  }
  return out;
}

/**
 * Builds the message handed to the outbound pipeline, applying codec/header defaults. A
 * `cloudEvent` option is resolved here, so plugins see its `id`; it throws a
 * `NatsConfigError` when it is invalid (e.g. there is no source).
 */
export function createOutgoingMessage(
  entry: ConnectionEntry,
  kind: OutgoingMessage['kind'],
  subject: string,
  data: unknown,
  opts: { headers?: NatsHeadersInit; codec?: Codec; cloudEvent?: CloudEventInit },
): OutgoingMessage {
  const codec = opts.codec ?? entry.options.codec ?? jsonCodec();
  const message: OutgoingMessage = {
    kind,
    connection: entry.name,
    subject,
    data,
    headers: toMsgHdrs(opts.headers),
    codec,
  };
  if (opts.cloudEvent) {
    const defaults: { source?: string; contentType?: string } = {};
    // Bytes are sent as they are: the codec's content type would not describe them.
    if (!(data instanceof Uint8Array)) defaults.contentType = codec.contentType;
    const source = entry.options.cloudEvents?.source;
    if (source !== undefined) defaults.source = source;
    message.cloudEvent = {
      meta: resolveCloudEvent(opts.cloudEvent, defaults),
      mode: opts.cloudEvent.mode ?? 'binary',
    };
  }
  return message;
}

/**
 * The terminal step of the outbound pipeline: the wire body of `m`. For a CloudEvent it
 * also writes the `ce-*` / `Content-Type` headers into `m.headers`, so call it before
 * reading them.
 */
export function encodeOutgoing(m: OutgoingMessage): Uint8Array {
  if (m.cloudEvent) {
    return writeCloudEvent(m.cloudEvent.meta, m.data, m.codec, m.cloudEvent.mode, m.headers);
  }
  return encodePayload(m.codec, m.data);
}

/** `ms` as a timer delay: at least 1 ms, at most the largest delay `setTimeout` honours. */
export function timerDelay(ms: number): number {
  return Math.min(Math.max(ms, 1), MAX_TIMER_DELAY);
}

/**
 * Logs `err`, a plugin failure after the call settled with `outcome`, which it does not
 * change. A plugin rethrowing the outcome's own error is not a failure of its own.
 */
export function logLateOutboundError(
  entry: ConnectionEntry,
  outcome: Promise<unknown>,
  err: unknown,
  text: string,
): void {
  const none = Symbol('none');
  void outcome
    .then(
      () => none,
      (own: unknown) => own,
    )
    .then((own) => {
      if (err !== own) entry.logger.error(text, errorTrace(err));
    });
}

export interface SendOutboundOptions {
  /** Names the call in errors and logs, e.g. `Request to "a.b"`. */
  what: string;
  /** How long (ms) the plugins have to pass the message on to the send; clamped to a timer delay. */
  timeout: number;
  /** The error when the plugins do not pass the message on in time. */
  expired?: (ms: number) => Error;
  /** The error when the plugins return without passing the message on; unset: the call settles with undefined. */
  skipped?: () => Error;
}

/**
 * Runs `message` through the outbound plugins of `entry`; their terminal step is `send`. The
 * plugins must pass the message on within `timeout`, else the call rejects and nothing is
 * sent. Once `send` started, the call settles with its outcome, without waiting for the
 * plugins; a plugin failing after that is logged. A plugin failing before it rejects the
 * call with its error.
 */
export function sendOutbound<R>(
  entry: ConnectionEntry,
  message: OutgoingMessage,
  send: (m: OutgoingMessage) => Promise<R> | R,
  opts: SendOutboundOptions,
): Promise<R> {
  const ms = timerDelay(opts.timeout);
  return new Promise<R>((resolve, reject) => {
    let outcome: Promise<R> | undefined;
    let expired: Error | undefined;
    const timer = setTimeout(() => {
      expired =
        opts.expired?.(ms) ??
        new NatsModuleError(
          `${opts.what}: the outbound plugins did not pass the message on within ${ms} ms`,
        );
      reject(expired);
    }, ms);
    timer.unref?.();
    compose(outboundMiddlewares(entry), async (m) => {
      // The caller gave up: nothing is sent.
      if (expired) throw expired;
      clearTimeout(timer);
      try {
        outcome = Promise.resolve(send(m));
      } catch (err) {
        outcome = Promise.reject(err);
      }
      outcome.then(resolve, reject);
      await outcome;
    })(message).then(
      () => {
        if (outcome || expired) return;
        clearTimeout(timer);
        if (opts.skipped) reject(opts.skipped());
        else resolve(undefined as R);
      },
      (err: unknown) => {
        if (outcome) {
          logLateOutboundError(
            entry,
            outcome,
            err,
            `${opts.what}: an outbound plugin failed after the message was sent`,
          );
        } else if (expired) {
          logLateOutboundError(
            entry,
            Promise.reject(expired),
            err,
            `${opts.what}: an outbound plugin failed after the call timed out`,
          );
        } else {
          clearTimeout(timer);
          reject(err);
        }
      },
    );
  });
}
