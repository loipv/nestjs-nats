import { type Msg, type PublishOptions, TimeoutError } from '@nats-io/transport-node';
import type { CloudEventInit } from '../cloudevents/types.js';
import { type Codec, decodePayload } from '../codec/codec.js';
import { fromMsgHdrs, type NatsHeadersInit } from '../codec/headers.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { redactText } from '../connection/redact.js';
import {
  NatsConfigError,
  NatsDecodeError,
  NatsModuleError,
  NatsRequestError,
} from '../errors/errors.js';
import {
  createOutgoingMessage,
  encodeOutgoing,
  hasOutboundPlugins,
  sendOutbound,
} from '../pipeline/outbound.js';
import type { OutgoingMessage } from '../pipeline/types.js';
import { errorTrace, isNoResponders } from '../util/errors.js';
import { MAX_TIMER_DELAY } from '../util/timer.js';

/** The default timeout (ms) of a request, and how long the outbound plugins have to pass a publish on. */
export const DEFAULT_REQUEST_TIMEOUT = 5000;
const SERVICE_ERROR_CODE_HEADER = 'Nats-Service-Error-Code';
const SERVICE_ERROR_HEADER = 'Nats-Service-Error';
/** How long `requestMany` waits for replies by default, ms. */
const DEFAULT_REQUEST_MANY_WAIT = 1000;

export class NatsClient {
  private readonly inflight = new Set<Promise<void>>();

  /** Instances are provided by `NatsModule`; this constructor is not part of the public API. */
  constructor(private readonly entry: ConnectionEntry) {
    // Publishes still in their outbound plugins are sent before the connection drains. Each
    // settles within its bound, and the registry bounds close hooks by the close timeout.
    const off = entry.onClose(async () => {
      off();
      await this.settleInflight();
    });
  }

  publish(
    subject: string,
    data?: unknown,
    opts: {
      headers?: NatsHeadersInit;
      codec?: Codec;
      reply?: string;
      /** Publishes a CloudEvent: its attributes go in `ce-*` headers or a JSON envelope. */
      cloudEvent?: CloudEventInit;
      /**
       * Receives the error if the message cannot be published, instead of the connection's
       * `onPublishError` (or the log).
       */
      onError?: (error: Error) => void;
    } = {},
  ): void {
    const { onError } = opts;
    if (onError !== undefined && typeof onError !== 'function') {
      throw new NatsConfigError(`Publish to "${subject}": onError must be a function`);
    }
    const message = createOutgoingMessage(this.entry, 'publish', subject, data, opts);
    const send = (m: OutgoingMessage): void => {
      const payload = encodeOutgoing(m);
      const publishOpts: PublishOptions = {};
      if (m.headers.keys().length > 0) publishOpts.headers = m.headers;
      if (opts.reply !== undefined) publishOpts.reply = opts.reply;
      this.entry.nc.publish(m.subject, payload, publishOpts);
    };
    const logFailure = (err: unknown): void => {
      if (dropped !== undefined && err === dropped) {
        this.entry.logger.error((err as Error).message);
        return;
      }
      const reason = redactText(err instanceof Error ? err.message : String(err));
      this.entry.logger.error(`Failed to publish to "${subject}": ${reason}`);
    };
    /** Hands `err` to the error handler, else logs it; a failing handler is logged too. */
    const handlerFailed = (handlerErr: unknown, err: unknown): void => {
      this.entry.logger.error(
        `Publish to "${subject}": the publish error handler failed`,
        errorTrace(handlerErr),
      );
      logFailure(err);
    };
    /**
     * Hands `err` to the error handler, else logs it. A handler that fails, or returns a
     * promise that rejects, is logged with `err`; `flush()` waits for a returned promise.
     */
    const failed = (err: unknown): void => {
      const error = err instanceof Error ? err : new Error(String(err));
      const { onPublishError } = this.entry.options;
      const handler: ((e: Error) => unknown) | undefined =
        onError ??
        (onPublishError && ((e) => onPublishError(e, { subject, connection: this.entry.name })));
      if (!handler) {
        logFailure(err);
        return;
      }
      let result: unknown;
      try {
        result = handler(error);
      } catch (handlerErr) {
        handlerFailed(handlerErr, err);
        return;
      }
      if (typeof (result as PromiseLike<unknown> | undefined)?.then !== 'function') return;
      const settled: Promise<void> = Promise.resolve(result as PromiseLike<unknown>).then(
        () => {
          this.inflight.delete(settled);
        },
        (handlerErr: unknown) => {
          this.inflight.delete(settled);
          handlerFailed(handlerErr, err);
        },
      );
      this.inflight.add(settled);
    };
    let dropped: Error | undefined;
    // Without plugins nothing is pending: publish at once, with no promise to track.
    if (!hasOutboundPlugins(this.entry)) {
      try {
        send(message);
      } catch (err) {
        failed(err);
      }
      return;
    }
    // Settles once the message is published (not waiting for the plugins), so `flush()` and
    // shutdown are not held by a plugin that never settles.
    const task = sendOutbound(this.entry, message, send, {
      what: `Publish to "${subject}"`,
      timeout: DEFAULT_REQUEST_TIMEOUT,
      expired: (ms) => {
        dropped = new NatsModuleError(
          `Publish to "${subject}": the outbound plugins did not pass the message on within ${ms} ms; dropped`,
        );
        return dropped;
      },
    }).then(
      () => {
        this.inflight.delete(task);
      },
      (err: unknown) => {
        this.inflight.delete(task);
        failed(err);
      },
    );
    this.inflight.add(task);
  }

  async request<T = unknown>(
    subject: string,
    data?: unknown,
    opts: { timeout?: number; headers?: NatsHeadersInit; codec?: Codec } = {},
  ): Promise<T> {
    const message = createOutgoingMessage(this.entry, 'request', subject, data, opts);
    const timeout = opts.timeout ?? DEFAULT_REQUEST_TIMEOUT;
    const send = async (m: OutgoingMessage): Promise<T> => {
      const payload = encodeOutgoing(m);
      let reply: Msg;
      try {
        reply = await this.entry.nc.request(m.subject, payload, {
          timeout,
          ...(m.headers.keys().length > 0 ? { headers: m.headers } : {}),
        });
      } catch (err) {
        if (err instanceof TimeoutError) {
          throw new NatsRequestError(`Request to "${m.subject}" timed out`, {
            code: 'timeout',
            subject: m.subject,
            cause: err,
          });
        }
        if (isNoResponders(err)) {
          throw new NatsRequestError(`No responders for "${m.subject}"`, {
            code: 'no_responders',
            subject: m.subject,
            cause: err,
          });
        }
        throw err;
      }
      const failure = serviceError(reply, m.subject);
      if (failure) throw failure;
      return decodePayload(m.codec, reply.data, reply.headers) as T;
    };
    // The plugins must pass the request on within its timeout; once it is sent, the result is
    // the reply (or the request's failure), without waiting for the plugins.
    return sendOutbound(this.entry, message, send, {
      what: `Request to "${subject}"`,
      // nats.js replaces a timeout of 0 with its own default.
      timeout: timeout || DEFAULT_REQUEST_TIMEOUT,
      skipped: () => new NatsModuleError(`Request to "${subject}" produced no reply`),
    });
  }

  /**
   * Sends one request and gathers every reply until `maxWait` (default 1000 ms) ends, or
   * `maxMessages` replies arrived, or, with `stall`, no reply came for `stall` ms. A reply
   * that carries a service error, or cannot be decoded, is in the result as its `error`.
   * Resolves `[]` when nobody answers.
   */
  async requestMany<T = unknown>(
    subject: string,
    data?: unknown,
    opts: {
      maxWait?: number;
      maxMessages?: number;
      stall?: number;
      headers?: NatsHeadersInit;
      codec?: Codec;
      cloudEvent?: CloudEventInit;
    } = {},
  ): Promise<Array<RequestManyReply<T>>> {
    const { maxWait = DEFAULT_REQUEST_MANY_WAIT, maxMessages, stall } = opts;
    const invalid = (field: string, value: unknown, expected: string) =>
      new NatsConfigError(
        `requestMany to "${subject}" has invalid ${field} ${String(value)}; expected ${expected}`,
      );
    // nats.js refuses a maxWait below 1 ms.
    if (!(maxWait >= 1 && maxWait <= MAX_TIMER_DELAY)) {
      throw invalid('maxWait', maxWait, `a number of ms in [1, ${MAX_TIMER_DELAY}]`);
    }
    if (maxMessages !== undefined && !(Number.isInteger(maxMessages) && maxMessages >= 1)) {
      throw invalid('maxMessages', maxMessages, 'an integer >= 1');
    }
    if (stall !== undefined && !(stall > 0 && stall <= maxWait)) {
      throw invalid('stall', stall, 'a number of ms in (0, maxWait]');
    }
    const message = createOutgoingMessage(this.entry, 'request', subject, data, opts);
    const send = async (m: OutgoingMessage): Promise<Array<RequestManyReply<T>>> => {
      const replies: Array<RequestManyReply<T>> = [];
      try {
        const iterator = await this.entry.nc.requestMany(m.subject, encodeOutgoing(m), {
          strategy: stall !== undefined ? 'stall' : maxMessages !== undefined ? 'count' : 'timer',
          maxWait,
          ...(maxMessages !== undefined ? { maxMessages } : {}),
          ...(stall !== undefined ? { stall } : {}),
          ...(m.headers.keys().length > 0 ? { headers: m.headers } : {}),
        });
        // nats.js counts replies only under 'count', and under 'stall' each reply replaces
        // the maxWait timer: both bounds are kept here.
        const stop = () => (iterator as { stop?: () => void }).stop?.();
        const deadline = setTimeout(stop, maxWait);
        try {
          for await (const reply of iterator) {
            replies.push(toReply<T>(reply, m));
            if (replies.length === maxMessages) {
              stop();
              break;
            }
          }
        } finally {
          clearTimeout(deadline);
        }
      } catch (err) {
        // Nobody answered: an empty result, not a failure.
        if (!isNoResponders(err)) throw err;
      }
      return replies;
    };
    return sendOutbound(this.entry, message, send, {
      what: `Request to "${subject}"`,
      timeout: maxWait,
      skipped: () => new NatsModuleError(`Request to "${subject}" produced no reply`),
    });
  }

  async flush(): Promise<void> {
    await this.settleInflight();
    await this.entry.nc.flush();
  }

  /** Waits until no publish is still in its outbound plugins. */
  private async settleInflight(): Promise<void> {
    while (this.inflight.size > 0) {
      await Promise.allSettled([...this.inflight]);
    }
  }
}

/**
 * A reply of `requestMany`: its decoded data and headers, or why it has none. Replies all
 * arrive on one inbox, so they carry no subject telling the responders apart: put an id in
 * the reply's data or headers for that.
 */
export type RequestManyReply<T> =
  | { data: T; headers: Record<string, string | string[]> }
  | { error: NatsRequestError | NatsDecodeError };

/** The service error a reply carries (`Nats-Service-Error-Code`), if any. */
function serviceError(reply: Msg, subject: string): NatsRequestError | undefined {
  const serviceCode = reply.headers?.get(SERVICE_ERROR_CODE_HEADER);
  if (!serviceCode) return undefined;
  // A non-integer header leaves `serviceCode` unset; its raw text is in the message.
  const numeric = Number(serviceCode);
  const description = reply.headers?.get(SERVICE_ERROR_HEADER) || undefined;
  return new NatsRequestError(
    `Service error ${serviceCode} from "${subject}"${description ? `: ${description}` : ''}`,
    {
      code: 'service_error',
      subject,
      serviceCode: Number.isInteger(numeric) ? numeric : undefined,
      description,
    },
  );
}

function toReply<T>(reply: Msg, m: OutgoingMessage): RequestManyReply<T> {
  const failure = serviceError(reply, m.subject);
  if (failure) return { error: failure };
  try {
    const data = decodePayload(m.codec, reply.data, reply.headers) as T;
    return { data, headers: fromMsgHdrs(reply.headers) };
  } catch (err) {
    if (err instanceof NatsDecodeError) return { error: err };
    throw err;
  }
}

/** Throws a `NatsConfigError` unless `value` is `undefined` or a function. */
export function assertPublishErrorHandler(value: unknown, connection: string): void {
  if (value === undefined || typeof value === 'function') return;
  throw new NatsConfigError(
    `NATS connection "${connection}" has invalid onPublishError; expected a function`,
  );
}
