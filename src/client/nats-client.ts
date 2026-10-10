import {
  type Msg,
  NoRespondersError,
  type PublishOptions,
  TimeoutError,
} from '@nats-io/transport-node';
import type { CloudEventInit } from '../cloudevents/types.js';
import { type Codec, decodePayload } from '../codec/codec.js';
import type { NatsHeadersInit } from '../codec/headers.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { redactText } from '../connection/redact.js';
import { NatsModuleError, NatsRequestError } from '../errors/errors.js';
import { createOutgoingMessage, encodeOutgoing, sendOutbound } from '../pipeline/outbound.js';
import type { OutgoingMessage } from '../pipeline/types.js';

const DEFAULT_REQUEST_TIMEOUT = 5000;
const SERVICE_ERROR_CODE_HEADER = 'Nats-Service-Error-Code';
const SERVICE_ERROR_HEADER = 'Nats-Service-Error';

function isNoResponders(err: unknown): boolean {
  if (err instanceof NoRespondersError) return true;
  const cause = err instanceof Error ? err.cause : undefined;
  return cause instanceof NoRespondersError;
}

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
    } = {},
  ): void {
    const message = createOutgoingMessage(this.entry, 'publish', subject, data, opts);
    let dropped: Error | undefined;
    // Settles once the message is published (not waiting for the plugins), so `flush()` and
    // shutdown are not held by a plugin that never settles.
    const task = sendOutbound(
      this.entry,
      message,
      (m) => {
        const payload = encodeOutgoing(m);
        const publishOpts: PublishOptions = {};
        if (m.headers.keys().length > 0) publishOpts.headers = m.headers;
        if (opts.reply !== undefined) publishOpts.reply = opts.reply;
        this.entry.nc.publish(m.subject, payload, publishOpts);
      },
      {
        what: `Publish to "${subject}"`,
        timeout: DEFAULT_REQUEST_TIMEOUT,
        expired: (ms) => {
          dropped = new NatsModuleError(
            `Publish to "${subject}": the outbound plugins did not pass the message on within ${ms} ms; dropped`,
          );
          return dropped;
        },
      },
    ).catch((err: unknown) => {
      if (dropped && err === dropped) return this.entry.logger.error(dropped.message);
      const reason = redactText(err instanceof Error ? err.message : String(err));
      this.entry.logger.error(`Failed to publish to "${subject}": ${reason}`);
    });
    this.inflight.add(task);
    void task.finally(() => this.inflight.delete(task));
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
      const serviceCode = reply.headers?.get(SERVICE_ERROR_CODE_HEADER);
      if (serviceCode) {
        // A non-integer header leaves `serviceCode` unset; its raw text is in the message.
        const numeric = Number(serviceCode);
        const description = reply.headers?.get(SERVICE_ERROR_HEADER) || undefined;
        throw new NatsRequestError(
          `Service error ${serviceCode} from "${m.subject}"${description ? `: ${description}` : ''}`,
          {
            code: 'service_error',
            subject: m.subject,
            serviceCode: Number.isInteger(numeric) ? numeric : undefined,
            description,
          },
        );
      }
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
