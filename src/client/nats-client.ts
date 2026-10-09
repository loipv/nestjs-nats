import {
  type Msg,
  NoRespondersError,
  type PublishOptions,
  TimeoutError,
} from '@nats-io/transport-node';
import { type Codec, decodePayload, encodePayload, jsonCodec } from '../codec/codec.js';
import { type HeadersInit, toMsgHdrs } from '../codec/headers.js';
import type { ConnectionEntry } from '../connection/connection-manager.js';
import { redactText } from '../connection/redact.js';
import { NatsModuleError, NatsRequestError } from '../errors/errors.js';
import { compose } from '../pipeline/compose.js';
import type { Next, OutgoingMessage } from '../pipeline/types.js';

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

  constructor(private readonly entry: ConnectionEntry) {}

  publish(
    subject: string,
    data?: unknown,
    opts: { headers?: HeadersInit; codec?: Codec; reply?: string } = {},
  ): void {
    const message = this.createMessage('publish', subject, data, opts);
    const run = compose(this.middlewares(), async (m) => {
      const payload = encodePayload(m.codec, m.data);
      const publishOpts: PublishOptions = {};
      if (m.headers.keys().length > 0) publishOpts.headers = m.headers;
      if (opts.reply !== undefined) publishOpts.reply = opts.reply;
      this.entry.nc.publish(m.subject, payload, publishOpts);
    });
    const task = run(message).catch((err: unknown) => {
      const reason = redactText(err instanceof Error ? err.message : String(err));
      this.entry.logger.error(`Failed to publish to "${subject}": ${reason}`);
    });
    this.inflight.add(task);
    void task.finally(() => this.inflight.delete(task));
  }

  async request<T = unknown>(
    subject: string,
    data?: unknown,
    opts: { timeout?: number; headers?: HeadersInit; codec?: Codec } = {},
  ): Promise<T> {
    const message = this.createMessage('request', subject, data, opts);
    let result: { value: unknown } | undefined;
    const run = compose(this.middlewares(), async (m) => {
      const payload = encodePayload(m.codec, m.data);
      let reply: Msg;
      try {
        reply = await this.entry.nc.request(m.subject, payload, {
          timeout: opts.timeout ?? DEFAULT_REQUEST_TIMEOUT,
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
        const description = reply.headers?.get(SERVICE_ERROR_HEADER) || undefined;
        throw new NatsRequestError(
          `Service error ${serviceCode} from "${m.subject}"${description ? `: ${description}` : ''}`,
          {
            code: 'service_error',
            subject: m.subject,
            serviceCode: Number(serviceCode),
            description,
          },
        );
      }
      result = { value: decodePayload(m.codec, reply.data, reply.headers) };
    });
    await run(message);
    if (!result) {
      throw new NatsModuleError(`Request to "${subject}" produced no reply`);
    }
    return result.value as T;
  }

  async flush(): Promise<void> {
    while (this.inflight.size > 0) {
      await Promise.allSettled([...this.inflight]);
    }
    await this.entry.nc.flush();
  }

  private middlewares(): Array<(m: OutgoingMessage, next: Next) => Promise<void>> {
    const out: Array<(m: OutgoingMessage, next: Next) => Promise<void>> = [];
    for (const { outbound } of this.entry.options.plugins ?? []) {
      if (outbound) out.push((m, next) => outbound.handle(m, next));
    }
    return out;
  }

  private createMessage(
    kind: OutgoingMessage['kind'],
    subject: string,
    data: unknown,
    opts: { headers?: HeadersInit; codec?: Codec },
  ): OutgoingMessage {
    return {
      kind,
      connection: this.entry.name,
      subject,
      data,
      headers: toMsgHdrs(opts.headers),
      codec: opts.codec ?? this.entry.options.codec ?? jsonCodec(),
    };
  }
}
