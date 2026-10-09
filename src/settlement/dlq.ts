import type { JsMsg } from '@nats-io/jetstream';
import { headers, type MsgHdrs } from '@nats-io/transport-node';
import type { HandlerInfo, InboundMessage } from '../pipeline/delivery.js';

export interface DlqOptions {
  /** Static subject, or a function of the handler and the failed message. */
  subject?: string | ((info: HandlerInfo, msg: InboundMessage) => string);
  mode?: 'jetstream' | 'core';
}

export interface DlqPolicy {
  subject: (info: HandlerInfo, msg: InboundMessage) => string;
  mode: 'jetstream' | 'core';
}

export const DLQ_HEADERS = {
  subject: 'Nats-DLQ-Subject',
  stream: 'Nats-DLQ-Stream',
  consumer: 'Nats-DLQ-Consumer',
  seq: 'Nats-DLQ-Seq',
  deliveries: 'Nats-DLQ-Deliveries',
  error: 'Nats-DLQ-Error',
  timestamp: 'Nats-DLQ-Timestamp',
} as const;

const MAX_ERROR_LENGTH = 1024;

function defaultSubject(info: HandlerInfo, msg: InboundMessage): string {
  return info.kind === 'consume' ? `dlq.${info.stream}.${info.durable}` : `dlq.${msg.subject}`;
}

/**
 * Resolves the DLQ configuration. Off unless enabled on the module or the handler;
 * `false` on the handler always wins; a handler object overrides a module object.
 */
export function resolveDlqPolicy(
  kind: 'subscribe' | 'consume',
  module?: DlqOptions | boolean,
  handler?: DlqOptions | boolean,
): DlqPolicy | false {
  if (handler === false) return false;
  const enabled = handler !== undefined ? handler : module;
  if (!enabled) return false;
  const opts: DlqOptions = {
    ...(typeof module === 'object' ? module : {}),
    ...(typeof handler === 'object' ? handler : {}),
  };
  const { subject } = opts;
  return {
    subject:
      typeof subject === 'function'
        ? subject
        : subject !== undefined
          ? () => subject
          : defaultSubject,
    mode: opts.mode ?? (kind === 'consume' ? 'jetstream' : 'core'),
  };
}

function isJsMsg(raw: InboundMessage['raw']): raw is JsMsg {
  return 'info' in raw && raw.info !== undefined;
}

/** Headers for a dead-lettered message: the originals plus the `Nats-DLQ-*` diagnostics. */
export function buildDlqHeaders(
  msg: InboundMessage,
  err: unknown,
  deliveries: number,
  now: Date,
): MsgHdrs {
  const out = headers();
  if (msg.headers) {
    for (const key of msg.headers.keys()) {
      for (const value of msg.headers.values(key)) out.append(key, value);
    }
  }
  out.set(DLQ_HEADERS.subject, msg.subject);
  if (isJsMsg(msg.raw)) {
    const { info } = msg.raw;
    out.set(DLQ_HEADERS.stream, info.stream);
    out.set(DLQ_HEADERS.consumer, info.consumer);
    out.set(DLQ_HEADERS.seq, String(info.streamSequence));
  }
  out.set(DLQ_HEADERS.deliveries, String(deliveries));
  const text = err instanceof Error ? err.message : String(err);
  out.set(DLQ_HEADERS.error, text.replace(/[\r\n]+/g, ' ').slice(0, MAX_ERROR_LENGTH));
  out.set(DLQ_HEADERS.timestamp, now.toISOString());
  return out;
}
