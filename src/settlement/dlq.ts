import type { JsMsg } from '@nats-io/jetstream';
import { headers, type MsgHdrs } from '@nats-io/transport-node';
import { DEFAULT_REQUEST_TIMEOUT } from '../client/nats-client.js';
import { jsonCodec } from '../codec/codec.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsModuleError } from '../errors/errors.js';
import { publishOutgoing } from '../jetstream/publish.js';
import type { HandlerInfo, InboundMessage } from '../pipeline/delivery.js';
import { encodeOutgoing, sendOutbound } from '../pipeline/outbound.js';
import type { OutgoingMessage } from '../pipeline/types.js';
import { isNoResponders } from '../util/errors.js';

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
  /** The original `Nats-Msg-Id`, if any. */
  msgId: 'Nats-DLQ-Msg-Id',
} as const;

const MAX_ERROR_LENGTH = 1024;

/**
 * Headers reserved by NATS (`Nats-Msg-Id`, `Nats-Expected-*`, `Nats-Rollup`, `Nats-TTL`,
 * `Nats-Incr`, `Nats-Schedule*`, `Nats-Batch-*`, ...): they were directives for the original
 * publish and would make the DLQ publish fail or misbehave (a copied `Nats-Msg-Id` makes
 * the DLQ stream drop the dead letters of every other consumer of the same message as
 * duplicates). None of them is copied to a dead letter.
 */
function isNatsHeader(key: string): boolean {
  return key.toLowerCase().startsWith('nats-');
}

function defaultSubject(info: HandlerInfo, msg: InboundMessage): string {
  switch (info.kind) {
    case 'consume':
      return `dlq.${info.stream}.${info.durable}`;
    case 'ordered':
      return `dlq.${info.stream}.ordered.${info.className}.${info.methodName}`;
    default:
      return `dlq.${msg.subject}`;
  }
}

/**
 * Resolves the DLQ configuration. Off unless enabled on the module or the handler;
 * `false` on the handler always wins; a handler object overrides a module object.
 */
export function resolveDlqPolicy(
  kind: 'subscribe' | 'consume' | 'ordered',
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
    mode: opts.mode ?? (kind === 'subscribe' ? 'core' : 'jetstream'),
  };
}

function isJsMsg(raw: InboundMessage['raw']): raw is JsMsg {
  return 'info' in raw && raw.info !== undefined;
}

/**
 * Headers for a dead-lettered message: the original user headers (every `Nats-*` header
 * left out) plus the `Nats-DLQ-*` diagnostics. The original `Nats-Msg-Id` is kept as
 * `Nats-DLQ-Msg-Id`. A dead letter of a JetStream message gets its own deterministic
 * `Nats-Msg-Id` (`dlq:<stream>:<consumer>:<stream sequence>`), so a re-sent dead letter is
 * deduplicated by the DLQ stream while each consumer's dead letter is kept; a dead letter
 * of a core message has none. `consumer` replaces the consumer name of the delivery info
 * (an ordered consumer's name is random: `ordered:<Class>.<method>` is used instead).
 */
export function buildDlqHeaders(
  msg: InboundMessage,
  err: unknown,
  deliveries: number,
  now: Date,
  consumer?: string,
): MsgHdrs {
  const out = headers();
  let originalMsgId: string | undefined;
  if (msg.headers) {
    for (const key of msg.headers.keys()) {
      if (isNatsHeader(key)) {
        if (originalMsgId === undefined && key.toLowerCase() === 'nats-msg-id') {
          originalMsgId = msg.headers.get(key);
        }
        continue;
      }
      for (const value of msg.headers.values(key)) out.append(key, value);
    }
  }
  out.set(DLQ_HEADERS.subject, msg.subject);
  if (isJsMsg(msg.raw)) {
    const { info } = msg.raw;
    const name = consumer ?? info.consumer;
    out.set(DLQ_HEADERS.stream, info.stream);
    out.set(DLQ_HEADERS.consumer, name);
    out.set(DLQ_HEADERS.seq, String(info.streamSequence));
    out.set('Nats-Msg-Id', `dlq:${info.stream}:${name}:${info.streamSequence}`);
  }
  if (originalMsgId) out.set(DLQ_HEADERS.msgId, originalMsgId);
  out.set(DLQ_HEADERS.deliveries, String(deliveries));
  const text = err instanceof Error ? err.message : String(err);
  out.set(DLQ_HEADERS.error, text.replace(/[\r\n]+/g, ' ').slice(0, MAX_ERROR_LENGTH));
  out.set(DLQ_HEADERS.timestamp, now.toISOString());
  return out;
}

/**
 * What `skipped` rejects with (one per call, so its stack is the call's): the outbound
 * plugins dropped the dead letter. A plugin calling `next()` after returning gets it too.
 */
class DeadLetterDropped extends NatsModuleError {
  constructor(subject: string) {
    super(`Dead letter to "${subject}": next() was called after the outbound plugins returned`);
  }
}

/**
 * Internal: sends a dead letter to an already resolved subject, through the outbound
 * plugins of the connection (marked by `OutgoingMessage.deadLetter`); `consumer` overrides
 * the consumer name of the headers (see `buildDlqHeaders`). Resolves `'dropped'` when a
 * plugin returned without passing it on; rejects when the publish fails or the plugins
 * fail or are too slow.
 */
export async function sendToDlq(
  entry: ConnectionEntry,
  subject: string,
  mode: DlqPolicy['mode'],
  msg: InboundMessage,
  err: unknown,
  deliveries: number,
  handler: HandlerInfo,
  consumer?: string,
): Promise<'sent' | 'dropped'> {
  const message: OutgoingMessage = {
    kind: mode === 'core' ? 'publish' : 'jetstream',
    connection: entry.name,
    subject,
    data: msg.data,
    headers: buildDlqHeaders(msg, err, deliveries, new Date(), consumer),
    codec: entry.options.codec ?? jsonCodec(),
    deadLetter: { subject: msg.subject, handler, error: err },
  };
  const what = `DLQ publish to "${subject}"`;
  const skipped = (): Error => new DeadLetterDropped(subject);
  try {
    if (mode === 'core') {
      await sendOutbound(
        entry,
        message,
        (m) => {
          const payload = encodeOutgoing(m);
          if (m.headers.keys().length > 0) {
            entry.nc.publish(m.subject, payload, { headers: m.headers });
          } else {
            entry.nc.publish(m.subject, payload);
          }
        },
        { what, timeout: DEFAULT_REQUEST_TIMEOUT, skipped },
      );
      return 'sent';
    }
    // A `duplicate` PubAck means this very dead letter (same stream, consumer and stream
    // sequence) is already stored: the message is dead-lettered either way.
    await publishOutgoing(
      entry,
      message,
      {},
      {
        what,
        skipped,
        wrap: (pubErr) =>
          isNoResponders(pubErr)
            ? new NatsModuleError(
                `Cannot publish to DLQ subject "${subject}": no stream captures this subject`,
                { cause: pubErr },
              )
            : pubErr,
      },
    );
    return 'sent';
  } catch (error) {
    if (error instanceof DeadLetterDropped) return 'dropped';
    throw error;
  }
}

/**
 * Publishes a failed message to its DLQ subject, with the original body and user headers
 * plus the `Nats-DLQ-*` headers (see `buildDlqHeaders`), through the outbound plugins. In
 * `'jetstream'` mode it waits for the PubAck and fails when no stream captures the subject.
 * Resolves `'dropped'` when an outbound plugin did not pass the dead letter on.
 */
export async function publishToDlq(
  entry: ConnectionEntry,
  dlq: DlqPolicy,
  info: HandlerInfo,
  msg: InboundMessage,
  err: unknown,
  deliveries: number,
): Promise<'sent' | 'dropped'> {
  return sendToDlq(entry, dlq.subject(info, msg), dlq.mode, msg, err, deliveries, info);
}
