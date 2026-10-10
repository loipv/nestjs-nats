import { resolveCloudEvent, writeCloudEvent } from '../cloudevents/codec.js';
import type { CloudEventInit } from '../cloudevents/types.js';
import { type Codec, encodePayload, jsonCodec } from '../codec/codec.js';
import { type NatsHeadersInit, toMsgHdrs } from '../codec/headers.js';
import type { ConnectionEntry } from '../connection/connection-manager.js';
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
