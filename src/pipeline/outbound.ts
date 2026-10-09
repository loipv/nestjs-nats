import { type Codec, jsonCodec } from '../codec/codec.js';
import { type HeadersInit, toMsgHdrs } from '../codec/headers.js';
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

/** Builds the message handed to the outbound pipeline, applying codec/header defaults. */
export function createOutgoingMessage(
  entry: ConnectionEntry,
  kind: OutgoingMessage['kind'],
  subject: string,
  data: unknown,
  opts: { headers?: HeadersInit; codec?: Codec },
): OutgoingMessage {
  return {
    kind,
    connection: entry.name,
    subject,
    data,
    headers: toMsgHdrs(opts.headers),
    codec: opts.codec ?? entry.options.codec ?? jsonCodec(),
  };
}
