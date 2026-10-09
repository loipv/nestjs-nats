import type { MsgHdrs } from '@nats-io/transport-node';
import type { Codec } from '../codec/codec.js';

export type Next = () => Promise<void>;

export interface OutgoingMessage {
  kind: 'publish' | 'request' | 'jetstream';
  connection: string;
  subject: string;
  data: unknown;
  headers: MsgHdrs;
  codec: Codec;
}

export interface OutboundMiddleware {
  handle(m: OutgoingMessage, next: Next): Promise<void>;
}

/** Placeholder until the inbound pipeline defines the real delivery type. */
export type Delivery = unknown;

export interface InboundMiddleware {
  stage?: 'outer' | 'inner';
  handle(d: Delivery, next: Next): Promise<void>;
}

export interface NatsPlugin {
  name: string;
  inbound?: InboundMiddleware;
  outbound?: OutboundMiddleware;
}
