import type { MsgHdrs } from '@nats-io/transport-node';
import type { Codec } from '../codec/codec.js';
import type { Delivery } from './delivery.js';

export type { Delivery } from './delivery.js';

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

export interface InboundMiddleware {
  stage?: 'outer' | 'inner';
  handle(d: Delivery, next: Next): Promise<void>;
}

export interface NatsPlugin {
  name: string;
  inbound?: InboundMiddleware;
  outbound?: OutboundMiddleware;
}
