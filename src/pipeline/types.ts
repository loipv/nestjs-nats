import type { MsgHdrs } from '@nats-io/transport-node';
import type { CloudEventMeta } from '../cloudevents/types.js';
import type { Codec } from '../codec/codec.js';
import type { Delivery } from './delivery.js';

export type { Delivery, MessageOutcome } from './delivery.js';

export type Next = () => Promise<void>;

export interface OutgoingMessage {
  kind: 'publish' | 'request' | 'jetstream';
  connection: string;
  subject: string;
  data: unknown;
  headers: MsgHdrs;
  codec: Codec;
  /**
   * Set when the publish carries a `cloudEvent` option: the CloudEvent resolved (id, time,
   * source filled in) before the outbound plugins run. It is written to the wire after
   * them, with the message's final `codec` and `headers`.
   */
  cloudEvent?: { meta: CloudEventMeta; mode: 'binary' | 'structured' };
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
