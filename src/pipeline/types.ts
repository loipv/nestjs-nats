import type { MsgHdrs } from '@nats-io/transport-node';
import type { CloudEventMeta } from '../cloudevents/types.js';
import type { Codec } from '../codec/codec.js';
import type { Delivery, HandlerInfo } from './delivery.js';

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
  /**
   * Set on every message of a `publishAtomic` batch (a single message too): its position in
   * the batch and the batch size. Its `next()` settles once the whole batch is committed.
   */
  atomic?: { index: number; count: number };
  /**
   * Set on a dead letter: the message a handler failed on, published to its DLQ subject
   * (`subject`). Its `data` is the original body (bytes) and its `headers` the dead letter's
   * (`Nats-DLQ-*`, and a `Nats-Msg-Id` for a dead letter of a JetStream message).
   */
  deadLetter?: {
    /** Subject the failed message arrived on. */
    subject: string;
    /** The handler that failed. */
    handler: HandlerInfo;
    /** The last error. */
    error: unknown;
  };
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
