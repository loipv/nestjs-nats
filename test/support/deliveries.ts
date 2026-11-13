import { type MsgHdrs } from '@nats-io/transport-node';
import { jsonCodec } from '../../src/codec/codec.js';
import { NatsBatchContext } from '../../src/context/nats-batch-context.js';
import { NatsContext, NatsJetStreamContext } from '../../src/context/nats-context.js';
import { NatsOrderedContext } from '../../src/context/nats-ordered-context.js';
import type { Delivery, HandlerInfo, InboundMessage } from '../../src/pipeline/delivery.js';
import { type MockLogger, mockLogger } from './logger.js';

/**
 * Deliveries as the adapters build them, for testing inbound plugins (`otel/tracing`,
 * `otel/metrics`): one message (`corePart`, `jsPart`, `orderedPart`) or a batch of them.
 */

const enc = new TextEncoder();

/** The logger of every delivery built here. */
export const logger: MockLogger = mockLogger();

export interface Part {
  message: InboundMessage;
  context: NatsContext;
}

export function corePart(
  opts: { subject?: string; reply?: string; headers?: MsgHdrs; data?: Uint8Array } = {},
): Part {
  const raw = {
    subject: opts.subject ?? 'o.c',
    reply: opts.reply ?? '',
    data: opts.data ?? enc.encode('{"id":1}'),
    headers: opts.headers,
  };
  return {
    message: {
      subject: raw.subject,
      data: raw.data,
      headers: raw.headers,
      raw: raw as never,
      attempt: 1,
    },
    context: new NatsContext({ kind: 'core', connection: 'default', raw: raw as never }),
  };
}

export function jsPart(opts: { headers?: MsgHdrs; deliveryCount?: number } = {}): Part {
  const raw = {
    subject: 'o.c',
    reply: '$JS.ACK.ORD.d.1.1.1.0.0',
    data: enc.encode('{"id":1}'),
    headers: opts.headers,
    info: {
      stream: 'ORD',
      consumer: 'billing',
      streamSequence: 42,
      deliveryCount: opts.deliveryCount ?? 3,
    },
  };
  return {
    message: {
      subject: raw.subject,
      data: raw.data,
      headers: raw.headers,
      raw: raw as never,
      attempt: opts.deliveryCount ?? 3,
    },
    context: new NatsJetStreamContext({
      kind: 'jetstream',
      connection: 'default',
      raw: raw as never,
    }),
  };
}

/** A message of an ordered consumer, on its second in-place attempt. */
export function orderedPart(): Part {
  const raw = {
    subject: 'o.c',
    reply: '$JS.ACK.ORD.ordered.1.1.1.0.0',
    data: enc.encode('{"id":1}'),
    headers: undefined,
    info: { stream: 'ORD', consumer: 'ordered-x', streamSequence: 42, deliveryCount: 1 },
  };
  return {
    message: { subject: raw.subject, data: raw.data, raw: raw as never, attempt: 2 },
    context: new NatsOrderedContext({ connection: 'default', raw: raw as never, attempt: 2 }),
  };
}

export const base = { connection: 'default', className: 'H', methodName: 'on' };
export const consumeHandler: HandlerInfo = {
  kind: 'consume',
  ...base,
  stream: 'ORD',
  durable: 'billing',
  delivery: 'pull',
};
export const subscribeHandler: HandlerInfo = {
  kind: 'subscribe',
  ...base,
  subject: 'o.*',
  queue: 'workers',
};
export const endpointHandler: HandlerInfo = {
  kind: 'service-endpoint',
  ...base,
  service: { name: 's', version: '1.0.0' },
  endpoint: 'e',
  subject: 'o.c',
};

export function delivery(handler: HandlerInfo, parts: Part[], batch = false): Delivery {
  const ctx = batch
    ? new NatsBatchContext({
        connection: 'default',
        messages: parts.map((p) => p.context),
        onFail: () => {},
      })
    : (parts[0] as Part).context;
  return {
    handler,
    messages: parts.map((p) => p.message),
    context: ctx,
    codec: jsonCodec(),
    logger,
    outcomes: new Map(),
  };
}
