import { headers } from '@nats-io/transport-node';
import type { ConnectionEntry } from '../dist/connection/connection-registry.js';
import type { NatsPlugin } from '../dist/pipeline/types.js';

const enc = new TextEncoder();

/** A connection entry over a connection that publishes nowhere. */
export function benchEntry(plugins: NatsPlugin[] = []): ConnectionEntry {
  const noop = () => {};
  return {
    name: 'default',
    options: { connection: {}, plugins },
    nc: { publish: noop, flush: async () => {} },
    logger: { log: noop, warn: noop, error: noop },
    onClose: () => () => {},
  } as unknown as ConnectionEntry;
}

/** A JetStream message as nats.js hands it over, with no-op settlement. */
export function jsMessage(body: unknown, msgId?: string) {
  const h = headers();
  if (msgId) h.set('Nats-Msg-Id', msgId);
  const noop = () => {};
  return {
    subject: 'ord.created',
    reply: '$JS.ACK.ORD.d.1.1.1.0.0',
    data: enc.encode(JSON.stringify(body)),
    headers: msgId ? h : undefined,
    ack: noop,
    nak: noop,
    term: noop,
    working: noop,
    info: { stream: 'ORD', consumer: 'd', streamSequence: 1, deliveryCount: 1 },
  };
}
