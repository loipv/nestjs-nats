import type { Delivery } from '../pipeline/delivery.js';

/** The consumer group of a delivery: a `consume` durable, or a `subscribe` queue group. */
export function consumerGroup(d: Delivery): string | undefined {
  switch (d.handler.kind) {
    case 'consume':
      return d.handler.durable;
    case 'subscribe':
      return d.handler.queue;
    default:
      return undefined;
  }
}
