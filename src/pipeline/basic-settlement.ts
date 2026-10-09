import { NatsJetStreamContext } from '../context/nats-context.js';
import { defaultIsRetryable } from '../settlement/retry.js';
import { errorMessage, errorTrace } from '../util/errors.js';
import { type Delivery, definitionOf } from './delivery.js';
import { isReplyDelivery } from './reply.js';
import type { InboundMiddleware, Next } from './types.js';

function label(d: Delivery): string {
  return `${d.handler.className}.${d.handler.methodName} on "${d.context.subject}"`;
}

function settle(d: Delivery, op: 'ack' | 'nak' | 'term', fn: () => void): void {
  try {
    fn();
  } catch (err) {
    d.logger.error(`Failed to ${op} message for ${label(d)}`, errorTrace(err));
  }
}

/**
 * Minimal settlement: acks/naks/terms JetStream messages, rethrows errors for reply
 * deliveries (the adapter answers with an error) and logs errors of plain subscriptions.
 */
export const basicSettlement: InboundMiddleware = {
  async handle(d: Delivery, next: Next): Promise<void> {
    const ctx = d.context instanceof NatsJetStreamContext ? d.context : undefined;
    try {
      await next();
    } catch (err) {
      if (d.handler.kind === 'consume') {
        d.logger.error(`Handler ${label(d)} failed`, errorTrace(err));
        if (ctx && !ctx.settled) {
          if (defaultIsRetryable(err)) settle(d, 'nak', () => ctx.nak());
          else settle(d, 'term', () => ctx.term(errorMessage(err)));
        }
        return;
      }
      if (isReplyDelivery(d)) throw err;
      d.logger.error(`Handler ${label(d)} failed`, errorTrace(err));
      return;
    }
    if (d.handler.kind !== 'consume') return;
    const def = definitionOf(d);
    if (def.kind === 'consume' && (def.options.ack ?? 'auto') === 'auto') {
      if (ctx && !ctx.settled) settle(d, 'ack', () => ctx.ack());
    }
  },
};
