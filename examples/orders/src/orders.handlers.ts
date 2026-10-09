import { Consume, Ctx, type NatsJetStreamContext, Payload, Subscribe } from '@loipv/nestjs-nats';
import { Injectable, Logger } from '@nestjs/common';

export interface OrderMessage {
  id: string;
  total: number;
  /** Makes the payment fail on purpose, so the example shows retry and then the DLQ. */
  fail?: boolean;
}

interface BilledOrder {
  id: string;
  total: number;
  status: 'billed';
}

@Injectable()
export class OrdersHandlers {
  private readonly logger = new Logger(OrdersHandlers.name);
  private readonly billed = new Map<string, BilledOrder>();

  // Core request/reply: the return value is sent back to the requester. The subject is
  // deliberately outside `orders.>`: a stream that captures a request's subject answers it with
  // a PubAck, which would reach the requester instead of this handler's reply.
  @Subscribe('query.orders.get')
  get(@Payload() req: { id: string }): BilledOrder | null {
    return this.billed.get(req.id) ?? null;
  }

  // JetStream pull consumer. A failed message is redelivered with backoff
  // (500 ms, 1 s, 2 s) and, after the third retry, published to the DLQ stream.
  @Consume({
    stream: 'ORDERS',
    durable: 'billing',
    config: { filter_subject: 'orders.created' },
    retry: { maxRetries: 3, initialDelay: 500 },
    dlq: true,
  })
  async bill(@Payload() order: OrderMessage, @Ctx() ctx: NatsJetStreamContext) {
    this.logger.log(`billing ${order.id} (delivery ${ctx.deliveryCount})`);
    if (order.fail) throw new Error(`payment gateway down for ${order.id}`);
    this.billed.set(order.id, { id: order.id, total: order.total, status: 'billed' });
    this.logger.log(`billed ${order.id}`);
    return { billed: true, id: order.id };
  }
}
