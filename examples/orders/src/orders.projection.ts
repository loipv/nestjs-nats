import {
  ConsumeOrdered,
  Ctx,
  type NatsOrderedContext,
  Payload,
  Subject,
  Subscribe,
} from '@loipv/nestjs-nats';
import { Injectable, Logger } from '@nestjs/common';

// In-memory read model: how many orders are in each status. Every instance of the app builds
// its own copy, because an ordered consumer gives every instance every message, in stream order.
@Injectable()
export class OrdersProjection {
  private readonly logger = new Logger(OrdersProjection.name);
  private readonly byStatus = new Map<string, number>();

  // Ephemeral, ack-less consumer. `deliverPolicy` defaults to 'all', so a restart replays the
  // whole stream and rebuilds the counts. The status is the last token of `orders.<status>`.
  @ConsumeOrdered({ stream: 'ORDERS', filterSubjects: 'orders.>' })
  project(
    @Payload() order: { id: string },
    @Subject() subject: string,
    @Ctx() ctx: NatsOrderedContext,
  ) {
    const status = subject.split('.').pop() ?? 'unknown';
    this.byStatus.set(status, (this.byStatus.get(status) ?? 0) + 1);
    this.logger.log(`order ${order.id} ${status} (stream seq ${ctx.streamSequence})`);
  }

  // Core request/reply, outside the stream's subjects (see `OrdersHandlers.get`).
  @Subscribe('query.orders.stats')
  stats(): Record<string, number> {
    return Object.fromEntries(this.byStatus);
  }
}
