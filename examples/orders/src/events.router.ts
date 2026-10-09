import {
  CloudEvent,
  type CloudEventMeta,
  CloudEventRouter,
  OnCloudEvent,
  Payload,
} from '@loipv/nestjs-nats';
import { Injectable, Logger } from '@nestjs/common';

// Receives CloudEvents on the core subject `events.>` and routes each one to the
// method whose type pattern matches best. Events nobody matches are acked and skipped.
@Injectable()
@CloudEventRouter({ subscribe: 'events.>' })
export class EventsRouter {
  private readonly logger = new Logger(EventsRouter.name);

  @OnCloudEvent('com.example.order.created')
  created(@Payload() data: { orderId: string }, @CloudEvent() event: CloudEventMeta) {
    this.logger.log(`order created: ${data.orderId} (event ${event.id})`);
    return { handled: 'created', orderId: data.orderId };
  }

  // `*` matches exactly one segment: order.shipped, order.cancelled, ...
  @OnCloudEvent('com.example.order.*')
  statusChanged(@CloudEvent() event: CloudEventMeta) {
    this.logger.log(`order status changed: ${event.type} (event ${event.id})`);
    return { handled: 'status-changed', type: event.type };
  }
}
