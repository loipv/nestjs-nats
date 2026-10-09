import 'reflect-metadata';
import { JetStreamClient, NatsClient } from '@loipv/nestjs-nats';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
import { setupTracing } from './tracing.js';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function bootstrap() {
  setupTracing();
  const logger = new Logger('main');
  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();
  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port);
  logger.log(`listening on http://localhost:${port} (try /health)`);

  // Sample traffic, so a fresh run shows every feature in the log.
  const js = app.get(JetStreamClient);
  const nats = app.get(NatsClient);

  // Handled by `OrdersHandlers.bill`.
  await js.publish('orders.created', { id: 'o-1', total: 42 }, { msgId: 'o-1' });
  // Fails on purpose: retried three times with backoff, then sent to `dlq.ORDERS.billing`.
  await js.publish('orders.created', { id: 'o-2', total: 10, fail: true }, { msgId: 'o-2' });

  // Routed by `EventsRouter` on the CloudEvent type.
  nats.publish(
    'events.orders',
    { orderId: 'o-1' },
    { cloudEvent: { type: 'com.example.order.created' } },
  );
  nats.publish(
    'events.orders',
    { orderId: 'o-1' },
    { cloudEvent: { type: 'com.example.order.shipped' } },
  );

  // Request/reply, once `o-1` has been billed.
  await sleep(500);
  logger.log(
    `query.orders.get o-1 -> ${JSON.stringify(await nats.request('query.orders.get', { id: 'o-1' }))}`,
  );
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
