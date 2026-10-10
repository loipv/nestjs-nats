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

  // Run-unique ids: a rerun within the stream's duplicate window would otherwise be
  // de-duplicated by the server and show no billing or DLQ activity.
  const run = Date.now();
  const ok = `o-1-${run}`;
  const bad = `o-2-${run}`;

  // Handled by `OrdersHandlers.bill`.
  await js.publish('orders.created', { id: ok, total: 42 }, { msgId: ok });
  // Fails on purpose: retried three times with backoff, then sent to `dlq.ORDERS.billing`.
  await js.publish('orders.created', { id: bad, total: 10, fail: true }, { msgId: bad });

  // Placed with `publishAtomic`: the order and its stock reservation are stored together or not
  // at all (needs NATS 2.12 and `allow_atomic` on the stream). Billed like the first order.
  const placed = `o-3-${run}`;
  const ack = await js.publishAtomic([
    { subject: 'orders.created', data: { id: placed, total: 15 } },
    { subject: 'stock.reserved', data: { orderId: placed, sku: 'sku-1', qty: 1 } },
  ]);
  logger.log(`placed ${placed}: ${ack.count} messages in one batch, last seq ${ack.seq}`);

  // Routed by `EventsRouter` on the CloudEvent type.
  nats.publish(
    'events.orders',
    { orderId: ok },
    { cloudEvent: { type: 'com.example.order.created' } },
  );
  nats.publish(
    'events.orders',
    { orderId: ok },
    { cloudEvent: { type: 'com.example.order.shipped' } },
  );

  // Request/reply, once the first order has been billed.
  await sleep(500);
  logger.log(
    `query.orders.get ${ok} -> ${JSON.stringify(await nats.request('query.orders.get', { id: ok }))}`,
  );
  // Counted by the ordered consumer of `OrdersProjection`.
  logger.log(
    `query.orders.stats -> ${JSON.stringify(await nats.request('query.orders.stats', {}))}`,
  );
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
