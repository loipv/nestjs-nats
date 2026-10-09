import { NatsModule } from '@loipv/nestjs-nats';
import { NatsHealthModule } from '@loipv/nestjs-nats/health';
import { tracing } from '@loipv/nestjs-nats/otel';
import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { EventsRouter } from './events.router.js';
import { HealthController } from './health.controller.js';
import { OrdersHandlers } from './orders.handlers.js';

@Module({
  imports: [
    NatsModule.forRoot({
      connection: { servers: process.env.NATS_URL ?? 'nats://localhost:4222' },
      // Created (or updated) while the application starts, before any handler runs.
      streams: [
        { name: 'ORDERS', subjects: ['orders.>'] },
        { name: 'DLQ', subjects: ['dlq.>'] },
      ],
      cloudEvents: { source: 'urn:example:orders' },
      plugins: [tracing()],
    }),
    TerminusModule,
    NatsHealthModule,
  ],
  controllers: [HealthController],
  providers: [OrdersHandlers, EventsRouter],
})
export class AppModule {}
