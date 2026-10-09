import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { NatsHealthIndicator } from './nats-health.indicator.js';

/** Provides {@link NatsHealthIndicator}; import it next to `NatsModule` (global core). */
@Module({
  imports: [TerminusModule],
  providers: [NatsHealthIndicator],
  exports: [NatsHealthIndicator],
})
export class NatsHealthModule {}
