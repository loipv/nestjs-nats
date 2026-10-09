import { Global, Module } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { NatsConnectionManager } from '../connection/connection-manager.js';
import { HandlerExplorer } from '../discovery/handler-explorer.js';
import { NatsLifecycle } from './nats-lifecycle.js';

@Global()
@Module({
  imports: [DiscoveryModule],
  providers: [NatsConnectionManager, NatsLifecycle, HandlerExplorer],
  exports: [NatsConnectionManager],
})
export class NatsCoreModule {}
