import { Global, Module } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { NatsConnectionManager } from '../connection/connection-manager.js';
import { ConnectionRegistry } from '../connection/connection-registry.js';
import { HandlerExplorer } from '../discovery/handler-explorer.js';
import { NatsLifecycle } from './nats-lifecycle.js';

@Global()
@Module({
  imports: [DiscoveryModule],
  providers: [ConnectionRegistry, NatsConnectionManager, NatsLifecycle, HandlerExplorer],
  // The registry is internal: exported for the module's own providers (and the health
  // indicator); users inject the `NatsConnectionManager` facade.
  exports: [ConnectionRegistry, NatsConnectionManager],
})
export class NatsCoreModule {}
