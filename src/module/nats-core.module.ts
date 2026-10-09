import { Global, Injectable, Module, type OnApplicationShutdown } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { NatsConnectionManager } from '../connection/connection-manager.js';

/** Temporary shutdown hook; replaced by NatsLifecycle. */
@Injectable()
export class NatsShutdown implements OnApplicationShutdown {
  constructor(private readonly manager: NatsConnectionManager) {}

  async onApplicationShutdown(): Promise<void> {
    await this.manager.closeAll();
  }
}

@Global()
@Module({
  imports: [DiscoveryModule],
  providers: [NatsConnectionManager, NatsShutdown],
  exports: [NatsConnectionManager],
})
export class NatsCoreModule {}
