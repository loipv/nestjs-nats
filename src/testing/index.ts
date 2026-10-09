export type { PublishedMessage, RequestStub } from './fake-connection.js';
export type {
  DispatchOptions,
  DispatchResult,
  DispatchSettlement,
  DispatchTarget,
} from './harness.js';
export { NatsTestHarness } from './harness.js';
export {
  createMockJetStreamClient,
  createMockNatsClient,
  type MockClientOptions,
  type MockJetStreamClient,
  type MockNatsClient,
} from './mock-clients.js';
export { NatsTestingModule } from './nats-testing.module.js';
