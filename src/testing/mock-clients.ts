import type { JetStreamClient as NatsJetStreamClient } from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/transport-node';
import { NatsClient } from '../client/nats-client.js';
import { type Codec, jsonCodec } from '../codec/codec.js';
import type { ConnectionEntry } from '../connection/connection-registry.js';
import { NatsConfigError, NatsModuleError } from '../errors/errors.js';
import { type IdempotencyStore, InMemoryIdempotencyStore } from '../idempotency/store.js';
import { JetStreamClient } from '../jetstream/jetstream-client.js';
import { createNatsLogger } from '../module/logger.js';
import type { NatsModuleOptions } from '../module/options.js';
import { DEFAULT_CONNECTION } from '../module/tokens.js';
import type { NatsPlugin } from '../pipeline/types.js';
import { FakeNatsConnection, type PublishedMessage, type RequestStub } from './fake-connection.js';

export interface MockClientOptions {
  /** Encodes what is published and decodes `published[i].data` (default JSON). */
  codec?: Codec;
  /** Outbound plugins run on every publish, as on a real connection. */
  plugins?: NatsPlugin[];
  /** Defaults of the CloudEvents the client publishes. */
  cloudEvents?: { source?: string };
}

/** A `NatsClient` over a fake connection: records publishes, answers requests from stubs. */
export type MockNatsClient = NatsClient & {
  /** Everything published (requests included, with their `reply` inbox), in order. */
  readonly published: PublishedMessage[];
  /** Stubs the requests to `subject` (a NATS subject pattern; the latest stub wins). */
  whenRequest(subject: string): RequestStub;
};

/** A `JetStreamClient` over a fake connection: records publishes, acks from a `'mock'` stream. */
export type MockJetStreamClient = JetStreamClient & {
  /** Every JetStream publish, in order (duplicates included). */
  readonly published: PublishedMessage[];
};

/** Internal: the fake connection behind an entry of the testing module or of a mock. */
export function fakeConnectionOf(entry: ConnectionEntry): FakeNatsConnection {
  const nc: unknown = entry.nc;
  if (!(nc instanceof FakeNatsConnection)) {
    throw new NatsModuleError(`NATS connection "${entry.name}" is not a fake connection`);
  }
  return nc;
}

class MockNatsClientImpl extends NatsClient {
  constructor(
    entry: ConnectionEntry,
    private readonly fake: FakeNatsConnection,
  ) {
    super(entry);
  }

  get published(): PublishedMessage[] {
    return this.fake.published;
  }

  whenRequest(subject: string): RequestStub {
    return this.fake.whenRequest(subject);
  }
}

class MockJetStreamClientImpl extends JetStreamClient {
  constructor(
    entry: ConnectionEntry,
    private readonly fake: FakeNatsConnection,
  ) {
    super(entry);
  }

  get published(): PublishedMessage[] {
    return this.fake.jsPublished;
  }
}

/** Internal: the mock `NatsClient` of an entry whose connection is fake. */
export function mockNatsClientOf(entry: ConnectionEntry): MockNatsClient {
  return new MockNatsClientImpl(entry, fakeConnectionOf(entry));
}

/** Internal: the mock `JetStreamClient` of an entry whose connection is fake. */
export function mockJetStreamClientOf(entry: ConnectionEntry): MockJetStreamClient {
  return new MockJetStreamClientImpl(entry, fakeConnectionOf(entry));
}

/** A connection entry over a new fake connection, for a standalone mock client. */
function mockEntry(options: MockClientOptions): ConnectionEntry {
  const { codec, plugins, cloudEvents } = options;
  const moduleOptions: NatsModuleOptions = {
    name: DEFAULT_CONNECTION,
    connection: {},
    ...(codec ? { codec } : {}),
    ...(plugins ? { plugins } : {}),
    ...(cloudEvents ? { cloudEvents } : {}),
  };
  const fake = new FakeNatsConnection(codec ?? jsonCodec());
  let store: IdempotencyStore | undefined;
  return {
    name: DEFAULT_CONNECTION,
    options: moduleOptions,
    nc: fake as unknown as NatsConnection,
    logger: createNatsLogger(undefined, 'NatsMockClient'),
    js: () => fake.jetstream() as unknown as NatsJetStreamClient,
    jsm: () =>
      Promise.reject(new NatsConfigError('JetStream management is not available on a mock client')),
    idempotencyStore() {
      store ??= new InMemoryIdempotencyStore();
      return store;
    },
    // A standalone mock is never closed: close hooks (batchers) never run.
    onClose: () => () => {},
  };
}

/**
 * A real `NatsClient` running on a fake connection: outbound plugins and CloudEvents work
 * as usual, publishes are recorded in `published` and requests are answered from
 * `whenRequest` stubs (no stub: `NatsRequestError` `'no_responders'`).
 */
export function createMockNatsClient(options: MockClientOptions = {}): MockNatsClient {
  return mockNatsClientOf(mockEntry(options));
}

/**
 * A real `JetStreamClient` running on a fake connection: publishes are recorded in
 * `published` and acked with `{ stream: 'mock', seq, duplicate }`, `duplicate` when the
 * `Nats-Msg-Id` was already published.
 */
export function createMockJetStreamClient(options: MockClientOptions = {}): MockJetStreamClient {
  return mockJetStreamClientOf(mockEntry(options));
}
