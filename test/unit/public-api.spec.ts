import { readFile } from 'node:fs/promises';
import { expect, expectTypeOf, it } from 'vitest';
import * as health from '../../src/health/index.js';
import type {
  AtomicMessage,
  AtomicPublishAck,
  AtomicPublishOptions,
  Batcher,
  BatchOptions,
  ClaimResult,
  CloudEventInit,
  CloudEventMeta,
  CloudEventRouterOptions,
  ConsumeOrderedOptions,
  ConsumePushOptions,
  Delivery,
  DlqOptions,
  HandlerInfo,
  IdempotencyOptions,
  IdempotencyStore,
  InboundMessage,
  InMemoryIdempotencyStoreOptions,
  JetStreamPublishOptions,
  MessageOutcome,
  NatsBatchContext,
  NatsConnectionManager,
  NatsContext,
  NatsHeadersInit,
  OrderedDeliverPolicy,
  PublishManyResult,
  RetryOptions,
} from '../../src/index.js';
import * as api from '../../src/index.js';
import * as otel from '../../src/otel/index.js';
import * as testing from '../../src/testing/index.js';

it('exports the runtime API', () =>
  expect(Object.keys(api).sort()).toEqual(
    [
      'CloudEvent',
      'CloudEventRouter',
      'Consume',
      'ConsumeOrdered',
      'ConsumePush',
      'Ctx',
      'DEFAULT_CONNECTION',
      'Headers',
      'InMemoryIdempotencyStore',
      'InjectJetStream',
      'InjectNatsClient',
      'InjectNatsConnection',
      'InjectStreamManager',
      'JetStreamClient',
      'Msg',
      'NatsClient',
      'NatsAtomicPublishError',
      'NatsConfigError',
      'NatsConnectionError',
      'NatsBatchContext',
      'NatsConnectionManager',
      'NatsContext',
      'NatsDecodeError',
      'NatsJetStreamContext',
      'NatsModule',
      'NatsModuleError',
      'NatsOrderedContext',
      'NatsRequestError',
      'NatsService',
      'NatsStreamManager',
      'NonRetryableError',
      'OnCloudEvent',
      'Payload',
      'ServiceEndpoint',
      'StreamConfigConflictError',
      'Subject',
      'Subscribe',
      'defaultIsRetryable',
      'getConnectionToken',
      'getJetStreamClientToken',
      'getNatsClientToken',
      'getStreamManagerToken',
      'jsonCodec',
    ].sort(),
  ));

it('does not pull optional peers', async () => {
  const src = await readFile(new URL('../../src/index.ts', import.meta.url), 'utf8');
  expect(src).not.toMatch(/@opentelemetry|@nestjs\/terminus/);
});

const srcRoot = new URL('../../src/', import.meta.url);

/** Every source file `entry` imports, transitively, with the package specifiers it uses. */
async function importGraph(entry: string): Promise<{ files: Set<string>; packages: string[] }> {
  const files = new Set<string>();
  const packages: string[] = [];
  const pending = [entry];
  for (let file = pending.pop(); file !== undefined; file = pending.pop()) {
    if (files.has(file)) continue;
    files.add(file);
    const code = await readFile(new URL(file, srcRoot), 'utf8');
    for (const [, spec] of code.matchAll(/(?:from|import)\s*\(?\s*'([^']+)'/g)) {
      if (!spec) continue;
      if (spec.startsWith('.')) {
        const target = new URL(spec.replace(/\.js$/, '.ts'), new URL(file, srcRoot));
        pending.push(target.pathname.slice(srcRoot.pathname.length));
      } else {
        packages.push(`${file} -> ${spec}`);
      }
    }
  }
  return { files, packages };
}

it('does not pull optional peers or subpath entries, even transitively', async () => {
  const { files, packages } = await importGraph('index.ts');
  const offenders = [
    ...[...files].filter((f) => /^(otel|health|testing)\//.test(f)),
    ...packages.filter((p) => / -> (@opentelemetry\/|@nestjs\/terminus|@nestjs\/testing)/.test(p)),
  ];
  expect(files.size).toBeGreaterThan(20);
  expect(offenders).toEqual([]);
});

it('keeps the testing entry free of the network lifecycle and optional peers', async () => {
  const { files, packages } = await importGraph('testing/index.ts');
  const offenders = [
    ...[...files].filter((f) =>
      /^(otel|health)\/|^module\/(nats-core\.module|nats-lifecycle|nats\.module)\.ts$/.test(f),
    ),
    ...packages.filter((p) => / -> (@opentelemetry\/|@nestjs\/terminus|@nestjs\/testing)/.test(p)),
  ];
  expect(files).toContain('adapters/handler-pipeline.ts');
  expect(offenders).toEqual([]);
});

it('exposes a narrow handler description on Delivery', () => {
  expectTypeOf<Delivery['handler']>().toEqualTypeOf<HandlerInfo>();
  type Common = {
    readonly connection: string;
    readonly className: string;
    readonly methodName: string;
  };
  expectTypeOf<Extract<HandlerInfo, { kind: 'subscribe' }>>().toEqualTypeOf<
    Common & { readonly kind: 'subscribe'; readonly subject: string; readonly queue?: string }
  >();
  expectTypeOf<Extract<HandlerInfo, { kind: 'consume' }>>().toEqualTypeOf<
    Common & {
      readonly kind: 'consume';
      readonly stream: string;
      readonly durable: string;
      readonly delivery: 'pull' | 'push';
    }
  >();
  expectTypeOf<Extract<HandlerInfo, { kind: 'ordered' }>>().toEqualTypeOf<
    Common & {
      readonly kind: 'ordered';
      readonly stream: string;
      readonly filterSubjects: readonly string[];
    }
  >();
  expectTypeOf<Extract<HandlerInfo, { kind: 'service-endpoint' }>>().toEqualTypeOf<
    Common & {
      readonly kind: 'service-endpoint';
      readonly service: { readonly name: string; readonly version: string };
      readonly endpoint: string;
      readonly subject: string;
    }
  >();
  expectTypeOf<Delivery['context']>().toEqualTypeOf<NatsContext | NatsBatchContext>();
  expectTypeOf<keyof Delivery>().toEqualTypeOf<
    'handler' | 'messages' | 'outcomes' | 'context' | 'codec' | 'logger' | 'result'
  >();
});

it('exports the reliability types', () => {
  expectTypeOf<RetryOptions>().toHaveProperty('maxRetries');
  expectTypeOf<DlqOptions>().toHaveProperty('subject');
  expectTypeOf<IdempotencyOptions>().toHaveProperty('ttl');
  expectTypeOf<IdempotencyStore>().toHaveProperty('claim');
  expectTypeOf<ClaimResult>().not.toBeAny();
  expectTypeOf<InMemoryIdempotencyStoreOptions>().toHaveProperty('maxKeys');
  expectTypeOf<BatchOptions>().toHaveProperty('maxSize');
  expectTypeOf<JetStreamPublishOptions>().not.toBeAny();
  expectTypeOf<PublishManyResult>().toHaveProperty('ok');
  expectTypeOf<AtomicMessage>().toHaveProperty('subject');
  expectTypeOf<AtomicPublishOptions>().toHaveProperty('timeout');
  expectTypeOf<AtomicPublishAck>().toHaveProperty('count');
  // A type only: instances come from JetStreamClient.batcher().
  expectTypeOf<Batcher>().toHaveProperty('add');
  expectTypeOf<Batcher>().toHaveProperty('flush');
  expectTypeOf<Batcher>().toHaveProperty('close');
  expectTypeOf<CloudEventRouterOptions>().toHaveProperty('unmatched');
  expectTypeOf<ConsumePushOptions>().toHaveProperty('deliverGroup');
  expectTypeOf<ConsumePushOptions>().toHaveProperty('durable');
  expectTypeOf<ConsumeOrderedOptions>().toHaveProperty('deliverPolicy');
  expectTypeOf<OrderedDeliverPolicy>().not.toBeAny();
});

it('exposes only the query API on NatsConnectionManager', () => {
  // Connecting, closing, the raw connection entries and the testing seams are internal.
  expectTypeOf<keyof NatsConnectionManager>().toEqualTypeOf<
    'has' | 'names' | 'status' | 'status$'
  >();
  const methods = Object.getOwnPropertyNames(api.NatsConnectionManager.prototype).filter(
    (name) => name !== 'constructor',
  );
  expect(methods.sort()).toEqual(['has', 'names', 'status', 'status$']);
});

it('keeps internal types out of the constructors of the DI-only classes', () => {
  // Injected as properties: a constructor parameter would hand users the internal registry
  // (and the handler explorer) through `ConstructorParameters`.
  expectTypeOf<ConstructorParameters<typeof health.NatsHealthIndicator>>().toEqualTypeOf<[]>();
  expectTypeOf<ConstructorParameters<typeof testing.NatsTestHarness>>().toEqualTypeOf<[]>();
});

it('exports NatsHeadersInit, not a HeadersInit shadowing the global one', () => {
  expectTypeOf<NatsHeadersInit>().toEqualTypeOf<Record<string, string | string[]>>();
  // @ts-expect-error renamed to NatsHeadersInit before 0.1.0
  type _Old = import('../../src/index.js').HeadersInit;
});

it('exports the CloudEvents types', () => {
  expectTypeOf<CloudEventMeta>().toEqualTypeOf<{
    id: string;
    source: string;
    type: string;
    specversion: string;
    time?: string;
    subject?: string;
    datacontenttype?: string;
    dataschema?: string;
    extensions: Record<string, string>;
  }>();
  expectTypeOf<CloudEventInit>().toHaveProperty('type').toEqualTypeOf<string>();
  expectTypeOf<CloudEventInit['mode']>().toEqualTypeOf<'binary' | 'structured' | undefined>();
  expectTypeOf<CloudEventRouterOptions['unmatched']>().toEqualTypeOf<
    'ack' | 'term' | 'dlq' | undefined
  >();
  expectTypeOf<CloudEventRouterOptions>().toMatchTypeOf<
    | { subscribe: string }
    | { consume: { stream: string; durable: string } }
    | { consumePush: { stream: string; durable: string; deliverGroup?: string } }
    | { consumeOrdered: { stream: string; headersOnly?: boolean } }
  >();
});

it('exports MessageOutcome as the outcome type of Delivery.outcomes', () => {
  expectTypeOf<Delivery['outcomes']>().toEqualTypeOf<Map<InboundMessage, MessageOutcome>>();
  expectTypeOf<MessageOutcome['action']>().toEqualTypeOf<
    'ack' | 'retry' | 'dlq' | 'term' | 'drop' | 'skipped' | 'replied' | 'none'
  >();
  // A handler that acked and then threw keeps its error on the 'ack' outcome.
  expectTypeOf<Extract<MessageOutcome, { action: 'ack' }>>().toEqualTypeOf<{
    action: 'ack';
    error?: unknown;
  }>();
});

it('exports the subpath entries', () => {
  expect(Object.keys(otel).sort()).toEqual(['tracing']);
  expect(Object.keys(health).sort()).toEqual(['NatsHealthIndicator', 'NatsHealthModule']);
  expect(Object.keys(testing).sort()).toEqual([
    'NatsTestHarness',
    'NatsTestingModule',
    'createMockJetStreamClient',
    'createMockNatsClient',
  ]);
  expectTypeOf<otel.TracingOptions>().toHaveProperty('captureHeaders');
  expectTypeOf<health.NatsHealthCheckOptions>().toHaveProperty('rttTimeout');
  expectTypeOf<testing.MockNatsClient>().toHaveProperty('published');
  expectTypeOf<testing.MockJetStreamClient>().toHaveProperty('published');
  expectTypeOf<testing.PublishedMessage>().toHaveProperty('raw');
  expectTypeOf<testing.DispatchOptions>().toHaveProperty('cloudEvent');
  expectTypeOf<testing.DispatchResult>().toHaveProperty('settlement');
});

it('keeps the subpath entries independent of each other', async () => {
  const forbidden = {
    'otel/index.ts': /^(health|testing)\//,
    'health/index.ts': /^(otel|testing)\//,
  } as const;
  for (const [entry, pattern] of Object.entries(forbidden)) {
    const { files } = await importGraph(entry);
    expect([...files].filter((f) => pattern.test(f))).toEqual([]);
  }
  const otelGraph = await importGraph('otel/index.ts');
  expect(otelGraph.packages.filter((p) => / -> @nestjs\/terminus/.test(p))).toEqual([]);
  const healthGraph = await importGraph('health/index.ts');
  expect(healthGraph.packages.filter((p) => / -> @opentelemetry\//.test(p))).toEqual([]);
});

it('does not export internals', () => {
  for (const name of [
    'Dispatcher',
    'BatchCollector',
    'createSettlement',
    'createIdempotency',
    'decide',
    'backoffDelay',
    'publishToDlq',
    'IdempotencyStoreError',
    'UnmatchedCloudEventError',
    'createTypeRouter',
    'createHandlerPipeline',
    'parseCloudEvent',
    'readCloudEvent',
    'readCloudEventMeta',
    'resolveCloudEvent',
    'writeCloudEvent',
    'cloudEventKey',
    'cloudEventMode',
  ])
    expect(api).not.toHaveProperty(name);
});
