import { readFile } from 'node:fs/promises';
import { expect, expectTypeOf, it } from 'vitest';
import type {
  Batcher,
  BatchOptions,
  ClaimResult,
  CloudEventRouterOptions,
  Delivery,
  DlqOptions,
  HandlerInfo,
  IdempotencyOptions,
  IdempotencyStore,
  InMemoryIdempotencyStoreOptions,
  JetStreamPublishOptions,
  NatsBatchContext,
  NatsContext,
  PublishManyResult,
  RetryOptions,
} from '../../src/index.js';
import * as api from '../../src/index.js';

it('exports the runtime API', () =>
  expect(Object.keys(api).sort()).toEqual(
    [
      'CloudEvent',
      'CloudEventRouter',
      'Consume',
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
      'NatsConfigError',
      'NatsConnectionError',
      'NatsBatchContext',
      'NatsConnectionManager',
      'NatsContext',
      'NatsDecodeError',
      'NatsJetStreamContext',
      'NatsModule',
      'NatsModuleError',
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
    Common & { readonly kind: 'consume'; readonly stream: string; readonly durable: string }
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
  // A type only: instances come from JetStreamClient.batcher().
  expectTypeOf<Batcher>().toHaveProperty('add');
  expectTypeOf<Batcher>().toHaveProperty('flush');
  expectTypeOf<Batcher>().toHaveProperty('close');
  expectTypeOf<CloudEventRouterOptions>().toHaveProperty('unmatched');
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
  ])
    expect(api).not.toHaveProperty(name);
});
