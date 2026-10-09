import { readFile } from 'node:fs/promises';
import { expect, expectTypeOf, it } from 'vitest';
import type { Delivery, HandlerInfo } from '../../src/index.js';
import * as api from '../../src/index.js';

it('exports the runtime API', () =>
  expect(Object.keys(api).sort()).toEqual(
    [
      'Consume',
      'Ctx',
      'DEFAULT_CONNECTION',
      'Headers',
      'InjectJetStream',
      'InjectNatsClient',
      'InjectNatsConnection',
      'InjectStreamManager',
      'JetStreamClient',
      'Msg',
      'NatsClient',
      'NatsConfigError',
      'NatsConnectionError',
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
      'Payload',
      'ServiceEndpoint',
      'StreamConfigConflictError',
      'Subject',
      'Subscribe',
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
  expectTypeOf<keyof Delivery>().toEqualTypeOf<
    'handler' | 'messages' | 'context' | 'codec' | 'logger' | 'result'
  >();
});
