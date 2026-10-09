import { readFile } from 'node:fs/promises';
import { expect, it } from 'vitest';
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
