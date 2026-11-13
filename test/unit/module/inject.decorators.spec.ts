import 'reflect-metadata';
import { Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { describe, expect, it } from 'vitest';
import {
  InjectJetStream,
  InjectNatsClient,
  InjectNatsConnection,
  InjectStreamManager,
} from '../../../src/module/inject.decorators.js';
import {
  getConnectionToken,
  getJetStreamClientToken,
  getNatsClientToken,
  getStreamManagerToken,
} from '../../../src/module/tokens.js';

@Injectable()
class ByConstructor {
  constructor(
    @InjectNatsClient() readonly defaultClient: string,
    @InjectNatsClient('orders') readonly client: string,
    @InjectNatsConnection('orders') readonly connection: string,
    @InjectJetStream('orders') readonly jetstream: string,
    @InjectStreamManager('orders') readonly streams: string,
  ) {}
}

@Injectable()
class ByProperty {
  @InjectNatsConnection() readonly defaultConnection!: string;
  @InjectJetStream() readonly defaultJetstream!: string;
  @InjectStreamManager() readonly defaultStreams!: string;
  @InjectNatsClient('orders') readonly client!: string;
}

/** Every token of the `default` and `orders` connections, provided as its own name. */
const providers = ['default', 'orders'].flatMap((name) =>
  [getConnectionToken, getNatsClientToken, getJetStreamClientToken, getStreamManagerToken].map(
    (get) => ({ provide: get(name), useValue: get(name) }),
  ),
);

async function resolve<T>(type: new (...args: never[]) => T): Promise<T> {
  const moduleRef = await Test.createTestingModule({ providers: [...providers, type] }).compile();
  const instance = moduleRef.get(type);
  await moduleRef.close();
  return instance;
}

describe('inject decorators', () => {
  it('inject the providers of the named connection, or of the default one', async () => {
    const injected = await resolve(ByConstructor);
    expect({ ...injected }).toEqual({
      defaultClient: getNatsClientToken('default'),
      client: getNatsClientToken('orders'),
      connection: getConnectionToken('orders'),
      jetstream: getJetStreamClientToken('orders'),
      streams: getStreamManagerToken('orders'),
    });
  });

  it('work on properties too', async () => {
    const injected = await resolve(ByProperty);
    expect(injected.defaultConnection).toBe(getConnectionToken('default'));
    expect(injected.defaultJetstream).toBe(getJetStreamClientToken('default'));
    expect(injected.defaultStreams).toBe(getStreamManagerToken('default'));
    expect(injected.client).toBe(getNatsClientToken('orders'));
  });
});
