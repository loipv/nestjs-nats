import { describe, expect, it } from 'vitest';
import {
  DEFAULT_CONNECTION,
  getConnectionToken,
  getJetStreamClientToken,
  getModuleOptionsToken,
  getNatsClientToken,
  getStreamManagerToken,
} from '../../../src/module/tokens.js';

const getters = {
  connection: getConnectionToken,
  client: getNatsClientToken,
  jetstream: getJetStreamClientToken,
  streamManager: getStreamManagerToken,
  options: getModuleOptionsToken,
};

describe('tokens', () => {
  it('names each provider of a connection', () => {
    expect(getConnectionToken('orders')).toBe('NATS_CONNECTION:orders');
    expect(getNatsClientToken('orders')).toBe('NATS_CLIENT:orders');
    expect(getJetStreamClientToken('orders')).toBe('NATS_JETSTREAM:orders');
    expect(getStreamManagerToken('orders')).toBe('NATS_STREAM_MANAGER:orders');
    expect(getModuleOptionsToken('orders')).toBe('NATS_OPTIONS:orders');
  });

  it.each(Object.entries(getters))('%s defaults to the default connection', (_, get) => {
    expect(DEFAULT_CONNECTION).toBe('default');
    expect(get()).toBe(get(DEFAULT_CONNECTION));
  });

  it('gives every provider of every connection its own token', () => {
    const tokens = ['default', 'orders'].flatMap((name) =>
      Object.values(getters).map((get) => get(name)),
    );
    expect(new Set(tokens).size).toBe(tokens.length);
  });
});
