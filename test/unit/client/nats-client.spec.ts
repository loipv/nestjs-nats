import type { NatsConnection } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';
import { NatsClient } from '../../../src/client/nats-client.js';
import { jsonCodec } from '../../../src/codec/codec.js';
import {
  type ConnectionEntry,
  ConnectionRegistry,
} from '../../../src/connection/connection-registry.js';
import type { NatsPlugin } from '../../../src/pipeline/types.js';
import { FakeNatsConnection } from '../../../src/testing/fake-connection.js';

class FakeConnectionManager extends ConnectionRegistry {
  constructor(private readonly fake: unknown) {
    super();
  }

  protected override openConnection(): Promise<NatsConnection> {
    return Promise.resolve(this.fake as NatsConnection);
  }
}

describe('NatsClient at close', () => {
  it('sends a publish still in its outbound plugins before the connection drains', async () => {
    const fake = new FakeNatsConnection(jsonCodec());
    const events: string[] = [];
    fake.onPublish((m) => events.push(`publish ${m.subject}`));
    const drain = fake.drain.bind(fake);
    vi.spyOn(fake, 'drain').mockImplementation(() => {
      events.push('drain');
      return drain();
    });
    const slow: NatsPlugin = {
      name: 'slow',
      outbound: {
        async handle(_m, next) {
          await new Promise((resolve) => setTimeout(resolve, 50));
          await next();
        },
      },
    };
    const mgr = new FakeConnectionManager(fake);
    const entry = await mgr.connect('default', { connection: {}, logger: false, plugins: [slow] });
    const client = new NatsClient(entry);
    client.publish('o', 1);
    await mgr.closeAll();
    expect(events).toEqual(['publish o', 'drain']);
  });
});

describe('NatsClient.publish without outbound plugins', () => {
  function setup(publish: () => void) {
    const error = vi.fn();
    const entry = {
      name: 'default',
      options: { connection: {} },
      nc: { publish: vi.fn(publish), flush: vi.fn(async () => {}) },
      logger: { log: vi.fn(), warn: vi.fn(), error },
      onClose: () => () => {},
    } as unknown as ConnectionEntry;
    return { client: new NatsClient(entry), nc: entry.nc, error };
  }

  it('publishes at once', () => {
    const { client, nc } = setup(() => {});
    client.publish('o', { n: 1 }, { headers: { a: '1' } });
    expect(nc.publish).toHaveBeenCalledOnce();
    const [subject, payload, opts] = vi.mocked(nc.publish).mock.calls[0] as unknown as [
      string,
      Uint8Array,
      { headers: { get(k: string): string } },
    ];
    expect(subject).toBe('o');
    expect(new TextDecoder().decode(payload)).toBe('{"n":1}');
    expect(opts.headers.get('a')).toBe('1');
  });

  it('logs a failure at once, with credentials masked, and never throws', () => {
    const { client, error } = setup(() => {
      throw new Error('closed nats://u:secret@h');
    });
    expect(() => client.publish('o', 1)).not.toThrow();
    expect(error).toHaveBeenCalledExactlyOnceWith('Failed to publish to "o": closed nats://***@h');
  });

  it('still throws an invalid cloudEvent to the caller', () => {
    const { client, nc } = setup(() => {});
    expect(() => client.publish('o', 1, { cloudEvent: { type: 't' } })).toThrow(/source/);
    expect(nc.publish).not.toHaveBeenCalled();
  });
});
