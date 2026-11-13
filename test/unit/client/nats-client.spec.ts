import type { NatsConnection } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';
import { NatsClient } from '../../../src/client/nats-client.js';
import { jsonCodec } from '../../../src/codec/codec.js';
import { ConnectionRegistry } from '../../../src/connection/connection-registry.js';
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
