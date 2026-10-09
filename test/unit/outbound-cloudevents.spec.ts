import { type MsgHdrs } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';
import { NatsClient } from '../../src/client/nats-client.js';
import type { ConnectionEntry } from '../../src/connection/connection-manager.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import { JetStreamClient } from '../../src/jetstream/jetstream-client.js';
import type { NatsModuleOptions } from '../../src/module/options.js';
import type { NatsPlugin } from '../../src/pipeline/types.js';

type Published = { subject: string; data: Uint8Array; opts: { headers?: MsgHdrs } };

function fakeEntry(options: Partial<NatsModuleOptions> = {}) {
  const published: Published[] = [];
  const jsPublished: Published[] = [];
  const entry = {
    name: 'default',
    options: { connection: {}, ...options },
    logger: { error: vi.fn(), warn: vi.fn(), log: vi.fn(), debug: vi.fn() },
    nc: {
      publish: (subject: string, data: Uint8Array, opts: { headers?: MsgHdrs }) => {
        published.push({ subject, data, opts });
      },
      flush: async () => {},
    },
    js: () => ({
      publish: async (subject: string, data: Uint8Array, opts: { headers?: MsgHdrs }) => {
        jsPublished.push({ subject, data, opts });
        return { stream: 'S', seq: 1, duplicate: false };
      },
    }),
    onClose: () => () => {},
  } as unknown as ConnectionEntry;
  return { entry, published, jsPublished };
}

const text = (b: Uint8Array) => new TextDecoder().decode(b);

describe('NatsClient.publish with cloudEvent', () => {
  it('publishes binary CloudEvents by default', async () => {
    const { entry, published } = fakeEntry();
    const client = new NatsClient(entry);
    client.publish('o.c', { a: 1 }, { cloudEvent: { type: 't', source: 's' } });
    await client.flush();
    const h = published[0]?.opts.headers;
    expect(h?.get('ce-type')).toBe('t');
    expect(h?.get('ce-source')).toBe('s');
    expect(h?.get('ce-specversion')).toBe('1.0');
    expect(h?.get('ce-id')).toBeTruthy();
    expect(h?.get('Content-Type')).toBe('application/json');
    expect(JSON.parse(text(published[0]?.data as Uint8Array))).toEqual({ a: 1 });
  });

  it('uses the module source', async () => {
    const { entry, published } = fakeEntry({ cloudEvents: { source: 'urn:svc' } });
    const client = new NatsClient(entry);
    client.publish('o.c', 1, { cloudEvent: { type: 't' } });
    await client.flush();
    expect(published[0]?.opts.headers?.get('ce-source')).toBe('urn:svc');
  });

  it('publishes structured CloudEvents', async () => {
    const { entry, published } = fakeEntry();
    const client = new NatsClient(entry);
    client.publish('o.c', { a: 1 }, { cloudEvent: { type: 't', source: 's', mode: 'structured' } });
    await client.flush();
    expect(published[0]?.opts.headers?.get('Content-Type')).toBe('application/cloudevents+json');
    expect(published[0]?.opts.headers?.get('ce-type')).toBe('');
    const env = JSON.parse(text(published[0]?.data as Uint8Array));
    expect(env).toMatchObject({ specversion: '1.0', type: 't', source: 's', data: { a: 1 } });
  });

  it('lets plugins see the resolved CloudEvent and its modified headers', async () => {
    const seen: string[] = [];
    const plugin: NatsPlugin = {
      name: 'p',
      outbound: {
        async handle(m, next) {
          seen.push(m.cloudEvent?.meta.id ?? 'none');
          m.headers.set('ce-extra', 'x');
          m.headers.set('x-plugin', '1');
          await next();
        },
      },
    };
    const { entry, published } = fakeEntry({ plugins: [plugin] });
    const client = new NatsClient(entry);
    client.publish('o.c', 1, { cloudEvent: { type: 't', source: 's', id: 'fixed' } });
    await client.flush();
    expect(seen).toEqual(['fixed']);
    expect(published[0]?.opts.headers?.get('ce-id')).toBe('fixed');
    expect(published[0]?.opts.headers?.get('x-plugin')).toBe('1');
  });

  it('throws NatsConfigError synchronously without a source', () => {
    const { entry } = fakeEntry();
    const client = new NatsClient(entry);
    expect(() => client.publish('x', 1, { cloudEvent: { type: 't' } })).toThrow(NatsConfigError);
  });

  it('does not add CloudEvent headers without the option', async () => {
    const { entry, published } = fakeEntry({ cloudEvents: { source: 's' } });
    const client = new NatsClient(entry);
    client.publish('o.c', 1);
    await client.flush();
    expect(published[0]?.opts.headers).toBeUndefined();
  });
});

describe('JetStreamClient.publish with cloudEvent', () => {
  it('sets Nats-Msg-Id from the CloudEvent id, keeping an explicit msgId', async () => {
    const { entry, jsPublished } = fakeEntry();
    const js = new JetStreamClient(entry);
    await js.publish('o.c', 1, { cloudEvent: { type: 't', source: 's', id: 'ce-1' } });
    await js.publish('o.c', 1, {
      cloudEvent: { type: 't', source: 's', id: 'ce-2' },
      msgId: 'mine',
    });
    expect(jsPublished[0]?.opts.headers?.get('Nats-Msg-Id')).toBe('ce-1');
    expect(jsPublished[0]?.opts.headers?.get('ce-id')).toBe('ce-1');
    expect(jsPublished[1]?.opts.headers?.get('Nats-Msg-Id')).toBe('mine');
    expect(jsPublished[1]?.opts.headers?.get('ce-id')).toBe('ce-2');
  });

  it('rejects without a source', async () => {
    const { entry } = fakeEntry();
    const js = new JetStreamClient(entry);
    await expect(js.publish('o.c', 1, { cloudEvent: { type: 't' } })).rejects.toBeInstanceOf(
      NatsConfigError,
    );
  });

  it('applies to publishMany results', async () => {
    const { entry, jsPublished } = fakeEntry({ cloudEvents: { source: 'urn:svc' } });
    const js = new JetStreamClient(entry);
    const res = await js.publishMany([
      { subject: 'a', data: 1, opts: { cloudEvent: { type: 't', id: 'x' } } },
      { subject: 'b', data: 1, opts: { cloudEvent: { type: 't' } } },
    ]);
    expect(res.map((r) => r.ok)).toEqual([true, true]);
    expect(jsPublished.map((p) => p.opts.headers?.get('ce-source'))).toEqual([
      'urn:svc',
      'urn:svc',
    ]);
  });
});
