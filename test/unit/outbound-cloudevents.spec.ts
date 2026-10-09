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

describe('CloudEvent datacontenttype on publish', () => {
  const bytes = new Uint8Array([1, 2, 3]);

  it('omits datacontenttype for Uint8Array data unless given (binary)', async () => {
    const { entry, published } = fakeEntry();
    const client = new NatsClient(entry);
    client.publish('o.c', bytes, { cloudEvent: { type: 't', source: 's' } });
    client.publish('o.c', bytes, {
      cloudEvent: { type: 't', source: 's', datacontenttype: 'application/octet-stream' },
    });
    await client.flush();
    expect(published[0]?.opts.headers?.has('Content-Type', 'insensitive')).toBe(false);
    expect(published[0]?.data).toEqual(bytes);
    expect(published[1]?.opts.headers?.get('Content-Type')).toBe('application/octet-stream');
  });

  it('omits datacontenttype for Uint8Array data unless given (structured)', async () => {
    const { entry, published } = fakeEntry();
    const client = new NatsClient(entry);
    const ce = { type: 't', source: 's', mode: 'structured' } as const;
    client.publish('o.c', bytes, { cloudEvent: ce });
    client.publish('o.c', bytes, { cloudEvent: { ...ce, datacontenttype: 'image/png' } });
    await client.flush();
    const env0 = JSON.parse(text(published[0]?.data as Uint8Array));
    expect('datacontenttype' in env0).toBe(false);
    expect(env0.data_base64).toBe(Buffer.from(bytes).toString('base64'));
    const env1 = JSON.parse(text(published[1]?.data as Uint8Array));
    expect(env1.datacontenttype).toBe('image/png');
  });

  it("uses the given datacontenttype, else the codec's, for other data", async () => {
    const { entry, published } = fakeEntry();
    const client = new NatsClient(entry);
    client.publish('o.c', { a: 1 }, { cloudEvent: { type: 't', source: 's' } });
    client.publish(
      'o.c',
      { a: 1 },
      {
        cloudEvent: { type: 't', source: 's', datacontenttype: 'application/vnd.acme+json' },
      },
    );
    await client.flush();
    expect(published[0]?.opts.headers?.get('Content-Type')).toBe('application/json');
    expect(published[1]?.opts.headers?.get('Content-Type')).toBe('application/vnd.acme+json');
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

  it('keeps a Nats-Msg-Id header given by the caller over the CloudEvent id', async () => {
    const { entry, jsPublished } = fakeEntry();
    const js = new JetStreamClient(entry);
    const cloudEvent = { type: 't', source: 's', id: 'ce-1' };
    await js.publish('o.c', 1, { cloudEvent, headers: { 'Nats-Msg-Id': 'from-header' } });
    await js.publish('o.c', 1, { cloudEvent, headers: { 'nats-msg-id': 'lower' } });
    await js.publish('o.c', 1, {
      cloudEvent,
      headers: { 'Nats-Msg-Id': 'from-header' },
      msgId: 'opt',
    });
    await js.publish('o.c', 1, { headers: { 'Nats-Msg-Id': 'plain' } });
    const ids = (i: number) => jsPublished[i]?.opts.headers?.values('Nats-Msg-Id', 'insensitive');
    expect(ids(0)).toEqual(['from-header']);
    expect(ids(1)).toEqual(['lower']);
    expect(ids(2)).toEqual(['opt']);
    expect(ids(3)).toEqual(['plain']);
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
