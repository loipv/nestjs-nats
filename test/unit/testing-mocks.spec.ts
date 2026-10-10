import { describe, expect, it } from 'vitest';
import { NatsRequestError } from '../../src/errors/errors.js';
import type { NatsPlugin } from '../../src/pipeline/types.js';
import { createMockJetStreamClient, createMockNatsClient } from '../../src/testing/index.js';

const enc = new TextEncoder();

describe('createMockNatsClient', () => {
  it('records publishes with decoded data and headers', () => {
    const mock = createMockNatsClient();
    mock.publish('o', { a: 1 }, { headers: { x: '1' } });
    expect(mock.published).toHaveLength(1);
    expect(mock.published[0]).toEqual({
      subject: 'o',
      data: { a: 1 },
      raw: enc.encode('{"a":1}'),
      headers: { x: '1' },
    });
  });

  it('records the reply subject of a publish', () => {
    const mock = createMockNatsClient();
    mock.publish('o', 1, { reply: 'inbox' });
    expect(mock.published[0]).toMatchObject({ subject: 'o', data: 1, reply: 'inbox' });
  });

  it('keeps the raw bytes as data when they cannot be decoded', () => {
    const mock = createMockNatsClient();
    const bytes = enc.encode('not json');
    mock.publish('o', bytes);
    expect(mock.published[0]?.data).toEqual(bytes);
    expect(mock.published[0]?.raw).toEqual(bytes);
  });

  it('decodes with the codec of the client', () => {
    const codec = {
      contentType: 'text/plain',
      encode: (d: unknown) => enc.encode(String(d)),
      decode: (b: Uint8Array) => `decoded:${new TextDecoder().decode(b)}`,
    };
    const mock = createMockNatsClient({ codec });
    mock.publish('o', 'hi');
    expect(mock.published[0]?.data).toBe('decoded:hi');
  });

  it('stubs requests and fails unknown ones with no_responders', async () => {
    const mock = createMockNatsClient();
    mock.whenRequest('q').reply(5);
    await expect(mock.request('q', { n: 1 })).resolves.toBe(5);
    // The request itself is recorded, with its reply inbox.
    expect(mock.published[0]).toMatchObject({ subject: 'q', data: { n: 1 } });
    expect(mock.published[0]?.reply).toEqual(expect.any(String));
    const err = await mock.request('other').catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NatsRequestError);
    expect(err).toMatchObject({ code: 'no_responders', subject: 'other' });
  });

  it('stubs failing requests with a service error', async () => {
    const mock = createMockNatsClient();
    mock.whenRequest('q').fail({ code: 409, description: 'Conflict' });
    await expect(mock.request('q')).rejects.toMatchObject({
      code: 'service_error',
      serviceCode: 409,
      description: 'Conflict',
    });
  });

  it('matches request stubs with wildcards, the latest stub winning', async () => {
    const mock = createMockNatsClient();
    mock.whenRequest('q.>').reply('any');
    await expect(mock.request('q.a.b')).resolves.toBe('any');
    mock.whenRequest('q.a.b').reply('exact');
    await expect(mock.request('q.a.b')).resolves.toBe('exact');
  });

  it('records CloudEvents', () => {
    const mock = createMockNatsClient({ cloudEvents: { source: 'svc' } });
    mock.publish('o', { a: 1 }, { cloudEvent: { type: 'o.created' } });
    mock.publish('o', { b: 2 }, { cloudEvent: { type: 'o.updated', mode: 'structured' } });
    expect(mock.published[0]?.cloudEvent).toMatchObject({
      type: 'o.created',
      source: 'svc',
      specversion: '1.0',
    });
    expect(mock.published[0]?.data).toEqual({ a: 1 });
    expect(mock.published[1]?.cloudEvent?.type).toBe('o.updated');
    expect(mock.published[1]?.data).toEqual({ b: 2 });
    mock.publish('plain', 1);
    expect(mock.published[2]?.cloudEvent).toBeUndefined();
  });

  it('runs outbound plugins', async () => {
    const plugin: NatsPlugin = {
      name: 'tag',
      outbound: {
        async handle(m, next) {
          await Promise.resolve();
          m.headers.set('x-tag', 'yes');
          await next();
        },
      },
    };
    const mock = createMockNatsClient({ plugins: [plugin] });
    mock.publish('o', 1);
    await mock.flush();
    expect(mock.published[0]?.headers).toEqual({ 'x-tag': 'yes' });
  });
});

describe('createMockJetStreamClient', () => {
  it('marks duplicate JetStream msgIds', async () => {
    const js = createMockJetStreamClient();
    const first = await js.publish('o', { a: 1 }, { msgId: 'm1' });
    const second = await js.publish('o', { a: 1 }, { msgId: 'm1' });
    const third = await js.publish('o', { a: 2 }, { msgId: 'm2' });
    expect(first).toMatchObject({ stream: 'mock', seq: 1, duplicate: false });
    expect(second).toMatchObject({ stream: 'mock', seq: 2, duplicate: true });
    expect(third).toMatchObject({ stream: 'mock', seq: 3, duplicate: false });
    expect(js.published.map((m) => m.data)).toEqual([{ a: 1 }, { a: 1 }, { a: 2 }]);
    expect(js.published[0]?.headers).toEqual({ 'Nats-Msg-Id': 'm1' });
  });

  it('records CloudEvents and publishMany', async () => {
    const js = createMockJetStreamClient({ cloudEvents: { source: 's' } });
    const results = await js.publishMany([
      { subject: 'a', data: 1 },
      { subject: 'b', data: 2, opts: { cloudEvent: { type: 't', id: 'e1' } } },
    ]);
    expect(results.every((r) => r.ok)).toBe(true);
    expect(js.published.map((m) => m.subject)).toEqual(['a', 'b']);
    expect(js.published[1]?.cloudEvent).toMatchObject({ type: 't', id: 'e1', source: 's' });
    // Without msgId, the CloudEvent id is the Nats-Msg-Id.
    expect(js.published[1]?.headers['Nats-Msg-Id']).toBe('e1');
  });
});

describe('createMockJetStreamClient publishAtomic', () => {
  it('records each message of the batch with its position', async () => {
    const mock = createMockJetStreamClient();
    await mock.publishAtomic([
      { subject: 'a.1', data: { n: 1 } },
      { subject: 'a.2', data: { n: 2 } },
    ]);
    expect(mock.published).toHaveLength(2);
    expect(mock.published.map((m) => m.data)).toEqual([{ n: 1 }, { n: 2 }]);
    const [first, second] = mock.published;
    expect(first?.atomic).toEqual({ batch: expect.any(String), index: 0, count: 2 });
    expect(second?.atomic).toEqual({ batch: first?.atomic?.batch, index: 1, count: 2 });
  });
});
