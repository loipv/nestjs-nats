import { headers, type MsgHdrs } from '@nats-io/transport-node';
import { describe, expect, it } from 'vitest';
import {
  cloudEventKey,
  cloudEventMode,
  readCloudEvent,
  readCloudEventMeta,
  resolveCloudEvent,
  writeCloudEvent,
} from '../../src/cloudevents/codec.js';
import { CE_STRUCTURED_CONTENT_TYPE, type CloudEventMeta } from '../../src/cloudevents/types.js';
import { type Codec, jsonCodec } from '../../src/codec/codec.js';
import { NatsConfigError, NatsDecodeError } from '../../src/errors/errors.js';

const textCodec: Codec = {
  contentType: 'text/plain',
  encode: (d) => new TextEncoder().encode(String(d)),
  decode: (b) => new TextDecoder().decode(b),
};

const meta = (over: Partial<CloudEventMeta> = {}): CloudEventMeta => ({
  id: 'e1',
  source: 'svc',
  type: 'com.x.created',
  specversion: '1.0',
  datacontenttype: 'application/json',
  extensions: {},
  ...over,
});

function hdrs(init: Record<string, string>): MsgHdrs {
  const h = headers();
  for (const [k, v] of Object.entries(init)) h.set(k, v);
  return h;
}

function binaryMsg(attrs: Record<string, string>, data: Uint8Array = new Uint8Array(0)) {
  const init: Record<string, string> = { 'ce-specversion': '1.0', 'ce-type': 't' };
  for (const [k, v] of Object.entries(attrs)) init[`ce-${k}`] = v;
  return { data, headers: hdrs(init) };
}

function structuredMsg(env: unknown, ct = CE_STRUCTURED_CONTENT_TYPE) {
  return {
    data: new TextEncoder().encode(JSON.stringify(env)),
    headers: hdrs({ 'Content-Type': ct }),
  };
}

const fullEnv = { specversion: '1.0', id: '1', source: 's', type: 't' };

describe('cloudEventMode', () => {
  it('detects binary case-insensitively', () => {
    expect(cloudEventMode(hdrs({ 'CE-SpecVersion': '1.0' }))).toBe('binary');
  });
  it('detects structured by content type prefix', () => {
    expect(
      cloudEventMode(hdrs({ 'content-type': 'application/cloudevents+json; charset=utf-8' })),
    ).toBe('structured');
  });
  it('returns undefined otherwise', () => {
    expect(cloudEventMode(undefined)).toBeUndefined();
    expect(cloudEventMode(hdrs({ 'Content-Type': 'application/json' }))).toBeUndefined();
  });
});

describe('writeCloudEvent / readCloudEvent', () => {
  it('roundtrips binary mode', () => {
    const h = headers();
    const m = meta({
      time: '2024-01-01T00:00:00.000Z',
      subject: 'sub',
      dataschema: 'urn:s',
      extensions: { tenant: 't1' },
    });
    const body = writeCloudEvent(m, { a: 1 }, jsonCodec(), 'binary', h);
    expect(h.get('ce-specversion')).toBe('1.0');
    expect(h.get('ce-id')).toBe('e1');
    expect(h.get('ce-source')).toBe('svc');
    expect(h.get('ce-type')).toBe('com.x.created');
    expect(h.get('ce-time')).toBe('2024-01-01T00:00:00.000Z');
    expect(h.get('ce-subject')).toBe('sub');
    expect(h.get('ce-dataschema')).toBe('urn:s');
    expect(h.get('ce-tenant')).toBe('t1');
    expect(h.get('Content-Type')).toBe('application/json');
    const out = readCloudEvent({ data: body, headers: h }, jsonCodec());
    expect(out.data).toEqual({ a: 1 });
    expect(out.meta).toEqual(m);
  });

  it('roundtrips structured mode with JSON data', () => {
    const h = headers();
    const body = writeCloudEvent(
      meta({ extensions: { tenant: 't1' } }),
      { a: 1 },
      jsonCodec(),
      'structured',
      h,
    );
    expect(h.get('Content-Type')).toBe(CE_STRUCTURED_CONTENT_TYPE);
    const env = JSON.parse(new TextDecoder().decode(body));
    expect(env).toMatchObject({
      specversion: '1.0',
      id: 'e1',
      source: 'svc',
      type: 'com.x.created',
      datacontenttype: 'application/json',
      tenant: 't1',
      data: { a: 1 },
    });
    const out = readCloudEvent({ data: body, headers: h }, jsonCodec());
    expect(out.data).toEqual({ a: 1 });
    expect(out.meta.extensions).toEqual({ tenant: 't1' });
    expect(out.meta.id).toBe('e1');
  });

  it('uses data_base64 for non-JSON codecs and decodes it on read', () => {
    const h = headers();
    const body = writeCloudEvent(
      meta({ datacontenttype: 'text/plain' }),
      'hello',
      textCodec,
      'structured',
      h,
    );
    const env = JSON.parse(new TextDecoder().decode(body));
    expect(env.data).toBeUndefined();
    expect(env.data_base64).toBe(Buffer.from('hello').toString('base64'));
    expect(readCloudEvent({ data: body, headers: h }, textCodec).data).toBe('hello');
  });

  it('uses data_base64 for Uint8Array data even with the JSON codec', () => {
    const h = headers();
    const bytes = new Uint8Array([1, 2, 3]);
    const body = writeCloudEvent(meta(), bytes, jsonCodec(), 'structured', h);
    const env = JSON.parse(new TextDecoder().decode(body));
    expect(env.data_base64).toBe(Buffer.from(bytes).toString('base64'));
    expect(env.data).toBeUndefined();
  });

  it('omits data fields for undefined data', () => {
    const h = headers();
    const body = writeCloudEvent(meta(), undefined, jsonCodec(), 'structured', h);
    const env = JSON.parse(new TextDecoder().decode(body));
    expect('data' in env).toBe(false);
    expect('data_base64' in env).toBe(false);
    expect(readCloudEvent({ data: body, headers: h }, jsonCodec()).data).toBeUndefined();
  });

  it('does not let extensions override core attributes on write', () => {
    const h = headers();
    writeCloudEvent(meta({ extensions: { id: 'evil' } }), 1, jsonCodec(), 'binary', h);
    expect(h.get('ce-id')).toBe('e1');
  });

  it('reads a foreign structured event with data_base64 and a non-JSON content type', () => {
    const env = {
      ...fullEnv,
      datacontenttype: 'text/plain',
      data_base64: Buffer.from('hi').toString('base64'),
    };
    const out = readCloudEvent(structuredMsg(env), textCodec);
    expect(out.data).toBe('hi');
    expect(out.meta.datacontenttype).toBe('text/plain');
  });

  it('reads structured data as the raw JSON value, not through the codec', () => {
    const out = readCloudEvent(structuredMsg({ ...fullEnv, data: 'plain string' }), textCodec);
    expect(out.data).toBe('plain string');
  });

  it('reads binary attributes case-insensitively and collects extensions', () => {
    const msg = {
      data: new TextEncoder().encode('{"x":1}'),
      headers: hdrs({
        'CE-SpecVersion': '1.0',
        'CE-Id': '9',
        'ce-source': 's',
        'ce-type': 't',
        'ce-tenant': 'acme',
        'CE-Region': 'eu',
        'content-type': 'application/json',
      }),
    };
    const out = readCloudEvent(msg, jsonCodec());
    expect(out.meta).toEqual({
      id: '9',
      source: 's',
      type: 't',
      specversion: '1.0',
      datacontenttype: 'application/json',
      extensions: { tenant: 'acme', region: 'eu' },
    });
    expect(out.data).toEqual({ x: 1 });
  });

  it('wraps codec failures on binary data in NatsDecodeError', () => {
    const msg = binaryMsg({ id: '1', source: 's' }, new TextEncoder().encode('{bad'));
    expect(() => readCloudEvent(msg, jsonCodec())).toThrow(NatsDecodeError);
  });

  it.each(['id', 'source', 'type', 'specversion'])('rejects a missing %s (binary)', (attr) => {
    const init: Record<string, string> = {
      'ce-specversion': '1.0',
      'ce-id': '1',
      'ce-source': 's',
      'ce-type': 't',
    };
    delete init[`ce-${attr}`];
    // keep binary detection alive when specversion is the missing one
    const h = hdrs(init);
    if (attr === 'specversion') h.set('ce-specversion', '');
    expect(() => readCloudEvent({ data: new Uint8Array(0), headers: h }, jsonCodec())).toThrow(
      NatsDecodeError,
    );
  });

  it.each(['id', 'source', 'type', 'specversion'])('rejects a missing %s (structured)', (attr) => {
    const env: Record<string, string> = { ...fullEnv };
    delete env[attr];
    expect(() => readCloudEvent(structuredMsg(env), jsonCodec())).toThrow(NatsDecodeError);
  });

  it('rejects a non-CloudEvent message', () => {
    expect(() =>
      readCloudEvent({ data: new TextEncoder().encode('{}'), headers: headers() }, jsonCodec()),
    ).toThrow(NatsDecodeError);
    expect(() => readCloudEvent({ data: new Uint8Array(0) }, jsonCodec())).toThrow(NatsDecodeError);
  });

  it('rejects broken structured JSON and non-object envelopes', () => {
    const bad = {
      data: new TextEncoder().encode('{nope'),
      headers: hdrs({ 'Content-Type': CE_STRUCTURED_CONTENT_TYPE }),
    };
    expect(() => readCloudEvent(bad, jsonCodec())).toThrow(NatsDecodeError);
    expect(() => readCloudEvent(structuredMsg([1]), jsonCodec())).toThrow(NatsDecodeError);
  });
});

describe('readCloudEventMeta', () => {
  it('returns meta without decoding data', () => {
    const msg = binaryMsg({ id: '1', source: 's' }, new TextEncoder().encode('{bad'));
    expect(readCloudEventMeta(msg)?.id).toBe('1');
  });
  it('reads structured meta', () => {
    expect(readCloudEventMeta(structuredMsg(fullEnv))?.source).toBe('s');
  });
  it('returns undefined for non-CE or invalid messages', () => {
    expect(readCloudEventMeta({ data: new Uint8Array(0), headers: headers() })).toBeUndefined();
    expect(readCloudEventMeta(binaryMsg({ source: 's' }))).toBeUndefined();
    expect(
      readCloudEventMeta({
        data: new TextEncoder().encode('{x'),
        headers: hdrs({ 'Content-Type': CE_STRUCTURED_CONTENT_TYPE }),
      }),
    ).toBeUndefined();
  });
});

describe('resolveCloudEvent', () => {
  it('fills id, time, specversion and content type', () => {
    expect(
      resolveCloudEvent(
        { type: 't' },
        { source: 's', contentType: 'application/json' },
        { now: () => new Date(0), id: () => 'u1' },
      ),
    ).toEqual({
      id: 'u1',
      source: 's',
      type: 't',
      specversion: '1.0',
      time: '1970-01-01T00:00:00.000Z',
      datacontenttype: 'application/json',
      extensions: {},
    });
  });

  it('prefers init values and converts Date time to ISO', () => {
    const m = resolveCloudEvent(
      {
        type: 't',
        source: 'mine',
        id: 'fixed',
        time: new Date('2024-05-06T07:08:09.000Z'),
        subject: 'sub',
        dataschema: 'urn:x',
        extensions: { a: 'b' },
      },
      { source: 'default', contentType: 'text/plain' },
    );
    expect(m).toMatchObject({
      id: 'fixed',
      source: 'mine',
      time: '2024-05-06T07:08:09.000Z',
      subject: 'sub',
      dataschema: 'urn:x',
      extensions: { a: 'b' },
      datacontenttype: 'text/plain',
    });
  });

  it('generates a random id and current time by default', () => {
    const m = resolveCloudEvent({ type: 't' }, { source: 's', contentType: 'x' });
    expect(m.id).toMatch(/^[0-9a-f-]{36}$/);
    expect(Number.isNaN(Date.parse(m.time as string))).toBe(false);
  });

  it('requires a source', () => {
    expect(() => resolveCloudEvent({ type: 't' }, { contentType: 'x' })).toThrow(NatsConfigError);
    expect(() => resolveCloudEvent({ type: 't' }, { contentType: 'x' })).toThrow(
      'CloudEvent source is required: set cloudEvent.source or NatsModule cloudEvents.source',
    );
  });
});

describe('cloudEventKey', () => {
  it('builds the idempotency key', () => {
    expect(cloudEventKey(binaryMsg({ id: '7', source: 'svc' }))).toBe('svc|7');
  });
  it('returns undefined for non-CloudEvents', () => {
    expect(cloudEventKey({ data: new Uint8Array(0), headers: headers() })).toBeUndefined();
  });
});
