import { describe, expect, it } from 'vitest';
import { decodePayload, encodePayload, jsonCodec } from '../../../src/codec/codec.js';
import { NatsDecodeError } from '../../../src/errors/errors.js';

describe('codec', () => {
  it('roundtrips JSON', () => {
    const c = jsonCodec();
    expect(decodePayload(c, encodePayload(c, { a: 1 }))).toEqual({ a: 1 });
  });

  it('exposes the JSON content type', () => {
    expect(jsonCodec().contentType).toBe('application/json');
  });

  it('passes Uint8Array through untouched', () => {
    const b = new Uint8Array([1, 2]);
    expect(encodePayload(jsonCodec(), b)).toBe(b);
  });

  it('encodes undefined as empty and decodes empty as undefined', () => {
    expect(encodePayload(jsonCodec(), undefined).length).toBe(0);
    expect(decodePayload(jsonCodec(), new Uint8Array(0))).toBeUndefined();
  });

  it.each([
    ['a function', () => 1],
    ['a symbol', Symbol('s')],
    ['a toJSON returning undefined', { toJSON: () => undefined }],
  ])('rejects %s instead of sending an empty payload', (_, value) => {
    expect(() => encodePayload(jsonCodec(), value)).toThrow(TypeError);
    expect(() => encodePayload(jsonCodec(), value)).toThrow(/not JSON-serializable/);
  });

  it('encodes undefined passed straight to jsonCodec().encode as empty', () => {
    expect(jsonCodec().encode(undefined).length).toBe(0);
  });

  it('wraps decode failures in NatsDecodeError', () => {
    const bad = new TextEncoder().encode('hello');
    expect(() => decodePayload(jsonCodec(), bad)).toThrow(NatsDecodeError);
  });

  it('keeps the original error as cause on decode failure', () => {
    const bad = new TextEncoder().encode('hello');
    try {
      decodePayload(jsonCodec(), bad);
      expect.unreachable();
    } catch (e) {
      expect((e as NatsDecodeError).cause).toBeInstanceOf(SyntaxError);
    }
  });
});
