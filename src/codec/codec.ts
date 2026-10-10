import type { MsgHdrs } from '@nats-io/transport-node';
import { NatsDecodeError } from '../errors/errors.js';

export interface Codec {
  readonly contentType: string;
  encode(data: unknown): Uint8Array;
  decode(bytes: Uint8Array, headers?: MsgHdrs): unknown;
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * The default codec: JSON. `undefined` encodes as an empty payload; a value
 * `JSON.stringify` cannot represent at the top level (a function, a symbol, a `toJSON`
 * returning `undefined`) throws a `TypeError`, as circular values and BigInts already do,
 * instead of silently sending an empty payload.
 */
export function jsonCodec(): Codec {
  return {
    contentType: 'application/json',
    encode: (data) => {
      if (data === undefined) return new Uint8Array(0);
      const json = JSON.stringify(data);
      if (json === undefined) {
        throw new TypeError(`jsonCodec: value is not JSON-serializable (${typeof data})`);
      }
      return textEncoder.encode(json);
    },
    decode: (bytes) => JSON.parse(textDecoder.decode(bytes)),
  };
}

export function encodePayload(codec: Codec, data: unknown): Uint8Array {
  if (data instanceof Uint8Array) return data;
  if (data === undefined) return new Uint8Array(0);
  return codec.encode(data);
}

export function decodePayload(codec: Codec, bytes: Uint8Array, headers?: MsgHdrs): unknown {
  if (bytes.length === 0) return undefined;
  try {
    return codec.decode(bytes, headers);
  } catch (cause) {
    throw new NatsDecodeError(`Failed to decode payload with ${codec.contentType} codec`, {
      cause,
    });
  }
}
