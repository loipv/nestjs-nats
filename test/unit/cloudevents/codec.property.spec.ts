import { headers as natsHeaders } from '@nats-io/transport-node';
import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import {
  cloudEventKeyOf,
  parseCloudEvent,
  readCloudEvent,
  readCloudEventMeta,
  writeCloudEvent,
} from '../../../src/cloudevents/codec.js';
import type { CloudEventMeta } from '../../../src/cloudevents/types.js';
import { jsonCodec } from '../../../src/codec/codec.js';
import { NatsDecodeError } from '../../../src/errors/errors.js';

const RESERVED = [
  'id',
  'source',
  'type',
  'specversion',
  'time',
  'subject',
  'dataschema',
  'datacontenttype',
  'data',
  'data_base64',
];

/** A value a NATS header can carry: printable ASCII, no CR/LF, not empty. */
const headerValue = fc
  .string({ unit: fc.integer({ min: 0x20, max: 0x7e }).map((c) => String.fromCharCode(c)) })
  .filter((s) => s.trim() === s && s.length > 0);

const extensionName = fc
  .stringMatching(/^[a-z0-9]{1,12}$/)
  .filter((name) => !RESERVED.includes(name));

const meta: fc.Arbitrary<CloudEventMeta> = fc.record(
  {
    id: headerValue,
    source: headerValue,
    type: headerValue,
    specversion: fc.constant('1.0'),
    time: headerValue,
    subject: headerValue,
    dataschema: headerValue,
    datacontenttype: fc.constant('application/json'),
    extensions: fc.dictionary(extensionName, headerValue, { maxKeys: 4 }),
  },
  { requiredKeys: ['id', 'source', 'type', 'specversion', 'extensions'] },
);

/** What `data` is after a trip through JSON (`-0` becomes `0`, and so on). */
const viaJson = (data: unknown) => JSON.parse(JSON.stringify(data)) as unknown;

describe('CloudEvents codec properties', () => {
  it.each(['binary', 'structured'] as const)('reads back what it writes (%s)', (mode) => {
    fc.assert(
      fc.property(meta, fc.jsonValue(), (m, data) => {
        const headers = natsHeaders();
        const body = writeCloudEvent(m, data, jsonCodec(), mode, headers);
        const read = readCloudEvent({ data: body, headers }, jsonCodec());
        expect(read.meta).toEqual(m);
        expect(read.data).toEqual(viaJson(data));
      }),
    );
  });

  it('reads the attributes of anything without throwing', () => {
    fc.assert(
      fc.property(
        fc.dictionary(fc.string({ maxLength: 20 }), headerValue, { maxKeys: 6 }),
        fc.uint8Array({ maxLength: 64 }),
        (attrs, data) => {
          const headers = natsHeaders();
          for (const [key, value] of Object.entries(attrs)) {
            if (/^[A-Za-z0-9-]+$/.test(key)) headers.set(key, value);
          }
          const result = readCloudEventMeta({ data, headers });
          if (result !== undefined) {
            for (const attr of ['id', 'source', 'type', 'specversion'] as const) {
              expect(result[attr]).toBeTruthy();
            }
          }
        },
      ),
    );
  });

  it('fails on malformed input only with a NatsDecodeError', () => {
    const structured = fc.oneof(
      fc.uint8Array({ maxLength: 64 }),
      fc.jsonValue().map((v) => new TextEncoder().encode(JSON.stringify(v))),
    );
    fc.assert(
      fc.property(structured, fc.boolean(), (data, decode) => {
        const headers = natsHeaders();
        headers.set('Content-Type', 'application/cloudevents+json');
        try {
          const parsed = parseCloudEvent({ data, headers });
          if (decode) parsed.data(jsonCodec());
        } catch (err) {
          expect(err).toBeInstanceOf(NatsDecodeError);
        }
      }),
    );
  });

  it('keeps a | in the source from shifting the separator', () => {
    fc.assert(
      fc.property(fc.string(), fc.string(), fc.string(), (x, y, z) => {
        expect(cloudEventKeyOf({ source: `${x}|${y}`, id: z })).not.toBe(
          cloudEventKeyOf({ source: x, id: `${y}|${z}` }),
        );
      }),
    );
  });

  it('gives different (source, id) pairs different keys', () => {
    const piped = fc.string({ unit: fc.constantFrom('a', 'b', '|', '%'), maxLength: 4 });
    fc.assert(
      // A small alphabet with `|`, so pairs that a naive `source|id` would merge come up.
      fc.property(piped, piped, piped, piped, (s1, i1, s2, i2) => {
        fc.pre(s1 !== s2 || i1 !== i2);
        expect(cloudEventKeyOf({ source: s1, id: i1 })).not.toBe(
          cloudEventKeyOf({ source: s2, id: i2 }),
        );
      }),
    );
  });
});
