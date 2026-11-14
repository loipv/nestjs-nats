import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import {
  diffConfig,
  STREAM_IMMUTABLE_FIELDS,
  withoutNulls,
} from '../../../src/jetstream/stream-sync.js';

/** A JSON value as the server would hold it: what `JSON.parse` gives (no `-0`, no `undefined`). */
const json = fc.jsonValue().map((v) => JSON.parse(JSON.stringify(v)) as unknown);

const fieldName = fc
  .stringMatching(/^[a-z][a-z_]{0,15}$/)
  .filter((f) => f !== 'metadata' && f !== '__proto__' && f !== 'constructor');

/** A declaration: top-level fields with JSON values, and string metadata. */
const declaration = fc.record(
  {
    fields: fc.dictionary(fieldName, json, { maxKeys: 6 }),
    metadata: fc.dictionary(fc.stringMatching(/^[a-z][a-z.]{0,10}$/), fc.string(), {
      maxKeys: 3,
    }),
  },
  { requiredKeys: ['fields'] },
);

const toConfig = (d: { fields: Record<string, unknown>; metadata?: Record<string, string> }) =>
  d.metadata ? { ...d.fields, metadata: d.metadata } : { ...d.fields };

const isZero = (v: unknown) =>
  v === false ||
  v === 0 ||
  v === '' ||
  (Array.isArray(v) && v.length === 0) ||
  (typeof v === 'object' && v !== null && !Array.isArray(v) && Object.keys(v).length === 0);

describe('stream-sync properties', () => {
  it('finds no drift when the server holds every declared field, plus its own', () => {
    fc.assert(
      fc.property(declaration, fc.dictionary(fieldName, json, { maxKeys: 4 }), (d, extra) => {
        const declared = toConfig(d);
        const current = {
          ...extra,
          ...declared,
          metadata: { ...d.metadata, '_nats.level': '1', '_nats.ver': '2.12.0', other: 'x' },
        };
        expect(diffConfig(current, declared, STREAM_IMMUTABLE_FIELDS)).toEqual([]);
      }),
    );
  });

  it('finds no drift for declared zero values the server leaves out', () => {
    fc.assert(
      fc.property(declaration, (d) => {
        const declared = toConfig(d);
        const current = Object.fromEntries(
          Object.entries(declared).filter(([k, v]) => k === 'metadata' || !isZero(v)),
        );
        expect(diffConfig(current, declared, STREAM_IMMUTABLE_FIELDS)).toEqual([]);
      }),
    );
  });

  it('reports exactly the declared field the server holds differently', () => {
    const changed = fc.oneof(fc.constantFrom('storage', 'retention'), fieldName);
    fc.assert(
      fc.property(
        declaration,
        changed,
        fc.integer({ min: 1, max: 1_000_000 }),
        (d, field, value) => {
          const declared = { ...toConfig(d), [field]: value };
          const current = { ...declared, [field]: value + 1 };
          expect(diffConfig(current, declared, STREAM_IMMUTABLE_FIELDS)).toEqual([
            {
              field,
              current: value + 1,
              declared: value,
              immutable: (STREAM_IMMUTABLE_FIELDS as readonly string[]).includes(field),
            },
          ]);
        },
      ),
    );
  });

  it("reports drifted metadata without the server's own _nats. keys", () => {
    const key = fc.stringMatching(/^[a-z]{1,8}$/);
    fc.assert(
      fc.property(key, fc.string(), fc.string(), (k, declaredValue, currentValue) => {
        fc.pre(declaredValue !== currentValue);
        const declared = { metadata: { [k]: declaredValue } };
        const current = { metadata: { [k]: currentValue, '_nats.level': '1' } };
        expect(diffConfig(current, declared, STREAM_IMMUTABLE_FIELDS)).toEqual([
          {
            field: 'metadata',
            current: { [k]: currentValue },
            declared: { [k]: declaredValue },
            immutable: false,
          },
        ]);
      }),
    );
  });

  it('withoutNulls drops the null fields only', () => {
    fc.assert(
      fc.property(fc.dictionary(fieldName, fc.oneof(json, fc.constant(null))), (declared) => {
        const out = withoutNulls(declared);
        for (const [key, value] of Object.entries(declared)) {
          if (value === null) expect(out).not.toHaveProperty(key);
          else expect(out[key]).toBe(value);
        }
        expect(Object.values(out)).not.toContain(null);
      }),
    );
  });
});
