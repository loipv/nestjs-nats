import { describe, expect, it } from 'vitest';
import { NatsConfigError } from '../../../src/errors/errors.js';
import {
  consumerKey,
  mergeConsumers,
  mergeStreams,
  resetsRemovedFields,
} from '../../../src/module/declarations.js';
import type {
  ConsumerDeclaration,
  NatsModuleOptions,
  StreamDeclaration,
} from '../../../src/module/options.js';

const options = (removedFields: unknown) =>
  ({ connection: {}, jetstream: { removedFields } }) as NatsModuleOptions;

/** Declarations as untyped callers may write them (`null` means left out). */
const streams = (...decls: Array<Record<string, unknown>>) => decls as StreamDeclaration[];
const consumer = (stream: string, config: Record<string, unknown>) =>
  ({ stream, config }) as ConsumerDeclaration;

describe('resetsRemovedFields', () => {
  it.each([
    [undefined, false],
    ['ignore', false],
    ['reset', true],
  ])('is %j → %s', (mode, expected) => {
    expect(resetsRemovedFields('orders', options(mode))).toBe(expected);
  });

  it('is false without jetstream options', () => {
    expect(resetsRemovedFields('orders', { connection: {} })).toBe(false);
  });

  it('rejects any other value, naming the connection', () => {
    const check = () => resetsRemovedFields('orders', options('drop'));
    expect(check).toThrow(NatsConfigError);
    expect(check).toThrow(
      `NATS connection "orders" jetstream.removedFields must be 'reset' or 'ignore', got "drop"`,
    );
  });
});

describe('mergeStreams', () => {
  it('keeps a stream declared once as it is', () => {
    const a = { name: 'A', subjects: ['a.>'] };
    const [merged] = mergeStreams(streams(a));
    expect(merged).toBe(a);
  });

  it('merges the declarations of one stream, in the order names first appear', () => {
    expect(
      mergeStreams(
        streams({ name: 'A', subjects: ['a.>'] }, { name: 'B' }, { name: 'A', max_age: 10 }),
      ),
    ).toEqual([{ name: 'A', subjects: ['a.>'], max_age: 10 }, { name: 'B' }]);
  });

  it('accepts a field declared twice with deeply equal values', () => {
    expect(
      mergeStreams(streams({ name: 'A', subjects: ['a.>'] }, { name: 'A', subjects: ['a.>'] })),
    ).toEqual([{ name: 'A', subjects: ['a.>'] }]);
  });

  it('treats null and undefined as left out', () => {
    expect(
      mergeStreams(
        streams({ name: 'A', max_age: null, replicas: undefined }, { name: 'A', max_age: 5 }),
      ),
    ).toEqual([{ name: 'A', max_age: 5 }]);
  });

  it('rejects a field declared with different values, naming the stream and the field', () => {
    const merge = () => mergeStreams(streams({ name: 'A', max_age: 1 }, { name: 'A', max_age: 2 }));
    expect(merge).toThrow(NatsConfigError);
    expect(merge).toThrow('JetStream stream "A" is declared more than once with different max_age');
  });

  it('merges metadata by key', () => {
    expect(
      mergeStreams(
        streams(
          { name: 'A', metadata: { team: 'orders' } },
          { name: 'A', metadata: { tier: 'gold', team: 'orders' } },
        ),
      ),
    ).toEqual([{ name: 'A', metadata: { team: 'orders', tier: 'gold' } }]);
  });

  it('rejects a metadata key declared with different values', () => {
    expect(() =>
      mergeStreams(
        streams({ name: 'A', metadata: { team: 'a' } }, { name: 'A', metadata: { team: 'b' } }),
      ),
    ).toThrow('JetStream stream "A" is declared more than once with different metadata key "team"');
  });
});

describe('mergeConsumers', () => {
  it('merges the declarations of one durable of one stream', () => {
    expect(
      mergeConsumers([
        consumer('S', { durable_name: 'd', ack_wait: 30 }),
        consumer('S', { durable_name: 'd', max_deliver: 5 }),
      ]),
    ).toEqual([{ stream: 'S', config: { durable_name: 'd', ack_wait: 30, max_deliver: 5 } }]);
  });

  it('keeps the same durable name on different streams apart', () => {
    const decls = [
      consumer('S1', { durable_name: 'd', ack_wait: 1 }),
      consumer('S2', { durable_name: 'd', ack_wait: 2 }),
    ];
    expect(mergeConsumers(decls)).toEqual(decls);
  });

  it('rejects a field declared with different values, naming the consumer', () => {
    expect(() =>
      mergeConsumers([
        consumer('S', { durable_name: 'd', ack_wait: 1 }),
        consumer('S', { durable_name: 'd', ack_wait: 2 }),
      ]),
    ).toThrow('JetStream consumer "S/d" is declared more than once with different ack_wait');
  });
});

describe('consumerKey', () => {
  it('identifies a consumer by its stream and durable name', () => {
    const key = (stream: string, durable: string) =>
      consumerKey({ stream, config: { durable_name: durable } });
    expect(key('S', 'd')).toBe(key('S', 'd'));
    expect(key('S', 'd')).not.toBe(key('S2', 'd'));
    // Names containing the separator of a naive "stream/durable" key do not collide.
    expect(key('a/b', 'c')).not.toBe(key('a', 'b/c'));
  });
});
