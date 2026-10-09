import { describe, expect, it } from 'vitest';
import {
  CONSUMER_IMMUTABLE_FIELDS,
  diffConfig,
  STREAM_IMMUTABLE_FIELDS,
  withConsumerDefaults,
} from '../../src/jetstream/stream-sync.js';

describe('stream-sync', () => {
  it('only compares declared keys', () =>
    expect(
      diffConfig(
        { name: 'S', max_msgs: 10, replicas: 1 },
        { name: 'S', max_msgs: 10 },
        STREAM_IMMUTABLE_FIELDS,
      ),
    ).toEqual([]));

  it('flags immutable fields', () =>
    expect(diffConfig({ storage: 'file' }, { storage: 'memory' }, STREAM_IMMUTABLE_FIELDS)).toEqual(
      [{ field: 'storage', current: 'file', declared: 'memory', immutable: true }],
    ));

  it('marks other differing fields as mutable', () =>
    expect(diffConfig({ max_msgs: 10 }, { max_msgs: 20 }, STREAM_IMMUTABLE_FIELDS)).toEqual([
      { field: 'max_msgs', current: 10, declared: 20, immutable: false },
    ]));

  it('compares arrays deeply', () =>
    expect(
      diffConfig({ subjects: ['a'] }, { subjects: ['a', 'b'] }, STREAM_IMMUTABLE_FIELDS)[0].field,
    ).toBe('subjects'));

  it('treats equal arrays and nested objects as equal, array order matters', () => {
    expect(
      diffConfig(
        { subjects: ['a', 'b'], placement: { cluster: 'c' } },
        { subjects: ['a', 'b'], placement: { cluster: 'c' } },
        STREAM_IMMUTABLE_FIELDS,
      ),
    ).toEqual([]);
    expect(
      diffConfig({ subjects: ['a', 'b'] }, { subjects: ['b', 'a'] }, STREAM_IMMUTABLE_FIELDS),
    ).toHaveLength(1);
  });

  it('treats a declared key missing from current as a difference', () =>
    expect(diffConfig({}, { max_age: 5 }, STREAM_IMMUTABLE_FIELDS)).toEqual([
      { field: 'max_age', current: undefined, declared: 5, immutable: false },
    ]));

  it('skips declared keys whose value is undefined', () =>
    expect(diffConfig({ max_age: 5 }, { max_age: undefined }, STREAM_IMMUTABLE_FIELDS)).toEqual(
      [],
    ));

  it('knows consumer immutable fields', () =>
    expect(
      diffConfig({ ack_policy: 'none' }, { ack_policy: 'explicit' }, CONSUMER_IMMUTABLE_FIELDS)[0]
        .immutable,
    ).toBe(true));

  it('defaults consumer to explicit ack and unlimited deliveries', () =>
    expect(withConsumerDefaults({ durable_name: 'd' })).toMatchObject({
      ack_policy: 'explicit',
      max_deliver: -1,
    }));

  it('keeps user max_deliver', () =>
    expect(withConsumerDefaults({ max_deliver: 3 }).max_deliver).toBe(3));
});
