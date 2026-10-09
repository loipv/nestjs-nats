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

  it('ignores server-injected _nats. metadata keys', () =>
    expect(
      diffConfig(
        {
          metadata: {
            '_nats.level': '3',
            '_nats.req.level': '0',
            '_nats.ver': '2.12.15',
            team: 'x',
          },
        },
        { metadata: { team: 'x' } },
        STREAM_IMMUTABLE_FIELDS,
      ),
    ).toEqual([]));

  it('reports metadata drift without the _nats. keys', () =>
    expect(
      diffConfig(
        { metadata: { '_nats.ver': '2.12.15', team: 'x' } },
        { metadata: { team: 'y' } },
        STREAM_IMMUTABLE_FIELDS,
      ),
    ).toEqual([
      { field: 'metadata', current: { team: 'x' }, declared: { team: 'y' }, immutable: false },
    ]));

  it('treats a declared empty metadata as equal to server-only metadata', () =>
    expect(
      diffConfig(
        { metadata: { '_nats.ver': '2.12.15' } },
        { metadata: {} },
        STREAM_IMMUTABLE_FIELDS,
      ),
    ).toEqual([]));

  it.each([
    ['false', false],
    ['0', 0],
    ["''", ''],
    ['[]', []],
    ['{}', {}],
  ])('treats an absent current value as equal to declared %s', (_, value) =>
    expect(diffConfig({}, { field: value }, CONSUMER_IMMUTABLE_FIELDS)).toEqual([]),
  );

  it('still flags a declared true against an absent current value', () =>
    expect(diffConfig({}, { headers_only: true }, CONSUMER_IMMUTABLE_FIELDS)).toEqual([
      { field: 'headers_only', current: undefined, declared: true, immutable: false },
    ]));

  it('compares nested objects as a subset of current, recursively', () => {
    expect(
      diffConfig(
        { placement: { cluster: 'c', tags: ['a'], extra: 1 }, consumer_limits: {} },
        { placement: { cluster: 'c' }, consumer_limits: {} },
        STREAM_IMMUTABLE_FIELDS,
      ),
    ).toEqual([]);
    expect(
      diffConfig(
        { republish: { src: 'a', dest: 'b' } },
        { republish: { src: 'a', dest: 'b', headers_only: false } },
        STREAM_IMMUTABLE_FIELDS,
      ),
    ).toEqual([]);
    expect(
      diffConfig(
        { placement: { cluster: 'c' } },
        { placement: { cluster: 'd' } },
        STREAM_IMMUTABLE_FIELDS,
      ),
    ).toHaveLength(1);
  });

  it('compares objects inside arrays as subsets', () =>
    expect(
      diffConfig(
        { sources: [{ name: 'A', opt_start_seq: 0, filter_subject: 'x' }] },
        { sources: [{ name: 'A' }] },
        STREAM_IMMUTABLE_FIELDS,
      ),
    ).toEqual([]));
});
