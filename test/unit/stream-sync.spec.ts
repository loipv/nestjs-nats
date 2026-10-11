import { AckPolicy, JetStreamApiCodes, JetStreamApiError } from '@nats-io/jetstream';
import { describe, expect, it, vi } from 'vitest';
import type { ConnectionEntry } from '../../src/connection/connection-registry.js';
import { NatsStreamManager } from '../../src/jetstream/stream-manager.js';
import {
  CONSUMER_IMMUTABLE_FIELDS,
  diffConfig,
  pushConsumerDefaults,
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

  it('builds the push consumer defaults', () => {
    expect(
      pushConsumerDefaults({ stream: 'S', durable: 'D', deliverGroup: 'g', maxAckPending: 2 }),
    ).toStrictEqual({
      // nats.js and the server reject flow control and idle heartbeats with a deliver group;
      // its max_ack_pending is shared by every instance, so the server default applies.
      deliver_subject: '_nats.push.S.D',
      deliver_group: 'g',
    });
    expect(pushConsumerDefaults({ stream: 'S', durable: 'D', maxAckPending: 1 })).toStrictEqual({
      deliver_subject: '_nats.push.S.D',
      flow_control: true,
      idle_heartbeat: 5_000_000_000,
      max_ack_pending: 1,
    });
  });
});

describe('NatsStreamManager.ensureConsumer create defaults', () => {
  const push = pushConsumerDefaults({
    stream: 'S',
    durable: 'D',
    deliverGroup: 'g',
    maxAckPending: 2,
  });

  function manager(current?: { config: Record<string, unknown> }) {
    const consumers = {
      info: vi.fn(() =>
        current
          ? Promise.resolve(current)
          : Promise.reject(
              new JetStreamApiError({
                code: 404,
                err_code: JetStreamApiCodes.ConsumerNotFound,
                description: 'consumer not found',
              }),
            ),
      ),
      add: vi.fn((_stream: string, cfg: unknown) => Promise.resolve({ config: cfg })),
      update: vi.fn(() => Promise.resolve({})),
    };
    const entry = {
      options: {},
      logger: { log: vi.fn(), warn: vi.fn(), error: vi.fn() },
      jsm: () => Promise.resolve({ consumers }),
    } as unknown as ConnectionEntry;
    return { sm: new NatsStreamManager(entry), consumers };
  }

  it('merges them into a created consumer, the declaration winning', async () => {
    const { sm, consumers } = manager();
    await sm.ensureConsumer('S', { durable_name: 'D', max_ack_pending: 7 }, undefined, push);
    expect(consumers.add).toHaveBeenCalledExactlyOnceWith('S', {
      deliver_subject: '_nats.push.S.D',
      deliver_group: 'g',
      max_ack_pending: 7,
      durable_name: 'D',
      ack_policy: AckPolicy.Explicit,
      max_deliver: -1,
    });
  });

  it('ignores them for an existing consumer', async () => {
    const { sm, consumers } = manager({
      config: { durable_name: 'D', ack_policy: AckPolicy.Explicit, deliver_subject: 'other' },
    });
    await sm.ensureConsumer('S', { durable_name: 'D' }, undefined, push);
    expect(consumers.add).not.toHaveBeenCalled();
    expect(consumers.update).not.toHaveBeenCalled();
  });
});

describe('NatsStreamManager: a declared null is an omitted field', () => {
  const notFound = (err_code: number) =>
    Promise.reject(new JetStreamApiError({ code: 404, err_code, description: 'not found' }));

  function manager(current?: { stream?: object; consumer?: object }) {
    const streams = {
      info: vi.fn(() =>
        current?.stream
          ? Promise.resolve({ config: current.stream })
          : notFound(JetStreamApiCodes.StreamNotFound),
      ),
      add: vi.fn((cfg: unknown) => Promise.resolve({ config: cfg })),
      update: vi.fn(() => Promise.resolve({})),
    };
    const consumers = {
      info: vi.fn(() =>
        current?.consumer
          ? Promise.resolve({ config: current.consumer })
          : notFound(JetStreamApiCodes.ConsumerNotFound),
      ),
      add: vi.fn((_stream: string, cfg: unknown) => Promise.resolve({ config: cfg })),
      update: vi.fn(() => Promise.resolve({})),
    };
    const entry = {
      options: {},
      logger: { log: vi.fn(), warn: vi.fn(), error: vi.fn() },
      jsm: () => Promise.resolve({ streams, consumers }),
    } as unknown as ConnectionEntry;
    return { sm: new NatsStreamManager(entry), streams, consumers };
  }

  it('creates a stream without it', async () => {
    const { sm, streams } = manager();
    await sm.ensureStream({ name: 'S', subjects: ['a'], duplicate_window: null } as never);
    expect(streams.add).toHaveBeenCalledExactlyOnceWith({ name: 'S', subjects: ['a'] });
  });

  it('does not count it as stream drift', async () => {
    const { sm, streams } = manager({
      stream: { name: 'S', subjects: ['a'], duplicate_window: 120_000_000_000 },
    });
    const declared = { name: 'S', subjects: ['a'], duplicate_window: null } as never;
    await sm.ensureStream(declared);
    expect(streams.update).not.toHaveBeenCalled();
    await expect(sm.ensureStream(declared, 'verify')).resolves.toBeDefined();
  });

  it('creates a consumer without it', async () => {
    const { sm, consumers } = manager();
    await sm.ensureConsumer('S', { durable_name: 'D', description: null } as never);
    expect(consumers.add).toHaveBeenCalledExactlyOnceWith('S', {
      durable_name: 'D',
      ack_policy: AckPolicy.Explicit,
      max_deliver: -1,
    });
  });

  it('does not count it as consumer drift', async () => {
    const { sm, consumers } = manager({
      consumer: { durable_name: 'D', ack_policy: AckPolicy.Explicit, max_deliver: -1 },
    });
    const declared = { durable_name: 'D', max_deliver: null } as never;
    await sm.ensureConsumer('S', declared);
    expect(consumers.update).not.toHaveBeenCalled();
    await expect(sm.ensureConsumer('S', declared, 'verify')).resolves.toBeDefined();
  });
});
