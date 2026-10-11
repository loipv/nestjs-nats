import { AckPolicy, JetStreamApiCodes, JetStreamApiError } from '@nats-io/jetstream';
import { describe, expect, it, vi } from 'vitest';
import type { ConnectionEntry } from '../../src/connection/connection-registry.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import { NatsStreamManager } from '../../src/jetstream/stream-manager.js';
import {
  CONSUMER_IMMUTABLE_FIELDS,
  CONSUMER_KEPT_FIELDS,
  declaredFields,
  diffConfig,
  parseSnapshot,
  pushConsumerDefaults,
  removedFields,
  SNAPSHOT_KEY,
  STREAM_IMMUTABLE_FIELDS,
  STREAM_KEPT_FIELDS,
  snapshotOf,
  syncMetadata,
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
      update: vi.fn((_stream: string, _durable: string, cfg: unknown) =>
        Promise.resolve({ config: cfg }),
      ),
    };
    const entry = {
      name: 'default',
      nc: { info: undefined },
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
      config: {
        durable_name: 'D',
        ack_policy: AckPolicy.Explicit,
        deliver_subject: 'other',
      },
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
      update: vi.fn((_name: string, cfg: unknown) => Promise.resolve({ config: cfg })),
    };
    const consumers = {
      info: vi.fn(() =>
        current?.consumer
          ? Promise.resolve({ config: current.consumer })
          : notFound(JetStreamApiCodes.ConsumerNotFound),
      ),
      add: vi.fn((_stream: string, cfg: unknown) => Promise.resolve({ config: cfg })),
      update: vi.fn((_stream: string, _durable: string, cfg: unknown) =>
        Promise.resolve({ config: cfg }),
      ),
    };
    const entry = {
      name: 'default',
      nc: { info: undefined },
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
      stream: {
        name: 'S',
        subjects: ['a'],
        duplicate_window: 120_000_000_000,
      },
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
      consumer: {
        durable_name: 'D',
        ack_policy: AckPolicy.Explicit,
        max_deliver: -1,
      },
    });
    const declared = { durable_name: 'D', max_deliver: null } as never;
    await sm.ensureConsumer('S', declared);
    expect(consumers.update).not.toHaveBeenCalled();
    await expect(sm.ensureConsumer('S', declared, 'verify')).resolves.toBeDefined();
  });

  describe('snapshot helpers', () => {
    it('declaredFields lists sorted defined fields without metadata or excluded ones', () =>
      expect(
        declaredFields(
          {
            name: 'S',
            subjects: ['a'],
            max_age: 5,
            metadata: { x: '1' },
            description: undefined,
            max_msgs: null as never,
          },
          ['name'],
        ),
      ).toEqual(['max_age', 'subjects']));

    it('snapshotOf joins the names', () => {
      expect(snapshotOf(['a', 'b'])).toBe('a,b');
      expect(snapshotOf([])).toBe('');
    });

    it('parseSnapshot reads the metadata key', () => {
      expect(parseSnapshot(undefined)).toBeUndefined();
      expect(parseSnapshot({})).toBeUndefined();
      expect(parseSnapshot({ [SNAPSHOT_KEY]: '' })).toEqual([]);
      expect(parseSnapshot({ [SNAPSHOT_KEY]: 'a,b' })).toEqual(['a', 'b']);
    });

    it('removedFields splits reset and kept, skipping fields absent on the server', () =>
      expect(
        removedFields(
          { max_age: 5, storage: 'file', subjects: ['a'] },
          ['max_age', 'storage', 'subjects', 'description'],
          ['subjects'],
          STREAM_IMMUTABLE_FIELDS,
        ),
      ).toEqual({ reset: ['max_age'], kept: ['storage'] }));

    it('removedFields is empty without a previous snapshot', () =>
      expect(removedFields({ max_age: 5 }, undefined, [], STREAM_IMMUTABLE_FIELDS)).toEqual({
        reset: [],
        kept: [],
      }));

    it('lists the fields the server refuses to reset', () => {
      expect(STREAM_KEPT_FIELDS).toEqual([
        ...STREAM_IMMUTABLE_FIELDS,
        'deny_delete',
        'deny_purge',
        'allow_msg_ttl',
        'sealed',
        'mirror',
        'allow_msg_counter',
        'allow_msg_schedules',
      ]);
      expect(CONSUMER_KEPT_FIELDS).toEqual([
        ...CONSUMER_IMMUTABLE_FIELDS,
        'deliver_subject',
        'flow_control',
        'idle_heartbeat',
        'max_waiting',
      ]);
    });

    it('syncMetadata replaces declared metadata and keeps current otherwise', () => {
      const s = 'a,b';
      const current = { a: '0', b: '2', '_nats.ver': 'x' };
      expect(syncMetadata({ a: '1' }, current, s)).toEqual({
        a: '1',
        [SNAPSHOT_KEY]: s,
      });
      expect(syncMetadata(undefined, current, s)).toEqual({
        a: '0',
        b: '2',
        [SNAPSHOT_KEY]: s,
      });
      expect(syncMetadata(undefined, undefined, s)).toEqual({
        [SNAPSHOT_KEY]: s,
      });
    });

    it('diffConfig ignores the snapshot key and compares declared metadata exactly', () => {
      expect(
        diffConfig(
          { metadata: { '_nats.ver': 'x', [SNAPSHOT_KEY]: 'a', team: 'x' } },
          { metadata: { team: 'x' } },
          STREAM_IMMUTABLE_FIELDS,
          true,
        ),
      ).toEqual([]);
      expect(
        diffConfig(
          {
            metadata: {
              '_nats.ver': 'x',
              [SNAPSHOT_KEY]: 'a',
              team: 'x',
              old: '1',
            },
          },
          { metadata: { team: 'x' } },
          STREAM_IMMUTABLE_FIELDS,
          true,
        ),
      ).toEqual([
        {
          field: 'metadata',
          current: { team: 'x', old: '1' },
          declared: { team: 'x' },
          immutable: false,
        },
      ]);
    });

    it('diffConfig compares declared metadata as a subset unless told to compare it exactly', () =>
      expect(
        diffConfig(
          { metadata: { '_nats.ver': 'x', [SNAPSHOT_KEY]: 'a', team: 'x', old: '1' } },
          { metadata: { team: 'x' } },
          STREAM_IMMUTABLE_FIELDS,
        ),
      ).toEqual([]));
  });
});

describe('NatsStreamManager: fields removed from a declaration', () => {
  type Config = Record<string, unknown>;

  /**
   * A fake `jsm` that keeps the configs it is given. `add` and `update` merge the sent config
   * like nats.js and the server do (an `undefined` field is dropped) and echo the result back;
   * with `keepsMetadata: false`, the server drops `metadata` like one older than 2.10. The
   * connection opts in with `removedFields: 'reset'` unless told otherwise.
   */
  function manager(
    init: { stream?: Config; consumer?: Config } = {},
    {
      keepsMetadata = true,
      version,
      removedFields = 'reset',
    }: {
      keepsMetadata?: boolean;
      version?: string;
      /** `'default'`: the option is not set. */
      removedFields?: 'reset' | 'ignore' | 'default';
    } = {},
  ) {
    const state: { stream?: Config; consumer?: Config } = { ...init };
    const notFound = (err_code: number) =>
      Promise.reject(new JetStreamApiError({ code: 404, err_code, description: 'not found' }));
    const store = (cfg: Config): Config => {
      const kept = JSON.parse(JSON.stringify(cfg)) as Config;
      if (!keepsMetadata) delete kept.metadata;
      return kept;
    };
    const streams = {
      info: vi.fn(() =>
        state.stream
          ? Promise.resolve({ config: state.stream })
          : notFound(JetStreamApiCodes.StreamNotFound),
      ),
      add: vi.fn((cfg: Config) => {
        state.stream = store(cfg);
        return Promise.resolve({ config: state.stream });
      }),
      update: vi.fn((_name: string, cfg: Config) => {
        state.stream = store({ ...state.stream, ...cfg });
        return Promise.resolve({ config: state.stream });
      }),
    };
    const consumers = {
      info: vi.fn(() =>
        state.consumer
          ? Promise.resolve({ config: state.consumer })
          : notFound(JetStreamApiCodes.ConsumerNotFound),
      ),
      add: vi.fn((_stream: string, cfg: Config) => {
        state.consumer = store(cfg);
        return Promise.resolve({ config: state.consumer });
      }),
      update: vi.fn((_stream: string, _durable: string, cfg: Config) => {
        state.consumer = store({ ...state.consumer, ...cfg });
        return Promise.resolve({ config: state.consumer });
      }),
    };
    const logger = { log: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const entry = {
      name: 'main',
      nc: { info: version === undefined ? undefined : { version } },
      options: removedFields === 'default' ? {} : { jetstream: { removedFields } },
      logger,
      jsm: () => Promise.resolve({ streams, consumers }),
    } as unknown as ConnectionEntry;
    return { sm: new NatsStreamManager(entry), entry, streams, consumers, logger, state };
  }

  const snap = (fields: string) => ({ [SNAPSHOT_KEY]: fields });
  const updateOf = (mock: { mock: { calls: unknown[][] } }): Config =>
    mock.mock.calls[0]?.at(-1) as Config;

  describe('streams', () => {
    it('creates a stream with the snapshot in its metadata', async () => {
      const { sm, streams } = manager();
      await sm.ensureStream({ name: 'S', subjects: ['a'], max_age: 5, metadata: { team: 'x' } });
      expect(streams.add).toHaveBeenCalledExactlyOnceWith({
        name: 'S',
        subjects: ['a'],
        max_age: 5,
        metadata: { team: 'x', ...snap('max_age,subjects') },
      });
    });

    it('only writes the snapshot to a stream that has none', async () => {
      const { sm, streams, logger } = manager({
        stream: { name: 'S', subjects: ['a'], max_age: 5 },
      });
      await sm.ensureStream({ name: 'S', subjects: ['a'] });
      expect(streams.update).toHaveBeenCalledExactlyOnceWith('S', {
        subjects: ['a'],
        metadata: snap('subjects'),
      });
      expect(Object.values(updateOf(streams.update))).not.toContain(undefined);
      expect(logger.log).not.toHaveBeenCalled();
    });

    it('resets a field removed from the declaration', async () => {
      const { sm, streams, logger, state } = manager({
        stream: { name: 'S', subjects: ['a'], max_age: 5, metadata: snap('max_age,subjects') },
      });
      await sm.ensureStream({ name: 'S', subjects: ['a'] });
      const sent = updateOf(streams.update);
      expect(sent).toHaveProperty('max_age');
      expect(sent.max_age).toBeUndefined();
      expect(sent.metadata).toEqual(snap('subjects'));
      expect(logger.log).toHaveBeenCalledExactlyOnceWith(
        'JetStream stream "S": reset max_age to the server default (removed from the declaration)',
      );
      expect(state.stream).not.toHaveProperty('max_age');
    });

    it.each([
      ['null', null],
      ['undefined', undefined],
    ])('resets a field previously declared with a value, now declared %s', async (_, value) => {
      const { sm, streams, logger } = manager({
        stream: { name: 'S', subjects: ['a'], max_age: 5, metadata: snap('max_age,subjects') },
      });
      await sm.ensureStream({ name: 'S', subjects: ['a'], max_age: value as never });
      const sent = updateOf(streams.update);
      expect(sent).toHaveProperty('max_age');
      expect(sent.max_age).toBeUndefined();
      expect(logger.log).toHaveBeenCalledExactlyOnceWith(
        'JetStream stream "S": reset max_age to the server default (removed from the declaration)',
      );
    });

    it('warns and keeps a removed immutable field', async () => {
      const { sm, streams, logger } = manager({
        stream: {
          name: 'S',
          subjects: ['a'],
          storage: 'file',
          metadata: snap('storage,subjects'),
        },
      });
      await sm.ensureStream({ name: 'S', subjects: ['a'] });
      expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
        'JetStream stream "S" keeps storage "file": it cannot be changed (removed from the declaration)',
      );
      expect(updateOf(streams.update)).not.toHaveProperty('storage');
      expect(logger.log).not.toHaveBeenCalled();
    });

    it.each([
      ['deny_delete', true],
      ['deny_purge', true],
      ['allow_msg_ttl', true],
      ['sealed', true],
      ['mirror', { name: 'M' }],
      ['allow_msg_counter', true],
      ['allow_msg_schedules', true],
    ])('warns and keeps a removed %s, which the server cannot reset', async (field, value) => {
      const { sm, streams, logger } = manager({
        stream: { name: 'S', subjects: ['a'], [field]: value, metadata: snap(`${field},subjects`) },
      });
      await sm.ensureStream({ name: 'S', subjects: ['a'] });
      expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
        `JetStream stream "S" keeps ${field} ${JSON.stringify(value)}: it cannot be changed (removed from the declaration)`,
      );
      expect(updateOf(streams.update)).not.toHaveProperty(field);
      expect(logger.log).not.toHaveBeenCalled();
    });

    it('logs no reset when the update is rejected', async () => {
      const { sm, streams, logger } = manager({
        stream: { name: 'S', subjects: ['a'], max_age: 5, metadata: snap('max_age,subjects') },
      });
      streams.update.mockRejectedValueOnce(new Error('rejected'));
      await expect(sm.ensureStream({ name: 'S', subjects: ['a'] })).rejects.toThrow('rejected');
      expect(logger.log).not.toHaveBeenCalled();
    });

    it('only updates a stale snapshot when a field was added', async () => {
      const { sm, streams, logger } = manager({
        stream: { name: 'S', subjects: ['a'], max_age: 5, metadata: snap('subjects') },
      });
      await sm.ensureStream({ name: 'S', subjects: ['a'], max_age: 5 });
      expect(streams.update).toHaveBeenCalledExactlyOnceWith('S', {
        subjects: ['a'],
        max_age: 5,
        metadata: snap('max_age,subjects'),
      });
      expect(logger.log).not.toHaveBeenCalled();
      expect(logger.warn).not.toHaveBeenCalled();
    });

    it.each(['_nats.level', SNAPSHOT_KEY])(
      'rejects a declared metadata key %s before contacting the server',
      async (key) => {
        const { sm, streams } = manager();
        await expect(
          sm.ensureStream({ name: 'S', subjects: ['a'], metadata: { [key]: '1' } }),
        ).rejects.toThrow(
          new NatsConfigError(
            `JetStream stream "S" declares metadata key "${key}", which the library or the server sets: declare no "_nats." key nor "${SNAPSHOT_KEY}"`,
          ),
        );
        expect(streams.info).not.toHaveBeenCalled();
      },
    );

    it('does not update when nothing changed', async () => {
      const { sm, streams } = manager();
      const declared = { name: 'S', subjects: ['a'], max_age: 5 };
      await sm.ensureStream(declared);
      await sm.ensureStream(declared);
      expect(streams.add).toHaveBeenCalledOnce();
      expect(streams.update).not.toHaveBeenCalled();
    });

    it('keeps the metadata a stream was once declared with when it no longer is', async () => {
      const { sm, streams, state } = manager();
      await sm.ensureStream({ name: 'S', subjects: ['a'], metadata: { team: 'x' } });
      await sm.ensureStream({ name: 'S', subjects: ['a'] });
      expect(streams.update).not.toHaveBeenCalled();
      expect(state.stream?.metadata).toEqual({ team: 'x', ...snap('subjects') });
    });

    it("keeps another tool's metadata when none is declared", async () => {
      const { sm, streams } = manager({
        stream: {
          name: 'S',
          subjects: ['a'],
          metadata: { 'other.tool': 'x', '_nats.level': '1' },
        },
      });
      await sm.ensureStream({ name: 'S', subjects: ['a'] });
      expect(updateOf(streams.update).metadata).toEqual({
        'other.tool': 'x',
        ...snap('subjects'),
      });
    });

    it('reports a removed field in verify', async () => {
      const { sm, streams } = manager({
        stream: {
          name: 'S',
          subjects: ['a'],
          max_age: 5,
          storage: 'file',
          metadata: snap('max_age,storage,subjects'),
        },
      });
      await expect(sm.ensureStream({ name: 'S', subjects: ['b'] }, 'verify')).rejects.toThrow(
        'JetStream stream "S" differs from its declaration: subjects (current: ["a"], declared: ["b"]), max_age (removed from the declaration, current: 5), storage (removed from the declaration, current: "file")',
      );
      expect(streams.update).not.toHaveBeenCalled();
    });

    it('turns the feature off when the server does not keep metadata', async () => {
      const { sm, entry, streams, logger } = manager(
        { stream: { name: 'S', subjects: ['a'], max_age: 5 } },
        { keepsMetadata: false },
      );
      const declared = { name: 'S', subjects: ['a'] };
      await sm.ensureStream(declared);
      expect(streams.update).toHaveBeenCalledOnce();
      expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
        'JetStream on connection "main" does not keep stream/consumer metadata (NATS server 2.10 or later is needed); removed fields are not detected',
      );
      // Another manager of the same connection shares the flag.
      await new NatsStreamManager(entry).ensureStream(declared);
      expect(streams.update).toHaveBeenCalledOnce();
      expect(logger.warn).toHaveBeenCalledOnce();
    });

    it('skips the snapshot on a server older than 2.10', async () => {
      const { sm, entry, streams, consumers, logger } = manager({}, { version: '2.9.25' });
      const declared = { name: 'S', subjects: ['a'] };
      await sm.ensureStream(declared);
      expect(streams.add).toHaveBeenCalledExactlyOnceWith({ name: 'S', subjects: ['a'] });
      await new NatsStreamManager(entry).ensureStream(declared);
      expect(streams.update).not.toHaveBeenCalled();
      await sm.ensureConsumer('S', { durable_name: 'D' });
      expect(consumers.add).toHaveBeenCalledExactlyOnceWith('S', {
        durable_name: 'D',
        ack_policy: AckPolicy.Explicit,
        max_deliver: -1,
      });
      expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
        'JetStream on connection "main" does not keep stream/consumer metadata (NATS server 2.10 or later is needed); removed fields are not detected',
      );
    });

    it('makes no snapshot-only update to an existing stream on a server older than 2.10', async () => {
      const { sm, streams } = manager(
        { stream: { name: 'S', subjects: ['a'], max_age: 5 } },
        { version: '2.9.0' },
      );
      await sm.ensureStream({ name: 'S', subjects: ['a'] });
      expect(streams.update).not.toHaveBeenCalled();
    });

    it.each(['2.10.0', '2.10.0-beta.2', '2.11.4', '3.0.0'])(
      'writes the snapshot on server %s',
      async (version) => {
        const { sm, streams, logger } = manager({}, { version });
        await sm.ensureStream({ name: 'S', subjects: ['a'] });
        expect(streams.add).toHaveBeenCalledExactlyOnceWith({
          name: 'S',
          subjects: ['a'],
          metadata: snap('subjects'),
        });
        expect(logger.warn).not.toHaveBeenCalled();
      },
    );
  });

  describe('consumers', () => {
    it('creates a consumer with the snapshot in its metadata', async () => {
      const { sm, consumers } = manager();
      await sm.ensureConsumer('S', { durable_name: 'D', max_ack_pending: 3 });
      expect(consumers.add).toHaveBeenCalledExactlyOnceWith('S', {
        durable_name: 'D',
        max_ack_pending: 3,
        ack_policy: AckPolicy.Explicit,
        max_deliver: -1,
        metadata: snap('max_ack_pending'),
      });
    });

    it('resets a field removed from the declaration', async () => {
      const { sm, consumers, logger } = manager({
        consumer: {
          durable_name: 'D',
          ack_policy: AckPolicy.Explicit,
          max_ack_pending: 3,
          metadata: snap('max_ack_pending'),
        },
      });
      await sm.ensureConsumer('S', { durable_name: 'D' });
      const sent = updateOf(consumers.update);
      expect(consumers.update.mock.calls[0]?.slice(0, 2)).toEqual(['S', 'D']);
      expect(sent).toHaveProperty('max_ack_pending');
      expect(sent.max_ack_pending).toBeUndefined();
      expect(sent.metadata).toEqual(snap(''));
      expect(logger.log).toHaveBeenCalledExactlyOnceWith(
        'JetStream consumer "S/D": reset max_ack_pending to the server default (removed from the declaration)',
      );
    });

    it('warns and keeps a removed immutable field', async () => {
      const { sm, consumers, logger } = manager({
        consumer: {
          durable_name: 'D',
          ack_policy: AckPolicy.Explicit,
          deliver_policy: 'new',
          metadata: snap('deliver_policy'),
        },
      });
      await sm.ensureConsumer('S', { durable_name: 'D' });
      expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
        'JetStream consumer "S/D" keeps deliver_policy "new": it cannot be changed (removed from the declaration)',
      );
      expect(updateOf(consumers.update)).not.toHaveProperty('deliver_policy');
    });

    it.each([
      ['deliver_subject', 'push.S'],
      ['flow_control', true],
      ['idle_heartbeat', 5_000_000_000],
      ['max_waiting', 100],
    ])('warns and keeps a removed %s, which the server cannot reset', async (field, value) => {
      const { sm, consumers, logger } = manager({
        consumer: {
          durable_name: 'D',
          ack_policy: AckPolicy.Explicit,
          [field]: value,
          metadata: snap(field),
        },
      });
      await sm.ensureConsumer('S', { durable_name: 'D' });
      expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
        `JetStream consumer "S/D" keeps ${field} ${JSON.stringify(value)}: it cannot be changed (removed from the declaration)`,
      );
      expect(updateOf(consumers.update)).not.toHaveProperty(field);
    });

    it('leaves the consumer name out of the snapshot', async () => {
      const { sm, consumers } = manager();
      await sm.ensureConsumer('S', { durable_name: 'D', name: 'D', max_ack_pending: 3 });
      expect(updateOf(consumers.add).metadata).toEqual(snap('max_ack_pending'));
    });

    it('logs no reset when the update is rejected', async () => {
      const { sm, consumers, logger } = manager({
        consumer: {
          durable_name: 'D',
          ack_policy: AckPolicy.Explicit,
          max_ack_pending: 3,
          metadata: snap('max_ack_pending'),
        },
      });
      consumers.update.mockRejectedValueOnce(new Error('rejected'));
      await expect(sm.ensureConsumer('S', { durable_name: 'D' })).rejects.toThrow('rejected');
      expect(logger.log).not.toHaveBeenCalled();
    });

    it('rejects a declared reserved metadata key', async () => {
      const { sm, consumers } = manager();
      await expect(
        sm.ensureConsumer('S', { durable_name: 'D', metadata: { '_nats.x': '1' } }),
      ).rejects.toThrow(
        'JetStream consumer "S/D" declares metadata key "_nats.x", which the library or the server sets',
      );
      expect(consumers.info).not.toHaveBeenCalled();
    });

    it('sends the library default of a removed push field, unless the server has it', async () => {
      const { sm, consumers, logger, state } = manager({
        consumer: {
          durable_name: 'D',
          ack_policy: AckPolicy.Explicit,
          deliver_subject: '_nats.push.S.D',
          flow_control: true,
          idle_heartbeat: 5_000_000_000,
          max_ack_pending: 7,
          metadata: snap('max_ack_pending'),
        },
      });
      const push = pushConsumerDefaults({ stream: 'S', durable: 'D', maxAckPending: 4 });
      await sm.ensureConsumer('S', { durable_name: 'D' }, undefined, push);
      expect(updateOf(consumers.update).max_ack_pending).toBe(4);
      expect(state.consumer?.max_ack_pending).toBe(4);
      expect(logger.log).toHaveBeenCalledExactlyOnceWith(
        'JetStream consumer "S/D": reset max_ack_pending to the library default (removed from the declaration)',
      );
      expect(logger.warn).not.toHaveBeenCalled();
    });

    it('keeps deliver_group moved from config to the deliverGroup option', async () => {
      const { sm, consumers, logger, state } = manager({
        consumer: {
          durable_name: 'D',
          ack_policy: AckPolicy.Explicit,
          deliver_subject: '_nats.push.S.D',
          deliver_group: 'g',
          metadata: snap('deliver_group'),
        },
      });
      const push = pushConsumerDefaults({
        stream: 'S',
        durable: 'D',
        deliverGroup: 'g',
        maxAckPending: 4,
      });
      await sm.ensureConsumer('S', { durable_name: 'D' }, undefined, push);
      const sent = updateOf(consumers.update);
      expect(Object.entries(sent).filter(([, v]) => v === undefined)).toEqual([]);
      expect(sent.metadata).toEqual(snap(''));
      expect(state.consumer?.deliver_group).toBe('g');
      expect(logger.log).not.toHaveBeenCalled();
      await expect(
        sm.ensureConsumer('S', { durable_name: 'D' }, 'verify', push),
      ).resolves.toBeDefined();
    });

    it('reports a removed push field in verify only when it differs from the library default', async () => {
      const { sm } = manager({
        consumer: {
          durable_name: 'D',
          ack_policy: AckPolicy.Explicit,
          deliver_subject: '_nats.push.S.D',
          deliver_group: 'h',
          metadata: snap('deliver_group'),
        },
      });
      const push = pushConsumerDefaults({
        stream: 'S',
        durable: 'D',
        deliverGroup: 'g',
        maxAckPending: 4,
      });
      await expect(sm.ensureConsumer('S', { durable_name: 'D' }, 'verify', push)).rejects.toThrow(
        'JetStream consumer "S/D" differs from its declaration: deliver_group (removed from the declaration, current: "h")',
      );
    });

    it('reports a removed field in verify', async () => {
      const { sm, consumers } = manager({
        consumer: {
          durable_name: 'D',
          ack_policy: AckPolicy.Explicit,
          max_ack_pending: 3,
          metadata: snap('max_ack_pending'),
        },
      });
      await expect(sm.ensureConsumer('S', { durable_name: 'D' }, 'verify')).rejects.toThrow(
        'JetStream consumer "S/D" differs from its declaration: max_ack_pending (removed from the declaration, current: 3)',
      );
      expect(consumers.update).not.toHaveBeenCalled();
    });

    it('never counts the push create defaults in the snapshot', async () => {
      const { sm, consumers, logger, state } = manager();
      const push = pushConsumerDefaults({ stream: 'S', durable: 'D', maxAckPending: 4 });
      const declared = { durable_name: 'D', description: 'd' };
      await sm.ensureConsumer('S', declared, undefined, push);
      expect(state.consumer).toMatchObject({
        deliver_subject: '_nats.push.S.D',
        flow_control: true,
        max_ack_pending: 4,
        metadata: snap('description'),
      });
      await sm.ensureConsumer('S', declared, undefined, push);
      expect(consumers.update).not.toHaveBeenCalled();
      expect(logger.log).not.toHaveBeenCalled();
    });
  });

  describe.each(['default', 'ignore'] as const)('removedFields %s', (removedFields) => {
    const opts = { removedFields };

    it('writes no snapshot when it creates', async () => {
      const { sm, streams, consumers } = manager({}, opts);
      await sm.ensureStream({ name: 'S', subjects: ['a'], metadata: { team: 'x' } });
      expect(streams.add).toHaveBeenCalledExactlyOnceWith({
        name: 'S',
        subjects: ['a'],
        metadata: { team: 'x' },
      });
      await sm.ensureConsumer('S', { durable_name: 'D' });
      expect(consumers.add).toHaveBeenCalledExactlyOnceWith('S', {
        durable_name: 'D',
        ack_policy: AckPolicy.Explicit,
        max_deliver: -1,
      });
    });

    it('never resets a removed field, nor reports it in verify', async () => {
      const { sm, streams, consumers, logger } = manager(
        {
          stream: { name: 'S', subjects: ['a'], max_age: 5, metadata: snap('max_age,subjects') },
          consumer: {
            durable_name: 'D',
            ack_policy: AckPolicy.Explicit,
            max_ack_pending: 3,
            metadata: snap('max_ack_pending'),
          },
        },
        opts,
      );
      await sm.ensureStream({ name: 'S', subjects: ['a'] });
      await sm.ensureConsumer('S', { durable_name: 'D' });
      await expect(
        sm.ensureStream({ name: 'S', subjects: ['a'] }, 'verify'),
      ).resolves.toBeDefined();
      await expect(sm.ensureConsumer('S', { durable_name: 'D' }, 'verify')).resolves.toBeDefined();
      expect(streams.update).not.toHaveBeenCalled();
      expect(consumers.update).not.toHaveBeenCalled();
      expect(logger.warn).not.toHaveBeenCalled();
      expect(logger.log).not.toHaveBeenCalled();
    });

    it('does not warn about a server older than 2.10', async () => {
      const { sm, logger } = manager({}, { ...opts, version: '2.9.0' });
      await sm.ensureStream({ name: 'S', subjects: ['a'] });
      expect(logger.warn).not.toHaveBeenCalled();
    });

    it('accepts any declared metadata key, as before', async () => {
      const { sm, streams } = manager({}, opts);
      await sm.ensureStream({ name: 'S', metadata: { '_nats.x': '1', [SNAPSHOT_KEY]: 'a' } });
      expect(streams.add).toHaveBeenCalledOnce();
    });

    it('compares declared metadata as a subset and sends it as declared', async () => {
      const { sm, streams } = manager(
        { stream: { name: 'S', subjects: ['a'], metadata: { team: 'x', other: 'y' } } },
        opts,
      );
      await sm.ensureStream({ name: 'S', subjects: ['a'], metadata: { team: 'x' } });
      expect(streams.update).not.toHaveBeenCalled();
      await sm.ensureStream({ name: 'S', subjects: ['a'], metadata: { team: 'z' } });
      expect(streams.update).toHaveBeenCalledExactlyOnceWith('S', {
        subjects: ['a'],
        metadata: { team: 'z' },
      });
    });
  });
});
