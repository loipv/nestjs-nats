import { AckPolicy, JetStreamApiCodes, JetStreamApiError } from '@nats-io/jetstream';
import { describe, expect, it, vi } from 'vitest';
import type { ConnectionEntry } from '../../../src/connection/connection-registry.js';
import { NatsStreamManager } from '../../../src/jetstream/stream-manager.js';
import { diffConfig, pushConsumerDefaults } from '../../../src/jetstream/stream-sync.js';

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

describe('NatsStreamManager: metadata left by removedFields reset (0.2)', () => {
  function manager(stream: object) {
    const streams = {
      info: vi.fn(() => Promise.resolve({ config: stream })),
      add: vi.fn(),
      update: vi.fn((_name: string, cfg: unknown) => Promise.resolve({ config: cfg })),
    };
    const entry = {
      name: 'default',
      nc: { info: { version: '2.12.0' } },
      options: {},
      logger: { log: vi.fn(), warn: vi.fn(), error: vi.fn() },
      jsm: () => Promise.resolve({ streams }),
    } as unknown as ConnectionEntry;
    return { sm: new NatsStreamManager(entry), streams };
  }

  it('is no drift for a declaration without metadata', async () => {
    const { sm, streams } = manager({
      name: 'S',
      subjects: ['s.>'],
      max_age: 5,
      metadata: { 'nestjs-nats.declared': 'max_age,subjects' },
    });
    const declared = { name: 'S', subjects: ['s.>'] };
    await expect(sm.ensureStream(declared, 'verify')).resolves.toBeDefined();
    await sm.ensureStream(declared, 'create-or-update');
    expect(streams.update).not.toHaveBeenCalled();
  });

  it('is no drift for a declaration with metadata of its own', async () => {
    const { sm, streams } = manager({
      name: 'S',
      subjects: ['s.>'],
      metadata: { team: 'a', 'nestjs-nats.declared': 'x' },
    });
    const declared = { name: 'S', subjects: ['s.>'], metadata: { team: 'a' } };
    await expect(sm.ensureStream(declared, 'verify')).resolves.toBeDefined();
    await sm.ensureStream(declared, 'create-or-update');
    expect(streams.update).not.toHaveBeenCalled();
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
});

describe('NatsStreamManager: a field removed from a declaration', () => {
  type Config = Record<string, unknown>;

  /**
   * A fake `jsm` that keeps the configs it is given. `add` and `update` merge the sent config
   * like nats.js and the server do (an `undefined` field is dropped) and echo the result back.
   */
  function manager(init: { stream?: Config; consumer?: Config } = {}, version?: string) {
    const state: { stream?: Config; consumer?: Config } = { ...init };
    const notFound = (err_code: number) =>
      Promise.reject(new JetStreamApiError({ code: 404, err_code, description: 'not found' }));
    const store = (cfg: Config): Config => JSON.parse(JSON.stringify(cfg)) as Config;
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
      options: {},
      logger,
      jsm: () => Promise.resolve({ streams, consumers }),
    } as unknown as ConnectionEntry;
    return { sm: new NatsStreamManager(entry), entry, streams, consumers, logger, state };
  }

  /** The metadata `removedFields: 'reset'` (removed in 0.3.0) left on the server. */
  const snap = (fields: string) => ({ 'nestjs-nats.declared': fields });

  it('writes no snapshot when it creates', async () => {
    const { sm, streams, consumers } = manager({});
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
    const { sm, streams, consumers, logger } = manager({
      stream: { name: 'S', subjects: ['a'], max_age: 5, metadata: snap('max_age,subjects') },
      consumer: {
        durable_name: 'D',
        ack_policy: AckPolicy.Explicit,
        max_ack_pending: 3,
        metadata: snap('max_ack_pending'),
      },
    });
    await sm.ensureStream({ name: 'S', subjects: ['a'] });
    await sm.ensureConsumer('S', { durable_name: 'D' });
    await expect(sm.ensureStream({ name: 'S', subjects: ['a'] }, 'verify')).resolves.toBeDefined();
    await expect(sm.ensureConsumer('S', { durable_name: 'D' }, 'verify')).resolves.toBeDefined();
    expect(streams.update).not.toHaveBeenCalled();
    expect(consumers.update).not.toHaveBeenCalled();
    expect(logger.warn).not.toHaveBeenCalled();
    expect(logger.log).not.toHaveBeenCalled();
  });

  it('does not warn about a server older than 2.10', async () => {
    const { sm, logger } = manager({}, '2.9.0');
    await sm.ensureStream({ name: 'S', subjects: ['a'] });
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it('accepts any declared metadata key, as before', async () => {
    const { sm, streams } = manager({});
    await sm.ensureStream({ name: 'S', metadata: { '_nats.x': '1', 'nestjs-nats.declared': 'a' } });
    expect(streams.add).toHaveBeenCalledOnce();
  });

  it('compares declared metadata as a subset and sends it as declared', async () => {
    const { sm, streams } = manager({
      stream: { name: 'S', subjects: ['a'], metadata: { team: 'x', other: 'y' } },
    });
    await sm.ensureStream({ name: 'S', subjects: ['a'], metadata: { team: 'x' } });
    expect(streams.update).not.toHaveBeenCalled();
    await sm.ensureStream({ name: 'S', subjects: ['a'], metadata: { team: 'z' } });
    expect(streams.update).toHaveBeenCalledExactlyOnceWith('S', {
      subjects: ['a'],
      metadata: { team: 'z' },
    });
  });
});

describe('diffConfig', () => {
  it("ignores the server's own _nats. metadata keys, and keys not declared", () => {
    const current = { metadata: { team: 'a', other: 'b', '_nats.level': '1' } };
    expect(diffConfig(current, { metadata: { team: 'a' } }, [])).toEqual([]);
    expect(diffConfig(current, { metadata: { team: 'b' } }, [])).toEqual([
      {
        field: 'metadata',
        current: { team: 'a', other: 'b' },
        declared: { team: 'b' },
        immutable: false,
      },
    ]);
  });
});
