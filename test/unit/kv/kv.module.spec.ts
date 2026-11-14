import 'reflect-metadata';
import type { DynamicModule, FactoryProvider } from '@nestjs/common';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ConnectionRegistry } from '../../../src/connection/connection-registry.js';
import { NatsConfigError } from '../../../src/errors/errors.js';
import { getKvToken, NatsKvModule, openKvBucket } from '../../../src/kv/kv.module.js';
import { getConnectionToken } from '../../../src/module/tokens.js';

const kvm = vi.hoisted(() => ({
  create: vi.fn(),
  open: vi.fn(),
  js: undefined as unknown,
}));

vi.mock('@nats-io/kv', () => ({
  Kvm: class {
    constructor(js: unknown) {
      kvm.js = js;
    }
    create = kvm.create;
    open = kvm.open;
  },
}));

const KV = { bucket: 'the kv' };

/** A stream-not-found error as `jsm.streams.info` rejects with. */
const notFound = () => new Error('stream not found');

function entryWith(sync: string | undefined, streamConfig: Record<string, unknown> | Error = {}) {
  const info = vi.fn(async () => {
    if (streamConfig instanceof Error) throw streamConfig;
    return { config: streamConfig };
  });
  const logger = { log: vi.fn(), warn: vi.fn(), error: vi.fn() };
  const js = { name: 'js' };
  const entry = {
    name: 'default',
    options: { connection: {}, ...(sync ? { jetstream: { sync } } : {}) },
    logger,
    js: () => js,
    jsm: async () => ({ streams: { info } }),
  };
  return { entry: entry as never, info, logger, js };
}

function errorOf(promise: Promise<unknown>) {
  return promise.then(
    () => {
      throw new Error('expected a rejection');
    },
    (err: unknown) => err as Error,
  );
}

beforeEach(() => {
  kvm.create.mockReset().mockResolvedValue(KV);
  kvm.open.mockReset().mockResolvedValue(KV);
});

describe('getKvToken', () => {
  it('names the bucket and its connection', () => {
    expect(getKvToken('s')).toBe('nats:kv:default:s');
    expect(getKvToken('s', 'other')).toBe('nats:kv:other:s');
  });
});

describe('NatsKvModule.forFeature', () => {
  it.each([
    ['no bucket', { buckets: [] }, 'NatsKvModule.forFeature needs at least one bucket'],
    [
      'an invalid bucket name',
      { buckets: [{ bucket: 'a.b' }] },
      'NatsKvModule.forFeature has invalid bucket "a.b"; expected letters, digits, - or _',
    ],
    [
      'a bucket declared twice',
      { buckets: [{ bucket: 's' }, { bucket: 's' }] },
      'NatsKvModule.forFeature declares bucket "s" twice',
    ],
  ])('rejects %s', (_, options, message) => {
    expect(() => NatsKvModule.forFeature(options)).toThrow(new NatsConfigError(message));
  });

  it('gives each bucket an exported provider waiting for its connection', () => {
    const module: DynamicModule = NatsKvModule.forFeature({
      connection: 'other',
      buckets: [{ bucket: 'a' }, { bucket: 'b', history: 5 }],
    });
    const providers = module.providers as FactoryProvider[];
    expect(providers.map((p) => p.provide)).toEqual(['nats:kv:other:a', 'nats:kv:other:b']);
    for (const p of providers) {
      expect(p.inject).toEqual([getConnectionToken('other'), ConnectionRegistry]);
    }
    expect(module.exports).toEqual(['nats:kv:other:a', 'nats:kv:other:b']);
  });

  it('closes the connections when a bucket cannot be opened', async () => {
    const { entry } = entryWith('verify', notFound());
    const registry = { get: () => entry, abortStartup: vi.fn(async () => {}) };
    const [provider] = NatsKvModule.forFeature({ buckets: [{ bucket: 's' }] })
      .providers as FactoryProvider[];
    await expect(provider?.useFactory({}, registry)).rejects.toThrow(NatsConfigError);
    expect(registry.abortStartup).toHaveBeenCalledOnce();
  });
});

describe('openKvBucket', () => {
  it('opens the bucket with sync none, on the connection’s JetStream client', async () => {
    const { entry, info, js } = entryWith('none');
    await expect(openKvBucket(entry, { bucket: 's', history: 5 }, 'default')).resolves.toBe(KV);
    expect(kvm.js).toBe(js);
    expect(kvm.open).toHaveBeenCalledExactlyOnceWith('s');
    expect(kvm.create).not.toHaveBeenCalled();
    expect(info).not.toHaveBeenCalled();
  });

  it('creates the bucket with create-only, comparing nothing', async () => {
    const { entry, info } = entryWith('create-only');
    await openKvBucket(entry, { bucket: 's', history: 5 }, 'default');
    expect(kvm.create).toHaveBeenCalledExactlyOnceWith('s', { history: 5 });
    expect(info).not.toHaveBeenCalled();
  });

  describe('create-or-update (the default)', () => {
    it('creates the bucket, then warns about each declared field the bucket differs on', async () => {
      const { entry, info, logger } = entryWith(undefined, {
        max_msgs_per_subject: 1,
        max_age: 3_600_000_000_000,
      });
      await openKvBucket(entry, { bucket: 's', history: 5, ttl: 3_600_000 }, 'default');
      expect(kvm.create).toHaveBeenCalledExactlyOnceWith('s', { history: 5, ttl: 3_600_000 });
      expect(info).toHaveBeenCalledExactlyOnceWith('KV_s');
      expect(logger.warn).toHaveBeenCalledExactlyOnceWith(
        'KV bucket "s" on connection "default" has history 1, declared 5; the library does not update KV buckets: change it with `nats kv` or recreate the bucket',
      );
    });

    it('logs nothing for a bucket matching its declaration', async () => {
      const { entry, logger } = entryWith('create-or-update', {
        max_msgs_per_subject: 5,
        max_age: 1_000_000_000,
        max_bytes: 1024,
        max_msg_size: 256,
        num_replicas: 1,
        storage: 'file',
      });
      await openKvBucket(
        entry,
        {
          bucket: 's',
          history: 5,
          ttl: 1000,
          max_bytes: 1024,
          maxValueSize: 256,
          replicas: 1,
          storage: 'file' as never,
        },
        'default',
      );
      expect(logger.warn).not.toHaveBeenCalled();
    });

    it('compares nothing when only the bucket is declared', async () => {
      const { entry, info } = entryWith('create-or-update');
      await openKvBucket(entry, { bucket: 's' }, 'default');
      expect(info).not.toHaveBeenCalled();
    });

    it('leaves undeclared fields alone, whatever the server reports', async () => {
      const { entry, logger } = entryWith('create-or-update', {
        max_msgs_per_subject: 1,
        max_age: 0,
      });
      await openKvBucket(entry, { bucket: 's', max_bytes: 10 }, 'default');
      expect(logger.warn).toHaveBeenCalledOnce();
      expect(logger.warn.mock.calls[0]?.[0]).toMatch(/has max_bytes undefined, declared 10/);
    });
  });

  describe('verify', () => {
    it('fails when the bucket does not exist (open alone would not tell)', async () => {
      const { entry } = entryWith('verify', notFound());
      const err = await errorOf(openKvBucket(entry, { bucket: 's' }, 'default'));
      expect(err).toBeInstanceOf(NatsConfigError);
      expect(err.message).toBe('KV bucket "s" on connection "default" does not exist');
    });

    it('fails when the bucket differs from its declaration', async () => {
      const { entry } = entryWith('verify', { max_msgs_per_subject: 1, max_age: 0 });
      const err = await errorOf(
        openKvBucket(entry, { bucket: 's', history: 5, ttl: 0 }, 'default'),
      );
      expect(err).toBeInstanceOf(NatsConfigError);
      expect(err.message).toBe(
        'KV bucket "s" on connection "default" differs from its declaration: history (current: 1, declared: 5)',
      );
    });

    it('opens a bucket matching its declaration, never creating one', async () => {
      const { entry } = entryWith('verify', { max_msgs_per_subject: 5 });
      await expect(openKvBucket(entry, { bucket: 's', history: 5 }, 'default')).resolves.toBe(KV);
      expect(kvm.create).not.toHaveBeenCalled();
    });

    it('checks that a bucket declared without limits exists', async () => {
      const { entry, info } = entryWith('verify', { max_msgs_per_subject: 1 });
      await openKvBucket(entry, { bucket: 's' }, 'default');
      expect(info).toHaveBeenCalledExactlyOnceWith('KV_s');
    });

    it('rethrows other failures as they are', async () => {
      const boom = new Error('permissions violation');
      const { entry } = entryWith('verify', boom);
      await expect(openKvBucket(entry, { bucket: 's' }, 'default')).rejects.toBe(boom);
    });
  });
});
