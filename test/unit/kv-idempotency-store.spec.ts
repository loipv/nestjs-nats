import { createHash } from 'node:crypto';
import { JetStreamApiCodes, JetStreamApiError } from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/transport-node';
import { Logger } from '@nestjs/common';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { NatsConfigError, NatsModuleError } from '../../src/errors/errors.js';
import { KvIdempotencyStore, type KvIdempotencyStoreOptions } from '../../src/kv/index.js';

// `new Kvm(nc)` builds whatever the test installs here (see the vi.mock below).
const kvm = vi.hoisted(() => ({ create: undefined as undefined | ((nc: unknown) => unknown) }));

vi.mock('@nats-io/kv', () => ({
  Kvm: class {
    constructor(nc: unknown) {
      if (!kvm.create) throw new Error('no fake Kvm installed');
      // biome-ignore lint/correctness/noConstructorReturn: hands back the test's fake
      return kvm.create(nc) as object;
    }
  },
}));

const apiError = (code: number, description: string) =>
  new JetStreamApiError({ err_code: code, code: 400, description });
const wrongSequence = () =>
  apiError(JetStreamApiCodes.StreamWrongLastSequence, 'wrong last sequence');
const streamNotFound = () => apiError(JetStreamApiCodes.StreamNotFound, 'stream not found');

/** Yields to the other pending promises, so concurrent calls interleave like on a server. */
const tick = () => new Promise<void>((r) => setImmediate(r));

interface Stored {
  revision: number;
  operation: 'PUT' | 'DEL' | 'PURGE';
  value: string;
}

/** An in-memory KV bucket with revisions, close enough to `@nats-io/kv` for the store. */
class FakeKv {
  readonly entries = new Map<string, Stored>();
  private seq = 0;

  readonly create = vi.fn(async (k: string, data: string) => {
    await tick();
    if (this.entries.get(k)?.operation === 'PUT') throw wrongSequence();
    return this.write(k, 'PUT', data);
  });
  readonly get = vi.fn(async (k: string) => {
    await tick();
    const e = this.entries.get(k);
    if (!e) return null;
    return {
      key: k,
      revision: e.revision,
      operation: e.operation,
      value: new TextEncoder().encode(e.value),
      string: () => e.value,
      json: () => JSON.parse(e.value),
    };
  });
  readonly update = vi.fn(async (k: string, data: string, version: number) => {
    await tick();
    if ((this.entries.get(k)?.revision ?? 0) !== version) throw wrongSequence();
    return this.write(k, 'PUT', data);
  });
  readonly put = vi.fn(async (k: string, data: string) => {
    await tick();
    return this.write(k, 'PUT', data);
  });
  readonly delete = vi.fn(async (k: string) => {
    await tick();
    this.write(k, 'DEL', '');
  });
  readonly status = vi.fn(async () => ({}));

  value(key: string): string | undefined {
    return this.entries.get(enc(key))?.value;
  }

  private write(k: string, operation: Stored['operation'], value: string): number {
    this.seq += 1;
    this.entries.set(k, { revision: this.seq, operation, value });
    return this.seq;
  }
}

class FakeKvm {
  constructor(
    readonly nc: unknown,
    private readonly buckets: Map<string, FakeKv>,
  ) {}

  readonly open = vi.fn(async (name: string) => {
    const kv = this.buckets.get(name);
    if (!kv) throw streamNotFound();
    return kv;
  });
  readonly create = vi.fn(async (name: string, _opts?: unknown) => {
    const kv = this.buckets.get(name) ?? new FakeKv();
    this.buckets.set(name, kv);
    return kv;
  });
}

const enc = (key: string) => Buffer.from(key, 'utf8').toString('base64url');

const BUCKET = 'nestjs_nats_idempotency';
const nc = { tag: 'nc' } as unknown as NatsConnection;

let buckets: Map<string, FakeKv>;
let kvms: FakeKvm[];
let now: number;

beforeEach(() => {
  buckets = new Map();
  kvms = [];
  now = 1_000_000;
  kvm.create = (c) => {
    const k = new FakeKvm(c, buckets);
    kvms.push(k);
    return k;
  };
});

afterEach(() => vi.restoreAllMocks());

/** A bound store on a bucket that already exists. */
async function boundStore(
  opts: KvIdempotencyStoreOptions = {},
): Promise<{ store: KvIdempotencyStore; kv: FakeKv }> {
  const name = opts.bucket ?? BUCKET;
  const kv = buckets.get(name) ?? new FakeKv();
  buckets.set(name, kv);
  const store = new KvIdempotencyStore({ now: () => now, ...opts });
  await store.bind({ connection: 'default', nc });
  return { store, kv };
}

describe('KvIdempotencyStore options', () => {
  it.each<[string, KvIdempotencyStoreOptions]>([
    ['an empty bucket', { bucket: '' }],
    ['a bucket with a dot', { bucket: 'a.b' }],
    ['a bucket with a space', { bucket: 'a b' }],
    ['a bucket that is not a string', { bucket: 5 as unknown as string }],
    ['a zero maxAge', { maxAge: 0 }],
    ['a negative maxAge', { maxAge: -1 }],
    ['a fractional maxAge', { maxAge: 1.5 }],
    ['a maxAge above 2^31-1 seconds', { maxAge: 2_147_483_647_001 }],
    ['a NaN maxAge', { maxAge: Number.NaN }],
    ['an unknown storage', { storage: 'disk' as 'file' }],
    ['zero replicas', { replicas: 0 }],
    ['fractional replicas', { replicas: 1.5 }],
    ['an empty instanceId', { instanceId: '' }],
    ['an instanceId that is not a string', { instanceId: 7 as unknown as string }],
  ])('rejects %s', (_name, opts) => {
    expect(() => new KvIdempotencyStore(opts)).toThrow(NatsConfigError);
  });

  it('accepts the bounds', () => {
    expect(
      () =>
        new KvIdempotencyStore({
          bucket: 'A-z_09',
          maxAge: 2_147_483_647_000,
          storage: 'memory',
          replicas: 3,
          instanceId: 'i1',
        }),
    ).not.toThrow();
    expect(() => new KvIdempotencyStore()).not.toThrow();
  });
});

describe('KvIdempotencyStore bind', () => {
  it('creates the bucket when it is missing', async () => {
    const store = new KvIdempotencyStore();
    await store.bind({ connection: 'default', nc });
    expect(kvms).toHaveLength(1);
    expect(kvms[0]?.nc).toBe(nc);
    expect(kvms[0]?.open).toHaveBeenCalledWith(BUCKET);
    expect(kvms[0]?.create).toHaveBeenCalledWith(BUCKET, {
      history: 1,
      ttl: 86_400_000,
      storage: 'file',
      replicas: 1,
    });
    expect(await store.claim('k', 1000)).toBe('claimed');
  });

  it('creates the bucket with the configured options', async () => {
    const store = new KvIdempotencyStore({
      bucket: 'b1',
      maxAge: 60_000,
      storage: 'memory',
      replicas: 3,
    });
    await store.bind({ connection: 'default', nc });
    expect(kvms[0]?.create).toHaveBeenCalledWith('b1', {
      history: 1,
      ttl: 60_000,
      storage: 'memory',
      replicas: 3,
    });
  });

  it('creates the bucket when the lazily opened bucket has no stream', async () => {
    // Kvm.open does not look the bucket up: its status() is what finds the stream missing.
    const lazy = new FakeKv();
    lazy.status.mockRejectedValue(streamNotFound());
    kvm.create = (c) => {
      const k = new FakeKvm(c, buckets);
      k.open.mockResolvedValue(lazy);
      kvms.push(k);
      return k;
    };
    const store = new KvIdempotencyStore();
    await store.bind({ connection: 'default', nc });
    expect(kvms[0]?.create).toHaveBeenCalledTimes(1);
    expect(await store.claim('k', 1000)).toBe('claimed');
    expect(buckets.get(BUCKET)?.create).toHaveBeenCalled();
    expect(lazy.create).not.toHaveBeenCalled();
  });

  it('only opens an existing bucket', async () => {
    const { kv } = await boundStore();
    expect(kvms[0]?.open).toHaveBeenCalledWith(BUCKET);
    expect(kv.status).toHaveBeenCalled();
    expect(kvms[0]?.create).not.toHaveBeenCalled();
  });

  it('propagates other open errors', async () => {
    const boom = apiError(JetStreamApiCodes.JetStreamNotEnabledForAccount, 'jetstream not enabled');
    kvm.create = (c) => {
      const k = new FakeKvm(c, buckets);
      k.open.mockRejectedValue(boom);
      kvms.push(k);
      return k;
    };
    const store = new KvIdempotencyStore();
    await expect(store.bind({ connection: 'default', nc })).rejects.toBe(boom);
    expect(kvms[0]?.create).not.toHaveBeenCalled();
  });

  it('refuses a second bind', async () => {
    const { store } = await boundStore();
    const second = store.bind({ connection: 'other', nc });
    await expect(second).rejects.toThrow(NatsConfigError);
    await expect(second).rejects.toThrow(
      'KvIdempotencyStore is already bound to NATS connection "default"; give each connection its own store',
    );
    await expect(store.bind({ connection: 'default', nc })).rejects.toThrow(NatsConfigError);
  });

  it('refuses use before bind', async () => {
    const store = new KvIdempotencyStore();
    const text =
      'KvIdempotencyStore is not bound to a NATS connection; pass it as idempotency.store of NatsModule.forRoot()';
    for (const op of [
      () => store.claim('k', 1000),
      () => store.complete('k', 1000),
      () => store.release('k'),
    ]) {
      const p = op();
      await expect(p).rejects.toThrow(NatsModuleError);
      await expect(p).rejects.toThrow(text);
    }
  });

  it('close does nothing', async () => {
    const { store, kv } = await boundStore();
    await store.close();
    expect(await store.claim('k', 1000)).toBe('claimed');
    expect(kv.delete).not.toHaveBeenCalled();
  });
});

describe('KvIdempotencyStore claim', () => {
  it('claims a new key', async () => {
    const { store, kv } = await boundStore();
    expect(await store.claim('k', 1000)).toBe('claimed');
    expect(kv.create).toHaveBeenCalledWith(enc('k'), JSON.stringify({ s: 'p', x: now + 1000 }));
    expect(kv.value('k')).toBe(`{"s":"p","x":${now + 1000}}`);
  });

  it('reports a completed key as duplicate', async () => {
    const { store } = await boundStore();
    await store.claim('k', 1000);
    await store.complete('k', 1000);
    expect(await store.claim('k', 1000)).toBe('duplicate');
  });

  it('reports a live claim as in-progress', async () => {
    const { store } = await boundStore();
    await store.claim('k', 1000);
    expect(await store.claim('k', 1000)).toBe('in-progress');
  });

  it('claims again after release', async () => {
    const { store, kv } = await boundStore();
    await store.claim('k', 1000);
    await store.release('k');
    expect(kv.delete).toHaveBeenCalledWith(enc('k'));
    expect(await store.claim('k', 1000)).toBe('claimed');
  });

  it.each(['p', 'd'])('claims an expired %s entry with the revision read', async (s) => {
    const { store, kv } = await boundStore();
    await store.claim('k', 1000);
    if (s === 'd') await store.complete('k', 1000);
    const revision = kv.entries.get(enc('k'))?.revision;
    now += 1000; // x > now() no longer holds
    expect(await store.claim('k', 500)).toBe('claimed');
    expect(kv.update).toHaveBeenCalledWith(
      enc('k'),
      JSON.stringify({ s: 'p', x: now + 500 }),
      revision,
    );
    expect(kv.value('k')).toBe(`{"s":"p","x":${now + 500}}`);
  });

  it.each([
    ['not JSON', 'nope'],
    ['JSON null', 'null'],
    ['a missing state', '{"x":99999999999}'],
    ['an unknown state', '{"s":"q","x":99999999999}'],
    ['a missing expiry', '{"s":"d"}'],
    ['a string expiry', '{"s":"d","x":"99999999999"}'],
  ])('claims over an unparsable value (%s)', async (_name, raw) => {
    const { store, kv } = await boundStore();
    await kv.put(enc('k'), raw);
    expect(await store.claim('k', 1000)).toBe('claimed');
    expect(kv.update).toHaveBeenCalledTimes(1);
    expect(kv.value('k')).toBe(`{"s":"p","x":${now + 1000}}`);
  });

  it('claims over a deleted entry reported by get', async () => {
    const { store, kv } = await boundStore();
    await kv.delete(enc('k'));
    // As if the key was deleted between a failed create and the get.
    kv.create.mockRejectedValueOnce(wrongSequence());
    const revision = kv.entries.get(enc('k'))?.revision;
    expect(await store.claim('k', 1000)).toBe('claimed');
    expect(kv.update).toHaveBeenCalledWith(enc('k'), expect.any(String), revision);
  });

  it('retries the claim when the entry vanished before the get', async () => {
    const { store, kv } = await boundStore();
    kv.create.mockRejectedValueOnce(wrongSequence());
    expect(await store.claim('k', 1000)).toBe('claimed');
    expect(kv.create).toHaveBeenCalledTimes(2);
    expect(kv.update).not.toHaveBeenCalled();
  });

  it('one of two concurrent claims of an expired key wins', async () => {
    const { store: a, kv } = await boundStore();
    const b = new KvIdempotencyStore({ now: () => now });
    await b.bind({ connection: 'default', nc });
    await a.claim('k', 1000);
    now += 2000;
    const results = await Promise.all([a.claim('k', 1000), b.claim('k', 1000)]);
    expect(results.sort()).toEqual(['claimed', 'in-progress']);
    // Both read the same revision; the second update lost the race and the claim was retried.
    expect(kv.update).toHaveBeenCalledTimes(2);
    expect(kv.update.mock.calls[0]?.[2]).toBe(kv.update.mock.calls[1]?.[2]);
    expect(kv.create).toHaveBeenCalledTimes(4);
  });

  it('gives up after 3 revision conflicts', async () => {
    const { store, kv } = await boundStore();
    await store.claim('k', 1000);
    now += 2000;
    const errors = [wrongSequence(), wrongSequence(), wrongSequence()];
    for (const e of errors) kv.update.mockRejectedValueOnce(e);
    await expect(store.claim('k', 1000)).rejects.toBe(errors[2]);
    expect(kv.update).toHaveBeenCalledTimes(3);
  });

  it('treats an unknown-sequence error as a revision conflict', async () => {
    const { store, kv } = await boundStore();
    await store.claim('k', 1000);
    now += 2000;
    kv.update.mockRejectedValueOnce(
      apiError(JetStreamApiCodes.StreamWrongLastSequenceUnknown, 'wrong last sequence unknown'),
    );
    expect(await store.claim('k', 1000)).toBe('claimed');
    expect(kv.update).toHaveBeenCalledTimes(2);
  });

  it('propagates other KV errors unchanged', async () => {
    const { store, kv } = await boundStore();
    const boom = new Error('timeout');
    kv.create.mockRejectedValueOnce(boom);
    await expect(store.claim('k', 1000)).rejects.toBe(boom);
    const getErr = new Error('get failed');
    kv.create.mockRejectedValueOnce(wrongSequence());
    kv.get.mockRejectedValueOnce(getErr);
    await expect(store.claim('k', 1000)).rejects.toBe(getErr);
    const putErr = new Error('put failed');
    kv.put.mockRejectedValueOnce(putErr);
    await expect(store.complete('k', 1000)).rejects.toBe(putErr);
  });
});

describe('KvIdempotencyStore keys', () => {
  it('encodes keys as base64url', async () => {
    const { store, kv } = await boundStore();
    const key = 'consume:S/d|a%2Fb:é';
    expect(await store.claim(key, 1000)).toBe('claimed');
    const stored = Buffer.from(key, 'utf8').toString('base64url');
    expect([...kv.entries.keys()]).toEqual([stored]);
    expect(stored).toMatch(/^[-\w]+$/);
    expect(Buffer.from(stored, 'base64url').toString('utf8')).toBe(key);
  });

  it('stores a key encoded in at most 256 characters as its encoding', async () => {
    const { store, kv } = await boundStore();
    // 192 bytes encode to exactly 256 characters.
    const key = `consume:S/d|${'x'.repeat(180)}`;
    expect(enc(key)).toHaveLength(256);
    expect(await store.claim(key, 1000)).toBe('claimed');
    expect([...kv.entries.keys()]).toEqual([enc(key)]);
  });

  it('stores a longer key under its sha256', async () => {
    const { store, kv } = await boundStore();
    const key = `consume:S/d|${'x'.repeat(181)}`;
    expect(enc(key)).toHaveLength(258);
    expect(await store.claim(key, 1000)).toBe('claimed');
    const hashed = `h.${createHash('sha256').update(key, 'utf8').digest('base64url')}`;
    expect([...kv.entries.keys()]).toEqual([hashed]);
  });

  it('hashes long keys: distinct per key, stable for one key', async () => {
    const { store, kv } = await boundStore();
    const a = `consume:S/d|${'a'.repeat(5 * 1024)}`;
    const b = `consume:S/d|${'a'.repeat(5 * 1024 - 1)}b`;
    expect(await store.claim(a, 1000)).toBe('claimed');
    expect(await store.claim(b, 1000)).toBe('claimed');
    const keys = [...kv.entries.keys()];
    expect(keys).toHaveLength(2);
    for (const k of keys) expect(k).toMatch(/^h\.[-\w]{43}$/);

    expect(await store.claim(a, 1000)).toBe('in-progress');
    await store.complete(a, 1000);
    expect(await store.claim(a, 1000)).toBe('duplicate');
    await store.release(b);
    expect(await store.claim(b, 1000)).toBe('claimed');
    expect([...kv.entries.keys()]).toHaveLength(2);
  });

  it('hashes a long ordered key after the instanceId prefix', async () => {
    const { store, kv } = await boundStore({ instanceId: 'i1' });
    const rest = `C.m:${'k'.repeat(300)}`;
    await store.claim(`ordered:${rest}`, 1000);
    const hashed = `h.${createHash('sha256').update(`ordered:i1:${rest}`, 'utf8').digest('base64url')}`;
    expect([...kv.entries.keys()]).toEqual([hashed]);
  });

  it('prefixes ordered keys with instanceId', async () => {
    const { store, kv } = await boundStore({ instanceId: 'i1' });
    await store.claim('ordered:C.m:k', 1000);
    await store.complete('ordered:C.m:k', 1000);
    await store.claim('consume:S/d|k', 1000);
    expect([...kv.entries.keys()].sort()).toEqual(
      [enc('ordered:i1:C.m:k'), enc('consume:S/d|k')].sort(),
    );
    await store.release('ordered:C.m:k');
    expect(kv.delete).toHaveBeenCalledWith(enc('ordered:i1:C.m:k'));
  });

  it('warns once about ordered keys without instanceId', async () => {
    const warn = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
    const { store, kv } = await boundStore();
    await store.claim('consume:S/d|k', 1000);
    expect(warn).not.toHaveBeenCalled();
    await store.claim('ordered:C.m:k1', 1000);
    await store.claim('ordered:C.m:k2', 1000);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(
      'KvIdempotencyStore without instanceId is shared by the instances of @ConsumeOrdered handlers: each instance skips the messages another processed. Set instanceId.',
    );
    expect(kv.value('ordered:C.m:k1')).toBeDefined();
  });
});

describe('KvIdempotencyStore complete', () => {
  it('complete puts done with the new expiry', async () => {
    const { store, kv } = await boundStore();
    await store.claim('k', 1000);
    now += 300;
    await store.complete('k', 5000);
    expect(kv.put).toHaveBeenCalledWith(enc('k'), JSON.stringify({ s: 'd', x: now + 5000 }));
    expect(kv.value('k')).toBe(`{"s":"d","x":${now + 5000}}`);
    now += 4999;
    expect(await store.claim('k', 1000)).toBe('duplicate');
    now += 1;
    expect(await store.claim('k', 1000)).toBe('claimed');
  });
});

describe('optional peer', () => {
  it('the main entry does not import @nats-io/kv', async () => {
    vi.resetModules();
    vi.doMock('@nats-io/kv', () => {
      throw new Error('not installed');
    });
    try {
      // The subpath does need it: proves the mock bites.
      await expect(import('../../src/kv/index.js')).rejects.toThrow();
      await expect(import('../../src/index.js')).resolves.toHaveProperty('NatsModule');
    } finally {
      vi.doUnmock('@nats-io/kv');
      vi.resetModules();
    }
  });
});
