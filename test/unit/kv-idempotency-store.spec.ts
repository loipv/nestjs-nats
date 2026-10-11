import { createHash } from 'node:crypto';
import { JetStreamApiCodes, JetStreamApiError, type JetStreamClient } from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/transport-node';
import { Logger } from '@nestjs/common';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { NatsConfigError, NatsModuleError } from '../../src/errors/errors.js';
import { KvIdempotencyStore, type KvIdempotencyStoreOptions } from '../../src/kv/index.js';

// `new Kvm(js)` builds whatever the test installs here (see the vi.mock below).
const kvm = vi.hoisted(() => ({
  create: undefined as undefined | ((client: unknown) => unknown),
}));

vi.mock('@nats-io/kv', () => ({
  Kvm: class {
    constructor(client: unknown) {
      if (!kvm.create) throw new Error('no fake Kvm installed');
      // biome-ignore lint/correctness/noConstructorReturn: hands back the test's fake
      return kvm.create(client) as object;
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
  /** The bucket's max age in ms, as `status().ttl` reports it (0: unlimited). */
  ttl = 0;
  private seq = 0;

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
  // Like the server, `previousSeq` is the expected last revision of the key, 0 for none: a
  // delete or purge marker is a revision too.
  readonly put = vi.fn(async (k: string, data: string, opts?: { previousSeq?: number }) => {
    await tick();
    const expected = opts?.previousSeq;
    if (expected !== undefined && (this.entries.get(k)?.revision ?? 0) !== expected) {
      throw wrongSequence();
    }
    return this.write(k, 'PUT', data);
  });
  readonly delete = vi.fn(async (k: string) => {
    await tick();
    this.write(k, 'DEL', '');
  });
  readonly status = vi.fn(async () => ({ ttl: this.ttl }));

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
    readonly client: unknown,
    private readonly buckets: Map<string, FakeKv>,
  ) {}

  readonly open = vi.fn(async (name: string, _opts?: unknown) => {
    const kv = this.buckets.get(name);
    if (!kv) throw streamNotFound();
    return kv;
  });
  readonly create = vi.fn(async (name: string, opts?: { ttl?: number }) => {
    let kv = this.buckets.get(name);
    if (!kv) {
      kv = new FakeKv();
      kv.ttl = opts?.ttl ?? 0;
      this.buckets.set(name, kv);
    }
    return kv;
  });
}

const enc = (key: string) => Buffer.from(key, 'utf8').toString('base64url');

const BUCKET = 'nestjs_nats_idempotency';
const nc = { tag: 'nc' } as unknown as NatsConnection;
const js = { tag: 'js' } as unknown as JetStreamClient;
const ctx = (connection = 'default') => ({ connection, nc, js });
const OPEN_OPTS = { allow_direct: false };

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
  await store.bind(ctx());
  return { store, kv };
}

describe('KvIdempotencyStore options', () => {
  it.each<[string, KvIdempotencyStoreOptions]>([
    ['an empty bucket', { bucket: '' }],
    ['a bucket with a dot', { bucket: 'a.b' }],
    ['a bucket with a space', { bucket: 'a b' }],
    ['a bucket that is not a string', { bucket: 5 as unknown as string }],
    ['a bucket of 65 characters', { bucket: 'b'.repeat(65) }],
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

  it('names the bucket rule', () => {
    expect(() => new KvIdempotencyStore({ bucket: 'b'.repeat(65) })).toThrow(
      `KvIdempotencyStore bucket "${'b'.repeat(65)}" is invalid; expected at most 64 letters, digits, "-" or "_"`,
    );
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
    expect(() => new KvIdempotencyStore({ bucket: 'b'.repeat(64) })).not.toThrow();
  });
});

describe('KvIdempotencyStore bind', () => {
  it('creates the bucket when it is missing', async () => {
    const store = new KvIdempotencyStore();
    await store.bind(ctx());
    expect(kvms).toHaveLength(1);
    // Built on the connection's JetStream client, so its jetstream.options apply.
    expect(kvms[0]?.client).toBe(js);
    expect(kvms[0]?.open).toHaveBeenCalledWith(BUCKET, OPEN_OPTS);
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
    await store.bind(ctx());
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
      k.open.mockResolvedValueOnce(lazy);
      kvms.push(k);
      return k;
    };
    const store = new KvIdempotencyStore();
    await store.bind(ctx());
    expect(kvms[0]?.create).toHaveBeenCalledTimes(1);
    expect(await store.claim('k', 1000)).toBe('claimed');
    expect(buckets.get(BUCKET)?.put).toHaveBeenCalled();
    expect(lazy.put).not.toHaveBeenCalled();
  });

  it('only opens an existing bucket', async () => {
    const { kv } = await boundStore();
    expect(kvms[0]?.open).toHaveBeenCalledWith(BUCKET, OPEN_OPTS);
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
    await expect(store.bind(ctx())).rejects.toBe(boom);
    expect(kvms[0]?.create).not.toHaveBeenCalled();
  });

  it('refuses a second bind', async () => {
    const { store } = await boundStore();
    const second = store.bind(ctx('other'));
    await expect(second).rejects.toThrow(NatsConfigError);
    await expect(second).rejects.toThrow(
      'KvIdempotencyStore is already bound to NATS connection "default"; give each connection its own store',
    );
    await expect(store.bind(ctx())).rejects.toThrow(NatsConfigError);
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

  it('reads from the stream leader, also in a bucket it created', async () => {
    // create() returns a handle with direct gets when the server has them: the store reopens it.
    const store = new KvIdempotencyStore();
    await store.bind(ctx());
    expect(kvms[0]?.open).toHaveBeenCalledTimes(2);
    expect(kvms[0]?.open).toHaveBeenNthCalledWith(2, BUCKET, OPEN_OPTS);
    expect(kvms[0]?.create.mock.invocationCallOrder[0]).toBeLessThan(
      kvms[0]?.open.mock.invocationCallOrder[1] as number,
    );
  });

  it('close releases the binding', async () => {
    const { store, kv } = await boundStore();
    await store.close();
    await expect(store.claim('k', 1000)).rejects.toThrow(NatsModuleError);
    expect(kv.delete).not.toHaveBeenCalled();
    expect(kv.put).not.toHaveBeenCalled();
  });

  it('binds again after close', async () => {
    const { store, kv } = await boundStore();
    expect(await store.claim('k', 1000)).toBe('claimed');
    await store.close();
    await store.bind(ctx('other'));
    expect(kvms).toHaveLength(2);
    expect(await store.claim('k', 1000)).toBe('in-progress');
    expect(await store.claim('k2', 1000)).toBe('claimed');
    expect(kv.value('k2')).toBeDefined();
    // Bound again: a further bind is refused, naming the new connection.
    await expect(store.bind(ctx())).rejects.toThrow(
      'KvIdempotencyStore is already bound to NATS connection "other"; give each connection its own store',
    );
  });

  it('binds again after a failed bind', async () => {
    const boom = new Error('no JetStream');
    kvm.create = (c) => {
      const k = new FakeKvm(c, buckets);
      if (kvms.length === 0) k.open.mockRejectedValueOnce(boom);
      kvms.push(k);
      return k;
    };
    const store = new KvIdempotencyStore();
    await expect(store.bind(ctx())).rejects.toBe(boom);
    await store.bind(ctx());
    expect(await store.claim('k', 1000)).toBe('claimed');
  });
});

describe('KvIdempotencyStore claim', () => {
  it('claims a new key', async () => {
    const { store, kv } = await boundStore();
    expect(await store.claim('k', 1000)).toBe('claimed');
    expect(kv.put).toHaveBeenCalledWith(enc('k'), JSON.stringify({ s: 'p', x: now + 1000 }), {
      previousSeq: 0,
    });
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

  it('claims over a delete marker with the revision read', async () => {
    const { store, kv } = await boundStore();
    await kv.delete(enc('k'));
    const revision = kv.entries.get(enc('k'))?.revision;
    expect(await store.claim('k', 1000)).toBe('claimed');
    // The marker is the key's last revision: the write expecting none fails first.
    expect(kv.put).toHaveBeenCalledTimes(1);
    expect(kv.update).toHaveBeenCalledWith(enc('k'), expect.any(String), revision);
    expect(kv.value('k')).toBe(`{"s":"p","x":${now + 1000}}`);
  });

  it('claims over a purge marker with the revision read', async () => {
    const { store, kv } = await boundStore();
    kv.entries.set(enc('k'), { revision: 7, operation: 'PURGE', value: '' });
    expect(await store.claim('k', 1000)).toBe('claimed');
    expect(kv.update).toHaveBeenCalledWith(enc('k'), expect.any(String), 7);
  });

  it('reports in-progress when another claim lands between the get and the update', async () => {
    const { store, kv } = await boundStore();
    await kv.delete(enc('k'));
    const revision = kv.entries.get(enc('k'))?.revision;
    kv.update.mockImplementationOnce(async (k: string) => {
      // Another instance claims the deleted key first.
      await kv.put(k, JSON.stringify({ s: 'p', x: now + 1000 }));
      throw wrongSequence();
    });
    expect(await store.claim('k', 1000)).toBe('in-progress');
    expect(kv.update).toHaveBeenCalledTimes(1);
    expect(kv.update).toHaveBeenCalledWith(enc('k'), expect.any(String), revision);
    const claims = kv.put.mock.calls.filter(([, , opts]) => opts?.previousSeq === 0);
    expect(claims).toHaveLength(2);
    expect(kv.get).toHaveBeenCalledTimes(2);
  });

  it('retries the claim when the entry vanished before the get', async () => {
    const { store, kv } = await boundStore();
    kv.put.mockRejectedValueOnce(wrongSequence());
    expect(await store.claim('k', 1000)).toBe('claimed');
    expect(kv.put).toHaveBeenCalledTimes(2);
    expect(kv.put).toHaveBeenLastCalledWith(enc('k'), expect.any(String), { previousSeq: 0 });
    expect(kv.update).not.toHaveBeenCalled();
  });

  it('one of two concurrent claims of an expired key wins', async () => {
    const { store: a, kv } = await boundStore();
    const b = new KvIdempotencyStore({ now: () => now });
    await b.bind(ctx());
    await a.claim('k', 1000);
    now += 2000;
    const results = await Promise.all([a.claim('k', 1000), b.claim('k', 1000)]);
    expect(results.sort()).toEqual(['claimed', 'in-progress']);
    // Both read the same revision; the second update lost the race and the claim was retried.
    expect(kv.update).toHaveBeenCalledTimes(2);
    expect(kv.update.mock.calls[0]?.[2]).toBe(kv.update.mock.calls[1]?.[2]);
    const claims = kv.put.mock.calls.filter(([, , opts]) => opts?.previousSeq === 0);
    expect(claims).toHaveLength(4);
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
    kv.put.mockRejectedValueOnce(boom);
    await expect(store.claim('k', 1000)).rejects.toBe(boom);
    const getErr = new Error('get failed');
    kv.put.mockRejectedValueOnce(wrongSequence());
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

describe('KvIdempotencyStore bucket max age', () => {
  const text = (bucket: string, maxAge: number, ttl: number) =>
    `KvIdempotencyStore bucket "${bucket}" keeps entries for ${maxAge} ms, less than the idempotency ttl ${ttl} ms; raise its max age`;

  it('warns once when a claim outlives the max age of an existing bucket', async () => {
    const warn = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
    const kv = new FakeKv();
    kv.ttl = 1000;
    buckets.set('b1', kv);
    const { store } = await boundStore({ bucket: 'b1' });
    expect(await store.claim('k1', 1000)).toBe('claimed');
    expect(warn).not.toHaveBeenCalled();
    expect(await store.claim('k2', 2000)).toBe('claimed');
    expect(await store.claim('k3', 3000)).toBe('claimed');
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(text('b1', 1000, 2000));
  });

  it('compares with the max age of a bucket it created', async () => {
    const warn = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
    const store = new KvIdempotencyStore({ maxAge: 60_000, now: () => now });
    await store.bind(ctx());
    await store.claim('k1', 60_000);
    expect(warn).not.toHaveBeenCalled();
    await store.claim('k2', 60_001);
    expect(warn).toHaveBeenCalledWith(text(BUCKET, 60_000, 60_001));
  });

  it('does not warn for a bucket without max age', async () => {
    const warn = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
    const { store, kv } = await boundStore();
    expect(kv.ttl).toBe(0);
    await store.claim('k', 10 ** 12);
    expect(warn).not.toHaveBeenCalled();
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
