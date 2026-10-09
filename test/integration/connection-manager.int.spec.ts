import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NatsConnectionManager } from '../../src/connection/connection-manager.js';
import { NatsConfigError, NatsConnectionError } from '../../src/errors/errors.js';
import { type NatsTestServer, startNats } from './support/nats-server.js';

describe('NatsConnectionManager', () => {
  let srv: NatsTestServer;
  let mgr: NatsConnectionManager;
  const extra: NatsTestServer[] = [];

  beforeAll(async () => {
    srv = await startNats();
    mgr = new NatsConnectionManager();
  });

  afterAll(async () => {
    await mgr.closeAll();
    await srv.stop();
    await Promise.all(extra.map((s) => s.stop()));
  });

  it('connects and reports connected status', async () => {
    const e = await mgr.connect('default', {
      connection: { servers: srv.url },
      logger: false,
    });
    expect(mgr.status().state).toBe('connected');
    expect(e.nc.isClosed()).toBe(false);
    expect(mgr.has('default')).toBe(true);
    expect(mgr.names()).toEqual(['default']);
    expect(mgr.get()).toBe(e);
  });

  it('rejects duplicate names', async () => {
    await expect(mgr.connect('default', { connection: { servers: srv.url } })).rejects.toThrow(
      NatsConfigError,
    );
  });

  it('fails fast on bad credentials without leaking the password', async () => {
    const auth = await startNats(['--user', 'app', '--pass', 's3cr3t']);
    extra.push(auth);
    const p = mgr.connect('bad', {
      connection: { servers: auth.url, user: 'app', pass: 'wrong-s3cr3t' },
      logger: false,
    });
    await expect(p).rejects.toThrow(NatsConnectionError);
    await expect(p).rejects.not.toThrow(/s3cr3t/);
    const err = (await p.then(
      () => undefined,
      (e: unknown) => e,
    )) as NatsConnectionError;
    expect(err.connection).toBe('bad');
    expect(JSON.stringify({ ...err, message: err.message })).not.toMatch(/s3cr3t/);
    expect(mgr.has('bad')).toBe(false);
  });

  it('lazily creates and caches js and jsm', async () => {
    const e = mgr.get();
    expect(e.js()).toBe(e.js());
    const jsm = await e.jsm();
    expect(await e.jsm()).toBe(jsm);
  });

  it('throws NatsConfigError for unknown connection', () => {
    expect(() => mgr.get('nope')).toThrow(NatsConfigError);
    expect(() => mgr.status('nope')).toThrow(NatsConfigError);
  });

  it('closeAll drains every connection', async () => {
    await mgr.closeAll();
    expect(mgr.status().state).toBe('closed');
    await mgr.closeAll();
  });
});
