import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
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
    const err = (await p.then(
      () => undefined,
      (e: unknown) => e,
    )) as NatsConnectionError;
    expect(err).toBeInstanceOf(NatsConnectionError);
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

  it('closeAll closes multiple live connections and tolerates concurrent calls', async () => {
    const m = new NatsConnectionManager();
    const a = await m.connect('a', { connection: { servers: srv.url }, logger: false });
    const b = await m.connect('b', { connection: { servers: srv.url }, logger: false });
    await expect(Promise.all([m.closeAll(), m.closeAll()])).resolves.toBeDefined();
    expect(a.nc.isClosed()).toBe(true);
    expect(b.nc.isClosed()).toBe(true);
    expect(m.status('a').state).toBe('closed');
    expect(m.status('b').state).toBe('closed');
    await expect(m.closeAll()).resolves.toBeUndefined();
  });

  it('status$ emits events and status() follows the lifecycle', async () => {
    const m = new NatsConnectionManager();
    const e = await m.connect('s', { connection: { servers: srv.url }, logger: false });
    const events: string[] = [];
    const sub = m.status$('s').subscribe((ev) => events.push(`${ev.connection}:${ev.type}`));
    const other = m.status$('other');
    const otherEvents: unknown[] = [];
    const otherSub = other.subscribe((ev) => otherEvents.push(ev));
    const closing = m.closeAll();
    expect(['draining', 'closed']).toContain(m.status('s').state);
    await closing;
    await e.nc.closed();
    await vi.waitFor(() => expect(events.length).toBeGreaterThan(0));
    expect(events.every((ev) => ev.startsWith('s:'))).toBe(true);
    expect(otherEvents).toEqual([]);
    expect(m.status('s').state).toBe('closed');
    sub.unsubscribe();
    otherSub.unsubscribe();
  });

  it('logs disconnects without leaking credentials and reports reconnecting', async () => {
    const auth = await startNats(['--user', 'app', '--pass', 'pw-s3cr3t']);
    const m = new NatsConnectionManager();
    const lines: string[] = [];
    const record = (msg: unknown) => lines.push(String(msg));
    const e = await m.connect('r', {
      connection: { servers: auth.url, user: 'app', pass: 'pw-s3cr3t' },
      logger: { log: record, warn: record, error: record },
    });
    const seen: string[] = [];
    m.status$('r').subscribe((ev) => seen.push(ev.type));
    await auth.stop();
    await vi.waitFor(() => expect(seen).toContain('disconnect'));
    expect(m.status('r').state).toBe('reconnecting');
    expect(lines.some((l) => l.includes('disconnected'))).toBe(true);
    expect(lines.join('\n')).not.toMatch(/s3cr3t/);
    await e.nc.close();
    await m.closeAll();
  });

  it('closeAll drains every connection', async () => {
    await mgr.closeAll();
    expect(mgr.status().state).toBe('closed');
    await mgr.closeAll();
  });
});

describe('NatsConnectionManager close hooks', () => {
  let srv: NatsTestServer;

  beforeAll(async () => {
    srv = await startNats();
  });

  afterAll(async () => {
    await srv.stop();
  });

  it('runs close hooks before draining, logs failures and honours unregistering', async () => {
    const lines: string[] = [];
    const logger = {
      log() {},
      warn() {},
      debug() {},
      verbose() {},
      error: (m: string) => lines.push(m),
    };
    const m = new NatsConnectionManager();
    const e = await m.connect('h', { connection: { servers: srv.url }, logger });
    const seen: string[] = [];
    e.onClose(async () => {
      seen.push(`ok:${e.nc.isDraining()}`);
    });
    e.onClose(async () => {
      throw new Error('hook failed');
    });
    e.onClose(async () => {
      seen.push('late');
    });
    const off = e.onClose(async () => {
      seen.push('removed');
    });
    off();
    await expect(m.closeAll()).resolves.toBeUndefined();
    expect(seen).toEqual(['ok:false', 'late']);
    expect(lines.some((l) => l.includes('close hook failed') && l.includes('hook failed'))).toBe(
      true,
    );
    expect(e.nc.isClosed()).toBe(true);
  });

  it('closes forcefully when close hooks and drain outlast timeoutMs', async () => {
    const warnings: string[] = [];
    const logger = {
      log() {},
      error() {},
      debug() {},
      verbose() {},
      warn: (m: string) => warnings.push(m),
    };
    const m = new NatsConnectionManager();
    const e = await m.connect('stuck', { connection: { servers: srv.url }, logger });
    e.onClose(() => new Promise<void>(() => {}));
    const t0 = Date.now();
    await expect(m.closeAll({ timeoutMs: 200 })).resolves.toBeUndefined();
    expect(Date.now() - t0).toBeGreaterThanOrEqual(190);
    expect(Date.now() - t0).toBeLessThan(1000);
    expect(e.nc.isClosed()).toBe(true);
    expect(warnings).toEqual([
      'NATS connection "stuck" did not drain within 200ms; closed forcefully',
    ]);
  });

  it('closes forcefully when the server is gone', async () => {
    const gone = await startNats();
    const warnings: string[] = [];
    const errors: string[] = [];
    const logger = {
      log() {},
      debug() {},
      verbose() {},
      error: (m: string) => errors.push(m),
      warn: (m: string) => warnings.push(m),
    };
    const m = new NatsConnectionManager();
    const e = await m.connect('gone', { connection: { servers: gone.url }, logger });
    const sub = e.nc.subscribe('gone.>');
    const loop = (async () => {
      for await (const _ of sub) {
        // nothing is published
      }
    })();
    await e.nc.flush();
    await gone.stop();
    await vi.waitFor(() => expect(m.status('gone').state).toBe('reconnecting'), {
      timeout: 5000,
    });
    const t0 = Date.now();
    await expect(m.closeAll({ timeoutMs: 300 })).resolves.toBeUndefined();
    expect(Date.now() - t0).toBeLessThan(1500);
    expect(e.nc.isClosed()).toBe(true);
    // The subscription's iterator ends, so nothing keeps waiting on it.
    await expect(loop).resolves.toBeUndefined();
    expect(sub.isClosed()).toBe(true);
    expect(warnings).toContain(
      'NATS connection "gone" did not drain within 300ms; closed forcefully',
    );
    // The abandoned drain must not surface as a failure.
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(errors).toEqual([]);
  });
});
