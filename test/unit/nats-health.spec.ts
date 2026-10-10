import { HealthIndicatorService } from '@nestjs/terminus';
import { describe, expect, it, vi } from 'vitest';
import type { NatsConnectionManager } from '../../src/connection/connection-manager.js';
import { NatsConfigError } from '../../src/errors/errors.js';
import { NatsHealthIndicator } from '../../src/health/nats-health.indicator.js';

type State = 'connected' | 'reconnecting' | 'draining' | 'closed';

interface FakeOptions {
  state?: State;
  server?: string;
  rtt?: () => Promise<number>;
  accountInfo?: () => Promise<unknown>;
  jsm?: () => Promise<unknown>;
  pass?: string;
}

function setup(opts: FakeOptions = {}) {
  const getAccountInfo = vi.fn(opts.accountInfo ?? (async () => ({})));
  const rtt = vi.fn(opts.rtt ?? (async () => 2.6));
  const entry = {
    nc: { rtt },
    options: { connection: { pass: opts.pass } },
    jsm: vi.fn(opts.jsm ?? (async () => ({ getAccountInfo }))),
  };
  const manager = {
    has: (name: string) => name === 'default',
    get: () => entry,
    status: (name: string) => ({
      name,
      state: opts.state ?? 'connected',
      ...((opts.state ?? 'connected') === 'connected'
        ? { server: opts.server ?? 'nats://u:p@h:4222' }
        : {}),
    }),
  } as unknown as NatsConnectionManager;
  return {
    ind: new NatsHealthIndicator(new HealthIndicatorService(), manager),
    entry,
    rtt,
    getAccountInfo,
  };
}

const never = () => new Promise<never>(() => {});

describe('NatsHealthIndicator', () => {
  it('is up with server and rtt', async () => {
    const { ind } = setup({ rtt: async () => 3 });
    expect(await ind.isHealthy('nats')).toEqual({
      nats: { status: 'up', connection: 'default', server: 'nats://***@h:4222', rtt: 3 },
    });
  });

  it('rounds the rtt to milliseconds', async () => {
    const { ind } = setup({ rtt: async () => 2.6 });
    expect((await ind.isHealthy('nats')).nats).toMatchObject({ rtt: 3 });
  });

  it('is down for an unknown connection', async () => {
    const { ind } = setup();
    expect(await ind.isHealthy('nats', { connection: 'x' })).toEqual({
      nats: { status: 'down', connection: 'x', reason: 'unknown connection' },
    });
  });

  it.each(['closed', 'draining', 'reconnecting'] as const)('is down while %s', async (state) => {
    const { ind, rtt } = setup({ state });
    expect(await ind.isHealthy('nats')).toEqual({
      nats: { status: 'down', connection: 'default', reason: state },
    });
    expect(rtt).not.toHaveBeenCalled();
  });

  it.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY, 2 ** 31])(
    'rejects rttTimeout %s with a NatsConfigError',
    async (rttTimeout) => {
      const { ind, rtt } = setup();
      const err = await ind.isHealthy('nats', { rttTimeout }).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NatsConfigError);
      expect((err as Error).message).toContain('rttTimeout');
      expect(rtt).not.toHaveBeenCalled();
    },
  );

  it('accepts the largest timer delay as rttTimeout', async () => {
    const { ind } = setup({ rtt: async () => 1 });
    expect((await ind.isHealthy('nats', { rttTimeout: 2 ** 31 - 1 })).nats).toMatchObject({
      status: 'up',
    });
  });

  it('is down when rtt exceeds the timeout', async () => {
    const { ind } = setup({ rtt: never });
    expect(await ind.isHealthy('nats', { rttTimeout: 50 })).toEqual({
      nats: { status: 'down', connection: 'default', reason: 'rtt timeout after 50ms' },
    });
  });

  it('is down when rtt fails', async () => {
    const { ind } = setup({
      rtt: async () => {
        throw new Error('connection closed');
      },
    });
    expect((await ind.isHealthy('nats')).nats).toMatchObject({
      status: 'down',
      reason: 'rtt failed: connection closed',
    });
  });

  it('is down when rtt throws synchronously', async () => {
    const { ind } = setup({
      rtt: () => {
        throw new Error('boom');
      },
    });
    expect((await ind.isHealthy('nats')).nats).toMatchObject({
      status: 'down',
      reason: 'rtt failed: boom',
    });
  });

  it('does not touch JetStream unless asked', async () => {
    const { ind, entry } = setup();
    await ind.isHealthy('nats');
    expect(entry.jsm).not.toHaveBeenCalled();
  });

  it('checks JetStream when asked', async () => {
    const ok = setup();
    expect((await ok.ind.isHealthy('nats', { jetstream: true })).nats).toMatchObject({
      status: 'up',
    });
    expect(ok.getAccountInfo).toHaveBeenCalledOnce();

    const bad = setup({
      accountInfo: async () => {
        throw new Error('jetstream not enabled');
      },
    });
    expect((await bad.ind.isHealthy('nats', { jetstream: true })).nats).toMatchObject({
      status: 'down',
      connection: 'default',
      reason: 'jetstream unavailable: jetstream not enabled',
    });
  });

  it('is down when the JetStream manager cannot be created', async () => {
    const { ind } = setup({
      jsm: async () => {
        throw new Error('no responders');
      },
    });
    expect((await ind.isHealthy('nats', { jetstream: true })).nats).toMatchObject({
      status: 'down',
      reason: 'jetstream unavailable: no responders',
    });
  });

  it('is down when JetStream does not answer within the timeout', async () => {
    const { ind } = setup({ accountInfo: never });
    expect(await ind.isHealthy('nats', { jetstream: true, rttTimeout: 50 })).toEqual({
      nats: {
        status: 'down',
        connection: 'default',
        reason: 'jetstream unavailable: timeout after 50ms',
      },
    });
  });

  it('never leaks credentials', async () => {
    const rttErr = setup({
      pass: 's3cr3t',
      rtt: async () => {
        throw new Error('failed nats://u:hunter2@h:4222?pass=s3cr3t');
      },
    });
    const jsErr = setup({
      pass: 's3cr3t',
      accountInfo: async () => {
        throw new Error('bad creds for nats://u:hunter2@h:4222 (pass=s3cr3t)');
      },
    });
    for (const result of [
      await rttErr.ind.isHealthy('nats'),
      await jsErr.ind.isHealthy('nats', { jetstream: true }),
    ]) {
      const text = JSON.stringify(result);
      expect(text).not.toContain('s3cr3t');
      expect(text).not.toContain('hunter2');
      expect((result.nats as { status: string }).status).toBe('down');
    }
  });

  it('never throws, even for an unexpected failure', async () => {
    const manager = {
      has: () => {
        throw new Error('kaput');
      },
    } as unknown as NatsConnectionManager;
    const ind = new NatsHealthIndicator(new HealthIndicatorService(), manager);
    expect((await ind.isHealthy('nats')).nats).toMatchObject({
      status: 'down',
      connection: 'default',
      reason: 'check failed: kaput',
    });
  });
});
