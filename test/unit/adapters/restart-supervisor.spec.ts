import type { LoggerService } from '@nestjs/common';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { HandlerState } from '../../../src/adapters/adapter.js';
import { RestartSupervisor } from '../../../src/adapters/restart-supervisor.js';
import { DEFAULT_RESTART, type RestartPolicy } from '../../../src/settlement/restart.js';

function setup(
  opts: {
    policy?: Partial<RestartPolicy> | false;
    reopen?: () => Promise<void>;
    closed?: () => boolean;
    random?: () => number;
  } = {},
) {
  const logger = { log: vi.fn(), warn: vi.fn(), error: vi.fn() };
  const states: HandlerState[] = [];
  const reopen = vi.fn(opts.reopen ?? (() => Promise.resolve()));
  const supervisor = new RestartSupervisor({
    policy: opts.policy === false ? false : { ...DEFAULT_RESTART, ...opts.policy },
    label: 'L',
    logger: logger as unknown as LoggerService,
    isConnectionClosed: opts.closed ?? (() => false),
    reopen,
    onState: (s) => states.push(s),
    random: opts.random ?? (() => 0.5),
  });
  return { supervisor, logger, states, reopen };
}

describe('RestartSupervisor', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('schedules restarts with backoff delays', async () => {
    let calls = 0;
    const { supervisor, logger, states } = setup({
      reopen: () => (++calls <= 2 ? Promise.reject(new Error('boom')) : Promise.resolve()),
    });
    supervisor.started();
    supervisor.ended(new Error('x'));
    expect(logger.warn).toHaveBeenLastCalledWith('Restarting L in 1000 ms (attempt 1)');
    await vi.advanceTimersByTimeAsync(1000);
    expect(logger.warn).toHaveBeenCalledWith('Restart of L failed (attempt 1): boom');
    expect(logger.warn).toHaveBeenLastCalledWith('Restarting L in 2000 ms (attempt 2)');
    await vi.advanceTimersByTimeAsync(2000);
    await vi.advanceTimersByTimeAsync(4000);
    expect(logger.log).toHaveBeenCalledWith('Restarted L (attempt 3)');
    expect(states).toEqual(['running', 'restarting', 'restarting', 'restarting', 'running']);
  });

  it('clamps the delay to the longest timer Node supports', async () => {
    const max = 2 ** 31 - 1;
    const { supervisor, logger, reopen } = setup({
      policy: { initialDelay: max, maxDelay: max, jitter: 1 },
      random: () => 1,
    });
    supervisor.started();
    supervisor.ended();
    expect(logger.warn).toHaveBeenLastCalledWith(`Restarting L in ${max} ms (attempt 1)`);
    // Node fires a longer timer after 1 ms.
    await vi.advanceTimersByTimeAsync(1000);
    expect(reopen).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(max - 1000);
    expect(reopen).toHaveBeenCalledOnce();
  });

  it('shows the attempt limit when maxAttempts is finite', () => {
    const { supervisor, logger } = setup({ policy: { maxAttempts: 3 } });
    supervisor.started();
    supervisor.ended();
    expect(logger.warn).toHaveBeenCalledWith('Restarting L in 1000 ms (attempt 1/3)');
  });

  it('resets the attempt count after running for maxDelay', async () => {
    const { supervisor, logger } = setup();
    supervisor.started();
    supervisor.ended();
    await vi.advanceTimersByTimeAsync(1000);
    await vi.advanceTimersByTimeAsync(DEFAULT_RESTART.maxDelay);
    supervisor.ended();
    expect(logger.warn).toHaveBeenLastCalledWith('Restarting L in 1000 ms (attempt 1)');
  });

  it('keeps counting when the reopened intake dies before maxDelay', async () => {
    const { supervisor, logger } = setup();
    supervisor.started();
    supervisor.ended();
    await vi.advanceTimersByTimeAsync(1000);
    supervisor.ended();
    expect(logger.warn).toHaveBeenLastCalledWith('Restarting L in 2000 ms (attempt 2)');
  });

  it('gives up after maxAttempts', async () => {
    const { supervisor, logger, states } = setup({
      policy: { maxAttempts: 2 },
      reopen: () => Promise.reject(new Error('no')),
    });
    supervisor.started();
    supervisor.ended();
    await vi.advanceTimersByTimeAsync(1000);
    await vi.advanceTimersByTimeAsync(2000);
    expect(logger.error).toHaveBeenCalledWith(
      'Gave up restarting L after 2 attempts; no more messages will be received',
    );
    expect(supervisor.state).toBe('failed');
    expect(states.at(-1)).toBe('failed');
    expect(vi.getTimerCount()).toBe(0);
    const warns = logger.warn.mock.calls.length;
    supervisor.ended();
    expect(logger.warn.mock.calls.length).toBe(warns);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('gives up when the reopened intake dies after the last attempt', async () => {
    const { supervisor, logger, states, reopen } = setup({ policy: { maxAttempts: 1 } });
    supervisor.started();
    supervisor.ended();
    await vi.advanceTimersByTimeAsync(1000);
    expect(logger.log).toHaveBeenCalledWith('Restarted L (attempt 1)');
    expect(supervisor.restartable).toBe(false);
    supervisor.ended();
    expect(logger.error).toHaveBeenCalledWith(
      'Gave up restarting L after 1 attempts; no more messages will be received',
    );
    expect(states).toEqual(['running', 'restarting', 'running', 'failed']);
    expect(vi.getTimerCount()).toBe(0);
    expect(reopen).toHaveBeenCalledTimes(1);
  });

  it('fails without scheduling when the connection is closed at the end', () => {
    const { supervisor, logger, reopen } = setup({ closed: () => true });
    supervisor.started();
    expect(supervisor.restartable).toBe(false);
    supervisor.ended();
    expect(supervisor.state).toBe('failed');
    expect(logger.warn).not.toHaveBeenCalled();
    expect(logger.error).not.toHaveBeenCalled();
    expect(reopen).not.toHaveBeenCalled();
  });

  it('gives up when the connection closed before the restart ran', async () => {
    let closed = false;
    const { supervisor, logger, reopen } = setup({ closed: () => closed });
    supervisor.started();
    supervisor.ended();
    closed = true;
    await vi.advanceTimersByTimeAsync(1000);
    expect(logger.error).toHaveBeenCalledWith('Gave up restarting L: the connection is closed');
    expect(supervisor.state).toBe('failed');
    expect(reopen).not.toHaveBeenCalled();
  });

  it('fails at once with policy false', () => {
    const { supervisor, logger } = setup({ policy: false });
    supervisor.started();
    expect(supervisor.restartable).toBe(false);
    supervisor.ended();
    expect(supervisor.state).toBe('failed');
    expect(logger.warn).not.toHaveBeenCalled();
    expect(logger.error).not.toHaveBeenCalled();
  });

  it('ignores ended() while restarting', () => {
    const { supervisor, logger } = setup();
    supervisor.started();
    supervisor.ended();
    supervisor.ended();
    expect(logger.warn).toHaveBeenCalledTimes(1);
  });

  it('stop() cancels a pending restart', async () => {
    const { supervisor, reopen } = setup();
    supervisor.started();
    supervisor.ended();
    supervisor.stop();
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(reopen).not.toHaveBeenCalled();
  });

  it('a reopen finishing after stop() changes nothing', async () => {
    let resolve!: () => void;
    const { supervisor, logger } = setup({
      reopen: () =>
        new Promise<void>((r) => {
          resolve = r;
        }),
    });
    supervisor.started();
    supervisor.ended();
    await vi.advanceTimersByTimeAsync(1000);
    supervisor.stop();
    resolve();
    await supervisor.settled();
    expect(logger.log).not.toHaveBeenCalled();
    expect(supervisor.state).toBe('restarting');
    expect(vi.getTimerCount()).toBe(0);
  });

  it('counts an intake that ends during its reopen as a failed attempt', async () => {
    let resolve!: () => void;
    const { supervisor, logger, states } = setup({
      reopen: () =>
        new Promise<void>((r) => {
          resolve = r;
        }),
    });
    supervisor.started();
    supervisor.ended();
    await vi.advanceTimersByTimeAsync(1000);
    supervisor.ended();
    resolve();
    await supervisor.settled();
    expect(logger.warn).toHaveBeenCalledWith(
      'Restart of L failed (attempt 1): the intake ended while restarting',
    );
    expect(logger.warn).toHaveBeenLastCalledWith('Restarting L in 2000 ms (attempt 2)');
    expect(logger.log).not.toHaveBeenCalled();
    expect(states).toEqual(['running', 'restarting', 'restarting']);
  });

  it('counts an intake that ends right after its reopen resolved', async () => {
    const ref: { s?: RestartSupervisor } = {};
    const { supervisor, logger, states } = setup({
      reopen: () => {
        const p = Promise.resolve();
        // Runs before the supervisor's own continuation on the same promise.
        void p.then(() => ref.s?.ended());
        return p;
      },
    });
    ref.s = supervisor;
    supervisor.started();
    supervisor.ended();
    await vi.advanceTimersByTimeAsync(1000);
    expect(logger.warn).toHaveBeenCalledWith(
      'Restart of L failed (attempt 1): the intake ended while restarting',
    );
    expect(logger.warn).toHaveBeenLastCalledWith('Restarting L in 2000 ms (attempt 2)');
    expect(logger.log).not.toHaveBeenCalled();
    expect(states).toEqual(['running', 'restarting', 'restarting']);
  });

  it('counts one failed attempt when the reopen rejects after the intake ended', async () => {
    let reject!: (e: Error) => void;
    const { supervisor, logger } = setup({
      reopen: () =>
        new Promise<void>((_, rej) => {
          reject = rej;
        }),
    });
    supervisor.started();
    supervisor.ended();
    await vi.advanceTimersByTimeAsync(1000);
    supervisor.ended();
    reject(new Error('boom'));
    await supervisor.settled();
    expect(logger.warn).toHaveBeenCalledWith('Restart of L failed (attempt 1): boom');
    expect(logger.warn).toHaveBeenLastCalledWith('Restarting L in 2000 ms (attempt 2)');
    expect(logger.warn).toHaveBeenCalledTimes(3);
  });

  it('treats a synchronous throw from reopen as a failed attempt', async () => {
    const { supervisor, logger } = setup({
      reopen: () => {
        throw new Error('sync boom');
      },
    });
    supervisor.started();
    supervisor.ended();
    await vi.advanceTimersByTimeAsync(1000);
    expect(logger.warn).toHaveBeenCalledWith('Restart of L failed (attempt 1): sync boom');
    expect(logger.warn).toHaveBeenLastCalledWith('Restarting L in 2000 ms (attempt 2)');
  });

  it('an intake that ended before started() is restarted at once', () => {
    const { supervisor, logger, states } = setup();
    supervisor.ended();
    expect(states).toEqual([]);
    supervisor.started();
    expect(states).toEqual(['running', 'restarting']);
    expect(logger.warn).toHaveBeenCalledWith('Restarting L in 1000 ms (attempt 1)');
  });

  it('an intake that ended before started() ends failed when not restartable', () => {
    const { supervisor, states } = setup({ policy: false });
    supervisor.ended();
    supervisor.started();
    expect(states).toEqual(['running', 'failed']);
  });
});
