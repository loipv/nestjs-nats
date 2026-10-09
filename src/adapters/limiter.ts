import { NatsConfigError } from '../errors/errors.js';

/** Counting semaphore bounding how many messages a handler processes at once. */
export interface Limiter {
  /** Resolves once a slot is free; waiters are served in FIFO order. */
  acquire(): Promise<void>;
  release(): void;
  /** Resolves when no slot is held. */
  idle(): Promise<void>;
  readonly active: number;
}

export function createLimiter(concurrency: number): Limiter {
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new NatsConfigError(`concurrency must be an integer >= 1 (got ${concurrency})`);
  }
  let active = 0;
  const waiters: Array<() => void> = [];
  let idleWaiters: Array<() => void> = [];

  return {
    get active() {
      return active;
    },
    acquire() {
      if (active < concurrency) {
        active++;
        return Promise.resolve();
      }
      return new Promise<void>((resolve) => waiters.push(resolve));
    },
    release() {
      if (active === 0) return;
      const next = waiters.shift();
      // Hand the slot straight to the next waiter so `active` never dips below the cap.
      if (next) return next();
      active--;
      if (active === 0) {
        const pending = idleWaiters;
        idleWaiters = [];
        for (const resolve of pending) resolve();
      }
    },
    idle() {
      if (active === 0) return Promise.resolve();
      return new Promise<void>((resolve) => idleWaiters.push(resolve));
    },
  };
}
