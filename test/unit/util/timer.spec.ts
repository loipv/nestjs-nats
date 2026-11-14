import { describe, expect, it } from 'vitest';
import { MAX_TIMER_DELAY, timerDelay } from '../../../src/util/timer.js';

describe('timerDelay', () => {
  it('is the largest delay setTimeout honours', () => {
    expect(MAX_TIMER_DELAY).toBe(2_147_483_647);
  });

  it.each([
    [0, 1],
    [-5, 1],
    [1.5, 1.5],
    [5000, 5000],
    [2 ** 31, MAX_TIMER_DELAY],
    [Number.POSITIVE_INFINITY, MAX_TIMER_DELAY],
  ])('clamps %s to %s', (ms, expected) => {
    expect(timerDelay(ms)).toBe(expected);
  });
});
