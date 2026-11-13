import { describe, expect, it } from 'vitest';
import { isPowerOfTwo } from '../../../src/util/power-of-two.js';

describe('isPowerOfTwo', () => {
  it.each([1, 2, 4, 8, 64, 1024, 2 ** 30])('is true for %i', (n) => {
    expect(isPowerOfTwo(n)).toBe(true);
  });

  it.each([0, -1, -2, 3, 6, 12, 1023])('is false for %i', (n) => {
    expect(isPowerOfTwo(n)).toBe(false);
  });
});
