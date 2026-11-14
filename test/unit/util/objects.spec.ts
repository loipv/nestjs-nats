import { describe, expect, it } from 'vitest';
import { withoutUndefined } from '../../../src/util/objects.js';

describe('withoutUndefined', () => {
  it('drops the keys set to undefined and keeps every other value', () => {
    expect(withoutUndefined({ a: 1, b: undefined, c: null, d: 0, e: false, f: '' })).toStrictEqual({
      a: 1,
      c: null,
      d: 0,
      e: false,
      f: '',
    });
  });

  it('lets defaults survive a spread', () => {
    expect({ ...{ a: 1, b: 2 }, ...withoutUndefined({ a: undefined, b: 3 }) }).toEqual({
      a: 1,
      b: 3,
    });
  });
});
