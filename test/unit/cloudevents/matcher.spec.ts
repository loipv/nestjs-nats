import { describe, expect, it } from 'vitest';
import { createTypeRouter } from '../../../src/cloudevents/matcher.js';
import { NatsConfigError } from '../../../src/errors/errors.js';

describe('createTypeRouter', () => {
  const r = createTypeRouter([
    { pattern: 'com.shop.>', target: 'gt' },
    { pattern: 'com.shop.order.*', target: 'star' },
    { pattern: 'com.shop.order.created', target: 'exact' },
    { pattern: 'com.*.order.created', target: 'star2' },
  ]);

  it.each([
    ['com.shop.order.created', 'exact'],
    ['com.shop.order.paid', 'star'],
    ['com.shop.user.x.y', 'gt'],
    ['com.shop', undefined],
    ['other', undefined],
    ['', undefined],
    ['com.x.order.created', 'star2'],
  ])('%s -> %s', (type, target) => {
    expect(r(type)).toBe(target);
  });

  it('prefers more literal segments among non-tail patterns', () => {
    const router = createTypeRouter([
      { pattern: 'a.*.*', target: 'loose' },
      { pattern: 'a.b.*', target: 'tight' },
    ]);
    expect(router('a.b.c')).toBe('tight');
    expect(router('a.x.c')).toBe('loose');
  });

  it('prefers more literal segments among tail patterns', () => {
    const router = createTypeRouter([
      { pattern: 'a.>', target: 'short' },
      { pattern: 'a.b.>', target: 'long' },
    ]);
    expect(router('a.b.c')).toBe('long');
    expect(router('a.x')).toBe('short');
  });

  it('prefers a non-tail wildcard pattern over a tail pattern', () => {
    const router = createTypeRouter([
      { pattern: 'a.b.>', target: 'tail' },
      { pattern: 'a.*.c', target: 'star' },
    ]);
    expect(router('a.b.c')).toBe('star');
  });

  it('falls back to declaration order on a full tie', () => {
    const router = createTypeRouter([
      { pattern: 'a.*.c', target: 'first' },
      { pattern: 'a.b.*', target: 'second' },
    ]);
    expect(router('a.b.c')).toBe('first');
  });

  it('requires at least one segment for >', () => {
    const router = createTypeRouter([{ pattern: 'a.>', target: 1 }]);
    expect(router('a')).toBeUndefined();
    expect(router('a.b')).toBe(1);
  });

  it('does not match when segment counts differ', () => {
    const router = createTypeRouter([{ pattern: 'a.*', target: 1 }]);
    expect(router('a')).toBeUndefined();
    expect(router('a.b.c')).toBeUndefined();
  });

  it.each(['a..b', 'a.>.b', '', '.a', 'a.'])('rejects invalid pattern %j', (p) => {
    expect(() => createTypeRouter([{ pattern: p, target: 1 }])).toThrow(NatsConfigError);
  });

  it('rejects duplicate patterns', () => {
    expect(() =>
      createTypeRouter([
        { pattern: 'a', target: 1 },
        { pattern: 'a', target: 2 },
      ]),
    ).toThrow(NatsConfigError);
  });
});
