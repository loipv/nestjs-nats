import { describe, expect, it } from 'vitest';
import { subjectMatches } from '../../../src/testing/subject.js';

describe('subjectMatches', () => {
  it.each([
    ['a.b', 'a.b', true],
    ['a.*', 'a.b', true],
    ['a.*', 'a.b.c', false],
    ['a.*', 'a', false],
    ['*.b', 'a.b', true],
    ['a.>', 'a.b', true],
    ['a.>', 'a.b.c', true],
    ['a.>', 'a', false],
    ['>', 'a.b', true],
    ['a.b', 'a.c', false],
    ['a.*.c', 'a.x.c', true],
    ['a.*', 'a.*', true],
  ])('%s ~ %s is %s', (pattern, subject, expected) =>
    expect(subjectMatches(pattern, subject)).toBe(expected),
  );
});
