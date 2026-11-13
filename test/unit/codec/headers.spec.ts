import { headers } from '@nats-io/transport-node';
import { describe, expect, it } from 'vitest';
import { fromMsgHdrs, toMsgHdrs } from '../../../src/codec/headers.js';

describe('headers', () => {
  it('maps single and multi values', () => {
    const h = toMsgHdrs({ a: '1', b: ['x', 'y'] });
    expect(fromMsgHdrs(h)).toEqual({ a: '1', b: ['x', 'y'] });
  });

  it('returns {} for undefined', () => {
    expect(fromMsgHdrs(undefined)).toEqual({});
  });

  it('returns an empty MsgHdrs when no init is given', () => {
    expect(fromMsgHdrs(toMsgHdrs())).toEqual({});
  });

  it('appends to a provided base instead of creating a new one', () => {
    const base = headers();
    base.set('existing', 'v');
    const out = toMsgHdrs({ added: ['1', '2'] }, base);
    expect(out).toBe(base);
    expect(fromMsgHdrs(out)).toEqual({ existing: 'v', added: ['1', '2'] });
  });
});
