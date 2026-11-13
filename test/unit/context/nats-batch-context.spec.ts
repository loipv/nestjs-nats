import { describe, expect, it } from 'vitest';
import { NatsBatchContext } from '../../../src/context/nats-batch-context.js';
import { ABORT_HANDLER } from '../../../src/context/nats-context.js';

const make = () => new NatsBatchContext({ connection: 'default', messages: [], onFail: () => {} });

describe('NatsBatchContext signal', () => {
  it('is a signal that is not aborted, the same on every read', () => {
    const ctx = make();
    expect(ctx.signal).toBeInstanceOf(AbortSignal);
    expect(ctx.signal.aborted).toBe(false);
    expect(ctx.signal).toBe(ctx.signal);
  });

  it('is aborted with the reason given to ABORT_HANDLER, keeping the first one', () => {
    const ctx = make();
    const { signal } = ctx;
    const first = new Error('timed out');
    ctx[ABORT_HANDLER](first);
    ctx[ABORT_HANDLER](new Error('second'));
    expect(signal.aborted).toBe(true);
    expect(signal.reason).toBe(first);
  });

  it('is already aborted when first read after ABORT_HANDLER', () => {
    const ctx = make();
    ctx[ABORT_HANDLER](new Error('timed out'));
    expect(ctx.signal.aborted).toBe(true);
  });
});
