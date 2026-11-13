import { headers } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';
import { NatsContext, NatsJetStreamContext } from '../../../src/context/nats-context.js';
import { NatsOrderedContext } from '../../../src/context/nats-ordered-context.js';
import { NatsConfigError } from '../../../src/errors/errors.js';

function fakeJsMsg() {
  const h = headers();
  h.append('x', '1');
  h.append('multi', 'a');
  h.append('multi', 'b');
  return {
    subject: 'orders.created',
    reply: '',
    headers: h,
    ack: vi.fn(),
    nak: vi.fn(),
    term: vi.fn(),
    working: vi.fn(),
    info: { stream: 'S', streamSequence: 7, deliveryCount: 2 },
  };
}

describe('NatsOrderedContext', () => {
  const orderedRaw = () => ({
    ...fakeJsMsg(),
    reply: '$JS.ACK.S.c.1.42.7.0.3',
    info: { stream: 'S', streamSequence: 42, deliverySequence: 7, pending: 3 },
  });
  const ctxOf = (raw = orderedRaw()) =>
    new NatsOrderedContext({ connection: 'default', raw: raw as never, attempt: 2 });

  it('is an ordered NatsContext', () => {
    const ctx = ctxOf();
    expect(ctx).toBeInstanceOf(NatsContext);
    expect(ctx).not.toBeInstanceOf(NatsJetStreamContext);
    expect(ctx.kind).toBe('ordered');
    expect(ctx.connection).toBe('default');
    expect(ctx.subject).toBe('orders.created');
    expect(ctx.headers).toEqual({ x: '1', multi: ['a', 'b'] });
  });

  it('exposes the delivery info and the attempt', () => {
    const ctx = ctxOf();
    expect([ctx.stream, ctx.streamSequence, ctx.deliverySequence, ctx.pending]).toEqual([
      'S',
      42,
      7,
      3,
    ]);
    expect(ctx.attempt).toBe(2);
  });

  it('has no replyTo', () => expect(ctxOf().replyTo).toBeUndefined());

  it.each([
    ['ack', (c: NatsOrderedContext) => c.ack()],
    ['nak', (c: NatsOrderedContext) => c.nak(100)],
    ['term', (c: NatsOrderedContext) => c.term('bad')],
    ['working', (c: NatsOrderedContext) => c.working()],
  ])('%s() throws a NatsConfigError without touching the message', (_, call) => {
    const raw = orderedRaw();
    const ctx = ctxOf(raw);
    expect(() => call(ctx)).toThrow(NatsConfigError);
    expect(() => call(ctx)).toThrow('ordered consumer messages cannot be settled');
    expect(raw.ack).not.toHaveBeenCalled();
    expect(raw.nak).not.toHaveBeenCalled();
    expect(raw.term).not.toHaveBeenCalled();
    expect(raw.working).not.toHaveBeenCalled();
  });
});
