import { headers } from '@nats-io/transport-node';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { NatsContext, NatsJetStreamContext } from '../../src/context/nats-context.js';

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

describe('NatsContext', () => {
  it('exposes kind, connection, subject, headers, replyTo, raw', () => {
    const raw = { subject: 'a.b', reply: 'inbox.1', headers: undefined } as never;
    const ctx = new NatsContext({ kind: 'core', connection: 'default', raw });
    expect(ctx.kind).toBe('core');
    expect(ctx.connection).toBe('default');
    expect(ctx.subject).toBe('a.b');
    expect(ctx.headers).toEqual({});
    expect(ctx.replyTo).toBe('inbox.1');
    expect(ctx.raw).toBe(raw);
  });

  it('returns undefined replyTo when there is no reply subject', () => {
    const ctx = new NatsContext({
      kind: 'core',
      connection: 'default',
      raw: { subject: 'a', reply: '' } as never,
    });
    expect(ctx.replyTo).toBeUndefined();
  });

  it('computes headers once', () => {
    const raw = fakeJsMsg();
    const ctx = new NatsContext({ kind: 'service', connection: 'c', raw: raw as never });
    expect(ctx.headers).toEqual({ x: '1', multi: ['a', 'b'] });
    expect(ctx.headers).toBe(ctx.headers);
  });
});

describe('NatsJetStreamContext', () => {
  let raw: ReturnType<typeof fakeJsMsg>;
  let ctx: NatsJetStreamContext;
  beforeEach(() => {
    raw = fakeJsMsg();
    ctx = new NatsJetStreamContext({ kind: 'jetstream', connection: 'default', raw: raw as never });
  });

  it('is a NatsContext', () => expect(ctx).toBeInstanceOf(NatsContext));

  it('exposes delivery info', () =>
    expect([ctx.stream, ctx.seq, ctx.deliveryCount]).toEqual(['S', 7, 2]));

  it('settles once', () => {
    expect(ctx.settled).toBe(false);
    ctx.ack();
    ctx.nak();
    expect(ctx.settled).toBe(true);
    expect(raw.ack).toHaveBeenCalledTimes(1);
    expect(raw.nak).not.toHaveBeenCalled();
  });

  it('passes nak delay through', () => {
    ctx.nak(500);
    expect(raw.nak).toHaveBeenCalledWith(500);
    expect(ctx.settled).toBe(true);
  });

  it('settles on term and forwards the reason', () => {
    ctx.term('bad');
    ctx.term('again');
    expect(raw.term).toHaveBeenCalledTimes(1);
    expect(raw.term).toHaveBeenCalledWith('bad');
    expect(ctx.settled).toBe(true);
  });

  it('working() does not settle and can repeat', () => {
    ctx.working();
    ctx.working();
    expect(raw.working).toHaveBeenCalledTimes(2);
    expect(ctx.settled).toBe(false);
    ctx.ack();
    expect(raw.ack).toHaveBeenCalledTimes(1);
  });
});
