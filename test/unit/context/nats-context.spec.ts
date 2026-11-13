import { headers } from '@nats-io/transport-node';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ABORT_HANDLER,
  CLOUD_EVENT,
  NatsContext,
  NatsJetStreamContext,
} from '../../../src/context/nats-context.js';

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

  describe('cloudEvent', () => {
    const ceHeaders = () => {
      const h = headers();
      h.set('ce-specversion', '1.0');
      h.set('ce-id', '1');
      h.set('ce-source', 's');
      h.set('ce-type', 't');
      return h;
    };
    const ctxOf = (h?: ReturnType<typeof headers>) =>
      new NatsContext({
        kind: 'core',
        connection: 'default',
        raw: { subject: 'a', reply: '', data: new Uint8Array(), headers: h } as never,
      });

    it('is read lazily, once, from the raw message', () => {
      const ctx = ctxOf(ceHeaders());
      const first = ctx.cloudEvent;
      expect(first).toMatchObject({ id: '1', source: 's', type: 't', specversion: '1.0' });
      expect(ctx.cloudEvent).toBe(first);
    });

    it('is undefined for a message that is not a CloudEvent', () => {
      expect(ctxOf().cloudEvent).toBeUndefined();
      expect(ctxOf(headers()).cloudEvent).toBeUndefined();
    });

    it('can be preset through the internal setter', () => {
      const ctx = ctxOf();
      const meta = { id: 'x', source: 's', type: 't', specversion: '1.0', extensions: {} };
      ctx[CLOUD_EVENT] = meta;
      expect(ctx.cloudEvent).toBe(meta);
    });
  });

  it('returns undefined replyTo when there is no reply subject', () => {
    const ctx = new NatsContext({
      kind: 'core',
      connection: 'default',
      raw: { subject: 'a', reply: '' } as never,
    });
    expect(ctx.replyTo).toBeUndefined();
  });

  it('has no replyTo for a jetstream-kind context', () => {
    const ctx = new NatsContext({
      kind: 'jetstream',
      connection: 'default',
      raw: { subject: 'a', reply: '$JS.ACK.S.d.1.1.1.0.0' } as never,
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

  it('has no replyTo even though the message carries the $JS.ACK subject', () => {
    const jsRaw = { ...fakeJsMsg(), reply: '$JS.ACK.S.d.1.7.7.0.0' };
    const jsCtx = new NatsJetStreamContext({
      kind: 'jetstream',
      connection: 'default',
      raw: jsRaw as never,
    });
    expect(jsCtx.replyTo).toBeUndefined();
  });

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

  it('stays unsettled when raw ack throws', () => {
    raw.ack.mockImplementation(() => {
      throw new Error('closed');
    });
    expect(() => ctx.ack()).toThrow('closed');
    expect(ctx.settled).toBe(false);
  });

  it('stays unsettled when raw nak throws, and can be settled later', () => {
    raw.nak.mockImplementationOnce(() => {
      throw new Error('closed');
    });
    expect(() => ctx.nak(100)).toThrow('closed');
    expect(ctx.settled).toBe(false);
    ctx.nak();
    expect(ctx.settled).toBe(true);
    expect(raw.nak).toHaveBeenCalledTimes(2);
  });

  it('stays unsettled when raw term throws', () => {
    raw.term.mockImplementation(() => {
      throw new Error('closed');
    });
    expect(() => ctx.term('bad')).toThrow('closed');
    expect(ctx.settled).toBe(false);
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

describe.each([
  [
    'NatsContext',
    () =>
      new NatsContext({
        kind: 'core',
        connection: 'default',
        raw: { subject: 's', reply: '' } as never,
      }),
  ],
  [
    'NatsJetStreamContext',
    () =>
      new NatsJetStreamContext({
        kind: 'jetstream',
        connection: 'default',
        raw: fakeJsMsg() as never,
      }),
  ],
])('%s signal', (_, make) => {
  it('is a signal that is not aborted', () => {
    const { signal } = make();
    expect(signal).toBeInstanceOf(AbortSignal);
    expect(signal.aborted).toBe(false);
  });

  it('is the same signal on every read', () => {
    const ctx = make();
    expect(ctx.signal).toBe(ctx.signal);
  });

  it('is aborted with the reason given to ABORT_HANDLER', () => {
    const ctx = make();
    const { signal } = ctx;
    const reason = new Error('timed out');
    ctx[ABORT_HANDLER](reason);
    expect(signal.aborted).toBe(true);
    expect(signal.reason).toBe(reason);
  });

  it('is already aborted when first read after ABORT_HANDLER', () => {
    const ctx = make();
    const reason = new Error('timed out');
    ctx[ABORT_HANDLER](reason);
    expect(ctx.signal.aborted).toBe(true);
    expect(ctx.signal.reason).toBe(reason);
  });

  it('keeps the first reason', () => {
    const ctx = make();
    const first = new Error('first');
    ctx[ABORT_HANDLER](first);
    ctx[ABORT_HANDLER](new Error('second'));
    expect(ctx.signal.reason).toBe(first);
  });
});
