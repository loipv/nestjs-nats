import { BadRequestException, InternalServerErrorException } from '@nestjs/common';
import { describe, expect, it, vi } from 'vitest';
import { NatsContext, NatsJetStreamContext } from '../../src/context/nats-context.js';
import { NatsDecodeError, NonRetryableError } from '../../src/errors/errors.js';
import { basicSettlement } from '../../src/pipeline/basic-settlement.js';
import type { Delivery } from '../../src/pipeline/delivery.js';

function fakeJsMsg() {
  return {
    subject: 'orders.created',
    reply: '$JS.ACK.S.d.1.1.1.0.0',
    headers: undefined,
    ack: vi.fn(),
    nak: vi.fn(),
    term: vi.fn(),
    working: vi.fn(),
    info: { stream: 'S', streamSequence: 1, deliveryCount: 1 },
  };
}

function logger() {
  return { log: vi.fn(), error: vi.fn(), warn: vi.fn(), debug: vi.fn(), verbose: vi.fn() };
}

function consumeDelivery(ack?: 'auto' | 'manual') {
  const raw = fakeJsMsg();
  const log = logger();
  const context = new NatsJetStreamContext({
    kind: 'jetstream',
    connection: 'default',
    raw: raw as never,
  });
  const d = {
    handler: {
      kind: 'consume',
      options: { stream: 'S', durable: 'd', ack },
      className: 'H',
      methodName: 'on',
    },
    messages: [],
    context,
    logger: log,
  } as unknown as Delivery;
  return { d, raw, log, context };
}

function coreDelivery(kind: 'subscribe' | 'service-endpoint', reply: string) {
  const log = logger();
  const context = new NatsContext({
    kind: kind === 'subscribe' ? 'core' : 'service',
    connection: 'default',
    raw: { subject: 'a.b', reply } as never,
  });
  const d = {
    handler: { kind, className: 'H', methodName: 'on' },
    messages: [],
    context,
    logger: log,
  } as unknown as Delivery;
  return { d, log };
}

const nextWith = (err: unknown) => async () => {
  if (err !== undefined) throw err;
};

describe('basicSettlement', () => {
  it.each([
    ['auto', undefined, 'ack'],
    ['auto', new Error('x'), 'nak'],
    ['auto', new NatsDecodeError('x'), 'term'],
    ['auto', new NonRetryableError('x'), 'term'],
    ['auto', new BadRequestException(), 'term'],
    ['auto', new InternalServerErrorException(), 'nak'],
    ['manual', undefined, null],
    ['manual', new Error('x'), 'nak'],
    ['manual', new NatsDecodeError('x'), 'term'],
  ] as const)('consume ack=%s error=%s → %s', async (ack, err, expected) => {
    const { d, raw, log } = consumeDelivery(ack);
    await expect(basicSettlement.handle(d, nextWith(err))).resolves.toBeUndefined();
    for (const op of ['ack', 'nak', 'term'] as const) {
      expect(raw[op]).toHaveBeenCalledTimes(op === expected ? 1 : 0);
    }
    expect(log.error).toHaveBeenCalledTimes(err === undefined ? 0 : 1);
  });

  it('defaults to auto ack when ack is not set', async () => {
    const { d, raw } = consumeDelivery(undefined);
    await basicSettlement.handle(d, nextWith(undefined));
    expect(raw.ack).toHaveBeenCalledTimes(1);
  });

  it('terminates with the error message as reason', async () => {
    const { d, raw } = consumeDelivery('auto');
    await basicSettlement.handle(d, nextWith(new NonRetryableError('poison')));
    expect(raw.term).toHaveBeenCalledWith('poison');
  });

  it('does not settle again when the handler already settled', async () => {
    const { d, raw, context } = consumeDelivery('auto');
    await basicSettlement.handle(d, async () => {
      context.nak(100);
    });
    expect(raw.nak).toHaveBeenCalledTimes(1);
    expect(raw.ack).not.toHaveBeenCalled();
  });

  it('does not settle on error when the handler already settled', async () => {
    const { d, raw, context } = consumeDelivery('auto');
    await basicSettlement.handle(d, async () => {
      context.term('handled');
      throw new Error('after');
    });
    expect(raw.term).toHaveBeenCalledTimes(1);
    expect(raw.nak).not.toHaveBeenCalled();
  });

  it('logs (does not silently swallow) a failing ack', async () => {
    const { d, raw, log } = consumeDelivery('auto');
    raw.ack.mockImplementation(() => {
      throw new Error('ack failed');
    });
    await expect(basicSettlement.handle(d, nextWith(undefined))).resolves.toBeUndefined();
    expect(log.error).toHaveBeenCalledTimes(1);
    expect(String(log.error.mock.calls[0]?.[0])).toContain('ack');
  });

  it('logs a failing nak after a handler error', async () => {
    const { d, raw, log } = consumeDelivery('auto');
    raw.nak.mockImplementation(() => {
      throw new Error('nak failed');
    });
    await expect(basicSettlement.handle(d, nextWith(new Error('x')))).resolves.toBeUndefined();
    expect(log.error).toHaveBeenCalledTimes(2);
  });

  it('rethrows for reply deliveries', async () => {
    const err = new Error('x');
    const { d, log } = coreDelivery('subscribe', '_INBOX.1');
    await expect(basicSettlement.handle(d, nextWith(err))).rejects.toBe(err);
    expect(log.error).not.toHaveBeenCalled();
  });

  it('rethrows for service endpoints', async () => {
    const err = new Error('x');
    const { d } = coreDelivery('service-endpoint', '_INBOX.1');
    await expect(basicSettlement.handle(d, nextWith(err))).rejects.toBe(err);
  });

  it('swallows for plain subscriptions', async () => {
    const { d, log } = coreDelivery('subscribe', '');
    await expect(basicSettlement.handle(d, nextWith(new Error('x')))).resolves.toBeUndefined();
    expect(log.error).toHaveBeenCalledTimes(1);
  });

  it('does nothing on success for plain subscriptions', async () => {
    const { d, log } = coreDelivery('subscribe', '');
    await expect(basicSettlement.handle(d, nextWith(undefined))).resolves.toBeUndefined();
    expect(log.error).not.toHaveBeenCalled();
  });
});
