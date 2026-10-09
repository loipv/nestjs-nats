import { headers, type MsgHdrs } from '@nats-io/transport-node';
import { BadRequestException } from '@nestjs/common';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { ConnectionEntry } from '../../src/connection/connection-manager.js';
import { NatsContext, NatsJetStreamContext } from '../../src/context/nats-context.js';
import { NatsDecodeError, NatsModuleError, NonRetryableError } from '../../src/errors/errors.js';
import {
  type Delivery,
  HANDLER_DEFINITION,
  type InboundMessage,
  RETRY_SCHEDULER,
} from '../../src/pipeline/delivery.js';
import { type DlqPolicy, publishToDlq, resolveDlqPolicy } from '../../src/settlement/dlq.js';
import { type RetryPolicy, resolveRetryPolicy } from '../../src/settlement/retry.js';
import { createSettlement } from '../../src/settlement/settlement.js';

const retry = resolveRetryPolicy({ initialDelay: 100, jitter: 0 }) as RetryPolicy;
const consumeDlq = resolveDlqPolicy('consume', true) as DlqPolicy;
const coreDlq = resolveDlqPolicy('subscribe', true) as DlqPolicy;

function logger() {
  return { log: vi.fn(), error: vi.fn(), warn: vi.fn(), debug: vi.fn(), verbose: vi.fn() };
}

function fakeEntry(opts: { jsPublish?: () => Promise<unknown> } = {}) {
  const jsPublish = vi.fn(opts.jsPublish ?? (() => Promise.resolve({ stream: 'DLQ', seq: 1 })));
  const ncPublish = vi.fn();
  const entry = {
    name: 'default',
    options: {},
    logger: logger(),
    nc: { publish: ncPublish },
    js: () => ({ publish: jsPublish }),
  } as unknown as ConnectionEntry;
  return { entry, jsPublish, ncPublish };
}

function fakeJsMsg(attempt: number) {
  return {
    subject: 'ord.created',
    reply: '$JS.ACK.ORD.d.1.1.1.0.0',
    data: new TextEncoder().encode('{"id":1}'),
    headers: undefined,
    ack: vi.fn(),
    nak: vi.fn(),
    term: vi.fn(),
    working: vi.fn(),
    info: { stream: 'ORD', consumer: 'd', streamSequence: 7, deliveryCount: attempt },
  };
}

function consumeDelivery(attempt = 1, ack?: 'auto' | 'manual') {
  const raw = fakeJsMsg(attempt);
  const log = logger();
  const context = new NatsJetStreamContext({
    kind: 'jetstream',
    connection: 'default',
    raw: raw as never,
  });
  const handler = {
    kind: 'consume',
    stream: 'ORD',
    durable: 'd',
    className: 'H',
    methodName: 'on',
  };
  const msg: InboundMessage = {
    subject: raw.subject,
    data: raw.data,
    headers: undefined,
    raw: raw as never,
    attempt,
  };
  const d = {
    handler,
    [HANDLER_DEFINITION]: { ...handler, options: { stream: 'ORD', durable: 'd', ack } },
    messages: [msg],
    context,
    logger: log,
  } as unknown as Delivery;
  return { d, raw, log, context, msg };
}

function coreDelivery(opts: { reply?: string; attempt?: number; kind?: string } = {}) {
  const log = logger();
  const raw = { subject: 'orders.x', reply: opts.reply ?? '', data: new Uint8Array([1, 2]) };
  const kind = opts.kind ?? 'subscribe';
  const context = new NatsContext({
    kind: kind === 'subscribe' ? 'core' : 'service',
    connection: 'default',
    raw: raw as never,
  });
  const msg: InboundMessage = {
    subject: raw.subject,
    data: raw.data,
    raw: raw as never,
    attempt: opts.attempt ?? 1,
  };
  const schedule = vi.fn();
  const handler = { kind, subject: 'orders.*', className: 'H', methodName: 'on' };
  const d = {
    handler,
    [HANDLER_DEFINITION]: { ...handler, options: {} },
    [RETRY_SCHEDULER]: schedule,
    messages: [msg],
    context,
    logger: log,
  } as unknown as Delivery;
  return { d, log, msg, schedule };
}

const nextWith = (err: unknown) => async () => {
  if (err !== undefined) throw err;
};

const settled = (raw: ReturnType<typeof fakeJsMsg>) => ({
  ...(raw.ack.mock.calls.length ? { ack: raw.ack.mock.calls.length } : {}),
  ...(raw.nak.mock.calls.length ? { nak: raw.nak.mock.calls.map((c) => c[0]) } : {}),
  ...(raw.term.mock.calls.length ? { term: raw.term.mock.calls.length } : {}),
});

afterEach(() => {
  vi.useRealTimers();
});

describe('createSettlement — consume', () => {
  it.each([
    ['success auto → ack', undefined, 1, { ack: 1 }],
    ['retryable attempt 1 → nak(100)', new Error('x'), 1, { nak: [100] }],
    ['retryable attempt 3 → nak(400)', new Error('x'), 3, { nak: [400] }],
    ['exhausted, dlq off → term', new Error('x'), 6, { term: 1 }],
    ['non-retryable, dlq off → term', new BadRequestException(), 1, { term: 1 }],
    ['decode error → term', new NatsDecodeError('bad'), 1, { term: 1 }],
    ['NonRetryableError → term', new NonRetryableError('no'), 1, { term: 1 }],
  ])('consume: %s', async (_, err, attempt, expected) => {
    const { entry } = fakeEntry();
    const { d, raw } = consumeDelivery(attempt);
    const s = createSettlement({ entry, retry, dlq: false });
    await expect(s.handle(d, nextWith(err))).resolves.toBeUndefined();
    expect(settled(raw)).toEqual(expected);
  });

  it('retry: false terms on the first failure', async () => {
    const { entry } = fakeEntry();
    const { d, raw } = consumeDelivery(1);
    await createSettlement({ entry, retry: false, dlq: false }).handle(d, nextWith(new Error('x')));
    expect(settled(raw)).toEqual({ term: 1 });
  });

  it('terms with the error message as reason when dropping', async () => {
    const { entry } = fakeEntry();
    const { d, raw } = consumeDelivery(1);
    await createSettlement({ entry, retry, dlq: false }).handle(
      d,
      nextWith(new NonRetryableError('poison')),
    );
    expect(raw.term).toHaveBeenCalledWith('poison');
  });

  it('defaults to auto ack when ack is not set', async () => {
    const { entry } = fakeEntry();
    const { d, raw } = consumeDelivery(1, undefined);
    await createSettlement({ entry, retry, dlq: false }).handle(d, nextWith(undefined));
    expect(raw.ack).toHaveBeenCalledTimes(1);
  });

  it('exhausted with dlq → publishes then terms', async () => {
    const order: string[] = [];
    const { entry, jsPublish } = fakeEntry({
      jsPublish: () => {
        order.push('publish');
        return Promise.resolve({ stream: 'DLQ', seq: 1 });
      },
    });
    const { d, raw, log } = consumeDelivery(6);
    raw.term.mockImplementation(() => order.push('term'));
    await createSettlement({ entry, retry, dlq: consumeDlq }).handle(d, nextWith(new Error('x')));
    expect(jsPublish).toHaveBeenCalledTimes(1);
    const [subject, data, opts] = jsPublish.mock.calls[0] as unknown as [
      string,
      Uint8Array,
      { headers: { get(k: string): string } },
    ];
    expect(subject).toBe('dlq.ORD.d');
    expect(data).toBe(raw.data);
    expect(opts.headers.get('Nats-DLQ-Deliveries')).toBe('6');
    expect(opts.headers.get('Nats-DLQ-Error')).toBe('x');
    expect(order).toEqual(['publish', 'term']);
    expect(raw.nak).not.toHaveBeenCalled();
    expect(log.error).toHaveBeenCalledTimes(1);
    expect(log.error.mock.calls[0]?.[0]).toBe(
      'H.on on "ord.created" failed, sent to DLQ dlq.ORD.d after 6 attempts: x',
    );
  });

  it('non-retryable with dlq → publishes on the first attempt', async () => {
    const { entry, jsPublish } = fakeEntry();
    const { d, raw } = consumeDelivery(1);
    await createSettlement({ entry, retry, dlq: consumeDlq }).handle(
      d,
      nextWith(new BadRequestException('nope')),
    );
    expect(jsPublish).toHaveBeenCalledTimes(1);
    expect(settled(raw)).toEqual({ term: 1 });
  });

  it('dlq publish failure → nak with backoff and error log', async () => {
    const { entry } = fakeEntry({ jsPublish: () => Promise.reject(new Error('pub down')) });
    const { d, raw, log } = consumeDelivery(6);
    await createSettlement({ entry, retry, dlq: consumeDlq }).handle(d, nextWith(new Error('x')));
    // backoff(6) with initialDelay 100, factor 2: 3200 ms.
    expect(settled(raw)).toEqual({ nak: [3200] });
    expect(log.error).toHaveBeenCalledTimes(1);
    const line = String(log.error.mock.calls[0]?.[0]);
    expect(line).toContain('dlq.ORD.d');
    expect(line).toContain('pub down');
    expect(line).toContain('retrying in 3200ms');
  });

  it('dlq publish failure with retry off naks with the default backoff', async () => {
    const { entry } = fakeEntry({ jsPublish: () => Promise.reject(new Error('pub down')) });
    const { d, raw } = consumeDelivery(1);
    await createSettlement({ entry, retry: false, dlq: consumeDlq, random: () => 0.5 }).handle(
      d,
      nextWith(new Error('x')),
    );
    expect(settled(raw)).toEqual({ nak: [1000] });
  });

  it('manual ack success does nothing', async () => {
    const { entry } = fakeEntry();
    const { d, raw } = consumeDelivery(1, 'manual');
    await createSettlement({ entry, retry, dlq: false }).handle(d, nextWith(undefined));
    expect(settled(raw)).toEqual({});
  });

  it('manual ack failure follows the settlement table', async () => {
    const { entry } = fakeEntry();
    const { d, raw } = consumeDelivery(1, 'manual');
    await createSettlement({ entry, retry, dlq: false }).handle(d, nextWith(new Error('x')));
    expect(settled(raw)).toEqual({ nak: [100] });
  });

  it('does not resettle a message the handler settled', async () => {
    const { entry } = fakeEntry();
    const { d, raw, context, log } = consumeDelivery(1);
    await createSettlement({ entry, retry, dlq: consumeDlq }).handle(d, async () => {
      context.nak(5);
      throw new Error('after');
    });
    expect(settled(raw)).toEqual({ nak: [5] });
    expect(log.warn).not.toHaveBeenCalled();
    expect(log.error).not.toHaveBeenCalled();
  });

  it('does not ack again when the handler settled on success', async () => {
    const { entry } = fakeEntry();
    const { d, raw, context } = consumeDelivery(1);
    await createSettlement({ entry, retry, dlq: false }).handle(d, async () => {
      context.term('handled');
    });
    expect(settled(raw)).toEqual({ term: 1 });
  });

  it('logs (does not swallow silently) a failing ack', async () => {
    const { entry } = fakeEntry();
    const { d, raw, log } = consumeDelivery(1);
    raw.ack.mockImplementation(() => {
      throw new Error('ack failed');
    });
    await expect(
      createSettlement({ entry, retry, dlq: false }).handle(d, nextWith(undefined)),
    ).resolves.toBeUndefined();
    expect(log.error).toHaveBeenCalledTimes(1);
    expect(String(log.error.mock.calls[0]?.[0])).toMatch(/Failed to ack .*H\.on/);
    expect(String(log.error.mock.calls[0]?.[1])).toContain('ack failed');
  });

  it('logs a failing nak after a handler error', async () => {
    const { entry } = fakeEntry();
    const { d, raw, log } = consumeDelivery(1);
    raw.nak.mockImplementation(() => {
      throw new Error('nak failed');
    });
    await expect(
      createSettlement({ entry, retry, dlq: false }).handle(d, nextWith(new Error('x'))),
    ).resolves.toBeUndefined();
    expect(log.error).toHaveBeenCalledTimes(1);
    expect(String(log.error.mock.calls[0]?.[0])).toMatch(/Failed to nak/);
  });

  it('logs a failing term after dead-lettering', async () => {
    const { entry } = fakeEntry();
    const { d, raw, log } = consumeDelivery(6);
    raw.term.mockImplementation(() => {
      throw new Error('term failed');
    });
    await createSettlement({ entry, retry, dlq: consumeDlq }).handle(d, nextWith(new Error('x')));
    expect(log.error).toHaveBeenCalledWith(
      expect.stringMatching(/Failed to term/),
      expect.stringContaining('term failed'),
    );
  });

  it('heartbeats while the handler runs', async () => {
    vi.useFakeTimers();
    const { entry } = fakeEntry();
    const { d, raw } = consumeDelivery(1);
    const s = createSettlement({ entry, retry, dlq: false, heartbeatMs: 100 });
    const done = s.handle(d, () => new Promise<void>((resolve) => setTimeout(resolve, 350)));
    await vi.advanceTimersByTimeAsync(350);
    await done;
    expect(raw.working).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(1000);
    expect(raw.working).toHaveBeenCalledTimes(3);
    expect(raw.ack).toHaveBeenCalledTimes(1);
  });

  it('keeps heartbeating until settlement completes, including the DLQ publish', async () => {
    vi.useFakeTimers();
    let resolvePublish!: () => void;
    const { entry } = fakeEntry({
      jsPublish: () =>
        new Promise((resolve) => {
          resolvePublish = () => resolve({ stream: 'DLQ', seq: 1 });
        }),
    });
    const { d, raw } = consumeDelivery(6);
    const s = createSettlement({ entry, retry, dlq: consumeDlq, heartbeatMs: 100 });
    const done = s.handle(d, nextWith(new Error('x')));
    // The handler failed at once; the slow PubAck must not let the ack deadline lapse.
    await vi.advanceTimersByTimeAsync(250);
    expect(raw.working).toHaveBeenCalledTimes(2);
    expect(raw.term).not.toHaveBeenCalled();
    resolvePublish();
    await done;
    expect(raw.term).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1000);
    expect(raw.working).toHaveBeenCalledTimes(2);
  });

  it('stops heartbeating a message the handler already settled', async () => {
    vi.useFakeTimers();
    const { entry } = fakeEntry();
    const { d, raw, context } = consumeDelivery(1, 'manual');
    const s = createSettlement({ entry, retry, dlq: false, heartbeatMs: 100 });
    const done = s.handle(d, async () => {
      await new Promise<void>((resolve) => setTimeout(resolve, 150));
      context.ack();
      await new Promise<void>((resolve) => setTimeout(resolve, 300));
    });
    await vi.advanceTimersByTimeAsync(450);
    await done;
    expect(raw.working).toHaveBeenCalledTimes(1);
  });

  it('logs one settlement line, not the stack', async () => {
    const { entry } = fakeEntry();
    const { d, log } = consumeDelivery(1);
    await createSettlement({ entry, retry, dlq: false }).handle(d, nextWith(new Error('boom')));
    expect(log.warn).toHaveBeenCalledTimes(1);
    expect(log.warn).toHaveBeenCalledWith(
      'H.on on "ord.created" failed (attempt 1/6), retrying in 100ms: boom',
    );
    expect(log.error).not.toHaveBeenCalled();
  });

  it('logs an error line when giving up', async () => {
    const { entry } = fakeEntry();
    const { d, log } = consumeDelivery(6);
    await createSettlement({ entry, retry, dlq: false }).handle(d, nextWith(new Error('boom')));
    expect(log.error).toHaveBeenCalledWith(
      'H.on on "ord.created" failed, gave up after 6 attempts: boom',
    );
    expect(log.error.mock.calls[0]).toHaveLength(1);
  });
});

describe('createSettlement — subscribe', () => {
  it('core retryable → schedules retry', async () => {
    const { entry } = fakeEntry();
    const { d, msg, schedule, log } = coreDelivery();
    await createSettlement({ entry, retry, dlq: false }).handle(d, nextWith(new Error('x')));
    expect(schedule).toHaveBeenCalledWith(msg, 100);
    expect(log.warn).toHaveBeenCalledWith(
      'H.on on "orders.x" failed (attempt 1/6), retrying in 100ms: x',
    );
  });

  it('core exhausted with dlq → publishes', async () => {
    const { entry, ncPublish, jsPublish } = fakeEntry();
    const { d, schedule, log } = coreDelivery({ attempt: 6 });
    await createSettlement({ entry, retry, dlq: coreDlq }).handle(d, nextWith(new Error('x')));
    expect(schedule).not.toHaveBeenCalled();
    expect(jsPublish).not.toHaveBeenCalled();
    expect(ncPublish).toHaveBeenCalledTimes(1);
    const [subject, data, opts] = ncPublish.mock.calls[0] as unknown as [
      string,
      Uint8Array,
      { headers: { get(k: string): string } },
    ];
    expect(subject).toBe('dlq.orders.x');
    expect(Array.from(data)).toEqual([1, 2]);
    expect(opts.headers.get('Nats-DLQ-Deliveries')).toBe('6');
    expect(log.error).toHaveBeenCalledWith(
      'H.on on "orders.x" failed, sent to DLQ dlq.orders.x after 6 attempts: x',
    );
  });

  it('core exhausted without dlq → logs and drops', async () => {
    const { entry, ncPublish } = fakeEntry();
    const { d, schedule, log } = coreDelivery({ attempt: 6 });
    await createSettlement({ entry, retry, dlq: false }).handle(d, nextWith(new Error('x')));
    expect(schedule).not.toHaveBeenCalled();
    expect(ncPublish).not.toHaveBeenCalled();
    expect(log.error).toHaveBeenCalledWith(
      'H.on on "orders.x" failed, gave up after 6 attempts: x',
    );
  });

  it('core dlq publish failure → logs an error', async () => {
    const { entry, ncPublish } = fakeEntry();
    ncPublish.mockImplementation(() => {
      throw new Error('closed');
    });
    const { d, log } = coreDelivery({ attempt: 6 });
    await expect(
      createSettlement({ entry, retry, dlq: coreDlq }).handle(d, nextWith(new Error('x'))),
    ).resolves.toBeUndefined();
    expect(log.error).toHaveBeenCalledTimes(1);
    expect(String(log.error.mock.calls[0]?.[0])).toMatch(/dlq\.orders\.x.*closed/);
  });

  it('reply deliveries rethrow without retry', async () => {
    const err = new Error('x');
    const { entry } = fakeEntry();
    const { d, schedule, log } = coreDelivery({ reply: '_INBOX.1' });
    await expect(
      createSettlement({ entry, retry, dlq: coreDlq }).handle(d, nextWith(err)),
    ).rejects.toBe(err);
    expect(schedule).not.toHaveBeenCalled();
    expect(log.warn).not.toHaveBeenCalled();
    expect(log.error).not.toHaveBeenCalled();
  });

  it('rethrows for service endpoints', async () => {
    const err = new Error('x');
    const { entry } = fakeEntry();
    const { d } = coreDelivery({ kind: 'service-endpoint', reply: '_INBOX.1' });
    await expect(
      createSettlement({ entry, retry: false, dlq: false }).handle(d, nextWith(err)),
    ).rejects.toBe(err);
  });

  it('does nothing on success', async () => {
    const { entry } = fakeEntry();
    const { d, schedule, log } = coreDelivery();
    await createSettlement({ entry, retry, dlq: coreDlq }).handle(d, nextWith(undefined));
    expect(schedule).not.toHaveBeenCalled();
    expect(log.error).not.toHaveBeenCalled();
    expect(log.warn).not.toHaveBeenCalled();
  });
});

describe('publishToDlq', () => {
  const info = { kind: 'consume', stream: 'ORD', durable: 'd' } as never;

  it('maps "no responders" to a NatsModuleError naming the subject', async () => {
    const { NoRespondersError, RequestError } = await import('@nats-io/transport-node');
    const cause = new RequestError('no responders', {
      cause: new NoRespondersError('nowhere.x'),
    });
    const { entry } = fakeEntry({
      jsPublish: () => Promise.reject(new Error('jetstream is not enabled', { cause })),
    });
    const { msg } = consumeDelivery(1);
    const dlq: DlqPolicy = { subject: () => 'nowhere.x', mode: 'jetstream' };
    const p = publishToDlq(entry, dlq, info, msg, new Error('x'), 1);
    await expect(p).rejects.toBeInstanceOf(NatsModuleError);
    await expect(p).rejects.toThrow(/nowhere\.x.*no stream captures this subject/);
  });

  it('passes other publish errors through', async () => {
    const err = new Error('timeout');
    const { entry } = fakeEntry({ jsPublish: () => Promise.reject(err) });
    const { msg } = consumeDelivery(1);
    await expect(publishToDlq(entry, consumeDlq, info, msg, new Error('x'), 1)).rejects.toBe(err);
  });

  it('keeps the original headers and body', async () => {
    const { entry, jsPublish } = fakeEntry();
    const { msg } = consumeDelivery(2);
    const h = headers();
    h.set('x-trace', 'abc');
    msg.headers = h;
    await publishToDlq(entry, consumeDlq, info, msg, new Error('x'), 2);
    const call = jsPublish.mock.calls[0] as unknown as [unknown, unknown, { headers: MsgHdrs }];
    const opts = call[2];
    expect(opts.headers.get('x-trace')).toBe('abc');
    expect(opts.headers.get('Nats-DLQ-Stream')).toBe('ORD');
  });
});
