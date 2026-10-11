import { headers, type MsgHdrs } from '@nats-io/transport-node';
import { BadRequestException } from '@nestjs/common';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { type Codec, jsonCodec } from '../../src/codec/codec.js';
import type { ConnectionEntry } from '../../src/connection/connection-registry.js';
import { NatsContext, NatsJetStreamContext } from '../../src/context/nats-context.js';
import { NatsOrderedContext } from '../../src/context/nats-ordered-context.js';
import { NatsDecodeError, NatsModuleError, NonRetryableError } from '../../src/errors/errors.js';
import { IdempotencyStoreError } from '../../src/idempotency/idempotency.js';
import {
  ABORT_SIGNAL,
  type Delivery,
  HANDLER_DEFINITION,
  type InboundMessage,
  RETRY_SCHEDULER,
} from '../../src/pipeline/delivery.js';
import type { NatsPlugin, OutgoingMessage } from '../../src/pipeline/types.js';
import { type DlqPolicy, publishToDlq, resolveDlqPolicy } from '../../src/settlement/dlq.js';
import { type RetryPolicy, resolveRetryPolicy } from '../../src/settlement/retry.js';
import { createSettlement } from '../../src/settlement/settlement.js';

const retry = resolveRetryPolicy({ initialDelay: 100, jitter: 0 }) as RetryPolicy;
const consumeDlq = resolveDlqPolicy('consume', true) as DlqPolicy;
const coreDlq = resolveDlqPolicy('subscribe', true) as DlqPolicy;
const orderedDlq = resolveDlqPolicy('ordered', undefined, true) as DlqPolicy;

function logger() {
  return { log: vi.fn(), error: vi.fn(), warn: vi.fn(), debug: vi.fn(), verbose: vi.fn() };
}

function fakeEntry(
  opts: { jsPublish?: () => Promise<unknown>; plugins?: NatsPlugin[]; codec?: Codec } = {},
) {
  const jsPublish = vi.fn(opts.jsPublish ?? (() => Promise.resolve({ stream: 'DLQ', seq: 1 })));
  const ncPublish = vi.fn();
  const entry = {
    name: 'default',
    options: { plugins: opts.plugins, codec: opts.codec },
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
    outcomes: new Map(),
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
    outcomes: new Map(),
    context,
    logger: log,
  } as unknown as Delivery;
  return { d, log, msg, schedule };
}

function orderedDelivery(opts: { attempt?: number; signal?: AbortSignal | null } = {}) {
  const log = logger();
  const attempt = opts.attempt ?? 1;
  const raw = {
    subject: 'proj.created',
    reply: '$JS.ACK.ORDERS.ordered_x.1.42.7.0.0',
    data: new Uint8Array([1, 2]),
    headers: undefined,
    info: {
      stream: 'ORDERS',
      consumer: 'ordered_x',
      streamSequence: 42,
      deliverySequence: 7,
      pending: 0,
    },
  };
  const context = new NatsOrderedContext({ connection: 'default', raw: raw as never, attempt });
  const msg: InboundMessage = { subject: raw.subject, data: raw.data, raw: raw as never, attempt };
  const schedule = vi.fn();
  const controller = new AbortController();
  const signal = opts.signal === undefined ? controller.signal : opts.signal;
  const handler = {
    kind: 'ordered',
    stream: 'ORDERS',
    filterSubjects: [],
    className: 'Proj',
    methodName: 'on',
  };
  const d = {
    handler,
    [HANDLER_DEFINITION]: { ...handler, options: { stream: 'ORDERS' } },
    [RETRY_SCHEDULER]: schedule,
    ...(signal ? { [ABORT_SIGNAL]: signal } : {}),
    messages: [msg],
    outcomes: new Map(),
    context,
    logger: log,
  } as unknown as Delivery;
  return { d, log, msg, schedule, controller };
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

describe('createSettlement — outcomes', () => {
  it('records the decision it settled with, next to the raw call', async () => {
    const { entry } = fakeEntry();
    const failing = consumeDelivery(1);
    const err = new Error('x');
    await createSettlement({ entry, retry, dlq: false }).handle(failing.d, nextWith(err));
    expect(failing.d.outcomes.get(failing.msg)).toEqual({
      action: 'retry',
      delayMs: 100,
      error: err,
    });
    expect(failing.raw.nak).toHaveBeenCalledExactlyOnceWith(100);

    const ok = consumeDelivery(1);
    await createSettlement({ entry, retry, dlq: false }).handle(ok.d, nextWith(undefined));
    expect(ok.d.outcomes.get(ok.msg)).toEqual({ action: 'ack' });
  });

  it('records a core retry and a replied request', async () => {
    const { entry } = fakeEntry();
    const core = coreDelivery();
    const err = new Error('x');
    await createSettlement({ entry, retry, dlq: false }).handle(core.d, nextWith(err));
    expect(core.d.outcomes.get(core.msg)).toEqual({ action: 'retry', delayMs: 100, error: err });

    const reply = coreDelivery({ reply: '_INBOX.1' });
    await expect(
      createSettlement({ entry, retry, dlq: false }).handle(reply.d, nextWith(err)),
    ).rejects.toBe(err);
    expect(reply.d.outcomes.get(reply.msg)).toEqual({ action: 'replied', error: err });
  });
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

  it('bounds the term reason: CR/LF stripped, at most 256 chars', async () => {
    const { entry } = fakeEntry();
    const { d, raw } = consumeDelivery(1);
    const long = `line1\r\nline2\n${'x'.repeat(1000)}`;
    await createSettlement({ entry, retry, dlq: false }).handle(
      d,
      nextWith(new NonRetryableError(long)),
    );
    const reason = raw.term.mock.calls[0]?.[0] as string;
    expect(reason).toHaveLength(256);
    expect(reason).not.toMatch(/[\r\n]/);
    expect(reason.startsWith('line1 line2 xxx')).toBe(true);
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

  it('keeps the error of a handler that acked and then threw', async () => {
    const { entry } = fakeEntry();
    const { d, raw, context, msg } = consumeDelivery(1);
    const err = new Error('after ack');
    await createSettlement({ entry, retry, dlq: false }).handle(d, async () => {
      context.ack();
      throw err;
    });
    expect(settled(raw)).toEqual({ ack: 1 });
    expect(d.outcomes.get(msg)).toEqual({ action: 'ack', error: err });
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

describe('createSettlement — ordered', () => {
  const twoRetries = resolveRetryPolicy({
    initialDelay: 100,
    jitter: 0,
    maxRetries: 2,
  }) as RetryPolicy;
  const dlqHeaders = (call: unknown) =>
    (call as [string, Uint8Array, { headers: MsgHdrs }])[2].headers;

  it('does nothing on success', async () => {
    const { entry, jsPublish } = fakeEntry();
    const { d, msg, schedule } = orderedDelivery();
    await createSettlement({ entry, retry: twoRetries, dlq: orderedDlq }).handle(
      d,
      nextWith(undefined),
    );
    expect(d.outcomes.get(msg)).toEqual({ action: 'none' });
    expect(schedule).not.toHaveBeenCalled();
    expect(jsPublish).not.toHaveBeenCalled();
  });

  it('retries a retryable failure through the scheduler', async () => {
    const { entry, jsPublish } = fakeEntry();
    const { d, msg, schedule, log } = orderedDelivery({ attempt: 1 });
    const err = new Error('x');
    await createSettlement({ entry, retry: twoRetries, dlq: orderedDlq }).handle(d, nextWith(err));
    expect(schedule).toHaveBeenCalledExactlyOnceWith(msg, 100);
    expect(d.outcomes.get(msg)).toEqual({ action: 'retry', delayMs: 100, error: err });
    expect(jsPublish).not.toHaveBeenCalled();
    expect(log.warn).toHaveBeenCalledWith(
      'Proj.on on "proj.created" failed (attempt 1/3), retrying in 100ms: x',
    );
  });

  it('dead-letters once the retries are exhausted, under its own consumer name', async () => {
    const { entry, jsPublish } = fakeEntry();
    const { d, msg, schedule, log } = orderedDelivery({ attempt: 3 });
    const err = new Error('x');
    await createSettlement({ entry, retry: twoRetries, dlq: orderedDlq }).handle(d, nextWith(err));
    expect(schedule).not.toHaveBeenCalled();
    expect(jsPublish).toHaveBeenCalledTimes(1);
    expect(jsPublish).toHaveBeenCalledWith('dlq.ORDERS.ordered.Proj.on', expect.anything(), {
      headers: expect.anything(),
    });
    const h = dlqHeaders(jsPublish.mock.calls[0]);
    expect(h.get('Nats-DLQ-Consumer')).toBe('ordered:Proj.on');
    expect(h.get('Nats-Msg-Id')).toBe('dlq:ORDERS:ordered:Proj.on:42');
    expect(h.get('Nats-DLQ-Seq')).toBe('42');
    expect(h.get('Nats-DLQ-Deliveries')).toBe('3');
    expect(d.outcomes.get(msg)).toEqual({
      action: 'dlq',
      subject: 'dlq.ORDERS.ordered.Proj.on',
      error: err,
    });
    expect(log.error).toHaveBeenCalledWith(
      'Proj.on on "proj.created" failed, sent to DLQ dlq.ORDERS.ordered.Proj.on after 3 attempts: x',
    );
  });

  it('dead-letters a NonRetryableError on the first attempt', async () => {
    const { entry, jsPublish } = fakeEntry();
    const { d, msg, schedule } = orderedDelivery({ attempt: 1 });
    const err = new NonRetryableError('poison');
    await createSettlement({ entry, retry: twoRetries, dlq: orderedDlq }).handle(d, nextWith(err));
    expect(schedule).not.toHaveBeenCalled();
    expect(jsPublish).toHaveBeenCalledTimes(1);
    expect(d.outcomes.get(msg)).toMatchObject({ action: 'dlq', error: err });
  });

  it('drops a NonRetryableError with an error log when the DLQ is off', async () => {
    const { entry, jsPublish } = fakeEntry();
    const { d, msg, schedule, log } = orderedDelivery({ attempt: 1 });
    const err = new NonRetryableError('poison');
    await createSettlement({ entry, retry: twoRetries, dlq: false }).handle(d, nextWith(err));
    expect(schedule).not.toHaveBeenCalled();
    expect(jsPublish).not.toHaveBeenCalled();
    expect(d.outcomes.get(msg)).toEqual({ action: 'drop', error: err });
    expect(log.error).toHaveBeenCalledWith(
      'Proj.on on "proj.created" failed, gave up after 1 attempt: poison',
    );
  });

  it('retries a failing DLQ publish with backoff until it succeeds', async () => {
    vi.useFakeTimers();
    let calls = 0;
    const { entry, jsPublish } = fakeEntry({
      jsPublish: () =>
        ++calls <= 2
          ? Promise.reject(new Error('no stream'))
          : Promise.resolve({ stream: 'DLQ', seq: 1 }),
    });
    const { d, msg, log } = orderedDelivery({ attempt: 3 });
    const err = new Error('x');
    let done = false;
    const p = createSettlement({ entry, retry: twoRetries, dlq: orderedDlq })
      .handle(d, nextWith(err))
      .then(() => {
        done = true;
      });
    await vi.advanceTimersByTimeAsync(0);
    expect(jsPublish).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(100);
    expect(jsPublish).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(199);
    expect(jsPublish).toHaveBeenCalledTimes(2);
    expect(done).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await p;
    expect(jsPublish).toHaveBeenCalledTimes(3);
    const failures = log.error.mock.calls.filter((c) => String(c[0]).includes('could not be sent'));
    expect(failures).toHaveLength(2);
    expect(String(failures[0]?.[0])).toContain('retrying in 100ms');
    expect(String(failures[0]?.[0])).toContain('no stream');
    expect(String(failures[1]?.[0])).toContain('retrying in 200ms');
    expect(d.outcomes.get(msg)).toEqual({
      action: 'dlq',
      subject: 'dlq.ORDERS.ordered.Proj.on',
      error: err,
    });
  });

  it('caps the DLQ publish backoff at maxDelay and uses the default policy when retries are off', async () => {
    vi.useFakeTimers();
    let calls = 0;
    const { entry, jsPublish } = fakeEntry({
      jsPublish: () =>
        ++calls <= 1
          ? Promise.reject(new Error('no stream'))
          : Promise.resolve({ stream: 'DLQ', seq: 1 }),
    });
    const { d, log } = orderedDelivery({ attempt: 1 });
    const p = createSettlement({
      entry,
      retry: false,
      dlq: orderedDlq,
      random: () => 0.5,
    }).handle(d, nextWith(new Error('x')));
    await vi.advanceTimersByTimeAsync(1000);
    await p;
    expect(jsPublish).toHaveBeenCalledTimes(2);
    expect(String(log.error.mock.calls[0]?.[0])).toContain('retrying in 1000ms');
  });

  it('abandons the DLQ publish retry when the signal aborts', async () => {
    vi.useFakeTimers();
    const { entry, jsPublish } = fakeEntry({
      jsPublish: () => Promise.reject(new Error('no stream')),
    });
    const { d, msg, log, controller } = orderedDelivery({ attempt: 3 });
    const err = new Error('x');
    const p = createSettlement({ entry, retry: twoRetries, dlq: orderedDlq }).handle(
      d,
      nextWith(err),
    );
    await vi.advanceTimersByTimeAsync(50);
    controller.abort();
    await p;
    expect(jsPublish).toHaveBeenCalledTimes(1);
    expect(d.outcomes.get(msg)).toEqual({ action: 'drop', error: err });
    expect(log.warn).toHaveBeenCalledTimes(1);
    expect(String(log.warn.mock.calls[0]?.[0])).toContain('abandoned at shutdown');
    expect(String(log.warn.mock.calls[0]?.[0])).toContain('42');
    expect(vi.getTimerCount()).toBe(0);
  });

  it('abandons at once, without a retry line, when the signal has already aborted', async () => {
    const { entry, jsPublish } = fakeEntry({
      jsPublish: () => Promise.reject(new Error('no stream')),
    });
    const { d, msg, log, controller } = orderedDelivery({ attempt: 3 });
    controller.abort();
    const err = new Error('x');
    await createSettlement({ entry, retry: twoRetries, dlq: orderedDlq }).handle(d, nextWith(err));
    expect(jsPublish).toHaveBeenCalledTimes(1);
    expect(d.outcomes.get(msg)).toEqual({ action: 'drop', error: err });
    expect(log.error).not.toHaveBeenCalled();
    expect(log.warn).toHaveBeenCalledExactlyOnceWith(
      expect.stringMatching(
        /not sent to DLQ .*: retry abandoned at shutdown \(stream sequence 42\): no stream$/,
      ),
    );
  });

  it('drops like a core message when the DLQ publish fails without a signal', async () => {
    const { entry, jsPublish } = fakeEntry({
      jsPublish: () => Promise.reject(new Error('no stream')),
    });
    const { d, msg, log } = orderedDelivery({ attempt: 3, signal: null });
    const err = new Error('x');
    await createSettlement({ entry, retry: twoRetries, dlq: orderedDlq }).handle(d, nextWith(err));
    expect(jsPublish).toHaveBeenCalledTimes(1);
    expect(dlqHeaders(jsPublish.mock.calls[0]).get('Nats-DLQ-Consumer')).toBe('ordered:Proj.on');
    expect(d.outcomes.get(msg)).toEqual({ action: 'drop', error: err });
    expect(log.error).toHaveBeenCalledTimes(1);
    expect(String(log.error.mock.calls[0]?.[0])).toMatch(/could not be sent to DLQ.*no stream/);
  });

  it('reschedules on an idempotency store failure, like a core message', async () => {
    const { entry, jsPublish } = fakeEntry();
    const { d, msg, schedule } = orderedDelivery({ attempt: 9 });
    const err = new IdempotencyStoreError(new Error('redis down'));
    await createSettlement({ entry, retry: twoRetries, dlq: orderedDlq }).handle(d, nextWith(err));
    expect(schedule).toHaveBeenCalledTimes(1);
    expect(jsPublish).not.toHaveBeenCalled();
    expect(d.outcomes.get(msg)).toMatchObject({ action: 'retry', error: err });
  });
});

describe('createSettlement — idempotency store unavailable', () => {
  const storeDown = () => new IdempotencyStoreError(new Error('redis down'));
  const never = vi.fn(() => false);
  const strict = resolveRetryPolicy({
    initialDelay: 100,
    maxDelay: 1000,
    jitter: 0,
    maxRetries: 5,
    isRetryable: never,
  }) as RetryPolicy;

  it('naks with the backoff even when isRetryable says false, without consulting it', async () => {
    const { entry, jsPublish } = fakeEntry();
    const { d, raw, log } = consumeDelivery(2);
    never.mockClear();
    await createSettlement({ entry, retry: strict, dlq: consumeDlq }).handle(
      d,
      nextWith(storeDown()),
    );
    expect(settled(raw)).toEqual({ nak: [200] });
    expect(never).not.toHaveBeenCalled();
    expect(jsPublish).not.toHaveBeenCalled();
    expect(log.warn).toHaveBeenCalledWith(
      expect.stringMatching(
        /H\.on on "ord\.created".*idempotency store unavailable.*redis down.*retrying in 200ms/,
      ),
    );
  });

  it('still naks (never dead-letters or terms) once the attempts are exhausted', async () => {
    const { entry, jsPublish } = fakeEntry();
    const { d, raw } = consumeDelivery(20);
    await createSettlement({ entry, retry: strict, dlq: consumeDlq }).handle(
      d,
      nextWith(storeDown()),
    );
    // Capped at maxDelay.
    expect(settled(raw)).toEqual({ nak: [1000] });
    expect(jsPublish).not.toHaveBeenCalled();
  });

  it('caps the jittered delay at maxDelay', async () => {
    const { entry } = fakeEntry();
    const { d, raw } = consumeDelivery(20);
    const jittery = resolveRetryPolicy({
      initialDelay: 100,
      maxDelay: 1000,
      jitter: 0.5,
    }) as RetryPolicy;
    await createSettlement({ entry, retry: jittery, dlq: false, random: () => 1 }).handle(
      d,
      nextWith(storeDown()),
    );
    expect(settled(raw)).toEqual({ nak: [1000] });
  });

  it('uses the default backoff when retries are off', async () => {
    const { entry } = fakeEntry();
    const { d, raw } = consumeDelivery(1);
    await createSettlement({ entry, retry: false, dlq: false, random: () => 0.5 }).handle(
      d,
      nextWith(storeDown()),
    );
    expect(settled(raw)).toEqual({ nak: [1000] });
  });

  it('reschedules core messages with the backoff and no attempt cap', async () => {
    const { entry, ncPublish } = fakeEntry();
    const { d, schedule, msg, log } = coreDelivery({ attempt: 30 });
    never.mockClear();
    await createSettlement({ entry, retry: strict, dlq: coreDlq }).handle(d, nextWith(storeDown()));
    expect(schedule).toHaveBeenCalledExactlyOnceWith(msg, 1000);
    expect(never).not.toHaveBeenCalled();
    expect(ncPublish).not.toHaveBeenCalled();
    expect(log.warn).toHaveBeenCalledWith(expect.stringMatching(/idempotency store unavailable/));
    expect(log.error).not.toHaveBeenCalled();
  });

  it('reschedules core messages even when retries are off', async () => {
    const { entry } = fakeEntry();
    const { d, schedule, msg } = coreDelivery({ attempt: 1 });
    await createSettlement({ entry, retry: false, dlq: false, random: () => 0.5 }).handle(
      d,
      nextWith(storeDown()),
    );
    expect(schedule).toHaveBeenCalledExactlyOnceWith(msg, 1000);
  });

  it('still rethrows for reply deliveries', async () => {
    const { entry } = fakeEntry();
    const { d, schedule } = coreDelivery({ reply: '_INBOX.1' });
    const err = storeDown();
    await expect(
      createSettlement({ entry, retry, dlq: false }).handle(d, nextWith(err)),
    ).rejects.toBe(err);
    expect(schedule).not.toHaveBeenCalled();
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

  describe('through the outbound plugins', () => {
    const plugin = (fn: (m: OutgoingMessage, next: () => Promise<void>) => Promise<void>) =>
      [{ name: 'p', outbound: { handle: fn } }] as unknown as NatsPlugin[];
    const handlerInfo = { kind: 'consume', stream: 'ORD', durable: 'd' } as never;
    const coreInfo = { kind: 'subscribe', subject: 'ord.created' } as never;

    it('hands the dead letter to the outbound plugins, marked', async () => {
      const seen: OutgoingMessage[] = [];
      const { entry } = fakeEntry({
        plugins: plugin(async (m, next) => {
          seen.push(m);
          await next();
        }),
      });
      const { msg } = consumeDelivery(1);
      const error = new Error('x');
      await publishToDlq(entry, consumeDlq, handlerInfo, msg, error, 1);
      expect(seen).toHaveLength(1);
      const m = seen[0] as OutgoingMessage;
      expect(m.kind).toBe('jetstream');
      expect(m.subject).toBe('dlq.ORD.d');
      expect(m.data).toEqual(msg.data);
      expect(m.headers.get('Nats-DLQ-Stream')).toBe('ORD');
      expect(m.deadLetter).toEqual({ subject: msg.subject, handler: handlerInfo, error });
    });

    it('marks a core dead letter as a publish', async () => {
      const seen: OutgoingMessage[] = [];
      const { entry } = fakeEntry({
        plugins: plugin(async (m, next) => {
          seen.push(m);
          await next();
        }),
      });
      const { msg } = consumeDelivery(1);
      await publishToDlq(entry, coreDlq, coreInfo, msg, new Error('x'), 1);
      expect(seen[0]?.kind).toBe('publish');
      expect(seen[0]?.deadLetter?.handler).toBe(coreInfo);
    });

    it('publishes what the plugins pass on', async () => {
      const plugins = plugin(async (m, next) => {
        m.headers.set('x-tenant', 't1');
        await next();
      });
      const js = fakeEntry({ plugins });
      const { msg } = consumeDelivery(1);
      await expect(
        publishToDlq(js.entry, consumeDlq, handlerInfo, msg, new Error('x'), 1),
      ).resolves.toBe('sent');
      const jsOpts = (
        js.jsPublish.mock.calls[0] as unknown as [unknown, unknown, { headers: MsgHdrs }]
      )[2];
      expect(jsOpts.headers.get('x-tenant')).toBe('t1');
      const core = fakeEntry({ plugins });
      await expect(
        publishToDlq(core.entry, coreDlq, coreInfo, msg, new Error('x'), 1),
      ).resolves.toBe('sent');
      const ncOpts = (
        core.ncPublish.mock.calls[0] as unknown as [unknown, unknown, { headers: MsgHdrs }]
      )[2];
      expect(ncOpts.headers.get('x-tenant')).toBe('t1');
    });

    it('encodes data a plugin replaced', async () => {
      const { entry, jsPublish } = fakeEntry({
        plugins: plugin(async (m, next) => {
          m.data = { a: 1 };
          await next();
        }),
      });
      const { msg } = consumeDelivery(1);
      await publishToDlq(entry, consumeDlq, handlerInfo, msg, new Error('x'), 1);
      const body = (jsPublish.mock.calls[0] as unknown as [unknown, Uint8Array])[1];
      expect(jsonCodec().decode(body)).toEqual({ a: 1 });
    });

    it('still publishes when a plugin deletes Nats-Msg-Id', async () => {
      const { entry, jsPublish } = fakeEntry({
        plugins: plugin(async (m, next) => {
          m.headers.delete('Nats-Msg-Id');
          await next();
        }),
      });
      const { msg } = consumeDelivery(1);
      await expect(
        publishToDlq(entry, consumeDlq, handlerInfo, msg, new Error('x'), 1),
      ).resolves.toBe('sent');
      const opts = (
        jsPublish.mock.calls[0] as unknown as [unknown, unknown, { headers: MsgHdrs }]
      )[2];
      expect(opts.headers.has('Nats-Msg-Id')).toBe(false);
    });

    it('reports a dead letter a plugin dropped', async () => {
      const js = fakeEntry({ plugins: plugin(async () => {}) });
      const { msg } = consumeDelivery(1);
      await expect(
        publishToDlq(js.entry, consumeDlq, handlerInfo, msg, new Error('x'), 1),
      ).resolves.toBe('dropped');
      expect(js.jsPublish).not.toHaveBeenCalled();
      const core = fakeEntry({ plugins: plugin(async () => {}) });
      await expect(
        publishToDlq(core.entry, coreDlq, coreInfo, msg, new Error('x'), 1),
      ).resolves.toBe('dropped');
      expect(core.ncPublish).not.toHaveBeenCalled();
    });

    it('rejects with the error of a plugin that throws before next()', async () => {
      const boom = new Error('boom');
      const { entry, jsPublish } = fakeEntry({
        plugins: plugin(async () => {
          throw boom;
        }),
      });
      const { msg } = consumeDelivery(1);
      await expect(
        publishToDlq(entry, consumeDlq, handlerInfo, msg, new Error('x'), 1),
      ).rejects.toBe(boom);
      expect(jsPublish).not.toHaveBeenCalled();
    });

    it('rejects when the plugins do not pass the dead letter on in time', async () => {
      vi.useFakeTimers();
      try {
        const { entry, jsPublish } = fakeEntry({
          plugins: plugin(() => new Promise<void>(() => {})),
        });
        const { msg } = consumeDelivery(1);
        const p = publishToDlq(entry, consumeDlq, handlerInfo, msg, new Error('x'), 1);
        const assertion = expect(p).rejects.toThrow(/did not pass the message on within/);
        await vi.advanceTimersByTimeAsync(5000);
        await assertion;
        await expect(p).rejects.toBeInstanceOf(NatsModuleError);
        expect(jsPublish).not.toHaveBeenCalled();
      } finally {
        vi.useRealTimers();
      }
    });
  });
});
