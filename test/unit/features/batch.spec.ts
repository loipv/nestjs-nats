import { headers } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';
import { jsonCodec } from '../../../src/codec/codec.js';
import type {
  ConnectionEntry,
  ConnectionRegistry,
} from '../../../src/connection/connection-registry.js';
import { NatsBatchContext } from '../../../src/context/nats-batch-context.js';
import { NatsContext, NatsJetStreamContext } from '../../../src/context/nats-context.js';
import { NatsParamType } from '../../../src/decorators/param.decorators.js';
import type { HandlerDefinition } from '../../../src/discovery/handler-explorer.js';
import { validateHandlers } from '../../../src/discovery/handler-validator.js';
import { NatsParamsFactory } from '../../../src/discovery/nats-params-factory.js';
import { NatsConfigError, NatsDecodeError } from '../../../src/errors/errors.js';
import { createIdempotency } from '../../../src/idempotency/idempotency.js';
import type { ClaimResult, IdempotencyStore } from '../../../src/idempotency/store.js';
import { compose } from '../../../src/pipeline/compose.js';
import {
  createDelivery,
  FAILURES,
  type InternalDelivery,
  SKIPPED,
} from '../../../src/pipeline/delivery.js';
import { invokeHandler } from '../../../src/pipeline/invoker.js';
import { type DlqPolicy, resolveDlqPolicy } from '../../../src/settlement/dlq.js';
import { type RetryPolicy, resolveRetryPolicy } from '../../../src/settlement/retry.js';
import { createSettlement } from '../../../src/settlement/settlement.js';
import { mockLogger } from '../../support/logger.js';

const enc = new TextEncoder();
const retry = resolveRetryPolicy({ initialDelay: 100, jitter: 0 }) as RetryPolicy;

function jsRaw(body: string, attempt = 1, msgId?: string) {
  const h = headers();
  if (msgId) h.set('Nats-Msg-Id', msgId);
  return {
    subject: 'ord.created',
    reply: '$JS.ACK.ORD.d.1.1.1.0.0',
    data: enc.encode(body),
    headers: msgId ? h : undefined,
    ack: vi.fn(),
    nak: vi.fn(),
    term: vi.fn(),
    working: vi.fn(),
    info: { stream: 'ORD', consumer: 'd', streamSequence: 1, deliveryCount: attempt },
  };
}

function coreRaw(body: string, subject = 'orders.x') {
  const h = headers();
  h.set('x-id', body);
  return { subject, reply: '', data: enc.encode(body), headers: h };
}

type Invoke = (payload: unknown, ctx: unknown) => Promise<unknown>;

/** A JetStream batch delivery of `bodies` (attempt per message from `attempts`). */
function jsBatch(bodies: string[], invoke: Invoke, attempts: number[] = [], msgIds: string[] = []) {
  const raws = bodies.map((b, i) => jsRaw(b, attempts[i] ?? 1, msgIds[i]));
  const def = {
    kind: 'consume',
    connection: 'default',
    className: 'H',
    methodName: 'on',
    options: { stream: 'ORD', durable: 'd', batch: {} },
    invoke: vi.fn(invoke),
  } as unknown as HandlerDefinition;
  const log = mockLogger();
  const d = createDelivery({
    definition: def,
    connection: 'default',
    batch: true,
    codec: jsonCodec(),
    logger: log,
    parts: raws.map((raw) => ({
      message: {
        subject: raw.subject,
        data: raw.data,
        headers: raw.headers,
        raw: raw as never,
        attempt: raw.info.deliveryCount,
      },
      context: new NatsJetStreamContext({
        kind: 'jetstream',
        connection: 'default',
        raw: raw as never,
      }),
    })),
  });
  return { d, raws, def, log, invoke: def.invoke as unknown as ReturnType<typeof vi.fn> };
}

function coreBatch(bodies: string[], invoke: Invoke) {
  const raws = bodies.map((b) => coreRaw(b));
  const schedule = vi.fn();
  const def = {
    kind: 'subscribe',
    connection: 'default',
    className: 'H',
    methodName: 'on',
    subject: 'orders.*',
    options: { batch: {} },
    invoke: vi.fn(invoke),
  } as unknown as HandlerDefinition;
  const d = createDelivery({
    definition: def,
    connection: 'default',
    batch: true,
    codec: jsonCodec(),
    logger: mockLogger(),
    retry: schedule,
    parts: raws.map((raw) => ({
      message: {
        subject: raw.subject,
        data: raw.data,
        headers: raw.headers,
        raw: raw as never,
        attempt: 1,
      },
      context: new NatsContext({ kind: 'core', connection: 'default', raw: raw as never }),
    })),
  });
  return { d, raws, schedule, invoke: def.invoke as unknown as ReturnType<typeof vi.fn> };
}

const fakeEntry = () =>
  ({ name: 'default', options: {}, logger: mockLogger() }) as unknown as ConnectionEntry;

function pipeline(...core: Parameters<typeof compose<InternalDelivery>>[0]) {
  return compose<InternalDelivery>(core, invokeHandler);
}

const settlement = () => createSettlement({ entry: fakeEntry(), retry, dlq: false });

const toMw =
  (mw: { handle: (d: never, n: () => Promise<void>) => Promise<void> }) =>
  (d: InternalDelivery, next: () => Promise<void>) =>
    mw.handle(d as never, next);

describe('batch invoker', () => {
  it('excludes undecodable messages and records them as failures', async () => {
    const { d, invoke } = jsBatch(['1', 'hello', '3'], () => Promise.resolve());
    await invokeHandler(d);
    expect(invoke).toHaveBeenCalledTimes(1);
    const [payload, ctx] = invoke.mock.calls[0] as [unknown, NatsBatchContext];
    expect(payload).toEqual([1, 3]);
    expect(ctx).toBeInstanceOf(NatsBatchContext);
    expect(ctx.kind).toBe('batch');
    expect(ctx.connection).toBe('default');
    expect(ctx.messages.map((m) => m.raw)).toEqual([d.messages[0]?.raw, d.messages[2]?.raw]);
    expect([...d[FAILURES].keys()]).toEqual([d.messages[1]]);
    expect(d[FAILURES].get(d.messages[1] as never)).toBeInstanceOf(NatsDecodeError);
  });

  it('does not invoke the handler when nothing is left', async () => {
    const { d, invoke } = jsBatch(['hello', '2'], () => Promise.resolve());
    d[SKIPPED].add(d.messages[1] as never);
    await invokeHandler(d);
    expect(invoke).not.toHaveBeenCalled();
    expect(d[FAILURES].size).toBe(1);
  });

  it('leaves skipped messages out of the payload', async () => {
    const { d, invoke } = jsBatch(['1', '2', '3'], () => Promise.resolve());
    d[SKIPPED].add(d.messages[0] as never);
    await invokeHandler(d);
    expect(invoke.mock.calls[0]?.[0]).toEqual([2, 3]);
  });

  it('records failures from ctx.fail(index) and from the returned { failed }', async () => {
    const { d, raws } = jsBatch(['1', 'hello', '3', '4'], async (_payload, ctx) => {
      const c = ctx as NatsBatchContext;
      // Index 1 of the payload [1, 3, 4] is the message carrying 3.
      c.fail(1, new Error('by index'));
      return { failed: [{ msg: raws[3], error: new Error('by msg') }] };
    });
    await invokeHandler(d);
    const failures = d[FAILURES];
    expect(failures.size).toBe(3);
    expect(failures.get(d.messages[1] as never)).toBeInstanceOf(NatsDecodeError);
    expect((failures.get(d.messages[2] as never) as Error).message).toBe('by index');
    expect((failures.get(d.messages[3] as never) as Error).message).toBe('by msg');
    expect(failures.has(d.messages[0] as never)).toBe(false);
  });

  it('ctx.fail rejects an unknown target', async () => {
    const { d } = jsBatch(['1'], async (_payload, ctx) => {
      (ctx as NatsBatchContext).fail(5, new Error('x'));
    });
    await expect(invokeHandler(d)).rejects.toThrow(/5/);
  });

  it('logs and skips invalid entries of the returned { failed }, keeping the valid ones', async () => {
    const foreign = jsRaw('9');
    const { d, raws, log } = jsBatch(['1', '2', '3'], async () => ({
      failed: [
        { msg: 7, error: new Error('bad index') },
        { msg: foreign, error: new Error('foreign') },
        null,
        { msg: raws[2], error: new Error('valid') },
      ],
    }));
    await expect(invokeHandler(d)).resolves.toBeUndefined();
    const failures = d[FAILURES];
    expect([...failures.keys()]).toEqual([d.messages[2]]);
    expect((failures.get(d.messages[2] as never) as Error).message).toBe('valid');
    expect(log.error).toHaveBeenCalledTimes(3);
    for (const call of log.error.mock.calls) {
      expect(call[0]).toMatch(/H\.on returned an invalid entry in \{ failed \}/);
    }
    expect(log.error.mock.calls[0]?.[0]).toMatch(/Cannot fail message 7 of a batch of 3/);
  });

  it('settles the valid messages of a batch whose { failed } has an invalid entry', async () => {
    const { d, raws } = jsBatch(['1', '2'], async () => ({
      failed: [
        { msg: 5, error: new Error('bad') },
        { msg: 1, error: new Error('two fails') },
      ],
    }));
    await pipeline(toMw(settlement()))(d);
    expect(raws[0]?.ack).toHaveBeenCalledTimes(1);
    expect(raws[1]?.nak).toHaveBeenCalledExactlyOnceWith(100);
    expect(raws[1]?.ack).not.toHaveBeenCalled();
  });

  it('never stores a reply result', async () => {
    const { d } = jsBatch(['1'], () => Promise.resolve('reply'));
    await invokeHandler(d);
    expect(d.result).toBeUndefined();
  });
});

describe('batch params', () => {
  const factory = new NatsParamsFactory();
  const raws = [coreRaw('a', 's.1'), coreRaw('b', 's.2')];
  const ctx = new NatsBatchContext({
    connection: 'default',
    messages: raws.map(
      (raw) => new NatsContext({ kind: 'core', connection: 'default', raw: raw as never }),
    ),
    onFail: () => {},
  });
  const items = [{ id: 1 }, { id: 2 }];
  const args = [items, ctx];

  it('maps @Payload(prop) over items', () => {
    expect(factory.exchangeKeyForValue(NatsParamType.PAYLOAD, undefined as never, args)).toBe(
      items,
    );
    expect(factory.exchangeKeyForValue(NatsParamType.PAYLOAD, 'id', args)).toEqual([1, 2]);
    expect(
      factory.exchangeKeyForValue(NatsParamType.PAYLOAD, 'id', [[null, { id: 3 }], ctx]),
    ).toEqual([undefined, 3]);
  });

  it('maps headers, subjects and raw messages per message', () => {
    expect(factory.exchangeKeyForValue(NatsParamType.HEADERS, undefined as never, args)).toEqual([
      { 'x-id': 'a' },
      { 'x-id': 'b' },
    ]);
    expect(factory.exchangeKeyForValue(NatsParamType.HEADERS, 'x-id', args)).toEqual(['a', 'b']);
    expect(factory.exchangeKeyForValue(NatsParamType.SUBJECT, undefined as never, args)).toEqual([
      's.1',
      's.2',
    ]);
    expect(factory.exchangeKeyForValue(NatsParamType.MSG, undefined as never, args)).toEqual(raws);
    expect(factory.exchangeKeyForValue(NatsParamType.CONTEXT, undefined as never, args)).toBe(ctx);
  });
});

describe('batch settlement', () => {
  it('settles each message: ack succeeded, retry/dlq failed', async () => {
    const { d, raws } = jsBatch(
      ['1', '2', '3'],
      async (_p, ctx) => {
        (ctx as NatsBatchContext).fail(1, new Error('x'));
      },
      [1, 2, 1],
    );
    await pipeline(toMw(settlement()))(d);
    expect(raws[0]?.ack).toHaveBeenCalledTimes(1);
    expect(raws[2]?.ack).toHaveBeenCalledTimes(1);
    expect(raws[1]?.nak).toHaveBeenCalledExactlyOnceWith(200);
    expect(raws[1]?.ack).not.toHaveBeenCalled();
  });

  it('terms an undecodable message and acks the rest', async () => {
    const { d, raws } = jsBatch(['1', 'hello'], () => Promise.resolve());
    await pipeline(toMw(settlement()))(d);
    expect(raws[0]?.ack).toHaveBeenCalledTimes(1);
    expect(raws[1]?.term).toHaveBeenCalledTimes(1);
    expect(raws[1]?.ack).not.toHaveBeenCalled();
  });

  it('handler throw settles every message with its own attempt', async () => {
    const { d, raws } = jsBatch(['1', '2'], () => Promise.reject(new Error('boom')), [1, 6]);
    await pipeline(toMw(settlement()))(d);
    expect(raws[0]?.nak).toHaveBeenCalledExactlyOnceWith(100);
    expect(raws[1]?.term).toHaveBeenCalledTimes(1);
    expect(raws[0]?.ack).not.toHaveBeenCalled();
  });

  it('keeps the own failure of a message when the handler throws too', async () => {
    const { d, raws } = jsBatch(['1', 'hello'], () => Promise.reject(new Error('boom')));
    await pipeline(toMw(settlement()))(d);
    expect(raws[0]?.nak).toHaveBeenCalledExactlyOnceWith(100);
    // Undecodable: non-retryable, so it is dropped rather than retried with the batch.
    expect(raws[1]?.term).toHaveBeenCalledTimes(1);
    expect(raws[1]?.nak).not.toHaveBeenCalled();
  });

  it('dead-letters an exhausted failed message and acks the others', async () => {
    const jsPublish = vi.fn(() => Promise.resolve({ stream: 'DLQ', seq: 1 }));
    const entry = {
      name: 'default',
      options: {},
      logger: mockLogger(),
      js: () => ({ publish: jsPublish }),
    } as unknown as ConnectionEntry;
    const { d, raws } = jsBatch(
      ['1', '2', '3'],
      async (_p, ctx) => {
        (ctx as NatsBatchContext).fail(1, new Error('poison'));
      },
      [1, 6, 1],
    );
    const dlq = resolveDlqPolicy('consume', true) as DlqPolicy;
    await pipeline(toMw(createSettlement({ entry, retry, dlq })))(d);
    expect(jsPublish).toHaveBeenCalledTimes(1);
    const [subject, data, opts] = jsPublish.mock.calls[0] as unknown as [
      string,
      Uint8Array,
      { headers: { get(k: string): string } },
    ];
    expect(subject).toBe('dlq.ORD.d');
    expect(data).toBe(raws[1]?.data);
    expect(opts.headers.get('Nats-DLQ-Error')).toBe('poison');
    expect(opts.headers.get('Nats-DLQ-Deliveries')).toBe('6');
    expect(raws[1]?.term).toHaveBeenCalledTimes(1);
    expect(raws[1]?.ack).not.toHaveBeenCalled();
    expect(raws[0]?.ack).toHaveBeenCalledTimes(1);
    expect(raws[2]?.ack).toHaveBeenCalledTimes(1);
  });

  it('retries failed core messages in process, one by one', async () => {
    const { d, schedule } = coreBatch(['1', '2', '3'], async (_p, ctx) => {
      (ctx as NatsBatchContext).fail(2, new Error('x'));
    });
    await pipeline(toMw(settlement()))(d);
    expect(schedule).toHaveBeenCalledExactlyOnceWith(d.messages[2], 100);
  });

  it('retries every core message when the handler throws', async () => {
    const { d, schedule } = coreBatch(['1', '2'], () => Promise.reject(new Error('x')));
    await pipeline(toMw(settlement()))(d);
    expect(schedule.mock.calls).toEqual([
      [d.messages[0], 100],
      [d.messages[1], 100],
    ]);
  });
});

describe('batch idempotency', () => {
  function store(results: ClaimResult[]) {
    let i = 0;
    return {
      claim: vi.fn(() => Promise.resolve(results[i++] ?? 'claimed')),
      complete: vi.fn(() => Promise.resolve()),
      release: vi.fn(() => Promise.resolve()),
    } satisfies IdempotencyStore;
  }

  it('claims each message, settles skipped ones and completes or releases the rest', async () => {
    const s = store(['claimed', 'duplicate', 'in-progress', 'claimed']);
    const { d, raws, invoke } = jsBatch(
      ['1', '2', '3', '4'],
      async (_p, ctx) => {
        (ctx as NatsBatchContext).fail(1, new Error('x'));
      },
      [],
      ['a', 'b', 'c', 'd'],
    );
    const idem = createIdempotency({ store: s, ttl: 1000, inProgressNakMs: 50 });
    await pipeline(toMw(settlement()), toMw(idem))(d);
    expect(invoke.mock.calls[0]?.[0]).toEqual([1, 4]);
    expect(raws[1]?.ack).toHaveBeenCalledTimes(1);
    expect(raws[2]?.nak).toHaveBeenCalledExactlyOnceWith(50);
    expect(raws[0]?.ack).toHaveBeenCalledTimes(1);
    expect(raws[3]?.nak).toHaveBeenCalledExactlyOnceWith(100);
    expect(s.complete.mock.calls).toEqual([['consume:ORD/d|a', 1000]]);
    expect(s.release.mock.calls).toEqual([['consume:ORD/d|d']]);
  });

  it('releases every claim when the handler throws', async () => {
    const s = store([]);
    const { d } = jsBatch(['1', '2'], () => Promise.reject(new Error('x')), [], ['a', 'b']);
    const idem = createIdempotency({ store: s, ttl: 1000, inProgressNakMs: 50 });
    await pipeline(toMw(settlement()), toMw(idem))(d);
    expect(s.release.mock.calls).toEqual([['consume:ORD/d|a'], ['consume:ORD/d|b']]);
    expect(s.complete).not.toHaveBeenCalled();
  });
});

describe('batch validation', () => {
  const mgr = {
    has: (name: string) => name === 'default',
    get: () => ({ options: {} }),
  } as unknown as ConnectionRegistry;

  function consumeDef(options: Record<string, unknown>): HandlerDefinition {
    return {
      kind: 'consume',
      connection: 'default',
      className: 'H',
      methodName: 'on',
      instance: {},
      wrapper: { isDependencyTreeStatic: () => true, isTransient: false },
      invoke: async () => undefined,
      options: { stream: 'S', durable: 'd', ...options },
    } as never;
  }

  function subscribeDef(options: Record<string, unknown>): HandlerDefinition {
    return { ...consumeDef({}), kind: 'subscribe', subject: 'a', options } as never;
  }

  it('rejects batch with manual ack', () =>
    expect(() => validateHandlers([consumeDef({ ack: 'manual', batch: {} })], mgr)).toThrow(
      NatsConfigError,
    ));

  it.each([
    ['maxSize 0', { maxSize: 0 }],
    ['fractional maxSize', { maxSize: 1.5 }],
    ['negative maxWait', { maxWait: -1 }],
    ['NaN maxWait', { maxWait: Number.NaN }],
    ['maxWait beyond the timer range', { maxWait: 2 ** 31 }],
  ])('rejects %s', (_, batch) => {
    expect(() => validateHandlers([consumeDef({ batch })], mgr)).toThrow(NatsConfigError);
    expect(() => validateHandlers([subscribeDef({ batch })], mgr)).toThrow(NatsConfigError);
  });

  it('accepts valid batch options', () => {
    expect(() =>
      validateHandlers(
        [consumeDef({ batch: { maxSize: 1, maxWait: 0 } }), subscribeDef({ batch: {} })],
        mgr,
      ),
    ).not.toThrow();
  });
});
