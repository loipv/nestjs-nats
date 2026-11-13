import { headers } from '@nats-io/transport-node';
import { EMPTY, of } from 'rxjs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { jsonCodec } from '../../../src/codec/codec.js';
import { NatsBatchContext } from '../../../src/context/nats-batch-context.js';
import { NatsContext } from '../../../src/context/nats-context.js';
import { NatsDecodeError, NatsHandlerTimeoutError } from '../../../src/errors/errors.js';
import {
  createDelivery,
  type DeliveryPart,
  FAILURES,
  HANDLER_DEFINITION,
  type InternalDelivery,
  LINGERING,
  SKIPPED,
} from '../../../src/pipeline/delivery.js';
import { invokeHandler } from '../../../src/pipeline/invoker.js';
import { never } from '../../support/async.js';
import { mockLogger } from '../../support/logger.js';

const enc = new TextEncoder();

function delivery(opts: {
  invoke: (payload: unknown, ctx: NatsContext) => unknown;
  data?: Uint8Array;
}) {
  const raw = { subject: 'a.b', reply: '', headers: undefined, data: opts.data };
  const context = new NatsContext({ kind: 'core', connection: 'default', raw: raw as never });
  const handler = {
    kind: 'subscribe',
    subject: 'a.b',
    connection: 'default',
    className: 'H',
    methodName: 'on',
  };
  return {
    handler,
    [HANDLER_DEFINITION]: { ...handler, options: {}, invoke: opts.invoke },
    [SKIPPED]: new Set(),
    messages: [{ subject: 'a.b', data: opts.data ?? enc.encode('{"a":1}'), raw }],
    context,
    codec: jsonCodec(),
    logger: { log() {}, error() {}, warn() {} },
  } as unknown as InternalDelivery;
}

describe('invokeHandler', () => {
  it('decodes the first message and stores the result', async () => {
    const invoke = vi.fn(async (p: unknown) => ({ got: p }));
    const d = delivery({ invoke });
    await invokeHandler(d);
    expect(invoke).toHaveBeenCalledWith({ a: 1 }, d.context);
    expect(d.result).toEqual({ got: { a: 1 } });
  });

  it('passes undefined for an empty payload', async () => {
    const invoke = vi.fn(async () => 1);
    await invokeHandler(delivery({ invoke, data: new Uint8Array(0) }));
    expect(invoke).toHaveBeenCalledWith(undefined, expect.any(NatsContext));
  });

  it('rejects with NatsDecodeError on undecodable payloads without invoking', async () => {
    const invoke = vi.fn();
    await expect(
      invokeHandler(delivery({ invoke, data: enc.encode('{nope') })),
    ).rejects.toBeInstanceOf(NatsDecodeError);
    expect(invoke).not.toHaveBeenCalled();
  });

  it('does not invoke a skipped message and leaves the result undefined', async () => {
    const invoke = vi.fn(() => 'x');
    const d = delivery({ invoke });
    const [msg] = d.messages;
    if (msg) d[SKIPPED].add(msg);
    await invokeHandler(d);
    expect(invoke).not.toHaveBeenCalled();
    expect(d.result).toBeUndefined();
  });

  it('treats synchronous throws like rejections', async () => {
    const d = delivery({
      invoke: () => {
        throw new Error('sync');
      },
    });
    const p = invokeHandler(d);
    expect(p).toBeInstanceOf(Promise);
    await expect(p).rejects.toThrow('sync');
  });

  it('propagates rejections', async () => {
    const d = delivery({ invoke: async () => Promise.reject(new Error('async')) });
    await expect(invokeHandler(d)).rejects.toThrow('async');
  });

  it('unwraps observable results', async () => {
    const d = delivery({ invoke: () => of(1, 5) });
    await invokeHandler(d);
    expect(d.result).toBe(5);
  });

  it('rejects for an empty observable returned by a raw invoker', async () => {
    // Real handlers go through Nest's ExternalContextCreator, which unwraps observables
    // itself; this only pins the invoker's own fallback for a bare invoke function.
    await expect(invokeHandler(delivery({ invoke: () => EMPTY }))).rejects.toThrow();
  });
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

function corePart(data: string, h?: ReturnType<typeof headers>): DeliveryPart {
  const raw = { subject: 'a.b', reply: '', data: enc.encode(data), headers: h };
  return {
    message: { subject: raw.subject, data: raw.data, headers: h, raw: raw as never, attempt: 1 },
    context: new NatsContext({ kind: 'core', connection: 'default', raw: raw as never }),
  };
}

function batchDelivery(invoke: (items: unknown, ctx: NatsBatchContext) => unknown) {
  const definition = {
    kind: 'subscribe',
    subject: 'a.b',
    connection: 'default',
    className: 'H',
    methodName: 'batch',
    options: { batch: {} },
    invoke,
  };
  const parts = [corePart('{"i":1}'), corePart('{"i":2}')];
  const d = createDelivery({
    definition: definition as never,
    connection: 'default',
    batch: true,
    parts,
    codec: jsonCodec(),
    logger: mockLogger(),
  });
  return { d, parts };
}

function routerDelivery(type: string, invoke: (data: unknown, ctx: NatsContext) => unknown) {
  const h = headers();
  for (const [k, v] of Object.entries({
    'ce-specversion': '1.0',
    'ce-id': '1',
    'ce-source': 'src',
    'ce-type': type,
  }))
    h.set(k, v);
  const target = { methodName: 'created', invoke };
  const definition = {
    kind: 'subscribe',
    subject: 'a.b',
    connection: 'default',
    className: 'Router',
    methodName: '@CloudEventRouter',
    options: {},
    invoke: () => Promise.reject(new Error('not called')),
    router: {
      route: (t: string) => (t === 'order.created' ? target : undefined),
      unmatched: 'ack',
    },
  };
  return createDelivery({
    definition: definition as never,
    connection: 'default',
    batch: false,
    parts: [corePart('{"id":1}', h)],
    codec: jsonCodec(),
    logger: mockLogger(),
  });
}

describe('invokeHandler timeout', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('fails a run that outlives its timeout, aborting ctx.signal and keeping the run', async () => {
    let seen: NatsContext | undefined;
    const run = deferred<string>();
    const d = delivery({
      invoke: (_p, ctx) => {
        seen = ctx;
        return run.promise;
      },
    });
    const failed = invokeHandler(d, { timeout: 100 }).catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(100);
    const err = await failed;
    expect(err).toBeInstanceOf(NatsHandlerTimeoutError);
    expect(err).toMatchObject({ handler: 'H.on', timeout: 100 });
    expect(seen?.signal.aborted).toBe(true);
    expect(seen?.signal.reason).toBe(err);
    run.resolve('late');
    await expect(d[LINGERING]).resolves.toBe(true);
    expect(d.result).toBeUndefined();
  });

  it('keeps the result of a run that finishes in time, leaving no timer', async () => {
    const d = delivery({ invoke: async () => 1 });
    await invokeHandler(d, { timeout: 100 });
    expect(d.result).toBe(1);
    expect(d[LINGERING]).toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('sets no timer without a timeout', async () => {
    const d = delivery({ invoke: () => never() });
    void invokeHandler(d);
    await Promise.resolve();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('aborts the batch, the view the handler gets and every message, ignoring a late { failed }', async () => {
    let view: NatsBatchContext | undefined;
    const run = deferred<unknown>();
    const { d, parts } = batchDelivery((_items, ctx) => {
      view = ctx;
      return run.promise;
    });
    const failed = invokeHandler(d, { timeout: 100 }).catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(100);
    expect(await failed).toMatchObject({ handler: 'H.batch', timeout: 100 });
    expect(view?.signal.aborted).toBe(true);
    expect((d.context as NatsBatchContext).signal.aborted).toBe(true);
    for (const { context } of parts) expect(context.signal.aborted).toBe(true);
    run.resolve({ failed: [{ msg: 0, error: new Error('late') }] });
    await expect(d[LINGERING]).resolves.toBe(true);
    expect(d[FAILURES].size).toBe(0);
  });

  it('names the route a router timed out on', async () => {
    const d = routerDelivery('order.created', () => never());
    const failed = invokeHandler(d, { timeout: 100 }).catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(100);
    expect(await failed).toMatchObject({ handler: 'Router.created', timeout: 100 });
  });

  it('sets no timer for a CloudEvent no route matches', async () => {
    const d = routerDelivery('order.deleted', () => never());
    await invokeHandler(d, { timeout: 100 });
    expect(vi.getTimerCount()).toBe(0);
  });
});
