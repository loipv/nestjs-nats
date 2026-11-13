import { EMPTY, of } from 'rxjs';
import { describe, expect, it, vi } from 'vitest';
import { jsonCodec } from '../../../src/codec/codec.js';
import { NatsContext } from '../../../src/context/nats-context.js';
import { NatsDecodeError } from '../../../src/errors/errors.js';
import {
  HANDLER_DEFINITION,
  type InternalDelivery,
  SKIPPED,
} from '../../../src/pipeline/delivery.js';
import { invokeHandler } from '../../../src/pipeline/invoker.js';

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
