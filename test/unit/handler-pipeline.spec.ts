import { describe, expect, it, vi } from 'vitest';
import { createHandlerPipeline, defaultPipelineDeps } from '../../src/adapters/handler-pipeline.js';
import { type Codec, jsonCodec } from '../../src/codec/codec.js';
import type { ConnectionEntry } from '../../src/connection/connection-manager.js';
import { NatsContext, NatsJetStreamContext } from '../../src/context/nats-context.js';
import type { HandlerDefinition } from '../../src/discovery/handler-explorer.js';
import type { IdempotencyStore } from '../../src/idempotency/store.js';
import { createDelivery } from '../../src/pipeline/delivery.js';
import type { NatsPlugin } from '../../src/pipeline/types.js';

const enc = new TextEncoder();

function logger() {
  return { log: vi.fn(), error: vi.fn(), warn: vi.fn(), debug: vi.fn() };
}

function entryWith(options: Record<string, unknown> = {}, store?: IdempotencyStore) {
  return {
    name: 'default',
    options,
    logger: logger(),
    nc: { publish: vi.fn() },
    js: () => ({ publish: vi.fn(() => Promise.resolve({ seq: 1 })) }),
    idempotencyStore: vi.fn(() => store),
  } as unknown as ConnectionEntry;
}

function fakeStore() {
  return {
    claim: vi.fn(() => Promise.resolve('claimed' as const)),
    complete: vi.fn(() => Promise.resolve()),
    release: vi.fn(() => Promise.resolve()),
  } satisfies IdempotencyStore;
}

const base = {
  connection: 'default',
  className: 'H',
  methodName: 'on',
  instance: {},
  wrapper: {},
};

function subscribeDef(options: Record<string, unknown> = {}, invoke = vi.fn()) {
  return {
    ...base,
    kind: 'subscribe',
    subject: 'a.b',
    options,
    invoke,
  } as unknown as HandlerDefinition;
}

function consumeDef(options: Record<string, unknown> = {}, invoke = vi.fn()) {
  return {
    ...base,
    kind: 'consume',
    options: { stream: 'S', durable: 'd', ...options },
    invoke,
  } as unknown as HandlerDefinition;
}

function endpointDef(invoke = vi.fn()) {
  return {
    ...base,
    kind: 'service-endpoint',
    endpoint: 'e',
    options: {},
    service: { name: 'svc', version: '1.0.0' },
    invoke,
  } as unknown as HandlerDefinition;
}

function jsDelivery(def: HandlerDefinition, codec: Codec, attempt = 1, msgId?: string) {
  const raw = {
    subject: 'a.b',
    reply: '',
    data: enc.encode('{"x":1}'),
    headers: msgId ? { get: (k: string) => (k === 'Nats-Msg-Id' ? msgId : '') } : undefined,
    ack: vi.fn(),
    nak: vi.fn(),
    term: vi.fn(),
    working: vi.fn(),
    info: { stream: 'S', consumer: 'd', streamSequence: 1, deliveryCount: attempt },
  };
  const d = createDelivery({
    definition: def,
    connection: 'default',
    batch: false,
    parts: [
      {
        message: {
          subject: raw.subject,
          data: raw.data,
          headers: raw.headers as never,
          raw: raw as never,
          attempt,
        },
        context: new NatsJetStreamContext({
          kind: 'jetstream',
          connection: 'default',
          raw: raw as never,
        }),
      },
    ],
    codec,
    logger: logger(),
  });
  return { d, raw };
}

describe('createHandlerPipeline', () => {
  it('takes the codec from the handler, then the module, then JSON', () => {
    const handlerCodec = { ...jsonCodec(), contentType: 'h' };
    const moduleCodec = { ...jsonCodec(), contentType: 'm' };
    const deps = defaultPipelineDeps;
    expect(
      createHandlerPipeline(
        subscribeDef({ codec: handlerCodec }),
        entryWith({ codec: moduleCodec }),
        deps,
      ).codec,
    ).toBe(handlerCodec);
    expect(
      createHandlerPipeline(subscribeDef(), entryWith({ codec: moduleCodec }), deps).codec,
    ).toBe(moduleCodec);
    expect(createHandlerPipeline(subscribeDef(), entryWith(), deps).codec.contentType).toBe(
      'application/json',
    );
  });

  it('resolves the handler info, retry and DLQ policies of a consumer', () => {
    const p = createHandlerPipeline(
      consumeDef({ retry: { maxRetries: 2 } }),
      entryWith({ retry: { initialDelay: 7 }, dlq: true }),
      defaultPipelineDeps,
    );
    expect(p.info).toMatchObject({ kind: 'consume', stream: 'S', durable: 'd', className: 'H' });
    expect(p.retry).toMatchObject({ maxRetries: 2, initialDelay: 7 });
    expect(p.dlq).toMatchObject({ mode: 'jetstream' });
  });

  it('defaults a subscription DLQ to core mode', () => {
    const p = createHandlerPipeline(subscribeDef({ dlq: true }), entryWith(), defaultPipelineDeps);
    expect(p.dlq).toMatchObject({ mode: 'core' });
  });

  it('never retries nor dead-letters service endpoints', () => {
    const p = createHandlerPipeline(
      endpointDef(),
      entryWith({ retry: { maxRetries: 3 }, dlq: true }),
      defaultPipelineDeps,
    );
    expect(p.retry).toBe(false);
    expect(p.dlq).toBe(false);
    expect(p.info).toMatchObject({ kind: 'service-endpoint', service: { name: 'svc' } });
  });

  it('runs plugins, settlement and the handler', async () => {
    const seen: string[] = [];
    const plugin: NatsPlugin = {
      name: 'p',
      inbound: {
        async handle(_d, next) {
          seen.push('plugin');
          await next();
        },
      },
    };
    const invoke = vi.fn(async (payload: unknown) => {
      seen.push(`handler:${JSON.stringify(payload)}`);
      return 'ok';
    });
    const def = consumeDef({}, invoke);
    const p = createHandlerPipeline(def, entryWith({ plugins: [plugin] }), defaultPipelineDeps);
    const { d, raw } = jsDelivery(def, p.codec);
    await p.run(d);
    expect(seen).toEqual(['plugin', 'handler:{"x":1}']);
    expect(d.result).toBe('ok');
    expect(raw.ack).toHaveBeenCalledOnce();
    expect([...d.outcomes.values()]).toEqual([{ action: 'ack' }]);
  });

  it('naks a failing consumer message with the resolved backoff', async () => {
    const def = consumeDef(
      { retry: { initialDelay: 25, jitter: 0 } },
      vi.fn(() => Promise.reject(new Error('x'))),
    );
    const p = createHandlerPipeline(def, entryWith(), defaultPipelineDeps);
    const { d, raw } = jsDelivery(def, p.codec);
    await p.run(d);
    expect(raw.nak).toHaveBeenCalledWith(25);
  });

  it('takes the idempotency store from deps, falling back to the connection store', async () => {
    const custom = fakeStore();
    const own = fakeStore();
    const def = consumeDef(
      { idempotency: true },
      vi.fn(() => Promise.resolve()),
    );
    const entry = entryWith({}, own);
    const deps = { idempotencyStore: vi.fn(() => custom as IdempotencyStore | undefined) };
    const p = createHandlerPipeline(def, entry, deps);
    expect(deps.idempotencyStore).toHaveBeenCalledWith(entry, def);
    await p.run(jsDelivery(def, p.codec, 1, 'm1').d);
    expect(custom.claim).toHaveBeenCalledWith('consume:S/d|m1', 300_000);
    expect(own.claim).not.toHaveBeenCalled();

    const fallback = createHandlerPipeline(def, entry, { idempotencyStore: () => undefined });
    await fallback.run(jsDelivery(def, fallback.codec, 1, 'm2').d);
    expect(own.claim).toHaveBeenCalledWith('consume:S/d|m2', 300_000);
  });

  it('does not touch an idempotency store unless the handler enables it', () => {
    const entry = entryWith({}, fakeStore());
    const deps = { idempotencyStore: vi.fn(() => undefined) };
    createHandlerPipeline(subscribeDef(), entry, deps);
    expect(deps.idempotencyStore).not.toHaveBeenCalled();
    expect(entry.idempotencyStore).not.toHaveBeenCalled();
  });

  it('records outcomes of core deliveries', async () => {
    const def = subscribeDef(
      {},
      vi.fn(() => Promise.resolve()),
    );
    const p = createHandlerPipeline(def, entryWith(), defaultPipelineDeps);
    const raw = { subject: 'a.b', reply: '', data: enc.encode('1'), headers: undefined };
    const d = createDelivery({
      definition: def,
      connection: 'default',
      batch: false,
      parts: [
        {
          message: { subject: 'a.b', data: raw.data, raw: raw as never, attempt: 1 },
          context: new NatsContext({ kind: 'core', connection: 'default', raw: raw as never }),
        },
      ],
      codec: p.codec,
      logger: logger(),
    });
    await p.run(d);
    expect([...d.outcomes.values()]).toEqual([{ action: 'none' }]);
  });
});
