import { describe, expect, it } from 'vitest';
import { jsonCodec } from '../../../src/codec/codec.js';
import { NatsContext } from '../../../src/context/nats-context.js';
import type { HandlerDefinition } from '../../../src/discovery/handler-explorer.js';
import {
  buildInboundPipeline,
  type Delivery,
  HANDLER_DEFINITION,
  type InternalDelivery,
  SKIPPED,
  toHandlerInfo,
} from '../../../src/pipeline/delivery.js';
import type { InboundMiddleware, NatsPlugin, Next } from '../../../src/pipeline/types.js';

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

const base = {
  connection: 'default',
  className: 'C',
  methodName: 'm',
  instance: {},
  wrapper: {},
  invoke: () => Promise.resolve(),
};

describe('toHandlerInfo', () => {
  it('describes a subscription without Nest internals', () =>
    expect(
      toHandlerInfo({
        ...base,
        kind: 'subscribe',
        subject: 'a.*',
        options: { queue: 'q', concurrency: 2 },
      } as unknown as HandlerDefinition),
    ).toStrictEqual({
      kind: 'subscribe',
      connection: 'default',
      className: 'C',
      methodName: 'm',
      subject: 'a.*',
      queue: 'q',
    }));

  it('omits queue when the subscription has none', () =>
    expect(
      toHandlerInfo({
        ...base,
        kind: 'subscribe',
        subject: 'a',
        options: {},
      } as unknown as HandlerDefinition),
    ).toStrictEqual({
      kind: 'subscribe',
      connection: 'default',
      className: 'C',
      methodName: 'm',
      subject: 'a',
    }));

  it('describes a consumer', () =>
    expect(
      toHandlerInfo({
        ...base,
        kind: 'consume',
        delivery: 'pull',
        options: { stream: 'S', durable: 'd', ack: 'manual', config: {} },
      } as unknown as HandlerDefinition),
    ).toStrictEqual({
      kind: 'consume',
      connection: 'default',
      className: 'C',
      methodName: 'm',
      stream: 'S',
      durable: 'd',
      delivery: 'pull',
    }));

  it('describes a push consumer', () =>
    expect(
      toHandlerInfo({
        ...base,
        kind: 'consume',
        delivery: 'push',
        options: { stream: 'S', durable: 'd', deliverGroup: 'g' },
      } as unknown as HandlerDefinition),
    ).toStrictEqual({
      kind: 'consume',
      connection: 'default',
      className: 'C',
      methodName: 'm',
      stream: 'S',
      durable: 'd',
      delivery: 'push',
    }));

  it.each([
    ['a single subject', 'orders.>', ['orders.>']],
    ['several subjects', ['a', 'b'], ['a', 'b']],
    ['none', undefined, []],
  ])('describes an ordered consumer with %s', (_, filterSubjects, expected) =>
    expect(
      toHandlerInfo({
        ...base,
        kind: 'ordered',
        options: { stream: 'S', ...(filterSubjects !== undefined ? { filterSubjects } : {}) },
      } as unknown as HandlerDefinition),
    ).toStrictEqual({
      kind: 'ordered',
      stream: 'S',
      filterSubjects: expected,
      connection: 'default',
      className: 'C',
      methodName: 'm',
    }),
  );

  it('describes a service endpoint with its effective subject', () => {
    const def = {
      ...base,
      kind: 'service-endpoint',
      endpoint: 'get',
      options: {},
      service: { name: 'svc', version: '1.0.0', description: 'x' },
    } as unknown as HandlerDefinition;
    expect(toHandlerInfo(def)).toStrictEqual({
      kind: 'service-endpoint',
      connection: 'default',
      className: 'C',
      methodName: 'm',
      service: { name: 'svc', version: '1.0.0' },
      endpoint: 'get',
      subject: 'get',
    });
    expect(
      toHandlerInfo({ ...def, options: { subject: 'svc.get' } } as HandlerDefinition),
    ).toMatchObject({ subject: 'svc.get' });
  });

  it('is frozen', () =>
    expect(
      Object.isFrozen(
        toHandlerInfo({
          ...base,
          kind: 'subscribe',
          subject: 'a',
          options: {},
        } as unknown as HandlerDefinition),
      ),
    ).toBe(true));
});

describe('buildInboundPipeline', () => {
  it('orders outer plugins, core, inner plugins, terminal', async () => {
    const log: string[] = [];
    const mw = (n: string, stage?: 'outer' | 'inner'): InboundMiddleware => ({
      stage,
      async handle(_d, next: Next) {
        log.push(`>${n}`);
        await next();
        log.push(`<${n}`);
      },
    });
    const plugins: NatsPlugin[] = [
      { name: 'i1', inbound: mw('i1', 'inner') },
      { name: 'o1', inbound: mw('o1', 'outer') },
      { name: 'none' },
      { name: 'u', inbound: mw('u') },
      { name: 'o2', inbound: mw('o2', 'outer') },
    ];
    const run = buildInboundPipeline({ plugins, core: [mw('c1'), mw('c2')] }, async () => {
      log.push('t');
    });
    await run(delivery({ invoke: () => undefined }));
    expect(log).toEqual([
      '>o1',
      '>o2',
      '>c1',
      '>c2',
      '>i1',
      '>u',
      't',
      '<u',
      '<i1',
      '<c2',
      '<c1',
      '<o2',
      '<o1',
    ]);
  });

  it('passes the same delivery to every middleware', async () => {
    const d = delivery({ invoke: () => undefined });
    const seen: Delivery[] = [];
    const run = buildInboundPipeline(
      {
        plugins: [
          {
            name: 'p',
            inbound: {
              handle: async (x, next) => {
                seen.push(x);
                await next();
              },
            },
          },
        ],
        core: [],
      },
      async (x) => {
        seen.push(x);
      },
    );
    await run(d);
    expect(seen).toEqual([d, d]);
  });
});
