import { headers as natsHeaders } from '@nats-io/transport-node';
import { Injectable } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { Test } from '@nestjs/testing';
import { describe, expect, it, vi } from 'vitest';
import type { CloudEventMeta } from '../../src/cloudevents/types.js';
import { jsonCodec } from '../../src/codec/codec.js';
import type {
  ConnectionEntry,
  ConnectionRegistry,
} from '../../src/connection/connection-registry.js';
import { NatsContext, NatsJetStreamContext } from '../../src/context/nats-context.js';
import {
  CloudEventRouter,
  type CloudEventRouterOptions,
  OnCloudEvent,
} from '../../src/decorators/cloudevent.decorators.js';
import { Subscribe } from '../../src/decorators/handler.decorators.js';
import { CloudEvent, Payload } from '../../src/decorators/param.decorators.js';
import { type HandlerDefinition, HandlerExplorer } from '../../src/discovery/handler-explorer.js';
import { validateHandlers } from '../../src/discovery/handler-validator.js';
import { NatsConfigError, NatsDecodeError } from '../../src/errors/errors.js';
import { compose } from '../../src/pipeline/compose.js';
import {
  createDelivery,
  type DeliveryPart,
  type InboundMessage,
  type InternalDelivery,
  toHandlerInfo,
} from '../../src/pipeline/delivery.js';
import { invokeHandler } from '../../src/pipeline/invoker.js';
import { toServiceError } from '../../src/pipeline/reply.js';
import { resolveDlqPolicy } from '../../src/settlement/dlq.js';
import { type RetryPolicy, resolveRetryPolicy } from '../../src/settlement/retry.js';
import { createSettlement } from '../../src/settlement/settlement.js';

const RETRY_ON = resolveRetryPolicy({ maxRetries: 5, initialDelay: 100, jitter: 0 }) as RetryPolicy;
const NOT_JSON = new TextEncoder().encode('not json{');

const calls: Array<{ handler: string; data: unknown; type?: string }> = [];

@CloudEventRouter({ subscribe: 'events.>' })
@Injectable()
class Orders {
  @OnCloudEvent('com.o.created')
  created(@Payload() data: unknown, @CloudEvent() ce: CloudEventMeta) {
    calls.push({ handler: 'created', data, type: ce.type });
    return 'created';
  }

  @OnCloudEvent('com.o.*')
  any(@Payload() data: unknown, @CloudEvent() ce: CloudEventMeta) {
    calls.push({ handler: 'any', data, type: ce.type });
    return 'any';
  }

  @Subscribe('plain')
  plain() {}
}

@Injectable()
class OrphanHandlers {
  @OnCloudEvent('a.b')
  on() {}
}

@CloudEventRouter({ subscribe: 'x' })
@Injectable()
class EmptyRouter {
  notAHandler() {}
}

@CloudEventRouter({ subscribe: 'x' })
@Injectable()
class DupRouter {
  @OnCloudEvent('a.b')
  one() {}

  @OnCloudEvent('a.b')
  two() {}
}

@CloudEventRouter({ subscribe: 'x' })
@Injectable()
class BadPatternRouter {
  @OnCloudEvent('a.>.b')
  on() {}
}

@CloudEventRouter({ subscribe: 'x' })
@Injectable()
class MixedRouter {
  @Subscribe('y')
  @OnCloudEvent('a.b')
  on() {}
}

@CloudEventRouter({ subscribe: 'x', consume: { stream: 'S', durable: 'd' } } as never)
@Injectable()
class BothRouter {
  @OnCloudEvent('a.b')
  on() {}
}

@CloudEventRouter({ subscribe: 'x', unmatched: 'nack' } as never)
@Injectable()
class BadPolicyRouter {
  @OnCloudEvent('a.b')
  on() {}
}

@CloudEventRouter({ subscribe: 'x', batch: { maxSize: 10 } } as never)
@Injectable()
class BatchRouter {
  @OnCloudEvent('a.b')
  on() {}
}

@CloudEventRouter({ consume: { stream: 'S', durable: 'd' }, ack: 'manual' } as never)
@Injectable()
class ManualAckRouter {
  @OnCloudEvent('a.b')
  on() {}
}

async function explore(...providers: Array<new (...args: never[]) => unknown>) {
  const moduleRef = await Test.createTestingModule({
    imports: [DiscoveryModule],
    providers: [HandlerExplorer, ...providers],
  }).compile();
  try {
    return moduleRef.get(HandlerExplorer).explore();
  } finally {
    await moduleRef.close();
  }
}

/** A router definition of `Orders` with the given options. */
async function routerDef(options: CloudEventRouterOptions): Promise<HandlerDefinition> {
  @CloudEventRouter(options)
  @Injectable()
  class R {
    @OnCloudEvent('com.o.created')
    created(@Payload() data: unknown) {
      calls.push({ handler: 'created', data });
      return 'created';
    }
  }
  const defs = await explore(R);
  const def = defs.find((d) => d.className === 'R');
  if (!def) throw new Error('no router definition');
  return def;
}

function logger() {
  return { log: vi.fn(), error: vi.fn(), warn: vi.fn(), debug: vi.fn(), verbose: vi.fn() };
}

function fakeEntry() {
  return {
    name: 'default',
    options: {},
    logger: logger(),
    nc: { publish: vi.fn() },
    js: () => ({ publish: vi.fn(() => Promise.resolve({ seq: 1 })) }),
  } as unknown as ConnectionEntry;
}

function ceHeaders(attrs: Record<string, string>) {
  const h = natsHeaders();
  for (const [k, v] of Object.entries(attrs)) h.set(`ce-${k}`, v);
  h.set('Content-Type', 'application/json');
  return h;
}

const BASE = { specversion: '1.0', id: 'e1', source: 'urn:t' };
const body = new TextEncoder().encode('{"n":1}');

function corePart(attrs: Record<string, string>, reply = ''): DeliveryPart {
  const raw = { subject: 'events.o', reply, data: body, headers: ceHeaders(attrs) };
  return {
    message: {
      subject: raw.subject,
      data: raw.data,
      headers: raw.headers,
      raw: raw as never,
      attempt: 1,
    },
    context: new NatsContext({ kind: 'core', connection: 'default', raw: raw as never }),
  };
}

function jsPart(attrs: Record<string, string>, data = body) {
  const raw = {
    subject: 'events.o',
    reply: '$JS.ACK.S.d.1.1.1.0.0',
    data,
    headers: ceHeaders(attrs),
    ack: vi.fn(),
    nak: vi.fn(),
    term: vi.fn(),
    working: vi.fn(),
    info: { stream: 'S', consumer: 'd', streamSequence: 1, deliveryCount: 1 },
  };
  const part: DeliveryPart = {
    message: {
      subject: raw.subject,
      data: raw.data,
      headers: raw.headers,
      raw: raw as never,
      attempt: 1,
    },
    context: new NatsJetStreamContext({
      kind: 'jetstream',
      connection: 'default',
      raw: raw as never,
    }),
  };
  return { part, raw };
}

async function run(
  def: HandlerDefinition,
  part: DeliveryPart,
  dlq = false,
  retry: RetryPolicy | false = false,
) {
  const entry = fakeEntry();
  const kind = def.kind === 'consume' ? 'consume' : 'subscribe';
  const settlement = createSettlement({
    entry,
    retry,
    dlq: resolveDlqPolicy(kind, dlq),
  });
  const d = createDelivery({
    definition: def,
    connection: 'default',
    batch: false,
    parts: [part],
    codec: jsonCodec(),
    logger: entry.logger,
  });
  let error: unknown;
  try {
    await compose<InternalDelivery>([(dd, next) => settlement.handle(dd, next)], invokeHandler)(d);
  } catch (err) {
    error = err;
  }
  return { d, entry, error, outcome: d.outcomes.get(d.messages[0] as InboundMessage) };
}

describe('@CloudEventRouter discovery', () => {
  it('creates one definition per router class', async () => {
    const defs = await explore(Orders);
    const routers = defs.filter((d) => d.methodName === '@CloudEventRouter');
    expect(routers).toHaveLength(1);
    const [router] = routers as [HandlerDefinition];
    expect(router).toMatchObject({ kind: 'subscribe', subject: 'events.>', className: 'Orders' });
    expect(router.router?.unmatched).toBe('ack');
    // @OnCloudEvent methods are not handlers of their own; other handlers still are.
    expect(defs.map((d) => d.methodName).sort()).toEqual(['@CloudEventRouter', 'plain']);
    expect(toHandlerInfo(router)).toEqual({
      kind: 'subscribe',
      connection: 'default',
      className: 'Orders',
      methodName: '@CloudEventRouter',
      subject: 'events.>',
    });
  });

  it('maps consume routers to consume definitions', async () => {
    const def = await routerDef({
      consume: { stream: 'S', durable: 'd', config: { ack_wait: 1 } },
      connection: 'c',
      concurrency: 2,
      unmatched: 'term',
    });
    expect(def).toMatchObject({
      kind: 'consume',
      delivery: 'pull',
      connection: 'c',
      methodName: '@CloudEventRouter',
      options: { stream: 'S', durable: 'd', config: { ack_wait: 1 }, concurrency: 2 },
    });
    expect(def.router?.unmatched).toBe('term');
  });

  it('builds a push consumer definition from consumePush', async () => {
    const def = await routerDef({
      consumePush: { stream: 'S', durable: 'd', deliverGroup: 'g', config: { ack_wait: 1 } },
    });
    expect(def).toMatchObject({
      kind: 'consume',
      delivery: 'push',
      methodName: '@CloudEventRouter',
      options: { stream: 'S', durable: 'd', deliverGroup: 'g', config: { ack_wait: 1 } },
    });
  });

  it('builds an ordered consumer definition from consumeOrdered', async () => {
    const def = await routerDef({
      consumeOrdered: {
        stream: 'S',
        filterSubjects: 'a.>',
        deliverPolicy: 'new',
        headersOnly: true,
      },
    });
    expect(def).toMatchObject({
      kind: 'ordered',
      methodName: '@CloudEventRouter',
      options: { stream: 'S', filterSubjects: 'a.>', deliverPolicy: 'new', headersOnly: true },
    });
  });

  it.each([
    ['subscribe and consumePush', { subscribe: 'x', consumePush: { stream: 'S', durable: 'd' } }],
    [
      'consumePush and consumeOrdered',
      { consumePush: { stream: 'S', durable: 'd' }, consumeOrdered: { stream: 'S' } },
    ],
    ['no source', {}],
  ])('rejects a router with %s', async (_, options) => {
    await expect(routerDef(options as never)).rejects.toThrow(
      /^@CloudEventRouter R must set exactly one of subscribe, consume, consumePush or consumeOrdered$/,
    );
  });

  it('counts a source set to undefined as unset', async () => {
    const def = await routerDef({
      consume: undefined,
      consumePush: { stream: 'S', durable: 'd' },
    } as never);
    expect(def).toMatchObject({ kind: 'consume', delivery: 'push' });
  });

  it('rejects concurrency with consumeOrdered', async () => {
    await expect(routerDef({ consumeOrdered: { stream: 'S' }, concurrency: 2 })).rejects.toThrow(
      new NatsConfigError('@CloudEventRouter R with consumeOrdered does not support concurrency'),
    );
  });

  it('rejects a deliverGroup that conflicts with config.deliver_group', async () => {
    const def = await routerDef({
      consumePush: { stream: 'S', durable: 'd', deliverGroup: 'g', config: { deliver_group: 'h' } },
    });
    const mgr = {
      has: () => true,
      get: () => ({ options: {} }),
    } as unknown as ConnectionRegistry;
    const wrapper = { isDependencyTreeStatic: () => true, isTransient: false };
    expect(() => validateHandlers([{ ...def, wrapper } as never], mgr)).toThrow(NatsConfigError);
  });

  it.each([
    ['orphan @OnCloudEvent', OrphanHandlers],
    ['router without routes', EmptyRouter],
    ['duplicate pattern', DupRouter],
    ['invalid pattern', BadPatternRouter],
    ['@OnCloudEvent combined with @Subscribe', MixedRouter],
    ['both subscribe and consume', BothRouter],
    ['an invalid unmatched policy', BadPolicyRouter],
    ['batch', BatchRouter],
    ['ack', ManualAckRouter],
  ])('rejects %s', async (_, cls) => {
    await expect(explore(cls)).rejects.toThrow(NatsConfigError);
  });
});

describe('@CloudEventRouter dispatch', () => {
  it('dispatches by type with precedence', async () => {
    const [def] = (await explore(Orders)).filter((d) => d.methodName === '@CloudEventRouter') as [
      HandlerDefinition,
    ];
    calls.length = 0;
    const exact = await run(def, corePart({ ...BASE, type: 'com.o.created' }));
    expect(exact.d.result).toBe('created');
    const wild = await run(def, corePart({ ...BASE, type: 'com.o.paid' }));
    expect(wild.d.result).toBe('any');
    expect(calls).toEqual([
      { handler: 'created', data: { n: 1 }, type: 'com.o.created' },
      { handler: 'any', data: { n: 1 }, type: 'com.o.paid' },
    ]);
  });

  it('acks unmatched by default', async () => {
    const def = await routerDef({ consume: { stream: 'S', durable: 'd' } });
    calls.length = 0;
    const { part, raw } = jsPart({ ...BASE, type: 'com.other' });
    const { outcome, entry } = await run(def, part);
    expect(calls).toEqual([]);
    expect(outcome).toEqual({ action: 'ack' });
    expect(raw.ack).toHaveBeenCalled();
    expect(entry.logger.debug).toHaveBeenCalledWith(
      expect.stringContaining('No handler for CloudEvent type "com.other"'),
    );
  });

  it.each([
    ['term', { action: 'term' }],
    ['dlq', { action: 'dlq', subject: 'dlq.S.d' }],
  ] as const)('unmatched %s', async (policy, expected) => {
    const def = await routerDef({ consume: { stream: 'S', durable: 'd' }, unmatched: policy });
    calls.length = 0;
    const { part, raw } = jsPart({ ...BASE, type: 'com.other' });
    const { outcome } = await run(def, part, true);
    expect(calls).toEqual([]);
    expect(outcome).toMatchObject(expected);
    expect(raw.term).toHaveBeenCalledTimes(1);
    expect(raw.nak).not.toHaveBeenCalled();
  });

  it('drops unmatched core messages with policy term', async () => {
    const def = await routerDef({ subscribe: 'events.>', unmatched: 'term' });
    const { outcome, error } = await run(def, corePart({ ...BASE, type: 'com.other' }));
    expect(error).toBeUndefined();
    expect(outcome).toMatchObject({ action: 'drop' });
  });

  it('dead-letters unmatched core messages with policy dlq', async () => {
    const def = await routerDef({ subscribe: 'events.>', unmatched: 'dlq' });
    const { outcome } = await run(def, corePart({ ...BASE, type: 'com.other' }), true);
    expect(outcome).toMatchObject({ action: 'dlq', subject: 'dlq.events.o' });
  });

  it('falls back to term with a warning when unmatched dlq has no DLQ', async () => {
    const def = await routerDef({ consume: { stream: 'S', durable: 'd' }, unmatched: 'dlq' });
    const { part, raw } = jsPart({ ...BASE, type: 'com.other' });
    const { outcome, entry } = await run(def, part, false);
    expect(outcome).toMatchObject({ action: 'term' });
    expect(raw.term).toHaveBeenCalledTimes(1);
    expect(entry.logger.warn).toHaveBeenCalledTimes(1);
    expect(entry.logger.warn).toHaveBeenCalledWith(expect.stringContaining('com.other'));

    const core = await routerDef({ subscribe: 'events.>', unmatched: 'dlq' });
    const second = await run(core, corePart({ ...BASE, type: 'com.other' }), false);
    expect(second.outcome).toMatchObject({ action: 'drop' });
    expect(second.entry.logger.warn).toHaveBeenCalledTimes(1);
  });

  it('terms CloudEvents missing a required attribute', async () => {
    const def = await routerDef({ consume: { stream: 'S', durable: 'd' } });
    calls.length = 0;
    const { part, raw } = jsPart({ specversion: '1.0', id: 'e1', type: 'com.o.created' });
    const { outcome } = await run(def, part);
    expect(calls).toEqual([]);
    expect(outcome).toMatchObject({ action: 'term' });
    expect(raw.term).toHaveBeenCalledTimes(1);
  });

  it('replies 404 to an unmatched request', async () => {
    const def = await routerDef({ subscribe: 'events.>', unmatched: 'term' });
    const { outcome, error } = await run(def, corePart({ ...BASE, type: 'com.other' }, '_INBOX.1'));
    expect(outcome).toMatchObject({ action: 'replied', error });
    expect(toServiceError(error)).toEqual({
      code: 404,
      description: 'No handler for CloudEvent type "com.other"',
    });
  });

  it('replies 404 to an unmatched request with policy ack too', async () => {
    const def = await routerDef({ subscribe: 'events.>' });
    const { outcome, error } = await run(def, corePart({ ...BASE, type: 'com.other' }, '_INBOX.1'));
    expect(outcome).toMatchObject({ action: 'replied', error });
    expect(toServiceError(error)).toEqual({
      code: 404,
      description: 'No handler for CloudEvent type "com.other"',
    });
  });

  it('never decodes the data of an unmatched CloudEvent', async () => {
    const def = await routerDef({ consume: { stream: 'S', durable: 'd' } });
    const { part, raw } = jsPart({ ...BASE, type: 'com.other' }, NOT_JSON);
    const { outcome, entry } = await run(def, part);
    expect(outcome).toEqual({ action: 'ack' });
    expect(raw.ack).toHaveBeenCalledTimes(1);
    expect(entry.logger.error).not.toHaveBeenCalled();
  });

  it('fails a matched CloudEvent whose data cannot be decoded', async () => {
    const def = await routerDef({ consume: { stream: 'S', durable: 'd' } });
    calls.length = 0;
    const { part, raw } = jsPart({ ...BASE, type: 'com.o.created' }, NOT_JSON);
    const { outcome } = await run(def, part, false, RETRY_ON);
    expect(calls).toEqual([]);
    expect(outcome).toMatchObject({ action: 'term' });
    expect((outcome as { error?: unknown }).error).toBeInstanceOf(NatsDecodeError);
    expect(raw.term).toHaveBeenCalledTimes(1);
    expect(raw.nak).not.toHaveBeenCalled();
  });

  it('terms without retrying when retries are on', async () => {
    const def = await routerDef({ consume: { stream: 'S', durable: 'd' }, unmatched: 'term' });
    const invalid = jsPart({ specversion: '1.0', id: 'e1', type: 'com.o.created' });
    const missing = await run(def, invalid.part, false, RETRY_ON);
    expect(missing.outcome).toMatchObject({ action: 'term' });
    expect((missing.outcome as { error?: unknown }).error).toBeInstanceOf(NatsDecodeError);
    expect(invalid.raw.nak).not.toHaveBeenCalled();
    expect(invalid.raw.term).toHaveBeenCalledTimes(1);

    const unmatched = jsPart({ ...BASE, type: 'com.other' });
    const result = await run(def, unmatched.part, false, RETRY_ON);
    expect(result.outcome).toMatchObject({ action: 'term' });
    expect(unmatched.raw.nak).not.toHaveBeenCalled();
    expect(unmatched.raw.term).toHaveBeenCalledTimes(1);
  });

  it('logs an unmatched dead letter as a routing decision, not a handler failure', async () => {
    const def = await routerDef({ consume: { stream: 'S', durable: 'd' }, unmatched: 'dlq' });
    const { part } = jsPart({ ...BASE, type: 'com.other' });
    const { outcome, entry } = await run(def, part, true);
    expect(outcome).toMatchObject({ action: 'dlq', subject: 'dlq.S.d' });
    expect(entry.logger.error).not.toHaveBeenCalled();
    expect(entry.logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('No handler for CloudEvent type "com.other"'),
    );
  });

  it('settles a push router like a consume router', async () => {
    const def = await routerDef({ consumePush: { stream: 'S', durable: 'd' }, unmatched: 'term' });
    calls.length = 0;
    const matched = jsPart({ ...BASE, type: 'com.o.created' });
    const ok = await run(def, matched.part);
    expect(calls).toHaveLength(1);
    expect(ok.outcome).toEqual({ action: 'ack' });
    expect(matched.raw.ack).toHaveBeenCalledTimes(1);
    const other = jsPart({ ...BASE, type: 'com.other' });
    const res = await run(def, other.part);
    expect(res.outcome).toMatchObject({ action: 'term' });
    expect(other.raw.term).toHaveBeenCalledTimes(1);
  });

  it('settles an ordered router without acks', async () => {
    const def = await routerDef({ consumeOrdered: { stream: 'S' } });
    calls.length = 0;
    const ok = await run(def, jsPart({ ...BASE, type: 'com.o.created' }).part);
    expect(calls).toHaveLength(1);
    expect(ok.outcome).toEqual({ action: 'none' });

    const term = await routerDef({ consumeOrdered: { stream: 'S' }, unmatched: 'term' });
    const dropped = await run(term, jsPart({ ...BASE, type: 'com.other' }).part);
    expect(dropped.outcome).toMatchObject({ action: 'drop' });

    const dlqDef = await routerDef({ consumeOrdered: { stream: 'S' }, unmatched: 'dlq' });
    const dead = await run(dlqDef, jsPart({ ...BASE, type: 'com.other' }).part, true);
    expect(dead.outcome).toMatchObject({ action: 'dlq' });
    expect(dead.d.result).toBeUndefined();

    const noDlq = await run(dlqDef, jsPart({ ...BASE, type: 'com.other' }).part, false);
    expect(noDlq.outcome).toMatchObject({ action: 'drop' });
    expect(noDlq.entry.logger.warn).toHaveBeenCalledWith(expect.stringContaining('com.other'));
  });
});
