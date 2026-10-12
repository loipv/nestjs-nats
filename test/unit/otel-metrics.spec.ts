import type { MsgHdrs } from '@nats-io/transport-node';
import { diag, metrics as metricsApi } from '@opentelemetry/api';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { jsonCodec } from '../../src/codec/codec.js';
import { NatsBatchContext } from '../../src/context/nats-batch-context.js';
import { NatsContext, NatsJetStreamContext } from '../../src/context/nats-context.js';
import { NatsOrderedContext } from '../../src/context/nats-ordered-context.js';
import { type MetricsOptions, metrics } from '../../src/otel/index.js';
import type {
  Delivery,
  HandlerInfo,
  InboundMessage,
  MessageOutcome,
} from '../../src/pipeline/delivery.js';
import type { NatsPlugin } from '../../src/pipeline/types.js';
import { setupMetrics, type TestMetrics } from '../support/otel.js';

const enc = new TextEncoder();

const logger = { log: vi.fn(), error: vi.fn(), warn: vi.fn(), debug: vi.fn(), verbose: vi.fn() };

interface Part {
  message: InboundMessage;
  context: NatsContext;
}

function corePart(
  opts: { subject?: string; reply?: string; headers?: MsgHdrs; data?: Uint8Array } = {},
): Part {
  const raw = {
    subject: opts.subject ?? 'o.c',
    reply: opts.reply ?? '',
    data: opts.data ?? enc.encode('{"id":1}'),
    headers: opts.headers,
  };
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

function jsPart(opts: { headers?: MsgHdrs; deliveryCount?: number } = {}): Part {
  const raw = {
    subject: 'o.c',
    reply: '$JS.ACK.ORD.d.1.1.1.0.0',
    data: enc.encode('{"id":1}'),
    headers: opts.headers,
    info: {
      stream: 'ORD',
      consumer: 'billing',
      streamSequence: 42,
      deliveryCount: opts.deliveryCount ?? 3,
    },
  };
  return {
    message: {
      subject: raw.subject,
      data: raw.data,
      headers: raw.headers,
      raw: raw as never,
      attempt: opts.deliveryCount ?? 3,
    },
    context: new NatsJetStreamContext({
      kind: 'jetstream',
      connection: 'default',
      raw: raw as never,
    }),
  };
}

/** A message of an ordered consumer, on its second in-place attempt. */
function orderedPart(): Part {
  const raw = {
    subject: 'o.c',
    reply: '$JS.ACK.ORD.ordered.1.1.1.0.0',
    data: enc.encode('{"id":1}'),
    headers: undefined,
    info: { stream: 'ORD', consumer: 'ordered-x', streamSequence: 42, deliveryCount: 1 },
  };
  return {
    message: { subject: raw.subject, data: raw.data, raw: raw as never, attempt: 2 },
    context: new NatsOrderedContext({ connection: 'default', raw: raw as never, attempt: 2 }),
  };
}

const base = { connection: 'default', className: 'H', methodName: 'on' };
const consumeHandler: HandlerInfo = {
  kind: 'consume',
  ...base,
  stream: 'ORD',
  durable: 'billing',
  delivery: 'pull',
};
const subscribeHandler: HandlerInfo = {
  kind: 'subscribe',
  ...base,
  subject: 'o.*',
  queue: 'workers',
};
const endpointHandler: HandlerInfo = {
  kind: 'service-endpoint',
  ...base,
  service: { name: 's', version: '1.0.0' },
  endpoint: 'e',
  subject: 'o.c',
};

function delivery(handler: HandlerInfo, parts: Part[], batch = false): Delivery {
  const ctx = batch
    ? new NatsBatchContext({
        connection: 'default',
        messages: parts.map((p) => p.context),
        onFail: () => {},
      })
    : (parts[0] as Part).context;
  return {
    handler,
    messages: parts.map((p) => p.message),
    context: ctx,
    codec: jsonCodec(),
    logger,
    outcomes: new Map(),
  };
}

const orderedHandler: HandlerInfo = { kind: 'ordered', ...base, stream: 'ORD', filterSubjects: [] };
const plainSubscribe: HandlerInfo = { kind: 'subscribe', ...base, subject: 'o.*' };

const BOUNDARIES = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10];
const PROCESS = 'messaging.process.duration';
const CONSUMED = 'messaging.client.consumed.messages';

interface HistogramValue {
  count: number;
  sum: number;
  buckets: { boundaries: number[]; counts: number[] };
}

describe('metrics()', () => {
  let t: TestMetrics;

  beforeEach(() => {
    t = setupMetrics();
  });
  afterEach(async () => {
    await t.shutdown();
    vi.restoreAllMocks();
  });

  const handle = (
    plugin: NatsPlugin,
    d: Delivery,
    next: () => Promise<void> = async () => {},
  ): Promise<void> => (plugin.inbound as NonNullable<NatsPlugin['inbound']>).handle(d, next);

  const run = async (
    d: Delivery,
    outcomes: MessageOutcome[] = [],
    options?: MetricsOptions,
  ): Promise<void> => {
    d.messages.forEach((m, i) => {
      const o = outcomes[i];
      if (o) d.outcomes.set(m, o);
    });
    await handle(metrics(options), d);
  };

  const commonConsume = {
    'messaging.system': 'nats',
    'messaging.operation.name': 'process',
    'nats.connection': 'default',
    'nats.handler': 'H.on',
    'nats.handler.kind': 'consume',
    'messaging.consumer.group.name': 'billing',
    'nats.stream': 'ORD',
  };

  it('is named otel-metrics and runs in the outer stage', () => {
    const plugin = metrics();
    expect(plugin.name).toBe('otel-metrics');
    expect(plugin.inbound?.stage).toBe('outer');
  });

  it('records one process duration per delivery', async () => {
    await run(delivery(consumeHandler, [jsPart()]), [{ action: 'ack' }]);
    const points = await t.points(PROCESS);
    expect(points).toHaveLength(1);
    const value = points[0]?.value as HistogramValue;
    expect(value.count).toBe(1);
    expect(value.buckets.boundaries).toEqual(BOUNDARIES);
    expect(points[0]?.attributes).toEqual(commonConsume);
  });

  const outcomes: Array<[string, MessageOutcome]> = [
    ['ack', { action: 'ack' }],
    ['retry', { action: 'retry', delayMs: 10, error: new Error('r') }],
    ['dlq', { action: 'dlq', subject: 'dlq.o', error: new Error('d') }],
    ['term', { action: 'term' }],
    ['drop', { action: 'drop', error: new Error('x') }],
    ['skipped', { action: 'skipped', reason: 'duplicate' }],
    ['replied', { action: 'replied' }],
    ['none', { action: 'none' }],
  ];

  it.each(outcomes)('counts each message with its outcome: %s', async (action, outcome) => {
    await run(delivery(consumeHandler, [jsPart()]), [outcome]);
    const points = await t.points(CONSUMED);
    expect(points).toHaveLength(1);
    expect(points[0]?.value).toBe(1);
    expect(points[0]?.attributes['nats.outcome']).toBe(action);
    const hasError = 'error' in outcome && outcome.error !== undefined;
    expect('error.type' in (points[0]?.attributes ?? {})).toBe(hasError);
  });

  it('counts a message without an outcome as unsettled', async () => {
    await run(delivery(consumeHandler, [jsPart()]));
    const points = await t.points(CONSUMED);
    expect(points[0]?.attributes['nats.outcome']).toBe('unsettled');
  });

  it('counts an acked message whose handler threw', async () => {
    await run(delivery(consumeHandler, [jsPart()]), [{ action: 'ack', error: new TypeError('x') }]);
    const points = await t.points(CONSUMED);
    expect(points[0]?.attributes).toMatchObject({
      'nats.outcome': 'ack',
      'error.type': 'TypeError',
    });
  });

  it('records a batch once and counts every message', async () => {
    const d = delivery(consumeHandler, [jsPart(), jsPart(), jsPart()], true);
    await run(d, [
      { action: 'ack' },
      { action: 'ack' },
      { action: 'retry', delayMs: 1, error: new Error('r') },
    ]);
    const hist = await t.points(PROCESS);
    expect(hist).toHaveLength(1);
    expect((hist[0]?.value as HistogramValue | undefined)?.count).toBe(1);
    const counts = Object.fromEntries(
      (await t.points(CONSUMED)).map((p) => [p.attributes['nats.outcome'], p.value]),
    );
    expect(counts).toEqual({ ack: 2, retry: 1 });
  });

  it('records the error of next() and rethrows it', async () => {
    const err = new RangeError('boom');
    const plugin = metrics();
    await expect(
      handle(plugin, delivery(consumeHandler, [jsPart()]), () => Promise.reject(err)),
    ).rejects.toBe(err);
    const hist = await t.points(PROCESS);
    expect(hist[0]?.attributes['error.type']).toBe('RangeError');
    const counted = await t.points(CONSUMED);
    expect(counted[0]?.attributes['nats.outcome']).toBe('unsettled');
  });

  it('uses _OTHER for a non-Error throw', async () => {
    const plugin = metrics();
    await expect(
      handle(plugin, delivery(consumeHandler, [jsPart()]), () => Promise.reject('nope')),
    ).rejects.toBe('nope');
    expect((await t.points(PROCESS))[0]?.attributes['error.type']).toBe('_OTHER');
  });

  const kinds: Array<[string, HandlerInfo, () => Part, Record<string, string | undefined>]> = [
    ['subscribe with queue', subscribeHandler, corePart, { group: 'workers', stream: undefined }],
    ['subscribe without queue', plainSubscribe, corePart, { group: undefined, stream: undefined }],
    ['ordered', orderedHandler, orderedPart, { group: undefined, stream: 'ORD' }],
    ['service-endpoint', endpointHandler, corePart, { group: undefined, stream: undefined }],
  ];

  it.each(kinds)('sets group and stream by handler kind: %s', async (_n, handler, part, want) => {
    await run(delivery(handler, [part()]), [{ action: 'ack' }]);
    for (const name of [PROCESS, CONSUMED]) {
      const attrs = (await t.points(name))[0]?.attributes ?? {};
      expect(attrs['messaging.consumer.group.name']).toBe(want.group);
      expect(attrs['nats.stream']).toBe(want.stream);
      expect('messaging.consumer.group.name' in attrs).toBe(want.group !== undefined);
      expect('nats.stream' in attrs).toBe(want.stream !== undefined);
    }
  });

  it('omits the subject by default', async () => {
    await run(delivery(consumeHandler, [jsPart()]), [{ action: 'ack' }]);
    for (const name of [PROCESS, CONSUMED]) {
      for (const p of await t.points(name)) {
        expect('messaging.destination.name' in p.attributes).toBe(false);
      }
    }
  });

  it('records the subject with subjects: true', async () => {
    await run(delivery(consumeHandler, [jsPart()]), [{ action: 'ack' }], { subjects: true });
    for (const name of [PROCESS, CONSUMED]) {
      expect((await t.points(name))[0]?.attributes['messaging.destination.name']).toBe('o.c');
    }
  });

  it('maps the subject with a subjects function', async () => {
    await run(delivery(consumeHandler, [jsPart()]), [{ action: 'ack' }], {
      subjects: (s) => s.replace(/\.\w+$/, '.*'),
    });
    for (const name of [PROCESS, CONSUMED]) {
      expect((await t.points(name))[0]?.attributes['messaging.destination.name']).toBe('o.*');
    }
  });

  it('omits the subject when the subjects function returns undefined', async () => {
    await run(delivery(consumeHandler, [jsPart()]), [{ action: 'ack' }], {
      subjects: () => undefined,
    });
    for (const name of [PROCESS, CONSUMED]) {
      expect('messaging.destination.name' in ((await t.points(name))[0]?.attributes ?? {})).toBe(
        false,
      );
    }
  });

  it("uses each message's subject for the counter", async () => {
    const d = delivery(
      consumeHandler,
      [corePart({ subject: 'a.1' }), corePart({ subject: 'a.2' })],
      true,
    );
    await run(d, [{ action: 'ack' }, { action: 'ack' }], { subjects: true });
    const hist = await t.points(PROCESS);
    expect(hist).toHaveLength(1);
    expect(hist[0]?.attributes['messaging.destination.name']).toBe('a.1');
    const subjects = (await t.points(CONSUMED))
      .map((p) => p.attributes['messaging.destination.name'])
      .sort();
    expect(subjects).toEqual(['a.1', 'a.2']);
  });

  it('warns once when the subjects function throws', async () => {
    const warn = vi.spyOn(diag, 'warn').mockImplementation(() => {});
    const plugin = metrics({
      subjects: () => {
        throw new Error('bad');
      },
    });
    for (let i = 0; i < 2; i++) {
      const d = delivery(consumeHandler, [jsPart()]);
      d.outcomes.set(d.messages[0] as InboundMessage, { action: 'ack' });
      await handle(plugin, d);
    }
    expect(warn).toHaveBeenCalledTimes(1);
    for (const name of [PROCESS, CONSUMED]) {
      for (const p of await t.points(name)) {
        expect('messaging.destination.name' in p.attributes).toBe(false);
      }
    }
    expect((await t.points(CONSUMED))[0]?.value).toBe(2);
  });

  it('records once a provider is registered after creation', async () => {
    await t.shutdown();
    const plugin = metrics();
    await expect(handle(plugin, delivery(consumeHandler, [jsPart()]))).resolves.toBeUndefined();
    t = setupMetrics();
    await handle(plugin, delivery(consumeHandler, [jsPart()]));
    const points = await t.points(PROCESS);
    expect(points).toHaveLength(1);
    expect((points[0]?.value as HistogramValue | undefined)?.count).toBe(1);
  });

  it('follows a provider change', async () => {
    const plugin = metrics();
    await handle(plugin, delivery(consumeHandler, [jsPart()]));
    expect(metricsApi.getMeterProvider()).toBe(t.provider);
    await t.shutdown();
    t = setupMetrics();
    await handle(plugin, delivery(consumeHandler, [jsPart()]));
    const points = await t.points(PROCESS);
    expect(points).toHaveLength(1);
    expect((points[0]?.value as HistogramValue | undefined)?.count).toBe(1);
  });

  it('uses meterName', async () => {
    await handle(metrics({ meterName: 'x' }), delivery(consumeHandler, [jsPart()]));
    expect(await t.scopes()).toEqual(['x']);
    await t.shutdown();
    t = setupMetrics();
    await handle(metrics(), delivery(consumeHandler, [jsPart()]));
    expect(await t.scopes()).toEqual(['@loipv/nestjs-nats']);
  });
});
