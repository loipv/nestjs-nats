import { BadRequestException } from '@nestjs/common';
import { describe, expect, it, vi } from 'vitest';
import { jsonCodec } from '../../../src/codec/codec.js';
import type { ConnectionEntry } from '../../../src/connection/connection-registry.js';
import type { NatsBatchContext } from '../../../src/context/nats-batch-context.js';
import { NatsContext, NatsJetStreamContext } from '../../../src/context/nats-context.js';
import type { HandlerDefinition } from '../../../src/discovery/handler-definition.js';
import { createIdempotency } from '../../../src/idempotency/idempotency.js';
import type { ClaimResult, IdempotencyStore } from '../../../src/idempotency/store.js';
import { compose } from '../../../src/pipeline/compose.js';
import {
  createDelivery,
  type DeliveryPart,
  type InboundMessage,
  type InternalDelivery,
  type MessageOutcome,
} from '../../../src/pipeline/delivery.js';
import type { InboundMiddleware } from '../../../src/pipeline/types.js';
import { type DlqPolicy, resolveDlqPolicy } from '../../../src/settlement/dlq.js';
import { type RetryPolicy, resolveRetryPolicy } from '../../../src/settlement/retry.js';
import { createSettlement } from '../../../src/settlement/settlement.js';
import { mockLogger } from '../../support/logger.js';

const retry = resolveRetryPolicy({ initialDelay: 100, jitter: 0 }) as RetryPolicy;
const consumeDlq = resolveDlqPolicy('consume', true) as DlqPolicy;
const coreDlq = resolveDlqPolicy('subscribe', true) as DlqPolicy;

function fakeEntry(jsPublish: () => Promise<unknown> = () => Promise.resolve({ seq: 1 })) {
  return {
    name: 'default',
    options: {},
    logger: mockLogger(),
    nc: { publish: vi.fn() },
    js: () => ({ publish: vi.fn(jsPublish) }),
  } as unknown as ConnectionEntry;
}

const jsPart = (attempt: number, id?: string): DeliveryPart => {
  const raw = {
    subject: 'ord.created',
    reply: '$JS.ACK.ORD.d.1.1.1.0.0',
    data: new TextEncoder().encode('{"id":1}'),
    headers: id ? { get: (k: string) => (k === 'Nats-Msg-Id' ? id : '') } : undefined,
    ack: vi.fn(),
    nak: vi.fn(),
    term: vi.fn(),
    working: vi.fn(),
    info: { stream: 'ORD', consumer: 'd', streamSequence: 7, deliveryCount: attempt },
  };
  return {
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
  };
};

const corePart = (attempt = 1, reply = '', id?: string): DeliveryPart => {
  const raw = {
    subject: 'orders.x',
    reply,
    data: new Uint8Array([1]),
    headers: id ? { get: (k: string) => (k === 'Nats-Msg-Id' ? id : '') } : undefined,
  };
  return {
    message: {
      subject: raw.subject,
      data: raw.data,
      headers: raw.headers as never,
      raw: raw as never,
      attempt,
    },
    context: new NatsContext({ kind: 'core', connection: 'default', raw: raw as never }),
  };
};

function delivery(
  kind: 'consume' | 'subscribe' | 'service-endpoint',
  parts: DeliveryPart[],
  opts: {
    batch?: boolean;
    ack?: 'auto' | 'manual';
    retry?: InternalDelivery['retry' & never];
  } = {},
) {
  const definition = {
    kind,
    connection: 'default',
    className: 'H',
    methodName: 'on',
    ...(kind === 'consume' ? { options: { stream: 'ORD', durable: 'd', ack: opts.ack } } : {}),
    ...(kind === 'subscribe' ? { subject: 'orders.*', options: {} } : {}),
    ...(kind === 'service-endpoint'
      ? { endpoint: 'e', options: {}, service: { name: 's', version: '1.0.0' } }
      : {}),
  } as unknown as HandlerDefinition;
  const schedule = vi.fn();
  return {
    d: createDelivery({
      definition,
      connection: 'default',
      batch: opts.batch ?? false,
      parts,
      codec: jsonCodec(),
      logger: mockLogger(),
      ...(kind === 'subscribe' ? { retry: schedule } : {}),
    }),
    schedule,
  };
}

const run = (
  d: InternalDelivery,
  mws: InboundMiddleware[],
  handler: (d: InternalDelivery) => Promise<void> = async () => {},
) =>
  compose<InternalDelivery>(
    mws.map((mw) => (dd, next) => mw.handle(dd, next)),
    handler,
  )(d);

const outcome = (d: InternalDelivery, i = 0) => d.outcomes.get(d.messages[i] as InboundMessage);

describe('Delivery.outcomes — JetStream', () => {
  it.each<[string, Error | undefined, number, boolean, MessageOutcome]>([
    ['success', undefined, 1, false, { action: 'ack' }],
    [
      'retryable',
      new Error('x'),
      1,
      false,
      { action: 'retry', delayMs: 100, error: expect.any(Error) },
    ],
    [
      'exhausted, dlq on',
      new Error('x'),
      6,
      true,
      { action: 'dlq', subject: 'dlq.ORD.d', error: expect.any(Error) },
    ],
    [
      'non-retryable, dlq off',
      new BadRequestException(),
      1,
      false,
      { action: 'term', error: expect.any(BadRequestException) },
    ],
  ])('records %s', async (_, err, attempt, withDlq, expected) => {
    const { d } = delivery('consume', [jsPart(attempt)]);
    const mw = createSettlement({ entry: fakeEntry(), retry, dlq: withDlq ? consumeDlq : false });
    await run(d, [mw], async () => {
      if (err) throw err;
    });
    expect(outcome(d)).toEqual(expected);
  });

  it('records the handler error, as a retry, when the DLQ publish fails', async () => {
    const { d } = delivery('consume', [jsPart(6)]);
    const boom = new Error('handler');
    const entry = fakeEntry(() => Promise.reject(new Error('dlq down')));
    await run(d, [createSettlement({ entry, retry, dlq: consumeDlq })], async () => {
      throw boom;
    });
    expect(outcome(d)).toMatchObject({ action: 'retry', error: boom });
    expect(d.outcomes.size).toBe(1);
  });

  it('records a retry when the idempotency store is down', async () => {
    const { d } = delivery('consume', [jsPart(1, 'm1')]);
    const store = {
      claim: vi.fn(() => Promise.reject(new Error('redis'))),
      complete: vi.fn(),
      release: vi.fn(),
    } as unknown as IdempotencyStore;
    const idem = createIdempotency({ store, ttl: 1000, inProgressNakMs: 50 });
    await run(d, [createSettlement({ entry: fakeEntry(), retry, dlq: false }), idem]);
    expect(outcome(d)).toMatchObject({ action: 'retry', delayMs: 100 });
  });

  it('records manual ack, left unacked, and nak(delay)', async () => {
    const acked = delivery('consume', [jsPart(1)], { ack: 'manual' });
    const left = delivery('consume', [jsPart(1)], { ack: 'manual' });
    const naked = delivery('consume', [jsPart(1)], { ack: 'manual' });
    const noDelay = delivery('consume', [jsPart(1)], { ack: 'manual' });
    const termed = delivery('consume', [jsPart(1)], { ack: 'manual' });
    const mw = createSettlement({ entry: fakeEntry(), retry, dlq: false });
    await run(acked.d, [mw], async (d) => (d.context as NatsJetStreamContext).ack());
    await run(left.d, [mw]);
    await run(naked.d, [mw], async (d) => (d.context as NatsJetStreamContext).nak(250));
    await run(noDelay.d, [mw], async (d) => (d.context as NatsJetStreamContext).nak());
    await run(termed.d, [mw], async (d) => (d.context as NatsJetStreamContext).term());
    expect(outcome(acked.d)).toEqual({ action: 'ack' });
    expect(outcome(left.d)).toEqual({ action: 'none' });
    expect(outcome(naked.d)).toMatchObject({ action: 'retry', delayMs: 250 });
    expect(outcome(noDelay.d)).toMatchObject({ action: 'retry', delayMs: 0 });
    expect(outcome(termed.d)).toMatchObject({ action: 'term' });
  });

  it('keeps the manual settlement and adds the error the handler then threw', async () => {
    const { d } = delivery('consume', [jsPart(1)], { ack: 'manual' });
    const boom = new Error('after nak');
    await run(d, [createSettlement({ entry: fakeEntry(), retry, dlq: false })], async (dd) => {
      (dd.context as NatsJetStreamContext).nak(250);
      throw boom;
    });
    expect(outcome(d)).toEqual({ action: 'retry', delayMs: 250, error: boom });
  });

  it('records skipped duplicates and in-progress messages', async () => {
    for (const reason of ['duplicate', 'in-progress'] as const) {
      const { d } = delivery('consume', [jsPart(1, 'm1')]);
      const store = {
        claim: vi.fn(() => Promise.resolve(reason as ClaimResult)),
        complete: vi.fn(),
        release: vi.fn(),
      } as unknown as IdempotencyStore;
      const idem = createIdempotency({ store, ttl: 1000, inProgressNakMs: 50 });
      await run(d, [createSettlement({ entry: fakeEntry(), retry, dlq: false }), idem]);
      expect(outcome(d)).toEqual({ action: 'skipped', reason });
    }
  });

  it('is visible to an outer plugin after next()', async () => {
    const { d } = delivery('consume', [jsPart(1)]);
    let seen: MessageOutcome | undefined;
    const outer: InboundMiddleware = {
      stage: 'outer',
      async handle(dd, next) {
        await next();
        seen = dd.outcomes.get(dd.messages[0] as InboundMessage);
      },
    };
    await run(d, [outer, createSettlement({ entry: fakeEntry(), retry, dlq: false })]);
    expect(seen).toEqual({ action: 'ack' });
  });

  it('records one outcome per batch message', async () => {
    const { d } = delivery('consume', [jsPart(1), jsPart(2), jsPart(1)], { batch: true });
    await run(d, [createSettlement({ entry: fakeEntry(), retry, dlq: false })], async (dd) => {
      (dd.context as NatsBatchContext).fail(1, new Error('x'));
    });
    expect(d.outcomes.size).toBe(3);
    expect(outcome(d, 0)).toEqual({ action: 'ack' });
    expect(outcome(d, 1)).toMatchObject({ action: 'retry', delayMs: 200 });
    expect(outcome(d, 2)).toEqual({ action: 'ack' });
  });
});

describe('Delivery.outcomes — core', () => {
  it('records core retry (in-process), drop and dlq', async () => {
    const retrying = delivery('subscribe', [corePart(1)]);
    await run(
      retrying.d,
      [createSettlement({ entry: fakeEntry(), retry, dlq: false })],
      async () => {
        throw new Error('x');
      },
    );
    expect(outcome(retrying.d)).toMatchObject({ action: 'retry', delayMs: 100 });
    expect(retrying.schedule).toHaveBeenCalledTimes(1);

    const dropped = delivery('subscribe', [corePart(6)]);
    await run(
      dropped.d,
      [createSettlement({ entry: fakeEntry(), retry, dlq: false })],
      async () => {
        throw new Error('x');
      },
    );
    expect(outcome(dropped.d)).toMatchObject({ action: 'drop' });

    const dead = delivery('subscribe', [corePart(6)]);
    await run(dead.d, [createSettlement({ entry: fakeEntry(), retry, dlq: coreDlq })], async () => {
      throw new Error('x');
    });
    expect(outcome(dead.d)).toMatchObject({ action: 'dlq' });
  });

  it('records none for a successful core message and a core drop when the DLQ fails', async () => {
    const ok = delivery('subscribe', [corePart(1)]);
    await run(ok.d, [createSettlement({ entry: fakeEntry(), retry, dlq: false })]);
    expect(outcome(ok.d)).toEqual({ action: 'none' });

    const entry = fakeEntry();
    (entry.nc as unknown as { publish: () => never }).publish = () => {
      throw new Error('closed');
    };
    const failed = delivery('subscribe', [corePart(6)]);
    await run(failed.d, [createSettlement({ entry, retry, dlq: coreDlq })], async () => {
      throw new Error('x');
    });
    expect(outcome(failed.d)).toMatchObject({ action: 'drop' });
  });

  it('records skipped core duplicates', async () => {
    const { d } = delivery('subscribe', [corePart(1, '', 'm1')]);
    const store = {
      claim: vi.fn(() => Promise.resolve('duplicate' as ClaimResult)),
      complete: vi.fn(),
      release: vi.fn(),
    } as unknown as IdempotencyStore;
    const idem = createIdempotency({ store, ttl: 1000, inProgressNakMs: 50 });
    await run(d, [createSettlement({ entry: fakeEntry(), retry, dlq: false }), idem]);
    expect(outcome(d)).toEqual({ action: 'skipped', reason: 'duplicate' });
  });

  it('records replied with and without error, and rethrows the error', async () => {
    for (const kind of ['subscribe', 'service-endpoint'] as const) {
      const ok = delivery(kind, [corePart(1, '_INBOX.x')]);
      const mw = createSettlement({ entry: fakeEntry(), retry, dlq: false });
      await run(ok.d, [mw]);
      expect(outcome(ok.d)).toEqual({ action: 'replied' });

      const failing = delivery(kind, [corePart(1, '_INBOX.x')]);
      const boom = new Error('x');
      await expect(
        run(failing.d, [mw], async () => {
          throw boom;
        }),
      ).rejects.toBe(boom);
      expect(outcome(failing.d)).toEqual({ action: 'replied', error: boom });
    }
  });
});
