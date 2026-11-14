import { test } from 'vitest';
import { jsonCodec } from '../dist/codec/codec.js';
import { NatsJetStreamContext } from '../dist/context/nats-context.js';
import type { HandlerDefinition } from '../dist/discovery/handler-definition.js';
import { createIdempotency } from '../dist/idempotency/idempotency.js';
import type { IdempotencyStore } from '../dist/idempotency/store.js';
import { createDelivery } from '../dist/pipeline/delivery.js';
import { jsMessage } from './support.js';

/** A store answering every call after `ms`, like a remote one (KV, Redis). */
function remoteStore(ms: number): IdempotencyStore {
  const later = <T>(value: T) => new Promise<T>((r) => setTimeout(() => r(value), ms));
  return {
    claim: () => later('claimed' as const),
    complete: () => later(undefined),
    release: () => later(undefined),
  };
}

const def = {
  kind: 'consume',
  connection: 'default',
  className: 'H',
  methodName: 'on',
  options: { stream: 'ORD', durable: 'd', batch: {} },
  invoke: async () => undefined,
} as unknown as HandlerDefinition;
const noop = () => {};
const logger = { log: noop, warn: noop, error: noop, debug: noop };

let batchNo = 0;
function batch(size: number) {
  const n = batchNo++;
  return createDelivery({
    definition: def,
    connection: 'default',
    batch: true,
    codec: jsonCodec(),
    logger,
    parts: Array.from({ length: size }, (_, i) => {
      const raw = jsMessage({ i }, `b${n}-m${i}`);
      return {
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
    }),
  });
}

test('batch idempotency, store 1 ms away', async ({ bench }) => {
  const idem = createIdempotency({ store: remoteStore(1), ttl: 60_000, inProgressNakMs: 100 });
  // At most 16 claims are in flight: 10 messages take one round trip, 100 take about 7.
  await bench.compare(
    bench('claim and complete a batch of 10', async () => {
      await idem.handle(batch(10), async () => {});
    }),
    bench('claim and complete a batch of 100', async () => {
      await idem.handle(batch(100), async () => {});
    }),
    { iterations: 20, time: 0 },
  );
});
