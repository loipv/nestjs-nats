import { test } from 'vitest';
import { jsonCodec } from '../dist/codec/codec.js';
import { NatsJetStreamContext } from '../dist/context/nats-context.js';
import type { HandlerDefinition } from '../dist/discovery/handler-definition.js';
import { createIdempotency } from '../dist/idempotency/idempotency.js';
import { InMemoryIdempotencyStore } from '../dist/idempotency/store.js';
import { compose } from '../dist/pipeline/compose.js';
import { createDelivery, type InternalDelivery } from '../dist/pipeline/delivery.js';
import { invokeHandler } from '../dist/pipeline/invoker.js';
import type { InboundMiddleware } from '../dist/pipeline/types.js';
import { type RetryPolicy, resolveRetryPolicy } from '../dist/settlement/retry.js';
import { createSettlement } from '../dist/settlement/settlement.js';
import { benchEntry, jsMessage } from './support.js';

const def = {
  kind: 'consume',
  connection: 'default',
  className: 'H',
  methodName: 'on',
  options: { stream: 'ORD', durable: 'd' },
  invoke: async (payload: unknown) => payload,
} as unknown as HandlerDefinition;
const noop = () => {};
const logger = { log: noop, warn: noop, error: noop, debug: noop };
const retry = resolveRetryPolicy({ initialDelay: 100, jitter: 0 }) as RetryPolicy;
const settlement = createSettlement({ entry: benchEntry(), retry, dlq: false });
const toMw = (mw: InboundMiddleware) => (d: InternalDelivery, next: () => Promise<void>) =>
  mw.handle(d as never, next);

let n = 0;
function delivery() {
  const raw = jsMessage({ id: n }, `m${n++}`);
  return createDelivery({
    definition: def,
    connection: 'default',
    batch: false,
    codec: jsonCodec(),
    logger,
    parts: [
      {
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
      },
    ],
  });
}

test('inbound pipeline, one JetStream message', async ({ bench }) => {
  const plain = compose<InternalDelivery>([toMw(settlement)], invokeHandler);
  const store = new InMemoryIdempotencyStore();
  const idem = createIdempotency({ store, ttl: 60_000, inProgressNakMs: 100 });
  const withIdempotency = compose<InternalDelivery>([toMw(settlement), toMw(idem)], invokeHandler);
  try {
    await bench.compare(
      bench('settlement + invoke', async () => {
        await plain(delivery());
      }),
      bench('settlement + in-memory idempotency + invoke', async () => {
        await withIdempotency(delivery());
      }),
    );
  } finally {
    await store.close();
  }
});
