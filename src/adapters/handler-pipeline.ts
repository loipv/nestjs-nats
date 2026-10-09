import { type Codec, jsonCodec } from '../codec/codec.js';
import type { ConnectionEntry } from '../connection/connection-manager.js';
import type { HandlerDefinition } from '../discovery/handler-explorer.js';
import { resolveIdempotency } from '../idempotency/idempotency.js';
import type { IdempotencyStore } from '../idempotency/store.js';
import {
  buildInboundPipeline,
  type HandlerInfo,
  type InternalDelivery,
  toHandlerInfo,
} from '../pipeline/delivery.js';
import { invokeHandler } from '../pipeline/invoker.js';
import { type DlqPolicy, resolveDlqPolicy } from '../settlement/dlq.js';
import { type RetryPolicy, resolveRetryPolicy } from '../settlement/retry.js';
import { createSettlement } from '../settlement/settlement.js';

export interface HandlerPipelineDeps {
  /**
   * The idempotency store of a handler that enables idempotency (called once, only then);
   * `undefined` for the connection's own store (`entry.idempotencyStore()`).
   */
  idempotencyStore(
    entry: ConnectionEntry,
    handler: HandlerDefinition,
  ): IdempotencyStore | undefined;
}

/** Every handler uses its connection's idempotency store. */
export const defaultPipelineDeps: HandlerPipelineDeps = {
  idempotencyStore: (entry) => entry.idempotencyStore(),
};

/** The inbound pipeline of one handler, with the policies it was built from. */
export interface HandlerPipeline {
  /** Runs a delivery through the plugins, settlement, idempotency and the handler. */
  run(d: InternalDelivery): Promise<void>;
  codec: Codec;
  info: HandlerInfo;
  /** `false` for service endpoints: requests are never retried. */
  retry: RetryPolicy | false;
  /** `false` for service endpoints: errors are answered with an error reply. */
  dlq: DlqPolicy | false;
}

/**
 * Internal: builds the inbound pipeline of a handler from its definition and its
 * connection's options: the codec (handler, else module, else JSON), the retry and DLQ
 * policies, the settlement, the idempotency middleware (when enabled), the plugins and the
 * invoker. Shared by the adapters and the testing harness.
 */
export function createHandlerPipeline(
  def: HandlerDefinition,
  entry: ConnectionEntry,
  deps: HandlerPipelineDeps,
): HandlerPipeline {
  const codec = def.options.codec ?? entry.options.codec ?? jsonCodec();
  const info = toHandlerInfo(def);
  const plugins = entry.options.plugins ?? [];
  if (def.kind === 'service-endpoint') {
    // Requests are never retried: errors are answered with an error reply.
    const settlement = createSettlement({ entry, retry: false, dlq: false });
    const run = buildInboundPipeline({ plugins, core: [settlement] }, invokeHandler);
    return { run, codec, info, retry: false, dlq: false };
  }
  const retry = resolveRetryPolicy(entry.options.retry, def.options.retry);
  const dlq = resolveDlqPolicy(def.kind, entry.options.dlq, def.options.dlq);
  const settlement = createSettlement({ entry, retry, dlq });
  const idempotency = resolveIdempotency(
    entry,
    def.options.idempotency,
    retry,
    () => deps.idempotencyStore(entry, def) ?? entry.idempotencyStore(),
  );
  const run = buildInboundPipeline(
    { plugins, core: idempotency ? [settlement, idempotency] : [settlement] },
    invokeHandler,
  );
  return { run, codec, info, retry, dlq };
}
