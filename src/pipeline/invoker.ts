import type { JsMsg } from '@nats-io/jetstream';
import type { Msg as NatsMsg } from '@nats-io/transport-node';
import { isObservable, lastValueFrom } from 'rxjs';
import { decodePayload } from '../codec/codec.js';
import { NatsBatchContext } from '../context/nats-batch-context.js';
import type { NatsContext } from '../context/nats-context.js';
import { FAILURES, HANDLER_DEFINITION, type InternalDelivery, SKIPPED } from './delivery.js';

/** What a batch handler may return to report the messages that failed. */
interface BatchResult {
  failed: Array<{ msg: number | NatsMsg | JsMsg; error: unknown }>;
}

function isBatchResult(value: unknown): value is BatchResult {
  return (
    typeof value === 'object' &&
    value !== null &&
    Array.isArray((value as Partial<BatchResult>).failed)
  );
}

async function unwrap(result: unknown): Promise<unknown> {
  // Nest's ExternalContextCreator already unwraps observables (an empty one rejects with
  // EmptyError there); this only covers invokers that hand back an Observable directly.
  return isObservable(result) ? await lastValueFrom(result) : result;
}

/**
 * Terminal of the inbound pipeline: decodes the payload, invokes the handler (with Nest
 * guards/pipes/interceptors/filters) and stores its result on the delivery. Batches are
 * handed over as an array of the decoded payloads (see `invokeBatch`). Synchronous throws
 * surface as rejections.
 */
export async function invokeHandler(d: InternalDelivery): Promise<void> {
  if (d.context instanceof NatsBatchContext) return invokeBatch(d, d.context);
  const msg = d.messages[0];
  // A skipped message (e.g. an idempotency duplicate) is not handled and has no result.
  if (msg && d[SKIPPED].has(msg)) return;
  const payload = msg ? decodePayload(d.codec, msg.data, msg.headers) : undefined;
  d.result = await unwrap(await d[HANDLER_DEFINITION].invoke(payload, d.context));
}

/**
 * Invokes a batch handler with the decoded payloads of the messages that are not skipped.
 * Undecodable messages are left out and recorded as failures; so are the ones the handler
 * fails through `ctx.fail()` or returns as `{ failed }`. A batch never has a result.
 */
async function invokeBatch(d: InternalDelivery, batch: NatsBatchContext): Promise<void> {
  const items: unknown[] = [];
  const contexts: NatsContext[] = [];
  d.messages.forEach((msg, i) => {
    const context = batch.messages[i];
    if (d[SKIPPED].has(msg) || !context) return;
    try {
      items.push(decodePayload(d.codec, msg.data, msg.headers));
      contexts.push(context);
    } catch (err) {
      d[FAILURES].set(msg, err);
    }
  });
  if (items.length === 0) return;
  // The handler's view: index-aligned with the payloads it receives.
  const view = new NatsBatchContext({
    connection: batch.connection,
    messages: contexts,
    onFail: (context, error) => batch.fail(context.raw, error),
  });
  const result = await unwrap(await d[HANDLER_DEFINITION].invoke(items, view));
  if (isBatchResult(result)) {
    for (const { msg, error } of result.failed) view.fail(msg, error);
  }
}
