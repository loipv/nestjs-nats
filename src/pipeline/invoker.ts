import type { JsMsg } from '@nats-io/jetstream';
import type { Msg as NatsMsg } from '@nats-io/transport-node';
import { isObservable, lastValueFrom } from 'rxjs';
import { parseCloudEvent } from '../cloudevents/codec.js';
import { UnmatchedCloudEventError } from '../cloudevents/errors.js';
import { decodePayload } from '../codec/codec.js';
import { NatsBatchContext } from '../context/nats-batch-context.js';
import { CLOUD_EVENT, type NatsContext } from '../context/nats-context.js';
import type { CloudEventRouting } from '../discovery/handler-explorer.js';
import { NatsModuleError } from '../errors/errors.js';
import { errorMessage } from '../util/errors.js';
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
 * handed over as an array of the decoded payloads (see `invokeBatch`); a CloudEvent router
 * hands the message to its matching route (see `invokeRoute`). Synchronous throws
 * surface as rejections.
 */
export async function invokeHandler(d: InternalDelivery): Promise<void> {
  if (d.context instanceof NatsBatchContext) return invokeBatch(d, d.context);
  const msg = d.messages[0];
  // A skipped message (e.g. an idempotency duplicate) is not handled and has no result.
  if (msg && d[SKIPPED].has(msg)) return;
  const { router } = d[HANDLER_DEFINITION];
  if (router) return invokeRoute(d, d.context, router);
  const payload = msg ? decodePayload(d.codec, msg.data, msg.headers) : undefined;
  d.result = await unwrap(await d[HANDLER_DEFINITION].invoke(payload, d.context));
}

/**
 * Reads the CloudEvent attributes of the message (a `NatsDecodeError` when it is not a
 * valid CloudEvent), presets `ctx.cloudEvent` and invokes the `@OnCloudEvent` route
 * matching its type with its decoded `data` (a `NatsDecodeError` when undecodable). With no matching route, policy `'ack'` succeeds without a result; the others,
 * and a request under any policy (for its 404 reply), throw an `UnmatchedCloudEventError`
 * for settlement to apply.
 */
async function invokeRoute(
  d: InternalDelivery,
  ctx: NatsContext,
  router: CloudEventRouting,
): Promise<void> {
  const msg = d.messages[0];
  if (!msg) throw new NatsModuleError('A CloudEvent router delivery holds no message');
  // Attributes only: the data is decoded once a route matched, so an unmatched event
  // with an undecodable body still follows the `unmatched` policy.
  const event = parseCloudEvent(msg);
  const { meta } = event;
  ctx[CLOUD_EVENT] = meta;
  const target = router.route(meta.type);
  if (target) {
    d.result = await unwrap(await target.invoke(event.data(d.codec), ctx));
    return;
  }
  if (router.unmatched === 'ack' && ctx.replyTo === undefined) {
    d.logger.debug?.(
      `No handler for CloudEvent type "${meta.type}" in ${d.handler.className} on "${msg.subject}"; acknowledged`,
    );
    return;
  }
  throw new UnmatchedCloudEventError(meta.type, router.unmatched);
}

/**
 * Invokes a batch handler with the decoded payloads of the messages that are not skipped.
 * Undecodable messages are left out and recorded as failures; so are the ones the handler
 * fails through `ctx.fail()` or returns as `{ failed }`. An invalid entry of `{ failed }`
 * (unknown index, message not in the batch) is logged and skipped; the rest of the batch
 * is settled normally. A batch never has a result.
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
  if (!isBatchResult(result)) return;
  const { className, methodName } = d.handler;
  for (const entry of result.failed as unknown[]) {
    try {
      if (typeof entry !== 'object' || entry === null) {
        throw new TypeError(`expected { msg, error }, got ${String(entry)}`);
      }
      const { msg, error } = entry as BatchResult['failed'][number];
      view.fail(msg, error);
    } catch (err) {
      d.logger.error(
        `${className}.${methodName} returned an invalid entry in { failed }, ignored: ${errorMessage(err)}`,
      );
    }
  }
}
