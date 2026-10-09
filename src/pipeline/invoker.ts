import { isObservable, lastValueFrom } from 'rxjs';
import { decodePayload } from '../codec/codec.js';
import { HANDLER_DEFINITION, type InternalDelivery, SKIPPED } from './delivery.js';

/**
 * Terminal of the inbound pipeline: decodes the first message (unless it is skipped), invokes the handler
 * (with Nest guards/pipes/interceptors/filters) and stores its result on the delivery.
 * Synchronous throws surface as rejections.
 */
export async function invokeHandler(d: InternalDelivery): Promise<void> {
  const msg = d.messages[0];
  // A skipped message (e.g. an idempotency duplicate) is not handled and has no result.
  if (msg && d[SKIPPED].has(msg)) return;
  const payload = msg ? decodePayload(d.codec, msg.data, msg.headers) : undefined;
  const result = await d[HANDLER_DEFINITION].invoke(payload, d.context);
  // Nest's ExternalContextCreator already unwraps observables (an empty one rejects with
  // EmptyError there); this only covers invokers that hand back an Observable directly.
  d.result = isObservable(result) ? await lastValueFrom(result) : result;
}
