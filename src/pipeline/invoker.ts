import { isObservable, lastValueFrom } from 'rxjs';
import { decodePayload } from '../codec/codec.js';
import type { Delivery } from './delivery.js';

/**
 * Terminal of the inbound pipeline: decodes the first message, invokes the handler
 * (with Nest guards/pipes/interceptors/filters) and stores its result on the delivery.
 * Synchronous throws surface as rejections.
 */
export async function invokeHandler(d: Delivery): Promise<void> {
  const msg = d.messages[0];
  const payload = msg ? decodePayload(d.codec, msg.data, msg.headers) : undefined;
  const result = await d.handler.invoke(payload, d.context);
  // Nest's ExternalContextCreator already unwraps observables (an empty one rejects with
  // EmptyError there); this only covers invokers that hand back an Observable directly.
  d.result = isObservable(result) ? await lastValueFrom(result) : result;
}
