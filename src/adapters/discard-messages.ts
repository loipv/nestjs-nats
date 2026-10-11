import type { ConsumerMessages, JsMsg } from '@nats-io/jetstream';

/**
 * Closes consumer `messages` that nothing reads, and reads them to their end: nats.js
 * resolves `close()`, and releases a push consumer's subscription, only once the
 * iterator has been read past the stop. Each message still buffered goes to `discard`.
 * Never rejects; a failing close or iterator goes to `onError`.
 */
export async function discardMessages(
  messages: ConsumerMessages,
  discard: (msg: JsMsg) => void,
  onError: (err: unknown) => void,
): Promise<void> {
  let closing: Promise<void>;
  try {
    closing = messages.close().then(() => undefined, onError);
  } catch (err) {
    onError(err);
    closing = Promise.resolve();
  }
  try {
    for await (const msg of messages) discard(msg);
  } catch (err) {
    onError(err);
  }
  await closing;
}
