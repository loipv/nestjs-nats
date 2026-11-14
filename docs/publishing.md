# Publishing

[Back to the README](../README.md)

## `NatsClient`: core NATS

```ts
// Fire-and-forget. Delivery errors are logged (or handed to onError), never thrown.
client.publish('orders.created', { id: 42, total: 99.5 }, { headers: { 'x-tenant': 't1' } });

// Request/reply. The default timeout is 5000 ms.
const found = await client.request<OrderDto>('orders.get', { id: 42 }, { timeout: 2000 });

// Wait until pending publishes have been flushed to the server.
await client.flush();
```

Invalid arguments are the exception: a bad header name or value, or an invalid `cloudEvent`, makes `publish()` throw synchronously, before anything is sent.

`request()` throws `NatsRequestError` with one of these `code` values:

| `code` | Meaning |
|---|---|
| `'timeout'` | No reply within the timeout |
| `'no_responders'` | Nobody is subscribed to the subject |
| `'service_error'` | The responder replied with an error. `serviceCode` (the `Nats-Service-Error-Code` header as a number; left unset when the header is not an integer) and `description` are filled in |

Outbound plugins must pass a request on within its timeout, or `request()` rejects with a `NatsModuleError` and nothing is sent; once it is sent, `request()` settles with the reply (or the `NatsRequestError`) without waiting for the plugins to return. A message given to `publish()` that the plugins do not pass on within 5000 ms is dropped, with an error logged, or handed to its [error handler](#publish-errors). See [Outbound timing](./plugins.md#outbound-timing).

### Publish errors

`publish()` returns before the message is sent, so it cannot throw a failure to send: the payload cannot be encoded (a circular value, a BigInt), the connection is closed or draining, an outbound plugin failed before passing the message on, or the plugins did not pass it on within 5000 ms. By default the library logs these. To act on them (retry, keep the message in an outbox, count them), give the connection an `onPublishError`, or a single call an `onError`, which takes precedence:

```ts
import { NatsModule, type NatsClient } from '@loipv/nestjs-nats';

NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  onPublishError: (error, { subject, connection }) => {
    console.warn(`publish to ${subject} on ${connection} failed: ${error.message}`);
  },
});

declare const client: NatsClient;
declare const outbox: { save(subject: string, data: unknown): void };
const event = { id: 42 };
client.publish('audit.order', event, { onError: () => outbox.save('audit.order', event) });
```

- **Instead of the log.** An error handed to a handler is not logged. If the handler throws, or returns a promise that rejects, the library logs that (`the publish error handler failed`) and then the original error, and `publish()` still does not throw. `flush()` waits for a promise the handler returns.
- **When.** On a connection without outbound plugins, the handler runs before `publish()` returns. With plugins, it runs when the failure happens, and `flush()` waits for it.
- **Not errors.** A plugin that drops a message on purpose (returns without calling `next()`), or fails after the message was sent, does not call the handler: the message was not lost to a failure. Invalid arguments still make `publish()` throw synchronously.
- **Unmasked.** The handler gets the error itself. Logs mask credentials; mask them yourself if you log the error.

### Many replies

`requestMany()` sends one request and gathers every reply, for scatter-gather: asking each instance of a service for its status, or each shard for its part of an answer.

```ts
import type { NatsClient } from '@loipv/nestjs-nats';

declare const client: NatsClient;

const replies = await client.requestMany<{ healthy: boolean }>('svc.status', undefined, {
  maxWait: 500, // stop after 500 ms (default 1000)
  maxMessages: 10, // or once 10 replies arrived
});
for (const reply of replies) {
  if ('error' in reply) console.warn(reply.error.message);
  else console.log(reply.data.healthy);
}
```

It stops when `maxWait` ends, when `maxMessages` replies arrived, or, with `stall`, when no reply came for `stall` ms after the last one; never later than `maxWait`. Each reply is `{ data, headers }`, or `{ error }` when it carries a service error (a `NatsRequestError` with `code: 'service_error'`) or cannot be decoded (a `NatsDecodeError`): one failing responder does not hide the others. Replies all arrive on one inbox, so they do not tell the responders apart: put an id in the reply's data or headers for that. `maxWait` is at least 1 ms. When nobody is subscribed to the subject, it resolves `[]`. Other failures, such as a closed connection, reject. `headers`, `codec` and `cloudEvent` work as for `request()`, and the request goes through the outbound plugins like one, which have `maxWait` to pass it on.

## `JetStreamClient`

```ts
import { Injectable } from '@nestjs/common';
import { JetStreamClient } from '@loipv/nestjs-nats';

@Injectable()
class OrderPublisher {
  constructor(private readonly js: JetStreamClient) {}

  async created(order: { id: string }) {
    const ack = await this.js.publish('orders.created', order, {
      msgId: order.id, // the server de-duplicates within the stream's duplicate window
    });
    return ack.seq; // ack.duplicate is true if this message was a duplicate
  }
}
```

`publish()` also accepts `expect` (optimistic concurrency, for example `{ lastSubjectSequence: 41 }`), `headers`, `codec` and `timeout`. `msgId` is sent as the `Nats-Msg-Id` header, so plugins can see it. Prefer `msgId` over passing the header yourself: a `Nats-Msg-Id` header in `headers` is still honoured (it also replaces the CloudEvent `id` default), and one with another case, such as `nats-msg-id`, is sent as `Nats-Msg-Id`, but `msgId` states the intent and wins if both are given. To publish a [CloudEvent](./cloudevents.md), pass `cloudEvent` instead of building its headers. To publish many messages at once, see [Batch publishing](./batching.md#batch-publishing), and for all-or-nothing, [Atomic batches](#atomic-batches).

Outbound plugins must pass the message on within `timeout` (default the JetStream client's), or `publish()` rejects with a `NatsModuleError` and nothing is sent. Once the message is sent, `publish()` settles with the `PubAck` (or the publish failure) without waiting for the plugins to return; a plugin failing after that is logged. See [Outbound timing](./plugins.md#outbound-timing).

### Atomic batches

`publishAtomic()` stores several messages as one unit: the stream keeps all of them, in order, or none.

```ts
import { Injectable } from '@nestjs/common';
import { JetStreamClient } from '@loipv/nestjs-nats';

@Injectable()
class OrderPlacer {
  constructor(private readonly js: JetStreamClient) {}

  async place(order: { id: string; sku: string; qty: number }) {
    const ack = await this.js.publishAtomic([
      { subject: 'orders.created', data: order },
      { subject: 'stock.reserved', data: { orderId: order.id, sku: order.sku, qty: order.qty } },
    ]);
    return ack.seq; // the sequence of the last message; ack.count is 2
  }
}
```

Each message is `{ subject, data?, headers?, codec?, cloudEvent? }`. The result is `{ stream, seq, count, batch, duplicate: false }`: `seq` is the sequence of the last message and `batch` is the batch id (unset for a single message). `opts` takes `timeout` (the wait for the commit's ack, and for the outbound plugins to pass every message on before the batch is sent, in ms, above `0` and at most `2147483647`, which is 2^31-1: a larger or infinite value throws a `NatsConfigError`; default the JetStream client's, at most 2^31-1) and `expect` (checked on the first message; there is no `lastMsgID`).

- **Requirements.** NATS server 2.12 or later, and a stream declared with `allow_atomic: true` (see [Streams](./streams.md)). A stream without `allow_atomic` rejects the batch.
- **Limits.** 1 to 1000 messages per batch. The server abandons a batch that is idle for 10 seconds, and allows 50 open batches per stream.
- **No de-duplication.** A message with a `Nats-Msg-Id` header makes `publishAtomic()` throw a `NatsConfigError` before anything is sent, and a CloudEvent's `id` is not used as `Nats-Msg-Id`, unlike in `publish()`. Retrying a batch whose commit ack was lost can store it twice.
- **One message** is published like `JetStreamClient.publish()`: it needs no `allow_atomic`, `batch` is unset, and its failures are those of a batch: a `NatsAtomicPublishError` when sending fails or when the outbound plugins do not pass it on (within `timeout`, or at all; nothing is sent then), and the plugin's own error when a plugin throws before passing it on.
- **Failures.** A server or transport error rejects with a `NatsAtomicPublishError` (`subject`, `count`, a `cause` with credentials masked). Some commit-time refusals come from nats.js as the generic error "batch didn't contain number of published messages": that error is wrapped the same way, so it arrives as a `NatsAtomicPublishError` whose message ends with that text and whose `cause` is a copy of that error with credentials masked. Nothing is stored when the server refused the batch or when it was aborted before being sent. A timeout waiting for the commit's ack is different: the server may have stored the batch anyway, so check the stream before retrying (see No de-duplication above). Invalid arguments throw a `NatsConfigError` instead.
- **Plugins.** Every message goes through the outbound plugins in array order, with `message.atomic = { index, count }`, and the batch is sent once all of them are ready. A plugin that throws before that aborts the whole batch: nothing is sent and `publishAtomic()` rejects with the plugin's error. If the plugins have not passed every message on within `timeout`, the batch is aborted too: nothing is sent and `publishAtomic()` rejects with a `NatsAtomicPublishError` ("aborted: not every message was staged within … ms"). Once the batch is sent, `publishAtomic()` settles with the commit's result, the ack or a `NatsAtomicPublishError`, without waiting for the plugins to return. A plugin that throws after that (after its `next()`, or giving up on a `next()` still waiting for the commit) is logged as an error on the connection's logger when it does, and does not change the result. [Tracing](./tracing.md) adds `messaging.batch.message_count` to the spans.

[`publishMany()`](./batching.md#batch-publishing) is different: it publishes messages independently, several in flight at once, and some can fail while others are stored. Use `publishAtomic()` when partial results are not acceptable (an order and its stock reservation), and `publishMany()` for throughput.

## Codecs

Payloads are JSON by default. A `Uint8Array` is always sent as-is, `undefined` is sent as an empty payload, and an empty payload decodes to `undefined`. A value JSON cannot represent (a function, a symbol, a `toJSON()` returning `undefined`, as well as circular values and BigInts) is not sent: encoding throws a `TypeError`, which `request()` and `JetStreamClient.publish()` reject with and `NatsClient.publish()` logs (or hands to its [error handler](#publish-errors)). You can provide your own codec per connection (`codec` in `forRoot`), per handler, or per publish call:

```ts
import type { Codec } from '@loipv/nestjs-nats';

const textCodec: Codec = {
  contentType: 'text/plain',
  encode: (data) => new TextEncoder().encode(String(data)),
  decode: (bytes) => new TextDecoder().decode(bytes),
};
```

If decoding fails, the error is a `NatsDecodeError`. Requests then get a `400` reply, and JetStream messages are terminated.
