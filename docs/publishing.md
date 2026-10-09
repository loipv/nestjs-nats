# Publishing

[Back to the README](../README.md)

## `NatsClient`: core NATS

```ts
// Fire-and-forget. Errors are logged, never thrown.
client.publish('orders.created', { id: 42, total: 99.5 }, { headers: { 'x-tenant': 't1' } });

// Request/reply. The default timeout is 5000 ms.
const found = await client.request<OrderDto>('orders.get', { id: 42 }, { timeout: 2000 });

// Wait until pending publishes have been flushed to the server.
await client.flush();
```

`request()` throws `NatsRequestError` with one of these `code` values:

| `code` | Meaning |
|---|---|
| `'timeout'` | No reply within the timeout |
| `'no_responders'` | Nobody is subscribed to the subject |
| `'service_error'` | The responder replied with an error. `serviceCode` and `description` are filled in |

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

`publish()` also accepts `expect` (optimistic concurrency, for example `{ lastSubjectSequence: 41 }`), `headers`, `codec` and `timeout`. `msgId` is sent as the `Nats-Msg-Id` header, so plugins can see it. To publish a [CloudEvent](./cloudevents.md), pass `cloudEvent` instead of building its headers. To publish many messages at once, see [Batch publishing](./batching.md#batch-publishing).

## Codecs

Payloads are JSON by default. A `Uint8Array` is always sent as-is, and an empty payload decodes to `undefined`. You can provide your own codec per connection (`codec` in `forRoot`), per handler, or per publish call:

```ts
import type { Codec } from '@loipv/nestjs-nats';

const textCodec: Codec = {
  contentType: 'text/plain',
  encode: (data) => new TextEncoder().encode(String(data)),
  decode: (bytes) => new TextDecoder().decode(bytes),
};
```

If decoding fails, the error is a `NatsDecodeError`. Requests then get a `400` reply, and JetStream messages are terminated.
