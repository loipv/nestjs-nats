# Batching

[Back to the README](../README.md)

## Batch processing

With `batch`, a handler receives messages in groups instead of one at a time. It works for `@Subscribe` and `@Consume`. A batch is handed to the handler when `maxSize` messages are collected, or `maxWait` ms after the first message of the batch, whichever comes first.

| Option | Default | Description |
|---|---|---|
| `maxSize` | `100` | Messages per batch, an integer `>= 1` |
| `maxWait` | `1000` | Milliseconds to wait after the first message of a batch, `>= 0` |

```ts
import { Consume, Ctx, NatsBatchContext, NatsJetStreamContext, Payload } from '@loipv/nestjs-nats';

class AnalyticsHandlers {
  constructor(private readonly analytics: AnalyticsService) {}

  @Consume({
    stream: 'ORDERS',
    durable: 'analytics',
    batch: { maxSize: 50, maxWait: 500 },
  })
  async store(@Payload() items: OrderDto[], @Ctx() ctx: NatsBatchContext) {
    for (const [i, order] of items.entries()) {
      const { deliveryCount } = ctx.messages[i] as NatsJetStreamContext;
      try {
        await this.analytics.record(order, deliveryCount);
      } catch (err) {
        ctx.fail(i, err); // only this message is retried
      }
    }
  }
}
```

- **Parameters.** `@Payload()` is the array of decoded payloads. `@Ctx()` is a `NatsBatchContext` whose `messages[i]` is the context of `items[i]`, a `NatsJetStreamContext` for `@Consume`. `@Headers()`, `@Subject()` and `@Msg()` are arrays too, aligned with the payloads, and `@Payload('id')` maps the property over the items.
- **Partial failure.** `ctx.fail(target, error)` marks one message as failed, where `target` is its index in `items` or its raw message. It throws if `target` is not part of the batch. You can also return `{ failed: [{ msg, error }] }`, where `msg` is an index or a raw message. An invalid entry there is logged as an error and ignored. A failed message goes through the usual retry and DLQ rules on its own, and the others are acknowledged. If the handler throws, every message of the batch fails.
- **Undecodable messages.** A message that cannot be decoded is left out of `items`. It is not retried. If a DLQ is configured, it is dead-lettered; if that publish fails, a `@Consume` message is `nak`'d with the backoff delay so the DLQ is tried again, and a `@Subscribe` message is logged and dropped. Without a DLQ, a `@Consume` message is terminated and a `@Subscribe` message is logged and dropped. The rest of the batch is still handled.
- **Idempotency and retries** apply to each message of a batch separately, with one exception: if the idempotency store fails to claim the key of any message, the whole batch is retried (see [Idempotency](./idempotency.md)).
- **Concurrency.** A batch counts as one unit of `concurrency`.
- **No replies.** A batch never replies, so a request that lands in a `@Subscribe` batch is not answered.
- **No manual ack.** `batch` with `ack: 'manual'` fails startup with a `NatsConfigError`, because the library settles each message of a batch itself.
- **`maxWait` and `ack_wait`.** Messages waiting for their batch are heartbeated like the others (see [Heartbeats](./retry-and-dlq.md#what-happens-to-a-failed-message)). When `maxWait` is at least the consumer's `ack_wait`, they rely on those heartbeats alone, so a late one (for example a blocked event loop) can get them redelivered, and the library logs a warning at startup. Keep `maxWait` well below `ack_wait`.
- **Shutdown.** The batch being collected is handed to the handler. See [Lifecycle and graceful shutdown](./lifecycle.md).

## Batch publishing

`JetStreamClient` can publish many messages while keeping several in flight, which is much faster than awaiting each `publish()` in turn.

### `publishMany()`

```ts
import { Injectable } from '@nestjs/common';
import { JetStreamClient } from '@loipv/nestjs-nats';

@Injectable()
class OrderImporter {
  constructor(private readonly js: JetStreamClient) {}

  async import(orders: Array<{ id: string }>) {
    const results = await this.js.publishMany(
      orders.map((order) => ({
        subject: 'orders.created',
        data: order,
        opts: { msgId: order.id },
      })),
      { window: 128 },
    );
    return results.flatMap((result, i) => (result.ok ? [] : [{ order: orders[i], error: result.error }]));
  }
}
```

Each item is `{ subject, data?, opts? }`, where `opts` is the same as for `publish()`. The result array has one entry per item, in the same order: `{ ok: true, ack }` or `{ ok: false, error }`. A failed item never makes `publishMany()` reject. `window` is the number of publishes waiting for their PubAck at the same time (default `256`). Every item goes through `publish()`, so outbound plugins apply.

### `batcher()`

A batcher collects single publishes and sends them together through `publishMany()`.

```ts
import { type Batcher, JetStreamClient } from '@loipv/nestjs-nats';

class EventPublisher {
  private readonly batcher: Batcher;

  constructor(js: JetStreamClient) {
    this.batcher = js.batcher({ maxSize: 200, maxWait: 20 });
  }

  async track(events: Array<{ id: string }>) {
    // Add concurrently: each `add` resolves when its own message is acknowledged.
    const acks = await Promise.all(
      events.map((event) => this.batcher.add('events.tracked', event, { msgId: event.id })),
    );
    return acks.length;
  }
}
```

| Option | Default | Description |
|---|---|---|
| `maxSize` | `100` | Flush when this many messages are collected (an integer `>= 1`) |
| `maxWait` | `50` | Flush this many ms after the first message of a batch |
| `window` | `256` | In-flight publishes per flush, as for `publishMany()` |

`add(subject, data?, opts?)` returns a promise of the message's `PubAck`, rejected with that message's own error if it fails. `flush()` sends what is collected and waits for every result. `close()` flushes and then refuses further `add()` calls. `pending` is the number of messages not yet acknowledged or failed. A batcher is flushed and closed when its connection closes, before the connection drains.
