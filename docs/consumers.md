# Pull, push and ordered consumers

[Back to the README](../README.md)

The library has three ways to read a JetStream stream, each with its own decorator:

| | `@Consume` (pull) | `@ConsumePush` (push) | `@ConsumeOrdered` (ordered) |
|---|---|---|---|
| **Use it for** | Work queues and most handlers: the default choice | Low latency, or when the server should push to a group of instances | Projections, caches and read models that need every message in stream order |
| **Consumer** | Durable | Durable | Ephemeral, created and recreated by the client |
| **Load** | Instances with the same `durable` share the messages | Instances with the same `deliverGroup` share the messages. Without it, only one instance can bind | Fan-out: every instance receives every message |
| **Ack** | `ack: 'auto'` or `'manual'` | `ack: 'auto'` or `'manual'` | None. A failed message is retried in place, then dead-lettered or dropped |
| **Concurrency and `batch`** | Yes | Yes | No: strictly one message at a time, in order |
| **Reading** | The library pulls just enough messages to keep its slots busy | The server pushes messages as they arrive, up to `max_ack_pending` | The client reads ahead and re-creates the consumer on a gap |
| **Heartbeat** | Every `ack_wait / 2` for messages received, with a [small gap](./retry-and-dlq.md#what-happens-to-a-failed-message) | Every `ack_wait / 2` for every message received, no gap | None, nothing to acknowledge |
| **Shutdown** | Messages being handled or waiting are `nak`'d after `drainTimeout` | Same as pull | A pending retry wait is abandoned. Messages not finished are not redelivered |
| **Retry** | `nak(delay)`: the server redelivers | `nak(delay)`: the server redelivers | In place, with the backoff delay, before the next message |
| **Dead-letter queue** | `dlq.<stream>.<durable>` | `dlq.<stream>.<durable>` | `dlq.<stream>.ordered.<Class>.<method>` |
| **`@CloudEventRouter`** | Yes, with `consume` | No | No |

Declare the stream and, for pull and push, the durable consumer in [`forRoot` or with `config`](./streams.md). All three work with `retry`, `dlq`, `idempotency` and `codec`. The options of each decorator are listed in [Handlers](./handlers.md).

## Pull: `@Consume`

A pull consumer asks the server for messages when it has room for them. It is the right default: simple to reason about, and the library never receives more than it can hold. See [`@Consume`](./handlers.md#consumeoptions-jetstream-pull-consumer).

## Push: `@ConsumePush`

A push consumer has a delivery subject, and the server sends messages to it as soon as they exist.

```ts
import { Ctx, ConsumePush, type NatsJetStreamContext, Payload } from '@loipv/nestjs-nats';

class BillingHandlers {
  @ConsumePush({
    stream: 'ORDERS',
    durable: 'billing-push',
    deliverGroup: 'billing', // instances of this group share the durable's messages
    config: { filter_subject: 'orders.created', max_ack_pending: 100 },
    concurrency: 10,
  })
  async bill(@Payload() order: { id: string }, @Ctx() ctx: NatsJetStreamContext) {
    console.log(order.id, ctx.deliveryCount);
  }
}
```

- **Without `deliverGroup`, only one instance can bind.** A second instance fails startup with a `NatsConfigError` that suggests a `deliverGroup`. Give every instance the same `deliverGroup` (or `config.deliver_group`, which must agree with it) to load-balance.
- **Messages are read at once.** The handler does not wait for a free slot: messages the server sent queue in the process, and every one of them is heartbeated while it waits. What bounds the queue is the consumer's `max_ack_pending`. A consumer the library creates without a group gets `max_ack_pending` equal to what the handler can hold (`concurrency`, or `max(batch.maxSize, concurrency)` with `batch`), plus `flow_control: true`, `idle_heartbeat: 5 s` and a delivery subject `_nats.push.<stream>.<durable>`.
- **A group shares one `max_ack_pending`, so the library leaves it at the server default (1000).** NATS does not allow `flow_control` or `idle_heartbeat` together with a delivery group, so a grouped consumer has neither and never reports missed heartbeats. Set `config.max_ack_pending` to bound the queue per group.
- **Defaults apply only to a consumer the library creates.** A consumer that already exists is used as it is, and startup rejects one that is a pull consumer, has a different delivery group than the decorator, or has an unlimited `max_ack_pending`.
- **The rest is as for `@Consume`:** `ack`, `retry`, `dlq` (same subject), `idempotency`, `batch`, and `nak` of everything not finished at a forced shutdown. A `batch` still cannot be combined with `ack: 'manual'`.

## Ordered: `@ConsumeOrdered`

An ordered consumer is an ephemeral consumer without acknowledgements that the client recreates whenever it detects a gap, so it always delivers messages in stream order. Use it where every instance needs the whole ordered history or live feed of a stream: an in-memory projection, a cache.

```ts
import { ConsumeOrdered, Ctx, NatsOrderedContext, Payload } from '@loipv/nestjs-nats';

class OrdersProjection {
  private readonly byStatus = new Map<string, number>();

  @ConsumeOrdered({ stream: 'ORDERS', filterSubjects: 'orders.status.>', deliverPolicy: 'all' })
  project(@Payload() event: { status: string }, @Ctx() ctx: NatsOrderedContext) {
    this.byStatus.set(event.status, (this.byStatus.get(event.status) ?? 0) + 1);
    console.log(`seq ${ctx.streamSequence}, ${ctx.pending} to go`);
  }
}
```

- **Fan-out.** Every instance receives every message. Use `@Consume` or `@ConsumePush` with a group to split work.
- **Sequential.** One message at a time. There is no `concurrency`, `batch`, `durable`, `ack` or `config` option.
- **No acknowledgement.** The context is a `NatsOrderedContext`: `stream`, `streamSequence`, `deliverySequence`, `pending` and `attempt` (the 1-based attempt of the handler on this message) next to the usual `subject`, `headers` and `raw`. Its `ack()`, `nak()`, `term()` and `working()` throw a `NatsConfigError`.
- **Where it starts.** `deliverPolicy` is `'all'` (default), `'last'`, `'new'`, `'last_per_subject'`, `{ startSeq }` or `{ startTime }`. `filterSubjects` narrows the stream, and `headersOnly: true` delivers no payload (`@Payload()` is `undefined`).
- **Failures.** A failing message is retried in place, with the backoff delay, so the order holds. When retries run out or the error is not retryable, it is dead-lettered if a DLQ is configured, otherwise logged and dropped; then the next message follows. See [Retry and dead-letter queue](./retry-and-dlq.md#ordered-consumers).
- **Shutdown can drop a message.** Nothing is acknowledged, so the message being handled, one waiting for its retry (abandoned with a warning), and the ones the client had read ahead are not redelivered. They come back only if the next start replays them, which depends on `deliverPolicy` (`'all'`, or a `startSeq` or `startTime` that covers them). With `'new'` or `'last'` they are skipped. See [Lifecycle](./lifecycle.md).
- **Deleted consumer.** If the consumer is deleted on the server, the client recreates it after about 75 seconds with the default heartbeats. A consumer started with `'new'`, `'last'` or `startTime` that had received no message yet then restarts from sequence 1.
