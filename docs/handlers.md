# Handlers

[Back to the README](../README.md)

## `@Subscribe(subject, options?)`: core NATS

```ts
@Subscribe('orders.*', { queue: 'order-workers', concurrency: 4 })
onOrderEvent(@Payload() event: OrderEvent, @Subject() subject: string) {
  // `subject` is the concrete subject, e.g. 'orders.created'
}
```

| Option | Default | Description |
|---|---|---|
| `connection` | `'default'` | Connection name |
| `queue` | none | Queue group, which load-balances messages across instances |
| `concurrency` | `1` | Maximum number of messages processed at once. `1` keeps the arrival order |
| `codec` | module codec | Codec for this handler |
| `retry` | module's, else on | Retry policy, or `false`. See [Retry and dead-letter queue](./retry-and-dlq.md) |
| `dlq` | module's, else off | Dead-letter queue: `true`, or `{ subject?, mode? }` |
| `idempotency` | off | `true` or `{ ttl?, keyExtractor? }`. See [Idempotency](./idempotency.md) |
| `batch` | off | `{ maxSize?, maxWait? }`. See [Batch processing](./batching.md#batch-processing) |
| `disabled` | `false` | `true`, or a function returning a boolean, to skip the handler. See [Disabling a handler](#disabling-a-handler) |

**Request/reply.** If the incoming message has a reply subject and the handler returns something other than `undefined`, the return value is encoded and sent back. If the handler throws, the requester gets an error reply and the message is not retried. See [Errors](./errors.md).

**Keep request subjects out of streams.** If a JetStream stream's subjects capture a request subject (a stream on `orders.>` captures `orders.get`), the stream stores the request and answers it with a PubAck. The stream and your handler both reply, and the first reply to arrive wins: usually the PubAck (`{ stream, seq }`), not the handler's reply. Use request subjects that no stream captures, as the [orders example](../examples/orders) does with `query.orders.get` next to its `orders.>` stream. See [Streams and consumers](./streams.md).

## `@Consume(options)`: JetStream pull consumer

```ts
@Consume({
  stream: 'ORDERS',
  durable: 'billing',
  config: { filter_subject: 'orders.created' }, // optional: create or update the consumer at startup
})
async bill(@Payload() order: OrderDto, @Ctx() ctx: NatsJetStreamContext) {
  console.log(ctx.stream, ctx.seq, ctx.deliveryCount);
}
```

| Option | Default | Description |
|---|---|---|
| `stream`, `durable` | required | Stream and durable consumer name |
| `config` | none | Consumer config. If set, the consumer is created or updated at startup. If not, it must already exist |
| `ack` | `'auto'` | `'auto'` or `'manual'` |
| `concurrency` | `1` | Maximum number of messages processed at once. The library asks the server for `concurrency` messages at a time (`max(batch.maxSize, concurrency)` with `batch`), and pulls more as it hands them to free slots |
| `connection`, `codec`, `disabled` | | As for `@Subscribe` |
| `retry`, `dlq`, `idempotency`, `batch` | | As for `@Subscribe`. `batch` cannot be combined with `ack: 'manual'` |

A consumer the library creates defaults to `ack_policy: 'explicit'` and `max_deliver: -1`, unless you set them. These defaults are never applied to a consumer that already exists.

With `ack: 'auto'` the outcome of each message is decided as follows:

| Outcome | Action |
|---|---|
| Handler succeeds | `ack()` |
| Retryable error, retries left | `nak(delay)`: redelivered after the backoff delay |
| `NatsDecodeError`, `NonRetryableError`, an `HttpException` with a 4xx status (for example from `ValidationPipe` or a guard), or retries exhausted | Sent to the DLQ and `term()`, if a DLQ is configured; otherwise `term()`: the message is not redelivered |

The full rules, including `@Subscribe`, are in [Retry and dead-letter queue](./retry-and-dlq.md).

With `ack: 'manual'` you settle the message yourself with `ctx.ack()`, `ctx.nak(delayMs?)` or `ctx.term(reason?)`. If the handler throws before settling, the library handles the failure like in `'auto'` mode (retry, then DLQ). If it throws after settling, the message stays as you settled it, and the error is kept on the [message outcome](./plugins.md#observing-outcomes) (so a [tracing](./tracing.md) span is marked as failed). If the handler returns without settling, the message stays unacknowledged until the consumer's `ack_wait` runs out. Only the first settle call takes effect. Long-running work needs no `ctx.working()` call: the library [extends the ack deadline for you](./retry-and-dlq.md).

## `@ConsumePush(options)`: JetStream push consumer

```ts
@ConsumePush({
  stream: 'ORDERS',
  durable: 'billing-push',
  deliverGroup: 'billing',
  config: { filter_subject: 'orders.created' },
})
async bill(@Payload() order: OrderDto, @Ctx() ctx: NatsJetStreamContext) {
  console.log(ctx.stream, ctx.seq, ctx.deliveryCount);
}
```

Takes every option of `@Consume`, and settles messages, retries and dead-letters them the same way. One more option:

| Option | Default | Description |
|---|---|---|
| `deliverGroup` | none | The consumer's `deliver_group`: instances that use the same group share the messages of the durable. Without it only one instance can bind the consumer. If `config.deliver_group` is also set, both must be equal |

The server pushes messages as they arrive, so the handler reads them without waiting for a free slot, and `max_ack_pending` bounds how many are in the process. The defaults of a consumer the library creates, and the rest of the differences, are in [Pull, push and ordered consumers](./consumers.md#push-consumepush).

## `@ConsumeOrdered(options)`: JetStream ordered consumer

```ts
@ConsumeOrdered({ stream: 'ORDERS', filterSubjects: 'orders.status.>' })
project(@Payload() event: OrderEvent, @Ctx() ctx: NatsOrderedContext) {
  console.log(ctx.streamSequence, ctx.pending);
}
```

An ephemeral consumer without acknowledgements: every instance receives every message, one at a time and in stream order.

| Option | Default | Description |
|---|---|---|
| `stream` | required | Stream to read. It must exist |
| `filterSubjects` | whole stream | A subject or an array of subjects |
| `deliverPolicy` | `'all'` | `'all'`, `'last'`, `'new'`, `'last_per_subject'`, `{ startSeq }` or `{ startTime }` (a `Date` or an ISO 8601 string) |
| `headersOnly` | `false` | Deliver headers without the payload (`@Payload()` is `undefined`) |
| `connection`, `codec`, `disabled` | | As for `@Subscribe` |
| `retry`, `dlq`, `idempotency` | | As for `@Subscribe`. A failed message is retried in place, then dead-lettered or dropped. See [Retry and dead-letter queue](./retry-and-dlq.md#ordered-consumers) |

There is no `durable`, `config`, `ack`, `concurrency` or `batch`, and an option that does not belong to `@ConsumeOrdered` fails startup with a `NatsConfigError`. `ctx.ack()`, `ctx.nak()`, `ctx.term()` and `ctx.working()` throw. A message the handler has not finished when the application shuts down is not redelivered unless the next start replays it: see [Pull, push and ordered consumers](./consumers.md#ordered-consumeordered).

**`@CloudEventRouter` and the new consumers.** A [`@CloudEventRouter`](./cloudevents.md#receiving-cloudevents) can receive from `subscribe`, `consume`, `consumePush` or `consumeOrdered`.

## `@NatsService()` and `@ServiceEndpoint()`: NATS services

```ts
import { ConflictException, Injectable } from '@nestjs/common';
import { NatsService, Payload, ServiceEndpoint } from '@loipv/nestjs-nats';

@Injectable()
@NatsService({ name: 'inventory', version: '1.0.0', description: 'Stock levels' })
export class InventoryService {
  @ServiceEndpoint('reserve', { subject: 'inventory.reserve' })
  reserve(@Payload() dto: { sku: string; qty: number }) {
    if (dto.qty > 10) throw new ConflictException('out of stock');
    return { reserved: dto.qty };
  }
}
```

Each `@NatsService` class becomes one service registered with `@nats-io/services`, which gives it discovery, `PING`, `INFO` and `STATS`. An endpoint's return value is always sent as the reply; returning `undefined` sends an empty payload. A thrown error is sent with `respondError(code, description)`. An endpoint method in a class without `@NatsService`, or an endpoint name the services framework rejects, fails startup with a `NatsConfigError`. `disabled` on `@ServiceEndpoint` skips one endpoint; on `@NatsService` it skips every endpoint of the class, so the service is not registered.

## Disabling a handler

Every handler decorator takes a `disabled` option (default `false`): `@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered`, `@ServiceEndpoint`, `@NatsService` and [`@CloudEventRouter`](./cloudevents.md#receiving-cloudevents). A disabled handler is left out at startup as if its decorator were absent: nothing subscribes, consumes or registers it, and its options are not validated, so a disabled `@Consume` may keep the durable of the handler replacing it. Startup logs `NATS handler Class.method (kind) is disabled` for each one.

Decorator arguments are evaluated when the class is imported, which is often before `ConfigModule` has loaded `.env`. Pass a function to read the configuration at startup instead. It is called during application startup and must return the same value each time:

```ts
import { Injectable } from '@nestjs/common';
import { Consume, Payload, Subscribe } from '@loipv/nestjs-nats';

@Injectable()
export class BillingHandlers {
  @Consume({
    stream: 'ORDERS',
    durable: 'billing',
    disabled: () => process.env.BILLING_CONSUMER_DISABLED === 'true',
  })
  bill(@Payload() order: { id: string }) {}

  @Subscribe('billing.debug', { disabled: true })
  debug() {}
}
```

A function that throws, or a value that is not a boolean, fails startup with a `NatsConfigError` naming the handler. Disabling a `@Consume` or `@ConsumePush` handler does not delete its durable consumer: messages stay on it until a handler consumes them again. In tests, [`NatsTestHarness.dispatch()`](./testing.md#natstestingmodule-and-natstestharness) rejects a target naming a disabled handler.

## Parameters, pipes and Nest enhancers

| Decorator | Value |
|---|---|
| `@Payload(property?, ...pipes)` | Decoded message body, or one property of it |
| `@Headers(name?)` | All headers as `Record<string, string \| string[]>`, or one header |
| `@Subject()` | The subject the message arrived on |
| `@Msg()` | The raw `Msg` (core and services) or `JsMsg` (JetStream, ordered consumers included) |
| `@Ctx()` | `NatsContext`, `NatsJetStreamContext` in `@Consume` and `@ConsumePush` handlers, or `NatsOrderedContext` in `@ConsumeOrdered` handlers |

Handlers run through Nest's `ExternalContextCreator`, so everything you use on HTTP controllers works here too:

```ts
import { createParamDecorator, ExecutionContext, Injectable, UseGuards, UsePipes, ValidationPipe } from '@nestjs/common';
import { NatsContext, Payload, Subscribe } from '@loipv/nestjs-nats';

const Tenant = createParamDecorator((_: unknown, ctx: ExecutionContext) =>
  ctx.switchToRpc().getContext<NatsContext>().headers['x-tenant'],
);

@Injectable()
class OrderHandlers {
  constructor(private readonly orders: OrderService) {}

  @Subscribe('orders.create')
  @UseGuards(TenantGuard)
  @UsePipes(new ValidationPipe({ whitelist: true }))
  create(@Payload() dto: CreateOrderDto, @Tenant() tenant: string) {
    return this.orders.create(tenant, dto);
  }
}
```

- `context.getType()` returns `'nats'`, so enhancers can branch on it.
- `switchToRpc().getData()` returns the payload, and `switchToRpc().getContext()` returns the `NatsContext`.
- A global `ValidationPipe` (`app.useGlobalPipes()` or `APP_PIPE`) validates `@Payload()` only, never the context or headers.
- An exception filter handles an error by returning a truthy value, as in Nest: the outcome becomes a success, and the value is the reply to a request. An error that no filter handles is rethrown as it is, and the library logs it; Nest's `ExceptionsHandler` does not log it again.
- The payload is decoded before the handler is invoked, so a `NatsDecodeError` (and, in a [CloudEvent router](./cloudevents.md#receiving-cloudevents), an error in the event's attributes) happens before guards, pipes, interceptors and exception filters run, and none of them sees it. See [Errors](./errors.md).
- Return a value or `undefined`. An Observable that completes without emitting counts as a failure (Nest throws an `EmptyError`): a JetStream message is retried like after any error, and a request gets a `500` reply.
