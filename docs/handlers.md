# Handlers

[Back to the README](../README.md)

Each handler decorator declares one handler. Applying the same decorator twice to one method (two `@Subscribe`, say), or twice to one class (`@NatsService`, `@CloudEventRouter`), throws a `NatsConfigError` when the class is defined. Decorators of different kinds can share a method: `@Subscribe` and `@Consume` on one method make two handlers. To receive several subjects, use a [wildcard](#subscribesubject-options-core-nats) or one method per subject.

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
| `slowConsumer` | `10000` | Messages waiting for the handler past which the connection logs a warning, or `false`. See [Slow consumers](#slow-consumers) |
| `codec` | module codec | Codec for this handler |
| `retry` | module's, else on | Retry policy, or `false`. See [Retry and dead-letter queue](./retry-and-dlq.md) |
| `dlq` | module's, else off | Dead-letter queue: `true`, or `{ subject?, mode? }` |
| `idempotency` | off | `true` or `{ ttl?, keyExtractor? }`. See [Idempotency](./idempotency.md) |
| `batch` | off | `{ maxSize?, maxWait? }`. See [Batch processing](./batching.md#batch-processing) |
| `disabled` | `false` | `true`, or a function returning a boolean, to skip the handler. See [Disabling a handler](#disabling-a-handler) |

**Request/reply.** If the incoming message has a reply subject and the handler returns something other than `undefined`, the return value is encoded and sent back. If the handler throws, the requester gets an error reply and the message is not retried. See [Errors](./errors.md).

**Keep request subjects out of streams.** If a JetStream stream's subjects capture a request subject (a stream on `orders.>` captures `orders.get`), the stream stores the request and answers it with a PubAck. The stream and your handler both reply, and the first reply to arrive wins: usually the PubAck (`{ stream, seq }`), not the handler's reply. Use request subjects that no stream captures, as the [orders example](../examples/orders) does with `query.orders.get` next to its `orders.>` stream. See [Streams and consumers](./streams.md).

### Slow consumers

Core NATS pushes messages as fast as they are published. While every `concurrency` slot is busy, the ones that arrive wait in the subscription, in memory, and core NATS has no way to hold them back. When more than `slowConsumer` messages are waiting, the connection's [logger](./connections.md#logging) warns:

```
NATS connection "default" subscription "orders.*" is a slow consumer: 10001 messages pending
```

It warns once each time the backlog crosses the threshold, not for every message. The warning is all it does: no message is dropped. For a subject that can receive more than the handler keeps up with, such as one that untrusted clients publish to, use [`@Consume`](#consumeoptions-jetstream-pull-consumer): a JetStream consumer pulls only what its handler can take.

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

With `ack: 'manual'` you settle the message yourself with `ctx.ack()`, `ctx.nak(delayMs?)` or `ctx.term(reason?)`. If the handler throws before settling, the library handles the failure like in `'auto'` mode (retry, then DLQ). If it throws after settling, the message stays as you settled it, and the error is kept on the [message outcome](./plugins.md#observing-outcomes) (so a [tracing](./tracing.md) span is marked as failed). If the handler returns without settling, the message stays unacknowledged until the consumer's `ack_wait` runs out. Only the first settle call takes effect, and `ctx.working()` does nothing once the message is settled. Long-running work needs no `ctx.working()` call: the library [extends the ack deadline for you](./retry-and-dlq.md).

## `@ConsumePush(options)`: JetStream push consumer

> **Experimental:** `@ConsumePush` may change or be removed before 1.0. See [Stability](../README.md#stability).

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

## Values from configuration

Decorator arguments are evaluated when the class is imported, which is often before `ConfigModule` has loaded `.env`. Any argument of `@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered`, `@NatsService`, `@ServiceEndpoint` and [`@CloudEventRouter`](./cloudevents.md#receiving-cloudevents) can instead be a function that returns it: the subject, the endpoint name, or the whole options object. The function receives `{ get }`, which returns any provider of the application (looked up in every module, like `moduleRef.get(token, { strict: false })`):

```ts
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Consume, Payload, Subscribe } from '@loipv/nestjs-nats';

@Injectable()
export class BillingHandlers {
  @Consume(({ get }) => ({
    stream: get(ConfigService).getOrThrow<string>('ORDERS_STREAM'),
    durable: 'billing',
    // Values read from `.env` are strings.
    concurrency: Number(get(ConfigService).get<string>('BILLING_CONCURRENCY') ?? 1),
  }))
  bill(@Payload() order: { id: string }) {}

  @Subscribe(({ get }) => `${get(ConfigService).getOrThrow<string>('TENANT')}.orders.created`)
  onCreated(@Payload() order: { id: string }) {}
}
```

- **Once, at startup.** Each function runs once per application, when the handlers are first discovered, and what it returns (or throws) is kept. A function shared by several decorators also runs once and gives them all the same value. Discovery happens at application bootstrap, or earlier, during the library's module init, when a connection sets [`jetstream.removedFields: 'reset'`](./streams.md#removed-fields). So read providers that are ready once constructed, such as `ConfigService`, rather than ones that load their values in their own `onModuleInit`.
- **Synchronous.** A function returning a Promise fails startup. Asynchronous configuration belongs in [`NatsModule.forRootAsync()`](./connections.md).
- **Errors.** A function that throws (including `get` of a provider that does not exist, or of a request-scoped one) fails startup with a `NatsConfigError` naming the handler, such as `NATS handler BillingHandlers.bill failed to resolve its @Consume options: ...`, with the original error as `cause`. So does a subject or endpoint name that is not a non-empty string, or options that are not an object; the options of `@Subscribe` and `@ServiceEndpoint` may also be `undefined`. The resolved values are then validated like written ones.
- **Typos.** TypeScript does not reject unknown properties in an object a function returns, so `concurency: 2` compiles there although it would not in a written object. Add `satisfies` to have it checked: `@Consume(() => ({ stream: 'ORDERS', durable: 'billing' }) satisfies ConsumeOptions)`.
- **Not for streams.** Streams and consumers declared in `forRoot` or `forFeature` take no functions; declare those whose names come from configuration in `NatsModule.forRootAsync()`. `@OnCloudEvent` type patterns are fixed too.

### Disabling a handler

Every handler decorator takes a `disabled` option (default `false`): `@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered`, `@ServiceEndpoint`, `@NatsService` and [`@CloudEventRouter`](./cloudevents.md#receiving-cloudevents). A disabled handler is left out at startup as if its decorator were absent: nothing subscribes, consumes or registers it, and its options are not validated, so a disabled `@Consume` may keep the durable of the handler replacing it. Startup logs `NATS handler Class.method (kind) is disabled` for each one.

`disabled` can be a function too. It receives the same `{ get }` and runs once per application, like the functions above. It can also be part of what an options function returns; that function still runs for a disabled handler, which is how its `disabled` is read. The subject or endpoint name of a disabled handler is not resolved, nor is any argument of an endpoint of a disabled `@NatsService`, so their functions may read configuration that is absent where the handler is off:

```ts
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Consume, Payload, Subscribe } from '@loipv/nestjs-nats';

@Injectable()
export class AuditHandlers {
  @Consume({
    stream: 'ORDERS',
    durable: 'audit',
    disabled: ({ get }) => get(ConfigService).get<string>('AUDIT_DISABLED') === 'true',
  })
  audit(@Payload() order: { id: string }) {}

  @Subscribe('audit.debug', { disabled: true })
  debug() {}
}
```

A function that throws, or a value that is not a boolean, fails startup with a `NatsConfigError` naming the handler. Disabling a `@Consume` or `@ConsumePush` handler does not delete its durable consumer: messages stay on it until a handler consumes them again. In tests, [`NatsTestHarness.dispatch()`](./testing.md#natstestingmodule-and-natstestharness) rejects a target naming a disabled handler.

## Handler timeout

A handler that never returns holds its slot forever: with `concurrency: 1` its consumer stops, and a `@Consume` message is heartbeated, so the server never redelivers it. Give handlers a deadline with `timeout` (in ms) on a handler, or with `handlerTimeout`, the default of every handler of a connection:

```ts
import { Injectable } from '@nestjs/common';
import { Consume, Ctx, NatsJetStreamContext, NatsModule, Payload } from '@loipv/nestjs-nats';

NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  handlerTimeout: 30_000,
});

@Injectable()
export class BillingHandlers {
  @Consume({ stream: 'ORDERS', durable: 'billing', timeout: 5_000 })
  async bill(@Payload() order: { id: string }, @Ctx() ctx: NatsJetStreamContext) {
    // ctx.signal aborts the request when the handler times out.
    await fetch(`https://billing.example.com/orders/${order.id}`, {
      method: 'POST',
      signal: ctx.signal,
    });
  }
}
```

There is no timeout by default. A handler's `timeout` wins over `handlerTimeout`, and `timeout: false` turns it off for that handler. A value must be a number of ms above `0` and at most `2147483647`; anything else fails startup with a `NatsConfigError`. Every handler decorator takes `timeout`, `@CloudEventRouter` included. The timeout covers the handler with its guards, pipes, interceptors and exception filters, not decoding the payload, plugins or the idempotency store.

When the timeout passes:

- The run fails with a `NatsHandlerTimeoutError`, handled like any handler error: it is retryable by default (`retry.isRetryable` can change that), then dead-lettered or dropped. See [Retry and dead-letter queue](./retry-and-dlq.md).
- The handler's slot is freed at once, so the next message runs.
- `ctx.signal` is aborted with that error as its reason. Every context has a `signal`, a batch's too; it is never aborted otherwise. Pass it to what the handler awaits so the run stops.
- What the handler returns later is ignored and logged as a warning. An error it throws later is logged as an error, unless it is the abort itself (the `NatsHandlerTimeoutError`, or an `AbortError`), which is a warning.

| Handler | After the timeout |
|---|---|
| `@Consume`, `@ConsumePush` | `nak(delay)`, or dead-lettered, as for any error. The message is no longer heartbeated. With `ack: 'manual'`, a `ctx.ack()` the handler calls later does nothing: the message is already settled |
| `@Subscribe` | Retried in the process after the delay |
| Request to `@Subscribe`, `@ServiceEndpoint` | The requester gets an error reply `504`, `Handler timed out` |
| `batch` | The whole batch fails, each message with its own attempt. A `{ failed }` the handler returns later is ignored |
| `@ConsumeOrdered` | Retried in place after the delay |
| `@CloudEventRouter` | The timeout applies to the `@OnCloudEvent` method the event is routed to |

Limits:

- **A handler cannot be stopped from outside.** One that ignores `ctx.signal` keeps running until it ends on its own. Meanwhile its message can be redelivered and run again alongside it, unless the handler enables [idempotency](./idempotency.md): the key of a timed-out run stays in progress until that run ends.
- **With idempotency, the retry waits for the timed-out run.** A `@Subscribe` retry is scheduled once that run ends, and only if it failed. An ordered consumer waits for that run before it retries the message in place, and moves on to the next message if the run succeeded. A JetStream message is redelivered meanwhile and `nak`'d as in progress each time. Each of these redeliveries counts towards its attempts and the consumer's `max_deliver`, so after a long run that then fails, the message can go to the DLQ (or, under a small `max_deliver`, stop being redelivered) after one more attempt.
- **More runs than `concurrency`.** A timed-out run that is still going on holds no slot, so more runs than `concurrency` can be in progress at once.
- **Ordered consumers lose their order.** Without idempotency, the in-place retry of a timed-out `@ConsumeOrdered` message, and the messages after it, run while the timed-out run is still going on: the consumer no longer handles one message at a time in stream order. With idempotency it waits for that run first, as described above.
- **Listeners.** A `ctx.signal` listener that throws raises an uncaught exception, as for any `EventTarget` in Node.
- **Shutdown** waits for timed-out runs still going on, within `shutdown.drainTimeout`. See [Lifecycle](./lifecycle.md).

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
