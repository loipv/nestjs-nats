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

**Request/reply.** If the incoming message has a reply subject and the handler returns something other than `undefined`, the return value is encoded and sent back. If the handler throws, the requester gets an error reply and the message is not retried. See [Errors](./errors.md).

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
| `connection`, `codec` | | As for `@Subscribe` |
| `retry`, `dlq`, `idempotency`, `batch` | | As for `@Subscribe`. `batch` cannot be combined with `ack: 'manual'` |

A consumer the library creates defaults to `ack_policy: 'explicit'` and `max_deliver: -1`, unless you set them. These defaults are never applied to a consumer that already exists.

With `ack: 'auto'` the outcome of each message is decided as follows:

| Outcome | Action |
|---|---|
| Handler succeeds | `ack()` |
| Retryable error, retries left | `nak(delay)`: redelivered after the backoff delay |
| `NatsDecodeError`, `NonRetryableError`, an `HttpException` with a 4xx status (for example from `ValidationPipe` or a guard), or retries exhausted | Sent to the DLQ and `term()`, if a DLQ is configured; otherwise `term()`: the message is not redelivered |

The full rules, including `@Subscribe`, are in [Retry and dead-letter queue](./retry-and-dlq.md).

With `ack: 'manual'` you settle the message yourself with `ctx.ack()`, `ctx.nak(delayMs?)` or `ctx.term(reason?)`. If the handler throws before settling, the library handles the failure like in `'auto'` mode (retry, then DLQ). If the handler returns without settling, the message stays unacknowledged until the consumer's `ack_wait` runs out. Only the first settle call takes effect. Long-running work needs no `ctx.working()` call: the library [extends the ack deadline for you](./retry-and-dlq.md).

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

Each `@NatsService` class becomes one service registered with `@nats-io/services`, which gives it discovery, `PING`, `INFO` and `STATS`. An endpoint's return value is always sent as the reply; returning `undefined` sends an empty payload. A thrown error is sent with `respondError(code, description)`. An endpoint method in a class without `@NatsService`, or an endpoint name the services framework rejects, fails startup with a `NatsConfigError`.

## Parameters, pipes and Nest enhancers

| Decorator | Value |
|---|---|
| `@Payload(property?, ...pipes)` | Decoded message body, or one property of it |
| `@Headers(name?)` | All headers as `Record<string, string \| string[]>`, or one header |
| `@Subject()` | The subject the message arrived on |
| `@Msg()` | The raw `Msg` (core and services) or `JsMsg` (JetStream) |
| `@Ctx()` | `NatsContext`, or `NatsJetStreamContext` in `@Consume` handlers |

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
- An exception filter that handles an error without rethrowing turns the outcome into a success.
