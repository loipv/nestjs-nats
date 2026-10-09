# @loipv/nestjs-nats

Decorator-driven [NATS](https://nats.io) integration for [NestJS](https://nestjs.com): core subscriptions, JetStream consumers and NATS services as plain Nest providers, with pipes, guards, interceptors and exception filters working the way they do for HTTP controllers.

> [!WARNING]
> **Status: 0.x, under active development, not production-ready yet.**
> The API may change between minor versions until 1.0. See [Known limitations](#known-limitations).

## Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Connections](#connections)
- [Handlers](#handlers)
- [Parameters, pipes and Nest enhancers](#parameters-pipes-and-nest-enhancers)
- [Publishing](#publishing)
- [Streams and consumers](#streams-and-consumers)
- [Retry and dead-letter queue](#retry-and-dead-letter-queue)
- [Idempotency](#idempotency)
- [Batch processing](#batch-processing)
- [Batch publishing](#batch-publishing)
- [CloudEvents](#cloudevents)
- [Codecs](#codecs)
- [Plugins](#plugins)
- [Tracing (OpenTelemetry)](#tracing-opentelemetry)
- [Health checks](#health-checks)
- [Testing](#testing)
- [Errors](#errors)
- [Lifecycle and graceful shutdown](#lifecycle-and-graceful-shutdown)
- [Known limitations](#known-limitations)
- [License](#license)

## Features

- `@Subscribe()` for core NATS subjects, with wildcards, queue groups and request/reply.
- `@Consume()` for JetStream pull consumers, with automatic or manual acknowledgement.
- `@NatsService()` and `@ServiceEndpoint()` on top of the [NATS services framework](https://github.com/nats-io/nats.js/tree/main/services), so services are discoverable with `nats micro`.
- Parameter decorators (`@Payload`, `@Headers`, `@Subject`, `@Msg`, `@Ctx`) that work with pipes, including a global `ValidationPipe`, and with guards, interceptors and exception filters.
- `NatsClient` for fire-and-forget publish and request/reply, and `JetStreamClient` for publishing with acknowledgements and de-duplication.
- Declarative streams and consumers, synced at startup with four modes from create-or-update to read-only verification.
- Retry with exponential backoff and an optional dead-letter queue (DLQ), idempotency with a pluggable store, and batch processing for `@Subscribe` and `@Consume`.
- Batched JetStream publishing with `publishMany()` and an auto-flushing `batcher()`.
- Several named connections in one application, each passing its options straight to the NATS client, so every NATS auth method and TLS/mTLS works unchanged.
- [CloudEvents](#cloudevents) 1.0 in binary and structured mode: publish with a `cloudEvent` option, and route incoming events to methods by `type` with `@CloudEventRouter()` and `@OnCloudEvent()`.
- An inbound and outbound plugin pipeline for cross-cutting concerns, with a per-message outcome for plugins to observe.
- [OpenTelemetry tracing](#tracing-opentelemetry) from producer to consumer, as a plugin (`@loipv/nestjs-nats/otel`).
- A [Terminus health indicator](#health-checks) (`@loipv/nestjs-nats/health`).
- [Testing utilities](#testing) (`@loipv/nestjs-nats/testing`): mock clients and a harness that runs messages through your handlers without a NATS server.
- Startup fails fast on a bad connection, bad credentials or invalid handler configuration. Shutdown stops intake, waits for in-flight handlers and drains connections, all within a bounded time.

## Requirements

| | Version |
|---|---|
| Node.js | `>= 22.12` |
| NestJS | `11.x` or `12.x` |
| NATS client | `@nats-io/transport-node`, `@nats-io/jetstream`, `@nats-io/services` `3.x` |
| NATS server | JetStream enabled, needed only for `@Consume` and `JetStreamClient` |
| `@opentelemetry/api` | `^1.9.0`, optional: only for `@loipv/nestjs-nats/otel` |
| `@nestjs/terminus` | `11.x` or `12.x`, optional: only for `@loipv/nestjs-nats/health` |

The package is **ESM-only**. CommonJS applications, which includes most Nest 11 projects, can still `require()` it, because Node 22.12+ loads ES modules through `require(esm)`.

## Installation

```bash
npm install @loipv/nestjs-nats @nats-io/transport-node @nats-io/jetstream @nats-io/services
```

The library also has `@nestjs/common`, `@nestjs/core`, `reflect-metadata` and `rxjs` as peer dependencies. A Nest application already has them.

The optional entry points have optional peer dependencies. The main entry point never imports them, so install one only when you use its entry point:

```bash
npm install @opentelemetry/api   # for @loipv/nestjs-nats/otel
npm install @nestjs/terminus     # for @loipv/nestjs-nats/health
```

`@loipv/nestjs-nats/testing` needs nothing extra. Your tests will normally use `@nestjs/testing` next to it.

## Quick start

```ts
import { Injectable, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { NatsClient, NatsModule, Payload, Subscribe } from '@loipv/nestjs-nats';

@Injectable()
class GreetingHandlers {
  // Fire-and-forget subscription.
  @Subscribe('greetings.sent')
  onGreeting(@Payload() body: { name: string }) {
    console.log(`Hello, ${body.name}!`);
  }

  // Request/reply: the return value is sent back to the requester.
  @Subscribe('greetings.echo')
  echo(@Payload() body: { name: string }) {
    return { reply: `Hi ${body.name}` };
  }
}

@Module({
  imports: [NatsModule.forRoot({ connection: { servers: 'nats://localhost:4222' } })],
  providers: [GreetingHandlers],
})
class AppModule {}

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();

  const client = app.get(NatsClient);
  client.publish('greetings.sent', { name: 'Ada' });
  console.log(await client.request('greetings.echo', { name: 'Ada' })); // { reply: 'Hi Ada' }
}
bootstrap();
```

Handlers can live in any provider or controller, in any module. The library finds them at startup. For an HTTP app, use `NestFactory.create()` as usual; no `connectMicroservice()` is needed.

## Connections

### Options and authentication

`connection` is passed unchanged to `connect()` from `@nats-io/transport-node`, so every client option works: servers, reconnect policy, user/password, token, NKey, JWT/`.creds`, `multiAuthenticator`, TLS and mTLS.

```ts
import { readFileSync } from 'node:fs';
import { credsAuthenticator } from '@nats-io/transport-node';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { NatsModule } from '@loipv/nestjs-nats';

NatsModule.forRootAsync({
  imports: [ConfigModule],
  inject: [ConfigService],
  useFactory: (config: ConfigService) => ({
    connection: {
      servers: config.getOrThrow<string>('NATS_URL'),
      name: 'orders-service', // client name shown in server monitoring
      authenticator: credsAuthenticator(readFileSync(config.getOrThrow<string>('NATS_CREDS'))),
      tls: { caFile: config.get<string>('NATS_CA') },
    },
  }),
});
```

`forRootAsync` also accepts `useClass` and `useExisting`, given a class that implements `NatsOptionsFactory`.

- **Fail fast.** If the first connection attempt fails, for example because the server is unreachable or the credentials are wrong, bootstrap throws a `NatsConnectionError` instead of retrying forever. Once connected, reconnects follow the NATS client's normal policy.
- **No secrets in output.** Error messages and logs never include passwords, tokens, seeds or creds, and credentials embedded in server URLs are replaced with `***`.
- **Rotating credentials.** An authenticator is called again on every reconnect. To pick up a rotated `.creds` file, pass a function that re-reads the file, instead of `credsAuthenticator(bytes)`, which reads it once.

### Several connections

Give each connection a `name`. Handlers and injected clients choose a connection by that name; when no name is given, `'default'` is used.

```ts
import { Injectable, Module } from '@nestjs/common';
import { InjectNatsClient, NatsClient, NatsModule, Payload, Subscribe } from '@loipv/nestjs-nats';

@Injectable()
class AuditForwarder {
  constructor(@InjectNatsClient('audit') private readonly audit: NatsClient) {}

  @Subscribe('orders.*', { connection: 'default' })
  forward(@Payload() order: unknown) {
    this.audit.publish('audit.orders', order);
  }
}

@Module({
  imports: [
    NatsModule.forRoot({ connection: { servers: 'nats://orders:4222' } }),
    NatsModule.forRoot({ name: 'audit', connection: { servers: 'nats://audit:4222' } }),
  ],
  providers: [AuditForwarder],
})
class AppModule {}
```

Inject per connection with `@InjectNatsClient(name?)`, `@InjectJetStream(name?)`, `@InjectStreamManager(name?)` and `@InjectNatsConnection(name?)`. The last one gives you the raw `NatsConnection`. For the `'default'` connection, `NatsClient`, `JetStreamClient` and `NatsStreamManager` can also be injected by class (`constructor(private readonly client: NatsClient)`, `app.get(NatsClient)`). For a named connection outside a constructor, use the token functions: `getNatsClientToken(name?)`, `getJetStreamClientToken(name?)`, `getStreamManagerToken(name?)` and `getConnectionToken(name?)`.

Registering two connections with the same name throws a `NatsConfigError`.

## Handlers

### `@Subscribe(subject, options?)`: core NATS

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
| `retry` | module's, else on | Retry policy, or `false`. See [Retry and dead-letter queue](#retry-and-dead-letter-queue) |
| `dlq` | module's, else off | Dead-letter queue: `true`, or `{ subject?, mode? }` |
| `idempotency` | off | `true` or `{ ttl?, keyExtractor? }`. See [Idempotency](#idempotency) |
| `batch` | off | `{ maxSize?, maxWait? }`. See [Batch processing](#batch-processing) |

**Request/reply.** If the incoming message has a reply subject and the handler returns something other than `undefined`, the return value is encoded and sent back. If the handler throws, the requester gets an error reply and the message is not retried. See [Errors](#errors).

### `@Consume(options)`: JetStream pull consumer

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

The full rules, including `@Subscribe`, are in [Retry and dead-letter queue](#retry-and-dead-letter-queue).

With `ack: 'manual'` you settle the message yourself with `ctx.ack()`, `ctx.nak(delayMs?)` or `ctx.term(reason?)`. If the handler throws before settling, the library handles the failure like in `'auto'` mode (retry, then DLQ). If the handler returns without settling, the message stays unacknowledged until the consumer's `ack_wait` runs out. Only the first settle call takes effect. Long-running work needs no `ctx.working()` call: the library [extends the ack deadline for you](#retry-and-dead-letter-queue).

### `@NatsService()` and `@ServiceEndpoint()`: NATS services

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

## Publishing

### `NatsClient`: core NATS

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

### `JetStreamClient`

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

`publish()` also accepts `expect` (optimistic concurrency, for example `{ lastSubjectSequence: 41 }`), `headers`, `codec` and `timeout`. `msgId` is sent as the `Nats-Msg-Id` header, so plugins can see it. To publish a [CloudEvent](#cloudevents), pass `cloudEvent` instead of building its headers. To publish many messages at once, see [Batch publishing](#batch-publishing).

## Streams and consumers

Declare streams and consumers in `forRoot` or, from a feature module, in `forFeature`. They are synced while the application initialises, before any handler starts. All streams are synced first, then all consumers.

```ts
NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  jetstream: { sync: 'create-or-update' },
  streams: [{ name: 'ORDERS', subjects: ['orders.>'], max_age: 7 * 24 * 3600 * 1e9 }],
  consumers: [{ stream: 'ORDERS', config: { durable_name: 'analytics', filter_subject: 'orders.*' } }],
});

// In a feature module:
NatsModule.forFeature({ connection: 'default', streams: [{ name: 'PAYMENTS', subjects: ['payments.>'] }] });
```

`jetstream.sync` sets how declarations are applied:

| Mode | Missing | Exists, mutable field differs | Exists, immutable field differs |
|---|---|---|---|
| `create-or-update` (default) | create | update | `StreamConfigConflictError` |
| `create-only` | create | leave as is | leave as is |
| `verify` | `NatsConfigError` | `NatsConfigError` | `NatsConfigError` |
| `none` | skip, the server is not contacted | | |

- **Immutable fields.** For streams these are `storage` and `retention`. For consumers they are `ack_policy`, `deliver_policy`, `opt_start_seq`, `opt_start_time` and `replay_policy`.
- **Never deletes.** The library never deletes or recreates a stream or consumer.
- **Only declared fields are compared.** Values the server fills in are ignored: its own `_nats.*` metadata keys, and fields that are absent but declared as `false`, `0`, `''`, `[]` or `{}`.
- **Duration units.** Durations such as `max_age` are in nanoseconds, as in the NATS API.

`NatsStreamManager` (inject it by class, or with `@InjectStreamManager(name?)` for a named connection) is available for runtime work: `ensureStream()`, `ensureConsumer()`, `info()`, `purge()`, `delete()`, and `jsm()` for the raw `JetStreamManager`.

## Retry and dead-letter queue

A failed message is retried with exponential backoff. When retries run out, or the error is not worth retrying, the message can be published to a dead-letter queue (DLQ). Retry is **on** by default. The DLQ is **off**.

Set `retry` and `dlq` on `NatsModule.forRoot()` for every handler of a connection, or on a single `@Subscribe` / `@Consume`. Options of a handler are merged over those of the module (a retry option set to `undefined` keeps the inherited value), a handler's `retry: false` turns retries off for it, and a handler's `retry` object turns them back on when the module disabled them. Invalid values fail startup with a `NatsConfigError`. The examples in this section and the next ones use placeholder types and services (`OrderDto`, `PaymentService`, ...) that you replace with your own.

```ts
import { Consume, NatsModule, NonRetryableError, Payload, defaultIsRetryable } from '@loipv/nestjs-nats';

NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  retry: { maxRetries: 3, initialDelay: 500 },
  dlq: true,
});

class BillingHandlers {
  constructor(private readonly payments: PaymentService) {}

  @Consume({
    stream: 'ORDERS',
    durable: 'billing',
    retry: {
      maxRetries: 8,
      // Replaces the default check. Compose with `defaultIsRetryable` to keep its rules.
      isRetryable: (err) => defaultIsRetryable(err) && !(err instanceof PaymentDeclinedError),
    },
    dlq: { subject: 'dlq.billing' },
  })
  async bill(@Payload() order: OrderDto) {
    if (order.total <= 0) throw new NonRetryableError('empty order'); // straight to the DLQ
    await this.payments.charge(order);
  }
}
```

### Retry options

| Option | Default | Description |
|---|---|---|
| `maxRetries` | `5` | Redeliveries after the first attempt, so a message is delivered at most `maxRetries + 1` times |
| `initialDelay` | `1000` | Delay in ms before the first redelivery. At most `2147483647` |
| `factor` | `2` | Multiplier of the delay for each further redelivery (`>= 1`) |
| `maxDelay` | `60000` | Upper bound of the delay in ms, before jitter. Must be `>= initialDelay` and at most `2147483647` |
| `jitter` | `0.2` | Random spread of each delay: `0.2` means plus or minus 20 % (between `0` and `1`) |
| `isRetryable` | `defaultIsRetryable` | `(err) => boolean`. Errors that return `false` are not retried |

The delay before redelivery `n` is `min(maxDelay, initialDelay * factor^(n - 1))`, with the jitter applied after the cap, so a delay can exceed `maxDelay` by up to `jitter` times 100 %. `defaultIsRetryable` returns `false` for `NatsDecodeError`, `NonRetryableError` and an `HttpException` with a 4xx status, and `true` for everything else.

### What happens to a failed message

| Handler | Retryable error, retries left | Non-retryable error, or retries exhausted |
|---|---|---|
| `@Consume`, `ack: 'auto'` | `nak(delay)`: the server redelivers after the delay | If a DLQ is configured, publish to it, then `term()`. Otherwise `term()` |
| `@Consume`, `ack: 'manual'` | If the handler threw before settling: as for `'auto'`. A message you settled yourself is left alone | As for `'auto'` |
| `@Subscribe`, no reply subject | The message is processed again in the same process after the delay | If a DLQ is configured, publish to it. Otherwise it is logged and dropped |
| `@Subscribe`, request (has a reply subject) | No retry: the requester gets an error reply | No DLQ: the requester gets an error reply |
| `@ServiceEndpoint` | No retry: the caller gets an error reply | No DLQ: the caller gets an error reply |

- **Core retries live in the process.** A retry of a `@Subscribe` message is a timer, not a redelivery by the server. See [Known limitations](#known-limitations).
- **Heartbeats.** Every `ack_wait / 2`, the library calls `working()` on each `@Consume` message it has received and not yet settled: messages being handled or dead-lettered, messages waiting for a free slot, and messages collected into a batch or waiting with their batch for a slot. A slow handler or a slow DLQ publish does not make the server redeliver them. One gap remains: messages the NATS client has already pulled but not yet handed to the library are not heartbeated. While every slot is busy there can be up to `concurrency` of them (`max(batch.maxSize, concurrency)` with `batch`). With `concurrency: 1` and no `batch` there are none. Otherwise, if a handler can run longer than the consumer's `ack_wait`, such a message can be redelivered and processed twice: set `ack_wait` above the longest run of the handler.
- **`max_deliver`.** The server stops redelivering a message after the consumer's `max_deliver`, and the message then never reaches the DLQ. At startup the library logs a warning when a consumer has `max_deliver` greater than `0` and at most `maxRetries`. Use `max_deliver` of at least `maxRetries + 1`, or `-1`, which is the default for consumers the library creates.

### Dead-letter queue

`dlq` is `true`, or an object, on the module or a handler. `dlq: false` on a handler always wins.

| Option | Default | Description |
|---|---|---|
| `subject` | see below | A non-empty subject, or a function `(handler: HandlerInfo, msg: InboundMessage) => string` |
| `mode` | `'jetstream'` for `@Consume`, `'core'` for `@Subscribe` | `'jetstream'` waits for the PubAck. `'core'` is a plain publish |

The default subject is `dlq.<stream>.<durable>` for `@Consume` and `dlq.<actual subject>` for `@Subscribe`. In `'jetstream'` mode a stream must capture the subject. This stream captures every default subject:

```ts
NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  streams: [{ name: 'DLQ', subjects: ['dlq.>'] }],
  dlq: true,
});
```

A dead letter has the original body, unchanged, and the original headers except the ones whose name starts with `Nats-` (in any case), plus these:

| Header | Value |
|---|---|
| `Nats-DLQ-Subject` | Subject the message arrived on |
| `Nats-DLQ-Stream` | Stream of the original message (JetStream messages only) |
| `Nats-DLQ-Consumer` | Consumer that failed to process it (JetStream messages only) |
| `Nats-DLQ-Seq` | Stream sequence of the original message (JetStream messages only) |
| `Nats-DLQ-Deliveries` | Number of attempts made |
| `Nats-DLQ-Error` | Message of the last error. At most 1024 characters, line breaks replaced by spaces |
| `Nats-DLQ-Timestamp` | When it was dead-lettered, ISO 8601 |
| `Nats-DLQ-Msg-Id` | The original `Nats-Msg-Id`, if the message had one |

The `Nats-` headers of the original message (`Nats-Msg-Id`, `Nats-Expected-*`, `Nats-Rollup`, `Nats-TTL`, `Nats-Incr`, `Nats-Schedule*`, `Nats-Batch-*` and so on) are not copied: they were directives for the original publish and would make the DLQ publish fail or misbehave. Instead, a dead letter of a JetStream message gets its own `Nats-Msg-Id`, `dlq:<stream>:<consumer>:<stream sequence>`. Each consumer that fails the same message therefore keeps its own dead letter in a shared DLQ stream, while the same dead letter sent twice is de-duplicated by the DLQ stream. A dead letter of a core message has no `Nats-Msg-Id`.

If publishing to the DLQ fails, a `@Consume` message is not lost: it is `nak`'d with the backoff delay and an error is logged, so it comes back and the DLQ is tried again. A `@Subscribe` message that cannot be dead-lettered is logged and dropped. DLQ publishes do not go through outbound plugins.

## Idempotency

Idempotency stops a message from being processed twice: a redelivery after a failure that already succeeded, or a producer that published the same message twice. It is **off** by default. Enable it per handler:

```ts
import { Consume, Payload } from '@loipv/nestjs-nats';

class InvoiceHandlers {
  constructor(private readonly invoices: InvoiceService) {}

  @Consume({
    stream: 'ORDERS',
    durable: 'invoices',
    idempotency: {
      ttl: 600_000,
      // Used when the message has neither a `Nats-Msg-Id` header nor CloudEvent attributes.
      keyExtractor: (msg) => msg.headers?.get('x-order-id') || undefined,
    },
  })
  async invoice(@Payload() order: OrderDto) {
    await this.invoices.create(order);
  }
}
```

`idempotency: true` uses the defaults. On `NatsModule.forRoot()`, `idempotency: { store?, ttl? }` sets the default store and TTL of the handlers that enable it. It does not enable idempotency by itself.

| Option | Default | Description |
|---|---|---|
| `ttl` | module's, else `300000` | How long, in ms, a processed message is remembered. It must be a number above `0` |
| `keyExtractor` | none | `(msg: InboundMessage) => string \| undefined`. Called only if the message has neither a `Nats-Msg-Id` header nor CloudEvent attributes |

- **Key.** The key is the first of these that exists: the `Nats-Msg-Id` header, which `JetStreamClient.publish()` sets from `msgId` (or from the `id` of a [CloudEvent](#cloudevents)); for a CloudEvent, `<source>|<id>`; what `keyExtractor` returns. A message without a key is processed as usual.
- **Scope.** Keys are scoped per handler, as `consume:<stream>/<durable>` or `subscribe:<Class>.<method>`, so handlers that share a store never swallow each other's messages. The connection name is not part of the scope, so several instances of an application agree on keys when they share a store.
- **Flow.** The key is claimed before the handler runs. If the handler succeeds, the key is marked as done for `ttl`. If it fails, the key is released, so the retry runs.
- **Duplicates.** A message whose key is done is skipped, and acknowledged if it is a JetStream message. A message whose key is being processed by another delivery is skipped, and a JetStream message is `nak`'d, with the retry `initialDelay` (or 1000 ms when retries are off) as delay, so it comes back later.
- **No reply for duplicates.** A skipped `@Subscribe` request gets no reply, and its requester times out.
- **Store errors.** If the store fails to claim a key, the handler does not run and the message is retried after the retry backoff (the default one when retries are off), capped at `maxDelay`, and a warning is logged. This does not depend on `isRetryable` or on the retries left: a store outage delays messages but never dead-letters or drops them. A JetStream message is `nak`'d, and each redelivery increments its `deliveryCount`, so after a long outage it may have few or no retries left when it finally runs. A consumer's `max_deliver` still applies. A `@Subscribe` message is retried in the process, and a request gets an error reply.
- **`ttl` must exceed the longest run of the handler.** A claim that expires while the handler still runs lets a redelivery start a second run. After the `ttl` has passed, a redelivered message is processed again.

### Stores

The default store is an `InMemoryIdempotencyStore`: one per connection, shared by the handlers of that connection. It lives in the memory of one process, so it does not de-duplicate across instances and forgets everything on restart. It keeps up to `maxKeys` keys (default `100000`) and evicts the least recently used one. To share state across instances, implement `IdempotencyStore` over a shared database and pass it as `idempotency.store`:

```ts
import {
  type ClaimResult,
  type IdempotencyStore,
  InMemoryIdempotencyStore,
  NatsModule,
} from '@loipv/nestjs-nats';
import { Redis } from 'ioredis';

class RedisIdempotencyStore implements IdempotencyStore {
  constructor(private readonly redis: Redis) {}

  async claim(key: string, ttlMs: number): Promise<ClaimResult> {
    if ((await this.redis.set(key, 'in-progress', 'PX', ttlMs, 'NX')) === 'OK') return 'claimed';
    return (await this.redis.get(key)) === 'done' ? 'duplicate' : 'in-progress';
  }

  async complete(key: string, ttlMs: number): Promise<void> {
    await this.redis.set(key, 'done', 'PX', ttlMs);
  }

  async release(key: string): Promise<void> {
    await this.redis.del(key);
  }
}

NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  idempotency: { store: new RedisIdempotencyStore(new Redis()), ttl: 600_000 },
});

// The in-memory store can also be created directly, for example to bound its size.
const store = new InMemoryIdempotencyStore({ maxKeys: 10_000 });
```

`claim` returns `'claimed'` when the caller should process the message, `'duplicate'` when it was already processed and `'in-progress'` when another delivery holds the key. A store may also implement `close()`. The library calls it at shutdown, for the stores it created and for the one given in the module options.

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
- **Idempotency and retries** apply to each message of a batch separately.
- **Concurrency.** A batch counts as one unit of `concurrency`.
- **No replies.** A batch never replies, so a request that lands in a `@Subscribe` batch is not answered.
- **No manual ack.** `batch` with `ack: 'manual'` fails startup with a `NatsConfigError`, because the library settles each message of a batch itself.
- **`maxWait` and `ack_wait`.** Messages waiting for their batch are heartbeated like the others (see [Heartbeats](#what-happens-to-a-failed-message)). When `maxWait` is at least the consumer's `ack_wait`, they rely on those heartbeats alone, so a late one (for example a blocked event loop) can get them redelivered, and the library logs a warning at startup. Keep `maxWait` well below `ack_wait`.
- **Shutdown.** The batch being collected is handed to the handler. See [Lifecycle and graceful shutdown](#lifecycle-and-graceful-shutdown).

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

## CloudEvents

The library reads and writes [CloudEvents](https://cloudevents.io) 1.0 following the [NATS protocol binding](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/nats-protocol-binding.md), in both modes:

- **Binary mode.** Attributes travel as `ce-<name>` headers (written in lower case, read in any case), `datacontenttype` as the `Content-Type` header, and the body is the event's data, encoded by the codec.
- **Structured mode.** The body is a JSON envelope with `Content-Type: application/cloudevents+json`.

### Publishing CloudEvents

Pass `cloudEvent` to `NatsClient.publish()` or to `JetStreamClient.publish()` (also in the `opts` of `publishMany()` and `batcher().add()`). The data you pass is the event's `data`.

```ts
import { Injectable } from '@nestjs/common';
import { JetStreamClient, NatsClient, NatsModule } from '@loipv/nestjs-nats';

NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  cloudEvents: { source: 'https://example.com/orders' }, // default `source` of every event
});

@Injectable()
class OrderEvents {
  constructor(
    private readonly nats: NatsClient,
    private readonly js: JetStreamClient,
  ) {}

  created(order: { id: string; total: number }) {
    // Binary mode, the default: `ce-*` headers plus the JSON body.
    this.nats.publish('orders.created', order, {
      cloudEvent: { type: 'com.example.order.created', subject: order.id },
    });
  }

  async createdDurably(order: { id: string; total: number }) {
    // Structured mode. Without `msgId`, `Nats-Msg-Id` is the event's `id`.
    const ack = await this.js.publish('orders.created', order, {
      cloudEvent: {
        type: 'com.example.order.created',
        mode: 'structured',
        extensions: { tenant: 'acme' },
      },
    });
    return ack.duplicate;
  }
}
```

| Option | Default | Description |
|---|---|---|
| `type` | required | The event type, for example `com.example.order.created` |
| `source` | `cloudEvents.source` of the module | Required: without it, and without the module default, publishing throws a `NatsConfigError` |
| `id` | `crypto.randomUUID()` | Must not be empty |
| `time` | now | An ISO 8601 string or a `Date` |
| `subject`, `dataschema` | none | CloudEvents attributes of the same name |
| `extensions` | none | Extension attributes. A name must match `[a-z0-9]+` and must not be a standard attribute, or publishing throws a `NatsConfigError`. Values are strings |
| `mode` | `'binary'` | `'binary'` or `'structured'` |

`specversion` is always `'1.0'`, and `datacontenttype` is the content type of the codec (`application/json` by default).

- **Errors.** An invalid `cloudEvent` throws a `NatsConfigError`: synchronously from `NatsClient.publish()`, as a rejection from `JetStreamClient.publish()`. `NatsClient.request()` has no `cloudEvent` option.
- **De-duplication.** `JetStreamClient.publish()` sets `Nats-Msg-Id` to the event's `id` unless you pass `msgId`, so the stream's duplicate window de-duplicates events with the same `id`.
- **Structured data.** With the JSON codec the data is the envelope's `data` member. With another codec, or when the data is a `Uint8Array`, the encoded bytes go into `data_base64`.
- **Outbound plugins** see the resolved event (`id`, `time` and `source` filled in) as `message.cloudEvent` of the `OutgoingMessage`, and the event is written to the wire after they have run.

### Receiving CloudEvents

A `@CloudEventRouter()` class receives CloudEvents on one core subscription (`subscribe`) or one JetStream consumer (`consume`), and hands each event to the `@OnCloudEvent(typePattern)` method whose pattern matches the event's `type` best. In those methods `@Payload()` is the event's `data` and `@CloudEvent()` is its attributes, a `CloudEventMeta`. Pipes, guards, interceptors and exception filters work as for any handler.

```ts
import { Injectable } from '@nestjs/common';
import {
  CloudEvent,
  type CloudEventMeta,
  CloudEventRouter,
  OnCloudEvent,
  Payload,
} from '@loipv/nestjs-nats';

@Injectable()
@CloudEventRouter({
  consume: { stream: 'EVENTS', durable: 'order-projector' },
  unmatched: 'dlq',
  dlq: true,
})
class OrderProjector {
  constructor(private readonly orders: OrderService) {}

  @OnCloudEvent('com.example.order.created')
  created(@Payload() order: OrderDto, @CloudEvent() event: CloudEventMeta) {
    return this.orders.project(order, event.id);
  }

  // `*` matches one segment: order.paid, order.shipped, ...
  @OnCloudEvent('com.example.order.*')
  changed(@Payload() data: unknown, @CloudEvent() event: CloudEventMeta) {
    return this.orders.touch(event.subject, data);
  }

  // A trailing `>` matches one or more segments. One method can handle several patterns.
  @OnCloudEvent('com.example.>')
  @OnCloudEvent('legacy.order.>')
  other(@CloudEvent() event: CloudEventMeta) {
    this.orders.noteIgnored(event.type);
  }
}
```

`@CloudEventRouter()` takes `connection`, `concurrency`, `codec`, `retry`, `dlq` and `idempotency` like `@Subscribe` and `@Consume`, plus these:

| Option | Description |
|---|---|
| `subscribe`, `queue` | Receive on a core subject (with an optional queue group). Set exactly one of `subscribe` or `consume` |
| `consume` | `{ stream, durable, config? }`: receive from a JetStream consumer, as for `@Consume` |
| `unmatched` | What to do with an event no `@OnCloudEvent` matches: `'ack'` (default), `'term'` or `'dlq'` |

- **Patterns.** A type is split on `.`. `*` matches exactly one segment, and a trailing `>` matches one or more. When several patterns match, an exact type wins over a pattern with `*`, which wins over one ending in `>`; among patterns of the same kind, the one with more literal segments wins, then the one declared first. An empty segment, a `>` that is not last or a repeated pattern fails startup with a `NatsConfigError`.
- **Attributes first.** The router reads the attributes and picks the method before it decodes the event's data. An unmatched event follows `unmatched` even if its data cannot be decoded. If the data of a matched event cannot be decoded, that is a `NatsDecodeError`, handled like any decode failure.
- **Not a CloudEvent.** A message without the required attributes (`id`, `source`, `type`, `specversion`), or with a structured body that is not a JSON object, fails with a `NatsDecodeError`. It is not retried: it is dead-lettered if a DLQ is configured, else terminated (JetStream) or dropped (core), and a request gets a `400` reply.
- **Unmatched events.** `'ack'` acknowledges and skips the event (logged at debug level). `'term'` terminates a JetStream message, or drops a core one. `'dlq'` sends it to the [dead-letter queue](#dead-letter-queue); if none is configured, it behaves like `'term'` and logs a warning. An unmatched event is never retried. A request (a core message with a reply subject) that is unmatched gets a `404` error reply with the message `No handler for CloudEvent type "<type>"`, unless `unmatched` is `'ack'`, which sends no reply.
- **Rules.** A router has no `batch` or `ack` option: it handles one event at a time and settles it automatically. It needs at least one `@OnCloudEvent` method, and an `@OnCloudEvent` method cannot also be a `@Subscribe`, `@Consume` or `@ServiceEndpoint`. Breaking these rules fails startup with a `NatsConfigError`.
- **Idempotency.** With `idempotency` enabled, the key of an event is its `Nats-Msg-Id` header if it has one (the `id` of an event published with `JetStreamClient`), else `<source>|<id>`. See [Idempotency](#idempotency).
- **Outside a router.** In any other handler `@CloudEvent()` and `ctx.cloudEvent` give the attributes of the message, or `undefined` when it is not a valid CloudEvent. In a batch handler `@CloudEvent()` is an array aligned with the payloads. `@Payload()` is not unwrapped there: for a structured event it is the whole envelope.

A router is one handler for the connection's plugins, retries and dead-letter queue. Its `HandlerInfo` has the class name and `methodName: '@CloudEventRouter'`.

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

## Plugins

A plugin can wrap every inbound delivery and every outbound message of a connection, in the style of Koa middleware:

```ts
import { NatsModule, type NatsPlugin } from '@loipv/nestjs-nats';

const timing: NatsPlugin = {
  name: 'timing',
  inbound: {
    stage: 'outer', // 'outer' runs before settlement; 'inner' (default) runs closest to the handler
    async handle(delivery, next) {
      const started = performance.now();
      try {
        await next();
      } finally {
        const { className, methodName } = delivery.handler;
        delivery.logger.log(`${className}.${methodName} took ${performance.now() - started} ms`);
      }
    },
  },
  outbound: {
    async handle(message, next) {
      message.headers.set('x-sent-at', new Date().toISOString());
      await next();
    },
  },
};

NatsModule.forRoot({ connection: { servers: 'nats://localhost:4222' }, plugins: [timing] });
```

The inbound order is: `outer` plugins, then acknowledgement handling, then `inner` plugins, then decoding and the handler. `delivery.handler` is a read-only `HandlerInfo` that describes the handler (`kind`, `connection`, `className`, `methodName`, plus `subject`/`queue`, `stream`/`durable` or `service`/`endpoint`, depending on `kind`). Per-handler logic is better written as a regular Nest interceptor.

### Observing outcomes

`delivery.outcomes` is a `Map` from each message of the delivery (`delivery.messages`) to a `MessageOutcome` that says what became of it. The settlement and idempotency middlewares fill it in, so it is complete once the `next()` of an `outer` plugin has resolved or rejected. Read it there, after awaiting `next()`:

```ts
import type { NatsPlugin } from '@loipv/nestjs-nats';

const deadLetterAlerts: NatsPlugin = {
  name: 'dead-letter-alerts',
  inbound: {
    stage: 'outer',
    async handle(delivery, next) {
      try {
        await next();
      } finally {
        for (const [message, outcome] of delivery.outcomes) {
          if (outcome.action === 'dlq') {
            delivery.logger.warn(`${message.subject} dead-lettered to ${outcome.subject}`);
          }
        }
      }
    },
  },
};
```

| `action` | Meaning | Other fields |
|---|---|---|
| `'ack'` | Acknowledged: automatically after success, or by the handler itself | |
| `'retry'` | Handed back for another attempt: a JetStream `nak(delay)`, or an in-process retry of a core message | `delayMs`, `error` |
| `'dlq'` | Failed for good and published to the dead-letter subject | `subject`, `error` |
| `'term'` | A JetStream message terminated without a dead letter | `error?` |
| `'drop'` | A core message given up on without a dead letter | `error?` |
| `'skipped'` | Left out of the handler: a duplicate, or being processed by another delivery | `reason`: `'duplicate'` or `'in-progress'` |
| `'replied'` | A request or service endpoint: the adapter sends the reply (an error reply when `error` is set) | `error?` |
| `'none'` | Nothing to settle: a success on a core subscription, or a manually acknowledged message left unacknowledged | |

A message with no entry was not settled by the library, for example because an `outer` plugin threw before calling `next()`.

## Tracing (OpenTelemetry)

`@loipv/nestjs-nats/otel` provides a plugin that traces every publish, request and delivery, and propagates the trace context in the message headers, so a trace continues from the producer to the consumer. It needs the `@opentelemetry/api` peer dependency (`^1.9.0`):

```bash
npm install @opentelemetry/api
```

**Setting up OpenTelemetry is your job.** The plugin only uses the API. Register a tracer provider with an exporter, a context manager (for example `AsyncLocalStorageContextManager`) so spans stay linked across `await`, and the W3C Trace Context propagator before the application starts. `NodeSDK` from `@opentelemetry/sdk-node` does all three by default. Without a registered SDK the plugin does nothing: no spans, no headers added.

```ts
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { NatsModule } from '@loipv/nestjs-nats';
import { tracing } from '@loipv/nestjs-nats/otel';

// Start the SDK before the application is created.
const sdk = new NodeSDK({ serviceName: 'orders-service', traceExporter: new OTLPTraceExporter() });
sdk.start();

NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  plugins: [tracing({ captureHeaders: ['x-tenant'] })],
});
```

| Option | Default | Description |
|---|---|---|
| `tracerName` | `'@loipv/nestjs-nats'` | Name of the tracer the spans are created with |
| `captureHeaders` | none | Header names to record as `messaging.header.<lower-case name>` attributes (a string, or an array when the header has several values). Names match in any case. Do not list headers that carry secrets |

### Spans

| Span | Kind | Created for |
|---|---|---|
| `publish <subject>` | `PRODUCER` | `NatsClient.publish()` and `JetStreamClient.publish()`, including every item of `publishMany()` and `batcher()` |
| `send <subject>` | `CLIENT` | `NatsClient.request()` |
| `process <subject>` | `CONSUMER` | A message (or batch) handled by a `@Subscribe`, `@Consume` or `@CloudEventRouter` that sends no reply |
| `process <subject>` | `SERVER` | A message that expects a reply: a `@ServiceEndpoint`, or a `@Subscribe` handler for a request |

- **Propagation.** An outbound span's context is injected into the message headers (`traceparent`, `tracestate` and whatever else your propagator writes). A `process` span of a single message is a child of the context extracted from the message's headers, or starts a new trace if there is none. For a structured [CloudEvent](#cloudevents) without a `traceparent` header, the context is read from the envelope's `traceparent` and `tracestate` extension attributes.
- **Batches.** A `process` span of a batch starts a new trace and has a link to the context of each message that carried one.
- **Span scope.** The `process` span covers the handler and the settlement (ack, retry, dead-lettering), and spans you create in the handler are its children.
- **Dead letters.** DLQ publishes do not go through plugins, so they have no `publish` span.

### Attributes

| Attribute | On | Value |
|---|---|---|
| `messaging.system` | all | `nats` |
| `messaging.destination.name` | all | The subject |
| `messaging.operation.type` | all | `send` for `publish` and `send` spans, `process` for `process` spans |
| `messaging.operation.name` | all | `publish`, `request` or `process` |
| `messaging.message.id` | `publish`, `send`, and single-message `process` | The `Nats-Msg-Id` header, else the CloudEvent `id`, when there is one |
| `messaging.message.body.size` | single-message `process` | Size of the body in bytes |
| `messaging.consumer.group.name` | `process` | The durable name of a `@Consume`, or the queue group of a `@Subscribe` that has one |
| `messaging.batch.message_count` | batch `process` | Number of messages |
| `messaging.nats.stream`, `messaging.nats.seq`, `messaging.nats.delivery_count` | single-message `process` of a JetStream message | Stream, stream sequence and delivery count |
| `messaging.header.<name>` | as set by `captureHeaders` | The header's value |

### Events and errors

The `process` span records what became of the messages as events:

| Event | Attributes | When |
|---|---|---|
| `nats.retry` | `nats.retry.delay_ms`, `nats.attempt` | A message was handed back for another attempt |
| `nats.dlq` | `nats.dlq.subject` | A message was dead-lettered |
| `nats.skipped` | `nats.skipped.reason` | An idempotency duplicate (`duplicate`) or a message being processed elsewhere (`in-progress`) |

An error, whether the handler threw it or a failed message carries it, is recorded as an exception event and sets the span status to `ERROR`. The status of a successful span is left unset. A failed publish or request records the error on its span too.

Notes:

- A batch span is named after the subject of its first message, and a batch has no per-message attributes (`messaging.message.id`, `messaging.message.body.size`, `messaging.nats.*`, captured headers).
- The spans of service endpoints have no `messaging.consumer.group.name`, because a service endpoint has no queue group or durable name.

## Health checks

`@loipv/nestjs-nats/health` provides a [Terminus](https://docs.nestjs.com/recipes/terminus) health indicator. It needs the `@nestjs/terminus` peer dependency (`11.x` or `12.x`). Import `NatsHealthModule` next to `NatsModule` and `TerminusModule`, and call the indicator from a health check:

```ts
import { Controller, Get, Module } from '@nestjs/common';
import { HealthCheck, HealthCheckService, TerminusModule } from '@nestjs/terminus';
import { NatsModule } from '@loipv/nestjs-nats';
import { NatsHealthIndicator, NatsHealthModule } from '@loipv/nestjs-nats/health';

@Controller('health')
class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly nats: NatsHealthIndicator,
  ) {}

  @Get()
  @HealthCheck()
  check() {
    return this.health.check([() => this.nats.isHealthy('nats', { jetstream: true })]);
  }
}

@Module({
  imports: [
    NatsModule.forRoot({ connection: { servers: 'nats://localhost:4222' } }),
    TerminusModule,
    NatsHealthModule,
  ],
  controllers: [HealthController],
})
class AppModule {}
```

`isHealthy(key, options?)` accepts:

| Option | Default | Description |
|---|---|---|
| `connection` | `'default'` | Name of the connection to check |
| `rttTimeout` | `1000` | Longest wait, in ms, for the round trip to the server and, separately, for the JetStream check |
| `jetstream` | `false` | Also require JetStream to answer an account info request |

When the connection is up, the result is `{ nats: { status: 'up', connection, server, rtt } }`, where `rtt` is the round trip time in ms. Otherwise it is `{ nats: { status: 'down', connection, reason } }`, and `HealthCheckService` turns that into a `503` response. `reason` is one of:

- `unknown connection`
- `reconnecting`, `draining` or `closed`: the state of the connection
- `rtt timeout after <n>ms` or `rtt failed: <error>`
- `jetstream unavailable: <error>` (also for a timeout)
- `check failed: <error>`

The indicator never throws, and the result holds no credentials: the server URL is redacted and error text is scrubbed of the connection's password and token. The check can wait up to `rttTimeout` for the round trip plus up to `rttTimeout` for the JetStream check, so up to twice `rttTimeout` in the worst case. Set your probe's timeout above that.

## Testing

`@loipv/nestjs-nats/testing` lets you test code that uses the library without a NATS server. It needs nothing beyond the library itself, and works with any test runner. The examples use `@nestjs/testing` and `node:assert`.

### Mock clients

`createMockNatsClient()` and `createMockJetStreamClient()` return a real `NatsClient` and `JetStreamClient` that run on a fake connection. Outbound plugins and CloudEvents work as in production. Everything published is recorded in `published`.

```ts
import assert from 'node:assert/strict';
import { NatsRequestError } from '@loipv/nestjs-nats';
import { createMockJetStreamClient, createMockNatsClient } from '@loipv/nestjs-nats/testing';

const nats = createMockNatsClient({ cloudEvents: { source: 'urn:test' } });

nats.publish('orders.created', { id: 'o1' }, { cloudEvent: { type: 'order.created' } });
await nats.flush(); // publishes are asynchronous: flush before you assert
assert.equal(nats.published[0]?.subject, 'orders.created');
assert.deepEqual(nats.published[0]?.data, { id: 'o1' });
assert.equal(nats.published[0]?.cloudEvent?.type, 'order.created');

// Requests are answered from stubs. With no stub, request() rejects with code 'no_responders'.
nats.whenRequest('orders.get').reply({ id: 'o1', total: 10 });
assert.deepEqual(await nats.request('orders.get', { id: 'o1' }), { id: 'o1', total: 10 });
nats.whenRequest('orders.cancel').fail({ code: 409, description: 'already shipped' });
await assert.rejects(
  nats.request('orders.cancel', { id: 'o1' }),
  (err) => err instanceof NatsRequestError && err.code === 'service_error',
);

const js = createMockJetStreamClient();
await js.publish('orders.created', { id: 'o1' }, { msgId: 'o1' });
const again = await js.publish('orders.created', { id: 'o1' }, { msgId: 'o1' });
assert.equal(again.duplicate, true); // acked by a stream called 'mock'
assert.equal(js.published.length, 2);
```

| Member | Description |
|---|---|
| `published` | The recorded messages, in order. Each has `subject`, `data` (decoded with the codec, the event's data for a CloudEvent), `raw` (the bytes), `headers`, `cloudEvent` (when the message is one) and `reply` (for a request). On the core client, requests are included |
| `whenRequest(subject).reply(value)` | Answers requests to `subject` (a NATS subject pattern) with `value`. The latest matching stub wins |
| `whenRequest(subject).fail({ code, description })` | Answers with a service error |

Options: `codec`, `plugins` and `cloudEvents` (`{ source? }`). The JetStream mock acks with `{ stream: 'mock', seq, duplicate }`, where `duplicate` is `true` for a repeated `Nats-Msg-Id`. A standalone mock is never closed, so a `batcher()` of a mock is not flushed for you: call its `flush()` yourself.

### `NatsTestingModule` and `NatsTestHarness`

`NatsTestingModule.forRoot(options?)` replaces `NatsModule.forRoot()` in a test. It takes the same options except `connection`: `name`, `codec`, `plugins`, `retry`, `dlq`, `idempotency`, `cloudEvents` and so on. It never connects, syncs streams or subscribes. It provides the same injectable clients (`NatsClient` and `JetStreamClient` are the mocks above), `NatsConnectionManager` and `NatsTestHarness`. `NatsStreamManager` and the raw connection are provided too, but JetStream management is not available and subscriptions are refused.

Messages reach your handlers through `NatsTestHarness.dispatch()`. It runs a fake message through the handler's real inbound pipeline: your plugins, retry decisions, dead-letter queue, idempotency, and Nest's guards, pipes, interceptors and exception filters. The harness never waits for a delay and never runs a retry again; it only reports the outcome.

```ts
import assert from 'node:assert/strict';
import { Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Consume, NatsClient, NonRetryableError, Payload } from '@loipv/nestjs-nats';
import { NatsTestHarness, NatsTestingModule } from '@loipv/nestjs-nats/testing';

@Injectable()
class BillingHandlers {
  constructor(private readonly nats: NatsClient) {}

  @Consume({ stream: 'ORDERS', durable: 'billing', dlq: true })
  async bill(@Payload() order: { id: string; total: number }) {
    if (order.total <= 0) throw new NonRetryableError('empty order');
    this.nats.publish('orders.billed', { id: order.id });
    return { billed: true };
  }
}

const app = await Test.createTestingModule({
  imports: [NatsTestingModule.forRoot()],
  providers: [BillingHandlers],
}).compile();
await app.init();
const harness = NatsTestHarness.create(app);

const ok = await harness.dispatch(
  { handler: 'BillingHandlers.bill' },
  { id: 'o1', total: 10 },
  { subject: 'orders.created' },
);
assert.equal(ok.settlement, 'ack');
assert.deepEqual(ok.result, { billed: true });

const bad = await harness.dispatch({ handler: 'BillingHandlers.bill' }, { id: 'o2', total: 0 });
assert.equal(bad.settlement, 'dlq');
assert.equal(bad.dlq[0]?.subject, 'dlq.ORDERS.billing');
```

`dispatch(target, payload, options?)` takes a target and returns a `DispatchResult`:

| Target | Selects |
|---|---|
| `{ handler: 'Class.method' }` | A handler by class and method name. For a CloudEvent router it is `'Class.@CloudEventRouter'` |
| `{ subject: '...' }` | The one handler that receives from that subject. For `@Consume` the subject is matched against the consumer's `filter_subject`(s) |

It rejects with a `NatsConfigError` when the handlers are misconfigured (as bootstrap would report), or when the target matches no handler or several. For a batch handler, `payload` is a non-empty array with one message per entry.

| Option | Description |
|---|---|
| `headers` | Message headers |
| `subject` | The message's actual subject. Defaults to the target's subject, else to the handler's own: its subject pattern (which can be a wildcard), its first `filter_subject`, or the stream name for a `@Consume` without a filter. Pass it for realistic values in `@Subject()` and `ctx.subject` |
| `deliveryCount` | JetStream delivery count (default `1`); above `1` the message counts as redelivered |
| `attempt` | Core delivery attempt (default `1`), as after in-process retries |
| `request` | Core subscriptions: the message is a request, with a reply subject |
| `cloudEvent` | Sends the payload as a [CloudEvent](#cloudevents), with the same options as publishing |

| `DispatchResult` field | Description |
|---|---|
| `result` | What the handler returned (never set for a batch) |
| `settlement` | What became of the first message: `'ack'`, `'nak'` (a JetStream retry), `'retry'` (a core retry), `'term'`, `'dlq'`, `'drop'`, `'skipped'`, `'replied'` or `'none'` |
| `outcomes` | The [`MessageOutcome`](#observing-outcomes) of every message, aligned with the payloads |
| `nakDelay` | The redelivery delay in ms when `settlement` is `'nak'` |
| `reply` | For a request or service endpoint: `{ data }`, or `{ error: { code, description } }` |
| `dlq` | The dead letters published while the messages were processed |

Notes:

- Idempotency stores keep their state across the dispatches of one application, so dispatching the same message twice shows a duplicate.
- Dead-letter publishes, and requests sent by your handlers, also appear in the `published` of the mock clients of the testing module.
- A dispatched message is a fake message, so `ctx.raw` is a stand-in that records what you call on it. It is not a real `Msg` or `JsMsg`.

## Errors

| Class | Thrown when |
|---|---|
| `NatsModuleError` | Base class of all errors below |
| `NatsConfigError` | Invalid configuration found at startup. Examples: unknown connection name, duplicate durable, endpoint without `@NatsService`, request-scoped handler provider, missing consumer, an invalid `@CloudEventRouter`. Also thrown when publishing an invalid `cloudEvent` (no source, bad extension name) |
| `NatsConnectionError` | The first connection attempt failed, including authentication errors |
| `StreamConfigConflictError` | A declared stream or consumer differs in an immutable field. The error carries `field`, `current` and `declared` |
| `NatsDecodeError` | A payload could not be decoded, or a message is not a valid CloudEvent |
| `NatsRequestError` | `NatsClient.request()` failed. See `code` |
| `NonRetryableError` | Throw it yourself to tell the library not to retry a message. It goes straight to the DLQ if there is one, else it is dropped |

A failing request handler or service endpoint replies with the `Nats-Service-Error` and `Nats-Service-Error-Code` headers:

| Error | Code | Description |
|---|---|---|
| `HttpException` | its status | its message |
| `NatsDecodeError` | `400` | `Bad Request` |
| anything else | `500` | `Internal Server Error`. Internal details are never sent |

## Lifecycle and graceful shutdown

1. **Module init:** connect, failing fast if this does not work, then sync declared streams and consumers.
2. **Application bootstrap:** discover handlers, validate them, and start subscriptions, consumers and services. If any step fails, everything already started is stopped and all connections are closed before the error is rethrown.
3. **Shutdown:** see below.

Call `app.enableShutdownHooks()` so this runs on `SIGTERM` and `SIGINT`.

Shutdown has bounded phases. `shutdown.drainTimeout` (default `10000` ms) is the longest any phase waits. If connections have different values, the largest one is used.

```ts
NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  shutdown: { drainTimeout: 15_000 },
});
```

1. **Stop intake.** Subscriptions and consumers stop receiving messages. A batch that is being collected is handed to its handler.
2. **Wait for in-flight handlers.** Phases 1 and 2 together wait for up to `drainTimeout`.
3. **Force stop.** JetStream messages that are being processed, collected into a batch, or waiting for a batch slot are `nak`'d so the server redelivers them. Pending in-process retries are cancelled, and collected core messages that never started are dropped. When something was forced, a warning per connection summarises the `nak`'d messages and cancelled retries, and a core handler may log a further warning about the dropped messages.
4. **Close connections.** Each connection runs its close hooks (open `batcher()`s flush here), then drains. If this takes longer than `drainTimeout`, the connection is closed without draining and a warning is logged. This also happens when the server is unreachable.
5. **Close idempotency stores.** The `close()` of each store is called, within the same bound.

In the worst case, shutdown takes about `2 × drainTimeout`: up to `drainTimeout` for phases 1 and 2, and up to `drainTimeout` more for phases 4 and 5. No step waits forever.

Shutdown runs in Nest's `beforeApplicationShutdown` hook, and Nest calls every provider's `onModuleDestroy` before that. Handlers therefore keep receiving and processing messages after your other providers' `onModuleDestroy` ran. If a handler uses a resource, such as a database client, close that resource in `onApplicationShutdown`, which runs after the library has stopped intake and waited for in-flight handlers.

Handlers are singletons. Handlers in `REQUEST`- or `TRANSIENT`-scoped providers are rejected at startup.

`NatsConnectionManager` exposes `status(name?)` (`connected`, `reconnecting`, `draining` or `closed`) and `status$(name?)`, an Observable of connection events such as disconnect, reconnect and lame-duck mode.

## Known limitations

These are tracked for upcoming releases:

- **Core retries are in-process.** Retries of `@Subscribe` messages are timers inside the process, so pending retries are lost if the process dies or shuts down. Use `@Consume` when a message must survive that.
- **Retries can reorder messages**, even with `concurrency: 1`: a failed message is redelivered after the delay, behind messages that arrived meanwhile.
- **DLQ publishing bypasses outbound plugins.** Dead letters are published directly, without going through `plugins[].outbound`.
- **Core batches never reply.** A message with a reply subject that arrives in a `@Subscribe` batch is never answered, so its requester times out.
- **Pulled messages are not all heartbeated.** With `concurrency` above 1, or with `batch`, a few messages the NATS client has pulled but not yet handed over are not kept alive. See [Heartbeats](#what-happens-to-a-failed-message).
- **Messages at shutdown.** Messages the server delivers while shutdown is stopping a consumer, and messages queued behind a handler that never finishes, are redelivered only after the consumer's `ack_wait` (30 s by default).
- **Duplicate error logs.** Handler errors that are not `HttpException`s are logged by Nest's `ExceptionsHandler` as well as by this library. `logger: false` silences only this library's logs.
- **Publish order.** If an outbound plugin awaits something, consecutive `publish()` calls are not guaranteed to leave in call order.
- **Undetected removals.** Stream and consumer sync compares only declared fields, so a field you remove from a declaration is not detected as drift.

## License

[MIT](./LICENSE)
