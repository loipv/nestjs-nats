# @loipv/nestjs-nats

Decorator-driven [NATS](https://nats.io) integration for [NestJS](https://nestjs.com): core subscriptions, JetStream consumers and NATS services as plain Nest providers, with pipes, guards, interceptors and exception filters working the way they do for HTTP controllers.

> [!WARNING]
> **Status: 0.x, under active development, not production-ready yet.**
> The API may change between minor versions until 1.0. See [Known limitations](#known-limitations) and the [roadmap](#roadmap).

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
- [Codecs](#codecs)
- [Plugins](#plugins)
- [Errors](#errors)
- [Lifecycle and graceful shutdown](#lifecycle-and-graceful-shutdown)
- [Known limitations](#known-limitations)
- [Roadmap](#roadmap)
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
- An inbound and outbound plugin pipeline for cross-cutting concerns.
- Startup fails fast on a bad connection, bad credentials or invalid handler configuration. Shutdown stops intake, waits for in-flight handlers and drains connections, all within a bounded time.

## Requirements

| | Version |
|---|---|
| Node.js | `>= 22.12` |
| NestJS | `11.x` or `12.x` |
| NATS client | `@nats-io/transport-node`, `@nats-io/jetstream`, `@nats-io/services` `3.x` |
| NATS server | JetStream enabled, needed only for `@Consume` and `JetStreamClient` |

The package is **ESM-only**. CommonJS applications, which includes most Nest 11 projects, can still `require()` it, because Node 22.12+ loads ES modules through `require(esm)`.

## Installation

```bash
npm install @loipv/nestjs-nats @nats-io/transport-node @nats-io/jetstream @nats-io/services
```

The library also has `@nestjs/common`, `@nestjs/core`, `reflect-metadata` and `rxjs` as peer dependencies. A Nest application already has them.

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
| `concurrency` | `1` | Maximum number of messages processed at once. The library only pulls more messages when a slot is free |
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

`publish()` also accepts `expect` (optimistic concurrency, for example `{ lastSubjectSequence: 41 }`), `headers`, `codec` and `timeout`. `msgId` is sent as the `Nats-Msg-Id` header, so plugins can see it. To publish many messages at once, see [Batch publishing](#batch-publishing).

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

Set `retry` and `dlq` on `NatsModule.forRoot()` for every handler of a connection, or on a single `@Subscribe` / `@Consume`. Options of a handler are merged over those of the module, a handler's `retry: false` turns retries off for it, and a handler's `retry` object turns them back on when the module disabled them. Invalid values fail startup with a `NatsConfigError`. The examples in this section and the next ones use placeholder types and services (`OrderDto`, `PaymentService`, ...) that you replace with your own.

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
| `initialDelay` | `1000` | Delay in ms before the first redelivery |
| `factor` | `2` | Multiplier of the delay for each further redelivery (`>= 1`) |
| `maxDelay` | `60000` | Upper bound of the delay in ms, before jitter. Must be `>= initialDelay` |
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
- **Heartbeats.** While a `@Consume` handler and its settlement run, the library calls `working()` on the message every `ack_wait / 2`, until the message is settled. A slow handler or a slow DLQ publish does not make the server redeliver the message. In a batch this covers every message of the batch.
- **`max_deliver`.** The server stops redelivering a message after the consumer's `max_deliver`, and the message then never reaches the DLQ. At startup the library logs a warning when a consumer has `max_deliver` greater than `0` and at most `maxRetries`. Use `max_deliver` of at least `maxRetries + 1`, or `-1`, which is the default for consumers the library creates.

### Dead-letter queue

`dlq` is `true`, or an object, on the module or a handler. `dlq: false` on a handler always wins.

| Option | Default | Description |
|---|---|---|
| `subject` | see below | A subject, or a function `(handler: HandlerInfo, msg: InboundMessage) => string` |
| `mode` | `'jetstream'` for `@Consume`, `'core'` for `@Subscribe` | `'jetstream'` waits for the PubAck. `'core'` is a plain publish |

The default subject is `dlq.<stream>.<durable>` for `@Consume` and `dlq.<actual subject>` for `@Subscribe`. In `'jetstream'` mode a stream must capture the subject. This stream captures every default subject:

```ts
NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  streams: [{ name: 'DLQ', subjects: ['dlq.>'] }],
  dlq: true,
});
```

A dead letter has the original body, unchanged, and the original headers, plus these:

| Header | Value |
|---|---|
| `Nats-DLQ-Subject` | Subject the message arrived on |
| `Nats-DLQ-Stream` | Stream of the original message (JetStream messages only) |
| `Nats-DLQ-Consumer` | Consumer that failed to process it (JetStream messages only) |
| `Nats-DLQ-Seq` | Stream sequence of the original message (JetStream messages only) |
| `Nats-DLQ-Deliveries` | Number of attempts made |
| `Nats-DLQ-Error` | Message of the last error. At most 1024 characters, line breaks replaced by spaces |
| `Nats-DLQ-Timestamp` | When it was dead-lettered, ISO 8601 |

The JetStream publish directives of the original message, `Nats-Expected-*`, `Nats-Rollup` and `Nats-TTL`, are not copied, because they were meant for the original stream. `Nats-Msg-Id` is copied, so the DLQ stream de-duplicates a dead letter that is sent twice.

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
      // Used when the message has no `Nats-Msg-Id` header.
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
| `keyExtractor` | none | `(msg: InboundMessage) => string \| undefined`. Called only if the message has no `Nats-Msg-Id` header |

- **Key.** The key is the `Nats-Msg-Id` header, which `JetStreamClient.publish()` sets from `msgId`, and otherwise what `keyExtractor` returns. A message without a key is processed as usual.
- **Scope.** Keys are scoped per handler, as `consume:<stream>/<durable>` or `subscribe:<Class>.<method>`, so handlers that share a store never swallow each other's messages. The connection name is not part of the scope, so several instances of an application agree on keys when they share a store.
- **Flow.** The key is claimed before the handler runs. If the handler succeeds, the key is marked as done for `ttl`. If it fails, the key is released, so the retry runs.
- **Duplicates.** A message whose key is done is skipped, and acknowledged if it is a JetStream message. A message whose key is being processed by another delivery is skipped, and a JetStream message is `nak`'d, with the retry `initialDelay` (or 1000 ms when retries are off) as delay, so it comes back later.
- **No reply for duplicates.** A skipped `@Subscribe` request gets no reply, and its requester times out.
- **Store errors.** If the store fails to claim a key, the message is handled as a failure and retried.
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
- **Partial failure.** `ctx.fail(target, error)` marks one message as failed, where `target` is its index in `items` or its raw message. You can also return `{ failed: [{ msg, error }] }`, where `msg` is an index or a raw message. A failed message goes through the usual retry and DLQ rules on its own, and the others are acknowledged. If the handler throws, every message of the batch fails.
- **Undecodable messages.** A message that cannot be decoded is left out of `items`. It is not retried: for `@Consume` it is dead-lettered if a DLQ is configured, otherwise terminated; for `@Subscribe` it is dead-lettered if a DLQ is configured, otherwise logged and dropped. The rest of the batch is still handled.
- **Idempotency and retries** apply to each message of a batch separately.
- **Concurrency.** A batch counts as one unit of `concurrency`.
- **No replies.** A batch never replies, so a request that lands in a `@Subscribe` batch is not answered.
- **No manual ack.** `batch` with `ack: 'manual'` fails startup with a `NatsConfigError`, because the library settles each message of a batch itself.
- **`maxWait` and `ack_wait`.** Messages waiting for their batch are not heartbeated. When `maxWait` is at least the consumer's `ack_wait`, the server may redeliver them while they wait, and the library logs a warning at startup. Keep `maxWait` well below `ack_wait`.
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
import { Batcher, JetStreamClient } from '@loipv/nestjs-nats';

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

## Errors

| Class | Thrown when |
|---|---|
| `NatsModuleError` | Base class of all errors below |
| `NatsConfigError` | Invalid configuration found at startup. Examples: unknown connection name, duplicate durable, endpoint without `@NatsService`, request-scoped handler provider, missing consumer |
| `NatsConnectionError` | The first connection attempt failed, including authentication errors |
| `StreamConfigConflictError` | A declared stream or consumer differs in an immutable field. The error carries `field`, `current` and `declared` |
| `NatsDecodeError` | A payload could not be decoded |
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

Handlers are singletons. Handlers in `REQUEST`- or `TRANSIENT`-scoped providers are rejected at startup.

`NatsConnectionManager` exposes `status(name?)` (`connected`, `reconnecting`, `draining` or `closed`) and `status$(name?)`, an Observable of connection events such as disconnect, reconnect and lame-duck mode.

## Known limitations

These are tracked for upcoming releases:

- **Core retries are in-process.** Retries of `@Subscribe` messages are timers inside the process, so pending retries are lost if the process dies or shuts down. Use `@Consume` when a message must survive that.
- **Retries can reorder messages**, even with `concurrency: 1`: a failed message is redelivered after the delay, behind messages that arrived meanwhile.
- **DLQ publishing bypasses outbound plugins.** Dead letters are published directly, without going through `plugins[].outbound`.
- **Core batches never reply.** A message with a reply subject that arrives in a `@Subscribe` batch is never answered, so its requester times out.
- **Messages at shutdown.** Messages the server delivers while shutdown is stopping a consumer, and messages queued behind a handler that never finishes, are redelivered only after the consumer's `ack_wait` (30 s by default).
- **Duplicate error logs.** Handler errors that are not `HttpException`s are logged by Nest's `ExceptionsHandler` as well as by this library. `logger: false` silences only this library's logs.
- **Publish order.** If an outbound plugin awaits something, consecutive `publish()` calls are not guaranteed to leave in call order.
- **Undetected removals.** Stream and consumer sync compares only declared fields, so a field you remove from a declaration is not detected as drift.

## Roadmap

Planned, **not available yet**:

- CloudEvents (binary and structured modes) with routing by `type`
- OpenTelemetry tracing from producer to consumer (`@loipv/nestjs-nats/otel`)
- Health checks for `@nestjs/terminus` (`@loipv/nestjs-nats/health`)
- Testing utilities (`@loipv/nestjs-nats/testing`)

## License

[MIT](./LICENSE)
