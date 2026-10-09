# @loipv/nestjs-nats

Decorator-driven [NATS](https://nats.io) integration for [NestJS](https://nestjs.com): core subscriptions, JetStream consumers and NATS services as plain Nest providers, with pipes, guards, interceptors and exception filters working the way they do for HTTP controllers.

> [!WARNING]
> **Status: 0.x, under active development, not production-ready yet.**
> Failed JetStream messages are currently redelivered **immediately** (no backoff), and shutdown is not yet bounded when the NATS server is unreachable. Retry with exponential backoff, DLQ and a bounded shutdown are next on the [roadmap](#roadmap). The API may change between minor versions until 1.0.

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
- Several named connections in one application, each passing its options straight to the NATS client, so every NATS auth method and TLS/mTLS works unchanged.
- An inbound and outbound plugin pipeline for cross-cutting concerns.
- Startup fails fast on a bad connection, bad credentials or invalid handler configuration. Shutdown stops intake, waits for in-flight handlers and drains connections.

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
import { getNatsClientToken, NatsClient, NatsModule, Payload, Subscribe } from '@loipv/nestjs-nats';

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

  const client = app.get<NatsClient>(getNatsClientToken());
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

Inject per connection with `@InjectNatsClient(name?)`, `@InjectJetStream(name?)`, `@InjectStreamManager(name?)` and `@InjectNatsConnection(name?)`. The last one gives you the raw `NatsConnection`. Outside constructors, for example with `app.get()`, use the matching token functions: `getNatsClientToken(name?)`, `getJetStreamClientToken(name?)`, `getStreamManagerToken(name?)` and `getConnectionToken(name?)`.

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

**Request/reply.** If the incoming message has a reply subject and the handler returns something other than `undefined`, the return value is encoded and sent back. If the handler throws, the requester gets an error reply. See [Errors](#errors).

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

A consumer the library creates defaults to `ack_policy: 'explicit'` and `max_deliver: -1`, unless you set them. These defaults are never applied to a consumer that already exists.

With `ack: 'auto'` the outcome of each message is decided as follows:

| Outcome | Action |
|---|---|
| Handler succeeds | `ack()` |
| `NatsDecodeError`, `NonRetryableError`, or an `HttpException` with a 4xx status (for example from `ValidationPipe` or a guard) | `term()`: the message is not redelivered |
| Any other error | `nak()`: the message is redelivered (immediately for now, see [Known limitations](#known-limitations)) |

With `ack: 'manual'` you settle the message yourself with `ctx.ack()`, `ctx.nak(delayMs?)` or `ctx.term(reason?)`. If the handler throws before settling, the message is nak'd. Only the first settle call takes effect. For long-running work, call `ctx.working()` to reset the server's ack timer.

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
import { InjectJetStream, JetStreamClient } from '@loipv/nestjs-nats';

@Injectable()
class OrderPublisher {
  constructor(@InjectJetStream() private readonly js: JetStreamClient) {}

  async created(order: { id: string }) {
    const ack = await this.js.publish('orders.created', order, {
      msgId: order.id, // the server de-duplicates within the stream's duplicate window
    });
    return ack.seq; // ack.duplicate is true if this message was a duplicate
  }
}
```

`publish()` also accepts `expect` (optimistic concurrency, for example `{ lastSubjectSequence: 41 }`), `headers`, `codec` and `timeout`. `msgId` is sent as the `Nats-Msg-Id` header, so plugins can see it.

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

`NatsStreamManager` (inject it with `@InjectStreamManager()`) is available for runtime work: `ensureStream()`, `ensureConsumer()`, `info()`, `purge()`, `delete()`, and `jsm()` for the raw `JetStreamManager`.

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
| `NonRetryableError` | Throw it yourself to tell the library not to redeliver a message |

A failing request handler or service endpoint replies with the `Nats-Service-Error` and `Nats-Service-Error-Code` headers:

| Error | Code | Description |
|---|---|---|
| `HttpException` | its status | its message |
| `NatsDecodeError` | `400` | `Bad Request` |
| anything else | `500` | `Internal Server Error`. Internal details are never sent |

## Lifecycle and graceful shutdown

1. **Module init:** connect, failing fast if this does not work, then sync declared streams and consumers.
2. **Application bootstrap:** discover handlers, validate them, and start subscriptions, consumers and services. If any step fails, everything already started is stopped and all connections are closed before the error is rethrown.
3. **Shutdown:** stop taking new messages, wait for in-flight handlers for up to `shutdown.drainTimeout` (default `10000` ms), then drain and close every connection.

Call `app.enableShutdownHooks()` so this runs on `SIGTERM` and `SIGINT`.

Handlers are singletons. Handlers in `REQUEST`- or `TRANSIENT`-scoped providers are rejected at startup.

`NatsConnectionManager` exposes `status(name?)` (`connected`, `reconnecting`, `draining` or `closed`) and `status$(name?)`, an Observable of connection events such as disconnect, reconnect and lame-duck mode.

## Known limitations

These are tracked for upcoming releases:

- **Retries.** A failing JetStream message is nak'd without delay, so a message that always fails is redelivered in a tight loop. Until backoff lands, throw `NonRetryableError` for errors that will not go away, or use `ack: 'manual'` with `ctx.nak(delayMs)`.
- **Shutdown can hang.** `drainTimeout` bounds only the wait for in-flight handlers. Draining subscriptions and connections while the server is unreachable can wait for the client's reconnect policy.
- **Pulled messages at shutdown.** Messages that were already pulled by a JetStream consumer when shutdown starts stay unacknowledged until the consumer's `ack_wait` (30 s by default) runs out, and only then are they redelivered.
- **Duplicate error logs.** Handler errors that are not `HttpException`s are logged by Nest's `ExceptionsHandler` as well as by this library. `logger: false` silences only this library's logs.
- **Publish order.** If an outbound plugin awaits something, consecutive `publish()` calls are not guaranteed to leave in call order.
- **Undetected removals.** Stream and consumer sync compares only declared fields, so a field you remove from a declaration is not detected as drift.

## Roadmap

Planned, **not available yet**:

- Retry with exponential backoff and a dead-letter queue (DLQ)
- Idempotency (duplicate prevention with TTL, with a pluggable store)
- Batch processing for consumers (size and timeout) and batched JetStream publishing
- CloudEvents (binary and structured modes) with routing by `type`
- OpenTelemetry tracing from producer to consumer (`@loipv/nestjs-nats/otel`)
- Health checks for `@nestjs/terminus` (`@loipv/nestjs-nats/health`)
- Testing utilities (`@loipv/nestjs-nats/testing`)
- A fully bounded graceful shutdown

## License

[MIT](./LICENSE)
