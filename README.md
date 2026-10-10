# @loipv/nestjs-nats

Decorator-driven [NATS](https://nats.io) integration for [NestJS](https://nestjs.com): core subscriptions, JetStream consumers and NATS services as plain Nest providers, with pipes, guards, interceptors and exception filters working the way they do for HTTP controllers.

> **Warning:** 0.x, under active development, not production-ready yet.
> The API may change between minor versions until 1.0. See [Known limitations](#known-limitations).

## Features

- `@Subscribe()` for core NATS subjects, with wildcards, queue groups and request/reply. See [Handlers](./docs/handlers.md).
- `@Consume()` for JetStream pull consumers, with automatic or manual acknowledgement. See [Handlers](./docs/handlers.md).
- `@ConsumePush()` for durable JetStream push consumers that share messages through a `deliverGroup`, and `@ConsumeOrdered()` for ordered consumers that give every instance every message in stream order. See [Pull, push and ordered consumers](./docs/consumers.md).
- `@NatsService()` and `@ServiceEndpoint()` on top of the [NATS services framework](https://github.com/nats-io/nats.js/tree/main/services), so services are discoverable with `nats micro`. See [Handlers](./docs/handlers.md).
- Parameter decorators (`@Payload`, `@Headers`, `@Subject`, `@Msg`, `@Ctx`) that work with pipes, including a global `ValidationPipe`, and with guards, interceptors and exception filters. See [Parameters, pipes and Nest enhancers](./docs/handlers.md#parameters-pipes-and-nest-enhancers).
- `NatsClient` for fire-and-forget publish and request/reply, and `JetStreamClient` for publishing with acknowledgements and de-duplication. See [Publishing](./docs/publishing.md).
- Declarative streams and consumers, synced at startup with four modes from create-or-update to read-only verification. See [Streams and consumers](./docs/streams.md).
- Retry with exponential backoff and an optional dead-letter queue (DLQ), idempotency with a pluggable store, and batch processing for `@Subscribe`, `@Consume` and `@ConsumePush`. See [Retry and dead-letter queue](./docs/retry-and-dlq.md), [Idempotency](./docs/idempotency.md) and [Batch processing](./docs/batching.md#batch-processing).
- Batched JetStream publishing with `publishMany()` and an auto-flushing `batcher()`. See [Batch publishing](./docs/batching.md#batch-publishing).
- All-or-nothing publishing with `publishAtomic()`: several messages stored as one atomic batch. See [Atomic batches](./docs/publishing.md#atomic-batches).
- Several named connections in one application, each passing its options straight to the NATS client, so every NATS auth method and TLS/mTLS works unchanged. See [Connections](./docs/connections.md).
- [CloudEvents](./docs/cloudevents.md) 1.0 in binary and structured mode: publish with a `cloudEvent` option, route incoming events to methods by `type` with `@CloudEventRouter()` and `@OnCloudEvent()`, and read an event's attributes in any handler with `@CloudEvent()`.
- An inbound and outbound plugin pipeline for cross-cutting concerns, with a per-message outcome for plugins to observe. See [Plugins](./docs/plugins.md).
- [OpenTelemetry tracing](./docs/tracing.md) from producer to consumer, as a plugin (`@loipv/nestjs-nats/otel`).
- A [Terminus health indicator](./docs/health.md) (`@loipv/nestjs-nats/health`).
- [Testing utilities](./docs/testing.md) (`@loipv/nestjs-nats/testing`): mock clients and a harness that runs messages through your handlers without a NATS server.
- Startup fails fast on a bad connection, bad credentials or invalid handler configuration. Shutdown stops intake, waits for in-flight handlers and drains connections, all within a bounded time. See [Lifecycle and graceful shutdown](./docs/lifecycle.md) and [Errors](./docs/errors.md).

## Requirements

| | Version |
|---|---|
| Node.js | `>= 22.12` |
| NestJS | `11.x` or `12.x` |
| NATS client | `@nats-io/transport-node`, `@nats-io/jetstream`, `@nats-io/services` `3.x` |
| NATS server | JetStream enabled, needed only for the JetStream consumers and `JetStreamClient`. `publishAtomic()` needs `2.12` or later |
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

## Documentation

| Page | Covers |
|---|---|
| [Connections](./docs/connections.md) | Options and authentication, several named connections |
| [Handlers](./docs/handlers.md) | `@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered`, `@NatsService`, parameter decorators, pipes and Nest enhancers |
| [Consumers](./docs/consumers.md) | Pull, push and ordered consumers compared: load, acknowledgement, shutdown, limits |
| [Publishing](./docs/publishing.md) | `NatsClient`, `JetStreamClient`, atomic batches, codecs |
| [Streams and consumers](./docs/streams.md) | Declarative streams and consumers, sync modes |
| [Retry and dead-letter queue](./docs/retry-and-dlq.md) | Retry options, failure handling, DLQ |
| [Idempotency](./docs/idempotency.md) | Keys and stores |
| [Batching](./docs/batching.md) | Batch processing, `publishMany()` and `batcher()` |
| [CloudEvents](./docs/cloudevents.md) | Publishing and receiving CloudEvents |
| [Plugins](./docs/plugins.md) | Inbound and outbound plugins, observing outcomes |
| [Tracing (OpenTelemetry)](./docs/tracing.md) | Spans, attributes, events and errors |
| [Health checks](./docs/health.md) | Terminus health indicator |
| [Testing](./docs/testing.md) | Mock clients, `NatsTestingModule` and `NatsTestHarness` |
| [Errors](./docs/errors.md) | Error classes and reply codes |
| [Lifecycle and graceful shutdown](./docs/lifecycle.md) | Startup, shutdown phases, connection status |

## Known limitations

These are tracked for upcoming releases:

- **Core retries are in-process.** Retries of `@Subscribe` messages are timers inside the process, so pending retries are lost if the process dies or shuts down. Use `@Consume` when a message must survive that.
- **Retries can reorder messages**, even with `concurrency: 1`: a failed message is redelivered after the delay, behind messages that arrived meanwhile.
- **DLQ publishing bypasses outbound plugins.** Dead letters are published directly, without going through `plugins[].outbound`.
- **Core batches never reply.** A message with a reply subject that arrives in a `@Subscribe` batch is never answered, so its requester times out.
- **Pulled messages are not all heartbeated.** With `concurrency` above 1, or with `batch`, a few messages the NATS client has pulled but not yet handed over are not kept alive. See [Heartbeats](./docs/retry-and-dlq.md#what-happens-to-a-failed-message).
- **Messages at shutdown.** Messages the server delivers while shutdown is stopping a consumer, and messages a `@Consume` NATS client has pulled but not yet handed over when a handler never finishes, are redelivered only after the consumer's `ack_wait` (30 s by default). Messages the library holds (queued for a slot, collected or waiting with a batch) are `nak`'d at the forced stop. See [Lifecycle](./docs/lifecycle.md).
- **Ordered consumers can drop a message at shutdown.** An ordered consumer acknowledges nothing, so a message it has not finished when the application stops is not redelivered, unless the next start replays it through `deliverPolicy`. See [Consumers](./docs/consumers.md#ordered-consumeordered).
- **Push without `deliverGroup` binds one instance.** A `@ConsumePush` without a `deliverGroup` can be bound by only one instance at a time. Give every instance the same group to share the messages. See [Consumers](./docs/consumers.md#push-consumepush).
- **Router: subscribe and pull only.** `@CloudEventRouter` cannot receive from a push or an ordered consumer.
- **Duplicate error logs.** Handler errors that are not `HttpException`s are logged by Nest's `ExceptionsHandler` as well as by this library. `logger: false` silences only this library's logs.
- **Publish order.** If an outbound plugin awaits something, consecutive `publish()` calls are not guaranteed to leave in call order.
- **Undetected removals.** Stream and consumer sync compares only declared fields, so a field you remove from a declaration is not detected as drift. Declaring a field as `0` or `null` that the server stores differently (`duplicate_window: 0`, `max_deliver: 0`) is detected as drift on every boot: omit it instead. See [Streams](./docs/streams.md).
- **Ended loops are not restarted.** If a consumer loop or a service endpoint ends unexpectedly, it is logged as an error but not restarted, and the health indicator keeps reporting up. Restart the process, for example from a liveness probe of your own. See [Health checks](./docs/health.md).
- **Store errors fail a whole batch.** If the idempotency store cannot claim one key of a batch, every message of the batch is retried. See [Batching](./docs/batching.md).
- **Unmatched requests get no reply.** A request that reaches a `@CloudEventRouter` with `unmatched: 'ack'` is acknowledged without a reply, so its requester times out. See [CloudEvents](./docs/cloudevents.md#receiving-cloudevents).
- **Request subjects inside a stream.** A core request to a subject a stream captures usually gets the stream's PubAck, not the handler's reply. Keep request subjects outside every stream. See [Streams](./docs/streams.md).
- **Test harness.** `dispatch()` ignores options that do not apply to the handler kind, the default subject can be a wildcard, and concurrent `dispatch()` calls can see each other's dead letters. See [Testing](./docs/testing.md).

## Changelog and releases

See [CHANGELOG.md](./CHANGELOG.md) for what changed in each version. Maintainers: see [RELEASING.md](./RELEASING.md) for how releases are published.

## License

[MIT](./LICENSE)

