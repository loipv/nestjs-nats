# @loipv/nestjs-nats

## 0.1.0

Initial release.

- Core NATS handlers with `@Subscribe()` (wildcards, queue groups, request/reply) and JetStream pull consumers with `@Consume()`, plus NATS services with `@NatsService()` and `@ServiceEndpoint()`. Parameter decorators, pipes, guards, interceptors and exception filters work as they do for HTTP controllers; an error no filter handles is logged once, by the library, and not again by Nest's `ExceptionsHandler`.
- `NatsClient` for publish and request/reply, and `JetStreamClient` for publishing with acknowledgements and de-duplication. Batched publishing with `publishMany()` and `batcher()`, and all-or-nothing publishing with `publishAtomic()` (NATS server 2.12 or later, `allow_atomic` stream).
- Durable JetStream push consumers with `@ConsumePush()`, with a `deliverGroup` to share messages between instances.
- Ordered JetStream consumers with `@ConsumeOrdered()`: every instance receives every message in stream order, and a failed message is retried in place.
- Declarative streams and consumers, synced at startup with four modes: `create-or-update`, `create-only`, `verify` and `none`. A field declared as `null` is treated as left out.
- CloudEvents 1.0 in binary and structured mode, with routing by event `type` through `@CloudEventRouter()` and `@OnCloudEvent()`, from a core subscription or a pull, push or ordered consumer. A request whose event no route matches gets a `404` reply, whatever the `unmatched` policy.
- Retry with exponential backoff and an optional dead-letter queue (DLQ) whose dead letters go through the outbound plugins, idempotency with a pluggable store, and batch processing for `@Subscribe`, `@Consume` and `@ConsumePush`. A request sent to a `@Subscribe` batch handler gets a `400` reply and is not handled.
- Several named connections per application, an inbound and outbound plugin pipeline, and OpenTelemetry tracing (`@loipv/nestjs-nats/otel`).
- Terminus health indicator (`@loipv/nestjs-nats/health`), which can also report handlers that gave up restarting (`handlers: true`), and testing utilities (`@loipv/nestjs-nats/testing`) that run messages through your handlers without a NATS server. `dispatch()` takes a `streamSequence` option (default `1`; the messages of a batch get consecutive sequences from it) for `ctx.seq`, `ctx.streamSequence` and `Nats-DLQ-Seq`, reports `retryDelay` for every `'retry'` settlement, and the mock `JetStreamClient` records `publishAtomic()` messages with their batch position.
- Handlers whose subscription, consumer or service ends unexpectedly are restarted with exponential backoff, per connection (`restart` option; `restart: false` turns it off, `maxAttempts` limits it). An ordered consumer resumes after the last message it handled, and a deleted pull consumer declared with `config` is created again.
- Fail-fast startup on bad connections or invalid handler configuration, and graceful shutdown that stops intake, waits for in-flight handlers and drains connections within a bounded time.
