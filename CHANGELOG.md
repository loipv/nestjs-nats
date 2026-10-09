# @loipv/nestjs-nats

## 0.1.0

Initial release.

- Core NATS handlers with `@Subscribe()` (wildcards, queue groups, request/reply) and JetStream pull consumers with `@Consume()`, plus NATS services with `@NatsService()` and `@ServiceEndpoint()`. Parameter decorators, pipes, guards, interceptors and exception filters work as they do for HTTP controllers.
- `NatsClient` for publish and request/reply, and `JetStreamClient` for publishing with acknowledgements and de-duplication. Batched publishing with `publishMany()` and `batcher()`.
- Declarative streams and consumers, synced at startup with four modes: `create-or-update`, `create-only`, `verify` and `none`.
- CloudEvents 1.0 in binary and structured mode, with routing by event `type` through `@CloudEventRouter()` and `@OnCloudEvent()`.
- Retry with exponential backoff and an optional dead-letter queue (DLQ), idempotency with a pluggable store, and batch processing for `@Subscribe` and `@Consume`.
- Several named connections per application, an inbound and outbound plugin pipeline, and OpenTelemetry tracing (`@loipv/nestjs-nats/otel`).
- Terminus health indicator (`@loipv/nestjs-nats/health`) and testing utilities (`@loipv/nestjs-nats/testing`) that run messages through your handlers without a NATS server.
- Fail-fast startup on bad connections or invalid handler configuration, and graceful shutdown that stops intake, waits for in-flight handlers and drains connections within a bounded time.
