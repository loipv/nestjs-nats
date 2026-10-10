# Tracing (OpenTelemetry)

[Back to the README](../README.md)

`@loipv/nestjs-nats/otel` provides a plugin that traces every publish, request and delivery, and propagates the trace context in the message headers, so a trace continues from the producer to the consumer. It needs the `@opentelemetry/api` peer dependency (`^1.9.0`):

```bash
npm install @opentelemetry/api
```

**Setting up OpenTelemetry is your job.** The plugin only uses the API. Register a tracer provider with an exporter, a context manager (for example `AsyncLocalStorageContextManager`) so spans stay linked across `await`, and the W3C Trace Context propagator before the application starts. `NodeSDK` from `@opentelemetry/sdk-node` does all three by default. Without a registered SDK the plugin does nothing: no spans, no headers added. The example below uses the Node SDK and the OTLP HTTP exporter:

```bash
npm install @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-http
```

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

## Spans

| Span | Kind | Created for |
|---|---|---|
| `publish <subject>` | `PRODUCER` | `NatsClient.publish()` and `JetStreamClient.publish()`, including every item of `publishMany()` and `batcher()`, and every message of `publishAtomic()` |
| `send <subject>` | `CLIENT` | `NatsClient.request()` |
| `process <subject>` | `CONSUMER` | A message (or batch) handled by a `@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered` or `@CloudEventRouter` that sends no reply |
| `process <subject>` | `SERVER` | A message that expects a reply: a `@ServiceEndpoint`, or a request to a `@Subscribe` handler or to a `@CloudEventRouter` on a core subject |

- **Propagation.** An outbound span's context is injected into the message headers (`traceparent`, `tracestate` and whatever else your propagator writes). A `process` span of a single message is a child of the context extracted from the message's headers, or starts a new trace if there is none. Without a `traceparent` header, the context comes from the [CloudEvents](./cloudevents.md) distributed tracing extension: the `ce-traceparent` and `ce-tracestate` headers of a binary event, else, for a structured event, the envelope's `traceparent` and `tracestate` attributes.
- **Publish parent.** A `publish` or `send` span is a child of the span active where you called `publish()`, `request()`, `publishMany()` or `batcher().add()`. A batcher publishes each message in the context of the `add()` call that collected it, whichever call or timer triggers the flush.
- **Batches.** A `process` span of a batch starts a new trace and has a link to the context of each message that carried one.
- **Span scope.** The `process` span covers the handler and the settlement (ack, retry, dead-lettering), and spans you create in the handler are its children.
- **Span end.** A `publish` or `send` span ends when the message was sent and its outcome (the ack, the reply or the error) is known. It can end just after `publish()` or `request()` has settled: they do not wait for the outbound plugins to return (see [Outbound timing](./plugins.md#outbound-timing)). If a plugin after the tracing plugin never passes the message on, the call rejects at its timeout, but the `publish` or `send` span never ends: the tracing plugin is still waiting for that plugin.
- **Atomic batches.** Each message of a `publishAtomic()` batch has its own `publish` span, and each span ends when the batch settles: with the commit's result once it is sent (or its error), or, for a batch aborted before sending (an outbound plugin failed, or staging outlasted the timeout), with the abort error. A span can end after `publishAtomic()` has already settled: once the batch is sent, `publishAtomic()` does not wait for the plugins to return. They carry `messaging.batch.message_count`.
- **Dead letters.** DLQ publishes do not go through plugins, so they have no `publish` span.

## Attributes

| Attribute | On | Value |
|---|---|---|
| `messaging.system` | all | `nats` |
| `messaging.destination.name` | all | The subject |
| `messaging.operation.type` | all | `send` for `publish` and `send` spans, `process` for `process` spans |
| `messaging.operation.name` | all | `publish`, `request` or `process` |
| `messaging.message.id` | `publish`, `send`, and single-message `process` | The `Nats-Msg-Id` header, else the CloudEvent `id`, when there is one |
| `messaging.message.body.size` | single-message `process` | Size of the body in bytes |
| `messaging.consumer.group.name` | `process` | The durable name of a `@Consume`, or the queue group of a `@Subscribe` that has one |
| `messaging.batch.message_count` | batch `process`, and `publish` of an atomic batch | Number of messages in the batch |
| `messaging.nats.stream`, `messaging.nats.seq`, `messaging.nats.delivery_count` | single-message `process` of a JetStream message | Stream, stream sequence and delivery count. An `@ConsumeOrdered` message has no delivery count (it is retried in place, never redelivered) |
| `messaging.header.<name>` | as set by `captureHeaders` | The header's value |

## Events and errors

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
