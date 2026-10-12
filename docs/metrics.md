# Metrics (OpenTelemetry)

[Back to the README](../README.md)

`@loipv/nestjs-nats/otel` provides a plugin that measures every delivery and every message the connection sends: handler duration, throughput, outcomes (retry, dead letter, duplicate), and errors. It uses the OpenTelemetry messaging metric names and needs the `@opentelemetry/api` peer dependency (`^1.9.0`):

```bash
npm install @opentelemetry/api
```

**Setting up OpenTelemetry is your job.** The plugin only uses the API. Register a meter provider with a metric reader before the application starts. `NodeSDK` from `@opentelemetry/sdk-node` does it when you give it a `metricReader`. Without a registered SDK the plugin does nothing. The example below uses the Node SDK and the OTLP HTTP exporter, next to the [tracing](./tracing.md) plugin:

```bash
npm install @opentelemetry/sdk-node @opentelemetry/sdk-metrics @opentelemetry/exporter-metrics-otlp-http
```

```ts
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { NatsModule } from '@loipv/nestjs-nats';
import { metrics, tracing } from '@loipv/nestjs-nats/otel';

// Start the SDK before the application is created.
const sdk = new NodeSDK({
  serviceName: 'orders-service',
  metricReader: new PeriodicExportingMetricReader({ exporter: new OTLPMetricExporter() }),
});
sdk.start();

NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  plugins: [tracing(), metrics()],
});
```

`metrics()` and `tracing()` are independent: use either one or both.

| Option | Default | Description |
|---|---|---|
| `meterName` | `'@loipv/nestjs-nats'` | Name of the meter the instruments are created with |
| `subjects` | `false` | Adds the `messaging.destination.name` attribute. `true` records the subject as it is. A function records what it returns for the subject, and nothing when it returns `undefined` |

**Every distinct value of an attribute is a new series.** Subjects often hold ids (`orders.123`), so they are off by default. If you turn them on, map them to a bounded set:

```ts
import { NatsModule } from '@loipv/nestjs-nats';
import { metrics } from '@loipv/nestjs-nats/otel';

NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  // orders.123 -> orders.*
  plugins: [metrics({ subjects: (subject) => subject.replace(/^orders\..+$/, 'orders.*') })],
});
```

If the function throws, the attribute is left out for that record and the error is reported once per plugin instance through the OpenTelemetry `diag` logger.

## Metrics

Histograms have the unit `s` and the buckets `0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10`. Counters have the unit `{message}`.

| Metric | Type | Recorded |
|---|---|---|
| `messaging.process.duration` | Histogram | Once per delivery (a batch is one delivery): the time from entering the plugin until the handler and the settlement (ack, retry, dead-lettering) are done |
| `messaging.client.consumed.messages` | Counter | Once per message of a delivery, with its outcome |
| `messaging.client.sent.messages` | Counter | Once per message sent (publish, request, JetStream publish, dead letter) |
| `messaging.client.operation.duration` | Histogram | With each sent message: the time from entering the plugin until the message was sent and its outcome (the ack, the reply or the error) is known |

## Attributes

| Attribute | On | Value |
|---|---|---|
| `messaging.system` | all | `nats` |
| `messaging.operation.name` | all | `process` for inbound; `request` or `publish` for sent messages |
| `nats.connection` | all | The connection name |
| `nats.handler` | inbound | `<ClassName>.<methodName>` |
| `nats.handler.kind` | inbound | `subscribe`, `consume`, `ordered` or `service-endpoint` |
| `messaging.consumer.group.name` | inbound | The durable name of a `@Consume`, or the queue group of a `@Subscribe` that has one |
| `nats.stream` | inbound, `consume` and `ordered` | The stream |
| `nats.outcome` | `messaging.client.consumed.messages` | The `action` of the message's [outcome](./plugins.md#observing-outcomes): `ack`, `retry`, `dlq`, `term`, `drop`, `skipped`, `replied` or `none`. `unsettled` when the library did not settle the message |
| `nats.publish.kind` | sent | `core` for `NatsClient.publish()`, `jetstream` or `request` |
| `nats.dead_letter` | sent | `true` for a dead letter. Absent on other messages |
| `error.type` | duration histograms; `messaging.client.consumed.messages` when the outcome has an error | The `name` of the error (`Error` subclasses), `_OTHER` for anything else, such as a thrown string |
| `messaging.destination.name` | all, only with `subjects` | The subject, or what your function returns. The inbound histogram takes the subject of the first message of the delivery |

## Counting

- Every attempt is counted once. A retry (a JetStream redelivery, an in-process retry of a core message, an in-place retry of an ordered consumer) is a new delivery.
- A message skipped by [idempotency](./idempotency.md) is counted with `nats.outcome` `skipped`.
- A dead letter is counted on the sending side, with `nats.dead_letter` `true`, as well as with outcome `dlq` on the receiving side.
- Each message of a `publishAtomic()` batch is counted on its own, and its duration lasts until the batch settles.
- A plugin after `metrics()` that never passes a message on: nothing is recorded for it, because the plugin is still waiting. The same holds for the `tracing()` spans.
- The plugin never swallows or adds errors: a failure is recorded, then rethrown as it was.

## Created before the SDK

The usual order is to create the module, which creates the plugin, and to start the SDK around it. The plugin does not keep a meter from creation time: on each record it checks the registered meter provider and creates its instruments again when the provider changed. Records made after `sdk.start()` therefore reach the provider, however early `metrics()` was called.
