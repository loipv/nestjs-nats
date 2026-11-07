# CloudEvents

[Back to the README](../README.md)

The library reads and writes [CloudEvents](https://cloudevents.io) 1.0 following the [NATS protocol binding](https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/nats-protocol-binding.md), in both modes:

- **Binary mode.** Attributes travel as `ce-<name>` headers (written in lower case, read in any case), `datacontenttype` as the `Content-Type` header, and the body is the event's data, encoded by the codec.
- **Structured mode.** The body is a JSON envelope with `Content-Type: application/cloudevents+json`.

## Publishing CloudEvents

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
| `datacontenttype` | the codec's content type, or none for `Uint8Array` data | The media type of the data, for example `text/plain` or `application/octet-stream` |
| `extensions` | none | Extension attributes. A name must match `[a-z0-9]+` and must not be a standard attribute, or publishing throws a `NatsConfigError`. Values are strings |
| `mode` | `'binary'` | `'binary'` or `'structured'` |

`specversion` is always `'1.0'`. Without a `datacontenttype` option, `datacontenttype` is the content type of the codec (`application/json` by default), except when the data is a `Uint8Array`: such bytes are sent as they are, and the event has no `datacontenttype`. Set it when you send bytes, so receivers know what they are (see [Receiving CloudEvents](#receiving-cloudevents)). The option does not change how the data is encoded: pass non-JSON data as a `Uint8Array`. `datacontenttype` is fixed when the event is created, so an outbound plugin that replaces `message.codec` does not update it.

- **Errors.** An invalid `cloudEvent` throws a `NatsConfigError`: synchronously from `NatsClient.publish()`, as a rejection from `JetStreamClient.publish()`. `NatsClient.request()` has no `cloudEvent` option.
- **De-duplication.** `JetStreamClient.publish()` sets `Nats-Msg-Id` to the event's `id` unless you pass `msgId` or a `Nats-Msg-Id` header, so the stream's duplicate window de-duplicates events with the same `id`. `msgId` wins over the header.
- **Structured data.** With the JSON codec the data is the envelope's `data` member. With another codec, or when the data is a `Uint8Array`, the encoded bytes go into `data_base64`.
- **Outbound plugins** see the resolved event attributes (`id`, `time` and `source` filled in) as `message.cloudEvent.meta` of the `OutgoingMessage`, and the mode as `message.cloudEvent.mode`. The event is written to the wire after they have run.

## Receiving CloudEvents

A `@CloudEventRouter()` class receives CloudEvents from one source: a core subscription (`subscribe`) or a JetStream pull (`consume`), push (`consumePush`) or ordered (`consumeOrdered`) consumer. It hands each event to the `@OnCloudEvent(typePattern)` method whose pattern matches the event's `type` best. In those methods `@Payload()` is the event's `data` and `@CloudEvent()` is its attributes, a `CloudEventMeta`. Pipes, guards, interceptors and exception filters work as for any handler.

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
| `subscribe`, `queue` | Receive on a core subject (with an optional queue group). Set exactly one of `subscribe`, `consume`, `consumePush` or `consumeOrdered` |
| `consume` | `{ stream, durable, config? }`: receive from a JetStream pull consumer, as for `@Consume` |
| `consumePush` | `{ stream, durable, config?, deliverGroup? }`: receive from a JetStream push consumer, as for `@ConsumePush` |
| `consumeOrdered` | `{ stream, filterSubjects?, deliverPolicy?, headersOnly? }`: receive from an ordered consumer, as for `@ConsumeOrdered`. `concurrency` is not supported |
| `unmatched` | What to do with an event no `@OnCloudEvent` matches: `'ack'` (default), `'term'` or `'dlq'` |
| `disabled` | `true`, or a function returning a boolean, to skip the router and all its routes. See [Disabling a handler](./handlers.md#disabling-a-handler) |

```ts
import { Injectable } from '@nestjs/common';
import { CloudEventRouter, OnCloudEvent, Payload } from '@loipv/nestjs-nats';

@Injectable()
@CloudEventRouter({
  consumePush: { stream: 'EVENTS', durable: 'audit', deliverGroup: 'audit' },
})
class AuditPush {
  @OnCloudEvent('com.example.order.>')
  audit(@Payload() data: unknown) {}
}

@Injectable()
@CloudEventRouter({ consumeOrdered: { stream: 'EVENTS', filterSubjects: ['events.orders.>'] } })
class OrderTimeline {
  @OnCloudEvent('com.example.order.*')
  apply(@Payload() data: unknown) {}
}
```

- **Patterns.** A type is split on `.`. `*` matches exactly one segment, and a trailing `>` matches one or more. When several patterns match, an exact type wins over a pattern with `*`, which wins over one ending in `>`; among patterns of the same kind, the one with more literal segments wins, then the one declared first. An empty segment, a `>` that is not last or a repeated pattern fails startup with a `NatsConfigError`.
- **Data.** `@Payload()` is the event's data. Inline structured `data` (a JSON value) is handed over as it is. Otherwise (the body of a binary event, or the `data_base64` of a structured one) the data is decoded with the handler's codec, unless the event declares a `datacontenttype` that is neither JSON (`application/json`, `text/json` or a `+json` suffix, in any case, parameters ignored) nor the codec's content type: then `@Payload()` is the raw bytes, a `Uint8Array`. So with the default JSON codec, an event with `datacontenttype: text/plain` gets its text as bytes (decode it with `new TextDecoder().decode(data)`), and an event without `datacontenttype` is decoded as JSON. The codec gets the declared type as the `Content-Type` header.
- **Attributes first.** The router reads the attributes and picks the method before it decodes the event's data. An unmatched event follows `unmatched` even if its data cannot be decoded. If the data of a matched event cannot be decoded, that is a `NatsDecodeError`, handled like any decode failure.
- **Not a CloudEvent.** A message without the required attributes (`id`, `source`, `type`, `specversion`), or with a structured body that is not a JSON object, fails with a `NatsDecodeError`. It is not retried: it is dead-lettered if a DLQ is configured, else terminated (JetStream) or dropped (core), and a request gets a `400` reply.
- **Unmatched events.** `'ack'` acknowledges and skips the event (logged at debug level). `'term'` terminates a JetStream message, or drops a core one. `'dlq'` sends it to the [dead-letter queue](./retry-and-dlq.md#dead-letter-queue); if none is configured, it behaves like `'term'` and logs a warning. An unmatched event is never retried. A request (a core message with a reply subject) that is unmatched gets a `404` error reply with the message `No handler for CloudEvent type "<type>"`, whatever the `unmatched` policy. What `unmatched` does depends on the source:

  | `unmatched` | Push | Ordered |
  |---|---|---|
  | `'ack'` (default) | ack, logged at debug level | skipped, logged at debug level |
  | `'term'` | `term` | the message is dropped, as for core |
  | `'dlq'` | dead-lettered, then `term`; if publishing fails the message is nak'd with backoff, as for pull; without a DLQ it behaves like `'term'` and logs a warning | dead-lettered, retried in place if publishing fails; without a DLQ it is dropped with a warning |

  Push and ordered consumers carry no request, so there is no `404` reply. The default DLQ subject of an ordered router is `dlq.<stream>.ordered.<Class>.@CloudEventRouter`.
- **Ordered and `headersOnly`.** With `headersOnly: true` the message has no body, so only binary-mode events can be routed, and their data is empty: `@Payload()` is `undefined` with the JSON codec, and an empty `Uint8Array` when the event declares a non-JSON `datacontenttype`. A structured event always fails with a `NatsDecodeError` (its body is empty, so it is not valid JSON), whatever `unmatched` says (see "Not a CloudEvent" above). It is dead-lettered if a DLQ is configured, else dropped, and logged.
- **Rules.** A router has no `batch` or `ack` option: it handles one event at a time and settles it automatically. It needs at least one `@OnCloudEvent` method, and an `@OnCloudEvent` method cannot also be a `@Subscribe`, `@Consume` or `@ServiceEndpoint`. Breaking these rules fails startup with a `NatsConfigError`.
- **Idempotency.** With `idempotency` enabled, the key of an event is its `Nats-Msg-Id` header if it has one (the `id` of an event published with `JetStreamClient`), else `<source>|<id>` with `<source>` percent-encoded (`encodeURIComponent`). See [Idempotency](./idempotency.md).
- **Outside a router.** In any other handler `@CloudEvent()` and `ctx.cloudEvent` give the attributes of the message, or `undefined` when it is not a valid CloudEvent. In a batch handler `@CloudEvent()` is an array aligned with the payloads. `@Payload()` is not unwrapped there: for a structured event it is the whole envelope.

A router is one handler for the connection's plugins, retries and dead-letter queue. Its `HandlerInfo` has the class name and `methodName: '@CloudEventRouter'`.
