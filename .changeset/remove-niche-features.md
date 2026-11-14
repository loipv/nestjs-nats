---
"@loipv/nestjs-nats": minor
---

Removes three experimental features:
- `jetstream.removedFields: 'reset'`, and the `RemovedFieldsMode` type. A field removed from a stream or consumer declaration now always keeps its value on the server. A connection that still sets `removedFields` to anything but `'ignore'` fails startup with a `NatsConfigError`, before anything is synced; in TypeScript, `removedFields` no longer type-checks, even as `'ignore'`. Handlers are now discovered only at application bootstrap.
- `batch` on `@Subscribe`. It is no longer part of `SubscribeOptions`, and a `@Subscribe` handler that still sets it fails startup with a `NatsConfigError`. `batch` on `@Consume` and `@ConsumePush` is unchanged.
- `JetStreamClient.batcher()` and the `Batcher` type. Nothing checks for it at startup: JavaScript code that still calls `js.batcher()` gets a `TypeError` at the call. `publishMany()` is unchanged.

The core subscription warning at forced stop now reads `Dropped <n> unstarted message(s) for <handler> at forced stop`, without `(collected or queued)`.

### Migrating from 0.2

- **`removedFields`:** remove the option. To change a field you removed from a declaration, declare the value you want or edit the stream or consumer by hand (`nats stream edit`, `nats consumer edit`). Streams and consumers synced with `'reset'` keep a `nestjs-nats.declared` metadata key: it causes no drift, and you can delete it by hand.
- **`@Subscribe({ batch })`:** move the handler to `@Consume({ stream, durable, batch })`, which also gives flow control and redelivery, or handle one message at a time.
- **`js.batcher()`:** collect the messages yourself and call `publishMany()`, or `await Promise.all(items.map((i) => js.publish(i.subject, i.data)))` over a bounded set.
