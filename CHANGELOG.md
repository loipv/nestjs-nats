# @loipv/nestjs-nats

## 0.3.0

### Minor Changes

- [`04f4e3d`](https://github.com/loipv/nestjs-nats/commit/04f4e3d253faaa98eee71fdcdb1e47eb0f85e545) Thanks [@loipv](https://github.com/loipv)! - Add handler timeouts: `timeout` on every handler decorator (`@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered`, `@ServiceEndpoint`, `@CloudEventRouter`) and `handlerTimeout`, the default of a connection's handlers. There is no timeout by default. A handler that outlives its timeout fails with the new `NatsHandlerTimeoutError` (retried, dead-lettered, or answered with a `504` reply), its slot is freed, and `ctx.signal`, a new `AbortSignal` on every context, is aborted. With idempotency, the key of a timed-out run stays in progress until that run ends, and shutdown waits for such runs within `drainTimeout`.

- [`9cd7178`](https://github.com/loipv/nestjs-nats/commit/9cd71787e98b0dd82785db3b6433e8ae5d49b4f8) Thanks [@loipv](https://github.com/loipv)! - `NatsKvModule.forFeature({ buckets })` and `@InjectKv(bucket)`, from `@loipv/nestjs-nats/kv`, declare JetStream KV buckets and inject nats.js's `KV`. Each bucket is created or checked at startup on the connection's JetStream client, following `jetstream.sync`: `create-or-update` creates a missing bucket and warns about the declared limits an existing one holds differently (the library does not update KV buckets), `create-only` creates it, `verify` fails startup when it is missing or differs, `none` opens it. `getKvToken(bucket, connection?)` is the token, for `overrideProvider` in tests.
  
  When a module provider fails at startup, the providers failing after it on the connections being closed no longer hide its error: startup now rejects with the first failure.

- [`2a9f833`](https://github.com/loipv/nestjs-nats/commit/2a9f8334ade48a27c71bfdaeb26683db7cacf7c7) Thanks [@loipv](https://github.com/loipv)! - New APIs:
  - `@Consume({ pull: { prefetch, expires } })` tunes the pull requests of a consumer: `prefetch` (default `0`) asks for messages beyond the handler's slots, `expires` (at least 1000 ms) how long one waits. With a handler `timeout`, startup warns when the prefetched messages can wait longer than `ack_wait`.
  - `NatsClient.requestMany()` sends one request and gathers every reply until `maxWait` (default 1000 ms), `maxMessages` or a `stall`. A reply carrying a service error, or that cannot be decoded, is in the result as its `error`; nobody answering gives `[]`.
  - `onPublishError` in `forRoot`, and `onError` on a `NatsClient.publish()` call, receive the errors of a publish (closed connection, a plugin failing, the plugins not passing the message on in time) instead of the log.

- [`ec22004`](https://github.com/loipv/nestjs-nats/commit/ec22004468bc84057726be60ce1cd4a919fc4780) Thanks [@loipv](https://github.com/loipv)! - Removes three experimental features:
  - `jetstream.removedFields: 'reset'`, and the `RemovedFieldsMode` type. A field removed from a stream or consumer declaration now always keeps its value on the server. A connection that still sets `removedFields` to anything but `'ignore'` fails startup with a `NatsConfigError`, before anything is synced; in TypeScript, `removedFields` no longer type-checks, even as `'ignore'`. Handlers are now discovered only at application bootstrap.
  - `batch` on `@Subscribe`. It is no longer part of `SubscribeOptions`, and a `@Subscribe` handler that still sets it fails startup with a `NatsConfigError`. `batch` on `@Consume` and `@ConsumePush` is unchanged.
  - `JetStreamClient.batcher()` and the `Batcher` type. Nothing checks for it at startup: JavaScript code that still calls `js.batcher()` gets a `TypeError` at the call. `publishMany()` is unchanged.
  
  The core subscription warning at forced stop now reads `Dropped <n> unstarted message(s) for <handler> at forced stop`, without `(collected or queued)`.
  
  ### Migrating from 0.2
  
  - **`removedFields`:** remove the option. To change a field you removed from a declaration, declare the value you want or edit the stream or consumer by hand (`nats stream edit`, `nats consumer edit`). Streams and consumers synced with `'reset'` keep a `nestjs-nats.declared` metadata key: it causes no drift, and you can delete it by hand.
  - **`@Subscribe({ batch })`:** move the handler to `@Consume({ stream, durable, batch })`, which also gives flow control and redelivery, or handle one message at a time.
  - **`js.batcher()`:** collect the messages yourself and call `publishMany()`, or `await Promise.all(items.map((i) => js.publish(i.subject, i.data)))` over a bounded set.

- [`f70ff70`](https://github.com/loipv/nestjs-nats/commit/f70ff70cf8863addd4a762d34bf93aad35488317) Thanks [@loipv](https://github.com/loipv)! - Security hardening:
  - `@Subscribe` takes a `slowConsumer` option (default `10000`, or `false`): when more messages than that wait for the handler, the connection logs `subscription "<subject>" is a slow consumer: <n> messages pending`, once each time the backlog crosses it. Core NATS cannot hold messages back, so this is a warning only; use `@Consume` for flow control.
  - `InMemoryIdempotencyStore` keeps a key longer than 256 characters as its SHA-256 hash, so publisher-controlled `Nats-Msg-Id` headers cannot grow its memory beyond `maxKeys` short keys.
  - Server errors, status stream failures and close failures are logged with the connection's password, token and URL credentials masked, like the other connection logs. A secret that contains another one is now masked whole.

- [`d591b5f`](https://github.com/loipv/nestjs-nats/commit/d591b5f54e1f93d1d9476b7980dc1c45d3eb4fcc) Thanks [@loipv](https://github.com/loipv)! - Handler decorator arguments can come from configuration: every argument of `@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered`, `@NatsService`, `@ServiceEndpoint` and `@CloudEventRouter` can be a function, called once at application startup with `{ get }`, which returns any provider of the application (such as `ConfigService`). A `disabled` function receives the same `{ get }`, and now runs once per application. A function that throws or returns a Promise, a subject or endpoint name that is not a non-empty string, or options that are not an object fail startup with a `NatsConfigError` naming the handler. The new `StartupValue` and `StartupContext` types are exported.

### Patch Changes

- [`4634883`](https://github.com/loipv/nestjs-nats/commit/4634883679a60b0a138c31b9addca791db445ddb) Thanks [@loipv](https://github.com/loipv)! - `NatsClient.request()` reports `no_responders` when the `NoRespondersError` is deeper in the error's cause chain, or comes from another copy of the NATS library, as dead-lettering already did; it used to report such a request as a plain error. Internal clean-up otherwise: handler discovery is split into smaller modules, and duplicated helpers are shared.

- [`734a7df`](https://github.com/loipv/nestjs-nats/commit/734a7df610dfa573e1a085746c9c7687c5508458) Thanks [@loipv](https://github.com/loipv)! - Mark features as experimental: they may change or be removed before 1.0, and get no new options until then. They are `publishMany()`, `@ConsumePush`, idempotency on `@ConsumeOrdered` (with the KV store's `instanceId`), and CloudEvents structured mode and wildcard type patterns. See Stability in the README. Nothing changes at runtime.

- [`3463696`](https://github.com/loipv/nestjs-nats/commit/3463696614d84de55ac412d2cff8c92af88e81f1) Thanks [@loipv](https://github.com/loipv)! - Handler timeout polish:
  - A timed-out run that rejects with an error caused by its abort, or with an axios `CanceledError`, is logged as stopped rather than failed.
  - `ctx.working()` does nothing once the message is settled, so a run that outlived its timeout cannot override the nak.
  - `NatsTestHarness.dispatch()` rejects an invalid module `handlerTimeout`, as bootstrap does.
  - The shutdown warning counts the timed-out runs it gave up waiting for.
  - Handlers without a timeout no longer pay for timeout bookkeeping.

- [`f9df5ad`](https://github.com/loipv/nestjs-nats/commit/f9df5ad4aa66ad3e4ded53e19d3dc01ebab1ad03) Thanks [@loipv](https://github.com/loipv)! - Performance:
  - `NatsClient.publish()` on a connection without outbound plugins publishes without creating promises, about 4 times faster (1.7 µs → 0.45 µs per message on a fake connection). A failure is logged at once instead of a microtask later, with the same message. With plugins, each publish allocates one promise fewer.
  - Batch idempotency claims the keys of a batch together, up to 16 at a time, instead of one after another: a batch of 100 messages on a `KvIdempotencyStore` waits for about 7 round-trips instead of 100. A key repeated within a batch is claimed once and its repeats are handled as in progress, as before. When a claim fails, the whole batch is retried without first acknowledging the duplicates found in it.

- [`c0c129f`](https://github.com/loipv/nestjs-nats/commit/c0c129f9706179489f12cd6cbd4d8b6765777f6a) Thanks [@loipv](https://github.com/loipv)! - Applying the same handler decorator twice to one method (two `@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered` or `@ServiceEndpoint`), or twice to one class (`@NatsService`, `@CloudEventRouter`), now throws a `NatsConfigError` when the class is defined. Before, one of them silently replaced the other, so its handler never ran. Decorators of different kinds on one method still declare one handler each.

## 0.2.0

### Minor Changes

- [`daa558a`](https://github.com/loipv/nestjs-nats/commit/daa558a481d159dee99c7111a9d20306bf5461ee) Thanks [@loipv](https://github.com/loipv)! - Add a `disabled` option to every handler decorator (`@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered`, `@ServiceEndpoint`, `@NatsService` and `@CloudEventRouter`) to skip a handler from configuration instead of commenting it out. It takes a boolean or a function called at application startup, after configuration is loaded. A disabled handler is not started or validated, and startup logs it.

## 0.1.0

Initial release.

- Core NATS handlers with `@Subscribe()` (wildcards, queue groups, request/reply) and JetStream pull consumers with `@Consume()`, plus NATS services with `@NatsService()` and `@ServiceEndpoint()`. Parameter decorators, pipes, guards, interceptors and exception filters work as they do for HTTP controllers; an error no filter handles is logged once, by the library, and not again by Nest's `ExceptionsHandler`.
- `NatsClient` for publish and request/reply, and `JetStreamClient` for publishing with acknowledgements and de-duplication. Batched publishing with `publishMany()` and `batcher()`, and all-or-nothing publishing with `publishAtomic()` (NATS server 2.12 or later, `allow_atomic` stream).
- Durable JetStream push consumers with `@ConsumePush()`, with a `deliverGroup` to share messages between instances.
- Ordered JetStream consumers with `@ConsumeOrdered()`: every instance receives every message in stream order, and a failed message is retried in place.
- Declarative streams and consumers, synced at startup with four modes: `create-or-update`, `create-only`, `verify` and `none`. A field declared as `null` is treated as left out. Opt-in `jetstream.removedFields: 'reset'`, for an application that is the only one declaring its streams and consumers, resets a field removed from a declaration to its default and reports it in `verify` (NATS server 2.10 or later), and merges the declarations of one stream or consumer within the application.
- CloudEvents 1.0 in binary and structured mode, with routing by event `type` through `@CloudEventRouter()` and `@OnCloudEvent()`, from a core subscription or a pull, push or ordered consumer. A request whose event no route matches gets a `404` reply, whatever the `unmatched` policy.
- Retry with exponential backoff and an optional dead-letter queue (DLQ) whose dead letters go through the outbound plugins, idempotency with a pluggable store (in memory by default, or `KvIdempotencyStore` from `@loipv/nestjs-nats/kv`, shared by every instance through a JetStream KV bucket), and batch processing for `@Subscribe`, `@Consume` and `@ConsumePush`. A request sent to a `@Subscribe` batch handler gets a `400` reply and is not handled.
- Several named connections per application, an inbound and outbound plugin pipeline, and OpenTelemetry tracing and metrics (`@loipv/nestjs-nats/otel`).
- Terminus health indicator (`@loipv/nestjs-nats/health`), which can also report handlers that gave up restarting (`handlers: true`), and testing utilities (`@loipv/nestjs-nats/testing`) that run messages through your handlers without a NATS server. `dispatch()` takes a `streamSequence` option (default `1`; the messages of a batch get consecutive sequences from it) for `ctx.seq`, `ctx.streamSequence` and `Nats-DLQ-Seq`, reports `retryDelay` for every `'retry'` settlement, and the mock `JetStreamClient` records `publishAtomic()` messages with their batch position.
- Handlers whose subscription, consumer or service ends unexpectedly are restarted with exponential backoff, per connection (`restart` option; `restart: false` turns it off, `maxAttempts` limits it). An ordered consumer resumes after the last message it handled, and a deleted pull consumer declared with `config` is created again.
- Fail-fast startup on bad connections or invalid handler configuration, and graceful shutdown that stops intake, waits for in-flight handlers and drains connections within a bounded time.
