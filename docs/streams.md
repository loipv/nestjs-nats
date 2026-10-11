# Streams and consumers

[Back to the README](../README.md)

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
- **Only declared fields are compared.** A field left out of a declaration is not checked: with the default `jetstream.removedFields: 'ignore'`, a field you remove from a declaration keeps its value on the server, and `verify` does not notice it. With `'reset'`, it goes back to its default and `verify` reports it, see [Removed fields](#removed-fields). Values the server fills in are ignored: its own `_nats.*` metadata keys, and fields that are absent but declared as `false`, `0`, `''`, `[]` or `{}`. A declared `metadata` only needs its own keys on the server with `'ignore'`, and must match exactly with `'reset'`.
- **Omit a field to keep the server default.** The server stores some fields in a normalised form, so declaring them as `0` never matches what it reports: `duplicate_window: 0` is stored as 2 minutes and `max_deliver: 0` as `-1`. `create-or-update` then updates the stream or consumer on every boot, and `verify` fails. Leave such a field out, or declare the real value (`max_deliver: -1`). A field declared as `null` is treated as left out: it is neither sent to the server nor compared.
- **Duration units.** Durations such as `max_age` are in nanoseconds, as in the NATS API.

**Stream subjects and request/reply.** A stream captures every message published on its subjects, core NATS requests included, and answers a message that has a reply subject with a PubAck. A request sent with `NatsClient.request()` to a subject a stream captures therefore usually gets the stream's PubAck, not the `@Subscribe` handler's reply: both answer, and the first reply to arrive wins, normally the PubAck. Keep request/reply subjects outside every stream's subjects, as the [orders example](../examples/orders) does (`query.orders.get` next to the `orders.>` stream).

**Atomic batch publish.** A stream must declare `allow_atomic: true` for [`JetStreamClient.publishAtomic()`](./publishing.md#atomic-batches) to store a batch of more than one message. It needs NATS server 2.12 or later.

```ts
NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  streams: [{ name: 'ORDERS', subjects: ['orders.>', 'stock.>'], allow_atomic: true }],
});
```

`NatsStreamManager` (inject it by class, or with `@InjectStreamManager(name?)` for a named connection) is available for runtime work: `ensureStream()`, `ensureConsumer()`, `info()`, `purge()`, `delete()`, and `jsm()` for the raw `JetStreamManager`.

## Removed fields

By default (`jetstream.removedFields: 'ignore'`), a field you take out of a declaration keeps its value on the server. With `'reset'`, the library remembers the declared fields of each stream and consumer, so `create-or-update` resets a removed field and `verify` reports it. Any other value fails bootstrap with a `NatsConfigError`.

```ts
NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  jetstream: { removedFields: 'reset' },
  streams: [{ name: 'ORDERS', subjects: ['orders.>'] }],
});
```

**Only where a stream is declared once.** The server keeps one list of declared fields per stream or consumer. If another service declares the same stream or consumer with other fields, each one resets the other's fields on every boot: a stream could lose its `subjects`, or `verify` fail. Turn `'reset'` on only in the application that is the only one declaring its streams and consumers. Within one application, declarations are merged:

- **Streams.** The declarations of one stream on a connection (`forRoot` and every `forFeature`) are merged into one, the union of their fields, and `metadata` is merged by key. A field or a metadata key declared with different values fails bootstrap: `JetStream stream "<name>" is declared more than once with different <field>`.
- **Consumers in `consumers`.** Merged the same way, by stream and `durable_name`.
- **A durable declared both in `consumers` and by a handler's `config`** (`@Consume`, `@ConsumePush`, or a `@CloudEventRouter` with `consume` or `consumePush`) fails bootstrap, before any handler starts: `JetStream consumer "<stream>/<durable>" is declared both in the module options and by <Class>.<method>; declare it in one place`.

**How it works.** On every create or update the library stores the names of the declared top-level fields in the stream's or consumer's `metadata`, under the key `nestjs-nats.declared` (for example `max_age,subjects`). `name`, `durable_name` and `metadata` itself are not listed, and neither are fields the library fills in by default, such as a push consumer's `deliver_subject`.

- **`create-or-update`** resets a field that was declared on the previous run and is not declared now to the server default, and logs `reset <field> to the server default`. A push consumer field the library sets when it creates the consumer (`max_ack_pending`, `deliver_group`) goes back to that value instead, and logs `reset <field> to the library default`; it is left alone when the server already has that value.
- **Kept fields.** A field that cannot be changed, or that the server refuses to turn off, keeps its value when removed, and the library logs a warning, `keeps <field> <value>: it cannot be changed`:
  - streams: `storage`, `retention`, `deny_delete`, `deny_purge`, `allow_msg_ttl`, `sealed`, `allow_msg_counter`, `allow_msg_schedules`, and `mirror`, whose removal would promote the mirror for good;
  - consumers: `ack_policy`, `deliver_policy`, `opt_start_seq`, `opt_start_time`, `replay_policy`, `deliver_subject`, `flow_control`, `idle_heartbeat` and `max_waiting`.
- **`verify`** reports each removed field, kept ones included, as drift, as `<field> (removed from the declaration, current: <value>)`, and writes nothing.
- **`create-only`** writes the list only when it creates the stream or consumer, and never resets anything.
- **The first run resets no removed fields.** A stream or consumer that has no list yet, because it was created before `'reset'` was turned on or elsewhere, only gets its list written. Fields removed before that run are not detected.
- **Permissions.** That first run updates every declared stream and consumer, unchanged ones included, so the application needs permission to update them, not only to read them.
- **`metadata`.** A declared `metadata` is compared exactly, ignoring `_nats.*` keys and `nestjs-nats.declared`, and the list is added to it. Declaring a `_nats.*` key or `nestjs-nats.declared` fails with a `NatsConfigError`: the server would never report it back. Without a declared `metadata`, the keys already on the server are kept, so another tool's keys survive. Removing `metadata` from a declaration does not delete its keys.
- **Tools that reconcile metadata.** A tool that also manages the stream or consumer and sets its whole `metadata`, such as the NACK controller or Terraform, deletes `nestjs-nats.declared`. The next boot writes it again, so every boot makes one update.
- **NATS server 2.10 or later.** Older servers do not keep metadata. The library checks the version the server reported when the connection opened and, as a fallback, whether each create or update response kept the list. On an older server it warns once per connection, `removed fields are not detected`, writes no list and resets nothing on that connection. Declarations are still synced as with `'ignore'`.
