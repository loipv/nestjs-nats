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
- **Only declared fields are compared.** A field left out of a declaration is not checked: a field you remove from a declaration keeps its value on the server, and `verify` does not notice it, see [Removed fields](#removed-fields). Values the server fills in are ignored: its own `_nats.*` metadata keys, and fields that are absent but declared as `false`, `0`, `''`, `[]` or `{}`. A declared `metadata` only needs its own keys on the server.
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

A field you take out of a declaration keeps its value on the server: the library never knows which fields an earlier version of the declaration had, and `verify` does not report it. To change such a field, declare the value you want, or edit the stream or consumer by hand (`nats stream edit`, `nats consumer edit`).

**Upgrading from 0.2.** Version 0.3.0 removed `jetstream.removedFields: 'reset'`. A connection that still sets it, or any value other than `'ignore'`, fails startup with a `NatsConfigError` before anything is synced: remove the option. Streams and consumers synced with `'reset'` keep a `nestjs-nats.declared` metadata key. It is harmless, causes no drift, and is dropped by the next update of a declaration that has its own `metadata`; you can also delete it by hand.
