# Idempotency

[Back to the README](../README.md)

Idempotency stops a message from being processed twice: a redelivery after a failure that already succeeded, or a producer that published the same message twice. It is **off** by default. Enable it per handler:

```ts
import { Consume, Payload } from '@loipv/nestjs-nats';

class InvoiceHandlers {
  constructor(private readonly invoices: InvoiceService) {}

  @Consume({
    stream: 'ORDERS',
    durable: 'invoices',
    idempotency: {
      ttl: 600_000,
      // Used when the message has neither a `Nats-Msg-Id` header nor CloudEvent attributes.
      keyExtractor: (msg) => msg.headers?.get('x-order-id') || undefined,
    },
  })
  async invoice(@Payload() order: OrderDto) {
    await this.invoices.create(order);
  }
}
```

`idempotency: true` uses the defaults. On `NatsModule.forRoot()`, `idempotency: { store?, ttl? }` sets the default store and TTL of the handlers that enable it. It does not enable idempotency by itself.

| Option | Default | Description |
|---|---|---|
| `ttl` | module's, else `300000` | How long, in ms, a processed message is remembered. It must be a number above `0` |
| `keyExtractor` | none | `(msg: InboundMessage) => string \| undefined`. Called only if the message has neither a `Nats-Msg-Id` header nor CloudEvent attributes |

- **Key.** The key is the first of these that exists: the `Nats-Msg-Id` header, which `JetStreamClient.publish()` sets from `msgId` (or from the `id` of a [CloudEvent](./cloudevents.md)); for a CloudEvent, `<source>|<id>`, where `<source>` is percent-encoded with `encodeURIComponent` (so a `|` in it becomes `%7C` and cannot collide with the separator); what `keyExtractor` returns. A message without a key is processed as usual.
- **Scope.** Keys are scoped per handler, as `consume:<stream>/<durable>` (`@Consume` and `@ConsumePush`), `ordered:<Class>.<method>` or `subscribe:<Class>.<method>`, so handlers that share a store never swallow each other's messages. The connection name is not part of the scope, so several instances of an application agree on keys when they share a store.
- **Ordered consumers and shared stores.** Every instance receives every message of a `@ConsumeOrdered` handler, and its scope does not name the instance. With a store shared between instances, an instance skips each message another instance has already processed (or is processing: it is skipped too, and never comes back), so every instance processes only part of the stream. Keep the default in-memory store on the connection of such a handler, or give the [JetStream KV store](#jetstream-kv-store) an `instanceId`. A store of your own must keep the keys that start with `ordered:` apart per instance, for example by prefixing them with an instance id. A `keyExtractor` cannot do it: it is not called for messages that have a `Nats-Msg-Id` header or CloudEvent attributes. See [Consumers](./consumers.md#ordered-consumeordered).
- **Flow.** The key is claimed before the handler runs. If the handler succeeds, the key is marked as done for `ttl`. If it fails, the key is released, so the retry runs. If the handler outlived its [`timeout`](./handlers.md#handler-timeout), the key stays in progress until that run ends, so a redelivery does not run alongside it: it is then marked as done if the run succeeded, or released if it failed. A `@Subscribe` retry, or an ordered consumer, waits for that run instead of finding its key in progress. A JetStream redelivery is `nak`'d as in progress meanwhile, and each one counts towards the message's attempts and `max_deliver`, as described under "Crashes and failed releases" below.
- **Duplicates.** A message whose key is done is skipped, and acknowledged if it is a JetStream message. A message whose key is being processed by another delivery is skipped, and a JetStream message is `nak`'d, with the retry `initialDelay` (or 1000 ms when retries are off) as delay, so it comes back later.
- **No reply for duplicates.** A skipped `@Subscribe` request gets no reply, and its requester times out.
- **Store errors.** If the store fails to claim a key, the handler does not run and the message is retried after the retry backoff (the default one when retries are off), capped at `maxDelay`, and a warning is logged. This does not depend on `isRetryable` or on the retries left: a store outage delays messages but never dead-letters or drops them. A JetStream message is `nak`'d, and each redelivery increments its `deliveryCount`, so after a long outage it may have few or no retries left when it finally runs. A consumer's `max_deliver` still applies. A `@Subscribe` message is retried in the process, and a request gets an error reply. In a [batch](./batching.md#batch-processing), a store error on one key fails the whole batch: the keys it had claimed are released and every message is retried.
- **`ttl` must exceed the longest run of the handler.** A claim that expires while the handler still runs lets a redelivery start a second run. After the `ttl` has passed, a redelivered message is processed again.
- **Crashes and failed releases.** A claim stays in the store when the process crashes or is force-stopped while the handler runs, or when the release after a failure fails (a store outage, say). With a store that outlives the process, such as the [JetStream KV store](#jetstream-kv-store), that claim blocks the message on every instance until its `ttl` expires: each redelivery is skipped as in progress and `nak`'d, and spends one delivery (its `deliveryCount` grows towards the consumer's `max_deliver`). So choose a `ttl` of the longest run of the handler plus a margin, not as long as possible.

## Stores

The default store is an `InMemoryIdempotencyStore`: one per connection, shared by the handlers of that connection. It lives in the memory of one process, so it does not de-duplicate across instances and forgets everything on restart. It keeps up to `maxKeys` keys (default `100000`) and evicts the least recently used one. To share state across instances, use the [JetStream KV store](#jetstream-kv-store), or implement `IdempotencyStore` over a shared database and pass it as `idempotency.store`:

```ts
import {
  type ClaimResult,
  type IdempotencyStore,
  InMemoryIdempotencyStore,
  NatsModule,
} from '@loipv/nestjs-nats';
import { Redis } from 'ioredis';

class RedisIdempotencyStore implements IdempotencyStore {
  constructor(private readonly redis: Redis) {}

  async claim(key: string, ttlMs: number): Promise<ClaimResult> {
    if ((await this.redis.set(key, 'in-progress', 'PX', ttlMs, 'NX')) === 'OK') return 'claimed';
    return (await this.redis.get(key)) === 'done' ? 'duplicate' : 'in-progress';
  }

  async complete(key: string, ttlMs: number): Promise<void> {
    await this.redis.set(key, 'done', 'PX', ttlMs);
  }

  async release(key: string): Promise<void> {
    await this.redis.del(key);
  }
}

NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  idempotency: { store: new RedisIdempotencyStore(new Redis()), ttl: 600_000 },
});

// The in-memory store can also be created directly, for example to bound its size.
const store = new InMemoryIdempotencyStore({ maxKeys: 10_000 });
```

`claim` returns `'claimed'` when the caller should process the message, `'duplicate'` when it was already processed and `'in-progress'` when another delivery holds the key. A store may also implement `close()`. The library calls it at shutdown, for the stores it created and for the one given in the module options.

A store may also implement `bind({ connection, nc, js })`. The library calls it once at module init, before any message is handled, with the name, the `NatsConnection` and the `JetStreamClient` (with the connection's `jetstream.options`) of the connection whose `idempotency.store` it is. An error from `bind` fails startup. A store whose every bind in this application failed is not closed by the library (it may belong to another running application), so a bind that allocates resources and then fails must release them itself. The KV store below uses `bind` to open its bucket on that connection.

### JetStream KV store

`KvIdempotencyStore`, from `@loipv/nestjs-nats/kv`, keeps the keys in a JetStream KV bucket. The instances of an application connected to the same NATS system share them, and they survive restarts, with no infrastructure beyond NATS. It needs JetStream and the optional peer dependency `@nats-io/kv`:

```bash
npm i @nats-io/kv
```

```ts
import { NatsModule } from '@loipv/nestjs-nats';
import { KvIdempotencyStore } from '@loipv/nestjs-nats/kv';

NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  idempotency: {
    store: new KvIdempotencyStore({
      bucket: 'orders_idempotency',
      // Stable per instance (a pod name, say): only needed with @ConsumeOrdered handlers.
      instanceId: process.env.HOSTNAME,
    }),
    ttl: 600_000,
  },
});
```

| Option | Default | Description |
|---|---|---|
| `bucket` | `'nestjs_nats_idempotency'` | The KV bucket. At most 64 letters, digits, `-` and `_` |
| `maxAge` | `86400000` | How long, in ms, a bucket the store creates keeps an entry after its last write. An integer above `0` and at most `2147483647000`. It must exceed the `ttl` of every handler. An existing bucket keeps its own max age |
| `storage` | `'file'` | `'file'` or `'memory'`, for a bucket the store creates |
| `replicas` | `1` | Replicas of a bucket the store creates |
| `instanceId` | none | Keeps the keys of `@ConsumeOrdered` handlers apart per instance (see below). A non-empty string |
| `now` | `Date.now` | The clock, in ms. For tests |

Invalid options throw a `NatsConfigError` from the constructor.

- **Bucket.** At module init the store opens its bucket. If the bucket does not exist, the store creates it with `history: 1`, `ttl: maxAge`, `storage` and `replicas`. An existing bucket is used as it is: the store never changes its configuration, so `maxAge`, `storage` and `replicas` do not apply to it.
- **TTL.** Each entry records when it expires, `now() + ttl`, and an expired entry counts as missing: the next claim takes it over. Each instance compares that time with its own clock, so a clock skew between instances lengthens or shortens the `ttl` by as much.
- **`maxAge` versus `ttl`.** The bucket's max age only removes old entries from the server. It is not the `ttl` of a key. If it were shorter than a handler's `ttl`, a key could disappear before it expires and a duplicate would be processed. The store reads the max age of its bucket at module init and logs a warning, once, the first time a claim's `ttl` exceeds it.
- **Storage.** An entry stays in the bucket until the max age has passed since its last write, also once its `ttl` has expired, and so does the marker a `release` leaves. The bucket holds about one entry per message processed (or released) within the max age. Keep it small: a `maxAge` of the largest `ttl` plus a margin is enough.
- **Concurrent claims.** Claims are compare-and-set writes on the key's revision, and the store reads keys from the stream leader (not with direct gets, which a lagging replica may answer). When two instances claim the same key at the same moment, one gets it and the other sees it in progress. A claim that loses the race 3 times in a row fails, and is handled as a store error: the message is retried.
- **Keys.** A key is stored as its UTF-8 bytes in base64url, so any character is allowed. When that takes more than 256 characters, the key is stored as `h.` followed by the base64url SHA-256 of the key instead, so that the KV subject stays well within the server's control line (4096 bytes by default; a longer subject makes the server close the connection). A long `Nats-Msg-Id` therefore cannot break the connection.
- **Release.** `release` deletes the key, even if it expired meanwhile and another instance claimed it, as the in-memory store does. A `ttl` longer than the longest run of the handler avoids it.
- **Errors.** KV errors are passed on unchanged. A failed claim is handled like any store error (see **Store errors** above): the message is retried, never dead-lettered.
- **Ordered consumers.** Every instance must process every message of a `@ConsumeOrdered` handler. Set `instanceId` to a value of its own on each instance: the store then prefixes the keys that start with `ordered:` with it. Without `instanceId`, the store logs a warning the first time it sees such a key.
- **One store per connection.** The store binds to the connection of the `forRoot()` it is given to, until that application closes. Giving the same store object to a second connection, or to a second application while the first one runs, fails startup with a `NatsConfigError`. Create one store per connection and application instance, for example in the factory of `NatsModule.forRootAsync()` (see [Connections](./connections.md)), rather than in a constant shared by several applications of one process. Several stores, on several connections or in several applications, can share a bucket. Once its application has closed, a store can be bound again.
