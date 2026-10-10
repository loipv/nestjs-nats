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
- **Ordered consumers and shared stores.** Every instance receives every message of a `@ConsumeOrdered` handler, and its scope does not name the instance. With a store shared between instances, an instance skips each message another instance has already processed (or is processing: it is skipped too, and never comes back), so every instance processes only part of the stream. Keep the default in-memory store on the connection of such a handler, or have the shared store keep the keys that start with `ordered:` apart per instance, for example by prefixing them with an instance id. A `keyExtractor` cannot do it: it is not called for messages that have a `Nats-Msg-Id` header or CloudEvent attributes. See [Consumers](./consumers.md#ordered-consumeordered).
- **Flow.** The key is claimed before the handler runs. If the handler succeeds, the key is marked as done for `ttl`. If it fails, the key is released, so the retry runs.
- **Duplicates.** A message whose key is done is skipped, and acknowledged if it is a JetStream message. A message whose key is being processed by another delivery is skipped, and a JetStream message is `nak`'d, with the retry `initialDelay` (or 1000 ms when retries are off) as delay, so it comes back later.
- **No reply for duplicates.** A skipped `@Subscribe` request gets no reply, and its requester times out.
- **Store errors.** If the store fails to claim a key, the handler does not run and the message is retried after the retry backoff (the default one when retries are off), capped at `maxDelay`, and a warning is logged. This does not depend on `isRetryable` or on the retries left: a store outage delays messages but never dead-letters or drops them. A JetStream message is `nak`'d, and each redelivery increments its `deliveryCount`, so after a long outage it may have few or no retries left when it finally runs. A consumer's `max_deliver` still applies. A `@Subscribe` message is retried in the process, and a request gets an error reply. In a [batch](./batching.md#batch-processing), a store error on one key fails the whole batch: the keys it had claimed are released and every message is retried.
- **`ttl` must exceed the longest run of the handler.** A claim that expires while the handler still runs lets a redelivery start a second run. After the `ttl` has passed, a redelivered message is processed again.

## Stores

The default store is an `InMemoryIdempotencyStore`: one per connection, shared by the handlers of that connection. It lives in the memory of one process, so it does not de-duplicate across instances and forgets everything on restart. It keeps up to `maxKeys` keys (default `100000`) and evicts the least recently used one. To share state across instances, implement `IdempotencyStore` over a shared database and pass it as `idempotency.store`:

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
