# KV buckets

[Back to the README](../README.md)

`NatsKvModule`, from `@loipv/nestjs-nats/kv`, declares JetStream KV buckets: each is created or checked at startup, on the connection's JetStream client (with its `domain`, `apiPrefix` and `timeout`), and injected with `@InjectKv`. It needs the optional peer dependency `@nats-io/kv`.

```ts
import type { KV } from '@nats-io/kv';
import { Injectable, Module } from '@nestjs/common';
import { InjectKv, NatsKvModule } from '@loipv/nestjs-nats/kv';

@Injectable()
class Sessions {
  constructor(@InjectKv('sessions') private readonly kv: KV) {}

  async touch(id: string): Promise<void> {
    await this.kv.put(id, new Date().toISOString());
  }
}

@Module({
  imports: [NatsKvModule.forFeature({ buckets: [{ bucket: 'sessions', ttl: 3_600_000, history: 1 }] })],
  providers: [Sessions],
})
class SessionsModule {}
```

`forFeature({ connection?, buckets })` takes the connection name (default `'default'`) and the buckets, each `{ bucket, ...limits }` with the limits of nats.js's `KvLimits` (`history`, `ttl` in ms, `max_bytes`, `maxValueSize`, `replicas`, `storage`, ...). The injected value is nats.js's own `KV`: `get`, `put`, `create`, `update`, `delete`, `watch`, `keys`, `history` and the rest. Values are bytes or strings, as in nats.js. Import `NatsKvModule.forFeature` in each module that injects a bucket; inject a bucket of another connection with `@InjectKv(bucket, connection)`.

`forFeature` throws a `NatsConfigError` at once when it has no bucket, a bucket name other than letters, digits, `-` and `_`, or the same bucket twice.

## At startup

What happens to each bucket follows the connection's [`jetstream.sync`](./streams.md):

| `sync` | Bucket missing | Bucket exists |
|---|---|---|
| `create-or-update` (default) | Created with the declared limits | Used as it is. Each declared limit it holds differently is logged as a warning: the library does not update KV buckets, change it with `nats kv` or recreate it |
| `create-only` | Created | Used as it is, nothing compared |
| `verify` | Startup fails: `KV bucket "<bucket>" on connection "<name>" does not exist` | Startup fails when a declared limit differs, listing each one |
| `none` | Not created: the first access fails | Opened, nothing compared |

The limits compared are `history`, `ttl`, `max_bytes`, `maxValueSize`, `replicas` and `storage`, and only those the declaration sets. A `history`, `max_bytes` or `maxValueSize` of `0` means unlimited, as the server stores it (`-1`), and a limit set to `undefined` is left out, so it keeps the nats.js default. A startup failure closes the connections already open, as any other failure of the module does.

## Testing

`NatsTestingModule` has no KV. Replace a bucket with your own fake:

```ts
import { getKvToken } from '@loipv/nestjs-nats/kv';
import { Test } from '@nestjs/testing';

declare const SessionsModule: new () => unknown;
declare const fakeKv: object;

const moduleRef = await Test.createTestingModule({ imports: [SessionsModule] })
  .overrideProvider(getKvToken('sessions'))
  .useValue(fakeKv)
  .compile();
```

`getKvToken(bucket, connection?)` is the token of a bucket. The KV-backed idempotency store is described in [Idempotency](./idempotency.md#jetstream-kv-store).
