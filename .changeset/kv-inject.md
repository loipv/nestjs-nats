---
"@loipv/nestjs-nats": minor
---

`NatsKvModule.forFeature({ buckets })` and `@InjectKv(bucket)`, from `@loipv/nestjs-nats/kv`, declare JetStream KV buckets and inject nats.js's `KV`. Each bucket is created or checked at startup on the connection's JetStream client, following `jetstream.sync`: `create-or-update` creates a missing bucket and warns about the declared limits an existing one holds differently (the library does not update KV buckets), `create-only` creates it, `verify` fails startup when it is missing or differs, `none` opens it. `getKvToken(bucket, connection?)` is the token, for `overrideProvider` in tests.
