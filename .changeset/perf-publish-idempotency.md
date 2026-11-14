---
"@loipv/nestjs-nats": patch
---

Performance:
- `NatsClient.publish()` on a connection without outbound plugins publishes without creating promises, about 4 times faster (1.7 µs → 0.45 µs per message on a fake connection). A failure is logged at once instead of a microtask later, with the same message. With plugins, each publish allocates one promise fewer.
- Batch idempotency claims the keys of a batch together, up to 16 at a time, instead of one after another: a batch of 100 messages on a `KvIdempotencyStore` waits for about 7 round-trips instead of 100. A key repeated within a batch is claimed once and its repeats are handled as in progress, as before. When a claim fails, the whole batch is retried without first acknowledging the duplicates found in it.
