---
"@loipv/nestjs-nats": minor
---

New APIs:
- `@Consume({ pull: { prefetch, expires } })` tunes the pull requests of a consumer: `prefetch` (default `0`) asks for messages beyond the handler's slots, `expires` (at least 1000 ms) how long one waits. With a handler `timeout`, startup warns when the prefetched messages can wait longer than `ack_wait`.
- `NatsClient.requestMany()` sends one request and gathers every reply until `maxWait` (default 1000 ms), `maxMessages` or a `stall`. A reply carrying a service error, or that cannot be decoded, is in the result as its `error`; nobody answering gives `[]`.
- `onPublishError` in `forRoot`, and `onError` on a `NatsClient.publish()` call, receive the errors of a publish (closed connection, a plugin failing, the plugins not passing the message on in time) instead of the log.
