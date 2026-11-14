---
"@loipv/nestjs-nats": minor
---

Security hardening:
- `@Subscribe` takes a `slowConsumer` option (default `10000`, or `false`): when more messages than that wait for the handler, the connection logs `subscription "<subject>" is a slow consumer: <n> messages pending`, once each time the backlog crosses it. Core NATS cannot hold messages back, so this is a warning only; use `@Consume` for flow control.
- `InMemoryIdempotencyStore` keeps a key longer than 256 characters as its SHA-256 hash, so publisher-controlled `Nats-Msg-Id` headers cannot grow its memory beyond `maxKeys` short keys.
- Server errors, status stream failures and close failures are logged with the connection's password, token and URL credentials masked, like the other connection logs. A secret that contains another one is now masked whole.
