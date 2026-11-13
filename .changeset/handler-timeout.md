---
"@loipv/nestjs-nats": minor
---

Add handler timeouts: `timeout` on every handler decorator (`@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered`, `@ServiceEndpoint`, `@CloudEventRouter`) and `handlerTimeout`, the default of a connection's handlers. There is no timeout by default. A handler that outlives its timeout fails with the new `NatsHandlerTimeoutError` (retried, dead-lettered, or answered with a `504` reply), its slot is freed, and `ctx.signal`, a new `AbortSignal` on every context, is aborted. With idempotency, the key of a timed-out run stays in progress until that run ends, and shutdown waits for such runs within `drainTimeout`.
