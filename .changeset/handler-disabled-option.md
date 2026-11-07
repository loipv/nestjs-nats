---
"@loipv/nestjs-nats": minor
---

Add a `disabled` option to every handler decorator (`@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered`, `@ServiceEndpoint`, `@NatsService` and `@CloudEventRouter`) to skip a handler from configuration instead of commenting it out. It takes a boolean or a function called at application startup, after configuration is loaded. A disabled handler is not started or validated, and startup logs it.
