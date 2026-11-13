---
"@loipv/nestjs-nats": patch
---

Applying the same handler decorator twice to one method (two `@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered` or `@ServiceEndpoint`), or twice to one class (`@NatsService`, `@CloudEventRouter`), now throws a `NatsConfigError` when the class is defined. Before, one of them silently replaced the other, so its handler never ran. Decorators of different kinds on one method still declare one handler each.
