---
"@loipv/nestjs-nats": minor
---

Handler decorator arguments can come from configuration: every argument of `@Subscribe`, `@Consume`, `@ConsumePush`, `@ConsumeOrdered`, `@NatsService`, `@ServiceEndpoint` and `@CloudEventRouter` can be a function, called once at application startup with `{ get }`, which returns any provider of the application (such as `ConfigService`). A `disabled` function receives the same `{ get }`, and now runs once per application. A function that throws or returns a Promise, a subject or endpoint name that is not a non-empty string, or options that are not an object fail startup with a `NatsConfigError` naming the handler. The new `StartupValue` and `StartupContext` types are exported.
