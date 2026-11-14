---
"@loipv/nestjs-nats": patch
---

Mark features as experimental: they may change or be removed before 1.0, and get no new options until then. They are `publishMany()`, `@ConsumePush`, idempotency on `@ConsumeOrdered` (with the KV store's `instanceId`), and CloudEvents structured mode and wildcard type patterns. See Stability in the README. Nothing changes at runtime.
