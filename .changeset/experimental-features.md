---
"@loipv/nestjs-nats": patch
---

Mark features as experimental: they may change or be removed before 1.0, and get no new options until then. They are `jetstream.removedFields: 'reset'`, `publishMany()` and `batcher()`, `@ConsumePush`, `batch` on `@Subscribe`, idempotency on `@ConsumeOrdered` (with the KV store's `instanceId`), and CloudEvents structured mode and wildcard type patterns. See Stability in the README. Nothing changes at runtime.
