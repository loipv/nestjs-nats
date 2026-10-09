# Plan 2 (Reliability) — follow-ups

Collected from per-task and final reviews of feat/reliability (222a3af..ac20e63).

## Rulings made during execution
- Ruling: DLQ publish must strip original JetStream publish-directive headers (`Nats-Expected-*`, `Nats-Rollup`, `Nats-TTL`) before publishing to the DLQ (keep `Nats-Msg-Id` so a re-DLQ after a failed term is deduped) — copying them would make DLQ publishes fail (expected-stream mismatch) or roll up the DLQ stream; carried into Task 3 — cost if wrong: DLQ message lacks those original headers.
- Ruling: core adapter Dispatcher gets an onRejected that logs a warn (message dropped because the subscription is stopping) — a core retry timer firing after stop would otherwise drop the message silently; carried into Task 3 — cost if wrong: one extra log line.
- Task 3: Ruling: heartbeat must keep running until settlement (incl. awaited DLQ publish) completes, not stop when next() finishes — plan text said stop after next(); a slow PubAck with short ack_wait could redeliver and double-DLQ; carried into Task 5 (which modifies settlement) — cost if wrong: none.
- Task 3: Ruling: outer-plugin failure outside settlement must nak with backoffDelay(retry || DEFAULT_RETRY, attempt) instead of immediate nak, and isNoResponders gets a name-based fallback (`e.name === 'NoRespondersError'`) — both small, carried into Task 5 — cost if wrong: none.
- Task 4: Ruling: scope stored idempotency keys per handler — prefix claim/complete/release keys with a stable identity: consume → 'consume:<stream>/<durable>|', subscribe → 'subscribe:<Class>.<method>|' (connection name excluded so replicas sharing a store still dedupe); idempotencyKey() order unchanged — spec §10 silent on namespacing, unscoped keys silently drop messages for a second idempotent handler — cost if wrong: renaming a handler class/method resets its dedupe window.
- Ruling: reverse Task 1 DLQ header ruling — dead letters get Nats-Msg-Id `dlq:<stream>:<consumer>:<streamSeq>` (JetStream) / none (core); original id kept as `Nats-DLQ-Msg-Id`; strip all original `Nats-*` headers — copied ids collide across consumers in a shared DLQ stream and new 2.12 directives break plain-stream publishes — cost if wrong: DLQ consumers lose original Nats-* headers (except the preserved id).
- Ruling: replace per-delivery heartbeat with an adapter-level heartbeat over every received-but-unsettled JetStream message, and cut prefetch to `batch ? max(maxSize, concurrency) : concurrency` — waiting messages otherwise pass ack_wait and are processed twice — cost if wrong: lower prefetch throughput.
- Ruling: idempotency store failures retry outside the retry/DLQ decision (internal IdempotencyStoreError, backoff, no attempt cap, isRetryable bypassed) — spec Review Focus 5 "retried, not lost" — cost if wrong: messages delayed indefinitely during a permanent store outage (logged each time).
- Ruling: Batcher becomes a type-only export (constructor typed on internals) — cost if wrong: users can't instanceof-check Batcher.
- Ruling: fix minors M1, M2, M4, M5, M9 (README) in the same wave; keep the rest deferred as triaged — cost if wrong: slightly larger fix diff.

## Open decision
- I1 residual: with `concurrency > 1` or `batch`, messages already pulled into the nats.js buffer but not yet handed to the library cannot be heartbeated; if a handler runs longer than `ack_wait` they may be redelivered and processed twice. Options: accept + document (current), `max_messages: 1` (≈4–35× lower raw throughput in a local probe), or rebuild intake on `fetch()`.

## Deferred findings
- Task 1: minor (deferred): explicit undefined option fields clobber defaults in resolveRetryPolicy (validator catches with confusing msg)
- Task 1: minor (deferred): backoff NaN when initialDelay 0 and huge attempt
- Task 1: minor (deferred): slice(0,1024) counts UTF-16 units; String(plain object) → [object Object]
- Task 1: minor (deferred): test gaps — multi-value header copy, CR/LF+truncate order, module isRetryable merge
- Task 1: minor (deferred): module-level retry only validated via handlers
- Task 2: minor (deferred): inline errorMessage copies remain in dlq.ts, connection-manager.ts, nats-client.ts
- Task 2: minor (deferred → Task 3 ruling covers): Dispatcher.reject silent without onRejected
- Task 2: minor (deferred): hung handler left in forceStop unit test
- Task 2: minor (deferred, doc): cancelRetries counts timers, not fired-but-queued pushes
- Task 3: minor (deferred): heartbeat adapter unit test uses real timers (CI drift); consume.int test name overpromises backoff
- Task 3: minor (deferred): Nats-Schedule*/Nats-Batch-* headers still copied to DLQ
- Task 4: minor (deferred, docs): release has no claim ownership — ttl must exceed longest handler run (README note)
- Task 4: minor (deferred): in-progress nak throwing falls through to settlement ack (tiny window)
- Task 4: note for Task 5: idempotency settleJs only settles d.messages[0]; batch needs per-message contexts
- Task 4: note for Task 7: stores via NatsConnectionManager.idempotencyStores() {connection, store, owned}; user store w/o idempotent handler never registered; same store on 2 connections listed twice → close once by identity
- Task 5: note for Task 7: batch flushed at stop still waiting for a dispatcher slot is not in the in-flight set → forceStop does not nak it (processed later or redelivered after ack_wait)
- Task 5: minor (deferred): nakked under-reports messages nak'd via onRejected at forced stop
- Task 5: minor (deferred): core batch messages with replyTo are retried in-process and never answered (document)
- Task 5: minor (deferred): malformed entry in returned { failed } fails whole batch (undocumented)
- Task 5: minor (deferred): settleJs indexOf O(n²) per batch; test gap for item collected after stop drained
- Task 5: note for Task 7: shutdown must not await idle() after forceStop (hung handler holds slot) — keep idle wait bounded before forceStop
- Task 6: minor (deferred): Math.ceil(window) lets fractional window exceed cap (use floor)
- Task 6: minor (deferred): conditional await of close hooks in closeAll (timing smell tied to status$ test)
- Task 6: minor (deferred): batcher created after closeAll snapshot never flushed; sparse input leaves hole
- Task 7: minor (deferred): nc.close() awaited unbounded (constraint says no unbounded wait; cheap to wrap in settlesWithin)
- Task 7: minor (deferred): store close often gets ~0ms budget when server gone, abandoned silently (no log)
- Task 7: minor (deferred): close hooks skipped on already-closed connections → Batcher not closed
- Task 7: minor (deferred): abort() doesn't forceStop adapters or close stores
- Task 7: minor (deferred): shutdown.int test comment overclaims; integration output has ExceptionsHandler noise from older tests without logger:false
- Task 8: minor (deferred): consume.int 'consumer deleted is logged' flaked once in full run (passes isolated)
- Task 8: minor (deferred): Batcher class exported with public constructor taking internal types; expectTypeOf tests never typechecked (no vitest typecheck / tsc over test/)
- Task 8: minor (deferred): README omits DLQ-publish-failure nak case for undecodable batch messages
- Final: minor (deferred): heartbeat comment vs README bound wording; empty original Nats-Msg-Id not carried; batch claim failure naks whole batch (undocumented)
