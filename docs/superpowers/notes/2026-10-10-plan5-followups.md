# Plan 5 (push/ordered consumers, atomic publish) — follow-ups

Collected from per-task and final reviews of feat/consumers-atomic (2792fdf..f345a42).

## Rulings made during execution
- Ruling: T5 Review Focus 5 test closes the connection deterministically from inside an outbound plugin (on message index n/2, before calling next()), so the batch send hits a closed connection; assert reject within timeout+1000 ms and 0 stored messages — a plain close-right-after-call races the fast in-memory staging — cost if wrong: mid-send close (between startBatch and commit) stays untested.
- Ruling: Task 2 push defaults omit flow_control/idle_heartbeat when deliverGroup is set (nats.js 3.4 and the server reject them with deliver_group); spec §2.1 + docs updated in Task 7 — brief's value was impossible — cost if wrong: grouped push consumers never report heartbeats_missed.
- Ruling: Task 2 review Important #1 (plan-mandated): grouped push consumers created by the library get NO max_ack_pending default (server default, 1000) — max_ack_pending is shared by the whole deliver group, so concurrency-of-one-instance caps the group; ungrouped keep max_ack_pending = capacity — cost if wrong: each grouped instance may buffer up to the shared limit (all heartbeated); users wanting tighter bounds set config.max_ack_pending.
- Ruling: Task 2 consequence — forceStop must nak push messages waiting for a slot (Dispatcher must track pushed-but-not-started messages so abandon() returns them); "already bound" from consume() wrapped in NatsConfigError suggesting deliverGroup; pull-consumer detection must not depend only on nats.js error text — cost if wrong: small extra code.
- Ruling: spec §3.1 Intake{open,eager}+Dispatcher.enqueue superseded by the brief's openPull/openPush + void push loop; §3.1 heartbeats_missed qualified for grouped consumers — spec text fixed in Task 7 — cost if wrong: none.
- Ruling: Task 4 keeps nats.js consume() defaults (30 s expires, 15 s idle heartbeat) for ordered consumers; recovery after a server-side consumer deletion takes ~75 s and the integration test allows 120 s — spec does not ask for faster recovery and shorter expires costs more pull requests — cost if wrong: slow recovery / slow CI file (~90 s).
- Ruling: Task 5 fake startBatch records messages at commit (not per add) — matches the server (nothing stored unless committed); spec §5 reads either way — cost if wrong: tests can't observe partially added fake batches.
- Ruling: I-2 — once the atomic batch was sent, publishAtomic settles with the commit result; plugin errors after that are logged, not thrown (before sending: still rejects with the plugin's error) — atomic batches cannot be deduped, so a reject for a stored batch invites duplicate retries — cost if wrong: a plugin's post-send failure is only visible in logs.
- Ruling: final-review must-fix deferred minors folded into the fix wave: README batch bullet (+@ConsumePush), publishing.md Failures wording, drop the harness migration note from CHANGELOG 0.1.0 (state current behaviour only), harness per-message streamSequence in a batch (M-1). allow_atomic on an existing stream verified working on 2.12 by the reviewer — cost if wrong: none.

## Parked at final review
- Final: parked — docs/publishing.md:77 + docs/errors.md:12 say "nothing is stored" on NatsAtomicPublishError, but a commit-ack timeout may have stored the batch (publishing.md:75 says so) — Ruling: docs-only inconsistency, no second fix wave; surfaced to the user — cost if wrong: a reader trusts "nothing stored" after a timeout and retries → duplicate batch.
- Final: parked — publishAtomic hangs if a plugin never settles after next() (pre-existing) — Ruling: real but plugin-contract violation; deferred — cost if wrong: a buggy plugin hangs the caller (no library timeout around plugins).
- Final: parked — publishing.md "cause is that error" wording (cause is a redacted copy) — Ruling: wording nit, deferred — cost if wrong: none.

## Deferred findings
- Task 1: minor (deferred): ordered deliverPolicy object with both startSeq and startTime passes (startTime ignored)
- Task 1: minor (deferred): ordered stream/filterSubjects not validated (empty string stream fails only at runtime)
- Task 1: minor (deferred): long doc-comment line handler-explorer.ts:262
- Task 2: minor (deferred): integration createTestApp leaves module/connection open when init() fails (test hygiene, support/app.ts)
- Task 2: minor (deferred): fallback consumers.info error replaces original getPushConsumer error
- Task 2: minor (deferred): already-bound wrapper matches err.name only (also "consumer already started")
- Task 3: minor (deferred): with signal already aborted, failed DLQ publish logs "retrying in" then "abandoned"
- Task 3: minor (deferred): replyTo guarded twice (base + NatsOrderedContext)
- Task 3: minor (deferred): sleep.spec setTimeout spy restored only on success path
- Task 4: minor (deferred): forceStop may report cancelledRetries 1 for a wait stop() already cancelled (waiting cleared only in sleep continuation)
- Task 4: minor (deferred): no adapter-level test that stop() ends a failing DLQ-publish retry loop (signal on the DLQ path)
- Task 4: minor (deferred): consumer_not_found status branch untested
- Task 5: minor (deferred): RF5 integration closes before startBatch — partial server-side batch (close between add calls) not exercised
- Task 6: minor (deferred): push harness DLQ path untested; noDlq test logs a Nest error line; mock atomic distinct batch ids untested
- Task 7: minor (deferred): CHANGELOG push+ordered share one bullet (brief: one per feature)
- Task 7: minor (deferred): README feature bullet "batch processing for @Subscribe and @Consume" omits @ConsumePush
- Task 7: minor (deferred): consumers.md table lacks explicit "Limits" row
- Task 7: minor (deferred): publishing.md Failures bullet reads as if the generic nats.js commit error is not a NatsAtomicPublishError
- Task 7: minor (deferred): CHANGELOG 0.1.0 carries a harness streamSequence migration note for an unreleased version

## Minor sweep (2026-10-10)
All parked and deferred items above were fixed in 9303422..bad4855 (re-reviewed clean), except where noted below.
- Ruling: publishAtomic settles with the commit result once the batch is sent (no waiting for plugin chains); before sending, staging is bounded by the timeout and aborts with NatsAtomicPublishError — cost if wrong: a slow but legitimate plugin pipeline (> timeout for the whole batch) is aborted.
- publishAtomic timeout must be in (0, 2^31-1] ms (setTimeout overflow).
- Finding: closing a connection after `add()` flushes the buffered adds and the commit, so the batch can be stored although the call rejects with a timeout — documented in docs/publishing.md and docs/errors.md.

## Remaining (not fixed)
- Single-message publishAtomic (and publish()) has no bound around plugins: a plugin that never settles hangs the call.
- The fallback JetStream client timeout used by publishAtomic is not range-checked (a client timeout > 2^31-1 would overflow).
- consume-ordered / consume-push integration tests print Nest error stack traces for their deliberately failing handlers (pre-existing pattern).
- Test gaps from the triage stay as listed above where not explicitly fixed.
