# Dead Letters Through Outbound Plugins Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Send every dead letter through the connection's outbound plugins, marked with `OutgoingMessage.deadLetter`. A plugin that drops a dead letter makes the failed message count as dead-lettered.

**Architecture:**
- `sendToDlq` (`src/settlement/dlq.ts`) builds an `OutgoingMessage` and sends it through the existing `sendOutbound` machinery: `publishOutgoing` for `jetstream` mode, and `sendOutbound` with an `nc.publish` terminal step for `core` mode.
- It reports `'sent'` or `'dropped'`.
- Settlement keeps its outcomes and logs a warn line instead of the usual error line when the dead letter was dropped.

**Tech Stack:** TypeScript (ESM, NodeNext), NestJS 11/12, nats.js v3, Vitest (`unit` and `integration` projects), testcontainers with `nats:2.12-alpine`.

**Spec:** `docs/superpowers/specs/2026-10-11-dlq-outbound-plugins-design.md`

## Global Constraints

- Every dead letter goes through the outbound plugins. There is no option to opt out.
- `deadLetter: { subject: string; handler: HandlerInfo; error: unknown }`. `subject` is the subject the failed message arrived on.
- Dead letter `kind` is `'publish'` in `core` mode and `'jetstream'` in `jetstream` mode.
- `data` is the original body bytes. `headers` are the result of `buildDlqHeaders`. `codec` is `entry.options.codec ?? jsonCodec()`. `cloudEvent` and `atomic` are not set.
- Timeouts: `jetstream` mode uses `jetStreamTimeout(entry)`, and `core` mode uses the `NatsClient` publish timeout (5000 ms, `DEFAULT_REQUEST_TIMEOUT`).
- Outcome rules:
  - A plugin that throws before `next()`, or does not call it within the timeout, makes the DLQ publish fail. Today's failure behavior applies: JetStream naks with backoff, ordered retries in place without limit, core logs and drops.
  - A plugin that returns without calling `next()` makes the message count as dead-lettered: outcome `'dlq'`, a JetStream message is `term`'d, and a warn line is logged.
  - A plugin failing after `next()` is logged by `sendOutbound` as today.
- Exact warn lines when the dead letter was dropped. Each one replaces the usual dead-lettered line:
  - Handler failure: `<label> failed, its dead letter to <subject> was dropped by an outbound plugin after <attempts>: <errorMessage>`
  - Unmatched CloudEvent: `<label>: <err.message>; its dead letter to <subject> was dropped by an outbound plugin (unmatched: 'dlq')`
- The `"no stream captures this subject"` `NatsModuleError` mapping of a no-responders publish failure is kept.
- No tracing changes: the span `publish <dlq subject>` already comes from the tracing plugin's outbound middleware. No new attribute.
- `publishToDlq` stays internal and is not exported.
- Integration env:

  ```
  DOCKER_HOST=unix:///Users/hungvv/.colima/default/docker.sock
  TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
  TESTCONTAINERS_RYUK_DISABLED=true
  ```

- Verification before each commit: `npm test`, `npm run typecheck`, `npm run lint`. The last task also runs `npm run test:int` and `npm run docs:check`.
- Commits end with `Claude-Session: https://claude.ai/code/session_016DmB9sNwwcck4F6EDJBSom`.

## Review Focus

1. **Shutdown while a dead letter sits in a slow plugin.** The publish must stay bounded by its timeout, so `idle()` cannot wait forever on it. Tested in Task 1 by a plugin that never calls `next()`.
2. **A plugin that replaces `data` with an object.** It must be encoded by the connection codec, not sent as `[object Object]`. Tested in Task 1.
3. **A plugin that deletes `Nats-Msg-Id`.** The publish still succeeds; only de-duplication is lost, and the docs warn about it. Tested in Task 1 by checking the publish happened without the header.
4. **An ordered consumer whose plugin keeps throwing.** It retries in place and the next message does not overtake. Tested in Task 2.
5. **`dlq` in the `/testing` harness.** The harness lists the dead letter as the plugin left it, and leaves it out when dropped while the outcome is still `'dlq'`. Tested in Task 3.

---

### Task 1: `sendToDlq` through the outbound plugins

**Files:**
- Modify:
  - `src/pipeline/types.ts`: add `deadLetter?` to `OutgoingMessage` with the spec's doc comment, and a type import of `HandlerInfo` from `./delivery.js`.
  - `src/settlement/dlq.ts`: `sendToDlq` and `publishToDlq`.
  - `src/client/nats-client.ts`: export `DEFAULT_REQUEST_TIMEOUT`.
- Test: `test/unit/settlement.spec.ts` (`describe('publishToDlq')`) and `test/unit/dlq.spec.ts`.

**Interfaces:**
- Produces:
  - `sendToDlq(entry: ConnectionEntry, subject: string, mode: DlqPolicy['mode'], msg: InboundMessage, err: unknown, deliveries: number, handler: HandlerInfo, consumer?: string): Promise<'sent' | 'dropped'>`. It rejects on DLQ failure, as today.
  - `publishToDlq(entry, dlq, info, msg, err, deliveries): Promise<'sent' | 'dropped'>`, which passes `info` as `handler`.

Approach:
- Build the message described in the Global Constraints.
- `jetstream` mode: call `publishOutgoing(entry, message, {}, { what: `DLQ publish to "${subject}"`, wrap, skipped })`. `wrap` keeps today's no-responders mapping.
- `core` mode: call `sendOutbound(entry, message, (m) => entry.nc.publish(m.subject, encodeOutgoing(m), headers-if-any), { what, timeout: DEFAULT_REQUEST_TIMEOUT, skipped })`.
- `skipped` returns a module-private sentinel error. Catch that exact error and return `'dropped'`.
- If importing `src/jetstream/publish.ts` from `src/settlement/dlq.ts` creates an import cycle that breaks at load time, move `DEFAULT_REQUEST_TIMEOUT` and the timeout helpers to `src/pipeline/outbound.ts` instead. Report which you did.

- [ ] **Step 1: Write the failing tests.** Use the `fakeEntry` helpers in `settlement.spec.ts`, extended with `options.plugins`, or a small helper in `dlq.spec.ts`.
  - `'hands the dead letter to the outbound plugins, marked'`: a recording plugin sees one message with:
    - `kind: 'jetstream'`, `subject: 'dlq.ORD.d'`, `data` equal to the original bytes;
    - `headers.get('Nats-DLQ-Stream') === 'ORD'`;
    - `deadLetter` equal to `{ subject: msg.subject, handler: info, error }`.

    In core mode, `kind` is `'publish'`.
  - `'publishes what the plugins pass on'`: a plugin sets `x-tenant: t1` and calls `next()`. The `jsPublish` headers include it, and the result is `'sent'`. The core-mode counterpart checks the `nc.publish` headers.
  - `'encodes data a plugin replaced'`: a plugin sets `m.data = { a: 1 }`. The published bytes decode to `{ a: 1 }` with the JSON codec.
  - `'still publishes when a plugin deletes Nats-Msg-Id'`: the publish happens, and its headers lack `Nats-Msg-Id`.
  - `'reports a dead letter a plugin dropped'`: a plugin returns without `next()`. The result is `'dropped'` and nothing is published.
  - `'rejects with the error of a plugin that throws before next()'`.
  - `'rejects when the plugins do not pass the dead letter on in time'`: a plugin never calls `next()`. Use fake timers and advance by the timeout. The call rejects with a `NatsModuleError` matching `/did not pass the message on within/`, and nothing is published.
  - The existing `publishToDlq` tests keep passing: no-responders mapping, error pass-through, headers and body.

- [ ] **Step 2: Run** `npx vitest run --project unit test/unit/settlement.spec.ts test/unit/dlq.spec.ts`. Expected: the new tests FAIL.
- [ ] **Step 3: Implement** as described. Update `settlement.ts` call sites only as far as needed to compile: pass `d.handler`. Task 2 handles the `'dropped'` result. Remove "Bypasses the outbound plugins" from the `publishToDlq` doc comment.
- [ ] **Step 4: Run** the files, then `npm test`, `npm run typecheck`, `npm run lint`. Expected: PASS.
- [ ] **Step 5: Commit** with message `feat(dlq): send dead letters through the outbound plugins`.

---

### Task 2: Settlement treats a dropped dead letter as dead-lettered

**Files:**
- Modify: `src/settlement/settlement.ts`. `deadLetter()` returns `{ subject, error?, dropped? }`, and `logDeadLettered` takes `dropped`.
- Test: `test/unit/settlement.spec.ts`, plus `test/unit/ordered-consumer.adapter.spec.ts` if its settlement path is easier to drive there.

**Interfaces:**
- Consumes: `sendToDlq(...): Promise<'sent' | 'dropped'>` (Task 1).

Behavior:
- `'dropped'` follows the success path in all four places: `deadLetterConsume`, `deadLetterSubscribe`, `deadLetterOrdered`, and the unmatched path.
  - A JetStream message is `term`'d.
  - The outcome is `{ action: 'dlq', subject, error: err }`.
  - Instead of the usual line, the exact warn line from the Global Constraints is logged.
- Failures are unchanged.

- [ ] **Step 1: Write the failing tests**, with a plugin that drops the dead letter:
  - `'terms a JetStream message whose dead letter a plugin dropped, and warns'`:
    - `term` called once;
    - outcome `{ action: 'dlq', subject: 'dlq.ORD.d' }`;
    - `logger.warn` called with exactly `<label> failed, its dead letter to dlq.ORD.d was dropped by an outbound plugin after <attempts>: boom`, where `<label>` and `<attempts>` come from the same helpers the existing "sent to DLQ" test asserts;
    - `logger.error` not called with `sent to DLQ`.
  - `'records dlq for a core message whose dead letter a plugin dropped'`.
  - `'warns with the unmatched form for an unmatched CloudEvent'`: the line ends with `(unmatched: 'dlq')`.
  - `'retries an ordered dead letter in place while a plugin throws'` (Review Focus 4): the plugin throws twice, then passes. The publish happens on the third attempt, the outcome is `'dlq'`, and two error lines are logged.

- [ ] **Step 2: Run** the file(s). Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the file(s), then `npm test`, `npm run typecheck`, `npm run lint`. Expected: PASS.
- [ ] **Step 5: Commit** with message `feat(dlq): a dead letter an outbound plugin drops counts as dead-lettered`.

---

### Task 3: Harness, integration test and docs

**Files:**
- Test:
  - `test/unit/testing-harness.spec.ts`.
  - `test/unit/otel-tracing.spec.ts`: one test asserting that a dead letter yields a `publish <dlq subject>` PRODUCER span whose parent is the consume span, and that the published headers carry a `traceparent` for it. Follow that file's existing in-memory exporter setup.
  - `test/integration/retry-dlq.int.spec.ts`: a new test, or a new describe with its own app.
- Docs:
  - `docs/plugins.md`: dead letters go through outbound plugins. Document the `deadLetter` field, that plugins should call `next()` because a dropped dead letter counts as dead-lettered, the timeout and throw behavior, and the `Nats-Msg-Id` warning.
  - `docs/retry-and-dlq.md:101`: replace "DLQ publishes do not go through outbound plugins." with the new behavior and a link to Plugins.
  - `docs/tracing.md`: a dead letter gets its own publish span, child of the consume span.
  - `README.md`: remove the "DLQ publishing bypasses outbound plugins" bullet.
  - `CHANGELOG.md`: in the 0.1.0 entry's retry/DLQ bullet, add that dead letters go through the outbound plugins.

- [ ] **Step 1: Write the tests.**
  - Harness:
    - `'lists the dead letter as the outbound plugins left it'`: a plugin adds `x-dl: 1` when `m.deadLetter` is set, and `result.dlq[0].headers['x-dl']` is `'1'`.
    - `'leaves out a dead letter a plugin dropped'`: `result.dlq` is `[]` and the outcome is `'dlq'`.
  - Tracing: as above.
  - Integration, `'dead letters go through the outbound plugins'`:
    - an app with streams `ORD` (`ord.>`) and `DLQ` (`dlq.>`);
    - a `@Consume` with `dlq: true` whose handler throws `NonRetryableError`;
    - a plugin that sets `x-dead-letter-by: plugin` when `m.deadLetter` is set;
    - publish `ord.1`, then eventually read the DLQ stream's last message on `dlq.ORD.<durable>`;
    - it has `x-dead-letter-by: plugin` and `Nats-DLQ-Subject: ord.1`.
- [ ] **Step 2: Run** the unit files and `npx vitest run --project integration test/integration/retry-dlq.int.spec.ts` with the env. Expected: PASS. These pin behavior that Tasks 1–2 already built. If one fails, fix the code, not the test, and say so.
- [ ] **Step 3: Write the docs changes.**
- [ ] **Step 4: Run** the full verification: `npm test`, `npm run test:int` (with the env), `npm run typecheck`, `npm run lint`, `npm run docs:check`. Expected: all pass.
- [ ] **Step 5: Commit** with message `test(dlq): dead letters through plugins end to end; docs: dead letters and outbound plugins`.
