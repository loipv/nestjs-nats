# Deferred-findings triage: @loipv/nestjs-nats (pre-0.1.0)

Sources: `docs/superpowers/notes/2026-10-09-plan{1..4}-followups.md`, every item checked against the current code on `main` at fa26fe1. All paths are relative to the repo root.

Skipped as instructed: the Plan 2 "Open decision" I1 residual (heartbeat); the "implementer choices accepted" and "deviation accepted" lines; and the "note for Task N" lines (each one I checked is fixed: per-message `settleJs`, store close-once by identity, batches waiting for a slot nak'd by `abandon()`, bounded idle before forceStop).

Working tree: `docs/superpowers/specs/2026-10-09-nestjs-nats-design.md` has one uncommitted line. It adds `datacontenttype?` to the §9 option list, which is the Plan 4 Task 2 item. I did not make that edit; my only change is this file.

Compound findings are split where their parts land in different buckets.

| Bucket | Count |
|---|---|
| BUG | 13 |
| API | 2 |
| DOCS | 14 |
| TOOLING | 12 |
| TEST | 21 |
| COSMETIC | 30 |
| GONE | 44 |

---

## BUG

Severity is in brackets. **[security]** marks the one credential leak.

| Plan/task | Finding | Evidence | Fix proposal | Effort |
|---|---|---|---|---|
| P1 T2 | **[high, security]** `NatsConnectionError.cause` is the raw nats.js error. For a malformed server URL it is Node's `TypeError: Invalid URL` with `input: 'http://[alice:s3cretpw@bad host:4222]'`, the **plaintext password**. The message is redacted, the cause is not. `util.inspect` (an unhandled bootstrap rejection, `console.error(err)` as in the example's `main.ts`) and pino-style err serializers print it. This contradicts docs/connections.md:32 "Error messages and logs never include passwords". I reproduced it against `dist`. | src/connection/connection-manager.ts:146-156 | Replace `cause` with a sanitized Error: copy `name` and `code`, set `message` to the redacted text, drop `input` and other own props. Or omit `cause` when `code === 'ERR_INVALID_URL'`. Add a unit test asserting `util.inspect(err)` contains no secret. | S |
| P1 T9 | **[medium]** A NATS service that dies from a subscription error (for example a permissions violation) goes unnoticed. `@nats-io/services` calls `stop(err)`, and `sub.closed` then resolves with the error, so `qi.stop()` is never called (node_modules/@nats-io/services/lib/service.js:486-488). The endpoint iterator never ends, nothing is logged, and shutdown's `idle()` waits the full `drainTimeout`. | src/adapters/service-endpoint.adapter.ts:71-99, 129-154 (no `service.stopped` watch) | After `add()`: `void service.stopped.then((err) => { if (err instanceof Error && !this.stopping) log error; for each iterator: iterator.stop(); })`. Keep the iterators in a field. | S |
| P1 T7 | **[low-med]** A 5xx reply from a service endpoint caused by a plugin or codec failure outside the handler is sent but never logged. The core adapter logs it; the service adapter does not. A silent 500. | src/adapters/service-endpoint.adapter.ts:178-183, 195-200 vs core-subscription.adapter.ts:154-161 | In `respondError`, `warn` when `code >= 500`, as the core adapter does. | S |
| P2 T7 | **[low]** `abort()` (failed init or bootstrap) neither force-stops adapters nor closes idempotency stores. A user store (Redis client) stays open, which keeps the process alive with `abortOnError: false` or in tests. | src/module/nats-lifecycle.ts:156-162 | In `abort()`, after `stopAdapters`: `forceStopAdapters(adapters)`, then after `closeAll`: `closeIdempotencyStores(drainTimeout)`. | S |
| P1 T3 (partial) | **[low]** Failures inside the library's own hooks are cleaned up now. A failure elsewhere still leaks open connections, because Nest runs no shutdown hooks: a second connection's `connect()` throwing during DI after the first opened, or another provider's `onModuleInit`/`onApplicationBootstrap` throwing. Matters with `abortOnError: false` and in `Test.createTestingModule().compile()`. | src/module/nats.module.ts:110-117, src/connection/connection-manager.ts:82-139 | When a connect fails, close every entry already opened before rethrowing (fail-fast means the app cannot start anyway). Document that callers must `app.close()` after a failed `init()`. | M |
| P2 T7 | **[low]** The forced `nc.close()` is awaited without a bound, which contradicts lifecycle.md:26 "No step waits forever". | src/connection/connection-manager.ts:235-241 | `await settlesWithin(entry.nc.close(), 1000)` and log when it does not settle. | S |
| P2 T7 | **[low]** Idempotency store close gets about 0 ms when `closeAll` used its whole bound (the server is gone), and abandoning it is silent. | src/module/nats-lifecycle.ts:112-114, 217 | Give stores their own minimum budget (for example `max(remaining, 1000)`) and `warn` when `settlesWithin` returns `false`. | S |
| P3 T2 | **[low]** The CloudEvent idempotency key `${source}\|${id}` is ambiguous when `source` contains `\|`: (`a\|b`, `c`) and (`a`, `b\|c`) collide, so a distinct event is skipped as a duplicate (silent loss). `source` is sender-controlled. Fix before 0.1.0: the key format becomes state persisted in users' external stores. | src/idempotency/idempotency.ts:55, src/cloudevents/codec.ts:309-312 | Length-prefix or JSON-encode it: `${source.length}:${source}\|${id}` or `JSON.stringify([source, id])`, in both places. | S |
| P3 T1 | **[low]** When a handler calls `ctx.ack()` and then throws, the error is lost: `manualOutcome` returns `{ action: 'ack' }` with no error, so the OTel span ends OK and outer plugins see a clean success. | src/settlement/settlement.ts:71-80, src/pipeline/delivery.ts:57, src/otel/tracing.ts:89 | Add `error?: unknown` to the `'ack'` variant of `MessageOutcome` (an additive type change) and set it in `manualOutcome`. Update public-api.spec. | S |
| P3 T6 | **[low]** `rttTimeout` is not validated. `Infinity`, `NaN` or a huge value makes `setTimeout` fire after 1 ms (Node clamps), so the indicator always reports `rtt timeout after Infinityms`. | src/health/nats-health.indicator.ts:60, src/util/timeout.ts:9 | Accept only finite values in (0, 2^31-1]; otherwise use the default (or return `down('invalid rttTimeout')`). | S |
| P1 T3 | **[low]** `NatsRequestError.serviceCode` is `Number(header)`, so a non-numeric `Nats-Service-Error-Code` gives `NaN`. | src/client/nats-client.ts:102 | `const n = Number(serviceCode); serviceCode: Number.isFinite(n) ? n : undefined`. | S |
| P1 T1 | **[low]** `jsonCodec().encode` silently sends an empty payload when `JSON.stringify` returns `undefined` (a function, a symbol, `toJSON` returning `undefined`). The receiver decodes `undefined`. | src/codec/codec.ts:16 | Throw a `TypeError` or `NatsModuleError` ("value is not JSON-serializable") when `JSON.stringify` returns `undefined`. | S |
| P1 T5 | **[low]** `@Payload(undefined, Pipe)` (for example a conditional property name) stores `pipes: [undefined, Pipe]`, and Nest throws a TypeError on the first message. | src/decorators/param.decorators.ts:44-50 | Treat `first === undefined` as "no property" and filter out `undefined` pipes. | S |

## API

These change the public surface; changing them after 0.1.0 is breaking.

| Plan/task | Finding | Evidence | Fix proposal | Effort |
|---|---|---|---|---|
| P1 T1 | The exported type `HeadersInit` shadows the DOM/undici global `HeadersInit` (`lib.dom` and `@types/node` both declare one). Users who import it lose the global in that file, and auto-import picks the wrong one. | src/index.ts:7, src/codec/headers.ts:3 | Rename to `NatsHeadersInit` (used in `publish`/`request`/`JetStreamPublishOptions`/`DispatchOptions` signatures). Optionally keep a deprecated alias. | S |
| P3 T7 | `NatsConnectionManager` (a public export) has `protected openConnection` / `createJetStream` / `createJetStreamManager` in its `.d.ts`. Users can subclass and override them, which turns internal test seams into public API. | src/connection/connection-manager.ts:141-170, src/testing/nats-testing.module.ts:41-55 | Move the seams off the class: a symbol-keyed hook object (`[CONNECTION_FACTORY]`), or a non-exported injectable factory the testing module overrides. At minimum mark them `/** @internal */` (plain `stripInternal` breaks the testing `.d.ts`, which overrides them). | S-M |

## DOCS

| Plan/task | Finding | Evidence | Fix proposal | Effort |
|---|---|---|---|---|
| P1 T3 | `NatsClient.publish()` throws synchronously on invalid arguments: bad header names or values (from `headers.append`) and an invalid `cloudEvent`. publishing.md says "Errors are logged, never thrown". | src/pipeline/outbound.ts:37,47; src/client/nats-client.ts:46; docs/publishing.md:8 (cloudevents.md:64 already admits the cloudEvent case) | Reword: "Delivery errors are logged, never thrown; invalid arguments (headers, `cloudEvent`) throw synchronously." Or wrap `createOutgoingMessage` and log. | S |
| P1 T6 | A handler returning an Observable that completes empty gets `EmptyError` from Nest: the message is retried/nak'd, or a request gets a 500. | src/pipeline/invoker.ts:27-31 | Add a bullet to handlers.md: "return a value or `undefined`; an empty Observable counts as a failure". | S |
| P1 T6 | The payload is decoded before the handler is invoked, so `NatsDecodeError` never reaches exception filters or interceptors. handlers.md:120 implies filters see every failure. | src/pipeline/invoker.ts:47, 97; docs/handlers.md:117-120 | Add: "decode errors (and CloudEvent attribute errors) happen before guards, pipes, interceptors and filters run". | S |
| P1 Final | Sync compares against server-normalized values. Declaring `duplicate_window: 0` (the server stores 2 min), `max_deliver: 0` (the server stores -1) or `null` drifts forever: a spurious update on every boot, or a false `verify` failure. | src/jetstream/stream-sync.ts:39-54; docs/streams.md:30 | Document "omit a field to keep the server default; don't declare 0 or null for server-defaulted fields". Optional code fix: treat `null` as absent in `diffConfig`. | S |
| P1 ruling | The logger context `NatsConnection:<name>` and the `logger` option (`false` or a `LoggerService`) are not documented anywhere except the duplicate-logs note. | src/connection/connection-manager.ts:89, src/module/logger.ts:11-18 | Add a "Logging" paragraph to connections.md or lifecycle.md. | S |
| P1 T8 | If a consume loop (or service endpoint loop) ends unexpectedly, it is logged but never restarted, and the health indicator still reports up. | src/adapters/jetstream-consumer.adapter.ts:237-241; service-endpoint.adapter.ts:149-153 | Document it under Known limitations ("restart the process"). A restart-with-backoff code fix is M. | S |
| P2 Final | If the idempotency store fails to claim one key of a batch, the whole batch fails with `IdempotencyStoreError` and every message is nak'd/retried. batching.md says idempotency applies "to each message separately". | src/idempotency/idempotency.ts:159-178; docs/batching.md:41 | Add the exception to batching.md and idempotency.md ("a store error retries the whole batch"). | S |
| P3 T1 | `Delivery.outcomes` records the settlement decision even when the raw `ack`/`nak`/`term` call threw (the failure is only logged). | src/settlement/settlement.ts:84-90 then `recordOutcome` | plugins.md: "outcomes record the decision; a failed settle call is logged, not reflected". | S |
| P3 T3 | A CloudEvent's `datacontenttype` is fixed when the message is created. An outbound plugin that replaces `message.codec` does not update it. | src/pipeline/outbound.ts:40-50; docs/cloudevents.md:67 | One sentence in cloudevents.md:67. | S |
| P3 Final | A caller header `nats-msg-id` (any non-canonical case) suppresses the CloudEvent-id default (the check is case-insensitive), but the server's JetStream de-dup is believed to be case-sensitive (unverified), so neither id de-duplicates. | src/jetstream/jetstream-client.ts:56-62 | Document "use the `msgId` option or the exact `Nats-Msg-Id` header". Or canonicalize the header's name to `Nats-Msg-Id` (code, S). | S |
| P3 T8 | Limitations stated on feature pages are not listed in README Known limitations: no reply for an unmatched `'ack'` request (cloudevents.md:124), the default dispatch subject can be a wildcard and concurrent `dispatch()` calls (testing.md:105,126), and the request-subject/stream trap (streams.md:33). | README.md:116-128 | Add the missing ones, or a "see also" line. | S |
| P4 T4 | The example's `main.ts` publishes with fixed `msgId`s `o-1`/`o-2`. Rerunning within the stream's 2-min duplicate window silently de-duplicates them, so the second run shows no billing or DLQ activity. | examples/orders/src/main.ts:24-26; examples/orders/README.md (no mention) | Add a README note (or use `msgId: \`o-1-${Date.now()}\``). | S |
| P4 Final | The PubAck note says the requester "gets the stream's PubAck", but strictly it is a race: whichever answers first wins. | docs/handlers.md:27, docs/streams.md:33 | "gets the stream's PubAck (usually first) instead of, or racing with, the handler's reply". | S |
| P3 T7 | The harness ignores dispatch options that do not apply to the handler kind (`deliveryCount` for core, `attempt`/`request` for `@Consume`) without telling the user. | src/testing/harness.ts DispatchOptions | One line in testing.md's options table, or throw `NatsConfigError`. | S |

## TOOLING

| Plan/task | Finding | Evidence | Fix proposal | Effort |
|---|---|---|---|---|
| P4 Final | **[security]** `actions/checkout@v5` and `actions/setup-node@v5` are mutable tags, also in the release job, which has `id-token: write` and `contents: write`. | .github/workflows/release.yml:23,26; ci.yml:20,21,43,44,68,71,78,88,89 | Pin by commit SHA with a version comment and add Dependabot for `github-actions`. | S |
| P4 Final | **[security]** The single release job runs `npm ci`, and with it every dependency's lifecycle scripts, while holding `id-token: write`, `contents: write` and `pull-requests: write`. A compromised devDependency could mint the OIDC token and publish. | .github/workflows/release.yml:17-32 | `npm ci --ignore-scripts` (verify that build and tests still work), or split into a version job and a publish-only job with minimal permissions. | M |
| P4 Final | `ci-ok` uses `if: always()`, so it shows as failed on cancelled, superseded runs. | .github/workflows/ci.yml:98 | `if: ${{ !cancelled() }}`, or skip when `contains(needs.*.result, 'cancelled')`. | S |
| P4 T5 | `cancel-in-progress: true` also applies to `main` pushes, so a quick second merge cancels CI for the previous main commit. RELEASING.md:76 relies on "CI on the main push" being green. | .github/workflows/ci.yml:11-13 | `cancel-in-progress: ${{ github.event_name == 'pull_request' }}`. | S |
| P4 T5 | The checks job builds twice (`npm run build`, then `docs:check` runs `npm run build` again). | .github/workflows/ci.yml:28,31; package.json `docs:check` | Add a build-less `docs:check:only` script and use it in CI. | S |
| P4 T6 | The version PR opened with `GITHUB_TOKEN` gets no CI. It is documented, but the workaround is manual (close and reopen). | RELEASING.md:76 | Use a GitHub App token in `changesets/action`. | M |
| P4 T6 | check-pack allows `CHANGELOG.md`, `README.md` and `LICENSE` but does not require them. | scripts/check-pack.mjs:21-25 | Assert that each required top-level file is present. | S |
| P4 T1 | check-pack reads paths relative to the cwd, gives no friendly message when `npm pack` fails (raw stack), and its src-spec rule is untested. The `.d.ts.map` part is moot: no `declarationMap` in tsconfig.json. | scripts/check-pack.mjs:7,9-13,55 | Resolve paths from `import.meta.dirname`; wrap `execFileSync` in try/catch with a message. | S |
| P4 T3 | Smoke script: `spawnSync` has no timeout; the entry lookup and `cpSync` run outside the per-fixture try, so one bad fixture aborts all; SIGINT/SIGTERM leak the temp dir; fixture deps float on `^11`/`^12`/`^3`; in the fixtures, an `app.close()` error in `finally` can mask the assertion error. | scripts/smoke.mjs:12-22,58-64; test/smoke/*/package.json; test/smoke/esm-nest12/index.mjs | Add `timeout` to spawnSync, move lookup/copy inside the try, register signal cleanup, and optionally pin fixture deps. | S |
| P4 T2 | check-docs uses a fixed temp dir `.docs-check-tmp` (concurrent runs clash) and handles only SIGINT. The placeholder names are listed twice. Only ```ts fences are checked; the docs currently have no other TS fences, so this is moot today. | scripts/check-docs.mjs:13,50-76,146,157 | Add a pid suffix to the temp dir, handle SIGTERM, derive `PLACEHOLDER_NAMES` from `Object.keys(placeholders)`, and accept `typescript`/`tsx` fences. | S |
| P4 T5 | The Nest 11 downgrade (`npm install --no-save ...@^11`) under npm 10 / Node 22 has not been verified locally. | .github/workflows/ci.yml:49-56 | Confirm on the first CI run (the version print step shows the outcome). | S |
| P4 T6 | The `version-packages` lockfile step has never run end to end at a bumped version. | package.json `version-packages`; RELEASING.md:49 | Covered by the rehearsal in RELEASING.md step 6; just do it. | S |

## TEST

| Plan/task | Finding | Evidence |
|---|---|---|
| P1 T1 | No tests for an empty-array header value, for `decodePayload` re-wrapping a codec's own `NatsDecodeError` (it does double-wrap: codec.ts:29-35), or for headers being forwarded to `codec.decode`. | test/unit/codec.spec.ts, test/unit/headers.spec.ts |
| P1 T3 | No positive `useExisting` test (only the rejection one), no test of the request short-circuit "produced no reply", no test of NoResponders reached through `err.cause` in `NatsClient`. | test/unit/nats-module.spec.ts:6; src/client/nats-client.ts:24-28,110-112 |
| P1 T4 | Bare `rejects.toThrow()`. | test/integration/jetstream.int.spec.ts:478 (also :370, test/unit/pipeline.spec.ts:166) |
| P1 T6 | No global pipe test (`APP_PIPE` / `useGlobalPipes`); only method-level `@UsePipes`. | test/unit/enhancers.spec.ts:123-126 |
| P1 T7 | The 400-reply test is ambiguous: the `orders.*` wildcard handler also answers the same request with 400. | test/integration/subscribe.int.spec.ts:24-27,144-149 |
| P1 T7 | The bootstrap-cleanup test fails in validation, before any adapter has started, so stopping started adapters is untested. | test/integration/subscribe.int.spec.ts:198-207 |
| P1 T9 | The unknown-connection service case is covered only in the validator, not in `createAdapters`. | test/unit/handler-validator.spec.ts:62 |
| P1 T9 | `setImmediate`-based `tick()` waits are brittle. | test/unit/{limiter,dispatcher,core-subscription.adapter,service-endpoint.adapter,jetstream-consumer.adapter}.spec.ts |
| P2 T1 | No tests for a multi-value header copied into a dead letter, for CR/LF stripping combined with truncation, or for a module-level `isRetryable` merged with handler options. | test/unit/dlq.spec.ts:25,81,94; test/unit/retry.spec.ts |
| P2 T3 | Heartbeat unit tests use real timers with call-count windows (drifts on CI). | test/unit/jetstream-consumer.adapter.spec.ts:364-424 |
| P2 T3 | consume.int "naks on error with a backoff" never asserts a delay. | test/integration/consume.int.spec.ts:175-179 |
| P2 T5 | No test for a message collected after `stop()` already drained (the `push` re-drain branch). | src/adapters/dispatcher.ts:78-79 |
| P2 T7 | Integration apps use Nest's default logger, so `ExceptionsHandler` noise appears in the output. | test/integration/support/app.ts:4-8 |
| P2 T8 | The consume.int "logs when the consumer is deleted" test flaked once. | test/integration/consume.int.spec.ts:267-288 |
| P3 T1 | Outcome tests lack the core store-error retry, a raw settle call that throws, and a DLQ subject given as a function. | test/unit/outcomes.spec.ts |
| P3 T3 | No test of CE extensions on the wire (only a `ce-extra` header set by a plugin), of a codec swapped by a plugin, or of `batcher()` with `cloudEvent`. | test/unit/outbound-cloudevents.spec.ts:79; test/unit/batcher.spec.ts |
| P3 T4 | The router DLQ unit cases do not assert the DLQ publish call. | test/unit/cloudevent-router.spec.ts |
| P3 T5 | No baggage-layering test (tracestate is covered). The guard regex matches single quotes only, which is fine because Biome enforces `quoteStyle: single`. | test/unit/otel-tracing.spec.ts:453-471; test/unit/public-api.spec.ts:90 |
| P3 T6 | The health int "down" test doesn't assert the reason; no test that `pass`/`token` are scrubbed from a reason; the test relies on `NatsModule` being global. | test/integration/health.int.spec.ts:51-59; test/unit/nats-health.spec.ts |
| P3 T7 | No interceptor test through the harness (guards and pipes are covered). | test/unit/testing-harness.spec.ts:180 |
| P3 T8 | The otel/health import-graph checks have no sanity assertion such as `files.size > N`, so a broken walk would pass without checking anything. | test/unit/public-api.spec.ts:216-230 |

## COSMETIC

| Plan/task | Finding | Evidence |
|---|---|---|
| P1 T1 | Error subclasses get an own `cause: undefined` property when no cause is given. | src/errors/errors.ts:18,42,71 |
| P1 T2 | `status$` forwards raw status `data` unredacted. Checked: nats-core puts only `url.host` in `server`, and gossiped URLs carry no credentials, so nothing secret leaks. | src/connection/connection-manager.ts:293-298; node_modules/@nats-io/nats-core/lib/servers.js:88-89,110,118 |
| P1 T2 | `events$` never completes, and entries are not cleared after `closeAll`. | src/connection/connection-manager.ts:78-80,218-223 |
| P1 T2 | A redundant `.catch` is attached on every `jsm()` call. | src/connection/connection-manager.ts:103-109 |
| P1 T3 | `reply` is not visible on `OutgoingMessage` to plugins (it can be added later without breaking anything). | src/pipeline/types.ts:10-23 |
| P1 T5 | A stale cast/comment about the JsMsg reply in the base `replyTo`. | src/context/nats-context.ts:48-53 |
| P1 T5 | A param decorator on a constructor parameter is silently ignored. | src/decorators/param.decorators.ts:30 |
| P1 T7 | Limiter `release()` at 0 is silently ignored. | src/adapters/limiter.ts:33 |
| P1 T7 | The core adapter imports `@nats-io/services` only for header constants (a required peer anyway). | src/adapters/core-subscription.adapter.ts:1 |
| P1 T9 | The respond/respondError and consume-loop code is near-duplicated across the core and service adapters. | core-subscription.adapter.ts:180-202 vs service-endpoint.adapter.ts:195-211 |
| P1 T9 | Dead guards and a duplicate `manager.get` in the lifecycle grouping. | src/module/nats-lifecycle.ts:125,138,148-149 |
| P1 T9 | The boolean returned by `msg.respond` is ignored. | core-subscription.adapter.ts:193-202; service-endpoint.adapter.ts:202-211 |
| P2 T1 | `backoffDelay` returns NaN for `initialDelay: 0` and an attempt above ~1024. Harmless: nats.js treats `nak(NaN)` as a plain nak and `setTimeout(NaN)` as ~1 ms; only the log shows "NaNms". | src/settlement/retry.ts:68-69 |
| P2 T1 | The DLQ error header slices UTF-16 units (can split a surrogate pair), and `String(plainObject)` gives `[object Object]` (same in OTel `toException`). | src/settlement/dlq.ts:116-117; src/otel/tracing.ts:35 |
| P2 T1 | Module-level `retry` is validated only through the handlers that use it. | src/discovery/handler-validator.ts:66-88 |
| P2 T2 | Inline `errorMessage` copies remain. | src/settlement/dlq.ts:116; connection-manager.ts:151,303; nats-client.ts:55 |
| P2 T2 | `cancelRetries` counts timers, not retries that fired and are queued for a slot, so the shutdown summary under-reports (those go to `onRejected`). | src/adapters/dispatcher.ts:142-148 |
| P2 T5 | `nakked` under-reports naks made through `onRejected` at forced stop. | src/adapters/jetstream-consumer.adapter.ts:73-76,147 |
| P2 T5 | `settleJs` uses `indexOf` (O(n²) per batch; bounded by `maxSize`). | src/idempotency/idempotency.ts:123 |
| P2 T6 | `Math.ceil(window)` lets a fractional window exceed the cap, and a sparse items array leaves a hole in the results. | src/jetstream/jetstream-client.ts:109,118 |
| P2 T6 | Close hooks are awaited only when there are any (a timing smell tied to the `status$` test). | src/connection/connection-manager.ts:247-249 |
| P2 T6/T7 | A batcher created after the `closeAll` snapshot, or on an already-closed connection, is never closed. Its items still reject when their timed flush fails, so nothing hangs. | src/connection/connection-manager.ts:227,248; src/jetstream/jetstream-client.ts:137 |
| P2 Final | The heartbeat comment says "max_messages - 1" while the docs say "up to concurrency". An empty original `Nats-Msg-Id` is not carried into the dead letter. | src/adapters/jetstream-consumer.adapter.ts:90-93 vs docs/retry-and-dlq.md:62; src/settlement/dlq.ts:114 |
| P3 T1 | The manual nak outcome has an `error: undefined` key, and `recordOutcome` is silent when the map is missing. | src/settlement/settlement.ts:75; src/pipeline/delivery.ts:95-97 |
| P3 T2 | CE parsing leniencies: both `data` and `data_base64` accepted (base64 wins), lenient base64, no `ce-datacontenttype` fallback, mixed-case duplicate headers resolved by order, string `time` not validated, a redundant `KNOWN` alias. | src/cloudevents/codec.ts:13,81,204-206,246-255 |
| P3 T3 | `request()` rejects `cloudEvent` only at the type level; at runtime `createOutgoingMessage` would honour it. | src/client/nats-client.ts:62-67 |
| P3 T4 | An unmatched `'ack'` request is recorded as `replied` though nothing is sent. The 404 description echoes the sender's `type` with no length cap (CR/LF stripped). | src/settlement/settlement.ts:341-348; src/adapters/core-subscription.adapter.ts:168; src/cloudevents/errors.ts:13 |
| P3 T5 | The batch span is named after the first message's subject; service endpoints have no `consumer.group.name` (no queue in `HandlerInfo`); an invalid `traceparent` header blocks the CE fallback; `getTracer` is called per call (intended). | src/otel/tracing.ts:105,113,120-121; src/otel/carrier.ts:71 |
| P3 T6 | Health redaction covers only URL userinfo, `pass` and `token`. rtt/JetStream errors never carry seeds or JWTs. | src/health/nats-health.indicator.ts:58-59 |
| P3 Final/P4 T4 | `writeCloudEvent` inlines data only for an exact `application/json` codec. The example's `bootstrap().catch` exits without `app.close()`. | src/cloudevents/codec.ts:298; examples/orders/src/main.ts:46-49 |

## GONE

| Plan/task | Finding | Evidence |
|---|---|---|
| P1 ruling | `drainTimeout` did not bound `sub.drain()`/`nc.drain()`. | src/module/nats-lifecycle.ts:104-113; src/connection/connection-manager.ts:225-242 |
| P1 T3 | `forRootAsync` without a factory threw a bare TypeError. | src/module/nats.module.ts:78-82 (`NatsConfigError`) |
| P1 T3/T4 | A failure in the library's own `onModuleInit`/`onApplicationBootstrap` left connections open (the DI/other-provider part is BUG above). | src/module/nats-lifecycle.ts:60-86,156-162 |
| P1 T4 | `diffConfig` treated omitted falsy fields and nested objects as drift. | src/jetstream/stream-sync.ts:23-54 |
| P1 T4 | Consumer defaults were applied before the diff. | src/jetstream/stream-manager.ts:121-129 |
| P1 T4 | No tests for a create-only consumer or a verify-drifted consumer. | test/integration/jetstream.int.spec.ts:271,284 |
| P1 T5/T6 | `ack`/`nak`/`term` marked the context settled before the raw call. | src/context/nats-context.ts:113-135 |
| P1 T5 | Weak coexistence test with `createParamDecorator`. | covered by test/unit/enhancers.spec.ts (`@Tenant` param, :90) |
| P1 T6 | Handler errors logged twice (kept by ruling, now documented). | README.md:126 |
| P1 T6 | No TRANSIENT-scope test (the APP_PIPE part is under TEST). | test/unit/handler-validator.spec.ts:65 |
| P1 T7 | `drainTimeout` did not bound adapter `stop()`. | src/module/nats-lifecycle.ts:104 |
| P1 T7 | The `drainTimeout` merge ignored unset connections. | src/module/nats-lifecycle.ts:221-228 |
| P1 T7 | Request-path 5xx not logged, core part (the service part is BUG). | src/adapters/core-subscription.adapter.ts:154-161 |
| P1 T7 | Docs: `onModuleDestroy` runs before shutdown. | docs/lifecycle.md:28 |
| P1 T8 | Docs: nak-on-stop / a pulled batch waits for `ack_wait`. | README.md:125; docs/lifecycle.md:22 |
| P1 T8/T9 | `errorTrace` duplicated x3. | src/util/errors.ts:7 (shared) |
| P1 T10 | Stray section comments in index.ts. | src/index.ts:1-84 |
| P1 T10 | The optional-peer guard was not transitive, and there was no type-level export test. | test/unit/public-api.spec.ts:81-150 |
| P1 ruling | Docs: publish order with async outbound plugins. | README.md:127 |
| P1 Final | Removed keys are never reported (now documented). | README.md:128 |
| P2 T1 | Explicit `undefined` retry fields clobbered the defaults. | src/settlement/retry.ts:40-58 |
| P2 T2 | `Dispatcher.reject` was silent without `onRejected`. | core-subscription.adapter.ts:53-56; jetstream-consumer.adapter.ts:73-76 |
| P2 T2 | A hung handler was left in a forceStop unit test. | Not found: jetstream-consumer.adapter.spec.ts:296-323 releases its gate |
| P2 T3 | `Nats-Schedule*`/`Nats-Batch-*` headers were copied to the DLQ. | src/settlement/dlq.ts:39-41,96-102 |
| P2 T4 | Docs: `release` has no claim ownership, so the TTL must exceed the handler run. | docs/idempotency.md:41 |
| P2 T4 | An in-progress nak that throws fell through to the settlement ack. | src/settlement/settlement.ts:301 (SKIPPED skipped); src/context/nats-context.ts:123-128 |
| P2 T5 | Docs: core batch requests are never answered. | README.md:123; docs/batching.md:43 |
| P2 T5 | A malformed `{ failed }` entry failed the whole batch. | src/pipeline/invoker.ts:113-125; docs/batching.md:39 |
| P2 T8 | `Batcher` exported with a public constructor. | src/index.ts:1 (type-only) |
| P2 T8 | `expectTypeOf` tests were never typechecked. | tsconfig.json `include: ["src","test"]`; .github/workflows/ci.yml:27 |
| P2 T8 | The README omitted the DLQ-failure nak for undecodable batch messages. | docs/batching.md:40 |
| P3 T3 | `Uint8Array` data was labelled with the codec's content type. | src/pipeline/outbound.ts:42-43 |
| P3 T3 | A user `Nats-Msg-Id` was overwritten by `ce.id`. | src/jetstream/jetstream-client.ts:56-62 |
| P3 T4 | An unparseable structured envelope is a decode error (now documented). | docs/cloudevents.md:123 |
| P3 T6 | Health: total wait up to 2 × `rttTimeout` (docs wording). | docs/health.md:54 |
| P3 T7 | Docs: wildcard default dispatch subject, DLQ attribution under concurrent dispatches, shared stores. | docs/testing.md:105,122,126 |
| P3 T7 | The `DispatchResult.result` doc for batches (now accurate). | src/testing/harness.ts (`DispatchResult.result`), src/pipeline/invoker.ts:83-90 |
| P3 T7 | No guard harness test. | test/unit/testing-harness.spec.ts:180 |
| P3 T8 | `/otel` exported `msgHdrsGetter`/`msgHdrsSetter`. | src/otel/index.ts:1 |
| P3 T8 | Docs said `message.cloudEvent` (actually `{ meta, mode }`); the OTel example had no npm install line. | docs/cloudevents.md:67; docs/tracing.md:14 |
| P3 Final | Spec §9 superseded by rulings; binary JSON codec + `text/plain` (documented). | docs/superpowers/specs/2026-10-09-nestjs-nats-design.md:285-287; docs/cloudevents.md:62 |
| P4 T2 | Spec §9 option list lacked `datacontenttype?`. | Fixed in an uncommitted working-tree edit (spec:286; `git diff`) |
| P4 T4 | Docs: a core request on a stream-captured subject gets the PubAck; stale `dist` in the example. | docs/handlers.md:27; docs/streams.md:33; package.json `example:check` (builds and reinstalls) |
| P4 T5/T6 | Smoke matrix lacked `fail-fast: false`; `changesets/action` on a mutable tag. | .github/workflows/ci.yml:64; .github/workflows/release.yml:35 (SHA-pinned v2.1.2) |

---

### Extra observation (not from the notes)

- `NatsConnectionManager.get()` is public and returns the non-exported internal `ConnectionEntry` type (`nc`, `jsm()`, `onClose()`, `idempotencyStore()`). That shape becomes de-facto public API in the `.d.ts`. Consider deciding this together with the protected-hooks API item.
