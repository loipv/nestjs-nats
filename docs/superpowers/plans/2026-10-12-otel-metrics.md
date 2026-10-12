# OpenTelemetry Metrics Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `metrics()`, an OpenTelemetry metrics plugin exported from `@loipv/nestjs-nats/otel` next to `tracing()`. It records delivery processing durations and per-message outcomes, plus outbound send counts and durations.

**Architecture:**
- `src/otel/metrics.ts` returns a `NatsPlugin` named `'otel-metrics'`. Its inbound middleware runs in the `'outer'` stage and reads `delivery.outcomes` after `next()`. Its outbound middleware times `next()`.
- Instruments are resolved on each record. The plugin caches the instruments of one `MeterProvider` and recreates them when `metrics.getMeterProvider()` returns a different object, because the metrics API has no proxy provider.

**Tech Stack:** TypeScript (ESM, NodeNext), `@opentelemetry/api` 1.9 (optional peer), `@opentelemetry/sdk-metrics` 2.x (devDependency, tests only), Vitest (`unit` and `integration` projects), testcontainers with `nats:2.12-alpine`.

**Spec:** `docs/superpowers/specs/2026-10-12-otel-metrics-design.md`

## Global Constraints

- `src/otel/metrics.ts` imports only `@opentelemetry/api` and library-internal modules. No SDK import outside `test/`.
- Plugin `name: 'otel-metrics'`. Inbound `stage: 'outer'`.
- Default `meterName` is `'@loipv/nestjs-nats'`.
- Instrument names, kinds and units:

  | Name | Kind | Unit |
  |---|---|---|
  | `messaging.process.duration` | Histogram | `s` |
  | `messaging.client.consumed.messages` | Counter | `{message}` |
  | `messaging.client.sent.messages` | Counter | `{message}` |
  | `messaging.client.operation.duration` | Histogram | `s` |

- Histograms pass `advice: { explicitBucketBoundaries: [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10] }`.
- Durations are `(performance.now() - start) / 1000`.
- Attribute values:
  - `messaging.system`: `'nats'`.
  - `error.type`: `err.name` when `err instanceof Error` and `err.name !== ''`, else `'_OTHER'`.
  - `nats.outcome`: the outcome's `action`, or `'unsettled'` when the message has no entry in `delivery.outcomes`.
  - `nats.publish.kind`: `'core'` for kind `publish`, `'jetstream'`, or `'request'`.
  - `nats.dead_letter`: `true` only when `m.deadLetter` is set. The attribute is absent otherwise.
- `messaging.destination.name` is recorded only when `subjects` is `true` (the raw subject) or a function returning a string. When the function throws, the attribute is omitted and `diag.warn` is called once per plugin instance.
- The plugin never swallows or adds errors. Errors from `next()` are rethrown as is, after recording.
- Integration env: `DOCKER_HOST=unix:///Users/hungvv/.colima/default/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock TESTCONTAINERS_RYUK_DISABLED=true`.
- Every commit message ends with `Claude-Session: https://claude.ai/code/session_016DmB9sNwwcck4F6EDJBSom`.

## Review Focus

1. **Plugin created before the SDK starts.** The usual `forRoot({ plugins: [metrics()] })` runs before `sdk.start()`. Records must reach the provider registered later (Task 1 test "records once a provider is registered after creation").
2. **A handler that throws a non-`Error`** (a string or a plain object). Expect `error.type: '_OTHER'` and the same value rethrown (Task 1).
3. **An acked message whose handler then threw** (`{ action: 'ack', error }`). The counter carries `nats.outcome: 'ack'` and `error.type` (Task 1).
4. **A `subjects` function that throws on every call.** Records still happen without the attribute, and `diag.warn` is called exactly once across several records (Task 1).
5. **A dead letter whose publish fails.** The sent counter carries both `nats.dead_letter: true` and `error.type`, and the error is rethrown (Task 2).

---

### Task 1: `metrics()` inbound, instrument lifecycle and options

**Files:**
- Create: `src/otel/metrics.ts`
- Modify: `src/otel/index.ts` (export `metrics` and `type MetricsOptions`)
- Modify: `test/support/otel.ts` (add `setupMetrics()`)
- Modify: `test/unit/public-api.spec.ts:268` and `test/unit/otel-tracing.spec.ts` (export list becomes `['metrics', 'tracing']`)
- Modify: `package.json`, `package-lock.json` (`npm install -D @opentelemetry/sdk-metrics@^2.11.0`)
- Test: `test/unit/otel-metrics.spec.ts`

**Interfaces:**
- Produces:
  - `export interface MetricsOptions { meterName?: string; subjects?: boolean | ((subject: string) => string | undefined) }`
  - `export function metrics(options?: MetricsOptions): NatsPlugin`
  - `test/support/otel.ts`:
    ```ts
    export interface TestMetrics {
      provider: MeterProvider;
      /** Collects and returns the data points of `name`, each `{ attributes, value }`. */
      points(name: string): Promise<Array<{ attributes: Attributes; value: unknown }>>;
      /** Shuts the provider down and unregisters the global meter provider. */
      shutdown(): Promise<void>;
    }
    export function setupMetrics(): TestMetrics;
    ```
    Build it on `MeterProvider({ readers: [reader] })`, where `reader` is a `MetricReader` subclass with no-op `onForceFlush`/`onShutdown`, registered through `metrics.setGlobalMeterProvider`. `shutdown()` calls `metrics.disable()`. Counter points have a `number` value. Histogram points have the SDK's `{ count, sum, buckets: { boundaries, counts } }` value.
- Task 2 adds the outbound middleware to the same `metrics()` and reuses its instrument cache, `errorType()` and subject helper.

Build the test deliveries the way `test/unit/otel-tracing.spec.ts` does (`corePart`, `jsPart`, `orderedPart`, `delivery()`, the `HandlerInfo` fixtures). Copy what you need into the new spec; do not import from another spec file. Use a fresh `setupMetrics()` in `beforeEach` and `shutdown()` in `afterEach`.

- [ ] **Step 1: Write the failing tests** in `test/unit/otel-metrics.spec.ts`

  | Test | Assertion |
  |---|---|
  | `is named otel-metrics and runs in the outer stage` | `plugin.name === 'otel-metrics'`, `plugin.inbound?.stage === 'outer'` |
  | `records one process duration per delivery` | One point on `messaging.process.duration`: `value.count === 1`, `value.buckets.boundaries` equal the spec list. Attributes equal `{ 'messaging.system': 'nats', 'messaging.operation.name': 'process', 'nats.connection': 'default', 'nats.handler': 'H.on', 'nats.handler.kind': 'consume', 'messaging.consumer.group.name': 'billing', 'nats.stream': 'ORD' }` |
  | `counts each message with its outcome` (`it.each` over `ack`, `retry`, `dlq`, `term`, `drop`, `skipped`, `replied`, `none`) | `next` records that outcome. `messaging.client.consumed.messages` has value 1 with `nats.outcome` equal to the action. `error.type` is present exactly when the outcome carries `error` |
  | `counts a message without an outcome as unsettled` | `nats.outcome: 'unsettled'` |
  | `counts an acked message whose handler threw` | outcome `{ action: 'ack', error: new TypeError('x') }` gives `nats.outcome: 'ack'`, `error.type: 'TypeError'` |
  | `records a batch once and counts every message` | Batch of 3 core parts, outcomes `ack`, `ack`, `retry`. Histogram `count === 1`. Counter points: `ack` → 2, `retry` → 1 |
  | `records the error of next() and rethrows it` | `next` rejects with `err = new RangeError('boom')`. `handle` rejects with the same `err` (`toBe`). Histogram has `error.type: 'RangeError'`, and messages are still counted (`unsettled`) |
  | `uses _OTHER for a non-Error throw` | `next` rejects with `'nope'`; rejects with `'nope'`; `error.type: '_OTHER'` |
  | `sets group and stream by handler kind` (`it.each`) | `subscribe` with queue → group `'workers'`, no `nats.stream`. `subscribe` without queue → neither. `ordered` (stream `'ORD'`) → `nats.stream: 'ORD'`, no group. `service-endpoint` → neither |
  | `omits the subject by default` | No `messaging.destination.name` on any point |
  | `records the subject with subjects: true` | Histogram and counter carry `messaging.destination.name: 'o.c'` |
  | `maps the subject with a subjects function` | `subjects: (s) => s.replace(/\.\w+$/, '.*')` gives `'o.*'`. A function returning `undefined` gives no attribute |
  | `uses each message's subject for the counter` | `subjects: true`, batch with subjects `a.1`, `a.2`. Histogram has `'a.1'`. Counter has one point each for `'a.1'` and `'a.2'` |
  | `warns once when the subjects function throws` | `vi.spyOn(diag, 'warn')`. A throwing function over two deliveries gives records without the attribute and `warn` called once |
  | `records once a provider is registered after creation` | Create the plugin with no global provider (call `metrics.disable()` first), handle a delivery (no throw), then `setupMetrics()`, handle again: one point with count 1 |
  | `follows a provider change` | Handle on provider A, shut A down, set up B, handle: B has exactly one record |
  | `uses meterName` | `metrics({ meterName: 'x' })`. The collected scope name is `'x'`; the default is `'@loipv/nestjs-nats'` (read `scopeMetrics[].scope.name` from `reader.collect()`, or expose it from `points`) |

- [ ] **Step 2: Run the tests to verify they fail**

  Run: `npx vitest run --project unit test/unit/otel-metrics.spec.ts`
  Expected: FAIL. `metrics` is not exported from `src/otel/index.js`.

- [ ] **Step 3: Implement `metrics()` (inbound) in `src/otel/metrics.ts`**

  - The instrument cache is one slot `{ provider, instruments }`, compared by identity with `metrics.getMeterProvider()` on each record.
  - `errorType(err: unknown): string` follows the Global Constraints.
  - A subject helper returns the attribute value or `undefined`. It catches throws, warning once through `diag.warn` with the error.
  - Inbound flow: record `start`, then `try { await next() } catch (err) { remember err; throw } finally { histogram.record(...); count each message }`.
  - Keep `consumerGroup()` behavior identical to the one in `tracing.ts`. Move it to a small shared module (`src/otel/attributes.ts`) and import it from both files.

  Also update the export lists in `test/unit/public-api.spec.ts` and `test/unit/otel-tracing.spec.ts` to `['metrics', 'tracing']`. Add `expectTypeOf<otel.MetricsOptions>().toHaveProperty('subjects')` next to the `TracingOptions` check.

- [ ] **Step 4: Run the tests to verify they pass**

  Run: `npm test && npm run typecheck && npm run lint`
  Expected: all unit tests PASS; typecheck and lint clean.

- [ ] **Step 5: Commit**

  ```bash
  git add src/otel test/support/otel.ts test/unit/otel-metrics.spec.ts test/unit/public-api.spec.ts test/unit/otel-tracing.spec.ts package.json package-lock.json
  git commit -m "feat(otel): metrics plugin, inbound durations and outcomes"
  ```

### Task 2: `metrics()` outbound

**Files:**
- Modify: `src/otel/metrics.ts`
- Test: `test/unit/otel-metrics.spec.ts`

**Interfaces:**
- Consumes: Task 1's instrument cache, `errorType()`, subject helper and `setupMetrics()`.
- Produces: `plugin.outbound`, recording `messaging.client.sent.messages` (+1) and `messaging.client.operation.duration` once `next()` settles.

Build `OutgoingMessage`s the way `outgoing()` in `test/unit/otel-tracing.spec.ts` does.

- [ ] **Step 1: Write the failing tests**

  | Test | Assertion |
  |---|---|
  | `counts and times a sent message` (`it.each`: `publish` → `publish`/`core`, `jetstream` → `publish`/`jetstream`, `request` → `request`/`request`) | Counter value 1 and histogram count 1, both with `{ 'messaging.system': 'nats', 'messaging.operation.name': <op>, 'nats.connection': 'default', 'nats.publish.kind': <kind> }` and no `nats.dead_letter` |
  | `marks dead letters` | `deadLetter: { subject: 'o.c', handler: consumeHandler, error: new Error() }` gives `nats.dead_letter: true` |
  | `records the error of next() and rethrows it` | `next` rejects with a `NatsModuleError` → same error rethrown. Both points carry `error.type: 'NatsModuleError'` |
  | `marks a failed dead letter` | Dead letter with `next` rejecting: `nats.dead_letter: true` and `error.type` on both points |
  | `counts each message of an atomic batch` | Three messages with `atomic: { index: i, count: 3 }` give counter value 3 |
  | `records nothing while next() is pending` | `next` returns a never-settling promise. After a microtask flush, no point on either instrument |
  | `applies subjects to outbound` | `subjects: true` → `messaging.destination.name: 'o.c'`. Default → absent |

- [ ] **Step 2: Run the tests to verify they fail**

  Run: `npx vitest run --project unit test/unit/otel-metrics.spec.ts`
  Expected: FAIL. `plugin.outbound` is undefined.

- [ ] **Step 3: Implement the outbound middleware in `src/otel/metrics.ts`** with the same `try/catch/finally` shape as inbound.

- [ ] **Step 4: Run the tests to verify they pass**

  Run: `npm test && npm run typecheck && npm run lint`
  Expected: PASS; clean.

- [ ] **Step 5: Commit**

  ```bash
  git add src/otel/metrics.ts test/unit/otel-metrics.spec.ts
  git commit -m "feat(otel): metrics plugin, outbound sends and durations"
  ```

### Task 3: Integration test, docs, smoke and changelog

**Files:**
- Create: `test/integration/otel-metrics.int.spec.ts`
- Create: `docs/metrics.md`
- Modify: `README.md` (feature bullet at line 23, `@opentelemetry/api` row at line 37, docs table at line 116)
- Modify: `docs/plugins.md`, `docs/tracing.md` (one link each to `./metrics.md`)
- Modify: `CHANGELOG.md:14` (add metrics to the 0.1.0 bullet that mentions tracing)
- Modify: `scripts/check-docs.mjs` (`STUB_MODULES`: add `@opentelemetry/exporter-metrics-otlp-http` with `export class OTLPMetricExporter { constructor(options?: Record<string, unknown>); }`)
- Modify: `test/smoke/esm-nest12/index.mjs`, `test/smoke/cjs-nest11/index.cjs` (import `metrics` next to `tracing`; `assert.equal(typeof metrics, 'function')`)

**Interfaces:**
- Consumes: `metrics()` from Tasks 1–2 and `setupMetrics()`.

- [ ] **Step 1: Write the integration test** in `test/integration/otel-metrics.int.spec.ts`

  Follow `test/integration/otel.int.spec.ts` (`startNats`, `createTestApp`, `eventually`), with `plugins: [metrics()]` and `setupMetrics()` in `beforeAll`.
  - `@Consume({ stream: 'MTR', durable: 'mtr', config: { filter_subject: 'mtr.fail' }, retry: { maxRetries: 1, initialDelay: 10 }, dlq: { subject: 'mtr.dlq' } })` that always throws a plain `Error`. Declare `streams: [{ name: 'MTR', subjects: ['mtr.fail'] }, { name: 'MTRDLQ', subjects: ['mtr.dlq'] }]`, because the DLQ publishes in `'jetstream'` mode by default and a stream must capture the subject. After publishing one message, `eventually`:
    - the consumed counter has `nats.outcome: 'retry'` → 1 and `'dlq'` → 1, with `nats.handler` set;
    - the sent counter has a point with `nats.dead_letter: true`.
  - A request to a `@Subscribe('mtr.get')` handler gives consumed `nats.outcome: 'replied'` and sent `nats.publish.kind: 'request'`.
  - `JetStreamClient.publish('mtr.fail', …)` gives sent `nats.publish.kind: 'jetstream'`.

- [ ] **Step 2: Run it**

  Run: `<integration env> npx vitest run --project integration test/integration/otel-metrics.int.spec.ts`
  Expected: PASS. It passes immediately because Tasks 1–2 implemented the behavior. If it fails, that is a bug in Tasks 1–2: fix it there and add a unit test that pins it.

- [ ] **Step 3: Write `docs/metrics.md`**

  Structure it like `docs/tracing.md`:
  1. `[Back to the README](../README.md)`.
  2. Install, and "Setting up OpenTelemetry is your job". The `ts` example uses `NodeSDK` with `metricReader: new PeriodicExportingMetricReader({ exporter: new OTLPMetricExporter() })` (`PeriodicExportingMetricReader` from `@opentelemetry/sdk-metrics`) and `plugins: [tracing(), metrics()]`.
  3. Options table: `meterName`, `subjects`. Include the series-count warning for `subjects` and a mapping-function example.
  4. Metrics table and attributes table, as in spec §4.1–4.2.
  5. "Counting": the bullets of spec §4.4, plus "a plugin after `metrics()` that never passes a message on: nothing is recorded for it".
  6. "Created before the SDK": records reach a provider registered later.

  Add the links in README, `docs/plugins.md` and `docs/tracing.md`, the CHANGELOG mention, the docs-check stub and the smoke imports.

- [ ] **Step 4: Run the full verification**

  Run: `npm test && npm run typecheck && npm run lint && npm run docs:check && npm run check:pkg && npm run test:smoke && <integration env> npm run test:int`
  Expected: all PASS. `docs:check` reports more blocks than before (40) and no broken links. Smoke prints `SMOKE OK` three times.

- [ ] **Step 5: Commit**

  ```bash
  git add test/integration/otel-metrics.int.spec.ts docs/metrics.md docs/plugins.md docs/tracing.md README.md CHANGELOG.md scripts/check-docs.mjs test/smoke
  git commit -m "docs(otel): metrics plugin, with integration and smoke coverage"
  ```
