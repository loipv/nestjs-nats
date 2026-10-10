# `@loipv/nestjs-nats` — Plan 5: Push consumer, ordered consumer, atomic batch publish

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thêm `@ConsumePush` (durable push, `deliverGroup`), `@ConsumeOrdered` (ordered pull, retry tại chỗ → DLQ) và `JetStreamClient.publishAtomic([...])` (atomic batch, NATS ≥ 2.12), kèm testing harness, docs và example, phát hành cùng 0.1.0.

**Architecture:** Push dùng lại `JetStreamConsumerAdapter` với một intake khác (đọc ngay, không chờ slot; `max_ack_pending` chặn hàng đợi) và vẫn là handler `kind: 'consume'` (thêm `delivery: 'push'`). Ordered là handler `kind: 'ordered'` với adapter mới, xử lý tuần tự; settlement dùng nhánh core (scheduler retry do adapter cung cấp, adapter chờ rồi chạy lại tại chỗ) cộng retry DLQ có `AbortSignal`. Atomic chạy từng message qua outbound plugin, bước cuối "stage" thay vì gửi, rồi `startBatch/add/commit` một lần; `next()` của plugin nhận kết quả commit.

**Tech Stack:** TypeScript ~6.0.3, NestJS 11/12, `@nats-io/jetstream` 3.4 (`consumers.getPushConsumer`, `consumers.get(stream, OrderedConsumerOptions)`, `startBatch`), vitest 5 + Testcontainers `nats:2.12-alpine`, Biome.

**Spec:** `docs/superpowers/specs/2026-10-10-consumers-atomic-design.md` (spec gốc: `docs/superpowers/specs/2026-10-09-nestjs-nats-design.md`).

**Ruling khi lập plan** (Task 1 sửa spec §4 cho khớp):
- Push **không** có kind riêng: `HandlerDefinition`/`HandlerInfo` giữ `kind: 'consume'` và thêm `delivery: 'pull' | 'push'` — settlement, DLQ subject mặc định `dlq.<stream>.<durable>`, idempotency scope `consume:<stream>/<durable>|`, validator trùng durable, harness đều dùng lại nguyên vẹn. Ordered dùng kind `'ordered'` (không phải `'consume-ordered'`), nên scope idempotency mặc định `ordered:<Class>.<method>` khớp spec §3.2 mà không cần code riêng.
- DLQ mặc định của ordered: subject `dlq.<stream>.ordered.<Class>.<method>`, mode `'jetstream'`.
- `@CloudEventRouter` không hỗ trợ push/ordered (chỉ `subscribe`/`consume` pull như hiện tại) — ghi vào docs, không code.

## Global Constraints

- Node `>= 22.12`; NestJS `11.x` và `12.x`; ESM-only; `@nats-io/*` `3.x` (đang dùng 3.4.0); TypeScript `~6.0.3`. Không thêm `dependencies` hay `devDependencies`.
- Mọi lỗi/log không lộ credential: lỗi transport/server bọc lại dùng `redactError` (`src/connection/redact.ts`) cho `cause`.
- Mọi export public mới phải có trong `src/index.ts` và trong kỳ vọng của `test/unit/public-api.spec.ts`.
- Shutdown luôn có giới hạn: không `await` nào trong `stop()/idle()/forceStop()` được chờ vô hạn; chờ retry phải huỷ được.
- Integration test: `nats:2.12-alpine` qua `test/integration/support/nats-server.ts`; local cần `DOCKER_HOST=unix:///Users/hungvv/.colima/default/docker.sock TESTCONTAINERS_RYUK_DISABLED=true`.
- Lệnh kiểm tra: `npm run lint`, `npm run typecheck`, `npm test`, `npm run test:int`, `npm run docs:check`, `npm run build && npm run check:pkg && npm run check:pack`, `npm run example:check`.
- Commit: `git -c user.email=loipv@bamboosoft.io -c user.name=loipv commit -m "<conventional>"`, không trailer attribution. Không push, không publish.
- Giữ nguyên hành vi `@Consume`, `@Subscribe`, `@ServiceEndpoint`, `publish`, `publishMany`, `batcher()`: toàn bộ test hiện có phải xanh sau mỗi task.

## Review Focus

1. **Push có `concurrency` lớn hơn `max_ack_pending` của consumer có sẵn** (vd. consumer `max_ack_pending: 1`, `concurrency: 4`) → chạy đúng, không treo, không lỗi. Test ở Task 2.
2. **Ordered: handler ném liên tục, `retry.initialDelay` dài (60 s), app shutdown** → `stop()` + `idle()` xong trong < 200 ms, log `abandoned at shutdown`. Test ở Task 4.
3. **Ordered: DLQ subject không có stream nào bắt** → retry publish DLQ vô hạn, mỗi lần log error, dừng ngay khi shutdown (outcome `drop` kèm lỗi). Test ở Task 3.
4. **Atomic: plugin async chậm ở message đầu, nhanh ở message sau** → thứ tự trên stream vẫn theo mảng. Test ở Task 5.
5. **Atomic: connection đóng giữa chừng** → `publishAtomic` reject (không treo quá timeout), stream không có message nào của batch. Test ở Task 5.

---

## File structure

```
src/decorators/handler.decorators.ts     # T1: ConsumePushOptions, ConsumeOrderedOptions, OrderedDeliverPolicy, @ConsumePush, @ConsumeOrdered
src/decorators/constants.ts              # T1: NATS_CONSUME_PUSH_METADATA, NATS_CONSUME_ORDERED_METADATA
src/discovery/handler-explorer.ts        # T1: kind 'ordered', consume.delivery
src/discovery/handler-validator.ts       # T1: luật mới
src/pipeline/delivery.ts                 # T1: HandlerInfo; T3: ABORT_SIGNAL, DeliveryInit.signal
src/jetstream/stream-manager.ts          # T2: ensureConsumer(createDefaults)
src/jetstream/stream-sync.ts             # T2: pushConsumerDefaults()
src/adapters/jetstream-consumer.adapter.ts  # T2: intake pull/push, kiểm tra push khi start
src/context/nats-ordered-context.ts      # T3 (mới): NatsOrderedContext
src/util/sleep.ts                        # T3 (mới): sleep(ms, signal)
src/settlement/settlement.ts, dlq.ts     # T3: nhánh ordered, consumer override, default subject
src/adapters/ordered-consumer.adapter.ts # T4 (mới)
src/module/nats-lifecycle.ts             # T1 (case tạm), T4 (wire adapter)
src/jetstream/atomic.ts                  # T5 (mới): publishAtomic + types
src/jetstream/jetstream-client.ts        # T5: publishAtomic()
src/errors/errors.ts                     # T5: NatsAtomicPublishError
src/pipeline/types.ts, src/otel/tracing.ts  # T5: OutgoingMessage.atomic, attribute
src/testing/fake-connection.ts           # T5: fake startBatch; T6: fake ordered JsMsg
src/testing/harness.ts                   # T6
src/index.ts, test/unit/public-api.spec.ts  # T1, T3, T5
docs/**, README.md, CHANGELOG.md, examples/orders/**, spec gốc §18  # T7
```

---

### Task 1: Decorator, definition và validation

**Files:**
- Modify: `src/decorators/handler.decorators.ts`, `src/decorators/constants.ts`, `src/discovery/handler-explorer.ts`, `src/discovery/handler-validator.ts`, `src/pipeline/delivery.ts` (`HandlerInfo`, `toHandlerInfo`), `src/module/nats-lifecycle.ts`, `src/testing/harness.ts` (chỉ cho exhaustive switch), `src/index.ts`, `docs/superpowers/specs/2026-10-10-consumers-atomic-design.md` §4
- Test: `test/unit/decorators.spec.ts`, `test/unit/handler-info.spec.ts`, `test/unit/handler-validator.spec.ts`, `test/unit/public-api.spec.ts`

**Interfaces:**
- Produces:
  - `interface ConsumePushOptions extends ConsumeOptions { deliverGroup?: string }`
  - `type OrderedDeliverPolicy = 'all' | 'last' | 'new' | 'last_per_subject' | { startSeq: number } | { startTime: Date | string }`
  - `interface ConsumeOrderedOptions { connection?; stream: string; filterSubjects?: string | string[]; deliverPolicy?: OrderedDeliverPolicy; headersOnly?: boolean; codec?: Codec; retry?: RetryOptions | false; dlq?: DlqOptions | boolean; idempotency?: boolean | IdempotencyOptions }`
  - `ConsumePush(options): MethodDecorator`, `ConsumeOrdered(options): MethodDecorator`; metadata `'nats:consume-push'`, `'nats:consume-ordered'`.
  - `HandlerKind = 'subscribe' | 'consume' | 'ordered' | 'service-endpoint'`; spec `{ kind: 'consume'; options: ConsumeOptions | ConsumePushOptions; delivery: 'pull' | 'push' }` và `{ kind: 'ordered'; options: ConsumeOrderedOptions }`.
  - `HandlerInfo` consume thêm `readonly delivery: 'pull' | 'push'`; variant mới `{ kind: 'ordered'; stream: string; filterSubjects: readonly string[] }` (chuỗi đơn → mảng 1 phần tử, không khai báo → `[]`).

- [ ] **Step 1: Viết test thất bại**
  - `decorators.spec`: `@ConsumePush({ stream: 'S', durable: 'D', deliverGroup: 'g' })` lưu `{ options }` dưới `'nats:consume-push'`; tương tự `@ConsumeOrdered` dưới `'nats:consume-ordered'`.
  - `handler-info.spec`: explorer trả def `@ConsumePush` với `kind: 'consume', delivery: 'push'`; `@Consume` có `delivery: 'pull'`; `toHandlerInfo` của `@ConsumeOrdered({ stream: 'S', filterSubjects: 'orders.>' })` = `{ kind: 'ordered', stream: 'S', filterSubjects: ['orders.>'], connection: 'default', className, methodName }`.
  - `handler-validator.spec` (mỗi case `toThrow(NatsConfigError)` + đoạn message):
    - `@Consume` và `@ConsumePush` cùng `S/D` → `'both consume durable "D"'`.
    - `deliverGroup: 'a'` + `config: { deliver_group: 'b' }` → `'deliverGroup "a" conflicts with config.deliver_group "b"'`.
    - `@ConsumePush` `ack: 'manual'` + `batch` → message batch hiện có.
    - ordered `deliverPolicy: { startSeq: 0 }` → `'invalid deliverPolicy.startSeq 0; expected an integer >= 1'`; `{ startTime: 'nope' }` → `'invalid deliverPolicy.startTime'`.
    - ordered có `concurrency: 2` (ép kiểu `as never`) → `'@ConsumeOrdered does not support concurrency'`; tương tự `durable`, `batch`, `ack`, `config`.
    - ordered với `retry: { maxRetries: -1 }` → lỗi retry hiện có.
  - `public-api.spec`: export `ConsumePush`, `ConsumeOrdered` và type `ConsumePushOptions`, `ConsumeOrderedOptions`, `OrderedDeliverPolicy`.
- [ ] **Step 2: Chạy, xác nhận FAIL** — `npx vitest run --project unit test/unit/decorators.spec.ts test/unit/handler-info.spec.ts test/unit/handler-validator.spec.ts test/unit/public-api.spec.ts`
- [ ] **Step 3: Cài đặt** theo Interfaces. `specsFor` đọc hai metadata mới; `@Consume` và spec `consume` của `defineRouter` có `delivery: 'pull'`. (Một method mang nhiều handler decorator vẫn được phép như hiện tại — mỗi decorator một definition.) Validator: luật ordered và `deliverGroup` thêm vào `validateHandlers`; retry/dlq/idempotency áp cho cả `'ordered'` (sửa guard `def.kind === 'subscribe' || def.kind === 'consume'`, nhưng `validateBatch` và `concurrency` chỉ cho subscribe/consume). `nats-lifecycle.ts` `createAdapters`: `case 'ordered': throw new NatsConfigError('@ConsumeOrdered is not wired yet')` (Task 4 thay). `harness.ts`: `subjectsOf` trả `filterSubjects` cho ordered. `resolveDlqPolicy(kind, ...)` nhận `'ordered'` (mode mặc định `'jetstream'`). Sửa spec §4 theo Ruling đầu plan, và xoá dòng "Một method chỉ được mang một handler decorator" (sai với code: explorer cho phép nhiều decorator trên một method).
- [ ] **Step 4: Chạy lại + toàn bộ unit** — `npm test`, `npm run typecheck`, `npm run lint`. Kỳ vọng: PASS.
- [ ] **Step 5: Commit** — `feat(decorators): @ConsumePush and @ConsumeOrdered definitions and validation`

---

### Task 2: Push runtime

**Files:**
- Modify: `src/adapters/jetstream-consumer.adapter.ts`, `src/jetstream/stream-manager.ts`, `src/jetstream/stream-sync.ts`
- Test: `test/unit/jetstream-consumer.adapter.spec.ts`, `test/unit/stream-sync.spec.ts`, `test/integration/consume-push.int.spec.ts` (mới)

**Interfaces:**
- Consumes: Task 1 `def.delivery`, `ConsumePushOptions.deliverGroup`.
- Produces:
  - `pushConsumerDefaults(o: { stream: string; durable: string; deliverGroup?: string; maxAckPending: number }): Partial<ConsumerConfig>` = `{ deliver_subject: '_nats.push.<stream>.<durable>', deliver_group?, flow_control: true, idle_heartbeat: 5_000_000_000, max_ack_pending }`.
  - `NatsStreamManager.ensureConsumer(stream, cfg, mode?, createDefaults?: Partial<ConsumerConfig>)` — `createDefaults` chỉ trộn vào khi **tạo** consumer (`{ ...createDefaults, ...withConsumerDefaults(cfg) }`, khai báo của user thắng), không tham gia so drift.

- [ ] **Step 1: Viết test unit thất bại** (`jetstream-consumer.adapter.spec`, fake consumer có sẵn trong file):
  - `push intake reads every message without waiting for a slot`: `delivery: 'push'`, `concurrency: 1`, handler chặn bằng deferred, fake push consumer yield 5 message → sau `start()` và một tick, cả 5 nằm trong tập heartbeat: tiến fake timer `ack_wait/2` → `working()` được gọi trên 5 message.
  - `push start rejects a pull consumer`: `info().config` không có `deliver_subject` → `start()` reject `NatsConfigError` chứa `'is not a push consumer'`.
  - `push start rejects a deliver_group mismatch`: options `deliverGroup: 'a'`, server `deliver_group: 'b'` → `'deliver_group "b"'`; options không có, server `'b'` → cũng lỗi.
  - `push start rejects unlimited max_ack_pending`: `max_ack_pending: -1` → `'max_ack_pending'`.
  - Review Focus 1: `max_ack_pending: 1`, `concurrency: 4`, 3 message lần lượt → cả 3 được xử lý và ack, `idle()` resolve.
  - `stream-sync.spec`: `pushConsumerDefaults({ stream: 'S', durable: 'D', deliverGroup: 'g', maxAckPending: 2 })` = object trên; `ensureConsumer` với `createDefaults` gọi `consumers.add` có các field đó, khi consumer đã có thì không gọi update vì `createDefaults`.
- [ ] **Step 2: Chạy, xác nhận FAIL** — `npx vitest run --project unit test/unit/jetstream-consumer.adapter.spec.ts test/unit/stream-sync.spec.ts`
- [ ] **Step 3: Cài đặt.** `start()`: khi `delivery === 'push'`, gọi `ensureConsumer(stream, { ...config, durable_name }, undefined, pushConsumerDefaults({ ..., maxAckPending: batch ? max(batch.maxSize, concurrency) : concurrency }))`, lấy `js().consumers.getPushConsumer(stream, durable)` (lỗi not-found map sang `NatsConfigError` như `getConsumer`), kiểm tra `info.config` (3 luật trên, message nêu `"<stream>/<durable>"` và `Class.method`), rồi `consume()` không option. Vòng lặp push: `unsettled.set(msg)` rồi `void this.dispatcher.push(msg, msg.info.deliveryCount).catch(log)` (không `await` — limiter đã FIFO nên thứ tự vào slot giữ nguyên; đọc ngay cũng là điều kiện để nats.js trả lời flow-control kịp). Pull giữ nguyên. Label thêm hậu tố ` (push)`. `watchStatus` bỏ qua `flow_control`/`heartbeat`.
- [ ] **Step 4: Chạy unit, PASS.**
- [ ] **Step 5: Viết integration** `consume-push.int.spec.ts`:
  - `shares messages across a deliver group`: 2 app, cùng `@ConsumePush({ stream, durable: 'p1', deliverGroup: 'g', config: {} })`, publish 20 → hợp hai tập id = 20 id, không trùng, mỗi app ≥ 1.
  - `creates the push consumer with its defaults`: `jsm.consumers.info` có `deliver_subject: '_nats.push.<stream>.p2'`, `flow_control: true`, `max_ack_pending: 2` (với `concurrency: 2`).
  - `heartbeats queued messages`: `config: { ack_wait: 2_000_000_000 }`, `concurrency: 2`, handler 5 s, publish 4 → mọi lần gọi handler có `ctx.deliveryCount === 1` và tổng 4 lần gọi.
  - `retries then dead-letters`: `retry: { maxRetries: 1, initialDelay: 50 }`, `dlq: true`, handler luôn ném → DLQ stream nhận 1 message, handler gọi 2 lần.
  - `rejects a pull consumer`: tạo pull consumer `p3` trước, `@ConsumePush` durable `p3` không `config` → bootstrap reject `NatsConfigError`.
  - `naks in-flight messages at shutdown`: như test tương ứng của `consume.int.spec.ts` nhưng push.
- [ ] **Step 6: Chạy integration, PASS** — `npx vitest run --project integration test/integration/consume-push.int.spec.ts` (kèm env Colima), rồi `npm test`.
- [ ] **Step 7: Commit** — `feat(consume): durable push consumers with deliver groups`

---

### Task 3: Ordered context, settlement và DLQ

**Files:**
- Create: `src/context/nats-ordered-context.ts`, `src/util/sleep.ts`
- Modify: `src/context/nats-context.ts` (`NatsContextKind` thêm `'ordered'`), `src/pipeline/delivery.ts`, `src/settlement/settlement.ts`, `src/settlement/dlq.ts`, `src/index.ts`
- Test: `test/unit/nats-context.spec.ts`, `test/unit/settlement.spec.ts`, `test/unit/dlq.spec.ts`, `test/unit/sleep.spec.ts` (mới), `test/unit/public-api.spec.ts`

**Interfaces:**
- Consumes: Task 1 kind `'ordered'`.
- Produces:
  - `class NatsOrderedContext extends NatsContext<JsMsg>` — `constructor(init: { connection: string; raw: JsMsg; attempt: number })` (kind `'ordered'`); getters `stream`, `streamSequence`, `deliverySequence`, `pending` (từ `raw.info`), `attempt`; `replyTo` luôn `undefined`; `ack()`, `nak()`, `term()`, `working()` ném `new NatsConfigError('ordered consumer messages cannot be settled')`.
  - `sleep(ms: number, signal?: AbortSignal): Promise<boolean>` — `true` khi hết giờ, `false` khi bị abort (kể cả abort trước khi gọi); timer `unref()`.
  - `ABORT_SIGNAL: unique symbol` trên `InternalDelivery` (`readonly [ABORT_SIGNAL]?: AbortSignal`), `DeliveryInit.signal?: AbortSignal`.
  - `sendToDlq(entry, subject, mode, msg, err, deliveries, consumer?: string)` và `buildDlqHeaders(msg, err, deliveries, now, consumer?: string)`: `consumer` thay `info.consumer` trong `Nats-DLQ-Consumer` và `Nats-Msg-Id` (`dlq:<stream>:<consumer>:<seq>`).
  - `defaultSubject` của ordered: `dlq.<stream>.ordered.<className>.<methodName>`.

- [ ] **Step 1: Viết test thất bại**
  - `nats-context.spec`: getters đọc từ fake `raw.info` (`stream: 'S', streamSequence: 42, deliverySequence: 7, pending: 3`), `attempt` từ init; `ack()` ném `NatsConfigError` đúng message; `@Ctx()` của ordered là `NatsOrderedContext` (qua harness ở Task 6 — ở đây chỉ class).
  - `sleep.spec` (fake timers): `sleep(100)` → `true` sau 100 ms; abort ở 50 ms → `false` ngay; signal đã abort → `false`.
  - `dlq.spec`: `buildDlqHeaders(msg, err, 2, now, 'ordered:Proj.on')` → `Nats-DLQ-Consumer: 'ordered:Proj.on'`, `Nats-Msg-Id: 'dlq:ORDERS:ordered:Proj.on:42'`; `resolveDlqPolicy('ordered', undefined, true)` cho info ordered `{ stream: 'ORDERS', className: 'Proj', methodName: 'on' }` → subject `'dlq.ORDERS.ordered.Proj.on'`, mode `'jetstream'`.
  - `settlement.spec`, delivery ordered (handler kind `'ordered'`, scheduler `retry` spy, `signal`):
    - thành công → outcome `none`, không publish.
    - lỗi, attempt 1, `maxRetries: 2` → scheduler gọi với `(msg, delay)`, outcome `retry`.
    - lỗi, attempt 3 → DLQ publish với `consumer = 'ordered:<Class>.<method>'`, outcome `dlq`.
    - `NonRetryableError` attempt 1 → DLQ ngay; DLQ tắt → outcome `drop`, log error.
    - Review Focus 3: DLQ publish ném 2 lần rồi thành công (fake timers) → 3 lần publish, mỗi lần lỗi log error có `'retrying in'`, outcome cuối `dlq`; một case khác abort signal trong lúc chờ → dừng, outcome `{ action: 'drop', error: <lỗi handler> }`, log warn `'abandoned at shutdown'`.
  - `public-api.spec`: export `NatsOrderedContext`.
- [ ] **Step 2: Chạy, xác nhận FAIL** — `npx vitest run --project unit test/unit/nats-context.spec.ts test/unit/sleep.spec.ts test/unit/dlq.spec.ts test/unit/settlement.spec.ts`
- [ ] **Step 3: Cài đặt.** Trong `createSettlement.handle`: `kind === 'ordered'` đi `settleSubscribe` nhưng nhánh dead-letter là `deadLetterOrdered`: gọi `deadLetter(...)` (truyền consumer `ordered:<Class>.<method>`); lỗi → `delay = min(policy.maxDelay, backoffDelay(retry || DEFAULT_RETRY, n))` với `n` = số lần thử DLQ, log error, `await sleep(delay, signal)`; bị abort → warn + outcome `drop`. Không có `signal` (không xảy ra với adapter) → hành xử như core (drop). `decide(...)` với ordered dùng `retry` khi có scheduler (như core).
- [ ] **Step 4: Chạy lại + `npm test`, PASS.**
- [ ] **Step 5: Commit** — `feat(ordered): ordered context, settlement and dead letters`

---

### Task 4: `OrderedConsumerAdapter`

**Files:**
- Create: `src/adapters/ordered-consumer.adapter.ts`
- Modify: `src/module/nats-lifecycle.ts` (thay case tạm của Task 1), `src/adapters/jetstream-consumer.adapter.ts` (export `isPowerOfTwo` hoặc chuyển sang `src/util/`)
- Test: `test/unit/ordered-consumer.adapter.spec.ts` (mới), `test/integration/consume-ordered.int.spec.ts` (mới)

**Interfaces:**
- Consumes: Task 3 `NatsOrderedContext`, `sleep`, `DeliveryInit.signal`, `DeliveryInit.retry`; `createHandlerPipeline`, `createDelivery`.
- Produces: `class OrderedConsumerAdapter implements HandlerAdapter` — `constructor(def: HandlerDefinition & { kind: 'ordered' }, entry: ConnectionEntry)`.

Vòng lặp (thuật toán — không suy ra được từ chữ ký):
```
for await (msg of messages):
  attempt = 1
  loop:
    requested = undefined
    d = createDelivery({ ..., parts: [{ message: {..., raw: msg, attempt}, context: new NatsOrderedContext({ connection, raw: msg, attempt }) }],
                         retry: (_m, delay) => { requested = delay }, signal: this.abort.signal })
    try { await run(d) } catch (err) { log error; requested = backoffDelay(retry || DEFAULT_RETRY, attempt) }
    if requested === undefined: break
    this.waiting = true; ok = await sleep(requested, signal); this.waiting = false
    if !ok: warn `Retry of <label> (stream seq <n>) abandoned at shutdown`; return
    attempt++
```

- [ ] **Step 1: Viết test unit thất bại** (fake ordered consumer: `consume()` trả async iterator điều khiển được + `status()`):
  - `processes messages in stream order with inline retries`: message 2 ném 2 lần rồi thành công (`retry: { maxRetries: 3, initialDelay: 10 }`, fake timers) → thứ tự gọi handler `[1, 2, 2, 2, 3]`; `attempt` trong context là 1,2,3 cho message 2.
  - Review Focus 2: handler luôn ném, `initialDelay: 60_000` → sau lần ném đầu, `stop()` rồi `idle()` resolve trong < 200 ms (thời gian thật), warn chứa `'abandoned at shutdown'`, `forceStop()` → `{ nakked: 0, cancelledRetries: 0 }` (đã huỷ ở `stop()`); trường hợp gọi `forceStop()` khi đang chờ mà chưa `stop()` → `cancelledRetries: 1`.
  - `start maps deliverPolicy`: `{ startSeq: 5 }` → `consumers.get` nhận `{ deliver_policy: 'by_start_sequence', opt_start_seq: 5 }`; `{ startTime: new Date(0) }` → `'by_start_time'`, `opt_start_time: '1970-01-01T00:00:00.000Z'`; `filterSubjects: 'a.>'` → `filter_subjects: ['a.>']`; `headersOnly: true` → `headers_only: true`.
  - `stream missing` → `start()` reject `NatsConfigError` chứa tên stream.
  - status `ordered_consumer_recreated` → `logger.debug`; `heartbeats_missed` count 1,2,3,4 → warn ở 1, 2, 4.
- [ ] **Step 2: Chạy, FAIL** — `npx vitest run --project unit test/unit/ordered-consumer.adapter.spec.ts`
- [ ] **Step 3: Cài đặt** adapter và wire trong `createAdapters` (`case 'ordered': new OrderedConsumerAdapter(def, entry)`). `stop()`: `abort.abort()`, đóng `messages` (không await, log lỗi như adapter JetStream). `idle()`: `await this.loop`. `forceStop()`: `cancelledRetries = this.waiting ? 1 : 0`, abort.
- [ ] **Step 4: Chạy unit, PASS.**
- [ ] **Step 5: Viết integration** `consume-ordered.int.spec.ts`:
  - `keeps stream order across retries`: publish `n=1..5`, handler ném 2 lần ở `n=3` → chuỗi `[1,2,3,3,3,4,5]`.
  - `fans out to every instance`: 2 app → mỗi app nhận đủ 5.
  - `dead-letters once across replicas`: 2 app, handler luôn ném ở `n=2`, `retry: false`, `dlq: true`, DLQ stream bắt `dlq.>` → DLQ stream có đúng 1 message, header `Nats-DLQ-Consumer` = `ordered:<Class>.<method>`.
  - `recovers after the consumer is deleted`: xoá consumer phía server (`jsm.consumers.delete(stream, ctx.raw.info.consumer)`), publish thêm 2 → nhận cả 2.
  - `starts from a stream sequence`: publish 5 trước, `deliverPolicy: { startSeq: 3 }` → nhận `[3,4,5]`.
- [ ] **Step 6: Chạy integration + `npm test`, PASS.**
- [ ] **Step 7: Commit** — `feat(ordered): ordered consumer adapter`

---

### Task 5: `publishAtomic`

**Files:**
- Create: `src/jetstream/atomic.ts`
- Modify: `src/jetstream/jetstream-client.ts`, `src/errors/errors.ts`, `src/pipeline/types.ts`, `src/otel/tracing.ts`, `src/testing/fake-connection.ts`, `src/index.ts`
- Test: `test/unit/atomic.spec.ts` (mới), `test/unit/otel-tracing.spec.ts`, `test/unit/public-api.spec.ts`, `test/integration/publish-atomic.int.spec.ts` (mới)

**Interfaces:**
- Produces (đúng tên/kiểu như spec §2.3): `AtomicMessage`, `AtomicPublishOptions`, `AtomicPublishAck`, `JetStreamClient.publishAtomic(messages, opts?)`; `publishAtomic(entry: ConnectionEntry, messages: readonly AtomicMessage[], opts?: AtomicPublishOptions): Promise<AtomicPublishAck>` trong `atomic.ts`; `class NatsAtomicPublishError extends NatsModuleError { readonly subject: string; readonly count: number }`; `OutgoingMessage.atomic?: { index: number; count: number }`; hằng `MAX_ATOMIC_MESSAGES = 1000`, `DEFAULT_ATOMIC_TIMEOUT = 5000` (dùng khi `entry.js().getOptions?.().timeout` không có).
- Fake JetStream (`fake-connection.ts`): `startBatch(subj, payload, opts)` → `{ add(subj, payload, opts), commit(subj, payload, opts) }` ghi từng message vào `jsPublished` với header `Nats-Batch-Id`/`Nats-Batch-Sequence` (+ `Nats-Batch-Commit: '1'` ở message cuối) như nats.js, `PublishedMessage.atomic = { batch, index, count }` (gán `count` lúc commit), trả `{ stream: 'mock', seq, count, batch, duplicate: false }`.

Staging (thuật toán):
```
staged = new Array(n); commit = deferred<void>(); aborted = false
chains = messages.map((m, i) => compose(outboundMiddlewares(entry), async (om) => {
  staged[i] = { subject: om.subject, payload: encodeOutgoing(om), headers: om.headers }
  if (staged.filter(Boolean).length === n) void send()   // gửi khi đủ
  await commit.promise                                   // next() của plugin nhận kết quả commit
})(outgoing[i]))
try { await Promise.all(chains) } catch (err) { if !sent: commit.reject(new NatsAtomicPublishError('aborted: ...', { cause })) ; throw err gốc }
```
`send()`: `b = await js.startBatch(s0.subject, s0.payload, { headers, expect, timeout })`, `b.add(...)` cho 1..n-2, `ack = await b.commit(last, { headers, timeout })`; lỗi → `NatsAtomicPublishError(`Atomic publish of ${n} messages to "${subject0}" failed: ${redactText(message)}`, { cause: redactError(err), subject, count })`. Các chuỗi được khởi chạy theo index nhưng ghi theo index nên plugin async không đảo thứ tự. `n === 1`: không staging, đi `JetStreamClient.publish`-như-cũ nhưng không gắn `Nats-Msg-Id` từ CloudEvent, trả `{ ...pubAck, count: 1, batch: undefined, duplicate: false }`.

- [ ] **Step 1: Viết test unit thất bại** (`atomic.spec`, mock client từ `/testing` + plugin tự viết):
  - validate: `[]` và 1001 message → `NatsConfigError` `'publishAtomic needs 1 to 1000 messages'`; header `{ 'nats-msg-id': 'x' }` → `NatsConfigError` chứa `'Nats-Msg-Id'`; không message nào được ghi.
  - `commits in array order`: 3 message → `published` theo thứ tự, header `Nats-Batch-Sequence` `'1','2','3'`, chỉ message cuối có `Nats-Batch-Commit`; ack `{ count: 3, batch: <id> }`.
  - Review Focus 4: plugin chờ 20 ms cho message index 0, 0 ms cho index 1 → thứ tự ghi `[m0, m1]`.
  - `aborts when a plugin throws`: plugin ném ở index 1 → `publishAtomic` reject đúng lỗi plugin; `published` rỗng; `next()` của plugin ở index 0 reject `NatsAtomicPublishError` message chứa `'aborted'`.
  - `plugins see the commit failure`: fake `commit` ném → mọi `next()` reject cùng `NatsAtomicPublishError`, `cause` không chứa chuỗi `'s3cretpw'` khi lỗi gốc có URL `nats://u:s3cretpw@h`.
  - `single message`: 1 message → không có header `Nats-Batch-*`, ack `count: 1`, `batch: undefined`.
  - `cloudEvent`: message có `cloudEvent` → không có header `Nats-Msg-Id`.
  - `timeout`: spy `startBatch`/`commit` nhận `timeout: 5000` khi không truyền; `opts.timeout: 1234` → `1234`.
  - `OutgoingMessage.atomic`: plugin thấy `{ index, count: 3 }`.
  - `otel-tracing.spec`: span publish của message atomic có attribute `messaging.batch.message_count: 3`.
  - `public-api.spec`: export `NatsAtomicPublishError` và các type.
- [ ] **Step 2: Chạy, FAIL** — `npx vitest run --project unit test/unit/atomic.spec.ts test/unit/otel-tracing.spec.ts test/unit/public-api.spec.ts`
- [ ] **Step 3: Cài đặt** theo Interfaces + thuật toán.
- [ ] **Step 4: Chạy unit + `npm test`, PASS.**
- [ ] **Step 5: Viết integration** `publish-atomic.int.spec.ts` (stream khai báo qua `NatsModule.forFeature({ streams: [{ name, subjects, allow_atomic: true }] })`):
  - `stores every message`: 3 message → ack `count: 3`; `jsm.streams.info` có `state.messages === 3`; seq của message cuối = `ack.seq`.
  - `rejects on a stream without allow_atomic`: → `NatsAtomicPublishError`; stream đó vẫn 0 message.
  - `stores nothing when a plugin throws`.
  - Review Focus 5: bắt đầu `publishAtomic` 500 message rồi `nc.close()` ngay → reject trong ≤ `timeout + 1000` ms; stream có 0 message (batch chưa commit bị server bỏ).
  - `traces produce → consume`: OTel test setup như `otel.int.spec.ts`; `@Consume` nhận message index 1 có cùng `traceId` với span publish của nó.
- [ ] **Step 6: Chạy integration, PASS.**
- [ ] **Step 7: Commit** — `feat(jetstream): atomic batch publish`

---

### Task 6: Testing harness và mock

**Files:**
- Modify: `src/testing/harness.ts`, `src/testing/fake-connection.ts` (fake JsMsg cho ordered nếu cần)
- Test: `test/unit/testing-harness.spec.ts`, `test/unit/testing-mocks.spec.ts`

**Interfaces:**
- Consumes: Task 1–5.
- Produces: `DispatchOptions.streamSequence?: number` (mặc định 1, kiểm tra như `positiveInteger`), dùng cho `NatsJetStreamContext.seq`/`NatsOrderedContext.streamSequence` và header DLQ. `describeHandler` hiển thị `(consume push S/D …)` và `(ordered S …)`.

- [ ] **Step 1: Viết test thất bại**
  - push: `dispatch({ handler: 'Svc.onPush' }, payload)` → `settlement: 'ack'`; ném + retry → `'nak'` + `nakDelay`.
  - ordered: thành công → `'none'`, `@Ctx()` là `NatsOrderedContext` với `streamSequence` = `options.streamSequence`; ném với retry → `'retry'` + `retryDelay` (harness không chạy lại); `attempt: 3` hết retry + DLQ → `'dlq'`, `dlq[0].headers['Nats-DLQ-Consumer'] === 'ordered:Svc.onOrdered'`; DLQ tắt → `'drop'`; `deliveryCount` bị bỏ qua.
  - `testing-mocks.spec`: `MockJetStreamClient.publishAtomic` 2 message → `published` có 2 phần tử với `atomic: { batch, index: 0|1, count: 2 }`.
- [ ] **Step 2: Chạy, FAIL** — `npx vitest run --project unit test/unit/testing-harness.spec.ts test/unit/testing-mocks.spec.ts`
- [ ] **Step 3: Cài đặt.** Ordered dùng `retry: () => {}` như subscribe và một `AbortSignal` mới; mapping `retry` → `'retry'` cho ordered (không phải `'nak'`).
- [ ] **Step 4: Chạy + `npm test`, PASS.**
- [ ] **Step 5: Commit** — `feat(testing): harness and mocks for push, ordered and atomic`

---

### Task 7: Docs, example và release notes

**Files:**
- Create: `docs/consumers.md`
- Modify: `docs/handlers.md`, `docs/publishing.md`, `docs/streams.md`, `docs/retry-and-dlq.md`, `docs/lifecycle.md`, `docs/tracing.md`, `docs/testing.md`, `README.md`, `CHANGELOG.md`, `docs/superpowers/specs/2026-10-09-nestjs-nats-design.md` §18, `examples/orders/src/**`, `examples/orders/test/**`, `examples/orders/README.md`

- [ ] **Step 1: Docs.**
  - `consumers.md`: bảng pull / push / ordered gồm các cột khi nào dùng, chia tải hay fan-out, ack, heartbeat, shutdown, giới hạn. Phải có: push không `deliverGroup` thì chỉ một instance bind được; push đọc ngay và `max_ack_pending` bị giới hạn; ordered fan-out, không ack, message bỏ dở khi shutdown chỉ quay lại nếu `deliverPolicy` replay.
  - `handlers.md`: thêm mục `@ConsumePush` và `@ConsumeOrdered`, cập nhật hàng `@Ctx()`, ghi chú `@CloudEventRouter` chỉ hỗ trợ subscribe và consume pull.
  - `publishing.md`: thêm mục "Atomic batches" với yêu cầu server ≥ 2.12 + `allow_atomic`, giới hạn 1000 message / 10 s / 50 batch, không dedupe, khác `publishMany`/`batcher()` ở đâu.
  - `streams.md`: `allow_atomic`.
  - `retry-and-dlq.md`: hàng ordered trong bảng settlement, retry DLQ vô hạn.
  - `lifecycle.md`: ordered khi shutdown.
  - `tracing.md`: attribute `messaging.batch.message_count`.
  - `testing.md`: `streamSequence` và các settlement của ordered.
  - README: thêm feature, link vào bảng docs, Known limitations (ordered bỏ dở message khi shutdown; push không `deliverGroup` chỉ một instance; router chỉ pull/subscribe).
  - CHANGELOG 0.1.0: một dòng cho mỗi tính năng.
  - Spec gốc §18: xoá ordered consumer, push consumer, atomic batch publish, trỏ sang spec mới.
- [ ] **Step 2: Example.** `examples/orders`:
  - Thêm `@ConsumeOrdered` projection đếm đơn theo trạng thái.
  - `main.ts` tạo đơn bằng `publishAtomic` gồm `orders.created` và `stock.reserved`; stream của example bật `allow_atomic: true`.
  - Thêm test harness: projection nhận event → `settlement: 'none'`.
- [ ] **Step 3: Kiểm tra** — `npm run docs:check`, `npm run example:check`, `npm run build && npm run check:pkg && npm run check:pack`, `npm run lint`. Kỳ vọng: tất cả OK.
- [ ] **Step 4: Commit** — `docs: push and ordered consumers, atomic batch publish`
