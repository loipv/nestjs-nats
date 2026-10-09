# `@loipv/nestjs-nats` — Plan 2: Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thay settlement tối giản của Plan 1 bằng retry + exponential backoff + DLQ, thêm idempotency, batch processing (consume) và batch publishing (`publishMany`/`Batcher`), và làm graceful shutdown có giới hạn thời gian thật sự — đủ để publish `0.1.0` lên npm an toàn.

**Architecture:** Hai adapter có hàng đợi (core subscription, JetStream consumer) dùng chung một `Dispatcher` (limiter, gom batch, lên lịch retry in-process, theo dõi message đang xử lý). Pipeline inbound giữ thứ tự của spec §6.1: `outer plugins → settlement → idempotency → inner plugins → invoker`; `createSettlement()` thay `basicSettlement`, quyết định ack/nak(delay)/term/DLQ theo từng message (kể cả trong batch). `NatsLifecycle` điều phối shutdown theo pha, mỗi pha bị chặn bởi `drainTimeout`; `NatsConnectionManager.closeAll()` chạy close hook (flush `Batcher`) rồi drain có timeout, quá hạn thì `close()` cứng.

**Tech Stack:** như Plan 1 — TypeScript 6.0.x ESM, NestJS 11/12, `@nats-io/*` 3.x, vitest 5 + `unplugin-swc`, Testcontainers (`nats:2.12-alpine`), Biome.

**Spec:** `docs/superpowers/specs/2026-10-09-nestjs-nats-design.md` (§7, §8.3, §10, §11, §14, §15). Follow-ups từ Plan 1: `docs/superpowers/notes/2026-10-09-plan1-followups.md`.

**Phạm vi** = spec §19 bước 2, cộng các follow-up Plan 1 giao cho Plan 2: `drainTimeout` không chặn được drain (bắt buộc trước khi publish), double logging, `settled` bị set trước khi gọi `raw`, request-path 5xx không được log, `drainTimeout` merge bỏ qua connection không đặt, `errorTrace` lặp lại. Ngoài phạm vi: CloudEvents (key idempotency từ CE `source|id` thêm ở Plan 3), OTel, health, `/testing`, docs site, CI matrix.

## Global Constraints

- Mọi Global Constraints của Plan 1 vẫn áp dụng: ESM-only với import `.js`; Node `>=22.12`; Nest `^11 || ^12`; không có `dependencies`; entry chính không import `@opentelemetry/api` / `@nestjs/terminus`; không log secret; TypeScript `~6.0.3`; commit Conventional Commits; mỗi task kết thúc bằng một commit.
- Mặc định retry (spec §7, chính xác): `maxRetries: 5`, `initialDelay: 1000`, `factor: 2`, `maxDelay: 60_000`, `jitter: 0.2`. Retry **bật** mặc định; `retry: false` để tắt.
- DLQ **tắt** mặc định. Subject mặc định `dlq.<stream>.<durable>` (consume) / `dlq.<subject thực tế>` (subscribe). `mode` mặc định `'jetstream'` cho consume, `'core'` cho subscribe.
- Header DLQ (chính xác): `Nats-DLQ-Subject`, `Nats-DLQ-Stream`, `Nats-DLQ-Consumer`, `Nats-DLQ-Seq`, `Nats-DLQ-Deliveries`, `Nats-DLQ-Error` (tối đa 1024 ký tự, bỏ CR/LF), `Nats-DLQ-Timestamp` (ISO 8601).
- Idempotency **tắt** mặc định; TTL mặc định `300_000` ms; `InMemoryIdempotencyStore` `maxKeys` mặc định `100_000`.
- Batch consume mặc định `maxSize: 100`, `maxWait: 1000` ms. Batcher (publish) mặc định `maxSize: 100`, `maxWait: 50` ms; `publishMany` `window` mặc định `256`.
- `shutdown.drainTimeout` mặc định `10_000` ms. Không bước shutdown nào được chờ vô hạn.
- Integration test: `DOCKER_HOST=unix:///Users/hungvv/.colima/default/docker.sock TESTCONTAINERS_RYUK_DISABLED=true`. Test thời gian dùng `initialDelay` nhỏ (≈50 ms) và `jitter: 0`.

## Review Focus

1. **DLQ bật với `mode: 'jetstream'` nhưng không stream nào bắt subject DLQ** → publish DLQ lỗi; message phải được `nak(backoff)` (không mất) **và** mỗi lần thất bại log error nêu rõ subject DLQ cùng gợi ý "no stream captures this subject". Test ở Task 3.
2. **Consumer có `max_deliver` > 0 và ≤ `maxRetries`** → warning lúc bootstrap nêu `Class.method`, stream/durable và hai con số (spec §7). Test ở Task 3.
3. **Handler chạy lâu hơn `ack_wait`** → heartbeat `working()` giữ message không bị redeliver; handler chỉ chạy đúng một lần. Test ở Task 3.
4. **NATS server mất kết nối lúc shutdown** → `app.close()` vẫn trả về trong khoảng `2 × drainTimeout` (+ sai số), không treo. Test ở Task 7.
5. **Idempotency store ném lỗi** → `claim` lỗi: message được retry (không mất, không xử lý trùng); `complete` lỗi: chỉ log warn, message vẫn được ack. Test ở Task 4.

---

## File structure

```
src/util/errors.ts                         # Task 2: errorMessage, errorTrace (gộp các bản copy)
src/settlement/retry.ts                    # Task 1
src/settlement/dlq.ts                      # Task 1 (config + headers), Task 3 (publish)
src/settlement/decide.ts                   # Task 1: quyết định retry/dlq/drop thuần
src/settlement/settlement.ts               # Task 3: createSettlement (thay basic-settlement.ts)
src/adapters/dispatcher.ts                 # Task 2 (single), Task 5 (batch)
src/idempotency/store.ts                   # Task 4
src/idempotency/idempotency.ts             # Task 4: middleware + key extraction
src/batch/collector.ts                     # Task 5
src/context/nats-batch-context.ts          # Task 5
src/batch/batcher.ts                       # Task 6
src/jetstream/jetstream-client.ts          # Task 6: publishMany, batcher
src/connection/connection-manager.ts       # Task 6: onClose hooks; Task 7: closeAll timeout
src/module/nats-lifecycle.ts               # Task 3 (wiring), Task 7 (shutdown phases)
src/decorators/handler.decorators.ts, src/module/options.ts, src/discovery/handler-validator.ts  # Task 1, 4, 5
src/pipeline/delivery.ts, invoker.ts, reply.ts, basic-settlement.ts (xoá ở Task 3)
src/index.ts, README.md                    # Task 8
```

---

### Task 1: Retry policy, DLQ config, decision table, options & validation

**Files:**
- Create: `src/settlement/retry.ts`, `src/settlement/dlq.ts`, `src/settlement/decide.ts`
- Modify: `src/module/options.ts`, `src/decorators/handler.decorators.ts`, `src/discovery/handler-validator.ts`
- Test: `test/unit/retry.spec.ts`, `test/unit/dlq.spec.ts`, `test/unit/decide.spec.ts`, `test/unit/handler-validator.spec.ts` (bổ sung)

**Interfaces:**
- Produces:
  - `interface RetryOptions { maxRetries?: number; initialDelay?: number; factor?: number; maxDelay?: number; jitter?: number; isRetryable?: (err: unknown) => boolean }`
  - `interface RetryPolicy { maxRetries: number; initialDelay: number; factor: number; maxDelay: number; jitter: number; isRetryable: (err: unknown) => boolean }`
  - `DEFAULT_RETRY: Readonly<Omit<RetryPolicy, 'isRetryable'>>` = các giá trị mặc định ở Global Constraints
  - `defaultIsRetryable(err: unknown): boolean` — `false` cho `NatsDecodeError`, `NonRetryableError`, `HttpException` 4xx; `true` cho phần còn lại (chuyển logic `isNonRetryable` từ `basic-settlement.ts` sang đây)
  - `resolveRetryPolicy(module?: RetryOptions | false, handler?: RetryOptions | false): RetryPolicy | false` — `handler === false` → `false`; `handler` object → `{...DEFAULT, ...(module || {}), ...handler}`; `handler === undefined` → `module === false ? false : {...DEFAULT, ...module}`; `isRetryable` mặc định `defaultIsRetryable`
  - `backoffDelay(policy: Pick<RetryPolicy, 'initialDelay' | 'factor' | 'maxDelay' | 'jitter'>, attempt: number, random: () => number = Math.random): number` — `attempt ≥ 1`; `Math.round(min(maxDelay, initialDelay × factor^(attempt−1)) × (1 + jitter × (2·random() − 1)))`
  - `interface DlqOptions { subject?: string | ((info: HandlerInfo, msg: InboundMessage) => string); mode?: 'jetstream' | 'core' }` — **lệch có chủ đích so với spec §7** (`(d: Delivery) => string`): hàm subject nhận từng message vì một batch có thể chứa message từ nhiều subject
  - `interface DlqPolicy { subject: (info: HandlerInfo, msg: InboundMessage) => string; mode: 'jetstream' | 'core' }`
  - `resolveDlqPolicy(kind: 'subscribe' | 'consume', module?: DlqOptions | boolean, handler?: DlqOptions | boolean): DlqPolicy | false` — `handler === false` → `false`; `true` ≡ `{}`; handler object ghi đè module object; không bật ở đâu → `false`; mặc định subject/mode theo Global Constraints
  - `DLQ_HEADERS = { subject: 'Nats-DLQ-Subject', stream: 'Nats-DLQ-Stream', consumer: 'Nats-DLQ-Consumer', seq: 'Nats-DLQ-Seq', deliveries: 'Nats-DLQ-Deliveries', error: 'Nats-DLQ-Error', timestamp: 'Nats-DLQ-Timestamp' } as const`
  - `buildDlqHeaders(msg: InboundMessage, err: unknown, deliveries: number, now: Date): MsgHdrs` — sao chép mọi header gốc; thêm `Subject` = `msg.subject`, `Deliveries`, `Error` (message lỗi, CR/LF → space, cắt 1024), `Timestamp`; nếu `msg.raw` là `JsMsg` (có `info`) thêm `Stream` = `info.stream`, `Consumer` = `info.consumer`, `Seq` = `info.streamSequence`
  - `type SettleDecision = { action: 'retry'; delayMs: number } | { action: 'dlq' } | { action: 'drop' }`
  - `decide(err: unknown, attempt: number, retry: RetryPolicy | false, dlq: DlqPolicy | false, random?: () => number): SettleDecision` — retryable (`retry && retry.isRetryable(err)`) và `attempt ≤ retry.maxRetries` → `retry` với `backoffDelay(retry, attempt)`; ngược lại `dlq` nếu `dlq` bật, else `drop`
  - Option mới (đều optional): `NatsModuleOptions.retry?: RetryOptions | false`, `.dlq?: DlqOptions | boolean`; `SubscribeOptions` và `ConsumeOptions` thêm `retry?: RetryOptions | false`, `dlq?: DlqOptions | boolean`
  - `validateHandlers` thêm: `maxRetries` số nguyên ≥ 0; `initialDelay` ≥ 0; `factor` ≥ 1; `maxDelay` ≥ `initialDelay`; `jitter` trong `[0, 1]` (kiểm tra trên policy đã resolve với module options) → `NatsConfigError` nêu `Class.method` và tên field. Chữ ký `validateHandlers(defs, mgr)` giữ nguyên — module options lấy qua `mgr.get(def.connection).options` (chỉ sau khi đã kiểm tra connection tồn tại).

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/retry.spec.ts
it('uses the spec defaults', () => expect(resolveRetryPolicy()).toMatchObject({ maxRetries: 5, initialDelay: 1000, factor: 2, maxDelay: 60000, jitter: 0.2 }));
it('handler false disables', () => expect(resolveRetryPolicy({ maxRetries: 9 }, false)).toBe(false));
it('module false disables unless handler opts in', () => {
  expect(resolveRetryPolicy(false)).toBe(false);
  expect(resolveRetryPolicy(false, { maxRetries: 2 })).toMatchObject({ maxRetries: 2, initialDelay: 1000 });
});
it('handler overrides module', () => expect(resolveRetryPolicy({ maxRetries: 3, factor: 3 }, { maxRetries: 1 })).toMatchObject({ maxRetries: 1, factor: 3 }));
it.each([[1, 1000], [2, 2000], [3, 4000], [7, 60000], [20, 60000]])('backoff attempt %i = %i without jitter', (n, ms) =>
  expect(backoffDelay({ ...DEFAULT_RETRY, jitter: 0 }, n)).toBe(ms));
it('applies jitter bounds', () => {
  expect(backoffDelay(DEFAULT_RETRY, 1, () => 0)).toBe(800);
  expect(backoffDelay(DEFAULT_RETRY, 1, () => 1)).toBe(1200);
});
it.each([
  [new NatsDecodeError('x'), false], [new NonRetryableError('x'), false], [new BadRequestException(), false],
  [new ForbiddenException(), false], [new InternalServerErrorException(), true], [new Error('x'), true],
])('defaultIsRetryable(%s) = %s', (err, expected) => expect(defaultIsRetryable(err)).toBe(expected));

// test/unit/dlq.spec.ts
it('is off by default', () => expect(resolveDlqPolicy('consume')).toBe(false));
it('defaults subject and mode per kind', () => {
  const c = resolveDlqPolicy('consume', true) as DlqPolicy;
  expect(c.mode).toBe('jetstream');
  expect(c.subject({ kind: 'consume', stream: 'ORD', durable: 'bill' } as HandlerInfo, msg('ord.x'))).toBe('dlq.ORD.bill');
  const s = resolveDlqPolicy('subscribe', {}) as DlqPolicy;
  expect(s.mode).toBe('core');
  expect(s.subject({ kind: 'subscribe' } as HandlerInfo, msg('orders.created'))).toBe('dlq.orders.created');
});
it('handler false wins over module true', () => expect(resolveDlqPolicy('consume', true, false)).toBe(false));
it('builds headers', () => {
  const h = buildDlqHeaders(jsMsg({ stream: 'ORD', consumer: 'bill', streamSequence: 7, headers: { a: '1' } }), new Error('boom\r\nline'), 6, new Date('2026-10-09T00:00:00Z'));
  expect(h.get('a')).toBe('1');
  expect(h.get('Nats-DLQ-Stream')).toBe('ORD'); expect(h.get('Nats-DLQ-Consumer')).toBe('bill');
  expect(h.get('Nats-DLQ-Seq')).toBe('7'); expect(h.get('Nats-DLQ-Deliveries')).toBe('6');
  expect(h.get('Nats-DLQ-Error')).toBe('boom line'); expect(h.get('Nats-DLQ-Timestamp')).toBe('2026-10-09T00:00:00.000Z');
});
it('truncates the error to 1024 chars', () => expect(buildDlqHeaders(coreMsg(), new Error('x'.repeat(5000)), 1, new Date()).get('Nats-DLQ-Error')).toHaveLength(1024));
it('omits JetStream headers for core messages', () => expect(buildDlqHeaders(coreMsg(), new Error('x'), 1, new Date()).has('Nats-DLQ-Stream')).toBe(false));

// test/unit/decide.spec.ts
const retry = { ...DEFAULT_RETRY, jitter: 0, isRetryable: defaultIsRetryable };
it.each([
  ['retryable, attempt 1', new Error('x'), 1, retry, false, { action: 'retry', delayMs: 1000 }],
  ['retryable, attempt 5 (= maxRetries)', new Error('x'), 5, retry, false, { action: 'retry', delayMs: 16000 }],
  ['exhausted, dlq on', new Error('x'), 6, retry, dlqOn, { action: 'dlq' }],
  ['exhausted, dlq off', new Error('x'), 6, retry, false, { action: 'drop' }],
  ['non-retryable, dlq on', new BadRequestException(), 1, retry, dlqOn, { action: 'dlq' }],
  ['retry disabled', new Error('x'), 1, false, false, { action: 'drop' }],
])('%s', (_, err, attempt, r, d, expected) => expect(decide(err, attempt, r, d)).toEqual(expected));

// test/unit/handler-validator.spec.ts (bổ sung)
it.each([
  ['negative maxRetries', { maxRetries: -1 }], ['fractional maxRetries', { maxRetries: 1.5 }],
  ['factor < 1', { factor: 0.5 }], ['maxDelay < initialDelay', { initialDelay: 10, maxDelay: 5 }], ['jitter > 1', { jitter: 2 }],
])('rejects retry with %s', (_, retry) => expect(() => validateHandlers([subscribeDef({ retry })], mgr)).toThrow(NatsConfigError));
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- retry dlq decide handler-validator`
Expected: FAIL — module chưa tồn tại / validator chưa kiểm tra retry.

- [ ] **Step 3: Implement** ba file mới, thêm option types, mở rộng validator theo Interfaces.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test && npm run lint && npx tsc --noEmit`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: retry policy, DLQ config and settlement decision table"
```

---

### Task 2: `Dispatcher` + shared error helpers; adapters refactored onto it

**Files:**
- Create: `src/util/errors.ts`, `src/adapters/dispatcher.ts`
- Modify: `src/adapters/core-subscription.adapter.ts`, `src/adapters/jetstream-consumer.adapter.ts`, `src/adapters/service-endpoint.adapter.ts`, `src/pipeline/basic-settlement.ts` (chỉ đổi import helper), `src/pipeline/delivery.ts`, `src/context/nats-context.ts`, `src/adapters/adapter.ts`
- Test: `test/unit/dispatcher.spec.ts`, `test/unit/nats-context.spec.ts` (bổ sung); toàn bộ integration suite hiện có phải vẫn xanh

**Interfaces:**
- Produces:
  - `errorMessage(err: unknown): string`, `errorTrace(err: unknown): string` trong `src/util/errors.ts`; xoá mọi bản copy cục bộ
  - `InboundMessage` thêm field public `attempt: number` (1-based; JetStream = `info.deliveryCount`; core = 1 rồi tăng theo retry in-process)
  - `NatsJetStreamContext.ack/nak/term`: chỉ đánh dấu `settled` **sau khi** lời gọi `raw` thành công (raw ném → `settled` vẫn `false`, lỗi được ném ra cho caller)
  - `interface DispatchItem<M> { msg: M; attempt: number }`
  - `class Dispatcher<M>`:
    - `constructor(opts: { concurrency: number; process: (items: DispatchItem<M>[]) => Promise<void>; logger: LoggerService; label: string; onRejected?: (msg: M) => void })` (Task 5 thêm `batch?`)
    - `push(msg: M, attempt?: number): Promise<void>` — mặc định `attempt = 1`; chờ `limiter.acquire()` (backpressure) rồi chạy `process([item])` không await, `.catch` log bằng `errorTrace`, `.finally` release; sau `stop()` → trả về ngay và gọi `opts.onRejected?.(msg)` (JetStream dùng để `nak()` message đã nhận nhưng chưa xử lý)
    - `scheduleRetry(item: DispatchItem<M>, delayMs: number): void` — `setTimeout` (unref) → `push(item.msg, item.attempt + 1)`; theo dõi timer để huỷ; sau `stop()` vẫn cho phép lên lịch (retry của message đang chạy) nhưng `cancelRetries()` sẽ huỷ
    - `stop(): void` — ngừng nhận `push` mới
    - `idle(): Promise<void>` — resolve khi không còn `process` nào đang chạy (không chờ retry đang hẹn giờ)
    - `cancelRetries(): number` — huỷ mọi timer retry đang chờ, trả về số lượng
    - `readonly active: number`
  - `HandlerAdapter` thêm `forceStop(): { nakked: number; cancelledRetries: number }` — Task 7 dùng; ở task này: core → `{ nakked: 0, cancelledRetries: dispatcher.cancelRetries() }`; JetStream → nak mọi context đang xử lý chưa settle (adapter giữ `Set<NatsJetStreamContext>` đang chạy) + `cancelRetries()`; service → `{ 0, 0 }`
  - Core và JetStream adapter dùng `Dispatcher` thay cho vòng limiter tự viết; hành vi quan sát được **không đổi** (vẫn `basicSettlement`); `msg.info.deliveryCount` được truyền làm `attempt` cho JetStream

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/dispatcher.spec.ts
it('caps concurrency and applies backpressure', async () => { /* concurrency 2, process chờ deferred; push thứ 3 pending tới khi 1 cái xong */ });
it('passes attempt through', async () => { await d.push('a', 3); expect(seen).toEqual([[{ msg: 'a', attempt: 3 }]]); });
it('isolates process failures', async () => { /* process ném → logger.error gọi với label; push tiếp vẫn chạy */ });
it('schedules retries with attempt + 1', async () => {
  vi.useFakeTimers(); d.scheduleRetry({ msg: 'a', attempt: 1 }, 500);
  await vi.advanceTimersByTimeAsync(500); expect(seen.at(-1)).toEqual([{ msg: 'a', attempt: 2 }]);
});
it('cancelRetries cancels pending timers', () => { d.scheduleRetry({ msg: 'a', attempt: 1 }, 500); expect(d.cancelRetries()).toBe(1); /* không chạy sau 500ms */ });
it('rejects pushes after stop', async () => { d.stop(); await d.push('x'); expect(onRejected).toHaveBeenCalledWith('x'); expect(seen).toEqual([]); });
it('idle waits for in-flight work only', async () => { /* 1 process đang chạy + 1 retry hẹn giờ → idle resolve khi process xong */ });

// test/unit/nats-context.spec.ts (bổ sung)
it('stays unsettled when raw ack throws', () => {
  raw.ack.mockImplementation(() => { throw new Error('closed'); });
  expect(() => ctx.ack()).toThrow('closed'); expect(ctx.settled).toBe(false);
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- dispatcher nats-context`
Expected: FAIL.

- [ ] **Step 3: Implement** `src/util/errors.ts`, `Dispatcher`, sửa context, chuyển ba adapter sang helper chung và hai adapter có hàng đợi sang `Dispatcher`; thêm `forceStop()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test && npm run test:int && npm run lint && npx tsc --noEmit`
Expected: PASS — toàn bộ unit + 91 integration test của Plan 1 vẫn xanh.

- [ ] **Step 5: Commit**

```bash
git commit -m "refactor: shared Dispatcher for queued adapters; settle only after raw succeeds"
```

---

### Task 3: Settlement middleware (retry, backoff, DLQ, heartbeat) replacing `basicSettlement`

**Files:**
- Create: `src/settlement/settlement.ts`
- Modify: `src/settlement/dlq.ts` (thêm `publishToDlq`), `src/pipeline/delivery.ts`, `src/adapters/core-subscription.adapter.ts`, `src/adapters/jetstream-consumer.adapter.ts`, `src/adapters/service-endpoint.adapter.ts`
- Delete: `src/pipeline/basic-settlement.ts` (và `test/unit/basic-settlement.spec.ts`, chuyển các case còn giá trị sang `settlement.spec.ts`)
- Test: `test/unit/settlement.spec.ts`, `test/integration/retry-dlq.int.spec.ts`

**Interfaces:**
- Consumes: Task 1 (`RetryPolicy`, `DlqPolicy`, `decide`, `backoffDelay`, `buildDlqHeaders`, `DEFAULT_RETRY`), Task 2 (`Dispatcher.scheduleRetry`, `InboundMessage.attempt`, `errorTrace`)
- Produces:
  - `RETRY_SCHEDULER: unique symbol` trong `delivery.ts`; `InternalDelivery` thêm `readonly [RETRY_SCHEDULER]?: (msg: InboundMessage, delayMs: number) => void` (core adapter gắn vào, gọi `dispatcher.scheduleRetry`)
  - `publishToDlq(entry: ConnectionEntry, dlq: DlqPolicy, info: HandlerInfo, msg: InboundMessage, err: unknown, deliveries: number): Promise<void>` — subject = `dlq.subject(info, msg)`; body = `msg.data` nguyên byte; headers = `buildDlqHeaders(...)`; `mode 'jetstream'` → `entry.js().publish(subject, data, { headers })` (cần PubAck); `'core'` → `entry.nc.publish(...)`. **Không** đi qua outbound plugin. Lỗi `NoRespondersError`/503 từ JetStream → ném `NatsModuleError` có message chứa subject và `no stream captures this subject`
  - `interface SettlementConfig { entry: ConnectionEntry; retry: RetryPolicy | false; dlq: DlqPolicy | false; heartbeatMs?: number; random?: () => number }`
  - `createSettlement(cfg: SettlementConfig): InboundMiddleware` — hành vi:
    | Delivery | Kết quả | Hành động |
    |---|---|---|
    | reply (`isReplyDelivery`) | lỗi | rethrow (adapter reply), không retry |
    | consume, ack auto | thành công | `ack()` các message chưa settle |
    | consume, ack manual | thành công | không làm gì |
    | consume | lỗi, message chưa settle | theo `decide(err, msg.attempt, retry, dlq)`: `retry` → `nak(delayMs)`; `dlq` → `publishToDlq` rồi `term()`, publish lỗi → `nak(backoffDelay(retry \|\| DEFAULT_RETRY, attempt))` + log error; `drop` → `term(errorMessage)` |
    | subscribe không reply | lỗi | `decide`: `retry` → `d[RETRY_SCHEDULER](msg, delayMs)`; `dlq` → `publishToDlq` (lỗi → log error); `drop` → log error |
    - Heartbeat: nếu `heartbeatMs` có giá trị và delivery là consume, gọi `working()` trên mọi message chưa settle mỗi `heartbeatMs` trong lúc `next()` chạy; dừng khi `next()` xong.
    - Logging (thay double logging của Plan 1): **không** log lại stack của handler error (Nest `ExceptionsHandler` đã log); chỉ log một dòng settlement: `warn` "`<Class.method> on "<subject>"` failed (attempt n/maxRetries+1), retrying in Xms: <message>"; `error` khi DLQ: "… sent to DLQ <subject> after n attempts: <message>"; `error` khi drop: "… gave up after n attempts: <message>"; lỗi settle (`raw` ném) → `error`.
  - Adapter: core và consume dùng `createSettlement` với `retry = resolveRetryPolicy(entry.options.retry, def.options.retry)`, `dlq = resolveDlqPolicy(kind, entry.options.dlq, def.options.dlq)`; service endpoint dùng `createSettlement({ entry, retry: false, dlq: false })`.
  - JetStream adapter khi `start()`: đọc `consumer.info(true)`; `heartbeatMs = ack_wait_ns / 1e6 / 2`; nếu `config.max_deliver > 0` và retry bật và `max_deliver ≤ retry.maxRetries` → `entry.logger.warn` "Consumer <stream>/<durable> for <Class.method> has max_deliver <n> ≤ maxRetries <m>; the server will stop redelivering before the DLQ is reached".
  - Core adapter: khi reply lỗi có `code ≥ 500`, log `warn` một dòng "Replied <code> to request on "<subject>" for <Class.method>: <message>" (follow-up "request-path 5xx không được log").

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/settlement.spec.ts — Delivery giả + NatsJetStreamContext trên JsMsg giả (vi.fn ack/nak/term/working); retry jitter 0, initialDelay 100
it.each([
  ['success auto → ack', undefined, 1, { ack: 1 }],
  ['retryable attempt 1 → nak(100)', new Error('x'), 1, { nak: [100] }],
  ['retryable attempt 3 → nak(400)', new Error('x'), 3, { nak: [400] }],
  ['exhausted, dlq off → term', new Error('x'), 6, { term: 1 }],
  ['non-retryable, dlq off → term', new BadRequestException(), 1, { term: 1 }],
])('consume: %s', async (_, err, attempt, expected) => { /* ... */ });
it('exhausted with dlq → publishes then terms', async () => { /* publishToDlq spy called with deliveries 6; term called after */ });
it('dlq publish failure → nak with backoff and error log', async () => { /* publish rejects → nak(backoff(6)), logger.error mentions DLQ subject */ });
it('manual ack success does nothing', async () => { /* no ack/nak/term */ });
it('does not resettle a message the handler settled', async () => { /* handler ctx.nak(5) then throws → only nak(5) */ });
it('core retryable → schedules retry', async () => { /* RETRY_SCHEDULER spy called with (msg, 100) */ });
it('core exhausted with dlq → publishes', async () => { /* attempt 6 */ });
it('reply deliveries rethrow without retry', async () => { /* replyTo set → rejects, scheduler not called */ });
it('heartbeats while the handler runs', async () => { vi.useFakeTimers(); /* heartbeatMs 100, handler 350ms → working ×3 */ });
it('logs one settlement line, not the stack', async () => { /* logger.warn called once with /retrying in 100ms/; logger.error not called with a stack */ });

// test/integration/retry-dlq.int.spec.ts — streams: ORD ['ord.>'], DLQ ['dlq.>']; retry { initialDelay: 50, jitter: 0, maxRetries: 2 }
it('retries with backoff then sends to DLQ with headers', async () => {
  await js.publish('ord.fail', { id: 1 });
  const m = await nextFromStream('DLQ', 'dlq.ORD.fail-consumer');
  expect(handlers.attempts('ord.fail')).toEqual([1, 2, 3]);
  expect(m.headers.get('Nats-DLQ-Deliveries')).toBe('3'); expect(m.headers.get('Nats-DLQ-Stream')).toBe('ORD');
  expect(m.json()).toEqual({ id: 1 });
  // khoảng cách giữa các lần ≈ 50ms rồi 100ms (dung sai rộng)
});
it('sends non-retryable errors to DLQ on the first attempt', async () => { /* handler ném BadRequestException → attempts [1], DLQ có message */ });
it('terms without DLQ when DLQ is off', async () => { /* num_ack_pending 0, không có message DLQ, attempts [1,2,3] */ });
it('naks and logs when no stream captures the DLQ subject', async () => {
  /* dlq { subject: 'nowhere.x' } → logger.error chứa 'nowhere.x' và 'no stream captures this subject'; message vẫn pending (không mất) */
});
it('retries core subscriptions in-process then publishes to core DLQ', async () => { /* rawNc.subscribe('dlq.orders.x') nhận message, attempts [1,2,3] */ });
it('keeps a slow handler alive with heartbeats', async () => {
  /* consumer ack_wait 2s, handler sleep 5s → handler chạy đúng 1 lần, num_ack_pending 0 sau đó */
});
it('warns when max_deliver <= maxRetries', async () => { /* consumer config max_deliver 2, retry maxRetries 5 → warn chứa 'max_deliver 2' và 'maxRetries 5' */ });
it('logs 5xx replies once', async () => { /* @Subscribe request handler ném Error → logger.warn 'Replied 500' */ });
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- settlement && npm run test:int -- retry-dlq`
Expected: FAIL.

- [ ] **Step 3: Implement** theo Interfaces; xoá `basic-settlement.ts`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test && npm run test:int && npm run lint && npx tsc --noEmit`
Expected: PASS (các test Plan 1 dựa vào nak tức thì phải được cập nhật sang retry `initialDelay` nhỏ — ghi rõ trong report từng test đã sửa và lý do).

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: retry with exponential backoff, DLQ and heartbeats in settlement"
```

---

### Task 4: Idempotency (store + middleware)

**Files:**
- Create: `src/idempotency/store.ts`, `src/idempotency/idempotency.ts`
- Modify: `src/module/options.ts`, `src/decorators/handler.decorators.ts`, `src/pipeline/delivery.ts`, `src/pipeline/invoker.ts`, các adapter core/JetStream (chèn middleware), `src/module/nats-lifecycle.ts` (thu thập store để đóng ở Task 7)
- Test: `test/unit/idempotency-store.spec.ts`, `test/unit/idempotency.spec.ts`, `test/integration/idempotency.int.spec.ts`

**Interfaces:**
- Produces:
  - `type ClaimResult = 'claimed' | 'duplicate' | 'in-progress'`
  - `interface IdempotencyStore { claim(key: string, ttlMs: number): Promise<ClaimResult>; complete(key: string, ttlMs: number): Promise<void>; release(key: string): Promise<void>; close?(): Promise<void> }`
  - `class InMemoryIdempotencyStore implements IdempotencyStore` — `constructor(opts?: { maxKeys?: number /* 100_000 */; now?: () => number })`; entry `{ state: 'in-progress' | 'done'; expiresAt }`; `claim` trên key hết hạn coi như mới; `in-progress` cũng hết hạn sau `ttlMs` (claim của process đã chết không giữ mãi); `complete` đặt `done` với hạn mới; vượt `maxKeys` → loại entry ít dùng nhất (Map theo thứ tự chèn, chạm vào thì xoá + chèn lại); quét hết hạn định kỳ bằng `setInterval(…, 60_000).unref()`; `close()` dừng interval và xoá dữ liệu
  - `interface IdempotencyOptions { ttl?: number; keyExtractor?: (msg: InboundMessage) => string | undefined }`
  - `NatsModuleOptions.idempotency?: { store?: IdempotencyStore; ttl?: number }` (TTL mặc định `300_000`; store mặc định: một `InMemoryIdempotencyStore` dùng chung cho connection, tạo lười lần đầu cần)
  - `SubscribeOptions.idempotency?: boolean | IdempotencyOptions`, `ConsumeOptions.idempotency?: boolean | IdempotencyOptions`
  - `idempotencyKey(msg: InboundMessage, extractor?: IdempotencyOptions['keyExtractor']): string | undefined` — thứ tự spec §10: header `Nats-Msg-Id` → (CloudEvent `source|id`: Plan 3) → `extractor(msg)`
  - `SKIPPED: unique symbol`; `InternalDelivery` thêm `readonly [SKIPPED]: Set<InboundMessage>` (adapter khởi tạo rỗng); invoker bỏ qua message thuộc `SKIPPED` (single: không gọi handler, `result` giữ `undefined`)
  - `createIdempotency(cfg: { store: IdempotencyStore; ttl: number; keyExtractor?: …; inProgressNakMs: number }): InboundMiddleware` — vị trí: ngay sau settlement (core list của adapter = `[settlement, idempotency]`). Mỗi message có key:
    - `claim` ném → rethrow (settlement retry) — không xử lý message
    - `duplicate` → thêm vào `SKIPPED`; JetStream: `ctx.ack()`; log debug
    - `in-progress` → thêm vào `SKIPPED`; JetStream: `ctx.nak(inProgressNakMs)`; core: bỏ qua
    - `claimed` → sau `next()`: thành công → `complete` (lỗi `complete` → `logger.warn`, không ném); lỗi → `release` (lỗi release → `logger.warn`) rồi rethrow
    - Không có key → xử lý bình thường, `logger.debug`
  - `inProgressNakMs = retry ? retry.initialDelay : 1000`
  - Request trùng (reply delivery) không được reply — ghi chú cho README (Task 8)

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/idempotency-store.spec.ts — now giả
it('claims, then reports in-progress, then duplicate after complete', async () => {
  expect(await s.claim('k', 1000)).toBe('claimed'); expect(await s.claim('k', 1000)).toBe('in-progress');
  await s.complete('k', 1000); expect(await s.claim('k', 1000)).toBe('duplicate');
});
it('expires done keys after ttl', async () => { await s.claim('k', 1000); await s.complete('k', 1000); now += 1001; expect(await s.claim('k', 1000)).toBe('claimed'); });
it('expires stale in-progress claims', async () => { await s.claim('k', 1000); now += 1001; expect(await s.claim('k', 1000)).toBe('claimed'); });
it('release frees the key', async () => { await s.claim('k', 1000); await s.release('k'); expect(await s.claim('k', 1000)).toBe('claimed'); });
it('evicts least recently used beyond maxKeys', async () => { /* maxKeys 2: a, b, chạm a, thêm c → b bị loại */ });

// test/unit/idempotency.spec.ts
it('prefers Nats-Msg-Id over the extractor', () => expect(idempotencyKey(msgWith({ 'Nats-Msg-Id': 'm1' }), () => 'x')).toBe('m1'));
it('acks JetStream duplicates without invoking', async () => { /* store trả 'duplicate' → ack, next không gọi handler (SKIPPED chứa msg) */ });
it('naks in-progress JetStream messages', async () => { /* nak(50) */ });
it('releases on failure and rethrows', async () => { /* next ném → release, rethrow */ });
it('completes on success and survives complete failures', async () => { /* complete ném → logger.warn, không ném */ });
it('rethrows claim failures so settlement retries', async () => { /* claim ném → rejects, next không gọi */ });

// test/integration/idempotency.int.spec.ts
it('dedupes core messages by Nats-Msg-Id', async () => { /* publish 2 lần cùng header Nats-Msg-Id → handler chạy 1 lần */ });
it('dedupes JetStream messages via keyExtractor', async () => { /* keyExtractor đọc header 'x-order-id'; 2 publish khác msgId cùng x-order-id → 1 lần, cả 2 được ack */ });
it('processes again after ttl', async () => { /* ttl 200 → publish, chờ 300ms, publish → 2 lần */ });
it('retries when the store fails to claim', async () => { /* store giả claim ném lần đầu → handler vẫn chạy đúng 1 lần sau retry */ });
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- idempotency && npm run test:int -- idempotency`
Expected: FAIL.

- [ ] **Step 3: Implement** theo Interfaces.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test && npm run test:int && npm run lint && npx tsc --noEmit`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: idempotency with pluggable store and in-memory default"
```

---

### Task 5: Batch processing (consume side)

**Files:**
- Create: `src/batch/collector.ts`, `src/context/nats-batch-context.ts`
- Modify: `src/adapters/dispatcher.ts`, `src/adapters/core-subscription.adapter.ts`, `src/adapters/jetstream-consumer.adapter.ts`, `src/pipeline/delivery.ts`, `src/pipeline/invoker.ts`, `src/settlement/settlement.ts`, `src/idempotency/idempotency.ts`, `src/discovery/nats-params-factory.ts`, `src/decorators/handler.decorators.ts`, `src/discovery/handler-validator.ts`
- Test: `test/unit/collector.spec.ts`, `test/unit/batch.spec.ts`, `test/integration/batch.int.spec.ts`

**Interfaces:**
- Consumes: Task 2 `Dispatcher`, Task 3 settlement, Task 4 `SKIPPED`
- Produces:
  - `interface BatchOptions { maxSize?: number /* 100 */; maxWait?: number /* 1000 */ }`; `SubscribeOptions.batch?` / `ConsumeOptions.batch?`
  - Validator: `batch` cùng `ack: 'manual'` → `NatsConfigError`; `maxSize` số nguyên ≥ 1; `maxWait` ≥ 0
  - `class BatchCollector<T>` — `constructor(opts: { maxSize: number; maxWait: number; flush: (items: T[]) => Promise<void> })`; `add(item): Promise<void>` (khi đạt `maxSize` → await `flush` — đây là backpressure); hẹn giờ `maxWait` tính từ item đầu của batch; `flushNow(): Promise<void>`; `readonly size: number`
  - `Dispatcher` opts thêm `batch?: { maxSize: number; maxWait: number }` — khi có: `push` đưa item vào collector; mỗi lần flush lấy **một** slot limiter rồi gọi `process(items)`; `stop()` gọi `flushNow()` cho batch đang gom (giao cho handler) trước khi ngừng; retry in-process của message trong batch quay lại collector với `attempt + 1`
  - `class NatsBatchContext` — `readonly kind: 'batch'`; `readonly connection: string`; `readonly messages: ReadonlyArray<NatsContext>` (mỗi phần tử là `NatsContext` hoặc `NatsJetStreamContext` của từng message); `fail(target: number | NatsMsg | JsMsg, error: unknown): void` (`number` = vị trí trong mảng payload handler nhận)
  - `Delivery.context` đổi type thành `NatsContext | NatsBatchContext` (public; ghi chú breaking 0.x trong commit)
  - `FAILURES: unique symbol`; `InternalDelivery` thêm `readonly [FAILURES]: Map<InboundMessage, unknown>`
  - Invoker với batch: decode từng message không thuộc `SKIPPED`; decode lỗi → ghi vào `FAILURES` (`NatsDecodeError`) và loại khỏi payload; còn lại rỗng → không gọi handler; handler nhận `payload: T[]` và `ctx: NatsBatchContext`; kết quả `{ failed: Array<{ msg: number | NatsMsg | JsMsg; error: unknown }> }` → ghi vào `FAILURES`
  - Params factory với `NatsBatchContext`: `PAYLOAD` → mảng (với `data` → `items.map(i => i?.[data])`); `HEADERS` → mảng headers mỗi message (với `data` → mảng giá trị); `SUBJECT` → mảng subject; `MSG` → mảng raw; `CONTEXT` → `NatsBatchContext`
  - Settlement với batch: handler ném → mọi message chưa settle/không SKIPPED đi theo bảng Task 3 với `attempt` riêng; thành công → message trong `FAILURES` đi theo bảng như lỗi, phần còn lại ack (consume auto); heartbeat áp dụng cho mọi message của batch; batch không bao giờ reply
  - Idempotency với batch: claim từng message; duplicate/in-progress → `SKIPPED` (+ ack/nak cho JetStream); sau `next()`: message thành công → `complete`, message trong `FAILURES` hoặc khi handler ném → `release`
  - JetStream `max_messages` = `Math.max(2 * concurrency, 2, batch ? batch.maxSize : 0)`

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/collector.spec.ts — fake timers
it('flushes at maxSize', async () => { /* maxSize 3: add ×3 → flush([a,b,c]) */ });
it('flushes after maxWait from the first item', async () => { /* maxWait 100: add a; +99ms chưa; +1ms → flush([a]) */ });
it('applies backpressure while a flush runs', async () => { /* flush chờ deferred; add thứ maxSize+1 pending */ });
it('flushNow delivers a partial batch', async () => { /* add a, b; flushNow → flush([a,b]) */ });

// test/unit/batch.spec.ts
it('excludes undecodable messages and records them as failures', async () => { /* 3 msg, msg[1] 'hello' → handler nhận 2 items; FAILURES có msg[1] với NatsDecodeError */ });
it('maps @Payload(prop) over items', () => { /* params factory với NatsBatchContext */ });
it('records failures from ctx.fail(index) and from the returned { failed }', async () => { /* ... */ });
it('settles each message: ack succeeded, retry/dlq failed', async () => { /* 3 JsMsg; fail(1) → ack m0, m2; nak(backoff(attempt)) m1 */ });
it('handler throw settles every message with its own attempt', async () => { /* attempts 1 và 6 → nak(100) và term */ });
it('rejects batch with manual ack', () => expect(() => validateHandlers([consumeDef({ ack: 'manual', batch: {} })], mgr)).toThrow(NatsConfigError));

// test/integration/batch.int.spec.ts
it('delivers JetStream batches by size', async () => { /* maxSize 5, maxWait 5000: publish 10 → handler nhận [5, 5] */ });
it('delivers a partial batch after maxWait', async () => { /* maxSize 100, maxWait 200: publish 3 → [3] trong < 1s */ });
it('acks succeeded and retries failed items', async () => { /* handler fail(1) lần đầu → item đó quay lại ở batch sau với deliveryCount 2; num_ack_pending cuối = 0 */ });
it('processes the rest when one message is not JSON', async () => { /* 3 msg, 1 'hello' → handler nhận 2; message hỏng bị term (DLQ off) */ });
it('batches core subscriptions', async () => { /* @Subscribe batch maxSize 3: publish 6 → [3,3] */ });
it('flushes the pending batch on shutdown', async () => { /* maxWait 60000: publish 2, app.close() → handler nhận [2] trước khi close resolve */ });
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- collector batch && npm run test:int -- batch`
Expected: FAIL.

- [ ] **Step 3: Implement** theo Interfaces.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test && npm run test:int && npm run lint && npx tsc --noEmit`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: batch processing for @Subscribe and @Consume"
```

---

### Task 6: `publishMany`, `Batcher`, connection close hooks

**Files:**
- Create: `src/batch/batcher.ts`
- Modify: `src/jetstream/jetstream-client.ts`, `src/connection/connection-manager.ts`
- Test: `test/unit/batcher.spec.ts`, `test/integration/publish-batch.int.spec.ts`

**Interfaces:**
- Produces:
  - `type JetStreamPublishOptions = { msgId?: string; expect?: Partial<StreamExpectations>; headers?: HeadersInit; codec?: Codec; timeout?: number }` (đặt tên cho kiểu option sẵn có của `publish`, export ở Task 8)
  - `type PublishManyResult = { ok: true; ack: PubAck } | { ok: false; error: Error }`
  - `JetStreamClient.publishMany(items: ReadonlyArray<{ subject: string; data?: unknown; opts?: JetStreamPublishOptions }>, opts?: { window?: number /* 256 */ }): Promise<PublishManyResult[]>` — tối đa `window` publish đang chờ PubAck cùng lúc; kết quả đúng thứ tự đầu vào; mỗi item đi qua `publish()` (outbound plugins áp dụng); không bao giờ reject (lỗi từng item nằm trong kết quả); `window < 1` → `NatsConfigError`
  - `JetStreamClient.batcher(opts?: { maxSize?: number /* 100 */; maxWait?: number /* 50 */; window?: number }): Batcher`
  - `class Batcher` — `add(subject: string, data?: unknown, opts?: JetStreamPublishOptions): Promise<PubAck>` (resolve bằng PubAck của chính message đó, reject bằng lỗi của nó); flush khi đủ `maxSize` hoặc hết `maxWait` từ item đầu; `flush(): Promise<void>` (chờ mọi item đã add có kết quả); `close(): Promise<void>` (flush rồi đóng; `add` sau đó → reject `NatsModuleError('Batcher is closed')`); `readonly pending: number`
  - `ConnectionEntry.onClose(hook: () => Promise<void>): () => void` — đăng ký hook (trả hàm huỷ đăng ký); mỗi `Batcher` tự đăng ký `close` khi tạo và huỷ khi `close()` xong
  - `closeAll()` chạy mọi close hook (`Promise.allSettled`, lỗi → log) **trước** khi drain

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/batcher.spec.ts — publishMany giả, fake timers
it('flushes at maxSize and resolves each add with its own ack', async () => { /* maxSize 2 */ });
it('flushes after maxWait', async () => { /* maxWait 50 */ });
it('rejects only the failed item', async () => { /* item 2 lỗi → add #2 reject, #1 resolve */ });
it('close flushes and rejects later adds', async () => { await b.close(); await expect(b.add('x')).rejects.toThrow('Batcher is closed'); });

// test/integration/publish-batch.int.spec.ts — stream PB ['pb.>']
it('publishMany keeps input order and reports per-item results', async () => {
  const r = await js.publishMany([{ subject: 'pb.a', data: 1 }, { subject: 'nostream.x', data: 2 }, { subject: 'pb.b', data: 3 }]);
  expect(r.map((x) => x.ok)).toEqual([true, false, true]); expect((r[0] as any).ack.seq).toBeLessThan((r[2] as any).ack.seq);
});
it('caps in-flight publishes at window', async () => { /* outbound plugin đếm đồng thời tối đa; window 4, 50 item → max ≤ 4 */ });
it('batcher publishes and resolves acks', async () => { /* add ×250 với maxSize 100 → 250 PubAck, stream có 250 message */ });
it('flushes batchers on app.close()', async () => { /* maxWait 60000: add 3 (không await), app.close() → 3 message trong stream */ });
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- batcher && npm run test:int -- publish-batch`
Expected: FAIL.

- [ ] **Step 3: Implement** theo Interfaces.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test && npm run test:int && npm run lint && npx tsc --noEmit`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: JetStream publishMany and auto-flushing Batcher"
```

---

### Task 7: Bounded graceful shutdown

**Files:**
- Modify: `src/module/nats-lifecycle.ts`, `src/connection/connection-manager.ts`, `src/adapters/*.ts` (nếu cần cho `forceStop`)
- Test: `test/unit/nats-lifecycle.spec.ts` (mới, adapter giả), `test/integration/shutdown.int.spec.ts`

**Interfaces:**
- Consumes: `HandlerAdapter.stop/idle/forceStop` (Task 2), close hooks (Task 6), idempotency stores (Task 4)
- Produces:
  - `drainTimeout()` = max trên **mọi** connection của giá trị hiệu lực (`options.shutdown?.drainTimeout ?? 10_000`)
  - `beforeApplicationShutdown()` theo pha (spec §14), mỗi bước chờ có giới hạn:
    1. `stop()` mọi adapter — chờ tối đa phần còn lại của `drainTimeout` (bộ đếm bắt đầu khi shutdown bắt đầu)
    2. chờ `idle()` mọi adapter trong phần còn lại của `drainTimeout`
    3. luôn gọi `forceStop()` mọi adapter (huỷ retry đang hẹn giờ; nếu đã quá hạn thì nak message JetStream chưa settle); tổng `nakked`/`cancelledRetries` > 0 → `logger.warn` một dòng tổng kết
    4. `manager.closeAll({ timeoutMs: drainTimeout })`
    5. `close?.()` mọi idempotency store đã dùng (khử trùng theo identity; lỗi → log)
  - `NatsConnectionManager.closeAll(opts?: { timeoutMs?: number }): Promise<void>` — với mỗi connection: chạy close hooks rồi `drain()`; cả hai bị chặn bởi `timeoutMs` (mặc định 10_000); quá hạn → `nc.close()` và `logger.warn` "NATS connection "<name>" did not drain within <n>ms; closed forcefully"; không bao giờ reject
  - Tổng thời gian shutdown tối đa ≈ `2 × drainTimeout` (pha 1–2 + pha 4); ghi vào README ở Task 8

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/nats-lifecycle.spec.ts — adapter giả, fake timers, drainTimeout 1000
it('bounds a hanging stop()', async () => { /* stop không bao giờ resolve → shutdown vẫn tiếp tục sau 1000ms và gọi closeAll */ });
it('bounds a hanging idle() and force-stops adapters', async () => { /* idle treo → forceStop gọi, warn tổng kết nakked */ });
it('cancels pending retries even when idle in time', async () => { /* forceStop luôn gọi; cancelledRetries 2 → warn */ });
it('uses the effective max drainTimeout across connections', () => { /* {a: 50, b: unset} → 10000 */ });
it('closes idempotency stores once', async () => { /* cùng store cho 2 handler → close gọi 1 lần */ });

// test/integration/shutdown.int.spec.ts
it('does not hang when the server is gone', async () => {
  /* drainTimeout 500; start app với @Subscribe + @Consume; srv.stop(); const t0 = Date.now(); await app.close();
     expect(Date.now() - t0).toBeLessThan(2 * 500 + 1500); */
});
it('naks in-flight JetStream messages past drainTimeout', async () => { /* handler 3s, drainTimeout 300 → close < 1.5s; message được redeliver cho app mới */ });
it('waits for in-flight handlers within drainTimeout', async () => { /* handler 200ms, drainTimeout 2000 → handler hoàn tất trước khi close resolve */ });
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- nats-lifecycle && npm run test:int -- shutdown`
Expected: FAIL.

- [ ] **Step 3: Implement** theo Interfaces.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test && npm run test:int && npm run lint && npx tsc --noEmit`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: bounded graceful shutdown with forced drain fallback"
```

---

### Task 8: Public API + README

**Files:**
- Modify: `src/index.ts`, `test/unit/public-api.spec.ts`, `README.md`

**Interfaces:**
- Produces — runtime export **thêm** (giữ nguyên danh sách Plan 1): `InMemoryIdempotencyStore`, `NatsBatchContext`, `Batcher`, `defaultIsRetryable`. Type export thêm: `RetryOptions`, `DlqOptions`, `IdempotencyStore`, `IdempotencyOptions`, `ClaimResult`, `BatchOptions`, `JetStreamPublishOptions`, `PublishManyResult`. Không export: `Dispatcher`, `BatchCollector`, `createSettlement`, `createIdempotency`, `decide`, `backoffDelay`, `publishToDlq`, các symbol nội bộ.
- README: thêm mục **Retry and dead-letter queue** (bảng mặc định, bảng settlement mới, header DLQ, ví dụ stream `DLQ` bắt `dlq.>`, `isRetryable`, cảnh báo `max_deliver`), **Idempotency** (key order, store tuỳ biến, in-memory chỉ trong một instance, request trùng không được reply), **Batch processing** (`@Payload() items: T[]`, `NatsBatchContext`, `fail()` / `{ failed }`, decode lỗi từng message), **Batch publishing** (`publishMany`, `batcher`), cập nhật **Lifecycle and graceful shutdown** (các pha, `≈ 2 × drainTimeout`), cập nhật banner trạng thái (bỏ hai cảnh báo đã được sửa), **Known limitations** (xoá mục đã sửa; thêm: retry in-process của core mất khi process chết; retry phá thứ tự dù `concurrency: 1`; DLQ publish không qua outbound plugins), **Roadmap** (bỏ mục đã làm).
- Mọi đoạn `ts` trong README phải typecheck với build thật (cách làm như lần trước: tách từng block ra file tạm ngoài `src`, chạy `tsc`).

- [ ] **Step 1: Write failing test** — cập nhật danh sách runtime export trong `public-api.spec.ts` (thêm 4 tên ở trên) và một type-level test import các type mới.

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- public-api`
Expected: FAIL.

- [ ] **Step 3: Implement** `src/index.ts` và README.

- [ ] **Step 4: Verify**

Run: `npm run build && npm run check:pkg && npm run lint && npm test && npm run test:int` + typecheck các đoạn code README
Expected: tất cả PASS; publint/attw sạch.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: export reliability API; docs: README for retry, DLQ, idempotency, batching"
```
