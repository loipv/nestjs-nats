# `@loipv/nestjs-nats` — Plan 3: Integrations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thêm CloudEvents (publish, đọc binary/structured, routing theo `type`), OpenTelemetry tracing (`@loipv/nestjs-nats/otel`), health check cho terminus (`@loipv/nestjs-nats/health`) và tiện ích test không cần NATS server (`@loipv/nestjs-nats/testing`).

**Architecture:** Một nền tảng chung trước: `Delivery.outcomes` ghi kết quả settle của từng message (ack/retry/dlq/term/drop/skipped/replied) để plugin `outer` (tracing) và test harness đọc. CloudEvents là một module thuần (`src/cloudevents/`) dùng ở outbound (encode), invoker (router), idempotency (key) và context (`@CloudEvent()`). Ba subpath là ba entry riêng trong `package.json#exports`, entry chính không bao giờ import chúng. Harness tái dùng đúng pipeline của adapter qua một hàm dựng pipeline dùng chung.

**Tech Stack:** như Plan 1–2; thêm optional peer `@opentelemetry/api` `^1.9.0` và `@nestjs/terminus` `^11.0.0 || ^12.0.0`; devDependencies `@opentelemetry/api`, `@opentelemetry/sdk-trace-base`, `@opentelemetry/context-async-hooks`, `@opentelemetry/core` (2.x, chỉ cho test), `@nestjs/terminus`.

**Spec:** `docs/superpowers/specs/2026-10-09-nestjs-nats-design.md` (§5 CloudEvent routing, §6.2, §9, §10 key, §12, §13, §16 "Cho user"). Follow-ups: `docs/superpowers/notes/2026-10-09-plan1-followups.md`, `docs/superpowers/notes/2026-10-09-plan2-followups.md`.

**Phạm vi** = spec §19 bước 3. Ngoài phạm vi: CI matrix, docs site, examples, quy trình publish (Plan 4); quyết định I1 còn mở của Plan 2 (heartbeat với `concurrency > 1`) giữ nguyên.

## Global Constraints

- Mọi Global Constraints của Plan 1–2 vẫn áp dụng (ESM-only, import `.js`, Node `>=22.12`, Nest `^11 || ^12`, không `dependencies`, không log secret, TS `~6.0.3`, Conventional Commits, mỗi task một commit; integration test với `DOCKER_HOST=unix:///Users/hungvv/.colima/default/docker.sock TESTCONTAINERS_RYUK_DISABLED=true`).
- Entry chính (`src/index.ts` và mọi module nó import, bắc cầu) **không bao giờ** import `@opentelemetry/*` hay `@nestjs/terminus`. Subpath: `./otel` (cần `@opentelemetry/api`), `./health` (cần `@nestjs/terminus`), `./testing` (không cần gì thêm).
- `package.json`: `peerDependencies` thêm `"@opentelemetry/api": "^1.9.0"` và `"@nestjs/terminus": "^11.0.0 || ^12.0.0"`, cả hai `peerDependenciesMeta.<name>.optional: true`.
- CloudEvents 1.0, NATS protocol binding: binary mode dùng header `ce-<attr>` (chữ thường khi ghi; đọc không phân biệt hoa thường), `datacontenttype` ↔ header `Content-Type`; structured mode `Content-Type: application/cloudevents+json`. Thuộc tính bắt buộc khi đọc: `id`, `source`, `type`, `specversion`. Khi ghi: `specversion: '1.0'`, `id` = `crypto.randomUUID()`, `time` = ISO 8601, mặc định `mode: 'binary'`.
- Span OTel (spec §12): `publish <subject>` PRODUCER, `process <subject>` CONSUMER (message đơn: con của context trích từ header; batch: trace mới + link), `send <subject>` CLIENT (request), `process <subject>` SERVER (service endpoint / request handler). Attribute: `messaging.system=nats`, `messaging.destination.name`, `messaging.operation.type`, `messaging.operation.name`, `messaging.message.id`, `messaging.consumer.group.name`, `messaging.batch.message_count`, `messaging.message.body.size`, `messaging.nats.stream`, `messaging.nats.seq`, `messaging.nats.delivery_count`, `messaging.header.<name>`.
- Health (spec §13): `isHealthy(key, { connection?: 'default', rttTimeout?: 1000, jetstream?: false })`, dùng `HealthIndicatorService`; output không chứa secret.

## Review Focus

1. **CloudEvent structured do hệ thống khác gửi, dữ liệu ở `data_base64` hoặc `datacontenttype` không phải JSON** → handler nhận đúng dữ liệu đã decode, không phải chuỗi base64 hay rác. Test ở Task 2.
2. **Pattern `com.x.>` với type `com.x`** (ít segment hơn) → không khớp (`>` cần ít nhất một segment); type có ký tự lạ/rỗng không làm router ném lỗi lạ. Test ở Task 2.
3. **Cài plugin `tracing()` nhưng chưa đăng ký OTel SDK** → không lỗi, không làm chậm đáng kể, message vẫn chạy bình thường. Test ở Task 5.
4. **Health check với tên connection không tồn tại** → trả `down` có lý do, không ném lỗi (terminus vẫn trả 503 bình thường). Test ở Task 6.
5. **`harness.dispatch()` tới đích không tồn tại hoặc mơ hồ** → `NatsConfigError` liệt kê các handler ứng viên, không chạy nhầm handler. Test ở Task 7.

---

## File structure

```
src/pipeline/delivery.ts, settlement/settlement.ts, idempotency/idempotency.ts   # Task 1: outcomes
src/cloudevents/types.ts  src/cloudevents/codec.ts  src/cloudevents/matcher.ts  # Task 2
src/pipeline/outbound.ts, client/nats-client.ts, jetstream/jetstream-client.ts, context/nats-context.ts,
  decorators/param.decorators.ts, discovery/nats-params-factory.ts, module/options.ts   # Task 3
src/decorators/cloudevent.decorators.ts, discovery/handler-explorer.ts, pipeline/invoker.ts,
  settlement/settlement.ts, cloudevents/errors.ts                                 # Task 4
src/otel/index.ts  src/otel/tracing.ts  src/otel/carrier.ts                        # Task 5
src/health/index.ts  src/health/nats-health.indicator.ts  src/health/nats-health.module.ts  # Task 6
src/adapters/handler-pipeline.ts (refactor), src/testing/index.ts, src/testing/fake-connection.ts,
  src/testing/mock-clients.ts, src/testing/nats-testing.module.ts, src/testing/harness.ts   # Task 7
src/index.ts, package.json, README.md, test/unit/entrypoints.spec.ts               # Task 8
```

---

### Task 1: `Delivery.outcomes` — kết quả settle của từng message

**Files:**
- Modify: `src/pipeline/delivery.ts`, `src/settlement/settlement.ts`, `src/idempotency/idempotency.ts`, ba adapter (khởi tạo map), `src/pipeline/types.ts` (re-export type)
- Test: `test/unit/outcomes.spec.ts`; bổ sung `test/unit/settlement.spec.ts`, `test/unit/idempotency.spec.ts`

**Interfaces:**
- Produces:
  ```ts
  export type MessageOutcome =
    | { action: 'ack' }
    | { action: 'retry'; delayMs: number; error: unknown }        // JetStream nak(delay) hoặc core retry in-process
    | { action: 'dlq'; subject: string; error: unknown }
    | { action: 'term'; error?: unknown }                           // JetStream term (không DLQ)
    | { action: 'drop'; error?: unknown }                           // core: bỏ, không DLQ
    | { action: 'skipped'; reason: 'duplicate' | 'in-progress' }
    | { action: 'replied'; error?: unknown }                        // reply delivery: lỗi → error reply
    | { action: 'none' };                                           // manual ack thành công, không settle
  ```
  - `Delivery` thêm `readonly outcomes: Map<InboundMessage, MessageOutcome>` (public). Adapter khởi tạo rỗng.
  - Settlement ghi đúng một outcome cho mỗi message nó xử lý (kể cả nhánh DLQ publish lỗi → `retry` với `error` là lỗi gốc của handler; idempotency store lỗi → `retry`). Reply delivery: thành công → `{ action: 'replied' }`, lỗi → `{ action: 'replied', error }` rồi rethrow như cũ. Idempotency ghi `skipped`. Message handler tự settle (manual) → outcome theo lời gọi (`ack`/`retry` với `delayMs` của `ctx.nak(delay)` hoặc 0 /`term`) — ghi trong `NatsJetStreamContext` qua một hook nội bộ, không thêm API public.
  - JSDoc: `outcomes` đầy đủ khi `next()` của một middleware `stage: 'outer'` đã resolve/reject.

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/outcomes.spec.ts — dùng createSettlement + idempotency thật với Delivery/JsMsg giả (như settlement.spec)
it.each([
  ['success', undefined, 1, { action: 'ack' }],
  ['retryable', new Error('x'), 1, { action: 'retry', delayMs: 100 }],
  ['exhausted, dlq on', new Error('x'), 6, { action: 'dlq', subject: 'dlq.ORD.d' }],
  ['non-retryable, dlq off', new BadRequestException(), 1, { action: 'term' }],
])('records %s', async (_, err, attempt, expected) => { /* expect(d.outcomes.get(msg)).toMatchObject(expected) */ });
it('records core drop and core retry', async () => { /* ... */ });
it('records replied with and without error', async () => { /* reply delivery, rejects khi lỗi, outcome replied */ });
it('records skipped duplicates', async () => { /* store 'duplicate' → { action: 'skipped', reason: 'duplicate' } */ });
it('records manual settlement', async () => { /* handler ctx.nak(250) → { action: 'retry', delayMs: 250 } */ });
it('is visible to an outer plugin after next()', async () => { /* plugin outer đọc d.outcomes sau await next() */ });
it('records one outcome per batch message', async () => { /* 3 msg, fail(1) → ack, retry, ack */ });
```

- [ ] **Step 2: Run tests to verify they fail** — Run: `npm test -- outcomes` — Expected: FAIL (`outcomes` không tồn tại).
- [ ] **Step 3: Implement** theo Interfaces.
- [ ] **Step 4: Verify** — Run: `npm test && npm run test:int && npm run lint && npm run typecheck` — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat: expose per-message settlement outcomes on Delivery"`

---

### Task 2: CloudEvents codec + type matcher (thuần)

**Files:**
- Create: `src/cloudevents/types.ts`, `src/cloudevents/codec.ts`, `src/cloudevents/matcher.ts`
- Test: `test/unit/cloudevents-codec.spec.ts`, `test/unit/cloudevents-matcher.spec.ts`

**Interfaces:**
- Produces:
  - `interface CloudEventMeta { id: string; source: string; type: string; specversion: string; time?: string; subject?: string; datacontenttype?: string; dataschema?: string; extensions: Record<string, string> }`
  - `interface CloudEventInit { type: string; source?: string; subject?: string; id?: string; time?: string | Date; dataschema?: string; extensions?: Record<string, string>; mode?: 'binary' | 'structured' }`
  - `CE_STRUCTURED_CONTENT_TYPE = 'application/cloudevents+json'`
  - `cloudEventMode(headers?: MsgHdrs): 'binary' | 'structured' | undefined` — binary nếu có header `ce-specversion` (không phân biệt hoa thường); structured nếu `Content-Type` bắt đầu bằng `application/cloudevents+json`
  - `readCloudEventMeta(msg: { data: Uint8Array; headers?: MsgHdrs }): CloudEventMeta | undefined` — không decode `data`; lỗi/không phải CE → `undefined`
  - `readCloudEvent(msg: { data: Uint8Array; headers?: MsgHdrs }, codec: Codec): { meta: CloudEventMeta; data: unknown }` — binary: attribute từ `ce-*`, extension = mọi `ce-<name>` khác, `datacontenttype` từ `Content-Type`, `data = decodePayload(codec, msg.data, headers)`; structured: parse JSON envelope, `data` lấy từ `data` (nguyên giá trị JSON) hoặc `data_base64` (base64 → bytes → `decodePayload(codec, bytes)`); thiếu attribute bắt buộc / JSON hỏng / không phải CE → `NatsDecodeError`
  - `resolveCloudEvent(init: CloudEventInit, defaults: { source?: string; contentType: string }, gen?: { now?: () => Date; id?: () => string }): CloudEventMeta` — `source = init.source ?? defaults.source`, thiếu cả hai → `NatsConfigError('CloudEvent source is required: set cloudEvent.source or NatsModule cloudEvents.source')`; `time` (Date → ISO); `datacontenttype = defaults.contentType`
  - `writeCloudEvent(meta: CloudEventMeta, data: unknown, codec: Codec, mode: 'binary' | 'structured', headers: MsgHdrs): Uint8Array` — binary: set `ce-specversion`, `ce-id`, `ce-source`, `ce-type`, `ce-time?`, `ce-subject?`, `ce-dataschema?`, `ce-<ext>`, `Content-Type = meta.datacontenttype`; body = `encodePayload(codec, data)`. structured: `Content-Type: application/cloudevents+json`; body = JSON envelope gồm mọi attribute + extension ở cấp cao nhất; nếu `codec.contentType === 'application/json'` và data không phải `Uint8Array` → `data: <giá trị>`, ngược lại `data_base64`
  - `cloudEventKey(msg: { data: Uint8Array; headers?: MsgHdrs }): string | undefined` — `` `${source}|${id}` `` từ `readCloudEventMeta`
  - Matcher: `createTypeRouter<T>(routes: Array<{ pattern: string; target: T }>): (type: string) => T | undefined`
    - segment tách bằng `.`; `*` khớp đúng một segment; `>` chỉ được ở cuối, khớp **một hoặc nhiều** segment; segment rỗng hoặc `>` không ở cuối → `NatsConfigError`; hai route cùng pattern → `NatsConfigError`
    - thứ tự ưu tiên: khớp chính xác (không wildcard) > pattern không có `>` (nhiều segment literal hơn thắng) > pattern có `>` (nhiều segment literal hơn thắng) > thứ tự khai báo

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/cloudevents-codec.spec.ts
it('roundtrips binary mode', () => { /* write → headers có ce-specversion '1.0', ce-id, Content-Type application/json → readCloudEvent trả meta + data gốc */ });
it('roundtrips structured mode with JSON data', () => { /* envelope.data là object */ });
it('uses data_base64 for non-JSON codecs and decodes it on read', () => { /* textCodec → envelope.data_base64; read với textCodec → chuỗi gốc */ });
it('reads a foreign structured event with data_base64 and a non-JSON content type', () => {
  const env = { specversion: '1.0', id: '1', source: 's', type: 't', datacontenttype: 'text/plain', data_base64: Buffer.from('hi').toString('base64') };
  expect(readCloudEvent(msgWith(env, { 'Content-Type': 'application/cloudevents+json' }), textCodec).data).toBe('hi');
});
it('reads binary attributes case-insensitively and collects extensions', () => { /* 'CE-Id', 'ce-tenant' → extensions.tenant */ });
it.each(['id', 'source', 'type', 'specversion'])('rejects a missing %s', (attr) => expect(() => readCloudEvent(withoutAttr(attr), jsonCodec())).toThrow(NatsDecodeError));
it('rejects a non-CloudEvent message', () => expect(() => readCloudEvent(plainMsg(), jsonCodec())).toThrow(NatsDecodeError));
it('fills id, time, specversion and content type', () => {
  expect(resolveCloudEvent({ type: 't' }, { source: 's', contentType: 'application/json' }, { now: () => new Date(0), id: () => 'u1' }))
    .toEqual({ id: 'u1', source: 's', type: 't', specversion: '1.0', time: '1970-01-01T00:00:00.000Z', datacontenttype: 'application/json', extensions: {} });
});
it('requires a source', () => expect(() => resolveCloudEvent({ type: 't' }, { contentType: 'x' })).toThrow(NatsConfigError));
it('builds the idempotency key', () => expect(cloudEventKey(binaryMsg({ id: '7', source: 'svc' }))).toBe('svc|7'));

// test/unit/cloudevents-matcher.spec.ts
const r = createTypeRouter([
  { pattern: 'com.shop.>', target: 'gt' }, { pattern: 'com.shop.order.*', target: 'star' },
  { pattern: 'com.shop.order.created', target: 'exact' }, { pattern: 'com.*.order.created', target: 'star2' },
]);
it.each([
  ['com.shop.order.created', 'exact'], ['com.shop.order.paid', 'star'], ['com.shop.user.x.y', 'gt'],
  ['com.shop', undefined], ['other', undefined], ['', undefined],
])('%s → %s', (type, target) => expect(r(type)).toBe(target));
it('prefers more literal segments among non-tail patterns', () => { /* 'a.b.*' vs 'a.*.*' cho 'a.b.c' → 'a.b.*' */ });
it.each(['a..b', 'a.>.b', ''])('rejects invalid pattern %s', (p) => expect(() => createTypeRouter([{ pattern: p, target: 1 }])).toThrow(NatsConfigError));
it('rejects duplicate patterns', () => expect(() => createTypeRouter([{ pattern: 'a', target: 1 }, { pattern: 'a', target: 2 }])).toThrow(NatsConfigError));
```

- [ ] **Step 2: Run tests to verify they fail** — Run: `npm test -- cloudevents` — Expected: FAIL.
- [ ] **Step 3: Implement** ba file theo Interfaces.
- [ ] **Step 4: Verify** — Run: `npm test && npm run lint && npm run typecheck` — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat: CloudEvents codec and type matcher"`

---

### Task 3: Publish CloudEvents, `@CloudEvent()`, khoá idempotency từ CloudEvent

**Files:**
- Modify: `src/pipeline/types.ts`, `src/pipeline/outbound.ts`, `src/client/nats-client.ts`, `src/jetstream/jetstream-client.ts`, `src/module/options.ts`, `src/context/nats-context.ts`, `src/context/nats-batch-context.ts` (nếu cần), `src/decorators/param.decorators.ts`, `src/discovery/nats-params-factory.ts`, `src/idempotency/idempotency.ts`
- Test: `test/unit/outbound-cloudevents.spec.ts`, bổ sung `test/unit/idempotency.spec.ts`, `test/unit/enhancers.spec.ts`; `test/integration/cloudevents-publish.int.spec.ts`

**Interfaces:**
- Consumes: Task 2
- Produces:
  - `NatsModuleOptions.cloudEvents?: { source?: string }`
  - `OutgoingMessage` thêm `cloudEvent?: { meta: CloudEventMeta; mode: 'binary' | 'structured' }` — đã resolve **trước** khi chạy outbound plugin (plugin thấy `meta.id`); terminal encode bằng `writeCloudEvent` thay cho `encodePayload` khi có
  - `NatsClient.publish(..., opts)` và `JetStreamPublishOptions` thêm `cloudEvent?: CloudEventInit` (áp dụng cho `publish`, `publishMany`, `batcher().add`). `NatsClient.request` không nhận `cloudEvent`.
  - JetStream: không có `msgId` mà có `cloudEvent` → header `Nats-Msg-Id = meta.id`
  - Thiếu source → `NatsConfigError` ném đồng bộ từ `NatsClient.publish` (như lỗi header không hợp lệ) và reject từ `JetStreamClient.publish`
  - `NatsContext` thêm getter `cloudEvent: CloudEventMeta | undefined` — tính lười một lần từ `raw` bằng `readCloudEventMeta`; router (Task 4) có thể gán sẵn qua một setter nội bộ
  - `CloudEvent(): ParameterDecorator` (`NatsParamType.CLOUD_EVENT`) → `ctx.cloudEvent`; trong batch → mảng `CloudEventMeta | undefined` căn theo vị trí
  - `idempotencyKey`: `Nats-Msg-Id` → `cloudEventKey(msg)` → `keyExtractor(msg)` (đúng thứ tự spec §10)

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/outbound-cloudevents.spec.ts — ConnectionEntry giả ghi lại publish
it('publishes binary CloudEvents by default', () => { /* client.publish('o.c', {a:1}, { cloudEvent: { type: 't', source: 's' } }) → headers ce-type 't', body JSON {a:1} */ });
it('uses the module source', () => { /* options.cloudEvents.source 'urn:svc' */ });
it('publishes structured CloudEvents', () => { /* mode structured → Content-Type application/cloudevents+json, body envelope */ });
it('lets plugins see the resolved CloudEvent', () => { /* outbound plugin đọc m.cloudEvent.meta.id */ });
it('throws NatsConfigError without a source', () => expect(() => client.publish('x', 1, { cloudEvent: { type: 't' } })).toThrow(NatsConfigError));
it('sets Nats-Msg-Id from the CloudEvent id for JetStream', async () => { /* js.publish không msgId → header Nats-Msg-Id = meta.id; có msgId → giữ msgId */ });
// test/unit/idempotency.spec.ts (bổ sung)
it('falls back to the CloudEvent source|id before the extractor', () => expect(idempotencyKey(ceMsg('svc', '7'), () => 'x')).toBe('svc|7'));
// test/unit/enhancers.spec.ts (bổ sung)
it('injects @CloudEvent() meta', async () => { /* invoke với ctx có raw là CE binary → tham số nhận { type: 't', ... } */ });
// test/integration/cloudevents-publish.int.spec.ts
it('roundtrips a binary CloudEvent through NATS to @CloudEvent()', async () => { /* @Subscribe handler nhận payload + @CloudEvent() meta */ });
it('dedupes JetStream CloudEvent publishes server-side', async () => { /* publish cùng cloudEvent.id 2 lần → PubAck thứ 2 duplicate */ });
```

- [ ] **Step 2: Run tests to verify they fail** — Run: `npm test -- outbound-cloudevents idempotency enhancers && npm run test:int -- cloudevents-publish` — Expected: FAIL.
- [ ] **Step 3: Implement** theo Interfaces.
- [ ] **Step 4: Verify** — Run: `npm test && npm run test:int && npm run lint && npm run typecheck` — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat: publish CloudEvents, @CloudEvent() and CloudEvent idempotency keys"`

---

### Task 4: `@CloudEventRouter` / `@OnCloudEvent`

**Files:**
- Create: `src/decorators/cloudevent.decorators.ts`, `src/cloudevents/errors.ts`
- Modify: `src/decorators/constants.ts`, `src/discovery/handler-explorer.ts`, `src/pipeline/invoker.ts`, `src/settlement/settlement.ts`, `src/pipeline/delivery.ts` (`toHandlerInfo`)
- Test: `test/unit/cloudevent-router.spec.ts`, `test/integration/cloudevent-router.int.spec.ts`

**Interfaces:**
- Consumes: Task 1 (outcomes), Task 2, Task 3
- Produces:
  - `type CloudEventRouterOptions = ({ subscribe: string; queue?: string } | { consume: { stream: string; durable: string; config?: Partial<ConsumerConfig> } }) & { unmatched?: 'ack' | 'term' | 'dlq'; connection?: string; concurrency?: number; codec?: Codec; retry?: RetryOptions | false; dlq?: DlqOptions | boolean; idempotency?: boolean | IdempotencyOptions }`
  - `CloudEventRouter(options: CloudEventRouterOptions): ClassDecorator`; `OnCloudEvent(typePattern: string): MethodDecorator`
  - Explorer: mỗi class `@CloudEventRouter` sinh **một** `HandlerDefinition` kind `'subscribe'` (subject = `subscribe`) hoặc `'consume'` (options từ `consume` + phần chung), `methodName: '@CloudEventRouter'`, thêm field nội bộ `router: { route: (type: string) => { methodName: string; invoke: HandlerBase['invoke'] } | undefined; unmatched: 'ack' | 'term' | 'dlq' }` (mỗi `@OnCloudEvent` được bọc bằng `ExternalContextCreator` như handler thường). Method `@OnCloudEvent` không phải handler riêng.
  - Explorer ném `NatsConfigError` (bootstrap dừng, lifecycle dọn dẹp như cũ) khi: `@OnCloudEvent` trong class không có `@CloudEventRouter`; router không có `@OnCloudEvent` nào; pattern không hợp lệ / trùng (từ `createTypeRouter`); một method vừa `@OnCloudEvent` vừa `@Subscribe`/`@Consume`.
  - `class UnmatchedCloudEventError extends NatsModuleError` (nội bộ) — `readonly type: string; readonly policy: 'term' | 'dlq'`
  - Invoker (message đơn, `def.router`): `readCloudEvent(msg, codec)` (lỗi → `NatsDecodeError` như mọi lỗi decode) → gán `ctx.cloudEvent` → `route(meta.type)`: có → `d.result = await target.invoke(data, ctx)`; không có → `unmatched === 'ack'` → `logger.debug` và thành công; ngược lại ném `UnmatchedCloudEventError`
  - Settlement: `UnmatchedCloudEventError` không qua `decide`: `policy 'dlq'` + DLQ bật → nhánh DLQ; `'dlq'` + DLQ tắt → như `'term'` kèm `logger.warn` một lần mỗi lần xảy ra; `'term'` → JetStream `term()` / core `drop` (log debug). Reply delivery (request tới router subscribe): unmatched với mọi `policy` khác `'ack'` → error reply `404` với description `No handler for CloudEvent type "<type>"` (spec không nói; quyết định của plan); `'ack'` → không reply.
  - `HandlerInfo` của router: như subscribe/consume, `methodName: '@CloudEventRouter'`; idempotency scope theo `consume:<stream>/<durable>` hoặc `subscribe:<Class>.@CloudEventRouter`

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/cloudevent-router.spec.ts — TestingModule thật + HandlerExplorer
it('creates one definition per router class', () => { /* 2 @OnCloudEvent → explore() có 1 def, methodName '@CloudEventRouter' */ });
it.each([
  ['orphan @OnCloudEvent', OrphanHandlers], ['router without routes', EmptyRouter],
  ['duplicate pattern', DupRouter], ['invalid pattern', BadPatternRouter],
])('rejects %s', (_, cls) => expect(() => explore(cls)).toThrow(NatsConfigError));
it('dispatches by type with precedence', async () => { /* invoke qua invokeHandler: 'com.o.created' → exact, 'com.o.paid' → wildcard */ });
it('acks unmatched by default', async () => { /* outcome ack, không handler nào chạy */ });
it.each([['term', { action: 'term' }], ['dlq', { action: 'dlq' }]])('unmatched %s', async (policy, outcome) => { /* consume, dlq bật */ });
it('falls back to term with a warning when unmatched dlq has no DLQ', async () => { /* ... */ });
it('terms CloudEvents missing a required attribute', async () => { /* ce-specversion có, thiếu ce-source → NatsDecodeError → term */ });
it('replies 404 to an unmatched request', async () => { /* core request */ });
// test/integration/cloudevent-router.int.spec.ts
it('routes binary and structured CloudEvents published by NatsClient', async () => { /* router subscribe 'events.>' */ });
it('routes a JetStream router and settles per outcome', async () => { /* consume router: matched → ack; unmatched term → term */ });
it('dedupes by CloudEvent source|id with idempotency', async () => { /* core router idempotency true, cùng ce id 2 lần → 1 lần */ });
```

- [ ] **Step 2: Run tests to verify they fail** — Run: `npm test -- cloudevent-router && npm run test:int -- cloudevent-router` — Expected: FAIL.
- [ ] **Step 3: Implement** theo Interfaces.
- [ ] **Step 4: Verify** — Run: `npm test && npm run test:int && npm run lint && npm run typecheck` — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat: CloudEvent routing with @CloudEventRouter and @OnCloudEvent"`

---

### Task 5: Subpath `@loipv/nestjs-nats/otel`

**Files:**
- Create: `src/otel/index.ts`, `src/otel/tracing.ts`, `src/otel/carrier.ts`
- Modify: `package.json` (exports `./otel`, optional peer, devDependencies), `tsconfig.build.json` nếu cần
- Test: `test/unit/otel-tracing.spec.ts`, `test/integration/otel.int.spec.ts`, `test/support/otel.ts` (dựng provider test)

**Interfaces:**
- Consumes: Task 1 (`Delivery.outcomes`), Task 3 (`OutgoingMessage.cloudEvent`, `ctx.cloudEvent`)
- Produces:
  - `package.json#exports["./otel"] = { "types": "./dist/otel/index.d.ts", "default": "./dist/otel/index.js" }`
  - `interface TracingOptions { tracerName?: string /* '@loipv/nestjs-nats' */; captureHeaders?: string[] }`
  - `tracing(options?: TracingOptions): NatsPlugin` — `name: 'otel-tracing'`; `inbound.stage: 'outer'`
  - `carrier.ts`: `msgHdrsGetter: TextMapGetter<MsgHdrs | undefined>`, `msgHdrsSetter: TextMapSetter<MsgHdrs>`; carrier inbound = header NATS, nếu không có `traceparent` và message là CloudEvent structured thì dùng `traceparent`/`tracestate` trong extension của envelope
  - Inbound: message đơn → `propagation.extract(ROOT_CONTEXT, carrier)` làm parent; span `process <msg.subject>`; kind `SERVER` nếu `isReplyDelivery(d)` ngược lại `CONSUMER`; batch → parent `ROOT_CONTEXT`, `links` = span context trích từ từng message (chỉ context hợp lệ); `next()` chạy trong `context.with(trace.setSpan(...))`
  - Attribute: `messaging.system='nats'`, `messaging.destination.name` (subject), `messaging.operation.type='process'`, `messaging.operation.name='process'`, `messaging.message.id` (`Nats-Msg-Id` hoặc CE id; message đơn), `messaging.consumer.group.name` (durable hoặc queue), `messaging.batch.message_count` (batch), `messaging.message.body.size` (message đơn, `data.length`), `messaging.nats.stream` / `.seq` / `.delivery_count` (JetStream, message đơn), `messaging.header.<tên viết thường>` cho `captureHeaders` (giá trị `string` hoặc `string[]`)
  - Sau `next()`: với mỗi outcome — `retry` → event `nats.retry` `{ 'nats.retry.delay_ms', 'nats.attempt' }`; `dlq` → event `nats.dlq` `{ 'nats.dlq.subject' }`; `skipped` → event `nats.skipped` `{ 'nats.skipped.reason' }`; có `error` (retry/dlq/term/drop/replied) → `recordException` + status `ERROR`. `next()` ném → `recordException` + `ERROR` + rethrow. Span luôn `end()`.
  - Outbound: `publish`/`jetstream` → span `publish <subject>` PRODUCER, `messaging.operation.type='send'`, `messaging.operation.name='publish'`; `request` → `send <subject>` CLIENT, `operation.name='request'`; `propagation.inject(context, m.headers, msgHdrsSetter)` trong context của span trước `next()`; `messaging.message.id` từ header `Nats-Msg-Id` hoặc `m.cloudEvent.meta.id`; lỗi → record + rethrow; `end()` sau `next()`
  - Không SDK → API no-op, không lỗi

- [ ] **Step 1: Write failing tests**

```ts
// test/support/otel.ts: setupTracing() → { exporter: InMemorySpanExporter, reset(), shutdown() } dùng BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] }),
//   AsyncLocalStorageContextManager().enable(), W3CTraceContextPropagator — đăng ký global, gỡ trong afterAll
// test/unit/otel-tracing.spec.ts
it('creates a PRODUCER span and injects traceparent', async () => { /* outbound.handle → header traceparent chứa traceId của span 'publish o.c' */ });
it('creates a CONSUMER child span of the extracted context', async () => { /* inbound với header traceparent → span 'process o.c' parentSpanContext.spanId === span cha */ });
it('uses SERVER for reply deliveries and CLIENT for requests', async () => { /* ... */ });
it('links batch spans to every message', async () => { /* 3 msg có traceparent → 3 links, không parent */ });
it('reads traceparent from a structured CloudEvent envelope', async () => { /* không header, envelope.traceparent */ });
it('records retry, dlq and skipped as events and errors as status', async () => { /* outcomes giả → events + status ERROR */ });
it('sets messaging attributes', async () => { /* JetStream msg: stream, seq, delivery_count, consumer.group.name, message.id, captureHeaders */ });
it('is a no-op without an SDK', async () => { /* trace.disable(); propagation.disable() → handle không ném, không header traceparent */ });
// test/integration/otel.int.spec.ts
it('keeps one trace id from publish to @Consume', async () => { /* NatsModule plugins [tracing()]; js.publish → consume → span process có traceId của span publish, parent = span publish */ });
it('traces request/reply as CLIENT → SERVER', async () => { /* ... */ });
```

- [ ] **Step 2: Run tests to verify they fail** — Run: `npm test -- otel && npm run test:int -- otel` — Expected: FAIL.
- [ ] **Step 3: Implement** theo Interfaces; thêm devDependencies `@opentelemetry/api@^1.9`, `@opentelemetry/sdk-trace-base@^2`, `@opentelemetry/context-async-hooks@^2`, `@opentelemetry/core@^2`.
- [ ] **Step 4: Verify** — Run: `npm run build && npm run check:pkg && npm test && npm run test:int && npm run lint && npm run typecheck`; `node --input-type=module -e "import('@loipv/nestjs-nats/otel').then(m => console.log(typeof m.tracing))"` — Expected: PASS; in ra `function`.
- [ ] **Step 5: Commit** — `git commit -m "feat: OpenTelemetry tracing plugin (@loipv/nestjs-nats/otel)"`

---

### Task 6: Subpath `@loipv/nestjs-nats/health`

**Files:**
- Create: `src/health/index.ts`, `src/health/nats-health.indicator.ts`, `src/health/nats-health.module.ts`
- Modify: `package.json` (exports `./health`, optional peer `@nestjs/terminus`, devDependency)
- Test: `test/unit/nats-health.spec.ts`, `test/integration/health.int.spec.ts`

**Interfaces:**
- Produces:
  - `package.json#exports["./health"]` như `./otel`
  - `interface NatsHealthCheckOptions { connection?: string /* 'default' */; rttTimeout?: number /* 1000 */; jetstream?: boolean /* false */ }`
  - `@Injectable() class NatsHealthIndicator` — `constructor(health: HealthIndicatorService, manager: NatsConnectionManager)`; `isHealthy<K extends string>(key: K, options?: NatsHealthCheckOptions): Promise<HealthIndicatorResult<K>>`
    - connection không tồn tại → `down({ connection, reason: 'unknown connection' })` (không ném)
    - `status().state` là `closed`/`draining`/`reconnecting` → `down({ connection, reason: <state> })`
    - `nc.rtt()` vượt `rttTimeout` hoặc lỗi → `down({ connection, reason: 'rtt timeout after <n>ms' | 'rtt failed: <message đã redact>' })`
    - `jetstream: true` → `(await entry.jsm()).getAccountInfo()` lỗi → `down({ connection, reason: 'jetstream unavailable: <message đã redact>' })`
    - còn lại → `up({ connection, server: <redactServers> , rtt: <ms làm tròn> })`
    - Mọi chờ đợi có timeout; không bao giờ ném
  - `@Module({ imports: [TerminusModule], providers: [NatsHealthIndicator], exports: [NatsHealthIndicator] }) class NatsHealthModule`

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/nats-health.spec.ts — HealthIndicatorService thật (new) + manager giả
it('is up with server and rtt', async () => expect(await ind.isHealthy('nats')).toEqual({ nats: { status: 'up', connection: 'default', server: 'nats://***@h:4222', rtt: 3 } }));
it('is down for an unknown connection', async () => expect((await ind.isHealthy('nats', { connection: 'x' })).nats).toMatchObject({ status: 'down', reason: 'unknown connection' }));
it.each(['closed', 'draining', 'reconnecting'])('is down while %s', async (state) => { /* ... */ });
it('is down when rtt exceeds the timeout', async () => { /* rtt treo, rttTimeout 50 → reason 'rtt timeout after 50ms' */ });
it('checks JetStream when asked', async () => { /* getAccountInfo ném → down 'jetstream unavailable: ...' */ });
it('never leaks credentials', async () => { /* lỗi chứa 'pass=s3cr3t' trong URL → reason không chứa 's3cr3t' */ });
// test/integration/health.int.spec.ts — TerminusModule + NatsHealthModule + HealthCheckService thật
it('reports up through HealthCheckService', async () => { /* check([() => ind.isHealthy('nats', { jetstream: true })]) → status 'ok' */ });
it('reports down after the server stops', async () => { /* srv.stop() → ServiceUnavailableException */ });
```

- [ ] **Step 2: Run tests to verify they fail** — Run: `npm test -- nats-health && npm run test:int -- health` — Expected: FAIL.
- [ ] **Step 3: Implement** theo Interfaces; devDependency `@nestjs/terminus@^12.1.0`.
- [ ] **Step 4: Verify** — Run: `npm run build && npm run check:pkg && npm test && npm run test:int && npm run lint && npm run typecheck`; `node --input-type=module -e "import('@loipv/nestjs-nats/health').then(m => console.log(typeof m.NatsHealthIndicator))"` — Expected: PASS; `function`.
- [ ] **Step 5: Commit** — `git commit -m "feat: terminus health indicator (@loipv/nestjs-nats/health)"`

---

### Task 7: Subpath `@loipv/nestjs-nats/testing`

**Files:**
- Create: `src/adapters/handler-pipeline.ts`, `src/testing/index.ts`, `src/testing/fake-connection.ts`, `src/testing/mock-clients.ts`, `src/testing/nats-testing.module.ts`, `src/testing/harness.ts`
- Modify: ba adapter (dùng `createHandlerPipeline`), `package.json` (exports `./testing`)
- Test: `test/unit/handler-pipeline.spec.ts`, `test/unit/testing-mocks.spec.ts`, `test/unit/testing-harness.spec.ts` (không cần NATS)

**Interfaces:**
- Consumes: Task 1 (outcomes), Task 3–4 (CloudEvents, router)
- Produces:
  - `createHandlerPipeline(def: HandlerDefinition, entry: ConnectionEntry, deps: { idempotencyStore(entry, handler): IdempotencyStore | undefined }): { run(d: InternalDelivery): Promise<void>; codec: Codec; info: HandlerInfo; retry: RetryPolicy | false; dlq: DlqPolicy | false }` — gom đúng logic dựng pipeline mà ba adapter đang lặp lại (codec, `toHandlerInfo`, `createSettlement`, idempotency, plugins, `invokeHandler`); adapter dùng lại, hành vi không đổi (toàn bộ integration test phải xanh)
  - `interface PublishedMessage { subject: string; data: unknown; raw: Uint8Array; headers: Record<string, string | string[]>; cloudEvent?: CloudEventMeta; reply?: string }` (`data` = payload đã decode bằng codec của client; CloudEvent → `data` của event; decode lỗi → `raw`)
  - `type MockNatsClient = NatsClient & { readonly published: PublishedMessage[]; whenRequest(subject: string): { reply(value: unknown): void; fail(error: { code: number; description: string }): void } }` — request không có stub → `NatsRequestError('no_responders')`
  - `type MockJetStreamClient = JetStreamClient & { readonly published: PublishedMessage[] }` — PubAck giả `{ stream: 'mock', seq: n, duplicate }` (`duplicate` khi `Nats-Msg-Id` đã thấy)
  - `createMockNatsClient(options?: { codec?: Codec; plugins?: NatsPlugin[]; cloudEvents?: { source?: string } }): MockNatsClient`; `createMockJetStreamClient(options?)`: `MockJetStreamClient` — là `NatsClient`/`JetStreamClient` thật chạy trên connection giả, nên plugin và CloudEvents hoạt động như thật
  - `NatsTestingModule.forRoot(options?: Omit<NatsModuleOptions, 'connection'>): DynamicModule` — global; thay cho `NatsModule.forRoot` trong test: không kết nối mạng, không sync stream, không subscribe; cung cấp các token của connection đó (client, JetStream, connection giả, class alias khi `name` là `'default'`), `NatsConnectionManager` dùng connection giả, `HandlerExplorer`, `NatsTestHarness`. `entry.jsm()` reject `NatsConfigError('JetStream management is not available in NatsTestingModule')`.
  - `@Injectable() class NatsTestHarness` — `static create(app: INestApplicationContext): NatsTestHarness` (= `app.get(NatsTestHarness)`); `dispatch(target: { handler: string } | { subject: string }, payload: unknown, options?: DispatchOptions): Promise<DispatchResult>`
    - Lệch có chủ đích so với spec §16 (`dispatch(subjectOrRoute, ...)`): đích là object để phân biệt `'Class.method'` với subject (cả hai đều chứa dấu chấm)
    - `target.handler` = `'Class.method'` (router: `'Class.@CloudEventRouter'`); `target.subject` khớp (wildcard NATS) với subject của `@Subscribe`, router subscribe, endpoint, hoặc `filter_subject(s)` trong `config` của `@Consume`; không khớp hoặc khớp nhiều → `NatsConfigError` liệt kê ứng viên
    - `interface DispatchOptions { headers?: HeadersInit; deliveryCount?: number /* 1, JetStream */; attempt?: number /* 1, core */; request?: boolean /* core subscribe: có reply subject */; cloudEvent?: CloudEventInit; subject?: string /* subject thực tế, mặc định = subject đích */ }`
    - handler batch: `payload` phải là mảng, mỗi phần tử một message
    - `interface DispatchResult { result?: unknown; settlement: 'ack' | 'nak' | 'term' | 'dlq' | 'retry' | 'drop' | 'skipped' | 'none' | 'replied'; outcomes: MessageOutcome[]; nakDelay?: number; reply?: { data?: unknown; error?: { code: number; description: string } }; dlq: PublishedMessage[] }` — `settlement` lấy từ outcome của message đầu (JetStream `retry` → `'nak'`, core `retry` → `'retry'`); retry core **không** tự chạy lại
    - idempotency store giữ trạng thái giữa các lần `dispatch` trong cùng app

- [ ] **Step 1: Write failing tests**

```ts
// test/unit/testing-mocks.spec.ts
it('records publishes with decoded data and headers', () => { /* mock.publish('o', {a:1}, { headers: { x: '1' } }) → published[0] */ });
it('stubs requests and fails unknown ones with no_responders', async () => { /* whenRequest('q').reply(5) */ });
it('records CloudEvents', () => { /* published[0].cloudEvent.type */ });
it('marks duplicate JetStream msgIds', async () => { /* lần 2 duplicate true */ });
// test/unit/testing-harness.spec.ts — Test.createTestingModule({ imports: [NatsTestingModule.forRoot({ retry: { initialDelay: 10, jitter: 0 } })], providers: [Handlers] })
it('acks a successful @Consume handler', async () => expect((await h.dispatch({ handler: 'Handlers.bill' }, { id: 1 })).settlement).toBe('ack'));
it('naks a failing @Consume handler with the backoff delay', async () => expect(await h.dispatch({ handler: 'Handlers.flaky' }, {})).toMatchObject({ settlement: 'nak', nakDelay: 10 }));
it('dead-letters an exhausted message', async () => { /* deliveryCount 6, dlq on → settlement 'dlq', dlq[0].headers['Nats-DLQ-Deliveries'] '6' */ });
it('runs pipes and guards', async () => { /* ValidationPipe → 400 BadRequest → term */ });
it('replies to a request', async () => { /* @Subscribe request: true → reply.data */ });
it('replies with service errors from endpoints', async () => { /* reply.error { code: 409, ... } */ });
it('routes CloudEvents', async () => { /* { handler: 'Events.@CloudEventRouter' }, cloudEvent { type: 'o.created', source: 's' } */ });
it('skips idempotent duplicates across dispatches', async () => { /* headers Nats-Msg-Id 'm1' hai lần → 'ack' rồi 'skipped' */ });
it('dispatches a batch', async () => { /* payload [1,2,3] → outcomes length 3 */ });
it('resolves handlers by subject', async () => { /* { subject: 'orders.created' } khớp @Subscribe('orders.*') */ });
it.each([[{ subject: 'nothing.here' }], [{ subject: 'shared.x' }], [{ handler: 'Nope.x' }]])('rejects unknown or ambiguous target %j', async (target) =>
  await expect(h.dispatch(target, {})).rejects.toThrow(NatsConfigError));
```

- [ ] **Step 2: Run tests to verify they fail** — Run: `npm test -- testing handler-pipeline` — Expected: FAIL.
- [ ] **Step 3: Implement** theo Interfaces (refactor adapter trước, chạy lại integration để chắc không đổi hành vi).
- [ ] **Step 4: Verify** — Run: `npm run build && npm run check:pkg && npm test && npm run test:int && npm run lint && npm run typecheck`; `node --input-type=module -e "import('@loipv/nestjs-nats/testing').then(m => console.log(typeof m.NatsTestHarness))"` — Expected: PASS; `function`.
- [ ] **Step 5: Commit** — `git commit -m "feat: testing utilities (@loipv/nestjs-nats/testing)"`

---

### Task 8: Public API, entrypoint guard, README

**Files:**
- Modify: `src/index.ts`, `test/unit/public-api.spec.ts`, `README.md`
- Create: `test/unit/entrypoints.spec.ts`

**Interfaces:**
- Produces:
  - Runtime export **thêm** vào entry chính: `CloudEventRouter`, `OnCloudEvent`, `CloudEvent`. Type export thêm: `CloudEventMeta`, `CloudEventInit`, `CloudEventRouterOptions`, `MessageOutcome`. Không export: codec/matcher CloudEvents nội bộ, `UnmatchedCloudEventError`, `createHandlerPipeline`.
  - Subpath export (đã có từ Task 5–7, kiểm lại): `/otel`: `tracing`, type `TracingOptions`; `/health`: `NatsHealthModule`, `NatsHealthIndicator`, type `NatsHealthCheckOptions`; `/testing`: `NatsTestingModule`, `NatsTestHarness`, `createMockNatsClient`, `createMockJetStreamClient`, type `MockNatsClient`, `MockJetStreamClient`, `PublishedMessage`, `DispatchOptions`, `DispatchResult`.
  - `entrypoints.spec.ts`: duyệt đồ thị import tĩnh (regex `from '...'`/`import('...')`) bắt đầu từ `src/index.ts`, theo các import tương đối; khẳng định không module nào trong đồ thị import chuỗi bắt đầu bằng `@opentelemetry/` hoặc `@nestjs/terminus`; và `src/otel/**` không bị import từ entry chính (tương tự `health`, `testing`).
  - README (cho npm user, tiếng Anh, mô tả đúng code): mục **CloudEvents** (publish binary/structured, `cloudEvents.source`, `@CloudEvent()`, `@CloudEventRouter` + pattern + ưu tiên + `unmatched` + 404 cho request, khoá idempotency `source|id`, `Nats-Msg-Id = ce.id`); **Tracing** (`/otel`, cài `@opentelemetry/api`, cấu hình SDK là việc của user, bảng span, attribute, events); **Health checks** (`/health`); **Testing** (`/testing`: mocks, `NatsTestingModule`, `NatsTestHarness.dispatch`); mục Plugins thêm `Delivery.outcomes`; cập nhật Features; xoá Roadmap (hoặc chỉ còn mục chưa làm nếu có); mọi block `ts` phải typecheck với build thật (cách làm như Plan 2).

- [ ] **Step 1: Write failing tests** — cập nhật danh sách runtime export trong `public-api.spec.ts` (thêm 3 tên), type-level test cho type mới; viết `entrypoints.spec.ts`.
- [ ] **Step 2: Run tests to verify they fail** — Run: `npm test -- public-api entrypoints` — Expected: FAIL ở public-api.
- [ ] **Step 3: Implement** `src/index.ts` và README.
- [ ] **Step 4: Verify** — Run: `npm run build && npm run check:pkg && npm run typecheck && npm run lint && npm test && npm run test:int` + typecheck các block README — Expected: tất cả PASS; publint/attw sạch cho cả 4 entrypoint.
- [ ] **Step 5: Commit** — `git commit -m "feat: export CloudEvents API; docs: README for CloudEvents, tracing, health and testing"`
