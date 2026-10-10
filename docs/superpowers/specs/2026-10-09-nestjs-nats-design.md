# `@loipv/nestjs-nats` — Design Spec

- **Ngày:** 2026-10-09
- **Trạng thái:** Đã duyệt thiết kế qua hội thoại, chờ review spec
- **Loại:** Architectural (project mới, thư viện public)

## 1. Mục tiêu & bối cảnh

Thư viện **public, publish lên npm** để tích hợp NATS (core + JetStream + Services) vào ứng dụng NestJS, hướng tới cộng đồng → ưu tiên API ổn định, semver nghiêm ngặt, tài liệu đầy đủ, dependency tối thiểu (tính năng phụ là opt-in).

### Tính năng (yêu cầu gốc)
1. Decorator-driven subscriber, consumer, service-endpoint handlers
2. JetStream stream management & publishing
3. CloudEvent routing
4. Parameter injection với pipes
5. Client wrapper đơn giản cho fire-and-forget pub/sub
6. OpenTelemetry tracing: produce → consume cùng trace ID
7. DLQ: retry tự động với exponential backoff
8. Health check tích hợp `@nestjs/terminus`
9. Graceful shutdown
10. Idempotency: chống trùng in-memory có TTL
11. Batch processing: gom theo size + timeout

### Tiêu chí thành công
- Mỗi tính năng trên có ít nhất một integration test end-to-end chạy với `nats-server` thật.
- CI xanh trên ma trận Nest 11/12 × Node 22/24, kèm smoke test app Nest 11 CJS `require()` được lib.
- `publint` và `@arethetypeswrong/cli` không báo lỗi.
- Không có tính năng phụ nào buộc user cài thêm dependency khi không dùng tới.

## 2. Quyết định nền tảng

| Hạng mục | Quyết định | Lý do |
|---|---|---|
| Kiến trúc | Dynamic module độc lập + `DiscoveryService` + `ExternalContextCreator` (không phải custom transport strategy) | Kiểm soát hoàn toàn vòng đời message (ack/nak/term, batch, DLQ, idempotency); chạy trong mọi loại app Nest; vẫn có pipes/guards/interceptors/filters |
| NestJS | `^11.0.0 \|\| ^12.0.0` | Nest 12 (ESM-only) đã phát hành 09/2026; 11.x vẫn được bảo trì |
| Node | `>=22.12` | Có `require(esm)` không cần flag → app CJS Nest 11 dùng được lib ESM |
| Module format | **ESM-only**, build bằng `tsc` (`module: NodeNext`), không bundler | Một bản build, không dual-package hazard |
| Client NATS | `@nats-io/transport-node`, `@nats-io/jetstream`, `@nats-io/services` `^3` | Client chính thức thế hệ mới |
| Kết nối | Nhiều named connection | Ảnh hưởng DI token và discovery, phải có từ đầu |
| License | MIT | Thông lệ hệ sinh thái Nest |
| Version | Bắt đầu `0.x`, lên `1.0.0` khi API ổn định | Cho phép điều chỉnh API theo feedback |

### Dependencies
- **peerDependencies:** `@nestjs/common`, `@nestjs/core` (`^11 || ^12`), `reflect-metadata`, `rxjs`, `@nats-io/transport-node`, `@nats-io/jetstream`, `@nats-io/services` (`^3`). `@nats-io/*` là peer vì lib expose type của chúng (`NatsConnection`, `JsMsg`, `PubAck`...).
- **optional peerDependencies** (`peerDependenciesMeta.optional`): `@opentelemetry/api`, `@nestjs/terminus`.
- **dependencies:** không có (mục tiêu).

## 3. Package layout

### Subpath exports

| Import | Nội dung | Peer cần thêm |
|---|---|---|
| `@loipv/nestjs-nats` | Module, decorators, clients, JetStream mgmt, CloudEvents, retry/DLQ, idempotency, batch, shutdown, errors | — |
| `@loipv/nestjs-nats/otel` | `tracing()` plugin | `@opentelemetry/api` |
| `@loipv/nestjs-nats/health` | `NatsHealthModule`, `NatsHealthIndicator` | `@nestjs/terminus` |
| `@loipv/nestjs-nats/testing` | `createMockNatsClient()`, `NatsTestHarness` | — |

Entry chính **không bao giờ** import tĩnh `@opentelemetry/api` hoặc `@nestjs/terminus`.

### Cấu trúc `src/`
Mỗi thư mục là một đơn vị có interface rõ, test độc lập được:

```
src/
  index.ts              # public API của entry chính
  module/               # NatsModule (forRoot/forRootAsync/forFeature), options, DI tokens, inject decorators
  connection/           # NatsConnectionManager: connect, fail-fast, status$, drain/close
  decorators/           # handler decorators + param decorators + metadata keys
  discovery/            # HandlerExplorer: quét, validate, tạo handler bằng ExternalContextCreator
  pipeline/             # Delivery, InboundMiddleware/OutboundMiddleware, compose, invoker
  adapters/             # source adapters: core subscription, JetStream consumer, service endpoint
  settlement/           # retry policy, backoff, DLQ publisher, settlement middleware
  idempotency/          # IdempotencyStore interface, InMemoryIdempotencyStore, middleware
  batch/                # batch collector (consume), Batcher (publish)
  jetstream/            # NatsStreamManager, stream/consumer sync, JetStreamClient
  client/               # NatsClient (core)
  cloudevents/          # CE codec (binary/structured), router, type matcher
  codec/                # Codec interface, jsonCodec
  shutdown/             # ShutdownCoordinator
  errors/               # error classes
  otel/                 # subpath /otel
  health/               # subpath /health
  testing/              # subpath /testing
```

## 4. Module & cấu hình

```ts
NatsModule.forRoot({
  name: 'default',                    // tên đăng ký (DI); mặc định 'default'
  connection: {                       // = ConnectionOptions của @nats-io/transport-node, pass-through nguyên vẹn
    servers: ['nats://localhost:4222'],
    name: 'order-service',            // tên client hiển thị trên server monitoring (khác `name` ở trên)
    authenticator: credsAuthenticator(creds),
    tls: { caFile: '/etc/nats/ca.pem' },
  },
  jetstream: {                        // tuỳ chọn
    options: { domain: 'hub' },       // JetStreamOptions pass-through
    sync: 'create-or-update',         // 'create-or-update' | 'create-only' | 'verify' | 'none'
  },
  streams: [{ name: 'ORDERS', subjects: ['orders.>'] }],
  consumers: [{ stream: 'ORDERS', config: { durable_name: 'billing', filter_subject: 'orders.created' } }],
  codec: jsonCodec(),
  retry: { maxRetries: 5, initialDelay: 1000, factor: 2, maxDelay: 60_000, jitter: 0.2 },
  dlq: undefined,                     // tắt mặc định; xem §7
  idempotency: { store: new InMemoryIdempotencyStore(), ttl: 300_000 },
  cloudEvents: { source: 'urn:service:orders' },
  plugins: [],                        // NatsPlugin[], xem §6.2
  shutdown: { drainTimeout: 10_000 },
  logger: undefined,                  // undefined = Nest Logger; false = tắt; hoặc LoggerService
});

NatsModule.forRootAsync({ name, imports, inject, useFactory, useClass, useExisting });
NatsModule.forFeature({ connection: 'default', streams: [...], consumers: [...] });
```

- **Option `connection` lồng riêng** (không flatten vào root) để tránh đụng tên `name` giữa tên đăng ký DI và tên client NATS. Toàn bộ `ConnectionOptions` được truyền nguyên vẹn vào `connect()`.
- Module là `global`. Mỗi connection có bộ DI token riêng sinh từ `name`.
- Inject: `@InjectNatsClient(name?)` → `NatsClient`, `@InjectJetStream(name?)` → `JetStreamClient`, `@InjectNatsConnection(name?)` → `NatsConnection`, `@InjectStreamManager(name?)` → `NatsStreamManager`. Thiếu `name` = `'default'`.
- Gọi `forRoot` hai lần cùng `name` → `NatsConfigError`.

### Auth
- Hỗ trợ **mọi** cơ chế của NATS qua pass-through `connection`: user/pass, token, NKey, JWT/`.creds` (`credsAuthenticator`, `jwtAuthenticator`), `multiAuthenticator`, TLS/mTLS (`tls.caFile/certFile/keyFile/handshakeFirst`). Lib **không** thêm helper auth riêng; README có ví dụ tự viết authenticator function để rotate credentials.
- **Fail-fast:** lần connect đầu lỗi (gồm `AUTHORIZATION_VIOLATION`, `AUTHENTICATION_EXPIRED`, không tới được server) → ném `NatsConnectionError`, app dừng bootstrap. Sau khi đã kết nối, reconnect theo cơ chế mặc định của client NATS.
- **Redaction:** log / health / error message chỉ chứa `servers` và tên; không bao giờ chứa `pass`, `token`, seed, nội dung creds.

## 5. Decorators & discovery

### Handler decorators

| Decorator | Đặt trên | Ý nghĩa |
|---|---|---|
| `@Subscribe(subject, opts?)` | method | Core NATS subscription. `opts`: `connection`, `queue`, `codec`, `concurrency`, `retry`, `dlq`, `idempotency`, `batch` |
| `@Consume(opts)` | method | JetStream pull consumer. `opts`: `connection`, `stream`, `durable`, `config?` (nếu có → lib tạo/cập nhật consumer), `ack: 'auto' \| 'manual'`, `codec`, `concurrency`, `retry`, `dlq`, `idempotency`, `batch` |
| `@NatsService(opts)` | class | Đăng ký một service của `@nats-io/services`: `name`, `version`, `description?`, `queue?`, `metadata?`, `connection?` |
| `@ServiceEndpoint(name, opts?)` | method trong `@NatsService` | Endpoint: `subject?`, `queue?`, `metadata?`, `codec?` |
| `@CloudEventRouter(opts)` | class | Một subscription/consumer cho cả class: `{ subscribe: subject, queue? }` hoặc `{ consume: { stream, durable, config? } }`, cộng `unmatched`, `connection`, `concurrency`, `retry`, `dlq`, `idempotency` |
| `@OnCloudEvent(typePattern)` | method trong `@CloudEventRouter` | Nhận CE có `type` khớp pattern |

- `@Subscribe`: nếu message có reply subject và handler trả giá trị khác `undefined` → encode và `respond()`.
- `@ServiceEndpoint`: giá trị trả về → `respond()`; throw → `respondError(code, description)`.

### CloudEvent type matching
- `type` được chia theo dấu `.`. `*` khớp đúng một segment, `>` khớp một hoặc nhiều segment cuối (giống NATS).
- Ưu tiên: khớp chính xác > pattern có nhiều segment literal hơn > pattern dùng `>`. Hai handler cùng pattern trong cùng router → `NatsConfigError` khi bootstrap.
- `unmatched`: `'ack'` (mặc định, log debug) | `'term'` | `'dlq'`. Với core subscription, `'term'` tương đương bỏ qua. `'dlq'` khi DLQ tắt → xử lý như `'term'` kèm log warning.

### Param decorators
- `@Payload(property?, ...pipes)` — dữ liệu đã decode (với CE: trường `data`; với batch: mảng).
- `@Headers(name?)` — `Record<string, string | string[]>` hoặc một giá trị.
- `@Subject()` — subject thực tế của message.
- `@Msg()` — `Msg` hoặc `JsMsg` thô (batch: mảng).
- `@CloudEvent()` — `CloudEventMeta` (`id`, `source`, `type`, `specversion`, `time?`, `subject?`, `datacontenttype?`, `dataschema?`, `extensions`).
- `@Ctx()` — `NatsContext` (core) / `NatsJetStreamContext` (thêm `ack()`, `nak(delay?)`, `term(reason?)`, `working()`, `deliveryCount`, `stream`, `seq`) / `NatsBatchContext` (thêm `messages`, `fail(msg, err)`).

### Tích hợp enhancer của Nest
- Metadata tham số lưu dưới `ROUTE_ARGS_METADATA` (cùng key `@nestjs/microservices` dùng) → `createParamDecorator` của Nest hoạt động.
- Handler được bọc bởi `ExternalContextCreator.create(..., contextType = 'nats')` và được gọi dạng `fn(payload, natsContext)`. Do đó `ExecutionContext.getType() === 'nats'`, `switchToRpc().getData()` = payload, `switchToRpc().getContext()` = `NatsContext`.
- Pipes (gồm global `ValidationPipe`), guards, interceptors, exception filters đều chạy. Filter nuốt lỗi (không rethrow) → coi như thành công.

### Thứ tự khởi động
1. Provider factory kết nối từng connection (fail-fast như §4).
2. `onModuleInit`: đồng bộ streams/consumers khai báo (§8.1).
3. `onApplicationBootstrap`: `HandlerExplorer` quét providers + controllers, validate, rồi bắt đầu subscribe/consume/đăng ký service.

### Validate khi bootstrap (`NatsConfigError`)
- `connection` không tồn tại.
- Hai handler cùng `stream` + `durable`.
- `@OnCloudEvent` ngoài class `@CloudEventRouter`; `@ServiceEndpoint` ngoài class `@NatsService`.
- Handler trong provider scope `REQUEST` hoặc `TRANSIENT` (v1 không hỗ trợ).
- `batch` dùng cùng `@CloudEventRouter`, `@ServiceEndpoint`, hoặc `ack: 'manual'` (v1 không hỗ trợ).
- `@Consume` không có `config` mà consumer không tồn tại trên server.

## 6. Message pipeline

### 6.1 Inbound

```
Source adapter (core sub | JS consumer | service endpoint)
  └─▶ [batch collector]                  (chỉ khi handler có `batch`)
       └─▶ Delivery { messages[], ctx, handler }
            ├─ plugin inbound middlewares (stage 'outer')   ← tracing
            ├─ settlement                                    (retry / DLQ / ack-nak-term)
            ├─ idempotency                                   (nếu bật)
            ├─ plugin inbound middlewares (stage 'inner')   ← mặc định cho plugin của user
            └─ invoker                                       (decode → CE dispatch → Nest enhancers → handler)
```

- `Delivery` luôn chứa `messages[]` (1 phần tử với handler đơn, N với batch). Middleware viết một lần cho cả hai trường hợp; public API handler vẫn tách `T` vs `T[]`.
- `interface InboundMiddleware { handle(d: Delivery, next: () => Promise<void>): Promise<void> }`.
- Lỗi trong bất kỳ middleware nào được bắt ở adapter, không làm crash process; được coi là handler thất bại.

### 6.2 Plugins
```ts
interface NatsPlugin {
  name: string;
  inbound?: InboundMiddleware & { stage?: 'outer' | 'inner' };   // mặc định 'inner'
  outbound?: OutboundMiddleware;
}
interface OutboundMiddleware { handle(m: OutgoingMessage, next: () => Promise<void>): Promise<void> }
```
Plugin theo connection (option `plugins` của `forRoot`). Logic riêng từng handler dùng interceptor của Nest.

### 6.3 Outbound
`OutgoingMessage { subject, data, headers, cloudEvent?, kind: 'publish' | 'request' | 'jetstream' }` đi qua: plugin outbound middlewares (tracing inject) → CloudEvent encoding → codec encode → gửi.

### 6.4 Concurrency & backpressure
- `concurrency` mặc định `1` (giữ thứ tự) cho mỗi handler / router / endpoint.
- JetStream: lib chỉ kéo thêm message khi còn slot; buffer pull được tính từ `concurrency` và `batch.maxSize`.

## 7. Retry, settlement & DLQ

### Cấu hình
- `retry`: mặc định **bật** với `{ maxRetries: 5, initialDelay: 1000, factor: 2, maxDelay: 60000, jitter: 0.2 }`; `retry: false` để tắt. Đặt ở module, ghi đè ở handler.
- Backoff cho lần retry thứ `n` (n ≥ 1): `min(maxDelay, initialDelay × factor^(n−1))`, nhân ngẫu nhiên trong `[1 − jitter, 1 + jitter]`.
- `isRetryable(err)`: mặc định `false` cho `NatsDecodeError`, `NonRetryableError`, `HttpException` có status 4xx (gồm lỗi `ValidationPipe`, `ForbiddenException` từ guard); `true` cho mọi lỗi khác. Có thể ghi đè ở module/handler.
- `dlq`: mặc định **tắt**. Bật bằng `dlq: true` hoặc `dlq: { subject?: string | ((d: Delivery) => string), mode?: 'jetstream' | 'core' }`.
  - Subject mặc định: `dlq.<stream>.<durable>` cho `@Consume`/router consume; `dlq.<subject thực tế>` cho `@Subscribe`/router subscribe.
  - `mode` mặc định: `'jetstream'` cho handler JetStream (publish có PubAck — cần một stream bắt subject DLQ, nếu không publish sẽ lỗi và message được nak); `'core'` cho core subscription.
- Header trên message DLQ (giữ nguyên body và header gốc): `Nats-DLQ-Subject`, `Nats-DLQ-Stream`, `Nats-DLQ-Consumer`, `Nats-DLQ-Seq`, `Nats-DLQ-Deliveries`, `Nats-DLQ-Error` (message lỗi, cắt tối đa 1024 ký tự), `Nats-DLQ-Timestamp` (ISO 8601).

### Settlement — JetStream (`ack: 'auto'`)
`deliveryCount` bắt đầu từ 1; tổng số lần xử lý tối đa = `maxRetries + 1`.

| Kết quả | Hành động |
|---|---|
| Thành công | `ack()` |
| Lỗi retryable và `deliveryCount ≤ maxRetries` | `nak(backoff(deliveryCount))` |
| Hết lượt hoặc non-retryable, DLQ bật | publish DLQ → thành công: `term()`; thất bại: `nak(backoff(deliveryCount))` |
| Hết lượt hoặc non-retryable, DLQ tắt | `term()` + log error |
| `retry: false` và lỗi | như "hết lượt" |

- **Heartbeat:** trong khi handler chạy, tự gọi `working()` mỗi `ack_wait / 2` (ack_wait lấy từ consumer info lúc bootstrap).
- **`ack: 'manual'`:** lib không tự ack khi thành công. Nếu handler throw khi message chưa được settle → áp dụng bảng trên.
- **Consumer do lib tạo:** nếu user không đặt `max_deliver`, lib đặt `-1` để quyết định DLQ nằm ở lib. Nếu consumer có `max_deliver > 0` và `≤ maxRetries` → log warning khi bootstrap (server sẽ ngừng giao trước khi lib kịp đẩy DLQ).

### Settlement — core subscription
- Không có reply subject: retry trong process bằng `setTimeout` theo cùng công thức backoff; hết lượt → DLQ (nếu bật) hoặc log error. Retry đang chờ bị mất nếu process chết (chấp nhận).
- **Có reply subject** (request/reply): **không retry**. Lỗi → reply với body rỗng và header `Nats-Service-Error` + `Nats-Service-Error-Code`.
- `@ServiceEndpoint`: không retry; lỗi → `respondError(code, description)`.
- Mã lỗi và mô tả khi reply: `HttpException` → status + message của nó; lỗi khác → `500` + `"Internal Server Error"` (không lộ chi tiết nội bộ). Exception filter có thể tuỳ biến.

### Batch
- Handler throw → toàn bộ messages trong batch đi theo bảng settlement (từng message với `deliveryCount` riêng).
- Partial failure: handler trả về `{ failed: Array<{ msg, error }> }` hoặc gọi `ctx.fail(msg, err)` → message thành công được ack, message thất bại đi theo settlement riêng.

## 8. JetStream & clients

### 8.1 Stream / consumer management
- Nguồn khai báo: `streams`/`consumers` trong `forRoot`/`forFeature` và `config` trong `@Consume` / `@CloudEventRouter({ consume })`.
- `jetstream.sync`:
  - `'create-or-update'` (mặc định): thiếu → tạo; có → so sánh, cập nhật trường mutable.
  - `'create-only'`: thiếu → tạo; có → không đụng.
  - `'verify'`: không tạo/sửa; lệch hoặc thiếu → `NatsConfigError`.
  - `'none'`: bỏ qua hoàn toàn.
- Lệch trường immutable (vd. `storage`, `retention`) → `StreamConfigConflictError` nêu rõ tên trường, giá trị hiện tại và giá trị khai báo. Lib **không bao giờ** tự xoá/tạo lại stream hay consumer.
- `NatsStreamManager`: `ensureStream(cfg)`, `ensureConsumer(stream, cfg)`, `info(stream)`, `purge(stream, opts?)`, `delete(stream)`, `jsm` (raw `JetStreamManager`).

### 8.2 `NatsClient` (core)
```ts
publish(subject: string, data?: unknown, opts?: { headers?: HeadersInit; codec?: Codec; cloudEvent?: CloudEventInit; reply?: string }): void
request<T>(subject: string, data?: unknown, opts?: { timeout?: number; headers?: HeadersInit; codec?: Codec }): Promise<T>
flush(): Promise<void>
```
`request` lỗi → `NatsRequestError` với `code`: `'timeout' | 'no_responders' | 'service_error'` (kèm `serviceCode`, `description` khi responder trả header lỗi).

### 8.3 `JetStreamClient`
```ts
publish(subject, data?, opts?: { msgId?; expect?: { lastMsgId?; streamName?; lastSequence?; lastSubjectSequence? }; headers?; codec?; cloudEvent?; timeout? }): Promise<PubAck>
publishMany(items: Array<{ subject; data?; opts? }>, opts?: { window?: number /* mặc định 256 */ }): Promise<Array<{ ok: true; ack: PubAck } | { ok: false; error: Error }>>
batcher(opts?: { maxSize?: number /* 100 */; maxWait?: number /* 50ms */; window?: number }): Batcher
```
`Batcher`: `add(subject, data?, opts?) → Promise<PubAck>` (resolve khi message của nó có PubAck), `flush() → Promise<void>`, `close() → Promise<void>`. Flush khi đủ `maxSize` hoặc hết `maxWait`. Mọi batcher được đăng ký với ShutdownCoordinator. v1 **không** có atomic batch: mỗi message có PubAck riêng.

### 8.4 Headers & codec
- `HeadersInit = Record<string, string | string[]>`.
- `interface Codec { contentType: string; encode(data: unknown): Uint8Array; decode(bytes: Uint8Array, headers: Headers): unknown }`. Mặc định `jsonCodec()`. `Uint8Array` được gửi nguyên byte, không encode. Payload rỗng decode thành `undefined`.
- Ưu tiên codec: tham số publish/handler > module.

## 9. CloudEvents

- Phiên bản spec: CloudEvents 1.0, NATS protocol binding.
- **Đọc:** binary mode (có header `ce-specversion`; attribute từ header `ce-*`, `datacontenttype` từ `Content-Type`) và structured mode (`Content-Type: application/cloudevents+json`; envelope JSON). Thiếu attribute bắt buộc (`id`, `source`, `type`, `specversion`) → `NatsDecodeError` (non-retryable).
  - Payload: nếu event khai báo `datacontenttype` không phải JSON (`application/json`, `text/json` hoặc hậu tố `+json`) và khác content type của codec → handler nhận `Uint8Array` thô; ngược lại giải mã bằng codec của handler.
- **Ghi:** option `cloudEvent: { type; source?; subject?; id?; time?; datacontenttype?; dataschema?; extensions?; mode?: 'binary' | 'structured' }` trên `publish`. Lib tự điền `id` (`crypto.randomUUID()`), `time` (ISO 8601), `specversion: '1.0'`, `datacontenttype` (lấy từ `cloudEvent.datacontenttype` nếu có, mặc định theo content type của codec, và bỏ qua khi data là `Uint8Array` không khai báo), `source` (từ `cloudEvents.source` của module; thiếu cả hai → `NatsConfigError`). Mặc định `mode: 'binary'`.
- JetStream: `Nats-Msg-Id` ưu tiên `msgId` > header `Nats-Msg-Id` của caller > `ce.id` → dedupe phía server.

## 10. Idempotency

```ts
interface IdempotencyStore {
  claim(key: string, ttlMs: number): Promise<'claimed' | 'duplicate' | 'in-progress'>;
  complete(key: string, ttlMs: number): Promise<void>;
  release(key: string): Promise<void>;
  close?(): Promise<void>;
}
```
- Bật per-handler bằng `idempotency: true | { ttl?; keyExtractor?: (d: Delivery['messages'][number]) => string | undefined }`; store và `ttl` mặc định lấy từ module. Mặc định **tắt**.
- Key: `Nats-Msg-Id` → CE `source + '|' + id` → `keyExtractor`. Không có key → xử lý bình thường, log debug.
- Luồng: `claim` trước khi gọi handler → thành công: `complete`; lỗi: `release`.
  - `duplicate` → ack (JetStream) / bỏ qua (core), không gọi handler.
  - `in-progress` → JetStream: `nak(retry.initialDelay)` (1000 ms nếu `retry: false`); core: bỏ qua.
- Batch: message `duplicate` bị loại khỏi batch trước khi gọi handler (và được ack).
- `InMemoryIdempotencyStore({ maxKeys = 100_000 })`: Map có TTL, quét hết hạn lười + định kỳ, vượt `maxKeys` → loại theo LRU. Chỉ có hiệu lực trong một instance (ghi rõ trong docs).

## 11. Batch processing (consume)

- Option `batch: { maxSize?: number /* 100 */; maxWait?: number /* 1000ms */ }` trên `@Subscribe` / `@Consume`.
- Flush khi đủ `maxSize` hoặc `maxWait` tính từ message đầu tiên của batch.
- Handler nhận `@Payload() items: T[]`, `@Msg() msgs`, `@Ctx() ctx: NatsBatchContext`.
- Batch handler không bao giờ reply, kể cả khi message có reply subject.
- Pipes áp dụng trên toàn mảng (user dùng `ParseArrayPipe` hoặc pipe tuỳ chỉnh nếu cần validate từng phần tử).

## 12. OpenTelemetry (`/otel`)

```ts
import { tracing } from '@loipv/nestjs-nats/otel';
plugins: [tracing({ captureHeaders?: string[], tracerName?: string })]
```
- Chỉ phụ thuộc `@opentelemetry/api`; không có SDK → no-op.
- Inbound stage `'outer'`; outbound inject header.
- Propagation qua global propagator (mặc định W3C `traceparent`/`tracestate` + baggage) vào header NATS. Structured CE không có header → đọc extension `traceparent` trong envelope.
- Span:
  | Tình huống | Span |
  |---|---|
  | publish / JetStream publish | `publish <subject>` (PRODUCER) |
  | consume message đơn | `process <subject>` (CONSUMER), **con** của context trích từ header → cùng trace ID |
  | consume batch | `process <subject>` (CONSUMER), trace mới, **link** tới từng message |
  | request | `send <subject>` (CLIENT) |
  | service endpoint / reply handler | `process <subject>` (SERVER), con của context từ header |
- Attributes: `messaging.system=nats`, `messaging.destination.name`, `messaging.operation.type`, `messaging.operation.name`, `messaging.message.id`, `messaging.consumer.group.name` (durable/queue), `messaging.batch.message_count`, `messaging.message.body.size`, `messaging.nats.stream`, `messaging.nats.seq`, `messaging.nats.delivery_count`, cộng `messaging.header.<name>` cho `captureHeaders`.
- Lỗi → `recordException` + status `ERROR`. Retry (nak), DLQ, duplicate được ghi thành span event.

## 13. Health (`/health`)

```ts
@Module({ imports: [TerminusModule, NatsHealthModule] })
// ...
() => natsHealth.isHealthy('nats', { connection?: 'default', rttTimeout?: 1000, jetstream?: false })
```
- Dùng `HealthIndicatorService` (terminus 11 & 12).
- Kiểm tra: connection không `closed`/`draining` và đang kết nối; `nc.rtt()` < `rttTimeout`; nếu `jetstream: true` → `jsm.getAccountInfo()` thành công.
- Kết quả `up` kèm `{ server, rtt }`; `down` kèm lý do (không chứa secret).
- Entry chính có `NatsConnectionManager.status(name)` cho user không dùng terminus.

## 14. Graceful shutdown

Yêu cầu user gọi `app.enableShutdownHooks()` (ghi trong README). Trong `beforeApplicationShutdown`, các connection xử lý song song:

1. Ngừng nhận mới: dừng consumers, `drain()` subscriptions, `stop()` services.
2. Flush batch collector đang gom dở (giao cho handler) và chờ handler đang chạy, tối đa `drainTimeout` (mặc định 10 000 ms). Flush mọi `Batcher`.
3. Quá timeout: message JetStream chưa settle → `nak()`; huỷ retry in-process đang chờ của core subscription và log số lượng.
4. `nc.drain()` rồi đóng; gọi `store.close?.()` của idempotency store.

## 15. Error handling & observability

| Class | Khi nào |
|---|---|
| `NatsModuleError` | base |
| `NatsConfigError` | cấu hình/validate khi bootstrap |
| `NatsConnectionError` | connect lần đầu thất bại (gồm auth) |
| `StreamConfigConflictError` | lệch trường immutable của stream/consumer |
| `NatsDecodeError` | không decode được payload/CloudEvent (non-retryable) |
| `NatsRequestError` | request thất bại (`code`) |
| `NonRetryableError` | user throw để vào DLQ ngay |

- `NatsConnectionManager.status$(name): Observable<NatsStatusEvent>` phát `disconnect`, `reconnect`, `ldm`, `error`, `staleConnection`.
- Log qua Nest `Logger` (context `NatsModule`), tắt bằng `logger: false` hoặc thay bằng `LoggerService`.

## 16. Testing

### Của lib
- **Unit** (vitest + `unplugin-swc` cho decorator metadata): backoff, `isRetryable`, bảng settlement (table-driven), codec, CE read/write binary + structured, type matcher, `InMemoryIdempotencyStore`, batch collector, `Batcher` (fake timers), stream config diff.
- **Integration** (Testcontainers, image `nats` có `-js`; app Nest thật): publish→consume; request/reply (thành công + lỗi); service endpoint; CE routing (exact, wildcard, unmatched); retry→DLQ (kiểm tra header); non-retryable → DLQ ngay; idempotency; batch + partial failure; batch publish; tracing (`InMemorySpanExporter`, cùng trace ID); health up/down; shutdown khi đang có handler chạy; fail-fast khi sai auth; `StreamConfigConflictError`; multi-connection.
- **CI matrix:** Nest 11 & 12 × Node 22 & 24; smoke test app Nest 11 CJS `require('@loipv/nestjs-nats')`; `publint`; `@arethetypeswrong/cli`.

### Cho user (`/testing`)
- `createMockNatsClient()` / `createMockJetStreamClient()`: ghi lại publish (`.published`), stub `request` (`.whenRequest(subject).reply(value)`).
- `NatsTestHarness.create(app)` + `harness.dispatch(subjectOrRoute, payload, { headers?, deliveryCount?, cloudEvent? })`: chạy message giả qua **đúng pipeline thật** (enhancers, idempotency, quyết định settlement) không cần NATS server; trả về `{ result, settlement: 'ack' | 'nak' | 'term' | 'dlq', reply? }`.

## 17. Build & release

- TypeScript strict, `tsc` → ESM `dist/` + `.d.ts` + sourcemap; `package.json` có `"type": "module"`, `exports` cho 4 subpath, `engines.node >= 22.12`, `sideEffects: false`.
- Biome (lint + format).
- Changesets (version + CHANGELOG). GitHub Actions: CI + publish với npm **trusted publishing (OIDC) + provenance**.
- Docs: `README.md` (quick start), `docs/` mỗi tính năng một trang, `examples/` app mẫu + `docker-compose.yml`.

## 18. Ngoài phạm vi v1

KV, Object Store, handler request/transient-scoped, adapter `@MessagePattern`/custom transport strategy, idempotency store phân tán built-in, schema registry, helper rotate credentials.

Ordered consumer, push consumer và atomic batch publish không còn nằm ngoài phạm vi: xem [2026-10-10-consumers-atomic-design.md](./2026-10-10-consumers-atomic-design.md).

## 19. Thứ tự triển khai (đầu vào cho implementation plan)

1. **Core:** connection + module + DI tokens, codec, errors, decorators, discovery, pipeline, adapters (`@Subscribe`, `@Consume`, `@ServiceEndpoint`), param decorators + enhancers, `NatsClient`, `JetStreamClient.publish`, stream/consumer sync.
2. **Độ tin cậy:** settlement/retry/DLQ, idempotency, batch consume, `publishMany` + `Batcher`, graceful shutdown.
3. **Tích hợp:** CloudEvents (codec + router), `/otel`, `/health`, `/testing`.
4. **Release:** docs, examples, CI matrix, publish pipeline.
