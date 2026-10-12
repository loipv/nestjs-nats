# Plugin OpenTelemetry metrics — Design Spec

- **Ngày:** 2026-10-12
- **Trạng thái:** Đã duyệt thiết kế qua hội thoại, chờ review spec
- **Loại:** Architectural (public API mới `metrics()` trong subpath `@loipv/nestjs-nats/otel`)
- **Sub-project:** SP-H của đợt xử lý limitation

## 1. Bối cảnh

`@loipv/nestjs-nats/otel` chỉ có `tracing()`. Muốn có dashboard và cảnh báo (throughput, độ trễ handler, tỉ lệ lỗi, retry, dead letter, duplicate), người dùng phải tự viết plugin, hoặc sinh metrics từ span bằng connector `spanmetrics` của OTel Collector. Cách thứ hai bị sampling làm sai số đếm và không phân biệt được outcome. Pipeline plugin đã có sẵn những gì một plugin metrics cần: `delivery.outcomes`, `delivery.handler` và `OutgoingMessage`.

## 2. Mục tiêu

1. Plugin `metrics()` đo mọi delivery và mọi message gửi đi của connection, đặt cạnh `tracing()`, dùng riêng hoặc dùng chung với nó.
2. Tên metric theo semantic conventions messaging của OpenTelemetry, thêm thuộc tính `nats.*` cho handler và outcome.
3. Số series có giới hạn theo mặc định. Subject chỉ được gắn khi người dùng bật.
4. Chỉ dùng `@opentelemetry/api` (peer tùy chọn đã có, `^1.9.0`). Không có SDK thì plugin không làm gì.

### Ngoài phạm vi

- Gauge trạng thái: số handler đang chạy, trạng thái connection, trạng thái handler.
- Consumer lag (`num_pending`, `num_ack_pending`).
- Exemplar, tùy chỉnh tên metric hoặc bucket.
- Gộp với `tracing()` thành một plugin.

## 3. API

```ts
export interface MetricsOptions {
  /** Name of the meter the instruments are created with. Default: '@loipv/nestjs-nats'. */
  meterName?: string;
  /**
   * Adds `messaging.destination.name`: `true` records the subject as is, a function records
   * what it returns for the subject (nothing when it returns `undefined`), for example to
   * turn `orders.123` into `orders.*`. Default: `false` (subjects can hold ids, and every
   * distinct value is a new series).
   */
  subjects?: boolean | ((subject: string) => string | undefined);
}

export function metrics(options?: MetricsOptions): NatsPlugin;
```

Export từ `src/otel/index.ts` cạnh `tracing`. Plugin có `name: 'otel-metrics'`, middleware inbound `stage: 'outer'` và một middleware outbound.

## 4. Metric

Histogram có đơn vị `s`, dùng bucket mà semconv khuyến nghị, truyền qua `advice.explicitBucketBoundaries`: `[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]`. Counter có đơn vị `{message}`.

### 4.1 Inbound

| Metric | Loại | Ghi khi | Thuộc tính ngoài phần chung |
|---|---|---|---|
| `messaging.process.duration` | Histogram | Một lần cho mỗi delivery (một batch là một delivery) | `error.type` khi `next()` ném lỗi |
| `messaging.client.consumed.messages` | Counter (+1) | Một lần cho mỗi message của delivery | `nats.outcome`; `error.type` khi outcome có `error` |

Thuộc tính chung của inbound:

| Thuộc tính | Giá trị |
|---|---|
| `messaging.system` | `nats` |
| `messaging.operation.name` | `process` |
| `nats.connection` | `delivery.handler.connection` |
| `nats.handler` | `<className>.<methodName>` |
| `nats.handler.kind` | `subscribe`, `consume`, `ordered` hoặc `service-endpoint` |
| `messaging.consumer.group.name` | Durable của `consume`, hoặc queue group của `subscribe` khi có |
| `nats.stream` | Stream của `consume` và `ordered` |
| `messaging.destination.name` | Chỉ khi bật `subjects`. Histogram dùng subject của message đầu tiên; counter dùng subject của từng message |

`nats.outcome` là `action` của `MessageOutcome` (`ack`, `retry`, `dlq`, `term`, `drop`, `skipped`, `replied`, `none`), hoặc `unsettled` khi message không có outcome.

### 4.2 Outbound

| Metric | Loại | Ghi khi |
|---|---|---|
| `messaging.client.sent.messages` | Counter (+1) | Mỗi `OutgoingMessage` mà `next()` đã settle |
| `messaging.client.operation.duration` | Histogram | Cùng thời điểm, đo từ lúc vào plugin |

Thuộc tính:

| Thuộc tính | Giá trị |
|---|---|
| `messaging.system` | `nats` |
| `messaging.operation.name` | `request` khi `kind` là `request`, ngược lại `publish` |
| `nats.connection` | `m.connection` |
| `nats.publish.kind` | `core` (kind `publish`), `jetstream` hoặc `request` |
| `nats.dead_letter` | `true` khi `m.deadLetter` được đặt; không có thuộc tính này ở message thường |
| `error.type` | Khi `next()` ném lỗi |
| `messaging.destination.name` | Chỉ khi bật `subjects` |

### 4.3 `error.type`

`err.name` khi `err` là `Error` có `name` không rỗng, ngược lại `_OTHER`.

### 4.4 Ngữ nghĩa đếm

- Retry (redelivery của JetStream, retry trong process của core, retry tại chỗ của consumer ordered) tạo delivery mới, nên mỗi lần thử được đếm một lần.
- Message bị bỏ qua vì idempotency được đếm với `nats.outcome='skipped'`.
- Dead letter được đếm ở phía outbound với `nats.dead_letter=true`, độc lập với outcome `dlq` ở phía inbound.
- Mỗi message của `publishAtomic()` được đếm riêng; duration tính đến khi batch settle.

## 5. Hành vi

### 5.1 Vòng đời instrument

API metrics không có proxy provider: meter lấy trước khi đăng ký SDK là noop vĩnh viễn. Vì vậy mỗi lần ghi, plugin lấy `metrics.getMeterProvider()` và so với provider đã cache (một ô). Khi khác, plugin gọi `provider.getMeter(meterName)` và tạo lại 4 instrument. Plugin tạo trước `sdk.start()` vẫn ghi đúng sau đó, và test thay provider được.

### 5.2 Inbound

1. Ghi `start = performance.now()`, gọi `await next()`.
2. Trong `finally`:
   - ghi histogram `(performance.now() - start) / 1000`, kèm `error.type` nếu `next()` ném lỗi;
   - đếm từng message trong `delivery.messages` theo outcome của nó.
3. Ném lại lỗi của `next()`, nếu có.

### 5.3 Outbound

Ghi `start`, `await next()`, rồi trong `finally` ghi counter và histogram (kèm `error.type` nếu `next()` ném lỗi), sau đó ném lại lỗi. Nếu một plugin đứng sau không bao giờ cho message đi tiếp, `next()` không settle và plugin không ghi gì cho message đó (giống span của `tracing()`).

### 5.4 Lỗi

- Plugin không đổi kết quả của pipeline: không nuốt lỗi và không thêm lỗi.
- Nếu hàm `subjects` ném lỗi, plugin bỏ `messaging.destination.name` cho lần ghi đó và báo qua `diag.warn` một lần cho mỗi instance plugin.
- Nếu việc ghi metric tự ném lỗi (ví dụ `subjects` trả về giá trị SDK không băm được, hoặc meter provider lỗi), plugin bỏ lỗi đó và báo qua `diag.warn` một lần cho mỗi instance plugin; kết quả của `next()` giữ nguyên.

## 6. Thành phần

| File | Nội dung |
|---|---|
| `src/otel/metrics.ts` | `MetricsOptions`, `metrics()` |
| `src/otel/index.ts` | Export `metrics`, `MetricsOptions` |
| `test/unit/otel-metrics.spec.ts` | Unit test |
| `test/integration/otel-metrics.int.spec.ts` | Integration test |
| `package.json` | devDependency `@opentelemetry/sdk-metrics` `^2.x` (cùng major với các gói SDK đang dùng) |

## 7. Test

### Unit (`MeterProvider` của `@opentelemetry/sdk-metrics` với một reader `collect()` thủ công)

- Mỗi `action` của outcome cho đúng `nats.outcome`, kèm `error.type` khi outcome có lỗi.
- Message không có outcome cho `unsettled`.
- Batch: một lần ghi histogram, mỗi message một lần đếm.
- `next()` ném lỗi: histogram có `error.type`, các message vẫn được đếm, lỗi được ném lại nguyên vẹn.
- Thuộc tính chung theo từng `kind` của handler (group, stream).
- Outbound publish, request, jetstream, dead letter, atomic, và lỗi gửi.
- `subjects`: `false`, `true`, hàm, hàm trả `undefined`, hàm ném lỗi (một `diag.warn`).
- Plugin tạo trước khi đăng ký provider vẫn ghi sau khi đăng ký; đổi provider giữa chừng; không có provider thì không ném lỗi.

### Integration (NATS thật)

- `@Consume` thất bại có retry rồi DLQ: số đếm `retry` và `dlq`, cùng một lần sent có `nats.dead_letter=true`.
- Request tới `@Subscribe`: outcome `replied`, sent `request`.
- `JetStreamClient.publish()`: sent `jetstream`.

### Smoke

`esm-nest12` và `cjs-nest11` import `metrics` từ `@loipv/nestjs-nats/otel` và kiểm tra nó là function. `no-peers` không đổi: nó đã kiểm tra `/otel` báo lỗi khi thiếu `@opentelemetry/api`.

## 8. Tài liệu

- `docs/metrics.md`: cài đặt (`NodeSDK` với `metricReader`), option, bảng metric và thuộc tính, ngữ nghĩa đếm, cảnh báo về số series khi bật `subjects`.
- Link từ README và từ `docs/plugins.md` / `docs/tracing.md`.
- CHANGELOG 0.1.0.
