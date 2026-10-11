# Dead letter đi qua outbound plugins — Design Spec

- **Ngày:** 2026-10-11
- **Trạng thái:** Đã duyệt thiết kế qua hội thoại, chờ review spec
- **Loại:** Architectural (đổi hợp đồng outbound plugin)
- **Sub-project:** SP-D của đợt xử lý limitation (README, "DLQ publishing bypasses outbound plugins")

## 1. Bối cảnh

`sendToDlq` (`src/settlement/dlq.ts`) publish dead letter thẳng bằng `entry.nc.publish` (mode `core`) hoặc `entry.js().publish` (mode `jetstream`), không qua `plugins[].outbound`. Vì vậy:

- tracing không có span cho lần publish dead letter;
- plugin gắn header (tenant, auth, chữ ký) không chạy cho dead letter;
- metrics plugin không đếm được dead letter ở phía publish.

## 2. Mục tiêu

1. Mọi dead letter đi qua outbound plugins của connection, giống mọi lần publish khác.
2. Plugin nhận ra dead letter qua một field đánh dấu, để tự bỏ qua nếu muốn.
3. Ngữ nghĩa settlement không đổi khi plugin cho dead letter đi qua; có quy tắc rõ ràng khi plugin bỏ dead letter, lỗi hoặc chậm.

### Ngoài phạm vi

- Opt-in hay opt-out theo cấu hình. Dead letter luôn đi qua plugin, không có option.
- Đổi định dạng header DLQ hoặc subject mặc định.

## 3. Hợp đồng plugin

`OutgoingMessage` (`src/pipeline/types.ts`) có thêm field:

```ts
/**
 * Set on a dead letter: the message a handler failed on, published to its DLQ subject
 * (`subject`). Its `data` is the original body (bytes) and its `headers` the dead letter's
 * (`Nats-DLQ-*` and its `Nats-Msg-Id`).
 */
deadLetter?: {
  /** Subject the failed message arrived on. */
  subject: string;
  /** The handler that failed. */
  handler: HandlerInfo;
  /** The last error. */
  error: unknown;
};
```

Các field khác của dead letter:

| Field | Giá trị |
|---|---|
| `kind` | `'publish'` với mode `core`, `'jetstream'` với mode `jetstream` |
| `connection` | Tên connection |
| `subject` | Subject DLQ đã resolve |
| `data` | Byte body gốc (`Uint8Array`), gửi nguyên. Plugin thay bằng giá trị khác thì `codec` sẽ encode |
| `headers` | Header DLQ như hiện nay (`buildDlqHeaders`) |
| `codec` | Codec của connection (`entry.options.codec ?? jsonCodec()`) |
| `cloudEvent`, `atomic` | Không đặt |

Plugin được sửa `headers`, `data` và `subject` như với mọi message khác. Tài liệu sẽ cảnh báo: đổi hoặc xóa `Nats-Msg-Id` làm stream DLQ mất khả năng khử trùng dead letter.

## 4. Kết quả theo hành vi của plugin

Dead letter đi qua `sendOutbound` (`src/pipeline/outbound.ts`):

- mode `jetstream`: qua `publishOutgoing` (`src/jetstream/publish.ts`), timeout là `jetStreamTimeout(entry)`;
- mode `core`: terminal step là `entry.nc.publish(subject, encodeOutgoing(m), { headers })`, timeout là `DEFAULT_REQUEST_TIMEOUT`, giống `NatsClient.publish`.

| Plugin | Kết quả DLQ | Settlement |
|---|---|---|
| Gọi `next()`, publish thành công | Đã dead-letter | Như hiện nay: JetStream thì `term`, outcome `'dlq'` |
| Gọi `next()`, publish lỗi (ví dụ không stream nào bắt subject) | DLQ lỗi | Như hiện nay: JetStream thì `nak` kèm backoff; ordered thì thử lại không giới hạn; core thì log và bỏ |
| Ném lỗi trước khi gọi `next()` | DLQ lỗi, với lỗi của plugin | Như dòng trên |
| Không gọi `next()` trong thời gian timeout | DLQ lỗi (`NatsModuleError` "the outbound plugins did not pass the message on within <ms> ms") | Như dòng trên |
| Trả về mà không gọi `next()` | Coi như **đã dead-letter** | Như thành công, kèm warn (§5) |
| Ném lỗi sau khi đã gọi `next()` | Theo kết quả publish | Lỗi của plugin được log như mọi lần publish (`logLateOutboundError`) |

Hàm nội bộ `sendToDlq` trả về `'sent' | 'dropped'`, hoặc ném lỗi như hiện nay. Settlement dùng giá trị trả về để log.

## 5. Log

- Dead letter bị plugin bỏ: warn
  `<label> failed, its dead letter to <subject> was dropped by an outbound plugin after <attempts>: <error>`.
  Dòng này thay cho dòng error `... failed, sent to DLQ <subject> after ...` thường gặp.
  Với CloudEvent unmatched: `<label>: <message>; its dead letter to <subject> was dropped by an outbound plugin (unmatched: 'dlq')`.
- Các dòng log khác không đổi.

## 6. Thành phần

- `src/pipeline/types.ts`: field `deadLetter` (import kiểu `HandlerInfo`).
- `src/settlement/dlq.ts`:
  - `sendToDlq(entry, subject, mode, msg, err, deliveries, consumer?, handler)` dựng `OutgoingMessage` và đi qua outbound;
  - trả `Promise<'sent' | 'dropped'>`;
  - `publishToDlq` được cập nhật tương ứng;
  - bỏ câu "Bypasses the outbound plugins" trong doc comment.
- `src/settlement/settlement.ts`: `deadLetter()` truyền `d.handler` và nhận `'dropped'`; `logDeadLettered` có biến thể "dropped" (§5); outcome vẫn là `{ action: 'dlq', subject, error }`.
- Harness `/testing`: không đổi code. Fake connection ghi nhận publish sau khi qua plugin, nên `result.dlq` chứa dead letter đã được plugin xử lý. Dead letter bị bỏ không có mặt trong `dlq`, dù outcome là `'dlq'`.
- `src/otel/tracing.ts`: không bắt buộc đổi. Span `publish <dlq subject>` là con của span consumer, vì publish chạy trong ngữ cảnh của handler. Có thể thêm attribute `messaging.nats.dead_letter: true` khi `m.deadLetter` được đặt; plan sẽ quyết.

## 7. Test

### Unit

- `dlq.spec` hoặc `settlement.spec`, với một plugin giả:
  - plugin thấy `deadLetter` đúng `{ subject, handler, error }`, `kind`, `subject` và `data` (byte gốc);
  - plugin thêm header thì header đó có mặt trong publish;
  - plugin bỏ dead letter: JetStream thì `term`, outcome `'dlq'`, có warn; core thì outcome `'dlq'`, có warn;
  - plugin ném lỗi trước `next()`: JetStream thì `nak`, outcome `'retry'`;
  - plugin không gọi `next()` trong timeout: thành lỗi DLQ;
  - không có plugin: hành vi giữ nguyên, các test hiện có vẫn pass.
- Ordered consumer: plugin ném lỗi thì dead letter được thử lại tại chỗ.
- Tracing: một dead letter tạo span `publish <dlq subject>` và `traceparent` mới.
- Harness: `result.dlq` chứa header do plugin thêm.

### Integration

- Một `@Consume` với `dlq: true`, plugin outbound gắn header `x-dead-letter-by: plugin` khi `m.deadLetter` được đặt. Handler ném `NonRetryableError`. Đọc stream DLQ và kiểm tra dead letter có header đó cùng `Nats-DLQ-*`.

## 8. Tài liệu

- `docs/plugins.md`: dead letter đi qua outbound plugins; field `deadLetter`; plugin nên gọi `next()` (bỏ dead letter thì message gốc bị coi là đã dead-letter); cảnh báo về `Nats-Msg-Id`.
- `docs/retry-and-dlq.md`: thay câu "DLQ publishes do not go through outbound plugins" bằng mô tả mới, kèm hành vi khi plugin lỗi, timeout hoặc bỏ dead letter.
- README: bỏ mục "DLQ publishing bypasses outbound plugins" trong Known limitations.
- `docs/tracing.md`: dead letter có span publish của riêng nó.
- `CHANGELOG.md`: gộp vào mục 0.1.0 (gói chưa publish; người dùng đã chọn cách này).
