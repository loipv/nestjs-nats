# `@loipv/nestjs-nats` — Push consumer, ordered consumer, atomic batch publish

- **Ngày:** 2026-10-10
- **Trạng thái:** Đã duyệt thiết kế qua hội thoại, chờ review spec
- **Loại:** Architectural (mở rộng thư viện public, thêm API)
- **Spec gốc:** [2026-10-09-nestjs-nats-design.md](./2026-10-09-nestjs-nats-design.md) — mọi quy tắc của spec gốc (Global Constraints, pipeline, settlement, shutdown có giới hạn, không lộ secret trong log/error) vẫn áp dụng; spec này chỉ mô tả phần thêm/khác.

## 1. Mục tiêu

Đưa ba mục ra khỏi §18 "Ngoài phạm vi v1" của spec gốc, phát hành cùng 0.1.0 (chưa publish):

1. **Push consumer (durable)** — `@ConsumePush`, chia tải qua `deliver_group`, semantics như `@Consume`.
2. **Ordered consumer (pull, ephemeral)** — `@ConsumeOrdered`, mỗi instance nhận mọi message theo đúng thứ tự stream, retry tại chỗ rồi DLQ.
3. **Atomic batch publish** — `JetStreamClient.publishAtomic([...])`, all-or-nothing (NATS ≥ 2.12, stream `allow_atomic: true`).

### Quyết định đã chốt (hội thoại 2026-10-10)
- Ordered: handler lỗi → **retry tại chỗ** (tuần tự, giữ thứ tự) theo policy `retry`, hết retry → DLQ (nếu bật) hoặc log rồi đi tiếp.
- Push: **durable push + `deliverGroup`**; ordered chỉ có bản pull.
- Atomic: **API dạng mảng** `publishAtomic(messages)`; không có builder.
- Khai báo: **decorator riêng** (`@ConsumePush`, `@ConsumeOrdered`), option type riêng; bên trong dùng chung pipeline/settlement/adapter JetStream.

### Tiêu chí thành công
- Mỗi tính năng có integration test end-to-end với `nats:2.12-alpine` (Testcontainers) như spec gốc.
- Không đổi hành vi của `@Consume`, `@Subscribe`, `@ServiceEndpoint`, `publish`, `publishMany`, `batcher()` (toàn bộ test hiện có vẫn xanh).
- `publint`, `attw`, `check:pack`, `docs:check` vẫn xanh; không thêm dependency.

## 2. API public

### 2.1 `@ConsumePush(options: ConsumePushOptions)`

```ts
interface ConsumePushOptions extends ConsumeOptions {
  /** Queue group: các instance cùng group chia nhau message. Map sang `deliver_group`. */
  deliverGroup?: string;
}
```

- Mọi option của `ConsumeOptions` (`connection`, `stream`, `durable`, `config`, `ack`, `codec`, `concurrency`, `retry`, `dlq`, `idempotency`, `batch`) giữ nguyên nghĩa.
- Khi lib **tạo** consumer (sync mode cho phép, consumer chưa có), defaults thêm vào config (user khai báo trong `config` thì thắng):
  - `deliver_subject`: `_nats.push.<stream>.<durable>`.
  - `deliver_group`: `deliverGroup` nếu có.
  - `flow_control: true`, `idle_heartbeat: 5 s`.
  - `max_ack_pending`: `batch ? max(batch.maxSize, concurrency) : concurrency`.
  - Các default hiện có của consumer (ack_policy explicit, …) áp dụng như `@Consume`.
- `deliverGroup` và `config.deliver_group` cùng khai báo mà khác nhau → `NatsConfigError`.

### 2.2 `@ConsumeOrdered(options: ConsumeOrderedOptions)`

```ts
type OrderedDeliverPolicy =
  | 'all' | 'last' | 'new' | 'last_per_subject'
  | { startSeq: number }       // by_start_sequence
  | { startTime: Date | string }; // by_start_time (ISO 8601)

interface ConsumeOrderedOptions {
  connection?: string;
  stream: string;
  filterSubjects?: string | string[];
  deliverPolicy?: OrderedDeliverPolicy; // mặc định 'all' (default của nats.js)
  headersOnly?: boolean;
  codec?: Codec;
  retry?: RetryOptions | false;
  dlq?: DlqOptions | boolean;
  idempotency?: boolean | IdempotencyOptions;
}
```

- Không có `durable`, `config`, `ack`, `concurrency`, `batch`: luôn tuần tự, consumer do nats.js quản lý (ephemeral, tự tạo lại khi lệch sequence).
- Mỗi instance của app nhận **mọi** message (fan-out), không chia tải.
- Context: `NatsOrderedContext` (export public), gồm `subject`, `headers`, `raw: JsMsg`, `connection`, `attempt`, `streamSequence`, `deliverySequence`, `pending`. `ack()/nak()/term()/working()` ném `NatsConfigError('ordered consumer messages cannot be settled')`. `@Ctx()` trả về context này.

### 2.3 `JetStreamClient.publishAtomic`

```ts
interface AtomicMessage {
  subject: string;
  data?: unknown;
  headers?: NatsHeadersInit;
  codec?: Codec;
  cloudEvent?: CloudEventInit;
}

interface AtomicPublishOptions {
  /** Timeout chờ ack commit (ms). Mặc định = timeout của JetStream client; không bao giờ 0. */
  timeout?: number;
  /** Expectations áp cho message đầu tiên (không có lastMsgID). */
  expect?: Partial<Omit<StreamExpectations, 'lastMsgID'>>;
}

interface AtomicPublishAck {
  stream: string;
  seq: number;          // sequence của message cuối
  count: number;        // số message đã lưu
  batch?: string;       // Nats-Batch-Id; undefined khi chỉ có 1 message
  duplicate: false;
  domain?: string;
}

publishAtomic(messages: AtomicMessage[], opts?: AtomicPublishOptions): Promise<AtomicPublishAck>;
```

- Kiểm tra trước khi gửi (ném `NatsConfigError`, không gửi gì): mảng rỗng, quá 1000 message, header `Nats-Msg-Id` (không phân biệt hoa thường) trên bất kỳ message nào.
- CloudEvent: `ce.id` **không** được dùng làm `Nats-Msg-Id` (khác `publish`).
- 1 message → `js.publish` thường (không cần `allow_atomic`); kết quả cùng dạng `AtomicPublishAck` với `batch: undefined`, `count: 1`.
- Lỗi server/transport → `NatsAtomicPublishError` (lớp mới, extends `NatsModuleError`, export public), `cause` là Error đã làm sạch (cùng quy tắc với `NatsConnectionError`), message nêu stream/subject đầu tiên và lý do. Không message nào của batch được lưu khi lỗi (đảm bảo của server).
- `NatsStreamManager` truyền `allow_atomic` trong `StreamConfig` như mọi field khác (không cần code riêng; thêm vào docs + test sync).

### 2.4 Plugin outbound

`OutgoingMessage` thêm field tuỳ chọn:

```ts
atomic?: { index: number; count: number };
```

Có mặt trên mọi message đi qua `publishAtomic` (kể cả trường hợp 1 message), `kind` vẫn là `'jetstream'`. OTel plugin thêm attribute `messaging.batch.message_count = count` cho span publish.

## 3. Runtime

### 3.1 Push — `JetStreamConsumerAdapter` + intake strategy

`JetStreamConsumerAdapter` tách phần nhận message thành interface nội bộ:

```ts
interface Intake {
  open(): Promise<ConsumerMessages>; // tạo/bind consumer, trả iterator
  readonly eager: boolean;            // true: đọc ngay, không chờ slot
}
```

- `PullIntake` (hiện tại): `consumers.get(stream, durable).consume({ max_messages })`, `eager: false` (vòng lặp `await dispatcher.push()`).
- `PushIntake`: `consumers.getPushConsumer(stream, durable).consume()`, `eager: true`: mỗi message được đưa vào `unsettled` (heartbeat) và xếp hàng dispatcher **không chờ slot** (`Dispatcher` thêm `enqueue(msg, attempt): void` dùng chung hàng đợi/limiter với `push`). Hàng đợi bị chặn bởi `max_ack_pending` phía server ⇒ mọi message đã nhận đều được heartbeat (push không có lỗ hổng heartbeat của pull).
- Khởi động push (sau `ensureConsumer` như `@Consume`), đọc `consumer.info()` và ném `NatsConfigError` khi:
  - consumer không có `deliver_subject` (là pull consumer);
  - `deliverGroup` khai báo khác `deliver_group` trên server (kể cả một bên có một bên không);
  - `max_ack_pending` ≤ 0 (không giới hạn).
- `checkMaxDeliver`, `checkBatchWait`, `watchStatus` (thêm `flow_control` không log; `heartbeats_missed` warn như hiện tại), settlement JetStream, retry `nak(delay)`, DLQ, idempotency (scope `consume:<stream>/<durable>|`), batch, `stop/idle/forceStop` dùng chung không đổi.
- Label log: `Class.method on "<stream>/<durable>" (push)`.

### 3.2 Ordered — `OrderedConsumerAdapter` (mới)

- `start()`: `js.consumers.get(stream, { filter_subjects, deliver_policy, opt_start_seq, opt_start_time, headers_only })` → `consume()` (prefetch mặc định của nats.js). Stream không tồn tại → `NatsConfigError`.
- Vòng lặp tuần tự: mỗi message chạy `createHandlerPipeline` (plugin outer/inner, idempotency, CloudEvent router, invoker) với settlement kiểu mới **`ordered`**:

| Kết quả handler | Hành động | Outcome |
|---|---|---|
| Thành công | Không làm gì | `none` |
| Lỗi retry được, còn lượt | Chờ `backoffDelay(policy, attempt)` tại chỗ (huỷ được), chạy lại với `attempt + 1`; message sau chờ | `retry` (`delayMs`) cho mỗi lần |
| Hết retry / `NonRetryableError` / `NatsDecodeError` / `HttpException` 4xx, DLQ bật | Publish DLQ (await). DLQ publish lỗi → retry publish với backoff của policy (hoặc `DEFAULT_RETRY`), **không giới hạn số lần**, log error mỗi lần, dừng khi stop | `dlq` |
| Như trên, DLQ tắt | Log error, đi tiếp | `drop` |

- `retry: false` → lỗi đầu tiên đi thẳng nhánh DLQ/drop.
- Idempotency store lỗi (`IdempotencyStoreError`): retry tại chỗ với backoff, không giới hạn, như quy tắc hiện có.
- Idempotency key scope: `ordered:<Class>.<method>|`.
- DLQ: headers như spec gốc §7; `Nats-DLQ-Consumer = ordered:<Class>.<method>`, `Nats-DLQ-Seq` = stream sequence, `Nats-DLQ-Deliveries` = số lần thử; `Nats-Msg-Id` của dead letter = `dlq:<stream>:ordered:<Class>.<method>:<streamSeq>` (các replica cùng dead-letter một message được DLQ stream dedupe trong `duplicate_window`).
- `status()`: `ordered_consumer_recreated` → log debug; `heartbeats_missed`, `stream_not_found` → warn (lũy thừa 2 như hiện tại).
- Shutdown:
  - `stop()`: đóng iterator, huỷ lần chờ retry đang diễn ra (log warn `retry abandoned at shutdown` kèm stream sequence).
  - `idle()`: chờ handler đang chạy (giới hạn bởi drainTimeout của lifecycle như các adapter khác).
  - `forceStop()`: `{ nakked: 0, cancelledRetries: <0|1> }`.
  - Message bị bỏ dở không được gửi lại trừ khi lần khởi động sau replay theo `deliverPolicy` — ghi vào docs.

### 3.3 Atomic — staging qua outbound pipeline

1. Validate (§2.3).
2. Với mỗi message i (theo thứ tự mảng): `createOutgoingMessage(entry, 'jetstream', …)` + `atomic: { index: i, count: n }`, chạy chuỗi outbound plugin. Bước cuối (terminal) **không gửi**: encode (`encodeOutgoing`), ghi vào `staged[i]`, rồi `await` một deferred commit dùng chung. Các chuỗi khởi chạy tuần tự theo index, nhưng ghi theo index nên plugin async không làm đảo thứ tự.
3. Khi đủ n message đã stage: `startBatch(m0)` → `add(m1..m(n-2))` (không ack) → `commit(m(n-1), { timeout })`; kiểm tra `ack.count === n`. Resolve/reject deferred ⇒ `next()` của mọi plugin nhận đúng kết quả commit (span OTel ghi lỗi nếu có).
4. Một chuỗi plugin ném lỗi trước khi stage ⇒ huỷ: không gửi gì, deferred reject với `NatsAtomicPublishError('aborted: …', { cause })`, `publishAtomic` reject với lỗi gốc của plugin.
5. Timeout: `opts.timeout ?? <timeout của JetStream client>`; không bao giờ truyền `0` (commit của nats.js mặc định 0).

## 4. Discovery & validation

- `HandlerDefinition` thêm `{ kind: 'consume-push'; options: ConsumePushOptions }` và `{ kind: 'consume-ordered'; options: ConsumeOrderedOptions }`. Metadata keys mới trong `decorators/constants.ts`.
- Validator (`NatsConfigError` khi bootstrap):
  - một `connection + stream + durable` dùng bởi hai handler bất kỳ trong `@Consume`/`@ConsumePush`;
  - `@ConsumePush` với `ack: 'manual'` + `batch` (như `@Consume`);
  - `deliverPolicy.startSeq` không phải số nguyên ≥ 1, `startTime` không parse được;
  - option không thuộc `ConsumeOrderedOptions` trên `@ConsumeOrdered` (`durable`, `concurrency`, `batch`, `ack`, `config`) — chặn cho JS thuần;
  - quy tắc retry/dlq/idempotency hiện có áp cho cả hai loại mới.
- Một method chỉ được mang một handler decorator (quy tắc hiện có, mở rộng cho hai decorator mới).

## 5. Testing (`/testing`)

- `NatsTestHarness.dispatch({ handler })`:
  - push: như `@Consume` (`settlement`: `ack | nak | term | dlq | skipped`, `nakDelay`).
  - ordered: như core (`none | retry | dlq | drop | skipped`, `retryDelay`; harness không chạy lại retry, `attempt` giả lập lần sau). Option mới `streamSequence` (mặc định 1) cho `ctx.streamSequence` và DLQ headers.
  - `deliveryCount` bị bỏ qua cho ordered (như core).
- Mock `JetStreamClient.publishAtomic`: chạy validate + outbound plugin như thật, ghi từng message vào danh sách published kèm `atomic: { batch, index, count }`, trả ack giả (`seq` tăng dần, `batch` = id giả khi n > 1).

## 6. Kế hoạch test của lib

- **Unit:** settlement ordered (bảng §3.2, huỷ retry khi stop, DLQ publish retry), Dispatcher `enqueue`, push intake (eager + heartbeat phủ hàng đợi), validator (§4), staging atomic (thứ tự với plugin async, huỷ khi plugin ném, `next()` thấy lỗi commit, timeout ≠ 0, 1 message → publish thường, validate n/msgId), harness + mock.
- **Integration (nats:2.12):**
  - Push: 2 app cùng `deliverGroup` chia tải (mỗi message xử lý đúng 1 lần); `ack_wait` 2 s + handler 5 s + `concurrency: 2` không redeliver; retry → DLQ; shutdown nak in-flight; consumer pull khai báo cho `@ConsumePush` → `NatsConfigError`.
  - Ordered: thứ tự giữ khi message giữa lỗi 2 lần rồi thành công; 2 replica dead-letter cùng message → DLQ có 1 bản; xoá consumer phía server → tự tạo lại và tiếp tục; `deliverPolicy: { startSeq }`.
  - Atomic: 3 message commit (seq liên tiếp, `count: 3`); stream thiếu `allow_atomic` → `NatsAtomicPublishError`, stream không có message mới; plugin ném → không lưu gì; 1001 message → `NatsConfigError`; trace OTel produce → consume cùng trace ID cho message trong batch.

## 7. Docs & release

- Trang mới `docs/consumers.md`: so sánh pull / push / ordered (khi nào dùng, fan-out vs chia tải, ack, heartbeat, shutdown, giới hạn).
- `docs/publishing.md`: mục "Atomic batches" (khác `batcher()`/`publishMany`: không atomic), yêu cầu server ≥ 2.12 + `allow_atomic`, giới hạn 1000 message / 10 s idle / 50 batch đang mở mỗi stream, không dedupe theo message.
- Cập nhật `README.md` (feature list, Known limitations: ordered bỏ dở message khi shutdown; fan-out), `docs/testing.md`, `docs/handlers.md` (mục cho hai decorator mới + bảng `@Ctx()`), `docs/streams.md` (`allow_atomic`), `docs/tracing.md` (attribute), CHANGELOG mục 0.1.0, spec gốc §18 (xoá 3 mục, trỏ sang spec này).
- `examples/orders`: thêm một `@ConsumeOrdered` projection và một `publishAtomic` (đơn + reserve kho) để `example:check` phủ API mới.

## 8. Ngoài phạm vi

Ordered push consumer; fast-ingest (`startFastIngest`, `allow_batched`); `batch`/`concurrency` cho ordered; atomic builder (`add`/`commit`); dedupe theo message trong atomic batch; health check theo từng consumer; tự restart vòng consume đã chết (giới hạn hiện có áp cho cả loại mới).

## 9. Review Focus (gợi ý cho plan)

1. Push với `concurrency` > `max_ack_pending` của consumer có sẵn: slot thừa không bao giờ đầy — chỉ cần chạy đúng, không treo.
2. Ordered: handler ném liên tục với `retry` dài + shutdown — stop phải huỷ chờ ngay, không đợi hết backoff.
3. Ordered: DLQ stream không tồn tại — retry publish vô hạn phải có log và dừng khi shutdown.
4. Atomic: plugin async chậm ở message đầu, nhanh ở message sau — thứ tự trên stream vẫn theo mảng.
5. Atomic: connection đang drain/đóng giữa chừng — `publishAtomic` reject (không treo), không batch nào bị commit một phần.
