# Restart handler và health theo handler — Design Spec

- **Ngày:** 2026-10-11
- **Trạng thái:** Đã duyệt thiết kế qua hội thoại, chờ review spec
- **Loại:** Architectural (đổi vòng đời adapter, thêm option module, mở rộng health indicator)
- **Sub-project:** SP-C của đợt xử lý limitation (README, "Ended loops are not restarted")

## 1. Bối cảnh

Một vòng nhận message (core subscription, JetStream pull/push consumer, ordered consumer, NATS service) có thể kết thúc mà lib không yêu cầu, ví dụ:

- mất quyền trên subject hoặc inbox (nats.js dừng subscription với lỗi permissions);
- server trả 400 hoặc một số mã 409 cho pull request;
- ordered consumer tạo lại thất bại 30 lần khi chưa nhận được message nào;
- một subscription của NATS service lỗi, làm service tự dừng.

Hiện nay lib chỉ log `... ended unexpectedly; no more messages will be received`. Handler chết cho tới khi process restart, còn health indicator vẫn báo `up`.

## 2. Mục tiêu

1. Tự mở lại vòng nhận message bị chết, với backoff lũy thừa có trần. Mặc định không bao giờ bỏ cuộc; có tùy chọn `maxAttempts`.
2. Health indicator (opt-in `handlers: true`) báo `down` khi có handler đã bỏ cuộc (`failed`). Handler đang restart vẫn `up` nhưng được liệt kê.
3. Cấu hình theo connection: `NatsModuleOptions.restart`.

### Ngoài phạm vi

- Override `restart` theo từng handler (YAGNI, có thể thêm sau mà không phá API).
- Restart khi connection đã đóng hẳn (nats.js không mở lại một connection đã đóng).
- Restart khi bootstrap thất bại: lần mở đầu tiên lỗi vẫn làm bootstrap fail như hiện nay.
- Readiness/liveness tách riêng (`handlers: 'failed' | 'stopped'`).

## 3. API công khai

### 3.1 Option module

```ts
export interface RestartOptions {
  /** Độ trễ trước lần restart đầu, ms (mặc định 1000). */
  initialDelay?: number;
  /** Hệ số nhân mỗi lần (mặc định 2). */
  factor?: number;
  /** Độ trễ tối đa, ms (mặc định 30_000). Cũng là thời gian chạy ổn định để reset bộ đếm. */
  maxDelay?: number;
  /** Jitter tương đối, trong [0, 1] (mặc định 0.2). */
  jitter?: number;
  /** Số lần restart liên tiếp tối đa trước khi bỏ cuộc (mặc định Infinity). */
  maxAttempts?: number;
}

interface NatsModuleOptions {
  // ...
  /** Restart vòng nhận message chết ngoài ý muốn; `false` để chỉ log (hành vi cũ). */
  restart?: RestartOptions | false;
}
```

`RestartOptions` được export từ `src/index.ts`.

Validate lúc bootstrap. Giá trị sai ném `NatsConfigError` có tên connection:

- `initialDelay`, `maxDelay` là số hữu hạn, nằm trong `[0, 2^31-1]`, và `initialDelay ≤ maxDelay`;
- `factor` là số hữu hạn ≥ 1;
- `jitter` nằm trong `[0, 1]`;
- `maxAttempts` là số nguyên ≥ 1 hoặc `Infinity`.

### 3.2 Health indicator

`NatsHealthCheckOptions.handlers?: boolean` (mặc định `false`). Khi bật:

- Có ít nhất một handler `failed` của connection: `down` với
  - `reason: 'handler failed: <label>'`; nếu nhiều hơn một thì thêm `', +<n> more'`;
  - `handlers: { failed: string[], restarting: string[] }`.
- Ngược lại: `up` như cũ, thêm `handlers: { restarting: string[] }` khi danh sách đó không rỗng.
- Thứ tự kiểm tra: trạng thái connection, rtt, jetstream (nếu bật), rồi handlers. Connection đã down thì không cần nhìn handler.

Khi không bật, kết quả giữ nguyên từng byte như hiện tại. `<label>` là label sẵn có của adapter, ví dụ `Orders.onCreated on "ORDERS/orders-worker"`, `Svc.get on service "svc"` và các dạng tương tự. Label không chứa secret.

## 4. Hành vi

### 4.1 Trạng thái

Mỗi adapter có một trạng thái: `running` → `restarting` → (`running` | `failed`).

- `running`: vòng nhận message đang chạy.
- `restarting`: đã có lịch restart, hoặc đang mở lại.
- `failed`: đã bỏ cuộc vì một trong ba lý do: đạt `maxAttempts`, `restart: false`, hoặc connection đã đóng. Từ trạng thái này không có restart nào nữa.

### 4.2 Khi nào restart

Vòng nhận message kết thúc trong khi adapter không được yêu cầu dừng (`stop()` chưa chạy):

- iterator ném lỗi;
- iterator tự kết thúc;
- với service: `service.stopped` resolve với lỗi, hoặc một endpoint loop tự kết thúc. Trường hợp sau thì lib dừng service trước rồi restart cả service.

Một lần mở lại thất bại (ném lỗi, kể cả `NatsConfigError` như consumer không tồn tại) được tính là một lần thử thất bại, và lib hẹn lần tiếp theo. Lý do là consumer hoặc quyền có thể được tạo lại hay cấp lại sau đó.

### 4.3 Backoff và bộ đếm

- Lần thử thứ `n` chờ `backoffDelay(policy, n)`, dùng lại hàm của `settlement/retry.ts`.
- Mở lại thành công thì trạng thái thành `running` và lib đặt một timer `maxDelay` (unref).
  - Nếu vòng chạy hết thời gian đó thì bộ đếm về 0.
  - Nếu vòng chết trước đó thì bộ đếm tiếp tục tăng.
  - Lý do: một core subscription mất quyền vẫn "mở thành công" rồi chết vài ms sau. Nếu reset ngay khi mở được thì `maxAttempts` không bao giờ đạt.
- Khi bộ đếm sắp vượt `maxAttempts`, lib chuyển sang `failed`.
- `ended()` gọi khi trạng thái không phải `running` thì bị bỏ qua. Ví dụ: nhiều endpoint của một service cùng kết thúc, hoặc iterator kết thúc trong lúc đang có lịch restart.

### 4.4 Connection đã đóng

Nếu `nc.isClosed()` tại lúc vòng chết hoặc tại lúc tới lượt mở lại, adapter chuyển sang `failed` ngay và không hẹn lịch. Khi connection đang reconnect thì không có gì đặc biệt: subscription core sống qua reconnect, và consumer của nats.js tự xử lý.

### 4.5 `restart: false`

Lib log lỗi như hiện tại, adapter chuyển sang `failed`, và vòng không được mở lại. Với `handlers: true`, health báo `down`.

### 4.6 Log (qua logger của connection)

| Sự kiện | Mức | Nội dung |
|---|---|---|
| Vòng chết | error | Giữ thông báo hiện có (`Consumer of <label> failed`, `... ended unexpectedly`, `NATS service ... stopped`), nhưng bỏ đuôi "no more messages will be received" khi sẽ restart |
| Hẹn restart | warn | `Restarting <label> in <ms> ms (attempt <n>[/<max>])` |
| Mở lại lỗi | warn | `Restart of <label> failed (attempt <n>): <error>` |
| Mở lại thành công | log | `Restarted <label> (attempt <n>)` |
| Bỏ cuộc | error | `Gave up restarting <label> after <n> attempts; no more messages will be received`, hoặc `... : the connection is closed` |

Các cảnh báo cấu hình lúc mở (ví dụ `max_deliver ≤ maxRetries`, `batch.maxWait >= ack_wait`) chỉ được log ở lần mở đầu tiên.

### 4.7 Shutdown

- `stop()` gọi `supervisor.stop()`: hủy timer restart và timer reset, đồng thời chặn mọi lần mở lại về sau.
- Nếu một lần mở lại đang dở khi `stop()` chạy, nguồn message vừa mở xong được đóng ngay, theo mẫu `if (this.stopping)` mà `start()` đang dùng.
- `idle()` chờ lần mở lại đang dở (nếu có), rồi chờ vòng hiện tại cùng dispatcher hoặc limiter như trước.
- `forceStop()` không đổi.
- Khi shutdown, lib xóa trạng thái handler của connection khỏi registry.

## 5. Thành phần

### 5.1 `src/settlement/restart.ts`

- `RestartOptions` (public) và `RestartPolicy` (đã resolve: mọi field bắt buộc).
- `DEFAULT_RESTART = { initialDelay: 1000, factor: 2, maxDelay: 30_000, jitter: 0.2, maxAttempts: Infinity }`.
- `resolveRestartPolicy(opts: RestartOptions | false | undefined, connection: string): RestartPolicy | false`: validate theo §3.1 và trộn với default.

### 5.2 `src/adapters/restart-supervisor.ts`

```ts
type HandlerState = 'running' | 'restarting' | 'failed';

class RestartSupervisor {
  constructor(deps: {
    policy: RestartPolicy | false;
    label: string;
    logger: LoggerService;
    isConnectionClosed: () => boolean;
    /** Mở lại nguồn message và chạy vòng mới; ném lỗi nếu không mở được. */
    reopen: () => Promise<void>;
    onState: (state: HandlerState) => void;
    random?: () => number; // test
  });
  readonly state: HandlerState;
  /** Lần mở đầu tiên (trong `start()`) đã thành công: trạng thái thành `running`. */
  started(): void;
  /** Vòng hiện tại đã kết thúc ngoài ý muốn. */
  ended(err?: unknown): void;
  /** Dừng hẳn: hủy timer, chặn reopen sau này. */
  stop(): void;
  /** Resolve khi không còn lần mở lại nào đang dở. */
  settled(): Promise<void>;
}
```

- Tất cả timer đều được `unref()`.
- Không phụ thuộc NATS, nên test được với fake timers.
- `onState` được gọi mỗi khi trạng thái đổi: `running` qua `started()` và sau mỗi lần mở lại thành công, `restarting` khi hẹn lịch, `failed` khi bỏ cuộc.

### 5.3 `ConnectionRegistry`

- `trackHandler(connection: string, label: string): { set(state: HandlerState): void }`. Mỗi lần gọi tạo một mục riêng, không gộp theo label, vì hai handler có thể trùng label.
- `handlerStates(connection: string): ReadonlyArray<{ handler: string; state: HandlerState }>`.
- `clearHandlers()`: được gọi khi lifecycle shutdown.

### 5.4 Các adapter

Mỗi adapter nhận `RestartSupervisor` (lifecycle tạo, kèm policy của connection) và tách phần mở nguồn message ra:

- **`CoreSubscriptionAdapter`:**
  - `open()` = `nc.subscribe` + `flush` + chạy `consume(sub)`.
  - Thêm cờ `stopping`.
  - Khối `finally` chỉ gọi `dispatcher.stop()` khi đang dừng hoặc supervisor đã `failed`. Dispatcher, cùng các retry đang chờ, được giữ qua restart.
- **`JetStreamConsumerAdapter`:**
  - `open()` = `openPull()` hoặc `openPush()` + `consume()`; `ensureConsumer` chạy lại nếu có khai báo `config`, nên consumer bị xóa được tạo lại theo sync mode.
  - Giữ dispatcher, heartbeat, `unsettled` và `inFlight`: message cũ vẫn ack được qua reply subject của chúng.
  - Thay `messages`, chạy lại `watchStatus`.
  - Các bước check (`checkMaxDeliver`, `checkBatchWait`, `checkPushConsumer`) vẫn chạy khi mở lại, nhưng warn chỉ log ở lần đầu (§4.6). Lỗi của check được tính là một lần thử thất bại.
- **`OrderedConsumerAdapter`:**
  - `open()` = tạo ordered consumer + `consume()`.
  - Nếu đã xử lý xong ít nhất một message, lần mở lại bắt đầu từ `streamSequence` cuối cùng + 1 (`DeliverPolicy.StartSequence`), thay cho `deliverPolicy` gốc. Như vậy stream không bị phát lại từ đầu với `deliverPolicy: 'all'`.
- **`NatsServiceAdapter`:**
  - `open()` = `Svcm.add` + `addEndpoint` cho mọi endpoint + `flush`.
  - Iterator và loop mới thay cho bộ cũ; limiter của từng endpoint được giữ.
  - `onServiceStopped(err)` khi chưa dừng: dừng các iterator rồi gọi `supervisor.ended(err)`. Một endpoint loop tự kết thúc khi service còn chạy: `stopService`, rồi `ended()`.

`HandlerAdapter` không đổi interface công khai. Supervisor là chi tiết bên trong mỗi adapter, được lifecycle truyền vào qua constructor.

### 5.5 `NatsLifecycle`

- Resolve `restart` của mỗi connection một lần.
- Tạo supervisor cho mỗi adapter, nối `onState` vào `registry.trackHandler(...)`.
- Gọi `registry.clearHandlers()` trong `shutdown()`.

## 6. Test

### Unit

- `restart.spec.ts`: default, trộn option, từng lỗi validate, `false`.
- `restart-supervisor.spec.ts` (fake timers, `random` cố định):
  - delay tăng theo backoff;
  - attempt tăng;
  - reset sau `maxDelay` ổn định, không reset nếu chết sớm;
  - `maxAttempts` dẫn tới `failed` và log error;
  - connection đóng thì `failed` ngay, cả lúc vòng chết lẫn lúc tới lượt mở lại;
  - `reopen` ném lỗi được tính là một lần thử;
  - `stop()` hủy timer, và reopen đang dở không đổi trạng thái sau `stop()`;
  - `ended()` lặp lại bị bỏ qua;
  - policy `false` thì `failed` ngay.
- Health: không có `handlers` thì giữ kết quả cũ; `failed` thì down kèm reason và details; `restarting` thì up kèm danh sách.
- Từng adapter, với fake subscription/consumer/service:
  - vòng chết thì `reopen` được gọi và vòng mới nhận message;
  - core: retry đang chờ vẫn chạy sau restart;
  - JetStream: message `unsettled` vẫn được heartbeat sau restart;
  - ordered: lần mở lại dùng `opt_start_seq` = seq cuối + 1;
  - service: service tự dừng thì cả service được tạo lại;
  - `stop()` khi đang có lịch hoặc đang mở lại thì đóng sạch, và `idle()` resolve.

### Integration (testcontainers)

- **Pull consumer:** có khai báo `config`; xóa consumer bằng JSM để vòng kết thúc (hoặc status `consumer_deleted` dẫn tới vòng chết, tùy cách nats.js 3.4 phản ứng; plan sẽ xác minh). Kiểm tra nó được tạo lại và nhận message tiếp.
- **Service:** user không có quyền subscribe trên subject của một endpoint, nên service tự dừng; với `restart: { maxAttempts: 2, initialDelay: 50 }`, health `handlers: true` báo `down` với reason `handler failed: ...`.
- **Core subscription:** mất quyền dẫn tới restart, rồi tới `failed` với `maxAttempts`. Nếu không dựng được bằng cấu hình quyền của server test, plan sẽ ghi rõ và chỉ để unit test cho trường hợp này.

## 7. Tài liệu

- `docs/health.md`:
  - option `handlers`, ví dụ readiness/liveness;
  - thay đoạn cuối ("does not restart it") bằng mô tả restart.
- `docs/connections.md` (bảng option) hoặc `docs/lifecycle.md`: mục "Restarting handlers" (option, default, bộ đếm, log, `false`).
- `docs/consumers.md`: ordered consumer tiếp tục từ seq cuối sau restart.
- README:
  - bỏ mục "Ended loops are not restarted" trong Known limitations;
  - thêm `restart` vào danh sách tính năng hoặc option nếu README có liệt kê.

## 8. Rủi ro

- **Reopen trong khi message cũ còn in-flight** (JetStream): ack đi qua reply subject riêng của từng message, không qua subscription, nên vẫn hợp lệ.
- **Push consumer không có `deliverGroup`:** nats.js có thể báo "already bound" nếu subscription cũ chưa được dọn. Lỗi được tính là một lần thử thất bại, và lần sau thường sẽ thành công.
- **Restart vô hạn khi lỗi vĩnh viễn** (ví dụ quyền bị thu hồi hẳn): log warn được giãn theo backoff, trần 30 s. Người dùng muốn bỏ cuộc thì đặt `maxAttempts` và theo dõi health.
