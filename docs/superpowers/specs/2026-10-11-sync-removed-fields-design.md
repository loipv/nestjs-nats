# Phát hiện và reset field bị xóa khỏi khai báo stream/consumer — Design Spec

- **Ngày:** 2026-10-11
- **Trạng thái:** Đã duyệt thiết kế qua hội thoại, chờ review spec
- **Loại:** Architectural (đổi hành vi sync và dữ liệu lib ghi lên server)
- **Sub-project:** SP-F của đợt xử lý limitation (README, "Undetected removals")

## 1. Bối cảnh

Stream/consumer sync (`src/jetstream/stream-manager.ts`) chỉ so sánh những field đang được khai báo (`diffConfig`). Khi có lệch, nó gọi `jsm.streams.update(name, cfg)` hoặc `jsm.consumers.update(stream, durable, cfg)`. Cả hai hàm của nats.js 3.4 đều gộp `cfg` vào config hiện có (`Object.assign(old.config, cfg)`).

Hệ quả: một field người dùng xóa khỏi khai báo vẫn giữ giá trị cũ trên server. `verify` cũng không nhận ra.

## 2. Mục tiêu

1. Ghi nhớ những field đã khai báo ở lần sync trước, để biết field nào đã bị xóa.
2. Ở `create-or-update`, đưa field bị xóa về mặc định của server.
3. Ở `verify`, báo field bị xóa là lệch.

### Ngoài phạm vi

- So sánh sâu bên trong field lồng nhau. Phát hiện chỉ ở cấp field cao nhất. Ngoại lệ là `metadata`, xem §4.
- Field khai báo là `0` mà server lưu ở dạng khác (`duplicate_window: 0`, `max_deliver: 0`). Đây là limitation riêng, vẫn giữ nguyên trong tài liệu.

## 3. Snapshot

- Key metadata là `SNAPSHOT_KEY = 'nestjs-nats.declared'`.
- Giá trị là tên các field cấp cao nhất của khai báo (sau `withoutNulls` và sau khi bỏ field `undefined`), **trừ `metadata`**, sắp xếp tăng dần rồi nối bằng `,`. Ví dụ `"max_age,retention,subjects"`. Không có field nào thì là `""`.
  - Stream: tính trên khai báo, bỏ `name` (luôn có).
  - Consumer: tính trên khai báo của người dùng (bỏ `durable_name`), không gồm mặc định lib thêm khi tạo (`withConsumerDefaults`, `createDefaults` của push).
- `diffConfig` bỏ qua key snapshot khi so `metadata`, giống các key `_nats.` của server.

## 4. `metadata` gửi lên server

Khi tạo hoặc update, lib gửi `metadata` như sau:

- **Khai báo có `metadata`:** `{ ...declared.metadata, [SNAPSHOT_KEY]: snapshot }`. Khi đó so sánh `metadata` là so **chính xác**, bỏ qua `_nats.*` và key snapshot: một key người dùng bị xóa khỏi `metadata` khai báo là lệch, và update sẽ xóa nó trên server.
- **Khai báo không có `metadata`:** `{ ...current metadata, trừ _nats.*, [SNAPSHOT_KEY]: snapshot }`. Khi tạo mới thì chỉ có key snapshot. Như vậy metadata do tool khác ghi được giữ lại.

## 5. Hành vi theo mode

Định nghĩa:
- `previous`: tập field trong snapshot hiện có trên server. Không có key snapshot thì là `undefined`.
- `removed`: `previous − declared − {'metadata'}`, chỉ gồm field mà server còn giá trị (`current[field] !== undefined`).
- `removedImmutable`: phần của `removed` nằm trong `STREAM_IMMUTABLE_FIELDS` hoặc `CONSUMER_IMMUTABLE_FIELDS`.
- `toReset`: `removed − removedImmutable`.

| Mode | Stream/consumer chưa có | Đã có |
|---|---|---|
| `create-or-update` | Tạo, kèm `metadata` có snapshot | Xem bên dưới |
| `create-only` | Tạo, kèm snapshot | Không đụng tới |
| `verify` | Lỗi như hiện nay | Diff hiện có, cộng mỗi field trong `toReset` và `removedImmutable` thành một mục lệch `<field> (removed from the declaration, current: <json>)`. Không ghi gì |
| `none` | — | — |

`create-or-update`, khi stream/consumer đã có:

1. Tính `diffs` như hiện nay. Xung đột field immutable được khai báo vẫn ném `StreamConfigConflictError`.
2. Với mỗi field trong `removedImmutable`: warn `JetStream stream "<name>" keeps <field> <json>: it cannot be changed (removed from the declaration)`. Với consumer, chữ là `consumer "<stream>/<durable>"`.
3. Update khi có ít nhất một trong ba điều: `diffs` không rỗng, `toReset` không rỗng, hoặc key snapshot trên server khác snapshot mới (thiếu hoặc khác giá trị).
   - Update gửi `{ ...cfg, metadata, ...Object.fromEntries(toReset.map(f => [f, undefined])) }`. JSON bỏ các key `undefined`, nên server áp dụng mặc định của nó.
   - Sau khi update thành công, log mỗi field trong `toReset`: `JetStream stream "<name>": reset <field> to the server default (removed from the declaration)`.
4. **Server không lưu metadata:** nếu response của update hoặc tạo không có key snapshot đúng giá trị vừa gửi, warn một lần cho mỗi connection: `JetStream on connection "<name>" does not keep stream/consumer metadata (NATS server 2.10 or later is needed); removed fields are not detected`. Sau đó tắt tính năng cho connection đó: không ghi snapshot, không tính `removed`. Như vậy update thuần vì snapshot không lặp lại ở mỗi lần boot.

Lần đầu chạy trên stream/consumer đã có sẵn (`previous` là `undefined`): không có `removed`; lib update một lần để ghi snapshot.

## 6. Thành phần

- `src/jetstream/stream-sync.ts`:
  - `SNAPSHOT_KEY`;
  - `declaredFields(cfg, exclude: string[]): string[]` (đã sắp xếp);
  - `snapshotOf(fields): string`;
  - `parseSnapshot(metadata?): string[] | undefined`;
  - `removedFields(current, previous, declared, immutable): { reset: string[]; immutable: string[] }`;
  - `syncMetadata(declared, current?, snapshot): Record<string, string>`;
  - `diffConfig`: bỏ qua key snapshot, và so `metadata` chính xác khi được khai báo (§4).
- `src/jetstream/stream-manager.ts`:
  - `ensureStream` và `ensureConsumer` theo §5;
  - một cờ theo connection, `metadataUnsupported`, giữ ở mức instance hoặc entry. `NatsStreamManager` được tạo nhiều lần cho một connection (lifecycle, adapter), nên cờ phải nằm ở nơi dùng chung, ví dụ một `WeakSet<ConnectionEntry>` trong module.

## 7. Test

### Unit (`test/unit/stream-sync.spec.ts`, với `jsm` giả)

- `declaredFields`, `snapshotOf`, `parseSnapshot`, `removedFields`, `syncMetadata` theo §3–§5.
- `diffConfig`:
  - bỏ qua key snapshot;
  - key user bị xóa khỏi `metadata` khai báo là lệch.
- `ensureStream`:
  - tạo mới kèm snapshot;
  - stream có từ trước, chưa có snapshot: chỉ update để ghi snapshot, không reset gì;
  - snapshot `max_age,subjects`, khai báo chỉ `subjects`: update có `max_age: undefined`, kèm log reset;
  - field immutable bị xóa: warn, không đưa vào update;
  - không có gì đổi và snapshot khớp: không update;
  - `verify`: lỗi có mục `max_age (removed from the declaration, ...)`;
  - metadata của tool khác được giữ khi khai báo không có `metadata`;
  - response không có key snapshot: warn đúng một lần, rồi các lần sau không update vì snapshot.
- `ensureConsumer`: các trường hợp tương ứng, gồm mặc định push không bị tính vào snapshot.

### Integration

- **Stream:** khai báo stream `RM` với `max_age` 60 s, boot app rồi đóng. Khai báo lại không có `max_age`, boot lại. `jsm.streams.info('RM')` cho `max_age` bằng `0`, và metadata có `nestjs-nats.declared`.
- **Consumer:** khai báo consumer với `max_ack_pending: 5`, boot. Khai báo lại không có field đó, boot lại. Server trả về `max_ack_pending` mặc định (khác `5`).
- **Verify:** sau lần boot đầu, mode `verify` với khai báo đã bỏ `max_age` làm bootstrap fail, lỗi nhắc `removed from the declaration`.

## 8. Tài liệu

- `docs/streams.md`: snapshot (`nestjs-nats.declared`), reset về mặc định ở `create-or-update`, báo lệch ở `verify`, field immutable chỉ warn, lần đầu chỉ ghi snapshot, yêu cầu server 2.10+ cho metadata, xử lý `metadata` của người dùng.
- README, mục "Undetected removals": bỏ phần field bị xóa, giữ ghi chú về khai báo `0`, và đổi tên mục cho phù hợp.
- `CHANGELOG.md`: gộp vào mục 0.1.0.
