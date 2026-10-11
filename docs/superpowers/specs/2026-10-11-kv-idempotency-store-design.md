# Idempotency store dùng JetStream KV — Design Spec

- **Ngày:** 2026-10-11
- **Trạng thái:** Đã duyệt thiết kế qua hội thoại, chờ review spec
- **Loại:** Architectural (subpath export mới, optional peer dependency mới, mở rộng interface `IdempotencyStore`)
- **Sub-project:** SP-G của đợt xử lý limitation

## 1. Bối cảnh

Store mặc định là `InMemoryIdempotencyStore`: mỗi connection một store, nằm trong bộ nhớ của một process. Nó không chống trùng giữa nhiều instance và mất hết khi restart. Người dùng muốn dùng store chung hiện phải tự viết `IdempotencyStore` trên Redis hoặc SQL (`docs/idempotency.md`). Hệ thống đã chạy NATS thì có sẵn JetStream KV.

## 2. Mục tiêu

1. Một store chống trùng giữa các instance, dùng JetStream KV, không cần hạ tầng nào ngoài NATS.
2. Cách dùng giống các store hiện có: `idempotency: { store }` trong `forRoot`. Lib tự đưa connection cho store.
3. `@nats-io/kv` là peer dependency tùy chọn. Ai không dùng KV store thì không phải cài.

### Ngoài phạm vi

- Một store dùng chung cho nhiều connection. Mỗi instance store gắn với đúng một connection.
- Đổi config của bucket đã có sẵn (store chỉ tạo bucket khi chưa có).
- TTL theo từng key do server quản lý (cần server 2.11+ và chỉ đặt được khi `create`). Spec dùng TTL logic (§4).

## 3. API

### 3.1 Subpath `@loipv/nestjs-nats/kv`

```ts
export interface KvIdempotencyStoreOptions {
  /** KV bucket name (1 to 64 of [-_A-Za-z0-9]; default 'nestjs_nats_idempotency'). */
  bucket?: string;
  /** How long the bucket keeps an entry after its last write, in ms (garbage collection; default 86_400_000). Must exceed every handler's ttl. */
  maxAge?: number;
  /** Bucket storage when the store creates it (default 'file'). */
  storage?: 'file' | 'memory';
  /** Bucket replicas when the store creates it (default 1). */
  replicas?: number;
  /**
   * Prefixes the keys of `@ConsumeOrdered` handlers (keys starting with `ordered:`), so the
   * instances sharing the bucket each process every message of an ordered consumer.
   */
  instanceId?: string;
  /** Clock in ms (tests; default Date.now). */
  now?: () => number;
}

export class KvIdempotencyStore implements IdempotencyStore {
  constructor(options?: KvIdempotencyStoreOptions);
}
```

- Option được validate trong constructor, lỗi ném `NatsConfigError`:
  - `bucket` khớp `/^[-\w]+$/` và dài không quá 64 ký tự (`expected at most 64 letters, digits, "-" or "_"`);
  - `maxAge` là số nguyên lớn hơn 0 và không quá 2^31-1 nhân 1000;
  - `storage` thuộc hai giá trị đã cho;
  - `replicas` là số nguyên ≥ 1;
  - `instanceId`, nếu có, là chuỗi không rỗng.
- `package.json`:
  - export `"./kv"` (types + default) giống các subpath khác;
  - `peerDependencies["@nats-io/kv"] = "^3.0.0"`, đánh dấu tùy chọn trong `peerDependenciesMeta`;
  - devDependency `@nats-io/kv` `^3.4.0`.
- Module của subpath chỉ import `@nats-io/kv` trong file đó. Entry chính (`@loipv/nestjs-nats`) không import `@nats-io/kv`.

### 3.2 Hook `bind` trên `IdempotencyStore`

```ts
export interface IdempotencyStoreContext {
  /** The connection name. */
  connection: string;
  /** The connection the store runs on. */
  nc: NatsConnection;
  /** The JetStream client of that connection, with its `jetstream.options` (domain, API prefix, timeout). */
  js: JetStreamClient;
}

export interface IdempotencyStore {
  // existing members...
  /**
   * Called once with the connection of `forRoot`, at module init, before any message is handled.
   */
  bind?(ctx: IdempotencyStoreContext): Promise<void> | void;
}
```

- `IdempotencyStoreContext` được export từ entry chính.
- Lifecycle gọi `bind` trong `onModuleInit`, trước khi sync stream, cho mọi connection có `options.idempotency.store` với `bind`. Store mặc định (in-memory) không có `bind`.
- `js` là `entry.js()` của connection đó, nên store dùng đúng `jetstream.options` (domain, API prefix) của connection.
- Lỗi từ `bind` làm init fail: lifecycle shutdown rồi ném lại lỗi, như các lỗi init khác. Khi shutdown, lifecycle không gọi `close()` trên store mà mọi lần `bind` trong app này đều lỗi: store đó có thể đang gắn với một app khác (bind lần hai của `KvIdempotencyStore` lỗi), và `close()` sẽ gỡ binding của app kia. Store đã bind thành công (kể cả khi store của connection khác bind lỗi) vẫn được đóng.

## 4. Hành vi của `KvIdempotencyStore`

### 4.1 Bind

- Bind lần đầu: mở bucket bằng `new Kvm(js).open(bucket, { allow_direct: false })` rồi gọi `status()` trên bucket đó. `Kvm.open` của `@nats-io/kv` 3.4 không tra stream, nên bucket chưa có chỉ lộ ra ở `status()` (lỗi stream not found). Khi đó tạo bằng `create(bucket, { history: 1, ttl: maxAge, storage, replicas })`, rồi mở lại bằng `open(bucket, { allow_direct: false })` và gọi `status()`. Bucket đã có thì để nguyên config.
- Đọc từ leader, không dùng direct get: direct get có thể do một replica chậm hơn leader trả lời, mà claim compare-and-set trên revision vừa đọc. Handle do `create` trả về dùng direct get khi server hỗ trợ, nên store mở lại bucket sau khi tạo. Stream vẫn giữ `allow_direct` mặc định cho client khác.
- `status().ttl` (max age của bucket, 0 là không giới hạn) được lưu lại. Lần đầu một `claim` có `ttlMs` lớn hơn max age đó (khi > 0), warn một lần bằng `Logger('KvIdempotencyStore')`: `KvIdempotencyStore bucket "<b>" keeps entries for <n> ms, less than the idempotency ttl <m> ms; raise its max age`.
- Gọi `bind` lần hai khi store vẫn đang gắn, kể cả cùng connection, ném `NatsConfigError` `KvIdempotencyStore is already bound to NATS connection "<a>"; give each connection its own store`.
- `close()` gỡ binding (connection và handle KV về `undefined`), không đóng connection (do lib sở hữu). Sau đó store bind lại được, ví dụ cho app thứ hai trong cùng process. Lifecycle gọi `close()` cho store trong option khi shutdown và sau init lỗi.
- `bind` lỗi thì binding được gỡ ngay, nên store bind lại được.
- Gọi `claim`, `complete` hay `release` trước khi bind ném `NatsModuleError` `KvIdempotencyStore is not bound to a NATS connection; pass it as idempotency.store of NatsModule.forRoot()`.

### 4.2 Key

- Với `instanceId`, khi `storeKey` bắt đầu bằng `ordered:`, key lưu là `ordered:<instanceId>:<phần còn lại>` trước khi mã hóa.
- Key KV là `base64url(utf8(key))`, không có padding, với `key` là key sau bước `instanceId` ở trên.
- Nếu chuỗi mã hóa đó dài hơn 256 ký tự, key KV là `h.` + `base64url(sha256(utf8(key)))` (43 ký tự sau `h.`). Lý do: `Nats-Msg-Id` do publisher đặt, có thể dài tùy ý; subject `$KV.<bucket>.<key>` vượt control line của server (mặc định 4096 byte) thì server đóng cả connection. Base64url không chứa `.`, nên key băm không trùng với key không băm.
- Không có `instanceId` mà gặp key `ordered:` lần đầu: warn một lần, bằng logger `Logger('KvIdempotencyStore')` của Nest:
  `KvIdempotencyStore without instanceId is shared by the instances of @ConsumeOrdered handlers: each instance skips the messages another processed. Set instanceId.`

### 4.3 Value và TTL logic

- Value là JSON `{"s":"p"|"d","x":<expiresAt ms>}`, với `p` là in-progress và `d` là done. `expiresAt = now() + ttlMs`.
- Một entry được coi là còn hiệu lực khi `x > now()`. Entry hết hạn được coi như không tồn tại.
- `max_age` của bucket chỉ để dọn rác. Lệch đồng hồ giữa các instance làm lệch TTL đúng bằng độ lệch đó.

### 4.4 Thao tác

| Thao tác | Cách làm |
|---|---|
| `claim(key, ttl)` | `put(k, p, { previousSeq: 0 })`: chỉ ghi được khi subject chưa có revision nào. Thành công thì trả `'claimed'`. Nếu subject đã có revision (kể cả marker DEL hay PURGE), server trả lỗi sai revision (10071), rồi `get(k)`:<br>• entry bị xóa, purge, hết hạn hoặc không đọc được: `update(k, p, revision)`. Thành công thì `'claimed'`. Lỗi sai revision thì lặp lại thao tác (tối đa 3 lần, sau đó ném lỗi, lib xử lý như store error và retry). Entry đã biến mất ở giữa chừng (`get` trả `null`) thì `put` lại.<br>• còn hiệu lực: trả `'duplicate'` nếu `s` là `d`, `'in-progress'` nếu `s` là `p`. |
| `complete(key, ttl)` | `put(k, d)` |
| `release(key)` | `delete(k)` |
| `close()` | Gỡ binding (§4.1); không đóng connection (do lib sở hữu) |

- Không dùng `kv.create`: trong `@nats-io/kv` 3.4 nó tự `get` rồi ghi đè marker DEL/PURGE, trùng với đường `get` → `update(revision)` của store.

- Value không đọc được (JSON lỗi hoặc thiếu field) được coi là hết hạn và bị ghi đè.
- Lỗi khác từ KV được ném nguyên, và lib xử lý theo đường "store error" hiện có (retry, không dead-letter).

## 5. Thành phần

- `src/idempotency/store.ts`: `IdempotencyStoreContext` và `bind?` trong interface.
- `src/module/nats-lifecycle.ts`: gọi `bind` trong `onModuleInit`.
- `src/kv/kv-idempotency-store.ts`: class, option, mã hóa key, TTL logic.
- `src/kv/index.ts`: export của subpath.
- `src/index.ts`: export `IdempotencyStoreContext`.
- `package.json`: export `./kv`, peer dependency, devDependency.
- `scripts/check-docs.mjs` và cấu hình kiểm tra package: cập nhật nếu cần cho subpath mới (đường dẫn kiểu của `docs:check`, `check:pkg`).

## 6. Test

### Unit (`test/unit/kv-idempotency-store.spec.ts`, với KV giả)

- Validate option.
- `claim`:
  - key mới thì `'claimed'`;
  - key done còn hạn thì `'duplicate'`;
  - key in-progress còn hạn thì `'in-progress'`;
  - key hết hạn thì `update` với đúng revision, trả `'claimed'`;
  - lỗi sai revision thì thử lại, và trả `'in-progress'` nếu instance khác vừa claim;
  - value hỏng thì ghi đè.
- `complete` và `release` gọi đúng `put` và `delete`; claim sau release thì `'claimed'`.
- Mã hóa key: ký tự `:`, `|`, `%`, `/`, unicode; key mã hóa ≤ 256 ký tự giữ nguyên, key dài hơn được băm (`h.<43 ký tự>`, khác nhau theo key, ổn định cho cùng key); tiền tố `instanceId` chỉ áp dụng cho key `ordered:`; warn đúng một lần khi không có `instanceId`.
- `bind`: lần hai thì lỗi; dùng trước bind thì lỗi; bucket chưa có thì được tạo với đúng config; bucket đã có thì chỉ mở.
- Lifecycle: `bind` được gọi một lần với `{ connection, nc }` trước khi sync stream; `bind` ném lỗi thì init fail.

### Integration

- Hai app, mỗi app một connection, cùng `bucket`. Mỗi app có một handler `@Subscribe` dùng queue group khác nhau, nên cả hai cùng nhận message, và bật `idempotency`. Publish hai message có cùng `Nats-Msg-Id`: handler chỉ chạy một lần tổng cộng trên hai app.
- Claim hết hạn: một store với `now` giả claim một key với ttl 1 ms, rồi tiến đồng hồ; claim lại thì `'claimed'` trên server thật.
- Bucket chưa có thì được tạo với `history: 1` và `max_age` đúng.
- Hai message có cùng một `Nats-Msg-Id` dài 5 KB: handler chạy một lần, connection không bị đóng (một message thường sau đó vẫn được xử lý).

## 7. Tài liệu

- `docs/idempotency.md`, mục "Stores": thêm "JetStream KV store" gồm cài `@nats-io/kv`, ví dụ, bảng option, cách hoạt động (TTL logic, lệch đồng hồ, `maxAge`), `instanceId` cho ordered consumer, một store cho mỗi connection. Thêm hook `bind` cho người tự viết store.
- README: dòng tính năng idempotency nhắc KV store.
- `CHANGELOG.md`: gộp vào mục 0.1.0.
