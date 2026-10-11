# CloudEventRouter trên push và ordered consumer — Design Spec

- **Ngày:** 2026-10-11
- **Trạng thái:** Đã duyệt thiết kế qua hội thoại, chờ review spec
- **Loại:** Architectural (mở rộng API công khai của `@CloudEventRouter`)
- **Sub-project:** SP-E của đợt xử lý limitation (README, "Router: subscribe and pull only")

## 1. Bối cảnh

`@CloudEventRouter` hiện chỉ nhận CloudEvent từ một core subscription (`subscribe`) hoặc một JetStream pull consumer (`consume`). Hàm `defineRouter` trong `src/discovery/handler-explorer.ts` dựng một `HandlerDefinition` loại `subscribe` hoặc `consume` với `delivery: 'pull'`.

Các phần còn lại đã hỗ trợ push và ordered:
- adapter push và adapter ordered;
- validator (`validateDeliverGroup`, `validateOrdered`);
- settlement của CloudEvent không khớp route (`failSubscribe` dùng cho cả core lẫn ordered);
- harness `/testing`.

## 2. Mục tiêu

Router nhận CloudEvent từ một durable push consumer (`consumePush`) hoặc một ordered consumer (`consumeOrdered`), với cùng hành vi như `@ConsumePush` và `@ConsumeOrdered`.

### Ngoài phạm vi

- Router trên NATS service endpoint.
- Batch cho router (vẫn không hỗ trợ).

## 3. API

```ts
export type CloudEventRouterOptions = (
  | { subscribe: string; queue?: string }
  | { consume: { stream: string; durable: string; config?: Partial<ConsumerConfig> } }
  | {
      consumePush: {
        stream: string;
        durable: string;
        config?: Partial<ConsumerConfig>;
        deliverGroup?: string;
      };
    }
  | {
      consumeOrdered: {
        stream: string;
        filterSubjects?: string | string[];
        deliverPolicy?: OrderedDeliverPolicy;
        headersOnly?: boolean;
      };
    }
) & {
  unmatched?: 'ack' | 'term' | 'dlq';
  connection?: string;
  concurrency?: number;
  codec?: Codec;
  retry?: RetryOptions | false;
  dlq?: DlqOptions | boolean;
  idempotency?: boolean | IdempotencyOptions;
};
```

- Router khai báo đúng **một** trong `subscribe`, `consume`, `consumePush` và `consumeOrdered`. Nếu không, bootstrap báo `NatsConfigError` `@CloudEventRouter <Class> must set exactly one of subscribe, consume, consumePush or consumeOrdered`.
- `consumePush` dựng một định nghĩa `{ kind: 'consume', delivery: 'push', options: { ...common, stream, durable, config?, deliverGroup? } }`, giống `@ConsumePush`.
- `consumeOrdered` dựng một định nghĩa `{ kind: 'ordered', options: { ...common, stream, filterSubjects?, deliverPolicy?, headersOnly? } }`, giống `@ConsumeOrdered`.
- `concurrency` không đi được với `consumeOrdered`. Lỗi là `NatsConfigError` `@CloudEventRouter <Class> with consumeOrdered does not support concurrency`, báo trong `defineRouter`, trước khi validator chạy.
- Các kiểm tra còn lại dùng validator hiện có, không viết lại:
  - `deliverGroup` phải khớp `config.deliver_group`;
  - `stream`, `filterSubjects` và `deliverPolicy` của ordered phải hợp lệ;
  - các kiểm tra retry, DLQ, idempotency và codec.

## 4. Hành vi

- **Push:** như `@ConsumePush`. Gồm deliver subject, nhóm `deliverGroup`, `max_ack_pending`, heartbeat, restart (SP-C) và dead letter qua plugin (SP-D).
- **Ordered:** như `@ConsumeOrdered`. Message được xử lý từng cái một theo thứ tự stream; retry diễn ra tại chỗ; không ack gì cả.
- **CloudEvent không khớp route:**

| `unmatched` | push | ordered |
|---|---|---|
| `'ack'` (mặc định) | ack, log debug | bỏ qua, log debug |
| `'term'` | `term` | bỏ message, như core |
| `'dlq'` | dead-letter rồi `term`; không có DLQ thì như `'term'` kèm warn | dead-letter, thử lại tại chỗ nếu publish lỗi; không có DLQ thì bỏ kèm warn |

Cả hai loại đều không có request, nên không có reply `404`. Subject DLQ mặc định của router ordered là `dlq.<stream>.ordered.<Class>.@CloudEventRouter`, theo quy tắc chung với `methodName` của router.

## 5. Thành phần

- `src/decorators/cloudevent.decorators.ts`: type ở §3 và doc comment của `CloudEventRouterOptions` (nhắc đủ bốn nguồn).
- `src/discovery/handler-explorer.ts` (`defineRouter`): hai nhánh mới, kiểm tra "đúng một nguồn" và kiểm tra `concurrency` khi dùng ordered.
- Không đổi adapter, settlement, validator hay harness.

## 6. Test

### Unit

- Explorer:
  - `consumePush` dựng `kind: 'consume'`, `delivery: 'push'` với đủ option;
  - `consumeOrdered` dựng `kind: 'ordered'` với đủ option;
  - khai báo hai nguồn, hoặc không có nguồn nào, thì báo lỗi bằng message mới;
  - `concurrency` cùng `consumeOrdered` thì báo lỗi;
  - `deliverGroup` lệch `config.deliver_group` thì báo lỗi qua validator.
- Harness:
  - dispatch tới router push: khớp route thì `ack`; `unmatched: 'term'` thì `term`;
  - dispatch tới router ordered: khớp route thì `'none'`; `unmatched: 'term'` thì `'drop'`; `unmatched: 'dlq'` có DLQ thì `'dlq'`.

### Integration

- Router `consumePush` (có `deliverGroup`, có khai báo `config`) nhận một CloudEvent binary và gọi đúng method `@OnCloudEvent`.
- Router `consumeOrdered` nhận hai CloudEvent khác `type`, xử lý theo đúng thứ tự stream và gọi đúng method cho từng event.

## 7. Tài liệu

- `docs/cloudevents.md`: hai nguồn mới, ví dụ ngắn, bảng hành vi của `unmatched` theo nguồn. Bỏ câu "(push and ordered consumers are not supported)".
- `docs/consumers.md`: dòng `@CloudEventRouter` trong bảng thành "Yes, with `consume`" / "Yes, with `consumePush`" / "Yes, with `consumeOrdered`".
- README: bỏ mục "Router: subscribe and pull only" trong Known limitations.
- `CHANGELOG.md`: gộp vào mục 0.1.0 (gói chưa publish).
