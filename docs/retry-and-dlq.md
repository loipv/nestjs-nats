# Retry and dead-letter queue

[Back to the README](../README.md)

A failed message is retried with exponential backoff. When retries run out, or the error is not worth retrying, the message can be published to a dead-letter queue (DLQ). Retry is **on** by default. The DLQ is **off**.

Set `retry` and `dlq` on `NatsModule.forRoot()` for every handler of a connection, or on a single `@Subscribe`, `@Consume`, `@ConsumePush` or `@ConsumeOrdered`. Options of a handler are merged over those of the module (a retry option set to `undefined` keeps the inherited value), a handler's `retry: false` turns retries off for it, and a handler's `retry` object turns them back on when the module disabled them. Invalid values fail startup with a `NatsConfigError`. The examples in this section and the next ones use placeholder types and services (`OrderDto`, `PaymentService`, ...) that you replace with your own.

```ts
import { Consume, NatsModule, NonRetryableError, Payload, defaultIsRetryable } from '@loipv/nestjs-nats';

NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  retry: { maxRetries: 3, initialDelay: 500 },
  dlq: true,
});

class BillingHandlers {
  constructor(private readonly payments: PaymentService) {}

  @Consume({
    stream: 'ORDERS',
    durable: 'billing',
    retry: {
      maxRetries: 8,
      // Replaces the default check. Compose with `defaultIsRetryable` to keep its rules.
      isRetryable: (err) => defaultIsRetryable(err) && !(err instanceof PaymentDeclinedError),
    },
    dlq: { subject: 'dlq.billing' },
  })
  async bill(@Payload() order: OrderDto) {
    if (order.total <= 0) throw new NonRetryableError('empty order'); // straight to the DLQ
    await this.payments.charge(order);
  }
}
```

## Retry options

| Option | Default | Description |
|---|---|---|
| `maxRetries` | `5` | Redeliveries after the first attempt, so a message is delivered at most `maxRetries + 1` times |
| `initialDelay` | `1000` | Delay in ms before the first redelivery. At most `2147483647` |
| `factor` | `2` | Multiplier of the delay for each further redelivery (`>= 1`) |
| `maxDelay` | `60000` | Upper bound of the delay in ms, before jitter. Must be `>= initialDelay` and at most `2147483647` |
| `jitter` | `0.2` | Random spread of each delay: `0.2` means plus or minus 20 % (between `0` and `1`) |
| `isRetryable` | `defaultIsRetryable` | `(err) => boolean`. Errors that return `false` are not retried |

The delay before redelivery `n` is `min(maxDelay, initialDelay * factor^(n - 1))`, with the jitter applied after the cap, so a delay can exceed `maxDelay` by up to `jitter` times 100 %. `defaultIsRetryable` returns `false` for `NatsDecodeError`, `NonRetryableError` and an `HttpException` with a 4xx status, and `true` for everything else.

## What happens to a failed message

| Handler | Retryable error, retries left | Non-retryable error, or retries exhausted |
|---|---|---|
| `@Consume`, `ack: 'auto'` | `nak(delay)`: the server redelivers after the delay | If a DLQ is configured, publish to it, then `term()`. Otherwise `term()` |
| `@Consume`, `ack: 'manual'` | If the handler threw before settling: as for `'auto'`. A message you settled yourself is left alone | As for `'auto'` |
| `@ConsumePush` | As for `@Consume` | As for `@Consume` |
| `@ConsumeOrdered` | The handler runs again in place after the delay, before the next message ([details](#ordered-consumers)) | If a DLQ is configured, publish to it (retrying without limit if that fails). Otherwise it is logged and dropped. Never `term()`, nothing is acknowledged |
| `@Subscribe`, no reply subject | The message is processed again in the same process after the delay | If a DLQ is configured, publish to it. Otherwise it is logged and dropped |
| `@Subscribe`, request (has a reply subject) | No retry: the requester gets an error reply | No DLQ: the requester gets an error reply |
| `@ServiceEndpoint` | No retry: the caller gets an error reply | No DLQ: the caller gets an error reply |

- **Core retries live in the process.** A retry of a `@Subscribe` message is a timer, not a redelivery by the server. See [Known limitations](../README.md#known-limitations).
- **Heartbeats.** Every `ack_wait / 2`, the library calls `working()` on each `@Consume` message it has received and not yet settled: messages being handled or dead-lettered, messages waiting for a free slot, and messages collected into a batch or waiting with their batch for a slot. A slow handler or a slow DLQ publish does not make the server redeliver them. `@ConsumePush` has no gap: the server pushes messages up to `max_ack_pending`, and every one received is heartbeated. For `@Consume`, one gap remains: messages the NATS client has already pulled but not yet handed to the library are not heartbeated. While every slot is busy there can be up to `concurrency` of them (`max(batch.maxSize, concurrency)` with `batch`). With `concurrency: 1` and no `batch` there are none. Otherwise, if a handler can run longer than the consumer's `ack_wait`, such a message can be redelivered and processed twice: set `ack_wait` above the longest run of the handler.
- **`max_deliver`.** The server stops redelivering a message after the consumer's `max_deliver`, and the message then never reaches the DLQ. At startup the library logs a warning when a consumer has `max_deliver` greater than `0` and at most `maxRetries`. Use `max_deliver` of at least `maxRetries + 1`, or `-1`, which is the default for consumers the library creates.

## Dead-letter queue

`dlq` is `true`, or an object, on the module or a handler. `dlq: false` on a handler always wins.

| Option | Default | Description |
|---|---|---|
| `subject` | see below | A non-empty subject, or a function `(handler: HandlerInfo, msg: InboundMessage) => string` |
| `mode` | `'jetstream'` for the JetStream consumers, `'core'` for `@Subscribe` | `'jetstream'` waits for the PubAck. `'core'` is a plain publish |

The default subject is `dlq.<stream>.<durable>` for `@Consume` and `@ConsumePush`, `dlq.<stream>.ordered.<Class>.<method>` for `@ConsumeOrdered`, and `dlq.<actual subject>` for `@Subscribe`. In `'jetstream'` mode a stream must capture the subject. This stream captures every default subject:

```ts
NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  streams: [{ name: 'DLQ', subjects: ['dlq.>'] }],
  dlq: true,
});
```

A dead letter has the original body, unchanged, and the original headers except the ones whose name starts with `Nats-` (in any case), plus these. This is what the outbound plugins receive — they can change it (see [Dead letters](./plugins.md#dead-letters)) — and with tracing, `traceparent` is the dead letter's own publish span.

| Header | Value |
|---|---|
| `Nats-DLQ-Subject` | Subject the message arrived on |
| `Nats-DLQ-Stream` | Stream of the original message (JetStream messages only) |
| `Nats-DLQ-Consumer` | Consumer that failed to process it (JetStream messages only) |
| `Nats-DLQ-Seq` | Stream sequence of the original message (JetStream messages only) |
| `Nats-DLQ-Deliveries` | Number of attempts made |
| `Nats-DLQ-Error` | Message of the last error. At most 1024 characters, line breaks replaced by spaces |
| `Nats-DLQ-Timestamp` | When it was dead-lettered, ISO 8601 |
| `Nats-DLQ-Msg-Id` | The original `Nats-Msg-Id`, if the message had one |

The `Nats-` headers of the original message (`Nats-Msg-Id`, `Nats-Expected-*`, `Nats-Rollup`, `Nats-TTL`, `Nats-Incr`, `Nats-Schedule*`, `Nats-Batch-*` and so on) are not copied: they were directives for the original publish and would make the DLQ publish fail or misbehave. Instead, a dead letter of a JetStream message gets its own `Nats-Msg-Id`, `dlq:<stream>:<consumer>:<stream sequence>`. Each consumer that fails the same message therefore keeps its own dead letter in a shared DLQ stream, while the same dead letter sent twice is de-duplicated by the DLQ stream. A dead letter of a core message has no `Nats-Msg-Id`.

If publishing to the DLQ fails, a `@Consume` message is not lost: it is `nak`'d with the backoff delay and an error is logged, so it comes back and the DLQ is tried again. A `@Subscribe` message that cannot be dead-lettered is logged and dropped. Dead letters of a batch are sent one at a time, each waiting for the outbound plugins (up to their timeout), so slow plugins lengthen the shutdown of a failing batch within `drainTimeout`. Dead letters go through the connection's outbound plugins, marked with `deadLetter`. A plugin that throws or does not pass one on in time makes the DLQ publish fail, as above, and a plugin that drops one makes the message count as dead-lettered. See [Dead letters](./plugins.md#dead-letters).

## Ordered consumers

An ordered consumer acknowledges nothing, so a failed message is neither `nak`'d nor `term`'d: it is retried in place, so the stream order holds, and the next message waits.

- **Retry.** A retryable error runs the handler again on the same message after the backoff delay (`ctx.attempt` counts the attempts). The wait is cancelled at shutdown. With `retry: false` the first error goes straight to the next step.
- **Dead-lettering.** When retries run out, or the error is not retryable, the message is published to the DLQ if there is one, then the consumer moves on. Without a DLQ the error is logged and the message dropped. The dead letter's `Nats-DLQ-Consumer` is `ordered:<Class>.<method>`, `Nats-DLQ-Seq` is the stream sequence, and its `Nats-Msg-Id` is `dlq:<stream>:ordered:<Class>.<method>:<stream sequence>`, so the instances that dead-letter the same message (every instance receives it) leave one dead letter in the DLQ stream, within its duplicate window.
- **A failing DLQ publish is retried without limit.** The consumer does not skip the message, so the stream stalls behind it, with an error logged on every attempt, until the publish succeeds or the application shuts down. This includes errors that can never succeed, such as a `dlq.subject` function that throws or a DLQ stream that does not exist. A plugin or idempotency store that fails outside the handler is retried in place without limit too.
- **Shutdown.** A retry wait is abandoned with a warning, and that message is not redelivered. See [Lifecycle](./lifecycle.md).
