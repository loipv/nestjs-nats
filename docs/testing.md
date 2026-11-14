# Testing

[Back to the README](../README.md)

`@loipv/nestjs-nats/testing` lets you test code that uses the library without a NATS server. It needs nothing beyond the library itself, and works with any test runner. The examples use `@nestjs/testing` and `node:assert`.

## Mock clients

`createMockNatsClient()` and `createMockJetStreamClient()` return a real `NatsClient` and `JetStreamClient` that run on a fake connection. Outbound plugins and CloudEvents work as in production. Everything published is recorded in `published`.

```ts
import assert from 'node:assert/strict';
import { NatsRequestError } from '@loipv/nestjs-nats';
import { createMockJetStreamClient, createMockNatsClient } from '@loipv/nestjs-nats/testing';

const nats = createMockNatsClient({ cloudEvents: { source: 'urn:test' } });

nats.publish('orders.created', { id: 'o1' }, { cloudEvent: { type: 'order.created' } });
await nats.flush(); // publishes are asynchronous: flush before you assert
assert.equal(nats.published[0]?.subject, 'orders.created');
assert.deepEqual(nats.published[0]?.data, { id: 'o1' });
assert.equal(nats.published[0]?.cloudEvent?.type, 'order.created');

// Requests are answered from stubs. With no stub, request() rejects with code 'no_responders'.
nats.whenRequest('orders.get').reply({ id: 'o1', total: 10 });
assert.deepEqual(await nats.request('orders.get', { id: 'o1' }), { id: 'o1', total: 10 });
nats.whenRequest('orders.cancel').fail({ code: 409, description: 'already shipped' });
await assert.rejects(
  nats.request('orders.cancel', { id: 'o1' }),
  (err) => err instanceof NatsRequestError && err.code === 'service_error',
);

const js = createMockJetStreamClient();
await js.publish('orders.created', { id: 'o1' }, { msgId: 'o1' });
const again = await js.publish('orders.created', { id: 'o1' }, { msgId: 'o1' });
assert.equal(again.duplicate, true); // acked by a stream called 'mock'
assert.equal(js.published.length, 2);
```

| Member | Description |
|---|---|
| `published` | The recorded messages, in order. Each has `subject`, `data` (decoded with the codec, the event's data for a CloudEvent), `raw` (the bytes), `headers`, `cloudEvent` (when the message is one), `reply` (for a request) and `atomic` (see below). On the core client, requests are included |
| `whenRequest(subject).reply(value)` | Answers requests to `subject` (a NATS subject pattern) with `value`. The latest matching stub wins |
| `whenRequest(subject).fail({ code, description })` | Answers with a service error |

Options: `codec`, `plugins` and `cloudEvents` (`{ source? }`). The JetStream mock acks with `{ stream: 'mock', seq, duplicate }`, where `duplicate` is `true` for a repeated `Nats-Msg-Id`. `publishAtomic()` of the JetStream mock runs the same checks and outbound plugins as the real one and records the messages of a batch of more than one message when it commits, each with `atomic: { batch, index, count }`. Its ack has `count` and a fake `batch` id (unset for a single message, which is published as usual). It does not check that a stream has `allow_atomic`.

## `NatsTestingModule` and `NatsTestHarness`

`NatsTestingModule.forRoot(options?)` replaces `NatsModule.forRoot()` in a test. It takes the same options except `connection`: `name`, `codec`, `plugins`, `retry`, `dlq`, `idempotency`, `cloudEvents` and so on. It never connects, syncs streams or subscribes. It provides the same injectable clients (`NatsClient` and `JetStreamClient` are the mocks above), `NatsConnectionManager` and `NatsTestHarness`. `NatsStreamManager` and the raw connection are provided too, but JetStream management is not available and subscriptions are refused.

Messages reach your handlers through `NatsTestHarness.dispatch()`. It runs a fake message through the handler's real inbound pipeline: your plugins, retry decisions, dead-letter queue, idempotency, and Nest's guards, pipes, interceptors and exception filters. The harness never waits for a delay and never runs a retry again; it only reports the outcome.

```ts
import assert from 'node:assert/strict';
import { Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Consume, NatsClient, NonRetryableError, Payload } from '@loipv/nestjs-nats';
import { NatsTestHarness, NatsTestingModule } from '@loipv/nestjs-nats/testing';

@Injectable()
class BillingHandlers {
  constructor(private readonly nats: NatsClient) {}

  @Consume({ stream: 'ORDERS', durable: 'billing', dlq: true })
  async bill(@Payload() order: { id: string; total: number }) {
    if (order.total <= 0) throw new NonRetryableError('empty order');
    this.nats.publish('orders.billed', { id: order.id });
    return { billed: true };
  }
}

const app = await Test.createTestingModule({
  imports: [NatsTestingModule.forRoot()],
  providers: [BillingHandlers],
}).compile();
await app.init();
const harness = NatsTestHarness.create(app);

const ok = await harness.dispatch(
  { handler: 'BillingHandlers.bill' },
  { id: 'o1', total: 10 },
  { subject: 'orders.created' },
);
assert.equal(ok.settlement, 'ack');
assert.deepEqual(ok.result, { billed: true });

const bad = await harness.dispatch({ handler: 'BillingHandlers.bill' }, { id: 'o2', total: 0 });
assert.equal(bad.settlement, 'dlq');
assert.equal(bad.dlq[0]?.subject, 'dlq.ORDERS.billing');
```

`dispatch(target, payload, options?)` takes a target and returns a `DispatchResult`:

| Target | Selects |
|---|---|
| `{ handler: 'Class.method' }` | A handler by class and method name. For a CloudEvent router it is `'Class.@CloudEventRouter'` |
| `{ subject: '...' }` | The one handler that receives from that subject. For `@Consume` and `@ConsumePush` the subject is matched against the consumer's `filter_subject`(s), for `@ConsumeOrdered` against its `filterSubjects` |

It rejects with a `NatsConfigError` when the handlers are misconfigured (as bootstrap would report), or when the target matches no handler or several, or names a [disabled](./handlers.md#disabling-a-handler) one. For a batch handler, `payload` is a non-empty array with one message per entry.

| Option | Description |
|---|---|
| `headers` | Message headers |
| `subject` | The message's actual subject. Defaults to the target's subject, else to the handler's own: its subject pattern (which can be a wildcard), its first `filter_subject`, or the stream name for a `@Consume` without a filter. Pass it for realistic values in `@Subject()` and `ctx.subject` |
| `deliveryCount` | JetStream delivery count (default `1`); above `1` the message counts as redelivered. Ignored for core handlers and `@ConsumeOrdered` |
| `attempt` | Core and ordered delivery attempt (default `1`), as after in-process retries (`ctx.attempt` of an ordered handler). Ignored for `@Consume` and `@ConsumePush` handlers |
| `streamSequence` | Stream sequence of a JetStream message (default `1`): `ctx.seq` of `@Consume` and `@ConsumePush`, `ctx.streamSequence` of `@ConsumeOrdered`, and the `Nats-DLQ-Seq` of its dead letters. The messages of a batch get `streamSequence`, `streamSequence + 1`, and so on. It does not increase from one dispatch to the next, so pass it when a test needs distinct sequences across dispatches |
| `request` | Core subscriptions: the message is a request, with a reply subject. Ignored for JetStream handlers |
| `cloudEvent` | Sends the payload as a [CloudEvent](./cloudevents.md), with the same options as publishing |

| `DispatchResult` field | Description |
|---|---|
| `result` | What the handler returned (never set for a batch) |
| `settlement` | What became of the first message: `'ack'`, `'nak'` (a JetStream retry), `'retry'` (a core retry, or an in-place retry of `@ConsumeOrdered`), `'term'`, `'dlq'`, `'drop'`, `'skipped'`, `'replied'` or `'none'` |
| `outcomes` | The [`MessageOutcome`](./plugins.md#observing-outcomes) of every message, aligned with the payloads |
| `nakDelay` | The redelivery delay in ms when `settlement` is `'nak'` |
| `retryDelay` | The delay in ms before the next attempt when `settlement` is `'retry'`, for a core or an ordered handler |
| `reply` | For a request or service endpoint: `{ data }`, or `{ error: { code, description } }` |
| `dlq` | The dead letters published while the messages were processed. A dead letter an outbound plugin dropped is not listed, though the settlement is `'dlq'`. |

Notes:

- `@ConsumePush` handlers settle like `@Consume` handlers (`'ack'`, `'nak'`, `'term'`, `'dlq'`, `'skipped'`). A `@ConsumeOrdered` handler settles like a core one, and nothing is acknowledged: `'none'` for a success, `'retry'` with `retryDelay` for a retryable error (the harness does not wait or run it again; dispatch again with a higher `attempt` to see the next one), `'dlq'` when retries are exhausted or the error is not retryable and a DLQ is on, `'drop'` without a DLQ, and `'skipped'` for an idempotency duplicate. The fake context is a `NatsOrderedContext`.
- Idempotency stores keep their state across the dispatches of one application, so dispatching the same message twice shows a duplicate.
- [Timeouts](./handlers.md#handler-timeout) apply with real timers: a handler that outlives its `timeout` fails the dispatch as it would fail the message. `dispatch()` does not wait for that run, which goes on in the background; with idempotency its key stays in progress until it ends, so dispatching the message again meanwhile finds its key in progress.
- `NatsTestingModule` has no connection, so it never calls a store's `bind`. A store that needs it, such as the [JetStream KV store](./idempotency.md#jetstream-kv-store), stays unbound and every claim fails as a store error. Use an `InMemoryIdempotencyStore` (the default) in tests.
- Dead-letter publishes, and requests sent by your handlers, also appear in the `published` of the mock clients of the testing module.
- A dispatched message is a fake message, so `ctx.raw` is a stand-in that records what you call on it. It is not a real `Msg` or `JsMsg`.
- The fake JetStream accepts every publish: there are no streams, `expect` and `timeout` are ignored, and only a repeated `Nats-Msg-Id` is reported as a `duplicate`. In particular a dead letter of a handler with a JetStream-mode DLQ is published successfully in the harness even when no stream would capture its subject.
- Await each `dispatch()` before starting the next one. The `dlq` of a result lists the dead letters published while that dispatch ran, so concurrent dispatches can see each other's.
