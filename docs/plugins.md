# Plugins

[Back to the README](../README.md)

A plugin can wrap every inbound delivery and every outbound message of a connection, in the style of Koa middleware:

```ts
import { NatsModule, type NatsPlugin } from '@loipv/nestjs-nats';

const timing: NatsPlugin = {
  name: 'timing',
  inbound: {
    stage: 'outer', // 'outer' runs before settlement; 'inner' (default) runs closest to the handler
    async handle(delivery, next) {
      const started = performance.now();
      try {
        await next();
      } finally {
        const { className, methodName } = delivery.handler;
        delivery.logger.log(`${className}.${methodName} took ${performance.now() - started} ms`);
      }
    },
  },
  outbound: {
    async handle(message, next) {
      message.headers.set('x-sent-at', new Date().toISOString());
      await next();
    },
  },
};

NatsModule.forRoot({ connection: { servers: 'nats://localhost:4222' }, plugins: [timing] });
```

The inbound order is: `outer` plugins, then acknowledgement handling, then `inner` plugins, then decoding and the handler. `delivery.handler` is a read-only `HandlerInfo` that describes the handler (`kind`, `connection`, `className`, `methodName`, plus `subject`/`queue`, `stream`/`durable` (and `delivery`, `'pull'` or `'push'`), `stream`/`filterSubjects` for an ordered consumer, or `service`/`endpoint`, depending on `kind`). Per-handler logic is better written as a regular Nest interceptor.

Outbound plugins also see the messages of [`publishAtomic()`](./publishing.md#atomic-batches): `message.atomic` is `{ index, count }` there (and `undefined` otherwise), and `next()` settles when the whole batch is committed.

## Outbound timing

An outbound plugin must pass the message on (call `next()`) within the timeout of the call: the `timeout` option of `JetStreamClient.publish()` (and of the items of `publishMany()` and `batcher()`; default the JetStream client's timeout) or of `NatsClient.request()` (default 5000 ms). If it does not, the call rejects with a `NatsModuleError` ("the outbound plugins did not pass the message on within … ms") and nothing is sent: a `next()` called later rejects with that error. The same holds once a plugin threw or returned without calling `next()`: a later `next()` sends nothing. `NatsClient.publish()` gives the plugins 5000 ms; a message not passed on by then is dropped, an error is logged, and `flush()` stops waiting for it.

Once the message is sent, the call settles with the outcome of the send (the `PubAck`, the reply, or the send's own error such as a `NatsRequestError`) without waiting for the plugins to return, so a plugin that never settles after `next()` does not hold the caller. A plugin that throws after that is logged as an error on the connection's logger and does not change the result; rethrowing the error `next()` rejected with is not logged. A plugin that throws before calling `next()` makes the call reject with that error (`NatsClient.publish()` logs it). [`publishAtomic()`](./publishing.md#atomic-batches) applies the same rule to a whole batch, and rejects with a `NatsAtomicPublishError` where these calls reject with a `NatsModuleError`.

## Dead letters

Every dead letter the library publishes (see [Retry and DLQ](./retry-and-dlq.md)) goes through the connection's outbound plugins, like any other publish. Its `OutgoingMessage` has `kind` `'jetstream'` in `jetstream` DLQ mode and `'publish'` in `core` mode, `data` is the original body as a `Uint8Array` (the raw bytes), and a value a plugin puts in its place is encoded with the codec. `headers` are the DLQ headers, and `deadLetter` is set to `{ subject, handler, error }`: the subject the failed message arrived on, the `HandlerInfo` of the handler that failed and the error. There is no option to opt out, so a plugin that must not touch dead letters checks `message.deadLetter`.

```ts
import type { NatsPlugin } from '@loipv/nestjs-nats';

const tagDeadLetters: NatsPlugin = {
  name: 'tag-dead-letters',
  outbound: {
    async handle(message, next) {
      if (message.deadLetter) {
        message.headers.set('x-failed-handler', message.deadLetter.handler.methodName);
      }
      await next();
    },
  },
};
```

- **Call `next()`.** A plugin that returns without calling it drops the dead letter: nothing is published, but the failed message counts as dead-lettered. Its outcome is `'dlq'`, a JetStream message is `term`'d, and a warning is logged. Drop one only on purpose.
- **Timeout.** The plugins get the JetStream client's timeout in `jetstream` mode and 5000 ms in `core` mode. A plugin that throws before `next()`, or does not pass the dead letter on in time, makes the DLQ publish fail: the existing failure behavior applies (a `@Consume` or `@ConsumePush` message is `nak`'d with the backoff delay, a `@ConsumeOrdered` message is retried in place without limit, a `@Subscribe` message is logged and dropped).
- **`Nats-Msg-Id`.** The DLQ stream de-duplicates on it; a dead letter of a core message has none. A plugin that changes or deletes `Nats-Msg-Id` on a dead letter loses that de-duplication.

## Observing outcomes

`delivery.outcomes` is a `Map` from each message of the delivery (`delivery.messages`) to a `MessageOutcome` that says what became of it. The settlement and idempotency middlewares fill it in, so it is complete once the `next()` of an `outer` plugin has resolved or rejected. Read it there, after awaiting `next()`:

```ts
import type { NatsPlugin } from '@loipv/nestjs-nats';

const deadLetterAlerts: NatsPlugin = {
  name: 'dead-letter-alerts',
  inbound: {
    stage: 'outer',
    async handle(delivery, next) {
      try {
        await next();
      } finally {
        for (const [message, outcome] of delivery.outcomes) {
          if (outcome.action === 'dlq') {
            delivery.logger.warn(`${message.subject} dead-lettered to ${outcome.subject}`);
          }
        }
      }
    },
  },
};
```

| `action` | Meaning | Other fields |
|---|---|---|
| `'ack'` | Acknowledged: automatically after success, or by the handler itself | `error?`: set when the handler acked and then threw |
| `'retry'` | Handed back for another attempt: a JetStream `nak(delay)`, or an in-process retry of a core message | `delayMs`, `error` |
| `'dlq'` | Failed for good and published to the dead-letter subject | `subject`, `error` |
| `'term'` | A JetStream message terminated without a dead letter | `error?` |
| `'drop'` | A core message given up on without a dead letter | `error?` |
| `'skipped'` | Left out of the handler: a duplicate, or being processed by another delivery | `reason`: `'duplicate'` or `'in-progress'` |
| `'replied'` | A request or service endpoint: the adapter sends the reply (an error reply when `error` is set) | `error?` |
| `'none'` | Nothing to settle: a success on a core subscription, or a manually acknowledged message left unacknowledged | |

An outcome records the decision, not the result of the call: if the underlying `ack`, `nak` or `term` throws, the failure is logged and the outcome is recorded as decided.

A message with no entry was not settled by the library, for example because an `outer` plugin threw before calling `next()`.
