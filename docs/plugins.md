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
