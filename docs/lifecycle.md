# Lifecycle and graceful shutdown

[Back to the README](../README.md)

1. **Module init:** connect, failing fast if this does not work, then sync declared streams and consumers.
2. **Application bootstrap:** discover handlers, validate them, and start subscriptions, consumers and services. If any step of module init or bootstrap fails, the library shuts down as described below (what was already started is stopped, connections and idempotency stores are closed) before the error is rethrown.
3. **Shutdown:** see below.

Call `app.enableShutdownHooks()` so this runs on `SIGTERM` and `SIGINT`.

**Startup failures outside the library.** Connections are opened while Nest creates the providers. If the library's own providers fail there (a connection fails, its name is already taken, or a `forRootAsync` factory throws), the connections already open are closed before the error is rethrown. Each gets 1 second to drain and is then closed without draining (abandoned after 1 more second), so the error is rethrown within about 2 seconds. A connection that is still connecting is not waited for: it keeps trying (with `waitOnFirstConnect: true`, until its server answers) and is closed as soon as it opens. When one of your providers throws during that phase, Nest runs no hook, so the connections opened before it stay open. With `NestFactory.create()`'s default `abortOnError: true` the process exits anyway. With `abortOnError: false`, or with `Test.createTestingModule().compile()`, there is no application to close. The process then exits on its own only if nothing else keeps it alive: connections left open, a connection still retrying, or your own timers and sockets all do. If another provider's `onModuleInit` or `onApplicationBootstrap` throws, call `app.close()` (or `moduleRef.close()`) after the failed `init()`: that runs the shutdown below and closes the connections.

Shutdown has bounded phases. `shutdown.drainTimeout` (default `10000` ms) is the longest any phase waits. If connections have different values, the largest one is used.

```ts
NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  shutdown: { drainTimeout: 15_000 },
});
```

1. **Stop intake.** Subscriptions and consumers stop receiving messages. An ordered consumer also abandons a retry wait in progress (logged as a warning). A batch that is being collected is handed to its handler.
2. **Wait for in-flight handlers**, including runs that outlived their [`timeout`](./handlers.md#handler-timeout) and are still going on. Phases 1 and 2 together wait for up to `drainTimeout`.
3. **Force stop.** JetStream messages that are being processed, collected into a batch, waiting for a batch slot, or queued for a free handler slot or for a batch (as many as a push consumer has received) are `nak`'d so the server redelivers them. Pending in-process retries are cancelled, and core messages that were collected or queued but never started are dropped. When something was forced, a warning per connection summarises the `nak`'d messages and cancelled retries, and a core handler may log a further warning about the dropped messages.
4. **Close connections.** Each connection runs its close hooks (open `batcher()`s flush here, and `NatsClient.publish()` messages still in the outbound plugins are sent), then drains. If this takes longer than `drainTimeout`, the connection is closed without draining and a warning is logged. This also happens when the server is unreachable. A forced close that does not finish within 1 second is abandoned with a warning.
5. **Close idempotency stores.** The `close()` of each store is called, within what is left of the bound of phase 4, but never less than 1 second (or `drainTimeout`, if that is shorter). A store that has not closed by then is abandoned with a warning.

**Ordered consumers** acknowledge nothing, so there is nothing to `nak`. The message being handled gets the wait for in-flight handlers like any other. A message still unfinished at the force stop, one waiting for its retry, and the messages the client had read ahead are not redelivered. They come back only if the next start replays them: with `deliverPolicy: 'all'`, or a `startSeq` or `startTime` that covers them. With `'new'` or `'last'` they are skipped. See [Pull, push and ordered consumers](./consumers.md#ordered-consumeordered).

In the worst case, shutdown takes about `2 × drainTimeout + 2` seconds: up to `drainTimeout` for phases 1 and 2, up to `drainTimeout` plus 1 second for a forced close in phase 4, and up to 1 second more for phase 5. No step waits forever.

Shutdown runs in Nest's `beforeApplicationShutdown` hook, and Nest calls every provider's `onModuleDestroy` before that. Handlers therefore keep receiving and processing messages after your other providers' `onModuleDestroy` ran. If a handler uses a resource, such as a database client, close that resource in `onApplicationShutdown`, which runs after the library has stopped intake and waited for in-flight handlers.

Handlers are singletons. Handlers in `REQUEST`- or `TRANSIENT`-scoped providers are rejected at startup.

`NatsConnectionManager` reports on the module's connections; opening and closing them is left to the module. It exposes `names()` (the registered connection names), `has(name)`, `status(name?)` (`connected`, `reconnecting`, `draining` or `closed`, with the `server` while connected) and `status$(name?)`, an Observable of connection events such as disconnect, reconnect and lame-duck mode.
