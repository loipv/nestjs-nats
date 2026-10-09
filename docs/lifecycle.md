# Lifecycle and graceful shutdown

[Back to the README](../README.md)

1. **Module init:** connect, failing fast if this does not work, then sync declared streams and consumers.
2. **Application bootstrap:** discover handlers, validate them, and start subscriptions, consumers and services. If any step fails, everything already started is stopped and all connections are closed before the error is rethrown.
3. **Shutdown:** see below.

Call `app.enableShutdownHooks()` so this runs on `SIGTERM` and `SIGINT`.

Shutdown has bounded phases. `shutdown.drainTimeout` (default `10000` ms) is the longest any phase waits. If connections have different values, the largest one is used.

```ts
NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  shutdown: { drainTimeout: 15_000 },
});
```

1. **Stop intake.** Subscriptions and consumers stop receiving messages. A batch that is being collected is handed to its handler.
2. **Wait for in-flight handlers.** Phases 1 and 2 together wait for up to `drainTimeout`.
3. **Force stop.** JetStream messages that are being processed, collected into a batch, or waiting for a batch slot are `nak`'d so the server redelivers them. Pending in-process retries are cancelled, and collected core messages that never started are dropped. When something was forced, a warning per connection summarises the `nak`'d messages and cancelled retries, and a core handler may log a further warning about the dropped messages.
4. **Close connections.** Each connection runs its close hooks (open `batcher()`s flush here), then drains. If this takes longer than `drainTimeout`, the connection is closed without draining and a warning is logged. This also happens when the server is unreachable.
5. **Close idempotency stores.** The `close()` of each store is called, within the same bound.

In the worst case, shutdown takes about `2 × drainTimeout`: up to `drainTimeout` for phases 1 and 2, and up to `drainTimeout` more for phases 4 and 5. No step waits forever.

Shutdown runs in Nest's `beforeApplicationShutdown` hook, and Nest calls every provider's `onModuleDestroy` before that. Handlers therefore keep receiving and processing messages after your other providers' `onModuleDestroy` ran. If a handler uses a resource, such as a database client, close that resource in `onApplicationShutdown`, which runs after the library has stopped intake and waited for in-flight handlers.

Handlers are singletons. Handlers in `REQUEST`- or `TRANSIENT`-scoped providers are rejected at startup.

`NatsConnectionManager` exposes `status(name?)` (`connected`, `reconnecting`, `draining` or `closed`) and `status$(name?)`, an Observable of connection events such as disconnect, reconnect and lame-duck mode.
