# Connections

[Back to the README](../README.md)

## Options and authentication

`connection` is passed unchanged to `connect()` from `@nats-io/transport-node`, so every client option works: servers, reconnect policy, user/password, token, NKey, JWT/`.creds`, `multiAuthenticator`, TLS and mTLS.

```ts
import { readFileSync } from 'node:fs';
import { credsAuthenticator } from '@nats-io/transport-node';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { NatsModule } from '@loipv/nestjs-nats';

NatsModule.forRootAsync({
  imports: [ConfigModule],
  inject: [ConfigService],
  useFactory: (config: ConfigService) => ({
    connection: {
      servers: config.getOrThrow<string>('NATS_URL'),
      name: 'orders-service', // client name shown in server monitoring
      authenticator: credsAuthenticator(readFileSync(config.getOrThrow<string>('NATS_CREDS'))),
      tls: { caFile: config.get<string>('NATS_CA') },
    },
  }),
});
```

`forRootAsync` also accepts `useClass` and `useExisting`, given a class that implements `NatsOptionsFactory`.

- **Fail fast.** If the first connection attempt fails, for example because the server is unreachable or the credentials are wrong, bootstrap throws a `NatsConnectionError` instead of retrying forever. Once connected, reconnects follow the NATS client's normal policy.
- **No secrets in output.** Error messages and logs never include passwords, tokens, seeds or creds, and credentials embedded in server URLs are replaced with `***`.
- **Rotating credentials.** An authenticator is called again on every reconnect. To pick up a rotated `.creds` file, pass a function that re-reads the file, instead of `credsAuthenticator(bytes)`, which reads it once.

## Logging

The library logs through Nest's `Logger`, with the context `NatsConnection:<name>` (for example `NatsConnection:default`), so the application's logger configuration applies. The `logger` option of `forRoot` changes that per connection: `false` silences this library's logs for the connection, and a `LoggerService` receives them instead of Nest's `Logger`. Handler errors are logged by the library only, not by Nest's `ExceptionsHandler`, so `false` silences them too.

```ts
NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  logger: false, // or a LoggerService, such as a pino-based one
});
```

## Several connections

Give each connection a `name`. Handlers and injected clients choose a connection by that name; when no name is given, `'default'` is used.

```ts
import { Injectable, Module } from '@nestjs/common';
import { InjectNatsClient, NatsClient, NatsModule, Payload, Subscribe } from '@loipv/nestjs-nats';

@Injectable()
class AuditForwarder {
  constructor(@InjectNatsClient('audit') private readonly audit: NatsClient) {}

  @Subscribe('orders.*', { connection: 'default' })
  forward(@Payload() order: unknown) {
    this.audit.publish('audit.orders', order);
  }
}

@Module({
  imports: [
    NatsModule.forRoot({ connection: { servers: 'nats://orders:4222' } }),
    NatsModule.forRoot({ name: 'audit', connection: { servers: 'nats://audit:4222' } }),
  ],
  providers: [AuditForwarder],
})
class AppModule {}
```

Inject per connection with `@InjectNatsClient(name?)`, `@InjectJetStream(name?)`, `@InjectStreamManager(name?)` and `@InjectNatsConnection(name?)`. The last one gives you the raw `NatsConnection`. For the `'default'` connection, `NatsClient`, `JetStreamClient` and `NatsStreamManager` can also be injected by class (`constructor(private readonly client: NatsClient)`, `app.get(NatsClient)`). For a named connection outside a constructor, use the token functions: `getNatsClientToken(name?)`, `getJetStreamClientToken(name?)`, `getStreamManagerToken(name?)` and `getConnectionToken(name?)`.

Registering two connections with the same name throws a `NatsConfigError`.

## Restarting handlers

A handler's intake (a core subscription, a pull, push or ordered consumer, or a NATS service) can end while the application is running: the permissions on its subject or on the reply inbox were revoked, the server rejected a pull request, the client could not recreate an ordered consumer, or a subscription of a service failed and stopped the service. The library then logs an error and opens the intake again, with an exponential backoff. The `restart` option of `forRoot` sets the backoff for every handler of the connection:

```ts
NatsModule.forRoot({
  connection: { servers: 'nats://localhost:4222' },
  restart: { initialDelay: 500, maxAttempts: 10 }, // or `false`
});
```

| Option | Default | Description |
|---|---|---|
| `initialDelay` | `1000` | Delay before the first attempt, in ms. At most `maxDelay` |
| `factor` | `2` | Multiplier of the delay for each further attempt (`>= 1`) |
| `maxDelay` | `30000` | Upper bound of the delay in ms, before jitter, and the time a reopened intake must run before the attempts count from 1 again. At most `2147483647` |
| `jitter` | `0.2` | Random spread of each delay: `0.2` means plus or minus 20 % (between `0` and `1`) |
| `maxAttempts` | `Infinity` | Attempts in a row before giving up: an integer `>= 1`, or `Infinity` to never give up |

An invalid value fails bootstrap with a `NatsConfigError` that names the connection. Attempt `n` waits `min(maxDelay, initialDelay * factor^(n - 1))`, with the jitter applied after the cap, as for [retries](./retry-and-dlq.md#retry-options).

- **What counts as an attempt.** An attempt fails when the intake cannot be opened, for example because the consumer does not exist or JetStream does not answer, and also when the reopened intake ends during its reopen or right after it. The count is reset only once a reopened intake has run for `maxDelay` ms, so an intake that opens and dies at once, like a subscription to a denied subject, still reaches `maxAttempts`.
- **What is kept.** A pull or push consumer declared with `config` is created again if it was deleted, following the [sync mode](./streams.md). JetStream messages the handler already holds keep their heartbeat and are acknowledged as usual, and pending `@Subscribe` retries still run. An ordered consumer resumes after the last message it handled (see [Consumers](./consumers.md#ordered-consumeordered)). A NATS service is recreated as a whole, with all its endpoints.
- **Startup is not retried.** The first open happens during bootstrap, and a failure there still fails bootstrap.
- **Giving up.** After `maxAttempts` attempts, the handler is `failed` and receives nothing more. Messages it held but had not started are handed back: JetStream messages are `nak`'d, and core messages are dropped with a warning. Use the [health indicator](./health.md#handlers) with `handlers: true` to notice it.
- **Closed connection.** A connection that is closed for good is never reopened by the client, so a handler whose intake ends then, or whose restart comes due then, is `failed` at once. A connection that is reconnecting needs nothing: core subscriptions survive a reconnect, and consumers recover on their own.
- **`restart: false`** keeps the behavior of earlier versions: the intake is not reopened, the error says that no more messages will be received, and the handler is `failed`.

The restarts are logged through the connection's [logger](#logging), with the handler named as in the [health indicator](./health.md#handlers), for example `Orders.onCreated on "ORDERS/orders-worker"`:

| Level | Message |
|---|---|
| error | The intake ended, as before: `Subscription of <label> failed`, `Consumer of <label> ended unexpectedly`, `NATS service "<name>" stopped: <error>` and the like. The tail `; no more messages will be received` (`requests` for a service) is added only when no restart follows |
| warn | `Restarting <label> in <ms> ms (attempt <n>)`, or `(attempt <n>/<max>)` when `maxAttempts` is finite |
| warn | `Restart of <label> failed (attempt <n>): <error>` |
| log | `Restarted <label> (attempt <n>)` |
| error | `Gave up restarting <label> after <n> attempts; no more messages will be received` |
| error | `Gave up restarting <label>: the connection is closed` |

Warnings about the consumer's configuration, such as `max_deliver` or `batch.maxWait`, are logged on the first open only. Shutdown cancels a pending restart, and closes an intake whose reopen finishes after shutdown started.
