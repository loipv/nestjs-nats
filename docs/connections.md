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
