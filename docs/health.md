# Health checks

[Back to the README](../README.md)

`@loipv/nestjs-nats/health` provides a [Terminus](https://docs.nestjs.com/recipes/terminus) health indicator. It needs the `@nestjs/terminus` peer dependency (`11.x` or `12.x`). Import `NatsHealthModule` next to `NatsModule` and `TerminusModule`, and call the indicator from a health check:

```ts
import { Controller, Get, Module } from '@nestjs/common';
import { HealthCheck, HealthCheckService, TerminusModule } from '@nestjs/terminus';
import { NatsModule } from '@loipv/nestjs-nats';
import { NatsHealthIndicator, NatsHealthModule } from '@loipv/nestjs-nats/health';

@Controller('health')
class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly nats: NatsHealthIndicator,
  ) {}

  @Get()
  @HealthCheck()
  check() {
    return this.health.check([() => this.nats.isHealthy('nats', { jetstream: true })]);
  }
}

@Module({
  imports: [
    NatsModule.forRoot({ connection: { servers: 'nats://localhost:4222' } }),
    TerminusModule,
    NatsHealthModule,
  ],
  controllers: [HealthController],
})
class AppModule {}
```

`isHealthy(key, options?)` accepts:

| Option | Default | Description |
|---|---|---|
| `connection` | `'default'` | Name of the connection to check |
| `rttTimeout` | `1000` | Longest wait, in ms, for the round trip to the server and, separately, for the JetStream check. A finite number above `0`, at most `2147483647` (2^31 - 1) |
| `jetstream` | `false` | Also require JetStream to answer an account info request |

When the connection is up, the result is `{ nats: { status: 'up', connection, server, rtt } }`, where `rtt` is the round trip time in ms. Otherwise it is `{ nats: { status: 'down', connection, reason } }`, and `HealthCheckService` turns that into a `503` response. `reason` is one of:

- `unknown connection`
- `reconnecting`, `draining` or `closed`: the state of the connection
- `rtt timeout after <n>ms` or `rtt failed: <error>`
- `jetstream unavailable: <error>` (also for a timeout)
- `check failed: <error>`

The indicator never throws for an unhealthy connection; only an invalid `rttTimeout` (a programming error) rejects, with a `NatsConfigError`. The result holds no credentials: the server URL is redacted and error text is scrubbed of the connection's password and token. The check can wait up to `rttTimeout` for the round trip plus up to `rttTimeout` for the JetStream check, so up to twice `rttTimeout` in the worst case. Set your probe's timeout above that.

The indicator checks the connection (and JetStream), not individual handlers. If a consumer loop or a service endpoint ends unexpectedly, the library logs an error but does not restart it, and the indicator keeps reporting `up`: restart the process, for example from a liveness probe of your own.
