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
| `handlers` | `false` | Also report the connection's message handlers: `down` when one gave up restarting. See [Handlers](#handlers) |

When the connection is up, the result is `{ nats: { status: 'up', connection, server, rtt } }`, where `rtt` is the round trip time in ms. Otherwise it is `{ nats: { status: 'down', connection, reason } }`, and `HealthCheckService` turns that into a `503` response. `reason` is one of:

- `unknown connection`
- `reconnecting`, `draining` or `closed`: the state of the connection
- `rtt timeout after <n>ms` or `rtt failed: <error>`
- `jetstream unavailable: <error>` (also for a timeout)
- `handler failed: <label>`, with `handlers: true`
- `check failed: <error>`

The indicator never throws for an unhealthy connection; only an invalid `rttTimeout` (a programming error) rejects, with a `NatsConfigError`. The result holds no credentials: the server URL is redacted and error text is scrubbed of the connection's password and token. The check can wait up to `rttTimeout` for the round trip plus up to `rttTimeout` for the JetStream check, so up to twice `rttTimeout` in the worst case. Set your probe's timeout above that.

## Handlers

A subscription, consumer or service whose loop ends on its own is restarted with a backoff, and a handler that gives up is reported as `failed`. See [Restarting handlers](./connections.md#restarting-handlers). Without `handlers`, the indicator checks the connection (and JetStream) only, as described above. With `handlers: true`, it checks the handlers of the connection last, once the connection, the round trip and JetStream (if asked) are fine:

- When at least one handler has `failed`, the result is `down`, and the details list the handlers that failed and the ones restarting:

  ```json
  {
    "nats": {
      "status": "down",
      "connection": "default",
      "reason": "handler failed: Orders.onCreated on \"ORDERS/orders-worker\", +1 more",
      "handlers": {
        "failed": ["Orders.onCreated on \"ORDERS/orders-worker\"", "service \"inventory\""],
        "restarting": []
      }
    }
  }
  ```

  The reason names the first handler that failed and ends with `, +<n> more` when others failed too.
- Otherwise the result is `up` as above. While some handlers are restarting, it also lists them: `{ status: 'up', connection, server, rtt, handlers: { restarting: ['Audit.onEvent on "audit.*"'] } }`. A restarting handler does not make the check `down`: it may come back on its own.

A handler is named by its class, method and source: `Class.method on "<subject>"` for `@Subscribe`, `on "<stream>/<durable>"` for `@Consume` (followed by ` (push)` for `@ConsumePush`), `on "<stream>" (ordered)` for `@ConsumeOrdered`, and `service "<name>"` for a `@NatsService`, which is restarted as a whole.

A `failed` handler stays failed until the process restarts, which is a job for a liveness probe. A readiness probe that fails only takes the instance out of rotation and does not bring the handler back, so keep `handlers` out of it, or use it there only when the instance has nothing to serve without its handlers:

```ts
import { Controller, Get } from '@nestjs/common';
import { HealthCheck, HealthCheckService } from '@nestjs/terminus';
import { NatsHealthIndicator } from '@loipv/nestjs-nats/health';

@Controller('health')
class ProbesController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly nats: NatsHealthIndicator,
  ) {}

  @Get('ready')
  @HealthCheck()
  ready() {
    return this.health.check([() => this.nats.isHealthy('nats', { jetstream: true })]);
  }

  @Get('live')
  @HealthCheck()
  live() {
    return this.health.check([() => this.nats.isHealthy('nats', { handlers: true })]);
  }
}
```

A liveness probe that also fails on the connection state restarts the process during a long reconnect too. If that is not wanted, give the probe a `failureThreshold` that outlasts the reconnects you expect.
