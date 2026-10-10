# Orders example

A small NestJS application that uses [`@loipv/nestjs-nats`](../../README.md) end to end. It runs against a local NATS server with JetStream.

| File | Shows |
|---|---|
| `src/app.module.ts` | `NatsModule.forRoot()` with the `ORDERS` (`orders.>`) and `DLQ` (`dlq.>`) streams, CloudEvents defaults and the `tracing()` plugin |
| `src/orders.handlers.ts` | `@Subscribe` request/reply, and a `@Consume` consumer (`billing`) with retry and a dead-letter queue |
| `src/events.router.ts` | `@CloudEventRouter` on `events.>` with two `@OnCloudEvent` methods |
| `src/health.controller.ts` | `GET /health` with the Terminus `NatsHealthIndicator` |
| `src/tracing.ts` | OpenTelemetry set up with `BasicTracerProvider` and a `ConsoleSpanExporter` |
| `src/main.ts` | Graceful shutdown (`enableShutdownHooks()`) and sample messages published at startup |
| `test/orders.handlers.spec.ts` | Tests with `NatsTestingModule` and `NatsTestHarness`, which need no NATS server |

## Run it

The example installs the library from the repository root (`"@loipv/nestjs-nats": "file:../.."`), so build the library first. From the repository root:

```bash
npm install
npm run build
```

Then, from this directory:

```bash
docker compose up -d      # NATS 2.12 with JetStream on :4222, monitoring on :8222
npm install
npm start                 # builds, then starts the app on :3000
curl localhost:3000/health
```

`npm start` publishes sample messages as it starts, so the log shows:

- `order o-1-<timestamp>` billed by `OrdersHandlers.bill`,
- `order o-2-<timestamp>` failing on purpose: three retries with backoff (about 0.5 s, 1 s and 2 s), then `sent to DLQ dlq.ORDERS.billing after 4 attempts`,
- two CloudEvents routed by `EventsRouter` on their `type`,
- a `query.orders.get` request answered with the billed order,
- OpenTelemetry spans (`publish ...`, `process ...`) printed to the console.

The order ids and their `msgId`s carry a timestamp, so you can run `npm start` again at once: fixed ids would be de-duplicated by the `ORDERS` stream within its duplicate window (2 minutes by default), and the second run would show no billing or dead letter.

`/health` answers `{"status":"ok", ...}` while the connection and JetStream are up. Stop the app with `Ctrl+C`: it stops intake, finishes in-flight messages and drains the connection. Stop NATS with `docker compose down`.

Environment variables:

| Variable | Default | |
|---|---|---|
| `NATS_URL` | `nats://localhost:4222` | NATS server to connect to |
| `PORT` | `3000` | HTTP port |

If `4222`, `8222` or `3000` are taken on your machine, change the left side of the port mappings in `docker-compose.yml` and set `NATS_URL` and `PORT` to match, for example `NATS_URL=nats://localhost:4223 PORT=3001 npm start`.

To look at the dead letter, open `http://localhost:8222/jsz?streams=true`: the `DLQ` stream holds one dead letter per run of `npm start`.

## Test it

```bash
npm test
```

The tests run messages through the real handlers with `NatsTestHarness`, without a NATS server: a good order is acked, a failing order is retried and, at its fourth attempt, dead-lettered, and a CloudEvent reaches the method that matches its type.

## After changing the library

`npm install` copies the built library into `node_modules` (see `.npmrc`) and does not refresh it by itself. After rebuilding the library, run `rm -rf node_modules/@loipv && npm install` here. From the repository root, `npm run example:check` does all of this: it builds the library, reinstalls it here, typechecks the example and runs its tests.
