# Tests

| Folder | What | Run |
|---|---|---|
| `unit/` | Tests without a NATS server | `npm test` |
| `integration/` | Tests against a real NATS server, started in Docker by testcontainers | `npm run test:int` |
| `smoke/` | The packed package loaded from CommonJS, ESM and without the optional peers | `npm run test:smoke` |
| `support/` | Helpers shared by unit tests | |
| `setup/` | Vitest setup files | |

## Unit test layout

`unit/` mirrors `src/`: the tests of `src/<dir>/<file>.ts` are in `unit/<dir>/<file>.spec.ts`.

- **One module, one spec.** A spec tests the module it is named after. A test of another module goes in that module's spec.
- **A large module can have several specs**, named `<file>.<aspect>.spec.ts`, such as `discovery/handler-explorer.enhancers.spec.ts`. Each one still tests only `<file>.ts`.
- **`unit/features/`** holds the tests of behaviour that spans several modules and belongs to none of them: batching end to end, the CloudEvent router, disabled handlers, message outcomes, outbound timing, CloudEvents on publish, and the public API of the entry points.
- **No spec** for files that hold only types, re-exports (`index.ts`), or an empty Nest `@Module` class.

## Helpers

Put a helper in `support/` once a second spec needs the same one. Fixtures that only one spec uses, such as its `setup()` or fake messages, stay in that spec.

| File | Exports |
|---|---|
| `support/async.ts` | `tick()`: waits one `setImmediate`. `never()`: a promise that never settles |
| `support/logger.ts` | `mockLogger()`: a `LoggerService` of `vi.fn()` mocks |
| `support/deliveries.ts` | Inbound deliveries and handler infos for testing inbound plugins |
| `support/otel.ts` | In-memory OpenTelemetry tracer and meter providers |

Integration tests have their own helpers in `integration/support/`.
