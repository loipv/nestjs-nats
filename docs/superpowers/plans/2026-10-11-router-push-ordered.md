# CloudEventRouter on Push and Ordered Consumers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `@CloudEventRouter` receive from a durable push consumer (`consumePush`) or an ordered consumer (`consumeOrdered`), behaving like `@ConsumePush` and `@ConsumeOrdered`.

**Architecture:** Only the router's options type and `defineRouter` in `src/discovery/handler-explorer.ts` change. They build the same `HandlerDefinition` shapes that `@ConsumePush` and `@ConsumeOrdered` produce. Adapters, settlement, validators and the harness already handle these shapes, including unmatched CloudEvents on ordered consumers.

**Tech Stack:** TypeScript (ESM, NodeNext), NestJS 11/12, nats.js v3, Vitest (`unit` and `integration` projects), testcontainers with `nats:2.12-alpine`.

**Spec:** `docs/superpowers/specs/2026-10-11-router-push-ordered-design.md`

## Global Constraints

- A router sets exactly one of `subscribe`, `consume`, `consumePush` and `consumeOrdered`. Otherwise it fails with `NatsConfigError` `@CloudEventRouter <Class> must set exactly one of subscribe, consume, consumePush or consumeOrdered`.
- `consumePush` → `{ kind: 'consume', delivery: 'push', options: { ...common, stream, durable, config?, deliverGroup? } }`.
- `consumeOrdered` → `{ kind: 'ordered', options: { ...common, stream, filterSubjects?, deliverPolicy?, headersOnly? } }`.
- `common` is the router's `connection`, `concurrency`, `codec`, `retry`, `dlq` and `idempotency`, with `undefined` values dropped, as today.
- `concurrency` together with `consumeOrdered` fails with `NatsConfigError` `@CloudEventRouter <Class> with consumeOrdered does not support concurrency`. This is checked in `defineRouter`.
- The existing validators handle the rest: `validateDeliverGroup`, `validateOrdered`, and the retry/DLQ/idempotency/codec checks.
- No changes to adapters, settlement, validators or the harness. A test that fails because one of them needs a change is a finding to report, not something to patch silently.
- Integration env:

  ```
  DOCKER_HOST=unix:///Users/hungvv/.colima/default/docker.sock
  TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
  TESTCONTAINERS_RYUK_DISABLED=true
  ```

- Verification before each commit: `npm test`, `npm run typecheck`, `npm run lint`. The last task also runs `npm run test:int` and `npm run docs:check`.
- Commits end with `Claude-Session: https://claude.ai/code/session_016DmB9sNwwcck4F6EDJBSom`.

## Review Focus

1. **`consumeOrdered` with `idempotency: true`.** It works as for `@ConsumeOrdered`: it needs a store per instance, and nothing new is required. Covered by the existing validators; Task 1 adds no test.
2. **A push router without `deliverGroup` on two instances.** It fails as `@ConsumePush` does ("already bound"). This is adapter behavior and is not re-tested.
3. **An unmatched CloudEvent on an ordered router with `unmatched: 'dlq'` but no DLQ.** The router warns and drops the message. Tested in Task 1 through the harness.
4. **`consumeOrdered` with `headersOnly: true`.** A structured CloudEvent then has no body to read its attributes from, so it is unmatched or fails to decode. This is documented in Task 2, not tested.
5. **A router declaring `consume: undefined` plus `consumePush`.** It counts as one source: an `undefined` key is not set. Tested in Task 1.

---

### Task 1: Router options and `defineRouter`

**Files:**
- Modify:
  - `src/decorators/cloudevent.decorators.ts`: the spec §3 type, with `OrderedDeliverPolicy` imported from `./handler.decorators.js`, and doc comments that name all four sources.
  - `src/discovery/handler-explorer.ts` (`defineRouter`, ~lines 149-237).
- Test: `test/unit/cloudevent-router.spec.ts`, in `describe('@CloudEventRouter discovery')` and `describe('@CloudEventRouter dispatch')` (harness), following the existing setups there.

**Interfaces:**
- Produces: the `CloudEventRouterOptions` variants `{ consumePush: {...} }` and `{ consumeOrdered: {...} }`.

- [ ] **Step 1: Write the failing tests.**
  - Discovery:
    - `'builds a push consumer definition from consumePush'`: `{ kind: 'consume', delivery: 'push', options: { stream: 'S', durable: 'd', deliverGroup: 'g', config: { ack_wait: 1 } } }`, with `methodName: '@CloudEventRouter'`.
    - `'builds an ordered consumer definition from consumeOrdered'`: `{ kind: 'ordered', options: { stream: 'S', filterSubjects: 'a.>', deliverPolicy: 'new', headersOnly: true } }`.
    - `it.each` over a router with `subscribe` and `consumePush`, a router with `consumePush` and `consumeOrdered`, and a router with no source. Each throws the exact "must set exactly one of subscribe, consume, consumePush or consumeOrdered" message.
    - `'counts a source set to undefined as unset'` (Review Focus 5): `{ consume: undefined, consumePush: {...} }` builds the push definition.
    - `'rejects concurrency with consumeOrdered'`: the exact message.
    - `'rejects a deliverGroup that conflicts with config.deliver_group'`: run `validateHandlers` on the definition and expect the existing validator's `NatsConfigError`.
  - Dispatch (harness):
    - A push router: a matched event gives settlement `'ack'` and calls the route; with `unmatched: 'term'`, an unmatched event gives `'term'`.
    - An ordered router: a matched event gives `'none'`; `unmatched: 'term'` gives `'drop'`; `unmatched: 'dlq'` with `dlq: true` gives `'dlq'`, with the dead letter in `result.dlq`; `unmatched: 'dlq'` without a DLQ gives `'drop'` and a warn (Review Focus 3).
- [ ] **Step 2: Run** `npx vitest run --project unit test/unit/cloudevent-router.spec.ts`. Expected: the new tests FAIL. The type errors in the test file count as failure.
- [ ] **Step 3: Implement.** Replace `hasSubscribe === hasConsume` with a count of the four keys whose value is not `undefined`. Add the two `spec` branches, and the `concurrency` check for `consumeOrdered`.
- [ ] **Step 4: Run** the file, then `npm test`, `npm run typecheck`, `npm run lint`. Expected: PASS.
- [ ] **Step 5: Commit** with message `feat(cloudevents): CloudEventRouter on push and ordered consumers`.

---

### Task 2: Integration tests and docs

**Files:**
- Test: `test/integration/cloudevent-router.int.spec.ts`. Add a new describe with its own app and server, following the file's setup.
- Docs:
  - `docs/cloudevents.md:71` and nearby: the four sources, a short `consumePush` and `consumeOrdered` example, and the spec §4 table of `unmatched` by source.
    - The default DLQ subject of an ordered router is `dlq.<stream>.ordered.<Class>.@CloudEventRouter`.
    - Note Review Focus 4: with `headersOnly: true`, only binary-mode CloudEvents can be routed, and their `data` is empty.
  - `docs/consumers.md:19`: the `@CloudEventRouter` row becomes "Yes, with `consume`" / "Yes, with `consumePush`" / "Yes, with `consumeOrdered`".
  - `README.md`: remove the "Router: subscribe and pull only" bullet.
  - `CHANGELOG.md`, 0.1.0 entry: the CloudEvents bullet says a router receives from a core subscription or a pull, push or ordered consumer.

- [ ] **Step 1: Write the integration tests.**
  - `'routes CloudEvents from a push consumer router'`:
    - stream `CEP` (`cep.>`);
    - `@CloudEventRouter({ consumePush: { stream: 'CEP', durable: 'cep-r', deliverGroup: 'g', config: { filter_subject: 'cep.>' } } })` with routes `a.created` and `a.deleted`;
    - publish one binary CloudEvent of each type with `NatsClient`/`JetStreamClient` `cloudEvent`;
    - eventually both routes are called with their `data`.
  - `'routes CloudEvents from an ordered consumer router in stream order'`:
    - stream `CEO` (`ceo.>`);
    - `@CloudEventRouter({ consumeOrdered: { stream: 'CEO' } })` with routes `x.one` and `x.two`, each pushing `[type, data]` into a shared array;
    - publish `x.one` (1), `x.two` (2), `x.one` (3);
    - eventually the array is `[['x.one', 1], ['x.two', 2], ['x.one', 3]]`.
- [ ] **Step 2: Run** `npx vitest run --project integration test/integration/cloudevent-router.int.spec.ts` with the env. Expected: PASS. If a test fails because an adapter, settlement, validator or the harness needs a change, stop and report it (Global Constraints).
- [ ] **Step 3: Write the docs changes.**
- [ ] **Step 4: Run** the full verification: `npm test`, `npm run test:int` (with the env), `npm run typecheck`, `npm run lint`, `npm run docs:check`. Expected: all pass.
- [ ] **Step 5: Commit** with message `test(cloudevents): push and ordered routers end to end; docs: router sources`.
