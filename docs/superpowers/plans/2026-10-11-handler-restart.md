# Handler Restart Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restart a handler's message intake (core subscription, JetStream pull/push consumer, ordered consumer, NATS service) when it ends unexpectedly, with capped exponential backoff and optional `maxAttempts`. Let the health indicator report handlers that gave up.

**Architecture:** A NATS-free `RestartSupervisor` (backoff, attempt count, state, timers) lives inside each adapter. Each adapter splits "open the intake and start its loop" into a re-callable `reopen()` and calls `supervisor.ended(err)` when its loop dies. The lifecycle resolves the connection's `restart` policy and passes it to each adapter, with a state tracker backed by `ConnectionRegistry`. `NatsHealthIndicator` reads those states when `handlers: true`.

**Tech Stack:** TypeScript (ESM, NodeNext), NestJS 11/12, nats.js v3 (`@nats-io/transport-node`, `jetstream`, `services`), Vitest (`unit` and `integration` projects), testcontainers with `nats:2.12-alpine`.

**Spec:** `docs/superpowers/specs/2026-10-11-handler-restart-design.md`

## Global Constraints

- Defaults: `{ initialDelay: 1000, factor: 2, maxDelay: 30_000, jitter: 0.2, maxAttempts: Infinity }`.
- Validation rules, each failure throwing `NatsConfigError` with the connection name:
  - `initialDelay` and `maxDelay` are finite and in `[0, 2^31-1]`, with `initialDelay ≤ maxDelay`.
  - `factor` is finite and ≥ 1.
  - `jitter` is in `[0, 1]`.
  - `maxAttempts` is an integer ≥ 1, or `Infinity`.
- Attempt `n` waits `backoffDelay(policy, n)`, using `src/settlement/retry.ts`. The attempt count resets once a reopened intake has run for `maxDelay` ms.
- The first open, in `start()`, is not supervised: a failure still fails bootstrap as today.
- When the connection is closed, there is no restart.
- With `restart: false`, logs stay exactly as today (with the "no more ... will be received" tail), and the handler state becomes `failed`.
- Every timer is `unref()`'d.
- Log lines, exact:
  - `Restarting <label> in <ms> ms (attempt <n>)` (warn). When `maxAttempts` is finite, the attempt part reads `(attempt <n>/<max>)`.
  - `Restart of <label> failed (attempt <n>): <errorMessage>` (warn).
  - `Restarted <label> (attempt <n>)` (log).
  - `Gave up restarting <label> after <n> attempts; no more messages will be received` (error).
  - `Gave up restarting <label>: the connection is closed` (error).
- Health with `handlers: true`:
  - When a handler has `failed`: `down` with `reason: 'handler failed: <label>'`. When more than one has failed, the reason ends with `, +<n> more`. Details are `handlers: { failed, restarting }`.
  - Otherwise `up`, with `handlers: { restarting }` only when that list is non-empty.
  - Without `handlers`, the result is unchanged.
- Config warnings at open (`checkMaxDeliver`, `checkBatchWait`) are logged on the first open only.
- An ordered consumer reopens at the last handled `streamSequence + 1` (`DeliverPolicy.StartSequence`). If no message was handled, it reopens with its declared options.
- Integration env:

  ```
  DOCKER_HOST=unix:///Users/hungvv/.colima/default/docker.sock
  TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
  TESTCONTAINERS_RYUK_DISABLED=true
  ```

- Verification before each commit: `npm test`, `npm run typecheck`, `npm run lint`. Before the final commit, also `npm run test:int` and `npm run docs:check`.
- Commits end with `Claude-Session: https://claude.ai/code/session_016DmB9sNwwcck4F6EDJBSom`.

## Review Focus

1. **`stop()` while a reopen is in flight.** A JetStream reopen that resolves after `stop()` must close the new `messages` at once, and `idle()` must still resolve. Tested in Task 5.
2. **Connection closed outside shutdown.** When a loop ends while `nc.isClosed()`, there is no reopen and no pending timer, and the state is `failed`. Tested in Task 4 (core adapter).
3. **Ordered consumer that handled nothing yet.** Its reopen must use the declared `deliverPolicy`, not `opt_start_seq: 1`. Tested in Task 6.
4. **Service whose endpoints all end at once.** Only one restart is scheduled. Tested in Task 7.
5. **Push consumer without `deliverGroup` refused as "already bound" on reopen.** It counts as a failed attempt and is retried; it neither crashes nor stops the process. Tested in Task 5.

---

### Task 1: Restart policy

**Files:**
- Create: `src/settlement/restart.ts`
- Modify: `src/module/options.ts` (add `restart?: RestartOptions | false` to `NatsModuleOptions`, with doc comment), `src/index.ts` (export `type RestartOptions` next to `RetryOptions`)
- Test: `test/unit/restart-policy.spec.ts`

**Interfaces:**
- Produces:
  - `interface RestartOptions { initialDelay?: number; factor?: number; maxDelay?: number; jitter?: number; maxAttempts?: number }`
  - `interface RestartPolicy`: the same fields, all required.
  - `const DEFAULT_RESTART: Readonly<RestartPolicy>`
  - `function resolveRestartPolicy(opts: RestartOptions | false | undefined, connection: string): RestartPolicy | false`

- [ ] **Step 1: Write the failing tests**
  - `'defaults when undefined'`: `resolveRestartPolicy(undefined, 'c')` equals `{ initialDelay: 1000, factor: 2, maxDelay: 30_000, jitter: 0.2, maxAttempts: Infinity }`.
  - `'is false when false'`.
  - `'merges given fields over defaults'`: `{ maxAttempts: 3 }` gives the defaults with `maxAttempts: 3`. Fields set to `undefined` keep their default.
  - `it.each` `'rejects %s'` over these inputs: `{ initialDelay: -1 }`, `{ initialDelay: NaN }`, `{ maxDelay: 2 ** 31 }`, `{ initialDelay: 5000, maxDelay: 1000 }`, `{ factor: 0.5 }`, `{ factor: Infinity }`, `{ jitter: 1.5 }`, `{ maxAttempts: 0 }`, `{ maxAttempts: 1.5 }`. Each throws a `NatsConfigError` whose message contains `NATS connection "c"` and the field name.
  - `'accepts maxAttempts Infinity and jitter 0'`.

- [ ] **Step 2: Run** `npx vitest run --project unit test/unit/restart-policy.spec.ts`. Expected: FAIL, module not found.

- [ ] **Step 3: Implement `src/settlement/restart.ts`**
  - Follow the style of `retry.ts`: drop `undefined` fields, then spread over the defaults.
  - Message format: `NATS connection "<name>" restart.<field> must be <rule>, got <value>`.

- [ ] **Step 4: Run** the test again. Expected: PASS. Then run `npm run typecheck`.

- [ ] **Step 5: Commit** with message `feat(restart): restart policy options and validation`.

---

### Task 2: RestartSupervisor

**Files:**
- Create: `src/adapters/restart-supervisor.ts`
- Modify: `src/adapters/adapter.ts` (add the types below)
- Test: `test/unit/restart-supervisor.spec.ts`

**Interfaces:**
- Consumes: `RestartPolicy` (Task 1), `backoffDelay` from `src/settlement/retry.ts`, `errorMessage` from `src/util/errors.ts`.
- Produces, in `adapter.ts`:
  ```ts
  export type HandlerState = 'running' | 'restarting' | 'failed';
  /** How an adapter restarts its intake; given by the lifecycle. */
  export interface RestartBinding {
    policy: RestartPolicy | false;
    /** Registers the adapter's handler under `label`; returns its state setter. */
    track(label: string): (state: HandlerState) => void;
  }
  export const NO_RESTART: RestartBinding; // { policy: false, track: () => () => {} }
  ```
- Produces, in `restart-supervisor.ts`:
  ```ts
  export class RestartSupervisor {
    constructor(deps: {
      policy: RestartPolicy | false;
      label: string;
      logger: LoggerService;
      isConnectionClosed: () => boolean;
      reopen: () => Promise<void>;
      onState: (state: HandlerState) => void;
      random?: () => number;
    });
    get state(): HandlerState | undefined; // undefined until started()
    /** Whether ended() would schedule a restart now. */
    get restartable(): boolean;
    started(): void;
    ended(err?: unknown): void;
    stop(): void;
    settled(): Promise<void>;
  }
  ```

The algorithm is not determined by the tests alone, so it is pinned here:

```
restartable = !stopped && policy !== false && !isConnectionClosed() && attempts < policy.maxAttempts
started():   state = running; onState
ended(err):  if stopped or state !== 'running': return
             clear stable timer
             if !restartable: state = failed; onState; return      // the adapter logged the end with its tail
             schedule()
schedule():  if attempts >= maxAttempts: fail(`Gave up restarting ${label} after ${attempts} attempts; no more messages will be received`); return
             attempts++; state = restarting; onState
             delay = backoffDelay(policy, attempts, random); warn "Restarting …"; timer = setTimeout(run, delay).unref()
run():       if stopped: return
             if isConnectionClosed(): fail(`Gave up restarting ${label}: the connection is closed`); return
             inFlight = reopen().then(
               ok  → if stopped return; state = running; onState; log "Restarted …"; stable = setTimeout(() => attempts = 0, maxDelay).unref()
               err → if stopped return; warn "Restart of … failed …"; schedule())
             settled() awaits inFlight (never rejects)
fail(msg):   state = failed; onState; logger.error(msg)
stop():      stopped = true; clear both timers (state unchanged)
```

- [ ] **Step 1: Write the failing tests** (`vi.useFakeTimers()`, `random: () => 0.5` so jitter is 0, `vi.fn` logger)
  - `'schedules restarts with backoff delays'`:
    - `reopen` rejects twice, then resolves.
    - After `started()` and `ended(new Error('x'))`: warn `Restarting L in 1000 ms (attempt 1)`.
    - Advance 1000 ms: warn `Restart of L failed (attempt 1): …`, then `Restarting L in 2000 ms (attempt 2)`.
    - Advance 2000 ms, then 4000 ms: log `Restarted L (attempt 3)`.
    - `onState` calls in order: `running, restarting, restarting, restarting, running`.
  - `'shows the attempt limit when maxAttempts is finite'`: with `maxAttempts: 3`, the first warn ends `(attempt 1/3)`.
  - `'resets the attempt count after running for maxDelay'`: a reopen succeeds; advance `maxDelay`; `ended()` warns `(attempt 1)`.
  - `'keeps counting when the reopened intake dies before maxDelay'`: a reopen succeeds; `ended()` at once warns `(attempt 2)`.
  - `'gives up after maxAttempts'`: with `maxAttempts: 2` and `reopen` always rejecting, error `Gave up restarting L after 2 attempts; no more messages will be received`. State is `failed`, no timer is pending (`vi.getTimerCount() === 0`), and a later `ended()` does nothing.
  - `'fails without scheduling when the connection is closed at the end'`: `isConnectionClosed` returns true. `restartable` is false, `ended()` sets `failed` with no log, and `reopen` is never called.
  - `'gives up when the connection closed before the restart ran'`: `isConnectionClosed` flips to true during the delay. Error `Gave up restarting L: the connection is closed`.
  - `'fails at once with policy false'`: no log, state is `failed`.
  - `'ignores ended() while restarting'`: two `ended()` calls give one warn.
  - `'stop() cancels a pending restart'`: `vi.getTimerCount() === 0`, and `reopen` is never called.
  - `'a reopen finishing after stop() changes nothing'`: `reopen` resolves after `stop()`. No `Restarted` log, the state stays `restarting`, and `settled()` resolves.

- [ ] **Step 2: Run** `npx vitest run --project unit test/unit/restart-supervisor.spec.ts`. Expected: FAIL, module not found.

- [ ] **Step 3: Implement** the supervisor as pinned above, plus `HandlerState`, `RestartBinding` and `NO_RESTART` in `adapter.ts`.

- [ ] **Step 4: Run** the tests again. Expected: PASS.

- [ ] **Step 5: Commit** with message `feat(restart): RestartSupervisor`.

---

### Task 3: Handler states in the registry and the health indicator

**Files:**
- Modify: `src/connection/connection-registry.ts` (add `handlers: Set<{ handler: string; state: HandlerState }>` to `InternalEntry`, plus the three methods below), `src/health/nats-health.indicator.ts` (`handlers?: boolean` option, with doc comment)
- Test: `test/unit/connection-registry.spec.ts`, `test/unit/nats-health.spec.ts`

**Interfaces:**
- Consumes: `HandlerState` (Task 2).
- Produces, on `ConnectionRegistry`:
  - `trackHandler(connection: string, label: string): (state: HandlerState) => void`. Each call adds its own entry, starting at `'running'`. It throws a `NatsModuleError` for an unknown connection, like `get()`.
  - `handlerStates(connection: string): ReadonlyArray<{ handler: string; state: HandlerState }>`, in registration order. It returns `[]` for an unknown connection.
  - `clearHandlers(): void`

- [ ] **Step 1: Write the failing tests**
  - Registry:
    - `'tracks handler states per connection, one entry per call'`: two `trackHandler('default', 'A')` calls give two entries.
    - `set('failed')` on one of them changes only that entry.
    - `clearHandlers()` empties every connection.
  - Health (extend the `setup()` fake with `handlerStates: () => opts.handlers ?? []`):
    - `'ignores handler states without the handlers option'`: with a `failed` handler, the result equals today's `up` result exactly.
    - `'is down when a handler failed'`: states `[{handler:'A',state:'failed'},{handler:'B',state:'restarting'},{handler:'C',state:'failed'}]` with `{ handlers: true }` give `{ nats: { status: 'down', connection: 'default', reason: 'handler failed: A, +1 more', handlers: { failed: ['A','C'], restarting: ['B'] } } }`.
    - `'is up and lists restarting handlers'`: the result has `handlers: { restarting: ['B'] }` added to the usual fields.
    - `'is up without a handlers field when all run'`.
    - `'checks the connection before handlers'`: in state `reconnecting` with a `failed` handler, the reason is `reconnecting`.

- [ ] **Step 2: Run** both files. Expected: FAIL.

- [ ] **Step 3: Implement.** The handler check goes after the JetStream check and before `session.up`, inside the existing `try`. `down()` takes the extra details: `session.down({ connection, reason, handlers })`.

- [ ] **Step 4: Run** both files. Expected: PASS.

- [ ] **Step 5: Commit** with message `feat(health): report handlers that gave up restarting`.

---

### Task 4: Core subscription restart

**Files:**
- Modify: `src/adapters/core-subscription.adapter.ts`
- Test: `test/unit/core-subscription.adapter.spec.ts`

**Interfaces:**
- Consumes: `RestartSupervisor`, `RestartBinding`, `NO_RESTART`.
- Produces: `new CoreSubscriptionAdapter(def, entry, restart: RestartBinding = NO_RESTART)`.

Behavior:
- `start()` opens through a private `open(): Promise<void>` (subscribe, start the loop, flush), then calls `supervisor.started()`. The supervisor's `reopen` is `open`, which first checks `stopping`.
- `stop()` sets `stopping` and calls `supervisor.stop()` before draining.
- `idle()` awaits `supervisor.settled()`, then the current `this.loop`, then the dispatcher.
- In `consume(sub)`:
  - On an error or a natural end while not `stopping`, log `Subscription of <label> failed` (with the error) or `Subscription of <label> ended unexpectedly`.
  - Add `; no more messages will be received` only when `!supervisor.restartable`.
  - Then call `supervisor.ended(err)`.
  - Call `dispatcher.stop()` only when `stopping` or the supervisor state is `failed`.
- The supervisor's `label` is the adapter's `label`. `isConnectionClosed` is `() => entry.nc.isClosed()`.

- [ ] **Step 1: Write the failing tests.** Extend `fakeSubscription()` with `fail(err)`, which makes the iterator throw, and make `nc.subscribe` return a new fake on each call. Use a policy `{ initialDelay: 10, factor: 1, maxDelay: 10, jitter: 0, maxAttempts: Infinity }`.
  - `'resubscribes after the subscription fails and handles new messages'`:
    - the first fake fails;
    - `nc.subscribe` is called twice;
    - a message pushed to the second fake reaches `invoke`;
    - the `track` setter saw `running, restarting, running`.
  - `'keeps a pending retry across the restart'`:
    - `retry: { initialDelay: 50, jitter: 0 }` with the handler failing once;
    - the subscription fails while the retry is pending;
    - the retry still invokes the handler.
  - `'does not restart when the connection is closed'`:
    - `nc.isClosed` returns true once the loop ends;
    - `nc.subscribe` is called once and `vi.getTimerCount()` stays unchanged;
    - the error log ends with `no more messages will be received`;
    - the state is `failed`.
  - `'logs as before and stays down without a restart binding'`: the existing default-constructed adapter logs `... no more messages will be received`.
  - `'stop() during the restart delay prevents the resubscribe'`: `idle()` resolves.

- [ ] **Step 2: Run** `npx vitest run --project unit test/unit/core-subscription.adapter.spec.ts`. Expected: the new tests FAIL.

- [ ] **Step 3: Implement** as described.

- [ ] **Step 4: Run** the file and then `npm test`. Expected: PASS.

- [ ] **Step 5: Commit** with message `feat(restart): restart core subscriptions that end unexpectedly`.

---

### Task 5: JetStream pull and push consumer restart

**Files:**
- Modify: `src/adapters/jetstream-consumer.adapter.ts`
- Test: `test/unit/jetstream-consumer.adapter.spec.ts`

**Interfaces:**
- Produces: `new JetStreamConsumerAdapter(def, entry, streams, restart: RestartBinding = NO_RESTART)`.

Behavior:
- Split `start()` into a private `open(first: boolean): Promise<void>` containing today's body:
  - pick the intake (`openPush()` or `openPull()`);
  - run the checks, with `checkMaxDeliver` and `checkBatchWait` warning only when `first`;
  - consume;
  - apply the `stopping` guard that closes fresh `messages`;
  - set `messages`, run `startHeartbeat`, set `loop`, start `watchStatus`.
- `start()` calls `open(true)` then `supervisor.started()`. `reopen` is `() => open(false)`.
- Keep the dispatcher, heartbeat, `unsettled` and `inFlight` across a reopen.
- `stop()` calls `supervisor.stop()` first.
- `idle()` awaits `supervisor.settled()` before `this.loop`.
- In `consume()`:
  - Keep the `stopping` guards.
  - Add `; no more messages will be received` to `Consumer of <label> failed` and `... ended unexpectedly` only when `!supervisor.restartable`. Today's "ended unexpectedly" message already has the tail; it becomes conditional.
  - Then call `supervisor.ended(err)`.

- [ ] **Step 1: Write the failing tests.** Follow the file's existing fake consumer helpers; `consume()` returns a new fake `messages` on each call.
  - `'reopens a pull consumer whose messages iterator fails, and keeps heartbeating held messages'`:
    - a message is held by a blocked handler when the first iterator throws;
    - after the delay, `consumers.get` (or `consume`) is called again;
    - the held message's `working()` is still called by the heartbeat after the reopen;
    - a message from the new iterator is handled.
  - `'warns about max_deliver only on the first open'`.
  - `'counts an already-bound push consumer on reopen as a failed attempt'` (Review Focus 5):
    - the push consumer's `consume()` throws `InvalidOperationError` "already bound" on the second open;
    - a warn starts with `Restart of C.m on "ORD/d" (push) failed (attempt 1)`;
    - a third open succeeds.
  - `'closes messages opened by a reopen that finishes after stop()'` (Review Focus 1):
    - the reopen's `consume()` resolves only after `stop()`;
    - that `messages.close` is called;
    - `idle()` resolves.
  - The existing tests that expect `ended unexpectedly` with no binding must still pass unchanged.

- [ ] **Step 2: Run** the file. Expected: the new tests FAIL.

- [ ] **Step 3: Implement** as described.

- [ ] **Step 4: Run** the file and `npm test`. Expected: PASS.

- [ ] **Step 5: Commit** with message `feat(restart): restart JetStream consumers that end unexpectedly`.

---

### Task 6: Ordered consumer restart

**Files:**
- Modify: `src/adapters/ordered-consumer.adapter.ts`
- Test: `test/unit/ordered-consumer.adapter.spec.ts`

**Interfaces:**
- Produces: `new OrderedConsumerAdapter(def, entry, restart: RestartBinding = NO_RESTART)`.

Behavior:
- Track `private lastSeq?: number`. Set it to `msg.info.streamSequence` after `handle(msg)` returns `true`.
- `open()` passes `consumers.get(stream, opts)` its options:
  - while `lastSeq` is undefined, `orderedConsumerOptions(def.options)`;
  - otherwise `{ ...orderedConsumerOptions(def.options) without deliver_policy/opt_start_seq/opt_start_time, deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: lastSeq + 1 }`.
- The same `open`/`started`/`reopen`/`stop`/`idle`/conditional-tail pattern as Task 5. The abort controller is not reset; it is only aborted by `stop()`/`forceStop()`.

- [ ] **Step 1: Write the failing tests**
  - `'reopens after the last handled message'`:
    - with `deliverPolicy: 'all'` and `filterSubjects: 'a.>'`, the handler sees stream seqs 1 and 2, then the iterator throws;
    - the second `consumers.get` call gets `{ filter_subjects: ['a.>'], deliver_policy: 'by_start_sequence', opt_start_seq: 3 }`.
  - `'reopens with the declared options when nothing was handled'` (Review Focus 3):
    - with `deliverPolicy: { startTime: '2026-01-01T00:00:00Z' }` and the iterator throwing before any message;
    - the second call gets `{ deliver_policy: 'by_start_time', opt_start_time: '2026-01-01T00:00:00Z' }`.
  - `'does not restart after stop()'`.

- [ ] **Step 2: Run** the file. Expected: FAIL.

- [ ] **Step 3: Implement.**

- [ ] **Step 4: Run** the file and `npm test`. Expected: PASS.

- [ ] **Step 5: Commit** with message `feat(restart): restart ordered consumers where they left off`.

---

### Task 7: NATS service restart

**Files:**
- Modify: `src/adapters/service-endpoint.adapter.ts`
- Test: `test/unit/service-endpoint.adapter.spec.ts`

**Interfaces:**
- Produces: `new NatsServiceAdapter(options, endpoints, entry, restart: RestartBinding = NO_RESTART)`. The supervisor label is `service "<name>"`.

Behavior:
- `open()` contains today's `start()` body:
  - `Svcm.add`;
  - the `stopping` guard;
  - the `stopped` hook;
  - `addEndpoint` for each endpoint;
  - `flush`.
- On a reopen, `open()` replaces `service`, `iterators` and `loops`. The new loops are added to the list; the old loops have already ended. Endpoint limiters are kept.
- A reopen that throws `NatsConfigError` for an invalid endpoint cannot happen, because the first open already validated it. It is still counted as a failure if it does.
- `onServiceStopped(err)`:
  - if not `stopping` and `err` is set, log `NATS service "<name>" stopped: <msg>`, with `; no more requests will be received` only when `!supervisor.restartable`;
  - stop the iterators;
  - call `supervisor.ended(err)`.
- In `consume()`, an endpoint loop that ends while not `stopping` and while the service did not stop by itself:
  - logs today's message, with the tail `; no more requests will be received` only when `!supervisor.restartable`;
  - stops the current service (`stopService`);
  - calls `supervisor.ended()`.
- `failed` is no longer a separate flag: use "the service this loop belongs to has stopped by itself". Track it per service instance, so loops from an old service never trigger a restart of the new one.
- `stop()` calls `supervisor.stop()` first.
- `idle()` awaits `supervisor.settled()`, then all loops and limiters.

- [ ] **Step 1: Write the failing tests.** Use the file's existing `Svcm` fakes; the fake `add()` returns a new fake service each call.
  - `'recreates the service when it stops with an error'`:
    - `add` is called twice;
    - both endpoints are added on the new service;
    - a request on the new iterator is answered.
  - `'schedules one restart when every endpoint ends at once'` (Review Focus 4):
    - two endpoint iterators end naturally;
    - exactly one `Restarting service "svc"` warn is logged.
  - `'an old service's loop ending does not restart the new one'`.
  - The existing tests `ended unexpectedly` and `stopped ... no more requests`, with no binding, still pass.

- [ ] **Step 2: Run** the file. Expected: FAIL.

- [ ] **Step 3: Implement.**

- [ ] **Step 4: Run** the file and `npm test`. Expected: PASS.

- [ ] **Step 5: Commit** with message `feat(restart): recreate NATS services that stop unexpectedly`.

---

### Task 8: Lifecycle wiring

**Files:**
- Modify: `src/module/nats-lifecycle.ts`
- Test: `test/unit/nats-lifecycle.spec.ts`

**Interfaces:**
- Consumes: `resolveRestartPolicy` (Task 1), `ConnectionRegistry.trackHandler`/`clearHandlers` (Task 3), and the adapter constructors (Tasks 4–7).

Behavior:
- In `createAdapters`, resolve `entry.options.restart` once per connection with `resolveRestartPolicy(…, entry.name)`, cached in a `Map`.
- Build a `RestartBinding` `{ policy, track: (label) => this.manager.trackHandler(entry.name, label) }` and pass it to every adapter as the last argument.
- An invalid policy throws during `onApplicationBootstrap`, before any adapter starts, so bootstrap fails and shuts down as today.
- Call `this.manager.clearHandlers()` at the end of `shutdown()`.

- [ ] **Step 1: Write the failing tests**, following the file's existing lifecycle test setup:
  - `'fails bootstrap on an invalid restart option'`: `restart: { factor: 0 }` rejects with a `NatsConfigError` naming `restart.factor`.
  - `'registers each handler as running after bootstrap'`: `registry.handlerStates('default')` lists the handler's label as `running`.
  - `'clears handler states at shutdown'`.

- [ ] **Step 2: Run** the file. Expected: FAIL.

- [ ] **Step 3: Implement.**

- [ ] **Step 4: Run** `npm test`, `npm run typecheck` and `npm run lint`. Expected: all pass.

- [ ] **Step 5: Commit** with message `feat(restart): restart handlers by default, per connection`.

---

### Task 9: Integration tests and documentation

**Files:**
- Modify: `test/integration/support/nats-server.ts`, by adding `reload(config: string): Promise<void>` to `NatsTestServer`. It copies `config` to `/etc/nats/test.conf` with `container.copyContentToContainer`, runs `container.exec(['kill', '-HUP', '1'])`, then waits until the container log shows `Reloaded server configuration` (use `eventually` from `test/integration/support/eventually.ts`, or poll `container.logs()`). It is available only when the server started with `config`.
- Create: `test/integration/restart.int.spec.ts`
- Modify: `test/integration/service.int.spec.ts:296` (the expectation becomes `/service "denied" stopped: .*den\.forbidden/i`; with the default restart the tail is gone)
- Docs:
  - `docs/health.md`: the `handlers` option row, the down/up shapes, and a readiness-vs-liveness note. Replace the last paragraph with: loops are restarted, see Connections.
  - `docs/connections.md`: a new section "Restarting handlers" covering the options table with defaults, attempt reset after `maxDelay`, the log lines, `false`, and the closed connection case.
  - `docs/consumers.md` (ordered section): after a restart it resumes after the last handled message.
  - `README.md`: remove the "Ended loops are not restarted" bullet.

Scenarios in `restart.int.spec.ts`, each with its own server:

1. `'restarts a pull consumer whose subscription was revoked, once permissions are back'`:
   - Server config `authorization { users = [{ user: "a", password: "pw" }] }`.
   - Stream `RST` (subjects `rst.>`), `@Consume({ stream: 'RST', durable: 'w', config: { ... } })`.
   - `restart: { initialDelay: 100, maxDelay: 500 }`, with a logger collecting warn/log lines.
   - Publish `rst.1`, which is handled.
   - `reload` with `permissions: { subscribe: { deny: ["_INBOX.>"] } }`; eventually a warn matches `/^Restart of .*RST\/w.* failed/`.
   - While denied, `NatsHealthIndicator.isHealthy('nats', { handlers: true })` is `up` and lists the handler under `handlers.restarting`.
   - `reload` the allow config. Eventually a log matches `/^Restarted .*RST\/w/`.
   - Publish `rst.2`, which is handled.
2. `'gives up on a denied core subscription after maxAttempts and reports it'`:
   - Server config denies subscribe on `den.core` for user `a`.
   - `@Subscribe('den.core')`, with `restart: { initialDelay: 50, maxDelay: 100, maxAttempts: 2 }`.
   - Eventually health with `handlers: true` is `down` with `reason` matching `/^handler failed: .*den\.core/`.
3. `'recreates a denied service until it gives up'`:
   - Same server config, with the service endpoint on `den.svc`.
   - `maxAttempts: 2`.
   - Eventually an error line `Gave up restarting service "<name>" after 2 attempts; no more messages will be received`, and health is `down`.
   - `app.close()` takes under 5 s.

For the health indicator, import `TerminusModule` and `NatsHealthModule`, and read `app.get(NatsHealthIndicator)`, as `test/integration/health.int.spec.ts` does.

- [ ] **Step 1: Add `reload()` and write the three scenarios.** Update `service.int.spec.ts:296`.
- [ ] **Step 2: Run** `npx vitest run --project integration test/integration/restart.int.spec.ts test/integration/service.int.spec.ts` with the integration env. Expected: PASS. If scenario 1 fails because the reload does not revoke the subscription on this image, stop and report; do not weaken the test.
- [ ] **Step 3: Write the docs changes.**
- [ ] **Step 4: Run** the full verification: `npm test`, `npm run test:int`, `npm run typecheck`, `npm run lint`, `npm run docs:check`. Expected: all pass.
- [ ] **Step 5: Commit** with message `test(restart): end-to-end restarts; docs: restarting handlers and handler health`.
