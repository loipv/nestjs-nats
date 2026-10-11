# KV Idempotency Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `KvIdempotencyStore` in a new `@loipv/nestjs-nats/kv` subpath. It is an idempotency store on JetStream KV that de-duplicates across instances. The library hands it the `forRoot` connection through a new optional `IdempotencyStore.bind` hook.

**Architecture:**
- `IdempotencyStore` gains `bind?(ctx)`. `NatsLifecycle.onModuleInit` calls it first, before stream sync.
- `KvIdempotencyStore` opens or creates its bucket in `bind`.
- Expiry is a logical TTL: the value holds the state and `expiresAt`. A claim on an expired key is overwritten with a compare-and-swap `update(k, v, revision)`.
- Keys are stored as base64url.
- `@nats-io/kv` is an optional peer, imported only by the subpath.

**Tech Stack:** TypeScript (ESM, NodeNext), NestJS 11/12, nats.js v3 (`@nats-io/kv` 3.4), Vitest (`unit` and `integration` projects), testcontainers with `nats:2.12-alpine`.

**Spec:** `docs/superpowers/specs/2026-10-11-kv-idempotency-store-design.md`

## Global Constraints

- Subpath `@loipv/nestjs-nats/kv`, built from `src/kv/index.ts`. The package `exports["./kv"]` follows the shape of the existing subpaths.
- `@nats-io/kv` is listed in `peerDependencies` as `^3.0.0` and marked optional in `peerDependenciesMeta`. It is also a devDependency at `^3.4.0`.
- Only files under `src/kv/` may import `@nats-io/kv`.
- Option defaults:

  | Option | Default |
  |---|---|
  | `bucket` | `'nestjs_nats_idempotency'` |
  | `maxAge` | `86_400_000` |
  | `storage` | `'file'` |
  | `replicas` | `1` |
  | `now` | `Date.now` |

- Option validation:

  | Option | Rule |
  |---|---|
  | `bucket` | matches `/^[-\w]+$/` |
  | `maxAge` | integer, `> 0` and `<= 2147483647000` |
  | `storage` | `'file'` or `'memory'` |
  | `replicas` | integer `>= 1` |
  | `instanceId` | non-empty string, if set |

  Invalid options throw `NatsConfigError` from the constructor.
- A bucket the store creates gets `{ history: 1, ttl: maxAge, storage, replicas }`. An existing bucket is only opened.
- Exact texts:
  - Second bind: `KvIdempotencyStore is already bound to NATS connection "<a>"; give each connection its own store` (`NatsConfigError`).
  - Use before bind: `KvIdempotencyStore is not bound to a NATS connection; pass it as idempotency.store of NatsModule.forRoot()` (`NatsModuleError`).
  - Missing `instanceId` warning, logged once per store through `new Logger('KvIdempotencyStore')`: `KvIdempotencyStore without instanceId is shared by the instances of @ConsumeOrdered handlers: each instance skips the messages another processed. Set instanceId.`
- Keys:
  - With `instanceId`, a key starting with `ordered:` is rewritten as `ordered:<instanceId>:<rest>`.
  - The stored KV key is `Buffer.from(key, 'utf8').toString('base64url')`.
- Values are JSON `{"s":"p"|"d","x":<ms>}`, where `x = now() + ttlMs`. An entry is live when `x > now()`. An unparsable value counts as expired.
- `claim`:
  - `create` succeeds → `'claimed'`.
  - Otherwise `get`:
    - missing, deleted, purged or expired → `update` with that revision. On a revision mismatch, retry the whole claim, up to 3 attempts, then throw the last error.
    - live → `'duplicate'` (`d`) or `'in-progress'` (`p`).
- `complete` → `put`. `release` → `delete`. `close()` does nothing.
- KV errors propagate unchanged.
- Integration env:

  ```
  DOCKER_HOST=unix:///Users/hungvv/.colima/default/docker.sock
  TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
  TESTCONTAINERS_RYUK_DISABLED=true
  ```

- Verification before each commit: `npm test`, `npm run typecheck`, `npm run lint`. The last task also runs `npm run test:int`, `npm run docs:check` and `npm run check:pkg`.
- Commits end with `Claude-Session: https://claude.ai/code/session_016DmB9sNwwcck4F6EDJBSom`.

## Review Focus

1. **The main entry must load without `@nats-io/kv` installed.** Nothing under `src/` outside `src/kv/` may import it. Tested in Task 2 by a grep-style unit test over the built import graph, or by a test that imports `src/index.ts` with `@nats-io/kv` mocked to throw.
2. **Two instances claim an expired key at the same moment.** Exactly one gets `'claimed'`; the other gets `'in-progress'`. Tested in Task 2 with a fake KV that rejects the second `update` with a wrong-revision error.
3. **`release` after the key expired and was re-claimed by someone else.** `release` deletes the key regardless; this matches the in-memory store. It is documented, not tested.
4. **A store given to `forRoot` of two connections.** The second bind fails init with the exact text. Tested in Task 1 or 2.
5. **Keys containing characters outside the base64url alphabet.** The encoding is reversible, and long keys fit, since KV allows long subjects. Tested in Task 2 with a 1 KB key.

---

### Task 1: The `bind` hook and the lifecycle call

**Files:**
- Modify:
  - `src/idempotency/store.ts`: `IdempotencyStoreContext` and `bind?`, with doc comments from the spec.
  - `src/index.ts`: export `type IdempotencyStoreContext`.
  - `src/module/nats-lifecycle.ts`: in `onModuleInit`, before `collectDeclarations()`, for every connection name whose `options.idempotency?.store?.bind` exists, `await store.bind({ connection: name, nc: entry.nc })`. Do this once per (store, connection) pair, in connection order. An error goes to the existing catch: shutdown, then rethrow.
- Test: `test/unit/nats-lifecycle.spec.ts`. Also update the public-API test if it lists type exports.

**Interfaces:**
- Produces: `interface IdempotencyStoreContext { connection: string; nc: NatsConnection }` and `IdempotencyStore.bind?(ctx: IdempotencyStoreContext): Promise<void> | void`.

- [ ] **Step 1: Write the failing tests.**
  - `'binds the idempotency store of each connection before syncing streams'`: a fake store with a `bind` spy. Check that it is called once with `{ connection: 'default', nc: entry.nc }`, and that it runs before the `jsm` `streams.add` call (compare call order).
  - `'fails init when a store fails to bind'`: `onModuleInit` rejects with the bind error, and the connections are closed.
  - `'binds a store shared by two connections once per connection'`: the same object is passed to two connections, so `bind` is called twice, once with each name. It is the store's job (Task 2) to refuse the second.
- [ ] **Step 2: Run** `npx vitest run --project unit test/unit/nats-lifecycle.spec.ts`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the file, `npm test`, `npm run typecheck`, `npm run lint`. Expected: PASS.
- [ ] **Step 5: Commit** with message `feat(idempotency): let a store bind to its connection at init`.

---

### Task 2: `KvIdempotencyStore`

**Files:**
- Create:
  - `src/kv/kv-idempotency-store.ts`: the class, its options type and helpers.
  - `src/kv/index.ts`: exports `KvIdempotencyStore` and `type KvIdempotencyStoreOptions`.
- Modify: `package.json`. Add `exports["./kv"]`, the peer dependency entry and its `peerDependenciesMeta`. Install the devDependency with `npm install -D @nats-io/kv@^3.4.0` so the lockfile updates.
- Test: `test/unit/kv-idempotency-store.spec.ts`.

**Interfaces:**
- Consumes: `IdempotencyStore`, `ClaimResult`, `IdempotencyStoreContext` (Task 1).
- Produces: `class KvIdempotencyStore implements IdempotencyStore` with `constructor(options?: KvIdempotencyStoreOptions)`.
- The class opens buckets through `new Kvm(nc)` from `@nats-io/kv`. For tests, allow an internal seam: a module-private factory the spec can replace with `vi.mock('@nats-io/kv', ...)`, or a second constructor parameter typed `@internal`. Pick one and say which.

- [ ] **Step 1: Write the failing tests**, against a fake `Kvm`/`KV` with in-memory revisions. It implements `create` (throws if a live entry exists), `get`, `update` (checks the revision), `put` and `delete` (writes a DEL marker that `create` overwrites):
  - Option validation: one `it.each` row per rule, each expecting `NatsConfigError`.
  - `'creates the bucket when it is missing'`: `open` rejects with stream-not-found, and `create` is called with `{ history: 1, ttl: 86_400_000, storage: 'file', replicas: 1 }`.
  - `'only opens an existing bucket'`.
  - `'refuses a second bind'`: the exact text.
  - `'refuses use before bind'`: the exact text.
  - Claim states:
    - a new key → `'claimed'`;
    - after `complete` → `'duplicate'`;
    - a live claim → `'in-progress'`;
    - after `release` → `'claimed'`;
    - an expired `p` or `d` (advance `now`) → `'claimed'` via `update` with the read revision;
    - an unparsable value → `'claimed'`.
  - `'one of two concurrent claims of an expired key wins'` (Review Focus 2).
  - `'gives up after 3 revision conflicts'`: the last error propagates.
  - `'encodes keys as base64url'`: a key with `consume:S/d|a%2Fb:é` round-trips to the stored key `Buffer.from(...).toString('base64url')`. A 1 KB key works too (Review Focus 5).
  - `'prefixes ordered keys with instanceId'`: `ordered:C.m:k` is stored as the encoding of `ordered:i1:C.m:k`, and other keys are left alone.
  - `'warns once about ordered keys without instanceId'`: spy on `Logger.prototype.warn`, claim two `ordered:` keys, and expect exactly one warn with the exact text.
  - `'complete puts done with the new expiry'`: the stored value is `{"s":"d","x":now+ttl}`.
  - `'the main entry does not import @nats-io/kv'` (Review Focus 1): `vi.mock('@nats-io/kv', () => { throw new Error('not installed') })`, then `await import('../../src/index.js')` resolves.
- [ ] **Step 2: Run** `npx vitest run --project unit test/unit/kv-idempotency-store.spec.ts`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the file, `npm test`, `npm run typecheck`, `npm run lint`. Expected: PASS.
- [ ] **Step 5: Commit** with message `feat(kv): KvIdempotencyStore, an idempotency store on JetStream KV`.

---

### Task 3: Integration tests, docs and package checks

**Files:**
- Test: create `test/integration/kv-idempotency.int.spec.ts`, using the server and app helpers in `test/integration/support/`.
- Docs:
  - `docs/idempotency.md`, "Stores" section: a "JetStream KV store" subsection covering install (`npm i @nats-io/kv`), an example, the options table, how it works (logical TTL, clock skew, `maxAge` versus `ttl`), `instanceId` for ordered consumers, and "one store per connection". Add a short note on `bind` for people writing their own store.
  - Update the "Ordered consumers and shared stores" bullet to point at `instanceId`.
  - `README.md`: mention the KV store on the idempotency feature line.
  - `CHANGELOG.md`, 0.1.0 entry: add the KV store to the idempotency bullet.

- [ ] **Step 1: Write the tests.**
  - `'de-duplicates across two applications sharing a bucket'`:
    - Server with JetStream.
    - App A and app B each have `forRoot` with `idempotency: { store: new KvIdempotencyStore({ bucket: 'idem' }) }`, one store object per app.
    - Each app has a `@Subscribe('idem.x', { queue: 'A' | 'B', idempotency: true })` handler that counts calls in a shared array.
    - Publish two messages on `idem.x` with the same `Nats-Msg-Id` header.
    - The array eventually has length 1 and stays 1 after a short settle. Use `eventually` for the 1. Then wait a bounded 300 ms and check again; say in a comment that this guards against a late duplicate.
  - `'reclaims an expired key on a real server'`: a store bound through an app, with a `now` you control. `claim('k', 1)` → `'claimed'`. Advance `now` by 10, then `claim('k', 1_000)` → `'claimed'`.
  - `'creates the bucket with history 1 and max age'`: after boot, `new Kvm(nc).open('idem2')` then `status()` shows `history === 1` and `ttl === maxAge`.
- [ ] **Step 2: Run** `npx vitest run --project integration test/integration/kv-idempotency.int.spec.ts` with the env. Expected: PASS.
- [ ] **Step 3: Write the docs changes.**
- [ ] **Step 4: Run** the full verification: `npm test`, `npm run test:int` (with the env), `npm run typecheck`, `npm run lint`, `npm run docs:check`, `npm run check:pkg`. Expected: all pass. `check:pkg` must accept the new subpath; fix the package metadata if it doesn't.
- [ ] **Step 5: Commit** with message `test(kv): KV idempotency end to end; docs: JetStream KV store`.
