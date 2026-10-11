# Sync Removed Fields Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remember which top-level fields a stream or consumer declaration had, in a metadata key, so that a field removed from the declaration is reset to the server default in `create-or-update` and reported as drift in `verify`.

**Architecture:**
- Pure helpers in `src/jetstream/stream-sync.ts` handle the snapshot, the removed fields, the metadata to send, and the exact `metadata` comparison.
- `ensureStream`/`ensureConsumer` in `src/jetstream/stream-manager.ts` use them.
- A module-level `WeakSet<ConnectionEntry>` turns the feature off per connection when the server does not keep metadata.

**Tech Stack:** TypeScript (ESM, NodeNext), nats.js v3 (`@nats-io/jetstream` 3.4), Vitest (`unit` and `integration` projects), testcontainers with `nats:2.12-alpine`.

**Spec:** `docs/superpowers/specs/2026-10-11-sync-removed-fields-design.md`

## Global Constraints

- `SNAPSHOT_KEY = 'nestjs-nats.declared'`.
- The value is the sorted top-level field names of the declaration, joined with `,`, after `withoutNulls` and with `undefined` fields dropped. `metadata` is excluded, and so are `name` (streams) and `durable_name` (consumers). With no fields the value is `""`. Library defaults (`withConsumerDefaults`, push `createDefaults`) never count.
- `metadata` sent to the server:
  - When declared: `{ ...declared.metadata, [SNAPSHOT_KEY]: snapshot }`. It is compared exactly, ignoring `_nats.*` and `SNAPSHOT_KEY`.
  - When not declared: `{ ...current metadata without _nats.*, [SNAPSHOT_KEY]: snapshot }`.
- Terms:
  - `removed` = fields in the previous snapshot − declared fields − `metadata`, keeping only fields where `current[field] !== undefined`.
  - `removedImmutable` = the part of `removed` in `STREAM_IMMUTABLE_FIELDS` / `CONSUMER_IMMUTABLE_FIELDS`.
  - `toReset` = `removed` − `removedImmutable`.
- `create-or-update` on an existing stream or consumer:
  - Conflicts on declared immutable fields still throw.
  - Each removed immutable field logs a warn, exactly: `JetStream stream "<name>" keeps <field> <json>: it cannot be changed (removed from the declaration)`. Consumers use `consumer "<stream>/<durable>"` in place of `stream "<name>"`.
  - An update is sent when diffs are non-empty, `toReset` is non-empty, or the server's snapshot differs from the new one. It sends `{ ...cfg, metadata, <each toReset field>: undefined }`.
  - After the update, each reset field logs, exactly: `JetStream stream "<name>": reset <field> to the server default (removed from the declaration)` (or the consumer form).
- `verify` adds one drift entry per `toReset` and `removedImmutable` field, rendered as `<field> (removed from the declaration, current: <json>)`, after the existing ones. It writes nothing.
- `create-only` writes the snapshot only when it creates.
- Metadata unsupported:
  - Detection: the create or update response lacks `SNAPSHOT_KEY` with the value sent.
  - Response: warn once per connection, exactly: `JetStream on connection "<name>" does not keep stream/consumer metadata (NATS server 2.10 or later is needed); removed fields are not detected`.
  - From then on that connection writes no snapshot and computes no `removed`.
- Integration env:

  ```
  DOCKER_HOST=unix:///Users/hungvv/.colima/default/docker.sock
  TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
  TESTCONTAINERS_RYUK_DISABLED=true
  ```

- Verification before each commit: `npm test`, `npm run typecheck`, `npm run lint`. The last task also runs `npm run test:int` and `npm run docs:check`.
- Commits end with `Claude-Session: https://claude.ai/code/session_016DmB9sNwwcck4F6EDJBSom`.

## Review Focus

1. **Push consumers declared with `config`.** Their `createDefaults` (`deliver_subject`, `flow_control`, `idle_heartbeat`, `max_ack_pending`, `deliver_group`) must never appear in the snapshot. Otherwise they would later be "removed" and reset, which breaks the push consumer. Tested in Task 2.
2. **A stream declared once with `metadata`, then without it.** The snapshot excludes `metadata`, so it is not "removed". The user keys stay on the server, because without a declaration the current metadata is kept. Tested in Task 1 (`syncMetadata`) and Task 2.
3. **Second boot with an unchanged declaration.** No update is sent, so there is no update churn. Tested in Task 2 and in the integration test (Task 3), which checks the stream's config `created`/update count is unchanged, or that `ensureStream` makes no update call.
4. **An unrelated tool's metadata key** on a stream declared without `metadata` survives the sync. Tested in Task 2.
5. **A declared value of `undefined`, or `null` via `withoutNulls`.** It is not in the snapshot. When it was previously declared with a value, it counts as removed and is reset. Tested in Task 1.

---

### Task 1: Snapshot helpers and exact `metadata` comparison

**Files:**
- Modify: `src/jetstream/stream-sync.ts`.
- Test: `test/unit/stream-sync.spec.ts`.

**Interfaces:**
- Produces, in `stream-sync.ts`:
  - `export const SNAPSHOT_KEY = 'nestjs-nats.declared'`.
  - `export function declaredFields(cfg: object, exclude: readonly string[]): string[]`. It returns sorted names, without `undefined`/`null` values, `metadata`, or `exclude`.
  - `export function snapshotOf(fields: readonly string[]): string`.
  - `export function parseSnapshot(metadata: Record<string, string> | undefined): string[] | undefined`. It returns `undefined` when the key is absent, and `[]` for `""`.
  - `export function removedFields(current: object, previous: readonly string[] | undefined, declared: readonly string[], immutable: readonly string[]): { reset: string[]; immutable: string[] }`.
  - `export function syncMetadata(declared: Record<string, string> | undefined, current: Record<string, string> | undefined, snapshot: string): Record<string, string>`.
- `diffConfig(current, declared, immutable)` keeps its signature. For `metadata` it ignores `SNAPSHOT_KEY` as well as `_nats.*`. When `metadata` is declared, it compares exactly: a key present on the server but missing from the declaration is a diff.

- [ ] **Step 1: Write the failing tests.**
  - `declaredFields({ name: 'S', subjects: ['a'], max_age: 5, metadata: {x:'1'}, description: undefined, max_msgs: null as never }, ['name'])` returns `['max_age', 'subjects']`.
  - `snapshotOf(['a','b'])` returns `'a,b'`, and `snapshotOf([])` returns `''`.
  - `parseSnapshot`:
    - `undefined` → `undefined`;
    - `{}` → `undefined`;
    - `{ [SNAPSHOT_KEY]: '' }` → `[]`;
    - `'a,b'` → `['a','b']`.
  - `removedFields({ max_age: 5, storage: 'file', subjects: ['a'] }, ['max_age','storage','subjects','description'], ['subjects'], STREAM_IMMUTABLE_FIELDS)` returns `{ reset: ['max_age'], immutable: ['storage'] }`. `description` is skipped because it is absent on the server.
  - `removedFields(cur, undefined, …)` returns `{ reset: [], immutable: [] }`.
  - `syncMetadata`:
    - declared `{a:'1'}` + current `{a:'0', b:'2', '_nats.ver':'x'}` → `{a:'1', [SNAPSHOT_KEY]: s}`;
    - declared `undefined` + the same current → `{a:'0', b:'2', [SNAPSHOT_KEY]: s}`;
    - declared `undefined` + current `undefined` → `{[SNAPSHOT_KEY]: s}`.
  - `diffConfig`:
    - `{ metadata: {'_nats.ver':'x', [SNAPSHOT_KEY]:'a', team:'x'} }` vs declared `{ metadata: { team: 'x' } }` gives `[]`;
    - `{ metadata: { team: 'x', old: '1' } }` vs declared `{ metadata: { team: 'x' } }` gives one `metadata` diff, whose `current` excludes `_nats.*` and the snapshot key.
  - Existing tests keep passing. The existing test "treats a declared empty metadata as equal to server-only metadata" must still pass.
- [ ] **Step 2: Run** `npx vitest run --project unit test/unit/stream-sync.spec.ts`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the file, `npm test`, `npm run typecheck`, `npm run lint`. Expected: PASS.
- [ ] **Step 5: Commit** with message `feat(streams): snapshot helpers for declared stream and consumer fields`.

---

### Task 2: `ensureStream` and `ensureConsumer` reset removed fields

**Files:**
- Modify: `src/jetstream/stream-manager.ts`.
- Test: `test/unit/stream-sync.spec.ts`, in the existing `NatsStreamManager` describes that use the fake `jsm`, extended so `add`/`update` echo back the sent config with metadata.

**Interfaces:**
- Consumes all Task 1 exports.

Behavior: Global Constraints and spec §5, in this order: diffs and conflicts, immutable warns, the update decision, the reset logs, and metadata-unsupported detection.
- The update payload is `{ ...cfg, metadata, ...resetUndefined }`, without `name` for streams, as today.
- Create payloads add `metadata` to today's payloads, for streams and for consumers. For consumers, `createDefaults` and `withConsumerDefaults` still apply, and the snapshot is computed from the user declaration only.
- The per-connection unsupported flag is `const metadataUnsupported = new WeakSet<ConnectionEntry>()` at module level.

- [ ] **Step 1: Write the failing tests.**
  - Streams:
    - `'creates a stream with the snapshot in its metadata'`.
    - `'only writes the snapshot to a stream that has none'`: the update contains `metadata` with the snapshot, no field is set to `undefined`, and no reset log appears.
    - `'resets a field removed from the declaration'`: the current config has `max_age: 5` and snapshot `'max_age,subjects'`; the declaration is `{ name, subjects }`. The update has an own property `max_age` set to `undefined`, and `logger.log` received the exact reset line.
    - `'warns and keeps a removed immutable field'`: the snapshot has `storage`, the declaration does not, and the current value is `'file'`. The warn line is exact, and `storage` is not in the update.
    - `'does not update when nothing changed'` (Review Focus 3).
    - `'keeps another tool's metadata when none is declared'` (Review Focus 4).
    - `'reports a removed field in verify'`: the error message contains `max_age (removed from the declaration, current: 5)`.
    - `'turns the feature off when the server does not keep metadata'`: `update` echoes the config without `metadata`. There is exactly one warn with the exact text, and a second `ensureStream` with the same declaration on the same entry makes no update call.
  - Consumers: the create, reset and verify counterparts, plus `'never counts the push create defaults in the snapshot'` (Review Focus 1). That test creates with `createDefaults`, then calls `ensureConsumer` again with the same declaration. It sees no reset and no update.
  - Existing manager tests keep passing. Adjust their expected `add`/`update` payloads only by the added `metadata`, and say so in the report.
- [ ] **Step 2: Run** the file. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the file, `npm test`, `npm run typecheck`, `npm run lint`. Expected: PASS.
- [ ] **Step 5: Commit** with message `feat(streams): reset fields removed from a declaration, and report them in verify`.

---

### Task 3: Integration tests and docs

**Files:**
- Test: `test/integration/jetstream.int.spec.ts`. Add a new describe with its own server. Use `createTestApp` from `support/app.ts`, closing each app before the next boot.
- Docs:
  - `docs/streams.md`: a section on removed fields. Cover the snapshot key, reset in `create-or-update`, drift in `verify`, the immutable warn, the first run writing the snapshot only, the 2.10+ requirement, and how `metadata` is handled.
  - `README.md:131`: rename and rewrite the "Undetected removals" bullet so only the `0`-declaration note remains, with its link.
  - `CHANGELOG.md` 0.1.0: in the declarative streams bullet, add that a field removed from a declaration is reset to the server default (and reported by `verify`).

- [ ] **Step 1: Write the tests.**
  - `'resets a stream field removed from the declaration'`:
    - boot with `streams: [{ name: 'RM', subjects: ['rm.>'], max_age: 60_000_000_000 }]`, then close;
    - boot with `streams: [{ name: 'RM', subjects: ['rm.>'] }]`, then close;
    - `jsm.streams.info('RM')` gives `config.max_age === 0` and `config.metadata['nestjs-nats.declared'] === 'subjects'`.
  - `'resets a consumer field removed from the declaration'`:
    - stream `RC`, consumer `{ stream: 'RC', config: { durable_name: 'c', max_ack_pending: 5 } }`; boot, then close;
    - boot again with `{ durable_name: 'c' }`;
    - `jsm.consumers.info('RC','c')` gives `config.max_ack_pending !== 5`.
  - `'fails verify for a removed field'`: after a first boot with `max_age`, boot with `jetstream: { sync: 'verify' }` and a declaration without `max_age`. `createTestApp` rejects with a message matching `/max_age \(removed from the declaration/`.
- [ ] **Step 2: Run** `npx vitest run --project integration test/integration/jetstream.int.spec.ts` with the env. Expected: PASS. If one fails because nats-server rejects an update with `undefined` fields, or does not reset to the default, stop and report with the server error. Do not weaken the test.
- [ ] **Step 3: Write the docs changes.**
- [ ] **Step 4: Run** the full verification: `npm test`, `npm run test:int` (with the env), `npm run typecheck`, `npm run lint`, `npm run docs:check`. Expected: all pass.
- [ ] **Step 5: Commit** with message `test(streams): removed fields end to end; docs: removed fields in stream sync`.
