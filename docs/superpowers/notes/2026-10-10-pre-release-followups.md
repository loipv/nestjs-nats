# Pre-release hardening — rulings and residuals

Branch fix/pre-release-hardening (3a8616a..HEAD). Scope: BUG, API, DOCS and TOOLING rows of
2026-10-10-pre-release-triage.md. TEST and COSMETIC rows stay deferred as listed there.

## Rulings
- Ruling: heartbeat gap with concurrency > 1 / batch stays documented, no code change (user decision) — see plan2 followups.
- Ruling: CloudEvent idempotency key is `encodeURIComponent(source)|id` — unambiguous separator; fixed before 0.1.0 because keys persist in external stores — cost if wrong: none (no published version).
- Ruling: `NatsConnectionError.cause` is a sanitized plain Error (name, code kept) — the raw nats/Node error can carry the plaintext password; `instanceof` on the cause no longer works (documented in docs/errors.md).
- Ruling: `NatsConnectionManager` is a facade (has, names, status, status$); internals live in the non-exported `ConnectionRegistry` — keeps internal methods out of the public .d.ts.
- Ruling: `HeadersInit` renamed to `NatsHeadersInit`, no alias — 0.1.0 not published yet.
- Ruling: `abortStartup()` does not wait for in-flight connects (waitOnFirstConnect retries forever) and closes open ones with a 1 s budget; a late connect is closed when it opens.
- Ruling: `ci-ok` uses `if: always()` and fails unless every need succeeded — `!cancelled()` let a manually cancelled run skip the required check, which branch protection treats as passing; a red ci-ok on superseded PR commits is harmless.
- Ruling: release/CI installs use `npm ci --ignore-scripts` instead of splitting the release job — nothing released needs install scripts; devDependency code still runs in prepublishOnly with the OIDC token (residual).
- Ruling: NatsClient / JetStreamClient / NatsStreamManager constructors keep naming the internal ConnectionEntry (pre-existing; instances come from DI; TSDoc says not public API). NatsHealthIndicator / NatsTestHarness use property injection.

## Residuals
- A non-NATS provider throwing during DI: Nest runs no hook and returns no app, so already-open connections leak until process exit (docs/lifecycle.md).
- A connect still dialing after an aborted startup cannot be cancelled (nats.js gives no handle); it is closed once it opens.
- jsonCodec: structured-mode CloudEvent whose `data` is a function/symbol is still dropped silently.
- Unverified until first push: Dependabot `cooldown`/grouping keys, Nest 11 downgrade under npm 10 in CI, version-packages lockfile step (rehearsal), GitHub App token for version-PR CI (owner setup).
- docs/streams.md server-normalization claims (`duplicate_window: 0`, `max_deliver: 0`) not re-checked against a live server.
