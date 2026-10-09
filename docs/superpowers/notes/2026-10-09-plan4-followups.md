# Plan 4 (Release) — follow-ups

Collected from per-task and final reviews of feat/release (232c3f3..c33d3eb).

## Rulings made during execution
- Ruling: Task 6 may modify .github/workflows/ci.yml to add `npm run check:workflows` to the checks job — plan text says the script runs in CI checks but omits ci.yml from T6 Files — cost if wrong: none.
- Ruling: accept check-docs temp dir inside repo (<repo>/.docs-check-tmp, git-ignored) instead of os.tmpdir() — package self-reference via exports only resolves inside the package tree — cost if wrong: none.
- Ruling: accept example request subject 'query.orders.get' instead of 'orders.get' — a core request on a subject captured by a JetStream stream is answered by the stream's PubAck before the handler replies; worth a docs note (carried to final review) — cost if wrong: none.
- Ruling: Task 6 replaces lint:workflows bind-mount form with a portable stdin loop over .github/workflows/*.yml (`docker run -i --rm rhysd/actionlint:1.7.7 -color - < file`) and adds timeout-minutes to CI jobs while editing ci.yml — bind mount hangs on Colima; no job timeout risks 6h hangs — cost if wrong: none.
- Ruling: if changesets/action has no released support for @changesets/cli v3, downgrade to latest @changesets/cli 2.x; pin the action by commit SHA either way — unverified v3 output parsing could skip tags/GitHub Releases — cost if wrong: on CLI 2.x until the action supports v3.
- Ruling: fix final-review minors M1–M7 (aggregate ci-ok job, private-repo note, smoke fail-fast + Node 24 install, npm-friendly warning, workflow_dispatch, PubAck docs trap) in the same wave — cost if wrong: slightly larger diff.

## Deferred findings
- Task 1: minor (deferred): check-pack only checks *.js.map (not .d.ts.map); cwd-relative reads; no friendly message if npm pack fails; src-spec rule untested
- Task 2: minor (deferred): fixed temp dir name (concurrent runs), SIGTERM not handled; only ```ts fences checked (typescript/tsx skipped); heading slug edge cases; placeholder list duplicated; spec §9 option list lacks datacontenttype? token
- Task 3: minor (deferred): smoke spawnSync has no timeout; entry lookup/cpSync outside per-fixture try; SIGTERM leaks temp dir; fixture deps float on ^11/^12; app.close error can mask assertion
- Task 4: minor (deferred): example README assumes fresh NATS (msgId dedupe within 2 min on rerun); bootstrap error exits without close; docs (handlers/streams/testing) should warn that a core request on a stream-captured subject gets the PubAck; docker-compose wording; stale dist
- Task 5: deviation accepted — Nest version print reads node_modules/@nestjs/*/package.json (npm ls fails after --no-save downgrade; Nest 12 exports block package.json import)
- Task 5: minor (deferred): checks job builds twice (build + docs:check); cancel-in-progress also on main; smoke matrix lacks fail-fast:false; Nest 11 install under npm 10 (Node 22) unverified locally
- Task 6: minor (deferred): check-pack doesn't require CHANGELOG.md; version PR needs manual CI handling (PAT/App token could automate); changesets/action@v1 mutable tag; lockfile step not run end-to-end at bumped version
- Final: minor (deferred): actions/checkout & setup-node not SHA-pinned; single-job release vs recommended split sub-actions; version PR (GITHUB_TOKEN) needs close/reopen for ci-ok; ci-ok fails on cancelled superseded runs; PubAck note wording is a race in strict terms
