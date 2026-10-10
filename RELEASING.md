# Releasing

This document is for maintainers of `@loipv/nestjs-nats`. Releases use [Changesets](https://github.com/changesets/changesets) for versioning and the changelog, and GitHub Actions for publishing through [npm trusted publishing](https://docs.npmjs.com/trusted-publishers) (OIDC). No npm token is stored in the repository.

## 1. One-time bootstrap

npm only lets you configure a trusted publisher for a package that already exists. The first version, `0.1.0`, is therefore published by hand from a maintainer machine. After that, every release goes through the workflow.

Do the steps in this order: publish `0.1.0` by hand, tag it, configure the trusted publisher, and only then push `main` to GitHub. The first `Release` run on `main` then has nothing to do, because there are no pending changesets and `0.1.0` is already on npm.

1. Log in to npm with an account that can publish to the `@loipv` scope (2FA enabled):

   ```sh
   npm login
   ```

2. From a clean checkout of the commit you are about to push as `main`, publish `0.1.0`:

   ```sh
   npm ci --ignore-scripts
   npm publish --access public --provenance=false
   ```

   `--ignore-scripts` keeps dependency install scripts from running on the machine that holds your npm session, as in the workflow; nothing needs them. `--provenance=false` is required: `publishConfig.provenance` is `true`, and npm refuses to generate provenance outside a supported CI provider (`EUSAGE: Automatic provenance generation not supported for provider: null`). `prepublishOnly` runs the build, tarball check, package checks, typecheck and unit tests first. npm asks for a one-time password (2FA). Do not create a changeset for `0.1.0`; the first changelog entry is written by hand in `CHANGELOG.md`.

3. Tag the published commit and push the tag. The workflow creates tags and GitHub Releases only for the versions it publishes, so `0.1.0` needs this by hand:

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

   Optionally, create a GitHub Release for `v0.1.0` and paste the `0.1.0` entry from `CHANGELOG.md` as its notes.

4. On [npmjs.com](https://www.npmjs.com), open the package, then **Settings** and **Trusted Publisher**, and configure:

   | Field | Value |
   |---|---|
   | Provider | GitHub Actions |
   | Owner | `loipv` |
   | Repository | `nestjs-nats` |
   | Workflow filename | `release.yml` |
   | Environment | leave empty |

   While you are in the settings, consider restricting publishing to trusted publishers only ("Require two-factor authentication and disallow tokens").

5. Push `main`. The `Release` workflow runs and finds nothing to version or publish.

6. Rehearse the pipeline once, right after trusted publishing is configured. Open a pull request with a throwaway `patch` changeset (for example a one-line note such as "Release pipeline check"), merge it, then merge the **chore: version packages** pull request it produces (close and reopen it first so CI runs on it, see [CI gating](#ci-gating)). Confirm that `0.1.1` appears on npm with a provenance badge and that the `v0.1.1` tag and GitHub Release were created. This proves the OIDC setup, the Changesets action and the tag and release creation end to end, before a real change depends on them.

If the `Release` workflow runs before the bootstrap is done (for example because `main` was pushed first), it is expected to fail at the publish step without publishing anything. After the bootstrap, run it again from the **Actions** tab (**Release**, **Run workflow** on `main`); it is a no-op, because `0.1.0` is already on npm.

## 2. Regular release flow

1. In every pull request that changes the published package, add a changeset:

   ```sh
   npm run changeset
   ```

   Choose the bump (`patch`, `minor` or `major`) and write the changelog summary. Commit the generated file in `.changeset/` with the PR. While the package is 0.x, use `minor` for breaking changes.

2. Merge the PR into `main`. The `Release` workflow runs on the push, finds pending changesets and opens (or updates) a pull request titled **chore: version packages**. It bumps `package.json` and prepends the new entries to `CHANGELOG.md`.

3. Review the **chore: version packages** PR, close and reopen it so CI runs on it (see [CI gating](#ci-gating)), and merge it once `ci-ok` passes. The `Release` workflow runs again, finds no pending changesets and publishes the new version to npm with provenance. It also pushes the `vX.Y.Z` git tag and creates a GitHub Release with the changelog entry.

The workflow runs on pushes to `main` and can also be started by hand from the **Actions** tab (**Run workflow**). It only runs for `loipv/nestjs-nats` on `main` (forks and other branches skip it), is serialized by a `release` concurrency group, and has `id-token: write` so npm can verify the run through OIDC. Because the job holds that token and write access to the repository, it installs with `npm ci --ignore-scripts`: no dependency install script runs (native binaries such as `@swc/core` arrive as optional dependencies and need none). The package's own `prepublishOnly` still runs on publish. CI installs the same way, so a dependency that starts to need an install script fails CI first; `npm run check:workflows` guards the flag in `release.yml`.

Every action in `.github/workflows/` is pinned by full commit SHA with a `# vX.Y.Z` comment, because tags are mutable and the release job holds write permissions; `npm run check:workflows` rejects an unpinned `uses:`. Dependabot (`.github/dependabot.yml`) proposes SHA bumps weekly; review the upstream release notes before merging one. `changesets/action` is on v2: action v2 requires `@changesets/cli` v3 and refuses to run with CLI v2 (action v1 is the line for CLI v2), so upgrade the two together.

### CI gating

`release.yml` does not wait for `ci.yml`, so make CI the gate yourself:

- **Protect `main`.** Enable branch protection (or a ruleset) on `main` that requires pull requests and requires the `ci-ok` status check to pass before merging. `ci-ok` is an aggregate job in `ci.yml` that fails unless every other CI job (`checks`, `test`, `smoke`, `example`, including every matrix cell) succeeded, so it is the only check you need to require. When you add a CI job, add it to `ci-ok`'s `needs` too. This guarantees every changeset PR has passed full CI. `ci-ok` always runs (`if: always()`), so a failed, skipped, timed-out or cancelled job fails it; it is never skipped, which matters because GitHub counts a skipped required check as passing. `npm run check:workflows` checks that `ci-ok` keeps `if: always()` and lists every other job in `needs`. A new push to a pull request cancels the previous run, so the superseded commit shows a red `ci-ok`. That is harmless: branch protection checks the head commit, whose own run reports `ci-ok`. Runs for pushes to `main` are never cancelled, so every `main` commit gets a CI result.
- **The version PR gets no CI runs by itself.** The workflow opens and updates **chore: version packages** with `GITHUB_TOKEN`, and events caused by `GITHUB_TOKEN` do not trigger other workflows. Its `ci-ok` check therefore stays at "Expected — Waiting for status to be reported", and with the protection above the PR cannot be merged. To run CI on it:
  1. Open the PR, click **Close pull request**, then **Reopen pull request**. You caused the `reopened` event, not `GITHUB_TOKEN`, so `ci.yml` runs on the PR's current head commit.
  2. Wait for `ci-ok` to pass, then merge.

  Do this again after every update of the PR: each merge to `main` while the PR is open makes the workflow push a new commit to the version branch, again with `GITHUB_TOKEN`, and that commit has no `ci-ok` yet. Close and reopen right before you merge, not earlier. CI on the `main` push that produced the update should also be green; it covers the same code minus the version bump. (Removing this step needs a GitHub App token for `changesets/action`, which this repository does not set up.)
- **What gates the publish itself.** Only `prepublishOnly` runs before `npm publish`: build, `check:pack`, `check:pkg`, typecheck and the unit tests. The integration tests, the Nest 11 matrix, the smoke tests and the example app are not part of it, which is why the points above matter.

## 3. Verify provenance

After a publish:

- Open the package page on npmjs.com. The version shows a **Provenance** badge that links to the exact commit and workflow run that built it.
- Or check from a terminal:

  ```sh
  npm view @loipv/nestjs-nats version
  npm audit signatures
  ```

  `npm audit signatures`, run in a project that depends on the package, verifies registry signatures and provenance attestations.

Note that `0.1.0` was published manually and has no provenance. Provenance starts with the first version published by the workflow.

## 4. Troubleshooting

### Publish fails with an authentication error (ENEEDAUTH, E401, E403 or "OIDC token exchange failed")

Check these in order:

- **npm CLI too old.** Trusted publishing needs npm CLI 11.5.1 or newer. The workflow runs `npm install -g --ignore-scripts npm@^11.5.1` before publishing; keep that step. Confirm the version in the workflow log.
- **Missing `id-token: write`.** The `release` job must declare `permissions: id-token: write` (together with `contents: write` and `pull-requests: write`). Without it GitHub does not issue an OIDC token and npm falls back to token authentication, which fails. `npm run check:workflows` guards this.
- **Wrong trusted publisher configuration.** On npmjs.com the provider must be GitHub Actions, owner `loipv`, repository `nestjs-nats` and workflow filename exactly `release.yml` (the file name, not the workflow's display name, and case-sensitive). If you rename the workflow file, update the npm configuration too.
- **Publishing from a fork or another repository.** Trusted publishing only matches the configured repository.
- **A leftover `NPM_TOKEN` or `NODE_AUTH_TOKEN` secret.** Do not add one; the workflow must not reference `NPM_TOKEN`.

### Publish fails with a provenance error

npm only generates provenance for packages built from a **public** GitHub repository. If `loipv/nestjs-nats` is private, the publish fails while `publishConfig.provenance` is `true`. Make the repository public before the first workflow release; do not turn provenance off to work around it.

### The version PR is not opened

The workflow only opens it when `main` contains changesets that are not yet released. Check that the merged PR included a file in `.changeset/` and that the `Release` run succeeded. The repository setting **Allow GitHub Actions to create and approve pull requests** must be enabled (Settings, Actions, General).

### The version was bumped but not published

`npm publish` runs `prepublishOnly` (build, tarball check, package checks, typecheck, unit tests). Read the failing step in the workflow log, fix it on `main` and push (or, if nothing needs fixing, run the workflow again by hand from the **Actions** tab); the workflow will try to publish again. A version that is already on npm is skipped.

### A dependency needs its install script

All installs in the workflows use `--ignore-scripts`. If a new dependency only works after its install script (a native addon built by `node-gyp`, a binary downloaded in `postinstall`), CI fails first. Prefer a dependency that ships prebuilt binaries as optional dependencies. Otherwise run just that package's script after the install (`npm rebuild <package>`) in the job that needs it, and keep it out of `release.yml` unless the release really runs that code; do not drop `--ignore-scripts` for the whole install.

### Publishing a version manually

Only as a last resort, from a clean checkout of `main` at the release commit: `npm ci --ignore-scripts && npm publish --provenance=false` (provenance is only available from GitHub Actions, and `publishConfig.provenance` would otherwise make the publish fail). Then restore the normal flow.
