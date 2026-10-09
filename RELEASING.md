# Releasing

This document is for maintainers of `@loipv/nestjs-nats`. Releases use [Changesets](https://github.com/changesets/changesets) for versioning and the changelog, and GitHub Actions for publishing through [npm trusted publishing](https://docs.npmjs.com/trusted-publishers) (OIDC). No npm token is stored in the repository.

## 1. One-time bootstrap

npm only lets you configure a trusted publisher for a package that already exists. The first version, `0.1.0`, is therefore published by hand from a maintainer machine. After that, every release goes through the workflow.

1. Log in to npm with an account that can publish to the `@loipv` scope (2FA enabled):

   ```sh
   npm login
   ```

2. From a clean checkout of `main`, publish `0.1.0`:

   ```sh
   npm ci
   npm publish --access public --provenance=false
   ```

   `--provenance=false` is required: `publishConfig.provenance` is `true`, and npm refuses to generate provenance outside a supported CI provider (`EUSAGE: Automatic provenance generation not supported for provider: null`). `prepublishOnly` runs the build, tarball check, package checks, typecheck and unit tests first. npm asks for a one-time password (2FA). Do not create a changeset for `0.1.0`; the first changelog entry is written by hand in `CHANGELOG.md`.

3. On [npmjs.com](https://www.npmjs.com), open the package, then **Settings** and **Trusted Publisher**, and configure:

   | Field | Value |
   |---|---|
   | Provider | GitHub Actions |
   | Owner | `loipv` |
   | Repository | `nestjs-nats` |
   | Workflow filename | `release.yml` |
   | Environment | leave empty |

   While you are in the settings, consider restricting publishing to trusted publishers only ("Require two-factor authentication and disallow tokens").

The `Release` workflow runs on every push to `main`. If it runs before this bootstrap is done, it is expected to fail at the publish step without publishing anything. After the bootstrap, re-running it is a no-op, because `0.1.0` is already on npm.

## 2. Regular release flow

1. In every pull request that changes the published package, add a changeset:

   ```sh
   npm run changeset
   ```

   Choose the bump (`patch`, `minor` or `major`) and write the changelog summary. Commit the generated file in `.changeset/` with the PR. While the package is 0.x, use `minor` for breaking changes.

2. Merge the PR into `main`. The `Release` workflow runs on the push, finds pending changesets and opens (or updates) a pull request titled **chore: version packages**. It bumps `package.json` and prepends the new entries to `CHANGELOG.md`.

3. Review and merge the **chore: version packages** PR. The `Release` workflow runs again, finds no pending changesets and publishes the new version to npm with provenance. It also creates the GitHub tag.

The workflow only runs for `loipv/nestjs-nats` (forks skip it), is serialized by a `release` concurrency group, and has `id-token: write` so npm can verify the run through OIDC.

### CI gating

`release.yml` does not wait for `ci.yml`, so make CI the gate yourself:

- **Protect `main`.** Enable branch protection (or a ruleset) on `main` that requires pull requests and requires the CI jobs (`checks`, `test`, `smoke`, `example`) to pass before merging. This guarantees every changeset PR has passed full CI.
- **The version PR gets no CI runs.** Pull requests opened by the workflow with `GITHUB_TOKEN` do not trigger other workflows, so **chore: version packages** shows no CI checks. Before merging it, confirm that CI on the `main` push that triggered the latest version PR update is green. Alternatively, close and reopen the PR to trigger CI on it (and satisfy required checks, if you require them for it).
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

- **npm CLI too old.** Trusted publishing needs npm CLI 11.5.1 or newer. The workflow runs `npm install -g npm@^11.5.1` before publishing; keep that step. Confirm the version in the workflow log.
- **Missing `id-token: write`.** The `release` job must declare `permissions: id-token: write` (together with `contents: write` and `pull-requests: write`). Without it GitHub does not issue an OIDC token and npm falls back to token authentication, which fails. `npm run check:workflows` guards this.
- **Wrong trusted publisher configuration.** On npmjs.com the provider must be GitHub Actions, owner `loipv`, repository `nestjs-nats` and workflow filename exactly `release.yml` (the file name, not the workflow's display name, and case-sensitive). If you rename the workflow file, update the npm configuration too.
- **Publishing from a fork or another repository.** Trusted publishing only matches the configured repository.
- **A leftover `NPM_TOKEN` or `NODE_AUTH_TOKEN` secret.** Do not add one; the workflow must not reference `NPM_TOKEN`.

### The version PR is not opened

The workflow only opens it when `main` contains changesets that are not yet released. Check that the merged PR included a file in `.changeset/` and that the `Release` run succeeded. The repository setting **Allow GitHub Actions to create and approve pull requests** must be enabled (Settings, Actions, General).

### The version was bumped but not published

`npm publish` runs `prepublishOnly` (build, tarball check, package checks, typecheck, unit tests). Read the failing step in the workflow log, fix it on `main` and push; the workflow will try to publish again. A version that is already on npm is skipped.

### Publishing a version manually

Only as a last resort, from a clean checkout of `main` at the release commit: `npm ci && npm publish --provenance=false` (provenance is only available from GitHub Actions, and `publishConfig.provenance` would otherwise make the publish fail). Then restore the normal flow.
