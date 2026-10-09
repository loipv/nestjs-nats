# Changesets

This folder is managed by [Changesets](https://github.com/changesets/changesets).

Every pull request that changes the published package (code, types or runtime behavior) needs a changeset:

```sh
npm run changeset
```

Pick the bump type (`patch`, `minor` or `major`; this is a 0.x package, so breaking changes use `minor`) and write a short summary for the changelog. Commit the generated Markdown file with your PR.

When changesets reach `main`, the release workflow opens or updates a "chore: version packages" pull request. Merging that pull request publishes the new version to npm. See [RELEASING.md](../RELEASING.md) for the full flow.
