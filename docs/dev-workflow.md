# Development workflow

`rift-enterprise` is the private, proprietary superset of the open-source
[Rift](https://github.com/achird-labs/rift). It follows an **open-core** model:

- The open-source core is vendored, read-only, as a git submodule at
  `vendor/rift`, pinned to a specific upstream commit.
- Enterprise-only crates live under `crates/` (e.g. `rift-ee`, `rift-cluster`)
  and depend on the core crates via path dependencies into `vendor/rift`.
- The enterprise workspace **excludes** `vendor/rift` (it is its own workspace),
  so `cargo` treats the core crates as ordinary path dependencies.

Because `vendor/rift` is read-only from this repo's perspective, **you cannot
change core behavior here** — core changes must go upstream first. That boundary
is the point: it keeps proprietary and open-source code cleanly separated.

## First checkout

```sh
git clone --recurse-submodules git@github.com:achird-labs/rift-enterprise.git
# or, if already cloned:
git submodule update --init --recursive
cargo check --workspace
```

## Syncing the open-source core

Automatic: the **Sync upstream Rift** GitHub Action runs daily (and on demand),
bumps `vendor/rift` to the latest public `master`, verifies the workspace still
builds, and opens a PR.

Manual/local:

```sh
scripts/sync-upstream.sh
```

## Adding an enterprise feature

1. Build it in a new or existing enterprise crate under `crates/`.
2. Depend on the core through the **`rift-ee` facade** (`rift-ee = { path =
   "../rift-ee" }`), never on `rift-mock-core` / `rift-http-proxy` directly.
   `rift-ee` re-exports the OSS crates plus the upstream extension seams under
   `rift_ee::seams`; keeping it the sole path means Cargo enforces the
   open-core boundary instead of a convention nobody checks. Only `rift-ee`
   itself carries the vendored path deps (declared in `[workspace.dependencies]`
   at the root).
3. Extend the core through those seams (flow store, sequencer, journal, proxy
   store, config reconcile + events, response decoration, server builder)
   rather than forking logic. If a seam you need does not exist, that is an
   upstream PR — see the cross-repo flow below — not a local fork.

## When an enterprise feature needs a core change first (cross-repo PR)

GitHub has no single PR that spans two repos, so split the work:

1. Make the core change **inside `vendor/rift`** and open an upstream PR:

   ```sh
   # edit files under vendor/rift/ ...
   scripts/upstream-pr.sh feat/rift-<issue>-<slug> "feat: add extension point for X"
   ```

2. Get that PR reviewed and merged into public Rift `master`.
3. Bump the submodule so the enterprise repo picks up the merged change:

   ```sh
   scripts/sync-upstream.sh
   ```

4. Now build the enterprise feature on top of the new core capability and open a
   normal PR in this repo.

Keep the upstream PR limited to genuinely open-source-appropriate changes
(extension points, bug fixes, generic capabilities). Proprietary logic stays in
`crates/` here.

## CI

`.github/workflows/ci.yml` runs fmt + clippy + test on every push/PR with the
submodule checked out. Note: PRs opened by the sync Action use the default
`GITHUB_TOKEN`, which by design does not re-trigger CI. To have CI run on sync
PRs automatically, add a repo secret with a fine-grained PAT and pass it to the
`create-pull-request` step's `token:` input.
