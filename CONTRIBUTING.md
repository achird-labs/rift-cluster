# Contributing to RiftCluster

Thanks for looking. This project is **beta** and maintained by one person, so the most useful thing
you can do is tell me where it is wrong — a reproduction, a misleading doc, or a benchmark that does
not replicate is worth more to me than a feature.

Everything here is Apache-2.0. There is no paid edition and no contributor licence agreement.

## Before you write code

**Core changes do not belong in this repo.** RiftCluster is the clustering superset of
[Rift](https://github.com/achird-labs/rift). The core is vendored read-only as a submodule at
`vendor/rift`, so changes to matching, predicates, imposters, or the HTTP surface must go
[upstream](https://github.com/achird-labs/rift) first. That boundary is deliberate: it keeps generic
capability where every Rift user gets it, rather than accumulating here.

What does belong here: Raft and membership, HRW flow-state ownership, fleet verification merging,
the cluster server binary, the console, deploy assets, and the docs describing any of it.

For anything larger than a bug fix, **open an issue first**. I would rather talk about the design
than decline a finished PR.

## Getting set up

```sh
git clone --recurse-submodules https://github.com/achird-labs/rift-cluster.git
cd rift-cluster
cargo check --workspace
```

If you cloned without `--recurse-submodules`, run `git submodule update --init --recursive`. Nothing
builds until you do — this is the single most common first-run failure.

## Before you open a PR

CI runs these, so run them first:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Clippy is `-D warnings`. There is no warning budget.

The container chaos suite (`cargo test -p cluster-chaos -- --ignored`) needs Docker and is slow; CI
runs it on the paths that affect it, and you do not need it locally for most changes.

## What a good PR looks like

- **One concern.** A PR that fixes a bug and reformats a module is two PRs.
- **A test that fails without the change.** For a bug fix, the test is the evidence the bug existed.
- **A description that says why, not what.** The diff already says what.
- **Docs updated in the same PR** if behaviour changed. `docs/architecture/` and `docs/rfc/` are the
  design of this system; a change that makes them wrong is not finished.

Commit messages: imperative mood, and explain the reasoning where it is not obvious.

## Reporting a bug

Include the version (`rift-cluster-server --version`), how many nodes, and the smallest
`imposters.json` that reproduces it. If it is a performance claim, include the hardware, the load
generator and its concurrency level — this project's whole argument is measurement discipline, and
a number without its conditions cannot be acted on.

**If you think a published benchmark is unfair, say so.** The harness is in the repo. A correction
that narrows a gap I have claimed is the single most valuable issue you can file, and it will be
published as a correction rather than quietly fixed.

## Security

Do not open a public issue for a vulnerability. Use GitHub's private
[security advisory](https://github.com/achird-labs/rift-cluster/security/advisories/new) flow.
