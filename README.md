# Rift Enterprise

Private, proprietary **enterprise edition** of [Rift](https://github.com/achird-labs/rift)
— the home of the distributed Rift and other commercial features.

This repository is **open-core**: the open-source Rift is vendored read-only as a
git submodule under [`vendor/rift`](vendor/rift), and enterprise-only crates are
layered on top under [`crates/`](crates).

```
rift-enterprise/
├── Cargo.toml              # workspace: enterprise crates; vendor/rift excluded
├── vendor/rift/            # git submodule → achird-labs/rift (read-only core)
├── crates/
│   ├── rift-ee/            # enterprise facade: OSS crates + extension seams
│   ├── rift-cluster/       # distributed clustering (control plane, membership)
│   └── rift-ee-server/     # the enterprise binary: OSS server + cluster backends
├── scripts/
│   ├── sync-upstream.sh    # bump vendor/rift to upstream master
│   └── upstream-pr.sh      # open a cross-repo PR against public Rift
└── .github/workflows/
    ├── ci.yml              # fmt + clippy + test (with submodule)
    └── sync-upstream.yml   # daily submodule bump → PR
```

## Quick start

```sh
git clone --recurse-submodules git@github.com:achird-labs/rift-enterprise.git
cd rift-enterprise
cargo check --workspace
```

Run the enterprise server — identical to the open-source `rift` without
`--cluster`, a cluster node with it:

```sh
cargo run -p rift-ee-server -- --port 2525 --datadir ./data
```

See [`docs/rift-ee-server.md`](docs/rift-ee-server.md) for its flags, startup
guards, probes and the `/_cluster/*` operator surface, and
[`docs/dev-workflow.md`](docs/dev-workflow.md) for syncing the core, adding
features, and the cross-repo PR flow.

## Crate layout

The vendored core publishes its engine as **`rift-mock-core`** and its server
layer as **`rift-http-proxy`**. Enterprise crates never depend on either
directly: `rift-ee` re-exports both, along with the upstream extension seams
(`rift_ee::seams`) that enterprise backends implement. Depending only on
`rift-ee` is what keeps the open-core boundary checkable by Cargo rather than by
convention.

## Licensing

Enterprise code in this repo is proprietary — see [`LICENSE`](LICENSE). The
vendored core under `vendor/rift` remains Apache-2.0.
