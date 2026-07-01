# Rift Enterprise

Private, proprietary **enterprise edition** of [Rift](https://github.com/EtaCassiopeia/rift)
— the home of the distributed Rift and other commercial features.

This repository is **open-core**: the open-source Rift is vendored read-only as a
git submodule under [`vendor/rift`](vendor/rift), and enterprise-only crates are
layered on top under [`crates/`](crates).

```
rift-enterprise/
├── Cargo.toml              # workspace: enterprise crates; vendor/rift excluded
├── vendor/rift/            # git submodule → EtaCassiopeia/rift (read-only core)
├── crates/
│   ├── rift-ee/            # enterprise facade over the OSS core
│   └── rift-cluster/       # distributed clustering (control plane, membership)
├── scripts/
│   ├── sync-upstream.sh    # bump vendor/rift to upstream master
│   └── upstream-pr.sh      # open a cross-repo PR against public Rift
└── .github/workflows/
    ├── ci.yml              # fmt + clippy + test (with submodule)
    └── sync-upstream.yml   # daily submodule bump → PR
```

## Quick start

```sh
git clone --recurse-submodules git@github.com:EtaCassiopeia/rift-enterprise.git
cd rift-enterprise
cargo check --workspace
```

See [`docs/dev-workflow.md`](docs/dev-workflow.md) for syncing the core, adding
features, and the cross-repo PR flow.

## Licensing

Enterprise code in this repo is proprietary — see [`LICENSE`](LICENSE). The
vendored core under `vendor/rift` remains Apache-2.0.
