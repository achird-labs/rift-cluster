# RiftCluster

**Apache-2.0.** Distributed clustering for [Rift](https://github.com/achird-labs/rift) —
an embedded Raft control plane, HRW flow-state ownership, and a server binary that
composes them with the Rift core.

The Rift core is vendored read-only as a git submodule under
[`vendor/rift`](vendor/rift), and the cluster crates are layered on top under
[`crates/`](crates). **The split is a build boundary, not a licence one** — both
repositories are Apache-2.0 and nothing here is withheld from the core.

> **Status: Phase 1.** What ships is the control plane, flow-state ownership,
> snapshot transfer and the durability modes. Later phases in `docs/` are design,
> not documentation of running code — and any performance figure in them is a
> design envelope rather than a measurement. Rift's measured benchmarks live in
> the [core repo](https://github.com/achird-labs/rift), with the harness, host,
> version and date attached to every number.

```
rift-cluster/
├── Cargo.toml                  # workspace: cluster crates; vendor/rift excluded
├── vendor/rift/                # git submodule → achird-labs/rift (read-only core)
├── crates/
│   ├── rift-cluster-base/      # facade: core crates + the upstream extension seams
│   ├── rift-cluster/           # distributed clustering (control plane, membership)
│   └── rift-cluster-server/    # the binary: core server + cluster backends
├── deploy/
│   ├── Dockerfile              # the rift-cluster-server image
│   ├── compose/                # a real 3-node cluster, with a verify script
│   └── k8s/                    # StatefulSet + Services + probes
├── scripts/
│   ├── sync-upstream.sh    # bump vendor/rift to upstream master
│   └── upstream-pr.sh      # open a cross-repo PR against public Rift
└── .github/workflows/
    ├── ci.yml              # fmt + clippy + test (with submodule)
    └── sync-upstream.yml   # daily submodule bump → PR
```

## Quick start

```sh
git clone --recurse-submodules git@github.com:achird-labs/rift-cluster.git
cd rift-cluster
cargo check --workspace
```

Run the cluster server — identical to the open-source `rift` without
`--cluster`, a cluster node with it:

```sh
cargo run -p rift-cluster-server -- --port 2525 --datadir ./data
```

Or run a real 3-node cluster:

```sh
deploy/compose/verify.sh   # builds, starts 3 nodes, asserts they form one cluster
```

See [`docs/rift-cluster-server.md`](docs/rift-cluster-server.md) for its flags, startup
guards, probes and the `/_cluster/*` operator surface,
[`deploy/README.md`](deploy/README.md) for containers and Kubernetes, and
[`docs/dev-workflow.md`](docs/dev-workflow.md) for syncing the core, adding
features, and the cross-repo PR flow.

## Crate layout

The vendored core publishes its engine as **`rift-mock-core`** and its server layer
as **`rift-http-proxy`**. Cluster crates never depend on either directly:
`rift-cluster-base` re-exports both, along with the upstream extension seams
(`rift_cluster_base::seams`) that cluster backends implement. Depending only on
`rift-cluster-base` is what keeps that boundary checkable by Cargo rather than by
convention — it is a dependency-hygiene rule, not a licence one.

## Licensing

**Apache-2.0** — see [`LICENSE`](LICENSE). The vendored core under `vendor/rift`
is Apache-2.0 too, so the whole tree is under one licence.

This repo previously carried a commercial licence as the proprietary half of an
open-core split. That split is closed: there is no paid edition, no withheld
feature set, and no plan for either.
