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

Nothing here needs a checkout, a submodule or a Rust toolchain. Tagging `vX.Y.Z`
publishes a multi-arch image and per-platform binaries, both carrying the web
console.

> **While this repository is private**, both the image and the raw file below are
> too: `docker login ghcr.io` with a personal access token carrying `read:packages`
> first, and fetch `cluster.yml` from a checkout or the release page rather than
> `raw.githubusercontent.com`. Everything else on this page is unchanged. This
> note goes away when the repo does.

**A single node**, with the console and the probe endpoints:

```sh
docker run -p 2525:2525 -p 2526:2526 \
  -e RIFT_CLUSTER=true \
  -e RIFT_CLUSTER_BIND=127.0.0.1:4790 \
  -e RIFT_CLUSTER_ALLOW_SOLO=true \
  -e RIFT_CLUSTER_SECRET=local-dev-secret \
  ghcr.io/achird-labs/rift-cluster-server:0.1.0

curl -s localhost:2525/imposters          # admin API
open http://localhost:2525/console        # the web console
```

A cluster of one, rather than the plain server, because **the console and the
`/readyz` probe are part of the clustered composition**: with `--cluster` off
this binary is the open-source server byte for byte, and the open-source server
has neither. `docker run -p 2525:2525 <image>` on its own is perfectly valid and
gives you exactly that — a Mountebank-compatible mock server on `:2525` — but
`/console` answers `404`, nothing binds `2526`, and because the image's health
check probes `2526` Docker will mark the container `unhealthy` while it serves
happily. See [`deploy/README.md`](deploy/README.md#a-single-node) for the whole
comparison.

**A real 3-node cluster**, from the published image — one file, no clone:

```sh
curl -sSLO https://raw.githubusercontent.com/achird-labs/rift-cluster/master/deploy/compose/cluster.yml
docker compose -f cluster.yml up -d

curl -s localhost:12526/readyz            # rift-1 probes
curl -s localhost:19090/metrics | grep rift_cluster
```

**Or a binary**, from the GitHub Release — see
[*Installing from a release*](docs/rift-cluster-server.md#installing-from-a-release)
for the checksum and macOS quarantine steps.

## Building from source

Only needed to develop RiftCluster itself; running it needs none of this.

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

Or build and run a real 3-node cluster from this checkout:

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
