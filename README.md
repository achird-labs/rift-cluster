# RiftCluster

**Distributed clustering for [Rift](https://github.com/achird-labs/rift)** — an embedded Raft
control plane, HRW flow-state ownership, and a server binary that composes them with the Rift core.
Apache-2.0.

Rift is a Mountebank-compatible mock server. RiftCluster makes a *fleet* of them behave like one:
configuration replicates through Raft, imposters and flow state have a defined owner, verification
reads merge across nodes instead of answering for whichever node you happened to reach, and the
whole thing runs with no external datastore.

## Why a cluster

A single mock server is easy. A fleet behind a load balancer is where mocking quietly stops being
correct — and each of these is a thing RiftCluster is built to fix:

- **Config drift.** You `PUT` an imposter, the LB routes it to one node, and the other two never
  hear about it. Here, writes go through Raft and every node converges.
- **Per-node verification.** `GET /imposters/:port/requests` returns whatever *that* node saw, so
  assertions pass or fail depending on routing. Here, reads merge across the fleet — and say so
  honestly when a merge is incomplete rather than silently under-reporting.
- **Stateful mocking that isn't.** Scenarios and flow state live in one process, so a
  sequence-dependent mock breaks the moment traffic spreads. Here, state has an owner.
- **No external dependencies.** No Redis, no Postgres, no ZooKeeper. Clustering is embedded — the
  operational surface is the binary and its data directory.

## Status

**Pre-release.** The control plane, flow-state ownership, snapshot transfer, durability modes,
multi-tenancy, the web console and the container/Kubernetes deployment path are in `master`. No
version has been tagged yet, so **there is no published image or binary to pull** — build from
source (below) is currently the only way to run it.

Documents under `docs/` describe design as well as shipped behavior, and the two are not always the
same thing. Any performance figure in them is a design envelope, not a measurement; Rift's measured
benchmarks live in the [core repo](https://github.com/achird-labs/rift) with harness, host, version
and date attached to every number.

## Build and run

```sh
git clone --recurse-submodules git@github.com:achird-labs/rift-cluster.git
cd rift-cluster
cargo check --workspace
```

The `--recurse-submodules` matters: the Rift core is vendored under `vendor/rift` and the build
needs it.

**A single node.** Without `--cluster` this binary is the open-source Rift server, byte for byte:

```sh
cargo run -p rift-cluster-server -- --port 2525 --datadir ./data
curl -s localhost:2525/imposters
```

**A real 3-node cluster**, built and verified from the checkout:

```sh
deploy/compose/verify.sh   # builds, starts 3 nodes, asserts they form one cluster
```

The web console and the `/readyz` probe are part of the *clustered* composition — with `--cluster`
off, `/console` answers `404` and nothing binds the probe port. That is deliberate, not a missing
feature: the un-clustered binary is the open-source server and the open-source server has neither.
[`deploy/README.md`](deploy/README.md#a-single-node) has the full comparison.

## Documentation

| | |
|---|---|
| [`docs/rift-cluster-server.md`](docs/rift-cluster-server.md) | The binary: flags, startup guards, probes, the `/_cluster/*` operator surface |
| [`deploy/README.md`](deploy/README.md) | Containers, Compose, Kubernetes, Helm |
| [`docs/architecture/`](docs/architecture/README.md) | How it works, in 15 chapters — topology, control plane, read/write paths, flow state, verification, tenancy, durability, operations, testing |
| [`docs/adr/`](docs/adr) · [`docs/rfc/`](docs/rfc) | Why it works that way: the decision record and the design proposals behind each milestone |
| [`docs/dev-workflow.md`](docs/dev-workflow.md) | Syncing the vendored core, adding features, the cross-repo PR flow |

## How the repository is organised

The Rift core is vendored read-only as a git submodule under [`vendor/rift`](vendor/rift); the
cluster crates layer on top under [`crates/`](crates). **That split is a build boundary, not a
licence one** — both repositories are Apache-2.0 and nothing here is withheld from the core.

The vendored core publishes its engine as `rift-mock-core` and its server layer as
`rift-http-proxy`. Cluster crates never depend on either directly: `rift-cluster-base` re-exports
both along with the upstream extension seams (`rift_cluster_base::seams`) that cluster backends
implement. Depending only on `rift-cluster-base` keeps that boundary checkable by Cargo rather than
by convention.

## Licence

**Apache-2.0** — see [`LICENSE`](LICENSE). The vendored core is Apache-2.0 too, so the whole tree is
under one licence.

This repository previously carried a commercial licence as the proprietary half of an open-core
split. That split is closed: there is no paid edition, no withheld feature set, and no plan for
either. Some documents under `docs/` still describe the old boundary and are being brought into line.
