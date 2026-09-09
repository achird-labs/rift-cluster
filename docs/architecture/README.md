# Distributed Rift — The Architecture Guide

This is the definitive guide to the design of **RiftCluster's distributed
edition**: a fleet of active-active Rift nodes behind a load balancer that share
one imposter set, keep stateful mocking features correct when a test's requests
are sprayed across nodes, survive full-cluster restarts without losing
configuration or flow state, and never lose an accepted admin request.

It was written to be read before the implementation existed; the system it
describes is now built, and narrowed — RFC-007 (**D-71**) reduced it to the
distributed core, and two chapters are retired. Where a chapter states a
guarantee, it also states the mechanism that provides it and the failure modes
that bound it. Nothing in here should be taken on faith: the normative source is
[the decision register](../decisions/DECISIONS.md), and where this guide and a
`D-n` disagree, the register wins and the guide has a bug.

## How to read this guide

Chapters 1–2 give the mental model: what the system is, what it refuses to be,
and the two-plane architecture everything else hangs from. Chapters 3–6 walk the
machinery: the control plane, the write path, the read path, and flow state —
which since #552 also carries proxyOnce, chapter 7 being retired. Chapter 9
covers the cross-cutting guarantee: what survives what. Chapters 10–13 are for
operators and implementers: running it, the upstream boundary, how correctness
is verified, and the router. Chapters 14–15 are the two cloud deployments.

Two chapters are **retired** and kept only as history and as link targets — 7
(the verification plane, D-74) and 8 (multi-tenancy and security, D-73). Each
opens with a callout saying what replaced it and where its still-true sections
moved. Nothing below their callouts describes running code.

If you read only three chapters, read **1 (Overview)**, **4 (Write Path)**, and
**6 (Flow State)** — they contain the three ideas the whole design balances on.

| # | Chapter | What it answers |
|---|---------|-----------------|
| 1 | [Overview & Design Goals](01-overview.md) | What is this, what won't it be, and why two planes? |
| 2 | [Topology & Request Routing](02-topology.md) | How traffic reaches nodes: ports, spaces, gateway, load balancers |
| 3 | [The Control Plane](03-control-plane.md) | Raft membership, the replicated state machine, storage, cold start |
| 4 | [The Write Path](04-write-path.md) | Life of an admin request: barrier, intents, exactly-once effect |
| 5 | [The Read Path](05-read-path.md) | Life of a mock request: matching, stateful gates, where RPCs happen |
| 6 | [Flow State](06-flow-state.md) | Ownership, replication, durability, recovery, fencing — and proxyOnce owner claims, moved here from Ch. 7 |
| 7 | ~~[The Verification Plane](07-verification-plane.md)~~ | **Retired** by D-71/D-74 (#552). The journal is upstream's own, per node; proxyOnce moved to Ch. 6 |
| 8 | ~~[Multi-Tenancy & Security](08-tenancy-security.md)~~ | **Retired** by D-71/D-73 (#550). The peer secret moved to Ch. 3; sessions and `/_fleet/*` to Ch. 10 |
| 9 | [Durability & Failure](09-durability-failure.md) | What survives what: the restart matrix and partition behavior |
| 10 | [Operations](10-operations.md) | Deployment, probes, the admin credential and sessions, runbooks, rolling upgrades, sizing |
| 11 | [The Upstream Boundary](11-upstream-boundary.md) | What lives upstream, what stays cluster, and how Cargo enforces it |
| 12 | [Testing & Correctness](12-testing.md) | The harness, chaos scenarios, and phase exit criteria |
| 13 | [The Router](13-router.md) | Single-port content routing: the route table, deterministic order, bind divergence |
| 14 | [Deploying on AWS](14-cloud-deployment.md) | EKS reference deployment, ECS/Fargate caveats, EC2, cost & checklist |
| 15 | [Deploying on Azure](15-azure-deployment.md) | AKS reference, why Container Apps is unsupported, VMSS, cost — Ch.14's checklist in Azure clothes |

## Status and source of truth

| Artifact | Role |
|---|---|
| [`docs/decisions/DECISIONS.md`](../decisions/DECISIONS.md) | **The decision register.** The only place a `D-n` is defined; it wins over everything below |
| [RFC-007](../rfc/RFC-007-distributed-core.md) (**D-71**) | The scope: what the cluster is, and what was removed to make it that |
| [ADR-001](../adr/ADR-001-raft-control-plane.md) (accepted) | Control-plane decision: embedded Raft (`openraft`) + `redb` |
| [RFC-001](../rfc/RFC-001-self-clustering-rift.md) v3.2 | The original design. Largely built; §7.5 (bar §7.5.3), §7.5.1, §7.5.2, §9, §10, §11.1, Appendix B and §8.1's monetization-boundary paragraph are retired and carry callouts |
| RFC-002, RFC-005 | Superseded in full (D-73, D-71). Read as history only |
| This guide | Explanatory — the *why* and the *how it fits together*. Must not contradict a decision |
| [`docs/design-index.toml`](../design-index.toml) | When each chapter was last read against the code, and by which sha |
| `vendor/rift` @ `v0.17.0-50-gb0bef7d` | Ground truth for every upstream citation |

When this guide and the register disagree, the register wins and the guide has
a bug — fix it in the same PR, or file it.

`scripts/design-check.py --strict` checks what can be checked mechanically:
every citation resolves, every amended section carries its callout, and the
register is well-formed. Whether a chapter's prose is *true* is a reading, and
`design-index.toml` records honestly when nobody has done one.
