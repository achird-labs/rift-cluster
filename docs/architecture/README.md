# Distributed Rift — The Architecture Guide

This is the definitive guide to the design of **Rift Enterprise's distributed
edition**: a fleet of active-active Rift nodes behind a load balancer that share
one imposter set, keep stateful mocking features correct when a test's requests
are sprayed across nodes, survive full-cluster restarts without losing
configuration or flow state, and never lose an accepted admin request.

It is written to be read before the implementation exists — its purpose is to
close design gaps and misunderstandings *now*, when they are cheap. Where a
chapter states a guarantee, it also states the mechanism that provides it and
the failure modes that bound it. Nothing in here should be taken on faith: the
normative sources are RFC-001 (v3), the ADR on the control plane
([#14](https://github.com/achird-labs/rift-enterprise/issues/14)), the gap
analyses in the design vault, and — above all — the tracked issues, which carry
the machine-checkable acceptance criteria.

## How to read this guide

Chapters 1–2 give the mental model: what the system is, what it refuses to be,
and the two-plane architecture everything else hangs from. Chapters 3–7 walk the
machinery: the control plane, the write path, the read path, flow state, and the
verification plane. Chapters 8–9 cover the cross-cutting guarantees: tenancy and
security, durability and failure. Chapters 10–12 are for operators and
implementers: running it, the open-core boundary, and how correctness is
verified.

If you read only three chapters, read **1 (Overview)**, **4 (Write Path)**, and
**6 (Flow State)** — they contain the three ideas the whole design balances on.

| # | Chapter | What it answers |
|---|---------|-----------------|
| 1 | [Overview & Design Goals](01-overview.md) | What is this, what won't it be, and why two planes? |
| 2 | [Topology & Request Routing](02-topology.md) | How traffic reaches nodes: ports, spaces, gateway, load balancers |
| 3 | [The Control Plane](03-control-plane.md) | Raft membership, the replicated state machine, storage, cold start |
| 4 | [The Write Path](04-write-path.md) | Life of an admin request: barrier, intents, exactly-once effect |
| 5 | [The Read Path](05-read-path.md) | Life of a mock request: matching, stateful gates, where RPCs happen |
| 6 | [Flow State](06-flow-state.md) | Ownership, replication, durability, recovery, fencing |
| 7 | [The Verification Plane](07-verification-plane.md) | Recorded requests, counters, clears, cursors/SSE, proxyOnce |
| 8 | [Multi-Tenancy & Security](08-tenancy-security.md) | Tenants, roles, audit, cluster-internal auth |
| 9 | [Durability & Failure](09-durability-failure.md) | What survives what: the restart matrix and partition behavior |
| 10 | [Operations](10-operations.md) | Deployment, probes, metrics, runbooks, rolling upgrades, sizing |
| 11 | [The Open-Core Boundary](11-open-core.md) | What lives upstream, what stays enterprise, and how Cargo enforces it |
| 12 | [Testing & Correctness](12-testing.md) | The harness, chaos scenarios, and phase exit criteria |

## Status and source of truth

| Artifact | Role |
|---|---|
| This guide | Explanatory — the *why* and the *how it fits together* |
| RFC-001 v3 (PR #3 / issue #5) | Normative design |
| ADR-001 (issue #14) | Control-plane decision: embedded Raft (`openraft`) + `redb` |
| Issues #6–#11 (epic #12) | Phase 1 implementation, re-scoped per ADR-001 |
| Issues #15, #16, #17 | Enable/disable replication, flow-state consistency+durability, RFC-002 tenancy |
| `vendor/rift` @ v0.14.0-64 (`919495e`) | Ground truth for every upstream citation |

When this guide and an RFC/issue disagree, the RFC/issue wins and the guide has
a bug — file it.
