# Chapter 11 — The Open-Core Boundary

RiftCluster is layered on an unmodified, Apache-2.0 Rift. That sentence is a
product commitment, an engineering discipline, and a build-system invariant —
this chapter covers all three.

## The shape: seams upstream, brains cluster

The open-source engine knows nothing about clusters. It exposes **generic
extension seams** — traits with `Local` default implementations that preserve
single-node behavior byte-for-byte — and the cluster crates supply
cluster-aware implementations. All eight seams are merged upstream
(`achird-labs/rift#311–#318`); Phase 0 of the program is *complete*:

| Seam (upstream issue) | Upstream trait | Cluster implementation |
|---|---|---|
| #311 | `FlowStore::compare_and_set` (+`CasOutcome`) | atomic scenario transitions — also fixed an OSS race |
| #312 | `FlowStoreProvider` | per-imposter `ClusteredFlowStore` injection |
| #313 | `ResponseSequencer` / `SequenceKey` | owner/Redis sequencing (Phase 4) |
| #314 | `RequestJournal` (+ cursor reads #603) | sharded CRDT journal, vector cursors |
| #315 | `ProxyRecordingStore` (claim/release) | owner claim state machine — also fixed a stuck-pending OSS bug |
| #316 | `apply_config` + `ImposterEvent` + `stub_key` + `move_stub` | incremental reconcile as the Raft apply step |
| #317 | `ServerBuilder` / `run_metrics_server` / `dispatch_to_port` | `rift-cluster-server` composes instead of forking `main.rs` |
| #318 | `BackendUnavailable` + `annotate()` + `ResponseDecorator` | every `Rift-Cluster-*` header, without core handlers knowing what a cluster is |

The pattern in #318 deserves a sentence: cluster backends *annotate* the
request task-locally ("degraded: kv-adopt", "revision: 421"), and an
cluster decorator translates annotations into response headers. The OSS
handlers never learn cluster vocabulary — which is what keeps the seams
honestly generic and upstreamable.

Four further seams are queued, same rules — generic names, `Local`/default-off
behavior, independently justifiable to an OSS maintainer: U-9 `AdminAuthorizer`
and U-10 principal-on-events (RFC-002, Chapter 8), U-11 the front-door route
table and listener (#19, Chapter 13), and U-12 the `ImposterSource` provider
trait with `file`/`https` built-ins (#20, Chapter 13).

## The dependency architecture

```mermaid
flowchart BT
    subgraph vendored["vendor/rift — Apache-2.0, read-only submodule @ pinned commit"]
        MC[rift-mock-core]
        HP[rift-http-proxy]
        TY[rift-types]
    end
    EE["rift-cluster-base — the facade<br/>re-exports crates + rift_cluster_base::seams"]
    CL["rift-cluster<br/>Raft, ring, RPC, stores, reconciler"]
    SV["rift-cluster-server (binary)<br/>CLI superset, composition"]
    SP["rift-cluster-spec<br/>OpenAPI 3.0 → imposter JSON<br/><i>reaches nothing — see below</i>"]

    MC --> EE
    HP --> EE
    TY --> EE
    EE --> CL
    EE --> SV
    CL --> SV
    SP --> SV

    style EE fill:#fff3cd,stroke:#b8860b
    style SP fill:#e7f5e7,stroke:#2d7a2d
```

**`rift-cluster-base` is the single doorway.** It alone carries path dependencies into
the submodule; `rift-cluster` and `rift-cluster-server` depend on `rift-cluster-base` and
nothing vendored. The consequence: reaching around the facade *fails to
resolve* — the boundary is enforced by Cargo, not by review vigilance. A
compile-time test in `rift-cluster-base` (`seams_resolve`) names every re-exported seam,
so an upstream rename breaks loudly at the facade with a one-line fix, instead
of surfacing as a confusing error deep in cluster code.

**A crate that needs no doorway is the cheapest kind.** `rift-cluster-spec` (RFC-004 §3.1,
issue #277) compiles an OpenAPI 3.0 document into imposter JSON and depends on neither the
facade nor anything vendored — not even `rift-types`. The alternative, emitting a typed
`ImposterConfig`, was checked and rejected: under the facade rule "typed output" means a
`rift-cluster-base` dependency, which drags the whole engine into what is a text-to-text
function. Instead it emits the same JSON a client would `PUT`, and `rift-cluster-server`
admits it through the gate every other write already passes. Type safety stays where it is
load-bearing — at admission — and the compiler stays a pure function of `(spec bytes,
options)` that golden files can pin. Read the arrow as *"produces JSON consumed by"*, not as
a code dependency in the other direction.

**One seam cannot be guarded that way, and gets its own tripwire.**
`ServerBuilder::manager()` is all-or-nothing: injecting a manager replaces
upstream's internal construction *wholesale*, so `compose::cluster_manager`
hand-mirrors it. A rename breaks the build, but an upstream **addition** — a
new `with_*` inside the `None` arm — does not: the clustered path just silently
stops getting it, at a pin bump, in a file nobody here edited. `rift-cluster-server`'s
`manager_parity` test compares the set of builder calls at the two construction
sites and fails naming the one that diverged (issue #30). When it fires during a
bump, mirror the call into `cluster_manager` or record it in that test's
`INTENTIONALLY_NOT_MIRRORED` with a reason — the point is that the divergence
becomes a decision instead of an accident.

Feature discipline rides the same manifest: `rift-http-proxy` is consumed
without its binary-only allocator default, with `redis-backend` / `javascript`
/ `quamina-matching` forwarded *explicitly* — because upstream's own history
(#777) proved that a default-on feature reaches nobody through a
`default-features = false` consumer, silently, with CI green.

## The read-only submodule and the two-repo flow

`vendor/rift` is pinned to an exact commit; CI builds against the pin, and a
daily job proposes bumps as reviewable PRs. Core changes are **never made
here** — the flow for "cluster feature needs a core capability" is: patch
inside `vendor/rift` → `scripts/upstream-pr.sh` opens the PR against
`achird-labs/rift` → merge upstream → `scripts/sync-upstream.sh` bumps the
pin → build the cluster feature. The friction is the point: every generic
capability lands where every Rift user gets it, and this repo holds only what is
genuinely cluster-specific. Both repos are Apache-2.0, so the boundary is about
where code *belongs*, not about what is withheld.

## What is cluster, and why it holds

Everything in `rift-cluster` and `rift-cluster-server`: the Raft control plane and
its storage, the ownership ring and fencing, HMAC RPC, the flow-state durable
tier, the sharded journal and vector cursors, the proxyOnce owner machine, the
Redis-strict backends, tenancy/RBAC/audit, `/_cluster/*`, the chaos harness
and k8s manifests.

The commercial moat, assessed honestly (decision D-6): it is **not** the trait
implementations — any competent team can implement `FlowStore` over a shared
Redis in days, and the existing OSS Redis flow store already covers
multi-instance scenario state for teams that accept a Redis dependency. What
is genuinely hard to reproduce is the *system*: zero-dependency
self-clustering with a durable, linearizable control plane; correctness under
partition with an honest, tested degradation contract; cluster-merged
verification; fleet operations. That is a product, not a patch — and pricing
follows the product, with the core single node remaining genuinely excellent so
the funnel stays honest.
