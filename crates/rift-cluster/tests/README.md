# Cluster integration harness

`tests/cluster.rs` is the in-process 3-node integration + failover harness for the
Raft control plane (issue #11, Phase-1 subset). It stands up real `RaftNode`s over
localhost TCP through the crate's public API and asserts on cluster state — never
on log output.

## What it covers, and what it does not

| In-process (here, now) | Container-based (deferred) |
|---|---|
| `test_config_sync_converges` | Envoy front + toxiproxy partition stack |
| `test_node_rejoin` | Chaos C4 (config write both sides of a partition), C6 (UDP drop) |
| `test_cold_start` + `test_uninitialized_fleet_never_ready` | admin-API / Prometheus metric assertions |
| `test_leader_failover` | `test_graceful_leave` (needs graceful voter removal) |

The deferred column needs the `rift-cluster-server` binary (issue #10) and the HTTP
config/metrics surface (issue #9); it lands in a follow-up under #11 once those
exist. This file is the harness those scenarios will reuse the shape of.

## Running

```sh
# the whole harness
cargo test -p rift-cluster --test cluster

# one scenario
cargo test -p rift-cluster --test cluster test_node_rejoin -- --nocapture
```

Each test spins its own cluster on freshly reserved ephemeral ports and its own
temp data directories. The scenarios are **serialized** by an internal
`TEST_LOCK` — one cluster runs at a time — because concurrent clusters compete
for localhost ports and CPU, which surfaces as spurious bind failures rather than
real defects. You do not need `--test-threads=1`; the lock enforces it.

## Adding a scenario

1. Add a `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]` fn.
2. `let mut cluster = TestCluster::start(3).await;` — a converged 3-voter cluster.
3. Drive it with the harness verbs: `write_on_leader`, `kill`, `restart`,
   `leader` / `wait_for_leader`.
4. Assert with the bounded pollers: `wait_converged`, `wait_voters`. **Never
   `sleep`-then-assert** — a poller returns the real last-observed state on
   timeout, so a genuine regression fails the assertion instead of passing by luck.

Restart-based scenarios rely on **fixed ports**: `TestCluster` reserves a port per
node up front and reuses it across `restart`, so the address peers recorded in
committed membership stays valid when a node rejoins. If you add a node mid-test,
reserve its port the same way.

## Interpreting a failure

- A `wait_converged` / `wait_voters` returning `false` means the cluster did not
  reach the expected state within the deadline — a real convergence/replication
  regression, not flake. Re-run once; if it reproduces, it is a bug, not infra.
- `wait_for_leader` returning `None` means no quorum could elect — check whether a
  test killed too many voters, or whether election/heartbeat timing regressed.
- Per the §12 flake policy: infra hiccups may be retried once; an **invariant
  violation is never retried** — it is a bug to file.
