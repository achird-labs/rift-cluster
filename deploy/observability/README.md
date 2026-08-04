# Observability pack

Prometheus + Grafana for the 3-node `rift-cluster` compose (issue #227,
RFC-003 SS5/SS6). This closes the latency-analytics parity line without
building a TSDB: every target customer already runs Prometheus, so this pack
ships the scrape config, the recording/alert rules, and the dashboards that
make a rift fleet correct to observe — not a hosted metrics backend.

## Running it locally

```sh
docker compose -f deploy/compose/docker-compose.yml \
               -f deploy/compose/observability.overlay.yml up
```

This layers Prometheus and Grafana onto the shipped 3-node topology. The
3-node compose stays dependency-free on its own — the overlay is opt-in, and
is what `deploy/compose/verify.sh` exercises when run with
`RIFT_OBSERVABILITY=1`.

Once it's up:

| What | URL |
|---|---|
| Grafana | http://localhost:13000 |
| Prometheus | http://localhost:19091 |
| Fleet Overview dashboard | http://localhost:13000/d/rift-fleet-overview |
| Latency Analytics dashboard | http://localhost:13000/d/rift-latency-analytics |
| Verification Plane dashboard | http://localhost:13000/d/rift-verification-plane |

**Grafana admin credentials are fixed dev-only defaults** (`admin` /
`rift-observability-dev-only`, set in `deploy/compose/observability.overlay.yml`).
This stack is a local/reference deployment. It is not production guidance —
a real deployment injects its own admin credentials from a secret store, the
same way `docker-compose.yml` injects `RIFT_CLUSTER_SECRET` inline for local
use only.

## What to copy into a real fleet

- `deploy/observability/prometheus/prometheus.yml` — the scrape config.
  Point it at your fleet's `--metrics-port` (9090 by default) instead of the
  compose service names.
- `deploy/observability/prometheus/rules/recording.yml` and `alerts.yml` —
  the fleet aggregation rules and the paging pack. These are the supported
  API (see below) — dashboards and alerts should read the recording rules,
  not the raw histograms, in your own tooling too.
- `deploy/observability/grafana/dashboards/*.json` and
  `deploy/observability/grafana/provisioning/` — import as-is, or point
  Grafana's file provisioner at these paths directly. The Prometheus
  datasource UID is pinned to `rift-prometheus` (see
  `provisioning/datasources/prometheus.yml`); if your Grafana already has a
  Prometheus datasource under a different UID, either rename it to match or
  find-and-replace the UID across the dashboard JSON — the panels reference
  it by UID specifically so the dashboards stay stable across imports rather
  than depending on provisioning order.

## Never average percentiles

Every latency panel and every latency-based alert reads from Prometheus
**recording rules**, not raw histograms queried ad hoc. This is the
supported API surface for latency data in this repo, and it is a
correctness requirement, not a style preference: a percentile is not a
linear quantity, so averaging three nodes' individually-computed p99s does
not produce the fleet's p99 — it produces a number that is usually close and
occasionally very wrong, in exactly the way that hides real tail-latency
regressions. The recording rules merge at the histogram-**bucket** level
across the fleet first, and take the quantile once:

- `rift:request_latency_ms:p50` / `:p95` / `:p99` — fleet-wide proxy
  latency. Each has its own `:by_instance`, `:by_method` and `:by_fault`
  sibling (e.g. `rift:request_latency_ms:p99:by_instance`) for the per-node,
  per-method and fault-split panels. The slices carry **distinct rule
  names**, not the same name distinguished by label presence: sharing one
  name would make the bare fleet selector match every slice at once and
  render a chart that looks fine and is wrong.
- `rift:upstream_latency_ms:p50` / `:p95` / `:p99` — the upstream-call
  equivalent.
- `rift:requests:rate1m` — fleet request rate.
- `rift:match_rate:ratio5m` — see the caveat below.
- `rift:journal_entries:sum`, `rift:journal_evictions:rate5m` — journal
  health.

If you add a new latency panel or alert, add or reuse a recording rule
first. A panel querying `histogram_quantile(...)` directly against raw
buckets is a sign the rule it should have used is missing, not a shortcut to
take.

## Match-rate reads slightly HIGH

The `rift:match_rate:ratio5m` panel (Latency Analytics dashboard) is derived
from `rift_requests_unmatched_total`, which is incremented at the single
point where the pull-on-miss net concludes a miss is **terminal** — that is,
where it decides *not* to retry.

The consequence is the bias. The no-match seam grants at most one retry and
is not consulted again, so a request that was retried and missed a *second*
time reaches neither decision point: it is a genuine miss that no counter
ever sees. `rift_requests_unmatched_total` therefore undercounts, and the
match rate derived from it reads slightly **HIGH**. A rescued request is
correctly excluded — that part is exact.

Two different bounds, and the distinction matters:

- On the **counter**, the undercount is at most
  `rate(rift_cluster_pull_on_miss_retries_total[5m])` — every missed retry is
  a retry, so the retry rate is a true ceiling.
- On the **ratio**, the overstatement is at most that same quantity divided
  by the request rate:

  ```
  sum(rate(rift_cluster_pull_on_miss_retries_total[5m]))
    / sum(rate(rift_requests_total[5m]))
  ```

  Use this one when reading the match-rate panel. The un-normalised retry
  rate is not a bound on a ratio: at low traffic it is far too small (0.001
  req/s of retries against 0.002 req/s of traffic is a half-point error, not
  a 0.001 one), and at high traffic it is vacuous. The normalised expression
  is plotted next to the match-rate stat for exactly this cross-check.

## Verification Plane: the journal panels, and what is still absent

Three panels cover merge-on-read's honesty signals (issue #319, once #223
registered their families):

| panel | rule | reads as |
|---|---|---|
| Merged reads stamped partial | `rift:journal_partial_reads:rate5m` | zero is healthy; sustained non-zero means a rostered peer is unreachable and verification reads may be missing recent entries |
| Journal merge latency (in-memory) | `rift:journal_merge_seconds:p95` / `:p99` | merge cost growing with shard count or entry volume |
| Peer pull failures by peer | `rift:journal_peer_pull_failures:rate5m` | one line elevated = one bad node; every line elevated = this node is partitioned |

**The latency panel is not budget pressure.** `rift_cluster_journal_merge_seconds`
times only the in-memory k-way merge over cached shards — the peer fan-out is a
separate phase with a ~2 s budget and no histogram at all. The registered
buckets top out at 0.5 s for exactly that reason. A panel or alert reading
"approaching the budget" from this family would be reading it wrong.

Only the partial-read rate alerts (`RiftJournalReadsDegraded`, `for: 10m`).
Merge latency has no SLO to breach, and per-peer failures that do not degrade a
read are the replica cache doing its job — alerting on either would be alerting
on the system working. The 10 m window exists because transient partials during
a rolling restart are `Rift-Cluster-Partial` behaving as designed; a shorter
window pages on every deploy until someone mutes it.

**Still absent:** the proxyOnce owner-claim panels. Those families arrive with
issue #226 and are not registered yet, so the dashboard carries a text panel
saying so rather than empty "No data" panels, which would be indistinguishable
from a healthy-but-quiet system.

## Compose smoke

`deploy/compose/verify.sh` brings the base 3-node stack up and asserts it
forms a single cluster, same as always. When run with `RIFT_OBSERVABILITY=1`,
it additionally layers in this overlay and asserts Prometheus reports 3/3
scrape targets up and Grafana serves all three dashboards by UID. The
default `verify.sh` run (no env var) is unaffected and stays
dependency-free.

## What CI checks, and when

Two lanes, split by cost (issue #316 — before it, only the static lane
existed and the runtime assertions above ran nowhere):

| lane | job | runs | asserts |
|---|---|---|---|
| static | `observability` | **every PR** | `promtool check config/rules`, `promtool test rules`, every referenced `rift_*` family is registered, every dashboard parses, every panel's `datasource.uid` matches provisioning, the overlay merges |
| runtime | `observability-runtime` | only when this pack, the overlay, `verify.sh` or the gate scripts change | `RIFT_OBSERVABILITY=1 verify.sh` — Prometheus scrapes 3/3, Grafana serves the dashboards |

The runtime lane is path-gated through `scripts/cluster-smoke-paths.sh
--job observability`, whose case table is the specification of when it runs;
`--self-test` pins it and runs before the gate is trusted.

Neither lane is a **required** status check. That is deliberate: a Grafana
provisioning typo should be loud, but it should not block an unrelated PR
from merging. The required set stays `build`, `public-api`, `cluster-smoke`.
