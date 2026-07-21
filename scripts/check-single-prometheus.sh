#!/usr/bin/env bash
# Assert the workspace links exactly ONE copy of the `prometheus` crate.
#
# Why this is worth a CI step of its own:
#
# `rift-cluster` registers its fleet gauges (`rift_cluster_members`,
# `rift_cluster_ring_epoch`, `rift_cluster_insecure`) into the `prometheus`
# crate's *global default registry*, because that is precisely what the
# open-source metrics server serves — `collect_metrics()` is a thin wrapper
# over `prometheus::gather()`. Registering there is what makes the cluster
# metrics appear on `GET /metrics` with no change to the open-source server.
#
# A second, semver-incompatible copy of `prometheus` would bring its own global
# registry, and the failure mode is silent in every way that matters:
#
#   * it compiles
#   * every test passes — a unit test inside rift-cluster only ever sees its own
#     resolved dependency graph, so it cannot detect a second copy being linked
#     into the final binary
#   * the binary runs normally
#   * `rift_cluster_*` is simply absent from /metrics, to be noticed by an
#     operator wondering where their dashboards went
#
# So the invariant is checked here rather than trusted to a comment on the
# workspace dependency.
#
# Deliberately NOT cargo-deny: expressing "one copy of this one crate" there
# means `multiple-versions = "deny"` plus a hand-maintained skip list of every
# legitimate duplicate. This workspace consumes a second vendored workspace
# (`vendor/rift`), so those duplicates already exist and are fine —
# thiserror 1.x/2.x, several icu/yoke/zerovec versions. A check that fails for
# uninteresting reasons trains people to ignore it, which costs more than it
# buys.
set -euo pipefail

cd "$(dirname "$0")/.."

# `cargo metadata` resolves the real graph, offline, with no extra tooling.
versions="$(cargo metadata --format-version 1 --all-features \
  | python3 -c '
import json, sys
meta = json.load(sys.stdin)
found = sorted({p["version"] for p in meta["packages"] if p["name"] == "prometheus"})
print("\n".join(found))
')"

count="$(printf '%s\n' "$versions" | grep -c . || true)"

if [ "$count" -eq 1 ]; then
  echo "OK: exactly one prometheus ($versions)"
  exit 0
fi

if [ "$count" -eq 0 ]; then
  # Not "fine because nothing is duplicated": if prometheus is gone entirely the
  # cluster gauges are not being registered anywhere, which is the same outcome
  # this check exists to prevent.
  echo "FAIL: no prometheus in the dependency graph."
  echo "The cluster metrics register into that crate's global registry; without"
  echo "it, rift_cluster_* reaches no /metrics endpoint."
  exit 1
fi

echo "FAIL: ${count} versions of prometheus in the dependency graph:"
printf '  %s\n' $versions
echo
echo "Each version carries its own global default registry. The open-source"
echo "metrics server serves prometheus::gather() from the copy IT links, so the"
echo "cluster gauges registered into the other copy would silently never appear"
echo "on /metrics — no error, no failing test, just missing metrics."
echo
echo "Fix by unifying the versions (see the prometheus entry in the workspace"
echo "Cargo.toml), not by relaxing this check."
exit 1
