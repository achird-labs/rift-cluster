#!/usr/bin/env bash
#
# check-observability-families.sh — the machine-checkable form of issue
# #227's acceptance criterion "no rule or panel references an unregistered
# metric family."
#
# Prometheus and Grafana both accept a typo'd metric name silently: a
# recording/alerting rule over a family that was never registered just
# evaluates to an empty vector forever, and a dashboard panel bound to it
# renders a quiet "No data" that looks identical to a healthy-but-quiet
# system. Neither fails loud, so this has to be a script, not something
# trusted to a reviewer's eye.
#
# The "known families" list is DERIVED, not hand-maintained: it comes from
# grepping the two registration sites for quoted `rift_*` string literals.
# A hand-maintained allowlist here would drift from the source the moment
# someone adds or renames a metric in Rust — grepping the registration call
# sites means this list is exactly as current as the code that emits
# `/metrics`.
#
# What counts as a "reference" is deliberately narrower than "the string
# appears in the file": only the value of an `expr:` key in Prometheus rule
# YAML, and the value of an `expr` key (at any nesting depth) in Grafana
# dashboard JSON, count. A metric name mentioned in a YAML comment or a
# dashboard's markdown documentation panel is prose, not a query — and this
# repo's own verification-plane.json dashboard deliberately DOCUMENTS three
# not-yet-registered journal families by name (explaining why their panels
# are absent, per issue #223/#226) specifically so an empty panel does not
# get silently misread as "no problem." A blunter "anywhere in the file"
# scan would fail that dashboard for doing the responsible thing.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CLUSTER_METRICS_SRC="$REPO_ROOT/crates/rift-cluster/src/metrics.rs"
ENGINE_METRICS_SRC="$REPO_ROOT/vendor/rift/crates/rift-mock-core/src/extensions/metrics.rs"
# Overridable so `--self-test` can point the scan at a throwaway fixture tree.
# Nothing but that self-test sets it; the default is the real pack.
OBSERVABILITY_DIR="${OBSERVABILITY_DIR:-$REPO_ROOT/deploy/observability}"

# `--self-test`: exercise this script's own failure branches against throwaway
# fixture trees.
#
# Issue #316 is about a gate that never ran. Adding gates here without a way to
# prove they FIRE would repeat that mistake one layer down: they run against the
# real pack, which is well-formed, so no failure branch is ever exercised and a
# regression in a detector would be indistinguishable from "nothing is broken."
# Each case re-invokes this script with `OBSERVABILITY_DIR` pointed at a fixture
# and asserts only the exit status — the same shape as
# `cluster-smoke-paths.sh --self-test`.
seed_fixture() {
  mkdir -p "$1/grafana/provisioning/datasources" "$1/grafana/dashboards" "$1/prometheus/rules"
  printf 'apiVersion: 1\ndatasources:\n  - name: Prometheus\n    uid: test-ds\n' \
    > "$1/grafana/provisioning/datasources/prometheus.yml"
  printf 'groups: []\n' > "$1/prometheus/rules/recording.yml"
  printf '{"panels":[{"datasource":{"type":"prometheus","uid":"test-ds"},"targets":[{"expr":"rift_requests_total"}]}]}\n' \
    > "$1/grafana/dashboards/ok.json"
}

self_test() {
  local root failures=0 total=0
  root="$(mktemp -d)"

  run_case() { # want(pass|fail) label dir
    local rc=0 got
    total=$((total + 1))
    OBSERVABILITY_DIR="$3" "$0" >/dev/null 2>&1 || rc=$?
    got=pass
    [ "$rc" -ne 0 ] && got=fail
    if [ "$got" = "$1" ]; then
      printf '  ok   %-4s %s\n' "$1" "$2"
    else
      printf '  FAIL want=%s got=%s  %s\n' "$1" "$got" "$2"
      failures=$((failures + 1))
    fi
  }

  seed_fixture "$root/clean"
  run_case pass "a well-formed pack passes" "$root/clean"

  # The vacuity guard this whole script exists to prevent, one layer down: the
  # family scan ends `|| true`, so before the validity pass a malformed
  # dashboard contributed nothing and the check reported success.
  seed_fixture "$root/malformed"
  printf '{"panels":[{"expr":\n' > "$root/malformed/grafana/dashboards/broken.json"
  run_case fail "a dashboard that does not parse is caught" "$root/malformed"

  seed_fixture "$root/mismatch"
  printf '{"panels":[{"datasource":{"type":"prometheus","uid":"WRONG"},"targets":[{"expr":"rift_requests_total"}]}]}\n' \
    > "$root/mismatch/grafana/dashboards/bad-uid.json"
  run_case fail "a panel naming an unpinned datasource uid is caught" "$root/mismatch"

  # Grafana's built-ins and template placeholders are spelled as uids but name
  # no datasource. Flagging them would fail every dashboard exported from the
  # UI — a gate that cries wolf on the standard authoring path.
  seed_fixture "$root/builtins"
  printf '{"annotations":{"list":[{"datasource":{"type":"grafana","uid":"-- Grafana --"}}]},"panels":[{"datasource":{"type":"prometheus","uid":"${DS_PROMETHEUS}"},"targets":[{"expr":"rift_requests_total"}]}]}\n' \
    > "$root/builtins/grafana/dashboards/exported.json"
  run_case pass "Grafana built-in and template uids are not offenders" "$root/builtins"

  # Fail closed: a verification gate that cannot find what it verifies must not
  # report success.
  seed_fixture "$root/no-provisioning"
  rm "$root/no-provisioning/grafana/provisioning/datasources/prometheus.yml"
  run_case fail "missing datasource provisioning fails closed" "$root/no-provisioning"

  seed_fixture "$root/unregistered"
  printf '{"panels":[{"datasource":{"type":"prometheus","uid":"test-ds"},"targets":[{"expr":"rift_not_a_real_family"}]}]}\n' \
    > "$root/unregistered/grafana/dashboards/unknown.json"
  run_case fail "a panel over an unregistered family is still caught" "$root/unregistered"

  rm -rf "$root"

  echo
  if [ "$failures" -eq 0 ]; then
    echo "OK: ${total} self-test cases behave as specified"
    return 0
  fi
  echo "FAIL: ${failures}/${total} self-test cases wrong."
  echo "A detector that no longer fires is indistinguishable from a clean pack."
  return 1
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
  exit $?
fi

for f in "$CLUSTER_METRICS_SRC" "$ENGINE_METRICS_SRC"; do
  if [[ ! -f "$f" ]]; then
    echo "FAIL: registration source not found: $f" >&2
    exit 1
  fi
done

if [[ ! -d "$OBSERVABILITY_DIR" ]]; then
  echo "FAIL: observability directory not found: $OBSERVABILITY_DIR" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "FAIL: jq is required to scan Grafana dashboard JSON and was not found on PATH" >&2
  exit 1
fi

# Every dashboard must be valid JSON, asserted BEFORE anything tries to read a
# field out of it (issue #316).
#
# The family scan below ends `|| true`, which is correct for its own purpose —
# a dashboard legitimately containing no `expr` at all makes the pipeline exit
# non-zero — but it also swallowed `jq`'s parse error. A malformed dashboard
# therefore contributed zero referenced families and sailed through: the check
# reported success having scanned nothing, which is the exact failure shape
# this whole script exists to prevent, one layer down. `jq empty` is the
# cheapest possible parse-only assertion.
malformed=()
while IFS= read -r -d '' f; do
  # jq's own diagnostic is kept: "does not parse" tells you which file is
  # broken, "line 47: unexpected }" tells you what to do about it.
  if ! parse_error="$(jq empty "$f" 2>&1)"; then
    malformed+=("$f: $parse_error")
  fi
done < <(find "$OBSERVABILITY_DIR" -name '*.json' -print0)

if [[ ${#malformed[@]} -gt 0 ]]; then
  echo "FAIL: dashboard JSON does not parse:" >&2
  printf '  %s\n' "${malformed[@]}" >&2
  echo >&2
  echo "A dashboard that does not parse contributes no metric references, so the" >&2
  echo "families check below would pass having scanned nothing." >&2
  exit 1
fi

# Every dashboard panel must name the datasource UID the provisioning file
# actually pins (issue #316).
#
# A mismatch is invisible to every other check here: the dashboard parses, its
# `expr` values name registered families, Prometheus is up and scraping — and
# Grafana still renders "Datasource <uid> was not found" on every panel. It is
# equally invisible to the runtime lane's assertions, which check that
# dashboards are *served*, not that their panels resolve.
# Missing provisioning is a hard failure, not a skip. Every other absent input
# in this script fails closed (the registration sources, the observability
# directory, an empty known-families list); a verification gate that quietly
# does nothing when it cannot find what it verifies is the same fail-green
# shape as a gate that is never invoked, which is the whole of issue #316.
datasource_provisioning="$OBSERVABILITY_DIR/grafana/provisioning/datasources/prometheus.yml"
if [[ ! -f "$datasource_provisioning" ]]; then
  echo "FAIL: datasource provisioning not found: $datasource_provisioning" >&2
  echo "Panels name a uid; with nothing pinning one, no panel can resolve." >&2
  exit 1
fi

# `|| true` for the same `set -euo pipefail` reason as `verify.sh`'s poll: a
# non-matching grep exits 1, pipefail promotes it, the assignment fails, and
# `set -e` kills the script — making the "no uid found" branch below
# unreachable. `head -1` can also SIGPIPE the grep on a longer file, which this
# covers too.
pinned_uid="$(grep -oE '^[[:space:]]*uid:[[:space:]]*[^[:space:]]+' "$datasource_provisioning" \
  | head -1 | sed -E 's/^[[:space:]]*uid:[[:space:]]*//' || true)"

if [[ -z "$pinned_uid" ]]; then
  echo "FAIL: no datasource uid found in $datasource_provisioning" >&2
  echo "Panels reference a uid; if provisioning pins none, nothing resolves." >&2
  exit 1
fi

# Only Prometheus-typed datasources are compared. Grafana's own built-ins are
# spelled as uids too — `-- Grafana --`, `-- Mixed --`, `-- Dashboard --` ride
# along in the `annotations` block of every dashboard exported from the UI, and
# a provisioning-variable placeholder is `${DS_PROMETHEUS}`. None of those name
# this datasource and none of them should be flagged: the current dashboards are
# hand-written and carry none, so a blunter filter is green today and fails
# spuriously on the first dashboard added the normal way. A gate that cries wolf
# on the standard authoring path is a gate people learn to route around.
uid_offenders=()
while IFS= read -r -d '' f; do
  while IFS= read -r uid; do
    [[ -z "$uid" || "$uid" == "$pinned_uid" ]] && continue
    case "$uid" in
      '-- '*' --' | '${'*'}') continue ;;
    esac
    uid_offenders+=("$(basename "$f"): $uid")
  done < <(jq -r '[.. | objects | select(has("datasource")) | .datasource | objects
                  | select((.type // "prometheus") == "prometheus") | .uid? // empty] | .[]' "$f")
done < <(find "$OBSERVABILITY_DIR" -name '*.json' -print0)

if [[ ${#uid_offenders[@]} -gt 0 ]]; then
  echo "FAIL: dashboard panels name a datasource uid that provisioning does not pin ($pinned_uid):" >&2
  printf '  %s\n' "${uid_offenders[@]}" >&2
  exit 1
fi

known_families="$(mktemp)"
referenced_families="$(mktemp)"
trap 'rm -f "$known_families" "$referenced_families"' EXIT

# Known families: every quoted `rift_...` string literal at either
# registration site, deduplicated. This intentionally also picks up names
# that appear only in `.expect(...)` messages or test assertions (e.g.
# `assert!(metrics.contains("rift_requests_total"))`) — harmless
# over-inclusion, since those always echo a name that is also the first
# argument to a `register_*!` call in the same file.
grep -hoE '"rift_[A-Za-z0-9_]*"' "$CLUSTER_METRICS_SRC" "$ENGINE_METRICS_SRC" \
  | tr -d '"' \
  | sort -u \
  > "$known_families"

if [[ ! -s "$known_families" ]]; then
  echo "FAIL: no rift_* names found at the registration sites — the grep pattern or source paths are stale" >&2
  exit 1
fi

# Referenced families, Prometheus side: the value of every `expr:` key
# under deploy/observability/ (recording rules, alert rules, and promtool
# test fixtures under rules/tests/ alike — a bad name in a test's synthetic
# `promql_expr_test.expr` is just as much a real reference as one in
# recording.yml). `_bucket`/`_sum`/`_count` suffixes are stripped before
# comparison since those are Prometheus's own histogram/summary decoration,
# not part of the registered family name. Recording-rule OUTPUT names never
# false-positive here: they are always `rift:thing:agg` (colon), and this
# pattern requires the character right after `rift` to be an underscore.
while IFS= read -r -d '' f; do
  grep -hoE '^[[:space:]]*expr:.*' "$f" || true
done < <(find "$OBSERVABILITY_DIR" \( -name '*.yml' -o -name '*.yaml' \) -print0) \
  | grep -oE 'rift_[A-Za-z0-9_]*' \
  | sed -E 's/_(bucket|sum|count)$//' \
  >> "$referenced_families" || true

# Referenced families, Grafana side: the value of every "expr" JSON key at
# any nesting depth, across every dashboard JSON another agent adds under
# deploy/observability/grafana/. jq's `..` walk means this does not care how
# deeply a panel/target is nested, and — critically — it does NOT look at
# "content", "description", "title", or any other prose field, which is
# what keeps a documentation panel from tripping this check.
while IFS= read -r -d '' f; do
  jq -r '[.. | .expr? // empty] | .[]' "$f"
done < <(find "$OBSERVABILITY_DIR" -name '*.json' -print0) \
  | grep -oE 'rift_[A-Za-z0-9_]*' \
  | sed -E 's/_(bucket|sum|count)$//' \
  >> "$referenced_families" || true

sort -u -o "$referenced_families" "$referenced_families"

offenders=()
while IFS= read -r family; do
  [[ -z "$family" ]] && continue
  if ! grep -Fxq "$family" "$known_families"; then
    offenders+=("$family")
  fi
done < "$referenced_families"

known_count="$(wc -l < "$known_families" | tr -d ' ')"
referenced_count="$(wc -l < "$referenced_families" | tr -d ' ')"

echo "Known families (from registration sites):        $known_count"
echo "Referenced families (under deploy/observability/): $referenced_count"

# ---------------------------------------------------------------------------
# Second gate: recording-rule names.
#
# A dashboard panel naming a recording rule that recording.yml does not define
# fails exactly the way an unregistered family does -- the query returns no
# data forever and the panel reads as "quiet", not "broken". The family check
# above cannot catch it, because `rift:foo:bar` is a rule output, not a metric
# family, and is deliberately skipped there.
rules_file="$OBSERVABILITY_DIR/prometheus/rules/recording.yml"
defined_rules="$(mktemp)"
referenced_rules="$(mktemp)"
trap 'rm -f "$known_families" "$referenced_families" "$defined_rules" "$referenced_rules"' EXIT

sed -n 's/^[[:space:]]*-[[:space:]]*record:[[:space:]]*\(rift:[A-Za-z0-9_:]*\).*/\1/p' \
  "$rules_file" | sort -u > "$defined_rules"

# `|| true` for the third time in this file, and the same reason each time: a
# pack that references no recording rule at all makes `grep` exit 1, `pipefail`
# promotes it, and `set -e` kills the script mid-check with no diagnostic.
# Unreachable against the real pack, which always references some — which is
# exactly why it survived until `--self-test` ran the script against a minimal
# one. "Zero matches" is a legitimate state here, not an error.
grep -rhoE 'rift:[A-Za-z0-9_:]+' \
  "$OBSERVABILITY_DIR/grafana" \
  "$OBSERVABILITY_DIR/prometheus" 2>/dev/null \
  | sort -u > "$referenced_rules" || true

rule_offenders=()
while IFS= read -r rule; do
  [[ -z "$rule" ]] && continue
  grep -qxF "$rule" "$defined_rules" || rule_offenders+=("$rule")
done < "$referenced_rules"

echo "Defined recording rules:                          $(wc -l < "$defined_rules" | tr -d ' ')"
echo "Referenced recording rules:                       $(wc -l < "$referenced_rules" | tr -d ' ')"

if [[ "${#offenders[@]}" -eq 0 && "${#rule_offenders[@]}" -eq 0 ]]; then
  echo "PASS: every rift_* family is registered, and every rift: rule referenced is defined."
  exit 0
fi

if [[ "${#rule_offenders[@]}" -gt 0 ]]; then
  echo "FAIL: ${#rule_offenders[@]} reference(s) to an undefined recording rule:"
  for o in "${rule_offenders[@]}"; do
    echo "  - $o"
  done
fi

if [[ "${#offenders[@]}" -eq 0 ]]; then
  exit 1
fi

echo "FAIL: ${#offenders[@]} unregistered metric family reference(s):"
for o in "${offenders[@]}"; do
  echo "  - $o"
done
exit 1
