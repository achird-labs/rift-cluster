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
OBSERVABILITY_DIR="$REPO_ROOT/deploy/observability"

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
rules_file="$REPO_ROOT/deploy/observability/prometheus/rules/recording.yml"
defined_rules="$(mktemp)"
referenced_rules="$(mktemp)"
trap 'rm -f "$known_families" "$referenced_families" "$defined_rules" "$referenced_rules"' EXIT

sed -n 's/^[[:space:]]*-[[:space:]]*record:[[:space:]]*\(rift:[A-Za-z0-9_:]*\).*/\1/p' \
  "$rules_file" | sort -u > "$defined_rules"

grep -rhoE 'rift:[A-Za-z0-9_:]+' \
  "$REPO_ROOT/deploy/observability/grafana" \
  "$REPO_ROOT/deploy/observability/prometheus" 2>/dev/null \
  | sort -u > "$referenced_rules"

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
