#!/usr/bin/env bash
# Split the container chaos tier across `cluster-smoke` shards (D-58).
#
# Reads scenario names on stdin, one per line, and emits the subset this shard
# runs. The caller turns that into libtest `--exact` filters:
#
#     names="$(cargo test -p cluster-chaos --test scenarios -- --ignored --list \
#              | sed -n 's/^\(.*\): test$/\1/p' \
#              | scripts/chaos-shard.sh --index 2 --total 4)"
#
# Why the names and not a `--skip` list: the shard's own count is then known, so
# `assert-scenarios-ran.sh` can be given that number as its floor instead of 1. A
# renamed or vanished scenario makes the shard run fewer tests than it selected
# and the job goes red — which is the failure `assert-scenarios-ran.sh` exists
# for, applied at the only place that knows the expected number.
#
# Quarantined scenarios are dropped here, from `chaos-quarantine.sh` rather than
# from a list of our own, for the reason that script's header gives: a second
# copy of the quarantine set rots.
#
# ## Partitioning
#
# Sort, then round-robin by position. Deterministic (so re-running a red shard
# runs the same scenarios), balanced to within one scenario, and derived from
# whatever `--list` reports rather than from a table that has to be edited when a
# scenario lands. The nightly's hand-maintained matrix is the counter-example:
# it lists 24 scenarios where the tier now has 36, and nothing said so.
#
# Balanced by COUNT, not by cost, and those differ here — the cheapest scenario
# is ~19 s and the dearest ~2 min, so a shard can draw an unlucky hand and run
# perhaps 30 % long. That is worth naming rather than fixing blind: the cost per
# scenario is only now being measured (`CHAOS_TIMING_LOG`, rendered as a step
# summary), and a weight table written before those numbers exist would be a
# guess that then rots like the nightly's. Pack by measured cost once there is
# measured cost.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

quarantined() {
    # `list` emits `--skip` and the name on alternating lines; take the names.
    "$repo_root/scripts/chaos-quarantine.sh" list "${1:-}" 2>/dev/null \
        | grep -v -- '^--skip$' || true
}

shard() {
    local index="$1" total="$2" skips selected count
    if ! [[ "$total" =~ ^[0-9]+$ ]] || [ "$total" -lt 1 ]; then
        echo "chaos-shard: --total must be a positive integer, got '$total'" >&2
        return 2
    fi
    if ! [[ "$index" =~ ^[0-9]+$ ]] || [ "$index" -lt 1 ] || [ "$index" -gt "$total" ]; then
        echo "chaos-shard: --index must be between 1 and $total, got '$index'" >&2
        return 2
    fi

    # A file rather than a `[ -n "$skips" ] && grep || cat` chain: when grep
    # filters out every line it exits 1, the `||` branch runs, and `cat` then
    # reads a stdin the grep already consumed. Empty-but-present is the shape
    # that has no special case -- `grep -Fxv -f /dev/null` passes everything
    # through.
    skips="$(mktemp)"
    quarantined "${SHARD_SCENARIOS_FILE:-}" >"$skips"

    # Sort and de-duplicate first: the partition is by position, so it is only
    # deterministic if the order is.
    selected="$(
        sort -u \
            | grep -v '^[[:space:]]*$' \
            | grep -Fxv -f "$skips" \
            | awk -v i="$index" -v n="$total" 'NR % n == i % n' \
            || true
    )"
    rm -f "$skips"

    count="$(printf '%s' "$selected" | grep -c . || true)"
    if [ "$count" -eq 0 ]; then
        # Never emit nothing. An empty filter list is not "run no scenarios" to
        # libtest -- it is no filter at all, so every shard would run the whole
        # tier and the sharding would silently become a four-fold duplication.
        echo "chaos-shard: shard $index/$total selected no scenarios; refusing to" >&2
        echo "emit an empty filter, which libtest reads as 'run everything'." >&2
        return 1
    fi
    printf '%s\n' "$selected"
}

self_test() {
    local failures=0 tmp
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' RETURN

    # A fixture with nothing quarantined, so the parser is exercised against a
    # known set rather than whatever the tree happens to hold.
    printf 'fn a_one() {}\n' >"$tmp/none.rs"
    printf '#[ignore = "quarantined: #7 -- flaky"]\nfn c_three() {}\n' >"$tmp/one.rs"

    local names
    names=$'c_three\na_one\nb_two\nd_four\ne_five'

    check() {
        local what="$1" want="$2" got="$3"
        if [ "$want" = "$got" ]; then
            printf '  ok   %s\n' "$what"
        else
            printf '  FAIL %s\n    want: %s\n    got:  %s\n' "$what" "$want" "$got"
            failures=$((failures + 1))
        fi
    }

    run_shard() {
        local fixture="$1" out
        SHARD_SCENARIOS_FILE="$fixture"
        out="$(printf '%s\n' "$names" | shard "$2" "$3" 2>/dev/null | tr '\n' ' ')"
        unset SHARD_SCENARIOS_FILE
        printf '%s' "${out% }"
    }

    # Every scenario lands in exactly one shard, and the union is the input.
    local union
    union="$( (run_shard "$tmp/none.rs" 1 2; echo; run_shard "$tmp/none.rs" 2 2) \
        | tr ' ' '\n' | grep -v '^$' | sort | tr '\n' ' ' | sed 's/ $//')"
    check "two shards partition the whole set" "a_one b_two c_three d_four e_five" "$union"

    # Deterministic: the same index twice gives the same answer.
    check "a shard is deterministic" \
        "$(run_shard "$tmp/none.rs" 1 3)" "$(run_shard "$tmp/none.rs" 1 3)"

    # Balanced to within one.
    local n1 n2 n3
    n1="$(run_shard "$tmp/none.rs" 1 3 | wc -w | tr -d ' ')"
    n2="$(run_shard "$tmp/none.rs" 2 3 | wc -w | tr -d ' ')"
    n3="$(run_shard "$tmp/none.rs" 3 3 | wc -w | tr -d ' ')"
    check "5 scenarios over 3 shards is 2/2/1" "2 2 1" "$(printf '%s %s %s' "$n1" "$n2" "$n3")"

    # One shard is the whole set, which is what a `--total 1` fallback must do.
    check "one shard runs everything" "a_one b_two c_three d_four e_five" \
        "$(run_shard "$tmp/none.rs" 1 1)"

    # A quarantined scenario is excluded, from the tag in the source.
    local with_quarantine
    with_quarantine="$( (run_shard "$tmp/one.rs" 1 2; echo; run_shard "$tmp/one.rs" 2 2) \
        | tr ' ' '\n' | grep -v '^$' | sort | tr '\n' ' ' | sed 's/ $//')"
    check "a quarantined scenario is dropped" "a_one b_two d_four e_five" "$with_quarantine"

    # The guard that matters most: an empty selection must fail, never emit
    # nothing. libtest reads no filters as "run every test", so a shard that
    # silently emitted nothing would run the whole tier -- once per shard.
    local rc=0
    printf '' | shard 1 2 >/dev/null 2>&1 || rc=$?
    check "an empty selection fails rather than emitting nothing" "1" "$rc"

    rc=0
    printf '%s\n' "$names" | shard 5 4 >/dev/null 2>&1 || rc=$?
    check "an out-of-range index fails" "2" "$rc"

    rc=0
    printf '%s\n' "$names" | shard 1 0 >/dev/null 2>&1 || rc=$?
    check "a zero shard count fails" "2" "$rc"

    echo
    if [ "$failures" -eq 0 ]; then
        echo "OK: chaos-shard partitions as specified"
        return 0
    fi
    echo "FAIL: ${failures} case(s) wrong."
    return 1
}

index=""
total=""
mode="shard"
while [ $# -gt 0 ]; do
    case "$1" in
        --index) index="${2:-}"; shift 2 ;;
        --total) total="${2:-}"; shift 2 ;;
        --self-test) mode="self-test"; shift ;;
        *)
            echo "usage: $0 --index I --total N   (scenario names on stdin)" >&2
            echo "       $0 --self-test" >&2
            exit 2
            ;;
    esac
done

case "$mode" in
    self-test) self_test ;;
    shard)
        if [ -z "$index" ] || [ -z "$total" ]; then
            echo "usage: $0 --index I --total N   (scenario names on stdin)" >&2
            exit 2
        fi
        shard "$index" "$total"
        ;;
esac
