#!/usr/bin/env bash
# Decide whether a sharded `cluster-smoke` run actually ran (D-58).
#
#     scripts/cluster-smoke-gate.sh --want true --prepare success --shards success
#     scripts/cluster-smoke-gate.sh --self-test
#
# `cluster-smoke` is a required status check (#104), and since D-58 the job that
# carries that name does no testing of its own: it fans the tier out over shards
# and then judges them. That makes this script the whole check. If it is wrong in
# the permissive direction, the ruleset certifies a tier that never ran — which is
# the failure #93 (a path filter that failed open into skipping) and #101 (a merge
# that outran the job) each produced once already, by a different route.
#
# So the rule is stated as a whitelist, not a blacklist: a combination has to be
# recognised as good to pass. `skipped` is a GitHub job result like any other, and
# the one that reads most like success while meaning nothing happened.
#
# Inputs are the `needs.<job>.result` values plus the prepare job's `run` output —
# the path filter's own verdict. The filter decides whether the tier was supposed
# to run at all, so it is the only thing that can distinguish "skipped because
# there was nothing to test" from "skipped because something upstream broke".
set -euo pipefail

gate() {
    local want="$1" prepare="$2" shards="$3"

    # The prepare job is unconditional -- it runs the filter self-test and the
    # filter itself -- so anything but success means the verdict below cannot be
    # trusted, whatever it says.
    if [ "$prepare" != "success" ]; then
        echo "cluster-smoke: the prepare job did not succeed (result: $prepare)." >&2
        echo "Nothing decided whether the tier should run, so this check cannot pass." >&2
        return 1
    fi

    case "$want" in
        true)
            if [ "$shards" = "success" ]; then
                echo "cluster-smoke: the tier ran and every shard passed."
                return 0
            fi
            echo "cluster-smoke: the filter said the cluster changed, so every shard had to" >&2
            echo "run and pass; the shard result is '$shards'." >&2
            return 1
            ;;
        false)
            # `skipped` is what the shards' own `if:` produces here, and it is
            # the right answer: a docs-only PR has no cluster change to test.
            # `success` is accepted too so that forcing the shards on by hand
            # does not read as an inconsistency.
            if [ "$shards" = "skipped" ] || [ "$shards" = "success" ]; then
                echo "cluster-smoke: no cluster-touching paths changed; the tier was not needed."
                return 0
            fi
            echo "cluster-smoke: the filter said nothing relevant changed, so the shards" >&2
            echo "should have been skipped; the shard result is '$shards'." >&2
            return 1
            ;;
        *)
            # An empty or unrecognised verdict from a job that reported success
            # is the filter failing open. Refuse it: this is the exact shape of
            # #93, where a broken filter read as "nothing changed" and the job
            # went green having run nothing.
            echo "cluster-smoke: the prepare job succeeded but its filter verdict is" >&2
            echo "'$want', which is neither 'true' nor 'false'. A missing verdict is a" >&2
            echo "broken filter, and a broken filter must not pass as 'nothing to do'." >&2
            return 1
            ;;
    esac
}

self_test() {
    local failures=0

    check() {
        local what="$1" want_rc="$2" rc=0
        shift 2
        gate "$@" >/dev/null 2>&1 || rc=$?
        if [ "$rc" = "$want_rc" ]; then
            printf '  ok   %s\n' "$what"
        else
            printf '  FAIL %s (want rc=%s, got rc=%s)\n' "$what" "$want_rc" "$rc"
            failures=$((failures + 1))
        fi
    }

    # The two ways a run legitimately passes.
    check "the tier ran and passed" 0 true success success
    check "nothing relevant changed" 0 false success skipped
    check "shards forced on with nothing to do" 0 false success success

    # The failure this gate exists for: shards that did not run when they had to.
    # `skipped` is the dangerous one -- it is not an error anywhere in GitHub's
    # model, so without this it would sail through.
    check "shards skipped when the filter said run" 1 true success skipped
    check "shards failed when the filter said run" 1 true success failure
    check "shards cancelled when the filter said run" 1 true success cancelled

    # A prepare job that did not succeed decided nothing.
    check "prepare failed" 1 true failure success
    check "prepare skipped" 1 true skipped skipped
    check "prepare cancelled" 1 "" cancelled skipped

    # The filter failing open: a green prepare with no verdict.
    check "an empty verdict is refused" 1 "" success skipped
    check "a junk verdict is refused" 1 maybe success success
    check "an empty verdict is refused even with green shards" 1 "" success success

    # Shards green but the filter said no -- accepted above; the inverse of the
    # dangerous case, and harmless.
    check "shards ran when not needed" 0 false success success

    echo
    if [ "$failures" -eq 0 ]; then
        echo "OK: cluster-smoke-gate passes only what actually ran"
        return 0
    fi
    echo "FAIL: ${failures} case(s) wrong."
    return 1
}

want=""
prepare=""
shards=""
mode="gate"
while [ $# -gt 0 ]; do
    case "$1" in
        --want) want="${2:-}"; shift 2 ;;
        --prepare) prepare="${2:-}"; shift 2 ;;
        --shards) shards="${2:-}"; shift 2 ;;
        --self-test) mode="self-test"; shift ;;
        *)
            echo "usage: $0 --want <true|false> --prepare <result> --shards <result>" >&2
            echo "       $0 --self-test" >&2
            exit 2
            ;;
    esac
done

case "$mode" in
    self-test) self_test ;;
    gate) gate "$want" "$prepare" "$shards" ;;
esac
