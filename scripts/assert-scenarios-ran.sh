#!/usr/bin/env bash
# Fail when a test run reports success having run nothing.
#
# Reads `cargo test` output on stdin, passes it straight back out, and exits
# non-zero unless at least `$1` tests passed (default 1).
#
# Why this exists: libtest exits **0** when a filter matches no test at all.
# Measured, on this suite:
#
#     $ cargo test ... -- --ignored --exact scenario_that_was_renamed
#     test result: ok. 0 passed; 0 failed; 27 filtered out          (exit 0)
#
# `0 passed` is a green job. That is exactly the shape of the nightly soak, which
# runs one scenario per iteration by `--exact '<matrix.scenario>'` against names
# hardcoded in the workflow: rename a scenario and its job soaks nothing, every
# night, reporting success, indefinitely.
#
# The same shape reaches the PR tier through any filter that stops matching — a
# runner-step edit, a bad skip set. Since #104 made `cluster-smoke` a required
# check, a green-but-empty run is worse than a red one: the ruleset then
# certifies the thing that did not run.
#
# (Issue #116 also claimed a quoted `"$skips"` would land here. It does not --
# libtest rejects it as an unrecognised option and exits 101. That is a real
# breakage but a loud one; this guard is for the silent kind.)
#
# A floor rather than an exact count, deliberately. An exact expected number is a
# second copy of the quarantine list, and it rots; the failure worth catching is
# catastrophic (zero), not marginal (17 where 18 were expected).
set -euo pipefail

# Sum the `N passed` figures across every `test result:` line, since one run can
# report several (one per test binary).
count_passed() {
    awk '
        /^test result:/ {
            for (i = 1; i <= NF; i++) {
                if ($i == "passed;") { total += $(i - 1) }
            }
        }
        END { print total + 0 }
    '
}

assert_ran() {
    local floor="$1" output passed
    output="$(cat)"
    printf '%s\n' "$output"

    passed="$(printf '%s\n' "$output" | count_passed)"
    if [ "$passed" -lt "$floor" ]; then
        printf '\n' >&2
        printf 'assert-scenarios-ran: %s tests passed, expected at least %s.\n' \
            "$passed" "$floor" >&2
        printf 'libtest exits 0 when a filter matches nothing, so this run would\n' >&2
        printf 'otherwise be a green check that tested nothing. Look at the skip\n' >&2
        printf 'arguments and the test-name filter above.\n' >&2
        return 1
    fi
    printf 'assert-scenarios-ran: %s passed (floor %s)\n' "$passed" "$floor"
}

self_test() {
    local failures=0

    check() {
        local what="$1" floor="$2" want="$3" input="$4" rc=0
        printf '%s' "$input" | assert_ran "$floor" >/dev/null 2>&1 || rc=$?
        if [ "$rc" = "$want" ]; then
            printf '  ok   %s\n' "$what"
        else
            printf '  FAIL %s (want rc=%s, got rc=%s)\n' "$what" "$want" "$rc"
            failures=$((failures + 1))
        fi
    }

    # The failure this exists for: a filter that matched nothing. libtest says
    # 0/0 and exits 0, and without this the job is green.
    check "an empty run fails" 1 1 \
        'test result: ok. 0 passed; 0 failed; 18 ignored; 0 measured; 21 filtered out; finished in 0.01s'

    check "a real run passes" 1 0 \
        'test result: ok. 18 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out; finished in 900s'

    # One `cargo test` invocation reports once per test binary; the floor is on
    # the total, so a suite of mostly-empty binaries still counts.
    check "counts across several binaries" 1 0 \
        'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s'

    check "several binaries, all empty, still fails" 1 1 \
        'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s'

    # The nightly runs exactly one scenario per iteration, so its floor is 1 and
    # a vacuous `--exact` on a renamed scenario has to fail it.
    check "nightly: one scenario satisfies a floor of 1" 1 0 \
        'test result: ok. 1 passed; 0 failed; 17 ignored; 0 measured; 3 filtered out; finished in 60s'

    check "nightly: a renamed scenario matches nothing and fails" 1 1 \
        'test result: ok. 0 passed; 0 failed; 18 ignored; 0 measured; 21 filtered out; finished in 0.01s'

    # No `test result:` line at all — the run died before libtest reported.
    # Nothing passed, so nothing ran.
    check "output with no result line fails" 1 1 'error: could not compile'

    check "an explicit higher floor is honoured" 5 1 \
        'test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1s'

    echo
    if [ "$failures" -eq 0 ]; then
        echo "OK: assert-scenarios-ran behaves as specified"
        return 0
    fi
    echo "FAIL: ${failures} case(s) wrong."
    return 1
}

case "${1:---assert}" in
    --self-test) self_test ;;
    --assert) assert_ran "${2:-1}" ;;
    *)
        echo "usage: $0 [--assert [floor] | --self-test]" >&2
        echo "  --assert     (default) read cargo test output on stdin, fail if too few passed" >&2
        echo "  --self-test  check the counting against a case table" >&2
        exit 2
        ;;
esac
