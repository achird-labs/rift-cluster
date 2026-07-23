#!/usr/bin/env bash
# Flake-quarantine convention for the container chaos tier (issue #73).
#
# A scenario that is persistently flaky or known-failing keeps its code and gets
#
#     #[ignore = "quarantined: #NNN -- one line of why"]
#
# where #NNN is an OPEN tracking issue. Deletion is never the remedy: a deleted
# scenario stops being evidence, and nothing then remembers the behaviour was
# ever covered.
#
#   list  -- emit `--skip <fn>` flags for every quarantined scenario, so CI and
#            the nightly derive their skips from the source of truth rather than
#            from a hardcoded list that silently rots. ONE TOKEN PER LINE, so a
#            caller reads them into an array (see below).
#   check -- fail if a quarantine tag names no issue, and (with GH_TOKEN) if the
#            issue it names is already closed.
#
# Both modes take an optional path to the scenarios file, so the parser can be
# exercised against a fixture rather than only against whatever the tree happens
# to hold -- which today is nothing quarantined at all.
#
# Why one token per line: `list` used to emit everything on one space-separated
# line, which forced its caller to write an unquoted `$skips` and lean on
# word-splitting. That works, but it leaves SC2086 pointing at the call site, and
# taking the lint's advice breaks the run:
#
#     $ cargo test ... -- --ignored "--skip a --skip b"
#     error: Unrecognized option: 'skip a --skip b'      (exit 101)
#
# Measured, not assumed -- and it corrects issue #116, which claimed libtest
# would read that argument as a *filter* and silently run nothing. It does not:
# the argument starts with `--`, so libtest rejects it as an option and the job
# goes red. Loud, but still broken, and still an edit the lint actively invites.
#
# One token per line makes the quoted array form the correct form, so the lint
# has nothing to say and there is no suppression comment left for anyone to
# delete.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scenarios="${2:-$repo_root/tests/cluster-chaos/tests/scenarios.rs}"

# Pair each `quarantined:` ignore with the `fn` name that follows it. Emitted as
# `<issue>\t<fn>` so both modes share one parse.
parse() {
  [ -f "$scenarios" ] || { echo "no scenarios file at $scenarios" >&2; exit 1; }
  awk '
    /#\[ignore[^]]*quarantined:/ {
      tag = $0
      issue = ""
      if (match(tag, /#[0-9]+/)) issue = substr(tag, RSTART + 1, RLENGTH - 1)
      pending = 1
      next
    }
    pending && /fn [a-zA-Z0-9_]+/ {
      match($0, /fn [a-zA-Z0-9_]+/)
      name = substr($0, RSTART + 3, RLENGTH - 3)
      print (issue == "" ? "-" : issue) "\t" name
      pending = 0
    }
  ' "$scenarios"
}

case "${1:-}" in
  list)
    while IFS=$'\t' read -r _issue fn; do
      [ -n "$fn" ] && printf -- '--skip\n%s\n' "$fn"
    done < <(parse)
    ;;

  check)
    status=0
    while IFS=$'\t' read -r issue fn; do
      [ -z "$fn" ] && continue
      if [ "$issue" = "-" ]; then
        echo "FAIL: '$fn' is quarantined without an issue reference (#NNN)" >&2
        status=1
        continue
      fi
      # Only reachable where a token exists; a local run checks the tag shape
      # and stops there rather than failing for lack of network.
      if [ -n "${GH_TOKEN:-}" ] && command -v gh >/dev/null 2>&1; then
        state="$(gh issue view "$issue" --json state --jq .state 2>/dev/null || echo UNKNOWN)"
        if [ "$state" = "CLOSED" ]; then
          echo "FAIL: '$fn' is quarantined on #$issue, which is closed -- un-quarantine it or re-file" >&2
          status=1
        elif [ "$state" = "UNKNOWN" ]; then
          echo "warn: could not read the state of #$issue for '$fn'" >&2
        fi
      fi
    done < <(parse)
    if [ "$status" = "0" ]; then
      echo "chaos quarantine tags OK ($(parse | grep -c . || true) quarantined)"
    fi
    exit "$status"
    ;;

  *)
    echo "usage: $0 {list|check}" >&2
    exit 2
    ;;
esac
