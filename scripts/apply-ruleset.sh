#!/usr/bin/env bash
# Apply .github/rulesets/master.json to this repository, idempotently.
#
# Rulesets are repo *settings*, so by default they exist only in GitHub's UI:
# invisible to review, undiffable, and silently divergent from anything written
# down about them. Keeping the JSON in the tree and applying it from here makes
# a change to "how may this repo be merged into" reviewable the same way a
# change to "how does this repo build" is.
#
#   scripts/apply-ruleset.sh            create or update
#   scripts/apply-ruleset.sh --dry-run  show what would change, touch nothing
#
# Idempotent by name: the ruleset is looked up by its `name` field, POSTed if
# absent and PUT if present. Re-running after editing the JSON is the intended
# way to change the live configuration.
set -euo pipefail

cd "$(dirname "$0")/.."
SPEC=".github/rulesets/master.json"
DRY_RUN=false

case "${1:-}" in
  --dry-run) DRY_RUN=true ;;
  "") ;;
  *)
    echo "usage: $0 [--dry-run]" >&2
    exit 2
    ;;
esac

[ -f "$SPEC" ] || {
  echo "FAIL: $SPEC not found" >&2
  exit 1
}

# Fail on malformed JSON here rather than letting the API reject it with a
# message about a field, which reads as "the rule is wrong" instead of "the file
# is not JSON".
python3 -c "import json,sys; json.load(open('$SPEC'))" || {
  echo "FAIL: $SPEC is not valid JSON" >&2
  exit 1
}

REPO="$(gh repo view --json nameWithOwner --jq .nameWithOwner)"
NAME="$(python3 -c "import json; print(json.load(open('$SPEC'))['name'])")"

echo "repo:    $REPO"
echo "ruleset: $NAME"

# `|| true`: a repo with no rulesets is a normal starting state, not an error.
EXISTING_ID="$(gh api "repos/$REPO/rulesets" --jq \
  ".[] | select(.name == \"$NAME\") | .id" 2>/dev/null || true)"

if [ -z "$EXISTING_ID" ]; then
  ACTION="create"
  METHOD="POST"
  ENDPOINT="repos/$REPO/rulesets"
else
  ACTION="update (id $EXISTING_ID)"
  METHOD="PUT"
  ENDPOINT="repos/$REPO/rulesets/$EXISTING_ID"
fi

echo "action:  $ACTION"

if [ "$DRY_RUN" = true ]; then
  echo
  echo "--dry-run: would $METHOD $ENDPOINT with:"
  cat "$SPEC"
  exit 0
fi

gh api --method "$METHOD" "$ENDPOINT" --input "$SPEC" >/dev/null

# Read back rather than trusting the write: the API accepts a rule it does not
# recognise by ignoring it, so "the call succeeded" and "the rule is live" are
# different claims. This is the same failure shape as #93 — a green result for
# something that did not happen.
echo
echo "live rules now enforced:"
gh api "repos/$REPO/rulesets" --jq \
  ".[] | select(.name == \"$NAME\") | .id" |
  while read -r id; do
    gh api "repos/$REPO/rulesets/$id" --jq \
      '"  enforcement: \(.enforcement)",
       "  bypass actors: \(.bypass_actors | length)",
       (.rules[] | "  rule: \(.type)" +
         (if .type == "required_status_checks"
          then " [" + ((.parameters.required_status_checks // [])
                       | map(.context) | join(", ")) + "]"
          else "" end))'
  done
