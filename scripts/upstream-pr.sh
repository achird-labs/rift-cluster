#!/usr/bin/env bash
# Open an upstream PR against the public Rift repo for a core change that a
# cluster feature depends on. Run this from the cluster repo root; it
# operates inside the vendor/rift submodule.
#
#   scripts/upstream-pr.sh <branch> "<PR title>"
#
# Stage the core changes inside vendor/rift first (edit files under
# vendor/rift/, then this script commits, pushes, and opens the PR).
set -euo pipefail

# The header above, without its comment markers.
usage() { sed -n '2,/^set -euo pipefail$/{/^set -euo pipefail$/d;s/^# \{0,1\}//;p;}' "$0"; }

# Anywhere in the arguments: `<branch> --help` would otherwise become a PR titled "--help".
for arg in "$@"; do
  case "$arg" in -h | --help) usage; exit 0 ;; esac
done

branch="${1:?usage: upstream-pr.sh <branch> \"<PR title>\"}"
title="${2:?usage: upstream-pr.sh <branch> \"<PR title>\"}"
upstream_repo="achird-labs/rift"

cd "$(dirname "$0")/../vendor/rift"

if git diff --quiet && git diff --cached --quiet; then
  echo "No changes staged in vendor/rift. Edit files there first." >&2
  exit 1
fi

git switch -c "$branch"
git add -A
git commit -m "$title"
git push -u origin "$branch"
gh pr create --repo "$upstream_repo" --base master --head "$branch" --title "$title" --fill

cat <<EOF

Upstream PR opened against $upstream_repo.
Once it merges, bump the submodule in the cluster repo:

    scripts/sync-upstream.sh
EOF
