#!/usr/bin/env bash
# Sync the vendor/rift submodule to the latest public Rift master (local run).
# Mirrors what the CI "Sync upstream Rift" workflow does.
set -euo pipefail
cd "$(dirname "$0")/.."

git submodule update --remote --recursive vendor/rift
rev=$(git -C vendor/rift rev-parse --short HEAD)

if git diff --quiet -- vendor/rift; then
  echo "vendor/rift already at upstream master ($rev)."
  exit 0
fi

git add vendor/rift
git commit -m "chore: bump vendor/rift to $rev"
echo "Bumped vendor/rift -> $rev. Run 'cargo check --workspace' before pushing."
