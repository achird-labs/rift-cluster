#!/usr/bin/env bash
# Guards an acceptance criterion of issue #6 that the compiler cannot: no
# openraft type appears in `rift-cluster`'s public API.
#
# openraft is meant to be an implementation detail. A leaked `StorageError` or
# `RaftError` in a signature makes every consumer's build depend on the exact
# openraft version this crate pins, turning a routine bump into a breaking
# change for them. The criterion was already violated when this script was
# written — `store::new` returned `openraft::StorageError` through a `pub mod` —
# which is why review vigilance was not enough.
#
# Needs nightly: rustdoc JSON, the only machine-readable view of a crate's
# public surface, is nightly-only. Run it yourself before pushing:
#
#     cargo install cargo-public-api --locked
#     scripts/check-public-api.sh
#
# CI runs this same script, so the exemption logic below cannot drift between
# the two.
set -euo pipefail

if ! command -v cargo-public-api >/dev/null 2>&1; then
    echo "cargo-public-api is not installed; install it with:" >&2
    echo "    cargo install cargo-public-api --version 0.51.0 --locked" >&2
    echo "(the same version CI pins, so you see what CI sees)" >&2
    exit 127
fi

api="$(cargo public-api -p rift-cluster --simplified)"
if [ -z "$api" ]; then
    echo "cargo public-api produced no output; refusing to report a pass" >&2
    exit 1
fi

# No exemptions, deliberately. An earlier draft had two — for `TypeConfig`'s
# `RaftTypeConfig` impl and the associated types cargo-public-api renders from
# it — until it turned out nothing outside this crate ever named `TypeConfig`.
# Making it `pub(crate)` removed the exposure at its source instead of
# whitelisting it, which leaves this check with no judgement calls to get wrong:
# any line naming openraft is a leak.
#
# Resist adding an exemption. Blanket-skipping `impl` lines in particular looks
# harmless but would silently permit `impl openraft::storage::RaftLogStorage for
# <public type>`, which is exactly the kind of leak this exists to catch — a
# consumer would need openraft in scope to call it.
#
# Known limit: this greps the rendered path, so an openraft type *re-exported*
# under a `rift_cluster::` path (`pub use openraft::BasicNode;`) renders without
# the substring and would pass. Types appearing in signatures keep their
# canonical path, so the common case is covered.
leaks="$(printf '%s\n' "$api" | grep -F 'openraft' || true)"

if [ -n "$leaks" ]; then
    echo "openraft leaked into rift-cluster's public API:" >&2
    echo "$leaks" >&2
    echo >&2
    echo "Keep openraft behind the crate boundary: make the item pub(crate), or" >&2
    echo "return a type this crate owns (see NodeError for the existing shape)." >&2
    exit 1
fi

echo "public API is openraft-free"
