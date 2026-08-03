#!/usr/bin/env bash
# Leave only the two wasm artifacts the console imports in `web/public/lint/`.
#
#     scripts/prune-wasm-out.sh web/public/lint
#
# `vite` copies `web/public/` VERBATIM into `dist/`, and the console embeds `dist/` byte for byte —
# so anything wasm-pack leaves in its out-dir ships inside the release binary and is served with no
# content type. `console::tests::content_types_cover_what_the_bundle_actually_contains` fails on
# exactly that, which is the whole reason this script exists rather than a comment asking people to
# be careful.
#
# It has now happened twice with two different files. First `web/public/lint/README.md`, a tracked
# placeholder (#219). Then `.gitignore`, which `wasm-pack` writes into its out-dir — the release
# lane's own comment claimed `--no-pack` suppressed it, and for wasm-pack v0.13.1 that is simply not
# true. Both were caught only by the console tests, which run on no lane but the release one, so the
# second sat latent until a publish was attempted.
#
# So the rule is enforced positively: name what may ship, delete everything else, and fail if either
# artifact is missing. A future wasm-pack that writes a third file is then a no-op here rather than a
# release-day surprise.
set -euo pipefail

out="${1:?usage: prune-wasm-out.sh <out-dir>}"

if [ ! -d "$out" ]; then
  echo "prune-wasm-out: no such directory: $out" >&2
  exit 1
fi

# The allowlist is the two files `lint.ts` actually loads. `--out-name rift_lint_wasm` is what fixes
# these names; change one and this list changes with it.
keep_js="rift_lint_wasm.js"
keep_wasm="rift_lint_wasm_bg.wasm"

removed=0
# `find`, not a glob: a bare `*` misses dotfiles, and a dotfile is precisely what got through last
# time. `-mindepth 1` so the directory itself is never a candidate.
while IFS= read -r -d '' path; do
  case "$(basename "$path")" in
    "$keep_js" | "$keep_wasm") continue ;;
  esac
  echo "prune-wasm-out: removing $path (would ship into the console bundle with no content type)"
  rm -rf "$path"
  removed=$((removed + 1))
done < <(find "$out" -mindepth 1 -maxdepth 1 -print0)

for required in "$keep_js" "$keep_wasm"; do
  if [ ! -f "${out}/${required}" ]; then
    echo "prune-wasm-out: ${out}/${required} is missing — wasm-pack did not produce what the console imports" >&2
    exit 1
  fi
done

echo "prune-wasm-out: ${out} holds exactly the console's two artifacts (${removed} other entr$([ "$removed" = 1 ] && echo y || echo ies) removed)"
