# rift-cluster-server — the RiftCluster binary.
#
# Build from the REPOSITORY ROOT, not from this directory: the build needs
# `vendor/rift` (the open-source submodule this binary composes) in its context.
#
#     git submodule update --init --recursive
#     docker build -f deploy/Dockerfile -t rift-cluster-server .
#
# The image carries the web console (#265). Three stages build it — wasm, then
# the bundle, then the binary — in that order, because each consumes the one
# before it.
#
# A checkout without the submodule fails at `cargo build` with a missing-path
# dependency rather than silently producing a binary without the engine in it.

# ---------------------------------------------------------------------------
# Console wasm — `rift-lint` compiled for the browser
# ---------------------------------------------------------------------------
# Built here and in the release lane, nowhere else. `crates/rift-lint-wasm` is
# excluded from the root workspace precisely so no PR lane needs a wasm
# toolchain (RFC-006 §12 Q1), and that rule is untouched by this file: these
# stages only ever run inside `docker build`.
#
# `web/` works without the artifact — `lint.ts` resolves `"unavailable"` and the
# pane says the server still validates every save — so this stage adds a
# capability to the image rather than gating it. The asserts are what turn
# "wasm-pack exited 0" into "the two files the console actually imports exist".
FROM rust:1.92-slim-bookworm AS wasm

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Pinned, and installed from the release tarball rather than an installer script
# — the same choice `release.yml` makes, for the same reason: this puts a binary
# on the path that produces a shipped artifact.
#
# The asset is selected by `TARGETARCH` rather than hard-coded to x86_64. CI is
# amd64 so the hard-coded form worked there, and on Docker Desktop it also
# "works" on an arm64 host — but only because a kernel-wide binfmt/Rosetta
# handler is silently emulating it. On a native arm64 Linux host with no such
# registration (an arm64 runner, an arm64 dev box) that would be a bare
# `exec format error`, which is a confusing way to learn about an image's
# assumptions.
ARG WASM_PACK_VERSION=v0.13.1
ARG TARGETARCH
RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
      amd64) triple="x86_64-unknown-linux-musl" ;; \
      arm64) triple="aarch64-unknown-linux-musl" ;; \
      *) echo "no pinned wasm-pack asset for TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    asset="wasm-pack-${WASM_PACK_VERSION}-${triple}"; \
    curl -sSfL "https://github.com/rustwasm/wasm-pack/releases/download/${WASM_PACK_VERSION}/${asset}.tar.gz" \
      | tar -xz --strip-components=1 -C /usr/local/bin "${asset}/wasm-pack"; \
    wasm-pack --version

WORKDIR /src
COPY . .

# `--target web` + `--no-pack`: the console imports the module directly as
# `/console/lint/rift_lint_wasm.js`, and there is no npm package to publish.
RUN wasm-pack build crates/rift-lint-wasm \
      --release --target web --no-pack \
      --out-dir ../../web/public/lint --out-name rift_lint_wasm \
    && test -f web/public/lint/rift_lint_wasm.js \
    && test -f web/public/lint/rift_lint_wasm_bg.wasm

# ---------------------------------------------------------------------------
# Console bundle
# ---------------------------------------------------------------------------
# `pnpm build` is what copies `web/public/` into `dist/`, so this stage must run
# AFTER the wasm stage and consume its output — building the two independently
# would produce a bundle with no linter in it and nothing to say so.
FROM node:22-slim AS web

ARG PNPM_VERSION=11.18.0
RUN corepack enable && corepack prepare "pnpm@${PNPM_VERSION}" --activate

WORKDIR /src/web
# Manifest, lockfile, AND `pnpm-workspace.yaml` — all three, before the install.
#
# The workspace file is not incidental here: it carries this project's
# `minimumReleaseAgeExclude`, and pnpm 11 runs a lockfile supply-chain policy
# pass on every install where a rejection is a hard failure, not a warning.
# Omitting it works right up until someone bumps a `web/` dependency to a
# freshly-published version and adds it to that exclude list — the edit the
# console scaffold already had to make four times. Their PR stays green (every
# console lane installs with the file present) and the failure surfaces later, on
# an unrelated PR, in `cluster-smoke` — a required check that builds this image —
# as a message about the lockfile rather than about a missing COPY.
COPY web/package.json web/pnpm-lock.yaml web/pnpm-workspace.yaml ./

# Corepack always honours `packageManager` in the manifest, so `PNPM_VERSION`
# above only pre-warms the cache — it does not pin anything. Asserted rather than
# assumed: without this, bumping `packageManager` alone would silently change the
# image's build toolchain and add an unpinned registry fetch at build time, which
# is precisely what naming a version here was supposed to prevent.
RUN declared="$(node -p "require('./package.json').packageManager")"; \
    if [ "$declared" != "pnpm@${PNPM_VERSION}" ]; then \
      echo "pnpm pin drift: Dockerfile says pnpm@${PNPM_VERSION}, web/package.json says ${declared}" >&2; \
      exit 1; \
    fi

RUN pnpm install --frozen-lockfile

COPY web/ ./
# The linter, from the stage above — never from the host. See `.dockerignore`.
COPY --from=wasm /src/web/public/lint ./public/lint
RUN pnpm build \
    && test -f dist/index.html \
    && test -f dist/lint/rift_lint_wasm_bg.wasm

# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------
FROM rust:1.92-slim-bookworm AS builder

# `ring` (via rustls) and the scripting engines compile C, so the toolchain is
# not optional. `git` is here for rift-cluster-base's build script, which reads the
# vendored submodule's pin to put the upstream version in `--version`; without
# it the banner degrades to `rift unknown` rather than failing the build.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential pkg-config git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# The build context has no git history (see .dockerignore — `.git` is many
# megabytes of no use to a compiler), and a submodule's `.git` is only a
# pointer into the parent repo's object store anyway. So the pin is passed in
# rather than discovered, and the image reports the same upstream version the
# host would:
#
#     docker build -f deploy/Dockerfile \
#       --build-arg RIFT_UPSTREAM_VERSION="$(git -C vendor/rift describe --tags --always)" .
#
# Left unset, the banner says `rift unknown`, which is wrong-free but unhelpful.
ARG RIFT_UPSTREAM_VERSION
ENV RIFT_UPSTREAM_VERSION=${RIFT_UPSTREAM_VERSION}

# The console bundle comes from the `web` stage, never from the host: a stale
# host-side `pnpm build` leaking in would produce an image whose console does not
# match its binary, and nothing would say so. `.dockerignore` excludes
# `web/dist` so the `COPY . .` above cannot carry one in, and this is what puts
# the bundle where `rust-embed` looks (`console.rs` declares
# `#[folder = "../../web/dist"]`).
COPY --from=web /src/web/dist ./web/dist

# `--locked` so an image can never be built from a silently-resolved dependency
# graph that differs from the one CI tested.
#
# `--features console` is the point of #265. Before it, the image an operator ran
# had no console at all, while the release lane proved a console-bearing binary
# builds and then threw it away. The lane's own guard that the bundle is really
# inside the binary is `scripts/check-console-embed.sh`; the equivalent proof for
# this image is `deploy/compose/verify.sh`, which asks a running node for
# `/console` rather than trusting the build.
RUN cargo build --locked --release -p rift-cluster-server --features console

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates is needed for outbound TLS (proxied imposters, HTTPS config
# sources). `git` is needed by the `git+https:`/`git+file:` imposter source
# (#136), which shells out to it rather than linking libgit2 — and which probes
# `git --version` at startup, so a runtime image without it would refuse to
# boot rather than fail later. Nothing else is installed on purpose — in
# particular no curl: the health check below execs the binary's own
# `healthcheck` subcommand, which is exactly why upstream put it in the binary.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 rift

COPY --from=builder /src/target/release/rift-cluster-server /usr/local/bin/rift-cluster-server

# The data and cluster-state directories are the only paths the process writes.
RUN mkdir -p /var/lib/rift && chown -R rift:rift /var/lib/rift
USER rift
WORKDIR /var/lib/rift

# 2525 admin API · 2526 probes (/readyz, /healthz) · 4790 cluster port
# · 9090 metrics. Imposter ports are assigned at runtime and are deliberately
# not listed: an imposter can bind any port, so enumerating a few here would
# imply a limit that does not exist.
EXPOSE 2525 2526 4790 9090

# Liveness only. Readiness is the orchestrator's business (`/readyz`), and a
# container health check that failed on not-ready would restart a node that is
# merely still joining — the exact failure /healthz exists to prevent.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["rift-cluster-server", "healthcheck", "--url", "http://127.0.0.1:2526/healthz"]

ENTRYPOINT ["rift-cluster-server"]
