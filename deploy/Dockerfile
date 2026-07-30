# rift-cluster-server — the RiftCluster binary.
#
# Build from the REPOSITORY ROOT, not from this directory: the build needs
# `vendor/rift` (the open-source submodule this binary composes) in its context.
#
#     git submodule update --init --recursive
#     docker build -f deploy/Dockerfile -t rift-cluster-server .
#
# A checkout without the submodule fails at `cargo build` with a missing-path
# dependency rather than silently producing a binary without the engine in it.

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

# `--locked` so an image can never be built from a silently-resolved dependency
# graph that differs from the one CI tested.
RUN cargo build --locked --release -p rift-cluster-server

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
