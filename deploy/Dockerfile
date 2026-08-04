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
#
# Two published FLAVORS (#270), selected with `--target`:
#
#   `runtime`        (default, last stage — what a bare `docker build` with no
#                     `--target` produces) — `debian:bookworm-slim`, carries `git` so
#                     `git+https:`/`git+file:` imposter sources work.
#   `runtime-static` (`-static` tag suffix) — `FROM scratch`, a musl binary and nothing
#                     else: no shell, no package manager, no git. Smaller and a narrower
#                     attack surface, at the cost of `git+` sources — see `deploy/README.md`'s
#                     image-flavors table for the full trade-off.
#
# `runtime` stays the LAST stage in this file on purpose: `cluster-smoke` and every local
# `docker build -f deploy/Dockerfile .` pass no `--target`, and a bare build resolves to
# whichever stage is last. Moving `runtime` up to make room for the static stages would
# silently change what those untargeted builds produce.

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
#
# The prune is not tidiness. `vite` copies `web/public/` verbatim into `dist/` and the console embeds
# `dist/` byte for byte, so anything else wasm-pack leaves here ships inside the binary with no
# content type — which `console::tests::content_types_cover_what_the_bundle_actually_contains` fails
# on. `--no-pack` does NOT suppress the `.gitignore` wasm-pack writes, despite the release lane's
# comment saying so; that shipped unnoticed because the console tests run on no lane but the release
# one. See `scripts/prune-wasm-out.sh`.
RUN wasm-pack build crates/rift-lint-wasm \
      --release --target web --no-pack \
      --out-dir ../../web/public/lint --out-name rift_lint_wasm \
    && test -f web/public/lint/rift_lint_wasm.js \
    && test -f web/public/lint/rift_lint_wasm_bg.wasm \
    && bash scripts/prune-wasm-out.sh web/public/lint

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
# Builder — static (musl)
# ---------------------------------------------------------------------------
# Produces the binary for `runtime-static` below (#270). This is a NATIVE-arch musl build, not
# a cross-compile: the release lane builds each architecture on its own runner (`ubuntu-latest`
# for amd64, `ubuntu-24.04-arm` for arm64 — see `.github/workflows/release.yml`'s `image` job
# matrix), so on every build that matters `TARGETARCH` names the host the build is already
# running on. Cross-compiling a musl target from a foreign host is its own can of worms (linker
# selection, `.cargo/config.toml` target overrides); native builds side-step it entirely rather
# than solving it.
FROM rust:1.92-slim-bookworm AS builder-static

# `musl-tools`/`musl-dev` alongside the same `build-essential pkg-config git` the default
# `builder` installs above — `ring` still needs a C toolchain regardless of libc, and `git` is
# here for the identical reason: rift-cluster-base's build script shells out to it to read the
# vendored submodule's pin for `--version`. `ca-certificates` is also installed here, but not
# for this stage's own build — it exists so `runtime-static` below has a CA bundle to COPY,
# since scratch has no package manager to fetch one with.
#
# None of this says anything about the RUNTIME stage that consumes this builder's output:
# `git` being installed here is a build-time need, not a claim about what ships. See
# `runtime-static` for what actually ends up in the image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential pkg-config git musl-tools musl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Same `case` pattern as the wasm stage's TARGETARCH switch above, and the same rule: an
# unrecognized value is a hard failure, never a silent default. This stage is only ever reached
# via an explicit `--target runtime-static` build under buildx with a `platforms` set (the
# release lane's `image` job matrix), so TARGETARCH is always populated by the time this runs —
# unlike the wasm stage, which also has to work under a plain `docker build` with no buildx
# platform at all.
ARG TARGETARCH
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) triple="x86_64-unknown-linux-musl" ;; \
      arm64) triple="aarch64-unknown-linux-musl" ;; \
      *) echo "no musl target for TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    echo "$triple" > /musl_triple; \
    rustup target add "$triple"

WORKDIR /src
COPY . .

# Same reasoning as `builder` above: no git history in the build context, so the pin is passed
# in rather than discovered.
ARG RIFT_UPSTREAM_VERSION
ENV RIFT_UPSTREAM_VERSION=${RIFT_UPSTREAM_VERSION}

# The console bundle is flavor-neutral — rust-embed puts the bytes in the binary regardless of
# libc — so this flavor consumes the exact same `web` stage output as the default builder
# rather than rebuilding it.
COPY --from=web /src/web/dist ./web/dist

RUN set -eux; \
    triple="$(cat /musl_triple)"; \
    cargo build --locked --release -p rift-cluster-server --features console --target "$triple"

# `COPY --from=` cannot interpolate a shell variable, and the triple is only known at build
# time (it depends on TARGETARCH). So the binary is staged to a fixed, triple-independent path
# here — in the one stage that actually knows the triple — rather than asking `runtime-static`'s
# COPY to resolve one.
RUN set -eux; \
    triple="$(cat /musl_triple)"; \
    cp "target/${triple}/release/rift-cluster-server" /rift-cluster-server-static

# A minimal `/etc/passwd`/`/etc/group` carrying only the `rift` user, generated here because
# `runtime-static` is `FROM scratch` and has no shell to run `useradd` with. Numeric uid/gid
# 10001 — the same identity the default flavor's `useradd --uid 10001` produces below — so the
# two flavors are interchangeable from an orchestrator's point of view (the same non-root uid
# in a `securityContext`, etc).
RUN set -eux; \
    printf 'rift:x:10001:10001:rift:/var/lib/rift:/sbin/nologin\n' > /etc/passwd.rift; \
    printf 'rift:x:10001:\n' > /etc/group.rift

# The data and cluster-state directories are the only paths the process writes — same as
# `runtime` below. Created and chowned here, numerically, because scratch has no NSS to resolve
# `rift` against `/etc/passwd` at COPY time even though it will at container runtime.
RUN mkdir -p /var/lib/rift && chown 10001:10001 /var/lib/rift

# ---------------------------------------------------------------------------
# Runtime — static (scratch)
# ---------------------------------------------------------------------------
# The `-static` flavor's whole reason to exist (#270): zero OS packages, zero shell, zero attack
# surface beyond the binary itself. `FROM scratch` has no package manager, no libc dynamic
# loader, no coreutils — nothing but what is explicitly COPYed in below.
#
# This is possible now, and was not before #270's Rust half: `git+https:`/`git+file:` imposter
# sourcing used to be probed with `git --version` at startup and treated a missing git as a
# reason to refuse to boot, which would have made a gitless scratch image fail before it ever
# served a request. It is now a detected CAPABILITY instead of a boot requirement — the server
# boots and serves, logs exactly `git not found; git+ imposter sources disabled in this image`
# at WARN, and the two `git+` schemes fail at DECLARATION time (`` `git+https:` sources are
# unavailable: no `git` binary on PATH; install git, or use the default (non-static) image if this is `-static` ``) rather than at
# boot. See `runtime` below for the flavor that keeps git and pays for it in image size and
# package surface instead.
FROM scratch AS runtime-static

COPY --from=builder-static /rift-cluster-server-static /usr/local/bin/rift-cluster-server

# Outbound TLS (proxied imposters, HTTPS config sources) needs a CA bundle even with no git and
# no package manager to fetch one from at runtime. `builder-static` installed `ca-certificates`
# purely to produce this file for the COPY below.
COPY --from=builder-static /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=builder-static /etc/passwd.rift /etc/passwd
COPY --from=builder-static /etc/group.rift /etc/group

COPY --from=builder-static --chown=10001:10001 /var/lib/rift /var/lib/rift

USER 10001:10001
WORKDIR /var/lib/rift

# Same ports, same reasoning as `runtime` below.
EXPOSE 2525 2526 4790 9090

# Same healthcheck as `runtime` below: the binary's own `healthcheck` subcommand, needing no
# shell to exec — which is exactly why this flavor can carry a healthcheck at all.
#
# Absolute paths here, unlike `runtime` below, and not as a style preference. A `FROM scratch`
# image config carries no `PATH` at all: `dockerd` injects a default one when the config omits
# it, so a bare `rift-cluster-server` resolves under Docker — but that injection is the daemon's
# behaviour, not part of the OCI config, and a bare name is therefore one runtime away from
# `exec: executable file not found in $PATH`. The default flavor gets its `PATH` from
# `debian:bookworm-slim`'s own config and has no such dependency.
# No `--url`, matching `runtime` below: the subcommand resolves its own target from the mode
# (#297) — the probe listener's `/healthz` when clustered, the admin API's `/health` otherwise.
# Passing an explicit probe URL here would make this flavor the only one that reports `unhealthy`
# for an un-clustered container, which is the behaviour #299 just removed.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/rift-cluster-server", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/rift-cluster-server"]

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
# This MUST remain the last stage in the file: `cluster-smoke` and every local
# `docker build -f deploy/Dockerfile .` build with no `--target`, which resolves to whichever
# stage is last. The two static stages above are inserted BEFORE this one for exactly that
# reason — appending them after `runtime` would silently change what an untargeted build
# produces.
FROM debian:bookworm-slim AS runtime

# ca-certificates is needed for outbound TLS (proxied imposters, HTTPS config
# sources). `git` is needed by the `git+https:`/`git+file:` imposter source
# (#136), which shells out to it rather than linking libgit2. This — the
# default — flavor ships git, so `git+` sources work exactly as before; the
# `-static` flavor (`runtime-static` above) does not, and declaring a `git+`
# source against it fails at declaration time rather than at boot — see that
# stage's comment and `deploy/README.md`'s image-flavors table. Nothing else
# is installed on purpose — in particular no curl: the health check below
# execs the binary's own `healthcheck` subcommand, which is exactly why
# upstream put it in the binary.
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
#
# No --url: the subcommand derives its target itself (#297) — the probe
# listener's /healthz when RIFT_CLUSTER is set, or when a probe listener turns
# out to be bound anyway (a node clustered purely by command-line arguments
# shows this exec no environment), and the admin API's /health otherwise. An
# un-clustered container is therefore healthy while it serves, and an
# overridden RIFT_CLUSTER_PROBE_BIND keeps the probe pointed where the
# listener actually bound.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["rift-cluster-server", "healthcheck"]

ENTRYPOINT ["rift-cluster-server"]
