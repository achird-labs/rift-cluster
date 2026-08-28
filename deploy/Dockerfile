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
# before it. Since D-60 the binary half is itself two stages, `planner` and
# `builder`, over a shared `chef` toolchain: the dependency build is keyed on a
# recipe of the manifests instead of on the source tree, so an ordinary source
# edit no longer rebuilds all 371 crates.
#
# A checkout without the submodule fails at `cargo build` with a missing-path
# dependency rather than silently producing a binary without the engine in it.
#
# Two published FLAVORS (#270), selected with `--target`:
#
#   `runtime`        (the production default — pass `--target runtime` explicitly: since
#                     issue #228 the file's LAST stage is the chaos suite's
#                     `runtime-faketime`, so a bare untargeted build no longer produces
#                     this) — `debian:bookworm-slim`, carries `git` so
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
# Builder — toolchain, shared by the planner and the build (D-60)
# ---------------------------------------------------------------------------
# `ring` (via rustls) and the scripting engines compile C, so the toolchain is
# not optional. `git` is here for rift-cluster-base's build script, which reads the
# vendored submodule's pin to put the upstream version in `--version`; without
# it the banner degrades to `rift unknown` rather than failing the build.
FROM rust:1.92-slim-bookworm AS chef

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential pkg-config git \
    && rm -rf /var/lib/apt/lists/*

# Pinned and `--locked`, and from crates.io rather than a third-party image: this
# stage's provenance is then the same registry the dependency graph already
# trusts, and there is no new publisher to keep an eye on. It costs a minute on a
# cold build and nothing on a warm one — the layer changes only when the version
# on this line does.
RUN cargo install cargo-chef --locked --version 0.1.72

WORKDIR /src

# ---------------------------------------------------------------------------
# Planner — reduce the tree to a dependency recipe
# ---------------------------------------------------------------------------
# `prepare` reads the manifests and lockfile and writes `recipe.json`. This stage
# takes the whole context and so is invalidated by any edit, but it does no
# compiling: it exists purely so the expensive stage below depends on the recipe
# rather than on the source.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------
# Why this is split at all, measured rather than assumed. `cargo build` here was
# 404 s of the image's ~460 s, and none of it could ever be reused: `COPY . .`
# sat immediately before it, so editing any file in the context — a test, a
# workflow, a chaos scenario — rebuilt all 371 crates. Measured split of that
# 404 s: 144 s to reach the first path crate (third-party dependencies), then
# 260 s for the nine path crates, of which the last 174 s is `rift-cluster` and
# `rift-cluster-server`. Only those last two change on an ordinary PR.
#
# So: build everything else in a layer keyed on the recipe, and let the source
# copy invalidate only what genuinely depends on the source.
FROM chef AS builder

# The vendored submodule FIRST, and on its own. `cargo chef prepare` writes a
# skeleton of the WORKSPACE MEMBERS only — measured: the recipe carries the five
# `crates/`+`tests/` manifests and nothing from `vendor/rift`, because that is a
# separate workspace consumed as path dependencies — so `cook` cannot resolve
# without the real directory on disk. Its own layer means the dependency build
# below is invalidated by a submodule pin bump, which is correct (the vendored
# crates ARE dependencies, and cook compiles them: verified), and by nothing else.
COPY vendor/rift vendor/rift

COPY --from=planner /src/recipe.json recipe.json
# The same package and features as the real build below. Feature resolution
# decides which dependencies exist and how they are compiled, so a `cook` that
# did not mirror them would warm a cache the build then misses.
RUN cargo chef cook --locked --release -p rift-cluster-server --features console \
        --recipe-path recipe.json

# Named roots, NOT `COPY . .`, and the omission of `vendor/rift` is the point
# (D-62). `cook` above compiled the five vendored crates, but a blanket copy
# rewrote them with fresh mtimes, so cargo's fingerprint saw them as dirty and
# rebuilt all five in the layer below — measured, from the warm build's own log:
# `rift-types`, `rift-http-proxy`, `rift-lint`, `rift-mock-core` and
# `rift-store-redis` all recompiling at 0.4 s, i.e. after the third-party graph
# had come back from cache. Leaving the submodule untouched is what lets `cook`'s
# work survive into this layer.
#
# The second effect is a narrower cache key: this layer no longer depends on
# `web/`, `scripts/`, `.github/` or anything else the compiler never reads, so
# editing a workflow or a chaos overlay stops invalidating the build.
#
# `docs/` is here because `crates/rift-cluster-server/src/openapi.rs` does
# `include_str!("../../../docs/api/openapi-ee.yaml")` — the OpenAPI contract is
# compiled in, so `docs/` is a build input and not documentation. That is easy to
# miss and would break the image build rather than anything subtler;
# `the_builder_copies_every_root_the_crates_compile_in` pins it, so a new
# compile-time file reference outside these roots fails a test first.
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY tests tests
COPY docs docs

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
# Deliberately NOT split over `chef` the way `builder` is (D-60). Nothing on a
# pull-request lane builds this stage — only `release.yml`, on a tag — so the
# split would buy no PR latency, and a mistake in it would surface for the first
# time during a release. The asymmetry is the point: the stage that runs on every
# cluster-touching PR is the one worth restructuring, and it is the one CI
# exercises on every change.
FROM rust:1.92-slim-bookworm AS builder-static

# `musl-tools`/`musl-dev` alongside the same `build-essential pkg-config git` the `chef` stage
# installs above — `ring` still needs a C toolchain regardless of libc, and `git` is
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
# NO LONGER the last stage in the file (issue #228): `runtime-faketime` — the chaos
# suite's clock-skew flavor, which must build FROM this stage — sits after it, so an
# untargeted build now resolves to *that*. The invariant therefore inverted: every build
# site in the repo pins `target:` explicitly (the base compose, both demo composes, the
# sources overlay, release.yml's matrix, and the README's build command), and
# `every_dockerfile_build_site_pins_a_target` in the chaos suite's `scenarios.rs` fails
# the build of any compose file that forgets — appending a stage can never again silently
# change what anyone builds.
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

# --- runtime-faketime: the chaos suite's clock-skew flavor (issue #228, C12) -----------------
#
# Identical to `runtime` plus libfaketime, selected only by
# `tests/cluster-chaos/compose/faketime.overlay.yml` via `build.target`. Never
# published and never the default: skewing a production node's clock is not a
# feature, and keeping the stage here (rather than a second Dockerfile) is what
# guarantees it can never drift from the runtime image it decorates. The
# symlink gives the overlay an architecture-independent LD_PRELOAD path —
# Debian installs the library under the multiarch triplet, which differs
# between the amd64 CI runner and an arm64 laptop.
FROM runtime AS runtime-faketime
# The runtime stage runs as `rift`; apt needs root, and the drop back is what
# keeps this flavor's only delta the library itself.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends libfaketime \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(find /usr/lib -name 'libfaketime.so.1' | head -n 1)" /usr/local/lib/libfaketime.so.1
USER rift
