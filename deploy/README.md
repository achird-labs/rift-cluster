# Deploying RiftCluster

The artifacts, in increasing order of how much they promise:

| Path | What it is | Verified how |
|---|---|---|
| `compose/cluster.yml` | The same 3-node cluster **from the published image** — no checkout, no submodule, no build | `compose/verify-pulled.sh`: statically on every PR, and for real against the just-published tag in the release lane |
| `Dockerfile` | The `rift-cluster-server` image, **with the web console** | Built and run by `compose/verify.sh`, which asks every node for `/console` |
| `compose/docker-compose.yml` | The same 3-node cluster, built from a checkout | Stood up and asserted by `compose/verify.sh` |
| `compose/front-door-demo.yml` | The "no nginx" front-door demo (one node, two virtual services) | Stood up by hand — see below |
| `k8s/statefulset.yaml` | A production-shaped StatefulSet. Kept for shops that refuse Helm; **the chart is the maintained path** | Schema only (`kubeconform -strict`) — see the caveat below |
| `helm/rift-cluster/` | The same topology as a chart, parameterized. Published as an OCI chart by the release lane | Schema only, but across a values matrix — `helm lint` + `helm template` + `kubeconform -strict` in CI, plus an assertion that the grace period stays derived. Same caveat below |

## Quick start

One file and Docker. No clone, no submodule, no toolchain:

> **No version has been tagged yet**, so the image and the raw URL below do not
> exist. Until `v0.1.0` is cut, build from a checkout — `deploy/compose/verify.sh`
> stands up the same 3-node cluster from source.

```sh
curl -sSLO https://raw.githubusercontent.com/achird-labs/rift-cluster/master/deploy/compose/cluster.yml
docker compose -f cluster.yml up -d

curl -s localhost:12526/readyz     # rift-1 probes
curl -s localhost:12525/imposters       # rift-1 admin API
curl -s localhost:12525/_fleet/members  # voters, leader, this node's bind failures
open http://localhost:12525/console
```

Node 1 founds the cluster; nodes 2 and 3 join through it. Ports are `N2525`
(admin, and the console at `/console`), `N2526` (probes), `N9090` (metrics),
where `N` is the node number.

The image tag is **pinned** in the file, so the same bytes keep meaning the same
thing. `latest` is a deliberate override, never the default:

```sh
RIFT_CLUSTER_VERSION=latest docker compose -f cluster.yml up -d
```

### A single node

The cluster file is three nodes because that is what a cluster is. For one:

```sh
docker run -p 2525:2525 -p 2526:2526 \
  -e RIFT_CLUSTER=true \
  -e RIFT_CLUSTER_BIND=127.0.0.1:4790 \
  -e RIFT_CLUSTER_ALLOW_SOLO=true \
  -e RIFT_CLUSTER_SECRET=local-dev-secret \
  ghcr.io/achird-labs/rift-cluster-server:0.1.0
```

Four environment variables rather than none, and the reason is worth stating
plainly because the short form looks like it should work:

```sh
docker run -p 2525:2525 ghcr.io/achird-labs/rift-cluster-server:0.1.0
```

That command is valid and it does serve — it is the **open-source Rift server**,
byte for byte, with a Mountebank-compatible admin API on `:2525`. What it does
not have is anything the clustered composition adds, and two of those are things
a reader of a quick start would reasonably expect:

| | bare `docker run` | with the four variables above |
|---|---|---|
| `/imposters` (admin API) | `200` | `200` |
| `/console` | **`404`** | `200` |
| `/readyz`, `/healthz` on `2526` | **nothing bound** | `200` |
| `docker ps` health | `healthy` | `healthy` |

Both the console and the probe listener hang off the clustered composition, so
with `--cluster` off neither exists. The health check follows the mode rather
than assuming one: the image's `HEALTHCHECK` runs the built-in `healthcheck`
subcommand with no URL, and the subcommand finds its target itself — the probe
listener's `/healthz` when `RIFT_CLUSTER` is set *or* a probe listener turns
out to be bound anyway (a node clustered purely by command-line arguments
shows the healthcheck exec no environment), and the admin API's `/health`
otherwise (#297). A bare `docker run` is therefore `healthy` while `/console`
still answers `404`, and both facts are by design. One caveat travels with the
fallback: `/health` sits behind `--api-key` like the rest of the admin API, so
an un-clustered container started with `RIFT_APIKEY` reports `unhealthy`. For
that shape, run the cluster of one above — its probe listener is deliberately
unauthenticated — or disable the image's `HEALTHCHECK`.

Each of the four is load-bearing. `--cluster` turns the composition on;
`--cluster-secret` authenticates the cluster port and has no default;
`--cluster-allow-solo` is what lets a seedless node found a cluster instead of
refusing to start (the refusal exists so a node that lost its seed list cannot
silently form a second cluster beside the real one); and `--cluster-bind` has no
default either, because a misconfigured node picking a cluster port for itself is
worse than one that will not start. `--cluster-probe-bind` is **not** in the list
on purpose — it already defaults to `0.0.0.0:2526`, so passing it changes nothing.

`127.0.0.1:4790` is deliberate: a solo node has no peers to reach it, so there is
no reason to expose the cluster port at all.

Note that this stack and `docker-compose.yml` deliberately share service and
container names (`rift-1`…`rift-3`), so they cannot run at the same time. Tear
one down before starting the other.

## Verifying it actually works

**Prerequisites:** Docker with the Compose plugin, and `jq` — every script below
reads `/_fleet/members` through it and refuses to start without it, rather than
comparing against empty strings and reporting a cluster that never formed.

Two scripts, because "the manifests work" is two different claims.

```sh
deploy/compose/verify.sh          # the built-from-source variant
deploy/compose/verify.sh --no-build   # reuse an image already in the docker store
```

Builds the image, starts three nodes, and asserts: all three report `/readyz`
200; **every node** sees three voters, exactly one member claiming leadership and
three reachable members (read from `GET /_fleet/members` on each node's admin
port — a split brain fails the check rather than passing as "three healthy
nodes", because a second member claiming `is_leader` is counted, not read past);
every node names the same leader id; the admin API answers on every node; the
console SPA shell is served on every node; and the image reports its own
identity, including the embedded upstream Rift. The identity check is skipped
under `--no-build`, where the bytes came from somewhere this script did not
build.

```sh
deploy/compose/smoke.sh                   # the core smoke check (RFC-007 §5)
deploy/compose/smoke.sh --attach          # against a fleet that is already up
```

`verify.sh` proves the cluster *forms*; `smoke.sh` proves it *works*. It layers
`smoke.overlay.yml` on the same compose file (an admin key, the router's listener,
and a fourth node behind the `join` profile) and asserts the distributed core's
promises on a real fleet: an imposter written on one node answers on every node
through the router; a route table written on one node is installed everywhere; a
stopped node catches up; a stopped leader is replaced and writes keep landing; a
node joins from a seed, serves the same imposters, and leaves the voter set when
it goes. Every removal under epic #544 runs it before and after. `--keep` leaves
the fleet up for hand-driving; `--attach` skips the build and teardown.

Both run in CI, in `ci.yml`'s **`compose-smoke`** job: it reuses the image
`cluster-smoke-prepare` already built (D-58), so neither script pays for a build,
and runs `verify.sh --no-build` then `smoke.sh --no-build` behind the same
`deploy/` path filter the chaos tier uses. It is deliberately **not** a required
check — it stands two real fleets up and carries every container-runtime flake
that implies — but it is what makes these two scripts something that goes red on
the PR that broke them. They had no invoker at all between #562 and this lane,
which is the failure mode a script has that a test does not:
`the_compose_verification_scripts_have_a_ci_invoker` in `tests/cluster-chaos`
now fails if the job or either invocation is removed.

```sh
deploy/compose/verify-pulled.sh --check   # static; no daemon, no pull
deploy/compose/verify-pulled.sh           # pulls the pinned tag and runs it
RIFT_CLUSTER_VERSION=0.2.0 deploy/compose/verify-pulled.sh
```

`verify.sh` cannot cover `cluster.yml`: it passes `--build`, so a pulled-image
manifest naming a nonexistent tag, or one that had drifted from the topology the
built variant asserts, would never fail it. `verify-pulled.sh` is that second
proof, and it splits in two because the halves can run in different places:

- `--check` needs nothing at all — no daemon, no network, no image — so it runs
  on **every PR** in `ci.yml`'s `build` job. It asserts the properties that make
  `cluster.yml` what it claims to be: no `build:` key anywhere; a tag pinned to
  the **workspace version**, with `latest` only as an override; every service
  resolving to that image once the `x-node` anchor is expanded; the same
  per-service published ports as `docker-compose.yml`; `stop_grace_period` still
  greater than the leave timeout; and — repo-wide — that no section titled "Quick
  start" begins with a clone. The pin is derived rather than written down twice,
  so bumping the workspace version fails this check until `cluster.yml` is bumped
  with it, and the file cannot age into naming a release nobody can pull.
- The full run needs a published image, so its only honest home is the **release
  lane**, after the tag is pushed (`release.yml`, job `image-manifest`). It pulls
  and asserts readiness on all three, three voters and one agreed leader from
  `GET /_fleet/members`, and the console SPA shell on every node — against the
  artifact users will actually get. On a real release it runs with no version
  override at all, so the pin *in the file* is the thing under test; only a
  prerelease, whose tag legitimately cannot match the pin, passes one in.

Between those two, the window where `cluster.yml` could rot unnoticed is the
window where nothing changed.

Both are scripts rather than `cargo test`s deliberately — they need a container
runtime, so they must not be able to fail the workspace's CI for a reason that
has nothing to do with the code.

## No-nginx front door demo

Rift's front door (`--front-door`, issue #19 / U-11) resolves a request by
`Host` header (or path) and dispatches it to the matching imposter, in
process. `compose/front-door-demo.yml` is the smallest thing that shows this
replacing a reverse-proxy sidecar: one `rift-cluster-server`, one exposed port, two
virtual services.

```sh
docker compose -f deploy/compose/front-door-demo.yml up --build
```

Then, from another terminal:

```sh
$ curl -s http://localhost:8080/ -H 'Host: payments.test'
payments service

$ curl -s http://localhost:8080/ -H 'Host: search.test'
search service
```

Same port, same process — `Host` is the only thing that decided which
imposter answered. The two virtual services and the route table binding them
together are declared once, in `compose/front-door-demo.config.json`, and
loaded at boot via `--configfile`: this demo is deliberately un-clustered, and
route writes over the admin API (`PUT /front-door/routes`) are a `--cluster`
feature (issue #131) — upstream never shipped one for the un-clustered path.

Tear it down the same way as the cluster demo:

```sh
docker compose -f deploy/compose/front-door-demo.yml down -v
```

## The admin plane is open until you give it a key

Every manifest here ships **without** an admin credential, and that is a choice
rather than an oversight: with no key set, the admin API on port 2525 answers
anyone who can reach it, so a reference deployment applies and works with nothing
to edit first. Set one — `MB_APIKEY` (equivalently `--api-key`) in compose, the
optional `adminApiKey` in the Helm chart, the commented `admin-api-key` Secret
entry in `deploy/k8s/statefulset.yaml` — and the **whole** admin plane closes
behind that one key. There is one credential, not a set of them: every admin
request carries it as a raw `Authorization: <key>` header, and the console
exchanges it once for a session cookie via `POST /session` rather than holding it
in the browser. What the key does *not* cover is deliberate: `/healthz` and
`/readyz` stay open so orchestrators can probe a node that is refusing admin
traffic, and the imposter data plane stays open because the mock is the thing
under test — putting a credential in front of it would break every caller it
exists to serve. An **empty** value is not the same as an unset one: it closes
the plane behind a credential nobody can present, so leave the variable out
entirely rather than setting it to `""`. `deploy/compose/smoke.overlay.yml` is
the worked example of the closed shape; `deploy/compose/verify.sh` and
`verify-pulled.sh` run against the open one and need no key.

## The rule these manifests exist to encode

On SIGTERM a node **fails readiness first**, keeps serving in-flight work for
`--cluster-leave-timeout`, and only then closes its listeners. So the
orchestrator's kill deadline must be at least **twice** the leave timeout:

| Setting | compose | k8s |
|---|---|---|
| leave timeout | `RIFT_CLUSTER_LEAVE_TIMEOUT: 5` | `RIFT_CLUSTER_LEAVE_TIMEOUT: 15` |
| kill deadline | `stop_grace_period: 15s` | `terminationGracePeriodSeconds: 40` |

Raise them together. Raising only the leave timeout means the process is killed
mid-drain and the graceful leave silently becomes a hard one — every in-flight
request turns into a client-visible error, which is the exact failure the drain
exists to prevent.

## Kubernetes caveats

**The manifest is schema-validated, not behaviourally verified.** There is no
cluster here, so it is checked against the Kubernetes JSON schemas:

```sh
docker run --rm -v "$PWD/deploy/k8s:/mnt" ghcr.io/yannh/kubeconform:latest \
  -strict -summary /mnt/statefulset.yaml
# Summary: 4 resources found in 1 file - Valid: 4, Invalid: 0
```

Note that `kubectl apply --dry-run=client` is *not* an offline check: it fetches
the API group list from a cluster and fails without one, which is why the
container above is used instead.

**The chart carries the same caveat, across more axes.** `ci.yml`'s `helm` job
runs `helm lint`, then renders a values matrix — defaults, everything-on,
single-node, and the three environment files — through the same
`kubeconform -strict`. That is a wider net than the raw manifest gets, and it is
still schema only: nothing here proves a cluster forms.

So a set of properties is asserted beyond the schema — each one a decision the
raw manifest argues for in a comment, and each one something a template edit
could quietly break while still emitting valid YAML:

- **A chart with no cluster secret must refuse to render.** `helm install` is one
  command, so a chart that happily installed a fleet with unconfigured peer
  authentication would be worse than the manifest it replaces. The refusal
  *reason* is matched too, not just the exit status — otherwise an unrelated
  template syntax error would score as a pass on the security guard.
- **A chart pointed at the `-static` flavor must refuse to render.** That image is
  `FROM scratch` and every pod's container command here is a shell script, so a
  `-static` tag renders a StatefulSet whose pods all fail at exec — at rollout,
  with a bare container error and nothing naming the cause. The refusal *reason*
  is matched on the guard's own prose rather than on `-static`, which is also a
  substring of the tag the check injects and so would match for the wrong reason.
- **`terminationGracePeriodSeconds` stays `2 × leaveTimeoutSeconds + 10`.** The
  manifest states "raise them together, never one alone" in a comment; the chart
  derives it, and CI checks the derivation at three values. Hard-coding it back
  to a literal would pass every schema check and reintroduce the SIGKILL-mid-drain
  that rule exists to prevent. `values.schema.json` closes the other half:
  Helm's `int` is a coercion, not a parse, so `leaveTimeoutSeconds=abc` would
  otherwise render a grace period of 10 and `=-5` a negative one, both of which
  `kubeconform -strict` accepts.
- **Two Services, distinctly named, and the seed points at the headless one.**
  Checked under a 53-character release name, because name truncation is invisible
  until the name is long: a `-peers` suffix appended to an already-truncated base
  disappears, the headless Service collapses onto the client one, and the seed
  address ends up naming a readiness-gating Service that publishes nothing during
  a cold start. That is a cluster that never forms, from a chart that lints,
  renders and schema-validates.
- **`publishNotReadyAddresses` appears exactly once, and `exec` survives.** The
  first on the client Service would route traffic to unconverged nodes; losing the
  second leaves `sh` as PID 1, so SIGTERM never reaches the server and the drain
  everything above is tuned around silently never happens.

```sh
helm install rift oci://ghcr.io/achird-labs/charts/rift-cluster \
  --version 0.1.0 -f deploy/helm/rift-cluster/values-eks.yaml
```

Example values for on-prem, EKS and AKS live beside the chart. The cluster secret
is mounted as a **file** in every one of them (`--cluster-secret-file`), never
inlined into an env var — env is visible to anything that can read the pod spec,
and it lands in `kubectl describe` and most telemetry agents' process metadata.

Schema-valid is a real but limited guarantee — it proves the manifest is
well-formed and every field exists, not that the cluster behaves as intended.
Treat this as a well-reasoned starting point, not as something proven the way
the compose setup is.

Two decisions worth understanding before changing them:

- **`publishNotReadyAddresses: true`** on the headless Service is required, not
  incidental. During a cold start no node is Ready until it has joined, so a
  Service that hid not-ready pods would make joining impossible — the cluster
  would deadlock waiting for itself.
- **Ordinal 0 bootstraps; the rest join.** A StatefulSet template is uniform but
  the founding node is not, so the container command branches on the pod
  ordinal. Handing `rift-0` a seed list containing only itself would deadlock it
  against a cluster that does not exist yet. The branch `exec`s the server so
  SIGTERM still reaches PID 1 — without that the graceful leave never runs.

The two Services are also deliberate: the headless one (peer discovery) ignores
readiness, and the `rift` one (client traffic) respects it. Collapsing them
would either break joining or route traffic at nodes that have not converged.

## Published artifacts

Tagging `vX.Y.Z` publishes artifacts that need no part of this checkout (#266):

```sh
docker pull ghcr.io/achird-labs/rift-cluster-server:vX.Y.Z    # amd64 + arm64
```

and per-platform binary tarballs with a `SHA256SUMS` on the GitHub Release. Both
carry the web console. See `docs/rift-cluster-server.md` → *Installing from a
release* for the download-and-verify steps.

The chart and the image are the portable artifacts every reference deployment
consumes (D-35), and the non-goals are deliberate: no Terraform / CloudFormation /
Bicep modules, no OS package-manager packages, no auto-update. Chapters 14 and 15
compose those two with each platform's own primitives; anything beyond that is the
operator's composition, not a shipped artifact.

The publish lane rides the same verification that already proved the artifact —
nothing is pushed on a build that failed `check-console-embed.sh` — and the
published image is smoke-tested after the push: pulled back by tag and asked for
`--version` and `/console`, then stood up as the **three-node `cluster.yml`
stack** and put through the same trio `verify.sh` asserts. That last step is what
makes the quick start at the top of this file a tested path rather than a
plausible one; a `cluster.yml` that named a tag the lane had not actually pushed
would fail the release rather than the first person to try it. `:latest` is
promoted only after those smokes pass, so an untagged pull can never resolve to a
build the smokes rejected — only the exact version tag exists before they run.

## Image flavors

Every tag above is actually two images (#270), selected by a `-static` suffix:

| Flavor | Base | Tags | Shell | OS packages |
|---|---|---|---|---|
| default | `debian:bookworm-slim` | `vX.Y.Z`, `latest` | Yes | the usual Debian slim set |
| `-static` | `FROM scratch` (musl) | `vX.Y.Z-static`, `latest-static` | No | zero |

```sh
docker pull ghcr.io/achird-labs/rift-cluster-server:vX.Y.Z-static
```

The static flavor trades the OS away entirely — no package manager, no libc dynamic loader, no
shell, nothing but the binary, a CA bundle, and a passwd entry for its non-root user — for the
narrowest attack surface this image can have. Two real limitations follow from that trade, worth
knowing before picking it:

- **No shell.** There is no `docker exec … sh` on this image — there is nothing to exec into.
  This also rules it out for Kubernetes today: the StatefulSet runs `/bin/sh -c` as its container
  command, with the ordinal-0 bootstrap branch inside the script it passes. That command lives on
  the pod template, so it is uniform across ordinals — a shell-less image does not merely break the
  founding node, it fails **every** pod with a container exec error. **The default flavor remains
  the recommended Kubernetes image** — the Helm chart's `image.repository` defaults to it, with no
  `-static` option exposed, and the chart **refuses to render** if `image.tag` is pointed at a
  `-static` tag, so this is a message from `helm install` rather than a discovery at rollout.

- **A different system allocator.** Neither flavor bundles mimalloc — this workspace takes
  `rift-http-proxy` with `default-features = false` (root `Cargo.toml`), so upstream's
  binary-only mimalloc default never reaches either image. What does differ is the C library
  underneath: the static flavor uses **musl's** allocator, the default flavor **glibc's**. musl's
  is markedly slower under allocation-heavy, highly concurrent load. Benchmark numbers therefore
  do not transfer between flavors — that is an allocator difference, not a regression, but a
  surprising one if the flavor switch is forgotten between runs.

## Images and the upstream pin

`--version` reports which open-source Rift is embedded, e.g.
`rift-cluster-server 0.1.0 (cluster, rift v0.15.0)`. A build context has no git
history, so pass the pin in:

```sh
docker build -f deploy/Dockerfile --target runtime \
  --build-arg RIFT_UPSTREAM_VERSION="$(git -C vendor/rift describe --tags --always)" \
  -t rift-cluster-server .
```

(`--target runtime` is required since issue #228: the Dockerfile's last stage is the
chaos suite's clock-skew flavor, so an untargeted build no longer produces the
production image.)

Omit it and the banner reads `rift unknown` — unhelpful, but never a *wrong*
version, which matters because this string ends up in bug reports.
