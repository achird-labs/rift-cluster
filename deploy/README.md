# Deploying RiftCluster

Five artifacts, in increasing order of how much they promise:

| Path | What it is | Verified how |
|---|---|---|
| `Dockerfile` | The `rift-cluster-server` image, **with the web console** | Built and run by `compose/verify.sh`, which asks every node for `/console` |
| `compose/docker-compose.yml` | A real 3-node cluster for local work | Stood up and asserted by `compose/verify.sh` |
| `compose/front-door-demo.yml` | The "no nginx" front-door demo (one node, two virtual services) | Stood up by hand — see below |
| `compose/sources-demo.yml` | The imposter-sources demo (three nodes + a config server) | Stood up by hand — see below; the properties it shows are asserted by chaos scenarios C20–C23 |
| `k8s/statefulset.yaml` | A production-shaped StatefulSet. Kept for shops that refuse Helm; **the chart is the maintained path** | Schema only (`kubeconform -strict`) — see the caveat below |
| `helm/rift-cluster/` | The same topology as a chart, parameterized. Published as an OCI chart by the release lane | Schema only, but across a values matrix — `helm lint` + `helm template` + `kubeconform -strict` in CI, plus an assertion that the grace period stays derived. Same caveat below |

## Quick start

```sh
git submodule update --init --recursive
docker compose -f deploy/compose/docker-compose.yml up --build

curl -s localhost:12526/readyz     # rift-1 probes
curl -s localhost:12525/imposters  # rift-1 admin API
curl -s localhost:19090/metrics | grep rift_cluster
```

Node 1 founds the cluster; nodes 2 and 3 join through it. Ports are `N2525`
(admin), `N2526` (probes), `N9090` (metrics), where `N` is the node number.

## Verifying it actually works

```sh
deploy/compose/verify.sh
```

Builds the image, starts three nodes, and asserts: all three report `/readyz`
200; the cluster has **three voters and exactly one leader** (read from
`rift_cluster_members`, so a split brain fails the check rather than passing as
"three healthy nodes"); the admin API answers on every node; and the image
reports its own identity, including the embedded upstream Rift.

It is a script rather than a `cargo test` deliberately — it needs a container
runtime, so it must not be able to fail the workspace's CI for a reason that has
nothing to do with the code.

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

## Imposter sources demo

Mocks that live somewhere else. `compose/sources-demo.yml` is the smallest thing
that shows the shape a central-registry deployment already has: **one environment
variable** says where the mocks come from, every node in the fleet serves them,
and rolling the fleet onto new content later is **one call** rather than a
redeploy.

```yaml
RIFT_IMPOSTERS: "http://source-origin:6600/imposters.json,file:/seed/local.json"
```

Two schemes in one list on purpose — some mocks come from a config server the
whole organisation shares, some are baked in beside the app. Under `--cluster`
the flag is sugar for declaring **pinned sources**, so both go through the
replicated log and reach every node.

`source-origin` is the config server: a fourth `rift-cluster-server`, run
un-clustered, whose imposter's response body *is* the config document. That is
why the demo needs no second image — and it is what makes the second half work,
since changing what the fleet should be serving becomes an ordinary admin-API
call rather than a volume edit and a restart.

```sh
docker compose -f deploy/compose/sources-demo.yml up --build
```

Once all three report ready, every node serves both sources' imposters:

```sh
$ curl -s localhost:17001/ ; curl -s localhost:27001/ ; curl -s localhost:37001/
payments v1
payments v1
payments v1

$ curl -s localhost:27002/
local seed
```

### Rolling the fleet with one call

The `/admin/sources*` endpoints ride the **cluster port**, not the admin API: a
source is a control-plane object authenticated with the cluster credential
(`--cluster-secret`), so plain `curl` cannot reach them — every request carries
an HMAC over its method, path and body. `cluster-curl` is the one-file client
for exactly that, and it lives in the crate that defines the format so the two
cannot drift:

```sh
$ cargo run -q -p rift-cluster --example cluster-curl -- \
    --secret local-development-cluster-secret \
    GET http://127.0.0.1:15790/admin/sources
```

Each source is named by an id derived from its URI, so it is stable across
restarts and identical on every node. For the HTTP source above that is
`http-source-origin-6600-imposters-json-7d18d4d6`.

Now change what the config server serves — an ordinary admin write against the
origin, no restart:

```sh
$ curl -s -X PUT localhost:16525/imposters/6600/stubs \
    -H 'Content-Type: application/json' \
    -d '{"stubs":[{"predicates":[{"equals":{"path":"/imposters.json"}}],
         "responses":[{"is":{"statusCode":200,
           "body":"{\"imposters\":[{\"port\":7001,\"protocol\":\"http\",\"name\":\"payments\",\"stubs\":[{\"responses\":[{\"is\":{\"statusCode\":200,\"body\":\"payments v2\\n\"}}]}]}]}"
         }}]}]}' > /dev/null
```

Nothing has changed in the fleet yet — a `pinned` source is pulled when you ask,
never on a timer. One call does it:

```sh
$ cargo run -q -p rift-cluster --example cluster-curl -- \
    --secret local-development-cluster-secret \
    POST http://127.0.0.1:15790/admin/sources/http-source-origin-6600-imposters-json-7d18d4d6/pull
{"revision":16,"digest":"61079cc5…","unchanged":false,"skipped":false,"changed":[7001]}
```

And every node has it — including the two that never received the call:

```sh
$ curl -s localhost:17001/ ; curl -s localhost:27001/ ; curl -s localhost:37001/
payments v2
payments v2
payments v2
```

The fleet fetched the document **once**, on the node that took the call; the
other two applied the bytes it submitted, because a fetch never happens in the
apply path. That is the property container scenario
`c20_source_pull_converges_and_fetches_once` asserts as an equality against the
config server's own request counter, rather than leaving it as a claim in prose.

(Boot is the one place three fetches are expected and correct: `--imposters` is
per node, so each one independently declares the same sources — by an id derived
from the URI, hence identical — and pulls them. The second and third pulls hit
the digest short circuit and write nothing. Fetch-once is a property of *a
pull*, not of a fleet's lifetime.)

A second pull with nothing changed writes no log entry at all and answers
`"unchanged": true` — which is what makes a `tracking` source (re-fetched on
`pollSecs`, by the **leader only**) affordable at a 30-second cadence. See
`docs/rift-cluster-server.md`'s "Imposter sources" section for the full surface:
tracking mode, drift, `onDrift`, provenance, and the credentialed providers.

Tear it down the same way as the other demos:

```sh
docker compose -f deploy/compose/sources-demo.yml down -v
```

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

Two properties are asserted beyond the schema, because they are the ones a
template edit could quietly break while staying valid YAML:

- **A chart with no cluster secret must refuse to render.** `helm install` is one
  command, so a chart that happily installed a fleet with unconfigured peer
  authentication would be worse than the manifest it replaces.
- **`terminationGracePeriodSeconds` stays `2 × leaveTimeoutSeconds + 10`.** The
  manifest states "raise them together, never one alone" in a comment; the chart
  derives it, and CI checks the derivation at three values. Hard-coding it back
  to a literal would pass every schema check and reintroduce the SIGKILL-mid-drain
  that rule exists to prevent.

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

Everything below builds from this checkout. If you only want to *run* a node,
tagging `vX.Y.Z` publishes artifacts that need none of it (#266):

```sh
docker pull ghcr.io/achird-labs/rift-cluster-server:vX.Y.Z    # amd64 + arm64
```

and per-platform binary tarballs with a `SHA256SUMS` on the GitHub Release. Both
carry the web console. See `docs/rift-cluster-server.md` → *Installing from a
release*; the pull-not-build quick start is #269's.

The publish lane rides the same verification that already proved the artifact —
nothing is pushed on a build that failed `check-console-embed.sh` — and the
published image is smoke-tested after the push by pulling it back by tag and
asking it for `--version` and `/console`.

## Images and the upstream pin

`--version` reports which open-source Rift is embedded, e.g.
`rift-cluster-server 0.1.0 (cluster, rift v0.15.0)`. A build context has no git
history, so pass the pin in:

```sh
docker build -f deploy/Dockerfile \
  --build-arg RIFT_UPSTREAM_VERSION="$(git -C vendor/rift describe --tags --always)" \
  -t rift-cluster-server .
```

Omit it and the banner reads `rift unknown` — unhelpful, but never a *wrong*
version, which matters because this string ends up in bug reports.
