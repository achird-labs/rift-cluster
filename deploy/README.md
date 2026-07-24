# Deploying Rift Enterprise

Four artifacts, in increasing order of how much they promise:

| Path | What it is | Verified how |
|---|---|---|
| `Dockerfile` | The `rift-ee-server` image | Built and run by `compose/verify.sh` |
| `compose/docker-compose.yml` | A real 3-node cluster for local work | Stood up and asserted by `compose/verify.sh` |
| `compose/front-door-demo.yml` | The "no nginx" front-door demo (one node, two virtual services) | Stood up by hand — see below |
| `k8s/statefulset.yaml` | A production-shaped StatefulSet | Schema only (`kubeconform -strict`) — see the caveat below |

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
replacing a reverse-proxy sidecar: one `rift-ee-server`, one exposed port, two
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

## Images and the upstream pin

`--version` reports which open-source Rift is embedded, e.g.
`rift-ee-server 0.1.0 (enterprise, rift v0.15.0)`. A build context has no git
history, so pass the pin in:

```sh
docker build -f deploy/Dockerfile \
  --build-arg RIFT_UPSTREAM_VERSION="$(git -C vendor/rift describe --tags --always)" \
  -t rift-ee-server .
```

Omit it and the banner reads `rift unknown` — unhelpful, but never a *wrong*
version, which matters because this string ends up in bug reports.
