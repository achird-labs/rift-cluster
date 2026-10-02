# How RiftCluster works — a reference guide to the internals

**Read it at <https://achird-labs.github.io/rift-cluster/>.**

A guide to how the cluster is designed and how it operates today: the two planes, consensus and
what goes over the wire, the write and request paths, flow ownership and stateful stubs, the
write-ahead logs and convergence, and membership. Most sections include a figure you can drive. You
can elect a leader, sign and tamper with a cluster request, route a request through the front door,
move a flow between owners, cut power mid-fsync, or watch a lagging node catch up from a snapshot.

| Page | Sections |
|---|---|
| [What the cluster is, and its two planes](https://achird-labs.github.io/rift-cluster/index.html) | 1 One node, and what it can't do · 2 The fleet and the load balancer · 3 Two planes |
| [Raft, and Raft under the hood](https://achird-labs.github.io/rift-cluster/consensus.html) | 4 Raft: agreeing on the boring things · 5 Raft under the hood (the cluster port, signed requests, discovery, timers, elections, heartbeats, replication) |
| [Writes, mock requests and routing](https://achird-labs.github.io/rift-cluster/requests.html) | 6 Life of an admin write · 7 Life of a mock request · 8 How a request finds its imposter (ports, spaces, the gateway, the front door, proxy stubs) |
| [Flow ownership and stateful stubs](https://achird-labs.github.io/rift-cluster/flow-state.html) | 9 Who owns a flow · 10 The state behind a stateful stub · 11 Replicas, handoff and fencing · 12 Flow state on disk |
| [Write-ahead logs and convergence](https://achird-labs.github.io/rift-cluster/logs.html) | 13 Two write-ahead logs · 14 How the fleet converges |
| [Membership, and what survives](https://achird-labs.github.io/rift-cluster/membership.html) | 15 Joining, leaving, retiring (including growing a cluster from one node) · 16 What survives what |

## What this is, and what it is not

It is an explanation, not a specification. The design of record is
[`docs/decisions/DECISIONS.md`](../decisions/DECISIONS.md) and the chapters in
[`docs/architecture/`](../architecture/). Where a page here and a `D-n` disagree, the register wins, and
the page is the one to fix. The figures are teaching models: the Raft figure ships whole logs, and
the convergence figure keeps one leader and scales the snapshot numbers down. The route playground,
the ring playground and the request signer run the same rules as the code (the signer computes a
real HMAC-SHA256 over the same fields, with a demo secret).

## Viewing and editing locally

The pages are plain HTML with no build step. Open `index.html` in a browser, or serve the folder:

```sh
python3 -m http.server --directory docs/explained 8000   # then open http://localhost:8000
```

- `assets/explained.css` holds every style; colours are tokens on `:root` with a dark-mode set.
- `assets/explained.js` holds every figure. Each block looks up its own element and returns early
  when it is not on the page, so a figure can move between pages without touching the script.
- A sequence figure is a `<div class="fig seq" id="…">` plus one `SeqScene("…", {…})` call: actors,
  then steps, each with a caption, arrows or notes, and the state each actor holds afterwards.

Publishing is automatic: `.github/workflows/docs-pages.yml` deploys this folder to GitHub Pages on
every push to `master` that touches it.
