# How RiftCluster works — an interactive walkthrough

**Read it at <https://achird-labs.github.io/rift-cluster/>.**

These pages build the design up from a single node and show where the obvious approach breaks. Most
of them include figures you can drive yourself. You can elect a leader, tamper with a signed
cluster request, commit a write, route a request through the front door, move a flow between
owners, pull the plug on a node mid-fsync, and watch a lagging node catch up from a snapshot.

| Page | Sections |
|---|---|
| [Why a cluster, and its two planes](https://achird-labs.github.io/rift-cluster/index.html) | 1 One node, and what it can't do · 2 The fleet and the load balancer · 3 What breaks with the obvious design · 4 Two planes |
| [Raft, and Raft under the hood](https://achird-labs.github.io/rift-cluster/consensus.html) | 5 Raft: agreeing on the boring things · 6 Raft under the hood (the cluster port, signed requests, discovery, timers, elections, heartbeats, replication) |
| [Writes, mock requests and routing](https://achird-labs.github.io/rift-cluster/requests.html) | 7 Life of an admin write · 8 Life of a mock request · 9 How a request finds its imposter (ports, spaces, the gateway, the front door, proxy stubs) |
| [Flow ownership and stateful stubs](https://achird-labs.github.io/rift-cluster/flow-state.html) | 10 Who owns a flow · 11 The state behind a stateful stub · 12 Replicas, handoff and fencing · 13 Flow state on disk |
| [Write-ahead logs and convergence](https://achird-labs.github.io/rift-cluster/logs.html) | 14 Two write-ahead logs · 15 How the fleet converges |
| [Membership, and what survives](https://achird-labs.github.io/rift-cluster/membership.html) | 16 Joining, leaving, retiring (including growing a cluster from one node) · 17 What survives what |

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
