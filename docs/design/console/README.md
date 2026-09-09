# Console design — five screens, one key

The console is the browser face of the distributed core (RFC-007 §3.1, D-71). Since #553 it is
**five screens in two groups** and a sign-in that takes the fleet's one API key:

| Group | Screen | What it is for | Backend |
|---|---|---|---|
| Mocks | **Imposters** | List, create, import, export, record; per-imposter detail and the stub editor | `GET/POST/PUT/DELETE /imposters*`, `POST /specs/compile` (stateless, D-72), stub CRUD by id, `savedProxyResponses` |
| Mocks | **Requests** | The recorded requests of one imposter **on the node the browser reached** (D-74) | `GET/DELETE /imposters/:port/requests`, upstream's own per-node journal |
| Mocks | **Scenarios** | Scenario states per space, a space's scoped stubs, flow-state entries; set / reset / tear down / clear | `/imposters/:port/scenarios*`, `/imposters/:port/spaces/*`, `/admin/imposters/:port/flow-state/*` |
| Mocks | **Router** | The replicated route table, in effective order, with pre-flight validation and a tester | `GET/PUT /front-door/routes`, `DELETE /front-door/routes/:id` |
| Fleet | **Cluster** | Members, leader, applied index, readiness, bound ports, the fleet's name — all **read-only** | `GET /_fleet/members`, `GET /_fleet/health` |

Sign in is `POST /session`: the API key set by `--api-key` is exchanged for the `rift_session`
cookie and the browser keeps no copy of the key (D-73, RFC-006 §5.3). There is one credential and
one identity, so there are no permission gates anywhere in the console — whoever signed in holds
the fleet, and every control is offered. Every mutation carries the `X-Rift-CSRF` header the
cookie session requires (RFC-006 §9.2).

The nav model is `web/src/app/nav.ts`, and `nav.test.ts` pins the five entries, their labels and
their two groups. There is no greyed "planned" run any more: RFC-006 §4's roadmap chips carried
screens that were promised and unbuilt, and every screen the reduced console promises is built.

> **Amended by D-71** (RFC-007 §3.2, #553) and **D-73** (#550): this document used to describe
> the RFC-006 §4 screen list — Administration, Sources, Specs, a fleet-merged Requests view, the
> route table's Hits column and not-installed treatment — through a self-contained prototype,
> `console-prototype.html`. Those screens are gone from the fleet. The prototype is kept in this
> directory as a **design artifact, not a client**: it still renders them, and the "state explorer"
> notes near the end of this file say how to read it. Nothing below that names them is normative.

## Imposters

The list is **this node's view of replicated state**, and the scope label says so. Reading it
never fans out: an imposter another node has applied and this one has not would not appear, and
the empty state distinguishes "no imposters, in this node's view" from "cannot confirm the fleet
is empty" when the fleet rail reports a degraded read. Unknown is not zero.

Four actions live in the header, because they act on the screen's subject, and they are listed
here in the order the header renders them (`Imposters.tsx`, left to right, the primary last). A
fifth, **Record**, is on the imposter detail, because it acts on one imposter:

- **Export** — a dialog, because what lands in the file (replay-ready vs as-configured, proxies
  kept or folded) needs more than a button label. A whole-set export is byte-preserving so the
  same fleet exports to the same file (`features/imposters/portable.ts`).
- **Import** — this console's own export format back in: a single imposter, an
  `{"imposters": [...]}` document or a bare list, with a pre-flight (which ports, which already
  exist, which repeat) and a choice between *Add* (N calls, reported per item) and *Replace all*
  (one `PUT /imposters`, behind a typed confirmation).
- **Import from OpenAPI spec** — WireMock Cloud's Import button (#553). An OpenAPI 3.0 document,
  chosen or pasted, JSON or YAML, plus a required port and an optional name. **Compile** sends it to
  `POST /specs/compile?port=…&name=…`, which answers the imposter it built and the operations it
  built it from and **stores nothing** (D-72): no record, no op, no applied-state read. The review
  step shows the port, the stub count and every operation with its method and path template. Only
  then does **Create imposter** send the compiled config through the ordinary create — the same
  `POST /imposters`, idempotency key and parked-write settling as everything else — so closing the
  dialog between the two steps leaves no trace anywhere. The compiler's refusals (an unsupported
  version, an external `$ref`, a parse failure) are the route's `400`, shown as the sentence inside
  the `Error` envelope; `401`/`403`/`503` are not about the document and get the console's own
  guidance instead. An oversize document is refused here, on the file's own byte count or the
  pasted text's, before anything is sent; the route's own `413` is still rendered as a sentence
  should it ever arrive. `?name=` is **percent-encoded** (`encodeURIComponent`, RFC 3986 — never
  `URLSearchParams`, whose `+` for a space the route would take literally) and the route decodes
  it, which is what lets an imposter be called `Pet Store` or `a&b` at all. Decoding is also what
  makes a control character reachable — `%0A` is a newline in a name that is then logged, rendered
  and echoed back — so the route refuses U+0000–U+001F and U+007F with the same `400` as a
  malformed escape. The dialog says in so many words that the document is compiled, not stored,
  because that is the one fact about the flow that the form does not make obvious. Its button is
  named for the format it takes rather than "Import OpenAPI" because *which document* is the
  distinction a reader needs beside a button that takes this console's own export format; it buys
  no disambiguation from that neighbour, whose name is a prefix of this one either way. Code:
  `web/src/features/import/`.
- **New imposter** — a three-step wizard (identity, first stub, review). The port is a form field
  and never auto-assigned: `createImposter` requires it because an auto-assigned port cannot
  replicate, each node would pick its own.

…and on the imposter detail, not in this header:

- **Record** — proxy-and-record against a real upstream, review the recorded stubs, and save them
  into the imposter. It needs an imposter to record *into*, which the list screen has not chosen
  yet. The recording panel is `web/src/screens/RecordingPanel.tsx`.

The **detail** carries the stubs (the form ⟷ raw-JSON editor, with lint-on-save and the
`If-Match` 409 that names both edits and offers reapply-or-discard, never an auto-merge), this
node's recorded requests, and settings. It has **no owner column, no flow-owner row and no ring
panel** — see *The mockup's `OWNER` column is wrong*, below.

## Requests

**The screen names the node it is reading from, always.** Since D-74 (#552) there is no fleet
merge to report on: `GET /imposters/:port/requests` is upstream's own per-imposter journal,
answered by whichever node the browser reached, and `numberOfRequests` is that node's count. So
the table is labelled with the answering node's id — the `node_id` that `GET /_fleet/members`
carries at its top level and that matches exactly one row of its `members` array. A request log
with no node on it is a log the reader will take for the fleet's, and it is not.

There is correspondingly **no partial-merge banner** here and no `Rift-Cluster-Partial` to render:
since D-74 the admin front stamps that header only on the two fleet reads that genuinely fan out,
and this one reaches exactly one node, so it can have missed none. The console asks for the header
on exactly one read — see *Where the partial header actually lands*, below.

**The console never fans out and merges client-side.** Reading all three nodes and stitching the
answers together would be inventing a fleet journal in the browser — with no cursor that means
anything across nodes, and no way to know what it missed. A user who wants the fleet's answer reads
each node and says so. What the screen offers is a *scope*: pick an imposter, read this node's log
for it, and see the node's name on the result. Paging follows upstream's own `x-rift-next-index`
cursor; `x-rift-truncated` is rendered as what it is — rows retention evicted before the reader
got to them — never folded into "empty".

An unreachable node's log is **unknown**, not empty, and the screen says so in those words.

## Scenarios

The console face of the flow-state tier, which RFC-007 v1.1 keeps (D-71, *the flow-state tier
stays*): scenario states per space, a space's scoped stubs, and flow-state entries, each with its
mutating verbs (set a state, reset, tear a space down, clear entries). Every route is upstream's
own and contracted in `openapi-ee.yaml`; the console adds nothing UI-only.

A flow has exactly one owner on the HRW ring (D-20), and this is the surface where flows are
enumerated — so this is where ownership would be shown if it were shown anywhere. It is not on the
imposter.

## Router

The screen that edits the replicated route table (`/front-door/routes`). The feature has been
called the "front door" since upstream issue #19; RFC-007 §6 names it for what it does, and the
console's label and heading follow. **The API path is unchanged** — renaming a path every client
has to follow is its own decision, to be taken once the API has stopped shrinking (#554).

### What the editor has to get right

The route list is ordered by `RouteTable::effective_order()`, not by authoring order — priority
descending, then host specificity (exact → one-label wildcard → no host clause), then path-prefix
length descending, then header-clause count, then id. That order is **independent of input order**,
so an editor showing the order you typed would be showing something that decides nothing. Disabled
routes are excluded from dispatch and shown with `—`. The ordering is ported to
`web/src/features/routes/order.ts` and tested against the upstream rules there.

> **Amended by D-70** (2026-09-01, #539), then **superseded by D-71** (2026-09-06, #545): D-70
> had the not-installed fact read from the local route-table response first and the dispatch-count
> fan-out only as a fallback. That fan-out is gone with the Hits column (RFC-007 §3.2), and the
> not-installed treatment itself went with tenancy (D-73, #550): with one fleet-wide table every
> stored route is installed, so the flag would be a constant `true` and it is removed from the
> contract and the screen alike.

The rule the treatment embodied is worth keeping in view even though its subject is gone: the
screen keyed all of it on a *positive* `installed: false`, never on a body that merely did not say,
because rendering "cannot take a request" off the back of a read the console could not complete
would be a confident claim sourced from an unknown. Every remaining unknown on this screen is held
to the same standard.

The editor validates before the write, mirroring `RouteTable::validate` / `Route::validate`:

| Error | Condition |
|---|---|
| `StripWithoutPrefix` | `strip_prefix` set with no `path_prefix` to strip |
| `MalformedHost` | more than one wildcard, or only a leading `*.` |
| `AmbiguousMatch` | two **enabled** routes that can both win at the same priority |

> **Route fields are snake_case.** `Route`, `RouteMatch` and `RouteTarget`
> (`front_door/route_table.rs`) carry no `serde(rename_all)`, so the wire is `path_prefix`,
> `strip_prefix`, `set_host` — unlike almost everything else in this admin API.
> `docs/api/openapi-ee.yaml` is authoritative; when in doubt, read the Rust struct.

Pre-flight matters because the server refuses the **whole table** rather than repairing part of it —
and because `PUT /front-door/routes` replaces everything while `DELETE /front-door/routes/:id`
removes one. A whole-table write from a long-open editor is a lost update waiting to happen, so the
editor loads a revision, sends it back as `If-Match`, and on a mismatch offers refresh-and-reapply
instead of overwriting. Deleting a single route is the safe operation and is preferred where that is
what was meant.

The **route tester** beside the table walks the same total order the editor computes and applies
the clauses the same way (`features/routes/probe.ts`). Its verdict is **this console's reading**:
the router has no probe endpoint to ask, and the panel says so rather than leaving it implied.

## Cluster

Members, the leader, each voter's applied index, readiness and its pending gates, the ports each
node actually holds the socket for, and the fleet's operator-set name. All of it is read from the
admin port's fleet projections (`/_fleet/members`, `/_fleet/health`), and all of it is **read-only**:
the screen never writes. `PUT /admin/fleet/name` exists on the API and the console does not call it
— the name is shown, and renaming a fleet is a CLI act.

A voter that did not answer is `—` per row, from the projection's own `reachable`/`last_applied`
fields, and the screen's degraded banner is `view.degraded` — a list of reasons `fleetView` derives
from the two bodies, not a header. `Rift-Cluster-Partial` is not what draws anything here.

**No trend charts.** These are point-in-time reads. A sparkline would imply history the API does
not have, which is RFC-006 §3 rule 2 ("nothing UI-only") applied to charts. Applied-spread renders
`—` for a node that did not answer, not `0`: unknown and zero are different facts, and rendering
unknown as zero is how a console launders a gap into a reassuring number.

**No membership or snapshot controls.** See the two *do not rebuild* sections below (D-21, D-24).
Observing membership and changing it are different powers; the screen has the first.

The fleet's name also sits in the top bar on every screen (#373): an operator with staging and
production open in two tabs can otherwise tell them apart only by port, while every destructive act
this console offers is fleet-wide.

## Where the partial header actually lands

The admin front stamps `Rift-Cluster-Partial` on exactly two reads, the ones that fan out to every
voter and can miss one: `GET /_fleet/members` and `GET /_fleet/health` (D-74; the stamp is the
`fleet::classify` branch of `admin_front.rs`, off `FleetBody::partial`, and `decorate.rs` only
names the header). The console asks for it on **one** of them: `/_fleet/health`, through
`apiGetDecorated` (`web/src/app/queries.ts`). That body's `parked_intents_fleet` is a **sum across
voters**, and the header is the only thing that can say the sum is a floor. `/_fleet/members` is
stamped too, but the console reads it with a plain `apiGet` and ignores the header: that body
carries its coverage per row (`reachable`, a `null` `last_applied`), so the header would tell it
nothing its own rows do not. The per-imposter spaces listing makes the same distinction in its
*body*, as a `partial` field, because that route has no header convention to reuse. Nothing else
in the console reads the header at all — the request log is a single node's journal (D-74) and is
never stamped.

The flag rides into `FleetView.parkedIntentsPartial`, and where it renders is the **Imposters**
screen: under the parked-intents tile, as *"at least this many — a node did not answer"*, beside
the two other things that tile can be (`null` — this node could not read its own queue; a plain
number — everything answered). That is the whole surface of the header in this console. Three
facts, three sentences, no two of them folded together.

## Rules that hold on every screen

**Unknown is never rendered as empty or zero.** A degraded fleet read, an unreachable node, a body
that omits a field — each is said in words, never folded into a reassuring default.

**Status is triple-encoded** — glyph shape (● ▲ ■ ○), colour, and word. This came from measurement:
the palette validator put green↔red at ΔE 5.8–7.2 under protanopia/deuteranopia, which no hue
tweak fixes inside a green/amber/red convention, so shape and word carry the meaning and colour
reinforces it. Every status colour clears 4.5:1 on both themes.

**Recorded payloads render as text, never markup.** The request log is the most
attacker-influenced surface in the console — whatever called the mock chose the path, headers and
body. RFC-006 §9.1 bans `dangerouslySetInnerHTML` by lint.

**Every identifier is monospace with tabular figures** — ports, revisions, node names, op-ids,
applied indices. These are values an operator pastes into curl.

**Every write says what it did.** A `202` is a write still committing, polled to a terminal state;
a write the console could not watch land is reported as *unconfirmed*, in those words, never as
saved (`features/writes/commit.ts`). Every mutating route that declares `Idempotency-Key` gets one,
held across an unknown outcome and rotated after a definitive answer (`features/writes/idempotency.ts`).

**Fonts are self-hosted, not fetched.** IBM Plex Sans and Plex Mono ship from `web/src/fonts/`,
so `default-src 'self'` and the air gap both hold; `bundle-offline.test.ts` proves it.

**Desktop only** (RFC-006 §10).

## The token block *is* the design system

The prototype's token block was adopted wholesale into `web/src/styles.css`, along with the
component vocabulary that hangs off it (`card`, `tile`, `pill`, `banner`, `method`, `diag`,
`tabs`, `order-rank`, `clause`, `wizard`). **Change a token in the prototype and in `styles.css`
together**, or the prototype resumes being a design of something we do not ship. Two things did not
carry over, both deliberate: the `:root[data-theme]` overrides (the console ships no theme toggle;
light is `:root`, dark comes from `prefers-color-scheme`) and the violet `--proto` scaffolding.

## The mockup's `OWNER` column is wrong — do not rebuild it

The Aug-2026 mockup (`RiftCluster Console.dc.html`, not checked in) draws an **`OWNER`** column on the imposter
table, a **`FLOW OWNER`** row in the imposter detail rail, and a **`THIS PORT ON THE RING`** panel.
The console shipped all three in #358. They encode an ownership that does not exist, and they have
been **removed** rather than filled in. Registered as **D-20** in `docs/decisions/DECISIONS.md`.

What is actually true:

- **Imposters, stubs and config have no owner.** They go through Raft, so a write propagates from
  the leader to every node and a node that was down catches up when it returns. Every node can
  serve any imposter and answer stateless requests against it. There is no owner to name.
- **A flow has exactly one owner.** One node holds and mutates a stateful flow's state
  (`KeyClass::FlowKv` in `raft/ring.rs`, HRW over the applied membership). A node that receives a
  request for a flow it does not own talks to the owner rather than answering from its own copy,
  and successor replicas hold copies so the state survives the owner leaving.
- **So a port has as many owners as it has flows** — and a "hash key" of `4645` was never a key at
  all. The real key is the flow id under its `ContextScope` prefix (`i{port}:` per imposter by
  default, `f:` fleet-wide), which is also why two imposters' same-named spaces are *one* flow with
  *one* owner under `Fleet` scope.

Ownership therefore belongs on the **flow-state surface**, where flows are actually enumerated —
tracked in [#359](https://github.com/achird-labs/rift-cluster/issues/359).

## The mockup's `Membership` panel is wrong — do not rebuild it

The fleet screen's **`Membership`** panel, with its **Add learner** and **Remove voter** actions, has
been **removed** rather than implemented. It was not a missing endpoint. It is not coming. Registered
as **D-21** in `docs/decisions/DECISIONS.md`.

**Membership changes only ever happen through a node's own lifecycle**: a node is started and
attempts to join, or a node leaves. The console is deliberately neither an admission nor an eviction
vector.

- Admission today is initiated by the **joining node**, over the signed cluster port (`join_via` →
  `/internal/v1/cluster/join` → `admit`). What can enter the fleet is therefore bounded by what an
  operator chose to *start*.
- An admin-API "add learner" taking an advertise address would be a second and weaker entry point —
  operator-supplied input written straight into the replicated membership log, which is the one log
  where a bad address is removable only by another membership change (#68).
- "Remove voter" is the milder half, but it belongs to the same lifecycle: a node leaves by leaving.
  The voter floor that makes departure safe (#69, #71) is enforced by the node and the leader, not
  by whoever is looking at a console.

The *facts* [#366](https://github.com/achird-labs/rift-cluster/issues/366) asserted were all
correct — the machinery is internal-only, there really is no admin route, the floor really is
enforced. It was wrong about what **should** exist. Treat "the console cannot do X to the fleet" as
a question about whether it *should*, not only whether it *can*.

The read-only fleet surface is unaffected: `/_fleet/members` and the Members panel continue to
show membership, because observing it and changing it are different powers.

## The mockup's `Snapshots` panel is wrong — do not rebuild it

Same ruling, same reason. **Trigger snapshot** and **Compact log** have been **removed** rather than
implemented. Snapshotting and log compaction are the cluster's own business. Registered as **D-24**
in `docs/decisions/DECISIONS.md`.

Unlike the Membership panel, this one was **redundant**. `RaftNode::raft_config`
(`crates/rift-cluster/src/raft/node.rs`) builds openraft's `Config::default()` and overrides only
the election and heartbeat timings, so a shipped node runs with `snapshot_policy =
LogsSinceLast(5000)` and `max_in_snapshot_log_to_keep = 1000` — both automatic. The single override,
`NodeConfig::snapshot_log_entries` behind the hidden `--cluster-snapshot-log-entries`, exists so the
chaos tier can exercise the snapshot wire path (#183) and still means `LogsSinceLast(n)`, never a
manual posture. Both paths are pinned by tests named in the register entry.

openraft *does* expose both operations (`trigger().snapshot()` and `trigger().purge_log(upto)`). The
panel was buildable. It is declined anyway. The panel's third, **`Durability & write path`**,
survives as a read and is tracked in [#394](https://github.com/achird-labs/rift-cluster/issues/394).

## The pattern across all three corrections

`OWNER` (#359), `Membership` (#366) and `Snapshots` (#365) were each specified against machinery
that was described **accurately**. Every API named exists; every constraint cited is real. A design
review that checks whether the facts are right approves all three. The question that catches them:

> Not "can the console do this?" but "**should** it?"

A design document can be internally coherent, correct about every API it names, and still describe a
product that should not exist. The same question retired Administration, Sources, Specs, the
merged Requests view and the Hits column in RFC-007.

## The prototype, as history

`console-prototype.html` is the self-contained, zero-dependency prototype the shipped console was
built from. It predates RFC-007 and still renders screens the fleet no longer has (Administration
with its tenant switcher, role matrix and key-shown-once panel; Sources; the merged Requests view;
the route table's Hits and not-installed treatment); it also still shows camelCase route fields,
which were corrected to snake_case in #189. It is kept because it is a **state explorer**, and
states are what mockups habitually omit. The violet strip at the top is scaffolding, and the state is
readable from the query string:

| Parameter | Values |
|---|---|
| `screen` | `login` · `imposters` · `stub` · `requests` · `routes` · `fleet` · `admin` (history) |
| `fleet` | `healthy` · `degraded` (one node unreachable) · `single` |
| `data` | `normal` · `empty` · `overflow` (200 imposters, 40+ char names) |
| `scopeNode` | `rift-1` · `rift-2` · `rift-3` (request log only) |
| `req` | a request id, e.g. `r-8812` (request log only) |
| `stubCase` | `simple` · `unmodelled` · `conflict` (stub editor only) |
| `role`, `adminTab` | history — there are no roles and no Administration screen |

The states still worth opening, because each separates an honest operator console from a plausible
one: `?screen=imposters&data=empty&fleet=degraded` (cannot confirm empty),
`?screen=requests&fleet=degraded&scopeNode=rift-3` (a node's log is unknown, not empty),
`?screen=fleet&fleet=degraded` (`—`, not `0`), `?screen=requests&req=r-8812` (the hostile row
that must render as text), `?screen=stub&stubCase=unmodelled` (the form refuses to open rather than
drop keys), `?screen=stub&stubCase=conflict` (the 409 names both edits).

```sh
open docs/design/console/console-prototype.html
```

It is not the component architecture (the real thing is React + TanStack Query with a client
generated from `openapi-ee.yaml`) and not a data contract (the schema is authoritative). No raster
screenshots are committed; the interactive file is the better artefact regardless.
