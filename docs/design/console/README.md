# Console design prototype — RFC-006 scope A

`console-prototype.html` is a **self-contained, zero-dependency** prototype of every RFC-006 §4 screen
whose backend has shipped or is sliced:

| Screen | Slice | Issue |
|---|---|---|
| Sign in — API key exchanged for a session cookie | C2 | [#185](https://github.com/achird-labs/rift-cluster/issues/185) |
| App shell, tenant switcher, imposters, cluster/fleet | C4 | [#187](https://github.com/achird-labs/rift-cluster/issues/187) |
| Stub editor — form ⟷ JSON, lint, 409 rebase | C5 | [#188](https://github.com/achird-labs/rift-cluster/issues/188) |
| Request log (per-node) and front-door route editor | C6 | [#189](https://github.com/achird-labs/rift-cluster/issues/189) |
| Tenants, principals, roles, audit | C7 | [#190](https://github.com/achird-labs/rift-cluster/issues/190) |

Scenarios and flow state (#149), sources (#20) and specs (#148) appear as greyed nav entries carrying
their issue number — a visible roadmap rather than a 404, which is what §4 asks for.

Open it in any browser. No build step, no server, no network access — which is deliberate: the real
console lives under the same constraint (RFC-006 §9.1's `default-src 'self'`, air-gapped, no CDN), so
a prototype that needed a CDN would be prototyping something we cannot ship.

```sh
open docs/design/console/console-prototype.html
```

## It is a state explorer, not a mockup

The violet dashed strip at the top is **prototype scaffolding, not product UI**. It exists because the
screens' hard problems are all *states*, and states are what mockups habitually omit. Four axes, and
the state is also readable from the query string so any combination is linkable:

| Parameter | Values |
|---|---|
| `screen` | `login` · `imposters` · `stub` · `requests` · `routes` · `fleet` · `admin` |
| `role` | `fleet-admin` · `editor` · `viewer` |
| `fleet` | `healthy` · `degraded` (one node unreachable) · `single` |
| `data` | `normal` · `empty` · `overflow` (200 imposters, 40+ char names) |
| `scopeNode` | `rift-1` · `rift-2` · `rift-3` (request log only) |
| `req` | a request id, e.g. `r-8812` (request log only) |
| `stubCase` | `simple` · `unmodelled` · `conflict` (stub editor only) |
| `adminTab` | `tenants` · `principals` · `audit` (administration only) |

```
console-prototype.html?screen=requests&fleet=degraded&scopeNode=rift-3
console-prototype.html?screen=imposters&data=empty&fleet=degraded
console-prototype.html?screen=stub&stubCase=conflict
console-prototype.html?screen=admin&adminTab=principals
```

## The states worth looking at first

These are the ones that separate an honest operator console from a plausible-looking one:

1. **`?screen=imposters&data=empty&fleet=degraded`** — a naive console says "no imposters" here and is
   *wrong*. An imposter configured on the node that did not answer would not appear. This prototype
   says "cannot confirm this tenant is empty" and names the coverage.
2. **`?screen=requests&fleet=degraded&scopeNode=rift-3`** — the scoped node is unreachable. Its log is
   **unknown**, not empty, and the screen says so in those words.
3. **`?screen=requests&data=empty`** — the reachable-and-genuinely-empty case, for contrast with (2).
   Two different screens, deliberately.
4. **`?screen=fleet&fleet=degraded`** — applied-spread renders `—`, not `0`. Unknown and zero are
   different facts, and rendering unknown as zero is how a console launders a gap into a reassuring
   number.
5. **`?screen=requests&req=r-8812`** — the hostile row (see below).
6. **`?screen=imposters&role=viewer`** — write affordances gone. UX only; the API is the boundary.
7. **`?screen=stub&stubCase=unmodelled`** — a stub using `space`, the scenario-FSM fields and
   `behaviors.wait`. The form **refuses to open** rather than dropping keys it cannot model, because
   the config the user saves must be the config they wrote.
8. **`?screen=stub&stubCase=conflict`** — the `If-Match` 409. It names both edits and offers
   reapply-or-discard; it never auto-merges.
9. **`?screen=routes`** — the route table in **effective order**, with the tie-break chain spelled out
   and pre-flight validation for the three errors the server actually raises.
10. **`?screen=admin&adminTab=principals`** then *Mint principal* — the key-shown-once panel, with no
    reveal-later action because there is nothing to reveal.

## Front-door routes: what the editor has to get right

The route list is ordered by `RouteTable::effective_order()`, not by authoring order — priority
descending, then host specificity (exact → one-label wildcard → no host clause), then path-prefix
length descending, then header-clause count, then id. That order is **independent of input order**, so
an editor showing the order you typed would be showing something that decides nothing. Disabled routes
are excluded from dispatch and shown with `—`.

**A non-default tenant's table is never in any order at all.** `desired_routes` compiles only the
default tenant's routes into the shared front door (`08-tenancy-security.md`), so for every other
tenant the screen is showing stored state that cannot take a request. The server says so with
`installed: false` on `GET /front-door/route-hits`, and the screen spends it four ways (#400): a
`role="status"` banner naming the fact and its reason, `—` in every rank cell, `not installed` in
place of the tie-break prose, and the rows listed in **stored** order rather than
`effective_order()` — sorting by a chain that is never evaluated would be the same fabrication the
rank column is being muted to avoid. Editing stays enabled — the table is real replicated state — and
all of it keys on a positive `installed: false`, never on a hits read that merely failed. That last
point is the rule, not an implementation detail: rendering "cannot take a request" off the back of a
read the console could not complete would be a confident claim sourced from an unknown.

**A fleet with no listener at all gets the same treatment, one level down (#403).** `--front-door`
is optional, so every node in a fleet can be running without one — and then every route reports an
honest zero, which the Hits column otherwise flags as the "wrong or dead" state. When the server
answers `front_door: "none"` the screen states that once in a banner and renders those zeros muted
rather than flagged, because flagging every row at once is the false diagnosis, not a warning. The
ranks and the evaluation order are untouched: unlike the not-installed case these routes really are
installed and really would be evaluated — there is simply nothing listening yet. And as above, only
the server's *proven* `none` counts; `unknown` renders exactly as today.

The editor validates before the write, mirroring `RouteTable::validate` / `Route::validate`:

| Error | Condition |
|---|---|
| `StripWithoutPrefix` | `strip_prefix` set with no `path_prefix` to strip |
| `MalformedHost` | more than one wildcard, or only a leading `*.` |
| `AmbiguousMatch` | two **enabled** routes that can both win at the same priority |

> **Route fields are snake_case.** `Route`, `RouteMatch` and `RouteTarget`
> (`front_door/route_table.rs`) carry no `serde(rename_all)`, so the wire is `path_prefix`,
> `strip_prefix`, `set_host` — unlike almost everything else in this admin API. The prototype in
> this directory still shows the camelCase spellings; it predates the correction (#189) and is kept
> as-is because it is a design artifact, not a client. `docs/api/openapi-ee.yaml` is authoritative,
> and it was itself wrong here until #189 — when in doubt, read the Rust struct.

Pre-flight matters because the server refuses the **whole table** rather than repairing part of it —
and because `PUT /front-door/routes` replaces everything while `DELETE /front-door/routes/:id` removes
one. A whole-table write from a long-open editor is a lost update waiting to happen, so the editor
loads a revision, sends it back, and on a mismatch offers refresh-and-reapply instead of overwriting.
Deleting a single route is the safe operation and should be preferred where that is what was meant.

## Administration: the two behaviours not to soften

**A key is shown once.** The fleet stores an argon2id hash, so there is nothing to reveal later and no
reveal action is offered — one would teach operators to expect a feature that cannot exist.

**A refusal is a committed row.** The audit fixture includes a quota refusal at revision 1282. Quotas
are validated where the op applies, so a refusal is a decision the fleet agreed to and belongs in the
stream like any other outcome; a viewer showing only successes hides the interesting half.

The role matrix is rendered as a matrix on purpose. `authz.rs::role_allows` is written as explicit
per-role arms precisely so a security reviewer can read the table, and the UI should have the same
property. Note `audit.read` is deliberately **not** a Viewer grant and deliberately **not** part of
`tenant.manage`, and `FleetAdmin` binds only on the fleet scope `*` — so it is never offered as an
in-tenant role.

## Design decisions, and why

**No trend charts on the fleet screen.** `/_fleet/health` and `/_fleet/members` are point-in-time
reads. A sparkline would imply history the API does not have, which is RFC-006 §3 rule 2 ("nothing
UI-only") applied to charts. The single chart is magnitude-by-identity over `numberOfRequests` — a
value the imposter body genuinely carries — and it is labelled *this node, not a fleet total*.

**The request log's scope label appears only for an incomplete merge.** #147 H landed the convergence
RFC-006 §4 promised: the screen reads the fleet's already-merged journal rather than one node's own,
so there is no per-node fact left to keep permanently in front of the reader, and the old
never-collapsing strip is gone with it. What survives is the one case an operator still needs told to
them before trusting a result — the merge's own `Rift-Cluster-Partial` header, stamped when the
fan-out could not reach every node inside its budget — and the label renders, undismissable, exactly
then.

**The console never fans out and merges client-side.** That would reinvent the verification plane
without its cursors or gap repair, producing a merged view with no way to know what it missed.

**Status is triple-encoded** — glyph shape (● ▲ ■ ○), colour, and word. This came from measurement:
the palette validator put green↔red at ΔE 5.8–7.2 under protanopia/deuteranopia, which no hue tweak
fixes inside a green/amber/red convention, so shape and word carry the meaning and colour reinforces
it. The same run caught a first-draft amber at 2.24:1 against white; light-mode amber is now `#8A5A00`
at 5.93:1. Every status colour clears 4.5:1 on both themes.

**Recorded payloads render as text, never markup.** Request `r-8812` carries a `<script>` tag in its
path and an `onerror` attribute in its user-agent **on purpose**. This is the most
attacker-influenced surface in the console — whatever called the mock chose the path, headers and
body. If that row ever executes, the escaping regressed. RFC-006 §9.1 additionally requires
`dangerouslySetInnerHTML` banned by lint in the real implementation.

**Every identifier is monospace with tabular figures** — ports, revisions, node names, op-ids, applied
indices. These are values an operator pastes into curl. `rift-tui` is the interaction precedent
RFC-006 §4 names deliberately, and this is where that lineage shows.

**Typography uses system stacks, not a webfont.** The CSP blocks font CDNs and the binary must work
air-gapped. Deliberately not Inter.

> The shipped console no longer follows this prototype here. It self-hosts IBM Plex Sans and Plex
> Mono from `web/src/fonts/`, which answers the same constraint a different way — the faces are in
> the bundle and served same-origin, so `default-src 'self'` and the air gap both still hold, and
> `bundle-offline.test.ts` proves it. The prototype keeps system stacks because it is a single file
> meant to open from disk with nothing beside it.

**Desktop only** (RFC-006 §10). The narrow-window collapse here is prototype convenience, not a
mobile layout.

## The mockup's `OWNER` column is wrong — do not rebuild it

The Aug-2026 mockup (`RiftCluster Console.dc.html`) draws an **`OWNER`** column on the imposter
table, a **`FLOW OWNER`** row in the imposter detail rail, and a **`THIS PORT ON THE RING`** panel.
The console shipped all three in #358. They encode an ownership that does not exist, and they have
been **removed** rather than filled in.

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
tracked in [#359](https://github.com/achird-labs/rift-cluster/issues/359), which was itself filed
from the mockup's framing and has been re-specified.

This section exists because the mockup is still the artifact people design from, and nothing in it
signals that these three elements are wrong. If you are porting a screen from it, this is the one
place that says so.

## The mockup's `Membership` panel is wrong — do not rebuild it

The fleet screen's **`Membership`** panel, with its **Add learner** and **Remove voter** actions, has
been **removed** rather than implemented. It was not a missing endpoint. It is not coming.

**Membership changes only ever happen through a node's own lifecycle**: a node is started and
attempts to join, or a node leaves. The console is deliberately neither an admission nor an eviction
vector.

Why the distinction matters, rather than being a matter of taste:

- Admission today is initiated by the **joining node**, over the signed cluster port (`join_via` →
  `/internal/v1/cluster/join` → `admit`). What can enter the fleet is therefore bounded by what an
  operator chose to *start*.
- An admin-API "add learner" taking an advertise address would be a second and weaker entry point —
  operator-supplied input written straight into the replicated membership log, which is the one log
  where a bad address is removable only by another membership change (#68).
- "Remove voter" is the milder half, but it belongs to the same lifecycle: a node leaves by leaving.
  The voter floor that makes departure safe (#69, #71) is enforced by the node and the leader, not
  by whoever is looking at a console.

Note that the *facts* [#366](https://github.com/achird-labs/rift-cluster/issues/366) asserted were
all correct — the machinery is internal-only, there really is no admin route, the floor really is
enforced. It was wrong about what **should** exist, which is why a premise check that only verifies
facts will wave this class of issue through. Treat "the console cannot do X to the fleet" as a
question about whether it *should*, not only whether it *can*.

The read-only fleet surface is unaffected: `/_fleet/members` and the `Members` panel continue to
show membership, because observing it and changing it are different powers.

## The mockup's `Snapshots` panel is wrong — do not rebuild it

Same ruling, same reason. **Trigger snapshot** and **Compact log** have been **removed** rather than
implemented. Snapshotting and log compaction are the cluster's own business, and an operator button
for them is not an operator's to press — not even a fleet admin's.

Unlike the Membership panel, this one was not merely unwise: it was **redundant**. The fleet already
does both, unprompted. `RaftNode::raft_config` (`crates/rift-cluster/src/raft/node.rs`) builds
openraft's `Config::default()` and overrides only the election and heartbeat timings, so a shipped
node runs with:

- `snapshot_policy = LogsSinceLast(5000)` — a snapshot every 5000 entries since the last, automatic;
- `max_in_snapshot_log_to_keep = 1000` — logs a snapshot already covers are purged automatically.

The single override is `NodeConfig::snapshot_log_entries`, reachable through a **hidden** flag —
`--cluster-snapshot-log-entries`, env `RIFT_CLUSTER_SNAPSHOT_LOG_ENTRIES` — that exists so the
container chaos tier can exercise the snapshot wire path at all (#183). It is `hide = true`, unset
on every shipped path, and its own doc says a fleet that sets it "is trading away log retention for
nothing". Note that setting it does **not** reach a manual posture either: `Some(n)` means
`LogsSinceLast(n)`, still automatic, and it additionally forces `max_in_snapshot_log_to_keep = 0`.
Both paths are covered — `a_shipped_fleet_snapshots_and_purges_without_being_asked` for the default,
`the_snapshot_knob_sets_the_policy_and_purges_immediately` for the override.

The first of those is deliberately a *different* claim from the older
`raft_config_default_leaves_the_snapshot_knobs_untouched`: that one says "we do not override
openraft", which would still pass if a future openraft defaulted to `SnapshotPolicy::Never` — a mode
that waits for a manual trigger nothing here calls, letting the log grow without bound.

Worth noting so it is not re-litigated: openraft *does* expose both operations, and they *are*
distinct (`trigger().snapshot()` and `trigger().purge_log(upto)`). The panel was buildable. It is
declined anyway.

The panel's third of the `fleet-ops` row, **`Durability & write path`**, survives — it is the only
one of the three that asked to *read* rather than to *act*, and reading back what a node is
configured to do changes nothing. Tracked as
[#394](https://github.com/achird-labs/rift-cluster/issues/394).

## The pattern across all three corrections

`OWNER` (#359), `Membership` (#366) and `Snapshots` (#365) were each specified against machinery
that was described **accurately**. Every API named exists; every constraint cited is real. A design
review that checks whether the facts are right approves all three.

The question that catches them is different, and it is worth asking of every panel in this mockup
before porting it:

> Not "can the console do this?" but "**should** it?"

A design document can be internally coherent, correct about every API it names, and still describe a
product that should not exist.

## What this prototype is not

- Not the component architecture. It is one file of vanilla JS; the real thing is React + TanStack
  Query with a client generated from `openapi-ee.yaml` (#184).
- Not a data contract. Field names here are indicative; the schema in #184 is authoritative.

## The token block *is* the design system now

This section used to say the palette was "a starting palette that has been contrast-validated, not
a finished system". That stopped being true: nothing else was ever specified, the console shipped
C4–C7 against an unrelated set of nine ad-hoc greys, and the two had visibly diverged — 27 tokens
here against 9 there, sharing three names and not one value.

So the token block was adopted wholesale into `web/src/styles.css`, along with the component
vocabulary that hangs off it (`card`, `tile`, `pill`, `banner`, `method`, `diag`, `key-once`,
`tabs`, `order-rank`, `clause`). **Change a token here and in `styles.css` together**, or this file
resumes being a prototype of something we do not ship.

Two things did not carry over, both deliberate:

- The `:root[data-theme]` overrides. They exist here because the prototype has a theme toggle; the
  console ships none, and a selector for a control that does not exist reads as a missing feature.
  Light is `:root`, dark comes from `prefers-color-scheme`, as it does here.
- The violet `--proto` pair and everything wearing it — the control strip and the notes footer.
  That is prototype scaffolding, and it says so above.

## Regenerating raster screenshots

None are committed: headless Chrome would not produce them in the environment this was authored in
(it launched the full browser stack and never wrote the file). If you want PNGs alongside this,
the intended command is:

```sh
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --headless=new --disable-gpu --user-data-dir="$(mktemp -d)" \
  --window-size=1500,1100 --force-device-scale-factor=2 --hide-scrollbars \
  --screenshot=shots/requests-degraded.png \
  "file://$PWD/docs/design/console/console-prototype.html?screen=requests&fleet=degraded&scopeNode=rift-3"
```

The interactive file is the better artefact regardless — a screenshot of a state explorer loses the
thing that makes it useful.
