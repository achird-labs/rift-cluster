# Console design prototype — RFC-006 scope A

`console-prototype.html` is a **self-contained, zero-dependency** prototype of every RFC-006 §4 screen
whose backend has shipped or is sliced:

| Screen | Slice | Issue |
|---|---|---|
| Sign in — API key exchanged for a session cookie | C2 | [#185](https://github.com/achird-labs/rift-enterprise/issues/185) |
| App shell, tenant switcher, imposters, cluster/fleet | C4 | [#187](https://github.com/achird-labs/rift-enterprise/issues/187) |
| Stub editor — form ⟷ JSON, lint, 409 rebase | C5 | [#188](https://github.com/achird-labs/rift-enterprise/issues/188) |
| Request log (per-node) and front-door route editor | C6 | [#189](https://github.com/achird-labs/rift-enterprise/issues/189) |
| Tenants, principals, roles, audit | C7 | [#190](https://github.com/achird-labs/rift-enterprise/issues/190) |

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

The editor validates before the write, mirroring `RouteTable::validate` / `Route::validate`:

| Error | Condition |
|---|---|
| `StripWithoutPrefix` | `stripPrefix` set with no `pathPrefix` to strip |
| `MalformedHost` | more than one wildcard, or only a leading `*.` |
| `AmbiguousMatch` | two **enabled** routes that can both win at the same priority |

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

**The request log's scope selector never collapses.** Per-node is the fact that screen must keep in
front of the reader, so the node in scope and the count of nodes *not* represented sit permanently
above the table rather than in a dismissible banner. When the merged journal arrives (#147) the label
drops and the shape stays — that is the convergence RFC-006 §4 promises.

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
air-gapped, so the real console faces the same choice. Deliberately not Inter.

**Desktop only** (RFC-006 §10). The narrow-window collapse here is prototype convenience, not a
mobile layout.

## What this prototype is not

- Not the component architecture. It is one file of vanilla JS; the real thing is React + TanStack
  Query with a client generated from `openapi-ee.yaml` (#184).
- Not a data contract. Field names here are indicative; the schema in #184 is authoritative.
- Not styled with the eventual design system. The token block at the top of the file is a starting
  palette that has been contrast-validated, not a finished system.

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
