import { Fragment } from "react";
import type { ReactNode } from "react";

import { ApiError } from "../api/client.ts";
import { FLEET_HEALTH_FIELDS, FLEET_MEMBER_FIELDS } from "../app/contract.ts";
import type { FleetView } from "../app/fleetView.ts";
import { useFleetView } from "../app/queries.ts";
import { Card, ErrorNote, Ident, Status, Tile, UNKNOWN } from "../components/primitives.tsx";
import { ControlPlane, HashRing } from "../components/fleetRail.tsx";
import { PendingPanel } from "../components/pending.tsx";

export function Fleet(): ReactNode {
  const fleet = useFleetView();

  if (fleet.isError) {
    /*
     * Both statuses mean the same thing here, and the console must say so rather than render one of
     * them as a broken page.
     *
     * `/_fleet/*` authorizes `Action::ClusterAdmin` with no tenant scope, so `decide` splits: a
     * principal bound to the requested tenant but lacking the role gets `InsufficientRole` → **403**,
     * while an unbound one gets the RFC-002 §8.4 → **404**. The likelier visitor — a tenant-admin
     * who bookmarked this screen — arrives on the 403 branch, so treating only 404 as "scope" would
     * hand exactly that person a generic error.
     */
    const status = fleet.error instanceof ApiError ? fleet.error.status : null;
    const scoped = status === 404 || status === 403;
    return (
      <section className="screen">
        <h1>Cluster &amp; fleet</h1>
        {scoped ? (
          <p className="error" role="alert">
            The fleet projection is fleet-scoped and is not available to this principal. A
            FleetAdmin binding is required to read <Ident>/_fleet/*</Ident>.
          </p>
        ) : (
          <ErrorNote error={fleet.error} context="Could not read this node's fleet view" />
        )}
      </section>
    );
  }

  return (
    <section className="screen">
      <header className="screen-head">
        <h1>Cluster &amp; fleet</h1>
        <p className="scope-label" data-testid="fleet-scope-label">
          {/* `/_fleet/*` is one node answering about itself. Presenting it as the fleet's own state
              would be the UI equivalent of a vacuous test — and there is no fleet-wide read to
              replace it with until the verification plane's merged journal lands (#147). */}
          This node&rsquo;s view of the fleet, read from this node only. Not a fleet-wide
          aggregate; another node may see a different membership.
        </p>
      </header>

      {fleet.isPending ? <p className="muted">Reading…</p> : null}
      {fleet.isSuccess ? <View view={fleet.data} /> : null}
    </section>
  );
}

function View({ view }: { view: FleetView }): ReactNode {
  return (
    <>
      {view.degraded.length > 0 ? (
        <div className="banner warn" data-testid="fleet-degraded" role="status">
          <span className="b-glyph" aria-hidden="true">
            ▲
          </span>
          <div>
            <strong>This node is degraded.</strong>
            <ul>
              {view.degraded.map((reason) => (
                <li key={reason}>{DEGRADATION_WORDING[reason]}</li>
              ))}
            </ul>
          </div>
        </div>
      ) : null}

      {/*
       * Tiles, not sparklines. `/_fleet/members` and `/_fleet/health` are point-in-time reads, so a
       * trend line would imply history the API does not have — RFC-006 §3 rule 2 applied to charts.
       *
       * Both grids are still driven by the contract field lists rather than hand-written cells, so
       * the traceability property holds exactly as before: a tile for a field the contract does not
       * publish fails `tsc`.
       */}
      <div className="tiles">
        {FLEET_MEMBER_FIELDS.map((field) => (
          <Tile
            key={field.key}
            label={field.label}
            testId={field.testId}
            plain={PLAIN_MEMBER_FIELDS.has(field.key)}
            value={<MemberValue view={view} field={field.key} />}
          />
        ))}
      </div>

      <Card title="Health">
        <dl className="detail">
          {FLEET_HEALTH_FIELDS.map((field) => (
            <div key={field.key} className="kv">
              <dt>{field.label}</dt>
              <dd data-testid={field.testId}>
                <HealthValue view={view} field={field.key} />
              </dd>
            </div>
          ))}
        </dl>
      </Card>

      {/*
       * Readiness gates as their own card, which is where the design puts them.
       *
       * `/readyz` publishes the gates that are still PENDING, not every gate and its state — so an
       * empty card is the good case and has to say so, rather than reading as a panel that failed
       * to load. The satisfied gates are not enumerable from here at all, which is why this counts
       * what is outstanding rather than listing a checklist.
       */}
      <Card title="Readiness gates">
        {view.pendingGates.length === 0 ? (
          <p className="muted" data-testid="fleet-gates-clear">
            No gate is holding readiness. <code>/readyz</code> reports only what is still pending, so
            this is the whole of what it has to say — the gates it has already satisfied are not
            enumerated.
          </p>
        ) : (
          <ul className="gate-list" data-testid="fleet-gates-pending">
            {view.pendingGates.map((gate) => (
              <li key={gate}>
                <span className="status status-warn">
                  <span className="g" aria-hidden="true">
                    &#9650;
                  </span>
                  pending
                </span>
                <Ident>{gate}</Ident>
              </li>
            ))}
          </ul>
        )}
      </Card>

      {/*
       * The ring and its members, side by side — the shape of the fleet next to the list of who is
       * in it. Both are read from `/_fleet/health` and `/_fleet/members`; neither is inferred.
       */}
      <div className="fleet-shape">
        <Card title="Ring">
          {/* The design heads this card with the fleet's name (#373). "Unnamed" rather than a
              blank: a fleet nobody has named yet is a real, honest state, not a loading gap —
              and the sharper place an operator actually needs this is the top bar (`Shell.tsx`),
              which is where staging and production, both open in two tabs, get told apart.

              "Unavailable" is deliberately a third word, not a second spelling of "Unnamed": the
              answering node reporting it could not read the name is a fault on that node, while
              an unnamed fleet is merely one nobody has got round to naming. Rendering both as
              "Unnamed" would send an operator to the wrong place. */}
          <div className="fleet-name">
            <span className="eyebrow">Fleet</span>
            <span className="ident" data-testid="fleet-name">
              {view.fleetNameUnavailable ? "Unavailable" : (view.fleetName ?? "Unnamed")}
            </span>
          </div>
          <HashRing fleet={view} />
        </Card>
        <Card title="Members">
          <ControlPlane fleet={view} />
        </Card>
      </div>

      {/*
       * The design drew three operational panels. One remains, and it is the only one of the three
       * that asked to *read* rather than to *act*.
       *
       * Membership (#366) and Snapshots (#365) are gone rather than pending. Neither was a missing
       * endpoint:
       *
       * - Membership changes happen only through a node's own lifecycle — a node is started and
       *   joins, or a node leaves. The console is deliberately not an admission or eviction vector.
       * - Snapshotting and log compaction are the cluster's own business. openraft's shipped
       *   defaults snapshot every 5000 entries and purge what a snapshot already covers, with no
       *   operator involvement; `a_shipped_fleet_snapshots_and_purges_without_being_asked` in
       *   `raft/node.rs` pins that. A button to force one would be an operator taking over a job
       *   the fleet already does.
       *
       * A pending panel is not neutral — it promises the capability arrives later. These two do
       * not, so they are removed instead.
       */}
      <div className="fleet-ops">
        <Card title="Durability &amp; write path">
          <PendingPanel issue={394} reason="The write barrier, its timeout, the flow fsync policy and the admin-write mode are configured on the node's command line and are not read back by any endpoint." />
        </Card>
      </div>

      {view.singleNode ? (
        <p className="hint" data-testid="fleet-single-note">
          A single-node fleet. One voter is this deployment&rsquo;s membership, not a shortfall.
        </p>
      ) : null}
    </>
  );
}

/**
 * Member fields that must not get the big-figure treatment.
 *
 * Everything here is an identifier, a set of them, or a pill. `last_applied` is the only member
 * field that is genuinely a magnitude, so it is the only one left rendered as one.
 */
const PLAIN_MEMBER_FIELDS = new Set<(typeof FLEET_MEMBER_FIELDS)[number]["key"]>([
  "voters",
  "is_leader",
  // Raft ids are 19 digits. At the tile's 25px figure size they overflow and are **silently
  // clipped** — `9597282464125895000` rendered as `959728246412`, with no ellipsis to say so, which
  // is a wrong value presented as a complete one rather than a cosmetic overflow. They are
  // identifiers, not magnitudes, so they get the identifier treatment.
  "node_id",
  "current_leader",
]);

const DEGRADATION_WORDING = {
  "not-ready": "Not ready: a load balancer should not route to it.",
  draining: "Draining: a graceful leave has begun and in-flight work is finishing.",
  isolated: "Isolated: it sees itself cut off from the rest of the fleet.",
  "no-leader": "No leader: it knows of no current raft leader.",
  evicted:
    "Evicted: it is not in the fleet's voter set. It owns no part of the ring and is no longer being replicated to, so anything it reports is whatever it held when it left.",
} as const satisfies Record<FleetView["degraded"][number], string>;

function MemberValue({
  view,
  field,
}: {
  view: FleetView;
  field: (typeof FLEET_MEMBER_FIELDS)[number]["key"];
}): ReactNode {
  switch (field) {
    case "node_id":
      return <Ident>{view.nodeId}</Ident>;
    case "current_leader":
      // `null` is "this node knows of no leader". Rendering it as 0 would name node 0 as leader.
      return <Ident>{view.leader ?? UNKNOWN}</Ident>;
    case "last_applied":
      return <Ident>{view.lastApplied ?? UNKNOWN}</Ident>;
    case "voters":
      return (
        <Ident>
          <IdList ids={view.voters} />
        </Ident>
      );
    case "is_leader":
      return view.isLeader ? (
        <Status tone="ok" label="this node is the leader" />
      ) : (
        <Status tone="idle" label="follower" />
      );
  }
}

/**
 * A comma-separated list of node ids that never breaks *inside* an id.
 *
 * `ids.join(", ")` produces a single text node, so the browser wraps it wherever it happens to fit
 * — and a raft node id is a 19-digit number, so on a narrow tile that lands mid-digit. The reader
 * then sees `334214098283493100` on one line and `0` on the next, which is not a hard-to-read id:
 * it is two plausible ids that do not exist. This is the one value on the screen where a line break
 * changes what it says.
 *
 * Each id therefore gets its own `nowrap` element and the separator stays outside it, so a wrap can
 * still happen between ids — which is what keeps a three-voter list from overflowing its tile.
 */
function IdList({ ids }: { ids: readonly (string | number)[] }): ReactNode {
  return (
    <>
      {ids.map((id, index) => (
        <Fragment key={String(id)}>
          {index === 0 ? null : ", "}
          <span className="nobreak">{id}</span>
        </Fragment>
      ))}
    </>
  );
}

function HealthValue({
  view,
  field,
}: {
  view: FleetView;
  field: (typeof FLEET_HEALTH_FIELDS)[number]["key"];
}): ReactNode {
  switch (field) {
    case "state":
      return <Status tone={view.ready ? "ok" : "warn"} label={view.state} />;
    case "ring":
      return (
        <Ident>
          epoch {view.ringEpoch} · <IdList ids={view.ringMembers} />
        </Ident>
      );
    case "pending_gates":
      return view.pendingGates.length === 0 ? (
        <span className="muted">none</span>
      ) : (
        <Ident>
          <IdList ids={view.pendingGates} />
        </Ident>
      );
    case "isolated":
      return view.isolated ? (
        <Status tone="bad" label="isolated" />
      ) : (
        <Status tone="ok" label="connected" />
      );
  }
}
