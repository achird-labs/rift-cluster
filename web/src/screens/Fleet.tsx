import { Fragment } from "react";
import type { ReactNode } from "react";

import { ApiError } from "../api/client.ts";
import { FLEET_HEALTH_FIELDS, FLEET_MEMBER_FIELDS } from "../app/contract.ts";
import type { FleetView } from "../app/fleetView.ts";
import { useFleetView } from "../app/queries.ts";
import { Card, ErrorNote, Ident, Status, Tile, UNKNOWN } from "../components/primitives.tsx";

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
