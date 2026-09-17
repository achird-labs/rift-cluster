import { Fragment } from "react";
import type { ReactNode } from "react";

import { ApiError } from "../api/client.ts";
import { FLEET_HEALTH_FIELDS, FLEET_MEMBER_FIELDS } from "../app/contract.ts";
import { WRITE_PATH_FIELDS, writePathByVoter, writePathDisagreements } from "../app/fleetView.ts";
import type { FleetView, WritePath } from "../app/fleetView.ts";
import { useFleetView } from "../app/queries.ts";
import { Card, ErrorNote, Ident, Status, Tile, UNKNOWN } from "../components/primitives.tsx";
import { ControlPlane, HashRing } from "../components/fleetRail.tsx";

export function Fleet(): ReactNode {
  const fleet = useFleetView();

  if (fleet.isError) {
    /*
     * Both statuses mean the same thing here, and the console must say so rather than render one of
     * them as a broken page.
     *
     * A node that is not clustered serves no `/_fleet/*` projection at all — there is nothing for it
     * to answer about — so a bookmark to this screen lands on a `404` (or a `403` from a front that
     * refuses the route outright). Neither is a fault in this session, and a generic error note
     * would send the reader looking for one.
     */
    const status = fleet.error instanceof ApiError ? fleet.error.status : null;
    const unavailable = status === 404 || status === 403;
    return (
      <section className="screen">
        <h1>Cluster &amp; fleet</h1>
        {unavailable ? (
          <p className="error" role="alert">
            This node serves no fleet projection. <Ident>/_fleet/*</Ident> exists only on a node
            started with <Ident>--cluster</Ident>.
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
              would be the UI equivalent of a vacuous test, and there is no fleet-wide read to
              replace it with. */}
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
       * Membership (#366, D-21) and Snapshots (#365, D-24) are gone rather than pending. Neither was a
       * missing endpoint:
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
          <WritePathTable view={view} />
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
 * Each voter's write-path flags, read back (#394, D-82). Read-only on purpose: these are node
 * startup flags, and the console has no business setting them.
 */
function WritePathTable({ view }: { view: FleetView }): ReactNode {
  const rows = writePathByVoter(view);
  const disagreements = writePathDisagreements(rows);
  return (
    <>
      <table className="dense" data-testid="write-path">
        <thead>
          <tr>
            <th>Node</th>
            {WRITE_PATH_FIELDS.map((field) => (
              <th key={field.key}>{field.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(({ id, writePath }) => (
            <tr key={id} data-testid={`write-path-${id}`}>
              <td className="nid nobreak">{id}</td>
              {writePath === null ? (
                <td
                  colSpan={WRITE_PATH_FIELDS.length}
                  className="muted"
                  title="This node did not answer, or runs a build that does not report its write path."
                >
                  unknown
                </td>
              ) : (
                WRITE_PATH_FIELDS.map((field) => (
                  <td
                    key={field.key}
                    className={disagreements.includes(field.key) ? "mono is-warn" : "mono"}
                  >
                    {formatWritePath(field.key, writePath)}
                  </td>
                ))
              )}
            </tr>
          ))}
        </tbody>
      </table>
      {disagreements.length > 0 ? (
        <p className="hint" data-testid="write-path-disagreement">
          The nodes were started with different{" "}
          {disagreements
            .map((key) => WRITE_PATH_FIELDS.find((field) => field.key === key)?.label.toLowerCase())
            .join(", ")}{" "}
          settings. A write&rsquo;s guarantees depend on which node answers it.
        </p>
      ) : null}
    </>
  );
}

function formatWritePath(key: (typeof WRITE_PATH_FIELDS)[number]["key"], settings: WritePath): string {
  switch (key) {
    case "write_barrier":
      return settings.write_barrier;
    case "write_barrier_timeout_seconds":
      return `${settings.write_barrier_timeout_seconds} s`;
    case "admin_async":
      return settings.admin_async ? "async (202 + op id)" : "sync";
    case "flow_fsync_interval_ms":
      return `${settings.flow_fsync_interval_ms} ms`;
  }
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
