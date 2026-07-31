import type { ReactNode } from "react";

import { ApiError } from "../api/client.ts";
import { FLEET_HEALTH_FIELDS, FLEET_MEMBER_FIELDS } from "../app/contract.ts";
import type { FleetView } from "../app/fleetView.ts";
import { useFleetView } from "../app/queries.ts";
import { ErrorNote, Ident, Status, UNKNOWN } from "../components/primitives.tsx";

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
        <h1>Cluster</h1>
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
        <h1>Cluster</h1>
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
        <div className="degraded" data-testid="fleet-degraded" role="status">
          <strong>This node is degraded.</strong>
          <ul>
            {view.degraded.map((reason) => (
              <li key={reason}>{DEGRADATION_WORDING[reason]}</li>
            ))}
          </ul>
        </div>
      ) : null}

      <dl className="facts">
        {FLEET_MEMBER_FIELDS.map((field) => (
          <div key={field.key} className="fact">
            <dt>{field.label}</dt>
            <dd data-testid={field.testId}>
              <MemberValue view={view} field={field.key} />
            </dd>
          </div>
        ))}
        {FLEET_HEALTH_FIELDS.map((field) => (
          <div key={field.key} className="fact">
            <dt>{field.label}</dt>
            <dd data-testid={field.testId}>
              <HealthValue view={view} field={field.key} />
            </dd>
          </div>
        ))}
      </dl>

      {view.singleNode ? (
        <p className="muted" data-testid="fleet-single-note">
          A single-node fleet. One voter is this deployment&rsquo;s membership, not a shortfall.
        </p>
      ) : null}
    </>
  );
}

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
      return <Ident>{view.voters.join(", ")}</Ident>;
    case "is_leader":
      return view.isLeader ? (
        <Status tone="ok" label="this node is the leader" />
      ) : (
        <Status tone="idle" label="follower" />
      );
  }
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
          epoch {view.ringEpoch} · {view.ringMembers.join(", ")}
        </Ident>
      );
    case "pending_gates":
      return view.pendingGates.length === 0 ? (
        <span className="muted">none</span>
      ) : (
        <Ident>{view.pendingGates.join(", ")}</Ident>
      );
    case "isolated":
      return view.isolated ? (
        <Status tone="bad" label="isolated" />
      ) : (
        <Status tone="ok" label="connected" />
      );
  }
}
