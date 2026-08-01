import type { ReactNode } from "react";

import type { components } from "../api/schema.ts";
import { IMPOSTER_COLUMNS } from "../app/contract.ts";
import type { FleetReadState, FleetView } from "../app/fleetView.ts";
import { viewConfidence } from "../app/fleetView.ts";
import { useFleetView, useImposters, useLifecycleToggle } from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { toHash } from "../app/routing.ts";
import { ImposterField } from "../components/imposterFields.tsx";
import {
  Card,
  Empty,
  ErrorNote,
  Truncated,
  UNKNOWN,
  UnconfirmedNote,
} from "../components/primitives.tsx";

type Imposter = components["schemas"]["Imposter"];

export function Imposters(): ReactNode {
  const { can } = useSession();
  const imposters = useImposters();
  // Only to qualify what the list shows. A principal without the fleet scope simply gets no
  // qualification — never a 404 error on a screen whose own read succeeded.
  const mayReadFleet = can("fleet.read");
  const fleet = useFleetView({ enabled: mayReadFleet });
  const toggle = useLifecycleToggle();

  const confidence = viewConfidence(fleetReadState(mayReadFleet, fleet));
  const mayToggle = can("imposter.lifecycle");

  return (
    <section className="screen">
      <header className="screen-head">
        <h1>Imposters</h1>
        <p className="scope-label" data-testid="imposters-scope-label">
          Served by this node from replicated state.
          {confidence.partial ? ` This node is degraded: ${confidence.reason}.` : ""}
          {confidence.unknown ? ` Caveat: ${confidence.reason}.` : ""}
        </p>
      </header>

      {imposters.isError ? <ErrorNote error={imposters.error} context="Could not list imposters" /> : null}
      {toggle.isError ? <ErrorNote error={toggle.error} context="That change did not take effect" /> : null}
      {toggle.data?.kind === "unobservable" ? <UnconfirmedNote reason={toggle.data.reason} /> : null}

      {imposters.isPending ? <p className="muted">Reading…</p> : null}

      {imposters.isSuccess && imposters.data.length === 0 ? (
        <EmptyState
          uncertain={confidence.partial || confidence.unknown}
          reason={confidence.reason}
        />
      ) : null}

      {imposters.isSuccess && imposters.data.length > 0 ? (
        <Card
          title={`${imposters.data.length} imposter${imposters.data.length === 1 ? "" : "s"}`}
          bleed
        >
          <div className="scroll-x">
            <table className="dense">
              <thead>
                <tr>
                  {IMPOSTER_COLUMNS.map((column) => (
                    <th key={column.key} className={column.numeric ? "numeric" : undefined}>
                      {column.label}
                    </th>
                  ))}
                  {mayToggle ? <th aria-label="Lifecycle" /> : null}
                </tr>
              </thead>
              <tbody>
                {imposters.data.map((imposter, index) => (
                  <Row
                    key={imposter.port ?? `unnamed-${index}`}
                    imposter={imposter}
                    mayToggle={mayToggle}
                    busy={toggle.isPending}
                    onToggle={(port, enable) => toggle.mutate({ port, enable })}
                  />
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      ) : null}
    </section>
  );
}

/**
 * Three states, kept distinct on purpose: read it, never asked, asked and failed.
 *
 * Folding "asked and failed" into "never asked" is the tempting simplification and the wrong one —
 * it would let a FleetAdmin whose health read just 500'd see the same unqualified list as a viewer
 * who was never entitled to the reading in the first place.
 */
function fleetReadState(
  mayRead: boolean,
  fleet: { data: FleetView | undefined; isError: boolean },
): FleetReadState {
  if (!mayRead) return { kind: "not-asked" };
  if (fleet.data !== undefined) return { kind: "read", view: fleet.data };
  return fleet.isError ? { kind: "unavailable" } : { kind: "not-asked" };
}

/**
 * The first thing every new operator sees — and the state a naive console gets wrong.
 *
 * "No imposters" asserts a fact about the tenant from one node's answer. When that node is degraded
 * an imposter it has not caught up on would not appear, so the honest sentence is that the list
 * cannot be confirmed, naming the coverage rather than implying a clean empty fleet.
 */
function EmptyState({
  uncertain,
  reason,
}: {
  uncertain: boolean;
  reason: string | null;
}): ReactNode {
  return (
    <Empty
      testId="imposters-empty"
      // The mark carries the distinction too: a settled empty reads as an empty set, an
      // unconfirmed one as the warning glyph the rest of the console uses for degraded.
      mark={uncertain ? "▲" : "○"}
      title={
        uncertain
          ? "Cannot confirm this tenant is empty"
          : "No imposters in this tenant, in this node’s view"
      }
      body={
        uncertain ? (
          <span className="warn-text">
            {reason}. An imposter this node has not applied would not appear here.
          </span>
        ) : (
          // The console reads imposters and edits their stubs; it does not create them (RFC-006's
          // slices scope C4 to read-only and C5 to the *stub* editor). Until a slice adds that,
          // this is the only place the console says how — so it says it, rather than leaving an
          // operator on an empty screen with no next step. The port is explicit because
          // `createImposter` requires it: an auto-assigned port cannot replicate across the fleet.
          <>
            The console does not create imposters yet. Create one against the admin API and it
            appears here.
          </>
        )
      }
    >
      {uncertain ? null : (
        <pre>{`curl -X POST $ADMIN/imposters \\
  -H 'Authorization: <your key>' \\
  -H 'Content-Type: application/json' \\
  -d '{"port":4545,"protocol":"http","stubs":[]}'`}</pre>
      )}
    </Empty>
  );
}

function Row({
  imposter,
  mayToggle,
  busy,
  onToggle,
}: {
  imposter: Imposter;
  mayToggle: boolean;
  busy: boolean;
  onToggle: (port: number, enable: boolean) => void;
}): ReactNode {
  const port = imposter.port;
  const label = imposter.name ?? (port === undefined ? UNKNOWN : String(port));

  return (
    <tr data-testid={`imposter-row-${port ?? "unnamed"}`}>
      {IMPOSTER_COLUMNS.map((column) => (
        <td key={column.key} className={column.numeric ? "numeric" : undefined}>
          <ImposterField imposter={imposter} field={column.key} renderName={nameLink(imposter)} />
        </td>
      ))}
      {mayToggle ? (
        <td>
          {/* Rendered only for a role that holds `LifecycleToggle`. RFC-006 §3 rule 3: this is
              presentation — the admin front re-checks the same action on the call itself. */}
          {port === undefined ? null : (
            <button
              className="btn sm"
              type="button"
              disabled={busy}
              onClick={() => onToggle(port, !imposter.enabled)}
            >
              {imposter.enabled ? "Disable" : "Enable"} {label}
            </button>
          )}
        </td>
      ) : null}
    </tr>
  );
}

/** The name cell is the one field the list renders differently: it links through to the detail. */
function nameLink(imposter: Imposter): (name: string) => ReactNode {
  return (name) => {
    const cell = (
      <Truncated value={name} testId={`imposter-name-${imposter.port ?? "unnamed"}`} />
    );
    return imposter.port === undefined ? (
      cell
    ) : (
      <a href={toHash({ screen: "imposter", port: imposter.port })}>{cell}</a>
    );
  };
}
