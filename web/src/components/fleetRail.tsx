import type { ReactNode } from "react";

import type { FleetView } from "../app/fleetView.ts";
import { Unshipped, UnshippedPanel } from "./unshipped.tsx";

/**
 * 2a's right rail: the hash ring, the control plane, and the merged tail.
 *
 * Complementary to the screen rather than part of it, so it is an `aside` with its own label — a
 * screen reader reaches the imposter table without walking a ring diagram to get there.
 */
export function FleetRail({ fleet }: { fleet: FleetView | undefined }): ReactNode {
  return (
    <aside className="rail-right" aria-label="Fleet">
      <HashRing fleet={fleet} />
      <ControlPlane fleet={fleet} />
      <LiveTail />
    </aside>
  );
}

/** Radius and stroke are the design's, so the ring reads at the size it was drawn at. */
const R = 112;
const CIRCUMFERENCE = 2 * Math.PI * R;

/**
 * The hash space, split between the ring's members.
 *
 * The segments are equal, and that is a statement about structure rather than a stand-in for one:
 * a ring of N members *is* N equal shares of the hash space, and `ringMembers` and `ringEpoch` are
 * both read from `/_fleet/health`. What the fleet does not publish is the other question the design
 * asks of this diagram — which *key* lands on which member — so the ownership legend carries the
 * marker while the ring itself stays real.
 *
 * Capped at three drawn segments because the design draws three and the palette defines three ring
 * steps; beyond that the member count in the centre is the honest summary, and a fourth near-
 * identical arc would be decoration rather than information.
 */
function HashRing({ fleet }: { fleet: FleetView | undefined }): ReactNode {
  const members = fleet?.ringMembers ?? [];
  const count = members.length;
  const drawn = Math.min(count, 3);
  const share = count === 0 ? 0 : CIRCUMFERENCE / count;

  return (
    <section className="rail-sect">
      <h2 className="eyebrow">Hash-space ownership</h2>
      <svg
        className="ring"
        viewBox="0 0 300 300"
        role="img"
        aria-label={
          count === 0
            ? "Ring membership is not available"
            : `Hash ring: ${String(count)} members at epoch ${String(fleet?.ringEpoch ?? 0)}`
        }
      >
        <circle className="ring-track" cx="150" cy="150" r={R} strokeWidth="26" />
        {Array.from({ length: drawn }, (_, i) => (
          <circle
            key={i}
            className={`ring-seg ring-seg-${String(i + 1)}`}
            cx="150"
            cy="150"
            r={R}
            strokeWidth="26"
            // One share drawn, the rest of the circumference left as gap; each segment rotated a
            // share further round. `rotate(-90)` starts the first at twelve o'clock.
            strokeDasharray={`${String(share)} ${String(CIRCUMFERENCE - share)}`}
            strokeDashoffset={String(-share * i)}
            transform="rotate(-90 150 150)"
          />
        ))}
        <text className="ring-label" x="150" y="146" textAnchor="middle">
          {count === 0 ? "—" : `${String(count)} ${count === 1 ? "member" : "members"}`}
        </text>
        <text className="ring-sub" x="150" y="166" textAnchor="middle">
          {fleet === undefined ? "ring unavailable" : `ring epoch ${String(fleet.ringEpoch)}`}
        </text>
      </svg>
      <div style={{ marginTop: "10px" }}>
        <Unshipped reason="Which key the ring assigns to which member is not published — /_fleet/health carries the members and the epoch, but no per-key ownership projection exists. The arcs show the ring's shape, not a measured distribution." />
      </div>
    </section>
  );
}

/**
 * One row per voter.
 *
 * The node ids and the leader are real, and so is `lastApplied` — but only for the node the console
 * is talking to. A peer's applied index is not merely unbuilt: `connect-src 'self'` means this page
 * can never dial another node, so it could only ever arrive through a fleet-wide aggregate endpoint
 * that does not exist. That distinction is worth keeping visible, which is why the marker sits per
 * row rather than over the whole panel.
 */
function ControlPlane({ fleet }: { fleet: FleetView | undefined }): ReactNode {
  if (fleet === undefined) {
    return (
      <section className="rail-sect">
        <h2 className="eyebrow">Control plane</h2>
        <UnshippedPanel reason="The fleet projection is scoped to fleet.read, and this principal is refused it." />
      </section>
    );
  }

  return (
    <section className="rail-sect">
      <h2 className="eyebrow">Control plane</h2>
      <div style={{ display: "flex", flexDirection: "column", gap: "9px" }}>
        {fleet.voters.map((id) => {
          const isSelf = id === fleet.nodeId;
          const isLeader = id === fleet.leader;
          return (
            <div className={`node-row${isSelf ? " is-self" : ""}`} key={id}>
              <span className={`dot ${isLeader ? "is-good" : "is-idle"}`} aria-hidden="true" />
              <span className="nid nobreak">{id}</span>
              {isLeader ? <span className="tag">leader</span> : null}
              {isSelf && fleet.lastApplied !== null ? (
                <span className="applied">{fleet.lastApplied}</span>
              ) : (
                <span className="applied">
                  <Unshipped
                    reason={
                      isSelf
                        ? "This node reported no applied index."
                        : "A peer's applied index cannot be read from here: the console is served under connect-src 'self', so it can only ever query its own node, and no fleet-wide aggregate endpoint publishes the others."
                    }
                  />
                </span>
              )}
            </div>
          );
        })}
      </div>
    </section>
  );
}

/**
 * The merged tail.
 *
 * Per-imposter recorded requests do exist (`/imposters/{port}/requests`, merged across writer shards
 * since #223), but a fleet-wide tail across every imposter — which is what the design draws, and
 * what the heading promises — has no endpoint. Assembling one client-side by fanning out across
 * every imposter would be a different thing wearing the same label: N reads on a 5s poll, ordered by
 * whatever came back first, presented as a single ordered stream.
 */
function LiveTail(): ReactNode {
  return (
    <section className="rail-sect" style={{ flex: 1, minHeight: 0 }}>
      <h2 className="eyebrow">
        Live tail · merged
        <span className="unshipped">not streaming</span>
      </h2>
      <UnshippedPanel reason="No fleet-wide request tail is published. Recorded requests are readable per imposter on the request log; a single merged stream across every imposter is not an endpoint the fleet offers." />
    </section>
  );
}
