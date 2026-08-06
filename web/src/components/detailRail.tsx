import type { ReactNode } from "react";

import type { FleetView } from "../app/fleetView.ts";
import { Ident } from "./primitives.tsx";
import { Pending, PendingPanel } from "./pending.tsx";

/**
 * The imposter detail's right rail: the write path, this port's place on the ring, and the
 * stub's recent hits.
 *
 * `aside`, like the fleet rail, and for the same reason: it annotates the imposter being edited
 * rather than being part of the editor, so a screen reader reaches the stub form without walking it.
 */
export function DetailRail({
  port,
  revision,
  fleet,
}: {
  port: number;
  revision: string | null;
  fleet: FleetView | undefined;
}): ReactNode {
  return (
    <aside className="rail-right" aria-label="This imposter on the fleet">
      <WritePath revision={revision} />
      <PortOnRing port={port} fleet={fleet} />
      <RecentHits />
    </aside>
  );
}

/**
 * What happened to the last write.
 *
 * The revision is real and is the one genuinely load-bearing fact here: `Rift-Cluster-Revision`
 * comes back on every admin write and is what tells an operator their edit was committed rather
 * than merely accepted. The *stages* the design draws — submitted, replicated to a quorum, applied — are
 * not published per write; the console learns only that the commit resolved.
 */
function WritePath({ revision }: { revision: string | null }): ReactNode {
  return (
    <section className="rail-sect">
      <h2 className="eyebrow">Write path</h2>
      <dl className="kv">
        <dt>Revision</dt>
        <dd>
          {revision === null ? (
            <Pending issue={361} reason="This read carried no Rift-Cluster-Revision header." />
          ) : (
            <Ident>{revision}</Ident>
          )}
        </dd>
        <dt>Stages</dt>
        <dd>
          <Pending issue={361} reason="Per-write progress — submitted, replicated to a quorum, applied — is not published. An admin write resolves to a commit outcome; the intermediate stages are not observable from the console." />
        </dd>
      </dl>
    </section>
  );
}

/**
 * Where this port sits on the ring.
 *
 * The ring's epoch and members are real. Which member owns *this port's* flow state is the HRW
 * question the fleet does not answer — the same gap the imposter list's OWNER column carries, and
 * the reason both say so rather than guessing at a hash.
 */
function PortOnRing({ port, fleet }: { port: number; fleet: FleetView | undefined }): ReactNode {
  return (
    <section className="rail-sect">
      <h2 className="eyebrow">This port on the ring</h2>
      <dl className="kv">
        <dt>Hash key</dt>
        <dd>
          <Ident>{port}</Ident>
        </dd>
        <dt>Ring epoch</dt>
        <dd>
          {fleet === undefined ? (
            <Pending issue={361} reason="The fleet projection is scoped to fleet.read, and this principal is refused it." />
          ) : (
            <Ident>{fleet.ringEpoch}</Ident>
          )}
        </dd>
        <dt>Flow owner</dt>
        <dd>
          <Pending issue={359} reason="No endpoint maps a key to its owning member. The ring's membership and epoch are published; the HRW assignment that decides which node holds this port's flow state is not." />
        </dd>
      </dl>
    </section>
  );
}

/**
 * Recent hits against the selected stub.
 *
 * `/imposters/{port}/requests` publishes the imposter's recorded requests, but nothing attributes a
 * request to the stub that matched it — so a per-stub hit list would have to be inferred by
 * re-running the match client-side, which is a guess about the server's own decision rather than a
 * reading of it. The request log is where the recorded traffic is honestly available.
 */
function RecentHits(): ReactNode {
  return (
    <section className="rail-sect" style={{ flex: 1, minHeight: 0 }}>
      <h2 className="eyebrow">Recent hits · this stub</h2>
      <PendingPanel issue={364} reason="Recorded requests are not attributed to the stub that matched them, so a per-stub hit list would be a client-side re-run of the server's own matching rather than a reading of it. The request log carries this imposter's traffic." />
    </section>
  );
}
