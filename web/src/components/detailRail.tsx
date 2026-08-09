import type { ReactNode } from "react";

import { Ident } from "./primitives.tsx";
import { Pending, PendingPanel } from "./pending.tsx";

/**
 * The imposter detail's right rail: the write path and the stub's recent hits.
 *
 * `aside`, like the fleet rail, and for the same reason: it annotates the imposter being edited
 * rather than being part of the editor, so a screen reader reaches the stub form without walking it.
 *
 * It took a `port` and a `fleet` until the ring panel below was removed; both existed only to
 * answer "who owns this port", which is not a question with an answer.
 */
export function DetailRail({ revision }: { revision: string | null }): ReactNode {
  return (
    <aside className="rail-right" aria-label="This imposter on the fleet">
      <WritePath revision={revision} />
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

/*
 * The design draws a "This port on the ring" panel here — hash key, ring epoch, flow owner. It is
 * gone rather than pending, because the panel's own title is the mistake: **a port does not sit on
 * the ring.**
 *
 * Imposters, stubs and config are replicated to every node through Raft, so every node serves them
 * and none owns them. The ring assigns owners to *flows* (`KeyClass::FlowKv`), keyed by an opaque
 * caller-supplied flow id — so a port has as many owners as it has flows, and two of the panel's
 * three rows asserted things that are not true of a port: its "hash key" was the port number (the
 * real key is the flow id under its `ContextScope` prefix), and its "flow owner" was a single
 * owner for a port that has none.
 *
 * The ring's epoch, the one real fact here, is on the fleet screen where it describes the fleet
 * rather than this imposter. Per-flow ownership belongs on the flow-state surface, where the flows
 * are actually enumerated — see #359.
 */

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
