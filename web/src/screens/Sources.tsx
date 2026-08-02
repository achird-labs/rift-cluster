import type { ReactNode } from "react";

import { ApiError } from "../api/client.ts";
import type { components } from "../api/schema.ts";
import { SOURCE_COLUMNS } from "../app/contract.ts";
import { useSources } from "../app/queries.ts";
import { assertNever } from "../components/imposterFields.tsx";
import { Card, Empty, ErrorNote, Ident, Status, UNKNOWN } from "../components/primitives.tsx";

type SourceRecord = components["schemas"]["SourceRecord"];

export function Sources(): ReactNode {
  const sources = useSources();

  if (sources.isError) {
    /*
     * Same discipline as `Fleet.tsx`: a `403` and a `404` both mean "this principal is not entitled
     * to read this", and only `source.read` can turn either into a working screen. Rendering either
     * as a broken page rather than a scope refusal would send a viewer who lacks the capability
     * looking for a bug that is not there.
     */
    const status = sources.error instanceof ApiError ? sources.error.status : null;
    const scoped = status === 404 || status === 403;
    return (
      <section className="screen">
        <h1>Sources</h1>
        {scoped ? (
          <p className="error" role="alert">
            Reading <Ident>/admin/sources</Ident> requires the <Ident>source.read</Ident>{" "}
            capability and is not available to this principal.
          </p>
        ) : (
          <ErrorNote error={sources.error} context="Could not read this tenant's sources" />
        )}
      </section>
    );
  }

  return (
    <section className="screen">
      <header className="screen-head">
        <h1>Sources</h1>
        <p className="scope-label">
          Imposter sources, their declared drift policy, and the ports they own.
        </p>
      </header>

      {/*
       * The permanent scope strip the request log and fleet screens already hold to: `nodeLocal` is
       * one node's reach to each source's upstream host at one moment, never replicated, so it must
       * name the node it came from rather than read as a claim the whole fleet would agree on.
       */}
      {sources.isSuccess ? (
        <div className="scope" data-testid="sources-node-scope" role="status">
          <span className="eyebrow">Scope</span>
          <span className="pill accent">
            <span className="g" aria-hidden="true">
              ◈
            </span>
            this node only
          </span>
          <span className="coverage">
            The <strong>Poll</strong> column is this node&rsquo;s own reach to each source&rsquo;s
            upstream host, answered by node {sources.data.nodeLocal.nodeId}. Polls run on the{" "}
            <strong>leader</strong>, so an empty poll column is not evidence that polling is
            healthy — on a follower it is empty whether or not anything is failing. Every other
            column, drift included, is the replicated record that every converged node agrees on.
          </span>
        </div>
      ) : null}

      {sources.isPending ? <p className="muted">Reading…</p> : null}

      {sources.isSuccess && sources.data.sources.length === 0 ? (
        <Empty
          testId="sources-empty"
          title="No sources declared for this tenant"
          body="Declare one against the admin API and it appears here."
        />
      ) : null}

      {sources.isSuccess && sources.data.sources.length > 0 ? (
        <Card
          title={`${sources.data.sources.length} source${sources.data.sources.length === 1 ? "" : "s"}`}
          bleed
        >
          <div className="scroll-x">
            <table className="dense">
              <thead>
                <tr>
                  {SOURCE_COLUMNS.map((column) => (
                    <th key={column.key} className={column.numeric ? "numeric" : undefined}>
                      {column.label}
                    </th>
                  ))}
                  <th>Drift</th>
                  <th>Poll (this node)</th>
                </tr>
              </thead>
              <tbody>
                {sources.data.sources.map((source) => (
                  <Row key={source.id} source={source} pollErrors={sources.data.nodeLocal.pollErrors} />
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      ) : null}
    </section>
  );
}

function Row({
  source,
  pollErrors,
}: {
  source: SourceRecord;
  pollErrors: Record<string, string>;
}): ReactNode {
  return (
    <tr data-testid={`source-row-${source.id}`}>
      {SOURCE_COLUMNS.map((column) => (
        <td
          key={column.key}
          className={column.numeric ? "numeric" : undefined}
          data-testid={column.key === "lastPulledAtSecs" ? `source-pulled-${source.id}` : undefined}
        >
          <SourceField source={source} field={column.key} />
        </td>
      ))}
      <td data-testid={`source-drift-${source.id}`}>
        <DriftCell source={source} />
      </td>
      <td data-testid={`source-poll-${source.id}`}>
        <PollCell error={pollErrors[source.id]} />
      </td>
    </tr>
  );
}

function SourceField({
  source,
  field,
}: {
  source: SourceRecord;
  // Narrowed to the columns this table actually declares, not `SourceColumn["key"]`'s full
  // `keyof Declared<SourceRecord>` — `drifted`, `lastOutcome` and the rest are read by
  // `driftState` below, never through this per-field switch, so `assertNever` should refuse a
  // column this table does not render rather than demand a case for every schema field.
  field: (typeof SOURCE_COLUMNS)[number]["key"];
}): ReactNode {
  switch (field) {
    case "id":
      return <Ident>{source.id}</Ident>;
    case "uri":
      // Not `Truncated`: an operator pastes this into curl, and the table's own `scroll-x` wrapper
      // is what carries a long one rather than a clipped label with no way to see the rest.
      return <Ident>{source.uri}</Ident>;
    case "mode":
      return source.mode;
    case "ports":
      return source.ports.length === 0 ? (
        <span className="muted">{UNKNOWN}</span>
      ) : (
        <Ident>{source.ports.join(", ")}</Ident>
      );
    case "pollSecs":
      // Absent for a `pinned` source (the contract says so) — rendered as unknown rather than 0,
      // which would read as "polls every 0 seconds" instead of "does not poll at all".
      return source.mode === "tracking" && source.pollSecs !== undefined ? (
        <Ident>{source.pollSecs}s</Ident>
      ) : (
        <span className="muted">{UNKNOWN}</span>
      );
    case "onDrift":
      return source.onDrift;
    case "lastVersion":
      // Absent when the source names no version, and also when it has never pulled. Either way the
      // honest cell is "unknown" — a blank one is indistinguishable from a version that is itself
      // an empty string.
      return source.lastVersion === undefined ? (
        <span className="muted">{UNKNOWN}</span>
      ) : (
        <Ident>{source.lastVersion}</Ident>
      );
    case "lastPulledAtSecs":
      // Never rendered through `Date` arithmetic on a missing value: `new Date(undefined * 1000)`
      // is `Invalid Date` and `new Date(0)` is 1970, and both would present "has never pulled" as
      // a timestamp that looks real.
      return source.lastPulledAtSecs === undefined ? (
        <span className="muted">{UNKNOWN} never pulled</span>
      ) : (
        <Ident>{new Date(source.lastPulledAtSecs * 1000).toISOString()}</Ident>
      );
    case "revision":
      // The log index that last wrote the record — always present, and the thing an operator
      // compares across nodes when asking whether a change has converged.
      return <Ident>{source.revision}</Ident>;
    default:
      return assertNever(field);
  }
}

/**
 * Drift is a **replicated** verdict, so it is answered confidently — and answered from the record
 * alone, never from this node's poll results.
 *
 * `store.rs` flips `drifted` in the imposter write/delete apply path, at the same log index on
 * every replica ("`drifted` is replicated state, not a node's opinion"). So `drifted: false` is a
 * current fleet fact, not the residue of the last pull.
 *
 * Folding a poll failure in here — the intuitive reading of "a source that could not be re-read is
 * not a source that has not drifted" — would be wrong twice. It manufactures doubt about something
 * the fleet knows; and because polls are **leader-only**, `pollErrors` is empty by construction on
 * a follower, so the verdict would flip between "unknown" and "clean" with nothing but the node
 * that answered. A per-node answer to a fleet question is the leak this screen exists to prevent.
 *
 * The genuine unknown — whether the *upstream document* has moved since the last pull — is real,
 * but it is a different fact from drift, and it is node-local. It gets its own cell.
 *
 * Three states, because "never pulled" is a state of the replicated record itself: a source that
 * has applied nothing has no drift answer yet, which is not the same as answering clean.
 */
type DriftState = "clean" | "drifted" | "never-pulled";

function driftState(source: SourceRecord): DriftState {
  if (source.drifted) return "drifted";
  if (source.lastOutcome === undefined) return "never-pulled";
  return "clean";
}

function DriftCell({ source }: { source: SourceRecord }): ReactNode {
  switch (driftState(source)) {
    case "clean":
      return <Status tone="ok" label="clean — no hand edits since the last pull" />;
    case "drifted":
      // `onDrift` is its own column rather than repeated here: "drifted" without it is a colour,
      // not an answer to "will this be repaired" — see `SourceField`'s `onDrift` case.
      return <Status tone="bad" label="drifted" />;
    case "never-pulled":
      return <Status tone="idle" label="unknown — never pulled" />;
  }
}

/**
 * This node's last poll of the source — the node-local half, kept in its own column so it cannot be
 * mistaken for the replicated verdict beside it.
 *
 * A source with no entry renders **nothing**, deliberately: absence is not evidence of health.
 * Polls run only on the leader, so a follower's map is empty whether or not anything is failing,
 * and a green "polling OK" here would be a fact invented from an absence. The scope strip carries
 * that caveat once, rather than every row carrying a hedge.
 */
function PollCell({ error }: { error: string | undefined }): ReactNode {
  if (error === undefined) return <span className="muted">{UNKNOWN}</span>;
  return (
    <>
      <Status tone="warn" label="last poll failed" />
      <div className="note">{error}</div>
    </>
  );
}
