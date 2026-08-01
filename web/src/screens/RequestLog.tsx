import { type ReactNode, useState } from "react";

import { ApiError } from "../api/client.ts";
import type { FleetReadState } from "../app/fleetView.ts";
import { useFleetView, useImposters, useRequestLog } from "../app/queries.ts";
import { Empty, Ident, Truncated } from "../components/primitives.tsx";
import type { MatchOutcome, OutcomeView } from "../features/requests/diagnostics.ts";
import { describeOutcome } from "../features/requests/diagnostics.ts";
import type { RecordedRequest } from "../features/requests/source.ts";
import { coverageFor, describeCoverage, headerValues, page } from "../features/requests/source.ts";

const PAGE_SIZE = 50;

export function RequestLog({ port }: { port: number | null }): ReactNode {
  if (port === null) return <ImposterPicker />;
  // Keyed by port so the pager offset does not survive a switch to another imposter: React would
  // otherwise reuse `Log` at the same tree position, and an imposter with traffic would render as
  // an empty table paged past its end — the same lie the unknown/empty split exists to prevent.
  return <Log key={port} port={port} />;
}

/**
 * The fleet reading, as the three-way fact it is.
 *
 * A failed `/_fleet/*` read is not the same as not having asked, and neither is the same as a
 * reading — `coverageFor` needs all three to avoid claiming coverage it cannot support.
 */
function useFleetReadState(): FleetReadState {
  const fleet = useFleetView({ polled: false });
  if (fleet.isSuccess) return { kind: "read", view: fleet.data };
  if (fleet.isError) return { kind: "unavailable" };
  return { kind: "not-asked" };
}

function Log({ port }: { port: number }): ReactNode {
  const [offset, setOffset] = useState(0);
  const log = useRequestLog(port);
  const coverage = coverageFor(useFleetReadState());

  return (
    <section className="screen">
      <header className="screen-head">
        <h1>Request log</h1>
        <p className="muted">
          Imposter <Ident>{port}</Ident>
        </p>
      </header>

      {/*
       * A permanent scope strip, not a dismissible banner — and deliberately the most prominent
       * thing on the screen after the title. Per-node is the fact this screen must keep in front of
       * the reader: an operator uses it to decide whether their system under test called the mock,
       * and one node's log answers that only for one node. When the merged journal lands (#147) the
       * label drops and this shape stays.
       */}
      <div className="scope" data-testid="request-scope-label" role="status">
        <span className="eyebrow">Scope</span>
        <span className="pill accent">
          <span className="g" aria-hidden="true">
            ◈
          </span>
          this node only
        </span>
        <span className="coverage">{describeCoverage(coverage)}</span>
      </div>

      {log.isPending ? <p className="muted">Reading…</p> : null}
      {log.isSuccess ? (
        <Rows state={log.data} offset={offset} onOffset={setOffset} />
      ) : null}
    </section>
  );
}

function Rows({
  state,
  offset,
  onOffset,
}: {
  state: { kind: "rows"; rows: RecordedRequest[] } | { kind: "unknown"; reason: string };
  offset: number;
  onOffset: (next: number) => void;
}): ReactNode {
  if (state.kind === "unknown") {
    /*
     * The distinction the issue calls the most important on this screen. An empty table here would
     * tell an operator their system under test never called the mock, when in fact this node simply
     * could not answer.
     */
    return (
      <div className="banner crit" data-testid="request-log-unknown" role="alert">
        <span className="b-glyph" aria-hidden="true">
          ■
        </span>
        <div>
          <strong>This node&rsquo;s log is unknown, not empty.</strong>
          <p>
            It could not be read, so nothing here says whether requests arrived. {state.reason}
          </p>
        </div>
      </div>
    );
  }

  if (state.rows.length === 0) {
    // The reachable-and-genuinely-empty case, which is a different screen from the one above on
    // purpose: this one is an answer, that one is the absence of an answer.
    return (
      <Empty
        testId="request-log-empty"
        title="No requests recorded for this imposter"
        body="This node answered. Nothing has called the imposter since its log was last cleared."
      />
    );
  }

  /*
   * Clamp before paging. The journal shrinks under the 2s poll — retention truncates it, and
   * `DELETE /imposters/:port/requests` empties it outright — so an offset that was valid a tick ago
   * can point past the end, which would render an empty table for an imposter that has traffic.
   */
  const clamped = Math.min(offset, Math.max(0, state.rows.length - 1));
  const start = Math.floor(clamped / PAGE_SIZE) * PAGE_SIZE;
  const view = page(state.rows, { offset: start, size: PAGE_SIZE });
  const first = start + 1;
  const last = start + view.rows.length;

  return (
    <>
      <section className="card">
        <div className="scroll-x">
          <table className="dense">
            <thead>
              <tr>
                <th style={{ width: "20ch" }}>Time</th>
                <th style={{ width: "10ch" }}>Method</th>
                <th>Path</th>
                <th style={{ width: "18ch" }}>From</th>
              </tr>
            </thead>
            <tbody>
              {/*
                Keyed on the *clamped* start, not the raw offset. Keying on `offset` would leave the
                keys unchanged when a shrinking journal clamps `start`, so React would reuse the
                components and an expanded row would re-render already-open against a different
                request.
              */}
              {view.rows.map((request, index) => (
                <Row key={`${start + index}`} request={request} />
              ))}
            </tbody>
          </table>
        </div>
        <nav className="pager">
          <button
            className="btn sm"
            type="button"
            disabled={start === 0}
            onClick={() => onOffset(Math.max(0, start - PAGE_SIZE))}
          >
            Previous
          </button>
          <button
            className="btn sm"
            type="button"
            disabled={!view.hasMore}
            onClick={() => onOffset(start + PAGE_SIZE)}
          >
            Next
          </button>
          <span className="count" data-testid="request-total">
            {first}–{last} of {view.total} on this node
          </span>
        </nav>
      </section>
    </>
  );
}

/**
 * The HTTP verb as a badge.
 *
 * The class is derived from a **closed set**, never interpolated from the value: `method` is
 * recorded from whatever called the mock, so `method-${request.method}` would put attacker-chosen
 * text into a class attribute. An unrecognised verb simply gets the neutral badge and still renders
 * its own text.
 */
const METHOD_TONES = new Set(["get", "post", "put", "patch", "delete", "head", "options"]);

function Method({ method }: { method: string | undefined }): ReactNode {
  if (method === undefined) return <span className="method">—</span>;
  const tone = method.toLowerCase();
  return (
    <span className={METHOD_TONES.has(tone) ? `method ${tone}` : "method"}>
      <Truncated value={method} max={7} />
    </span>
  );
}

/**
 * Every cell is text.
 *
 * Whatever called the mock chose this path, these headers and this body, so this is the most
 * attacker-influenced surface in the console (RFC-006 §9.1). React escapes by default and the raw
 * innerHTML escape hatch is banned by lint — this component exists so that stays true in one place
 * rather than at every call site.
 */
function Row({ request }: { request: RecordedRequest }): ReactNode {
  const [open, setOpen] = useState(false);
  return (
    <>
      <tr data-testid="request-row" className="clickable" aria-selected={open}>
        <td className="ident">{request.timestamp ?? "—"}</td>
        <td>
          <Method method={request.method} />
        </td>
        <td>
          <button
            type="button"
            className="linklike"
            data-testid="request-open"
            onClick={() => setOpen(!open)}
          >
            <span className="path">{request.path ?? "—"}</span>
          </button>
        </td>
        <td className="ident">
          <Truncated value={request.requestFrom ?? "—"} max={16} />
        </td>
      </tr>
      {open ? (
        <tr data-testid="request-detail">
          <td colSpan={4}>
            <dl className="detail">
              <div className="kv">
                <dt>Path</dt>
                <dd>
                  <pre className="payload">{request.path ?? "—"}</pre>
                </dd>
              </div>
              <div className="kv">
                <dt>Headers</dt>
                <dd>
                  <pre className="payload">{formatHeaders(request.headers)}</pre>
                </dd>
              </div>
              <div className="kv">
                <dt>Query</dt>
                <dd>
                  <pre className="payload">{formatQuery(request.query)}</pre>
                </dd>
              </div>
              <div className="kv">
                {/* A base64 body rendered unlabelled reads as corrupted text rather than as the
                    binary payload it is. The mode token is `binary` and the encoding it implies is
                    base64 — `ResponseMode` serializes lowercase, and a text body omits the field
                    entirely, so `binary` is the only value that ever arrives. */}
                <dt>{request._mode === "binary" ? "Body (base64)" : "Body"}</dt>
                <dd>
                  <pre className="payload">{request.body ?? "(no body)"}</pre>
                </dd>
              </div>
            </dl>
            <Diagnostics outcome={request.matchOutcome} />
          </td>
        </tr>
      ) : null}
    </>
  );
}

/**
 * Why this request was served by the stub it was — or by nothing (#208).
 *
 * The reason an operator opens this screen. Every sentence comes from `describeOutcome`, which is
 * also where the two distinctions that make the panel honest are enforced: an absent outcome is
 * *not* a failed match, and an unreadable one is *not* an absent one.
 */
function Diagnostics({ outcome }: { outcome: MatchOutcome | undefined }): ReactNode {
  const view = describeOutcome(outcome);
  /*
   * Four verdicts, three tones — and `none`/`unreadable` deliberately do NOT get the "unmatched"
   * amber. Amber here would read as "nothing matched", which is a verdict; those two states are
   * the *absence* of a verdict, and colouring them like one is exactly the laundering this panel
   * exists to prevent.
   */
  const tone =
    view.kind === "matched" ? "matched" : view.kind === "unmatched" ? "unmatched" : "no-verdict";
  return (
    <section className={`diag ${tone}`} data-testid="request-diagnostics">
      <div className="dh">
        <span aria-hidden="true">{DIAG_GLYPH[view.kind]}</span>
        Match
      </div>
      <Verdict view={view} />
      {view.kind === "matched" || view.kind === "unmatched" ? <TriedList view={view} /> : null}
    </section>
  );
}

const DIAG_GLYPH: Record<OutcomeView["kind"], string> = {
  matched: "●",
  unmatched: "▲",
  none: "○",
  unreadable: "○",
};

function Verdict({ view }: { view: OutcomeView }): ReactNode {
  switch (view.kind) {
    case "none":
      /*
       * Deliberately never the words "did not match". The schema says absence means *no outcome
       * was recorded*, and the three causes named here are all of them — so the sentence states
       * the absence and the hint states what can cause it, rather than inventing a verdict for an
       * entry nothing judged.
       */
      return (
        <p className="muted" data-testid="request-diagnostics-none">
          No match diagnostics recorded for this request. Nothing here says which stubs were
          considered: an entry recorded by an engine older than this field, a request that took the{" "}
          <code>X-Rift-Debug</code> path, and a matcher error all arrive without an outcome.
        </p>
      );
    case "unreadable":
      // A different sentence from "none" on purpose: this one says the node answered with
      // something wrong, which is a fault to chase rather than a routine gap.
      return (
        <p className="warn-text" data-testid="request-diagnostics-unreadable" role="status">
          Match diagnostics unreadable — this node recorded an outcome in a shape this console does
          not recognise. Nothing here says whether the request matched.
        </p>
      );
    case "matched":
      return <p data-testid="request-diagnostics-verdict">Matched: served by {view.label}.</p>;
    case "unmatched":
      return (
        <p data-testid="request-diagnostics-verdict">
          Nothing matched this request.
          {view.tried.length === 0
            ? " No candidate was visited — every stub was ruled out before it was evaluated."
            : null}
        </p>
      );
  }
}

/**
 * The candidates the matcher actually visited, in visit order.
 *
 * On a miss these are the stubs that were tried and why each fell out. On a hit they are the ones
 * passed over *before* the winner — which is how an operator sees that the stub they expected to
 * serve the request was visited and rejected, rather than never reached.
 */
function TriedList({
  view,
}: {
  view: Extract<OutcomeView, { kind: "matched" | "unmatched" }>;
}): ReactNode {
  if (view.tried.length === 0 && view.omitted === 0) return null;
  return (
    <>
      {view.tried.length === 0 ? null : (
        <>
          <p className="muted">
            {view.kind === "matched" ? "Passed over before the winner:" : "Candidates tried:"}
          </p>
          <ul className="plain" data-testid="request-tried">
            {view.tried.map((tried, index) => (
              // Keyed by position: this list is a visit order, and two candidates can carry the
              // same label when neither stub declares an id.
              <li key={`${index}`}>
                {tried.label} — {tried.why}
              </li>
            ))}
          </ul>
        </>
      )}
      {view.omitted === 0 ? null : (
        // The engine caps `tried` and counts the rest. Dropping the count would make "these are
        // the stubs that were tried" false with nothing on screen to say so.
        <p className="muted" data-testid="request-tried-omitted">
          {view.omitted} more {view.omitted === 1 ? "candidate was" : "candidates were"} visited and
          are not shown.
        </p>
      )}
    </>
  );
}

function formatHeaders(headers: Record<string, unknown> | undefined): string {
  const entries = Object.entries(headers ?? {});
  if (entries.length === 0) return "(none)";
  return entries.map(([name, value]) => `${name}: ${headerValues(value).join(", ")}`).join("\n");
}

function formatQuery(query: Record<string, string> | undefined): string {
  const entries = Object.entries(query ?? {});
  if (entries.length === 0) return "(none)";
  return entries.map(([name, value]) => `${name}=${value}`).join("\n");
}

/** The log is per-imposter, so with no port in the hash the screen asks which one. */
function ImposterPicker(): ReactNode {
  const imposters = useImposters();
  return (
    <section className="screen">
      <header className="screen-head">
        <h1>Request log</h1>
        <p className="muted">Choose an imposter to read this node&rsquo;s recorded requests for.</p>
      </header>
      {imposters.isError ? (
        <p className="error" role="alert">
          {imposters.error instanceof ApiError
            ? imposters.error.message
            : "Could not read the imposter list."}
        </p>
      ) : null}
      <ul className="plain">
        {(imposters.data ?? []).map((imposter) => (
          <li key={imposter.port}>
            <a href={`#/requests/${imposter.port}`}>
              <Ident>{imposter.port}</Ident> {imposter.name ?? ""}
            </a>
          </li>
        ))}
      </ul>
    </section>
  );
}
