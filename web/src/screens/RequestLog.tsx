import { type ReactNode, useState } from "react";

import { ApiError } from "../api/client.ts";
import type { FleetReadState } from "../app/fleetView.ts";
import { useFleetView, useImposters, useRequestLog } from "../app/queries.ts";
import { Ident, Truncated } from "../components/primitives.tsx";
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
        {/*
         * A permanent scope line, not a dismissible banner. Per-node is the fact this screen must
         * keep in front of the reader: an operator uses it to decide whether their system under
         * test called the mock, and one node's log answers that only for one node.
         */}
        <p className="scope-label" data-testid="request-scope-label" role="status">
          {describeCoverage(coverage)}
        </p>
        <p className="muted">
          Imposter <Ident>{port}</Ident>
        </p>
      </header>

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
      <p className="error" data-testid="request-log-unknown" role="alert">
        <strong>This node&rsquo;s log is unknown, not empty.</strong> It could not be read, so
        nothing here says whether requests arrived. {state.reason}
      </p>
    );
  }

  if (state.rows.length === 0) {
    return (
      <p className="muted" data-testid="request-log-empty">
        This node answered and has recorded no requests for this imposter.
      </p>
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
      <p className="muted" data-testid="request-total">
        {first}–{last} of {view.total} on this node
      </p>
      <table className="dense">
        <thead>
          <tr>
            <th>Time</th>
            <th>Method</th>
            <th>Path</th>
            <th>From</th>
          </tr>
        </thead>
        <tbody>
          {/*
            Keyed on the *clamped* start, not the raw offset. Keying on `offset` would leave the
            keys unchanged when a shrinking journal clamps `start`, so React would reuse the
            components and an expanded row would re-render already-open against a different request.
          */}
          {view.rows.map((request, index) => (
            <Row key={`${start + index}`} request={request} />
          ))}
        </tbody>
      </table>
      <nav className="pager">
        <button
          type="button"
          disabled={start === 0}
          onClick={() => onOffset(Math.max(0, start - PAGE_SIZE))}
        >
          Previous
        </button>
        <button type="button" disabled={!view.hasMore} onClick={() => onOffset(start + PAGE_SIZE)}>
          Next
        </button>
      </nav>
    </>
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
      <tr data-testid="request-row">
        <td>{request.timestamp ?? "—"}</td>
        <td>{request.method ?? "—"}</td>
        <td>
          <button
            type="button"
            className="linklike"
            data-testid="request-open"
            onClick={() => setOpen(!open)}
          >
            <Truncated value={request.path ?? "—"} max={60} />
          </button>
        </td>
        <td>{request.requestFrom ?? "—"}</td>
      </tr>
      {open ? (
        <tr data-testid="request-detail">
          <td colSpan={4}>
            <dl className="facts">
              <div className="fact">
                <dt>Path</dt>
                <dd>
                  <pre>{request.path ?? "—"}</pre>
                </dd>
              </div>
              <div className="fact">
                <dt>Headers</dt>
                <dd>
                  <pre>{formatHeaders(request.headers)}</pre>
                </dd>
              </div>
              <div className="fact">
                <dt>Query</dt>
                <dd>
                  <pre>{formatQuery(request.query)}</pre>
                </dd>
              </div>
              <div className="fact">
                {/* A base64 body rendered unlabelled reads as corrupted text rather than as the
                    binary payload it is. The mode token is `binary` and the encoding it implies is
                    base64 — `ResponseMode` serializes lowercase, and a text body omits the field
                    entirely, so `binary` is the only value that ever arrives. */}
                <dt>{request._mode === "binary" ? "Body (base64)" : "Body"}</dt>
                <dd>
                  <pre>{request.body ?? "(no body)"}</pre>
                </dd>
              </div>
            </dl>
          </td>
        </tr>
      ) : null}
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
