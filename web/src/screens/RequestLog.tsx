import { type ReactNode, useState } from "react";

import type { components } from "../api/schema.ts";
import { ApiError } from "../api/client.ts";
import type { FleetReadState } from "../app/fleetView.ts";
import {
  useClearRequests,
  useFleetView,
  useImposter,
  useImposters,
  useRequestLog,
} from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { Card, Confirm, Empty, ErrorNote, Ident, Truncated } from "../components/primitives.tsx";
import type { MatchOutcome, OutcomeView } from "../features/requests/diagnostics.ts";
import { describeOutcome } from "../features/requests/diagnostics.ts";
import type { RecordedRequest } from "../features/requests/source.ts";
import { coverageFor, describeCoverage, headerValues, page } from "../features/requests/source.ts";
import {
  type FieldSelection,
  defaultSelection,
  hasCatchAll,
  rowActionFor,
  stubFromRequest,
} from "../features/requests/stubFromRequest.ts";
import { StubEditor, type StubTarget } from "./StubEditor.tsx";

type Stub = components["schemas"]["Stub"];

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
  const { can } = useSession();
  const clear = useClearRequests();
  const [confirming, setConfirming] = useState(false);
  // `Action::SavedRequestsClear`, not `LifecycleToggle` — separate actions that happen to share a
  // role today. See `rbac.ts`.
  const mayClear = can("requests.clear");
  const mayWriteStubs = can("imposter.write");

  // Same read `ImposterDetail` drives its editor from: the body carries the stub list this screen's
  // shadow warning needs, and the `Rift-Cluster-Revision` header is the `If-Match` a save is
  // conditioned on. Called unconditionally (never behind `mayWriteStubs`) because a hook cannot come
  // and go across renders — `ImposterDetail` makes the same call.
  const imposter = useImposter(port);
  /*
   * `Array.isArray`, not a cast. The stub list arrives over the wire and this screen is the one an
   * operator opens while something is already wrong — a `stubs` that is `null`, an object, or a
   * scalar would otherwise reach `hasCatchAll(...).some` and `stubs.find(...)` and throw during
   * render, taking the log down at the worst possible moment. Same guard, and the same reason, as
   * `responseListOf` in `StubEditor.tsx`.
   */
  const rawStubs: unknown = imposter.data?.data.stubs;
  const stubs: Stub[] = Array.isArray(rawStubs) ? (rawStubs as Stub[]) : [];
  const revision = imposter.data?.revision ?? null;

  const [editing, setEditing] = useState<StubTarget | null>(null);
  // Bumped on every open so two "Stub this" clicks in a row get distinct React keys even when both
  // targets are `{kind: "new"}` — without it the editor would not remount and would keep showing the
  // first row's seed under the second row's button.
  const [editSeq, setEditSeq] = useState(0);
  const openEditor = (target: StubTarget): void => {
    setEditing(target);
    setEditSeq((n) => n + 1);
  };
  /*
   * While an editor is open its draft is the operator's, and re-seeding would remount and discard
   * whatever they have typed with no warning. So the row actions are disabled rather than allowed
   * to silently throw the draft away: closing the editor is an explicit act, and only then can a
   * new seed be derived. This is the other half of "re-deriving stops once the draft is edited" —
   * the selection is chosen BEFORE opening, and no path re-derives after.
   */
  const editorOpen = editing !== null;
  /*
   * The stub "Open stub" points at, if the list still carries it. A journal entry outlives the stub
   * that served it, so the id can be gone by the time the row is clicked — deleted between polls, or
   * read from a node behind on replication. `ImposterDetail` refuses to mount the editor in that
   * case and so does this screen: opening one over `{}` shows an empty document titled with the
   * missing id, and a save would then PUT `{}` over whatever the fleet actually has.
   */
  const existingStub =
    editing !== null && editing.kind === "existing"
      ? stubs.find((stub) => stub.id === editing.stubId)
      : undefined;
  const editorReady = editing !== null && (editing.kind === "new" || existingStub !== undefined);

  return (
    <section className="screen">
      <header className="screen-head">
        <h1>Request log</h1>
        <p className="muted">
          Imposter <Ident>{port}</Ident>
        </p>
        {mayClear ? (
          <>
            <div className="spacer" />
            <button
              className="btn sm danger"
              type="button"
              data-testid="clear-requests"
              onClick={() => setConfirming(true)}
            >
              Clear this node&rsquo;s log
            </button>
          </>
        ) : null}
      </header>

      {clear.isError ? (
        <ErrorNote error={clear.error} context="The log was not cleared" />
      ) : null}

      {confirming ? (
        <Confirm
          testId="confirm-clear-requests"
          title="Clear this node's recorded requests?"
          body={
            <>
              This empties what <b>this node</b> recorded for imposter {port}. Other nodes keep
              their own logs, and nothing restores these rows.
            </>
          }
          confirmLabel="Clear log"
          busy={clear.isPending}
          onCancel={() => setConfirming(false)}
          onConfirm={() => {
            clear.mutate({ port });
            setConfirming(false);
          }}
        />
      ) : null}

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

      {mayWriteStubs && editing !== null && !editorReady ? (
        <div className="banner warn" data-testid="stub-gone" role="status">
          <span className="b-glyph" aria-hidden="true">
            &#9650;
          </span>
          <div>
            The stub that answered this request is no longer in this imposter&rsquo;s list. It may
            have been deleted since the request was recorded. Nothing has been opened.
          </div>
        </div>
      ) : null}

      {mayWriteStubs && editorReady && editing !== null ? (
        <>
          {/*
            Gated on `kind === "new"`, not merely on the editor being open. The sentence is about
            APPENDING — "new stubs are appended, matching is first-match-wins" — which says nothing
            true about a stub that already exists somewhere in the list, possibly above the
            catch-all and firing perfectly well. A confidently wrong claim on this screen is the
            failure mode the whole module set is built to avoid.
          */}
          {editing.kind === "new" && hasCatchAll(stubs) ? (
            <div className="banner warn" data-testid="stub-shadow-warning" role="status">
              <span className="b-glyph" aria-hidden="true">
                ▲
              </span>
              <div>
                <strong>This imposter already has a stub that matches every request.</strong>
                <p>
                  New stubs are appended to the end of the list, and matching is first-match-wins,
                  so a stub saved below a catch-all is never reached — it will not fire.
                </p>
              </div>
            </div>
          ) : null}
          <StubEditor
            // Keyed by what is being edited, not by its content: an existing stub keeps its key
            // across the imposter's 5s poll (the same reason `ImposterDetail` keys this way), and a
            // new seeded stub gets a fresh key per `editSeq` so a second "Stub this" click remounts
            // with its own seed instead of reusing the first click's draft.
            key={editing.kind === "existing" ? `existing-${editing.stubId}` : `new-${editSeq}`}
            port={port}
            target={editing}
            original={editing.kind === "existing" ? existingStub : {}}
            revision={revision}
            onDone={() => setEditing(null)}
          />
        </>
      ) : null}

      {log.isPending ? <p className="muted">Reading…</p> : null}
      {log.isSuccess ? (
        <Rows
          state={log.data}
          offset={offset}
          onOffset={setOffset}
          mayWriteStubs={mayWriteStubs}
          onEdit={openEditor}
          editorOpen={editorOpen}
        />
      ) : null}
    </section>
  );
}

function Rows({
  state,
  offset,
  onOffset,
  mayWriteStubs,
  onEdit,
  editorOpen,
}: {
  state: { kind: "rows"; rows: RecordedRequest[] } | { kind: "unknown"; reason: string };
  offset: number;
  onOffset: (next: number) => void;
  mayWriteStubs: boolean;
  editorOpen: boolean;
  onEdit: (target: StubTarget) => void;
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
                <Row
                  key={`${start + index}`}
                  request={request}
                  mayWriteStubs={mayWriteStubs}
                  editorOpen={editorOpen}
                  onEdit={onEdit}
                />
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
function Row({
  request,
  mayWriteStubs,
  editorOpen,
  onEdit,
}: {
  request: RecordedRequest;
  mayWriteStubs: boolean;
  editorOpen: boolean;
  onEdit: (target: StubTarget) => void;
}): ReactNode {
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
            {mayWriteStubs ? (
              <StubRowAction request={request} onEdit={onEdit} editorOpen={editorOpen} />
            ) : null}
          </td>
        </tr>
      ) : null}
    </>
  );
}

/**
 * "Stub this" / "Open stub", per `rowActionFor` (#250).
 *
 * Gated on `imposter.write` by the caller, same as every other write control on this screen — this
 * component assumes it is only mounted when that capability is present.
 */
function StubRowAction({
  request,
  onEdit,
  editorOpen,
}: {
  request: RecordedRequest;
  onEdit: (target: StubTarget) => void;
  /** An editor is already open somewhere on this screen; see `editorOpen` in `Log`. */
  editorOpen: boolean;
}): ReactNode {
  const action = rowActionFor(request.matchOutcome);
  /*
   * Chosen up front, not while the editor is open: `StubEditor` owns the draft text once it opens
   * and exposes no "the operator has typed" signal this screen could watch, so there is no safe way
   * to re-derive the seed after the fact without silently overwriting a hand edit. Deriving the seed
   * once, from whatever is checked at the moment "Stub this" is clicked, sidesteps the problem
   * instead of solving it — the seed is a snapshot, and after that it is the operator's document.
   */
  const [selection, setSelection] = useState<FieldSelection>(defaultSelection);

  if (action.kind === "open") {
    return (
      <button
        className="btn sm"
        type="button"
        data-testid="request-open-stub"
        disabled={editorOpen}
        onClick={() => onEdit({ kind: "existing", stubId: action.stubId })}
      >
        Open stub
      </button>
    );
  }

  if (action.kind === "none") {
    return (
      <p className="muted" data-testid="request-no-stub-action">
        {action.reason === "matched-without-id"
          ? "The stub that matched declares no id, so it cannot be opened safely by id."
          : // The diagnostics panel directly above already says the outcome is unreadable. Offering
            // an action beside it would be this screen asserting two different things about one row.
            "These match diagnostics are unreadable, so there is nothing to act on."}
      </p>
    );
  }

  // action.kind === "stub": nothing matched (or nothing was recorded), so the useful verb is to
  // seed a new one from this request.
  const headerNames = Object.keys(request.headers ?? {});
  const toggleHeader = (name: string, checked: boolean): void => {
    const headers = new Set(selection.headers);
    if (checked) headers.add(name);
    else headers.delete(name);
    setSelection({ ...selection, headers });
  };

  return (
    <div className="stub-row-action">
      <fieldset className="stub-seed-fields" data-testid="stub-seed-fields">
        <legend>Match on</legend>
        <label className="check">
          <input
            type="checkbox"
            checked={selection.method}
            onChange={(event) => setSelection({ ...selection, method: event.target.checked })}
          />
          <span>Match on method</span>
        </label>
        <label className="check">
          <input
            type="checkbox"
            checked={selection.path}
            onChange={(event) => setSelection({ ...selection, path: event.target.checked })}
          />
          <span>Match on path</span>
        </label>
        <label className="check">
          <input
            type="checkbox"
            checked={selection.query}
            onChange={(event) => setSelection({ ...selection, query: event.target.checked })}
          />
          <span>Match on query</span>
        </label>
        <label className="check">
          <input
            type="checkbox"
            checked={selection.body}
            onChange={(event) => setSelection({ ...selection, body: event.target.checked })}
          />
          <span>Match on body</span>
        </label>
        {headerNames.map((name) => (
          <label className="check" key={name}>
            <input
              type="checkbox"
              checked={selection.headers.has(name)}
              onChange={(event) => toggleHeader(name, event.target.checked)}
            />
            <span>Match on header {name}</span>
          </label>
        ))}
      </fieldset>
      <button
        className="btn sm"
        type="button"
        data-testid="request-stub-this"
        /*
         * Disabled while an editor is open. Clicking again would remount `StubEditor` with a fresh
         * seed and silently discard whatever the operator had typed into the previous draft —
         * exactly the regeneration-over-hand-edits this flow is supposed to make impossible.
         * Closing the editor is an explicit act; only then is a new seed derived.
         */
        disabled={editorOpen}
        onClick={() => onEdit({ kind: "new", seed: stubFromRequest(request, selection) })}
      >
        Stub this
      </button>
      {editorOpen ? (
        <p className="muted" data-testid="stub-this-busy">
          Close the open stub editor before seeding another — re-seeding would discard the draft.
        </p>
      ) : null}
    </div>
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
      {imposters.isPending ? <p className="muted">Reading…</p> : null}
      {imposters.isSuccess && imposters.data.length === 0 ? (
        <Empty
          title="No imposters to read a log for"
          body="The request log is per-imposter. Create one and its recorded requests appear here."
        />
      ) : null}
      {imposters.isSuccess && imposters.data.length > 0 ? (
        <Card title="Choose an imposter" bleed>
          <div className="scroll-x">
            <table className="dense">
              <thead>
                <tr>
                  <th style={{ width: "12ch" }}>Port</th>
                  <th>Name</th>
                  <th style={{ width: "14ch" }} aria-label="Open" />
                </tr>
              </thead>
              <tbody>
                {imposters.data.map((imposter) => (
                  <tr key={imposter.port}>
                    <td>
                      <span className="port">{imposter.port}</span>
                    </td>
                    <td>
                      <Truncated value={imposter.name ?? "—"} />
                    </td>
                    <td>
                      <a className="btn sm" href={`#/requests/${imposter.port}`}>
                        Open log
                      </a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      ) : null}
    </section>
  );
}
