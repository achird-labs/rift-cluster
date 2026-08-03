import { type ReactNode, useState } from "react";

import { RawJsonBody } from "../api/client.ts";
import type { components } from "../api/schema.ts";
import type { FleetReadState } from "../app/fleetView.ts";
import { ISSUE_URL } from "../app/nav.ts";
import {
  StubConflict,
  useAddStub,
  useDiscardRecording,
  useFleetView,
  usePromoteRecording,
  useRecordedStubs,
} from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import {
  Confirm,
  Empty,
  ErrorNote,
  Ident,
  UNKNOWN,
  UnconfirmedNote,
} from "../components/primitives.tsx";
import {
  DEFAULT_GENERATOR_FIELDS,
  GENERATOR_FIELDS,
  type GeneratorField,
  PROXY_MODES,
  type ProxyMode,
  numberAt,
  proxyStubFor,
  recordAt,
  recordingState,
  stringAt,
} from "../features/recording/state.ts";

type Imposter = components["schemas"]["Imposter"];
type Stub = components["schemas"]["Stub"];

/**
 * The recording workflow (issue #246): start a proxy stub, review what it has captured — rendered
 * exactly as the engine returns it, never rewritten — then either promote the capture into the
 * imposter's own stub list or discard it.
 *
 * `imposter` and `revision` are the same values `ImposterDetail` already read for the stub table, not
 * a second fetch: the panel and the table are two views of one read, and re-fetching here would let
 * them disagree about which revision a write is conditioned on.
 */
export function RecordingPanel({
  port,
  imposter,
  revision,
}: {
  port: number;
  imposter: Imposter;
  revision: string | null;
}): ReactNode {
  const { can } = useSession();
  const mayWrite = can("imposter.write");
  const mayClear = can("requests.clear");
  const state = recordingState(imposter.stubs);

  const [formOpen, setFormOpen] = useState(false);
  const [promptingPromote, setPromptingPromote] = useState(false);
  const [promptingDiscard, setPromptingDiscard] = useState(false);

  // Only read while there is a recording to read. `?replayable=true&removeProxies=true` is a
  // different upstream projection of the same imposter, so polling it for an imposter that is not
  // recording is a request per 5s per open detail screen that can only ever answer "nothing".
  const recorded = useRecordedStubs(port, { enabled: state === "recording" });
  const promote = usePromoteRecording();
  const discard = useDiscardRecording();

  // Gated on the recorded read having *content*, not merely having resolved. Two separate reasons:
  // promoting before the review table loads is promoting blind, which is the one thing this screen
  // exists to prevent; and promoting an empty capture would `PUT {stubs: []}` — replacing the
  // imposter's entire stub list, proxy stub included, with nothing. A recording that has matched no
  // traffic yet is the normal state for the first minute of every recording, so that is a live
  // footgun rather than a hypothetical one.
  const recordedCount = recorded.isSuccess ? recorded.data.length : 0;
  const mayPromoteNow = mayWrite && state === "recording" && recordedCount > 0;

  return (
    <section className="card" data-testid="recording-panel">
      <div className="card-head">
        <h2>Recording</h2>
        <div className="spacer" />
        {state !== "recording" && mayWrite && !formOpen ? (
          <button className="btn sm" type="button" onClick={() => setFormOpen(true)}>
            Start recording
          </button>
        ) : null}
        {mayPromoteNow ? (
          <button className="btn sm" type="button" onClick={() => setPromptingPromote(true)}>
            Stop &amp; promote
          </button>
        ) : null}
        {state === "recording" && mayClear ? (
          <button
            className="btn sm danger"
            type="button"
            onClick={() => setPromptingDiscard(true)}
          >
            Discard recordings
          </button>
        ) : null}
      </div>

      <div className="card-body">
        <FleetCaveat />

        {formOpen ? (
          <StartRecordingForm port={port} revision={revision} onDone={() => setFormOpen(false)} />
        ) : null}

        {/*
         * Only while recording. A `replaying` imposter's stubs are its own, not a capture, and
         * rendering them under a "Recording" heading above the stub table that already lists them
         * would present ordinary configuration as something waiting to be promoted.
         */}
        {state === "recording" ? <RecordedSection recorded={recorded} /> : null}
      </div>

      {promptingPromote ? (
        <Confirm
          testId="confirm-promote-recording"
          title="Stop recording and promote captured stubs?"
          body={
            <>
              This replaces this imposter&rsquo;s whole stub list with what has been recorded so
              far, conditioned on the revision this screen last read. Every recorded response is
              promoted exactly as captured — nothing here rewrites it.
            </>
          }
          confirmLabel={`Promote ${recordedCount} recorded stub${recordedCount === 1 ? "" : "s"}`}
          busy={promote.isPending}
          onCancel={() => setPromptingPromote(false)}
          onConfirm={() => {
            const stubs = recorded.data ?? [];
            promote.mutate(
              {
                port,
                stubId: "",
                body: new RawJsonBody(JSON.stringify({ stubs })),
                revision,
              },
              {
                // Closed on any outcome the fleet accepted, not only `applied`. An `unobservable`
                // commit *was* accepted — the op-status projection that would confirm it is
                // fleet-admin-gated, so most principals never see the confirmation — and leaving
                // the dialog open with no message reads as failure, which is how an operator ends
                // up promoting twice. The note below says what is actually known.
                onSuccess: () => setPromptingPromote(false),
              },
            );
          }}
        />
      ) : null}

      {promptingDiscard ? (
        <Confirm
          testId="confirm-discard-recording"
          title="Discard everything this recording has captured?"
          body={
            <>
              This empties what has been captured so far on this node. The proxy stub keeps
              recording — new matches are still captured — but nothing recorded up to now survives
              this.
            </>
          }
          confirmLabel="Discard recordings"
          busy={discard.isPending}
          onCancel={() => setPromptingDiscard(false)}
          onConfirm={() => {
            discard.mutate({ port }, { onSuccess: () => setPromptingDiscard(false) });
          }}
        />
      ) : null}

      {/*
       * An accepted-but-unconfirmed write says so, rather than saying nothing. Under
       * `--cluster-admin-async` the write answers 202 and the op-status projection that would
       * confirm it is fleet-admin-gated, so for most principals this is the *ordinary* outcome —
       * silence here would read as failure for a write that landed.
       */}
      {promote.data?.kind === "unobservable" ? (
        <UnconfirmedNote reason={promote.data.reason} />
      ) : null}
      {discard.data?.kind === "unobservable" ? (
        <UnconfirmedNote reason={discard.data.reason} />
      ) : null}

      {/*
       * A promote is conditioned on the revision the review table was read at, so a concurrent
       * change refuses it rather than overwriting. That refusal needs the same treatment the stub
       * editor gives its own conflicts (`StubEditor.tsx:241-270`): say nothing was sent, say the
       * other change is still in place, and offer the retry explicitly at the *fresh* revision —
       * retrying with the stale one would 409 forever.
       *
       * What it cannot do is show a mine/theirs diff. This replaces the whole stub list rather than
       * one addressed stub, so `StubConflict.theirs` is empty by construction here and there is no
       * single "theirs" to render. The honest equivalent is to re-read: the recorded projection
       * polls, so by the time this is on screen the table beside it already shows what would now be
       * promoted.
       */}
      {promote.error instanceof StubConflict ? (
        <div className="degraded" data-testid="promote-conflict" role="alert">
          <strong>This imposter changed while you were reviewing.</strong> Nothing was promoted, and
          the other change is still in place. The table above has been re-read — promote again to
          apply what it now shows, on top of that change.
          <div className="acts">
            <button
              type="button"
              className="btn sm"
              disabled={promote.error.revision === null || promote.isPending}
              onClick={() => {
                const fresh = promote.error instanceof StubConflict ? promote.error.revision : null;
                if (fresh === null) return;
                promote.mutate({
                  port,
                  stubId: "",
                  body: new RawJsonBody(JSON.stringify({ stubs: recorded.data ?? [] })),
                  revision: fresh,
                });
              }}
            >
              Promote again
            </button>
          </div>
        </div>
      ) : promote.isError ? (
        <ErrorNote error={promote.error} context="Promote failed" />
      ) : null}
      {discard.isError ? <ErrorNote error={discard.error} context="Discard failed" /> : null}
    </section>
  );
}

/**
 * Recording is per node until #226 lands: a proxy stub captures only the traffic *this* node
 * answers, so another node's matches record nothing here. Shown in every state, including Empty —
 * before any recording starts is exactly when this caveat is cheapest to act on.
 *
 * **Suppressed only on a confirmed single-node fleet.** `/_fleet/*` authorizes
 * `Action::ClusterAdmin`, so for every role below fleet-admin — including the editors who do most
 * of the recording — the read answers 403/404. Treating that as "one node" would hide the warning
 * from exactly the people it is for, and would be a fact invented from a failed read. So the three
 * states are kept apart the way `fleetView.ts`'s `viewConfidence` requires: only a **reading** that
 * says `singleNode` silences this; an unavailable or not-yet-asked read still warns, and says that
 * it could not confirm the fleet's size.
 */
function FleetCaveat(): ReactNode {
  const fleet = useFleetView({ polled: false });
  const state: FleetReadState = fleet.isSuccess
    ? { kind: "read", view: fleet.data }
    : fleet.isError
      ? { kind: "unavailable" }
      : { kind: "not-asked" };

  if (state.kind === "read" && state.view.singleNode) return null;

  return (
    <div className="banner warn" data-testid="recording-fleet-caveat" role="status">
      <span className="b-glyph" aria-hidden="true">
        ▲
      </span>
      <div>
        <strong>Recording is per node, not fleet-wide.</strong>
        <p>
          A proxy stub records only the requests this node itself answers — traffic another node in
          this fleet serves captures nothing here, and there is no merged view of every node&rsquo;s
          captures. Until{" "}
          <a href={ISSUE_URL(226)} target="_blank" rel="noreferrer">
            #226
          </a>{" "}
          lands, drive the traffic you want recorded at the node you are watching.
        </p>
        {state.kind === "read" ? null : (
          <p className="muted" data-testid="recording-fleet-unconfirmed">
            This fleet&rsquo;s size could not be read from here — <Ident>/_fleet/*</Ident> needs a
            FleetAdmin binding — so this warning is shown whether or not the fleet has more than one
            node.
          </p>
        )}
      </div>
    </div>
  );
}

function RecordedSection({
  recorded,
}: {
  recorded: ReturnType<typeof useRecordedStubs>;
}): ReactNode {
  if (recorded.isError) {
    return <ErrorNote error={recorded.error} context="Could not read what this recording captured" />;
  }
  if (recorded.isPending) {
    return <p className="muted">Reading what has been captured…</p>;
  }
  if (recorded.data.length === 0) {
    return (
      <Empty
        testId="recorded-none"
        title="Nothing recorded yet"
        body="Nothing has matched the proxy since this recording started, or since it was last discarded."
      />
    );
  }
  return (
    <div className="scroll-x">
      <table className="dense">
        <thead>
          <tr>
            <th className="numeric">#</th>
            <th>Route</th>
            <th className="numeric">Status</th>
            <th>Body</th>
          </tr>
        </thead>
        <tbody>
          {recorded.data.map((stub, index) => (
            <RecordedRow key={index} stub={stub} index={index} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

/**
 * One captured request/response pair, rendered exactly as the `removeProxies` projection returns
 * it: the response is in **flat form** (`statusCode`/`headers`/`body`, no `is` wrapper), and this
 * reads those top-level fields directly rather than normalising into the wrapped shape. Promoting
 * later sends this same document back verbatim — rendering it any other way here would mean
 * reviewing a different document from the one that gets promoted.
 */
function RecordedRow({ stub, index }: { stub: Stub; index: number }): ReactNode {
  const equals = recordAt(stub.predicates?.[0], "equals");
  const method = stringAt(equals, "method");
  const path = stringAt(equals, "path");

  const response = stub.responses?.[0];
  const statusCode = numberAt(response, "statusCode");
  const body = recordAt(response, "body");

  return (
    <tr data-testid={`recorded-row-${index}`}>
      <td className="numeric">
        <Ident>{index}</Ident>
      </td>
      <td>
        {method === undefined ? null : <span className="method">{method}</span>}{" "}
        <Ident>{path ?? UNKNOWN}</Ident>
      </td>
      <td className="numeric">
        <Ident>{statusCode ?? UNKNOWN}</Ident>
      </td>
      <td>
        <pre className="payload" data-testid={`recorded-body-${index}`}>
          {formatBody(body)}
        </pre>
      </td>
    </tr>
  );
}

function formatBody(body: unknown): string {
  if (body === undefined) return "(no body)";
  if (typeof body === "string") return body;
  return JSON.stringify(body);
}

/**
 * The start-recording form: pick a target, a proxy mode and which fields the recorded stubs match
 * on, see the exact document that will be sent, then send it as the one proxy stub the imposter
 * gets (`proxyStubFor`).
 */
function StartRecordingForm({
  port,
  revision,
  onDone,
}: {
  port: number;
  revision: string | null;
  onDone: () => void;
}): ReactNode {
  const [to, setTo] = useState("");
  const [mode, setMode] = useState<ProxyMode>("proxyOnce");
  const [fields, setFields] = useState<GeneratorField[]>([...DEFAULT_GENERATOR_FIELDS]);
  const [caseSensitive, setCaseSensitive] = useState(false);
  const add = useAddStub();

  const toggleField = (field: GeneratorField): void => {
    setFields((current) =>
      current.includes(field) ? current.filter((f) => f !== field) : [...current, field],
    );
  };

  const stub = proxyStubFor({ to, mode, fields, caseSensitive });
  const preview = JSON.stringify(stub, null, 2);

  return (
    <form
      className="stub-form"
      data-testid="recording-start-form"
      onSubmit={(event) => {
        event.preventDefault();
        if (revision === null) return;
        add.mutate(
          { port, stubId: "", body: new RawJsonBody(preview), revision },
          // Closed on any accepted outcome — see the promote note above for why `unobservable` is
          // an acceptance, not a failure.
          { onSuccess: () => onDone() },
        );
      }}
    >
      <div className="field">
        <label htmlFor="recording-to">Proxy target</label>
        <input
          id="recording-to"
          type="url"
          required
          placeholder="https://api.example.com"
          value={to}
          onChange={(event) => setTo(event.target.value)}
        />
      </div>

      <fieldset>
        <legend className="eyebrow">Proxy mode</legend>
        {PROXY_MODES.map((option) => (
          <label key={option.value} className="check" data-testid={`proxy-mode-${option.value}`}>
            <input
              type="radio"
              name="proxy-mode"
              value={option.value}
              checked={mode === option.value}
              onChange={() => setMode(option.value)}
            />
            <span>
              <strong>{option.label}</strong>
              <span className="note">{option.description}</span>
            </span>
          </label>
        ))}
      </fieldset>

      <fieldset>
        <legend className="eyebrow">Match recorded stubs on</legend>
        <p className="muted" data-testid="generator-default-note">
          Method, path and query are selected by default — headers and body tend to vary per request
          in ways that would otherwise stop a recording from ever replaying.
        </p>
        {GENERATOR_FIELDS.map((field) => (
          <label key={field} className="check">
            <input
              type="checkbox"
              data-testid={`generator-${field}`}
              checked={fields.includes(field)}
              onChange={() => toggleField(field)}
            />
            {field}
          </label>
        ))}
      </fieldset>

      <label className="check">
        <input
          type="checkbox"
          checked={caseSensitive}
          onChange={(event) => setCaseSensitive(event.target.checked)}
        />
        Case-sensitive matching
      </label>

      <div className="field">
        <span className="eyebrow">This is what will be sent</span>
        <pre className="payload" data-testid="recording-json-preview">
          {preview}
        </pre>
      </div>

      {add.data?.kind === "unobservable" ? <UnconfirmedNote reason={add.data.reason} /> : null}
      {add.isError ? <ErrorNote error={add.error} context="Could not start recording" /> : null}

      <nav className="pager">
        <button
          type="submit"
          className="btn sm"
          disabled={revision === null || to === "" || add.isPending}
        >
          Start recording
        </button>
        <button type="button" className="btn sm" onClick={onDone}>
          Cancel
        </button>
      </nav>
    </form>
  );
}
