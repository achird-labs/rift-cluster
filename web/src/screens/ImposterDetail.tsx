import { type FormEvent, type ReactNode, useState } from "react";

import { ApiError, apiGetText } from "../api/client.ts";
import type { components } from "../api/schema.ts";
import { IMPOSTER_COLUMNS, type ImposterColumn } from "../app/contract.ts";
import { type TrySpec, useImportAddImposter, useImposter, useTryStub } from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { toHash } from "../app/routing.ts";
import { ImposterField } from "../components/imposterFields.tsx";
import { Card, Empty, ErrorNote, Ident, UNKNOWN } from "../components/primitives.tsx";
import {
  type ExportProjection,
  cloneImposter,
  exportFilename,
  exportQuery,
  selectImposter,
} from "../features/imposters/portable.ts";
import { projectPredicates } from "../features/stubs/predicates.ts";
import { type Sample, sampleRequest, toCurl } from "../features/stubs/sample.ts";
import { RecordingPanel } from "./RecordingPanel.tsx";
import { DeleteStubButton, StubEditor, type StubTarget } from "./StubEditor.tsx";

type Imposter = components["schemas"]["Imposter"];
type Stub = components["schemas"]["Stub"];

/**
 * Trigger a browser download of text this console already has in hand — never re-fetched, never
 * re-serialized. `apiGetText`'s whole point (#251) is bytes the download path must not touch.
 */
function downloadText(filename: string, text: string): void {
  const blob = new Blob([text], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  /*
   * Deferred a tick rather than revoked inline. Chrome copes with an immediate revoke, but Safari
   * and older Firefox can abort a download that is still being handed to the browser — the file
   * silently never arrives, which looks exactly like the export having failed.
   */
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

/** The message a failed export or clone step should show — the server's own words when it has them. */
function errorText(error: unknown): string {
  if (error instanceof ApiError) return error.body;
  return error instanceof Error ? error.message : String(error);
}

/**
 * Everything the list shows, plus `host` — which the list omits only for width. The detail screen
 * has room, and the bind address is one of the values an operator most often needs to paste.
 */
const DETAIL_FIELDS: readonly Pick<ImposterColumn, "key" | "label">[] = [
  ...IMPOSTER_COLUMNS,
  { key: "host", label: "Host" },
];

/**
 * One imposter, with its stubs editable by id (C5, #188).
 *
 * The read is `useImposter` rather than a bare `apiGet` for one reason: it also returns the
 * `Rift-Cluster-Revision` the response was stamped with, and that token is the `If-Match` every
 * write on this screen is conditioned on. A save sent without it is last-writer-wins, so the
 * revision travels from this read into the editor and no further logic is allowed to invent one.
 */
export function ImposterDetail({ port }: { port: number }): ReactNode {
  const { can, tenant } = useSession();
  const imposter = useImposter(port);
  const [editing, setEditing] = useState<StubTarget | null>(null);
  const [cloning, setCloning] = useState(false);
  const mayWrite = can("imposter.write");
  const mayRead = can("imposter.read");

  return (
    <section className="screen">
      <header className="screen-head">
        <a href={toHash({ screen: "imposters" })}>&larr; Imposters</a>
        <h1>
          Imposter <Ident>{port}</Ident>
        </h1>
        <p className="scope-label">Served by this node from replicated state.</p>
        <div className="spacer" />
        {mayWrite ? (
          <button
            className="btn"
            type="button"
            data-testid="clone-imposter"
            onClick={() => setCloning(true)}
          >
            Duplicate
          </button>
        ) : null}
      </header>

      {imposter.isError ? <ErrorNote error={imposter.error} context="Could not read this imposter" /> : null}
      {imposter.isPending ? <p className="muted">Reading…</p> : null}

      {imposter.isSuccess ? (
        <>
          {mayRead ? (
            <ExportImposterControl port={port} name={imposter.data.data.name} tenant={tenant} />
          ) : null}
          {cloning ? (
            <CloneImposter
              port={port}
              tenant={tenant}
              onDone={() => setCloning(false)}
              onCancel={() => setCloning(false)}
            />
          ) : null}
          <dl className="tiles">
            {DETAIL_FIELDS.map((field) => (
              <div key={field.key} className="tile">
                <dt>{field.label}</dt>
                <dd data-testid={`detail-${field.key}`}>
                  <ImposterField imposter={imposter.data.data} field={field.key} />
                </dd>
              </div>
            ))}
          </dl>
          <RecordingPanel port={port} imposter={imposter.data.data} revision={imposter.data.revision} />
          <Stubs
            port={port}
            imposter={imposter.data.data}
            revision={imposter.data.revision}
            mayWrite={mayWrite}
            editing={editing}
            onEdit={setEditing}
          />
        </>
      ) : null}
    </section>
  );
}

/** Export this one imposter, in either projection (#251). */
function ExportImposterControl({
  port,
  name,
  tenant,
}: {
  port: number;
  name: string | undefined;
  tenant: string | null;
}): ReactNode {
  const [busy, setBusy] = useState<ExportProjection | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function run(projection: ExportProjection): Promise<void> {
    setError(null);
    setBusy(projection);
    try {
      /*
       * The SET route, then select — not `/imposters/:port`. That route ignores `replayable`
       * entirely (it reads only `removeProxies`), so it answers with the full `ImposterDetail`:
       * `numberOfRequests`, the recorded `requests` journal with its headers and bodies, and
       * `_links` naming the serving node. Unstable across exports, and it would put captured
       * credentials into a file this screen tells the operator to commit. See `portable.ts`.
       */
      const setText = await apiGetText(`/imposters${exportQuery(projection)}`, { tenant });
      const selected = selectImposter(setText, port);
      if (selected.kind === "error") throw new Error(selected.message);
      const text = selected.text;
      downloadText(exportFilename(port, name), text);
    } catch (error_) {
      setError(errorText(error_));
    } finally {
      setBusy(null);
    }
  }

  return (
    <div className="card" data-testid="export-imposter">
      <div className="card-body">
        <span className="eyebrow">Export this imposter</span>
        <div className="field-row">
          <button
            className="btn sm"
            type="button"
            disabled={busy !== null}
            onClick={() => void run("replay-ready")}
          >
            {busy === "replay-ready" ? "Exporting…" : "Replay-ready"}
          </button>
          <span className="note">
            Default. Recorded proxy responses become static stubs; proxy stubs are dropped.
          </span>
        </div>
        <div className="field-row">
          <button
            className="btn sm"
            type="button"
            disabled={busy !== null}
            onClick={() => void run("as-configured")}
          >
            {busy === "as-configured" ? "Exporting…" : "As configured"}
          </button>
          <span className="note">Proxy stubs kept, so the importer goes on recording.</span>
        </div>
        {error === null ? null : (
          <p className="error" data-testid="export-imposter-error" role="alert">
            {error}
          </p>
        )}
      </div>
    </div>
  );
}

/**
 * Duplicate this imposter onto a new port (#251).
 *
 * Reads the source with `apiGetText` — the same route `ExportImposterControl` downloads from — and
 * the `as-configured` projection specifically: `cloneImposter`'s whole point is a variant to try
 * against the same mock, and `replay-ready` would silently drop any proxy stub on the way.
 */
function CloneImposter({
  port,
  tenant,
  onDone,
  onCancel,
}: {
  port: number;
  tenant: string | null;
  onDone: () => void;
  onCancel: () => void;
}): ReactNode {
  const [newPort, setNewPort] = useState("");
  const [newName, setNewName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const add = useImportAddImposter();

  async function submit(event: FormEvent): Promise<void> {
    event.preventDefault();
    const parsedPort = Number(newPort);
    if (!Number.isInteger(parsedPort) || parsedPort < 1 || parsedPort > 65535) {
      setError("A port must be a whole number between 1 and 65535.");
      return;
    }
    setError(null);
    setBusy(true);
    try {
      // Same reason as the export above: the per-port route would hand back the request journal and
      // `_links`, and `{ ...source }` would copy both into the new imposter — exactly what the
      // dialog promises does NOT happen.
      const setText = await apiGetText(`/imposters${exportQuery("as-configured")}`, { tenant });
      const selected = selectImposter(setText, port);
      if (selected.kind === "error") throw new Error(selected.message);
      const text = selected.text;
      let parsed: unknown;
      try {
        parsed = JSON.parse(text) as unknown;
      } catch (cause) {
        throw new Error(
          `the exported imposter could not be read as JSON: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      const cloned = cloneImposter(parsed, parsedPort, newName.trim() === "" ? null : newName.trim());
      if (cloned.kind === "error") {
        setError(cloned.message);
        return;
      }
      await add.mutateAsync(cloned.imposter);
      onDone();
    } catch (cause) {
      setError(errorText(cause));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Card title="Duplicate this imposter">
      <form className="stub-form" onSubmit={(event) => void submit(event)} data-testid="clone-form">
        <p className="hint">
          Stubs and recorded responses come along, byte for byte; the request log does not — it is
          per-node journal state, not part of the imposter&rsquo;s configuration, so the duplicate
          starts with an empty one.
        </p>
        <div className="field-row">
          <div className="field">
            <label htmlFor="clone-port">New port</label>
            <input
              id="clone-port"
              inputMode="numeric"
              value={newPort}
              onChange={(event) => setNewPort(event.target.value)}
              placeholder="4546"
            />
          </div>
          <div className="field">
            <label htmlFor="clone-name">New name</label>
            <input
              id="clone-name"
              value={newName}
              onChange={(event) => setNewName(event.target.value)}
              placeholder="billing-copy"
            />
          </div>
        </div>

        {error === null ? null : (
          <p className="error" data-testid="clone-error" role="alert">
            {error}
          </p>
        )}

        <div className="row">
          <button className="btn primary" type="submit" disabled={busy}>
            {busy ? "Duplicating…" : "Duplicate"}
          </button>
          <button className="btn" type="button" onClick={onCancel} disabled={busy}>
            Cancel
          </button>
        </div>
      </form>
    </Card>
  );
}

function Stubs({
  port,
  imposter,
  revision,
  mayWrite,
  editing,
  onEdit,
}: {
  port: number;
  imposter: Imposter;
  revision: string | null;
  mayWrite: boolean;
  editing: StubTarget | null;
  onEdit: (target: StubTarget | null) => void;
}): ReactNode {
  const stubs = imposter.stubs;
  const editingStub =
    editing?.kind === "existing" ? stubs?.find((stub) => stub.id === editing.stubId) : undefined;

  return (
    <>
      {mayWrite ? (
        <nav className="pager">
          <button className="btn sm" type="button" onClick={() => onEdit({ kind: "new" })}>
            Add stub
          </button>
        </nav>
      ) : null}

      {editing !== null && (editing.kind === "new" || editingStub !== undefined) ? (
        <StubEditor
          /*
           * Keyed by the target, not by the stub's content. The imposter is polled, so `original`
           * changes underneath this panel on every tick — remounting on that would discard the
           * operator's in-progress draft several times a minute.
           */
          key={editing.kind === "new" ? "new" : editing.stubId}
          port={port}
          target={editing}
          original={editingStub ?? {}}
          revision={revision}
          onDone={() => onEdit(null)}
        />
      ) : null}

      <StubTable
        port={port}
        stubs={stubs}
        revision={revision}
        mayWrite={mayWrite}
        onEdit={onEdit}
      />
    </>
  );
}

function StubTable({
  port,
  stubs,
  revision,
  mayWrite,
  onEdit,
}: {
  port: number;
  stubs: Stub[] | undefined;
  revision: string | null;
  mayWrite: boolean;
  onEdit: (target: StubTarget) => void;
}): ReactNode {
  if (stubs === undefined) {
    return <p className="muted">This response carried no stub list.</p>;
  }
  if (stubs.length === 0) {
    return (
      <Empty
        testId="imposter-no-stubs"
        title="No stubs"
        body="Every request to this imposter falls through to the default response."
      />
    );
  }
  return (
    <section className="card">
      <div className="scroll-x">
    <table className="dense">
      <thead>
        <tr>
          <th className="numeric">#</th>
          <th>Id</th>
          <th>Route</th>
          <th>Scenario</th>
          <th className="numeric">Predicates</th>
          <th className="numeric">Responses</th>
          <th>Try</th>
          {mayWrite ? <th>Actions</th> : null}
        </tr>
      </thead>
      <tbody>
        {stubs.map((stub, index) => (
          // Prefixed rather than bare `index`: an id and an index share one key space, so a stub
          // whose id happened to be "1" would collide with the stub at index 1.
          <tr key={stub.id ?? `index-${index}`} data-testid={`stub-row-${index}`}>
            <td className="numeric">
              <Ident>{index}</Ident>
            </td>
            <td>
              <Ident>{stub.id ?? UNKNOWN}</Ident>
            </td>
            <td>
              <Ident>{stub.routePattern ?? UNKNOWN}</Ident>
            </td>
            <td>{stub.scenarioName ?? UNKNOWN}</td>
            <td className="numeric">
              <Ident>{stub.predicates?.length ?? UNKNOWN}</Ident>
            </td>
            <td className="numeric">
              <Ident>{stub.responses?.length ?? UNKNOWN}</Ident>
            </td>
            {/*
              Its own cell, always present. Copying a curl is a READ — it exercises the mock rather
              than changing it — so gating it on `mayWrite` would deny the try-it affordance to
              exactly the role most likely to be diagnosing why a stub is not matching.
            */}
            <td>
              <CopyCurlButton port={port} stub={stub} />
              <TryStubButton port={port} stub={stub} />
            </td>
            {mayWrite ? (
              <td>
                <StubActions port={port} stub={stub} revision={revision} onEdit={onEdit} />
              </td>
            ) : null}
          </tr>
        ))}
      </tbody>
    </table>
      </div>
      {/*
        Once, under the table — not once per row. Gated on `mayWrite` as well as on the stubs
        themselves: a viewer has no Actions column at all, so an explanation of why a button they
        cannot see is disabled would be answering a question they never asked.
      */}
      {mayWrite && stubs.some((stub) => stub.id === undefined) ? (
        <p className="hint" id={IDLESS_NOTE_ID} data-testid={IDLESS_NOTE_ID}>
          {IDLESS_REASON}
        </p>
      ) : null}
    </section>
  );
}

/** Why an id-less stub cannot be edited by id. Shown once under the table, not once per row. */
const IDLESS_REASON =
  "This stub has no id, so there is no by-id address for it. Editing it by position could " +
  "overwrite a different stub if another editor inserts one first.";

/** Ties each inert Edit button to the single explanation below the table, for assistive tech. */
const IDLESS_NOTE_ID = "stub-idless-note";

/**
 * The write controls for one stub.
 *
 * A stub with no `id` gets them inert, with the reason on screen. It is **not** given an
 * index-addressed fallback: an index is a position, so an index-addressed write racing a concurrent
 * insert or delete replaces a different stub and answers `200`. Refusing to offer the action is the
 * only honest option — the fix is to give the stub an id, which is a change to the stub.
 *
 * The reason itself lives under the table rather than in this cell. It is two sentences and it is
 * identical for every id-less stub, so rendering it per row forced the Actions column to hold a
 * paragraph: the row grew to roughly 200px and the buttons beside it wrapped mid-word. The cell
 * keeps a short `no id` marker and points at the shared note through `aria-describedby`, so the
 * explanation is still one tab-stop away for a screen reader and still on screen for everyone else.
 */
/**
 * Copy a `curl` that exercises this stub.
 *
 * A read action, so it is offered to anyone who can see the stub — trying a mock is not editing it.
 *
 * The origin is this page's host with the IMPOSTER's port: the console is served from the admin
 * port, and the stub answers on its own. That is also why this copies a command instead of sending
 * the request itself — a browser fetch from the admin origin to the imposter's port is cross-origin
 * and a mock server sends no CORS headers, so an in-page "send" would be blocked for most stubs.
 * A command the operator runs in their own terminal has no such problem.
 */
function CopyCurlButton({ port, stub }: { port: number; stub: Stub }): ReactNode {
  const [state, setState] = useState<"idle" | "copied" | "failed">("idle");

  const projection = projectPredicates(stub);
  // A stub whose predicates the form cannot model is exactly the stub whose request cannot be
  // derived — offering a button that would produce `GET /` regardless would be a guess wearing a
  // command's clothes.
  if (projection.kind !== "predicates") return null;

  const sample = sampleRequest(projection.items);
  const origin = `${window.location.protocol}//${window.location.hostname}:${port}`;
  const command = toCurl(sample, origin);

  return (
    <span className="row">
      <button
        className="btn sm"
        type="button"
        data-testid={`copy-curl-${stub.id ?? "unnamed"}`}
        // The caveats ride on the control that produces the command, so an operator cannot copy a
        // partial request without the reason it is partial being one hover away.
        title={
          sample.caveats.length === 0
            ? command
            : `${command}\n\nThis request may not match:\n- ${sample.caveats.join("\n- ")}`
        }
        onClick={() => {
          void navigator.clipboard
            .writeText(command)
            .then(() => setState("copied"))
            .catch(() => setState("failed"));
        }}
      >
        Copy curl
      </button>
      {state === "copied" ? (
        <span className="muted" role="status">
          copied{sample.caveats.length === 0 ? "" : ` · ${sample.caveats.length} caveat(s)`}
        </span>
      ) : null}
      {state === "failed" ? (
        <span className="warn-text" role="status">
          clipboard blocked — the command is on the button&rsquo;s tooltip
        </span>
      ) : null}
    </span>
  );
}

/**
 * Send this stub's derived sample request to the imposter and show what came back (#335).
 *
 * The sibling of `CopyCurlButton` above, and offered under exactly the same *derivability* rule —
 * a stub whose predicates cannot be modelled gets neither, because a request that cannot be
 * derived must not be offered as one that was. Where the two differ is **who** gets it:
 * `imposter.try` is Operator-tier, because this one makes the server originate the request, which
 * advances scenario state, appends to the request log, and can trigger proxy recording. A Viewer
 * keeps the curl button, which answers the same diagnostic question without the server acting on
 * their behalf.
 *
 * Caveats are surfaced beside the result rather than only on the tooltip the curl button uses.
 * They matter more here: with curl the operator reads the command before running it, so a skipped
 * predicate is visible; here the request has already gone, and a non-match whose cause was a
 * caveat reads as a bug in the mock unless the caveat is on screen next to the answer.
 */
/**
 * The wire envelope for a derived sample. One place, because it is both what gets sent and — via
 * `send.variables` — the key the result is matched against.
 */
function tryEnvelope(sample: Sample): TrySpec {
  return {
    method: sample.method,
    path: sample.target,
    headers: sample.headers,
    // `null` is `sample.ts`'s "no body"; the contract's is an absent field.
    ...(sample.body === null ? {} : { body: sample.body }),
  };
}

function TryStubButton({ port, stub }: { port: number; stub: Stub }): ReactNode {
  const { can } = useSession();
  const send = useTryStub(port);

  const projection = projectPredicates(stub);
  const mayTry = can("imposter.try");
  if (!mayTry || projection.kind !== "predicates") return null;

  const sample = sampleRequest(projection.items);
  const key = stub.id ?? "unnamed";
  /*
   * Shown only when it answers the request this row would send *now*.
   *
   * `useMutation`'s `data` lives as long as the component instance, and a row keyed by stub id
   * survives an edit — so without this check, changing a predicate and pressing nothing leaves the
   * previous verdict on screen next to the new stub. That is the worst possible failure for this
   * particular panel: its entire purpose is "did my stub match", and the operator would read a
   * stale answer as the new one. Comparing against the request actually sent (`send.variables`)
   * ties the result to its cause rather than to which component happens to be mounted.
   */
  const sent = JSON.stringify(tryEnvelope(sample));
  const result =
    send.data !== undefined && JSON.stringify(send.variables?.request) === sent
      ? send.data
      : undefined;

  return (
    <span className="row">
      <button
        className="btn sm"
        type="button"
        data-testid={`try-stub-${key}`}
        disabled={send.isPending}
        onClick={() => {
          send.mutate({ request: tryEnvelope(sample) });
        }}
      >
        {send.isPending ? "Sending…" : "Send"}
      </button>
      {send.isError ? (
        <span className="warn-text" role="status" data-testid={`try-error-${key}`}>
          {/*
            The endpoint itself failed — the exchange never happened. Deliberately NOT rendered in
            the response panel as a status: telling an operator their mock answered 502 when the
            server could not reach it at all sends them after the wrong bug.
          */}
          could not send: {send.error.message}
        </span>
      ) : null}
      {result ? (
        <span className="stack" role="status" data-testid={`try-result-${key}`}>
          <span>
            <Ident>{result.status}</Ident> · {result.elapsedMs} ms
          </span>
          {result.headers.length > 0 ? (
            <span className="muted">
              {result.headers.map((header) => `${header.name}: ${header.value}`).join(" · ")}
            </span>
          ) : null}
          <code>{result.body}</code>
          {result.truncated ? (
            <span className="warn-text">
              body truncated at 1 MiB — what is shown is a prefix, not the whole answer
            </span>
          ) : null}
          {result.bodyLossy ? (
            <span className="warn-text">
              body was not valid UTF-8; replacement characters are shown
            </span>
          ) : null}
          {result.headersLossy ? (
            <span className="warn-text">
              a header value was not valid UTF-8; replacement characters are shown
            </span>
          ) : null}
          {sample.caveats.length > 0 ? (
            <span className="warn-text">
              This request may not match — {sample.caveats.length} caveat(s):{" "}
              {sample.caveats.join("; ")}
            </span>
          ) : null}
        </span>
      ) : null}
    </span>
  );
}

function StubActions({
  port,
  stub,
  revision,
  onEdit,
}: {
  port: number;
  stub: Stub;
  revision: string | null;
  onEdit: (target: StubTarget) => void;
}): ReactNode {
  const stubId = stub.id;
  if (stubId === undefined) {
    return (
      <span data-testid="stub-not-addressable">
        <button
          className="btn sm"
          type="button"
          disabled
          title={IDLESS_REASON}
          aria-describedby={IDLESS_NOTE_ID}
        >
          Edit
        </button>{" "}
        <span className="muted">no id</span>
      </span>
    );
  }
  return (
    <>
      <button
        className="btn sm"
        type="button"
        aria-label={`Edit ${stubId}`}
        onClick={() => onEdit({ kind: "existing", stubId })}
      >
        Edit
      </button>
      <DeleteStubButton port={port} stubId={stubId} revision={revision} />
    </>
  );
}
