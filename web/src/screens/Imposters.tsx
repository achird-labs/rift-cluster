import { type ChangeEvent, type FormEvent, type ReactNode, useEffect, useState } from "react";

import { ApiError, apiGetText } from "../api/client.ts";
import type { components } from "../api/schema.ts";
import { IMPOSTER_COLUMNS } from "../app/contract.ts";
import type { FleetReadState, FleetView } from "../app/fleetView.ts";
import { viewConfidence } from "../app/fleetView.ts";
import {
  useCreateImposter,
  useDeleteImposter,
  useFleetView,
  useImportAddImposter,
  useImposters,
  useLifecycleToggle,
  useReplaceImposters,
} from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { toHash } from "../app/routing.ts";
import { ImposterField } from "../components/imposterFields.tsx";
import {
  Card,
  Confirm,
  Empty,
  ErrorNote,
  Truncated,
  UNKNOWN,
  UnconfirmedNote,
} from "../components/primitives.tsx";
import {
  type ExportProjection,
  type ImportEntry,
  type ImportPlan,
  exportQuery,
  exportSetFilename,
  importPlan,
  parseImportDocument,
  renderSetDocument,
} from "../features/imposters/portable.ts";
import { type Finding, lintStub } from "../features/stubs/lint.ts";

type Imposter = components["schemas"]["Imposter"];

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

/** The message a failed export or import step should show — the server's own words when it has them. */
function errorText(error: unknown): string {
  if (error instanceof ApiError) return error.body;
  return error instanceof Error ? error.message : String(error);
}

export function Imposters(): ReactNode {
  const { can, tenant } = useSession();
  const imposters = useImposters();
  const create = useCreateImposter();
  const remove = useDeleteImposter();
  const [creating, setCreating] = useState(false);
  const [importing, setImporting] = useState(false);
  const [confirming, setConfirming] = useState<Imposter | null>(null);
  // Only to qualify what the list shows. A principal without the fleet scope simply gets no
  // qualification — never a 404 error on a screen whose own read succeeded.
  const mayReadFleet = can("fleet.read");
  const fleet = useFleetView({ enabled: mayReadFleet });
  const toggle = useLifecycleToggle();

  const confidence = viewConfidence(fleetReadState(mayReadFleet, fleet));
  const mayToggle = can("imposter.lifecycle");
  const mayCreate = can("imposter.write");
  // `imposter.delete`, not `imposter.write`: they are separate actions server-side and granted from
  // separate arms, so gating on the wrong one is a drift waiting to happen (see `rbac.ts`).
  const mayDelete = can("imposter.delete");
  const mayExport = can("imposter.read");
  // `rbac.ts` has no `imposter.exportSet`/`imposter.import` — export reads through the same read
  // gate every other list on this screen uses, import-add writes through the same write gate
  // `New imposter` does, and Replace all additionally needs delete because it is one.
  /*
   * `imposter.delete` ALONE, transcribed from the action that actually authorizes the call:
   * `action_for(Terminated::ReplaceAllImposters) => Action::ImposterDelete` in `admin_front.rs`.
   * Both capabilities start at Editor today so `&& mayCreate` would decide identically — which is
   * exactly why it is wrong to write. `rbac.ts` makes the point: transcribing the real action is
   * what stops the table going stale silently the day one of the grants moves.
   */
  const mayReplace = mayDelete;
  const existingPorts = imposters.data?.flatMap((i) => (i.port === undefined ? [] : [i.port])) ?? [];

  return (
    <section className="screen">
      <header className="screen-head">
        <h1>Imposters</h1>
        <p className="scope-label" data-testid="imposters-scope-label">
          Served by this node from replicated state.
          {confidence.partial ? ` This node is degraded: ${confidence.reason}.` : ""}
          {confidence.unknown ? ` Caveat: ${confidence.reason}.` : ""}
        </p>
        <div className="spacer" />
        {mayCreate ? (
          <button
            className="btn"
            type="button"
            data-testid="open-import"
            onClick={() => setImporting(true)}
          >
            Import
          </button>
        ) : null}
        {mayCreate ? (
          <button
            className="btn primary"
            type="button"
            data-testid="new-imposter"
            onClick={() => setCreating(true)}
          >
            New imposter
          </button>
        ) : null}
      </header>

      {mayExport ? <ExportSetControl tenant={tenant} /> : null}

      {importing ? (
        <ImportPanel
          existingPorts={existingPorts}
          existingCount={imposters.data?.length ?? 0}
          mayWrite={mayCreate}
          mayReplace={mayReplace}
          onClose={() => setImporting(false)}
        />
      ) : null}

      {create.isError ? (
        <ErrorNote error={create.error} context="The imposter was not created" />
      ) : null}
      {remove.isError ? (
        <ErrorNote error={remove.error} context="The imposter was not deleted" />
      ) : null}
      {create.data?.kind === "unobservable" ? <UnconfirmedNote reason={create.data.reason} /> : null}
      {remove.data?.kind === "unobservable" ? <UnconfirmedNote reason={remove.data.reason} /> : null}

      {creating ? (
        <NewImposter
          busy={create.isPending}
          onCancel={() => setCreating(false)}
          onCreate={(body) =>
            create.mutate(body, {
              // Closes only on success. A refused create that dismissed its own form would take the
              // operator's typing with it and leave the error pointing at a screen with no form.
              onSuccess: () => setCreating(false),
            })
          }
        />
      ) : null}

      {confirming === null ? null : (
        <Confirm
          testId="confirm-delete-imposter"
          title={`Delete ${confirming.name ?? `imposter ${confirming.port ?? ""}`}?`}
          body={
            <>
              This removes the imposter, its stubs, its recorded requests and its flow state across
              the fleet. Nothing undoes it.
            </>
          }
          confirmLabel={`Delete ${confirming.name ?? confirming.port ?? "imposter"}`}
          busy={remove.isPending}
          onCancel={() => setConfirming(null)}
          onConfirm={() => {
            const port = confirming.port;
            if (port !== undefined) remove.mutate({ port });
            setConfirming(null);
          }}
        />
      )}

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
                  {mayToggle || mayDelete ? <th aria-label="Actions" /> : null}
                </tr>
              </thead>
              <tbody>
                {imposters.data.map((imposter, index) => (
                  <Row
                    key={imposter.port ?? `unnamed-${index}`}
                    imposter={imposter}
                    mayToggle={mayToggle}
                    mayDelete={mayDelete}
                    busy={toggle.isPending}
                    onToggle={(port, enable) => toggle.mutate({ port, enable })}
                    onDelete={() => setConfirming(imposter)}
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
  mayDelete,
  busy,
  onToggle,
  onDelete,
}: {
  imposter: Imposter;
  mayToggle: boolean;
  mayDelete: boolean;
  busy: boolean;
  onToggle: (port: number, enable: boolean) => void;
  onDelete: () => void;
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
      {mayToggle || mayDelete ? (
        <td>
          {/* Rendered only for a role that holds the matching action. RFC-006 §3 rule 3: this is
              presentation — the admin front re-checks the same action on the call itself. */}
          {port === undefined ? null : (
            <span className="row">
              {mayToggle ? (
                <button
                  className="btn sm"
                  type="button"
                  disabled={busy}
                  aria-label={`${imposter.enabled ? "Disable" : "Enable"} ${label}`}
                  onClick={() => onToggle(port, !imposter.enabled)}
                >
                  {imposter.enabled ? "Disable" : "Enable"}
                </button>
              ) : null}
              {mayDelete ? (
                <button
                  className="btn sm danger"
                  type="button"
                  data-testid={`delete-imposter-${port}`}
                  aria-label={`Delete ${label}`}
                  onClick={onDelete}
                >
                  Delete
                </button>
              ) : null}
            </span>
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

/**
 * The create form.
 *
 * **The port is a required field, not a convenience the console hides.** `createImposter` refuses an
 * auto-assigned port because each node would pick its own and the imposter could not replicate — so
 * the operator names it, and a blank one is refused here rather than sent.
 *
 * Protocol is a closed choice because the engine's is: `manager.rs` accepts `http` and `https` and
 * answers `InvalidProtocol` for anything else. Choosing `https` reveals the PEM pair, because an
 * https imposter without a cert fails at creation by design — upstream fails loudly there rather
 * than silently serving cleartext, and a form that let you submit one would just relay that error.
 *
 * No `If-Match`: there is no prior revision of an imposter that does not exist yet. A port already
 * in use comes back as the fleet's own refusal, which is the only check that sees every node.
 */
function NewImposter({
  busy,
  onCreate,
  onCancel,
}: {
  busy: boolean;
  onCreate: (body: Imposter) => void;
  onCancel: () => void;
}): ReactNode {
  const [port, setPort] = useState("");
  const [protocol, setProtocol] = useState("http");
  const [name, setName] = useState("");
  const [recordRequests, setRecordRequests] = useState(true);
  const [cert, setCert] = useState("");
  const [certKey, setCertKey] = useState("");
  const [invalid, setInvalid] = useState<string | null>(null);

  function submit(event: FormEvent): void {
    event.preventDefault();
    // A port is 1–65535 and nothing else. Checked here so an obvious typo is a sentence next to the
    // field rather than a round trip that comes back as a 400 with no field to point at.
    const parsed = Number(port);
    if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) {
      setInvalid("Port must be a whole number between 1 and 65535.");
      return;
    }
    if (protocol === "https" && (cert.trim() === "" || certKey.trim() === "")) {
      setInvalid("An https imposter needs both a certificate and a key, or it refuses to start.");
      return;
    }
    setInvalid(null);
    onCreate({
      port: parsed,
      protocol,
      recordRequests,
      // Sent explicitly rather than left to the schema default: the contract marks it required (it
      // carries a default, which `openapi-typescript` renders as non-optional), and a newly created
      // imposter that arrived disabled would look like a create that half-worked.
      enabled: true,
      // Omitted rather than sent empty: the contract's fields are optional, and a blank name is not
      // the same fact as no name.
      ...(name.trim() === "" ? {} : { name: name.trim() }),
      ...(protocol === "https" ? { cert: cert.trim(), key: certKey.trim() } : {}),
    });
  }

  return (
    <Card title="New imposter">
      <form className="stub-form" onSubmit={submit} data-testid="new-imposter-form">
        <div className="field-row">
          <div className="field">
            <label htmlFor="new-port">Port</label>
            <input
              id="new-port"
              inputMode="numeric"
              value={port}
              onChange={(event) => setPort(event.target.value)}
              placeholder="4545"
            />
          </div>
          <div className="field">
            <label htmlFor="new-protocol">Protocol</label>
            <select
              id="new-protocol"
              value={protocol}
              onChange={(event) => setProtocol(event.target.value)}
            >
              <option value="http">http</option>
              <option value="https">https</option>
            </select>
          </div>
          <div className="field">
            <label htmlFor="new-name">Name</label>
            <input
              id="new-name"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="checkout-api"
            />
          </div>
        </div>

        {protocol === "https" ? (
          <div className="field-row">
            <div className="field">
              <label htmlFor="new-cert">Certificate (PEM)</label>
              <textarea id="new-cert" value={cert} onChange={(e) => setCert(e.target.value)} />
            </div>
            <div className="field">
              <label htmlFor="new-key">Private key (PEM)</label>
              <textarea id="new-key" value={certKey} onChange={(e) => setCertKey(e.target.value)} />
            </div>
          </div>
        ) : null}

        {/*
          Checked by default, which **diverges from the API**: the contract's `recordRequests`
          defaults to `false`, so `POST /imposters` with the field omitted records nothing.
          Console-created imposters are almost always created in order to be watched, and "why is
          my request log empty" is the confusion that costs a debugging cycle — so the console
          opts in and says so at the control rather than silently inheriting a default that makes
          its own request log useless. The cost is stated because it is unbounded until retention
          trims it.
        */}
        <label className="check">
          <input
            type="checkbox"
            checked={recordRequests}
            onChange={(event) => setRecordRequests(event.target.checked)}
          />
          <span>
            Record requests
            <span className="note">
              The request log shows nothing without this. Every request is held in memory until
              retention trims it — turn it off for an imposter under load.
            </span>
          </span>
        </label>

        {invalid === null ? null : (
          <p className="error" data-testid="new-imposter-invalid" role="alert">
            {invalid}
          </p>
        )}

        <div className="row">
          <button className="btn primary" type="submit" disabled={busy}>
            {busy ? "Creating…" : "Create imposter"}
          </button>
          <button className="btn" type="button" onClick={onCancel} disabled={busy}>
            Cancel
          </button>
        </div>
      </form>
    </Card>
  );
}

/**
 * Export the whole tenant, in either projection (#251).
 *
 * Two buttons, not a select-then-go: the projections carry a real semantic difference (whether the
 * import goes on recording), and naming both up front is worth more than one fewer click.
 */
function ExportSetControl({ tenant }: { tenant: string | null }): ReactNode {
  const [busy, setBusy] = useState<ExportProjection | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function run(projection: ExportProjection): Promise<void> {
    setError(null);
    setBusy(projection);
    try {
      const text = await apiGetText(`/imposters${exportQuery(projection)}`, { tenant });
      downloadText(exportSetFilename(tenant), text);
    } catch (error_) {
      setError(errorText(error_));
    } finally {
      setBusy(null);
    }
  }

  return (
    <div className="card" data-testid="export-imposters">
      <div className="card-body">
        <span className="eyebrow">Export every imposter in this tenant</span>
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
          <p className="error" data-testid="export-imposters-error" role="alert">
            {error}
          </p>
        )}
      </div>
    </div>
  );
}

/** One imposter's outcome from an `Add` run. */
type ImportResult = { port: number | null; ok: boolean; message?: string };

/**
 * The import panel (#251): paste or choose a file, see a pre-flight before anything is written,
 * then either `Add` (per-imposter, one request each) or the destructive `Replace all`.
 */
function ImportPanel({
  existingPorts,
  existingCount,
  mayWrite,
  mayReplace,
  onClose,
}: {
  existingPorts: readonly number[];
  existingCount: number;
  mayWrite: boolean;
  mayReplace: boolean;
  onClose: () => void;
}): ReactNode {
  const [text, setText] = useState("");
  const [findings, setFindings] = useState<Finding[] | "unavailable" | "pending">("pending");
  const [results, setResults] = useState<ImportResult[] | null>(null);
  const [running, setRunning] = useState(false);
  const [confirmingReplace, setConfirmingReplace] = useState(false);

  const add = useImportAddImposter();
  const replace = useReplaceImposters();

  const doc = parseImportDocument(text);
  const plan = doc.kind === "ok" ? importPlan(doc.entries, existingPorts) : null;

  // Advisory only, exactly as `StubEditor`'s pane: the server validates every write, and its
  // refusal — not this — is what an operator must act on.
  useEffect(() => {
    let current = true;
    setFindings("pending");
    void lintStub(text).then((result) => {
      if (current) setFindings(result);
    });
    return () => {
      current = false;
    };
  }, [text]);

  function onTextChange(next: string): void {
    setText(next);
    setResults(null);
  }

  async function handleFile(event: ChangeEvent<HTMLInputElement>): Promise<void> {
    const file = event.target.files?.[0];
    // Cleared unconditionally, so choosing the same file again after an edit still fires a change.
    event.target.value = "";
    if (file === undefined) return;
    onTextChange(await file.text());
  }

  async function runAdd(entries: ImportEntry[]): Promise<void> {
    setRunning(true);
    const collected: ImportResult[] = [];
    setResults(collected);
    for (const entry of entries) {
      try {
        const outcome = await add.mutateAsync(entry.imposter);
        // A failure of one must not abort the rest — the loop always continues to the next entry.
        collected.push({
          port: entry.port,
          ok: true,
          ...(outcome.kind === "unobservable" ? { message: outcome.reason } : {}),
        });
      } catch (error) {
        collected.push({ port: entry.port, ok: false, message: errorText(error) });
      }
      setResults([...collected]);
    }
    setRunning(false);
  }

  async function runReplace(entries: ImportEntry[]): Promise<void> {
    setConfirmingReplace(false);
    setRunning(true);
    try {
      await replace.mutateAsync(renderSetDocument(entries));
    } catch {
      // Nothing to do here: `replace.isError`/`replace.error` below renders it. An empty catch would
      // be the swallow; this one exists only to keep the rejection from becoming an unhandled one.
    }
    setRunning(false);
  }

  return (
    <Card title="Import imposters" testId="import-panel">
      <div className="field">
        <label htmlFor="import-text">Imposter JSON to import</label>
        <textarea
          id="import-text"
          rows={10}
          value={text}
          onChange={(event) => onTextChange(event.target.value)}
          placeholder='A single imposter, an {"imposters": [...]} document, or a bare list.'
        />
      </div>
      <div className="field">
        <label htmlFor="import-file">Or choose a file</label>
        <input
          id="import-file"
          type="file"
          accept=".json,application/json"
          onChange={(event) => void handleFile(event)}
        />
      </div>

      {doc.kind === "error" ? (
        <p className="error" data-testid="import-error" role="alert">
          {doc.message}
        </p>
      ) : null}

      {plan === null ? null : <ImportPreflight plan={plan} />}

      <ImportLint findings={findings} />

      {mayWrite ? (
        <nav className="pager">
          <button
            className="btn primary"
            type="button"
            data-testid="import-add"
            disabled={doc.kind !== "ok" || running}
            onClick={() => void runAdd(doc.kind === "ok" ? doc.entries : [])}
          >
            {running ? "Adding…" : "Add"}
          </button>
          {mayReplace ? (
            <button
              className="btn danger"
              type="button"
              data-testid="import-replace"
              disabled={doc.kind !== "ok" || running}
              onClick={() => setConfirmingReplace(true)}
            >
              Replace all
            </button>
          ) : null}
          <button className="btn" type="button" onClick={onClose} disabled={running}>
            Close
          </button>
        </nav>
      ) : null}

      {replace.isError ? <ErrorNote error={replace.error} context="The set was not replaced" /> : null}
      {replace.isSuccess ? (
        <p className="hint" role="status">
          Replaced. The list above reflects it once this reads back.
        </p>
      ) : null}

      {results === null ? null : <ImportResults results={results} />}

      {confirmingReplace ? (
        <Confirm
          testId="confirm-replace-imposters"
          title="Replace every imposter on this tenant?"
          body={
            <>
              This destroys the {existingCount} imposter{existingCount === 1 ? "" : "s"} currently
              on this screen — their stubs, their recorded requests and their flow state — and
              replaces them with exactly what this document names. Nothing undoes it.
            </>
          }
          confirmLabel={`Replace ${existingCount} imposter${existingCount === 1 ? "" : "s"}`}
          busy={running}
          onCancel={() => setConfirmingReplace(false)}
          onConfirm={() => void runReplace(doc.kind === "ok" ? doc.entries : [])}
        />
      ) : null}
    </Card>
  );
}

/**
 * What an import would do, worked out before anything is written (AC in #251): how many, which
 * ports, which already exist, which repeat within the document, and how many carry no port.
 */
function ImportPreflight({ plan }: { plan: ImportPlan }): ReactNode {
  const ports = plan.entries.map((entry) => entry.port ?? "no port").join(", ");
  return (
    <div className="hint" data-testid="import-preflight" role="status">
      <p>
        {plan.entries.length} imposter{plan.entries.length === 1 ? "" : "s"} in this document
        {plan.entries.length === 0 ? "" : `: ${ports}`}.
      </p>
      {plan.collisions.length === 0 ? null : (
        <p className="warn-text">
          Already served by this fleet: {plan.collisions.join(", ")}. <b>Add</b> will be refused for
          these; <b>Replace all</b> will overwrite them.
        </p>
      )}
      {plan.duplicates.length === 0 ? null : (
        <p className="warn-text">
          Named more than once in this document: {plan.duplicates.join(", ")} — only the last of
          each survives <b>Replace all</b>, and <b>Add</b> refuses every repeat after the first.
        </p>
      )}
      {plan.portless === 0 ? null : (
        <p className="warn-text">
          {plan.portless} carr{plan.portless === 1 ? "ies" : "y"} no usable port and will be
          refused.
        </p>
      )}
    </div>
  );
}

/** Advisory lint for the pasted document, the same discipline `StubEditor`'s pane follows. */
function ImportLint({ findings }: { findings: Finding[] | "unavailable" | "pending" }): ReactNode {
  return (
    <div className="stub-lint" data-testid="import-lint">
      {findings === "pending" ? <p className="muted">Linting…</p> : null}
      {findings === "unavailable" ? (
        <p className="muted">
          lint unavailable — the server still validates every write, and its refusal is what
          counts.
        </p>
      ) : null}
      {Array.isArray(findings) ? (
        findings.length === 0 ? (
          <p className="muted">No findings.</p>
        ) : (
          <ul>
            {findings.map((finding) => (
              <li key={`${finding.code}-${finding.location ?? ""}-${finding.message}`}>
                <strong>{finding.severity}</strong> {finding.code}: {finding.message}
                {finding.location === undefined ? null : ` (${finding.location})`}
              </li>
            ))}
          </ul>
        )
      ) : null}
    </div>
  );
}

/** Every imposter `Add` has attempted so far, in the order it attempted them. */
function ImportResults({ results }: { results: ImportResult[] }): ReactNode {
  return (
    <ul data-testid="import-results">
      {results.map((result, index) => (
        <li
          key={`${result.port ?? "no-port"}-${index}`}
          data-testid={`import-result-${result.port ?? "no-port"}-${index}`}
        >
          <b>{result.port ?? "(no port)"}</b>: {result.ok ? "ok" : "failed"}
          {result.message === undefined ? null : ` — ${result.message}`}
        </li>
      ))}
    </ul>
  );
}
