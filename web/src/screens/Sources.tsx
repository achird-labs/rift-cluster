import { type FormEvent, type ReactNode, useState } from "react";

import { ApiError } from "../api/client.ts";
import type { components } from "../api/schema.ts";
import { SOURCE_COLUMNS } from "../app/contract.ts";
import {
  type SourcePullReport,
  type SourceWrite,
  useDeleteSource,
  usePullSource,
  useSources,
  useUpsertSource,
} from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { assertNever } from "../components/imposterFields.tsx";
import {
  Card,
  Confirm,
  Empty,
  ErrorNote,
  Ident,
  Status,
  UNKNOWN,
  UnconfirmedNote,
} from "../components/primitives.tsx";

type SourceRecord = components["schemas"]["SourceRecord"];

/** The one source a `pull` most recently reported on, so the report renders under its own row. */
type LastPull = { id: string; report: SourcePullReport } | null;

export function Sources(): ReactNode {
  const sources = useSources();
  const { can } = useSession();
  // Declare/edit and refresh all condition on the same action the server checks writes against
  // (`Action::SourceWrite`, granted alongside `ImposterWrite`); delete is its own action, the same
  // discipline `imposter.delete` already holds to elsewhere in this console — see `rbac.ts`.
  const mayWrite = can("imposter.write");
  const mayDelete = can("imposter.delete");
  const upsert = useUpsertSource();
  const remove = useDeleteSource();
  const pull = usePullSource();

  const [editing, setEditing] = useState<SourceRecord | "new" | null>(null);
  const [confirmingDelete, setConfirmingDelete] = useState<SourceRecord | null>(null);
  const [lastPull, setLastPull] = useState<LastPull>(null);

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
        {mayWrite ? (
          <>
            <div className="spacer" />
            <button
              className="btn primary"
              type="button"
              data-testid="new-source"
              onClick={() => setEditing("new")}
            >
              Declare source
            </button>
          </>
        ) : null}
      </header>

      {upsert.isError ? (
        <ErrorNote error={upsert.error} context="The source was not saved" />
      ) : null}
      {remove.isError ? (
        <ErrorNote error={remove.error} context="The source was not forgotten" />
      ) : null}
      {pull.isError ? <ErrorNote error={pull.error} context="The pull did not run" /> : null}
      {upsert.data?.kind === "unobservable" ? <UnconfirmedNote reason={upsert.data.reason} /> : null}
      {remove.data?.kind === "unobservable" ? <UnconfirmedNote reason={remove.data.reason} /> : null}

      {editing !== null ? (
        <SourceForm
          // Forces a remount when the target changes — switching from "new" to an existing source,
          // or from one row's "Edit" to another's, without closing the form first — so the fields
          // that seeded from `existing` on mount actually reseed. Without the `key`, React reuses
          // the same component instance and its `useState` initial values never re-run, leaving the
          // previous target's text sitting in the fields under a form that now claims to be a
          // different source's.
          key={editing === "new" ? "new" : editing.id}
          existing={editing === "new" ? null : editing}
          busy={upsert.isPending}
          onCancel={() => setEditing(null)}
          onSave={(body) => upsert.mutate(body, { onSuccess: () => setEditing(null) })}
        />
      ) : null}

      {confirmingDelete === null ? null : (
        <Confirm
          testId="confirm-delete-source"
          title={`Forget ${confirmingDelete.id}?`}
          body={
            <>
              Forgetting a source never cascades: nothing it produced is undeployed. Its imposters
              keep running, orphaned from the source and from then on hand-managed — there is
              nothing left to reapply them from.
            </>
          }
          confirmLabel={`Forget ${confirmingDelete.id}`}
          busy={remove.isPending}
          onCancel={() => setConfirmingDelete(null)}
          onConfirm={() => {
            remove.mutate({ id: confirmingDelete.id });
            setConfirmingDelete(null);
          }}
        />
      )}

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
                  {mayWrite || mayDelete ? <th aria-label="Actions" /> : null}
                </tr>
              </thead>
              <tbody>
                {sources.data.sources.map((source) => (
                  <Row
                    key={source.id}
                    source={source}
                    pollErrors={sources.data.nodeLocal.pollErrors}
                    mayWrite={mayWrite}
                    mayDelete={mayDelete}
                    pullBusy={pull.isPending}
                    pullReport={lastPull?.id === source.id ? lastPull.report : null}
                    onEdit={() => setEditing(source)}
                    onDelete={() => setConfirmingDelete(source)}
                    onPull={() =>
                      pull.mutate(
                        { id: source.id },
                        { onSuccess: (report) => setLastPull({ id: source.id, report }) },
                      )
                    }
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

function Row({
  source,
  pollErrors,
  mayWrite,
  mayDelete,
  pullBusy,
  pullReport,
  onEdit,
  onDelete,
  onPull,
}: {
  source: SourceRecord;
  pollErrors: Record<string, string>;
  mayWrite: boolean;
  mayDelete: boolean;
  pullBusy: boolean;
  pullReport: SourcePullReport | null;
  onEdit: () => void;
  onDelete: () => void;
  onPull: () => void;
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
        {/* Rendered next to the drift verdict rather than the poll column: a pull report is what
            "refresh now" just did to the replicated record, the same kind of fact drift is — not a
            property of this node's reach to the upstream host, which is what the poll column says. */}
        {pullReport === null ? null : <PullReportView report={pullReport} />}
      </td>
      <td data-testid={`source-poll-${source.id}`}>
        <PollCell error={pollErrors[source.id]} />
      </td>
      {mayWrite || mayDelete ? (
        <td>
          <span className="row">
            {mayWrite ? (
              <button
                className="btn sm"
                type="button"
                data-testid={`source-edit-${source.id}`}
                onClick={onEdit}
              >
                Edit
              </button>
            ) : null}
            {mayWrite ? (
              <button
                className="btn sm"
                type="button"
                data-testid={`source-pull-${source.id}`}
                disabled={pullBusy}
                onClick={onPull}
              >
                {pullBusy ? "Pulling…" : "Pull now"}
              </button>
            ) : null}
            {mayDelete ? (
              <button
                className="btn sm danger"
                type="button"
                data-testid="source-delete"
                aria-label={`Forget ${source.id}`}
                onClick={onDelete}
              >
                Forget
              </button>
            ) : null}
          </span>
        </td>
      ) : null}
    </tr>
  );
}

/**
 * What a pull just reported.
 *
 * The three outcomes are genuinely different and an operator has to be able to tell them apart:
 * `unchanged` means the fetched content matched what the source last applied, so nothing reached
 * the log at all; `skipped` means the pull DID commit a decision not to apply, because the source
 * had drifted and its policy said to leave it; and otherwise `changed` names the ports it created,
 * replaced or removed. Collapsing the first two into "nothing happened" would hide a drifted source
 * that is silently no longer tracking.
 *
 * `warnings` is server-authored text about what the pull did NOT apply. It is rendered rather than
 * dropped for the usual reason: the caveat is the part the operator needs.
 */
function PullReportView({ report }: { report: SourcePullReport }): ReactNode {
  return (
    <div className="note" data-testid="source-pull-report">
      {report.skipped ? (
        <span>
          Pull <b>skipped</b> — this source had drifted and its policy left it alone. The fleet does
          not hold the fetched content.
        </span>
      ) : report.unchanged ? (
        <span>
          <b>Unchanged</b> — what was fetched matched what this source last applied, so nothing was
          written.
        </span>
      ) : report.changed.length === 0 ? (
        <span>Applied, and no port changed.</span>
      ) : (
        <span>
          Applied — changed {report.changed.length === 1 ? "port" : "ports"}{" "}
          <b>{report.changed.join(", ")}</b>.
        </span>
      )}
      {report.version === null ? null : <span> Version {report.version}.</span>}
      {report.warnings.length === 0 ? null : (
        <ul className="plain" data-testid="source-pull-warnings">
          {report.warnings.map((warning) => (
            <li key={warning} className="warn-text">
              {warning}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

const SOURCE_MODES = ["pinned", "tracking"] as const satisfies readonly SourceRecord["mode"][];
const ON_DRIFT_POLICIES = [
  "overwrite",
  "skip",
  "fail",
] as const satisfies readonly SourceRecord["onDrift"][];

/**
 * Declare or edit a source — one form, because `POST /admin/sources` is an upsert and there is no
 * separate create route to give a second form to.
 *
 * `id` is locked once a source exists. The route addresses a source by the `id` **in its body**,
 * not a path segment, so changing it here on an edit would not rename the source — it would declare
 * a second one and leave the first behind, silently. The same one-way-field trap `NewImposter`'s
 * port avoids, for the same reason: there is nothing server-side to catch it.
 *
 * `onDrift`'s options are `SOURCE_COLUMNS`' own values, transcribed once as `ON_DRIFT_POLICIES`
 * rather than re-typed here — the column that already renders `source.onDrift` and this select draw
 * from the same three-value type, so neither can drift from the other without `tsc` noticing.
 */
function SourceForm({
  existing,
  busy,
  onSave,
  onCancel,
}: {
  existing: SourceRecord | null;
  busy: boolean;
  onSave: (body: SourceWrite) => void;
  onCancel: () => void;
}): ReactNode {
  const [id, setId] = useState(existing?.id ?? "");
  const [uri, setUri] = useState(existing?.uri ?? "");
  const [mode, setMode] = useState<SourceRecord["mode"]>(existing?.mode ?? "pinned");
  const [pollSecs, setPollSecs] = useState(existing?.pollSecs?.toString() ?? "");
  const [onDrift, setOnDrift] = useState<SourceRecord["onDrift"]>(existing?.onDrift ?? "fail");
  const [authRef, setAuthRef] = useState(existing?.authRef ?? "");
  const [invalid, setInvalid] = useState<string | null>(null);

  function submit(event: FormEvent): void {
    event.preventDefault();
    if (id.trim() === "") return setInvalid("A source needs an id.");
    if (uri.trim() === "") return setInvalid("A source needs a URI.");
    let parsedPollSecs: number | undefined;
    if (mode === "tracking") {
      const parsed = Number(pollSecs);
      // Only the shape is checked here — a whole number of seconds. *How short is too short* is
      // deliberately not asserted client-side: the server enforces its own floor, and duplicating
      // a number here would drift the day that floor changes. The refusal that comes back is the
      // one place that number is allowed to live.
      if (pollSecs.trim() === "" || !Number.isInteger(parsed) || parsed < 1) {
        setInvalid(
          "A tracking source needs a poll interval, in whole seconds. The server enforces its " +
            "own minimum — its refusal, not this hint, is the authority on what that floor is.",
        );
        return;
      }
      parsedPollSecs = parsed;
    }
    setInvalid(null);
    onSave({
      id: id.trim(),
      uri: uri.trim(),
      mode,
      onDrift,
      // Omitted rather than sent empty: `authRef` absent is "no credential", not a name that
      // happens to be the empty string.
      ...(authRef.trim() === "" ? {} : { authRef: authRef.trim() }),
      ...(parsedPollSecs === undefined ? {} : { pollSecs: parsedPollSecs }),
    });
  }

  return (
    <Card title={existing === null ? "Declare source" : `Edit ${existing.id}`}>
      <form className="stub-form" onSubmit={submit} data-testid="source-form">
        <div className="field-row">
          <div className="field">
            <label htmlFor="source-id">Id</label>
            <input
              id="source-id"
              value={id}
              onChange={(event) => setId(event.target.value)}
              disabled={existing !== null}
              placeholder="payments"
            />
          </div>
          <div className="field">
            <label htmlFor="source-uri">URI</label>
            <input id="source-uri" value={uri} onChange={(event) => setUri(event.target.value)} />
          </div>
        </div>

        <div className="field-row">
          <div className="field">
            <label htmlFor="source-mode">Mode</label>
            <select
              id="source-mode"
              value={mode}
              onChange={(event) => setMode(event.target.value as SourceRecord["mode"])}
            >
              {SOURCE_MODES.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </div>
          <div className="field">
            <label htmlFor="source-drift">On drift</label>
            <select
              id="source-drift"
              value={onDrift}
              onChange={(event) => setOnDrift(event.target.value as SourceRecord["onDrift"])}
            >
              {ON_DRIFT_POLICIES.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </div>
        </div>

        {mode === "tracking" ? (
          <div className="field">
            <label htmlFor="source-poll">Poll interval (seconds)</label>
            <input
              id="source-poll"
              inputMode="numeric"
              value={pollSecs}
              onChange={(event) => setPollSecs(event.target.value)}
              placeholder="60"
            />
            <p className="hint">
              The server enforces a minimum poll interval. If this is too short, its refusal — not
              this hint — is the authority on what the floor actually is.
            </p>
          </div>
        ) : null}

        <div className="field">
          <label htmlFor="source-auth">Credential reference (never a secret)</label>
          <input
            id="source-auth"
            value={authRef}
            onChange={(event) => setAuthRef(event.target.value)}
            placeholder="optional"
          />
          <p className="hint">
            The name of a credential already configured on this node — never the credential
            itself. The server refuses a URI carrying embedded userinfo precisely so a secret can
            never reach the replicated log; do not paste one into the URI field either.
          </p>
        </div>

        {invalid === null ? null : (
          <p className="error" data-testid="source-invalid" role="alert">
            {invalid}
          </p>
        )}

        <div className="row">
          <button className="btn primary" type="submit" data-testid="source-save" disabled={busy}>
            {busy ? "Saving…" : "Save source"}
          </button>
          <button className="btn" type="button" onClick={onCancel} disabled={busy}>
            Cancel
          </button>
        </div>
      </form>
    </Card>
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
