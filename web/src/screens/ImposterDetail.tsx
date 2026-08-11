import { Fragment, type FormEvent, type ReactNode, useState } from "react";

import { ApiError, apiGetText } from "../api/client.ts";
import type { components } from "../api/schema.ts";
import { IMPOSTER_COLUMNS, type ImposterColumn } from "../app/contract.ts";
import type { FleetView } from "../app/fleetView.ts";
import {
  type TrySpec,
  useClearRequests,
  useDeleteImposter,
  useFleetView,
  useImportAddImposter,
  useImposter,
  useTryStub,
} from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { toHash, useHashQuery } from "../app/routing.ts";
import { DetailRail } from "../components/detailRail.tsx";
import { Pending, PendingPanel } from "../components/pending.tsx";
import { ImposterField } from "../components/imposterFields.tsx";
import { Card, Confirm, Empty, ErrorNote, Ident, UNKNOWN, UNNAMED } from "../components/primitives.tsx";
import {
  type ExportProjection,
  cloneImposter,
  exportFilename,
  exportQuery,
  selectImposter,
} from "../features/imposters/portable.ts";
import { matchOrder } from "../features/stubs/matchOrder.ts";
import { projectPredicates } from "../features/stubs/predicates.ts";
import { type Sample, sampleRequest, toCurl } from "../features/stubs/sample.ts";
import { RecordingPanel } from "./RecordingPanel.tsx";
import { RequestLog } from "./RequestLog.tsx";
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
  // Only to annotate this port's place on the ring. A principal without the fleet scope gets the
  // rail without the epoch, never a 404 on a screen whose own read succeeded.
  const fleet = useFleetView({ enabled: can("fleet.read") });

  // In the hash query so a tab is linkable and survives a reload, the same rule the imposter
  // list's filters follow. An unknown value falls back rather than throwing: a stale bookmark
  // should render the imposter, not a blank screen.
  const [search, setSearch] = useHashQuery();
  const requested = new URLSearchParams(search).get("tab");
  const tab: DetailTab =
    DETAIL_TABS.find((entry) => entry.id === requested)?.id ?? "stubs";
  const setTab = (next: DetailTab): void => {
    const params = new URLSearchParams(search);
    if (next === "stubs") params.delete("tab");
    else params.set("tab", next);
    setSearch(params.toString());
  };

  const name = imposter.isSuccess ? imposter.data.data.name : undefined;
  const revision = imposter.isSuccess ? imposter.data.revision : null;

  return (
    <section className="screen">
      {/* The name and port together, then the identity line under them —
          tenant and revision, which are the two things an operator checks before editing. */}
      <header className="screen-head detail-head">
        <a className="btn" href={toHash({ screen: "imposters" })}>
          &larr; Imposters
        </a>
        <div className="detail-title">
          <h1>
            {name ?? UNNAMED} <Ident>{port}</Ident>
          </h1>
          <p className="scope-label">
            tenant <Ident>{tenant ?? "—"}</Ident>
            {revision === null ? null : (
              <>
                {" · "}
                <Ident>{revision}</Ident>
              </>
            )}
          </p>
        </div>
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

      <DetailTabs current={tab} onPick={setTab} />

      {imposter.isError ? <ErrorNote error={imposter.error} context="Could not read this imposter" /> : null}
      {imposter.isPending ? <p className="muted">Reading…</p> : null}

      {imposter.isSuccess ? (
        <>
          {cloning ? (
            <CloneImposter
              port={port}
              tenant={tenant}
              onDone={() => setCloning(false)}
              onCancel={() => setCloning(false)}
            />
          ) : null}

          {tab === "stubs" ? (
            /* The design splits this tab three ways — match order, the editor, the fleet rail. `Stubs`
               already carries the first two side by side, so the split here is between it and the
               rail rather than a third column bolted on. */
            <div className="screen-split">
              <div className="screen-main">
                <Stubs
                  port={port}
                  imposter={imposter.data.data}
                  revision={imposter.data.revision}
                  mayWrite={mayWrite}
                  editing={editing}
                  onEdit={setEditing}
                />
              </div>
              <DetailRail revision={imposter.data.revision} />
            </div>
          ) : null}

          {tab === "requests" ? <RequestLog port={port} /> : null}

          {tab === "ownership" ? (
            <OwnershipTab revision={imposter.data.revision} fleet={fleet.data} />
          ) : null}

          {tab === "settings" ? (
            <>
              <RiftKnobs imposter={imposter.data.data} />
              {mayRead ? (
                <ExportImposterControl port={port} name={imposter.data.data.name} tenant={tenant} />
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
              <RecordingPanel
                port={port}
                imposter={imposter.data.data}
                revision={imposter.data.revision}
              />
              <DangerZone port={port} name={imposter.data.data.name} />
            </>
          ) : null}
        </>
      ) : null}
    </section>
  );
}

const DETAIL_TABS = [
  { id: "stubs", label: "Stubs" },
  { id: "requests", label: "Requests" },
  { id: "ownership", label: "Ownership" },
  { id: "settings", label: "Settings" },
] as const;

type DetailTab = (typeof DETAIL_TABS)[number]["id"];

/**
 * The detail's tab strip.
 *
 * Real buttons in a `tablist`, not links: the tabs switch a panel within one screen rather than
 * navigating, and `aria-selected` carries the choice so the underline is not the only signal. The
 * selection lives in the hash query, so a tab is linkable and survives a reload — the same rule the
 * imposter list's filters follow.
 */
function DetailTabs({
  current,
  onPick,
}: {
  current: DetailTab;
  onPick: (tab: DetailTab) => void;
}): ReactNode {
  return (
    <div className="tabs" role="tablist" aria-label="Imposter sections">
      {DETAIL_TABS.map((entry) => (
        <button
          key={entry.id}
          type="button"
          role="tab"
          data-testid={`detail-tab-${entry.id}`}
          aria-selected={entry.id === current}
          onClick={() => onPick(entry.id)}
        >
          {entry.label}
        </button>
      ))}
    </div>
  );
}

/**
 * How this imposter's flow state is placed, and whether its socket came up.
 *
 * An imposter is served by every node — its config and stubs are replicated, so dispatch targets the
 * imposter object rather than a socket on one machine. What *is* placed is each **flow**: one node
 * holds a given flow's state and serializes writes to it. So this tab describes the rule and the
 * handoff, and sends the reader to the flow-state screen for any particular flow's owner — an
 * imposter does not have one, it has as many as it has flows.
 */
function OwnershipTab({
  revision,
  fleet,
}: {
  revision: string | null;
  fleet: FleetView | undefined;
}): ReactNode {
  return (
    <div className="screen-split">
      <div className="screen-main">
        <Card title="Flow-state placement">
          <p className="muted">
            The owner is computed from committed membership at this node&rsquo;s applied index, never
            negotiated: rendezvous hashing picks the highest-scoring ready node for the flow id, so
            every node that has applied the same membership reaches the same answer without talking
            to the others.
          </p>
          {/*
            There were `Owner`, `Successors` and `Fencing tuple` rows here, each pending on #359 and
            each implying this imposter has one of them. It does not: placement is per flow, and an
            imposter has as many owners as it has flows. Naming the scope is the honest replacement
            — it is also the thing an operator needs, because `contextScope` decides whether two
            imposters' same-named flows are one flow or two.
          */}
          <dl className="kv-grid">
            <dt>Placed by</dt>
            <dd className="muted">
              Flow id, within this imposter&rsquo;s context scope — see a flow&rsquo;s own owner on
              the flow-state screen.
            </dd>
            <dt>Ring epoch</dt>
            <dd>
              {fleet === undefined ? (
                <Pending
                  issue={361}
                  reason="The fleet projection is scoped to fleet.read, and this principal is refused it."
                />
              ) : (
                <Ident>{fleet.ringEpoch}</Ident>
              )}
            </dd>
            <dt>On handoff</dt>
            {/* Not a reading — a statement of what the cluster does, which is the thing an operator
                needs before they move a node. Worth stating precisely: two of these four are
                preserved and two are deliberately not. */}
            <dd className="warn-text">
              FSM and KV adopt · sequence cursors reset · proxyOnce claims are re-taken
            </dd>
          </dl>
        </Card>

        <Card title="Bind status per node">
          <PendingPanel
            issue={370}
            reason="Whether this imposter's listener came up is not reported per node. A port can be taken on one node and free on another, so this is a real condition — and a survivable one."
          />
          <p className="hint">
            A node that cannot bind still serves this imposter through the front door — dispatch
            targets the imposter object, not its socket. What a failed bind breaks is the
            direct-to-port path, on that node only.
          </p>
        </Card>
      </div>
      <DetailRail revision={revision} />
    </div>
  );
}

/**
 * Every act on this imposter that cannot be undone from here, in one place.
 *
 * They existed already and were scattered — a clear on the request log, a delete on the list, a
 * flow reset on the scenarios screen. Gathering them is the design's improvement and it is a real
 * one: an operator about to do something irreversible should see the whole set, because the
 * question "is this the one I want" is only answerable next to the alternatives.
 *
 * Each is gated on the capability that authorizes the call rather than on a blanket "may write" —
 * `rbac.ts` makes the point that transcribing the real action is what stops the table going stale.
 */
function DangerZone({ port, name }: { port: number; name: string | undefined }): ReactNode {
  const { can } = useSession();
  const clear = useClearRequests();
  const remove = useDeleteImposter();
  const [confirming, setConfirming] = useState<"clear" | "delete" | null>(null);

  const mayClear = can("requests.clear");
  const mayDelete = can("imposter.delete");
  if (!mayClear && !mayDelete) return null;

  return (
    <div className="card danger-zone" data-testid="danger-zone">
      <div className="card-body">
        <h2>Danger zone</h2>
        <p className="muted">
          Each of these is a replicated control op — it lands on every node, and nothing here undoes
          it.
        </p>
        <div className="row">
          {mayClear ? (
            <button
              className="btn danger"
              type="button"
              data-testid="danger-clear-requests"
              onClick={() => setConfirming("clear")}
            >
              Clear recorded requests
            </button>
          ) : null}
          {mayDelete ? (
            <button
              className="btn danger"
              type="button"
              data-testid="danger-delete-imposter"
              onClick={() => setConfirming("delete")}
            >
              Delete imposter
            </button>
          ) : null}
        </div>
      </div>

      {confirming === "clear" ? (
        <Confirm
          testId="confirm-danger-clear"
          title="Clear this imposter's recorded requests?"
          body={
            <>
              This empties the recorded requests for imposter {port} <b>fleet-wide</b> — the clear
              commits through Raft to every node, and nothing restores these rows.
            </>
          }
          confirmLabel="Clear log"
          requireTyped={String(port)}
          busy={clear.isPending}
          onCancel={() => setConfirming(null)}
          onConfirm={() => {
            clear.mutate({ port });
            setConfirming(null);
          }}
        />
      ) : null}

      {confirming === "delete" ? (
        <Confirm
          testId="confirm-danger-delete"
          title={`Delete ${name ?? String(port)}?`}
          body={
            <>
              This removes the imposter, its stubs, its recorded requests and its flow state across
              the fleet. Nothing undoes it.
            </>
          }
          confirmLabel={`Delete ${name ?? String(port)}`}
          requireTyped={String(port)}
          busy={remove.isPending}
          onCancel={() => setConfirming(null)}
          onConfirm={() => {
            remove.mutate({ port });
            setConfirming(null);
          }}
        />
      ) : null}
    </div>
  );
}

/** The knobs #370 publishes, in the order the design draws them. */
const RIFT_KNOBS = [
  { key: "durability", label: "flowState.durability" },
  { key: "flowIdSource", label: "flowIdSource" },
  { key: "readConsistency", label: "readConsistency" },
] as const;

/**
 * One knob's value, and whether this imposter chose it.
 *
 * "Inherited" is the compiled-in default, not a fleet-level setting — there is no fleet override
 * for these knobs, so the badge must not imply there is somewhere else to go and change it.
 */
function Knob({ knob }: { knob: components["schemas"]["ResolvedKnob"] }): ReactNode {
  const inherited = knob.source === "default";
  return (
    <>
      <Ident>{knob.value}</Ident>{" "}
      <span
        className={inherited ? "badge muted" : "badge"}
        title={
          inherited
            ? "Inherited: this imposter does not set the key, so the built-in default applies."
            : "Set on this imposter: the key is on its document, so changing the default will not change this."
        }
      >
        {inherited ? "inherited" : "set here"}
      </span>
    </>
  );
}

/**
 * The per-imposter `_rift` knobs the design draws.
 *
 * Three of the four are published on the imposter document (#370): `durability` and
 * `readConsistency` are decorated onto the read by the EE front, because upstream's `_rift` is an
 * allowlist that deliberately omits them, and `flowIdSource` is repeated into the same block so all
 * three read with one shape. `contextScope` is still pending under the RFC-005 state epic (#288).
 *
 * Read-only: the issue asks for the document to *carry* the knobs. Writing them through the panel
 * is not part of it.
 */
function RiftKnobs({ imposter }: { imposter: Imposter }): ReactNode {
  const resolved = imposter._rift?.flowStateResolved;
  return (
    <Card title="_rift · per-imposter knobs">
      <dl className="kv-grid">
        <dt>flowState.contextScope</dt>
        <dd>
          <Pending
            issue={288}
            reason="The contextScope knob is tracked under the RFC-005 state epic. Until it lands, a flow's context scope is a fleet default rather than a per-imposter choice."
          />
        </dd>
        {RIFT_KNOBS.map(({ key, label }) => (
          // A Fragment, not a wrapper element: `dt`/`dd` must be direct children of the `dl` for
          // the grid to lay out and for assistive tech to pair them.
          <Fragment key={key}>
            <dt>{label}</dt>
            <dd data-testid={`rift-knob-${key}`}>
              {/* Optional-chained per knob, not just on the block: a response carrying a partial
                  `flowStateResolved` should degrade to "unknown" rather than throw on the tab. */}
              {resolved?.[key] ? <Knob knob={resolved[key]} /> : UNKNOWN}
            </dd>
          </Fragment>
        ))}
      </dl>
    </Card>
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

  const open = editing !== null && (editing.kind === "new" || editingStub !== undefined);

  return (
    /*
     * The design's two columns: the stubs in match order, then whichever one is open.
     *
     * Match order is the screen's whole subject — the matcher walks this list top to bottom and the
     * first stub whose predicates hold answers — so it stays on screen while a stub is edited. The
     * table underneath used to be the only way to see it, which meant the order vanished the moment
     * an operator opened a stub to change it.
     */
    <div className="stub-workspace">
      <StubList
        stubs={stubs}
        editing={editing}
        mayWrite={mayWrite}
        onEdit={onEdit}
      />

      <div className="stub-pane">
      {open ? (
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
      ) : (
        /*
         * Nothing open: the full table, which carries what the column cannot — route, scenario,
         * predicate and response counts, and the per-stub try controls. The design has no such
         * table because its list is the whole navigator; keeping it here is a console-specific
         * choice that costs nothing while a stub is open and loses nothing while none is.
         */
        <StubTable
          port={port}
          stubs={stubs}
          revision={revision}
          mayWrite={mayWrite}
          onEdit={onEdit}
        />
      )}
      </div>
    </div>
  );
}

/**
 * The stubs in the order the matcher walks them.
 *
 * Each row says what it matches and what it answers, which is the pair an operator is comparing
 * when they ask why a request went where it did. A stub with no predicates is marked: it answers
 * everything from that position on, so every stub below it is unreachable — the one property of
 * this list that is invisible from the rows themselves.
 */
function StubList({
  stubs,
  editing,
  mayWrite,
  onEdit,
}: {
  stubs: Stub[] | undefined;
  editing: StubTarget | null;
  mayWrite: boolean;
  onEdit: (target: StubTarget | null) => void;
}): ReactNode {
  const entries = matchOrder(stubs);

  return (
    <aside className="stub-list" aria-label="Stubs in match order">
      <h2 className="eyebrow">Stubs · match order</h2>

      {/*
        Nothing said here when there is nothing to list.
        
        The pane beside this one already states both empty cases — no stubs, versus a response that
        carried no stub list — with the fuller copy and the testids that pin the distinction. Saying
        it twice put the same sentence on screen in two places, which is how it read.
      */}
      {entries.length === 0 ? null : (
        <ol className="stub-list-rows">
          {entries.map((entry) => {
            const selected =
              editing?.kind === "existing" && entry.id !== null && editing.stubId === entry.id;
            return (
              <li key={`${String(entry.index)}-${entry.id ?? "unnamed"}`}>
                <button
                  type="button"
                  className={`stub-list-row${selected ? " is-selected" : ""}`}
                  data-testid={`stub-list-${String(entry.index)}`}
                  /* A stub with no id has no by-id address, so it cannot be opened — the same rule
                     the table's edit action follows, stated here as a disabled control rather than
                     a click that silently does nothing. */
                  disabled={entry.id === null}
                  aria-current={selected ? "true" : undefined}
                  onClick={() => entry.id !== null && onEdit({ kind: "existing", stubId: entry.id })}
                >
                  <span className="stub-list-head">
                    <span className="stub-list-n">#{entry.index}</span>
                    {entry.method === null ? null : (
                      <span className="stub-list-method">{entry.method}</span>
                    )}
                    <span className="stub-list-target">
                      {entry.target ?? (entry.catchAll ? "matches everything" : "no path predicate")}
                    </span>
                  </span>
                  <span className="stub-list-meta">
                    {entry.catchAll ? <span className="stub-list-catchall">catch-all</span> : null}
                    <span>{entry.answer ?? "no response"}</span>
                    {entry.responses > 1 ? <span>· cycles {entry.responses}</span> : null}
                    <span className="stub-list-id">{entry.id ?? "no id"}</span>
                  </span>
                </button>
              </li>
            );
          })}
        </ol>
      )}

      {mayWrite ? (
        <button
          type="button"
          className="stub-list-add"
          data-testid="stub-list-add"
          onClick={() => onEdit({ kind: "new" })}
        >
          + add stub
        </button>
      ) : null}
    </aside>
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
