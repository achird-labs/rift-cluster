import { type ChangeEvent, type FormEvent, type ReactNode, useEffect, useRef, useState } from "react";

import { ApiError, apiGetText } from "../api/client.ts";
import type { components } from "../api/schema.ts";
import { IMPOSTER_COLUMNS } from "../app/contract.ts";
import type { FleetReadState, FleetView } from "../app/fleetView.ts";
import { viewConfidence } from "../app/fleetView.ts";
import {
  useClearRequests,
  useCreateImposter,
  useDeleteImposter,
  useFleetView,
  useImportAddImposter,
  useImposters,
  useLifecycleToggle,
  useReplaceImposters,
  useSources,
} from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { toHash, useHashQuery } from "../app/routing.ts";
import { ExportDialog } from "../components/exportDialog.tsx";
import { FleetRail } from "../components/fleetRail.tsx";
import { ImposterField, stubCountOf } from "../components/imposterFields.tsx";
import { Pending } from "../components/pending.tsx";
import { useToast } from "../components/toast.tsx";
import {
  type BulkAction,
  BulkBar,
  BulkReport,
  ImposterFilters,
  SortHeader,
} from "../components/imposterList.tsx";
import { type BulkResult, runBulk } from "../features/imposters/bulk.ts";
import {
  EMPTY_QUERY,
  actionablePorts,
  decodeQuery,
  encodeQuery,
  driftedPorts,
  sourceOwnedPorts,
  unclassifiedCount,
  visibleImposters,
} from "../features/imposters/list.ts";
import {
  Card,
  Confirm,
  Empty,
  ErrorNote,
  Truncated,
  UNKNOWN,
  UNNAMED,
  UnconfirmedNote,
} from "../components/primitives.tsx";
import {
  type ExportOptions,
  type ImportEntry,
  type ImportPlan,
  exportOptionsQuery,
  exportSetFilename,
  importPlan,
  parseImportDocument,
  renderSetDocument,
} from "../features/imposters/portable.ts";
import { type Finding, lintStub } from "../features/stubs/lint.ts";
import type { CommitOutcome } from "../features/writes/commit.ts";

type Imposter = components["schemas"]["Imposter"];
type SourceRecord = components["schemas"]["SourceRecord"];

/**
 * The screen's four tiles.
 *
 * Three of them are real and one is a marker, and the split is the whole point of building it this
 * way rather than filling all four with plausible numbers:
 *
 * - **Imposters / stubs** — counted from the list this screen already holds.
 * - **Sources / drifted** — counted from `/admin/sources`, the same read the provenance filter uses.
 *   Absent entirely for a principal without `source.read`, rather than shown as zero: "you may not
 *   ask" and "the answer is none" are different facts.
 * - **Requests · fleet sum** — real since #363 declared `numberOfRequests` on the contract. It had
 *   reached the imposter body only through a non-exhaustive index signature, which is exactly the
 *   client-side guess `contract.ts` refuses, so summing it here would have laundered a value the
 *   contract rejected one file away. The tile is genuinely fleet-wide (#223 rewrites each entry to
 *   the sum across every node's slot) and says so — or says it is a floor, when `countsArePartial`
 *   reports that the fan-out missed a node.
 * - **Parked intents** — no endpoint publishes the parked-write queue at all (#360).
 */
function ImposterTiles({
  imposters,
  sources,
  maySeeSources,
  countsArePartial,
}: {
  imposters: readonly Imposter[];
  sources: readonly SourceRecord[] | undefined;
  maySeeSources: boolean;
  /** The fleet sum could not reach every node, so it is a floor rather than a total (#363). */
  countsArePartial: boolean;
}): ReactNode {
  /*
   * `stubCountOf`, not `stubs?.length`. The list projection omits the stub array and sends
   * `stubCount` instead, so reading the array here summed to zero on the one screen this tile is
   * for — the same trap `imposterFields.tsx` documents for the column beside it. `null` means the
   * response carried neither, which is not the same fact as an imposter with no stubs, so the
   * total is only shown when every row answered.
   */
  const counts = imposters.map((imposter) => stubCountOf(imposter));
  const stubTotal = counts.every((n) => n !== null)
    ? counts.reduce((sum: number, n) => sum + (n ?? 0), 0)
    : null;
  /*
   * Same discipline as `stubTotal` above, for the same reason (#363). `numberOfRequests` is
   * optional in the contract, so a row without one has an *unknown* count — not a zero — and
   * adding zero for it would quietly understate the fleet total while looking like an answer.
   * The sum is therefore only shown when every row answered.
   */
  const requestCounts = imposters.map((imposter) => imposter.numberOfRequests);
  const requestTotal = requestCounts.every((n) => n !== undefined)
    ? requestCounts.reduce((sum: number, n) => sum + (n ?? 0), 0)
    : null;
  const drifted = sources?.filter((source) => source.drifted === true).length ?? 0;

  return (
    <dl className="tiles">
      <div className="tile">
        <dt className="eyebrow">Imposters</dt>
        <dd className="v">{imposters.length}</dd>
        <dd className="note">
          {stubTotal === null ? "stub count not in this response" : `${String(stubTotal)} ${stubTotal === 1 ? "stub" : "stubs"}`}
        </dd>
      </div>

      <div className={`tile${countsArePartial ? " is-warn" : ""}`}>
        <dt className="eyebrow">Requests · fleet sum</dt>
        <dd className="v" data-testid="tile-requests">
          {requestTotal === null ? UNKNOWN : requestTotal}
        </dd>
        {/*
          Three different notes for three different facts, because collapsing them is how a floor
          gets read as a total.

          `countsArePartial` is per-response and says the fan-out missed a node, so the number
          under it is a lower bound. It is not a permanent caveat: a complete merge says nothing,
          on the same reasoning the request log's scope strip records — a warning that is always
          on is one nobody reads on the day it means something.
        */}
        <dd className="note">
          {requestTotal === null
            ? "not every imposter in this response carried a count"
            : countsArePartial
              ? "at least this many — a node did not answer in time"
              : "summed across every node"}
        </dd>
      </div>

      {maySeeSources ? (
        <div className={`tile${drifted > 0 ? " is-warn" : ""}`}>
          <dt className="eyebrow">Sources</dt>
          <dd className="v">{sources?.length ?? 0}</dd>
          <dd className="note">
            {drifted === 0 ? "none drifted" : `${String(drifted)} drifted`}
          </dd>
        </div>
      ) : null}

      <div className="tile">
        <dt className="eyebrow">Parked intents</dt>
        <dd className="v-plain">
          <Pending issue={360} reason="The parked-write queue is not published. Under --cluster-admin-async a write can be accepted and replayed later, but no endpoint reports how many are outstanding." />
        </dd>
        <dd className="note">accepted, awaiting replay</dd>
      </div>
    </dl>
  );
}

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
  /*
   * Unwrapped once. The read carries two facts with different scopes (#363) — the rows, and
   * whether the fleet sum on them is complete — so every use below names which one it means.
   */
  const listed = imposters.data?.imposters ?? [];
  const countsArePartial = imposters.data?.partial ?? false;
  const create = useCreateImposter();
  const remove = useDeleteImposter();
  const [creating, setCreating] = useState(false);
  const [importing, setImporting] = useState(false);
  const [exporting, setExporting] = useState(false);
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
  const mayClear = can("requests.clear");
  const existingPorts = listed.flatMap((i) => (i.port === undefined ? [] : [i.port]));

  // ── Filter, sort, selection ───────────────────────────────────────────────
  // The query lives in the URL so a filtered view is linkable and survives a reload; the screen
  // holds no copy of it. `decodeQuery` is total, so a stale bookmark renders rather than throwing.
  const [search, setSearch] = useHashQuery();
  const query = decodeQuery(search);
  const setQuery = (next: typeof query): void => setSearch(encodeQuery(next));

  /*
   * Provenance, joined rather than assumed: every `SourceRecord` carries the `ports` it owns, so the
   * union of those IS the source-owned set. It needs `source.read`, which the imposter list itself
   * does not, so a principal without it never issues the call and is never offered the filter — the
   * same shape as the fleet-health read above.
   */
  const maySeeSources = can("source.read");
  const sources = useSources({ enabled: maySeeSources });
  const sourceOwned = sourceOwnedPorts(sources.data?.sources);
  const drifted = driftedPorts(sources.data?.sources);

  const all = listed;
  const rows = visibleImposters(all, query, sourceOwned, drifted);
  const unclassified = unclassifiedCount(all, query, sourceOwned);

  const [selected, setSelected] = useState<ReadonlySet<number>>(new Set());

  /*
   * A tick is dropped as soon as its imposter leaves the fleet.
   *
   * Selection is a set of bare port numbers, and ports are reused constantly — a source pull, an
   * import, or another operator can recreate one. Without this, ticking 4545, watching it be
   * deleted, and seeing a *different* imposter appear at 4545 leaves the new one silently ticked and
   * one click from a bulk delete the operator never asked for. `effective` intersecting with the
   * visible rows keeps the count honest and is exactly what hides that.
   *
   * Returns `current` unchanged when nothing was pruned, so this cannot loop.
   */
  const livePorts = actionablePorts(all);
  const liveKey = livePorts.join(",");
  useEffect(() => {
    const live = new Set(livePorts);
    setSelected((current) => {
      const next = new Set([...current].filter((port) => live.has(port)));
      return next.size === current.size ? current : next;
    });
    // Keyed on `liveKey`, not `livePorts`: the array is a fresh identity every render and would
    // re-run this on every one.
  }, [liveKey]);
  const clearRequests = useClearRequests();
  const [running, setRunning] = useState<BulkAction | null>(null);
  const [progress, setProgress] = useState<{ completed: number; total: number } | null>(null);
  const [report, setReport] = useState<{ result: BulkResult; verb: string } | null>(null);
  const [confirmingBulk, setConfirmingBulk] = useState<BulkAction | null>(null);
  const batchOwnsOutcome = running !== null || report !== null;

  /*
   * Selection is intersected with what the filter currently shows, every render.
   *
   * The acceptance criterion is that the count shown is the count acted on. Keeping a selection of
   * rows that are no longer visible would break that in the most confusing possible way: narrow the
   * filter, press Delete, and imposters you cannot see disappear. So narrowing the filter narrows
   * the selection, and the number on the bar is always the number of rows on screen that are ticked.
   */
  const visiblePorts = new Set(actionablePorts(rows));
  const effective = actionablePorts(rows).filter((port) => selected.has(port));
  const allVisibleSelected = effective.length > 0 && effective.length === visiblePorts.size;

  const bulkActions: BulkAction[] = [
    ...(mayDelete
      ? [{ key: "delete" as const, label: "Delete", verb: "deleted", destructive: true }]
      : []),
    ...(mayToggle
      ? [
          { key: "enable" as const, label: "Enable", verb: "enabled", destructive: false },
          { key: "disable" as const, label: "Disable", verb: "disabled", destructive: false },
        ]
      : []),
    ...(mayClear
      ? [{ key: "clear" as const, label: "Clear request log", verb: "cleared", destructive: true }]
      : []),
  ];

  // Exhaustive over the narrowed `BulkActionKey`, with no `default`. A fifth action whose case is
  // forgotten is then a compile error rather than a silent fall-through to clearing request logs.
  function callFor(action: BulkAction): (port: number) => Promise<CommitOutcome> {
    switch (action.key) {
      case "delete":
        return (port) => remove.mutateAsync({ port });
      case "enable":
        return (port) => toggle.mutateAsync({ port, enable: true });
      case "disable":
        return (port) => toggle.mutateAsync({ port, enable: false });
      case "clear":
        return (port) => clearRequests.mutateAsync({ port });
    }
  }

  async function runAction(action: BulkAction): Promise<void> {
    const ports = [...effective];
    setRunning(action);
    setProgress({ completed: 0, total: ports.length });
    setReport(null);
    const result = await runBulk(ports, callFor(action), (completed, total) =>
      setProgress({ completed, total }),
    );
    setRunning(null);
    setProgress(null);
    setReport({ result, verb: action.verb });
    /*
     * Only the items that actually landed leave the selection. A refused imposter stays ticked so
     * the operator can retry it without hunting for it again, and a still-committing one stays
     * because nobody has established what happened to it yet.
     */
    const settled = new Set(
      result.results.filter((item) => item.outcome.kind === "done").map((item) => item.port),
    );
    setSelected((current) => new Set([...current].filter((port) => !settled.has(port))));
  }

  return (
    /*
     * The landing shape: the screen, then a fixed fleet rail. The rail is complementary rather
     * than part of the list — it annotates the fleet the imposters live on — so it is an `aside`
     * outside `.screen-main` and after it in the DOM.
     */
    <section className="screen screen-split">
      <div className="screen-main">
        <header className="screen-head">
          <h1>Imposters</h1>
          <p className="scope-label" data-testid="imposters-scope-label">
            Served by this node from replicated state.
            {confidence.partial ? ` This node is degraded: ${confidence.reason}.` : ""}
            {confidence.unknown ? ` Caveat: ${confidence.reason}.` : ""}
          </p>
          <div className="spacer" />
          {/* Export sits with Import and New imposter rather than in a card below the tiles: it is
              an action on this screen's subject, and the header is where this screen's actions are.
              It opens a dialog because the choice it carries — what actually lands in the file —
              needs more than a button label to state. */}
          {mayExport ? (
            <button
              className="btn"
              type="button"
              data-testid="export-imposters"
              onClick={() => setExporting(true)}
            >
              Export
            </button>
          ) : null}
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

        <ImposterTiles
          imposters={all}
          sources={sources.data?.sources}
          maySeeSources={maySeeSources}
          countsArePartial={countsArePartial}
        />

        {mayExport && exporting ? (
        <ExportSetControl tenant={tenant} count={all.length} onClose={() => setExporting(false)} />
      ) : null}

        {importing ? (
          <ImportPanel
            existingPorts={existingPorts}
            existingCount={listed.length}
            mayWrite={mayCreate}
            mayReplace={mayReplace}
            onClose={() => setImporting(false)}
          />
        ) : null}

        {create.isError ? (
          <ErrorNote error={create.error} context="The imposter was not created" />
        ) : null}
        {/*
          Suppressed while a batch owns the outcome. These notes are the SINGLE-row vocabulary: during
          a bulk run they report only the last item, name no port, and sit alongside a per-item report
          that already says more — and they outlive dismissing it.
        */}
        {remove.isError && !batchOwnsOutcome ? (
          <ErrorNote error={remove.error} context="The imposter was not deleted" />
        ) : null}
        {create.data?.kind === "unobservable" ? <UnconfirmedNote reason={create.data.reason} /> : null}
        {remove.data?.kind === "unobservable" && !batchOwnsOutcome ? (
          <UnconfirmedNote reason={remove.data.reason} />
        ) : null}

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
        {toggle.isError && !batchOwnsOutcome ? (
          <ErrorNote error={toggle.error} context="That change did not take effect" />
        ) : null}
        {toggle.data?.kind === "unobservable" && !batchOwnsOutcome ? (
          <UnconfirmedNote reason={toggle.data.reason} />
        ) : null}

        {imposters.isPending ? <p className="muted">Reading…</p> : null}

        {imposters.isSuccess && listed.length === 0 ? (
          <EmptyState
            uncertain={confidence.partial || confidence.unknown}
            reason={confidence.reason}
          />
        ) : null}

        {imposters.isSuccess && listed.length > 0 ? (
          <Card title="Imposters" bleed>
            <ImposterFilters
              query={query}
              onChange={setQuery}
              onReset={() => setQuery(EMPTY_QUERY)}
              shown={rows.length}
              total={all.length}
              unclassified={unclassified}
              showOwner={sourceOwned !== null}
            />

            {bulkActions.length > 0 ? (
              <BulkBar
                count={effective.length}
                actions={bulkActions}
                running={running}
                progress={progress}
                onAct={(action) => setConfirmingBulk(action)}
                onClear={() => setSelected(new Set())}
              />
            ) : null}

            {report === null ? null : (
              <BulkReport
                result={report.result}
                verb={report.verb}
                onDismiss={() => setReport(null)}
              />
            )}

            {rows.length === 0 ? (
              <p className="muted" data-testid="imposters-no-matches">
                No imposter in this tenant matches that filter.
              </p>
            ) : (
              <div className="scroll-x">
                <table className="dense">
                  <thead>
                    <tr>
                      {bulkActions.length > 0 ? (
                        <th className="select-col">
                          {/*
                            Select-all means "everything the current filter shows", which is what the
                            label says out loud. Selecting rows the operator cannot see is the one
                            behaviour a bulk delete must never have.
                          */}
                          <input
                            type="checkbox"
                            data-testid="imposter-select-all"
                            aria-label={`Select all ${visiblePorts.size} shown`}
                            checked={allVisibleSelected}
                            disabled={running !== null || visiblePorts.size === 0}
                            onChange={(event) =>
                              /*
                                Union in, and remove only what is shown — individual ticks accumulate
                                across filter changes, so a select-all that REPLACED the set would
                                silently discard ticks made under a previous filter. The two paths
                                have to agree or the count is a lie in one of them.
                              */
                              setSelected((current) => {
                                const next = new Set(current);
                                for (const port of visiblePorts) {
                                  if (event.target.checked) next.add(port);
                                  else next.delete(port);
                                }
                                return next;
                              })
                            }
                          />
                        </th>
                      ) : null}
                      {IMPOSTER_COLUMNS.map((column) =>
                        column.key === "port" || column.key === "name" || column.key === "stubs" ? (
                          <SortHeader
                            key={column.key}
                            label={column.label}
                            column={column.key}
                            query={query}
                            onChange={setQuery}
                            numeric={column.numeric}
                          />
                        ) : (
                          <th key={column.key} className={column.numeric ? "numeric" : undefined}>
                            {column.label}
                          </th>
                        ),
                      )}
                      {/* The design draws an `Owner` column here. There is no such thing: an
                          imposter has no owner. Imposters, stubs and config are replicated to
                          every node, so every node serves them — only a *flow* is owned, and a
                          port has as many owners as it has flows (#359). The column is gone
                          rather than pending, because a column that can never be filled is a
                          promise, not a roadmap. `Provenance` is a real join this screen already
                          computes for its filter, so it renders. */}
                      <th style={{ width: "16ch" }}>Provenance</th>
                      {mayToggle || mayDelete ? <th aria-label="Actions" /> : null}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((imposter, index) => (
                      <Row
                        key={imposter.port ?? `unnamed-${index}`}
                        imposter={imposter}
                        sourceOwned={sourceOwned}
                        drifted={drifted}
                        mayToggle={mayToggle}
                        mayDelete={mayDelete}
                        busy={toggle.isPending}
                        selectable={bulkActions.length > 0}
                        selected={imposter.port !== undefined && selected.has(imposter.port)}
                        selectionDisabled={running !== null}
                        onSelect={(port, checked) =>
                          setSelected((current) => {
                            const next = new Set(current);
                            if (checked) next.add(port);
                            else next.delete(port);
                            return next;
                          })
                        }
                        onToggle={(port, enable) => toggle.mutate({ port, enable })}
                        onDelete={() => setConfirming(imposter)}
                      />
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Card>
        ) : null}

        {confirmingBulk === null ? null : (
          <Confirm
            testId="confirm-bulk-imposters"
            title={`${confirmingBulk.label} ${effective.length} imposter${effective.length === 1 ? "" : "s"}?`}
            body={
              <>
                {/*
                  The exact count and, for a delete, the exact ports. There is no bulk endpoint — this
                  is one call per imposter, and some may be refused — so the dialog says that here
                  rather than letting a half-applied batch be the first the operator hears of it.
                */}
                This runs one request per imposter. Some may be refused; the batch does not stop at the
                first failure, and every outcome is reported.
                {confirmingBulk.key === "delete" ? (
                  <>
                    {" "}
                    Deleting removes each imposter, its stubs, its recorded requests and its flow state
                    across the fleet. Nothing undoes it. Ports:{" "}
                    <code data-testid="confirm-bulk-ports">{effective.join(", ")}</code>
                  </>
                ) : null}
              </>
            }
            confirmLabel={`${confirmingBulk.label} ${effective.length}`}
            busy={running !== null}
            onCancel={() => setConfirmingBulk(null)}
            onConfirm={() => {
              const action = confirmingBulk;
              setConfirmingBulk(null);
              void runAction(action);
            }}
          />
        )}
      </div>
      <FleetRail fleet={fleet.data} />
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
  selectable,
  selected,
  selectionDisabled,
  onSelect,
  onToggle,
  onDelete,
  sourceOwned,
  drifted,
}: {
  imposter: Imposter;
  mayToggle: boolean;
  mayDelete: boolean;
  busy: boolean;
  selectable: boolean;
  selected: boolean;
  selectionDisabled: boolean;
  onSelect: (port: number, checked: boolean) => void;
  onToggle: (port: number, enable: boolean) => void;
  onDelete: () => void;
  /** `null` when `source.read` was refused or the read has not landed — not "hand-created". */
  sourceOwned: ReadonlySet<number> | null;
  drifted: ReadonlySet<number> | null;
}): ReactNode {
  const port = imposter.port;
  const label = imposter.name ?? (port === undefined ? UNKNOWN : String(port));

  return (
    <tr data-testid={`imposter-row-${port ?? "unnamed"}`}>
      {selectable ? (
        <td className="select-col">
          {/*
            No checkbox for an imposter with no port. Every bulk call is `/imposters/{port}`, so
            there is nothing to send — and `actionablePorts` drops it on the other side, which is
            what keeps the checkbox column and the acted-on set in agreement by construction.
          */}
          {port === undefined ? null : (
            <input
              type="checkbox"
              data-testid={`imposter-select-${port}`}
              aria-label={`Select ${label}`}
              checked={selected}
              disabled={selectionDisabled}
              onChange={(event) => onSelect(port, event.target.checked)}
            />
          )}
        </td>
      ) : null}
      {IMPOSTER_COLUMNS.map((column) => (
        <td
          key={column.key}
          className={column.numeric ? "numeric" : undefined}
          data-testid={`imposter-cell-${column.key}-${port ?? "unnamed"}`}
        >
          <ImposterField imposter={imposter} field={column.key} renderName={nameLink(imposter)} />
        </td>
      ))}
      {/*
        Provenance is real, and the three states are genuinely different facts: a source owns this
        port and has drifted from it, a source owns it cleanly, or nothing declared it. `null` — the
        source read was refused or has not happened — is a fourth, and says so rather than claiming
        the imposter was hand-created.
      */}
      <td className="ident">
        {sourceOwned === null ? (
          <span className="muted">not read</span>
        ) : port !== undefined && drifted?.has(port) === true ? (
          <span className="status status-warn">
            <span className="g" aria-hidden="true">
              &#9650;
            </span>
            drifted
          </span>
        ) : port !== undefined && sourceOwned.has(port) ? (
          "source"
        ) : (
          "hand-created"
        )}
      </td>
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

/**
 * The name cell is the one field the list renders differently: it links through to the detail.
 *
 * It is also the **only** route to that screen — no other cell is clickable — so the absent-name
 * case has to stay linked. `name` is optional on `POST /imposters`, and imported configs and
 * imposter sources routinely omit it, so a nameless imposter is an ordinary thing to have rather
 * than a malformed one. Falling back to a bare `—` here (which is what the shared field renderer
 * does when no render prop is passed) left those rows with nothing to click and no way to reach
 * their stubs, recording panel or export.
 *
 * The port is deliberately not reused as the label — it already has its own column — so the cell
 * says what is actually true of the imposter and the link carries the port for a screen reader.
 */
function nameLink(imposter: Imposter): (name: string | undefined) => ReactNode {
  return (name) => {
    const port = imposter.port;
    const testId = `imposter-name-${port ?? "unnamed"}`;
    const cell =
      name === undefined ? (
        <span className="muted" data-testid={testId}>
          {UNNAMED}
        </span>
      ) : (
        <Truncated value={name} testId={testId} />
      );
    // Still no link without a port: every detail route is `#/imposters/{port}`, so there is
    // nowhere to send them — the same reason the row's checkbox and actions are withheld.
    return port === undefined ? (
      cell
    ) : (
      <a
        href={toHash({ screen: "imposter", port })}
        aria-label={name === undefined ? `Open unnamed imposter on port ${port}` : undefined}
      >
        {cell}
      </a>
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
  const [step, setStep] = useState(0);
  const dialogRef = useRef<HTMLDivElement>(null);
  /*
   * Focus follows the step.
   *
   * Advancing replaces the button that was focused — Next and Create carry distinct keys so React
   * swaps the node rather than mutating it — which left focus on `<body>`, outside the dialog.
   * Escape then reached nothing and a keyboard user was silently ejected mid-decision.
   *
   * An effect rather than a `key` on the dialog: keying it would remount the whole subtree on every
   * step, which is a great deal of churn to move a caret.
   */
  useEffect(() => {
    dialogRef.current?.focus();
  }, [step]);
  // The optional first stub. A predicate-less imposter answers every request with a bare 200, which
  // is a fine thing to want and a surprising thing to get by accident — so the step exists, and
  // skipping it is a choice rather than an omission.
  const [stubMethod, setStubMethod] = useState("GET");
  const [stubPath, setStubPath] = useState("");
  const [stubStatus, setStubStatus] = useState("200");
  const [stubBody, setStubBody] = useState("");

  /*
   * The body this form will POST, built once and used by both the preview and the submit.
   *
   * One builder rather than two, deliberately: a preview assembled separately from the payload is
   * a screenshot of a different request, and the failure is silent — it looks right and sends
   * something else. `null` when the port is not yet a port, which is also what stops the preview
   * rendering a body that could not be sent.
   */
  const parsedPort = Number(port);
  const portOk = Number.isInteger(parsedPort) && parsedPort >= 1 && parsedPort <= 65535;
  const draft: Imposter | null = portOk
    ? {
        port: parsedPort,
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
        /*
         * The first stub, only when a path was given. An empty predicate would match everything,
         * which is the behaviour an imposter already has with no stubs at all — so a blank step
         * adds nothing rather than adding a catch-all nobody asked for.
         */
        ...(stubPath.trim() === ""
          ? {}
          : {
              stubs: [
                {
                  predicates: [
                    { equals: { method: stubMethod, path: stubPath.trim() } },
                  ],
                  responses: [
                    {
                      is: {
                        statusCode: Number(stubStatus) || 200,
                        ...(stubBody.trim() === "" ? {} : { body: stubBody }),
                      },
                    },
                  ],
                },
              ],
            }),
      }
    : null;

  function submit(event: FormEvent): void {
    event.preventDefault();
    // A port is 1–65535 and nothing else. Checked here so an obvious typo is a sentence next to the
    // field rather than a round trip that comes back as a 400 with no field to point at.
    if (draft === null) {
      setInvalid("Port must be a whole number between 1 and 65535.");
      return;
    }
    if (protocol === "https" && (cert.trim() === "" || certKey.trim() === "")) {
      setInvalid("An https imposter needs both a certificate and a key, or it refuses to start.");
      return;
    }
    setInvalid(null);
    onCreate(draft);
  }

  return (
    <div className="scrim" onKeyDown={(event) => { if (event.key === "Escape") onCancel(); }}>
      {/*
        `tabIndex={-1}` and focused on every step change.
        
        Advancing a step replaces the button that was focused (they carry distinct keys, so React
        swaps the node rather than mutating it), which left focus on `<body>` — outside the dialog.
        Escape then reached nothing, and a keyboard user was silently ejected from the modal
        mid-decision. Moving focus to the dialog keeps both the key handler and the user inside it.
      */}
      <div
        className="confirm wizard"
        role="dialog"
        aria-modal="true"
        aria-label="New imposter"
        data-testid="new-imposter-wizard"
        tabIndex={-1}
        ref={dialogRef}
      >
        <header className="wizard-head">
          <div>
            <h2>New imposter</h2>
            <p className="muted">POST /imposters &mdash; a replicated control op</p>
          </div>
          <ol className="wizard-steps">
            {WIZARD_STEPS.map((label, index) => (
              <li
                key={label}
                className={index === step ? "is-current" : index < step ? "is-done" : undefined}
                aria-current={index === step ? "step" : undefined}
              >
                <span className="wizard-dot">{index < step ? "\u2713" : index + 1}</span>
                {label}
              </li>
            ))}
          </ol>
        </header>

      <form className="stub-form wizard-form" onSubmit={submit} data-testid="new-imposter-form">
        <div className="wizard-body">
        {step === 0 ? (
        <>
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
        </>
        ) : null}

        {step === 1 ? (
          <>
            <p className="muted">
              An imposter with no stubs answers every request with a bare 200. Give it one now — the
              rest go in the editor.
            </p>
            <div className="field-row">
              <div className="field">
                <label htmlFor="new-stub-method">Method</label>
                <select
                  id="new-stub-method"
                  value={stubMethod}
                  onChange={(event) => setStubMethod(event.target.value)}
                >
                  {["GET", "POST", "PUT", "PATCH", "DELETE"].map((verb) => (
                    <option key={verb} value={verb}>
                      {verb}
                    </option>
                  ))}
                </select>
              </div>
              <div className="field grow">
                <label htmlFor="new-stub-path">Path</label>
                <input
                  id="new-stub-path"
                  value={stubPath}
                  data-testid="new-stub-path"
                  placeholder="/v1/orders"
                  onChange={(event) => setStubPath(event.target.value)}
                />
              </div>
              <div className="field">
                <label htmlFor="new-stub-status">Status</label>
                <input
                  id="new-stub-status"
                  inputMode="numeric"
                  value={stubStatus}
                  onChange={(event) => setStubStatus(event.target.value)}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="new-stub-body">Body</label>
              <textarea
                id="new-stub-body"
                value={stubBody}
                placeholder='{"ok":true}'
                onChange={(event) => setStubBody(event.target.value)}
              />
            </div>
            <p className="hint">
              Leave the path blank to create the imposter with no stubs at all — which is a real
              choice, not a skipped step: it answers everything with a bare 200 until you add one.
            </p>
          </>
        ) : null}

        {step === 2 ? <ReviewStep draft={draft} /> : null}

        {invalid === null ? null : (
          <p className="error" data-testid="new-imposter-invalid" role="alert">
            {invalid}
          </p>
        )}

        </div>

        <footer className="wizard-foot">
          <span className={invalid === null ? "muted" : "warn-text"}>
            {step === 2
              ? "Writes replicate — every node converges on this imposter."
              : `Step ${String(step + 1)} of ${String(WIZARD_STEPS.length)}`}
          </span>
          <div className="row">
            <button className="btn" type="button" onClick={onCancel} disabled={busy}>
              Cancel
            </button>
            {step > 0 ? (
              <button
                className="btn"
                type="button"
                data-testid="wizard-back"
                onClick={() => setStep(step - 1)}
                disabled={busy}
              >
                Back
              </button>
            ) : null}
            {/*
              Distinct `key`s, and they are load-bearing rather than tidiness.
              
              Without them React reuses one DOM node for both — same position, same element type —
              and only mutates its type and handler. A click already in flight on "Next" then lands
              on "Create imposter" and creates the imposter without the operator ever seeing the
              review step. Keying them apart makes React replace the node, so that click hits a
              detached element and does nothing, which is the correct outcome for a press that was
              aimed at a different control.
            */}
            {step < WIZARD_STEPS.length - 1 ? (
              <button
                key="advance"
                className="btn primary"
                type="button"
                data-testid="wizard-next"
                /* Validating on the way forward rather than only at submit: the port is the one
                   field that can be wrong in a way the later steps depend on, and finding out at
                   the end means re-deciding the stub too. */
                onClick={() => {
                  if (step === 0 && draft === null) {
                    setInvalid("Port must be a whole number between 1 and 65535.");
                    return;
                  }
                  setInvalid(null);
                  setStep(step + 1);
                }}
                disabled={busy}
              >
                Next
              </button>
            ) : (
              <button key="create" className="btn primary" type="submit" disabled={busy}>
                {busy ? "Creating…" : "Create imposter"}
              </button>
            )}
          </div>
        </footer>
      </form>
      </div>
    </div>
  );
}

const WIZARD_STEPS = ["Identity", "First stub", "Review"] as const;

/**
 * The last step: exactly what will be sent, and what the fleet will do with it.
 *
 * The design ends this step with "`Idempotency-Key` is sent with the request, so a retry after a
 * timeout can never double-apply". The console does not send one — the API defines the header and
 * no call site sets it (#371) — so that sentence is not printed. What replaces it is the true
 * version, which is also the more useful one, because it tells an operator what a timeout here
 * actually means for them.
 */
function ReviewStep({ draft }: { draft: Imposter | null }): ReactNode {
  return (
    <div className="wizard-review">
      <div>
        <span className="eyebrow">Request body · POST /imposters</span>
        <pre className="payload" data-testid="new-imposter-preview">
          {draft === null ? "// a valid port is needed first" : JSON.stringify(draft, null, 2)}
        </pre>
      </div>
      <div className="wizard-aside">
        <div className="card">
          <div className="card-body">
            <span className="eyebrow">What happens on create</span>
            <ol className="wizard-happens">
              <li>
                <b>Submitted on this node</b>
                <span>durably parked before it is forwarded</span>
              </li>
              <li>
                <b>Forwarded to the leader</b>
                <span>committed on a majority of voters</span>
              </li>
              <li>
                <b>Applied fleet-wide</b>
                <span>the write resolves once the fleet has it, not when this node accepts it</span>
              </li>
              <li>
                <b>Bound on each node</b>
                <span>a node that cannot bind still serves it through the front door</span>
              </li>
            </ol>
          </div>
        </div>
        <p className="hint">
          If this request times out, the write may still have landed. The console sends no
          idempotency key (#371), so re-submitting is a second operation rather than a retry of the
          first — reload the list before creating it again.
        </p>
      </div>
    </div>
  );
}

/**
 * Export the whole tenant, in either projection (#251).
 *
 * Two buttons, not a select-then-go: the projections carry a real semantic difference (whether the
 * import goes on recording), and naming both up front is worth more than one fewer click.
 */
function ExportSetControl({
  tenant,
  count,
  onClose,
}: {
  tenant: string | null;
  count: number;
  onClose: () => void;
}): ReactNode {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const toast = useToast();

  async function run(options: ExportOptions): Promise<void> {
    setError(null);
    setBusy(true);
    try {
      const text = await apiGetText(`/imposters${exportOptionsQuery(options)}`, { tenant });
      const filename = exportSetFilename(tenant);
      downloadText(filename, text);
      // A download is the one action with no on-screen consequence at all: the dialog closes and the
      // file lands somewhere the console cannot see. Saying so is the whole point of the toast.
      toast({ tone: "good", message: `Exported ${String(count)} imposters`, meta: filename });
      onClose();
    } catch (error_) {
      setError(errorText(error_));
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <ExportDialog
        scope={{ kind: "tenant" }}
        tenant={tenant}
        imposterCount={count}
        busy={busy}
        onExport={(options) => void run(options)}
        onCancel={onClose}
      />
      {/* Kept outside the dialog on purpose: a failed export leaves the dialog open with the
          options the operator chose still in it, so retrying does not mean re-deciding. */}
      {error === null ? null : (
        <p className="error" data-testid="export-imposters-error" role="alert">
          {error}
        </p>
      )}
    </>
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
          // The widest act the console offers: every imposter in the tenant goes, replaced by a
          // document. Typing the count is the difference between meaning it and having clicked
          // through this dialog before.
          requireTyped={String(existingCount)}
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
