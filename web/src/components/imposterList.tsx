import type { ReactNode } from "react";

import type { BulkResult } from "../features/imposters/bulk.ts";
import { Pending } from "./pending.tsx";
import { summaryLine } from "../features/imposters/bulk.ts";
import type {
  ImposterQuery,
  OwnerFilter,
  RecordingFilter,
  SortDirection,
  SortKey,
  StateFilter,
} from "../features/imposters/list.ts";

/**
 * The controls above the imposter list, and the bar that appears with a selection.
 *
 * Split out of `Imposters.tsx` because that screen already carries the create form, the import
 * planner and the export path; the list's own controls are a separable thing with no dependency on
 * any of them. Every decision about *what* the filter means lives in `features/imposters/list.ts` —
 * this file only renders it and reports back.
 */

const STATE_OPTIONS: readonly { value: StateFilter; label: string }[] = [
  { value: "all", label: "Any state" },
  { value: "enabled", label: "Enabled" },
  { value: "disabled", label: "Disabled" },
];

const RECORDING_OPTIONS: readonly { value: RecordingFilter; label: string }[] = [
  { value: "all", label: "Any recording" },
  { value: "has", label: "Recording" },
  { value: "none", label: "Not recording" },
];

const OWNER_OPTIONS: readonly { value: OwnerFilter; label: string }[] = [
  { value: "all", label: "Any origin" },
  { value: "source", label: "Source-owned" },
  { value: "hand", label: "Hand-created" },
];

export function ImposterFilters({
  query,
  onChange,
  onReset,
  shown,
  total,
  unclassified,
  showOwner,
  bindFilterAvailable,
  bindUnclassified,
}: {
  query: ImposterQuery;
  onChange: (query: ImposterQuery) => void;
  onReset: () => void;
  shown: number;
  total: number;
  unclassified: number;
  /**
   * Offered only when this session actually has a reading of `GET /admin/sources` to join against.
   * Without one the filter could only answer "hand-created" for everything, which is a wrong answer
   * wearing the clothes of a real one.
   */
  showOwner: boolean;
  /**
   * Whether this session holds `fleet.read` (#369). Without it there is no bind reading to filter
   * on, so the pill stays in its pending form rather than silently filtering nothing — the same
   * reasoning `showOwner` above follows for the provenance filter.
   */
  bindFilterAvailable: boolean;
  /** Rows the bind filter excluded because their verdict is `"unknown"`, not because it is clean. */
  bindUnclassified: number;
}): ReactNode {
  const filtered = shown !== total;

  return (
    <div className="row filters" data-testid="imposter-filters">
      <label className="field">
        <span className="visually-hidden">Filter imposters</span>
        <input
          type="search"
          data-testid="imposter-filter-text"
          placeholder="Filter by name, port or protocol"
          value={query.text}
          onChange={(event) => onChange({ ...query, text: event.target.value })}
        />
      </label>

      <label className="field">
        <span className="visually-hidden">Filter by state</span>
        <select
          data-testid="imposter-filter-state"
          value={query.state}
          onChange={(event) => onChange({ ...query, state: event.target.value as StateFilter })}
        >
          {STATE_OPTIONS.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </label>

      <label className="field">
        <span className="visually-hidden">Filter by recording</span>
        <select
          data-testid="imposter-filter-recording"
          value={query.recording}
          onChange={(event) =>
            onChange({ ...query, recording: event.target.value as RecordingFilter })
          }
        >
          {RECORDING_OPTIONS.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </label>

      {showOwner ? (
        <label className="field">
          <span className="visually-hidden">Filter by origin</span>
          <select
            data-testid="imposter-filter-owner"
            value={query.owner}
            onChange={(event) => onChange({ ...query, owner: event.target.value as OwnerFilter })}
          >
            {OWNER_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
      ) : null}

      <div className="spacer" />

      {/*
        The design's quick filters, over the same query the controls above write.
        
        Not a replacement for them: the selects can express combinations these cannot, and the text
        box is what makes 200 imposters navigable. These are the three questions asked often enough
        to deserve one click — and `aria-pressed` carries the state, so the fill is not the only
        signal.
      */}
      <div className="pill-filters" role="group" aria-label="Quick filters">
        <button
          type="button"
          className="pill-filter"
          data-testid="quick-all"
          aria-pressed={query.state === "all" && query.drifted === "all"}
          onClick={() => onChange({ ...query, state: "all", drifted: "all" })}
        >
          All
        </button>
        <button
          type="button"
          className="pill-filter"
          data-testid="quick-drifted"
          aria-pressed={query.drifted === "drifted"}
          onClick={() =>
            onChange({ ...query, drifted: query.drifted === "drifted" ? "all" : "drifted" })
          }
        >
          Drifted
        </button>
        <button
          type="button"
          className="pill-filter"
          data-testid="quick-paused"
          aria-pressed={query.state === "disabled"}
          onClick={() =>
            onChange({ ...query, state: query.state === "disabled" ? "all" : "disabled" })
          }
        >
          Paused
        </button>
        {/* The design's fourth pill (#369). Real once this session holds `fleet.read` — without it
            there is no bind reading to filter on, and the pill says so rather than filtering
            nothing, the same rule `showOwner` follows for provenance above. */}
        {bindFilterAvailable ? (
          <button
            type="button"
            className="pill-filter"
            data-testid="quick-bind-failures"
            aria-pressed={query.bind === "failed"}
            onClick={() =>
              onChange({ ...query, bind: query.bind === "failed" ? "all" : "failed" })
            }
          >
            Bind failures
          </button>
        ) : (
          <span className="pill-filter is-pending">
            Bind failures{" "}
            <Pending
              issue={369}
              reason="The fleet projection is out of scope for this principal, so the console cannot tell which imposters failed to bind anywhere."
            />
          </span>
        )}
      </div>

      <span className="muted" data-testid="imposter-filter-count" aria-live="polite">
        {filtered ? `${shown} of ${total}` : `${total} imposter${total === 1 ? "" : "s"}`}
      </span>

      {filtered ? (
        <button className="btn sm" type="button" data-testid="imposter-filter-reset" onClick={onReset}>
          Clear filters
        </button>
      ) : null}

      {/*
        Rows the recording filter could not classify, said out loud. `GET /imposters` is not
        guaranteed to carry each imposter's stubs, and "has a recording" is derived from them — so
        a row whose stubs are absent is excluded from BOTH answers. Excluding it silently would
        make the list quietly incomplete in exactly the way a filter is trusted not to be.
      */}
      {unclassified > 0 ? (
        <span className="warn-text" data-testid="imposter-filter-unclassified">
          {unclassified} not shown: this node’s list did not include their stubs, so whether they
          are recording is unknown.
        </span>
      ) : null}

      {/*
        Same idiom as the recording filter's note above, for the bind filter (#369). A row whose
        `bindVerdict` is `"unknown"` is excluded from "Bind failures" exactly as a genuinely bound
        one is — the two must not be told apart by silence.
      */}
      {bindUnclassified > 0 ? (
        <span className="warn-text" data-testid="imposter-filter-bind-unclassified">
          {bindUnclassified} not shown: bind status could not be confirmed for every voter, so
          whether they are failing is unknown.
        </span>
      ) : null}
    </div>
  );
}

/** A column header that sorts. Clicking the active column flips direction. */
export function SortHeader({
  label,
  column,
  query,
  onChange,
  numeric,
}: {
  label: string;
  column: SortKey;
  query: ImposterQuery;
  onChange: (query: ImposterQuery) => void;
  numeric: boolean;
}): ReactNode {
  const active = query.sort === column;
  const direction: SortDirection = active && query.direction === "asc" ? "desc" : "asc";

  return (
    <th
      className={numeric ? "numeric" : undefined}
      // `aria-sort` is what makes the current order audible rather than only visible — the
      // arrow glyph below says it to sighted users and nothing to anyone else.
      aria-sort={active ? (query.direction === "asc" ? "ascending" : "descending") : "none"}
    >
      <button
        className="btn-link"
        type="button"
        data-testid={`imposter-sort-${column}`}
        onClick={() => onChange({ ...query, sort: column, direction })}
      >
        {label}
        <span aria-hidden="true">{active ? (query.direction === "asc" ? " ▲" : " ▼") : ""}</span>
      </button>
    </th>
  );
}

export type BulkActionKey = "delete" | "enable" | "disable" | "clear";

export type BulkAction = {
  key: BulkActionKey;
  label: string;
  /** Past tense, for the result line: "17 deleted". */
  verb: string;
  destructive: boolean;
};

export function BulkBar({
  count,
  actions,
  running,
  progress,
  onAct,
  onClear,
}: {
  count: number;
  actions: readonly BulkAction[];
  running: BulkAction | null;
  progress: { completed: number; total: number } | null;
  onAct: (action: BulkAction) => void;
  onClear: () => void;
}): ReactNode {
  if (count === 0) return null;

  return (
    <div className="row bulk-bar" data-testid="imposter-bulk-bar" role="group" aria-label="Bulk actions">
      <strong data-testid="imposter-bulk-count">
        {count} selected
      </strong>
      <div className="spacer" />

      {running !== null && progress !== null ? (
        <span className="muted" data-testid="imposter-bulk-progress" aria-live="polite">
          {running.label}: {progress.completed} of {progress.total}
        </span>
      ) : (
        actions.map((action) => (
          <button
            key={action.key}
            className={action.destructive ? "btn sm danger" : "btn sm"}
            type="button"
            data-testid={`imposter-bulk-${action.key}`}
            onClick={() => onAct(action)}
          >
            {action.label}
          </button>
        ))
      )}

      <button
        className="btn sm"
        type="button"
        data-testid="imposter-bulk-clear"
        disabled={running !== null}
        onClick={onClear}
      >
        Clear selection
      </button>
    </div>
  );
}

/**
 * What a finished batch actually did — per item, never as one green toast.
 *
 * The refusals and the still-committing writes are listed individually with their port and the
 * fleet's own words. A summary alone would let an operator close this having read "17 deleted" and
 * never learn which three are still there.
 */
export function BulkReport({
  result,
  verb,
  onDismiss,
}: {
  result: BulkResult;
  verb: string;
  onDismiss: () => void;
}): ReactNode {
  /*
   * Typed as the narrowed union, not filtered into one. `Array.prototype.filter` does not narrow,
   * and widening it back with a cast would let a future `done` item reach a branch that reads
   * `.reason` off it.
   */
  const notable = result.results.flatMap((item) =>
    item.outcome.kind === "done" ? [] : [{ port: item.port, outcome: item.outcome }],
  );

  return (
    <div className="bulk-report" data-testid="imposter-bulk-report" role="status">
      <div className="row">
        <strong className={result.refused > 0 ? "error" : undefined} data-testid="imposter-bulk-summary">
          {summaryLine(result, verb)}
        </strong>
        <div className="spacer" />
        <button className="btn sm" type="button" data-testid="imposter-bulk-dismiss" onClick={onDismiss}>
          Dismiss
        </button>
      </div>
      {notable.length > 0 ? (
        <ul className="bulk-report-list">
          {notable.map((item) => (
            <li key={item.port} data-testid={`imposter-bulk-item-${item.port}`}>
              <code>{item.port}</code>{" "}
              {item.outcome.kind === "refused" ? (
                <>refused — {item.outcome.detail}</>
              ) : (
                <>still committing — {item.outcome.reason}</>
              )}
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
