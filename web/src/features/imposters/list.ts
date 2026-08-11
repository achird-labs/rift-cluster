import type { components } from "../../api/schema.ts";
import type { FleetView } from "../../app/fleetView.ts";
import { bindVerdict } from "../../app/fleetView.ts";
import { recordingState } from "../recording/state.ts";

type Imposter = components["schemas"]["Imposter"];

/**
 * Finding, narrowing and ordering a list this console already holds.
 *
 * Client-side by construction: `GET /imposters` returns the tenant's whole set and the screen has it
 * in hand, so a server-side query parameter would add a round trip, a second source of truth for
 * "what is in this list", and a reason for the filtered count to disagree with the rendered rows.
 *
 * Everything here is a pure function over that list. The screen owns the state and the URL; this
 * module owns the meaning of it.
 */

/** How a row's stub list answers "does this imposter have a recording?". */
export type RecordingFilter = "all" | "has" | "none";
export type StateFilter = "all" | "enabled" | "disabled";
export type OwnerFilter = "all" | "source" | "hand";
export type SortKey = "port" | "name" | "stubs";
export type SortDirection = "asc" | "desc";

export type DriftFilter = "all" | "drifted";

/**
 * Whether an imposter's bind status (#369) fails on at least one voter.
 *
 * Only "failed", not "bound"/"unknown" as separate values: `bindVerdict` already reduces the whole
 * fleet to one of three answers, and "show me the ones with a problem" is the one question a quick
 * filter needs to answer in one click. An operator wanting the finer distinction reads the imposter's
 * own Ownership tab.
 */
export type BindFilter = "all" | "failed";

export type ImposterQuery = {
  text: string;
  state: StateFilter;
  recording: RecordingFilter;
  /**
   * Source-owned vs hand-created. Decided by joining `GET /admin/sources` — every `SourceRecord`
   * carries the `ports` it currently owns, so the union of those ports IS the source-owned set.
   *
   * The join needs a capability the list itself does not (`source.read`), so a principal without it
   * is not offered the filter at all rather than shown one that silently answers "hand-created" for
   * everything. `sourceOwned` being `null` — refused, unread, or still loading — means exactly that.
   */
  owner: OwnerFilter;
  /**
   * Imposters whose owning source has drifted — hand-edited since its last pull.
   *
   * A separate dimension from `owner` rather than a third value of it, because they answer
   * different questions and an operator wants both at once: `owner: "source"` is "who created
   * this", `drifted: "drifted"` is "and has someone edited it behind the source's back". Folding
   * them together would make "source-owned AND drifted" unaskable.
   */
  drifted: DriftFilter;
  /** #369 — see `BindFilter`. */
  bind: BindFilter;
  sort: SortKey;
  direction: SortDirection;
};

export const EMPTY_QUERY: ImposterQuery = {
  text: "",
  state: "all",
  recording: "all",
  owner: "all",
  drifted: "all",
  bind: "all",
  sort: "port",
  direction: "asc",
};

export function isEmptyQuery(query: ImposterQuery): boolean {
  return (
    query.text === EMPTY_QUERY.text &&
    query.state === EMPTY_QUERY.state &&
    query.recording === EMPTY_QUERY.recording &&
    query.owner === EMPTY_QUERY.owner &&
    query.drifted === EMPTY_QUERY.drifted &&
    query.bind === EMPTY_QUERY.bind &&
    query.sort === EMPTY_QUERY.sort &&
    query.direction === EMPTY_QUERY.direction
  );
}

/**
 * The query as a URL query string, and back.
 *
 * Only non-default fields are written, so the default view's URL carries no query string at all —
 * a filtered view is linkable, and an unfiltered one is the plain `#/imposters` it always was.
 *
 * Parsing is total: every unrecognised value falls back to the default rather than throwing or
 * producing a state the UI cannot render. A hand-edited or stale bookmark is a normal thing to
 * receive here, and the rest of this module already treats an unparseable hash that way.
 */
export function encodeQuery(query: ImposterQuery): string {
  const params = new URLSearchParams();
  if (query.text.trim() !== "") params.set("q", query.text);
  if (query.state !== EMPTY_QUERY.state) params.set("state", query.state);
  if (query.recording !== EMPTY_QUERY.recording) params.set("rec", query.recording);
  if (query.owner !== EMPTY_QUERY.owner) params.set("owner", query.owner);
  if (query.drifted !== EMPTY_QUERY.drifted) params.set("drifted", query.drifted);
  if (query.bind !== EMPTY_QUERY.bind) params.set("bind", query.bind);
  if (query.sort !== EMPTY_QUERY.sort) params.set("sort", query.sort);
  if (query.direction !== EMPTY_QUERY.direction) params.set("dir", query.direction);
  return params.toString();
}

function oneOf<T extends string>(raw: string | null, allowed: readonly T[], fallback: T): T {
  return allowed.includes(raw as T) ? (raw as T) : fallback;
}

export function decodeQuery(search: string): ImposterQuery {
  const params = new URLSearchParams(search);
  return {
    text: params.get("q") ?? EMPTY_QUERY.text,
    state: oneOf(params.get("state"), ["all", "enabled", "disabled"], EMPTY_QUERY.state),
    recording: oneOf(params.get("rec"), ["all", "has", "none"], EMPTY_QUERY.recording),
    owner: oneOf(params.get("owner"), ["all", "source", "hand"], EMPTY_QUERY.owner),
    drifted: oneOf(params.get("drifted"), ["all", "drifted"], EMPTY_QUERY.drifted),
    bind: oneOf(params.get("bind"), ["all", "failed"], EMPTY_QUERY.bind),
    sort: oneOf(params.get("sort"), ["port", "name", "stubs"], EMPTY_QUERY.sort),
    direction: oneOf(params.get("dir"), ["asc", "desc"], EMPTY_QUERY.direction),
  };
}

/**
 * Whether an imposter's stub list was in the response at all.
 *
 * `stubs: undefined` is "this response did not include them", which is a different fact from an
 * imposter with zero stubs — `imposterFields.tsx` already renders the two differently, and the
 * distinction matters more here than there. A filter that silently folded unknown into "none" would
 * quietly hide rows from an operator who asked to see everything *without* a recording, and the
 * hiding would be invisible: the row simply is not there to notice.
 *
 * So unknown is its own answer, and `classifyRecording` returns it rather than guessing.
 */
export type RecordingClass = "has" | "none" | "unknown";

export function classifyRecording(imposter: Imposter): RecordingClass {
  if (imposter.stubs === undefined) return "unknown";
  return recordingState(imposter.stubs) === "recording" ? "has" : "none";
}

/** The number of stubs, or `null` when the response did not carry them. Never coerced to 0. */
export function stubCount(imposter: Imposter): number | null {
  return imposter.stubs === undefined ? null : imposter.stubs.length;
}

/**
 * Free-text match over name, port and protocol.
 *
 * Case-insensitive and substring, not prefix: an operator hunting `checkout-api` types "checkout",
 * and one hunting port 4545 types "45". Matching the port as a *string* is deliberate — "45" finding
 * 4545 and 14500 is the behaviour of every filter box anyone has used, and a numeric-equality match
 * would answer nothing for the partial input the box is being typed into.
 */
export function matchesText(imposter: Imposter, text: string): boolean {
  const needle = text.trim().toLowerCase();
  if (needle === "") return true;
  const haystacks = [
    imposter.name,
    imposter.port === undefined ? undefined : String(imposter.port),
    imposter.protocol,
  ];
  return haystacks.some(
    (value) => value !== undefined && value.toLowerCase().includes(needle),
  );
}

function matchesState(imposter: Imposter, filter: StateFilter): boolean {
  if (filter === "all") return true;
  return filter === "enabled" ? imposter.enabled : !imposter.enabled;
}

function matchesRecording(imposter: Imposter, filter: RecordingFilter): boolean {
  if (filter === "all") return true;
  return classifyRecording(imposter) === filter;
}

/**
 * `sourceOwned` is the union of every declared source's `ports`, or `null` when this session has no
 * reading of them. `null` makes the owner filter a no-op rather than a wrong answer: with nothing to
 * join against, "hand-created" would match every imposter including the source-owned ones.
 */
function matchesOwner(
  imposter: Imposter,
  filter: OwnerFilter,
  sourceOwned: ReadonlySet<number> | null,
): boolean {
  if (filter === "all" || sourceOwned === null) return true;
  const owned = imposter.port !== undefined && sourceOwned.has(imposter.port);
  return filter === "source" ? owned : !owned;
}

/**
 * `fleet` is `null` for exactly the reasons `sourceOwned` is: refused (`fleet.read` withheld), not
 * yet loaded, or the caller does not have one to offer. A port with no fleet reading to check is
 * `unknown`, never `failed` — the same `driftedPorts === null → false` rule `visibleImposters`
 * documents further down, chosen for the same reason: an operator asking for failures must never
 * see "none" stand in for "could not check", nor "everything" stand in for it either.
 */
function matchesBind(imposter: Imposter, filter: BindFilter, fleet: FleetView | null): boolean {
  if (filter === "all") return true;
  if (fleet === null || imposter.port === undefined) return false;
  return bindVerdict(fleet, imposter.port) === "failed";
}

export function filterImposters(
  imposters: readonly Imposter[],
  query: ImposterQuery,
  sourceOwned: ReadonlySet<number> | null = null,
  fleet: FleetView | null = null,
): Imposter[] {
  return imposters.filter(
    (imposter) =>
      matchesText(imposter, query.text) &&
      matchesState(imposter, query.state) &&
      matchesRecording(imposter, query.recording) &&
      matchesOwner(imposter, query.owner, sourceOwned) &&
      matchesBind(imposter, query.bind, fleet),
  );
}

/** The ports every declared source currently owns — the join the owner filter is built on. */
export function sourceOwnedPorts(
  sources: readonly { ports: number[] }[] | undefined,
): ReadonlySet<number> | null {
  return sources === undefined ? null : new Set(sources.flatMap((source) => source.ports));
}

/**
 * How many rows a recording filter could not classify, because their stubs were not in the response.
 *
 * The screen says this out loud next to the filter. A count of rows that are *neither* shown nor
 * excluded-for-a-reason-you-asked-for is exactly the kind of quiet omission that makes a list
 * untrustworthy, and it costs one sentence to be honest about instead.
 */
export function unclassifiedCount(
  imposters: readonly Imposter[],
  query: ImposterQuery,
  sourceOwned: ReadonlySet<number> | null = null,
  fleet: FleetView | null = null,
): number {
  if (query.recording === "all") return 0;
  /*
   * Every other filter first, then the unknowns among what survives.
   *
   * Expressed as "filter with recording disabled, then count the unknowns" rather than by repeating
   * the conjunction: an earlier version repeated it and omitted `owner`, so a row excluded because
   * the operator asked for hand-created only was reported as "not shown because we could not read
   * its stubs" — the count that exists to name the right reason, naming the wrong one. Deriving it
   * from `filterImposters` means a filter added later cannot be forgotten here.
   */
  return filterImposters(imposters, { ...query, recording: "all" }, sourceOwned, fleet).filter(
    (imposter) => classifyRecording(imposter) === "unknown",
  ).length;
}

/**
 * How many rows the bind-failures filter could not classify (#369), because their verdict is
 * `"unknown"` rather than a real "no failure".
 *
 * Same shape as `unclassifiedCount` and for the same reason: `bind: "failed"` excludes a row whose
 * `bindVerdict` is `"unknown"` exactly as it excludes one that is genuinely `"bound"`, and those are
 * different facts an operator deserves to be told apart. Silently folding "could not confirm" into
 * "not failing" is the one thing a bind-status filter must never do.
 *
 * `fleet === null` is not folded into "nothing to report" (blocker B3). `query.bind` is decoded
 * from the URL, so `?bind=failed` is reachable from a shared link by a session that cannot read the
 * fleet — `matchesBind` then excludes every row (no fleet reading to check any of them against),
 * and returning `0` here on top of that would render as "0 of N: nothing is failing", the exact
 * "checked and found healthy" claim this session never earned. So when there is no fleet reading at
 * all, every row the *other* filters admit is unclassified — not just the ones whose fleet-derived
 * verdict says so, because there is no such verdict to consult.
 */
export function bindUnclassifiedCount(
  imposters: readonly Imposter[],
  query: ImposterQuery,
  sourceOwned: ReadonlySet<number> | null = null,
  fleet: FleetView | null = null,
): number {
  if (query.bind === "all") return 0;
  const admitted = filterImposters(imposters, { ...query, bind: "all" }, sourceOwned, fleet);
  if (fleet === null) return admitted.length;
  return admitted.filter(
    (imposter) => imposter.port !== undefined && bindVerdict(fleet, imposter.port) === "unknown",
  ).length;
}

/**
 * Sort, with absent values last in BOTH directions.
 *
 * Reversing a comparator normally reverses everything, which would float the rows that have no value
 * to the top of a descending sort — "the imposters we know least about" is never what someone asked
 * for by clicking a column header. So absence is handled before direction, not by it.
 */
function compareOptional<T>(
  a: T | null | undefined,
  b: T | null | undefined,
  compare: (x: T, y: T) => number,
  direction: SortDirection,
): number {
  const aMissing = a === null || a === undefined;
  const bMissing = b === null || b === undefined;
  if (aMissing && bMissing) return 0;
  if (aMissing) return 1;
  if (bMissing) return -1;
  const ordered = compare(a, b);
  return direction === "asc" ? ordered : -ordered;
}

export function sortImposters(
  imposters: readonly Imposter[],
  sort: SortKey,
  direction: SortDirection,
): Imposter[] {
  // A copy: `Array.prototype.sort` is in-place, and the input is React Query's cached array.
  return [...imposters].sort((a, b) => {
    switch (sort) {
      case "port":
        return compareOptional(a.port, b.port, (x, y) => x - y, direction);
      case "name":
        return compareOptional(
          a.name,
          b.name,
          (x, y) => x.localeCompare(y, undefined, { numeric: true, sensitivity: "base" }),
          direction,
        );
      case "stubs":
        return compareOptional(stubCount(a), stubCount(b), (x, y) => x - y, direction);
    }
  });
}

/** Filter then sort — the order the screen renders. */
export function visibleImposters(
  imposters: readonly Imposter[],
  query: ImposterQuery,
  sourceOwned: ReadonlySet<number> | null = null,
  driftedPorts: ReadonlySet<number> | null = null,
  fleet: FleetView | null = null,
): Imposter[] {
  const matched = filterImposters(imposters, query, sourceOwned, fleet).filter((imposter) => {
    if (query.drifted === "all") return true;
    /*
     * `null` is "the drift set could not be read" — the same shape `sourceOwned` uses — and it
     * matches nothing rather than everything. Answering "all of them are drifted" for a principal
     * refused `source.read` would be the loudest possible wrong answer.
     */
    if (driftedPorts === null) return false;
    return imposter.port !== undefined && driftedPorts.has(imposter.port);
  });
  return sortImposters(matched, query.sort, query.direction);
}

/**
 * The ports owned by a source that has drifted.
 *
 * Same join as `sourceOwnedPorts`, narrowed to the sources reporting `drifted` — so it inherits the
 * same `null` meaning: the read was refused or has not happened, which is not the same fact as no
 * source having drifted.
 */
export function driftedPorts(
  sources: readonly { ports?: number[]; drifted?: boolean }[] | undefined,
): ReadonlySet<number> | null {
  if (sources === undefined) return null;
  return new Set(sources.filter((source) => source.drifted === true).flatMap((s) => s.ports ?? []));
}

/**
 * The ports a bulk action would act on.
 *
 * An imposter with no port cannot be addressed by any of the four calls — every one of them is
 * `/imposters/{port}` — so it is excluded here rather than at each call site. The screen never offers
 * it a checkbox either, so the two agree by construction.
 */
export function actionablePorts(imposters: readonly Imposter[]): number[] {
  return imposters.flatMap((imposter) => (imposter.port === undefined ? [] : [imposter.port]));
}
