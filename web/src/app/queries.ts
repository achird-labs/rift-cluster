import { useMutation, useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import type { UseMutationResult, UseQueryResult } from "@tanstack/react-query";

import {
  ApiError,
  RawJsonBody,
  type RevisionedRead,
  type SendResult,
  apiGet,
  apiGetMerged,
  apiGetWithRevision,
  apiSend,
} from "../api/client.ts";
import { type CommitOutcome, applied, settle } from "../features/writes/commit.ts";
import {
  API_PATHS,
  auditPath,
  bindingPath,
  frontDoorRoutePath,
  imposterPath,
  lifecyclePath,
  flowStateEntryPath,
  flowStatePath,
  principalPath,
  principalsPath,
  recordedStubsPath,
  requestsPath,
  savedProxyResponsesPath,
  scenarioStatePath,
  scenariosPath,
  scenariosResetPath,
  spacePath,
  spaceStubsPath,
  stubByIdPath,
  stubsPath,
  tenantPath,
  tryImposterPath,
} from "../api/paths.ts";
import type { components } from "../api/schema.ts";
import { type AuditRow, auditPage, readAuditRows } from "../features/admin/audit.ts";
import { stripApiKey } from "../features/admin/key.ts";
import {
  type Coverage,
  type RecordedRequest,
  coverageFor,
  readLog,
} from "../features/requests/source.ts";
import {
  type FlowStateRead,
  type ScenarioState,
  type SpaceState,
  readFlowStateEntry,
  readScenarios,
  readSpace,
} from "../features/scenarios/space.ts";
import { type Route, normalizeTable } from "../features/routes/order.ts";
import { type FleetView, fleetView } from "./fleetView.ts";
import { POLLED, POLLED_REQUESTS } from "./query.ts";
import { useSession } from "./session.tsx";

type Imposter = components["schemas"]["Imposter"];
type Stub = components["schemas"]["Stub"];
/** The try envelope and its answer, both straight off the contract (#335). */
export type TrySpec = components["schemas"]["TryRequest"];
export type TryResult = components["schemas"]["TryResponse"];
type FleetMembers = components["schemas"]["FleetMembers"];
type FleetHealth = components["schemas"]["FleetHealth"];
type RouteTable = components["schemas"]["RouteTable"];
type Tenant = components["schemas"]["Tenant"];
type TenantWrite = components["schemas"]["TenantWrite"];
type Principal = components["schemas"]["Principal"];
type AuditSink = components["schemas"]["AuditSink"];
type AuditSinkWrite = components["schemas"]["AuditSinkWrite"];
type PrincipalCreate = components["schemas"]["PrincipalCreate"];
type PrincipalUpdate = components["schemas"]["PrincipalUpdate"];
type IssuedPrincipal = components["schemas"]["IssuedPrincipal"];
type Role = components["schemas"]["Role"];
type SourceRecord = components["schemas"]["SourceRecord"];
type SourcesNodeLocal = components["schemas"]["SourcesNodeLocal"];
type FleetRequestPage = components["schemas"]["FleetRequestPage"];
export type FleetJournalCoverage = components["schemas"]["FleetJournalCoverage"];

/**
 * The tenant is part of every query key, not just the request headers.
 *
 * Without it, switching tenants would show the previous tenant's imposters from cache until the
 * refetch landed — one tenant's data rendered under another tenant's name, which is the worst
 * possible way to be briefly wrong in a multi-tenant console.
 */
function key(parts: readonly unknown[], tenant: string | null): unknown[] {
  return [...parts, { tenant }];
}

/**
 * The tenant's imposters, and whether the fleet sum on them is complete.
 *
 * `partial` is carried rather than dropped because `numberOfRequests` is a **fleet** figure
 * (issue #363): the front rewrites each entry's count to the sum across every node's slot for that
 * port, and stamps `Rift-Cluster-Partial` when a peer could not be reached inside the fan-out
 * budget. The sum is then a floor, not a total — and a floor presented as a total is the reading an
 * operator would act on.
 *
 * Shaped like `useSources` rather than merged into the array: the two facts have different scopes,
 * and a caller that does not care about coverage should have to ignore it explicitly rather than
 * never learn it exists.
 */
export type ImposterList = { imposters: Imposter[]; partial: boolean };

export function useImposters(): UseQueryResult<ImposterList> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["imposters"], tenant),
    queryFn: async (): Promise<ImposterList> => {
      const read = await apiGetMerged<{ imposters?: Imposter[] }>(API_PATHS.imposters, { tenant });
      // `imposters` is optional in the contract, so an absent array is a shape the schema permits —
      // a domain-optional read, not a swallowed failure. A non-2xx has already thrown in `client`.
      return { imposters: read.data.imposters ?? [], partial: read.partial };
    },
    ...POLLED,
  });
}

/**
 * The tenant's declared imposter sources, plus this node's own poll status.
 *
 * The two halves are read together, in one round trip, and kept apart in the return type rather
 * than merged: `sources` is the fleet-replicated projection, `nodeLocal` is true of the answering
 * node only. Flattening a poll error onto its source's record would render one node's transient
 * failure as a fleet-wide fact — see `Sources.tsx`.
 */
export function useSources(options: { enabled?: boolean } = {}): UseQueryResult<{
  sources: SourceRecord[];
  nodeLocal: SourcesNodeLocal;
}> {
  const { tenant } = useSession();
  return useQuery({
    // `source.read` is its own action server-side, so a principal that lacks it must not issue the
    // read at all — a 403 on a screen whose own read succeeded is noise, not information. The
    // imposter list passes `false`; `Sources.tsx` is only reachable with the capability and passes
    // nothing, keeping its existing behaviour exactly.
    enabled: options.enabled ?? true,
    queryKey: key(["sources"], tenant),
    queryFn: async () => {
      // Both keys are **required** by the contract — unlike `/imposters`, whose `imposters?` is
      // genuinely optional. So there is no `?? []` here: an absent `sources` would be a contract
      // violation, and defaulting it would turn a broken read into the confident on-screen claim
      // "no sources declared for this tenant". Let it surface instead.
      return apiGet<{ sources: SourceRecord[]; nodeLocal: SourcesNodeLocal }>(API_PATHS.sources, {
        tenant,
      });
    },
    ...POLLED,
  });
}

/**
 * Upsert a source declaration — `POST /admin/sources`. There is no separate create route: an id
 * already declared is replaced in place and a new one is created, which is why the console offers
 * one form for both rather than two (`Sources.tsx`'s `SourceForm`).
 *
 * Field casing follows every other admin-plane write body in this file (`AuditSinkWrite`,
 * `TenantWrite`, …): camelCase, matching `SourceRecord`'s own read-side fields — and matching the
 * vocabulary `control.rs::validate`'s own refusals already use on the wire (its poll-interval
 * refusal reads `"pollSecs {secs} is below the {MIN_POLL_SECS}s floor"`, camelCase, even though the
 * Rust field behind it is `poll_secs`). This route lands in parallel with this change; if it ships
 * a different casing, this type and the two hooks below are the only place to fix.
 */
export type SourceWrite = {
  id: string;
  uri: string;
  mode: SourceRecord["mode"];
  authRef?: string;
  onDrift: SourceRecord["onDrift"];
  pollSecs?: number;
};

export function useUpsertSource(): UseMutationResult<CommitOutcome, Error, SourceWrite> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body) => {
      const sent = await apiSend("POST", API_PATHS.sources, body, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["sources"] }),
  });
}

/**
 * Forget a source — `DELETE /admin/sources/{id}`.
 *
 * This never cascades: the apply path leaves a forgotten source's imposters exactly as they are,
 * only dropping their provenance, so its ports keep serving with no source left to reapply them
 * from. `Sources.tsx`'s confirm dialog states that in as many words — "delete" reads as "undeploy"
 * to an operator who has not read the apply path.
 */
export function useDeleteSource(): UseMutationResult<CommitOutcome, Error, { id: string }> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ id }) => {
      const sent = await apiSend(
        "DELETE",
        `${API_PATHS.sources}/${encodeURIComponent(id)}`,
        undefined,
        { tenant },
      );
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["sources"] }),
  });
}

/**
 * What one pull reported: which ports it changed, which it looked at and left alone, and whether it
 * ran at all.
 *
 * Transcribed from `PullReport` in the OpenAPI document this change extends — NOT guessed. An
 * earlier draft invented `changedPorts`/`unchangedPorts`, which meant a pull that had just replaced
 * a port rendered as "no ports changed": the screen confidently reported the opposite of what
 * happened. The real shape is `changed` (the ports created, replaced or removed), with `unchanged`
 * and `skipped` as BOOLEANS that mean different things — `unchanged` wrote no log entry at all,
 * while `skipped` committed a decision not to apply a drifted source.
 *
 * Still read defensively, because a screen must not throw on a malformed body — but defensive is
 * not the same as speculative, and the field names come from the contract.
 */
export type SourcePullReport = {
  revision: number | null;
  version: string | null;
  changed: number[];
  unchanged: boolean;
  skipped: boolean;
  /** Server-authored text about what the pull did NOT apply. Dropping it hides the caveat. */
  warnings: string[];
};

function readPullReport(body: unknown): SourcePullReport {
  const record = (body ?? {}) as Record<string, unknown>;
  const strings = (value: unknown): string[] =>
    Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === "string") : [];
  return {
    revision: typeof record.revision === "number" ? record.revision : null,
    version: typeof record.version === "string" ? record.version : null,
    changed: Array.isArray(record.changed)
      ? record.changed.filter((entry): entry is number => typeof entry === "number")
      : [],
    unchanged: record.unchanged === true,
    skipped: record.skipped === true,
    warnings: strings(record.warnings),
  };
}

/**
 * Pull one source now — `POST /admin/sources/{id}/pull`.
 *
 * Returns the parsed report directly rather than a `CommitOutcome`: the whole point of "refresh
 * now" is to show the operator what the pull just did, so `applied()` is the right assertion here —
 * a report that has not landed yet is not a report, and a route that answered `202` for this would
 * be a contract this hook does not yet understand, not a case to quietly paper over.
 */
export function usePullSource(): UseMutationResult<SourcePullReport, Error, { id: string }> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ id }) => {
      const sent = await apiSend<unknown>(
        "POST",
        `${API_PATHS.sources}/${encodeURIComponent(id)}/pull`,
        undefined,
        { tenant },
      );
      return readPullReport(applied(sent));
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["sources"] }),
  });
}

/**
 * Send a sample request to an imposter and hand back what it answered — `POST
 * /admin/imposters/{port}/try` (#335).
 *
 * `applied()` rather than a `CommitOutcome`: this is not a cluster write and has nothing to
 * converge, so there is no parked case to model — the endpoint either performed the exchange or
 * failed, and both answers are immediate.
 *
 * **No `invalidateQueries`, deliberately.** A try really does disturb server state (the request
 * log gains an entry, a scenario may advance), so refreshing the caches would be defensible — but
 * it would also mean pressing Send silently re-fetches the imposter and its stub table underneath
 * the response panel the operator is trying to read. The request log is polled on its own screen
 * and will show the entry there; what an operator wants here is the answer holding still.
 */
export function useTryStub(
  port: number,
): UseMutationResult<TryResult, Error, { request: TrySpec }> {
  const { tenant } = useSession();
  return useMutation({
    mutationFn: async ({ request }) => {
      const sent = await apiSend<TryResult>("POST", tryImposterPath(port), request, { tenant });
      return applied(sent);
    },
  });
}

/**
 * This node's own fleet reading.
 *
 * `enabled` is a caller's decision, not a fixed capability check, because the two callers want
 * opposite things from a principal that lacks the scope. The Cluster screen asks anyway and renders
 * the 404 as "fleet-scoped, not available to you" — someone who followed a bookmark deserves that
 * sentence. The imposter list only wants the reading to *qualify* what it shows, so it does not
 * ask: two guaranteed 404s behind every list load would be noise that means nothing.
 */
export function useFleetView(
  options: { enabled?: boolean; polled?: boolean } = {},
): UseQueryResult<FleetView> {
  return useQuery({
    queryKey: ["fleet"],
    queryFn: async () => {
      const [members, health] = await Promise.all([
        apiGet<FleetMembers>(API_PATHS.fleetMembers),
        // `apiGetMerged` for health alone: its `parked_intents_fleet` is summed across voters
        // (#360), and `Rift-Cluster-Partial` is the only signal that a node did not answer and the
        // sum is therefore a floor. The members read carries its coverage per row instead, so it
        // needs no header.
        apiGetMerged<FleetHealth>(API_PATHS.fleetHealth),
      ]);
      return fleetView(members, health.data, health.partial);
    },
    enabled: options.enabled ?? true,
    /*
     * `polled: false` reads the fleet once per mount instead of every 5s. `RecordingPanel`'s single
     * caller wants this reading only to name a caveat about fleet size, which changes on membership
     * events, not on every 5s tick — polling it there would be five-second noise for a sentence that
     * would not change. (The request log used to be a second caller of this option, before #147 H
     * moved its coverage off fleet topology entirely and onto the merge's own response headers.)
     */
    ...(options.polled === false ? {} : POLLED),
  });
}

/**
 * Enable or disable an imposter, resolving only once the write has actually landed.
 *
 * The `mutationFn` awaits the commit rather than the acknowledgement, which is what makes
 * `isPending` mean "committing" instead of "sent". Under `--cluster-admin-async` this route answers
 * `202` the moment the write is parked, and the console used to render that as done.
 *
 * A `failed` commit rejects, so the existing `ErrorNote` renders the fleet's own reason. An
 * `unobservable` one resolves — the write was accepted and this session simply cannot watch it —
 * and the screen says so rather than claiming either outcome.
 */
export function useLifecycleToggle(): UseMutationResult<
  CommitOutcome,
  Error,
  { port: number; enable: boolean }
> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ port, enable }) => {
      const sent = await apiSend("POST", lifecyclePath(port, enable), undefined, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    // Re-read rather than patch the cache: `SetEnabled` is a replicated op, so what the fleet
    // actually applied is the only thing worth showing. This is also what makes the list reflect
    // the change immediately instead of at the next poll tick.
    onSettled: () => client.invalidateQueries({ queryKey: ["imposters"] }),
  });
}

/**
 * One imposter's recorded requests, read from the fleet's merged journal (#147 H) — one already
 * combined answer rather than one node's own, with coverage and paging carried on the response
 * headers `apiGetMerged` reads (`Rift-Cluster-Partial`, `x-rift-next-index`, `x-rift-truncated`).
 */
export type RequestLogState =
  | {
      kind: "rows";
      rows: RecordedRequest[];
      coverage: Coverage;
      truncated: boolean;
      /**
       * The cursor the response that produced these rows issued, carried here rather than in a
       * ref so that it lives and dies with the cached rows it belongs to — see `useRequestLog`.
       * `null` means the merge offered no cursor, so the next poll starts from the beginning.
       */
      cursor: string | null;
      /**
       * Cursored polls since the last full read. `useRequestLog` drops the cursor once this
       * reaches `BASELINE_EVERY`, which is what stops the accumulated list drifting permanently
       * away from what the fleet actually holds.
       */
      pollsSinceBaseline: number;
    }
  | { kind: "unknown"; reason: string };

/**
 * Re-read the whole journal every this-many cursored polls. At the 2 s request-log cadence that is
 * roughly a minute, which bounds how long this screen can show rows the fleet has already cleared
 * or evicted — see the reasoning in `useRequestLog`.
 */
const BASELINE_EVERY = 30;

/**
 * Rows in recorded-timestamp order, the same order a single merged page arrives in.
 *
 * `Array.prototype.sort` is stable, so rows whose `timestamp` is absent — an entry from an engine
 * predating the field, which `RequestLog.tsx` renders as `—` — keep their arrival order relative
 * to each other instead of being shuffled by a comparator that cannot rank them.
 */
function byTimestamp(rows: RecordedRequest[]): RecordedRequest[] {
  return [...rows].sort((a, b) => (a.timestamp ?? "").localeCompare(b.timestamp ?? ""));
}

/**
 * A failed read resolves to `{ kind: "unknown" }` rather than rejecting, because on this screen the
 * two outcomes are different sentences and the query's own error state cannot tell them apart: an
 * empty array and an unreachable merge both arrive here as "no rows to show". `readLog` is the only
 * place that decision is made for the body; a transport failure (the `catch` below) is the same
 * verdict for a different reason.
 *
 * Resolving instead of rejecting opts this query out of `retryTransportFailures` — a `queryFn` that
 * never rejects is never retried. That is a deliberate trade and not a free one: a transient blip
 * shows the "unknown" alert immediately rather than after one silent retry. The 2s poll heals it on
 * the next tick, and on this screen an honest "could not read" for two seconds beats a retry that
 * delays the distinction this whole screen is built to preserve.
 */
export function useRequestLog(port: number): UseQueryResult<RequestLogState> {
  const { tenant } = useSession();
  const client = useQueryClient();
  const queryKey = key(["requests", port], tenant);
  return useQuery({
    queryKey,
    queryFn: async (): Promise<RequestLogState> => {
      /*
       * The cursor and the accumulated rows are read back out of the **cache**, not out of a ref.
       *
       * A ref is the obvious place for state that has to outlive one `queryFn` call, and it is
       * wrong here for one reason: it outlives too much. Clearing the log (the button on this very
       * screen) invalidates this query, but an invalidation is only a refetch — a ref-held cursor
       * survives it, so the refetch asks `?since=<pre-clear token>`, is correctly told there is
       * nothing after it, and appends an empty delta to rows the server has already discarded. The
       * operator clears the log and every entry stays exactly where it was.
       *
       * Keeping the pair in the cached value instead means anything that resets the cache resets
       * them too, which is precisely what `useClearRequests` now does. Switching imposters is
       * unaffected either way — `RequestLog.tsx` keys `Log` on `port`, and the key is per-port.
       */
      const held = client.getQueryData<RequestLogState>(queryKey);
      /*
       * Deltas, but never forever: every `BASELINE_EVERY` polls the cursor is dropped and the whole
       * journal is re-read.
       *
       * The server stamps `x-rift-next-index` on every 200, so a cursor, once held, is never
       * offered back as `null` — accumulating on it unconditionally means this screen never
       * reconciles with the fleet again. Three things then drift, and none of them announce
       * themselves: a clear issued anywhere *other* than this tab (another operator, the CLI, an
       * SDK) leaves every pre-clear row on screen for good, because the clear neither regresses the
       * token nor sets `truncated`; rows the fleet has since evicted under retention stay here
       * forever, so `request-total` counts a journal no node holds; and `[...held.rows, ...delta]`
       * re-copies a list that only grows, on the very screen an operator leaves open for an hour.
       *
       * Re-baselining bounds all three by time rather than trying to detect each one. The token's
       * `generation` field could eventually detect the clear case precisely (it is carried for
       * exactly that, though nothing reads it yet), but a periodic full read is what makes the
       * other two correct as well, and it costs one uncursored read per minute against a 2 s poll.
       */
      const resumable =
        held?.kind === "rows" && held.cursor !== null && held.pollsSinceBaseline < BASELINE_EVERY;
      const since = resumable && held?.kind === "rows" ? held.cursor : null;
      const path = since === null ? requestsPath(port) : `${requestsPath(port)}?since=${since}`;
      try {
        const merged = await apiGetMerged<unknown>(path, { tenant });
        const local = readLog(merged.data);
        if (local.kind === "unknown") return local;
        const resuming = since !== null && held?.kind === "rows";
        /*
         * A cursored fetch is the delta the merge is handing over on top of what this screen
         * already holds; an uncursored one is the merge's whole current answer, so it replaces.
         *
         * The concatenation is re-sorted because the contract says it must be: pages are ordered
         * by recorded timestamp *within* a page, and `openapi-ee.yaml` spells out that
         * concatenating them is not a globally sorted stream — a peer that becomes reachable
         * between polls contributes entries older than everything already returned. That is the
         * same degraded-fan-out moment the partial label exists to announce, so appending blind
         * would put a chronological screen out of order exactly when it is being relied on. This
         * is not the client-side *merge* the design doc bans — the server merged; this only
         * restores order across pages the server itself declares unordered.
         */
        const rows = resuming ? byTimestamp([...held.rows, ...local.rows]) : local.rows;
        return {
          kind: "rows",
          rows,
          coverage: coverageFor(merged.partial),
          /*
           * Sticky across the accumulation, unlike `partial`. The server sets `x-rift-truncated`
           * on the one read whose position predates the shard watermark; the next poll presents a
           * position above it and the header is gone — but the hole it announced is permanent and
           * sits in the middle of the rows still on screen. A notice that erased itself after one
           * 2 s tick would be a swallowed warning on the one screen built to keep "incomplete" and
           * "empty" distinguishable. Cleared by the baseline re-read, which is the point at which
           * the rows it describes are replaced. (`partial` is correctly per-response: an unreached
           * shard's position does not advance, so the next merge picks it up.)
           */
          truncated: merged.truncated || (resuming && held.kind === "rows" && held.truncated),
          cursor: merged.next,
          pollsSinceBaseline: resuming ? held.pollsSinceBaseline + 1 : 0,
        };
      } catch (error) {
        return {
          kind: "unknown",
          reason: error instanceof Error ? error.message : "the merge could not be reached",
        };
      }
    },
    ...POLLED_REQUESTS,
  });
}

/**
 * One imposter, read **with** the revision the fleet stamped it at.
 *
 * The token is not a nicety: it is the `If-Match` every stub write on this screen is conditioned
 * on, and without it a save is last-writer-wins. It travels with the body through the cache so a
 * write always quotes the revision of the state the operator was actually looking at.
 */
export function useImposter(port: number): UseQueryResult<RevisionedRead<Imposter>> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["imposter", port], tenant),
    queryFn: () => apiGetWithRevision<Imposter>(imposterPath(port), { tenant }),
    ...POLLED,
  });
}

/**
 * Raised when a stub write was refused because the imposter moved underneath the editor.
 *
 * Carries what it takes to *offer* a rebase and nothing that would perform one: the stub as it now
 * is, and a fresh token. The operator's own edit stays where it already is — in the editor — because
 * merging the two is a decision the console is not entitled to make. `theirs` is `null` when the
 * stub is gone entirely, which is a different sentence on screen.
 */
export class StubConflict extends Error {
  readonly theirs: Stub | null;
  readonly revision: string | null;

  constructor(theirs: Stub | null, revision: string | null) {
    super("this imposter changed since the editor read it");
    this.name = "StubConflict";
    this.theirs = theirs;
    this.revision = revision;
  }
}

/** A stub write's variables. `revision` is the token the read handed over; never invented here. */
export type StubWrite = {
  port: number;
  stubId: string;
  /** Sent verbatim so the raw editor stores the operator's own bytes. Absent for a delete. */
  body?: RawJsonBody;
  revision: string | null;
};

/**
 * Send a by-id stub write, turning the fleet's `409` into a rebase prompt.
 *
 * The re-read on conflict happens here rather than in the screen so that *every* caller of this
 * hook gets the fresh token: retrying with the stale one would 409 again forever, and retrying with
 * no token at all would win the race by discarding the other editor's work, which is the lost
 * update wearing a different hat.
 */
function useStubWrite(
  send: (write: StubWrite, tenant: string | null) => Promise<SendResult<unknown>>,
): UseMutationResult<CommitOutcome, Error, StubWrite> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (write) => {
      const conflict = async (): Promise<never> => {
        const fresh = await apiGetWithRevision<Imposter>(imposterPath(write.port), { tenant });
        const theirs = (fresh.data.stubs ?? []).find((stub) => stub.id === write.stubId) ?? null;
        throw new StubConflict(theirs, fresh.revision);
      };
      try {
        const outcome = await settle(await send(write, tenant), { tenant });
        if (outcome.kind === "failed") {
          /*
           * Under `--cluster-admin-async` the precondition is judged inside apply, AFTER the 202 —
           * so a stale token surfaces here as a failed commit whose detail carries the state
           * machine's `"revision conflict"` prefix, not as a synchronous 409. Same refusal, same
           * operator decision to make; it gets the same rebase prompt, not a raw error string.
           */
          if (outcome.detail.startsWith("revision conflict")) return conflict();
          throw new Error(outcome.detail);
        }
        return outcome;
      } catch (error) {
        if (!(error instanceof ApiError) || error.status !== 409) throw error;
        return conflict();
      }
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["imposter"] }),
  });
}

export function usePutStub(): UseMutationResult<CommitOutcome, Error, StubWrite> {
  return useStubWrite((write, tenant) =>
    apiSend("PUT", stubByIdPath(write.port, write.stubId), write.body, {
      tenant,
      ifMatch: write.revision,
    }),
  );
}

export function useDeleteStub(): UseMutationResult<CommitOutcome, Error, StubWrite> {
  return useStubWrite((write, tenant) =>
    apiSend("DELETE", stubByIdPath(write.port, write.stubId), undefined, {
      tenant,
      ifMatch: write.revision,
    }),
  );
}

/**
 * Append a stub.
 *
 * `POST` to the collection, not a `PUT` to an id: a by-id `PUT` answers `404` for an id that does
 * not exist yet, so "add" genuinely is a different route rather than the same one with a new id.
 * It carries the same `If-Match`, so appending cannot clobber a concurrent edit either.
 */
export function useAddStub(): UseMutationResult<CommitOutcome, Error, StubWrite> {
  return useStubWrite((write, tenant) =>
    apiSend("POST", stubsPath(write.port), addStubBody(write.body), {
      tenant,
      ifMatch: write.revision,
    }),
  );
}

/**
 * Wrap a stub in the envelope `addStub` requires: `{"stub": …}`, optionally with an `index`.
 *
 * The two stub-writing routes take **different bodies** and the console got it wrong: the by-id
 * `PUT` takes a bare `Stub`, `POST /imposters/:port/stubs` takes `{stub, index?}`. Sending the bare
 * stub to the collection answered `400 missing field 'stub'`, so appending a stub never worked at
 * all. Nothing caught it because the unit tests stub `fetch` — they assert what the client sends,
 * which is precisely the thing that was wrong; only the contract or a real server can say.
 *
 * Wrapped textually rather than by parsing and re-serialising, because the operator's own bytes are
 * the document (`StubEditor`'s second rule): key order and whitespace they chose survive the save,
 * and a round trip through `JSON.parse` would quietly normalise both.
 */
function addStubBody(body: RawJsonBody | undefined): RawJsonBody | undefined {
  return body === undefined ? undefined : new RawJsonBody(`{"stub":${body.text}}`);
}

/**
 * The recorded projection of one imposter (`replayable=true&removeProxies=true`) — the stubs a
 * recording has actually captured, in the flat response form the engine emits for them.
 *
 * Read as its own query, not folded into `useImposter`: it is a different upstream projection of
 * the same imposter, not a filtered view of the same body, so a poll of the plain read must not
 * step on this one's cache and vice versa.
 */
export function useRecordedStubs(
  port: number,
  options: { enabled?: boolean } = {},
): UseQueryResult<Stub[]> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["recorded-stubs", port], tenant),
    queryFn: async () => {
      const body = await apiGet<Imposter>(recordedStubsPath(port), { tenant });
      // `stubs` is optional in the contract, same reasoning as `useImposters`.
      return body.stubs ?? [];
    },
    // The caller decides, because only it knows whether this imposter is recording — an imposter
    // that is not has nothing to project, and polling it would be a request per 5s that can only
    // answer "nothing".
    enabled: options.enabled ?? true,
    ...POLLED,
  });
}

/**
 * Replace the whole stub list with a recording's captured stubs — "stop & promote".
 *
 * Reuses `useStubWrite` for its `409` → `StubConflict` handling: a promote is still a
 * concurrency-conditioned imposter write, and an operator who has just reviewed a page of recorded
 * responses deserves the same rebase prompt a hand-edited stub gets, not a bare error. `stubId` goes
 * unused — the route this posts to (`PUT /imposters/:port/stubs`) carries no id in its path — but
 * `StubWrite` is the shape every write on this screen already speaks, and a promote paying for a
 * field it never reads is cheaper than a second write pipeline that duplicates the conflict handling.
 */
export function usePromoteRecording(): UseMutationResult<CommitOutcome, Error, StubWrite> {
  return useStubWrite((write, tenant) =>
    apiSend("PUT", stubsPath(write.port), write.body, { tenant, ifMatch: write.revision }),
  );
}

/**
 * Discard everything a recording has captured so far, without touching the proxy stub itself — the
 * imposter keeps recording; only what it has captured up to now is cleared.
 *
 * Gated by the caller on `requests.clear`, not `imposter.write`: `DELETE .../savedProxyResponses` is
 * not terminated by the admin front, so it reaches upstream and `principal.rs::map_action` folds it
 * onto the same `Action::SavedRequestsClear` as clearing the request log (RFC-002 §4.1). See
 * `rbac.ts`'s `requests.clear` note for the specific mapping this transcribes.
 */
export function useDiscardRecording(): UseMutationResult<CommitOutcome, Error, { port: number }> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ port }) => {
      const sent = await apiSend("DELETE", savedProxyResponsesPath(port), undefined, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: (_data, _error, { port }) => {
      void client.invalidateQueries({ queryKey: ["imposter", port] });
      void client.invalidateQueries({ queryKey: ["recorded-stubs", port] });
    },
  });
}

/**
 * Create an imposter.
 *
 * The port is part of the body and never auto-assigned: `createImposter` requires it explicitly
 * because an auto-assigned port cannot replicate across the fleet — the other nodes would each pick
 * their own. So this is a form field, not a convenience the console can hide.
 *
 * No `If-Match`. The route accepts one, but a create has nothing to condition on: there is no prior
 * revision of an imposter that does not exist. A port already in use comes back as the server's own
 * refusal, which is the check that matters and the only one that sees the whole fleet.
 */
export function useCreateImposter(): UseMutationResult<CommitOutcome, Error, Imposter> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body) => {
      const sent = await apiSend("POST", API_PATHS.imposters, body, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["imposters"] }),
  });
}

/**
 * Add one imposter carried in from an import document, or a clone (#251).
 *
 * Deliberately not `useCreateImposter`: that hook's body is typed `Imposter`, the schema-shaped
 * form `NewImposter` builds field by field. An import entry (and a clone's rewritten document) is
 * `parseImportDocument`/`cloneImposter`'s output — an already-assembled `Record<string, unknown>`
 * lifted out of the operator's own document — and casting it to `Imposter` to reuse the other hook
 * would just be the unsafety of `as` wearing a different hat. Same wire call, same settle
 * discipline, a body type that matches what actually gets sent.
 */
export function useImportAddImposter(): UseMutationResult<
  CommitOutcome,
  Error,
  Record<string, unknown>
> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (imposter) => {
      const sent = await apiSend("POST", API_PATHS.imposters, imposter, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["imposters"] }),
  });
}

/**
 * Replace the tenant's whole imposter set with an imported document (#251, "Replace all").
 *
 * The caller gates this on `imposter.delete` as well as `imposter.write` and routes it through the
 * destructive `Confirm` modal — every imposter this fleet currently serves that the document does
 * not name is gone once this lands, and neither of those facts is visible from this hook.
 */
export function useReplaceImposters(): UseMutationResult<
  CommitOutcome,
  Error,
  { imposters: Record<string, unknown>[] }
> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body) => {
      const sent = await apiSend("PUT", API_PATHS.imposters, body, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["imposters"] }),
  });
}

/**
 * Delete an imposter, and everything hanging off it.
 *
 * Authorized by `Action::ImposterDelete`, which is why the screen gates on `imposter.delete` rather
 * than `imposter.write` even though the two are granted together today (see `rbac.ts`).
 *
 * Both caches are invalidated: the detail read is keyed by port, and leaving it would let a
 * back-navigation render a deleted imposter from cache as though it still existed.
 */
export function useDeleteImposter(): UseMutationResult<CommitOutcome, Error, { port: number }> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ port }) => {
      const sent = await apiSend("DELETE", imposterPath(port), undefined, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: (_data, _error, { port }) => {
      void client.invalidateQueries({ queryKey: ["imposters"] });
      void client.removeQueries({ queryKey: ["imposter", port] });
    },
  });
}

/**
 * Empty one imposter's recorded requests on this node.
 *
 * `Action::SavedRequestsClear` — an Operator-tier "disturb" action, not an Editor-tier "redefine"
 * one, so the screen gates it on `imposter.lifecycle`. That grouping is `authz.rs`'s, not a guess:
 * clearing a log changes no configuration.
 *
 * Per-node like the log itself. Clearing here empties what *this* node recorded; another node's log
 * is untouched, which is the same scope caveat the screen already keeps in front of the reader.
 */
/**
 * The fleet's declared audit export sink.
 *
 * `404` is **not** an error here: the contract uses it for "no sink is declared" as well as for
 * "caller lacks fleet-scoped access" (RFC-002 §8.4, where the two must be indistinguishable). The
 * screen is only reachable by a principal that holds `cluster.admin`, so it resolves the absent case
 * to `null` and lets every other status reject — folding a genuine `503` into "no sink" would report
 * an unreachable node as a fleet that ships nowhere.
 */
export function useAuditSink(options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: ["audit-sink"],
    queryFn: async (): Promise<AuditSink | null> => {
      try {
        return await apiGet<AuditSink>(API_PATHS.auditSink);
      } catch (cause) {
        if (cause instanceof ApiError && cause.status === 404) return null;
        throw cause;
      }
    },
    enabled: options.enabled ?? true,
    ...POLLED,
  });
}

export function usePutAuditSink(): UseMutationResult<CommitOutcome, Error, AuditSinkWrite> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body) => {
      const sent = await apiSend("PUT", API_PATHS.auditSink, body);
      const outcome = await settle(sent, { tenant: null });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["audit-sink"] }),
  });
}

export function useDeleteAuditSink(): UseMutationResult<CommitOutcome, Error, void> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async () => {
      const sent = await apiSend("DELETE", API_PATHS.auditSink);
      const outcome = await settle(sent, { tenant: null });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["audit-sink"] }),
  });
}

export function useClearRequests(): UseMutationResult<CommitOutcome, Error, { port: number }> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ port }) => {
      const sent = await apiSend("DELETE", requestsPath(port), undefined, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    /*
     * `removeQueries`, not `invalidateQueries` (#147 H). Since the request log pages through a
     * server cursor, its cached value carries both the accumulated rows and the cursor that earned
     * them. An invalidation only refetches, so the pre-clear cursor would survive the clear: the
     * refetch would ask for entries *after* a position the server has just emptied past, be
     * correctly told there are none, and leave every cleared row on screen. Dropping the entry
     * makes the next read a genuine first read.
     */
    onSettled: () => client.removeQueries({ queryKey: ["requests"] }),
  });
}

/**
 * Scenario states for one imposter, under one space (#232).
 *
 * `flow: null` sends no `flowId`, and the imposter resolves its own default — the response echoes
 * which one it used, and that echo is what every other read on the screen is then scoped to. A
 * failed read becomes `unknown` rather than rejecting, because "this imposter declares no
 * scenarios" and "this node could not answer" are different sentences and only the type keeps them
 * apart.
 */
/**
 * Every imposter's default flow, read together.
 *
 * The flow-state screen is a fleet-wide view in the design — flows listed across imposters, with the
 * imposter as a prefix on the flow id rather than a choice to make first. Nothing enumerates flows
 * (#374: a space is created implicitly by whatever id a request carried), so what is actually
 * readable is each imposter's *default* flow, and that is what this fans out for.
 *
 * A fan-out, and worth saying why it is acceptable here when it is not for the request journal: a
 * flow list is a **set**, not a stream. The journal's fan-out was refused because ordering N
 * independent reads by whichever returned first would present network timing as journal order. A
 * set has no order to get wrong — each row is independently true, and a row that fails to load says
 * so on its own line rather than corrupting the others.
 *
 * `combine` folds the results in the query layer so the screen sees one value rather than N.
 */
/** One recorded request, tagged with the imposter and flow it was read from. */
export type FleetRequestRow = FleetRequestPage["requests"][number];

/**
 * The fleet-wide request journal — one read (#362), not the N-way client fan-out it replaces.
 *
 * The admin front now does the merge itself: `GET /admin/requests` walks every imposter the
 * caller's tenant owns and hands back one ordered page, so this hook is a single `apiGet` in the
 * same shape as `useSources` — no `useQueries`, no per-port cap, no client-side union.
 *
 * `coverage` is carried rather than dropped, same reasoning as `useImposters`' `partial`: the
 * server may cap how many imposters one page walks (`coverage.capped`/`coverage.omitted`), and a
 * capped page rendered as the whole fleet is exactly the wrong-but-quiet failure this type exists
 * to prevent. Ordering is still the part to be honest about even though the merge is now the
 * server's: rows are ordered by each request's own recorded timestamp, stamped by whichever node
 * served it, so entries recorded within milliseconds of each other on clock-skewed nodes can still
 * transpose — `RequestLog.tsx`'s caveat banner says so.
 *
 * The wire order is oldest-first, same as the per-imposter journal it merges (openapi-ee.yaml's
 * `savedRequests` description) — the right convention for a resumable cursor walk, and the wrong
 * one for a screen an operator reads top-down. Reversed here, once, rather than in the screen, so
 * every caller of this hook sees the newest-first order the log has always shown.
 */
export function useFleetRequests(): UseQueryResult<{
  rows: FleetRequestRow[];
  coverage: FleetJournalCoverage;
}> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["fleet-requests"], tenant),
    queryFn: async () => {
      const page = await apiGet<FleetRequestPage>(API_PATHS.fleetRequests, { tenant });
      return { rows: [...page.requests].reverse(), coverage: page.coverage };
    },
    ...POLLED,
  });
}

export function useAllScenarios(
  ports: readonly number[],
): { rows: { port: number; state: ScenarioState }[]; pending: boolean } {
  const { tenant } = useSession();
  return useQueries({
    queries: ports.map((port) => ({
      queryKey: key(["scenarios", port, null], tenant),
      queryFn: async (): Promise<ScenarioState> => {
        try {
          return readScenarios(await apiGet<unknown>(scenariosPath(port, null), { tenant }));
        } catch (error) {
          return {
            kind: "unknown" as const,
            reason: error instanceof Error ? error.message : "this node could not be reached",
          };
        }
      },
      ...POLLED,
    })),
    combine: (results) => ({
      rows: results.flatMap((result, index) => {
        const port = ports[index];
        if (port === undefined || result.data === undefined) return [];
        return [{ port, state: result.data }];
      }),
      pending: results.some((result) => result.isPending),
    }),
  });
}

export function useScenarios(port: number, flow: string | null): UseQueryResult<ScenarioState> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["scenarios", port, flow], tenant),
    queryFn: async (): Promise<ScenarioState> => {
      try {
        return readScenarios(await apiGet<unknown>(scenariosPath(port, flow), { tenant }));
      } catch (error) {
        return {
          kind: "unknown",
          reason: error instanceof Error ? error.message : "this node could not be reached",
        };
      }
    },
    ...POLLED,
  });
}

/**
 * One correlated-isolation space.
 *
 * `flowId` is `null` until the scenario read has resolved which flow the screen is looking at —
 * there is no route that lists spaces, so a space cannot be read before its id is known.
 */
export function useSpace(port: number, flowId: string | null): UseQueryResult<SpaceState> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["space", port, flowId], tenant),
    queryFn: async (): Promise<SpaceState> => {
      // `enabled` below keeps this unreachable with a null flow; the guard is here so the type
      // narrows rather than being asserted away.
      if (flowId === null) return { kind: "unknown", reason: "no flow selected" };
      try {
        return readSpace(await apiGet<unknown>(spacePath(port, flowId), { tenant }));
      } catch (error) {
        return {
          kind: "unknown",
          reason: error instanceof Error ? error.message : "this node could not be reached",
        };
      }
    },
    enabled: flowId !== null,
    ...POLLED,
  });
}

/**
 * One flow-state entry, read on demand.
 *
 * On demand rather than polled, and keyed by a key the operator typed, because the contract
 * publishes no route that lists a flow's entries — the panel can only answer about a key someone
 * names. A `404` becomes `absent` rather than an error: the contract documents it as "no such
 * entry", though see `ABSENT_ENTRY_CAVEAT` for why the screen does not read that as proof.
 */
export function useFlowStateEntry(
  port: number,
  flowId: string | null,
  entryKey: string | null,
): UseQueryResult<FlowStateRead> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["flow-state", port, flowId, entryKey], tenant),
    queryFn: async (): Promise<FlowStateRead> => {
      if (flowId === null || entryKey === null) {
        return { kind: "unknown", reason: "no key requested" };
      }
      try {
        return readFlowStateEntry(
          await apiGet<unknown>(flowStateEntryPath(port, flowId, entryKey), { tenant }),
        );
      } catch (error) {
        if (error instanceof ApiError && error.status === 404) return { kind: "absent" };
        return {
          kind: "unknown",
          reason: error instanceof Error ? error.message : "this node could not be reached",
        };
      }
    },
    enabled: flowId !== null && entryKey !== null,
  });
}

/**
 * Move one scenario to a state, **within one space**.
 *
 * `flowId` is always sent when the screen knows one. Omitting it is not a no-op: the route silently
 * writes the imposter's *default* flow, so a screen scoped to `checkout-1` that forgot it would
 * move a scenario in a space the operator is not looking at and report success.
 */
export function useSetScenarioState(): UseMutationResult<
  CommitOutcome,
  Error,
  { port: number; name: string; state: string; flowId: string | null }
> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ port, name, state, flowId }) => {
      const body = flowId === null ? { state } : { state, flowId };
      const sent = await apiSend("PUT", scenarioStatePath(port, name), body, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => {
      void client.invalidateQueries({ queryKey: ["scenarios"] });
      void client.invalidateQueries({ queryKey: ["space"] });
    },
  });
}

/** Reset every scenario in one space. Same `flowId` discipline as the write above. */
export function useResetScenarios(): UseMutationResult<
  CommitOutcome,
  Error,
  { port: number; flowId: string | null }
> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ port, flowId }) => {
      const sent = await apiSend(
        "POST",
        scenariosResetPath(port),
        flowId === null ? {} : { flowId },
        { tenant },
      );
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => {
      void client.invalidateQueries({ queryKey: ["scenarios"] });
      void client.invalidateQueries({ queryKey: ["space"] });
    },
  });
}

/** Tear one space down — its scoped stubs and its scenario states go with it. */
export function useTeardownSpace(): UseMutationResult<
  CommitOutcome,
  Error,
  { port: number; flowId: string }
> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ port, flowId }) => {
      const sent = await apiSend("DELETE", spacePath(port, flowId), undefined, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => {
      void client.invalidateQueries({ queryKey: ["space"] });
      void client.invalidateQueries({ queryKey: ["scenarios"] });
    },
  });
}

/**
 * Append a stub scoped to one space.
 *
 * Sent as `RawJsonBody` for the same reason the imposter's own stub editor does: the operator's
 * text is stored as they typed it rather than reordered by a parse-and-restringify round trip.
 */
export function useAddSpaceStub(): UseMutationResult<
  CommitOutcome,
  Error,
  { port: number; flowId: string; body: RawJsonBody }
> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ port, flowId, body }) => {
      const sent = await apiSend("POST", spaceStubsPath(port, flowId), body, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => {
      void client.invalidateQueries({ queryKey: ["space"] });
      // A space stub may declare a `scenarioName`, which adds a scenario to this space — so the
      // scenario list is stale too, and invalidating only the space would leave it a poll behind.
      void client.invalidateQueries({ queryKey: ["scenarios"] });
    },
  });
}

/**
 * Write one flow-state value.
 *
 * Gated in the UI on `space.stubWrite`, not on a flow-state capability — there is no
 * `FlowStateWrite` action, and `principal.rs::map_action` classifies this route as
 * `Action::SpaceStubWrite`. See the capability's own note in `rbac.ts`.
 */
export function useSetFlowStateEntry(): UseMutationResult<
  CommitOutcome,
  Error,
  { port: number; flowId: string; key: string; body: RawJsonBody }
> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ port, flowId, key: entryKey, body }) => {
      const sent = await apiSend("PUT", flowStateEntryPath(port, flowId, entryKey), body, {
        tenant,
      });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["flow-state"] }),
  });
}

/**
 * Clear flow state: one key when `key` is given, the whole space when it is not.
 *
 * One hook for both because the server authorizes them identically — `map_action` returns
 * `Action::FlowStateClear` for any `imposter.delete` under `/admin/imposters/`, whether or not the
 * path names a key.
 */
export function useClearFlowState(): UseMutationResult<
  CommitOutcome,
  Error,
  { port: number; flowId: string; key?: string }
> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ port, flowId, key: entryKey }) => {
      const path =
        entryKey === undefined
          ? flowStatePath(port, flowId)
          : flowStateEntryPath(port, flowId, entryKey);
      const sent = await apiSend("DELETE", path, undefined, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["flow-state"] }),
  });
}

export function useRouteTable(): UseQueryResult<Route[]> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["front-door-routes"], tenant),
    queryFn: async () =>
      normalizeTable(await apiGet<RouteTable>(API_PATHS.frontDoorRoutes, { tenant })),
    ...POLLED,
  });
}

/** Raised when the table moved underneath the editor, so the screen can offer refresh-and-reapply. */
export class RouteTableConflict extends Error {
  readonly current: Route[];

  constructor(current: Route[]) {
    super("the route table changed since it was loaded");
    this.name = "RouteTableConflict";
    this.current = current;
  }
}

/**
 * Replace the whole table, refusing to overwrite a concurrent edit.
 *
 * `If-Match` is not available here — `admin_front.rs:1811` restricts it to single-imposter
 * operations — so the precondition is a re-read compared against the table the draft was based on.
 *
 * This narrows the lost-update window; it does not close it. A write that commits between this
 * re-read and the `PUT` is still lost, and nothing client-side can prevent that. Closing it needs a
 * server-side precondition on this route (filed as a follow-up).
 */
export function usePutRoutes(): UseMutationResult<
  { stored: RouteTable | null; outcome: CommitOutcome },
  Error,
  { draft: Route[]; base: Route[] }
> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ draft, base }) => {
      const current = normalizeTable(
        await apiGet<RouteTable>(API_PATHS.frontDoorRoutes, { tenant }),
      );
      if (JSON.stringify(current) !== JSON.stringify(base)) {
        throw new RouteTableConflict(current);
      }
      const sent = await apiSend<RouteTable>(
        "PUT",
        API_PATHS.frontDoorRoutes,
        { routes: draft },
        { tenant },
      );
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      /*
       * A parked write has no body to adopt — the `202` carries op ids, not the stored table — so
       * `stored` is null there and the cache is left to the invalidation refetch. Seeding it from
       * the draft instead would paint the table as saved on the strength of a write we have not
       * confirmed, which is the whole bug.
       */
      return { stored: sent.kind === "applied" ? sent.data : null, outcome };
    },
    /*
     * Adopt the stored table the `PUT` returns straight into the cache.
     *
     * Without this the cached read stays at the pre-save table until the invalidation refetch
     * lands, and in that window the editor's adopt-when-clean effect sees a clean draft beside an
     * older `loaded` and reverts the screen to it. It converges, but a save that briefly shows as
     * undone — and stays that way if the refetch fails — is exactly the kind of quiet lie this
     * console is being careful about elsewhere.
     */
    onSuccess: ({ stored }) => {
      if (stored === null) return;
      client.setQueryData(key(["front-door-routes"], tenant), () => normalizeTable(stored));
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["front-door-routes"] }),
  });
}

/**
 * Remove one route by id.
 *
 * Preferred over a whole-table `PUT` whenever a single removal is what the operator meant: it
 * cannot take an unrelated concurrent edit down with it.
 */
export function useDeleteRoute(): UseMutationResult<CommitOutcome, Error, { routeId: string }> {
  const { tenant } = useSession();
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ routeId }) => {
      const sent = await apiSend("DELETE", frontDoorRoutePath(routeId), undefined, { tenant });
      const outcome = await settle(sent, { tenant });
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["front-door-routes"] }),
  });
}

/**
 * The admin plane (RFC-002). Every one of these routes addresses its tenant through the URL path,
 * never `X-Rift-Tenant` — see `paths.ts` — so, unlike the hooks above, none of these pass `tenant`
 * to `apiGet`/`apiSend`. `getAudit` is the one exception: it has no tenant path segment, so the
 * header is how a non-fleet-admin's rows are scoped at all.
 */

const ADMIN_TENANTS_KEY = ["admin-tenants"];
const adminTenantKey = (tenantId: string): unknown[] => ["admin-tenant", tenantId];
const adminPrincipalsKey = (tenantId: string): unknown[] => ["admin-principals", tenantId];

/**
 * `enabled` is the caller's decision because `TenantList` is `Action::ClusterAdmin` scoped to the
 * **fleet**, not to the caller's tenant. A tenant-admin holds no `*` binding, so this read is a
 * permanent 404 for them — asking anyway turns their Administration landing into a red error every
 * five seconds, which is the failure the `cluster.admin` capability was introduced to stop.
 */
export function useTenants(options: { enabled?: boolean } = {}): UseQueryResult<Tenant[]> {
  return useQuery({
    queryKey: ADMIN_TENANTS_KEY,
    queryFn: () => apiGet<Tenant[]>(API_PATHS.tenants),
    enabled: options.enabled ?? true,
    ...POLLED,
  });
}

/**
 * A pure existence-and-permission probe for one tenant (RFC-002 §8.4).
 *
 * The screen must never render anything from this query's data — only whether it errored, and with
 * which status. The API's anti-oracle (a cross-tenant probe and a nonexistent tenant answer
 * byte-identical `404`s) only holds if the console does not rebuild a distinguishing signal on top
 * of it by rendering content that happens to differ between the two.
 */
export function useTenantProbe(
  tenantId: string,
  options: { enabled: boolean },
): UseQueryResult<Tenant> {
  return useQuery({
    queryKey: adminTenantKey(tenantId),
    queryFn: () => apiGet<Tenant>(tenantPath(tenantId)),
    enabled: options.enabled,
    ...POLLED,
  });
}

export function useCreateTenant(): UseMutationResult<unknown, Error, TenantWrite> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body) => applied(await apiSend("POST", API_PATHS.tenants, body)),
    onSettled: () => client.invalidateQueries({ queryKey: ADMIN_TENANTS_KEY }),
  });
}

export function useSaveTenant(): UseMutationResult<
  unknown,
  Error,
  { tenantId: string; body: TenantWrite }
> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ tenantId, body }) => applied(await apiSend("PUT", tenantPath(tenantId), body)),
    onSettled: (_data, _error, vars) => {
      client.invalidateQueries({ queryKey: ADMIN_TENANTS_KEY });
      client.invalidateQueries({ queryKey: adminTenantKey(vars.tenantId) });
    },
  });
}

export function useDeleteTenant(): UseMutationResult<unknown, Error, { tenantId: string }> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ tenantId }) => applied(await apiSend("DELETE", tenantPath(tenantId))),
    onSettled: () => client.invalidateQueries({ queryKey: ADMIN_TENANTS_KEY }),
  });
}

export function usePrincipals(tenantId: string): UseQueryResult<Principal[]> {
  return useQuery({
    queryKey: adminPrincipalsKey(tenantId),
    queryFn: () => apiGet<Principal[]>(principalsPath(tenantId)),
    ...POLLED,
  });
}

/**
 * Mint a principal, handing the raw key to the caller **out of band**.
 *
 * `onIssued` receives the one-time `apiKey`; the mutation itself resolves to the *stripped* record,
 * so React Query never stores the key anywhere. Returning the full response and sanitising it in
 * `onSuccess` is not enough: `useMutation` keeps its own copy of the resolved value in the
 * MutationCache, where `setQueryData` cannot reach it, and it stays readable from the client (and
 * React Query Devtools) for `gcTime` after the panel is dismissed. The key exists for one moment,
 * and the only place it lives is the component state `onIssued` writes it into.
 */
export function useCreatePrincipal(
  tenantId: string,
  onIssued: (issued: IssuedPrincipal) => void,
): UseMutationResult<Omit<IssuedPrincipal, "apiKey">, Error, PrincipalCreate> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body) => {
      const issued = applied(await apiSend<IssuedPrincipal>("POST", principalsPath(tenantId), body));
      onIssued(issued);
      return stripApiKey(issued);
    },
    onSuccess: (created) => {
      client.setQueryData<Principal[]>(adminPrincipalsKey(tenantId), (existing) =>
        existing === undefined
          ? existing
          : [...existing, { ...created, auth: "apiKey", disabled: false }],
      );
    },
  });
}

export function useSavePrincipal(): UseMutationResult<
  unknown,
  Error,
  { tenantId: string; principalId: string; body: PrincipalUpdate }
> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ tenantId, principalId, body }) =>
      apiSend("PUT", principalPath(tenantId, principalId), body).then(applied),
    onSettled: (_data, _error, vars) =>
      client.invalidateQueries({ queryKey: adminPrincipalsKey(vars.tenantId) }),
  });
}

export function useDeletePrincipal(): UseMutationResult<
  unknown,
  Error,
  { tenantId: string; principalId: string }
> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ tenantId, principalId }) =>
      apiSend("DELETE", principalPath(tenantId, principalId)).then(applied),
    onSettled: (_data, _error, vars) =>
      client.invalidateQueries({ queryKey: adminPrincipalsKey(vars.tenantId) }),
  });
}

export function usePutBinding(): UseMutationResult<
  unknown,
  Error,
  { tenantId: string; principalId: string; role: Role }
> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ tenantId, principalId, role }) =>
      apiSend("PUT", bindingPath(tenantId, principalId), { role }).then(applied),
    onSettled: (_data, _error, vars) =>
      client.invalidateQueries({ queryKey: adminPrincipalsKey(vars.tenantId) }),
  });
}

export function useDeleteBinding(): UseMutationResult<
  unknown,
  Error,
  { tenantId: string; principalId: string }
> {
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ tenantId, principalId }) =>
      apiSend("DELETE", bindingPath(tenantId, principalId)).then(applied),
    onSettled: (_data, _error, vars) =>
      client.invalidateQueries({ queryKey: adminPrincipalsKey(vars.tenantId) }),
  });
}

/**
 * `since` is caller-owned state (RFC-002 §8's cursor is client-driven), not derived here — the
 * screen advances it with `nextSince` once a page has rendered, and that decision does not belong
 * inside the hook that reads one page.
 */
/** The page size the audit viewer asks for. Exported so the pager can tell a short page from a full one. */
export const AUDIT_PAGE_SIZE = 100;

export function useAuditRows(tenant: string | null, since: number): UseQueryResult<AuditRow[]> {
  return useQuery({
    queryKey: ["admin-audit", tenant, since],
    queryFn: async () =>
      auditPage(readAuditRows(await apiGet<unknown>(auditPath(since, AUDIT_PAGE_SIZE), { tenant }))),
    ...POLLED,
  });
}
