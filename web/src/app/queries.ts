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
import { keyedAttempt } from "../features/writes/idempotency.ts";
import {
  API_PATHS,
  frontDoorRoutePath,
  imposterPath,
  lifecyclePath,
  flowStateEntryPath,
  flowStatePath,
  recordedStubsPath,
  requestsPath,
  savedProxyResponsesPath,
  scenarioStatePath,
  scenariosPath,
  scenariosResetPath,
  spacePath,
  spaceStubsPath,
  spacesPath,
  stubByIdPath,
  stubsPath,
  tryImposterPath,
} from "../api/paths.ts";
import type { components } from "../api/schema.ts";
import {
  type Coverage,
  type RecordedRequest,
  coverageFor,
  readLog,
} from "../features/requests/source.ts";
import {
  type FlowStateRead,
  type ScenarioState,
  type SpaceListState,
  type SpaceState,
  readFlowStateEntry,
  readScenarios,
  readSpace,
  readSpaceList,
} from "../features/scenarios/space.ts";
import { type Route, normalizeTable } from "../features/routes/order.ts";
import { type FleetView, fleetView } from "./fleetView.ts";
import { POLLED, POLLED_REQUESTS } from "./query.ts";

type Imposter = components["schemas"]["Imposter"];
type Stub = components["schemas"]["Stub"];
/** The try envelope and its answer, both straight off the contract (#335). */
export type TrySpec = components["schemas"]["TryRequest"];
export type TryResult = components["schemas"]["TryResponse"];
type FleetMembers = components["schemas"]["FleetMembers"];
type FleetHealth = components["schemas"]["FleetHealth"];
type RouteTable = components["schemas"]["RouteTable"];
type FleetRequestPage = components["schemas"]["FleetRequestPage"];
export type FleetJournalCoverage = components["schemas"]["FleetJournalCoverage"];

/**
 * Every imposter the fleet serves, and whether the fleet sum on them is complete.
 *
 * `partial` is carried rather than dropped because `numberOfRequests` is a **fleet** figure
 * (issue #363): the front rewrites each entry's count to the sum across every node's slot for that
 * port, and stamps `Rift-Cluster-Partial` when a peer could not be reached inside the fan-out
 * budget. The sum is then a floor, not a total — and a floor presented as a total is the reading an
 * operator would act on.
 *
 * Carried beside the array rather than merged into it: the two facts have different scopes, and a
 * caller that does not care about coverage should have to ignore it explicitly rather than never
 * learn it exists.
 */
export type ImposterList = { imposters: Imposter[]; partial: boolean };

export function useImposters(): UseQueryResult<ImposterList> {
  return useQuery({
    queryKey: ["imposters"],
    queryFn: async (): Promise<ImposterList> => {
      const read = await apiGetMerged<{ imposters?: Imposter[] }>(API_PATHS.imposters);
      // `imposters` is optional in the contract, so an absent array is a shape the schema permits —
      // a domain-optional read, not a swallowed failure. A non-2xx has already thrown in `client`.
      return { imposters: read.data.imposters ?? [], partial: read.partial };
    },
    ...POLLED,
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
  return useMutation({
    mutationFn: async ({ request }) => {
      // Unkeyed (#389): the contract does not declare `Idempotency-Key` on this route, and
      // the fleet would ignore one — see `UNDECLARED` in features/writes/idempotency.ts.
      const sent = await apiSend<TryResult>("POST", tryImposterPath(port), request);
      return applied(sent);
    },
  });
}

/**
 * This node's own fleet reading.
 *
 * Always asked. It used to carry an `enabled` flag so a screen could decline to ask on behalf of a
 * principal whose role would only ever get a 404; #550 removed roles, so the read either lands or
 * says why, and every caller wants the same thing from it.
 */
export function useFleetView(options: { polled?: boolean } = {}): UseQueryResult<FleetView> {
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
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async ({ port, enable }) => {
      const sent = await keyed((idempotencyKey) =>
        apiSend("POST", lifecyclePath(port, enable), undefined, { idempotencyKey }),
      );
      const outcome = await settle(sent);
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
  const client = useQueryClient();
  const queryKey = ["requests", port];
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
        const merged = await apiGetMerged<unknown>(path);
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
  return useQuery({
    queryKey: ["imposter", port],
    queryFn: () => apiGetWithRevision<Imposter>(imposterPath(port)),
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
  send: (write: StubWrite, idempotencyKey: string) => Promise<SendResult<unknown>>,
): UseMutationResult<CommitOutcome, Error, StubWrite> {
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async (write) => {
      const conflict = async (): Promise<never> => {
        const fresh = await apiGetWithRevision<Imposter>(imposterPath(write.port));
        const theirs = (fresh.data.stubs ?? []).find((stub) => stub.id === write.stubId) ?? null;
        throw new StubConflict(theirs, fresh.revision);
      };
      try {
        const outcome = await settle(await keyed((idempotencyKey) => send(write, idempotencyKey)));
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
  return useStubWrite((write, idempotencyKey) =>
    apiSend("PUT", stubByIdPath(write.port, write.stubId), write.body, {
      ifMatch: write.revision,
      idempotencyKey,
    }),
  );
}

export function useDeleteStub(): UseMutationResult<CommitOutcome, Error, StubWrite> {
  return useStubWrite((write, idempotencyKey) =>
    apiSend("DELETE", stubByIdPath(write.port, write.stubId), undefined, {
      ifMatch: write.revision,
      idempotencyKey,
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
  return useStubWrite((write, idempotencyKey) =>
    apiSend("POST", stubsPath(write.port), addStubBody(write.body), {
      ifMatch: write.revision,
      idempotencyKey,
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
  return useQuery({
    queryKey: ["recorded-stubs", port],
    queryFn: async () => {
      const body = await apiGet<Imposter>(recordedStubsPath(port));
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
  return useStubWrite((write, idempotencyKey) =>
    apiSend("PUT", stubsPath(write.port), write.body, {
      ifMatch: write.revision,
      idempotencyKey,
    }),
  );
}

/**
 * Discard everything a recording has captured so far, without touching the proxy stub itself — the
 * imposter keeps recording; only what it has captured up to now is cleared.
 *
 * `DELETE .../savedProxyResponses` is not terminated by the admin front — it proxies upstream to
 * the embedded engine's own admin API.
 */
export function useDiscardRecording(): UseMutationResult<CommitOutcome, Error, { port: number }> {
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async ({ port }) => {
      const sent = await keyed((idempotencyKey) =>
        apiSend("DELETE", savedProxyResponsesPath(port), undefined, { idempotencyKey }),
      );
      const outcome = await settle(sent);
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
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async (body) => {
      const sent = await keyed((idempotencyKey) =>
        apiSend("POST", API_PATHS.imposters, body, { idempotencyKey }),
      );
      const outcome = await settle(sent);
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
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async (imposter) => {
      const sent = await keyed((idempotencyKey) =>
        apiSend("POST", API_PATHS.imposters, imposter, { idempotencyKey }),
      );
      const outcome = await settle(sent);
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["imposters"] }),
  });
}

/**
 * Replace the fleet's whole imposter set with an imported document (#251, "Replace all").
 *
 * The caller routes this through the destructive `Confirm` modal — every imposter this fleet
 * currently serves that the document does not name is gone once this lands, and that fact is not
 * visible from this hook.
 */
export function useReplaceImposters(): UseMutationResult<
  CommitOutcome,
  Error,
  { imposters: Record<string, unknown>[] }
> {
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async (body) => {
      const sent = await keyed((idempotencyKey) =>
        apiSend("PUT", API_PATHS.imposters, body, { idempotencyKey }),
      );
      const outcome = await settle(sent);
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["imposters"] }),
  });
}

/**
 * Delete an imposter, and everything hanging off it.
 *
 * Both caches are invalidated: the detail read is keyed by port, and leaving it would let a
 * back-navigation render a deleted imposter from cache as though it still existed.
 */
export function useDeleteImposter(): UseMutationResult<CommitOutcome, Error, { port: number }> {
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async ({ port }) => {
      const sent = await keyed((idempotencyKey) =>
        apiSend("DELETE", imposterPath(port), undefined, { idempotencyKey }),
      );
      const outcome = await settle(sent);
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
export function useClearRequests(): UseMutationResult<CommitOutcome, Error, { port: number }> {
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async ({ port }) => {
      const sent = await keyed((idempotencyKey) =>
        apiSend("DELETE", requestsPath(port), undefined, { idempotencyKey }),
      );
      const outcome = await settle(sent);
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
 * imposter as a prefix on the flow id rather than a choice to make first. This still fans out over
 * each imposter's *default* flow rather than enumerating every space across the fleet: `listSpaces`
 * (#374, see `useSpaces`) answers "which spaces does one imposter hold", not "every flow on every
 * imposter", and turning N single-imposter listings into one fleet-wide table is not what this hook
 * does today.
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
 * The admin front now does the merge itself: `GET /admin/requests` walks every imposter the fleet
 * serves and hands back one ordered page, so this hook is a single `apiGet` — no
 * `useQueries`, no per-port cap, no client-side union.
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
  return useQuery({
    queryKey: ["fleet-requests"],
    queryFn: async () => {
      const page = await apiGet<FleetRequestPage>(API_PATHS.fleetRequests);
      return { rows: [...page.requests].reverse(), coverage: page.coverage };
    },
    ...POLLED,
  });
}

export function useAllScenarios(
  ports: readonly number[],
): { rows: { port: number; state: ScenarioState }[]; pending: boolean } {
  return useQueries({
    queries: ports.map((port) => ({
      queryKey: ["scenarios", port, null],
      queryFn: async (): Promise<ScenarioState> => {
        try {
          return readScenarios(await apiGet<unknown>(scenariosPath(port, null)));
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
  return useQuery({
    queryKey: ["scenarios", port, flow],
    queryFn: async (): Promise<ScenarioState> => {
      try {
        return readScenarios(await apiGet<unknown>(scenariosPath(port, flow)));
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
  return useQuery({
    queryKey: ["space", port, flowId],
    queryFn: async (): Promise<SpaceState> => {
      // `enabled` below keeps this unreachable with a null flow; the guard is here so the type
      // narrows rather than being asserted away.
      if (flowId === null) return { kind: "unknown", reason: "no flow selected" };
      try {
        return readSpace(await apiGet<unknown>(spacePath(port, flowId)));
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
 * Every correlated-isolation space this imposter currently holds, fleet-wide (#374).
 *
 * Unlike `useSpace`, this needs no flow id: `listSpaces` is the union of every ring member's own
 * owned share, answered entirely from applied cluster state, so it is readable the moment a port is
 * known. Polled like the rest of this screen — a space list changes as traffic resolves new flow
 * ids and existing ones pick up entries, the same reasoning `useScenarios` and `useSpace` follow.
 */
export function useSpaces(port: number): UseQueryResult<SpaceListState> {
  return useQuery({
    queryKey: ["spaces", port],
    queryFn: async (): Promise<SpaceListState> => {
      try {
        return readSpaceList(await apiGet<unknown>(spacesPath(port)));
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
  return useQuery({
    queryKey: ["flow-state", port, flowId, entryKey],
    queryFn: async (): Promise<FlowStateRead> => {
      if (flowId === null || entryKey === null) {
        return { kind: "unknown", reason: "no key requested" };
      }
      try {
        return readFlowStateEntry(
          await apiGet<unknown>(flowStateEntryPath(port, flowId, entryKey)),
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
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async ({ port, name, state, flowId }) => {
      const body = flowId === null ? { state } : { state, flowId };
      const sent = await keyed((idempotencyKey) =>
        apiSend("PUT", scenarioStatePath(port, name), body, { idempotencyKey }),
      );
      const outcome = await settle(sent);
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
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async ({ port, flowId }) => {
      const sent = await keyed((idempotencyKey) =>
        apiSend(
          "POST",
          scenariosResetPath(port),
          flowId === null ? {} : { flowId },
          { idempotencyKey },
        ),
      );
      const outcome = await settle(sent);
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
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ port, flowId }) => {
      // Unkeyed (#389): the contract does not declare `Idempotency-Key` on this route, and
      // the fleet would ignore one — see `UNDECLARED` in features/writes/idempotency.ts.
      const sent = await apiSend("DELETE", spacePath(port, flowId), undefined);
      const outcome = await settle(sent);
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
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async ({ port, flowId, body }) => {
      const sent = await keyed((idempotencyKey) =>
        apiSend("POST", spaceStubsPath(port, flowId), body, { idempotencyKey }),
      );
      const outcome = await settle(sent);
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

/** Write one flow-state value. */
export function useSetFlowStateEntry(): UseMutationResult<
  CommitOutcome,
  Error,
  { port: number; flowId: string; key: string; body: RawJsonBody }
> {
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async ({ port, flowId, key: entryKey, body }) => {
      const sent = await keyed((idempotencyKey) =>
        apiSend("PUT", flowStateEntryPath(port, flowId, entryKey), body, {
          idempotencyKey,
        }),
      );
      const outcome = await settle(sent);
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
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async ({ port, flowId, key: entryKey }) => {
      const path =
        entryKey === undefined
          ? flowStatePath(port, flowId)
          : flowStateEntryPath(port, flowId, entryKey);
      const sent = await keyed((idempotencyKey) =>
        apiSend("DELETE", path, undefined, { idempotencyKey }),
      );
      const outcome = await settle(sent);
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["flow-state"] }),
  });
}

/**
 * The fleet's stored front-door table.
 *
 * Just the rows. `GET`/`PUT /front-door/routes` used to answer a `RouteTableView` — the rows plus
 * an `installed` boolean saying whether *this tenant's* table was compiled into the shared front
 * door (D-68). #550 left one fleet-wide table, so every stored route is installed and the flag,
 * the view schema and the console's whole not-installed treatment went with it.
 */
export function useRouteTable(): UseQueryResult<Route[]> {
  return useQuery({
    queryKey: ["front-door-routes"],
    queryFn: async (): Promise<Route[]> =>
      normalizeTable(await apiGet<RouteTable>(API_PATHS.frontDoorRoutes)),
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
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async ({ draft, base }) => {
      const current = normalizeTable(await apiGet<RouteTable>(API_PATHS.frontDoorRoutes));
      if (JSON.stringify(current) !== JSON.stringify(base)) {
        throw new RouteTableConflict(current);
      }
      const sent = await keyed((idempotencyKey) =>
        apiSend<RouteTable>(
          "PUT",
          API_PATHS.frontDoorRoutes,
          { routes: draft },
          { idempotencyKey },
        ),
      );
      const outcome = await settle(sent);
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
      client.setQueryData(["front-door-routes"], () => normalizeTable(stored));
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
  const client = useQueryClient();
  const keyed = keyedAttempt();
  return useMutation({
    mutationFn: async ({ routeId }) => {
      const sent = await keyed((idempotencyKey) =>
        apiSend("DELETE", frontDoorRoutePath(routeId), undefined, { idempotencyKey }),
      );
      const outcome = await settle(sent);
      if (outcome.kind === "failed") throw new Error(outcome.detail);
      return outcome;
    },
    onSettled: () => client.invalidateQueries({ queryKey: ["front-door-routes"] }),
  });
}
