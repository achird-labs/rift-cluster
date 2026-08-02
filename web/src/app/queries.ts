import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { UseMutationResult, UseQueryResult } from "@tanstack/react-query";

import {
  ApiError,
  RawJsonBody,
  type RevisionedRead,
  type SendResult,
  apiGet,
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
  principalPath,
  principalsPath,
  requestsPath,
  stubByIdPath,
  stubsPath,
  tenantPath,
} from "../api/paths.ts";
import type { components } from "../api/schema.ts";
import { type AuditRow, auditPage, readAuditRows } from "../features/admin/audit.ts";
import { stripApiKey } from "../features/admin/key.ts";
import { type LogState, readLog } from "../features/requests/source.ts";
import { type Route, normalizeTable } from "../features/routes/order.ts";
import { type FleetView, fleetView } from "./fleetView.ts";
import { POLLED, POLLED_REQUESTS } from "./query.ts";
import { useSession } from "./session.tsx";

type Imposter = components["schemas"]["Imposter"];
type Stub = components["schemas"]["Stub"];
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

export function useImposters(): UseQueryResult<Imposter[]> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["imposters"], tenant),
    queryFn: async () => {
      const body = await apiGet<{ imposters?: Imposter[] }>(API_PATHS.imposters, { tenant });
      // `imposters` is optional in the contract, so an absent array is a shape the schema permits —
      // a domain-optional read, not a swallowed failure. A non-2xx has already thrown in `client`.
      return body.imposters ?? [];
    },
    ...POLLED,
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
        apiGet<FleetHealth>(API_PATHS.fleetHealth),
      ]);
      return fleetView(members, health);
    },
    enabled: options.enabled ?? true,
    /*
     * `polled: false` reads the fleet once per mount instead of every 5s. The request log needs
     * this reading only to name its coverage — which changes on membership events, not on traffic —
     * and it must ask even as a principal that will be refused, since the per-node label is the
     * screen's exit criterion. Polling it there would put two guaranteed 404s every 5s behind a
     * screen most roles use, for a sentence that would not change.
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
 * One node's recorded requests.
 *
 * A failed read resolves to `{ kind: "unknown" }` rather than rejecting, because on this screen the
 * two outcomes are different sentences and the query's own error state cannot tell them apart: an
 * empty array and an unreachable node both arrive here as "no rows to show". `readLog` is the only
 * place that decision is made.
 *
 * Resolving instead of rejecting opts this query out of `retryTransportFailures` — a `queryFn` that
 * never rejects is never retried. That is a deliberate trade and not a free one: a transient blip
 * shows the "unknown" alert immediately rather than after one silent retry. The 2s poll heals it on
 * the next tick, and on this screen an honest "could not read" for two seconds beats a retry that
 * delays the distinction this whole screen is built to preserve.
 */
export function useRequestLog(port: number): UseQueryResult<LogState> {
  const { tenant } = useSession();
  return useQuery({
    queryKey: key(["requests", port], tenant),
    queryFn: async (): Promise<LogState> => {
      try {
        return readLog(await apiGet<unknown>(requestsPath(port), { tenant }));
      } catch (error) {
        return {
          kind: "unknown",
          reason: error instanceof Error ? error.message : "this node could not be reached",
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
    onSettled: () => client.invalidateQueries({ queryKey: ["requests"] }),
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
