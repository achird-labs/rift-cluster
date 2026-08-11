import type { ApiPath } from "./client.ts";

/**
 * Every admin route C4 asks for, in one place and typed as `ApiPath` — `keyof paths` from the
 * generated contract. A route the contract does not publish will not typecheck here, which is the
 * cheap half of RFC-006 §11's "no field sourced from a UI-only endpoint".
 */
export const API_PATHS = {
  whoami: "/admin/whoami",
  imposters: "/imposters",
  tenants: "/admin/tenants",
  fleetMembers: "/_fleet/members",
  fleetHealth: "/_fleet/health",
  session: "/session",
  frontDoorRoutes: "/front-door/routes",
  frontDoorRouteHits: "/front-door/route-hits",
  audit: "/admin/audit",
  /**
   * Where the fleet ships its audit rows. Fleet-scoped: a `TenantAdmin` trusted to read their own
   * tenant's rows is not thereby trusted to see — or redirect — where every tenant's rows go.
   */
  auditSink: "/admin/audit/sink",
  /**
   * The tenant's declared imposter sources, with this node's poll status kept structurally apart.
   * No member-path builder: the screen has no per-source route, so `#/sources/mocks` falls back
   * (see `routing.ts`).
   */
  sources: "/admin/sources",
  /**
   * The tenant's recorded requests across every imposter, merged server-side (#362) — replaces the
   * console's own N-way fan-out over `requestsPath(port)`. One read, one cursor, one stated coverage.
   */
  fleetRequests: "/admin/requests",
} as const satisfies Record<string, ApiPath>;

/** Path builders for the templated routes, so a port is interpolated in exactly one place. */
export const imposterPath = (port: number): string => `/imposters/${port}`;
export const lifecyclePath = (port: number, enabled: boolean): string =>
  `/imposters/${port}/${enabled ? "enable" : "disable"}`;
export const requestsPath = (port: number): string => `/imposters/${port}/requests`;

/** The stub collection. `POST` here appends a stub; it takes `If-Match` like the by-id writes do. */
export const stubsPath = (port: number): string => `/imposters/${port}/stubs`;

/**
 * Ask the server to send a sample request to this imposter and report what it answered (#335).
 *
 * Under `/admin/imposters/` rather than the canonical `/imposters/` prefix: the latter is
 * Mountebank's published imposter surface, and this is an EE-only affordance the admin front
 * terminates itself. Note what the path does *not* carry — no host, no scheme: the port is the
 * whole address, which is what keeps this from being a general-purpose fetch.
 */
export const tryImposterPath = (port: number): string => `/admin/imposters/${port}/try`;

/**
 * What a recording has captured, cleared. `DELETE` here is not terminated by the admin front — it
 * proxies upstream, and `principal.rs::map_action` folds it onto `Action::SavedRequestsClear`
 * alongside `savedRequests` and `requests` (RFC-002 §4.1) — see `rbac.ts`'s `requests.clear` for the
 * capability this authorizes against.
 */
export const savedProxyResponsesPath = (port: number): string =>
  `/imposters/${port}/savedProxyResponses`;

/**
 * The recorded projection of one imposter: proxy stubs stripped out (`removeProxies`) and only the
 * stubs a recording could actually replay (`replayable`), in the flat response form the engine
 * emits for a captured request (`vendor/rift/docs/mountebank/proxy.md:190-224`).
 *
 * Not a member of `API_PATHS`/`ApiPath`: the contract declares `GET /imposters/{port}` with no query
 * parameters at all, so a templated string is the only way to reach this projection, the same as
 * `auditPath` below.
 */
export const recordedStubsPath = (port: number): string =>
  `${imposterPath(port)}?replayable=true&removeProxies=true`;

/**
 * Scenario states for one imposter, read under one space.
 *
 * `flowId` rides in the **query string**, not the path — a separate contract parameter from the
 * `flowId` the space routes take in their path, and the two are not interchangeable. Omitted, the
 * imposter resolves its own default flow and says which one it used in the response; that echo is
 * what the screen displays, rather than guessing the word "default".
 */
export const scenariosPath = (port: number, flowId: string | null): string =>
  flowId === null
    ? `/imposters/${port}/scenarios`
    : `/imposters/${port}/scenarios?flowId=${encodeURIComponent(flowId)}`;

/** One scenario's state. The name is operator-chosen and reaches a path segment, so it is encoded. */
export const scenarioStatePath = (port: number, scenarioName: string): string =>
  `/imposters/${port}/scenarios/${encodeURIComponent(scenarioName)}/state`;

export const scenariosResetPath = (port: number): string => `/imposters/${port}/scenarios/reset`;

/**
 * Every correlated-isolation space this imposter currently holds, fleet-wide (#374).
 *
 * Deliberately the collection root, not a member path: `spacePath` addresses one space by the flow
 * id a caller already knows, and this addresses "which flow ids exist", which is EE-only — there is
 * no upstream route for this shape at all.
 */
export const spacesPath = (port: number): string => `/imposters/${port}/spaces`;

/** One correlated-isolation space, addressed by its flow id. */
export const spacePath = (port: number, flowId: string): string =>
  `/imposters/${port}/spaces/${encodeURIComponent(flowId)}`;

/**
 * The stubs scoped to one space.
 *
 * Not a variant of `stubsPath`: these stubs belong to the space and never appear on the imposter's
 * own stub list, so the two routes address different collections that happen to share a noun.
 */
export const spaceStubsPath = (port: number, flowId: string): string =>
  `${spacePath(port, flowId)}/stubs`;

/**
 * A space's whole flow-state scratchpad — the only route that addresses it collectively, and it is
 * a `DELETE`. There is deliberately no list builder here because the contract publishes no route
 * that lists a flow's entries: they are addressed one key at a time.
 */
export const flowStatePath = (port: number, flowId: string): string =>
  `/admin/imposters/${port}/flow-state/${encodeURIComponent(flowId)}`;

export const flowStateEntryPath = (port: number, flowId: string, key: string): string =>
  `${flowStatePath(port, flowId)}/${encodeURIComponent(key)}`;

/**
 * One stub, addressed by its stable id — the **only** way this console writes a stub.
 *
 * The contract also publishes `/imposters/{port}/stubs/{stubIndex}`, and it is deliberately not
 * built here. An index is a position, not an address: a concurrent edit that inserts or removes a
 * stub shifts every index after it, so an index-addressed write racing that edit silently replaces
 * a *different* stub — with a `200` and no way to notice. The by-id routes commit a `PatchStubs`
 * edit carrying only the touched stub, so they are unaffected. `contract-traceability.test.ts`
 * asserts no index-addressed stub route appears anywhere in `web/src`.
 */
export const stubByIdPath = (port: number, stubId: string): string =>
  `/imposters/${port}/stubs/by-id/${encodeURIComponent(stubId)}`;

/**
 * The poll target for a write that answered `202` (parked). Fleet-scoped, so a caller without
 * fleet-admin sees a 404 here whatever the write actually did — see `features/writes/commit.ts`.
 */
export const fleetOpPath = (opId: string): string =>
  `/_fleet/ops/${encodeURIComponent(opId)}`;
export const frontDoorRoutePath = (routeId: string): string =>
  `/front-door/routes/${encodeURIComponent(routeId)}`;

/**
 * The admin plane addresses a tenant through the path, never `X-Rift-Tenant` — every
 * `/admin/tenants/*` route below takes `tenantId` as a path segment, so these calls send no tenant
 * header at all.
 */
export const tenantPath = (tenantId: string): string =>
  `/admin/tenants/${encodeURIComponent(tenantId)}`;
export const principalsPath = (tenantId: string): string => `${tenantPath(tenantId)}/principals`;

/*
 * The principal id goes in **raw**, deliberately — this is not a missing `encodeURIComponent`.
 *
 * `tenancy::classify` splits `req.uri().path()` as hyper delivers it, and hyper does not normalise
 * (see the note in `console.rs`), so the server compares the percent-encoded text literally. Every
 * console-minted principal is `key:<sha256-hex>` (`api_key_principal_id`), and encoding it to
 * `key%3A…` matches no stored principal: disable, delete and every binding write answer 404/400.
 * Tenant ids are safe to encode only because they are constrained to `[a-z0-9-]`.
 */
export const principalPath = (tenantId: string, principalId: string): string =>
  `${principalsPath(tenantId)}/${principalId}`;
export const bindingPath = (tenantId: string, principalId: string): string =>
  `${tenantPath(tenantId)}/bindings/${principalId}`;

/**
 * Can this principal id be addressed in a URL path at all?
 *
 * The server matches the raw path, so the id cannot be encoded — and that leaves ids the console
 * simply cannot target. `require_principal_id` permits any non-control string up to 256 bytes
 * (deliberately wide, to hold an OIDC `subject`), so ids arrive here that a URL will mangle:
 *
 * - `#` and `?` **silently truncate** the path — `alice#bob` addresses `alice`, so "Delete
 *   alice#bob" would delete a *different, existing* principal with no error to correlate. This is
 *   the case that makes the check load-bearing rather than cosmetic.
 * - `/` adds a segment, so `classify` stops matching and the request falls through to a 404.
 * - whitespace and non-ASCII get encoded by the URL parser and then match nothing.
 *
 * Everything else in the RFC 3986 `pchar` set — including the `:` every minted id carries — is safe
 * unencoded. Callers use this to render a write control inert with a stated reason rather than
 * offering an action that would hit the wrong record.
 */
const PATH_SAFE_ID = /^[A-Za-z0-9\-._~:@!$&'()*+,;=%]+$/;

export const isAddressablePrincipalId = (principalId: string): boolean =>
  PATH_SAFE_ID.test(principalId);
export const auditPath = (since: number, limit: number): string =>
  `${API_PATHS.audit}?since=${since}&limit=${limit}`;
