import type { ApiPath } from "./client.ts";

/**
 * Every admin route C4 asks for, in one place and typed as `ApiPath` — `keyof paths` from the
 * generated contract. A route the contract does not publish will not typecheck here, which is the
 * cheap half of RFC-006 §11's "no field sourced from a UI-only endpoint".
 */
export const API_PATHS = {
  imposters: "/imposters",
  fleetMembers: "/_fleet/members",
  fleetHealth: "/_fleet/health",
  session: "/session",
  frontDoorRoutes: "/front-door/routes",
  specsCompile: "/specs/compile",
} as const satisfies Record<string, ApiPath>;

/** Path builders for the templated routes, so a port is interpolated in exactly one place. */
export const imposterPath = (port: number): string => `/imposters/${port}`;
export const lifecyclePath = (port: number, enabled: boolean): string =>
  `/imposters/${port}/${enabled ? "enable" : "disable"}`;
/**
 * One imposter's recorded requests, **on the node the browser reached** (D-74, #552).
 *
 * Upstream's own route, proxied verbatim to the embedded engine — there is no fleet-wide spelling
 * of this any more, and deliberately so: the request journal belongs to the engine, once, for every
 * deployment shape (RFC-007 §3.3). `?since=` here is upstream's **scalar** index, echoed back on
 * `x-rift-next-index`; nothing on this side parses either.
 */
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
 * proxies upstream to the embedded engine's own admin API.
 */
export const savedProxyResponsesPath = (port: number): string =>
  `/imposters/${port}/savedProxyResponses`;

/**
 * The recorded projection of one imposter: proxy stubs stripped out (`removeProxies`) and only the
 * stubs a recording could actually replay (`replayable`), in the flat response form the engine
 * emits for a captured request (`vendor/rift/docs/mountebank/proxy.md:190-224`).
 *
 * Not a member of `API_PATHS`/`ApiPath`: the contract declares `GET /imposters/{port}` with no query
 * parameters at all, so a templated string is the only way to reach this projection.
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

/** The poll target for a write that answered `202` (parked) — see `features/writes/commit.ts`. */
export const fleetOpPath = (opId: string): string =>
  `/_fleet/ops/${encodeURIComponent(opId)}`;
export const frontDoorRoutePath = (routeId: string): string =>
  `/front-door/routes/${encodeURIComponent(routeId)}`;

/**
 * `POST /specs/compile?port=<port>[&name=<name>]` — the one-shot OpenAPI compile (D-72).
 *
 * `port` is always present because the route requires it: there is no stored record to infer a
 * binding from, and a compiled imposter with no port is one the very next `POST /imposters` would
 * refuse. `name` is left off entirely when blank — the contract reads absent and empty the same,
 * and sending `name=` would only make a reader wonder which one was meant.
 */
export const compileSpecPath = (port: number, name: string): string => {
  const trimmed = name.trim();
  const query = new URLSearchParams({ port: String(port) });
  if (trimmed.length > 0) query.set("name", trimmed);
  return `${API_PATHS.specsCompile}?${query.toString()}`;
};
