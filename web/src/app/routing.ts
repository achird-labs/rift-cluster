import { useEffect, useState } from "react";

/** The four admin screens (RFC-002 §4), one route each so a bookmark or a "back" reaches the tab it left. */
export type AdminTab = "tenants" | "principals" | "bindings" | "audit" | "sink";

/** The screens C4 ships. Everything else in the nav is a planned entry with no route (see `nav.ts`). */
export type Route =
  | { screen: "imposters" }
  | { screen: "imposter"; port: number }
  | { screen: "cluster" }
  /** Imposter sources, fleet-replicated. There is no per-source route — see `parseHash`. */
  | { screen: "sources" }
  /** `port: null` is "no imposter chosen yet", which the screen answers with a picker. */
  | { screen: "requests"; port: number | null }
  | { screen: "routes" }
  /**
   * Scenarios, spaces and flow state for one imposter, scoped to one space.
   *
   * `flow: null` is "the imposter's own default flow", not "every flow" — no route lists spaces, so
   * there is nothing an all-flows view could read. The screen sends no `flowId`, and the imposter
   * echoes back the one it resolved.
   */
  | { screen: "scenarios"; port: number | null; flow: string | null }
  /** `tenant: null` is "no tenant chosen yet" — `tenants` alone still lists every tenant it may read. */
  | { screen: "admin"; tab: AdminTab; tenant: string | null };

const IMPOSTERS: Route = { screen: "imposters" };

/**
 * Hash routing rather than the History API, because the console is served from `/console/` by
 * `rust-embed` with an SPA fallback: a path-based route would be a real request the binary has to
 * recognise, and a hash never leaves the browser.
 */
/**
 * Screen *state* — a filter, a sort — rides in a query string after the route segments, and is
 * deliberately NOT part of `Route`.
 *
 * `Route` answers "which screen, scoped to what", and every construction site of it (`nav.ts`, the
 * back-links, this module's own fallback) means the screen and nothing else. Widening the union with
 * a filter would make all of them state a filter they have no opinion about, and would give "the
 * default view" two spellings. So the two are split: `parseHash` ignores the query string entirely,
 * and a screen that has state reads it with `useHashQuery`.
 */
function splitQuery(hash: string): [string, string] {
  const index = hash.indexOf("?");
  return index === -1 ? [hash, ""] : [hash.slice(0, index), hash.slice(index + 1)];
}

export function parseHash(hash: string): Route {
  const [path] = splitQuery(hash);
  const segments = path.replace(/^#\/?/, "").split("/").filter(Boolean);
  const [head, ...tail] = segments;

  if (head === "admin") return parseAdmin(tail);
  if (head === "scenarios") return parseScenarios(tail);
  // Every other screen takes at most one more segment; a longer hash is a stale or hand-edited
  // bookmark, not a route any of them recognise.
  if (tail.length > 1) return IMPOSTERS;
  const [second] = tail;

  if (head === undefined || head === "imposters") {
    if (second === undefined) return IMPOSTERS;
    const port = parsePort(second);
    return port === null ? IMPOSTERS : { screen: "imposter", port };
  }
  if (head === "cluster" && second === undefined) return { screen: "cluster" };
  // No second segment: there is no per-source screen, so `#/sources/mocks` is a stale bookmark and
  // falls back like every other unrecognised longer hash.
  if (head === "sources" && second === undefined) return { screen: "sources" };
  if (head === "requests") {
    if (second === undefined) return { screen: "requests", port: null };
    return { screen: "requests", port: parsePort(second) };
  }
  if (head === "routes" && second === undefined) return { screen: "routes" };

  // An unknown hash is a stale bookmark, not an error: the nav already says which screens are
  // unbuilt, so a 404 page would be a second, worse answer to a question already answered.
  return IMPOSTERS;
}

/**
 * `#/scenarios`, `#/scenarios/:port`, `#/scenarios/:port/:flowId`.
 *
 * A bad port yields the fallback rather than "no imposter, this flow": the flow segment only means
 * anything relative to an imposter, so keeping it while dropping the port would render a screen
 * scoped to nothing.
 */
function parseScenarios(tail: string[]): Route {
  const [portSegment, flowSegment, ...rest] = tail;
  if (rest.length > 0) return IMPOSTERS;
  if (portSegment === undefined) return { screen: "scenarios", port: null, flow: null };
  const port = parsePort(portSegment);
  if (port === null) return IMPOSTERS;
  if (flowSegment === undefined) return { screen: "scenarios", port, flow: null };
  /*
   * Decoded because `toHash` encodes it: a flow id is operator-chosen and may carry a `/`, which
   * has to survive the round trip as one segment rather than becoming two.
   *
   * The `try` is load-bearing, not defensive habit. `decodeURIComponent` throws `URIError` on a
   * malformed escape — `#/scenarios/4545/%` or a pasted `100%discount` — and this runs inside
   * `useRoute`'s `useState` initializer and its `hashchange` listener. There is no ErrorBoundary in
   * this console, so an uncaught throw here paints nothing at all *and* leaves the listener
   * throwing, so the operator cannot navigate out of it in-app. Every other unparseable hash in
   * this module falls back to the imposters screen; a malformed escape is one more of those.
   */
  try {
    return { screen: "scenarios", port, flow: decodeURIComponent(flowSegment) };
  } catch {
    return IMPOSTERS;
  }
}

function parseAdmin(tail: string[]): Route {
  const [tabSegment, tenantSegment, ...rest] = tail;
  if (rest.length > 0) return IMPOSTERS;
  const tab = parseAdminTab(tabSegment);
  return tab === null ? IMPOSTERS : { screen: "admin", tab, tenant: tenantSegment ?? null };
}

function parseAdminTab(raw: string | undefined): AdminTab | null {
  return raw === "tenants" ||
    raw === "principals" ||
    raw === "bindings" ||
    raw === "audit" ||
    raw === "sink"
    ? raw
    : null;
}

/** A port, or `null` — including for input that `Number()` would happily coerce (`""`, `"4545.5"`). */
function parsePort(raw: string): number | null {
  if (!/^\d+$/.test(raw)) return null;
  const port = Number(raw);
  return port >= 1 && port <= 65535 ? port : null;
}

export function toHash(route: Route): string {
  switch (route.screen) {
    case "imposters":
      return "#/imposters";
    case "imposter":
      return `#/imposters/${route.port}`;
    case "cluster":
      return "#/cluster";
    case "sources":
      return "#/sources";
    case "requests":
      return route.port === null ? "#/requests" : `#/requests/${route.port}`;
    case "routes":
      return "#/routes";
    case "scenarios":
      if (route.port === null) return "#/scenarios";
      return route.flow === null
        ? `#/scenarios/${route.port}`
        : `#/scenarios/${route.port}/${encodeURIComponent(route.flow)}`;
    case "admin":
      return route.tenant === null
        ? `#/admin/${route.tab}`
        : `#/admin/${route.tab}/${route.tenant}`;
  }
}

export function useRoute(): Route {
  const [route, setRoute] = useState(() => parseHash(window.location.hash));
  useEffect(() => {
    const onChange = (): void => setRoute(parseHash(window.location.hash));
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return route;
}

/** The raw query string a hash carries, without its `?`. */
export function hashQuery(hash: string): string {
  return splitQuery(hash)[1];
}

/**
 * A hash with its query string replaced — the route segments left exactly as they were.
 *
 * An empty `query` drops the `?` rather than leaving a bare one, so returning a screen to its
 * default view produces the same URL it had before anyone touched a filter. A trailing `?` would
 * make "default" and "explicitly default" two different bookmarks of the same view.
 */
export function withHashQuery(hash: string, query: string): string {
  const [path] = splitQuery(hash);
  const base = path === "" ? "#/imposters" : path;
  return query === "" ? base : `${base}?${query}`;
}

/**
 * Screen state in the URL, kept in step with the address bar in both directions.
 *
 * Written with `replaceState` rather than by assigning `location.hash`: typing in a filter box
 * produces one URL per keystroke, and pushing each of them would make the browser Back button walk
 * backwards through the operator's typing instead of leaving the screen. `hashchange` does not fire
 * for `replaceState`, which is why the setter also updates local state directly.
 */
export function useHashQuery(): [string, (query: string) => void] {
  const [query, setQueryState] = useState(() => hashQuery(window.location.hash));

  useEffect(() => {
    const onChange = (): void => setQueryState(hashQuery(window.location.hash));
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);

  const setQuery = (next: string): void => {
    window.history.replaceState(null, "", withHashQuery(window.location.hash, next));
    setQueryState(next);
  };

  return [query, setQuery];
}
