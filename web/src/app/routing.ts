import { useEffect, useState } from "react";

/** The four admin screens (RFC-002 §4), one route each so a bookmark or a "back" reaches the tab it left. */
export type AdminTab = "tenants" | "principals" | "bindings" | "audit";

/** The screens C4 ships. Everything else in the nav is a planned entry with no route (see `nav.ts`). */
export type Route =
  | { screen: "imposters" }
  | { screen: "imposter"; port: number }
  | { screen: "cluster" }
  /** `port: null` is "no imposter chosen yet", which the screen answers with a picker. */
  | { screen: "requests"; port: number | null }
  | { screen: "routes" }
  /** `tenant: null` is "no tenant chosen yet" — `tenants` alone still lists every tenant it may read. */
  | { screen: "admin"; tab: AdminTab; tenant: string | null };

const IMPOSTERS: Route = { screen: "imposters" };

/**
 * Hash routing rather than the History API, because the console is served from `/console/` by
 * `rust-embed` with an SPA fallback: a path-based route would be a real request the binary has to
 * recognise, and a hash never leaves the browser.
 */
export function parseHash(hash: string): Route {
  const segments = hash.replace(/^#\/?/, "").split("/").filter(Boolean);
  const [head, ...tail] = segments;

  if (head === "admin") return parseAdmin(tail);
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
  if (head === "requests") {
    if (second === undefined) return { screen: "requests", port: null };
    return { screen: "requests", port: parsePort(second) };
  }
  if (head === "routes" && second === undefined) return { screen: "routes" };

  // An unknown hash is a stale bookmark, not an error: the nav already says which screens are
  // unbuilt, so a 404 page would be a second, worse answer to a question already answered.
  return IMPOSTERS;
}

function parseAdmin(tail: string[]): Route {
  const [tabSegment, tenantSegment, ...rest] = tail;
  if (rest.length > 0) return IMPOSTERS;
  const tab = parseAdminTab(tabSegment);
  return tab === null ? IMPOSTERS : { screen: "admin", tab, tenant: tenantSegment ?? null };
}

function parseAdminTab(raw: string | undefined): AdminTab | null {
  return raw === "tenants" || raw === "principals" || raw === "bindings" || raw === "audit"
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
    case "requests":
      return route.port === null ? "#/requests" : `#/requests/${route.port}`;
    case "routes":
      return "#/routes";
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
