import { useEffect, useState } from "react";

/** The screens C4 ships. Everything else in the nav is a planned entry with no route (see `nav.ts`). */
export type Route =
  | { screen: "imposters" }
  | { screen: "imposter"; port: number }
  | { screen: "cluster" };

const IMPOSTERS: Route = { screen: "imposters" };

/**
 * Hash routing rather than the History API, because the console is served from `/console/` by
 * `rust-embed` with an SPA fallback: a path-based route would be a real request the binary has to
 * recognise, and a hash never leaves the browser.
 */
export function parseHash(hash: string): Route {
  const segments = hash.replace(/^#\/?/, "").split("/").filter(Boolean);
  const [head, tail, ...rest] = segments;
  if (rest.length > 0) return IMPOSTERS;

  if (head === undefined || head === "imposters") {
    if (tail === undefined) return IMPOSTERS;
    const port = parsePort(tail);
    return port === null ? IMPOSTERS : { screen: "imposter", port };
  }
  if (head === "cluster" && tail === undefined) return { screen: "cluster" };

  // An unknown hash is a stale bookmark, not an error: the nav already says which screens are
  // unbuilt, so a 404 page would be a second, worse answer to a question already answered.
  return IMPOSTERS;
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
