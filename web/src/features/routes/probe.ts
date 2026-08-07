import { type Route, effectiveOrder } from "./order.ts";

/** A request to try against the table. Every field optional — an operator probes what they know. */
export type Probe = {
  host: string;
  path: string;
  method: string;
  headers: readonly { name: string; value: string }[];
};

/** Why one route did or did not take the request. */
export type TraceEntry = {
  id: string;
  hit: boolean;
  /** The first clause that failed, or the reason it won. */
  why: string;
};

export type ProbeResult = {
  /** The route the front door would dispatch to, or `null` when every route misses. */
  winner: Route | null;
  /** Every enabled route, in evaluation order, with its verdict. */
  trace: readonly TraceEntry[];
};

/**
 * A host clause matches exactly, or as a single leading wildcard label.
 *
 * `*.example.com` matches `api.example.com` and NOT `example.com` itself, nor `a.b.example.com` —
 * one label, mirroring `RouteMatch`'s own rule. Compared case-insensitively because hostnames are.
 */
function hostMatches(clause: string, host: string): boolean {
  const want = clause.toLowerCase();
  const got = host.toLowerCase();
  if (!want.startsWith("*.")) return want === got;
  const suffix = want.slice(1); // ".example.com"
  if (!got.endsWith(suffix)) return false;
  const label = got.slice(0, got.length - suffix.length);
  return label.length > 0 && !label.includes(".");
}

/**
 * A path prefix matches on **segment** boundaries.
 *
 * `/api` matches `/api` and `/api/v1`, and does not match `/apiary` — the front door routes by path
 * segment, and a substring match would send an unrelated service's traffic to the wrong imposter.
 */
function pathMatches(prefix: string, path: string): boolean {
  if (!path.startsWith(prefix)) return false;
  const rest = path.slice(prefix.length);
  return rest === "" || rest.startsWith("/") || prefix.endsWith("/");
}

/**
 * Evaluate a probe against the route table, the way the front door would.
 *
 * **This is the console's own reading, not the server's verdict.** There is no route-probe endpoint
 * to ask, so this walks the same total order `effectiveOrder` computes — which already mirrors
 * `Route::host_rank`, the UTF-8 prefix length and the byte-wise id tiebreak — and applies the match
 * clauses in the same way. The screen says so where it renders the result, because a tester that
 * silently disagreed with the front door would be worse than no tester: it would be trusted.
 *
 * Disabled routes never appear. They are not dispatched, so a trace line for one would suggest they
 * sit somewhere in the chain.
 */
export function probeRoutes(routes: readonly Route[], probe: Probe): ProbeResult {
  const ordered = effectiveOrder(routes);
  const trace: TraceEntry[] = [];
  let winner: Route | null = null;

  for (const route of ordered) {
    // Everything after the winner is unreachable for this request, so it is not evaluated and not
    // traced — saying "would have matched" about a route the front door never reaches is noise
    // dressed as information.
    if (winner !== null) break;

    const match = route.match ?? {};
    let why: string | null = null;

    if (match.host !== undefined && !hostMatches(match.host, probe.host)) {
      why = `host ${probe.host === "" ? "(none given)" : probe.host} is not ${match.host}`;
    } else if (match.path_prefix !== undefined && !pathMatches(match.path_prefix, probe.path)) {
      why = `path ${probe.path} is not under ${match.path_prefix}`;
    } else if (
      match.method !== undefined &&
      match.method.toUpperCase() !== probe.method.toUpperCase()
    ) {
      why = `method ${probe.method} is not ${match.method}`;
    } else {
      const missing = (match.headers ?? []).find((clause) => {
        const supplied = probe.headers.find(
          (header) => header.name.toLowerCase() === (clause.name ?? "").toLowerCase(),
        );
        return supplied === undefined || supplied.value !== clause.value;
      });
      if (missing !== undefined) {
        why = `header ${missing.name ?? "(unnamed)"}: ${missing.value ?? ""} not supplied`;
      }
    }

    if (why === null) {
      winner = route;
      trace.push({ id: route.id, hit: true, why: "every clause matched — dispatched here" });
    } else {
      trace.push({ id: route.id, hit: false, why });
    }
  }

  return { winner, trace };
}
