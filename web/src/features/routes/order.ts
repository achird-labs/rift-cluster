import type { components } from "../../api/schema.ts";

/**
 * The front door's ordering and validation rules, ported from
 * `vendor/rift/crates/rift-http-proxy/src/front_door/route_table.rs`.
 *
 * Ported rather than fetched because there is no endpoint that returns either answer: the editor
 * has to show the evaluation order of a table the operator is still editing, and it has to say why
 * a table will be refused *before* sending it. Both are questions about a draft that does not exist
 * on the server yet.
 *
 * The server remains the authority. Everything here is advisory — when the two disagree, the
 * fleet's refusal is what the screen shows (see `Routes.tsx`).
 */

export type Route = components["schemas"]["Route"];
export type RouteTable = components["schemas"]["RouteTable"];

/**
 * Lower sorts earlier. Mirrors `Route::host_rank`: an exact host is more specific than a wildcard,
 * which is more specific than no host clause at all.
 */
function hostRank(route: Route): number {
  const host = route.match?.host;
  if (host === undefined) return 2;
  return host.startsWith("*.") ? 1 : 0;
}

const UTF8 = new TextEncoder();

/**
 * Rust's `String::len()` counts **UTF-8 bytes**; JavaScript's `.length` counts UTF-16 code units.
 * They agree on ASCII and diverge everywhere else — `/é` is 3 bytes but 2 units, so a naive port
 * ranks it against `/ab` differently from the front door. This screen exists to show the real
 * evaluation order, so it measures the way the server does.
 */
function pathPrefixLength(route: Route): number {
  const prefix = route.match?.path_prefix;
  return prefix === undefined ? 0 : UTF8.encode(prefix).length;
}

/**
 * Rust's `String: Ord` compares **UTF-8 bytes**; JavaScript's `<` compares UTF-16 code units, which
 * order differently above the BMP (a surrogate pair sorts below `U+FFFD` in UTF-16 and above it in
 * UTF-8). This is the final tiebreak, so getting it wrong reorders the table.
 */
function compareIds(a: string, b: string): number {
  const left = UTF8.encode(a);
  const right = UTF8.encode(b);
  const shared = Math.min(left.length, right.length);
  for (let i = 0; i < shared; i += 1) {
    const diff = (left[i] ?? 0) - (right[i] ?? 0);
    if (diff !== 0) return diff;
  }
  return left.length - right.length;
}

function headerCount(route: Route): number {
  return route.match?.headers?.length ?? 0;
}

/**
 * The routes in the order the front door evaluates them: a **total** order computed from the routes
 * alone, so the same table always resolves the same way no matter what order it arrived in.
 *
 * Disabled routes are excluded rather than sorted last — they are not dispatched at all, and giving
 * them a rank would suggest they sit somewhere in the chain.
 */
export function effectiveOrder(routes: readonly Route[]): Route[] {
  return routes
    .filter((route) => route.enabled)
    .slice()
    .sort(
      (a, b) =>
        b.priority - a.priority ||
        hostRank(a) - hostRank(b) ||
        pathPrefixLength(b) - pathPrefixLength(a) ||
        headerCount(b) - headerCount(a) ||
        compareIds(a.id, b.id),
    );
}

export type RouteErrorKind =
  | "EmptyId"
  | "DuplicateId"
  | "AmbiguousMatch"
  | "StripWithoutPrefix"
  | "MalformedHost"
  | "MalformedPathPrefix"
  | "MalformedMethod";

export type RouteError = { kind: RouteErrorKind; message: string };

/**
 * A valid HTTP method is any non-empty RFC 9110 token, which is what `hyper::Method` accepts.
 * Restricting this to the familiar verbs would refuse extension methods the fleet allows.
 */
const HTTP_TOKEN = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

function validateRoute(route: Route): RouteError | null {
  if (route.target.strip_prefix && route.match?.path_prefix === undefined) {
    return {
      kind: "StripWithoutPrefix",
      message: `route '${route.id}' sets strip_prefix but has no path_prefix to strip`,
    };
  }
  const host = route.match?.host;
  if (host !== undefined) {
    const rest = host.startsWith("*.") ? host.slice(2) : host;
    if (rest.includes("*") || rest.length === 0) {
      return {
        kind: "MalformedHost",
        message: `route '${route.id}' has host '${host}': a wildcard is one leading '*.' label, and nothing else may contain '*'`,
      };
    }
  }
  const prefix = route.match?.path_prefix;
  if (prefix !== undefined && !prefix.startsWith("/")) {
    return {
      kind: "MalformedPathPrefix",
      message: `route '${route.id}' has path_prefix '${prefix}', which must start with '/'`,
    };
  }
  const method = route.match?.method;
  if (method !== undefined && !HTTP_TOKEN.test(method)) {
    return {
      kind: "MalformedMethod",
      message: `route '${route.id}' has method '${method}', which is not a valid HTTP method`,
    };
  }
  return null;
}

/**
 * Do two routes match exactly the same requests?
 *
 * Mirrors the server's `first.matches == second.matches`, which is a derived `PartialEq` over
 * `RouteMatch` — and its `headers: Vec<HeaderMatch>` compares **in declaration order**. Sorting the
 * header list here would make the mirror *stricter* than the server: two routes carrying the same
 * clauses in different orders are distinct to the fleet, and calling them ambiguous would refuse a
 * table the fleet accepts.
 */
function sameMatch(a: Route, b: Route): boolean {
  const left = a.match ?? {};
  const right = b.match ?? {};
  if (
    (left.host ?? null) !== (right.host ?? null) ||
    (left.path_prefix ?? null) !== (right.path_prefix ?? null) ||
    (left.method ?? null) !== (right.method ?? null)
  ) {
    return false;
  }
  const leftHeaders = left.headers ?? [];
  const rightHeaders = right.headers ?? [];
  if (leftHeaders.length !== rightHeaders.length) return false;
  return leftHeaders.every((header, index) => {
    const other = rightHeaders[index];
    return (header.name ?? null) === (other?.name ?? null) &&
      (header.value ?? null) === (other?.value ?? null);
  });
}

/**
 * Every reason the server would refuse this table, as a whole.
 *
 * Partial acceptance is not offered by the server and is not offered here: a half-applied routing
 * table is a topology nobody designed. Returning every error rather than the first lets the editor
 * show the operator all of them in one pass.
 */
export function validateTable(routes: readonly Route[]): RouteError[] {
  const errors: RouteError[] = [];
  const seen = new Set<string>();

  for (const route of routes) {
    if (route.id.length === 0) {
      errors.push({ kind: "EmptyId", message: "route id must not be empty" });
      continue;
    }
    if (seen.has(route.id)) {
      errors.push({ kind: "DuplicateId", message: `duplicate route id '${route.id}'` });
    }
    seen.add(route.id);
    const error = validateRoute(route);
    if (error !== null) errors.push(error);
  }

  // Ambiguity is only a problem between routes that can both win. Two disabled twins, or an enabled
  // route beside its disabled spare, are how people stage a change.
  const enabled = routes.filter((route) => route.enabled);
  for (let i = 0; i < enabled.length; i += 1) {
    for (let j = i + 1; j < enabled.length; j += 1) {
      const first = enabled[i];
      const second = enabled[j];
      if (first === undefined || second === undefined) continue;
      if (first.priority === second.priority && sameMatch(first, second)) {
        errors.push({
          kind: "AmbiguousMatch",
          message: `routes '${first.id}' and '${second.id}' are both enabled and match exactly the same requests; give one of them a narrower match, or disable one`,
        });
      }
    }
  }
  return errors;
}

/**
 * Fill the fields the wire may omit.
 *
 * `priority`, `enabled` and `strip_prefix` all carry serde defaults on the server, so a stored table
 * can legitimately arrive without them. Defaulting on read keeps every comparison and sort in this
 * module total, instead of scattering `?? true` across the screen.
 */
export function normalizeTable(table: RouteTable | null | undefined): Route[] {
  return (table?.routes ?? []).map((route) => ({
    ...route,
    priority: route.priority ?? 0,
    enabled: route.enabled ?? true,
    target: { ...route.target, strip_prefix: route.target?.strip_prefix ?? false },
  }));
}

/** Why a route sits where it does — the "why is my route not winning" answer, in one line. */
export function orderReason(route: Route): string {
  const parts = [`priority ${route.priority}`];
  const host = route.match?.host;
  parts.push(
    host === undefined
      ? "no host clause"
      : host.startsWith("*.")
        ? `wildcard host ${host}`
        : `exact host ${host}`,
  );
  const prefix = route.match?.path_prefix;
  if (prefix !== undefined) {
    parts.push(`path prefix ${prefix} (${pathPrefixLength(route)} bytes)`);
  }
  const headers = headerCount(route);
  if (headers > 0) parts.push(`${headers} header clause${headers === 1 ? "" : "s"}`);
  parts.push(`id ${route.id}`);
  return parts.join(" → ");
}
