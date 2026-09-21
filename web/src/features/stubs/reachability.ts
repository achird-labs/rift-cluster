import { type Route, effectiveOrder } from "../routes/order.ts";
import type { Sample } from "./sample.ts";

/**
 * Turning "this stub is on imposter port N" into an address a caller can actually dial (D-91).
 *
 * The imposter's own port is the obvious answer and is frequently the wrong one. An imposter port
 * is bound *inside* the node: under the reference compose deployment those ports are deliberately
 * not published at all, and under Kubernetes the pod's port is not the Service's. So the URL the
 * console used to offer — this page's host with the imposter's port — resolves to nothing from the
 * browser in exactly the deployments the front door exists to serve.
 *
 * The front door is the listener the operator deliberately exposed, and the replicated route table
 * says which paths it dispatches where. When a route targets this imposter, a request through the
 * front door reaches the same stub at an address that is actually routable.
 *
 * **Neither form is universally right**, which is why this module derives them independently and
 * the caller offers both. The direct port is what you want from inside the node, from a sidecar, or
 * when the port is published (a local test fleet usually publishes it); the front-door form is what
 * you want from anywhere else. Picking one on the operator's behalf would be guessing at their
 * vantage point.
 */

/** A dialable form of one sample request. */
export type Reach = {
  /** Scheme and authority, no trailing slash. */
  origin: string;
  /** Path plus query, ready to append to `origin`. */
  target: string;
  /**
   * Headers the *route* requires on top of the sample's own — a `Host` for a host-matched route,
   * and any header clauses the match declares. Never the sample's headers; the caller merges.
   */
  headers: { name: string; value: string }[];
  /** The route this went through, for naming it on screen. `null` for the direct form. */
  routeId: string | null;
  /** Reasons this address may still not resolve, in the operator's words. */
  caveats: string[];
};

/**
 * `0.0.0.0` and `[::]` are bind hosts, not destinations — a node reports the address it bound, and
 * "all interfaces" is not one a client can connect to. The host the browser already used to reach
 * this node demonstrably works, so the port is the only half worth taking.
 *
 * A specific bind host is kept: a node bound to one interface is telling you which one, and that is
 * more informative than the host the console happens to be served on.
 */
const WILDCARD_HOSTS = new Set(["0.0.0.0", "::", "[::]"]);

/**
 * Split a reported `host:port` into its halves.
 *
 * Returns `null` rather than guessing when there is no port to take: a malformed value is a node
 * reporting something this console does not understand, and inventing a port from it would produce
 * a command that fails for a reason the operator cannot see.
 */
export function splitAddr(addr: string): { host: string; port: string } | null {
  // IPv6 literals are bracketed (`[::1]:2527`), so the port is after the LAST colon in the
  // unbracketed case and after `]:` in the bracketed one. Splitting on the first colon would read
  // `::1` as host `` and port `:1`.
  const match = /^(\[[^\]]*\]|[^:]*):(\d+)$/.exec(addr);
  if (match === null) return null;
  const [, host, port] = match;
  if (host === undefined || port === undefined) return null;
  return { host, port };
}

/**
 * The origin to dial this node's front door on, given what it reported and where the page is.
 *
 * `null` when the node reported no front door (started without `--front-door`), or reported one
 * this console cannot parse.
 */
export function frontDoorOrigin(
  frontDoor: string | null,
  location: { protocol: string; hostname: string },
): string | null {
  if (frontDoor === null) return null;
  const parts = splitAddr(frontDoor);
  if (parts === null) return null;
  const host = WILDCARD_HOSTS.has(parts.host) ? location.hostname : parts.host;
  // The page's scheme, not a guess: the front door's own TLS setting is not reported, and a
  // console served over https is overwhelmingly behind a terminator that fronts both listeners.
  return `${location.protocol}//${host}:${parts.port}`;
}

/**
 * Whether the port the browser reached this node on is the port the node thinks it is on.
 *
 * A mismatch is proof of address translation between the two — a published container port, a
 * Service, an ingress — and it is the one piece of evidence the console can actually get. The node
 * reports its own `admin_port`; the browser knows what it dialled. Neither knows the mapping, but
 * the *existence* of one is decidable, and that is the difference between a caveat an operator can
 * act on and a disclaimer they will scroll past.
 *
 * `undefined` when the fleet read has not landed, the node predates the field, or the page's port
 * is not a number — all of which are "unknown", reported as the generic warning rather than as
 * "no translation".
 */
function portsAreTranslated(
  reportedAdminPort: number | null | undefined,
  location: { port?: string },
): boolean | undefined {
  if (reportedAdminPort === undefined || reportedAdminPort === null) return undefined;
  // An empty `location.port` is the scheme's default (80/443), which is a real port the node could
  // genuinely be on — resolve it rather than treating it as unknown.
  const reached = location.port === undefined || location.port === "" ? undefined : Number(location.port);
  if (reached === undefined || Number.isNaN(reached)) return undefined;
  return reached !== reportedAdminPort;
}

/**
 * What to say about whether this address survives the trip from the browser to the node.
 *
 * Three different sentences for three different states of knowledge, because "may not be reachable"
 * said unconditionally is noise an operator learns to ignore — and the one deployment where it is
 * *certainly* wrong is the one where it most needs to be read.
 */
function translationCaveat(
  reportedAdminPort: number | null | undefined,
  location: { port?: string },
): string {
  const translated = portsAreTranslated(reportedAdminPort, location);
  if (translated === true) {
    return (
      `this node reports its admin port as ${String(reportedAdminPort)} but you reached it on ` +
      `${location.port ?? "?"}, so ports are being translated between you and it — the front-door ` +
      "port in this command is the node's own, and yours will differ"
    );
  }
  if (translated === false) {
    return (
      "the front door's address is the one this node bound; you reached its admin port " +
      "untranslated, so this one is most likely direct too"
    );
  }
  return (
    "the front door's address is the one this node bound; a published container port, a " +
    "Service or a load balancer in front of it is a mapping the node is never told about"
  );
}

/** Does this route's method clause admit the sample's method? */
function methodAdmits(route: Route, method: string): boolean {
  const want = route.match?.method;
  if (want === undefined) return true;
  return want.toUpperCase() === method.toUpperCase();
}

/**
 * The front-door path that delivers `target` to the imposter, or `null` if this route cannot.
 *
 * The two halves of `strip_prefix` are not symmetric, and getting them backwards produces a
 * command that 404s:
 *
 * - **`strip_prefix: true`** — the front door removes the prefix before forwarding, so the imposter
 *   sees whatever followed it. To have it see `target`, send `prefix + target`. Any target works.
 * - **`strip_prefix: false`** — the imposter sees the whole path, prefix included. So the path to
 *   send *is* `target`, and this route can only deliver it when `target` already starts with the
 *   prefix. A stub on `/products` behind a non-stripping `/catalog` route is genuinely unreachable
 *   through that route, and saying so is the honest answer.
 */
function frontDoorPath(route: Route, target: string): string | null {
  const prefix = route.match?.path_prefix;
  if (prefix === undefined) return target;
  const stripped = route.target.strip_prefix ?? false;
  if (stripped) {
    // A trailing slash on the prefix plus a leading one on the target would send `//products`,
    // which is a different path to a segment-aligned matcher.
    return `${prefix.replace(/\/$/, "")}${target}`;
  }
  return target.startsWith(prefix) ? target : null;
}

/**
 * The best front-door form of `sample` for an imposter on `port`, or `null` when no route delivers
 * it and a direct address is all there is.
 *
 * Route choice follows `effectiveOrder`, which is the front door's own precedence ported — so the
 * route named here is the route that would actually take the request, not merely one that could.
 * Re-deriving the ordering would be a second implementation of it, free to drift.
 */
export function frontDoorReach(
  port: number,
  sample: Sample,
  routes: readonly Route[],
  frontDoor: string | null,
  location: { protocol: string; hostname: string; port?: string },
  reportedAdminPort?: number | null,
): Reach | null {
  const origin = frontDoorOrigin(frontDoor, location);
  if (origin === null) return null;

  for (const route of effectiveOrder(routes)) {
    // `enabled` defaults to true in the contract, so only an explicit `false` disables.
    if (route.enabled === false) continue;
    if (route.target.port !== port) continue;
    if (!methodAdmits(route, sample.method)) continue;
    const target = frontDoorPath(route, sample.target);
    if (target === null) continue;

    const headers: { name: string; value: string }[] = [];
    const host = route.match?.host;
    if (host !== undefined) {
      // A wildcard host clause (`*.example.test`) matches a family, not a name. Substituting the
      // literal `*.` would send a Host the matcher rejects, so a concrete label is filled in and
      // flagged — the operator knows which of their names is the real one, and this does not.
      headers.push({
        name: "Host",
        value: host.startsWith("*.") ? `any${host.slice(1)}` : host,
      });
    }
    for (const clause of route.match?.headers ?? []) {
      if (clause.name === undefined || clause.value === undefined) continue;
      headers.push({ name: clause.name, value: clause.value });
    }

    const caveats: string[] = [];
    if (host !== undefined && host.startsWith("*.")) {
      caveats.push(
        `route '${route.id}' matches the wildcard host ${host}; the Host header below uses a ` +
          "placeholder label — substitute a real one",
      );
    }
    caveats.push(translationCaveat(reportedAdminPort, location));

    return { origin, target, headers, routeId: route.id, caveats };
  }
  return null;
}

/**
 * The imposter's own port on the host this page was served from — the address the console has
 * always offered, now carrying the reason it may not resolve.
 */
export function directReach(
  port: number,
  sample: Sample,
  location: { protocol: string; hostname: string; port?: string },
  routed: boolean,
  reportedAdminPort?: number | null,
): Reach {
  return {
    origin: `${location.protocol}//${location.hostname}:${port}`,
    target: sample.target,
    headers: [],
    routeId: null,
    caveats: [
      routed
        ? `imposter port ${port} is bound inside the node and may not be published; the ` +
          "front-door form beside this one goes through a route instead"
        : `imposter port ${port} is bound inside the node and may not be published — under the ` +
          "reference compose deployment these ports are deliberately not exposed",
      // The same evidence, and it bears harder here: a translated admin port means this imposter
      // port is almost certainly translated too, if it is published at all.
      ...(portsAreTranslated(reportedAdminPort, location) === true
        ? [
            `this node reports its admin port as ${String(reportedAdminPort)} but you reached it ` +
              `on ${location.port ?? "?"}, so ports are being translated — ${port} is the port ` +
              "inside the node, not the one you would dial",
          ]
        : []),
    ],
  };
}
