import { QueryClientProvider } from "@tanstack/react-query";
import type { QueryClient } from "@tanstack/react-query";
import { cleanup, render } from "@testing-library/react";
import type { ReactElement } from "react";
import { afterEach, vi } from "vitest";

import type { components } from "../api/schema.ts";
import { createQueryClient } from "../app/query.ts";
import { selectableTenants } from "../app/rbac.ts";
import { SessionProvider } from "../app/session.tsx";

type WhoAmI = components["schemas"]["WhoAmI"];
type Role = components["schemas"]["Role"];

// Registered here rather than in a global `setupFiles`: only the jsdom-flavoured tests import this
// module, and `cleanup` needs a document. Without it, every test after the first queries a body
// still holding the previous test's DOM — so "there is no enable button" would pass or fail on
// whatever ran before it.
afterEach(cleanup);

/**
 * One route's canned reply: either a JSON body, or a status to fail with.
 *
 * `headers` exists for the reads whose *header* is load-bearing — `Rift-Cluster-Revision` is the
 * `If-Match` every write on the imposter screens is conditioned on, and a reply without it makes
 * those writes correctly refuse to send.
 */
export type Reply = ({ json: unknown; status?: number } | { status: number; json?: unknown }) & {
  headers?: Record<string, string>;
};

/**
 * A fetch double keyed by the *path* the console asks for, so a test states the fleet's answers
 * rather than the order of calls. An unmatched path is a hard failure: a screen quietly reaching
 * for a route the test never modelled is exactly the drift these tests exist to catch.
 *
 * Matching is exact first, then falls back to the path with its query string stripped — so a test
 * that only cares about a route's *base* answer (most of them) does not have to enumerate every
 * `?since=` cursor the request log might send, while a test that DOES care about a specific cursored
 * URL (the server-cursor tests) can still model that exact string and have it win over the fallback.
 * The failure-on-unmatched-path behaviour is unchanged: a path that matches neither is still a hard
 * failure, not a silent 404.
 */
/** One call as it was actually sent, for tests that assert on the verb or the payload. */
export type SentRequest = {
  path: string;
  method: string;
  body: BodyInit | null | undefined;
  headers: Record<string, string>;
};

export function stubFetch(routes: Record<string, Reply>): {
  calls: string[];
  requests: SentRequest[];
} {
  const calls: string[] = [];
  const requests: SentRequest[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const path = typeof input === "string" ? input : input.toString();
      calls.push(path);
      requests.push({
        path,
        method: init?.method ?? "GET",
        body: init?.body,
        // Normalised to a plain record so a test can assert on `If-Match` without caring whether
        // the caller passed a `Headers`, an array of pairs, or an object literal.
        headers: Object.fromEntries(new Headers(init?.headers).entries()),
      });
      const reply = routes[path] ?? routes[path.split("?")[0] ?? path];
      if (reply === undefined) {
        return Promise.reject(new Error(`test stub has no reply for ${path}`));
      }
      const status = reply.status ?? 200;
      // `null`, not `""`, for a bodyless reply: the Fetch spec forbids a body on 204/205/304, so
      // `new Response("", { status: 204 })` throws a TypeError and the stub fails in a way that
      // looks like the code under test rejecting. `response.text()` reads "" from either.
      const body = reply.json === undefined ? null : JSON.stringify(reply.json);
      return Promise.resolve(new Response(body, { status, headers: reply.headers ?? {} }));
    }),
  );
  return { calls, requests };
}

export function whoamiWith(role: Role, tenants: string[] = ["acme"]): WhoAmI {
  return {
    principalId: "p-test",
    authorizationDisabled: false,
    bindings: tenants.map((tenant) => ({ tenant, role })),
  };
}

/**
 * Renders `ui` inside the same providers `main.tsx` mounts — the real `createQueryClient()`, not a
 * test-local one. A test client with retries and polling disabled would pass while the shipped
 * configuration polled a hidden tab forever.
 */
export function renderInApp(
  ui: ReactElement,
  options: {
    whoami: WhoAmI;
    tenant?: string | null;
    tenants?: string[];
    /** Pass one in to inspect the caches afterwards; otherwise the real production client is used. */
    client?: QueryClient;
  } = {
    whoami: whoamiWith("fleet-admin"),
  },
): ReturnType<typeof render> {
  const client = options.client ?? createQueryClient();
  const tenants = options.tenants ?? [];
  // Defaults to a tenant the principal is actually bound to, because that is what `App` resolves to
  // and what the operator sees. Defaulting to `null` would model a session with no selection, which
  // lands in `default` — a tenant these fixtures are deliberately not bound to — so every test would
  // silently be exercising an unbound principal.
  const initialTenant =
    options.tenant === undefined ? (selectableTenants(options.whoami)[0] ?? null) : options.tenant;
  return render(
    <QueryClientProvider client={client}>
      <SessionProvider whoami={options.whoami} tenants={tenants} initialTenant={initialTenant}>
        {ui}
      </SessionProvider>
    </QueryClientProvider>,
  );
}

/** Drive the browser's own tab-visibility signal, which is what the polling gate listens to. */
export function setTabVisibility(state: "visible" | "hidden"): void {
  Object.defineProperty(document, "visibilityState", { value: state, configurable: true });
  Object.defineProperty(document, "hidden", { value: state === "hidden", configurable: true });
  document.dispatchEvent(new Event("visibilitychange"));
  window.dispatchEvent(new Event(state === "hidden" ? "blur" : "focus"));
}
