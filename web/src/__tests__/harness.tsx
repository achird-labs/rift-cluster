import { QueryClientProvider } from "@tanstack/react-query";
import type { QueryClient } from "@tanstack/react-query";
import { cleanup, render } from "@testing-library/react";
import type { ReactElement } from "react";
import { afterEach, vi } from "vitest";

import { createQueryClient } from "../app/query.ts";

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
 *
 * A key may also be written `"<METHOD> <path>"`, and that form wins over the bare path. `/imposters`
 * is both the listing and the create, and until a test could say which it meant, one modelling the
 * create had to either break the listing or key its reply under a path the console never asks for —
 * `"/imposters "`, with a trailing space, which silently matched nothing and let the create fall
 * through to whatever the listing answered. A `202` create or a refusing one cannot be modelled at
 * all that way.
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
      const method = init?.method ?? "GET";
      calls.push(path);
      requests.push({
        path,
        method,
        body: init?.body,
        // Normalised to a plain record so a test can assert on `If-Match` without caring whether
        // the caller passed a `Headers`, an array of pairs, or an object literal.
        headers: Object.fromEntries(new Headers(init?.headers).entries()),
      });
      const base = path.split("?")[0] ?? path;
      const reply =
        routes[`${method} ${path}`] ??
        routes[`${method} ${base}`] ??
        routes[path] ??
        routes[base];
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

/**
 * Renders `ui` inside the same providers `main.tsx` mounts — the real `createQueryClient()`, not a
 * test-local one. A test client with retries and polling disabled would pass while the shipped
 * configuration polled a hidden tab forever.
 *
 * No session wrapper since #550: there is one credential and one identity, so a screen has no
 * per-principal behaviour left to model. `App` decides signed-in-or-not and mounts `Shell`; a
 * screen rendered here is already past that decision, exactly as it is in the running console.
 */
export function renderInApp(
  ui: ReactElement,
  options: {
    /** Pass one in to inspect the caches afterwards; otherwise the real production client is used. */
    client?: QueryClient;
  } = {},
): ReturnType<typeof render> {
  const client = options.client ?? createQueryClient();
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

/** Drive the browser's own tab-visibility signal, which is what the polling gate listens to. */
export function setTabVisibility(state: "visible" | "hidden"): void {
  Object.defineProperty(document, "visibilityState", { value: state, configurable: true });
  Object.defineProperty(document, "hidden", { value: state === "hidden", configurable: true });
  document.dispatchEvent(new Event("visibilitychange"));
  window.dispatchEvent(new Event(state === "hidden" ? "blur" : "focus"));
}

/**
 * Put the imposter detail on one of its tabs before rendering.
 *
 * The tab lives in the hash query so it is linkable, which means a test that wants the Settings
 * panel sets the hash rather than clicking through — the same way a bookmark would arrive on it.
 * Cleared by the same `window.location.hash = ""` the suites already run in `afterEach`.
 */
export function onDetailTab(tab: "stubs" | "requests" | "ownership" | "settings"): void {
  window.location.hash = tab === "stubs" ? "#/" : `#/?tab=${tab}`;
}
