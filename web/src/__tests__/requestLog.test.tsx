/** @vitest-environment jsdom */
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { type ReactNode, useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { REQUEST_POLL_INTERVAL_MS } from "../app/query.ts";
import { RequestLog } from "../screens/RequestLog.tsx";
import { renderInApp, setTabVisibility, stubFetch, whoamiWith } from "./harness.tsx";

const PORT = 4545;
const REQUESTS = `/imposters/${PORT}/requests`;

const THREE_NODE = {
  "/_fleet/members": {
    json: { node_id: 3, is_leader: false, current_leader: 1, last_applied: 12, voters: [1, 2, 3] },
  },
  "/_fleet/health": {
    json: {
      ready: true,
      state: "ready",
      pending_gates: [],
      isolated: false,
      ring: { m_idx: 4, members: [1, 2, 3] },
    },
  },
};

const SINGLE_NODE = {
  "/_fleet/members": {
    json: { node_id: 1, is_leader: true, current_leader: 1, last_applied: 3, voters: [1] },
  },
  "/_fleet/health": {
    json: {
      ready: true,
      state: "ready",
      pending_gates: [],
      isolated: false,
      ring: { m_idx: 1, members: [1] },
    },
  },
};

function recorded(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    requestFrom: "127.0.0.1:5000",
    method: "GET",
    path: "/v1/payments/status",
    query: {},
    // A single-valued header is a **bare string** on the wire, not a one-element array
    // (`multi_value_headers::serialize`). The array shape the server only emits for multi-valued
    // headers hid a crash in `formatHeaders`, so the common case is the fixture's default.
    headers: { "user-agent": "curl/8" },
    timestamp: "2026-07-31T10:00:00Z",
    ...overrides,
  };
}

beforeEach(() => {
  setTabVisibility("visible");
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  setTabVisibility("visible");
});

describe("the merged journal (#147 H — the epic's exit criterion)", () => {
  // #189 shipped this screen deliberately per-node-labelled and said the label would disappear on
  // its own once the merged journal landed. The epic's words: "M3 does not close with the console
  // still saying per-node." So the assertion is that the element is **absent**, not that its text
  // changed — an empty-but-present label would satisfy a weaker test and still say nothing true.
  it("renders no scope label at all when the merge reached every node", async () => {
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    // Wait for the rows, so this cannot pass merely by asserting on a screen that has not
    // rendered yet — the failure mode a bare `queryByTestId` null-check invites.
    expect(await screen.findByText("/v1/payments/status")).toBeTruthy();
    expect(screen.queryByTestId("request-scope-label")).toBeNull();
  });

  // A one-voter fleet used to get its own "this node is the whole fleet" copy. It is now simply a
  // complete merge like any other, so it must be label-free too rather than keeping a special case.
  it("renders no scope label on a single-node fleet either", async () => {
    stubFetch({ ...SINGLE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    expect(await screen.findByText("/v1/payments/status")).toBeTruthy();
    expect(screen.queryByTestId("request-scope-label")).toBeNull();
  });

  // The label does not vanish — it changes meaning, from "this is one node" to "this merge was
  // incomplete". Same testid, same role: the plumbing is what #189 built for this handover.
  it("shows the partial-merge label when the server stamps Rift-Cluster-Partial", async () => {
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: { json: [recorded()], headers: { "rift-cluster-partial": "true" } },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    const label = await screen.findByTestId("request-scope-label");
    expect(label.getAttribute("role")).toBe("status");
    // Says the merge was incomplete, and says nothing about "one node" — the copy this slice
    // deletes must not survive by being reused here.
    expect(label.textContent).toMatch(/incomplete|could not be reached|may be missing/i);
    expect(label.textContent).not.toMatch(/one node/i);
    expect(label.textContent).not.toMatch(/whole fleet/i);
  });

  /*
   * The other half of the criterion, and the half a regression would actually hit: "peer back →
   * label gone **without user action**". Asserting only that the label appears would be satisfied
   * by an implementation that latches it on forever — coverage held in a ref, or OR-ed with a
   * stale value — and an operator would go on being told the merge is incomplete long after it
   * healed. Coverage has to be derived fresh from each response, and only a poll-driven
   * disappearance proves it is.
   */
  it("drops the partial label on its own once the merge reaches every node again", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let partial = true;
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = typeof input === "string" ? input : input.toString();
        if (path === "/_fleet/members") {
          return Promise.resolve(
            new Response(JSON.stringify(THREE_NODE["/_fleet/members"].json), { status: 200 }),
          );
        }
        if (path === "/_fleet/health") {
          return Promise.resolve(
            new Response(JSON.stringify(THREE_NODE["/_fleet/health"].json), { status: 200 }),
          );
        }
        return Promise.resolve(
          new Response(JSON.stringify([recorded()]), {
            status: 200,
            headers: partial ? { "rift-cluster-partial": "true" } : {},
          }),
        );
      }),
    );
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await screen.findByTestId("request-scope-label");

    // The peer comes back. No click, no reload — just the next poll.
    partial = false;
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS * 2 + 100);

    await waitFor(() => expect(screen.queryByTestId("request-scope-label")).toBeNull());
    vi.useRealTimers();
  });

  // Coverage now comes from the response, not from fleet topology — so a principal refused
  // `/_fleet/*` gets the same complete-merge answer as anyone else, rather than a per-node label.
  // This is the test that proves the topology inference is really gone rather than merely unused.
  it("does not fall back to a per-node label when the fleet projection is refused", async () => {
    stubFetch({
      "/_fleet/members": { status: 404, json: { message: "not found" } },
      "/_fleet/health": { status: 404, json: { message: "not found" } },
      [REQUESTS]: { json: [recorded()] },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });

    expect(await screen.findByText("/v1/payments/status")).toBeTruthy();
    expect(screen.queryByTestId("request-scope-label")).toBeNull();
  });
});

describe("the per-node copy is gone from the source, not just from the screen", () => {
  /*
   * A grep-level assertion, in the suite, on purpose (an explicit acceptance criterion).
   *
   * Every other test here proves the label is not *rendered* in the states it exercises. None of
   * them can prove the strings are gone: a branch left behind for some state nobody wrote a test
   * for would keep saying "One node's traffic" and the suite would stay green. Reading the source
   * is the only assertion that closes that, and the criterion asks for exactly it — "so a
   * regression cannot ship silently".
   */
  it("no longer contains the per-node strings anywhere in web/src", async () => {
    const { readdir, readFile } = await import("node:fs/promises");
    const { join } = await import("node:path");

    async function sources(dir: string): Promise<string[]> {
      const found: string[] = [];
      for (const item of await readdir(dir, { withFileTypes: true })) {
        const full = join(dir, item.name);
        if (item.isDirectory()) found.push(...(await sources(full)));
        else if (/\.(ts|tsx)$/.test(item.name)) found.push(full);
      }
      return found;
    }

    // This file necessarily names the strings in order to assert they are absent, so it is the one
    // exemption — and it is exempted by path rather than by a looser pattern, so a *new* file
    // reintroducing the copy is still caught.
    const self = join("src", "__tests__", "requestLog.test.tsx");
    const files = (await sources("src")).filter((path) => !path.endsWith(self));

    /*
     * The two label *sentences*, not the loose words. "whole fleet" on its own appears in
     * unrelated prose that this slice has no business deleting — the audit endpoint's "a
     * FleetAdmin sees the whole fleet's rows" in the generated `schema.ts`, and similar in
     * `fleetView.ts` / `Sources.tsx`. Matching those would make this assertion fail for reasons
     * that have nothing to do with the request log, and the usual repair for a noisy guard is to
     * loosen it until it stops meaning anything. Anchoring on the exact copy keeps it strict
     * where it matters: either sentence reappearing anywhere under `src` fails this test.
     */
    const offenders: string[] = [];
    for (const file of files) {
      const text = await readFile(file, "utf8");
      if (/One node's traffic|This node is the whole fleet/i.test(text)) offenders.push(file);
    }
    expect(offenders).toEqual([]);
  });
});

describe("server cursor replaces client-side slicing", () => {
  // The screen used to refetch the whole journal every 2 s and slice it locally. The cursor #225
  // added makes each poll a delta fetch: send back the token the last response issued.
  it("sends the issued cursor as ?since= on the next poll", async () => {
    // `shouldAdvanceTime` so the fetch double's promises still settle while the clock is driven
    // manually — the idiom the polling tests below already use.
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { requests } = stubFetch({
      ...THREE_NODE,
      [REQUESTS]: {
        json: [recorded()],
        headers: { "x-rift-next-index": "eyJ2IjoxfQ" },
      },
      [`${REQUESTS}?since=eyJ2IjoxfQ`]: {
        json: [recorded({ path: "/v1/payments/second" })],
        headers: { "x-rift-next-index": "eyJ2IjoyfQ" },
      },
      // Stubbed explicitly so a third poll gets an empty delta. Without it the harness's
      // query-stripping fallback would answer the *baseline* page, appending a duplicate row and
      // making the assertions below flake on timing rather than on behaviour.
      [`${REQUESTS}?since=eyJ2IjoyfQ`]: { json: [], headers: { "x-rift-next-index": "eyJ2IjoyfQ" } },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    expect(await screen.findByText("/v1/payments/status")).toBeTruthy();

    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS + 1);

    await waitFor(() =>
      expect(requests.some((sent) => sent.path === `${REQUESTS}?since=eyJ2IjoxfQ`)).toBe(true),
    );
    // The delta is appended, not swapped in: the first page's row must still be on screen, or the
    // "incremental" poll has silently become a destructive one.
    expect(await screen.findByText("/v1/payments/second")).toBeTruthy();
    expect(screen.getByText("/v1/payments/status")).toBeTruthy();
  });

  /*
   * The cursor accumulates rows across polls, which makes "the journal got *smaller*" the case it
   * can silently get wrong — and the clear button is on this very screen.
   *
   * A clear invalidates the query, but an invalidation is just a refetch: it re-runs the same
   * `queryFn`, which still holds the cursor issued before the clear. That poll asks
   * `?since=<pre-clear token>`, is correctly told there is nothing after it, and appends an empty
   * delta to rows the server has already thrown away. The operator clears the log and watches
   * every entry stay exactly where it was.
   */
  it("re-reads from the start after a clear instead of resuming the pre-clear cursor", async () => {
    const { requests } = stubFetch({
      ...THREE_NODE,
      [REQUESTS]: { json: [recorded()], headers: { "x-rift-next-index": "eyJ2IjoxfQ" } },
      [`${REQUESTS}?since=eyJ2IjoxfQ`]: { json: [], headers: { "x-rift-next-index": "eyJ2IjoxfQ" } },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    expect(await screen.findByText("/v1/payments/status")).toBeTruthy();

    const user = userEvent.setup();
    await user.click(screen.getByTestId("clear-requests"));
    // A clear commits fleet-wide through Raft, so the dialog holds the act until the port is typed.
    await user.type(screen.getByTestId("confirm-typed"), "4545");
    await user.click(screen.getByTestId("confirm-destructive"));

    /*
     * Asserted on the *request*, not on the rendered rows, because the fetch double serves one
     * fixed journal: it cannot go empty the way a real server does after a DELETE, so a
     * rows-disappeared assertion would be testing the stub rather than the screen. The defect is
     * upstream of the rendering anyway — what went wrong was which URL the post-clear read asked
     * for. Resuming `?since=<pre-clear token>` gets an empty delta appended to rows the server has
     * just discarded, and the log never empties on screen no matter what the server says.
     */
    await waitFor(() => {
      const deleted = requests.findIndex((sent) => sent.method === "DELETE");
      expect(deleted).toBeGreaterThanOrEqual(0);
      const after = requests.slice(deleted + 1).filter((sent) => sent.method === "GET");
      expect(after.some((sent) => sent.path === REQUESTS)).toBe(true);
    });
  });

  /*
   * A blip in the middle of a cursored walk. The accumulation makes this the case worth pinning:
   * a failed poll discards the rows *and* the cursor, so what matters is that recovery is a
   * genuine restart rather than a resume from a cursor whose rows are gone — that would append
   * the delta to nothing and show a journal missing its beginning.
   *
   * Blanking to "unknown" on a failed read is the screen's existing, deliberate contract (an
   * unreadable log is not an empty one), so this asserts recovery, not that the rows survive.
   */
  it("restarts the walk from the beginning after a failed poll, losing and duplicating nothing", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let mode: "ok" | "down" = "ok";
    const seen: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = typeof input === "string" ? input : input.toString();
        if (path.startsWith("/_fleet/")) {
          const json =
            path === "/_fleet/members"
              ? THREE_NODE["/_fleet/members"].json
              : THREE_NODE["/_fleet/health"].json;
          return Promise.resolve(new Response(JSON.stringify(json), { status: 200 }));
        }
        seen.push(path);
        if (mode === "down") return Promise.resolve(new Response("{}", { status: 503 }));
        // A cursored read answers the **delta**, which here is empty — the one row was already
        // handed over by the uncursored read that issued the cursor. Serving the row again to a
        // `?since=` poll would be the fake contradicting the endpoint it stands in for, and the
        // duplicate it produced would look exactly like the accumulation bug this test hunts.
        const body = path.includes("?since=") ? [] : [recorded()];
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "x-rift-next-index": "eyJ2IjoxfQ" },
          }),
        );
      }),
    );
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });
    expect(await screen.findByText("/v1/payments/status")).toBeTruthy();

    mode = "down";
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS * 2 + 100);
    mode = "ok";
    seen.length = 0;
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS * 2 + 100);

    // The recovery read is uncursored — a resume would ask for entries after a token whose rows
    // the failure already threw away.
    await waitFor(() => expect(seen.some((path) => path === REQUESTS)).toBe(true));
    // And exactly one row is on screen, not two: the restart replaced rather than appended.
    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    vi.useRealTimers();
  });

  /*
   * The contract's own warning, made a test: "Concatenating pages is not a globally sorted stream —
   * a peer that becomes reachable between pages contributes entries older than everything already
   * returned" (`openapi-ee.yaml`, the `x-rift-next-index` description). That is the degraded-fan-out
   * moment the partial label announces, so a blind append puts a chronological screen out of order
   * exactly when an operator is leaning on it to find "the call I just made".
   */
  it("re-sorts an appended delta rather than trusting page order", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let served = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = typeof input === "string" ? input : input.toString();
        if (path.startsWith("/_fleet/")) {
          const json =
            path === "/_fleet/members"
              ? THREE_NODE["/_fleet/members"].json
              : THREE_NODE["/_fleet/health"].json;
          return Promise.resolve(new Response(JSON.stringify(json), { status: 200 }));
        }
        served += 1;
        // Page 2 is the peer coming back: its entry is OLDER than page 1's.
        const body =
          served === 1
            ? [recorded({ path: "/v1/late", timestamp: "2026-07-31T10:00:09Z" })]
            : [recorded({ path: "/v1/early", timestamp: "2026-07-31T10:00:01Z" })];
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "x-rift-next-index": `tok-${served}` },
          }),
        );
      }),
    );
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });
    expect(await screen.findByText("/v1/late")).toBeTruthy();

    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS + 100);
    await screen.findByText("/v1/early");

    await waitFor(() => {
      const shown = screen.getAllByTestId("request-row").map((row) => row.textContent ?? "");
      const early = shown.findIndex((text) => text.includes("/v1/early"));
      const late = shown.findIndex((text) => text.includes("/v1/late"));
      expect(early).toBeGreaterThanOrEqual(0);
      expect(late).toBeGreaterThanOrEqual(0);
      expect(early).toBeLessThan(late);
    });
    vi.useRealTimers();
  });

  /*
   * The accumulation has to reconcile with the fleet eventually, or this screen quietly becomes a
   * museum. The server stamps a cursor on every 200, so nothing ever *asks* it to re-baseline: a
   * clear issued from another tab, the CLI or an SDK regresses no token and sets no `truncated`,
   * and evicted rows likewise just stop being mentioned. Both leave rows on screen forever unless
   * the client periodically re-reads the whole thing.
   */
  it("drops the cursor periodically so the list re-baselines against the fleet", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const asked: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = typeof input === "string" ? input : input.toString();
        if (path.startsWith("/_fleet/")) {
          const json =
            path === "/_fleet/members"
              ? THREE_NODE["/_fleet/members"].json
              : THREE_NODE["/_fleet/health"].json;
          return Promise.resolve(new Response(JSON.stringify(json), { status: 200 }));
        }
        asked.push(path);
        return Promise.resolve(
          new Response(JSON.stringify(path.includes("?since=") ? [] : [recorded()]), {
            status: 200,
            headers: { "x-rift-next-index": "tok" },
          }),
        );
      }),
    );
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });
    expect(await screen.findByText("/v1/payments/status")).toBeTruthy();

    // Well past the re-baseline threshold.
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS * 40 + 100);

    await waitFor(() => {
      // More than the very first read asked without a cursor: the walk re-baselined at least once.
      const uncursored = asked.filter((path) => !path.includes("?since=")).length;
      expect(uncursored).toBeGreaterThan(1);
    });
    vi.useRealTimers();
  });

  // The honesty bit. Quiet, but present — a reader who lost entries to retention must not think
  // the gap is the system under test never having called the mock.
  it("surfaces a notice when the server stamps x-rift-truncated", async () => {
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: {
        json: [recorded()],
        headers: { "x-rift-truncated": "true" },
      },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    expect(
      await screen.findByText(/evicted|older entries/i, {}, { timeout: 2000 }),
    ).toBeTruthy();
  });

  /*
   * The gap the notice describes is permanent; the header announcing it is not. The server sets
   * `x-rift-truncated` on the single read whose position predates the shard watermark — the next
   * poll presents a position above it and the header is simply absent. Taking the notice from the
   * latest response alone would erase it after one 2 s tick while the hole it warned about is
   * still sitting in the middle of the rows on screen, which is a swallowed warning on the one
   * screen built to keep "incomplete" and "empty" apart.
   */
  it("keeps the truncation notice after the header stops being sent", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let first = true;
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = typeof input === "string" ? input : input.toString();
        if (path.startsWith("/_fleet/")) {
          const json =
            path === "/_fleet/members"
              ? THREE_NODE["/_fleet/members"].json
              : THREE_NODE["/_fleet/health"].json;
          return Promise.resolve(new Response(JSON.stringify(json), { status: 200 }));
        }
        const headers: Record<string, string> = { "x-rift-next-index": "tok" };
        if (first) headers["x-rift-truncated"] = "true";
        first = false;
        return Promise.resolve(
          new Response(JSON.stringify(path.includes("?since=") ? [] : [recorded()]), {
            status: 200,
            headers,
          }),
        );
      }),
    );
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });
    expect(await screen.findByText(/evicted|older entries/i)).toBeTruthy();

    // Two further polls, neither carrying the header.
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS * 2 + 100);

    expect(screen.queryByText(/evicted|older entries/i)).not.toBeNull();
    vi.useRealTimers();
  });
});

describe("unknown is not empty", () => {
  it("renders a node that answered with nothing as an empty log", async () => {
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    expect(await screen.findByTestId("request-log-empty")).toBeTruthy();
    expect(screen.queryByTestId("request-log-unknown")).toBeNull();
  });

  // The distinction the issue calls the most important on the screen: a node that cannot answer
  // has an *unknown* log, and rendering it as an empty table tells an operator their system under
  // test never called the mock.
  it("renders a node that could not answer as unknown, never as empty", async () => {
    stubFetch({ ...THREE_NODE, [REQUESTS]: { status: 503, json: { message: "unavailable" } } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    expect(await screen.findByTestId("request-log-unknown")).toBeTruthy();
    expect(screen.queryByTestId("request-log-empty")).toBeNull();
  });
});

describe("a busy imposter", () => {
  it("keeps the DOM bounded by paging a thousands-of-entries log", async () => {
    const many = Array.from({ length: 2500 }, (_, i) =>
      recorded({ path: `/v1/item/${i}`, timestamp: `2026-07-31T10:00:${String(i % 60).padStart(2, "0")}Z` }),
    );
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: many } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    // Synchronises on a row, not on the scope label: #229 deletes that label for a complete
    // merge, so waiting for it here would hang forever on the very state this suite now expects.
    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBeGreaterThan(0));
    expect(screen.getAllByTestId("request-row").length).toBeLessThanOrEqual(50);
    // The total is now a plain fleet figure. #189 qualified it "this node" precisely because it
    // was one node's count; #147 H makes it the merge's, so the qualifier would be a false one —
    // and the sweep of per-node copy this slice performs is what removes it.
    expect(screen.getByTestId("request-total").textContent).toContain("2500");
    expect(screen.getByTestId("request-total").textContent).not.toMatch(/this node/i);
  });
});

describe("attacker-influenced payloads (RFC-006 §9.1)", () => {
  // Whatever called the mock chose this path, header and body, so this is the most
  // attacker-influenced surface in the console.
  it("renders a script tag in the path and an onerror attribute in the user-agent as text", async () => {
    const hostile = recorded({
      path: "/<script>alert(1)</script>",
      headers: { "user-agent": '<img src=x onerror="alert(1)">' },
      body: "<script>alert('body')</script>",
    });
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [hostile] } });
    const { container } = renderInApp(<RequestLog port={PORT} />, {
      whoami: whoamiWith("fleet-admin"),
    });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    // The body and headers live in the collapsed detail row, so asserting before opening it would
    // check markup that was never mounted — the assertion would pass on an implementation that
    // renders them as HTML.
    await userEvent.setup().click(screen.getByTestId("request-open"));
    await screen.findByTestId("request-detail");

    expect(container.querySelector("script")).toBeNull();
    expect(container.querySelector("img")).toBeNull();
    // The text must still be *shown* — escaping that also hides the evidence is not a fix.
    expect(container.textContent).toContain("<script>alert(1)</script>");
    expect(container.textContent).toContain("<script>alert('body')</script>");
    expect(container.textContent).toContain('<img src=x onerror="alert(1)">');
  });
});

describe("why did this request not match (#208)", () => {
  // The question the screen exists to answer. Until the engine recorded an outcome per journal
  // entry it could not be answered here at all: the per-stub detail lived only on the
  // `X-Rift-Debug` response path, which is a different request judged against whatever the stubs
  // have since become.
  it("names every stub that was tried and the predicate that rejected it", async () => {
    const unmatched = recorded({
      matchOutcome: {
        matched: false,
        tried: [
          { stubIndex: 0, stubId: "payments", why: { reason: "failedPredicate", predicateIndex: 1 } },
          { stubIndex: 1, why: { reason: "skippedScenarioState" } },
        ],
        triedOmitted: 3,
      },
    });
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [unmatched] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));

    const diagnostics = await screen.findByTestId("request-diagnostics");
    expect(diagnostics.textContent).toContain('stub "payments"');
    expect(diagnostics.textContent).toContain("predicate 1 did not match");
    expect(diagnostics.textContent).toContain("stub #1");
    expect(diagnostics.textContent).toContain("scenario state did not match");
    // A silently truncated list would make "these are the stubs that were tried" false with
    // nothing on screen to say so.
    expect(diagnostics.textContent).toContain("3 more");
  });

  it("names the stub that served a matched request", async () => {
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: {
        json: [recorded({ matchOutcome: { matched: true, stubIndex: 2, stubId: "payments" } })],
      },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));

    const diagnostics = await screen.findByTestId("request-diagnostics");
    expect(diagnostics.textContent).toMatch(/matched/i);
    expect(diagnostics.textContent).toContain('stub "payments"');
  });

  // The schema states this in bold: absence means *no outcome was recorded* — an entry from an
  // engine predating the field, an `X-Rift-Debug` request, or a matcher error — never "did not
  // match". Rendering it as a miss would tell an operator their stub was rejected when nothing
  // ever judged it.
  it("says nothing was recorded rather than claiming the request did not match", async () => {
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));

    const diagnostics = await screen.findByTestId("request-diagnostics");
    expect(diagnostics.textContent).toMatch(/no match diagnostics recorded/i);
    expect(diagnostics.textContent).not.toMatch(/did not match/i);
    expect(diagnostics.textContent).not.toMatch(/nothing matched/i);
  });

  // A shape the console cannot read is not an entry with no outcome: one says the node answered
  // with something wrong, the other says nothing was recorded.
  it("calls an outcome it cannot read unreadable rather than absent", async () => {
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: "yes" } })] },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));

    const diagnostics = await screen.findByTestId("request-diagnostics");
    expect(diagnostics.textContent).toMatch(/unreadable/i);
    expect(diagnostics.textContent).not.toMatch(/no match diagnostics recorded/i);
  });

  // A stub id is operator-authored and reaches this screen through the journal, so it belongs to
  // the same attacker-influenced surface as the path and the body (RFC-006 §9.1).
  it("renders a script tag in a stub id as text", async () => {
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: {
        json: [
          recorded({
            matchOutcome: {
              matched: false,
              tried: [
                {
                  stubIndex: 0,
                  stubId: "<script>alert('stub')</script>",
                  why: { reason: "<img src=x onerror=\"alert(1)\">" },
                },
              ],
            },
          }),
        ],
      },
    });
    const { container } = renderInApp(<RequestLog port={PORT} />, {
      whoami: whoamiWith("fleet-admin"),
    });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));
    await screen.findByTestId("request-diagnostics");

    expect(container.querySelector("script")).toBeNull();
    expect(container.querySelector("img")).toBeNull();
    // Escaping that also hides the evidence is not a fix — the operator still needs to read the id.
    expect(container.textContent).toContain("<script>alert('stub')</script>");
    expect(container.textContent).toContain('<img src=x onerror="alert(1)">');
  });
});

describe("the header shapes the engine actually emits", () => {
  // `multi_value_headers::serialize` emits a scalar for one value and an array only for many, and
  // its deserializer tolerates JSON numbers because real recordings carry `"Content-Length": 124`.
  // Rendering assumed arrays, so expanding any ordinary row threw and unmounted the screen.
  it("renders single-string, multi-value and numeric header values without throwing", async () => {
    const request = recorded({
      headers: { "user-agent": "curl/8", "set-cookie": ["a=1", "b=2"], "content-length": 124 },
    });
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [request] } });
    const { container } = renderInApp(<RequestLog port={PORT} />, {
      whoami: whoamiWith("fleet-admin"),
    });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));
    await screen.findByTestId("request-detail");

    expect(container.textContent).toContain("user-agent: curl/8");
    expect(container.textContent).toContain("set-cookie: a=1, b=2");
    expect(container.textContent).toContain("content-length: 124");
  });

  // The mode token is `binary`, not `base64` — `ResponseMode` serializes lowercase variant names
  // and the *encoding* it implies is base64. This fixture asserted `base64` and the screen compared
  // against `base64`, so the two agreed with each other and disagreed with the engine: the label
  // could never render, and the test that existed to prove it did still passed (#212).
  it("labels a base64 body rather than showing it as text", async () => {
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: { json: [recorded({ body: "3q2+7w==", _mode: "binary" })] },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));
    expect((await screen.findByTestId("request-detail")).textContent).toContain("Body (base64)");
  });

  it("leaves a text body unlabelled, which is how absence of _mode reads", async () => {
    // Guards the guard: if the label were unconditional the test above would pass for the wrong
    // reason. A text body omits `_mode` entirely rather than sending "text".
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: { json: [recorded({ body: "hello" })] },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    await userEvent.setup().click(screen.getByTestId("request-open"));
    expect((await screen.findByTestId("request-detail")).textContent).not.toContain("(base64)");
  });
});

describe("switching imposters", () => {
  // The pager offset used to survive the switch, so an imposter with traffic rendered as an empty
  // table paged past its end — the same lie the unknown/empty split exists to prevent.
  it("resets the page when the imposter changes", async () => {
    const many = Array.from({ length: 200 }, (_, i) => recorded({ path: `/v1/item/${i}` }));
    stubFetch({
      ...THREE_NODE,
      [REQUESTS]: { json: many },
      "/imposters/4546/requests": { json: [recorded({ path: "/only-one" })] },
    });
    // Switches port from inside the provider tree, the way the router does — `rerender` would drop
    // the providers `renderInApp` wraps around the screen.
    function Switcher(): ReactNode {
      const [port, setPort] = useState(PORT);
      return (
        <>
          <button type="button" onClick={() => setPort(4546)}>
            switch imposter
          </button>
          <RequestLog port={port} />
        </>
      );
    }
    renderInApp(<Switcher />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(50));
    await userEvent.setup().click(screen.getByRole("button", { name: /next/i }));
    await waitFor(() =>
      expect(screen.getByTestId("request-total").textContent).toContain("51–100"),
    );

    await userEvent.setup().click(screen.getByRole("button", { name: /switch imposter/i }));
    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(1));
    expect(screen.getByTestId("request-total").textContent).toContain("1–1 of 1");
  });
});

describe("paging through a long log", () => {
  it("moves between pages when the pager is used", async () => {
    const many = Array.from({ length: 120 }, (_, i) => recorded({ path: `/v1/item/${i}` }));
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: many } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(50));
    expect(screen.getByTestId("request-total").textContent).toContain("1–50");

    await userEvent.setup().click(screen.getByRole("button", { name: /next/i }));
    await waitFor(() =>
      expect(screen.getByTestId("request-total").textContent).toContain("51–100"),
    );

    await userEvent.setup().click(screen.getByRole("button", { name: /previous/i }));
    await waitFor(() => expect(screen.getByTestId("request-total").textContent).toContain("1–50"));
  });

  // Retention truncates the journal under the 2s poll, and `DELETE …/requests` empties it outright.
  // An offset left pointing past the new end would render an empty table for an imposter that has
  // traffic — the unknown-vs-empty lie arriving by a different route.
  it("clamps to a valid page when the journal shrinks underneath it", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let rows = Array.from({ length: 200 }, (_, i) => recorded({ path: `/v1/item/${i}` }));
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = typeof input === "string" ? input : input.toString();
        const body =
          path === REQUESTS
            ? rows
            : path === "/_fleet/members"
              ? THREE_NODE["/_fleet/members"].json
              : THREE_NODE["/_fleet/health"].json;
        return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
      }),
    );
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(50));
    await userEvent.setup().click(screen.getByRole("button", { name: /next/i }));
    await userEvent.setup().click(screen.getByRole("button", { name: /next/i }));
    await waitFor(() =>
      expect(screen.getByTestId("request-total").textContent).toContain("101–150"),
    );

    rows = rows.slice(0, 60);
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS * 2 + 100);

    // Clamped to the last valid page, showing real rows rather than an empty table.
    await waitFor(() =>
      expect(screen.getByTestId("request-total").textContent).toContain("51–60 of 60"),
    );
    expect(screen.getAllByTestId("request-row").length).toBe(10);
    vi.useRealTimers();
  });

  it("shows the last partial page without over-reading", async () => {
    const many = Array.from({ length: 60 }, (_, i) => recorded({ path: `/v1/item/${i}` }));
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: many } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(50));
    await userEvent.setup().click(screen.getByRole("button", { name: /next/i }));

    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBe(10));
    expect(screen.getByTestId("request-total").textContent).toContain("51–60 of 60");
  });
});

describe("polling (RFC-006 §6)", () => {
  it("refetches on the 2s request-log interval while the tab is visible", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { calls } = stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });
    // Waits for the first load via a row — the scope label is gone on a complete merge (#229).
    await screen.findByTestId("request-row");

    const before = calls.filter((path) => path === REQUESTS).length;
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS * 3 + 100);
    expect(calls.filter((path) => path === REQUESTS).length).toBeGreaterThan(before);
  });

  it("stops polling while the tab is hidden and resumes when it is shown", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { calls } = stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("fleet-admin") });
    // Waits for the first load via a row — the scope label is gone on a complete merge (#229).
    await screen.findByTestId("request-row");

    setTabVisibility("hidden");
    const whileHidden = calls.filter((path) => path === REQUESTS).length;
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS * 6 + 100);
    expect(calls.filter((path) => path === REQUESTS).length).toBe(whileHidden);

    setTabVisibility("visible");
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS + 100);
    await waitFor(() =>
      expect(calls.filter((path) => path === REQUESTS).length).toBeGreaterThan(whileHidden),
    );
  });
});

describe("#250 — turning a request into a stub", () => {
  const IMPOSTER = `/imposters/${PORT}`;

  function imposterWith(stubs: unknown[]): Record<string, unknown> {
    return {
      port: PORT,
      host: "0.0.0.0",
      protocol: "http",
      name: "billing",
      recordRequests: true,
      enabled: true,
      stubs,
    };
  }

  /** Expand the row's detail panel, where the action lives. */
  async function openRow(): Promise<void> {
    await userEvent.setup().click(await screen.findByTestId("request-open"));
  }

  it("offers to stub an unmatched request", async () => {
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: false, tried: [] } })] },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();

    expect(await screen.findByTestId("request-stub-this")).toBeTruthy();
    expect(screen.queryByTestId("request-open-stub")).toBeNull();
  });

  it("offers to open the stub that answered a matched request", async () => {
    // The useful verb on a matched row is not "make a new stub" — one already answered it.
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: true, stubId: "s-1" } })] },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();

    expect(await screen.findByTestId("request-open-stub")).toBeTruthy();
    expect(screen.queryByTestId("request-stub-this")).toBeNull();
  });

  it("offers no edit action when the winning stub declares no id, and says why", async () => {
    // By-index editing is the documented lost-update window, so the console declines rather than
    // offering something unsafe — and explains the refusal instead of rendering a dead row.
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: true, stubIndex: 2 } })] },
      [IMPOSTER]: { json: imposterWith([{ predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();

    expect(await screen.findByTestId("request-no-stub-action")).toBeTruthy();
    expect(screen.queryByTestId("request-stub-this")).toBeNull();
    expect(screen.queryByTestId("request-open-stub")).toBeNull();
  });

  it("offers nothing at all to a reader who cannot write stubs", async () => {
    // The screen itself stays readable at `imposter.read`; only the actions are gated.
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: false, tried: [] } })] },
      [IMPOSTER]: { json: imposterWith([]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("viewer") });
    await openRow();

    expect(screen.queryByTestId("request-stub-this")).toBeNull();
    expect(screen.queryByTestId("request-no-stub-action")).toBeNull();
    expect(await screen.findByTestId("request-row")).toBeTruthy();
  });

  it("opens the editor seeded from the request, matching method and path by default", async () => {
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: {
        json: [
          recorded({
            method: "POST",
            path: "/v1/payments",
            matchOutcome: { matched: false, tried: [] },
          }),
        ],
      },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-stub-this"));

    const editor = (await screen.findByTestId("code-editor-fallback")) as HTMLTextAreaElement;
    await waitFor(() => expect(editor.value.length).toBeGreaterThan(0));
    // Plus a seeded id: a stub created without one can be neither edited nor removed afterwards,
    // so the editor gives every new stub an addressable name — including one seeded from a request.
    const draft = JSON.parse(editor.value) as Record<string, unknown> & { id: string };
    expect(draft.id).toMatch(/^stub-/);
    const { id: _id, ...rest } = draft;
    expect(rest).toEqual({
      predicates: [{ equals: { method: "POST", path: "/v1/payments" } }],
      responses: [
        { is: { statusCode: 200, headers: { "Content-Type": "application/json" }, body: "{}" } },
      ],
    });
  });

  it("honours the field selection chosen before the editor opens", async () => {
    // The selection is made up-front precisely so nothing re-derives over a hand-edited draft.
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: {
        json: [
          recorded({
            query: { page: "2" },
            matchOutcome: { matched: false, tried: [] },
          }),
        ],
      },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();

    const user = userEvent.setup();
    // Query is ON by default (it agrees with the recording flow), so exercise the selection in the
    // direction that actually changes something: turn it off, and opt a header IN.
    await user.click(await screen.findByRole("checkbox", { name: "Match on query" }));
    await user.click(screen.getByRole("checkbox", { name: "Match on header user-agent" }));
    await user.click(screen.getByTestId("request-stub-this"));

    const editor = (await screen.findByTestId("code-editor-fallback")) as HTMLTextAreaElement;
    await waitFor(() => expect(editor.value.length).toBeGreaterThan(0));
    const stub = JSON.parse(editor.value) as { predicates: { equals: Record<string, unknown> }[] };
    expect(stub.predicates[0]?.equals.query).toBeUndefined();
    expect(stub.predicates[0]?.equals.headers).toEqual({ "user-agent": "curl/8" });
  });

  it("refuses to re-seed over an open draft, rather than discarding it silently", async () => {
    /*
     * Clicking "Stub this" again remounts the editor with a fresh seed — which would throw away
     * whatever the operator had typed, with no warning. The selection is chosen BEFORE opening
     * precisely so nothing re-derives afterwards; this is the other half of that rule.
     */
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: false, tried: [] } })] },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-stub-this"));
    await screen.findByTestId("code-editor-fallback");

    expect((screen.getByTestId("request-stub-this") as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByTestId("stub-this-busy").textContent).toMatch(/discard the draft/i);
  });

  it("does not open an editor over a stub the imposter no longer carries", async () => {
    /*
     * A journal entry outlives the stub that served it: the id can be gone by the time the row is
     * clicked. Opening an editor over `{}` would show an empty document titled with the missing id,
     * and a save would then PUT `{}` over whatever the fleet actually has. `ImposterDetail` refuses
     * to mount in this case; so must this screen.
     */
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: true, stubId: "s-gone" } })] },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-open-stub"));

    expect((await screen.findByTestId("stub-gone")).textContent).toMatch(/no longer in this/i);
    expect(screen.queryByTestId("code-editor-fallback")).toBeNull();
  });

  it("does not claim a catch-all shadows an EXISTING stub being edited", async () => {
    /*
     * The warning is about appends — "new stubs are appended, first-match-wins". An existing stub
     * may sit above the catch-all and fire perfectly well, so saying it will never fire is simply
     * false, and a wrong claim on this screen is the failure mode the module set exists to avoid.
     */
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: true, stubId: "s-1" } })] },
      [IMPOSTER]: {
        json: imposterWith([
          { id: "s-1", predicates: [{ equals: { path: "/x" } }] },
          { id: "catch", responses: [{ is: { statusCode: 200 } }] },
        ]),
      },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-open-stub"));

    await screen.findByTestId("code-editor-fallback");
    expect(screen.queryByTestId("stub-shadow-warning")).toBeNull();
  });

  it("offers no action, and says why, when the match diagnostics cannot be read", async () => {
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: "yes" } })] },
      [IMPOSTER]: { json: imposterWith([]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();

    expect((await screen.findByTestId("request-no-stub-action")).textContent).toMatch(/unreadable/i);
    expect(screen.queryByTestId("request-stub-this")).toBeNull();
  });

  it("keeps rendering when a row's match outcome is null", async () => {
    // `diagnostics.ts` folds null into absence; reading `.matched` off it would throw and unmount
    // the screen an operator opened precisely because something was already wrong.
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: null })] },
      [IMPOSTER]: { json: imposterWith([]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();

    expect(await screen.findByTestId("request-stub-this")).toBeTruthy();
  });

  it("keeps rendering when the imposter's stub list is not an array", async () => {
    /*
     * The list comes off the wire, and this is the screen an operator opens when something is
     * already wrong. A `stubs: null` reaching `hasCatchAll(...).some` would throw during render and
     * take the log down at the worst possible moment.
     */
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: false, tried: [] } })] },
      [IMPOSTER]: {
        json: {
          port: PORT,
          host: "0.0.0.0",
          protocol: "http",
          name: "billing",
          recordRequests: true,
          enabled: true,
          stubs: null,
        },
      },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-stub-this"));

    expect(await screen.findByTestId("code-editor-fallback")).toBeTruthy();
    // No catch-all can be proven from a list that is not a list, so it must not claim one.
    expect(screen.queryByTestId("stub-shadow-warning")).toBeNull();
  });

  it("warns that a catch-all stub will shadow the one being added", async () => {
    /*
     * Stubs append and matching is first-match-wins, so a stub added below a catch-all never fires.
     * Without this the operator saves, sees no change, and has nothing on screen explaining why.
     */
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: false, tried: [] } })] },
      [IMPOSTER]: { json: imposterWith([{ id: "catch-all", responses: [{ is: { statusCode: 200 } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-stub-this"));

    expect((await screen.findByTestId("stub-shadow-warning")).textContent).toMatch(
      /every request|first match|never fire/i,
    );
  });

  it("does not warn when the imposter has no catch-all", async () => {
    stubFetch({
      ...SINGLE_NODE,
      [REQUESTS]: { json: [recorded({ matchOutcome: { matched: false, tried: [] } })] },
      [IMPOSTER]: { json: imposterWith([{ id: "s-1", predicates: [{ equals: { path: "/x" } }] }]) },
    });
    renderInApp(<RequestLog port={PORT} />, { whoami: whoamiWith("editor") });
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-stub-this"));

    await screen.findByTestId("code-editor-fallback");
    expect(screen.queryByTestId("stub-shadow-warning")).toBeNull();
  });
});

const FLEET_REQUESTS = "/admin/requests";

/** One `FleetRequestPage`, from `#362`'s merged endpoint — the shape `useFleetRequests` reads. */
function fleetPage(
  rows: { port: number; request: Record<string, unknown> }[],
  coverage: Record<string, unknown> = { covered: [PORT], total: 1, omitted: [], capped: false },
): Record<string, unknown> {
  return {
    requests: rows.map(({ port, request }) => ({ port, flowId: "default", request })),
    cursor: "tok",
    coverage,
    joined: rows.map(({ port }) => port),
  };
}

describe("the fleet journal is one read, not a fan-out (#362)", () => {
  // Three imposters, so a regression to the old per-port fan-out would be visible as three reads
  // rather than one — the acceptance criterion this epic exists to close.
  const THREE_IMPOSTERS = {
    ...SINGLE_NODE,
    "/imposters": {
      json: {
        imposters: [
          { port: 4545, name: "payments", protocol: "http" },
          { port: 4546, name: "billing", protocol: "http" },
          { port: 4547, name: "shipping", protocol: "http" },
        ],
      },
    },
  };

  it("issues exactly one request to the merged endpoint, never one per imposter", async () => {
    const { requests } = stubFetch({
      ...THREE_IMPOSTERS,
      [FLEET_REQUESTS]: {
        json: fleetPage([{ port: 4545, request: recorded() }], {
          covered: [4545, 4546, 4547],
          total: 3,
          omitted: [],
          capped: false,
        }),
      },
    });
    renderInApp(<RequestLog port={null} />, { whoami: whoamiWith("fleet-admin") });

    await screen.findByTestId("merged-request-row");

    const journalReads = requests.filter((sent) => sent.path.split("?")[0] === FLEET_REQUESTS);
    expect(journalReads.length).toBe(1);
    // And no per-port fallback alongside it — the fan-out this replaces.
    expect(requests.some((sent) => sent.path.startsWith("/imposters/4545/requests"))).toBe(false);
    expect(requests.some((sent) => sent.path.startsWith("/imposters/4546/requests"))).toBe(false);
    expect(requests.some((sent) => sent.path.startsWith("/imposters/4547/requests"))).toBe(false);
  });

  // The wire order is oldest-first (openapi-ee.yaml's `savedRequests` convention, which the merge
  // preserves); the screen's own label says "newest first". A pass-through here would silently make
  // that label false and show the log upside down.
  it("renders newest first even though the server answers oldest first", async () => {
    const ONE_IMPOSTER = {
      ...SINGLE_NODE,
      "/imposters": { json: { imposters: [{ port: PORT, name: "payments", protocol: "http" }] } },
    };
    stubFetch({
      ...ONE_IMPOSTER,
      [FLEET_REQUESTS]: {
        json: fleetPage([
          { port: PORT, request: recorded({ path: "/v1/oldest", timestamp: "2026-07-31T10:00:00Z" }) },
          { port: PORT, request: recorded({ path: "/v1/newest", timestamp: "2026-07-31T10:00:09Z" }) },
        ]),
      },
    });
    renderInApp(<RequestLog port={null} />, { whoami: whoamiWith("fleet-admin") });

    const rows = await screen.findAllByTestId("merged-request-row");
    expect(rows.length).toBe(2);
    expect(rows[0]?.textContent).toContain("/v1/newest");
    expect(rows[1]?.textContent).toContain("/v1/oldest");
  });
});

describe("the coverage cap banner reflects the server's coverage block (#362)", () => {
  const ONE_IMPOSTER = {
    ...SINGLE_NODE,
    "/imposters": { json: { imposters: [{ port: PORT, name: "payments", protocol: "http" }] } },
  };

  it("shows the banner and names the omitted ports when coverage.capped is true", async () => {
    stubFetch({
      ...ONE_IMPOSTER,
      [FLEET_REQUESTS]: {
        json: fleetPage([{ port: PORT, request: recorded() }], {
          covered: [PORT],
          total: 101,
          omitted: [4600, 4601],
          capped: true,
        }),
      },
    });
    renderInApp(<RequestLog port={null} />, { whoami: whoamiWith("fleet-admin") });

    const banner = await screen.findByTestId("merged-journal-partial");
    expect(banner.textContent).toMatch(/2 of 101/);
    expect(banner.textContent).toContain("4600");
    expect(banner.textContent).toContain("4601");
  });

  it("shows no cap banner when coverage.capped is false, however many are covered", async () => {
    stubFetch({
      ...ONE_IMPOSTER,
      [FLEET_REQUESTS]: {
        json: fleetPage([{ port: PORT, request: recorded() }], {
          covered: [PORT],
          total: 1,
          omitted: [],
          capped: false,
        }),
      },
    });
    renderInApp(<RequestLog port={null} />, { whoami: whoamiWith("fleet-admin") });

    await screen.findByTestId("merged-request-row");
    expect(screen.queryByTestId("merged-journal-partial")).toBeNull();
  });
});

describe("the fleet journal's node, status and latency columns (#364)", () => {
  // The merged journal reads every imposter the tenant has, so the fixture needs the listing as
  // well as the fleet-wide traffic.
  const FLEET_JOURNAL = {
    ...SINGLE_NODE,
    "/imposters": { json: { imposters: [{ port: PORT, name: "payments", protocol: "http" }] } },
  };

  /*
   * Asserted per cell, by testid, rather than over the row's text.
   *
   * The first draft of this test checked `row.textContent).toContain("3")` for the node id and
   * passed while the node cell was hard-coded to a dash — because "503" in the status cell also
   * contains a "3". A row-wide `toContain` is a assertion that cannot fail for the reason it
   * claims to test, and the node id is exactly the kind of short value that collides.
   */
  it("renders the three columns the engine now records", async () => {
    stubFetch({
      ...FLEET_JOURNAL,
      [FLEET_REQUESTS]: {
        json: fleetPage([{ port: PORT, request: recorded({ node: "rift-7", status: 503, latencyMs: 42 }) }]),
      },
    });
    renderInApp(<RequestLog port={null} />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("merged-cell-node")).textContent).toBe("rift-7");
    expect((await screen.findByTestId("merged-cell-status")).textContent).toContain("503");
    expect((await screen.findByTestId("merged-cell-latency")).textContent).toBe("42 ms");
  });

  /*
   * The half that matters. `status` and `latencyMs` are attached after the response exists, so an
   * entry recorded before that — the debug path, or a request journalled before an error — carries
   * neither. Rendering `0 ms` there would report an instant answer the engine never observed, and
   * rendering a status would invent one outright.
   */
  it("does not invent an outcome the engine never recorded", async () => {
    stubFetch({
      ...FLEET_JOURNAL,
      [FLEET_REQUESTS]: { json: fleetPage([{ port: PORT, request: recorded() }]) },
    });
    renderInApp(<RequestLog port={null} />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("merged-cell-node")).textContent).toBe("\u2014");
    expect((await screen.findByTestId("merged-cell-status")).textContent).toBe("\u2014");
    expect((await screen.findByTestId("merged-cell-latency")).textContent).toBe("\u2014");
  });

  // A latency of zero is a reading, not a gap: a stub answered from memory is sub-millisecond, and
  // blanking it would hide the fact that the mock was fast rather than unmeasured.
  it("renders a zero latency as a measurement, not as absence", async () => {
    stubFetch({
      ...FLEET_JOURNAL,
      [FLEET_REQUESTS]: {
        json: fleetPage([{ port: PORT, request: recorded({ node: "rift-1", status: 200, latencyMs: 0 }) }]),
      },
    });
    renderInApp(<RequestLog port={null} />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("merged-cell-latency")).textContent).toBe("0 ms");
  });
});
