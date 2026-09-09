/** @vitest-environment jsdom */
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { type ReactNode, useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { POLL_INTERVAL_MS, REQUEST_POLL_INTERVAL_MS } from "../app/query.ts";
import { RequestLog } from "../screens/RequestLog.tsx";
import { renderInApp, setTabVisibility, stubFetch } from "./harness.tsx";

const PORT = 4545;
const REQUESTS = `/imposters/${PORT}/requests`;

/*
 * Node ids are STRINGS on the wire, and the fixtures say so.
 *
 * A raft id is a `u64` and JSON numbers are IEEE-754 doubles wherever the reader is JavaScript, so
 * the contract sends `node_id` and `voters` as strings (`fleetView.ts` documents the rounding this
 * prevents). Since #552 the request log renders that id, which makes the fixture's spelling
 * load-bearing rather than incidental: a numeric fixture would exercise a shape the fleet does not
 * send. They are also deliberately not bare digits — a one-character id collides with substrings of
 * every other number on the screen, which is how the old fleet-journal node test passed against a
 * hard-coded dash.
 */
const THREE_NODE = {
  "/_fleet/members": {
    json: {
      node_id: "node-c",
      is_leader: false,
      current_leader: "node-a",
      last_applied: 12,
      voters: ["node-a", "node-b", "node-c"],
    },
  },
  "/_fleet/health": {
    json: {
      ready: true,
      state: "ready",
      pending_gates: [],
      isolated: false,
      ring: { m_idx: 4, members: ["node-a", "node-b", "node-c"] },
    },
  },
};

const SINGLE_NODE = {
  "/_fleet/members": {
    json: {
      node_id: "node-a",
      is_leader: true,
      current_leader: "node-a",
      last_applied: 3,
      voters: ["node-a"],
    },
  },
  "/_fleet/health": {
    json: {
      ready: true,
      state: "ready",
      pending_gates: [],
      isolated: false,
      ring: { m_idx: 1, members: ["node-a"] },
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

describe("the screen names the node it read from (D-74)", () => {
  /*
   * The point of #552 on this screen.
   *
   * The journal is upstream's own again and strictly per node: these rows are what the node the
   * browser reached recorded, and say nothing about the rest of the fleet. A table with no node on
   * it reads as the fleet's — which is how an operator concludes "the call never arrived" from a
   * log that is merely someone else's.
   *
   * Asserted on the id itself, from `/_fleet/members`' top-level `node_id`, rather than on the word
   * "node" appearing somewhere: a label that says "this node" without naming which one is the same
   * non-answer in friendlier words.
   */
  it("names the answering node beside the imposter", async () => {
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />);

    // Waits for the rows, so this cannot pass on a screen that has not rendered yet.
    expect(await screen.findByText("/v1/payments/status")).toBeTruthy();
    const label = await screen.findByTestId("request-scope-label");
    expect(label.textContent).toContain("node-c");
    expect(label.textContent).toContain(String(PORT));
  });

  // The name comes from the fleet cache every other screen already holds, not from a second read of
  // its own — and certainly not from `RecordedRequest.node`, which upstream's `LocalJournal` never
  // stamps, so every row on a live fleet carries it absent.
  it("reads the node id from the shared fleet cache, not a second fleet fetch", async () => {
    const { requests } = stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />);

    await screen.findByTestId("request-scope-label");
    expect(requests.filter((sent) => sent.path === "/_fleet/members").length).toBe(1);
  });

  /*
   * The node this console is talking to can change under a screen left open — a load balancer
   * reconnects the browser elsewhere, and the rows below change with it. Latching the first id seen
   * would leave the label naming a node whose journal is no longer on screen, which is worse than
   * no label: it is a confident wrong answer.
   */
  it("follows the answering node when the console is reconnected elsewhere", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let members = THREE_NODE["/_fleet/members"].json;
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = typeof input === "string" ? input : input.toString();
        if (path === "/_fleet/members") {
          return Promise.resolve(new Response(JSON.stringify(members), { status: 200 }));
        }
        if (path === "/_fleet/health") {
          return Promise.resolve(
            new Response(JSON.stringify(THREE_NODE["/_fleet/health"].json), { status: 200 }),
          );
        }
        return Promise.resolve(new Response(JSON.stringify([recorded()]), { status: 200 }));
      }),
    );
    renderInApp(<RequestLog port={PORT} />);

    await waitFor(() =>
      expect(screen.getByTestId("request-scope-label").textContent).toContain("node-c"),
    );

    members = { ...members, node_id: "node-b" };
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS * 2 + 100);

    await waitFor(() =>
      expect(screen.getByTestId("request-scope-label").textContent).toContain("node-b"),
    );
    expect(screen.getByTestId("request-scope-label").textContent).not.toContain("node-c");
    vi.useRealTimers();
  });

  /*
   * A refused `/_fleet/*` costs the *name* of the node, not the rows: the journal read stands on
   * its own. Saying which of the two failed is the whole job of the label here — blanking it would
   * leave the sentence claiming nothing, and defaulting to a node id would invent one.
   */
  it("says it could not name the node rather than dropping the label or inventing an id", async () => {
    stubFetch({
      "/_fleet/members": { status: 404, json: { message: "not found" } },
      "/_fleet/health": { status: 404, json: { message: "not found" } },
      [REQUESTS]: { json: [recorded()] },
    });
    renderInApp(<RequestLog port={PORT} />);

    expect(await screen.findByText("/v1/payments/status")).toBeTruthy();
    const label = await screen.findByTestId("request-scope-label");
    await waitFor(() => expect(label.textContent).toMatch(/could not name/i));
  });

  // The screen must not go on describing a fleet-wide merge that no longer exists — most of all in
  // the copy an operator reads when the table is empty, which is where a stale "the merge answered"
  // would say the fleet has nothing while one node was merely asked.
  it("describes an empty log as this node's, never as the fleet's", async () => {
    stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [] } });
    renderInApp(<RequestLog port={PORT} />);

    const empty = await screen.findByTestId("request-log-empty");
    expect(empty.textContent).toMatch(/this node/i);
    expect(empty.textContent).not.toMatch(/merge/i);
  });
});

/**
 * A file with its `/* … *\/` blocks and whole-line `//` comments removed.
 *
 * The guard below is about what the console still *calls*, and a comment saying why a route was
 * removed is the opposite of a regression — this repo documents removals at length, and a guard
 * that punished that would be repaired by deleting the explanation, which is the wrong repair.
 *
 * Deliberately conservative rather than a real tokenizer. A trailing `// …` after code on the same
 * line is left in place, because stripping from a bare `//` would also cut `"https://…"` out of a
 * string literal; the cost is that such a comment can still fail this test, which is the safe
 * direction to be wrong in. No file under `src` currently has one.
 */
function code(text: string): string {
  return text.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^[ \t]*\/\/.*$/gm, "");
}

describe("the fleet-merge surface is gone from the source, not just from the screen", () => {
  /*
   * A grep-level assertion, in the suite, on purpose — the same guard #229 used for the copy it
   * deleted, re-pointed at what D-74 deletes.
   *
   * Every other test here proves the merge machinery is not *reached* in the states it exercises.
   * None of them can prove it is gone: a hook nothing calls, a path constant nothing builds, a
   * component left mounted behind a flag would all keep the suite green while the console still
   * carried a client for routes the server no longer serves. Reading the source is the only
   * assertion that closes that.
   */
  it("names no removed route, hook or component anywhere in web/src", async () => {
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

    /*
     * Two exemptions, both by exact path rather than by a looser pattern, so a *new* file
     * reintroducing any of this is still caught.
     *
     * This file names the strings in order to assert they are absent. `api/schema.ts` is generated
     * from `docs/api/openapi-ee.yaml` and is the contract's own word on what exists — if a removed
     * route reappeared there the fix is in the contract, and a red assertion here would only point
     * at the wrong file.
     */
    const self = join("src", "__tests__", "requestLog.test.tsx");
    const generated = join("src", "api", "schema.ts");
    const files = (await sources("src")).filter(
      (path) => !path.endsWith(self) && !path.endsWith(generated),
    );

    const banned =
      /\/admin\/requests|savedRequests\/stream|useFleetRequests|FleetRequestPage|FleetJournalCoverage|MergedJournal|LiveTail|coverageFor|describeCoverage/;
    const offenders: string[] = [];
    for (const file of files) {
      if (banned.test(code(await readFile(file, "utf8")))) offenders.push(file);
    }
    expect(offenders).toEqual([]);
  });
});

describe("upstream's scalar cursor replaces client-side slicing", () => {
  /*
   * The screen used to refetch the whole journal every 2 s and slice it locally. `?since=` makes
   * each poll a delta fetch: send back the token the last response issued.
   *
   * The token is upstream's own **scalar** index since D-74 — the engine's position in its own
   * journal, not the opaque per-node vector the removed fleet merge used to encode. The fixtures
   * spell it that way so a client that quietly started parsing or arithmetic-ing the value would
   * be exercising the shape the engine really sends. Nothing on this side parses it either way: it
   * is round-tripped verbatim.
   */
  it("sends the issued cursor as ?since= on the next poll", async () => {
    // `shouldAdvanceTime` so the fetch double's promises still settle while the clock is driven
    // manually — the idiom the polling tests below already use.
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { requests } = stubFetch({
      ...THREE_NODE,
      [REQUESTS]: {
        json: [recorded()],
        headers: { "x-rift-next-index": "42" },
      },
      [`${REQUESTS}?since=42`]: {
        json: [recorded({ path: "/v1/payments/second" })],
        headers: { "x-rift-next-index": "43" },
      },
      // Stubbed explicitly so a third poll gets an empty delta. Without it the harness's
      // query-stripping fallback would answer the *baseline* page, appending a duplicate row and
      // making the assertions below flake on timing rather than on behaviour.
      [`${REQUESTS}?since=43`]: { json: [], headers: { "x-rift-next-index": "43" } },
    });
    renderInApp(<RequestLog port={PORT} />);

    expect(await screen.findByText("/v1/payments/status")).toBeTruthy();

    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS + 1);

    await waitFor(() =>
      expect(requests.some((sent) => sent.path === `${REQUESTS}?since=42`)).toBe(true),
    );
    // The delta is appended, not swapped in: the first page's row must still be on screen, or the
    // "incremental" poll has silently become a destructive one.
    expect(await screen.findByText("/v1/payments/second")).toBeTruthy();
    expect(screen.getByText("/v1/payments/status")).toBeTruthy();
  });

  /*
   * The engine does not promise a cursor back. Upstream's `handle_get_requests` stamps
   * `x-rift-next-index` only when its journal backend has stable indices, and a backend without
   * them ignores `since` and answers the whole journal with no header at all. A screen that treated
   * every cursored ask as a delta would append that whole journal to the rows it already holds —
   * every row on screen, twice — with nothing in the body to say so. `resuming` is therefore
   * derived from the answer: no cursor back means no merge, the cursor is dropped, and the next
   * poll is a full read.
   */
  it("does not append a cursored answer that came back without a cursor; it re-baselines", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { requests } = stubFetch({
      ...THREE_NODE,
      [REQUESTS]: {
        json: [recorded()],
        headers: { "x-rift-next-index": "42" },
      },
      // The whole journal, and no header: what a backend without stable indices answers to
      // `?since=`. Appended, this would put "/v1/payments/status" on screen twice.
      [`${REQUESTS}?since=42`]: {
        json: [recorded(), recorded({ path: "/v1/payments/second" })],
      },
    });
    renderInApp(<RequestLog port={PORT} />);

    expect(await screen.findByText("/v1/payments/status")).toBeTruthy();

    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS + 1);
    await waitFor(() =>
      expect(requests.some((sent) => sent.path === `${REQUESTS}?since=42`)).toBe(true),
    );
    // The un-cursored answer is not merged into the held rows...
    expect(screen.getAllByText("/v1/payments/status")).toHaveLength(1);
    expect(screen.queryByText("/v1/payments/second")).toBeNull();

    // ...and the cursor is gone: the very next poll asks for the whole journal again rather than
    // resuming from a token the engine has just shown it does not honour.
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS + 1);
    await waitFor(() => {
      const uncursored = requests.filter((sent) => sent.path === REQUESTS).length;
      expect(uncursored).toBeGreaterThan(1);
    });
    // Exactly one cursored ask was ever made — the drop is not "try the same token again".
    expect(requests.filter((sent) => sent.path.includes("?since=")).length).toBe(1);
    vi.useRealTimers();
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
      [REQUESTS]: { json: [recorded()], headers: { "x-rift-next-index": "42" } },
      [`${REQUESTS}?since=42`]: { json: [], headers: { "x-rift-next-index": "42" } },
    });
    renderInApp(<RequestLog port={PORT} />);

    expect(await screen.findByText("/v1/payments/status")).toBeTruthy();

    const user = userEvent.setup();
    await user.click(screen.getByTestId("clear-requests"));
    // Nothing restores the rows, so the dialog holds the act until the port is typed.
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
            headers: { "x-rift-next-index": "42" },
          }),
        );
      }),
    );
    renderInApp(<RequestLog port={PORT} />);
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
   * The delta is appended in the order the engine served it, and **not** re-sorted (D-74, #552).
   *
   * The screen used to sort the concatenation by `timestamp`, because the merged read it replaced
   * concatenated per-node pages the contract itself declared not to be a globally sorted stream: a
   * peer coming back between polls contributed entries older than everything already returned. One
   * engine's journal has no such case — it is an append-only sequence, and `?since=` continues it
   * exactly where the last response stopped — so the sort can only ever move rows the engine had
   * already placed correctly.
   *
   * Pinned with an out-of-order `timestamp`, which is the field the old sort keyed on: a clock step
   * (NTP, a container resume) makes a later request carry an earlier stamp, and journal position is
   * still the truth about what arrived first. A re-sort would silently reorder the log on exactly
   * the machine whose clock cannot be trusted to reorder it.
   */
  it("appends a delta in journal order rather than re-sorting it by timestamp", async () => {
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
        // Page 2 arrived later but is stamped earlier — the clock stepped back between them.
        const body =
          served === 1
            ? [recorded({ path: "/v1/first", timestamp: "2026-07-31T10:00:09Z" })]
            : [recorded({ path: "/v1/second", timestamp: "2026-07-31T10:00:01Z" })];
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "x-rift-next-index": String(served) },
          }),
        );
      }),
    );
    renderInApp(<RequestLog port={PORT} />);
    expect(await screen.findByText("/v1/first")).toBeTruthy();

    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS + 100);
    await screen.findByText("/v1/second");

    await waitFor(() => {
      const shown = screen.getAllByTestId("request-row").map((row) => row.textContent ?? "");
      const first = shown.findIndex((text) => text.includes("/v1/first"));
      const second = shown.findIndex((text) => text.includes("/v1/second"));
      expect(first).toBeGreaterThanOrEqual(0);
      expect(second).toBeGreaterThanOrEqual(0);
      expect(first).toBeLessThan(second);
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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);

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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);

    expect(await screen.findByTestId("request-log-empty")).toBeTruthy();
    expect(screen.queryByTestId("request-log-unknown")).toBeNull();
  });

  // The distinction the issue calls the most important on the screen: a node that cannot answer
  // has an *unknown* log, and rendering it as an empty table tells an operator their system under
  // test never called the mock.
  it("renders a node that could not answer as unknown, never as empty", async () => {
    stubFetch({ ...THREE_NODE, [REQUESTS]: { status: 503, json: { message: "unavailable" } } });
    renderInApp(<RequestLog port={PORT} />);

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
    renderInApp(<RequestLog port={PORT} />);

    // Synchronises on a row rather than on the scope label, which renders before the journal read
    // lands — waiting on it would let this assert against a table that has not been filled yet.
    await waitFor(() => expect(screen.getAllByTestId("request-row").length).toBeGreaterThan(0));
    expect(screen.getAllByTestId("request-row").length).toBeLessThanOrEqual(50);
    // The pager counts what this node holds, which is what the scope line above the table already
    // says it is — so the number stands unqualified here.
    expect(screen.getByTestId("request-total").textContent).toContain("2500");
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
    const { container } = renderInApp(<RequestLog port={PORT} />);

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
    renderInApp(<RequestLog port={PORT} />);

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
    renderInApp(<RequestLog port={PORT} />);

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
    renderInApp(<RequestLog port={PORT} />);

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
    renderInApp(<RequestLog port={PORT} />);

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
    const { container } = renderInApp(<RequestLog port={PORT} />);

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
    const { container } = renderInApp(<RequestLog port={PORT} />);

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
    renderInApp(<RequestLog port={PORT} />);

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
    renderInApp(<RequestLog port={PORT} />);

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
    renderInApp(<Switcher />);

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
    renderInApp(<RequestLog port={PORT} />);

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
    renderInApp(<RequestLog port={PORT} />);

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
    renderInApp(<RequestLog port={PORT} />);

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
    renderInApp(<RequestLog port={PORT} />);
    // Waits for the first load via a row: the scope label renders before the journal read lands.
    await screen.findByTestId("request-row");

    const before = calls.filter((path) => path === REQUESTS).length;
    await vi.advanceTimersByTimeAsync(REQUEST_POLL_INTERVAL_MS * 3 + 100);
    expect(calls.filter((path) => path === REQUESTS).length).toBeGreaterThan(before);
  });

  it("stops polling while the tab is hidden and resumes when it is shown", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { calls } = stubFetch({ ...THREE_NODE, [REQUESTS]: { json: [recorded()] } });
    renderInApp(<RequestLog port={PORT} />);
    // Waits for the first load via a row: the scope label renders before the journal read lands.
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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);
    await openRow();

    expect(await screen.findByTestId("request-no-stub-action")).toBeTruthy();
    expect(screen.queryByTestId("request-stub-this")).toBeNull();
    expect(screen.queryByTestId("request-open-stub")).toBeNull();
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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);
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
    renderInApp(<RequestLog port={PORT} />);
    await openRow();
    await userEvent.setup().click(await screen.findByTestId("request-stub-this"));

    await screen.findByTestId("code-editor-fallback");
    expect(screen.queryByTestId("stub-shadow-warning")).toBeNull();
  });
});

describe("with no imposter in the hash (D-74)", () => {
  const LISTING = {
    ...SINGLE_NODE,
    "/imposters": {
      json: {
        imposters: [
          { port: 4545, name: "payments", protocol: "http" },
          // Nameless on purpose: `name` is optional on the contract, and a chooser that links by
          // name would leave this one unreachable — the defect #321 fixed on the imposter table.
          { port: 4546, protocol: "http" },
        ],
      },
    },
  };

  /*
   * The portless route used to render the **fleet** journal: one read of `GET /admin/requests`,
   * which the admin front assembled by merging every node's writer shard. That route and its
   * subsystem are gone (D-74), and the console must not replace it with a client-side fan-out —
   * ordering N per-node reads by whichever returned first would present network timing as journal
   * order under the label of one stream. So this route asks which imposter, and nothing else.
   */
  it("offers a per-imposter chooser and reads no journal at all", async () => {
    const { requests } = stubFetch(LISTING);
    renderInApp(<RequestLog port={null} />);

    const picker = await screen.findByTestId("request-picker");
    expect(picker.textContent).toContain("4545");
    expect(picker.textContent).toContain("4546");
    // Every port is a link, named or not, and it points at that imposter's own log.
    const links = [...picker.querySelectorAll("a")].map((a) => a.getAttribute("href"));
    expect(links).toEqual(["#/requests/4545", "#/requests/4546"]);

    // No journal read of any spelling — neither the removed fleet route nor a fan-out over the
    // per-imposter one.
    expect(requests.some((sent) => sent.path.includes("/requests"))).toBe(false);
    expect(requests.some((sent) => sent.path.startsWith("/admin/"))).toBe(false);
  });

  it("says there is nothing to choose rather than rendering an empty list", async () => {
    stubFetch({ ...SINGLE_NODE, "/imposters": { json: { imposters: [] } } });
    renderInApp(<RequestLog port={null} />);

    expect(await screen.findByTestId("request-picker-empty")).toBeTruthy();
    expect(screen.queryByTestId("request-picker")).toBeNull();
  });
});
