/** @vitest-environment jsdom */
import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Imposters } from "../screens/Imposters.tsx";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

/**
 * The fleet rail's `Live tail · merged` panel (#362).
 *
 * It was a `PendingPanel` until the fleet journal existed, because the only way to fill it from the
 * browser would have been a client-side fan-out — N reads per poll, ordered by whichever returned
 * first, wearing the label of one ordered stream. These tests pin that it now reads the merged
 * endpoint instead, and that it still refuses to present a rail-sized slice as the whole journal.
 */

const SINGLE_NODE = {
  "/_fleet/members": {
    json: { node_id: 1, is_leader: true, current_leader: 1, last_applied: 9, voters: [1] },
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
  "/imposters": { json: { imposters: [{ port: 4545, name: "payments", protocol: "http" }] } },
};

function row(port: number, method: string, path: string, status?: number) {
  return {
    port,
    flowId: "default",
    request: {
      method,
      path,
      timestamp: "2026-07-31T10:00:00Z",
      ...(status === undefined ? {} : { status }),
    },
  };
}

function page(rows: ReturnType<typeof row>[]) {
  return {
    requests: rows,
    cursor: "tok",
    coverage: { covered: [4545], total: 1, omitted: [], capped: false },
    joined: [],
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("the fleet rail's merged tail (#362)", () => {
  it("renders rows from the merged endpoint rather than a pending marker", async () => {
    stubFetch({
      ...SINGLE_NODE,
      "/admin/requests": { json: page([row(4545, "GET", "/v1/charges", 201)]) },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    const tail = await screen.findByTestId("merged-tail");

    expect(tail.textContent).toContain("/v1/charges");
    expect(tail.textContent).toContain("4545");
    expect(tail.textContent).toContain("201");
  });

  it("reads the merged endpoint once, never one request per imposter", async () => {
    const { requests } = stubFetch({
      ...SINGLE_NODE,
      "/imposters": {
        json: {
          imposters: [
            { port: 4545, name: "payments", protocol: "http" },
            { port: 4546, name: "billing", protocol: "http" },
          ],
        },
      },
      "/admin/requests": { json: page([row(4545, "GET", "/v1/charges")]) },
    });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    await screen.findByTestId("merged-tail");

    expect(requests.some((sent) => sent.path.startsWith("/imposters/4545/requests"))).toBe(false);
    expect(requests.some((sent) => sent.path.startsWith("/imposters/4546/requests"))).toBe(false);
  });

  // A rail shows a slice. Presenting that slice as the journal is the failure this panel's whole
  // history is about, so the footer that points at the full log is part of the contract.
  it("caps the rail and says the request log is the full journal", async () => {
    const many = Array.from({ length: 20 }, (_, i) =>
      row(4545, "GET", `/v1/charge-${String(i)}`),
    );
    stubFetch({ ...SINGLE_NODE, "/admin/requests": { json: page(many) } });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    await screen.findByTestId("merged-tail");

    const rendered = screen.getAllByTestId("merged-tail-row");
    expect(rendered.length).toBeLessThan(many.length);
    expect(screen.getByTestId("merged-tail").parentElement?.textContent).toContain(
      "request log is the full journal",
    );
  });

  it("says so when the journal cannot be read, rather than showing an empty tail", async () => {
    stubFetch({ ...SINGLE_NODE, "/admin/requests": { status: 503 } });
    renderInApp(<Imposters />, { whoami: whoamiWith("fleet-admin") });

    // A 5xx is a hiccup by `retryTransportFailures`, so the honest message is one retry and one
    // backoff away — deliberately, and longer than the default `findBy` window.
    expect(await screen.findByTestId("merged-tail-error", undefined, { timeout: 5000 })).toBeTruthy();
    expect(screen.queryByTestId("merged-tail")).toBeNull();
  });
});
