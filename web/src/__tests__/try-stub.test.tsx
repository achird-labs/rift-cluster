/** @vitest-environment jsdom */
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { createQueryClient } from "../app/query.ts";
import { ImposterDetail } from "../screens/ImposterDetail.tsx";
import { type Reply, renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

/**
 * Issue #335: the console's Send button.
 *
 * `sample.ts` (#334) already derives the request and has its own tests, and the endpoint's
 * containment is gated in Rust. What only these tests can prove is the wiring: that the derived
 * sample is what gets posted, that the answer is *shown* rather than swallowed, that a failure is
 * distinguishable from a mock that answered badly, and that the Operator/Viewer line the action
 * table draws is the line the UI actually draws.
 */

const PORT = 4545;
const TRY_PATH = `/admin/imposters/${PORT}/try`;

const STUB = {
  id: "s-1",
  predicates: [{ equals: { method: "GET", path: "/users" } }],
  responses: [{ is: { statusCode: 200, body: "[]" } }],
};

function imposter(stubs: unknown[]): Record<string, unknown> {
  return {
    port: PORT,
    host: "0.0.0.0",
    protocol: "http",
    name: "billing",
    recordRequests: false,
    enabled: true,
    stubs,
  };
}

function fleet(tryReply: Reply): ReturnType<typeof stubFetch> {
  return stubFetch({
    [`/imposters/${PORT}`]: { json: imposter([STUB]) },
    [TRY_PATH]: tryReply,
  });
}

const ANSWERED: Reply = {
  json: {
    status: 201,
    headers: [{ name: "content-type", value: "application/json" }],
    body: '{"ok":true}',
    elapsedMs: 7,
  },
};

async function send(): Promise<void> {
  const button = await screen.findByTestId(`try-stub-${STUB.id}`);
  await userEvent.setup().click(button);
}

describe("the Send button", () => {
  it("posts the request derived from the stub's own predicates", async () => {
    const { requests } = fleet(ANSWERED);
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("operator") });

    await send();

    await waitFor(() => expect(requests.some((r) => r.path === TRY_PATH)).toBe(true));
    const sent = requests.find((r) => r.path === TRY_PATH);
    expect(sent?.method).toBe("POST");
    // The stub's `equals` predicate names both, so the sample is derived, not defaulted — a
    // `GET /` here would mean the button is sending a guess with the stub's name on it.
    const body = JSON.parse(String(sent?.body)) as Record<string, unknown>;
    expect(body.method).toBe("GET");
    expect(body.path).toBe("/users");
    // The envelope carries no addressing of its own; the port in the path is the whole address.
    expect(body).not.toHaveProperty("host");
    expect(body).not.toHaveProperty("scheme");
    expect(body).not.toHaveProperty("url");
  });

  it("shows the status, headers and body the imposter answered", async () => {
    fleet(ANSWERED);
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("operator") });

    await send();

    const panel = await screen.findByTestId(`try-result-${STUB.id}`);
    expect(panel.textContent).toContain("201");
    expect(panel.textContent).toContain("content-type");
    expect(panel.textContent).toContain('{"ok":true}');
    expect(panel.textContent).toContain("7 ms");
  });

  it("shows a 4xx from the mock as the mock's own answer, not as a failure", async () => {
    // The endpoint answers 200; the imposter's 404 rides inside. An operator diagnosing a stub
    // that does not match is looking at *exactly* this case, so rendering it as an error would
    // hide the one answer they came for.
    fleet({
      json: { status: 404, headers: [], body: "no such route", elapsedMs: 2 },
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("operator") });

    await send();

    const panel = await screen.findByTestId(`try-result-${STUB.id}`);
    expect(panel.textContent).toContain("404");
    expect(panel.textContent).toContain("no such route");
    expect(screen.queryByTestId(`try-error-${STUB.id}`)).toBeNull();
  });

  it("distinguishes the endpoint failing from the mock answering", async () => {
    // A 502 means the exchange never happened. Showing it in the response panel as "status 502"
    // would tell the operator their mock returned 502, which is a different bug to chase.
    fleet({ status: 502, json: { code: "backend-unavailable", message: "could not reach it" } });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("operator") });

    await send();

    expect(await screen.findByTestId(`try-error-${STUB.id}`)).not.toBeNull();
    expect(screen.queryByTestId(`try-result-${STUB.id}`)).toBeNull();
  });

  it("says when the body it is showing was cut", async () => {
    fleet({
      json: { status: 200, headers: [], body: "xxx", truncated: true, elapsedMs: 1 },
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("operator") });

    await send();

    // Silence here would let an operator read a truncated body as a mismatch in their mock.
    const cut = await screen.findByTestId(`try-result-${STUB.id}`);
    expect(cut.textContent).toMatch(/truncated/i);
  });

  it("says when the body it is showing was not valid UTF-8", async () => {
    fleet({
      json: { status: 200, headers: [], body: "��", bodyLossy: true, elapsedMs: 1 },
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("operator") });

    await send();

    const lossy = await screen.findByTestId(`try-result-${STUB.id}`);
    expect(lossy.textContent).toMatch(/not valid UTF-8|replacement/i);
  });

  it("says when a header value was not valid UTF-8", async () => {
    // Its own flag, and its own line: a fault-injecting mock garbles headers on purpose, and the
    // console renders the value verbatim — so an unflagged substitution shows characters the mock
    // never sent, in exactly the header the operator is inspecting.
    fleet({
      json: {
        status: 200,
        headers: [{ name: "x-sig", value: "��" }],
        body: "ok",
        headersLossy: true,
        elapsedMs: 1,
      },
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("operator") });

    await send();

    const panel = await screen.findByTestId(`try-result-${STUB.id}`);
    expect(panel.textContent).toMatch(/header value was not valid UTF-8/i);
  });

  it("drops a result once the stub it answered has changed", async () => {
    // The panel's entire purpose is "did my stub match". A `useMutation`'s `data` outlives the
    // request that produced it, and a row keyed by stub id survives an edit — so without tying the
    // result to what was actually sent, editing a predicate leaves the previous verdict on screen
    // beside the new stub and the operator reads a stale answer as the new one.
    const before = { ...STUB };
    const after = {
      ...STUB,
      predicates: [{ equals: { method: "GET", path: "/changed" } }],
    };
    let current: unknown = before;
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = typeof input === "string" ? input : input.toString();
        if (path === TRY_PATH) {
          return Promise.resolve(
            new Response(JSON.stringify({ status: 200, headers: [], body: "old", elapsedMs: 1 })),
          );
        }
        return Promise.resolve(new Response(JSON.stringify(imposter([current]))));
      }),
    );

    // Its own client so the refetch below can be driven directly. `rerender` is not usable here:
    // it re-renders the bare element *outside* the providers `renderInApp` mounted, which is a
    // different failure. Invalidating is also the real mechanism — it is what a save does.
    const client = createQueryClient();
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("operator"), client });
    await send();
    expect((await screen.findByTestId(`try-result-${STUB.id}`)).textContent).toContain("old");

    // The stub is edited underneath the row — same id, so the row is not remounted — and the
    // answer it produced no longer describes it.
    current = after;
    await client.invalidateQueries();

    await waitFor(() => expect(screen.queryByTestId(`try-result-${STUB.id}`)).toBeNull());
  });

  it("is offered to an Operator and withheld from a Viewer, who keeps Copy curl", async () => {
    fleet(ANSWERED);
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("viewer") });

    // The line #335 settled: a Viewer may compose a request, but not have the server send one.
    expect(await screen.findByTestId(`copy-curl-${STUB.id}`)).not.toBeNull();
    expect(screen.queryByTestId(`try-stub-${STUB.id}`)).toBeNull();
  });

  it("offers neither control for a stub whose predicates cannot be modelled", async () => {
    // Exactly the rule `CopyCurlButton` already follows, and the two must not diverge: a stub
    // whose predicates the form cannot represent (`kind: "rawOnly"`) is the one whose request
    // cannot be derived, so offering a Send there would be a guess that mutates state. Note this
    // is *not* the same as a sample with caveats — see the test below, where both are offered.
    const unmodellable = { id: "s-9", predicates: "not even an array" };
    stubFetch({
      [`/imposters/${PORT}`]: { json: imposter([unmodellable]) },
      [TRY_PATH]: ANSWERED,
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("operator") });

    await screen.findByText(/billing/);
    await waitFor(() => expect(screen.queryByTestId("copy-curl-s-9")).toBeNull());
    expect(screen.queryByTestId("try-stub-s-9")).toBeNull();
  });

  it("still offers Send for a caveated sample, and says the request may not match", async () => {
    // A `not` group cannot be turned into a value, so `sample.ts` skips it and records a caveat —
    // the sample is still sendable, it just may not match. Withholding Send here would deny the
    // affordance to exactly the stub an operator is most likely to be puzzling over; offering it
    // *without* surfacing the caveat would let them read a non-match as a bug in the mock rather
    // than as a request that never satisfied the predicate in the first place.
    const caveated = {
      id: "s-8",
      predicates: [{ equals: { path: "/x" } }, { not: { equals: { path: "/y" } } }],
      responses: [{ is: { statusCode: 200 } }],
    };
    stubFetch({
      [`/imposters/${PORT}`]: { json: imposter([caveated]) },
      [TRY_PATH]: ANSWERED,
    });
    renderInApp(<ImposterDetail port={PORT} />, { whoami: whoamiWith("operator") });

    const button = await screen.findByTestId("try-stub-s-8");
    expect(button).not.toBeNull();
    await userEvent.setup().click(button);

    const panel = await screen.findByTestId("try-result-s-8");
    expect(panel.textContent).toMatch(/caveat/i);
  });
});
