/** @vitest-environment jsdom */
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Scenarios } from "../screens/Scenarios.tsx";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

const PORT = 4545;
const FLOW = "checkout-1";

const SCENARIOS_PATH = `/imposters/${PORT}/scenarios?flowId=${FLOW}`;
const SPACE_PATH = `/imposters/${PORT}/spaces/${FLOW}`;

/** A node that answers every read this screen makes, for the flow the route names. */
function healthy(overrides: Record<string, { json?: unknown; status?: number }> = {}) {
  return {
    [SCENARIOS_PATH]: {
      json: {
        flowId: FLOW,
        scenarios: [
          { name: "checkout", state: "awaiting-payment" },
          { name: "shipping", state: "start" },
        ],
      },
    },
    [SPACE_PATH]: {
      json: {
        space: FLOW,
        stubs: [{ responses: [{ is: { statusCode: 200 } }] }],
        scenarios: [{ name: "checkout", state: "awaiting-payment" }],
        numberOfRequests: 7,
      },
    },
    ...overrides,
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
  // The screen's tab lives in the hash, so a test that opened one would otherwise leave the next
  // one on it — an order dependence that surfaces as an unrelated failure.
  window.location.hash = "";
});

/**
 * Render the screen, optionally on one of its tabs.
 *
 * Spaces and scenario definitions are their own tabs now, so a test about a space arrives on the
 * one its panel lives on — by hash, the way a bookmark would, rather than by clicking through.
 */
function renderScreen(
  role: Parameters<typeof whoamiWith>[0],
  flow: string | null = FLOW,
  tab: "scenarios" | "spaces" | "defs" = "scenarios",
) {
  if (tab !== "scenarios") window.location.hash = `#/?tab=${tab}`;
  return renderInApp(<Scenarios port={PORT} flow={flow} />, {
    whoami: whoamiWith(role, ["acme"]),
    tenants: ["acme"],
    tenant: "acme",
  });
}

describe("scenarios — the states, under the flow they belong to", () => {
  it("lists every scenario with its state, naming the flow they were read under", async () => {
    stubFetch(healthy());
    renderScreen("viewer");

    expect((await screen.findByTestId("scenario-state-checkout")).textContent).toContain(
      "awaiting-payment",
    );
    expect((await screen.findByTestId("scenario-state-shipping")).textContent).toContain("start");
    // Scenario state is per-space, so a list that does not name its flow is a set of states
    // attributed to nothing in particular.
    expect((await screen.findByTestId("resolved-flow")).textContent).toContain(FLOW);
  });

  it("says an imposter has no scenarios rather than leaving the panel blank", async () => {
    stubFetch(healthy({ [SCENARIOS_PATH]: { json: { flowId: FLOW, scenarios: [] } } }));
    renderScreen("viewer");

    await screen.findByTestId("scenarios-empty");
    expect(screen.queryByTestId("scenarios-unknown")).toBeNull();
  });

  it("renders an unreadable scenario list as unknown, never as an imposter with no scenarios", async () => {
    // The distinction the issue names. An empty table here would tell an operator their stubs
    // declare no scenarios — a confident statement about their configuration, made from a failed read.
    stubFetch(healthy({ [SCENARIOS_PATH]: { status: 503 } }));
    renderScreen("viewer");

    const unknown = await screen.findByTestId("scenarios-unknown");
    expect(unknown.textContent).toMatch(/unknown, not empty/i);
    expect(screen.queryByTestId("scenarios-empty")).toBeNull();
  });
});

describe("scenario controls are gated on the action that authorizes each one", () => {
  it("offers a viewer neither reset nor set-state", async () => {
    stubFetch(healthy());
    renderScreen("viewer");

    await screen.findByTestId("scenario-state-checkout");
    expect(screen.queryByTestId("reset-scenarios")).toBeNull();
    expect(screen.queryByTestId("set-scenario-state-checkout")).toBeNull();
  });

  it("offers an operator reset but NOT set-state", async () => {
    /*
     * The asymmetry that makes this table worth transcribing. `POST .../scenarios/reset` maps to
     * `Action::ScenarioReset` (Operator) and `PUT .../scenarios/{name}/state` to
     * `Action::ScenarioWrite` (Editor) — different arms of `role_allows`. Gating both on one
     * "may write scenarios" notion would draw an operator a control that answers 403 every time.
     */
    stubFetch(healthy());
    renderScreen("operator");

    await screen.findByTestId("reset-scenarios");
    expect(screen.queryByTestId("set-scenario-state-checkout")).toBeNull();
  });

  it("offers an editor both", async () => {
    stubFetch(healthy());
    renderScreen("editor");

    await screen.findByTestId("reset-scenarios");
    await screen.findByTestId("set-scenario-state-checkout");
  });

  it("sets a scenario's state under the flow in view, not under the imposter's default", async () => {
    // `setScenarioState` takes `flowId` in the body and silently writes the *default* flow when it
    // is omitted — so a screen scoped to a flow that failed to send it would move a scenario in a
    // space the operator is not looking at.
    const routes = healthy({
      [`/imposters/${PORT}/scenarios/checkout/state`]: { json: { flowId: FLOW, name: "checkout", state: "paid" } },
    });
    stubFetch(routes);
    renderScreen("editor");

    const user = userEvent.setup();
    await user.click(await screen.findByTestId("set-scenario-state-checkout"));
    await user.clear(await screen.findByTestId("scenario-state-input"));
    await user.type(await screen.findByTestId("scenario-state-input"), "paid");
    await user.click(await screen.findByTestId("scenario-state-save"));

    const mock = globalThis.fetch as unknown as { mock: { calls: [string, RequestInit][] } };
    await waitFor(() => {
      const call = mock.mock.calls.find(([path]) => path.endsWith("/scenarios/checkout/state"));
      expect(call).toBeTruthy();
      expect(JSON.parse(String(call?.[1]?.body))).toEqual({ state: "paid", flowId: FLOW });
    });
  });

  it("still reports a failed state write after the editor was closed", async () => {
    /*
     * Cancel closes the editor, and the write it was waiting on can fail afterwards. With the error
     * note scoped to the open editor the row would drop its only error surface, `onSettled` would
     * refetch, and the old value would reappear with nothing on screen to say the write was
     * refused — a failure that looks exactly like a no-op.
     */
    stubFetch(healthy({ [`/imposters/${PORT}/scenarios/checkout/state`]: { status: 403 } }));
    renderScreen("editor");

    const user = userEvent.setup();
    await user.click(await screen.findByTestId("set-scenario-state-checkout"));
    await user.clear(await screen.findByTestId("scenario-state-input"));
    await user.type(await screen.findByTestId("scenario-state-input"), "paid");
    await user.click(await screen.findByTestId("scenario-state-save"));
    await waitFor(() =>
      expect(screen.getByRole("alert").textContent).toMatch(/state was not set/i),
    );

    // Now close the editor. The note must survive it — that is the whole point: scoped to the open
    // editor, it would vanish here and the refetched old value would be the only thing on screen.
    await user.click(screen.getByRole("button", { name: /cancel/i }));
    expect(screen.queryByTestId("scenario-state-input")).toBeNull();
    expect(screen.getByRole("alert").textContent).toMatch(/state was not set/i);
  });

  it("seeds the state editor when it opens, from the state showing at that moment", async () => {
    // The row outlives the 5s poll, so a draft seeded once at mount holds whatever the state was
    // when the screen first painted — and submitting it after the scenario moved would silently
    // revert it. Seeding on open is what keeps the prefill and the displayed cell the same fact.
    stubFetch(healthy());
    renderScreen("editor");

    await userEvent.setup().click(await screen.findByTestId("set-scenario-state-checkout"));
    const input = (await screen.findByTestId("scenario-state-input")) as HTMLInputElement;
    expect(input.value).toBe("awaiting-payment");
  });

  it("resets only the flow in view", async () => {
    // Same trap on the reset route: an omitted `flowId` resets the imposter's default flow, which
    // is never what a screen scoped to a named space means.
    stubFetch(healthy({ [`/imposters/${PORT}/scenarios/reset`]: { json: { flowId: FLOW, reset: true } } }));
    renderScreen("operator");

    const user = userEvent.setup();
    await user.click(await screen.findByTestId("reset-scenarios"));
    await user.click(await screen.findByTestId("confirm-destructive"));

    const mock = globalThis.fetch as unknown as { mock: { calls: [string, RequestInit][] } };
    await waitFor(() => {
      const call = mock.mock.calls.find(([path]) => path.endsWith("/scenarios/reset"));
      expect(JSON.parse(String(call?.[1]?.body))).toEqual({ flowId: FLOW });
    });
  });
});

describe("a space's stubs are not the imposter's stubs", () => {
  it("renders them in their own table, carrying the caveat permanently", async () => {
    /*
     * The issue's first "must not get wrong". Rendering these beside `/imposters/{port}/stubs`
     * would imply an ownership that does not exist: a space's stubs match only for requests that
     * resolve to that flow, and they never appear on the imposter's own stub list.
     */
    stubFetch(healthy());
    renderScreen("viewer", FLOW, "spaces");

    const table = await screen.findByTestId("space-stubs");
    expect(table.textContent).toBeTruthy();
    const caveat = await screen.findByTestId("space-stub-caveat");
    expect(caveat.textContent).toMatch(/not the imposter's stubs/i);
  });

  it("says when a space stub shadows the rest of its space", async () => {
    /*
     * Issue #336's companion. A predicate-less stub matching everything is correct Mountebank
     * semantics, so nothing refuses it — but this table is addressed by position and has no ids,
     * so "why is stub 1 not answering" has no visible cause without this banner. It is also the
     * shape #336 could silently install before the upstream fix.
     */
    stubFetch(
      healthy({
        [SPACE_PATH]: {
          json: {
            space: FLOW,
            stubs: [
              { responses: [{ is: { statusCode: 200 } }] },
              { predicates: [{ equals: { path: "/late" } }], responses: [{ is: { statusCode: 201 } }] },
            ],
            scenarios: [],
          },
        },
      }),
    );
    renderScreen("viewer", FLOW, "spaces");

    const warning = await screen.findByTestId("space-stub-shadow-warning");
    expect(warning.textContent).toMatch(/matches every request in this space/i);
    // Names the position, because position is the only handle this collection has.
    expect(warning.textContent).toMatch(/\b0\b/);
  });

  it("does not warn when the catch-all is last and shadows nothing", async () => {
    // `healthy()`'s default space is exactly this: one predicate-less stub, nothing after it. That
    // is an ordinary space-wide default, and warning about it would be crying wolf.
    stubFetch(healthy());
    renderScreen("viewer", FLOW, "spaces");

    await screen.findByTestId("space-stubs");
    expect(screen.queryByTestId("space-stub-shadow-warning")).toBeNull();
  });

  it("reports how many requests resolved to the space", async () => {
    stubFetch(healthy());
    renderScreen("viewer", FLOW, "spaces");
    expect((await screen.findByTestId("space-requests")).textContent).toContain("7");
  });

  it("does not invent a count of zero for a body that carried none", async () => {
    // "Nothing reached this space" is the question the operator is asking, not an answer the
    // console may supply on the node's behalf.
    stubFetch(healthy({ [SPACE_PATH]: { json: { space: FLOW, stubs: [], scenarios: [] } } }));
    renderScreen("viewer", FLOW, "spaces");
    await waitFor(async () =>
      expect((await screen.findByTestId("space-requests")).textContent).toContain("—"),
    );
  });

  it("renders a space holding nothing as an answer", async () => {
    stubFetch(healthy({ [SPACE_PATH]: { json: { space: FLOW, stubs: [], scenarios: [], numberOfRequests: 0 } } }));
    renderScreen("viewer", FLOW, "spaces");
    await screen.findByTestId("space-empty");
    expect(screen.queryByTestId("space-unknown")).toBeNull();
  });

  it("renders a space that could not be read as the absence of one", async () => {
    stubFetch(healthy({ [SPACE_PATH]: { status: 503 } }));
    renderScreen("viewer", FLOW, "spaces");
    await screen.findByTestId("space-unknown");
    expect(screen.queryByTestId("space-empty")).toBeNull();
  });

  it("offers a viewer neither teardown nor stub-scoping", async () => {
    stubFetch(healthy());
    renderScreen("viewer", FLOW, "spaces");
    await screen.findByTestId("space-stubs");
    expect(screen.queryByTestId("space-teardown")).toBeNull();
    expect(screen.queryByTestId("space-add-stub")).toBeNull();
  });

  it("offers an operator teardown but NOT stub-scoping", async () => {
    // `SpaceTeardown` is Operator, `SpaceStubWrite` is Editor — the same disturb/redefine split as
    // the scenario controls.
    stubFetch(healthy());
    renderScreen("operator", FLOW, "spaces");
    await screen.findByTestId("space-teardown");
    expect(screen.queryByTestId("space-add-stub")).toBeNull();
  });

  it("offers an editor both", async () => {
    stubFetch(healthy());
    renderScreen("editor", FLOW, "spaces");
    await screen.findByTestId("space-add-stub");
    await screen.findByTestId("space-teardown");
  });
});

describe("flow state", () => {
  const KEY = "cart";
  const ENTRY_PATH = `/admin/imposters/${PORT}/flow-state/${FLOW}/${KEY}`;

  it("reads the key the operator names, since no route lists them", async () => {
    // There is no list-entries route. The panel therefore asks for a key rather than pretending to
    // offer an inventory it cannot build.
    stubFetch(healthy({ [ENTRY_PATH]: { json: { flowId: FLOW, key: KEY, value: { items: 2 } } } }));
    renderScreen("viewer");

    const user = userEvent.setup();
    await user.type(await screen.findByTestId("flow-state-key"), KEY);
    await user.click(await screen.findByTestId("flow-state-read"));

    expect((await screen.findByTestId("flow-state-value")).textContent).toContain("items");
  });

  it("does not claim a key is unset on a 404 it cannot attribute", async () => {
    /*
     * `getFlowStateEntry` documents 404 as "no such entry" — but RFC-002 §8.4 renders
     * `NotBoundToTenant` as 404 too, and so does a missing imposter. Rendering all three as "not
     * set" would tell an operator their key is empty when they may simply be reading an imposter
     * that is not theirs.
     */
    stubFetch(healthy({ [ENTRY_PATH]: { status: 404 } }));
    renderScreen("viewer");

    const user = userEvent.setup();
    await user.type(await screen.findByTestId("flow-state-key"), KEY);
    await user.click(await screen.findByTestId("flow-state-read"));

    const absent = await screen.findByTestId("flow-state-absent");
    expect(absent.textContent).toMatch(/does not by itself prove/i);
  });

  it("renders a stored null as a value, not as an absent key", async () => {
    // The contract declares `value` as any JSON including `null`, so a null is stored data.
    stubFetch(healthy({ [ENTRY_PATH]: { json: { flowId: FLOW, key: KEY, value: null } } }));
    renderScreen("viewer");

    const user = userEvent.setup();
    await user.type(await screen.findByTestId("flow-state-key"), KEY);
    await user.click(await screen.findByTestId("flow-state-read"));

    expect((await screen.findByTestId("flow-state-value")).textContent).toContain("null");
    expect(screen.queryByTestId("flow-state-absent")).toBeNull();
  });

  /** Name a key and ask for it, which is what brings the per-key controls on screen. */
  async function readTheKey() {
    const user = userEvent.setup();
    await user.type(await screen.findByTestId("flow-state-key"), KEY);
    await user.click(await screen.findByTestId("flow-state-read"));
    await screen.findByTestId("flow-state-result");
  }

  it("offers an operator clear but NOT set — the server gates the write on space.stubWrite", async () => {
    /*
     * The mapping that is not guessable from the route. `PUT .../flow-state/{flow}/{key}` is
     * classified as `imposter.write` with a space, which `principal.rs::map_action` turns into
     * `Action::SpaceStubWrite` (Editor) — there is no `FlowStateWrite`. The `DELETE` beside it maps
     * to `Action::FlowStateClear` (Operator).
     *
     * So this panel is deliberately asymmetric for an operator, and making it symmetrical would
     * mean drawing a control the server refuses.
     */
    stubFetch(healthy({ [ENTRY_PATH]: { json: { flowId: FLOW, key: KEY, value: 1 } } }));
    renderScreen("operator");
    await readTheKey();

    await screen.findByTestId("flow-state-clear-key");
    await screen.findByTestId("flow-state-clear-all");
    expect(screen.queryByTestId("flow-state-set")).toBeNull();
  });

  it("offers an editor the set control the operator is refused", async () => {
    stubFetch(healthy({ [ENTRY_PATH]: { json: { flowId: FLOW, key: KEY, value: 1 } } }));
    renderScreen("editor");
    await readTheKey();

    await screen.findByTestId("flow-state-set");
  });

  it("offers a viewer no flow-state write at all, even with a key on screen", async () => {
    stubFetch(healthy({ [ENTRY_PATH]: { json: { flowId: FLOW, key: KEY, value: 1 } } }));
    renderScreen("viewer");
    await readTheKey();

    expect(screen.queryByTestId("flow-state-set")).toBeNull();
    expect(screen.queryByTestId("flow-state-clear-key")).toBeNull();
    expect(screen.queryByTestId("flow-state-clear-all")).toBeNull();
  });
});

describe("choosing what to look at", () => {
  it("asks which imposter when the route names none", async () => {
    stubFetch({ "/imposters": { json: { imposters: [{ port: PORT, name: "checkout-api" }] } } });
    renderInApp(<Scenarios port={null} flow={null} />, {
      whoami: whoamiWith("viewer", ["acme"]),
      tenants: ["acme"],
      tenant: "acme",
    });

    expect((await screen.findByRole("link", { name: /open/i })).getAttribute("href")).toBe(
      `#/scenarios/${PORT}`,
    );
  });

  it("reads the imposter's own default flow when the route names none, and says which it was", async () => {
    // `listScenarios` echoes the flow it resolved precisely so a caller that sent none can learn it.
    // The screen must show that resolved id rather than the word "default", which is a guess.
    stubFetch({
      [`/imposters/${PORT}/scenarios`]: { json: { flowId: "resolved-default", scenarios: [] } },
      [`/imposters/${PORT}/spaces/resolved-default`]: {
        json: { space: "resolved-default", stubs: [], scenarios: [], numberOfRequests: 0 },
      },
    });
    renderScreen("viewer", null);

    // `waitFor`, not a bare `find`: the strip renders immediately with a placeholder and the
    // resolved id only lands when the scenario read does. Asserting on first paint would pass on
    // the placeholder and never exercise the echo this test exists for.
    await waitFor(async () =>
      expect((await screen.findByTestId("resolved-flow")).textContent).toContain("resolved-default"),
    );
  });
});
