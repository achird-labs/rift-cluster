/** @vitest-environment jsdom */
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Sources } from "../screens/Sources.tsx";
import { parseHash, toHash } from "../app/routing.ts";
import { NAV, plannedEntries } from "../app/nav.ts";
import { can } from "../app/rbac.ts";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

/** A source that pulled cleanly: nothing drifted, nothing failing. */
const CLEAN = {
  id: "mocks",
  uri: "https://cfg.internal/imposters.json",
  mode: "pinned",
  onDrift: "overwrite",
  drifted: false,
  lastVersion: "v7",
  lastDigest: "sha256:abcd",
  lastPulledAtSecs: 1_764_000_000,
  lastOutcome: "applied",
  ports: [9301, 9302],
  revision: 42,
};

/** Hand-edited since it last applied — the state `onDrift` exists to decide about. */
const DRIFTED = {
  id: "payments",
  uri: "https://cfg.internal/payments.json",
  mode: "tracking",
  pollSecs: 30,
  onDrift: "skip",
  drifted: true,
  lastVersion: "v2",
  lastPulledAtSecs: 1_763_000_000,
  lastOutcome: "skipped",
  ports: [9401],
  revision: 17,
};

/** Declared but never pulled: it has no drift *answer*, which is not the same as "no drift". */
const NEVER_PULLED = {
  id: "fresh",
  uri: "https://cfg.internal/fresh.json",
  mode: "pinned",
  onDrift: "fail",
  drifted: false,
  ports: [],
  revision: 3,
};

function listing(sources: unknown[], pollErrors: Record<string, string> = {}) {
  return {
    "/admin/sources": {
      json: { sources, nodeLocal: { nodeId: 2, pollErrors } },
    },
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("sources screen", () => {
  it("lists each source with its uri, mode and the ports it owns", async () => {
    stubFetch(listing([CLEAN, DRIFTED]));
    renderInApp(<Sources />, { whoami: whoamiWith("viewer") });

    const row = await screen.findByTestId("source-row-mocks");
    expect(row.textContent).toContain("mocks");
    expect(row.textContent).toContain("https://cfg.internal/imposters.json");
    expect(row.textContent).toMatch(/pinned/i);
    expect(row.textContent).toContain("9301");
    expect(row.textContent).toContain("9302");

    const tracking = screen.getByTestId("source-row-payments");
    expect(tracking.textContent).toMatch(/tracking/i);
    // The cadence is what an operator checks when asking "why has this not updated".
    expect(tracking.textContent).toContain("30");
  });

  it("names a drifted source as drifted, and says what the next pull will do about it", async () => {
    stubFetch(listing([DRIFTED]));
    renderInApp(<Sources />, { whoami: whoamiWith("viewer") });

    const drift = await screen.findByTestId("source-drift-payments");
    expect(drift.textContent).toMatch(/drift/i);
    // `onDrift: skip` is the difference between "this will be repaired" and "this will be left
    // alone forever" — a drift badge that does not say which is a colour, not an answer.
    expect(screen.getByTestId("source-row-payments").textContent).toMatch(/skip/i);
  });

  it("keeps a failed poll out of the drift verdict, and shows it as its own node-scoped fact", async () => {
    /*
     * The issue framed this as "a source that could not be re-read is not a source that has not
     * drifted". The instinct is right; the field it names is not. `SourceRecord.drifted` is
     * *replicated* state — `store.rs` flips it in the imposter write/delete apply path, at the same
     * log index on every replica — so `drifted: false` is a current fleet fact, not the residue of
     * the last pull.
     *
     * Letting a poll error downgrade it to "unknown" would be wrong twice over. It manufactures
     * doubt about something the fleet actually knows; and because polls are **leader-only**, the
     * error map is empty by construction on a follower, so the same source would read "unknown" or
     * "clean" depending only on which node answered.
     *
     * So the two facts are rendered side by side and neither impersonates the other: drift stays a
     * confident replicated verdict, and the poll failure appears as this node's own observation.
     */
    stubFetch(listing([{ ...CLEAN, id: "stale" }], { stale: "connection refused" }));
    renderInApp(<Sources />, { whoami: whoamiWith("viewer") });

    const drift = await screen.findByTestId("source-drift-stale");
    expect(drift.textContent).toMatch(/clean|no drift/i);
    // The failure is not swallowed by that verdict — it is shown, in its own cell.
    const poll = screen.getByTestId("source-poll-stale");
    expect(poll.textContent).toMatch(/connection refused/i);
  });

  it("does not present an empty poll-error map as evidence that polling is healthy", async () => {
    /*
     * The follower case. Polls run on the leader, so `pollErrors` is empty on a follower whether or
     * not anything is failing. A green "polling OK" here would be a fact invented from an absence.
     */
    stubFetch(listing([CLEAN]));
    renderInApp(<Sources />, { whoami: whoamiWith("viewer") });

    const poll = await screen.findByTestId("source-poll-mocks");
    expect(poll.textContent).not.toMatch(/\bok\b|healthy|success/i);
    expect(screen.getByTestId("sources-node-scope").textContent).toMatch(
      /leader|empty.*not evidence|no error.*not/i,
    );
  });

  it("does not report a source that has never pulled as clean either", async () => {
    stubFetch(listing([NEVER_PULLED]));
    renderInApp(<Sources />, { whoami: whoamiWith("viewer") });

    const drift = await screen.findByTestId("source-drift-fresh");
    expect(drift.textContent).not.toMatch(/^clean/i);
    expect(drift.textContent).toMatch(/never pulled|unknown/i);
  });

  it("reports a genuinely clean source as clean", async () => {
    // The counterpart to the above: hedging must be reserved for the case that warrants it — a
    // source with no drift answer yet — or it degenerates into never answering the question.
    stubFetch(listing([CLEAN]));
    renderInApp(<Sources />, { whoami: whoamiWith("viewer") });

    expect((await screen.findByTestId("source-drift-mocks")).textContent).toMatch(/clean|no drift/i);
  });

  it("shows what each source last produced, and at which revision", async () => {
    // The issue's provenance criterion: "what it produced, at which revision". A screen that lists
    // sources without it cannot answer the question an operator actually arrives with.
    stubFetch(listing([CLEAN]));
    renderInApp(<Sources />, { whoami: whoamiWith("viewer") });

    const row = await screen.findByTestId("source-row-mocks");
    expect(row.textContent).toContain("v7");
    expect(row.textContent).toContain("42");
    expect(screen.getByTestId("source-pulled-mocks").textContent).not.toBe("");
  });

  it("renders a never-pulled source's provenance as unknown rather than as blanks or zeroes", async () => {
    stubFetch(listing([NEVER_PULLED]));
    renderInApp(<Sources />, { whoami: whoamiWith("viewer") });

    // `lastVersion` and `lastPulledAtSecs` are absent on the wire. Rendering either as `0`, or as
    // an empty cell indistinguishable from a real empty value, is the confident-answer-from-nothing
    // failure this screen exists to avoid.
    const pulled = await screen.findByTestId("source-pulled-fresh");
    expect(pulled.textContent).toMatch(/—|never|unknown/i);
    expect(pulled.textContent).not.toMatch(/1970|^0$/);
  });

  it("keeps the node-local poll status apart from the replicated record, and names the node", async () => {
    /*
     * `nodeLocal` is one node's reach to an external host at one moment; `sources` is fleet-
     * replicated state. Merging them would tell an operator the fleet is failing to poll when one
     * node is — the same honesty problem the request log solved with its per-node scope strip.
     */
    stubFetch(listing([CLEAN], { mocks: "502 from upstream" }));
    renderInApp(<Sources />, { whoami: whoamiWith("viewer") });

    const scope = await screen.findByTestId("sources-node-scope");
    expect(scope.textContent).toMatch(/this node/i);
    expect(scope.textContent).toContain("2");
    expect(scope.textContent).not.toMatch(/fleet-wide/i);
  });

  it("says so when the fleet has no sources, rather than rendering an empty table", async () => {
    stubFetch(listing([]));
    renderInApp(<Sources />, { whoami: whoamiWith("viewer") });

    expect((await screen.findByTestId("sources-empty")).textContent).toMatch(/no .*source/i);
  });

  it("explains a refusal as scope rather than as a missing page", async () => {
    // RFC-002 §8.4 makes "you may not" and "it is not there" indistinguishable on the wire.
    stubFetch({ "/admin/sources": { status: 404, json: { message: "not found" } } });
    renderInApp(<Sources />, { whoami: whoamiWith("viewer") });

    expect((await screen.findByRole("alert")).textContent).toMatch(
      /not available to this principal|source\.read/i,
    );
  });
});

describe("sources is a shipped screen, not a roadmap chip", () => {
  it("no longer appears as a planned entry pointing at its own issue", () => {
    // The gap #233 was filed for: the nav told an operator "not yet shipped, see #233" while the
    // screen it names is this one.
    expect(plannedEntries().some((entry) => entry.id === "sources")).toBe(false);
  });

  it("is a live entry routed to the sources screen and gated on source.read", () => {
    const entry = NAV.find((e) => e.id === "sources");
    expect(entry?.kind).toBe("live");
    if (entry?.kind !== "live") throw new Error("unreachable — asserted above");
    expect(entry.route).toEqual({ screen: "sources" });
    expect(entry.requires).toBe("source.read");
  });
});

describe("sources routing", () => {
  it("round-trips #/sources", () => {
    expect(parseHash("#/sources")).toEqual({ screen: "sources" });
    expect(toHash({ screen: "sources" })).toBe("#/sources");
  });

  it("falls back rather than inventing a member route", () => {
    // There is no per-source screen, so `#/sources/mocks` is a stale bookmark, not a route.
    expect(parseHash("#/sources/mocks")).toEqual({ screen: "imposters" });
  });
});

describe("source.read mirrors authz.rs", () => {
  it("starts at viewer, like the other reads", () => {
    // `Action::SourceRead` sits in `role_allows`'s viewer arm alongside `ImposterRead`.
    for (const role of ["viewer", "operator", "editor", "tenant-admin", "fleet-admin"] as const) {
      expect(can(whoamiWith(role), "acme", "source.read")).toBe(true);
    }
  });
});

describe("declare/edit form", () => {
  it("is offered to an editor, who holds imposter.write", async () => {
    stubFetch(listing([CLEAN]));
    renderInApp(<Sources />, { whoami: whoamiWith("editor") });

    expect(await screen.findByTestId("new-source")).toBeTruthy();
  });

  it("is hidden from a viewer, who still sees the list", async () => {
    // Presentation only — the admin front refuses the same principal either way — but a form that
    // can only ever answer 403 is worse than no form, and a hidden write control must not cost the
    // read the screen is still entitled to.
    stubFetch(listing([CLEAN]));
    renderInApp(<Sources />, { whoami: whoamiWith("viewer") });

    const row = await screen.findByTestId("source-row-mocks");
    expect(row.textContent).toContain("mocks");
    expect(screen.queryByTestId("new-source")).toBeNull();
    expect(screen.queryByTestId("source-edit-mocks")).toBeNull();
    expect(screen.queryByTestId("source-pull-mocks")).toBeNull();
    expect(screen.queryByTestId("source-delete")).toBeNull();
  });

  it("shows the poll interval only when mode is tracking, and hides it again for pinned", async () => {
    stubFetch(listing([CLEAN]));
    renderInApp(<Sources />, { whoami: whoamiWith("editor") });

    const user = userEvent.setup();
    await user.click(await screen.findByTestId("new-source"));
    // `CLEAN` is pinned, and `Declare source` (not `Edit`) starts from a blank form either way —
    // the default is `pinned`, so the field must not be there yet.
    expect(screen.queryByLabelText(/poll interval/i)).toBeNull();

    await user.selectOptions(screen.getByLabelText(/^mode$/i), "tracking");
    expect(screen.getByLabelText(/poll interval/i)).toBeTruthy();

    await user.selectOptions(screen.getByLabelText(/^mode$/i), "pinned");
    expect(screen.queryByLabelText(/poll interval/i)).toBeNull();
  });

  it("renders a server refusal verbatim rather than replacing it with a generic message", async () => {
    /*
     * The refusal text is written to be actionable (names the scheme, names what this build
     * actually serves) and must reach the operator unmodified — a console that swapped it for
     * "400 Bad Request" would throw away the one thing the server said that the operator can act
     * on. `GET` and `POST` share the `/admin/sources` path, so `stubFetch`'s path-only routing
     * cannot serve both from one entry; this mocks `fetch` directly, keyed on method as well.
     */
    const REFUSAL =
      "no imposter source is registered for the `ftp:` scheme; this build serves: git+https, file";
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        const path = typeof input === "string" ? input : input.toString();
        const method = init?.method ?? "GET";
        if (path === "/admin/sources" && method === "GET") {
          return Promise.resolve(
            new Response(JSON.stringify(listing([CLEAN])["/admin/sources"].json), { status: 200 }),
          );
        }
        if (path === "/admin/sources" && method === "POST") {
          return Promise.resolve(
            new Response(
              JSON.stringify({ errors: [{ code: "bad_data", type: "bad_data", message: REFUSAL }] }),
              { status: 400 },
            ),
          );
        }
        return Promise.reject(new Error(`test stub has no reply for ${method} ${path}`));
      }),
    );
    renderInApp(<Sources />, { whoami: whoamiWith("editor") });

    const user = userEvent.setup();
    await user.click(await screen.findByTestId("new-source"));
    await user.type(screen.getByLabelText(/^id$/i), "bad");
    await user.type(screen.getByLabelText(/^uri$/i), "ftp://host/x.json");
    await user.click(screen.getByTestId("source-save"));

    expect((await screen.findByRole("alert")).textContent).toContain(REFUSAL);
  });
});

describe("forgetting a source", () => {
  it("is hidden without imposter.delete", async () => {
    stubFetch(listing([CLEAN]));
    renderInApp(<Sources />, { whoami: whoamiWith("operator") });

    await screen.findByTestId("source-row-mocks");
    expect(screen.queryByTestId("source-delete")).toBeNull();
  });

  it("asks before it forgets, and names the orphan semantics rather than 'are you sure?'", async () => {
    stubFetch(listing([CLEAN]));
    renderInApp(<Sources />, { whoami: whoamiWith("editor") });

    await userEvent.setup().click(await screen.findByTestId("source-delete"));

    const dialog = await screen.findByTestId("confirm-delete-source");
    // The decided semantics: forgetting a source never cascades. Its imposters keep running,
    // hand-managed from then on, with nothing left to reapply them — not "are you sure?".
    expect(dialog.textContent).toMatch(/orphan/i);
    expect(dialog.textContent).toMatch(/hand-managed/i);
    expect(dialog.textContent).toMatch(/never cascades|not undeployed/i);
    // Nothing sent yet — the dialog only asks, it does not act.
    expect(vi.mocked(fetch).mock.calls.some(([, init]) => init?.method === "DELETE")).toBe(false);
  });

  it("sends the delete once confirmed", async () => {
    stubFetch({ ...listing([CLEAN]), "/admin/sources/mocks": { status: 204 } });
    renderInApp(<Sources />, { whoami: whoamiWith("editor") });

    const user = userEvent.setup();
    await user.click(await screen.findByTestId("source-delete"));
    await user.click(screen.getByTestId("confirm-destructive"));

    await waitFor(() =>
      expect(
        vi
          .mocked(fetch)
          .mock.calls.some(
            ([input, init]) => String(input) === "/admin/sources/mocks" && init?.method === "DELETE",
          ),
      ).toBe(true),
    );
  });
});

describe("pull now", () => {
  it("renders what a pull reported: changed ports, unchanged ports, and that it ran", async () => {
    stubFetch({
      ...listing([CLEAN]),
      "/admin/sources/mocks/pull": {
        json: { revision: 42, digest: "d", unchanged: false, skipped: false, changed: [9301], warnings: ["port 9302 was left alone: it is hand-managed"] },
      },
    });
    renderInApp(<Sources />, { whoami: whoamiWith("editor") });

    await userEvent.setup().click(await screen.findByTestId("source-pull-mocks"));

    const report = await screen.findByTestId("source-pull-report");
    expect(report.textContent).toMatch(/applied/i);
    expect(report.textContent).toContain("9301");
    // A pull that replaced a port must not read as "nothing changed". An earlier draft invented
    // `changedPorts`/`unchangedPorts`; the server sends `changed` plus BOOLEAN `unchanged`, so the
    // guessed shape rendered the exact opposite of what happened.
    expect(report.textContent).not.toMatch(/no port changed/i);
    // Server-authored caveats are the part the operator needs; dropping them hides what was skipped.
    expect(screen.getByTestId("source-pull-warnings").textContent).toContain("hand-managed");
  });

  it("distinguishes an unchanged pull from one that changed nothing", async () => {
    /*
     * `unchanged` means the fetched content matched what was last applied, so nothing reached the
     * log at all — a different fact from "it applied and no port moved", and different again from
     * a drifted source that was skipped. Collapsing them would hide a source that is silently no
     * longer tracking.
     */
    stubFetch({
      ...listing([CLEAN]),
      "/admin/sources/mocks/pull": {
        json: { revision: 42, digest: "d", unchanged: true, skipped: false, changed: [], warnings: [] },
      },
    });
    renderInApp(<Sources />, { whoami: whoamiWith("editor") });

    await userEvent.setup().click(await screen.findByTestId("source-pull-mocks"));

    const report = await screen.findByTestId("source-pull-report");
    expect(report.textContent).toMatch(/unchanged/i);
    expect(report.textContent).not.toMatch(/skipped/i);
  });

  it("renders a skipped pull as skipped, not as an empty change set", async () => {
    stubFetch({
      ...listing([DRIFTED]),
      "/admin/sources/payments/pull": {
        json: { revision: 42, digest: "d", unchanged: false, skipped: true, changed: [], warnings: [] },
      },
    });
    renderInApp(<Sources />, { whoami: whoamiWith("editor") });

    await userEvent.setup().click(await screen.findByTestId("source-pull-payments"));

    expect((await screen.findByTestId("source-pull-report")).textContent).toMatch(/skipped/i);
  });

  it("is hidden without imposter.write", async () => {
    stubFetch(listing([CLEAN]));
    renderInApp(<Sources />, { whoami: whoamiWith("operator") });

    await screen.findByTestId("source-row-mocks");
    expect(screen.queryByTestId("source-pull-mocks")).toBeNull();
  });
});
