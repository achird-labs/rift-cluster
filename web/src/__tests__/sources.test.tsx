/** @vitest-environment jsdom */
import { screen } from "@testing-library/react";
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
