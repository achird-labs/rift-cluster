import { describe, expect, it } from "vitest";

import { KEY_NOT_SHOWN_AGAIN, stripApiKey } from "./key.ts";

// Pure functions only — no DOM, no Web Storage, so this file stays in the default node environment.
// The assertions that the key never reaches `localStorage`/`sessionStorage` belong to the screen
// and live in `__tests__/admin.test.tsx`, which runs under jsdom.

/**
 * The API mints the raw key once, in the `201` from `POST /admin/tenants/:id/principals`, and
 * stores only an argon2id hash. Everything here exists so the console cannot accidentally imply
 * otherwise.
 */
describe("the minted key never outlives the moment it is shown", () => {
  const issued = {
    id: "p-9",
    displayName: "ci",
    role: "editor" as const,
    tenant: "acme",
    apiKey: "rk_live_SECRETVALUE",
  };

  // The cache is the dangerous one: a mutation result lands in it by default, survives navigation,
  // and is trivially readable from devtools long after the "shown once" panel is gone.
  it("strips the raw key from anything that could be cached", () => {
    const cached = stripApiKey(issued);
    expect(JSON.stringify(cached)).not.toContain("rk_live_SECRETVALUE");
    expect("apiKey" in cached).toBe(false);
  });

  it("keeps the rest of the record, so the caller can still show who was created", () => {
    const cached = stripApiKey(issued);
    expect(cached.id).toBe("p-9");
    expect(cached.displayName).toBe("ci");
    expect(cached.role).toBe("editor");
    expect(cached.tenant).toBe("acme");
  });

  it("does not mutate the response it was given", () => {
    stripApiKey(issued);
    expect(issued.apiKey).toBe("rk_live_SECRETVALUE");
  });

  // Wording is part of the contract with the operator: a key they cannot re-read must say so at the
  // moment they can still copy it.
  it("states plainly that the key will not be shown again", () => {
    expect(KEY_NOT_SHOWN_AGAIN.toLowerCase()).toContain("not be shown again");
  });
});
