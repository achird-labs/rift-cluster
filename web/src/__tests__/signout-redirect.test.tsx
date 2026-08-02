/** @vitest-environment jsdom */
import { QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { App } from "../App.tsx";
import { createQueryClient } from "../app/query.ts";

afterEach(() => {
  vi.unstubAllGlobals();
  window.location.hash = "";
});

/**
 * A fleet that stops recognising the session once `DELETE /session` lands.
 *
 * Stateful on purpose — this is the whole scenario. A fixed-reply stub cannot express "authorized,
 * then not", which is exactly why the first sign-out test passed against a console that never
 * reached the login screen.
 */
function fleetThatSignsOut(): { deleted: () => boolean } {
  let signedOut = false;
  vi.stubGlobal(
    "fetch",
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/session" && init?.method === "DELETE") {
        signedOut = true;
        return Promise.resolve(new Response(null, { status: 204 }));
      }
      if (path === "/admin/whoami") {
        return Promise.resolve(
          signedOut
            ? new Response(JSON.stringify({ errors: [{ message: "no session" }] }), { status: 401 })
            : new Response(
                JSON.stringify({
                  principalId: "key:abc",
                  displayName: "Demo Editor",
                  authorizationDisabled: false,
                  bindings: [{ tenant: "default", role: "editor" }],
                }),
                { status: 200 },
              ),
        );
      }
      // Every screen read answers empty; this test is about the session, not the data.
      return Promise.resolve(new Response(JSON.stringify({ imposters: [] }), { status: 200 }));
    }),
  );
  return { deleted: () => signedOut };
}

describe("signing out returns the operator to the login screen", () => {
  it("renders the login screen after the session ends", async () => {
    // The failure this pins: `queryClient.clear()` empties the cache but leaves the mounted
    // `whoami` observer holding its last successful result, so nothing refetches and the console
    // keeps rendering the previous principal's shell. Asserting on the cache — as the first
    // version of this test did — passes against exactly that bug.
    const fleet = fleetThatSignsOut();
    render(
      <QueryClientProvider client={createQueryClient()}>
        <App />
      </QueryClientProvider>,
    );

    await screen.findByTestId("identity");
    await userEvent.setup().click(await screen.findByTestId("sign-out"));

    await waitFor(() => expect(fleet.deleted()).toBe(true));
    expect(await screen.findByLabelText(/api key/i)).toBeTruthy();
    expect(screen.queryByTestId("sign-out")).toBeNull();
  });
});
