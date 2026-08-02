/** @vitest-environment jsdom */
import { QueryClient } from "@tanstack/react-query";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { SignOut } from "../app/SignOut.tsx";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("sign out", () => {
  it("asks the server to end the session rather than only forgetting it locally", async () => {
    // The cookie is HttpOnly, so nothing on this page can clear it. A console that dropped its
    // cache without calling `DELETE /session` would show the login screen while every request it
    // made still carried the previous principal — signed out in appearance only.
    const { calls } = stubFetch({ "/session": { status: 204 } });
    renderInApp(<SignOut />, { whoami: whoamiWith("editor") });

    await userEvent.setup().click(screen.getByTestId("sign-out"));

    await waitFor(() => expect(calls).toContain("/session"));
    expect(vi.mocked(fetch).mock.calls[0]?.[1]).toMatchObject({ method: "DELETE" });
  });

  it("removes the previous principal's cached reads rather than refetching them", async () => {
    /*
     * Removal, not invalidation. Invalidation keeps the old data readable while the refetches are in
     * flight, so one operator's imposters and tenants would stay on screen for whoever signs in
     * next.
     *
     * `whoami` is the deliberate exception and is reset instead, so the mounted observer refetches
     * and its 401 moves the console to the login screen — see `signout-redirect.test.tsx`, which
     * covers that end of it. This test owns the other end: everything that is not `whoami` is gone.
     */
    const client = new QueryClient();
    client.setQueryData(["imposters"], [{ port: 4545, name: "someone-elses" }]);
    client.setQueryData(["tenants"], ["acme"]);
    stubFetch({ "/session": { status: 204 } });
    renderInApp(<SignOut />, { whoami: whoamiWith("editor"), client });

    await userEvent.setup().click(screen.getByTestId("sign-out"));

    await waitFor(() => expect(client.getQueryData(["imposters"])).toBeUndefined());
    expect(client.getQueryData(["tenants"])).toBeUndefined();
  });

  it("says so when the server refused, because the session is then still live", async () => {
    // The failure that matters: reporting a sign-out that did not happen leaves the operator
    // believing they are off a fleet they are still authenticated against.
    const client = new QueryClient();
    client.setQueryData(["imposters"], [{ port: 4545 }]);
    stubFetch({ "/session": { status: 500, json: { errors: [{ message: "boom" }] } } });
    renderInApp(<SignOut />, { whoami: whoamiWith("editor"), client });

    await userEvent.setup().click(screen.getByTestId("sign-out"));

    expect((await screen.findByRole("alert")).textContent).toMatch(/still signed in/i);
    // And the cache is untouched: it would be a lie to clear it while the session survives.
    expect(client.getQueryData(["imposters"])).toBeDefined();
  });
});
