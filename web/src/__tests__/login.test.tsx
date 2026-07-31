/** @vitest-environment jsdom */
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Login } from "../screens/Login.tsx";
import { stubFetch } from "./harness.tsx";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("sign in (C2's POST /session)", () => {
  it("exchanges the API key for a session cookie and reports success", async () => {
    stubFetch({ "/session": { status: 200 } });
    const onAuthenticated = vi.fn();
    render(<Login onAuthenticated={onAuthenticated} />);

    const user = userEvent.setup();
    await user.type(screen.getByLabelText(/api key/i), "sk-test-key");
    await user.click(screen.getByRole("button", { name: /sign in/i }));

    await waitFor(() => expect(onAuthenticated).toHaveBeenCalled());
    const mock = globalThis.fetch as unknown as { mock: { calls: [string, RequestInit][] } };
    const call = mock.mock.calls.find(([path]) => path === "/session");
    expect(call?.[1]?.method).toBe("POST");
    expect(JSON.parse(call?.[1]?.body as string)).toEqual({ apiKey: "sk-test-key" });
  });

  it("never puts the key in storage or the URL", async () => {
    // RFC-006 §9.3: `POST /session` is the one moment the long-lived API key transits the page, so
    // it belongs in component state only and should be dropped as soon as the call returns.
    stubFetch({ "/session": { status: 200 } });
    render(<Login onAuthenticated={vi.fn()} />);

    const user = userEvent.setup();
    const field = screen.getByLabelText(/api key/i) as HTMLInputElement;
    await user.type(field, "sk-secret");
    await user.click(screen.getByRole("button", { name: /sign in/i }));

    await waitFor(() => expect(field.value).toBe(""));
    expect(JSON.stringify({ ...window.localStorage })).not.toContain("sk-secret");
    expect(JSON.stringify({ ...window.sessionStorage })).not.toContain("sk-secret");
    expect(window.location.href).not.toContain("sk-secret");
  });

  it("masks the key as it is typed", async () => {
    render(<Login onAuthenticated={vi.fn()} />);
    expect((screen.getByLabelText(/api key/i) as HTMLInputElement).type).toBe("password");
  });

  it("reports a rejected key without claiming the server is broken", async () => {
    stubFetch({ "/session": { status: 401, json: { message: "invalid" } } });
    render(<Login onAuthenticated={vi.fn()} />);

    const user = userEvent.setup();
    await user.type(screen.getByLabelText(/api key/i), "wrong");
    await user.click(screen.getByRole("button", { name: /sign in/i }));

    expect((await screen.findByRole("alert")).textContent).toMatch(/key.*not.*(accept|verif)|invalid/i);
  });
});