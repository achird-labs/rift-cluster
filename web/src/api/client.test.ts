import { afterEach, describe, expect, it, vi } from "vitest";

import { ApiError, CSRF_HEADER, TENANT_HEADER, apiGet, apiSend } from "./client.ts";

function mockFetch(response: Response): ReturnType<typeof vi.fn> {
  const fetchMock = vi.fn().mockResolvedValue(response);
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status });
}

function headersOf(fetchMock: ReturnType<typeof vi.fn>): Record<string, string> {
  const init = fetchMock.mock.calls[0]?.[1] as RequestInit | undefined;
  return (init?.headers ?? {}) as Record<string, string>;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("tenant selection", () => {
  it("sends X-Rift-Tenant with the tenant in view", () => {
    const fetchMock = mockFetch(json({ imposters: [] }));
    void apiGet("/imposters", { tenant: "acme" });
    expect(headersOf(fetchMock)[TENANT_HEADER]).toBe("acme");
  });

  it("omits the header entirely when no tenant is selected", () => {
    // RFC-002 §8.1: absent means the principal's default tenant. Sending an empty string instead
    // would be a *claim* of a tenant named "", which resolves to a tenant the caller is not bound
    // to and 404s.
    const fetchMock = mockFetch(json({ imposters: [] }));
    void apiGet("/imposters");
    expect(TENANT_HEADER in headersOf(fetchMock)).toBe(false);
  });

  it("carries the tenant on mutations too", () => {
    const fetchMock = mockFetch(json({ message: "ok" }));
    void apiSend("POST", "/imposters/4545/disable", undefined, { tenant: "globex" });
    const headers = headersOf(fetchMock);
    expect(headers[TENANT_HEADER]).toBe("globex");
    expect(headers[CSRF_HEADER]).toBe("1");
  });

  it("uses the exact header name the admin front reads", () => {
    // `admin_front.rs::requested_tenant` matches `x-rift-tenant`; HTTP header names are
    // case-insensitive, but the contract declares this casing and the proxy path forwards it
    // verbatim, so pinning it keeps the two documents honest with each other.
    expect(TENANT_HEADER).toBe("X-Rift-Tenant");
  });
});

describe("error surfacing", () => {
  it("throws with the status so a screen can tell 404 from 503", () => {
    // The fleet projection answers 404 for a role that lacks it and 503 when the node is not
    // ready. Those are different sentences on screen, so the status has to survive the client.
    const fetchMock = mockFetch(json({ message: "nope" }, 404));
    expect(fetchMock).toBeDefined();
    return apiGet("/_fleet/members").then(
      () => expect.unreachable("expected a rejection"),
      (error: unknown) => {
        expect(error).toBeInstanceOf(ApiError);
        expect((error as ApiError).status).toBe(404);
      },
    );
  });

  it("still sends no CSRF header on a read", () => {
    const fetchMock = mockFetch(json({}));
    void apiGet("/admin/whoami");
    expect(CSRF_HEADER in headersOf(fetchMock)).toBe(false);
  });
});
