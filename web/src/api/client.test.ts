import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ApiError,
  CSRF_HEADER,
  IDEMPOTENCY_HEADER,
  REVISION_HEADER,
  RawJsonBody,
  TENANT_HEADER,
  apiGet,
  apiGetWithRevision,
  apiSend,
} from "./client.ts";

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

describe("a parked write is not an applied write (#211)", () => {
  /*
   * `202 AcceptedParked` means the write is durably parked and still committing. The console used
   * to see every 2xx as the same thing, so it told the operator "saved" for a write that had not
   * landed yet — and never polled the op id the response hands back for exactly that purpose.
   */
  it("reports a 202 as parked, carrying the op id from the body", async () => {
    mockFetch(json({ opId: "11111111-1111-4111-8111-111111111111" }, 202));
    const result = await apiSend("POST", "/imposters/4545/disable");
    expect(result).toEqual({
      kind: "parked",
      opIds: ["11111111-1111-4111-8111-111111111111"],
    });
  });

  it("prefers the derived opIds of a multi-op mutation over the base id", async () => {
    // `admin_front.rs:1916-1918`: a multi-op mutation parks only the derived ids, never the base.
    // A client polling the bare base of a batch PUT /imposters would 404 forever.
    mockFetch(
      json(
        {
          opId: "00000000-0000-4000-8000-000000000000",
          opIds: [
            "22222222-2222-4222-8222-222222222222",
            "33333333-3333-4333-8333-333333333333",
          ],
        },
        202,
      ),
    );
    const result = await apiSend("PUT", "/imposters");
    expect(result).toEqual({
      kind: "parked",
      opIds: [
        "22222222-2222-4222-8222-222222222222",
        "33333333-3333-4333-8333-333333333333",
      ],
    });
  });

  it("falls back to the Rift-Cluster-Op-Id header when the 202 carries no body", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response("", {
          status: 202,
          headers: { "Rift-Cluster-Op-Id": "44444444-4444-4444-8444-444444444444" },
        }),
      ),
    );
    const result = await apiSend("DELETE", "/front-door/routes/edge");
    expect(result).toEqual({
      kind: "parked",
      opIds: ["44444444-4444-4444-8444-444444444444"],
    });
  });

  it("still reports an ordinary committed write as applied, with its body", async () => {
    mockFetch(json({ routes: [] }));
    const result = await apiSend("PUT", "/front-door/routes", { routes: [] });
    expect(result).toEqual({ kind: "applied", data: { routes: [] } });
  });

  it("treats an empty 204 as applied, not as parked", async () => {
    // `null`, not `""` — the Response constructor rejects any body on a 204.
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 204 })));
    const result = await apiSend("DELETE", "/admin/tenants/acme");
    expect(result).toEqual({ kind: "applied", data: null });
  });
});

describe("a malformed 202 body still finds an op id to follow", () => {
  it("falls back to opId when opIds carries nothing usable", async () => {
    // A contract violation, but one worth surviving: returning `[]` here would report the write as
    // unobservable while the base id sitting right beside it was perfectly pollable.
    mockFetch(json({ opIds: [42, null], opId: "55555555-5555-4555-8555-555555555555" }, 202));
    const result = await apiSend("POST", "/imposters/4545/enable");
    expect(result).toEqual({
      kind: "parked",
      opIds: ["55555555-5555-4555-8555-555555555555"],
    });
  });
});

describe("reading the revision the write will be conditioned on", () => {
  it("hands back the Rift-Cluster-Revision header beside the body", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ port: 4545 }), {
          status: 200,
          headers: { [REVISION_HEADER]: "default:4545@17" },
        }),
      ),
    );
    const read = await apiGetWithRevision<{ port: number }>("/imposters/4545");
    expect(read).toEqual({ data: { port: 4545 }, revision: "default:4545@17" });
  });

  it("reports a missing header as null rather than inventing a token", async () => {
    // An unconditioned write is the lost-update bug. The caller has to be able to tell "I have no
    // token" from "I have one", so absence is a value here and the editor refuses to save on it.
    mockFetch(json({ port: 4545 }));
    expect((await apiGetWithRevision("/imposters/4545")).revision).toBeNull();
  });

  it("still throws on a non-2xx, exactly as apiGet does", async () => {
    mockFetch(json({ message: "gone" }, 404));
    await expect(apiGetWithRevision("/imposters/4545")).rejects.toBeInstanceOf(ApiError);
  });
});

describe("conditioning a write on a revision", () => {
  it("sends If-Match when the caller supplies a token", () => {
    const fetchMock = mockFetch(json({ port: 4545 }));
    void apiSend("PUT", "/imposters/4545/stubs/by-id/s-1", {}, { ifMatch: "default:4545@17" });
    expect(headersOf(fetchMock)["If-Match"]).toBe("default:4545@17");
  });

  it("omits If-Match entirely when there is no token", () => {
    const fetchMock = mockFetch(json({ port: 4545 }));
    void apiSend("PUT", "/imposters/4545/stubs/by-id/s-1", {});
    expect("If-Match" in headersOf(fetchMock)).toBe(false);
  });

  it("sends a raw body byte for byte instead of reserializing it", async () => {
    // The raw-JSON editor saves the operator's own text. Round-tripping it through
    // `JSON.parse`/`JSON.stringify` would reorder keys and drop their formatting — a diff against
    // the stored stub that the operator never made.
    const fetchMock = mockFetch(json({ port: 4545 }));
    const text = '{\n  "id": "s-1",\n  "space": "a"\n}';
    await apiSend("PUT", "/imposters/4545/stubs/by-id/s-1", new RawJsonBody(text));
    expect((fetchMock.mock.calls[0]?.[1] as RequestInit).body).toBe(text);
    expect(headersOf(fetchMock)["Content-Type"]).toBe("application/json");
  });
});

describe("idempotency key (#371)", () => {
  it("sends Idempotency-Key on a mutation when one is supplied", () => {
    const fetchMock = mockFetch(json({}));
    void apiSend("PUT", "/imposters/4545/stubs", [], { idempotencyKey: "op-1" });
    expect(headersOf(fetchMock)[IDEMPOTENCY_HEADER]).toBe("op-1");
  });

  // The console sent none at all before this, which is the bug — so the absence case has to stay
  // an absence, not a blank header the front would then derive an op id from.
  it("omits the header entirely when no key is supplied", () => {
    const fetchMock = mockFetch(json({}));
    void apiSend("PUT", "/imposters/4545/stubs", []);
    expect(IDEMPOTENCY_HEADER in headersOf(fetchMock)).toBe(false);
  });

  it("omits it for an empty or null key rather than sending a blank one", () => {
    for (const key of ["", null, undefined]) {
      const fetchMock = mockFetch(json({}));
      void apiSend("DELETE", "/imposters/4545", undefined, { idempotencyKey: key });
      expect(IDEMPOTENCY_HEADER in headersOf(fetchMock)).toBe(false);
      vi.unstubAllGlobals();
    }
  });

  // A read cannot double-apply, and one admin route refuses the header outright (minting a
  // principal), so sending it where it has no meaning would invite a 400 for nothing.
  it("never sends it on a GET", () => {
    const fetchMock = mockFetch(json({ imposters: [] }));
    void apiGet("/imposters", { idempotencyKey: "op-1" });
    expect(IDEMPOTENCY_HEADER in headersOf(fetchMock)).toBe(false);
  });
});
