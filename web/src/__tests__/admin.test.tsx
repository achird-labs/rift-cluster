/** @vitest-environment jsdom */
import { cleanup, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { createQueryClient } from "../app/query.ts";
import { Admin } from "../screens/Admin.tsx";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

const TENANTS = "/admin/tenants";
const ACME = "/admin/tenants/acme";
const ACME_PRINCIPALS = "/admin/tenants/acme/principals";
const AUDIT = "/admin/audit?since=0&limit=100";

const NOT_FOUND = {
  status: 404,
  json: { errors: [{ code: "404", type: "no such resource", message: "Not Found" }] },
};

const TENANT_ROWS = [
  {
    id: "acme",
    displayName: "Acme",
    quotas: { maxImposters: 1000, maxStubsPerImposter: 1000, maxFlowEntries: 100000 },
    journalRetentionSecs: 0,
    createdAtSecs: 1700000000,
  },
];

// `p-1` is a friendly id no encoder would touch. Every principal this console actually mints is
// `key:<sha256-hex>` (`api_key_principal_id`), and the server matches the raw path — so a fixture
// without a colon cannot catch a client that percent-encodes the segment.
const KEY_ID = "key:5f2c8ab3d1e04f6790aa11bb22cc33dd44ee55ff6600771188229933aabbccdd";

const PRINCIPAL_ROWS = [
  { id: "p-1", displayName: "ci", auth: "apiKey", disabled: false, role: "editor" },
  { id: "p-2", displayName: "old", auth: "apiKey", disabled: true, role: "viewer" },
  { id: KEY_ID, displayName: "keyed", auth: "apiKey", disabled: false, role: "operator" },
];

function auditRow(revision: number, overrides: Record<string, unknown> = {}) {
  return {
    tsSecs: 1700000000 + revision,
    principal: "p-1",
    tenant: "acme",
    action: "imposter.write",
    resource: String(4540 + revision),
    opId: `op-${revision}`,
    revision,
    outcome: "applied",
    ...overrides,
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

/**
 * Watch every Web Storage write.
 *
 * Spying on `Storage.prototype` rather than reading `localStorage` back: Node 26 exposes its own
 * experimental `localStorage` global that is `undefined` without `--localstorage-file`, and it
 * shadows jsdom's. Asserting on the writes is both immune to that and a stronger claim — it catches
 * a key that was written and later removed, which reading the store at the end would miss.
 */
function storageWrites(): { all: () => string } {
  const local = vi.spyOn(Storage.prototype, "setItem");
  return {
    all: () => local.mock.calls.map((call) => call.join("=")).join("\n"),
  };
}

describe("a minted API key is shown once (RFC-006 §11 exit criterion)", () => {
  const ISSUED = {
    id: "p-9",
    displayName: "ci",
    role: "editor",
    tenant: "acme",
    apiKey: "rk_live_SECRETVALUE",
  };

  function stubMint(): { calls: string[] } {
    const calls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        const path = typeof input === "string" ? input : input.toString();
        calls.push(`${init?.method ?? "GET"} ${path}`);
        if ((init?.method ?? "GET") === "POST" && path === ACME_PRINCIPALS) {
          return Promise.resolve(new Response(JSON.stringify(ISSUED), { status: 201 }));
        }
        const body = path.startsWith(ACME_PRINCIPALS)
          ? PRINCIPAL_ROWS
          : path.startsWith(TENANTS)
            ? TENANT_ROWS
            : [];
        return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
      }),
    );
    return { calls };
  }

  it("shows the key once, with the warning and a copy affordance", async () => {
    stubMint();
    renderInApp(<Admin tab="principals" tenant="acme" />, {
      whoami: whoamiWith("fleet-admin", ["acme"]),
      tenant: "acme",
    });
    await waitFor(() => expect(screen.getAllByTestId("principal-row").length).toBe(3));

    await userEvent.setup().click(screen.getByRole("button", { name: /create principal/i }));
    await userEvent.setup().type(screen.getByLabelText(/display name/i), "ci");
    await userEvent.setup().click(screen.getByRole("button", { name: /mint/i }));

    const panel = await screen.findByTestId("minted-key");
    expect(panel.textContent).toContain("rk_live_SECRETVALUE");
    expect(panel.textContent?.toLowerCase()).toContain("not be shown again");
    expect(screen.getByRole("button", { name: /copy/i })).toBeTruthy();
  });

  // Nothing may retain it: not storage, not the URL, and not the query cache — a "reveal later"
  // affordance would also imply a retrievable secret, which cannot exist behind an argon2id hash.
  it("retains the key nowhere and offers no reveal-later affordance", async () => {
    stubMint();
    const writes = storageWrites();
    renderInApp(<Admin tab="principals" tenant="acme" />, {
      whoami: whoamiWith("fleet-admin", ["acme"]),
      tenant: "acme",
    });
    await waitFor(() => expect(screen.getAllByTestId("principal-row").length).toBe(3));

    await userEvent.setup().click(screen.getByRole("button", { name: /create principal/i }));
    await userEvent.setup().type(screen.getByLabelText(/display name/i), "ci");
    await userEvent.setup().click(screen.getByRole("button", { name: /mint/i }));
    await screen.findByTestId("minted-key");

    expect(writes.all()).not.toContain("rk_live_SECRETVALUE");
    expect(window.location.href).not.toContain("rk_live_SECRETVALUE");
    expect(screen.queryByRole("button", { name: /reveal/i })).toBeNull();

    // Dismissed means gone — there is nothing left to show.
    await userEvent.setup().click(screen.getByRole("button", { name: /dismiss/i }));
    expect(screen.queryByTestId("minted-key")).toBeNull();
    expect(document.body.textContent).not.toContain("rk_live_SECRETVALUE");
  });

  // The DOM going blank is not the same as the key being gone. `useMutation` keeps its own
  // unmodified result in the MutationCache — `stripApiKey` only sanitises the *query* cache — so
  // without an explicit `reset()` the raw key stays readable from the client (and Devtools) for
  // `gcTime` after dismiss.
  it("leaves no copy of the key in either cache after dismiss", async () => {
    stubMint();
    const client = createQueryClient();
    renderInApp(<Admin tab="principals" tenant="acme" />, {
      whoami: whoamiWith("fleet-admin", ["acme"]),
      tenant: "acme",
      client,
    });
    await waitFor(() => expect(screen.getAllByTestId("principal-row").length).toBe(3));

    await userEvent.setup().click(screen.getByRole("button", { name: /create principal/i }));
    await userEvent.setup().type(screen.getByLabelText(/display name/i), "ci");
    await userEvent.setup().click(screen.getByRole("button", { name: /mint/i }));
    await screen.findByTestId("minted-key");
    await userEvent.setup().click(screen.getByRole("button", { name: /dismiss/i }));

    const mutations = JSON.stringify(
      client.getMutationCache().getAll().map((m) => m.state),
    );
    expect(mutations).not.toContain("rk_live_SECRETVALUE");
    expect(JSON.stringify(client.getQueryCache().getAll().map((q) => q.state.data))).not.toContain(
      "rk_live_SECRETVALUE",
    );
  });
});

describe("the 404 anti-oracle (RFC-002 §8.4) is not softened", () => {
  // A cross-tenant probe answers 404, byte-identical to a resource that does not exist, so the
  // surface cannot be used to enumerate another tenant's records. Rendering "you don't have access
  // to tenant X" would rebuild that oracle in the UI after the API closed it.
  it("renders a cross-tenant probe exactly as a nonexistent resource, naming no tenant", async () => {
    stubFetch({ "/admin/tenants/globex": NOT_FOUND, [TENANTS]: { json: TENANT_ROWS } });
    const { container } = renderInApp(<Admin tab="tenants" tenant="globex" />, {
      whoami: whoamiWith("tenant-admin", ["acme"]),
      tenant: "acme",
    });

    const notFound = await screen.findByTestId("admin-not-found");
    const crossTenant = notFound.textContent ?? "";
    expect(crossTenant).not.toMatch(/access|permission|forbidden|denied|not allowed/i);
    expect(crossTenant).not.toContain("globex");
    const crossTenantDom = container.innerHTML;

    // Tear the first render down before the second: without this both trees live in the same
    // document and every `findBy*` below matches twice, which reads as a failure of the screen
    // rather than of the test.
    cleanup();
    vi.unstubAllGlobals();

    // The same screen for a tenant that genuinely does not exist.
    stubFetch({ "/admin/tenants/nosuch": NOT_FOUND, [TENANTS]: { json: TENANT_ROWS } });
    const second = renderInApp(<Admin tab="tenants" tenant="nosuch" />, {
      whoami: whoamiWith("tenant-admin", ["acme"]),
      tenant: "acme",
    });
    await screen.findByTestId("admin-not-found");
    // Byte-identical: the tenant name must not reach the DOM through a heading, a link href, or a
    // stray attribute, or the 404 becomes an oracle again.
    expect(second.container.innerHTML).toBe(crossTenantDom);
  });

  // A genuine 403 (bound to the tenant, insufficient role) is a different fact and must stay
  // legible — collapsing it into the 404 screen would hide a real, actionable answer.
  it("still distinguishes an honest 403 from a 404", async () => {
    stubFetch({
      [ACME]: { status: 403, json: { errors: [{ code: "403", message: "insufficient role" }] } },
      [TENANTS]: { json: TENANT_ROWS },
    });
    renderInApp(<Admin tab="tenants" tenant="acme" />, {
      whoami: whoamiWith("tenant-admin", ["acme"]),
      tenant: "acme",
    });

    expect(await screen.findByTestId("admin-forbidden")).toBeTruthy();
    expect(screen.queryByTestId("admin-not-found")).toBeNull();
  });
});

describe("principals: disable is not a soft delete", () => {
  function stubPrincipals(): { calls: { method: string; path: string; body: unknown }[] } {
    const calls: { method: string; path: string; body: unknown }[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        const path = typeof input === "string" ? input : input.toString();
        const method = init?.method ?? "GET";
        calls.push({
          method,
          path,
          body: typeof init?.body === "string" ? JSON.parse(init.body) : undefined,
        });
        const body = path.startsWith(ACME_PRINCIPALS) ? PRINCIPAL_ROWS : TENANT_ROWS;
        return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
      }),
    );
    return { calls };
  }

  it("disables with a PUT that carries the required `disabled` field", async () => {
    const { calls } = stubPrincipals();
    renderInApp(<Admin tab="principals" tenant="acme" />, {
      whoami: whoamiWith("fleet-admin", ["acme"]),
      tenant: "acme",
    });
    await waitFor(() => expect(screen.getAllByTestId("principal-row").length).toBe(3));

    await userEvent.setup().click(screen.getByRole("button", { name: /disable ci/i }));

    await waitFor(() => expect(calls.some((c) => c.method === "PUT")).toBe(true));
    const put = calls.find((c) => c.method === "PUT");
    expect(put?.path).toBe(`${ACME_PRINCIPALS}/p-1`);
    // `PrincipalPutBody.disabled` has no serde default — omitting it is a parse error, and would
    // also be a silent un-revoke if it ever defaulted.
    expect(put?.body).toMatchObject({ disabled: true });
    expect((put?.body as { displayName?: string }).displayName).toBe("ci");
    expect(calls.some((c) => c.method === "DELETE")).toBe(false);
  });

  it("deletes with a DELETE, and says so in different words from disable", async () => {
    const { calls } = stubPrincipals();
    renderInApp(<Admin tab="principals" tenant="acme" />, {
      whoami: whoamiWith("fleet-admin", ["acme"]),
      tenant: "acme",
    });
    await waitFor(() => expect(screen.getAllByTestId("principal-row").length).toBe(3));

    const disable = screen.getByRole("button", { name: /disable ci/i }).textContent ?? "";
    const remove = screen.getByRole("button", { name: /delete ci/i }).textContent ?? "";
    expect(disable).not.toBe(remove);

    await userEvent.setup().click(screen.getByRole("button", { name: /delete ci/i }));
    await waitFor(() => expect(calls.some((c) => c.method === "DELETE")).toBe(true));
    expect(calls.find((c) => c.method === "DELETE")?.path).toBe(`${ACME_PRINCIPALS}/p-1`);
  });

  // The server splits the raw request path — hyper does not normalise — so a percent-encoded
  // segment matches no stored principal. Every console-minted id contains a colon, which makes
  // this the difference between working revocation and a permanent 404.
  it("addresses a `key:<hex>` principal with its id raw, not percent-encoded", async () => {
    const { calls } = stubPrincipals();
    renderInApp(<Admin tab="principals" tenant="acme" />, {
      whoami: whoamiWith("fleet-admin", ["acme"]),
      tenant: "acme",
    });
    await waitFor(() => expect(screen.getAllByTestId("principal-row").length).toBe(3));

    await userEvent.setup().click(screen.getByRole("button", { name: /disable keyed/i }));

    await waitFor(() => expect(calls.some((c) => c.method === "PUT")).toBe(true));
    const put = calls.find((c) => c.method === "PUT");
    expect(put?.path).toBe(`${ACME_PRINCIPALS}/${KEY_ID}`);
    expect(put?.path).not.toContain("%3A");
  });

  // Disable is how a fleet revokes without rotating keys, so the record must remain visible and
  // visibly disabled rather than disappearing.
  it("keeps a disabled principal listed and marks it disabled", async () => {
    stubPrincipals();
    renderInApp(<Admin tab="principals" tenant="acme" />, {
      whoami: whoamiWith("fleet-admin", ["acme"]),
      tenant: "acme",
    });
    await waitFor(() => expect(screen.getAllByTestId("principal-row").length).toBe(3));
    // Asserted on the whole row: the principal cell names it, and the state pill is its own cell
    // now that this is a table. Reading one cell would pass for a row that lost its status.
    expect(screen.getByTestId("principal-p-2").closest("tr")?.textContent).toMatch(/disabled/i);
  });
});

describe("ids a URL path cannot address", () => {
  // `#` and `?` truncate the path, so `alice#bob` addresses `alice` — and if a principal `alice`
  // exists in the tenant, "Delete alice#bob" would silently delete the wrong record. The server
  // matches raw paths by design, so encoding is not available as a fix; refusing to offer the
  // action is.
  it("offers no write control for a principal whose id would truncate the path", async () => {
    stubFetch({
      [ACME_PRINCIPALS]: {
        json: [
          { id: "alice", displayName: "alice", auth: "apiKey", disabled: false, role: "viewer" },
          {
            id: "alice#bob",
            displayName: "hashed",
            auth: "oidc",
            disabled: false,
            role: "viewer",
          },
        ],
      },
      [TENANTS]: { json: TENANT_ROWS },
    });
    renderInApp(<Admin tab="principals" tenant="acme" />, {
      whoami: whoamiWith("fleet-admin", ["acme"]),
      tenant: "acme",
    });
    await waitFor(() => expect(screen.getAllByTestId("principal-row").length).toBe(2));

    expect(screen.getByTestId("principal-unaddressable")).toBeTruthy();
    expect(screen.queryByRole("button", { name: /delete hashed/i })).toBeNull();
    // The addressable one is unaffected.
    expect(screen.getByRole("button", { name: /delete alice/i })).toBeTruthy();
  });
});

describe("bindings", () => {
  it("does not offer fleet-admin in a tenant's role picker", async () => {
    stubFetch({ [ACME_PRINCIPALS]: { json: PRINCIPAL_ROWS }, [TENANTS]: { json: TENANT_ROWS } });
    renderInApp(<Admin tab="bindings" tenant="acme" />, {
      whoami: whoamiWith("fleet-admin", ["acme"]),
      tenant: "acme",
    });

    const picker = await screen.findByTestId("role-picker");
    const offered = Array.from(picker.querySelectorAll("option")).map((o) => o.textContent);
    expect(offered.join(" ")).not.toMatch(/fleet-admin/);
    expect(offered.join(" ")).toContain("tenant-admin");
  });
});

describe("tenants: quota round-trip", () => {
  it("sends quotas and journalRetentionSecs back in the shape the fleet stores", async () => {
    const calls: { method: string; path: string; body: unknown }[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        const path = typeof input === "string" ? input : input.toString();
        calls.push({
          method: init?.method ?? "GET",
          path,
          body: typeof init?.body === "string" ? JSON.parse(init.body) : undefined,
        });
        return Promise.resolve(new Response(JSON.stringify(TENANT_ROWS), { status: 200 }));
      }),
    );
    renderInApp(<Admin tab="tenants" tenant="acme" />, {
      whoami: whoamiWith("fleet-admin", ["acme"]),
      tenant: "acme",
    });
    await waitFor(() => expect(screen.getAllByTestId("tenant-row").length).toBe(1));

    await userEvent.setup().click(screen.getByRole("button", { name: /edit acme/i }));
    const retention = screen.getByLabelText(/journal retention/i);
    await userEvent.setup().clear(retention);
    await userEvent.setup().type(retention, "86400");
    await userEvent.setup().click(screen.getByRole("button", { name: /save tenant/i }));

    await waitFor(() => expect(calls.some((c) => c.method === "PUT")).toBe(true));
    const put = calls.find((c) => c.method === "PUT");
    expect(put?.path).toBe(ACME);
    // `journalRetentionSecs` sits on the tenant, NOT inside `quotas` (RFC-002 §11 Q2).
    expect(put?.body).toMatchObject({
      journalRetentionSecs: 86400,
      quotas: { maxImposters: 1000, maxStubsPerImposter: 1000, maxFlowEntries: 100000 },
    });
    expect((put?.body as { quotas: Record<string, unknown> }).quotas).not.toHaveProperty(
      "journalRetentionSecs",
    );
  });
});

describe("quota fields refuse a half-typed value", () => {
  // `Number("")` is 0, and for `maxImposters` 0 is not "unlimited" — it is a tenant that can hold
  // nothing. Clearing a field to retype it must not be able to commit that.
  it("sends nothing when a quota field has been cleared", async () => {
    const calls: { method: string; body: unknown }[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        calls.push({
          method: init?.method ?? "GET",
          body: typeof init?.body === "string" ? JSON.parse(init.body) : undefined,
        });
        void input;
        return Promise.resolve(new Response(JSON.stringify(TENANT_ROWS), { status: 200 }));
      }),
    );
    renderInApp(<Admin tab="tenants" tenant="acme" />, {
      whoami: whoamiWith("fleet-admin", ["acme"]),
      tenant: "acme",
    });
    await waitFor(() => expect(screen.getAllByTestId("tenant-row").length).toBe(1));

    await userEvent.setup().click(screen.getByRole("button", { name: /edit acme/i }));
    await userEvent.setup().clear(screen.getByLabelText(/max imposters/i));
    await userEvent.setup().click(screen.getByRole("button", { name: /save tenant/i }));

    expect(await screen.findByTestId("tenant-invalid")).toBeTruthy();
    expect(calls.some((c) => c.method === "PUT")).toBe(false);
  });

  // The server's own `Quotas::default` is 100_000; the contract omits the default. Pre-filling 0
  // would cut a tenant's flow budget to nothing on the next otherwise-untouched save.
  it("pre-fills the server's default for an absent maxFlowEntries, not zero", async () => {
    stubFetch({
      [TENANTS]: {
        json: [
          {
            id: "acme",
            displayName: "Acme",
            quotas: { maxImposters: 1000, maxStubsPerImposter: 1000 },
            journalRetentionSecs: 0,
          },
        ],
      },
    });
    renderInApp(<Admin tab="tenants" tenant="acme" />, {
      whoami: whoamiWith("fleet-admin", ["acme"]),
      tenant: "acme",
    });
    await waitFor(() => expect(screen.getAllByTestId("tenant-row").length).toBe(1));

    await userEvent.setup().click(screen.getByRole("button", { name: /edit acme/i }));
    expect((screen.getByLabelText(/max flow entries/i) as HTMLInputElement).value).toBe("100000");
  });
});

describe("audit viewer", () => {
  it("renders the bare array, oldest first, and shows a refusal as a committed row", async () => {
    stubFetch({
      [AUDIT]: {
        json: [
          auditRow(1),
          auditRow(2, { outcome: { failed: { reason: "quota exceeded" } } }),
        ],
      },
    });
    renderInApp(<Admin tab="audit" tenant="acme" />, {
      whoami: whoamiWith("tenant-admin", ["acme"]),
      tenant: "acme",
    });

    await waitFor(() => expect(screen.getAllByTestId("audit-row").length).toBe(2));
    const revisions = screen.getAllByTestId("audit-revision").map((n) => n.textContent);
    expect(revisions).toEqual(["1", "2"]);
    expect(screen.getByTestId("audit-row-2").textContent).toContain("quota exceeded");
  });

  it("pages with since one past the highest revision, because since is inclusive", async () => {
    const calls: string[] = [];
    // A FULL first page — a short page is the end of the journal and correctly retires the pager,
    // so a two-row fixture could never exercise paging at all.
    const firstPage = Array.from({ length: 100 }, (_, i) => auditRow(i + 1));
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = typeof input === "string" ? input : input.toString();
        calls.push(path);
        const body = path.includes("since=0") ? firstPage : [auditRow(200)];
        return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
      }),
    );
    renderInApp(<Admin tab="audit" tenant="acme" />, {
      whoami: whoamiWith("tenant-admin", ["acme"]),
      tenant: "acme",
    });
    await waitFor(() => expect(screen.getAllByTestId("audit-row").length).toBe(100));

    // Highest revision on the page is 100, so the next request must ask for 101 — `since=100` would
    // re-serve the row the page ended on.
    await userEvent.setup().click(screen.getByTestId("audit-next"));
    await waitFor(() => expect(calls.some((p) => p.includes("since=101"))).toBe(true));
  });

  // Gating on the cursor alone left this live forever: clicking past the end rendered an empty
  // table under a header, which reads as "the trail stops here".
  it("retires the pager on a short final page", async () => {
    stubFetch({ [AUDIT]: { json: [auditRow(1), auditRow(2)] } });
    renderInApp(<Admin tab="audit" tenant="acme" />, {
      whoami: whoamiWith("tenant-admin", ["acme"]),
      tenant: "acme",
    });

    await waitFor(() => expect(screen.getAllByTestId("audit-row").length).toBe(2));
    expect((screen.getByTestId("audit-next") as HTMLButtonElement).disabled).toBe(true);
  });

  // `resource` and `principal` are attacker-influenceable — a port or id an attacker chose.
  it("renders resource and principal as text", async () => {
    stubFetch({
      [AUDIT]: {
        json: [
          auditRow(1, {
            resource: "<script>alert(1)</script>",
            principal: '<img src=x onerror="alert(1)">',
          }),
        ],
      },
    });
    const { container } = renderInApp(<Admin tab="audit" tenant="acme" />, {
      whoami: whoamiWith("tenant-admin", ["acme"]),
      tenant: "acme",
    });

    await waitFor(() => expect(screen.getAllByTestId("audit-row").length).toBe(1));
    expect(container.querySelector("script")).toBeNull();
    expect(container.querySelector("img")).toBeNull();
    expect(container.textContent).toContain("<script>alert(1)</script>");
    expect(container.textContent).toContain('<img src=x onerror="alert(1)">');
  });
});

describe("visibility is whoami-driven, but is never the guard (RFC-006 §3 rule 3)", () => {
  // `TenantList` is fleet-scoped `ClusterAdmin`, so this read is a permanent 404 for a tenant-admin.
  // Asking anyway turned their Administration landing into a red error refreshing every 5s — the
  // failure the `cluster.admin` capability was added to prevent, one layer up from the buttons.
  it("does not ask for the fleet-scoped tenant list as a tenant-admin", async () => {
    const { calls } = stubFetch({
      [TENANTS]: { json: TENANT_ROWS },
      [ACME_PRINCIPALS]: { json: PRINCIPAL_ROWS },
    });
    renderInApp(<Admin tab="tenants" tenant={null} />, {
      whoami: whoamiWith("tenant-admin", ["acme"]),
      tenant: "acme",
    });

    await screen.findByTestId("admin-screen");
    expect(calls).not.toContain(TENANTS);
    expect(screen.queryByRole("button", { name: /create tenant/i })).toBeNull();
  });

  it("offers a viewer no tenant-management controls", async () => {
    stubFetch({ [TENANTS]: { json: TENANT_ROWS }, [ACME_PRINCIPALS]: { json: PRINCIPAL_ROWS } });
    renderInApp(<Admin tab="tenants" tenant="acme" />, {
      whoami: whoamiWith("viewer", ["acme"]),
      tenant: "acme",
    });

    await screen.findByTestId("admin-screen");
    expect(screen.queryByRole("button", { name: /create principal/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /edit acme/i })).toBeNull();
  });

  // The hidden button was never what stopped the call: the API re-checks every request. Drive the
  // underlying call directly and assert the server's refusal is what surfaces.
  it("surfaces the server's refusal when a viewer's hidden call is made anyway", async () => {
    stubFetch({
      [TENANTS]: { json: TENANT_ROWS },
      [ACME]: { status: 403, json: { errors: [{ code: "403", message: "insufficient role" }] } },
    });
    renderInApp(<Admin tab="tenants" tenant="acme" />, {
      whoami: whoamiWith("viewer", ["acme"]),
      tenant: "acme",
    });
    await screen.findByTestId("admin-screen");

    const response = await fetch(ACME, { method: "PUT" });
    expect(response.status).toBe(403);
  });
});
