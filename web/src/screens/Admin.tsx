import { type FormEvent, type ReactNode, useState } from "react";

import { ApiError } from "../api/client.ts";
import { isAddressablePrincipalId } from "../api/paths.ts";
import type { components } from "../api/schema.ts";
import type { Capability } from "../app/rbac.ts";
import type { AdminTab } from "../app/routing.ts";
import { toHash } from "../app/routing.ts";
import {
  useAuditRows,
  useAuditSink,
  useCreatePrincipal,
  useCreateTenant,
  useDeleteAuditSink,
  useDeleteBinding,
  useDeletePrincipal,
  useDeleteTenant,
  usePrincipals,
  usePutAuditSink,
  usePutBinding,
  useSavePrincipal,
  useSaveTenant,
  useTenantProbe,
  useTenants,
  AUDIT_PAGE_SIZE,
} from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import {
  Card,
  Confirm,
  Empty,
  ErrorNote,
  Ident,
  Status,
  Tile,
  Truncated,
  UNKNOWN,
  UnconfirmedNote,
} from "../components/primitives.tsx";
import { failureReason, hasMorePages, isApplied, nextSince } from "../features/admin/audit.ts";
import { KEY_NOT_SHOWN_AGAIN } from "../features/admin/key.ts";
import { assignableRoles } from "../features/admin/roles.ts";

type Tenant = components["schemas"]["Tenant"];
type TenantWrite = components["schemas"]["TenantWrite"];
type Principal = components["schemas"]["Principal"];
type Role = components["schemas"]["Role"];
type IssuedPrincipal = components["schemas"]["IssuedPrincipal"];

const TAB_LABEL: Record<AdminTab, string> = {
  tenants: "Tenants",
  principals: "Principals",
  bindings: "Bindings",
  audit: "Audit",
  sink: "Audit sink",
};

/**
 * The capability each tab's own routes actually require.
 *
 * The tenancy surface splits across two actions and the tabs split with it: `TenantList`/`TenantRead`
 * and the whole audit-sink triple are `ClusterAdmin`, while listing and minting principals and
 * writing bindings are `TenantManage`. Reading a tenant's audit rows is `AuditRead`.
 *
 * Filtering on this is load-bearing rather than tidy. `tenants` is the tab the nav lands on, and it
 * probes `GET /admin/tenants/:id` before rendering anything — including the tab bar, deliberately,
 * because RFC-002 §8.4 requires a cross-tenant probe and a nonexistent tenant to produce
 * byte-identical DOM. For a TenantAdmin that probe answers `403`, so the screen rendered a bare
 * refusal with no navigation at all and the role could not reach the two tabs it exists for.
 */
export const ADMIN_TABS: readonly AdminTab[] = [
  "tenants",
  "principals",
  "bindings",
  "audit",
  "sink",
];

const TAB_CAPABILITY: Record<AdminTab, Capability> = {
  tenants: "cluster.admin",
  principals: "tenant.manage",
  bindings: "tenant.manage",
  audit: "audit.read",
  sink: "cluster.admin",
};

export function Admin({
  tab: requestedTab,
  tenant: routeTenant,
}: {
  tab: AdminTab;
  tenant: string | null;
}): ReactNode {
  const { can, tenant: sessionTenant } = useSession();
  const mayManage = can("tenant.manage");
  /*
   * Fall back to the tenant the session is already scoped to.
   *
   * The nav links here with `tenant: null`, and the only control that *set* a tenant was a row in
   * the Tenants tab — which lists `/admin/tenants`, a `ClusterAdmin` route. A TenantAdmin therefore
   * could not read that list, could not pick a tenant, and hit "Choose a tenant to see this" on
   * both Principals and Bindings forever: the role whose entire grant is `PrincipalCreate` and
   * `BindingPut` had no route to either.
   *
   * The session already knows the answer — `initialTenant` resolves it from the principal's own
   * bindings — so the screen asks it rather than requiring a fleet-scoped list to supply it. An
   * explicit tenant in the hash still wins, which keeps every existing link and the fleet-admin's
   * cross-tenant navigation working.
   */
  const tenant = routeTenant ?? sessionTenant;
  /*
   * The requested tab is honoured as asked, deliberately — including one this role cannot use.
   *
   * Redirecting away from `tenants` would strand nobody, but it would also delete the RFC-002 §8.4
   * branch: a TenantAdmin probing that tab is exactly the caller for whom a cross-tenant tenant and
   * a nonexistent one must answer with byte-identical DOM, and the screen renders that. The tab
   * *bar* is filtered by capability so nothing unusable is offered; a URL typed by hand still
   * reaches the honest refusal rather than being quietly rerouted.
   */
  const tab = requestedTab;
  /*
   * Only the `tenants` tab addresses one tenant directly by path (`GET /admin/tenants/:id`), so
   * it is the only tab that can answer this existence-and-permission question before any
   * tab-specific content exists to render — the other tabs' own tab-scoped queries surface the
   * same distinction later, through `AdminApiError` again.
   */
  const probe = useTenantProbe(tenant ?? "", {
    enabled: tab === "tenants" && tenant !== null && mayManage,
  });

  if (tab === "tenants" && tenant !== null && mayManage && probe.isError) {
    /*
     * Nothing else renders in this branch — not the header, not the tab nav — because both would
     * have to encode `tenant` to stay useful, and RFC-002 §8.4 requires a cross-tenant probe and a
     * nonexistent tenant to answer with byte-identical DOM, whichever tenant triggered it.
     *
     * The byte-identity guarantee is scoped to this tab. The other tabs render their own 404
     * through `AdminApiError` inside the full screen, so their tab hrefs still carry the tenant
     * name. That is not an oracle — the name is the prober's own input and nothing server-derived
     * distinguishes the two renders — but it is not byte-identical either, so the claim is made
     * here rather than for the screen as a whole.
     */
    return (
      <section className="screen" data-testid="admin-screen">
        <AdminApiError error={probe.error} />
      </section>
    );
  }

  return (
    <section className="screen" data-testid="admin-screen">
      <header className="screen-head">
        <h1>Administration</h1>
        <AdminTabs tab={tab} tenant={tenant} />
      </header>
      <Content tab={tab} tenant={tenant} />
    </section>
  );
}

function AdminTabs({ tab, tenant }: { tab: AdminTab; tenant: string | null }): ReactNode {
  const { can } = useSession();
  const tabs = ADMIN_TABS.filter((t) => can(TAB_CAPABILITY[t]));
  return (
    <nav className="admin-tabs">
      {tabs.map((t) => (
        <a
          key={t}
          href={toHash({ screen: "admin", tab: t, tenant })}
          aria-current={t === tab ? "page" : undefined}
        >
          {TAB_LABEL[t]}
        </a>
      ))}
    </nav>
  );
}

function Content({ tab, tenant }: { tab: AdminTab; tenant: string | null }): ReactNode {
  switch (tab) {
    case "tenants":
      return <TenantsTab />;
    case "principals":
      return tenant === null ? <NoTenantChosen /> : <PrincipalsTab tenant={tenant} />;
    case "bindings":
      return tenant === null ? <NoTenantChosen /> : <BindingsTab tenant={tenant} />;
    case "audit":
      return <AuditTab tenant={tenant} />;
    case "sink":
      return <AuditSinkTab />;
  }
}

function NoTenantChosen(): ReactNode {
  return <p className="muted">Choose a tenant to see this.</p>;
}

/**
 * The one place a `404`/`403` from the admin plane is rendered generically.
 *
 * The `404` branch is RFC-002 §8.4's anti-oracle, rendered as the screen's whole content rather
 * than layered over anything else: a cross-tenant probe and a nonexistent tenant must answer with
 * byte-identical DOM, and the only way to guarantee that is to render nothing else that could
 * differ between the two. No tenant name, and none of "access", "permission", "forbidden", "denied"
 * — that vocabulary is exactly the oracle a `404` exists to deny. A `403` is a different, honest
 * fact (the caller IS bound, the role just refuses it) and is kept in its own branch rather than
 * folded into the `404` one.
 */
function AdminApiError({ error }: { error: unknown }): ReactNode {
  const status = error instanceof ApiError ? error.status : null;
  if (status === 404) {
    return (
      <p className="muted" data-testid="admin-not-found" role="status">
        No such resource.
      </p>
    );
  }
  if (status === 403) {
    return (
      <p className="error" data-testid="admin-forbidden" role="alert">
        This principal&rsquo;s role does not permit that here.
      </p>
    );
  }
  return <ErrorNote error={error} context="Could not read the admin plane" />;
}

/* ------------------------------------------------------------------------------------------- */
/* Tenants                                                                                       */
/* ------------------------------------------------------------------------------------------- */

function TenantsTab(): ReactNode {
  const { can } = useSession();
  // Every `/admin/tenants` route is `Action::ClusterAdmin` (`tenancy.rs`), which stops at
  // fleet-admin — `tenant.manage` would draw a tenant-admin a list and a create button that
  // answer 404 and 400 respectively, every time.
  const mayManage = can("cluster.admin");
  // `TenantList` is fleet-scoped `ClusterAdmin`, so for anyone below fleet-admin this read is a
  // permanent 404 — asking anyway makes their Administration landing a red error on a 5s loop.
  const tenants = useTenants({ enabled: mayManage });
  const save = useSaveTenant();
  const del = useDeleteTenant();
  const create = useCreateTenant();
  const [editing, setEditing] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);

  return (
    <>
      {tenants.isError ? <ErrorNote error={tenants.error} context="Could not list tenants" /> : null}
      {create.isError ? <ErrorNote error={create.error} context="Could not create that tenant" /> : null}
      {save.isError ? <ErrorNote error={save.error} context="Could not save that tenant" /> : null}
      {del.isError ? <ErrorNote error={del.error} context="Could not delete that tenant" /> : null}

      {mayManage ? (
        creating ? (
          <CreateTenantForm
            onCancel={() => setCreating(false)}
            onSubmit={(body) => create.mutate(body, { onSuccess: () => setCreating(false) })}
          />
        ) : (
          <button className="btn" type="button" onClick={() => setCreating(true)}>
            Create tenant
          </button>
        )
      ) : null}

      {tenants.isPending ? <p className="muted">Reading…</p> : null}

      {tenants.isSuccess ? (
        <section className="card">
          <div className="scroll-x">
        <table className="dense">
          <thead>
            <tr>
              <th>Id</th>
              <th>Name</th>
              <th>Quotas</th>
              <th>Journal retention</th>
              {mayManage ? <th>Actions</th> : null}
            </tr>
          </thead>
          <tbody>
            {tenants.data.map((row) =>
              editing === row.id ? (
                <TenantEditRow
                  key={row.id}
                  tenant={row}
                  onCancel={() => setEditing(null)}
                  onSave={(body) =>
                    save.mutate({ tenantId: row.id, body }, { onSuccess: () => setEditing(null) })
                  }
                />
              ) : (
                <TenantRow
                  key={row.id}
                  tenant={row}
                  mayManage={mayManage}
                  onEdit={() => setEditing(row.id)}
                  onDelete={() => del.mutate({ tenantId: row.id })}
                />
              ),
            )}
          </tbody>
        </table>
          </div>
        </section>
      ) : null}
    </>
  );
}

function TenantRow({
  tenant,
  mayManage,
  onEdit,
  onDelete,
}: {
  tenant: Tenant;
  mayManage: boolean;
  onEdit: () => void;
  onDelete: () => void;
}): ReactNode {
  return (
    <tr data-testid="tenant-row">
      <td>
        <Ident>{tenant.id}</Ident>
      </td>
      <td>{tenant.displayName}</td>
      <td>
        {tenant.quotas?.maxImposters ?? UNKNOWN} imposters ·{" "}
        {tenant.quotas?.maxStubsPerImposter ?? UNKNOWN} stubs/imposter ·{" "}
        {tenant.quotas?.maxFlowEntries ?? UNKNOWN} flow entries
      </td>
      <td>{tenant.journalRetentionSecs === 0 ? "unlimited" : `${tenant.journalRetentionSecs}s`}</td>
      {mayManage ? (
        <td>
          <button className="btn" type="button" onClick={onEdit}>
            Edit {tenant.displayName}
          </button>
          <button className="btn" type="button" onClick={onDelete}>
            Delete {tenant.displayName}
          </button>
        </td>
      ) : null}
    </tr>
  );
}

/**
 * A whole number, or `null` when the text is not one.
 *
 * `Number("")` is `0` and `Number(" ")` is `0`, which is exactly the coercion that turns a cleared
 * quota field into a real quota of zero. `null` here means "the operator has not given me a number",
 * which is a different thing from the number zero and must not be sent as one.
 */
function parseWholeNumber(raw: string): number | null {
  if (!/^\s*\d+\s*$/.test(raw)) return null;
  const value = Number(raw);
  return Number.isSafeInteger(value) ? value : null;
}

function TenantEditRow({
  tenant,
  onCancel,
  onSave,
}: {
  tenant: Tenant;
  onCancel: () => void;
  onSave: (body: TenantWrite) => void;
}): ReactNode {
  /*
   * Quota fields are held as the raw strings the operator typed, not as numbers.
   *
   * `Number("")` is `0`, so parsing on every keystroke means the instant someone clears a field to
   * retype it, state says "zero". Submitting then sets a real quota to 0 — for `maxImposters` that
   * is not "unlimited", it is a tenant that can hold nothing. Parsing once, at submit, is what makes
   * a half-typed value impossible to send.
   */
  const [displayName, setDisplayName] = useState(tenant.displayName);
  const [maxImposters, setMaxImposters] = useState(String(tenant.quotas?.maxImposters ?? 1000));
  const [maxStubsPerImposter, setMaxStubsPerImposter] = useState(
    String(tenant.quotas?.maxStubsPerImposter ?? 1000),
  );
  // The server's own default is 100_000 (`Quotas::default`, `control.rs`) — the contract omits it.
  // Pre-filling 0 would silently cut a tenant's flow budget to nothing on the next untouched save.
  const [maxFlowEntries, setMaxFlowEntries] = useState(
    String(tenant.quotas?.maxFlowEntries ?? 100_000),
  );
  const [retention, setRetention] = useState(String(tenant.journalRetentionSecs));
  const [invalid, setInvalid] = useState<string[]>([]);

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    const fields = [
      ["Max imposters", maxImposters],
      ["Max stubs per imposter", maxStubsPerImposter],
      ["Max flow entries", maxFlowEntries],
      ["Journal retention (seconds)", retention],
    ] as const;

    // Only rejects input that is not a whole number at all. Deliberately NOT a range check: the
    // fleet decides what a legal quota is, and a client mirror stricter than the server refuses
    // values the fleet would accept.
    const bad = fields.filter(([, raw]) => parseWholeNumber(raw) === null).map(([label]) => label);
    setInvalid(bad);
    if (bad.length > 0) return;
    const parsed = fields.map(([, raw]) => parseWholeNumber(raw) ?? 0) as [
      number,
      number,
      number,
      number,
    ];

    onSave({
      id: tenant.id,
      displayName,
      // `journalRetentionSecs` sits on the tenant, never inside `quotas` (RFC-002 §11 Q2) — the
      // two are edited by the same form but sent as siblings, not nested.
      // Parsed values, not re-parsed with a `?? 0` fallback: that fallback is the exact coercion
      // `parseWholeNumber` exists to prevent, and it would come back silently if the guard above
      // were ever refactored away.
      quotas: {
        maxImposters: parsed[0],
        maxStubsPerImposter: parsed[1],
        maxFlowEntries: parsed[2],
      },
      journalRetentionSecs: parsed[3],
    });
  };

  return (
    <tr data-testid="tenant-edit-row">
      <td colSpan={5}>
        <form onSubmit={submit} aria-label={`Edit ${tenant.displayName}`}>
          <label>
            Display name
            <input value={displayName} onChange={(e) => setDisplayName(e.target.value)} />
          </label>
          <label>
            Max imposters
            <input
              type="number"
              value={maxImposters}
              onChange={(e) => setMaxImposters(e.target.value)}
            />
          </label>
          <label>
            Max stubs per imposter
            <input
              type="number"
              value={maxStubsPerImposter}
              onChange={(e) => setMaxStubsPerImposter(e.target.value)}
            />
          </label>
          <label>
            Max flow entries
            <input
              type="number"
              value={maxFlowEntries}
              onChange={(e) => setMaxFlowEntries(e.target.value)}
            />
          </label>
          <label>
            Journal retention (seconds)
            <input
              type="number"
              value={retention}
              onChange={(e) => setRetention(e.target.value)}
            />
          </label>
          {invalid.length > 0 ? (
            <p className="error" data-testid="tenant-invalid" role="alert">
              Not a whole number, so nothing was sent: {invalid.join(", ")}.
            </p>
          ) : null}
          <button className="btn primary" type="submit">Save tenant</button>
          <button className="btn" type="button" onClick={onCancel}>
            Cancel
          </button>
        </form>
      </td>
    </tr>
  );
}

function CreateTenantForm({
  onCancel,
  onSubmit,
}: {
  onCancel: () => void;
  onSubmit: (body: TenantWrite) => void;
}): ReactNode {
  const [id, setId] = useState("");
  const [displayName, setDisplayName] = useState("");

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    if (id.length === 0 || displayName.length === 0) return;
    onSubmit({
      id,
      displayName,
      // All three quota fields, always: `Quotas` has no serde default on the struct or any field,
      // so a present-but-partial object is a `missing field` parse error and a flat 400. These are
      // the server's own defaults (`Quotas::default`).
      quotas: { maxImposters: 1000, maxStubsPerImposter: 1000, maxFlowEntries: 100_000 },
      journalRetentionSecs: 0,
    });
  };

  return (
    <form onSubmit={submit} aria-label="Create tenant">
      <label>
        Tenant id
        <input value={id} onChange={(e) => setId(e.target.value)} />
      </label>
      <label>
        Display name
        <input value={displayName} onChange={(e) => setDisplayName(e.target.value)} />
      </label>
      <button className="btn primary" type="submit">Create tenant</button>
      <button className="btn" type="button" onClick={onCancel}>
        Cancel
      </button>
    </form>
  );
}

/* ------------------------------------------------------------------------------------------- */
/* Principals                                                                                     */
/* ------------------------------------------------------------------------------------------- */

function PrincipalsTab({ tenant }: { tenant: string }): ReactNode {
  const { can } = useSession();
  // The tenancy routes split across two actions and so do the controls: listing and minting are
  // `TenantManage`, but `PrincipalPut`/`PrincipalDelete` are `ClusterAdmin` scoped to the path
  // tenant — a tenant-admin is bound there, so those answer a genuine 403 rather than a 404.
  const mayManage = can("tenant.manage");
  const mayAdminister = can("cluster.admin");
  // The one place the raw key is allowed to exist: local component state, handed over out of band
  // by `useCreatePrincipal`, shown once, and gone the moment `dismiss` clears it. It is declared
  // before that hook because the hook takes the setter.
  const [minted, setMinted] = useState<IssuedPrincipal | null>(null);
  const principals = usePrincipals(tenant);
  const create = useCreatePrincipal(tenant, setMinted);
  const save = useSavePrincipal();
  const del = useDeletePrincipal();
  const [creating, setCreating] = useState(false);

  if (principals.isError) {
    return <AdminApiError error={principals.error} />;
  }

  return (
    <>
      {create.isError ? <ErrorNote error={create.error} context="Could not mint that principal" /> : null}
      {save.isError ? <ErrorNote error={save.error} context="Could not update that principal" /> : null}
      {del.isError ? <ErrorNote error={del.error} context="Could not delete that principal" /> : null}

      {minted !== null ? (
        <MintedKeyPanel
          issued={minted}
          // Dropping this state is genuinely all it takes: `useCreatePrincipal` resolves to the
          // stripped record, so the raw key never entered React Query's caches to begin with.
          onDismiss={() => setMinted(null)}
        />
      ) : null}

      {mayManage && minted === null ? (
        creating ? (
          <CreatePrincipalForm
            tenant={tenant}
            busy={create.isPending}
            onCancel={() => setCreating(false)}
            onSubmit={(body) =>
              // The key arrives through `useCreatePrincipal`'s `onIssued` (wired to `setMinted`),
              // not through this result — the result is the stripped record.
              create.mutate(body, { onSuccess: () => setCreating(false) })
            }
          />
        ) : (
          // Wrapped in a row: a bare button is a stretch-aligned child of the screen's flex column
          // and spans its full width, which reads as a broken banner rather than as an action.
          <div className="row">
            <button className="btn primary" type="button" onClick={() => setCreating(true)}>
              Create principal
            </button>
          </div>
        )
      ) : null}

      {principals.isPending ? <p className="muted">Reading…</p> : null}

      {principals.isSuccess ? (
        <Card
          title={`${principals.data.length} principal${principals.data.length === 1 ? "" : "s"}`}
          bleed
        >
          <div className="scroll-x">
            <table className="dense">
              <thead>
                <tr>
                  <th>Principal</th>
                  <th style={{ width: "16ch" }}>Role</th>
                  {/* Wide enough for the pill plus the cell's own 28px of padding — `dense` cells
                      clip, so a pill that overruns is silently cut in half rather than wrapped. */}
                  <th style={{ width: "18ch" }}>State</th>
                  {mayAdminister ? <th style={{ width: "18ch" }} aria-label="Actions" /> : null}
                </tr>
              </thead>
              <tbody>
          {principals.data.map((p) => (
            <PrincipalRow
              key={p.id}
              principal={p}
              mayManage={mayAdminister}
              busy={save.isPending || del.isPending}
              onToggle={() =>
                save.mutate({
                  tenantId: tenant,
                  principalId: p.id,
                  body: { displayName: p.displayName, disabled: !p.disabled },
                })
              }
              onDelete={() => del.mutate({ tenantId: tenant, principalId: p.id })}
            />
          ))}
              </tbody>
            </table>
          </div>
        </Card>
      ) : null}
    </>
  );
}

function MintedKeyPanel({
  issued,
  onDismiss,
}: {
  issued: IssuedPrincipal;
  onDismiss: () => void;
}): ReactNode {
  const [copyFailed, setCopyFailed] = useState(false);

  const copy = (): void => {
    /*
     * Clipboard access can be unavailable (no secure context, denied permission). A failed copy is
     * not a failed mint — the key is right there as selectable text — but it must be *said*: this
     * is the only moment the key exists, so an operator who clicks Copy and is silently given
     * nothing will discover it after the one thing they needed is unrecoverable.
     */
    const written = navigator.clipboard?.writeText(issued.apiKey);
    if (written === undefined) {
      setCopyFailed(true);
      return;
    }
    void written.then(() => setCopyFailed(false)).catch(() => setCopyFailed(true));
  };

  return (
    <div className="minted-key" data-testid="minted-key" role="alert">
      <div className="kh">
        <span aria-hidden="true">▲</span>
        Shown once
      </div>
      <p>
        <strong>{issued.displayName}</strong> was minted for role {issued.role}. {KEY_NOT_SHOWN_AGAIN}
      </p>
      <code>{issued.apiKey}</code>
      <div className="row">
        <button className="btn primary sm" type="button" onClick={copy}>
          Copy key
        </button>
        <button className="btn sm" type="button" onClick={onDismiss}>
          Dismiss
        </button>
      </div>
      {copyFailed ? (
        <p className="error" data-testid="copy-failed">
          Could not reach the clipboard — select the key above and copy it manually before
          dismissing.
        </p>
      ) : null}
    </div>
  );
}

function CreatePrincipalForm({
  tenant,
  busy,
  onCancel,
  onSubmit,
}: {
  tenant: string;
  busy: boolean;
  onCancel: () => void;
  onSubmit: (body: { displayName: string; role: Role }) => void;
}): ReactNode {
  const roles = assignableRoles(tenant);
  const [displayName, setDisplayName] = useState("");
  const [role, setRole] = useState<Role>(roles[0] ?? "viewer");

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    if (displayName.length === 0) return;
    onSubmit({ displayName, role });
  };

  return (
    <form onSubmit={submit} aria-label="Create principal">
      <label>
        Display name
        <input value={displayName} onChange={(e) => setDisplayName(e.target.value)} />
      </label>
      <label>
        Role
        <select value={role} onChange={(e) => setRole(e.target.value as Role)}>
          {roles.map((r) => (
            <option key={r} value={r}>
              {r}
            </option>
          ))}
        </select>
      </label>
      <button type="submit" disabled={busy}>
        Mint
      </button>
      <button className="btn" type="button" onClick={onCancel}>
        Cancel
      </button>
    </form>
  );
}

function PrincipalRow({
  principal,
  mayManage,
  busy,
  onToggle,
  onDelete,
}: {
  principal: Principal;
  mayManage: boolean;
  busy: boolean;
  onToggle: () => void;
  onDelete: () => void;
}): ReactNode {
  const addressable = isAddressablePrincipalId(principal.id);
  return (
    <tr data-testid="principal-row">
      {/*
       * The **name** leads and the id is secondary, truncated. A minted principal's id is
       * `key:<sha256-hex>` — not a credential, but 64 hex characters that look exactly like one, and
       * letting it lead made the list unreadable and taught the wrong instinct about what is safe to
       * paste. The whole id is still on the title and still selectable.
       */}
      <td data-testid={`principal-${principal.id}`}>
        <div className="id-cell">
          <span className="name">{principal.displayName}</span>
          <span className="meta" title={principal.id}>
            <Truncated value={principal.id} max={28} />
          </span>
        </div>
      </td>
      <td>{principal.role ?? UNKNOWN}</td>
      <td>
        {principal.disabled ? (
          <Status tone="idle" label="disabled" />
        ) : (
          <Status tone="ok" label="enabled" />
        )}
      </td>
      {mayManage ? (
        <td>
        {addressable ? (
          <span className="row">
            {/* The label is the verb alone; `aria-label` carries which principal it acts on. The
                name in the visible label wrapped every button onto two lines in a table where the
                row already says whose it is. */}
            <button
              className="btn sm"
              type="button"
              disabled={busy}
              aria-label={`${principal.disabled ? "Enable" : "Disable"} ${principal.displayName}`}
              onClick={onToggle}
            >
              {principal.disabled ? "Enable" : "Disable"}
            </button>
            <button
              className="btn sm danger"
              type="button"
              disabled={busy}
              aria-label={`Delete ${principal.displayName}`}
              onClick={onDelete}
            >
              Delete
            </button>
          </span>
        ) : (
          /*
           * Not merely disabled — unreachable. The server matches the raw path, so an id carrying
           * `#` or `?` would address a *different* principal (`alice#bob` → `alice`) and quietly
           * act on the wrong record. Saying so is the only honest option: the console cannot
           * encode its way out, and silently offering the button would be a destructive misfire.
           */
          <span className="muted" data-testid="principal-unaddressable">
            This id cannot be addressed in a URL path, so it cannot be changed from the console. Use
            the admin API directly.
          </span>
        )}
        </td>
      ) : null}
    </tr>
  );
}

/* ------------------------------------------------------------------------------------------- */
/* Bindings                                                                                       */
/* ------------------------------------------------------------------------------------------- */

function BindingsTab({ tenant }: { tenant: string }): ReactNode {
  const { can } = useSession();
  const mayManage = can("tenant.manage");
  const principals = usePrincipals(tenant);
  const putBinding = usePutBinding();
  const deleteBinding = useDeleteBinding();
  const roles = assignableRoles(tenant);
  const [principalId, setPrincipalId] = useState("");
  const [role, setRole] = useState<Role>(roles[0] ?? "viewer");

  if (principals.isError) {
    return <AdminApiError error={principals.error} />;
  }

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    if (principalId.length === 0) return;
    putBinding.mutate({ tenantId: tenant, principalId, role });
  };

  return (
    <>
      {putBinding.isError ? (
        <ErrorNote error={putBinding.error} context="Could not bind that role" />
      ) : null}
      {deleteBinding.isError ? (
        <ErrorNote error={deleteBinding.error} context="Could not remove that binding" />
      ) : null}

      {principals.isPending ? <p className="muted">Reading…</p> : null}

      {principals.isSuccess ? (
        <ul className="rows">
          {principals.data.map((p) => (
            <li key={p.id} data-testid="binding-row">
              <Ident>{p.displayName}</Ident> — {p.role ?? "no binding"}
              {mayManage ? (
                <button
                  type="button"
                  onClick={() => deleteBinding.mutate({ tenantId: tenant, principalId: p.id })}
                >
                  Unbind {p.displayName}
                </button>
              ) : null}
            </li>
          ))}
        </ul>
      ) : null}

      {mayManage ? (
        <form onSubmit={submit} aria-label="Bind a role">
          <label>
            Principal
            <select value={principalId} onChange={(e) => setPrincipalId(e.target.value)}>
              <option value="">Choose a principal</option>
              {(principals.data ?? []).map((p) => (
                <option key={p.id} value={p.id}>
                  {p.displayName}
                </option>
              ))}
            </select>
          </label>
          <label>
            Role
            {/* `fleet-admin` never appears here — `assignableRoles` refuses it for any scope but
                `*`, which is exactly the picker `BindingPut` would refuse anyway. */}
            <select
              data-testid="role-picker"
              value={role}
              onChange={(e) => setRole(e.target.value as Role)}
            >
              {roles.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
          </label>
          <button className="btn primary" type="submit">Bind</button>
        </form>
      ) : null}
    </>
  );
}

/* ------------------------------------------------------------------------------------------- */
/* Audit                                                                                          */
/* ------------------------------------------------------------------------------------------- */

function AuditTab({ tenant }: { tenant: string | null }): ReactNode {
  const [since, setSince] = useState(0);
  const rows = useAuditRows(tenant, since);

  if (rows.isError) {
    return <AdminApiError error={rows.error} />;
  }

  const cursor = rows.data === undefined ? null : nextSince(rows.data);

  return (
    <>
      {rows.isPending ? <p className="muted">Reading…</p> : null}
      {rows.isSuccess ? (
        <>
          <section className="card">
          <div className="scroll-x">
        <table className="dense">
            <thead>
              <tr>
                <th>Revision</th>
                <th>Principal</th>
                <th>Tenant</th>
                <th>Action</th>
                <th>Resource</th>
                <th>Outcome</th>
              </tr>
            </thead>
            <tbody>
              {rows.data.map((row) => (
                <tr key={row.opId} data-testid="audit-row">
                  <td data-testid="audit-revision">{row.revision}</td>
                  {/* `principal` and `resource` are attacker-influenceable (a chosen display name, a
                      chosen resource id) — rendered as plain JSX text, never through the banned
                      raw-HTML escape hatch (RFC-006 §9.1, enforced by lint). */}
                  <td>{row.principal ?? UNKNOWN}</td>
                  <td>{row.tenant}</td>
                  <td>{row.action}</td>
                  <td>{row.resource}</td>
                  <td data-testid={`audit-row-${row.revision}`}>
                    {isApplied(row.outcome) ? "applied" : `refused — ${failureReason(row.outcome)}`}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          </div>
        </section>
          <nav className="pager">
            <button
              className="btn"
              type="button"
              data-testid="audit-next"
              // A page shorter than the limit is the end of the journal. Gating on the cursor alone
              // left this live forever: clicking it then asked for a range past the end and rendered
              // an empty table under a header, which reads as "the trail stops here" — the one thing
              // an audit viewer must never imply.
              disabled={cursor === null || !hasMorePages(rows.data ?? [], AUDIT_PAGE_SIZE)}
              onClick={() => {
                if (cursor !== null) setSince(cursor);
              }}
            >
              Next (newer) page
            </button>
          </nav>
        </>
      ) : null}
    </>
  );
}

/**
 * Where the fleet ships its audit rows.
 *
 * `ClusterAdmin` throughout, and that is a deliberate ceiling rather than an oversight: a
 * `TenantAdmin` trusted to read their own tenant's audit rows is not thereby trusted to see — or
 * redirect — where every tenant's rows go. `authz.rs` puts `AuditSinkRead` in the same arm as the
 * writes for exactly that reason, so reading this screen is as privileged as changing it.
 */
function AuditSinkTab(): ReactNode {
  const { can } = useSession();
  const mayAdminister = can("cluster.admin");
  const sink = useAuditSink({ enabled: mayAdminister });
  const put = usePutAuditSink();
  const remove = useDeleteAuditSink();
  const [editing, setEditing] = useState(false);
  const [confirming, setConfirming] = useState(false);

  if (!mayAdminister) {
    return (
      <p className="muted" data-testid="sink-forbidden">
        The audit sink is fleet-scoped. A FleetAdmin binding is required to read or change it.
      </p>
    );
  }

  return (
    <div className="rows">
      {sink.isError ? <ErrorNote error={sink.error} context="Could not read the audit sink" /> : null}
      {put.isError ? <ErrorNote error={put.error} context="The sink was not saved" /> : null}
      {remove.isError ? <ErrorNote error={remove.error} context="The sink was not removed" /> : null}
      {put.data?.kind === "unobservable" ? <UnconfirmedNote reason={put.data.reason} /> : null}
      {remove.data?.kind === "unobservable" ? <UnconfirmedNote reason={remove.data.reason} /> : null}

      {sink.isPending ? <p className="muted">Reading…</p> : null}

      {sink.isSuccess && sink.data === null && !editing ? (
        <Empty
          testId="sink-none"
          title="No audit sink declared"
          body="Audit rows are retained in the fleet and readable on the Audit tab, but nothing is exporting them anywhere."
        >
          <button className="btn primary" type="button" onClick={() => setEditing(true)}>
            Declare a sink
          </button>
        </Empty>
      ) : null}

      {sink.isSuccess && sink.data !== null && !editing ? (
        <>
          <Card
            title="Declared sink"
            actions={
              <span className="row">
                <button className="btn sm" type="button" onClick={() => setEditing(true)}>
                  Edit
                </button>
                <button
                  className="btn sm danger"
                  type="button"
                  data-testid="remove-sink"
                  onClick={() => setConfirming(true)}
                >
                  Remove
                </button>
              </span>
            }
          >
            <dl className="detail" data-testid="sink-detail">
              <div className="kv">
                <dt>URI</dt>
                <dd>{sink.data.uri}</dd>
              </div>
              <div className="kv">
                <dt>Credential</dt>
                {/* A *name* the fleet resolves, never the credential. Absent is a real state — an
                    unauthenticated sink — not a missing field to paper over. */}
                <dd>{sink.data.authRef ?? <span className="muted">none</span>}</dd>
              </div>
              <div className="kv">
                <dt>Batch max rows</dt>
                <dd>{sink.data.batchMaxRows}</dd>
              </div>
              <div className="kv">
                <dt>Declared at revision</dt>
                <dd>{sink.data.revision}</dd>
              </div>
            </dl>
          </Card>

          <ExportStatus status={sink.data.exportStatus} />
        </>
      ) : null}

      {editing ? (
        <SinkForm
          existing={sink.data ?? null}
          busy={put.isPending}
          onCancel={() => setEditing(false)}
          onSave={(body) => put.mutate(body, { onSuccess: () => setEditing(false) })}
        />
      ) : null}

      {confirming ? (
        <Confirm
          testId="confirm-remove-sink"
          title="Remove the audit sink?"
          body="The fleet stops exporting audit rows. They are still retained and still readable on the Audit tab; nothing is shipped until a sink is declared again."
          confirmLabel="Remove sink"
          busy={remove.isPending}
          onCancel={() => setConfirming(false)}
          onConfirm={() => {
            remove.mutate();
            setConfirming(false);
          }}
        />
      ) : null}
    </div>
  );
}

/**
 * Export status, or the honest absence of it.
 *
 * **Only the leader runs the exporter**, so only the leader reports status. A follower answers with
 * the sink and no status at all — and the contract says so in as many words, "rather than a
 * fabricated all-zero one". Rendering absent as `0 rows shipped, not running` would turn "this node
 * cannot say" into "the export is broken", which is the same unknown-as-zero laundering the fleet
 * screen refuses.
 */
function ExportStatus({
  status,
}: {
  status: NonNullable<components["schemas"]["AuditSink"]["exportStatus"]> | undefined;
}): ReactNode {
  if (status === undefined) {
    return (
      <p className="hint" data-testid="sink-status-unknown">
        Export status is reported by the leader only, and this node is not it. Nothing here says
        whether the export is running — read this screen from the leader to see that.
      </p>
    );
  }
  return (
    <div className="tiles" data-testid="sink-status">
      <Tile
        label="Exporter"
        plain
        value={
          status.running ? (
            <Status tone="ok" label="running" />
          ) : (
            <Status tone="warn" label="not running" />
          )
        }
      />
      <Tile label="Rows shipped" value={status.shippedRows ?? UNKNOWN} />
      <Tile
        label="Consecutive failures"
        value={status.consecutiveFailures ?? UNKNOWN}
        // Spread rather than `tone={cond ? "warn" : undefined}`: `exactOptionalPropertyTypes` makes
        // an explicit `undefined` a different thing from an absent prop, and it is right to.
        {...((status.consecutiveFailures ?? 0) > 0 ? { tone: "warn" as const } : {})}
      />
      <Tile
        label="Last error"
        plain
        // `null` is "no error since the exporter started" and is a different fact from a missing
        // field; both render as a word rather than an empty cell.
        value={status.lastError ?? <span className="muted">none</span>}
      />
    </div>
  );
}

function SinkForm({
  existing,
  busy,
  onSave,
  onCancel,
}: {
  existing: components["schemas"]["AuditSink"] | null;
  busy: boolean;
  onSave: (body: components["schemas"]["AuditSinkWrite"]) => void;
  onCancel: () => void;
}): ReactNode {
  const [uri, setUri] = useState(existing?.uri ?? "");
  const [authRef, setAuthRef] = useState(existing?.authRef ?? "");
  const [batch, setBatch] = useState(existing?.batchMaxRows?.toString() ?? "");
  const [invalid, setInvalid] = useState<string | null>(null);

  function submit(event: FormEvent): void {
    event.preventDefault();
    if (uri.trim() === "") return setInvalid("A sink needs a URI.");
    let batchMaxRows: number | undefined;
    if (batch.trim() !== "") {
      const parsed = Number(batch);
      // Zero is refused rather than sent: the contract's default applies when the field is
      // *omitted*, and a literal 0 is a batch size that ships nothing, forever.
      if (!Number.isInteger(parsed) || parsed < 1) {
        return setInvalid("Batch max rows must be a whole number of 1 or more, or left blank for the server default.");
      }
      batchMaxRows = parsed;
    }
    setInvalid(null);
    onSave({
      uri: uri.trim(),
      ...(authRef.trim() === "" ? {} : { authRef: authRef.trim() }),
      ...(batchMaxRows === undefined ? {} : { batchMaxRows }),
    });
  }

  return (
    <Card title={existing === null ? "Declare audit sink" : "Edit audit sink"}>
      <form className="stub-form" onSubmit={submit} data-testid="sink-form">
        <div className="field">
          <label htmlFor="sink-uri">URI</label>
          <input id="sink-uri" value={uri} onChange={(e) => setUri(e.target.value)} />
        </div>
        <div className="field-row">
          <div className="field">
            <label htmlFor="sink-auth">Credential name</label>
            {/*
              Deliberately a plain text input, not a password field. This names a credential the
              fleet already holds — it is never the credential itself, and masking it would invite
              an operator to paste a secret into a field that ships it verbatim into the audit
              record.
            */}
            <input
              id="sink-auth"
              value={authRef}
              onChange={(e) => setAuthRef(e.target.value)}
              placeholder="optional"
            />
          </div>
          <div className="field">
            <label htmlFor="sink-batch">Batch max rows</label>
            <input
              id="sink-batch"
              inputMode="numeric"
              value={batch}
              onChange={(e) => setBatch(e.target.value)}
              placeholder="server default"
            />
          </div>
        </div>
        {invalid === null ? null : (
          <p className="error" data-testid="sink-invalid" role="alert">
            {invalid}
          </p>
        )}
        <div className="row">
          <button className="btn primary" type="submit" disabled={busy}>
            {busy ? "Saving…" : "Save sink"}
          </button>
          <button className="btn" type="button" onClick={onCancel} disabled={busy}>
            Cancel
          </button>
        </div>
        <p className="hint">
          A re-declared sink resumes exporting from its own recorded revision, so re-saving does not
          replay the whole stream.
        </p>
      </form>
    </Card>
  );
}
