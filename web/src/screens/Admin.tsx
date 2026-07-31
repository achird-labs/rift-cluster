import { type FormEvent, type ReactNode, useState } from "react";

import { ApiError } from "../api/client.ts";
import { isAddressablePrincipalId } from "../api/paths.ts";
import type { components } from "../api/schema.ts";
import type { AdminTab } from "../app/routing.ts";
import { toHash } from "../app/routing.ts";
import {
  useAuditRows,
  useCreatePrincipal,
  useCreateTenant,
  useDeleteBinding,
  useDeletePrincipal,
  useDeleteTenant,
  usePrincipals,
  usePutBinding,
  useSavePrincipal,
  useSaveTenant,
  useTenantProbe,
  useTenants,
  AUDIT_PAGE_SIZE,
} from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { ErrorNote, Ident, Status, UNKNOWN } from "../components/primitives.tsx";
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
};

export function Admin({ tab, tenant }: { tab: AdminTab; tenant: string | null }): ReactNode {
  const { can } = useSession();
  const mayManage = can("tenant.manage");
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
  const tabs: AdminTab[] = ["tenants", "principals", "bindings", "audit"];
  return (
    <nav className="admin-tabs">
      <ul>
        {tabs.map((t) => (
          <li key={t}>
            <a
              href={toHash({ screen: "admin", tab: t, tenant })}
              aria-current={t === tab ? "page" : undefined}
            >
              {TAB_LABEL[t]}
            </a>
          </li>
        ))}
      </ul>
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
          <button type="button" onClick={() => setCreating(true)}>
            Create tenant
          </button>
        )
      ) : null}

      {tenants.isPending ? <p className="muted">Reading…</p> : null}

      {tenants.isSuccess ? (
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
          <button type="button" onClick={onEdit}>
            Edit {tenant.displayName}
          </button>
          <button type="button" onClick={onDelete}>
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
          <button type="submit">Save tenant</button>
          <button type="button" onClick={onCancel}>
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
      <button type="submit">Create tenant</button>
      <button type="button" onClick={onCancel}>
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
          <button type="button" onClick={() => setCreating(true)}>
            Create principal
          </button>
        )
      ) : null}

      {principals.isPending ? <p className="muted">Reading…</p> : null}

      {principals.isSuccess ? (
        <div className="rows">
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
        </div>
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
      <p>
        <strong>{issued.displayName}</strong> was minted for role {issued.role}.
      </p>
      <p>{KEY_NOT_SHOWN_AGAIN}</p>
      <p>
        <Ident>{issued.apiKey}</Ident>
      </p>
      <button type="button" onClick={copy}>
        Copy key
      </button>
      <button type="button" onClick={onDismiss}>
        Dismiss
      </button>
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
      <button type="button" onClick={onCancel}>
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
    <div className="row" data-testid="principal-row">
      <span data-testid={`principal-${principal.id}`}>
        <Ident>{principal.id}</Ident> {principal.displayName} · {principal.role ?? UNKNOWN} ·{" "}
        {principal.disabled ? (
          <Status tone="idle" label="disabled" />
        ) : (
          <Status tone="ok" label="enabled" />
        )}
      </span>
      {mayManage ? (
        addressable ? (
          <>
            <button type="button" disabled={busy} onClick={onToggle}>
              {principal.disabled ? "Enable" : "Disable"} {principal.displayName}
            </button>
            <button type="button" disabled={busy} onClick={onDelete}>
              Delete {principal.displayName}
            </button>
          </>
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
        )
      ) : null}
    </div>
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
          <button type="submit">Bind</button>
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
          <nav className="pager">
            <button
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
