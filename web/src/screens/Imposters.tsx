import { type FormEvent, type ReactNode, useState } from "react";

import type { components } from "../api/schema.ts";
import { IMPOSTER_COLUMNS } from "../app/contract.ts";
import type { FleetReadState, FleetView } from "../app/fleetView.ts";
import { viewConfidence } from "../app/fleetView.ts";
import {
  useCreateImposter,
  useDeleteImposter,
  useFleetView,
  useImposters,
  useLifecycleToggle,
} from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { toHash } from "../app/routing.ts";
import { ImposterField } from "../components/imposterFields.tsx";
import {
  Card,
  Confirm,
  Empty,
  ErrorNote,
  Truncated,
  UNKNOWN,
  UnconfirmedNote,
} from "../components/primitives.tsx";

type Imposter = components["schemas"]["Imposter"];

export function Imposters(): ReactNode {
  const { can } = useSession();
  const imposters = useImposters();
  const create = useCreateImposter();
  const remove = useDeleteImposter();
  const [creating, setCreating] = useState(false);
  const [confirming, setConfirming] = useState<Imposter | null>(null);
  // Only to qualify what the list shows. A principal without the fleet scope simply gets no
  // qualification — never a 404 error on a screen whose own read succeeded.
  const mayReadFleet = can("fleet.read");
  const fleet = useFleetView({ enabled: mayReadFleet });
  const toggle = useLifecycleToggle();

  const confidence = viewConfidence(fleetReadState(mayReadFleet, fleet));
  const mayToggle = can("imposter.lifecycle");
  const mayCreate = can("imposter.write");
  // `imposter.delete`, not `imposter.write`: they are separate actions server-side and granted from
  // separate arms, so gating on the wrong one is a drift waiting to happen (see `rbac.ts`).
  const mayDelete = can("imposter.delete");

  return (
    <section className="screen">
      <header className="screen-head">
        <h1>Imposters</h1>
        <p className="scope-label" data-testid="imposters-scope-label">
          Served by this node from replicated state.
          {confidence.partial ? ` This node is degraded: ${confidence.reason}.` : ""}
          {confidence.unknown ? ` Caveat: ${confidence.reason}.` : ""}
        </p>
        {mayCreate ? (
          <>
            <div className="spacer" />
            <button
              className="btn primary"
              type="button"
              data-testid="new-imposter"
              onClick={() => setCreating(true)}
            >
              New imposter
            </button>
          </>
        ) : null}
      </header>

      {create.isError ? (
        <ErrorNote error={create.error} context="The imposter was not created" />
      ) : null}
      {remove.isError ? (
        <ErrorNote error={remove.error} context="The imposter was not deleted" />
      ) : null}
      {create.data?.kind === "unobservable" ? <UnconfirmedNote reason={create.data.reason} /> : null}
      {remove.data?.kind === "unobservable" ? <UnconfirmedNote reason={remove.data.reason} /> : null}

      {creating ? (
        <NewImposter
          busy={create.isPending}
          onCancel={() => setCreating(false)}
          onCreate={(body) =>
            create.mutate(body, {
              // Closes only on success. A refused create that dismissed its own form would take the
              // operator's typing with it and leave the error pointing at a screen with no form.
              onSuccess: () => setCreating(false),
            })
          }
        />
      ) : null}

      {confirming === null ? null : (
        <Confirm
          testId="confirm-delete-imposter"
          title={`Delete ${confirming.name ?? `imposter ${confirming.port ?? ""}`}?`}
          body={
            <>
              This removes the imposter, its stubs, its recorded requests and its flow state across
              the fleet. Nothing undoes it.
            </>
          }
          confirmLabel={`Delete ${confirming.name ?? confirming.port ?? "imposter"}`}
          busy={remove.isPending}
          onCancel={() => setConfirming(null)}
          onConfirm={() => {
            const port = confirming.port;
            if (port !== undefined) remove.mutate({ port });
            setConfirming(null);
          }}
        />
      )}

      {imposters.isError ? <ErrorNote error={imposters.error} context="Could not list imposters" /> : null}
      {toggle.isError ? <ErrorNote error={toggle.error} context="That change did not take effect" /> : null}
      {toggle.data?.kind === "unobservable" ? <UnconfirmedNote reason={toggle.data.reason} /> : null}

      {imposters.isPending ? <p className="muted">Reading…</p> : null}

      {imposters.isSuccess && imposters.data.length === 0 ? (
        <EmptyState
          uncertain={confidence.partial || confidence.unknown}
          reason={confidence.reason}
        />
      ) : null}

      {imposters.isSuccess && imposters.data.length > 0 ? (
        <Card
          title={`${imposters.data.length} imposter${imposters.data.length === 1 ? "" : "s"}`}
          bleed
        >
          <div className="scroll-x">
            <table className="dense">
              <thead>
                <tr>
                  {IMPOSTER_COLUMNS.map((column) => (
                    <th key={column.key} className={column.numeric ? "numeric" : undefined}>
                      {column.label}
                    </th>
                  ))}
                  {mayToggle || mayDelete ? <th aria-label="Actions" /> : null}
                </tr>
              </thead>
              <tbody>
                {imposters.data.map((imposter, index) => (
                  <Row
                    key={imposter.port ?? `unnamed-${index}`}
                    imposter={imposter}
                    mayToggle={mayToggle}
                    mayDelete={mayDelete}
                    busy={toggle.isPending}
                    onToggle={(port, enable) => toggle.mutate({ port, enable })}
                    onDelete={() => setConfirming(imposter)}
                  />
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      ) : null}
    </section>
  );
}

/**
 * Three states, kept distinct on purpose: read it, never asked, asked and failed.
 *
 * Folding "asked and failed" into "never asked" is the tempting simplification and the wrong one —
 * it would let a FleetAdmin whose health read just 500'd see the same unqualified list as a viewer
 * who was never entitled to the reading in the first place.
 */
function fleetReadState(
  mayRead: boolean,
  fleet: { data: FleetView | undefined; isError: boolean },
): FleetReadState {
  if (!mayRead) return { kind: "not-asked" };
  if (fleet.data !== undefined) return { kind: "read", view: fleet.data };
  return fleet.isError ? { kind: "unavailable" } : { kind: "not-asked" };
}

/**
 * The first thing every new operator sees — and the state a naive console gets wrong.
 *
 * "No imposters" asserts a fact about the tenant from one node's answer. When that node is degraded
 * an imposter it has not caught up on would not appear, so the honest sentence is that the list
 * cannot be confirmed, naming the coverage rather than implying a clean empty fleet.
 */
function EmptyState({
  uncertain,
  reason,
}: {
  uncertain: boolean;
  reason: string | null;
}): ReactNode {
  return (
    <Empty
      testId="imposters-empty"
      // The mark carries the distinction too: a settled empty reads as an empty set, an
      // unconfirmed one as the warning glyph the rest of the console uses for degraded.
      mark={uncertain ? "▲" : "○"}
      title={
        uncertain
          ? "Cannot confirm this tenant is empty"
          : "No imposters in this tenant, in this node’s view"
      }
      body={
        uncertain ? (
          <span className="warn-text">
            {reason}. An imposter this node has not applied would not appear here.
          </span>
        ) : (
          // The console reads imposters and edits their stubs; it does not create them (RFC-006's
          // slices scope C4 to read-only and C5 to the *stub* editor). Until a slice adds that,
          // this is the only place the console says how — so it says it, rather than leaving an
          // operator on an empty screen with no next step. The port is explicit because
          // `createImposter` requires it: an auto-assigned port cannot replicate across the fleet.
          <>
            The console does not create imposters yet. Create one against the admin API and it
            appears here.
          </>
        )
      }
    >
      {uncertain ? null : (
        <pre>{`curl -X POST $ADMIN/imposters \\
  -H 'Authorization: <your key>' \\
  -H 'Content-Type: application/json' \\
  -d '{"port":4545,"protocol":"http","stubs":[]}'`}</pre>
      )}
    </Empty>
  );
}

function Row({
  imposter,
  mayToggle,
  mayDelete,
  busy,
  onToggle,
  onDelete,
}: {
  imposter: Imposter;
  mayToggle: boolean;
  mayDelete: boolean;
  busy: boolean;
  onToggle: (port: number, enable: boolean) => void;
  onDelete: () => void;
}): ReactNode {
  const port = imposter.port;
  const label = imposter.name ?? (port === undefined ? UNKNOWN : String(port));

  return (
    <tr data-testid={`imposter-row-${port ?? "unnamed"}`}>
      {IMPOSTER_COLUMNS.map((column) => (
        <td key={column.key} className={column.numeric ? "numeric" : undefined}>
          <ImposterField imposter={imposter} field={column.key} renderName={nameLink(imposter)} />
        </td>
      ))}
      {mayToggle || mayDelete ? (
        <td>
          {/* Rendered only for a role that holds the matching action. RFC-006 §3 rule 3: this is
              presentation — the admin front re-checks the same action on the call itself. */}
          {port === undefined ? null : (
            <span className="row">
              {mayToggle ? (
                <button
                  className="btn sm"
                  type="button"
                  disabled={busy}
                  aria-label={`${imposter.enabled ? "Disable" : "Enable"} ${label}`}
                  onClick={() => onToggle(port, !imposter.enabled)}
                >
                  {imposter.enabled ? "Disable" : "Enable"}
                </button>
              ) : null}
              {mayDelete ? (
                <button
                  className="btn sm danger"
                  type="button"
                  data-testid={`delete-imposter-${port}`}
                  aria-label={`Delete ${label}`}
                  onClick={onDelete}
                >
                  Delete
                </button>
              ) : null}
            </span>
          )}
        </td>
      ) : null}
    </tr>
  );
}

/** The name cell is the one field the list renders differently: it links through to the detail. */
function nameLink(imposter: Imposter): (name: string) => ReactNode {
  return (name) => {
    const cell = (
      <Truncated value={name} testId={`imposter-name-${imposter.port ?? "unnamed"}`} />
    );
    return imposter.port === undefined ? (
      cell
    ) : (
      <a href={toHash({ screen: "imposter", port: imposter.port })}>{cell}</a>
    );
  };
}

/**
 * The create form.
 *
 * **The port is a required field, not a convenience the console hides.** `createImposter` refuses an
 * auto-assigned port because each node would pick its own and the imposter could not replicate — so
 * the operator names it, and a blank one is refused here rather than sent.
 *
 * Protocol is a closed choice because the engine's is: `manager.rs` accepts `http` and `https` and
 * answers `InvalidProtocol` for anything else. Choosing `https` reveals the PEM pair, because an
 * https imposter without a cert fails at creation by design — upstream fails loudly there rather
 * than silently serving cleartext, and a form that let you submit one would just relay that error.
 *
 * No `If-Match`: there is no prior revision of an imposter that does not exist yet. A port already
 * in use comes back as the fleet's own refusal, which is the only check that sees every node.
 */
function NewImposter({
  busy,
  onCreate,
  onCancel,
}: {
  busy: boolean;
  onCreate: (body: Imposter) => void;
  onCancel: () => void;
}): ReactNode {
  const [port, setPort] = useState("");
  const [protocol, setProtocol] = useState("http");
  const [name, setName] = useState("");
  const [recordRequests, setRecordRequests] = useState(true);
  const [cert, setCert] = useState("");
  const [certKey, setCertKey] = useState("");
  const [invalid, setInvalid] = useState<string | null>(null);

  function submit(event: FormEvent): void {
    event.preventDefault();
    // A port is 1–65535 and nothing else. Checked here so an obvious typo is a sentence next to the
    // field rather than a round trip that comes back as a 400 with no field to point at.
    const parsed = Number(port);
    if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) {
      setInvalid("Port must be a whole number between 1 and 65535.");
      return;
    }
    if (protocol === "https" && (cert.trim() === "" || certKey.trim() === "")) {
      setInvalid("An https imposter needs both a certificate and a key, or it refuses to start.");
      return;
    }
    setInvalid(null);
    onCreate({
      port: parsed,
      protocol,
      recordRequests,
      // Sent explicitly rather than left to the schema default: the contract marks it required (it
      // carries a default, which `openapi-typescript` renders as non-optional), and a newly created
      // imposter that arrived disabled would look like a create that half-worked.
      enabled: true,
      // Omitted rather than sent empty: the contract's fields are optional, and a blank name is not
      // the same fact as no name.
      ...(name.trim() === "" ? {} : { name: name.trim() }),
      ...(protocol === "https" ? { cert: cert.trim(), key: certKey.trim() } : {}),
    });
  }

  return (
    <Card title="New imposter">
      <form className="stub-form" onSubmit={submit} data-testid="new-imposter-form">
        <div className="field-row">
          <div className="field">
            <label htmlFor="new-port">Port</label>
            <input
              id="new-port"
              inputMode="numeric"
              value={port}
              onChange={(event) => setPort(event.target.value)}
              placeholder="4545"
            />
          </div>
          <div className="field">
            <label htmlFor="new-protocol">Protocol</label>
            <select
              id="new-protocol"
              value={protocol}
              onChange={(event) => setProtocol(event.target.value)}
            >
              <option value="http">http</option>
              <option value="https">https</option>
            </select>
          </div>
          <div className="field">
            <label htmlFor="new-name">Name</label>
            <input
              id="new-name"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="checkout-api"
            />
          </div>
        </div>

        {protocol === "https" ? (
          <div className="field-row">
            <div className="field">
              <label htmlFor="new-cert">Certificate (PEM)</label>
              <textarea id="new-cert" value={cert} onChange={(e) => setCert(e.target.value)} />
            </div>
            <div className="field">
              <label htmlFor="new-key">Private key (PEM)</label>
              <textarea id="new-key" value={certKey} onChange={(e) => setCertKey(e.target.value)} />
            </div>
          </div>
        ) : null}

        {/*
          Checked by default, which **diverges from the API**: the contract's `recordRequests`
          defaults to `false`, so `POST /imposters` with the field omitted records nothing.
          Console-created imposters are almost always created in order to be watched, and "why is
          my request log empty" is the confusion that costs a debugging cycle — so the console
          opts in and says so at the control rather than silently inheriting a default that makes
          its own request log useless. The cost is stated because it is unbounded until retention
          trims it.
        */}
        <label className="check">
          <input
            type="checkbox"
            checked={recordRequests}
            onChange={(event) => setRecordRequests(event.target.checked)}
          />
          <span>
            Record requests
            <span className="note">
              The request log shows nothing without this. Every request is held in memory until
              retention trims it — turn it off for an imposter under load.
            </span>
          </span>
        </label>

        {invalid === null ? null : (
          <p className="error" data-testid="new-imposter-invalid" role="alert">
            {invalid}
          </p>
        )}

        <div className="row">
          <button className="btn primary" type="submit" disabled={busy}>
            {busy ? "Creating…" : "Create imposter"}
          </button>
          <button className="btn" type="button" onClick={onCancel} disabled={busy}>
            Cancel
          </button>
        </div>
      </form>
    </Card>
  );
}
