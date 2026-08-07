import { type FormEvent, type ReactNode, useEffect, useState } from "react";

import { ApiError } from "../api/client.ts";
import { RouteTableConflict, useDeleteRoute, usePutRoutes, useRouteTable } from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { Card, Empty, ErrorNote, Ident, Status, UnconfirmedNote } from "../components/primitives.tsx";
import { Pending } from "../components/pending.tsx";
import type { Route } from "../features/routes/order.ts";
import { effectiveOrder, orderReason, validateTable } from "../features/routes/order.ts";
import { probeRoutes } from "../features/routes/probe.ts";
import { useToast } from "../components/toast.tsx";

export function RouteTableScreen(): ReactNode {
  const { can } = useSession();
  const table = useRouteTable();
  const mayWrite = can("imposter.write");

  if (table.isError) {
    return (
      <section className="screen">
        <h1>Front-door routes</h1>
        <ErrorNote error={table.error} context="Could not read this tenant's route table" />
      </section>
    );
  }

  return (
    <section className="screen">
      <header className="screen-head">
        <h1>Front-door routes</h1>
        <p className="scope-label">
          Listed in the order the front door evaluates them, which is computed from the routes
          themselves — not the order they were authored in.
        </p>
      </header>
      {table.isPending ? <p className="muted">Reading…</p> : null}
      {table.isSuccess ? (
        <div className="screen-split">
          <div className="screen-main">
            <Editor loaded={table.data} mayWrite={mayWrite} />
            <FrontDoorNotes />
          </div>
          <RouteTester routes={table.data} />
        </div>
      ) : null}
    </section>
  );
}

function Editor({ loaded, mayWrite }: { loaded: Route[]; mayWrite: boolean }): ReactNode {
  const [draft, setDraft] = useState<Route[]>(loaded);
  const [adding, setAdding] = useState(false);
  const [base, setBase] = useState<Route[]>(loaded);
  const [conflict, setConflict] = useState<Route[] | null>(null);
  const put = usePutRoutes();
  const remove = useDeleteRoute();

  const loadedKey = JSON.stringify(loaded);
  const draftKey = JSON.stringify(draft);
  const baseKey = JSON.stringify(base);

  /**
   * Is there a write out there we sent but could not watch land?
   *
   * While this holds, a poll of the table is not evidence of anything: it may predate the parked
   * write, and adopting it would present the pre-write table as current.
   */
  const unconfirmed = put.data?.outcome.kind === "unobservable";

  /*
   * Adopt a newly polled table only while the draft is clean. Overwriting a dirty draft with a poll
   * result would discard edits the operator is in the middle of making; keeping the stale base
   * instead is what lets the save-time re-read detect the concurrent change and offer a rebase.
   *
   * Suspended entirely while a write is unconfirmed. A parked `PUT` has not committed, so the very
   * next poll returns the table as it was *before* it — and adopting that would not merely show a
   * stale table, it would move `base` to it. The next save would then send
   * `{draft: stale, base: stale}`, sail through the optimistic-concurrency re-read, and undo the
   * in-flight write the moment it landed.
   */
  useEffect(() => {
    if (unconfirmed) return;
    if (draftKey === baseKey && loadedKey !== baseKey) {
      setDraft(loaded);
      setBase(loaded);
    }
  }, [loaded, loadedKey, draftKey, baseKey, unconfirmed]);

  const dirty = draftKey !== baseKey;
  const toast = useToast();
  const errors = validateTable(draft);
  const ordered = effectiveOrder(draft);
  const rank = new Map(ordered.map((route, index) => [route.id, index + 1]));
  /*
   * Rows are listed in evaluation order, not authoring order — that is the screen's whole job.
   * Disabled routes are not in `effectiveOrder` at all (they are never dispatched), so they are
   * appended after it rather than dropped: an operator still has to be able to see and re-enable
   * them.
   */
  const rows = [...ordered, ...draft.filter((route) => !route.enabled)];

  const save = (): void => {
    if (errors.length > 0) return;
    setConflict(null);
    put.mutate(
      { draft, base },
      {
        onError: (error) => {
          if (error instanceof RouteTableConflict) setConflict(error.current);
        },
        /*
         * Advance the base only on a write we watched commit. `unobservable` means the fleet took
         * it and we could not follow it, so treating the draft as the new baseline would call a
         * write saved on no evidence — and would leave the editor clean, ready to adopt the
         * pre-write table on the next poll.
         */
        onSuccess: ({ outcome }) => {
          if (outcome.kind === "applied") setBase(draft);
          /*
           * Both outcomes are confirmed, and they are not the same confirmation. `applied` is the
           * fleet holding this table; `unobservable` is the fleet having taken it while the console
           * lost sight of the commit — which is why that one is a `warn` and says so, rather than a
           * green tick over an unknown.
           */
          toast(
            outcome.kind === "applied"
              ? {
                  tone: "good",
                  message: `Route table saved — ${String(draft.length)} route${draft.length === 1 ? "" : "s"}`,
                  meta: "committed fleet-wide",
                }
              : {
                  tone: "warn",
                  message: "Route table accepted, not yet confirmed",
                  meta: "re-read this screen to see where it landed",
                },
          );
        },
      },
    );
  };

  const reapply = (current: Route[]): void => {
    // Rebase, never auto-merge: the operator's edits are kept as the draft and the freshly read
    // table becomes the base, so saving again is a decision they make with both tables on screen.
    setBase(current);
    setConflict(null);
  };

  return (
    <>
      {put.data?.outcome.kind === "unobservable" ? (
        <UnconfirmedNote reason={put.data.outcome.reason} />
      ) : null}
      {remove.data?.kind === "unobservable" ? <UnconfirmedNote reason={remove.data.reason} /> : null}
      {conflict !== null ? (
        <div className="banner warn" data-testid="route-conflict" role="alert">
          <span className="b-glyph" aria-hidden="true">
            ▲
          </span>
          <div>
            <strong>The route table changed while you were editing.</strong>
            <p>
              Your edits have not been sent, and the other change is still in place. Reapply your
              edits on top of the current table, or discard them.
            </p>
            <p>
              Now on the fleet:{" "}
              <span className="ident">
                {conflict.map((route) => route.id).join(", ") || "(empty table)"}
              </span>
            </p>
            <div className="row">
          <button className="btn sm" type="button" onClick={() => reapply(conflict)}>
            Reapply my edits
          </button>
          <button
            className="btn sm"
            type="button"
            onClick={() => {
              setDraft(conflict);
              setBase(conflict);
              setConflict(null);
            }}
          >
            Discard my edits
          </button>
            </div>
          </div>
        </div>
      ) : null}

      {errors.length > 0 ? (
        <div className="banner crit" data-testid="route-validation" role="alert">
          <span className="b-glyph" aria-hidden="true">
            ■
          </span>
          <div>
          <strong>The fleet would refuse this table as a whole.</strong>
          <ul>
            {errors.map((error) => (
              <li key={`${error.kind}-${error.message}`}>{error.message}</li>
            ))}
          </ul>
          </div>
        </div>
      ) : null}

      {/*
       * The server's refusal is the authority: the checks above are a mirror, and when the two
       * disagree the operator needs the fleet's own words, not this screen's paraphrase.
       */}
      {put.isError && !(put.error instanceof RouteTableConflict) ? (
        <p className="error" data-testid="route-server-error" role="alert">
          {put.error instanceof ApiError ? put.error.body : put.error.message}
        </p>
      ) : null}
      {remove.isError ? (
        <p className="error" data-testid="route-server-error" role="alert">
          {remove.error instanceof ApiError ? remove.error.body : remove.error.message}
        </p>
      ) : null}

      <section className="card">
        <div className="scroll-x">
      <table className="dense">
        <thead>
          <tr>
            <th style={{ width: "7ch" }}>Rank</th>
            <th>Id</th>
            <th>Match</th>
            <th>Target</th>
            {/* The design's HITS column. Nothing counts dispatches per route, so the figure is
                pending on #368 — the column is here because a route that has never taken a request
                is either wrong or dead, and both are worth a place to show. */}
            <th style={{ width: "12ch" }} className="numeric">
              Hits
            </th>
            <th>Why this order</th>
            {mayWrite ? <th>Actions</th> : null}
          </tr>
        </thead>
        <tbody>
          {rows.map((route) => (
            <tr key={route.id} data-testid="route-row">
              {/* A disabled route is excluded from dispatch, so it has no place in the chain. */}
              <td data-testid="route-rank">
                <span className={route.enabled ? "order-rank" : "order-rank off"}>
                  {rank.get(route.id) ?? "—"}
                </span>
              </td>
              <td data-testid="route-id">
                <Ident>{route.id}</Ident>
              </td>
              <td>
                <span className="match-clauses">
                  <span className="clause">{describeMatch(route)}</span>
                </span>
              </td>
              <td>
                <Ident>{route.target.port}</Ident>
                {route.target.strip_prefix ? " · strips prefix" : ""}
              </td>
              <td className="numeric">
                <Pending
                  issue={368}
                  reason="Nothing counts dispatches per route. A counter is fleet-wide state, so it wants the merge-on-read treatment the request journal already has."
                />
              </td>
              <td className="muted">{route.enabled ? orderReason(route) : "disabled"}</td>
              {mayWrite ? (
                <td>
                  <button
                    className="btn sm"
                    type="button"
                    onClick={() =>
                      setDraft(
                        draft.map((r) => (r.id === route.id ? { ...r, enabled: !r.enabled } : r)),
                      )
                    }
                  >
                    {route.enabled ? `Disable ${route.id}` : `Enable ${route.id}`}
                  </button>
                  {/*
                   * A single removal goes through DELETE rather than a whole-table PUT: it cannot
                   * take an unrelated concurrent edit down with it.
                   */}
                  <button
                    className="btn sm danger"
                    type="button"
                    onClick={() => remove.mutate({ routeId: route.id })}
                  >
                    Delete {route.id}
                  </button>
                </td>
              ) : null}
            </tr>
          ))}
        </tbody>
      </table>
        </div>
      </section>

      {draft.length === 0 ? (
        <Empty
          title="This tenant has no front-door routes"
          body="Every request reaches its imposter by port until a route is added here."
        />
      ) : null}

      {mayWrite && adding ? (
        <NewRoute
          existingIds={draft.map((route) => route.id)}
          onCancel={() => setAdding(false)}
          onAdd={(route) => {
            // Appended to the draft, not sent. The table is written whole, so a new route is
            // validated with the rest of it — `errors` already covers duplicate ids and the
            // ambiguity two routes only create together — and reaches the fleet on Save.
            setDraft([...draft, route]);
            setAdding(false);
          }}
        />
      ) : null}

      {mayWrite ? (
        <nav className="pager">
          <button
            className="btn"
            type="button"
            data-testid="add-route"
            onClick={() => setAdding(true)}
            disabled={adding}
          >
            Add route
          </button>
          {/* Disabled rather than silently no-op: `save()` returns early on a validation error, and
              a button that looks live but does nothing reads as a broken console. */}
          <button
            className="btn primary"
            type="button"
            onClick={save}
            disabled={put.isPending || errors.length > 0}
          >
            Save table
          </button>
          <button className="btn" type="button" onClick={() => setDraft(base)} disabled={!dirty}>
            Revert
          </button>
          {dirty ? <Status tone="warn" label="unsaved changes" /> : null}
        </nav>
      ) : null}
    </>
  );
}

function describeMatch(route: Route): string {
  const clauses: string[] = [];
  if (route.match?.host !== undefined) clauses.push(`host ${route.match.host}`);
  if (route.match?.path_prefix !== undefined) clauses.push(`path ${route.match.path_prefix}`);
  if (route.match?.method !== undefined) clauses.push(`method ${route.match.method}`);
  for (const header of route.match?.headers ?? []) {
    clauses.push(`${header.name ?? ""}: ${header.value ?? ""}`);
  }
  return clauses.length === 0 ? "everything (catch-all)" : clauses.join(" · ");
}

/**
 * Add one route to the draft table.
 *
 * Fields are the wire's, snake_case included: `path_prefix`, `strip_prefix`, `set_host` carry no
 * `serde(rename_all)` in `front_door/route_table.rs`, and this screen is where that already cost a
 * debugging cycle once (#189) — a camelCase guess here reads `undefined` and silently ranks the
 * route in an order the front door does not use.
 *
 * The only check here is a duplicate id, and only because the table's own validator reports it
 * against the whole table after the fact — catching it at the point of typing names the field.
 * Everything else (ambiguity, malformed host, strip-without-prefix) is deliberately left to
 * `validate`, which sees the table as a set: those are properties of the *combination*, and a form
 * that judged them alone would disagree with the fleet.
 */
function NewRoute({
  existingIds,
  onAdd,
  onCancel,
}: {
  existingIds: string[];
  onAdd: (route: Route) => void;
  onCancel: () => void;
}): ReactNode {
  const [id, setId] = useState("");
  const [port, setPort] = useState("");
  const [priority, setPriority] = useState("0");
  const [host, setHost] = useState("");
  const [pathPrefix, setPathPrefix] = useState("");
  const [method, setMethod] = useState("");
  const [stripPrefix, setStripPrefix] = useState(false);
  const [invalid, setInvalid] = useState<string | null>(null);

  function submit(event: FormEvent): void {
    event.preventDefault();
    const trimmedId = id.trim();
    if (trimmedId === "") return setInvalid("A route needs an id — it is how the API addresses it.");
    if (existingIds.includes(trimmedId)) return setInvalid(`The id ${trimmedId} is already used.`);
    const targetPort = Number(port);
    if (!Number.isInteger(targetPort) || targetPort < 1 || targetPort > 65535) {
      return setInvalid("Target port must be a whole number between 1 and 65535.");
    }
    const parsedPriority = Number(priority === "" ? "0" : priority);
    if (!Number.isInteger(parsedPriority)) return setInvalid("Priority must be a whole number.");

    // Empty clauses are omitted, never sent as "". A `path_prefix: ""` is a match clause that
    // matches everything, which is a materially different route from one with no path clause.
    const match: NonNullable<Route["match"]> = {};
    if (host.trim() !== "") match.host = host.trim();
    if (pathPrefix.trim() !== "") match.path_prefix = pathPrefix.trim();
    if (method.trim() !== "") match.method = method.trim().toUpperCase();

    setInvalid(null);
    onAdd({
      id: trimmedId,
      priority: parsedPriority,
      enabled: true,
      ...(Object.keys(match).length === 0 ? {} : { match }),
      target: { port: targetPort, strip_prefix: stripPrefix },
    });
  }

  return (
    <Card title="Add route">
      <form className="stub-form" onSubmit={submit} data-testid="new-route-form">
        <div className="field-row">
          <div className="field">
            <label htmlFor="route-id">Id</label>
            <input id="route-id" value={id} onChange={(e) => setId(e.target.value)} placeholder="checkout" />
          </div>
          <div className="field">
            <label htmlFor="route-port">Target port</label>
            <input id="route-port" inputMode="numeric" value={port} onChange={(e) => setPort(e.target.value)} placeholder="4545" />
          </div>
          <div className="field">
            <label htmlFor="route-priority">Priority</label>
            <input id="route-priority" inputMode="numeric" value={priority} onChange={(e) => setPriority(e.target.value)} />
          </div>
        </div>
        <div className="field-row">
          <div className="field">
            <label htmlFor="route-host">Host</label>
            <input id="route-host" value={host} onChange={(e) => setHost(e.target.value)} placeholder="api.example.com or *.example.com" />
          </div>
          <div className="field">
            <label htmlFor="route-path">Path prefix</label>
            <input id="route-path" value={pathPrefix} onChange={(e) => setPathPrefix(e.target.value)} placeholder="/orders" />
          </div>
          <div className="field">
            <label htmlFor="route-method">Method</label>
            <input id="route-method" value={method} onChange={(e) => setMethod(e.target.value)} placeholder="GET" />
          </div>
        </div>
        <label className="check">
          <input type="checkbox" checked={stripPrefix} onChange={(e) => setStripPrefix(e.target.checked)} />
          <span>
            Strip the path prefix before forwarding
            <span className="note">
              Needs a path prefix above — the fleet refuses the whole table otherwise
              (<code>StripWithoutPrefix</code>).
            </span>
          </span>
        </label>
        {invalid === null ? null : (
          <p className="error" data-testid="new-route-invalid" role="alert">
            {invalid}
          </p>
        )}
        <div className="row">
          <button className="btn primary" type="submit">
            Add to table
          </button>
          <button className="btn" type="button" onClick={onCancel}>
            Cancel
          </button>
        </div>
        <p className="hint">
          Added to the draft table. Nothing reaches the fleet until you save, and the table is
          validated as a whole first.
        </p>
      </form>
    </Card>
  );
}

/**
 * The two things the table does not say about itself.
 *
 * Both are static prose, and both earn their space by answering a question the route rows provoke:
 * what happens when nothing matches, and why a fleet behind a load balancer needs only one data
 * port. Neither is a reading, so neither pretends to be.
 */
function FrontDoorNotes(): ReactNode {
  return (
    <div className="front-door-notes">
      <Card title="Gateway fallback">
        <p className="muted">Consulted only after every route misses.</p>
        <p>
          A test harness can name its own target three ways — the imposter&rsquo;s own port, an
          <code> X-Rift-Port</code> header, or a route. An unmodified system under test can do none
          of them, which is the whole reason the route table exists.
        </p>
      </Card>
      <Card title="Why one port is enough">
        <p>
          Dispatch targets the imposter <em>object</em>, not its socket. An imposter whose own bind
          failed on one node is still served there through the front door — and behind a managed
          load balancer this is the only data port a Service has to expose.
        </p>
      </Card>
    </div>
  );
}

/**
 * Try a request against the table and see which route takes it.
 *
 * The verdict is **this console's reading**, not the front door's: there is no route-probe endpoint
 * to ask, so `probeRoutes` walks the same total order `effectiveOrder` computes and applies the
 * clauses the same way. That is said on the panel rather than left implied — a tester quietly
 * disagreeing with the real dispatcher would be worse than no tester, because it would be trusted.
 */
function RouteTester({ routes }: { routes: readonly Route[] }): ReactNode {
  const [host, setHost] = useState("");
  const [path, setPath] = useState("/");
  const [header, setHeader] = useState("");

  // `Name: value`, the way an operator would paste it out of curl. A line without a colon is not a
  // header clause and is ignored rather than guessed at.
  const headers = header.includes(":")
    ? [
        {
          name: header.slice(0, header.indexOf(":")).trim(),
          value: header.slice(header.indexOf(":") + 1).trim(),
        },
      ]
    : [];

  const result = probeRoutes(routes, { host, path, method: "GET", headers });

  return (
    <aside className="rail-right" aria-label="Route tester">
      <section className="rail-sect">
        <h2 className="eyebrow">Route tester</h2>
        <div className="field">
          <label htmlFor="probe-host">Host</label>
          <input
            id="probe-host"
            value={host}
            placeholder="payments.test"
            data-testid="probe-host"
            onChange={(event) => setHost(event.target.value)}
          />
        </div>
        <div className="field">
          <label htmlFor="probe-path">Path</label>
          <input
            id="probe-path"
            value={path}
            placeholder="/v1/orders"
            data-testid="probe-path"
            onChange={(event) => setPath(event.target.value)}
          />
        </div>
        <div className="field">
          <label htmlFor="probe-header">Header</label>
          <input
            id="probe-header"
            value={header}
            placeholder="X-Env: canary"
            data-testid="probe-header"
            onChange={(event) => setHeader(event.target.value)}
          />
        </div>
      </section>

      <section className="rail-sect">
        <div
          className={`probe-verdict ${result.winner === null ? "is-miss" : "is-hit"}`}
          data-testid="probe-verdict"
          role="status"
        >
          <strong>
            {result.winner === null ? "No route matches" : `Dispatched to ${result.winner.id}`}
          </strong>
          <p>
            {result.winner === null
              ? "The gateway fallback would be consulted, and a request that names no target reaches nothing."
              : `port ${String(result.winner.target.port)}${result.winner.target.strip_prefix ? " · strips prefix" : ""}`}
          </p>
        </div>
      </section>

      <section className="rail-sect">
        <h2 className="eyebrow">Evaluation trace</h2>
        {result.trace.length === 0 ? (
          <p className="hint">No enabled route to evaluate.</p>
        ) : (
          <ol className="trace">
            {result.trace.map((entry) => (
              <li key={entry.id} className={entry.hit ? "is-hit" : "is-miss"}>
                <span className="trace-id">{entry.id}</span>
                <span>{entry.why}</span>
              </li>
            ))}
          </ol>
        )}
        <p className="hint">
          Evaluated by this console against the table above — the front door has no probe endpoint
          to ask, so this is a reading of the same rules rather than its verdict.
        </p>
      </section>
    </aside>
  );
}
