import type { UseQueryResult } from "@tanstack/react-query";
import { type FormEvent, type ReactNode, useEffect, useState } from "react";

import { ApiError } from "../api/client.ts";
import {
  type RouteHits,
  RouteTableConflict,
  useDeleteRoute,
  usePutRoutes,
  useRouteHits,
  useRouteTable,
} from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { Card, Empty, ErrorNote, Ident, Status, UnconfirmedNote } from "../components/primitives.tsx";
import type { Route } from "../features/routes/order.ts";
import { effectiveOrder, orderReason, validateTable } from "../features/routes/order.ts";
import { probeRoutes } from "../features/routes/probe.ts";
import { useToast } from "../components/toast.tsx";

/**
 * Is this tenant's table *known* to be uninstalled?
 *
 * D-70: two endpoints report this, and D-68 derives both from one server function — so they cannot
 * disagree, and the only thing that differs between them is which one answered. The table read is
 * local and the hits read is a cluster-wide fan-out, so the table's copy is asked first: deriving
 * the banner from the fan-out alone made it vanish whenever that query was slow, degraded or
 * failed, which is precisely when an operator is most likely to be looking for it (#539).
 *
 * The `undefined` case — neither body carried the flag — is deliberately not `true`. Everything
 * this predicate gates is a confident structural claim ("these routes can never take a request"),
 * and putting that behind a read the console could not complete would be the same
 * bound-versus-unknown error #369 exists to prevent, one level up. Unknown does not weaken as
 * sources are added: "neither said" is unknown, never a majority of silence. One definition, used
 * by every call site, so the rule cannot drift between them.
 */
function isNotInstalled(
  fromTable: boolean | undefined,
  hits: RouteHits | undefined,
): boolean {
  return (fromTable ?? hits?.installed) === false;
}

/**
 * Is it *established* that no node in the fleet binds a front-door listener (#403)?
 *
 * The sibling of {@link isNotInstalled}, one level down and with the same discipline. Only the
 * server's proven `none` counts — it is claimable solely on full fleet coverage, so it can never
 * arrive alongside a partial answer. `unknown` is the same absence unproven and deliberately reads
 * as today: diagnosing "nothing can dispatch" off an unreachable peer is the identical error to
 * diagnosing "this route is dead" off a zero, which is the whole reason this issue exists.
 */
function hasNoFrontDoorAnywhere(hits: RouteHits | undefined): boolean {
  // `partial === false` is redundant against a correct server — `none` is unclaimable without full
  // coverage, so the two can never both be set — and it is here precisely because it is redundant.
  // The banner asserts something about every node in the fleet; making that claim conditional on
  // the console's own view of coverage costs one comparison and stops a server-side regression in
  // the fold from becoming a confident wrong statement on screen.
  return hits?.installed === true && hits.frontDoor === "none" && !hits.partial;
}

export function RouteTableScreen(): ReactNode {
  const { can } = useSession();
  const table = useRouteTable();
  /*
   * Read once here and passed down, rather than read again inside `Editor`. Two observers of one
   * query key is not one cache read: `Editor` mounts only after the table resolves, and at
   * `staleTime: 0` the later observer refetches on mount — so the screen issued the cluster-wide
   * fan-out twice per load. Gated on the table read because the error branch below renders no
   * table at all, and a fan-out polling behind that screen buys nothing.
   */
  const hits = useRouteHits({ enabled: table.isSuccess });
  const notInstalled = isNotInstalled(table.data?.installed, hits.data);
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
          {notInstalled
            ? "Listed in stored order. This tenant's table is not evaluated by the front door — see below."
            : "Listed in the order the front door evaluates them, which is computed from the routes themselves — not the order they were authored in."}
        </p>
      </header>
      {table.isPending ? <p className="muted">Reading…</p> : null}
      {table.isSuccess ? (
        <div className="screen-split">
          <div className="screen-main">
            <Editor
              loaded={table.data.routes}
              mayWrite={mayWrite}
              hits={hits}
              notInstalled={notInstalled}
            />
            <FrontDoorNotes />
          </div>
          <RouteTester routes={table.data.routes} notInstalled={notInstalled} />
        </div>
      ) : null}
    </section>
  );
}

function Editor({
  loaded,
  mayWrite,
  hits,
  notInstalled,
}: {
  loaded: Route[];
  mayWrite: boolean;
  hits: UseQueryResult<RouteHits>;
  /** Resolved once by the screen and passed down, so no component re-derives the rule. */
  notInstalled: boolean;
}): ReactNode {
  /*
   * The `!notInstalled` is the same deliberate redundancy as the `!hits.partial` inside
   * `hasNoFrontDoorAnywhere`, and it is new surface rather than belt-and-braces: since #539 the two
   * inert-fact banners read `installed` from *different* endpoints, so a server that ever
   * contradicted itself between them could put both on screen at once, each stating something the
   * other denies. D-68 makes that unrepresentable; this keeps the exclusion a property of this
   * component rather than of a remote invariant.
   */
  const noFrontDoor = hasNoFrontDoorAnywhere(hits.data) && !notInstalled;
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
   *
   * Except when the table is never installed, where `effectiveOrder` is computing a chain that does
   * not exist — sorting by it would present a fabricated order under a header that says these are
   * listed as stored. Stored order is the only true ordering available for that tenant, and it is
   * what the muted rank and "why" columns are consistent with.
   */
  const rows = notInstalled ? draft : [...ordered, ...draft.filter((route) => !route.enabled)];

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

      {/*
       * `role="status"`, and the accent family rather than warn/crit: nothing here is broken or
       * needs attention, and a tenant cannot act on it at all. It is a standing structural fact
       * about where this table lives, so it is stated once above the rows instead of repeated as
       * an alarm on each of them.
       */}
      {notInstalled ? (
        <div className="banner info" data-testid="routes-not-installed" role="status">
          <span className="b-glyph" aria-hidden="true">
            &#x25c8;
          </span>
          <div>
            <strong>These routes are stored, but not compiled into the front door.</strong>
            <p>
              The front door is a single shared listener with no tenant discriminator, so only the
              default tenant&rsquo;s table is installed. This table is replicated and readable —
              editing it here is real — but no request can ever be dispatched through it.
            </p>
            <p>
              The reasoning is recorded in <Ident>docs/architecture/08-tenancy-security.md</Ident>,
              under &ldquo;<code>desired_routes</code> is deliberately NOT unioned&rdquo;: an
              arriving data-plane request carries no tenant identity, so a shared table would let
              any tenant&rsquo;s catch-all capture front-door traffic fleet-wide.
            </p>
          </div>
        </div>
      ) : null}

      {/*
       * The sibling of the not-installed banner, one level down: these routes ARE compiled into
       * the shared table, but nothing in the fleet is listening on it. Same inert-fact family, and
       * mutually exclusive with the banner above — that one renders only on `installed: false`,
       * and the server omits `front_door` entirely there. `noFrontDoor` is guarded against the
       * two-source case at its declaration; see the note there.
       */}
      {noFrontDoor ? (
        <div className="banner info" data-testid="routes-no-front-door" role="status">
          <span className="b-glyph" aria-hidden="true">
            &#x25c8;
          </span>
          <div>
            <strong>No node in this fleet binds a front-door listener.</strong>
            <p>
              These routes are installed and would be evaluated, but there is nothing listening for
              a request to evaluate them against — so the counts below are zero because nothing
              could arrive, not because the routes are wrong.
            </p>
            <p>
              Start a node with <code>--front-door</code> to serve this table.
            </p>
          </div>
        </div>
      ) : null}

      <section className="card">
        <div className="scroll-x">
      {hits.data?.partial ? (
        <div className="scope" data-testid="route-hits-partial" role="status">
          <span className="eyebrow">Hits</span>
          <span className="pill accent">
            <span className="g" aria-hidden="true">
              &#x25c8;
            </span>
            partial merge
          </span>
          <span className="coverage">
            A node could not be reached, so each count is a floor — at least this many, possibly
            more.
          </span>
        </div>
      ) : null}
      <table className="dense">
        <thead>
          <tr>
            <th style={{ width: "7ch" }}>Rank</th>
            <th>Id</th>
            <th>Match</th>
            <th>Target</th>
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
              {/*
               * A disabled route is excluded from dispatch, so it has no place in the chain — and
               * when the whole table is uninstalled there is no chain for any row to have a place
               * in, so a rank number would be a claim about an order that does not exist.
               */}
              <td
                data-testid="route-rank"
                title={
                  notInstalled
                    ? "Not in any dispatch chain — this tenant's table is never installed."
                    : undefined
                }
              >
                <span className={route.enabled && !notInstalled ? "order-rank" : "order-rank off"}>
                  {notInstalled ? "—" : (rank.get(route.id) ?? "—")}
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
              <HitsCell
                id={route.id}
                enabled={route.enabled}
                hits={hits.data}
                unavailable={hits.isError}
                notInstalled={notInstalled}
              />
              <td className="muted" data-testid="route-why">
                {routeWhy(route, notInstalled)}
              </td>
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

/**
 * What the "why this order" column says about one route.
 *
 * `orderReason` prose ("wins on priority", "more specific host") describes a place in a live chain,
 * so on an uninstalled table it is not merely overridden but never computed. Not-installed outranks
 * "disabled" because it is the stronger fact: switching a route off explains its absence from a
 * chain that, for this tenant, does not exist either way.
 */
function routeWhy(route: Route, notInstalled: boolean): string {
  if (notInstalled) return "not installed";
  return route.enabled ? orderReason(route) : "disabled";
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
// D-54: this card names the path prefix and the imposter's own port because those are the only
// two addressing schemes that exist. The `X-Rift-Port` header and `p-<port>.` subdomain of
// RFC-001 §6.3 were withdrawn, not deferred — a front-door route expresses either.
function FrontDoorNotes(): ReactNode {
  return (
    <div className="front-door-notes">
      <Card title="Gateway fallback">
        <p className="muted">Consulted only after every route misses.</p>
        <p>
          A test harness can name its own target two ways — the imposter&rsquo;s own port, or the
          gateway prefix <code>/__rift/&lt;port&gt;/…</code>, which is stripped before dispatch so
          predicates and recordings still see the bare path. An unmodified system under test can do
          neither, which is the whole reason the route table exists.
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
function RouteTester({
  routes,
  notInstalled,
}: {
  routes: readonly Route[];
  notInstalled: boolean;
}): ReactNode {
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
        {/*
         * Without this the panel contradicts the banner: it would name a winning route on a table
         * the screen has just said can never take a request. The verdict stays — it is a true
         * reading of the rules, and it is what the table would do once installed — but it stops
         * being presented as something that could happen to this tenant today.
         */}
        <p className="hint" data-testid="probe-hint">
          Evaluated by this console against the table above — the front door has no probe endpoint
          to ask, so this is a reading of the same rules rather than its verdict.
          {notInstalled &&
            " This tenant's table is never installed, so no request would reach any of these routes in the first place — this is what it would do if it were."}
        </p>
      </section>
    </aside>
  );
}

/**
 * One route's HITS figure, in the five states it can honestly be in.
 *
 * The zero is the reason this column exists — a route that could have taken a request and did not
 * is either wrong or dead — so it is rendered as a number and flagged, never as an empty cell. The
 * other four states exist to keep that flag honest, by never printing a number the fleet did not
 * report and never flagging a zero the fleet has already explained:
 *
 * - "not installed" — this tenant's routes are never compiled into the shared front door, so a
 *   zero would be a claim about traffic where the truth is about installation. Tested first,
 *   because it outranks the dash: it is knowable from the table read alone (#539), and it stays
 *   true whether or not a count was ever obtained;
 * - a muted zero for a **disabled** route, which is excluded from dispatch;
 * - a muted zero when **no node in the fleet binds a listener** (#403) — nothing could have
 *   arrived, and flagging every row at once is a diagnosis rather than a warning;
 * - a dash while the count is unknown.
 */
function HitsCell({
  id,
  enabled,
  hits,
  unavailable,
  notInstalled,
}: {
  id: string;
  enabled: boolean;
  hits: RouteHits | undefined;
  unavailable: boolean;
  notInstalled: boolean;
}): ReactNode {
  /*
   * D-70's corollary: ahead of the unavailable branch, not behind it. A failed or in-flight
   * fan-out leaves the *count* unknown, but when the table body has already established that this
   * tenant's routes are never compiled in, "not installed" is both stronger and still true — and a
   * dash there would hide the very fact #539 exists to surface, in the state that made it worth
   * surfacing.
   */
  if (notInstalled) {
    return (
      <td
        className="numeric muted"
        data-testid="route-hits"
        title="Stored, but never compiled into the shared front door — only the default tenant's routes are installed, so this route cannot take a dispatch at all."
      >
        not installed
      </td>
    );
  }
  if (unavailable || hits === undefined) {
    return (
      <td className="numeric muted" data-testid="route-hits" title="Dispatch counts unavailable">
        &#x2014;
      </td>
    );
  }
  const count = hits.hits?.[id];
  if (count === undefined) {
    // The server keys the map by every id in the table it read, so this means the table moved
    // between the two reads. Unknown, not zero.
    return (
      <td className="numeric muted" data-testid="route-hits" title="No count reported for this route">
        &#x2014;
      </td>
    );
  }
  // A disabled route is filtered out of the dispatch chain by `effective_order`, so its zero is
  // explained rather than alarming — flagging it would tell an operator their route is broken
  // seconds after they switched it off with the button in the next column. The count itself still
  // shows: a route disabled after taking 40 requests really did take 40.
  if (count === 0 && !enabled) {
    return (
      <td
        className="numeric muted"
        data-testid="route-hits"
        title="Disabled, so it is excluded from dispatch and can claim nothing."
      >
        {count}
      </td>
    );
  }
  // The fleet binds no listener anywhere, so this zero is explained for the same reason a disabled
  // route's is: nothing could have arrived. Flagging it would put a warning on every row at once,
  // which is the false diagnosis #403 exists to remove — and the banner above already states the
  // cause once, where it belongs. The count still shows; a route that took 40 before the last
  // listener went away really did take 40.
  if (count === 0 && hasNoFrontDoorAnywhere(hits)) {
    return (
      <td
        className="numeric muted"
        data-testid="route-hits"
        title="No front-door listener is bound anywhere in the fleet, so no route can take a request."
      >
        {count}
      </td>
    );
  }
  return (
    <td
      className={count === 0 ? "numeric warn-text" : "numeric"}
      data-testid="route-hits"
      // A statement of fact, not a diagnosis. "Wrong or dead" is the usual explanation and the
      // reason this column exists, but it is not the only one — and where the fleet has told us
      // the cause (no listener bound anywhere, handled above) the cell says so instead of
      // asserting a cause it cannot know.
      title={
        count === 0
          ? "No request has reached this route since the fleet started."
          : undefined
      }
    >
      {count}
    </td>
  );
}
