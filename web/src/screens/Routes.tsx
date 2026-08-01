import { type ReactNode, useEffect, useState } from "react";

import { ApiError } from "../api/client.ts";
import { RouteTableConflict, useDeleteRoute, usePutRoutes, useRouteTable } from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { Empty, ErrorNote, Ident, Status, UnconfirmedNote } from "../components/primitives.tsx";
import type { Route } from "../features/routes/order.ts";
import { effectiveOrder, orderReason, validateTable } from "../features/routes/order.ts";

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
      {table.isSuccess ? <Editor loaded={table.data} mayWrite={mayWrite} /> : null}
    </section>
  );
}

function Editor({ loaded, mayWrite }: { loaded: Route[]; mayWrite: boolean }): ReactNode {
  const [draft, setDraft] = useState<Route[]>(loaded);
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

      {mayWrite ? (
        <nav className="pager">
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
