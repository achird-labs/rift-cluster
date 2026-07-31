import { useQuery } from "@tanstack/react-query";
import type { ReactNode } from "react";

import { apiGet } from "../api/client.ts";
import { imposterPath } from "../api/paths.ts";
import type { components } from "../api/schema.ts";
import { IMPOSTER_COLUMNS, type ImposterColumn } from "../app/contract.ts";
import { POLLED } from "../app/query.ts";
import { useSession } from "../app/session.tsx";
import { toHash } from "../app/routing.ts";
import { ImposterField } from "../components/imposterFields.tsx";
import { ErrorNote, Ident, UNKNOWN } from "../components/primitives.tsx";

type Imposter = components["schemas"]["Imposter"];
type Stub = components["schemas"]["Stub"];

/**
 * Everything the list shows, plus `host` — which the list omits only for width. The detail screen
 * has room, and the bind address is one of the values an operator most often needs to paste.
 */
const DETAIL_FIELDS: readonly Pick<ImposterColumn, "key" | "label">[] = [
  ...IMPOSTER_COLUMNS,
  { key: "host", label: "Host" },
];

/**
 * One imposter, read-only. C5 (#188) turns the stub rows into an editor; until then this shows
 * what the contract declares about each stub and nothing more.
 */
export function ImposterDetail({ port }: { port: number }): ReactNode {
  const { tenant } = useSession();
  const imposter = useQuery({
    queryKey: ["imposter", port, { tenant }],
    queryFn: () => apiGet<Imposter>(imposterPath(port), { tenant }),
    ...POLLED,
  });

  return (
    <section className="screen">
      <header className="screen-head">
        <a href={toHash({ screen: "imposters" })}>&larr; Imposters</a>
        <h1>
          Imposter <Ident>{port}</Ident>
        </h1>
        <p className="scope-label">Served by this node from replicated state.</p>
      </header>

      {imposter.isError ? <ErrorNote error={imposter.error} context="Could not read this imposter" /> : null}
      {imposter.isPending ? <p className="muted">Reading…</p> : null}

      {imposter.isSuccess ? (
        <>
          <dl className="facts">
            {DETAIL_FIELDS.map((field) => (
              <div key={field.key} className="fact">
                <dt>{field.label}</dt>
                <dd data-testid={`detail-${field.key}`}>
                  <ImposterField imposter={imposter.data} field={field.key} />
                </dd>
              </div>
            ))}
          </dl>
          <Stubs stubs={imposter.data.stubs} />
        </>
      ) : null}
    </section>
  );
}

function Stubs({ stubs }: { stubs: Stub[] | undefined }): ReactNode {
  if (stubs === undefined) {
    return <p className="muted">This response carried no stub list.</p>;
  }
  if (stubs.length === 0) {
    return <p className="muted">No stubs. Every request to this imposter falls through.</p>;
  }
  return (
    <table className="dense">
      <thead>
        <tr>
          <th className="numeric">#</th>
          <th>Id</th>
          <th>Route</th>
          <th>Scenario</th>
          <th className="numeric">Predicates</th>
          <th className="numeric">Responses</th>
        </tr>
      </thead>
      <tbody>
        {stubs.map((stub, index) => (
          // Prefixed rather than bare `index`: an id and an index share one key space, so a stub
          // whose id happened to be "1" would collide with the stub at index 1.
          <tr key={stub.id ?? `index-${index}`} data-testid={`stub-row-${index}`}>
            <td className="numeric">
              <Ident>{index}</Ident>
            </td>
            <td>
              <Ident>{stub.id ?? UNKNOWN}</Ident>
            </td>
            <td>
              <Ident>{stub.routePattern ?? UNKNOWN}</Ident>
            </td>
            <td>{stub.scenarioName ?? UNKNOWN}</td>
            <td className="numeric">
              <Ident>{stub.predicates?.length ?? UNKNOWN}</Ident>
            </td>
            <td className="numeric">
              <Ident>{stub.responses?.length ?? UNKNOWN}</Ident>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
