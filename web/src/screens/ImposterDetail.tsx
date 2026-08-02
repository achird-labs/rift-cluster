import { type ReactNode, useState } from "react";

import type { components } from "../api/schema.ts";
import { IMPOSTER_COLUMNS, type ImposterColumn } from "../app/contract.ts";
import { useImposter } from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { toHash } from "../app/routing.ts";
import { ImposterField } from "../components/imposterFields.tsx";
import { Empty, ErrorNote, Ident, UNKNOWN } from "../components/primitives.tsx";
import { DeleteStubButton, StubEditor, type StubTarget } from "./StubEditor.tsx";

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
 * One imposter, with its stubs editable by id (C5, #188).
 *
 * The read is `useImposter` rather than a bare `apiGet` for one reason: it also returns the
 * `Rift-Cluster-Revision` the response was stamped with, and that token is the `If-Match` every
 * write on this screen is conditioned on. A save sent without it is last-writer-wins, so the
 * revision travels from this read into the editor and no further logic is allowed to invent one.
 */
export function ImposterDetail({ port }: { port: number }): ReactNode {
  const { can } = useSession();
  const imposter = useImposter(port);
  const [editing, setEditing] = useState<StubTarget | null>(null);
  const mayWrite = can("imposter.write");

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
          <dl className="tiles">
            {DETAIL_FIELDS.map((field) => (
              <div key={field.key} className="tile">
                <dt>{field.label}</dt>
                <dd data-testid={`detail-${field.key}`}>
                  <ImposterField imposter={imposter.data.data} field={field.key} />
                </dd>
              </div>
            ))}
          </dl>
          <Stubs
            port={port}
            imposter={imposter.data.data}
            revision={imposter.data.revision}
            mayWrite={mayWrite}
            editing={editing}
            onEdit={setEditing}
          />
        </>
      ) : null}
    </section>
  );
}

function Stubs({
  port,
  imposter,
  revision,
  mayWrite,
  editing,
  onEdit,
}: {
  port: number;
  imposter: Imposter;
  revision: string | null;
  mayWrite: boolean;
  editing: StubTarget | null;
  onEdit: (target: StubTarget | null) => void;
}): ReactNode {
  const stubs = imposter.stubs;
  const editingStub =
    editing?.kind === "existing" ? stubs?.find((stub) => stub.id === editing.stubId) : undefined;

  return (
    <>
      {mayWrite ? (
        <nav className="pager">
          <button className="btn sm" type="button" onClick={() => onEdit({ kind: "new" })}>
            Add stub
          </button>
        </nav>
      ) : null}

      {editing !== null && (editing.kind === "new" || editingStub !== undefined) ? (
        <StubEditor
          /*
           * Keyed by the target, not by the stub's content. The imposter is polled, so `original`
           * changes underneath this panel on every tick — remounting on that would discard the
           * operator's in-progress draft several times a minute.
           */
          key={editing.kind === "new" ? "new" : editing.stubId}
          port={port}
          target={editing}
          original={editingStub ?? {}}
          revision={revision}
          onDone={() => onEdit(null)}
        />
      ) : null}

      <StubTable
        port={port}
        stubs={stubs}
        revision={revision}
        mayWrite={mayWrite}
        onEdit={onEdit}
      />
    </>
  );
}

function StubTable({
  port,
  stubs,
  revision,
  mayWrite,
  onEdit,
}: {
  port: number;
  stubs: Stub[] | undefined;
  revision: string | null;
  mayWrite: boolean;
  onEdit: (target: StubTarget) => void;
}): ReactNode {
  if (stubs === undefined) {
    return <p className="muted">This response carried no stub list.</p>;
  }
  if (stubs.length === 0) {
    return (
      <Empty
        testId="imposter-no-stubs"
        title="No stubs"
        body="Every request to this imposter falls through to the default response."
      />
    );
  }
  return (
    <section className="card">
      <div className="scroll-x">
    <table className="dense">
      <thead>
        <tr>
          <th className="numeric">#</th>
          <th>Id</th>
          <th>Route</th>
          <th>Scenario</th>
          <th className="numeric">Predicates</th>
          <th className="numeric">Responses</th>
          {mayWrite ? <th>Actions</th> : null}
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
            {mayWrite ? (
              <td>
                <StubActions port={port} stub={stub} revision={revision} onEdit={onEdit} />
              </td>
            ) : null}
          </tr>
        ))}
      </tbody>
    </table>
      </div>
    </section>
  );
}

/**
 * The write controls for one stub.
 *
 * A stub with no `id` gets them inert, with the reason on screen. It is **not** given an
 * index-addressed fallback: an index is a position, so an index-addressed write racing a concurrent
 * insert or delete replaces a different stub and answers `200`. Refusing to offer the action is the
 * only honest option — the fix is to give the stub an id, which is a change to the stub.
 */
function StubActions({
  port,
  stub,
  revision,
  onEdit,
}: {
  port: number;
  stub: Stub;
  revision: string | null;
  onEdit: (target: StubTarget) => void;
}): ReactNode {
  const stubId = stub.id;
  if (stubId === undefined) {
    return (
      <span data-testid="stub-not-addressable">
        <button className="btn sm" type="button" disabled>
          Edit
        </button>{" "}
        <span className="muted">
          This stub has no id, so there is no by-id address for it. Editing it by position could
          overwrite a different stub if another editor inserts one first.
        </span>
      </span>
    );
  }
  return (
    <>
      <button
        className="btn sm"
        type="button"
        aria-label={`Edit ${stubId}`}
        onClick={() => onEdit({ kind: "existing", stubId })}
      >
        Edit
      </button>
      <DeleteStubButton port={port} stubId={stubId} revision={revision} />
    </>
  );
}
