import { type ReactNode, useState } from "react";

import { RawJsonBody } from "../api/client.ts";
import {
  useAddSpaceStub,
  useClearFlowState,
  useFlowStateEntry,
  useImposters,
  useResetScenarios,
  useScenarios,
  useSetFlowStateEntry,
  useSetScenarioState,
  useSpace,
  useTeardownSpace,
} from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { Card, Confirm, Empty, ErrorNote, Ident, Truncated } from "../components/primitives.tsx";
import {
  ABSENT_ENTRY_CAVEAT,
  SPACE_STUB_CAVEAT,
  type ScenarioEntry,
  type Space,
  hasRequestCount,
  isEmptySpace,
} from "../features/scenarios/space.ts";

/**
 * Scenarios, spaces and flow state for one imposter (#232).
 *
 * The screen is organised around one fact the API forces and the UI must not hide: **everything
 * here is per-space**. A scenario's state, a space's stubs and a flow-state entry are all scoped to
 * a flow id, and there is no route that lists spaces or lists a flow's entries. So the screen reads
 * scenarios first — which is the one call that resolves and *echoes* a flow id, including the
 * imposter's default — and scopes the other two panels to whatever that read named.
 *
 * That ordering is why the flow is never assumed. Writing "default" on screen would be a guess; the
 * imposter's resolved id is a fact it told us.
 */
export function Scenarios({ port, flow }: { port: number | null; flow: string | null }): ReactNode {
  if (port === null) return <ImposterPicker />;
  // Keyed by port and flow so no panel's local state (a typed key, an open editor) survives a move
  // to a different imposter or space — the same reasoning as the request log's key.
  return <ForImposter key={`${port}:${flow ?? ""}`} port={port} flow={flow} />;
}

function ForImposter({ port, flow }: { port: number; flow: string | null }): ReactNode {
  const scenarios = useScenarios(port, flow);
  /*
   * The flow every other panel is scoped to, and it comes from the *response* rather than the
   * route. With no `flowId` in the hash the imposter resolves its own default and echoes it back;
   * until that lands there is no id to read a space under, which is what `null` means here.
   */
  const resolvedFlow =
    scenarios.data?.kind === "scenarios" ? scenarios.data.flowId : (flow ?? null);

  return (
    <section className="screen" data-testid="scenarios-screen">
      <header className="screen-head">
        <h1>Scenarios &amp; state</h1>
        <p className="muted">
          Imposter <Ident>{port}</Ident>
        </p>
      </header>

      {/*
       * A permanent scope strip, the same shape the request log uses. Per-space is the fact this
       * screen has to keep in front of the reader: every number and every control below acts on one
       * space, and an operator who reads them as the imposter's global state will reset a scenario
       * they did not mean to.
       */}
      <div className="scope" data-testid="scenarios-scope" role="status">
        <span className="eyebrow">Space</span>
        <span className="pill accent">
          <span className="g" aria-hidden="true">
            ◈
          </span>
          <span data-testid="resolved-flow">{resolvedFlow ?? "resolving…"}</span>
        </span>
        <span className="coverage">
          {flow === null
            ? "This imposter's default flow — the one it resolved for a request that named none. Other spaces are not shown."
            : "One space. Scenario states, scoped stubs and flow state all belong to this flow alone."}
        </span>
      </div>

      <ScenarioPanel port={port} flow={resolvedFlow} state={scenarios} />
      <SpacePanel port={port} flowId={resolvedFlow} />
      <FlowStatePanel port={port} flowId={resolvedFlow} />
    </section>
  );
}

function ScenarioPanel({
  port,
  flow,
  state,
}: {
  port: number;
  flow: string | null;
  state: ReturnType<typeof useScenarios>;
}): ReactNode {
  const { can } = useSession();
  const reset = useResetScenarios();
  const [confirming, setConfirming] = useState(false);
  // Two capabilities, not one: `POST .../scenarios/reset` is `Action::ScenarioReset` (Operator) and
  // `PUT .../scenarios/{name}/state` is `Action::ScenarioWrite` (Editor). See `rbac.ts`.
  const mayReset = can("scenario.reset");
  const mayWrite = can("scenario.write");

  return (
    <Card
      title="Scenarios"
      testId="scenarios-card"
      actions={
        mayReset ? (
          <button
            className="btn sm danger"
            type="button"
            data-testid="reset-scenarios"
            onClick={() => setConfirming(true)}
          >
            Reset all in this space
          </button>
        ) : undefined
      }
    >
      {reset.isError ? <ErrorNote error={reset.error} context="Scenarios were not reset" /> : null}

      {confirming ? (
        <Confirm
          testId="confirm-reset-scenarios"
          title="Reset every scenario in this space?"
          body={
            <>
              Every scenario returns to its initial state for flow{" "}
              <Ident>{flow ?? "the imposter's default"}</Ident>. Other spaces keep their own states,
              and nothing restores these.
            </>
          }
          confirmLabel="Reset scenarios"
          busy={reset.isPending}
          onCancel={() => setConfirming(false)}
          onConfirm={() => {
            reset.mutate({ port, flowId: flow });
            setConfirming(false);
          }}
        />
      ) : null}

      {state.isPending ? <p className="muted">Reading…</p> : null}
      {state.data?.kind === "unknown" ? (
        /*
         * Not an empty table. An imposter whose scenarios could not be read has an *unknown*
         * scenario set, and rendering it as none would tell an operator their stubs declare no
         * scenarios — a confident claim about their configuration, made from a failed read.
         */
        <div className="banner crit" data-testid="scenarios-unknown" role="alert">
          <span className="b-glyph" aria-hidden="true">
            ■
          </span>
          <div>
            <strong>These scenarios are unknown, not empty.</strong>
            <p>
              They could not be read, so nothing here says which scenarios exist or what state they
              are in. {state.data.reason}
            </p>
          </div>
        </div>
      ) : null}
      {state.data?.kind === "scenarios" && state.data.scenarios.length === 0 ? (
        <Empty
          testId="scenarios-empty"
          title="No scenarios in this space"
          body="This imposter answered. None of its stubs declare a scenario, so there is no state machine to steer here."
        />
      ) : null}
      {state.data?.kind === "scenarios" && state.data.scenarios.length > 0 ? (
        <ScenarioTable port={port} flow={flow} scenarios={state.data.scenarios} mayWrite={mayWrite} />
      ) : null}
    </Card>
  );
}

function ScenarioTable({
  port,
  flow,
  scenarios,
  mayWrite,
}: {
  port: number;
  flow: string | null;
  scenarios: ScenarioEntry[];
  mayWrite: boolean;
}): ReactNode {
  const [editing, setEditing] = useState<string | null>(null);

  return (
    <div className="scroll-x">
      <table className="dense">
        <thead>
          <tr>
            <th>Scenario</th>
            <th style={{ width: "26ch" }}>State in this space</th>
            {mayWrite ? <th style={{ width: "16ch" }} aria-label="Set state" /> : null}
          </tr>
        </thead>
        <tbody>
          {scenarios.map((scenario) => (
            <ScenarioRow
              key={scenario.name}
              port={port}
              flow={flow}
              scenario={scenario}
              mayWrite={mayWrite}
              editing={editing === scenario.name}
              onEdit={() => setEditing(scenario.name)}
              onClose={() => setEditing(null)}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ScenarioRow({
  port,
  flow,
  scenario,
  mayWrite,
  editing,
  onEdit,
  onClose,
}: {
  port: number;
  flow: string | null;
  scenario: ScenarioEntry;
  mayWrite: boolean;
  editing: boolean;
  onEdit: () => void;
  onClose: () => void;
}): ReactNode {
  const write = useSetScenarioState();
  const [draft, setDraft] = useState(scenario.state);

  return (
    <>
      <tr>
        <td>
          <Truncated value={scenario.name} />
        </td>
        <td data-testid={`scenario-state-${scenario.name}`}>
          <Ident>{scenario.state}</Ident>
        </td>
        {mayWrite ? (
          <td>
            <button
              className="btn sm"
              type="button"
              data-testid={`set-scenario-state-${scenario.name}`}
              // The draft is seeded here rather than at mount. This row survives the 5s poll, so a
              // draft initialised once holds whatever the state was when the screen first painted —
              // and submitting it after traffic (or another operator) moved the scenario would
              // silently revert it to a stale value.
              onClick={() => {
                setDraft(scenario.state);
                onEdit();
              }}
            >
              Set state
            </button>
          </td>
        ) : null}
      </tr>
      {/*
       * Outside the `editing` gate, deliberately. Cancel closes the editor, and a write already in
       * flight can fail after that — with the note inside the gate the row would unmount its only
       * error surface and the refetch would redisplay the old value with nothing to explain it.
       * Every other mutation on this screen renders its `ErrorNote` unconditionally; this matches.
       */}
      {write.isError ? (
        <tr>
          <td colSpan={3}>
            <ErrorNote error={write.error} context={`${scenario.name}'s state was not set`} />
          </td>
        </tr>
      ) : null}
      {editing ? (
        <tr>
          <td colSpan={3}>
            <form
              className="stub-form"
              onSubmit={(event) => {
                event.preventDefault();
                // `flowId` always rides along when one is known: omitted, the route writes the
                // imposter's *default* flow, which is not the space this screen is showing.
                write.mutate(
                  { port, name: scenario.name, state: draft, flowId: flow },
                  { onSuccess: onClose },
                );
              }}
            >
              <label>
                <span className="eyebrow">New state for {scenario.name}</span>
                <input
                  data-testid="scenario-state-input"
                  value={draft}
                  onChange={(event) => setDraft(event.target.value)}
                />
              </label>
              <button
                className="btn sm primary"
                type="submit"
                data-testid="scenario-state-save"
                disabled={write.isPending || draft.length === 0}
              >
                {write.isPending ? "Setting…" : "Set state"}
              </button>
              {/* Disabled while the write is in flight, like `Confirm`'s own Cancel: closing the
                  editor mid-write is what strands the outcome with nowhere obvious to land. */}
              <button
                className="btn sm"
                type="button"
                onClick={onClose}
                disabled={write.isPending}
              >
                Cancel
              </button>
            </form>
          </td>
        </tr>
      ) : null}
    </>
  );
}

function SpacePanel({ port, flowId }: { port: number; flowId: string | null }): ReactNode {
  const { can } = useSession();
  const space = useSpace(port, flowId);
  const teardown = useTeardownSpace();
  const [confirming, setConfirming] = useState(false);
  // `DELETE .../spaces/{flowId}` is `Action::SpaceTeardown` (Operator); `POST .../spaces/{flowId}/stubs`
  // is `Action::SpaceStubWrite` (Editor). Different tiers, so different controls.
  const mayTearDown = can("space.teardown");
  const mayAddStub = can("space.stubWrite");

  return (
    <Card
      title="Space"
      testId="space-card"
      actions={
        mayTearDown && flowId !== null ? (
          <button
            className="btn sm danger"
            type="button"
            data-testid="space-teardown"
            onClick={() => setConfirming(true)}
          >
            Tear down this space
          </button>
        ) : undefined
      }
    >
      {teardown.isError ? (
        <ErrorNote error={teardown.error} context="The space was not torn down" />
      ) : null}

      {confirming && flowId !== null ? (
        <Confirm
          testId="confirm-teardown-space"
          title="Tear down this space?"
          body={
            <>
              This drops the stubs scoped to flow <Ident>{flowId}</Ident> and every scenario state
              within it. The imposter&rsquo;s own stubs are untouched, and nothing restores this.
            </>
          }
          confirmLabel="Tear down space"
          busy={teardown.isPending}
          onCancel={() => setConfirming(false)}
          onConfirm={() => {
            teardown.mutate({ port, flowId });
            setConfirming(false);
          }}
        />
      ) : null}

      {space.isPending ? <p className="muted">Reading…</p> : null}
      {space.data?.kind === "unknown" ? (
        <div className="banner crit" data-testid="space-unknown" role="alert">
          <span className="b-glyph" aria-hidden="true">
            ■
          </span>
          <div>
            <strong>This space is unknown, not empty.</strong>
            <p>
              It could not be read, so nothing here says whether stubs are scoped to this flow.{" "}
              {space.data.reason}
            </p>
          </div>
        </div>
      ) : null}
      {space.data?.kind === "space" ? (
        <SpaceBody port={port} flowId={flowId} space={space.data.space} mayAddStub={mayAddStub} />
      ) : null}
    </Card>
  );
}

function SpaceBody({
  port,
  flowId,
  space,
  mayAddStub,
}: {
  port: number;
  flowId: string | null;
  space: Space;
  mayAddStub: boolean;
}): ReactNode {
  return (
    <>
      <dl className="detail">
        <div className="kv">
          <dt>Requests resolved to this space</dt>
          <dd data-testid="space-requests">
            {/* Never `0` for a body that carried no count: "nothing reached this space" is the
                question an operator opens this screen to answer, not one the console may answer
                for them. */}
            {hasRequestCount(space) ? space.numberOfRequests : "—"}
          </dd>
        </div>
      </dl>

      {isEmptySpace(space) ? (
        <Empty
          testId="space-empty"
          title="Nothing is scoped to this flow"
          body="This node answered. No stubs and no scenario states belong to this space — requests that resolve here are served by the imposter's own stubs alone."
        />
      ) : (
        <>
          {/* The caveat is permanent, not dismissible, and sits above the table rather than under
              it. A space's stubs are a different collection from the imposter's, and the whole risk
              is an operator reading this table as the imposter's stub list. */}
          <p className="muted" data-testid="space-stub-caveat">
            {SPACE_STUB_CAVEAT}
          </p>
          <div className="scroll-x">
            <table className="dense" data-testid="space-stubs">
              <thead>
                <tr>
                  <th style={{ width: "8ch" }}>#</th>
                  <th>Stub scoped to this flow</th>
                </tr>
              </thead>
              <tbody>
                {space.stubs.map((stub, index) => (
                  // Keyed by position: a space stub has no addressable id — there is no
                  // `by-id` route for this collection — so position is the only handle there is.
                  <tr key={`${index}`} data-testid="space-stub-row">
                    <td>{index}</td>
                    <td>
                      <pre className="payload">{JSON.stringify(stub, null, 2)}</pre>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {mayAddStub && flowId !== null ? <AddSpaceStub port={port} flowId={flowId} /> : null}
    </>
  );
}

const NEW_SPACE_STUB = JSON.stringify(
  { predicates: [{ equals: { path: "/example" } }], responses: [{ is: { statusCode: 200 } }] },
  null,
  2,
);

function AddSpaceStub({ port, flowId }: { port: number; flowId: string }): ReactNode {
  const add = useAddSpaceStub();
  const [open, setOpen] = useState(false);
  const [text, setText] = useState(NEW_SPACE_STUB);

  /*
   * Rendered whether or not the form is open, for the same reason as the scenario row above:
   * Cancel closes the form and an in-flight add can fail afterwards, so an error note living only
   * inside the open branch would have nowhere to appear.
   */
  const error = add.isError ? (
    <ErrorNote error={add.error} context="The stub was not added" />
  ) : null;

  if (!open) {
    return (
      <>
        {error}
        <button
          className="btn sm"
          type="button"
          data-testid="space-add-stub"
          onClick={() => setOpen(true)}
        >
          Scope a stub to this flow
        </button>
      </>
    );
  }

  return (
    <form
      className="stub-form"
      data-testid="space-add-stub-form"
      onSubmit={(event) => {
        event.preventDefault();
        // Sent verbatim, like the imposter's own stub editor: the operator's bytes are what gets
        // stored, rather than a parse-and-restringify that reorders their keys.
        add.mutate(
          { port, flowId, body: new RawJsonBody(text) },
          { onSuccess: () => setOpen(false) },
        );
      }}
    >
      {error}
      <label>
        <span className="eyebrow">Stub JSON, scoped to flow {flowId}</span>
        <textarea
          data-testid="space-stub-input"
          rows={10}
          value={text}
          onChange={(event) => setText(event.target.value)}
        />
      </label>
      <button
        className="btn sm primary"
        type="submit"
        data-testid="space-stub-save"
        disabled={add.isPending}
      >
        {add.isPending ? "Adding…" : "Add stub"}
      </button>
      <button
        className="btn sm"
        type="button"
        onClick={() => setOpen(false)}
        disabled={add.isPending}
      >
        Cancel
      </button>
    </form>
  );
}

function FlowStatePanel({ port, flowId }: { port: number; flowId: string | null }): ReactNode {
  const { can } = useSession();
  const [draftKey, setDraftKey] = useState("");
  /** The key actually asked for, which is not the one being typed. */
  const [readKey, setReadKey] = useState<string | null>(null);
  const entry = useFlowStateEntry(port, flowId, readKey);
  const clear = useClearFlowState();
  const [confirming, setConfirming] = useState(false);

  /*
   * The asymmetry that looks like a bug and is not. There is no `FlowStateWrite` action: the server
   * classifies `PUT .../flow-state/{flow}/{key}` as `Action::SpaceStubWrite` (Editor) and the
   * `DELETE` beside it as `Action::FlowStateClear` (Operator). So an operator may clear an entry
   * and may not set one, and the console must draw exactly that.
   */
  const maySet = can("space.stubWrite");
  const mayClear = can("flowState.clear");

  return (
    <Card
      title="Flow state"
      testId="flow-state-card"
      actions={
        mayClear && flowId !== null ? (
          <button
            className="btn sm danger"
            type="button"
            data-testid="flow-state-clear-all"
            onClick={() => setConfirming(true)}
          >
            Clear all entries
          </button>
        ) : undefined
      }
    >
      {/* Not a table with a "no entries" row: the contract publishes no route that lists a flow's
          entries, so an inventory is something this screen cannot build. Saying so is better than
          an empty grid that implies the flow is empty. */}
      <p className="muted">
        Entries are addressed one key at a time — the admin API publishes no route that lists them,
        so this panel can only answer about a key you name.
      </p>

      {clear.isError ? <ErrorNote error={clear.error} context="Flow state was not cleared" /> : null}

      {confirming && flowId !== null ? (
        <Confirm
          testId="confirm-clear-flow-state"
          title="Clear every flow-state entry in this space?"
          body={
            <>
              This empties the scratchpad for flow <Ident>{flowId}</Ident>. Scenario states and
              scoped stubs are untouched, and nothing restores these values.
            </>
          }
          confirmLabel="Clear flow state"
          busy={clear.isPending}
          onCancel={() => setConfirming(false)}
          onConfirm={() => {
            if (flowId !== null) clear.mutate({ port, flowId });
            setConfirming(false);
          }}
        />
      ) : null}

      <form
        className="stub-form"
        onSubmit={(event) => {
          event.preventDefault();
          setReadKey(draftKey);
        }}
      >
        <label>
          <span className="eyebrow">Key</span>
          <input
            data-testid="flow-state-key"
            value={draftKey}
            onChange={(event) => setDraftKey(event.target.value)}
          />
        </label>
        <button
          className="btn sm"
          type="submit"
          data-testid="flow-state-read"
          disabled={draftKey.length === 0 || flowId === null}
        >
          Read
        </button>
      </form>

      {readKey === null ? null : (
        <FlowStateResult
          port={port}
          flowId={flowId}
          entryKey={readKey}
          entry={entry}
          maySet={maySet}
          mayClear={mayClear}
        />
      )}
    </Card>
  );
}

function FlowStateResult({
  port,
  flowId,
  entryKey,
  entry,
  maySet,
  mayClear,
}: {
  port: number;
  flowId: string | null;
  entryKey: string;
  entry: ReturnType<typeof useFlowStateEntry>;
  maySet: boolean;
  mayClear: boolean;
}): ReactNode {
  const write = useSetFlowStateEntry();
  const clear = useClearFlowState();
  const [draft, setDraft] = useState<string | null>(null);

  const value = entry.data?.kind === "value" ? entry.data.entry.value : undefined;
  // `null` is a stored value, so it has to survive into the editor as the text `null` rather than
  // being treated as "nothing here".
  const asText = entry.data?.kind === "value" ? JSON.stringify(value, null, 2) : "";

  return (
    <div className="detail" data-testid="flow-state-result">
      {entry.isPending ? <p className="muted">Reading…</p> : null}

      {entry.data?.kind === "unknown" ? (
        <div className="banner crit" data-testid="flow-state-unknown" role="alert">
          <span className="b-glyph" aria-hidden="true">
            ■
          </span>
          <div>
            <strong>This entry is unknown, not unset.</strong>
            <p>It could not be read, so nothing here says whether the key holds a value. {entry.data.reason}</p>
          </div>
        </div>
      ) : null}

      {entry.data?.kind === "absent" ? (
        /*
         * Deliberately does not say "not set" and stop there. The contract documents this 404 as
         * "no such entry", but RFC-002 §8.4 renders a tenant the principal is not bound to as 404
         * as well — so the status is consistent with reading someone else's imposter, and claiming
         * the key is unset would be a guess dressed as an answer.
         */
        <p className="warn-text" data-testid="flow-state-absent" role="status">
          No value returned for <Ident>{entryKey}</Ident>. {ABSENT_ENTRY_CAVEAT}
        </p>
      ) : null}

      {entry.data?.kind === "value" ? (
        <div className="kv">
          <dt>
            <Ident>{entryKey}</Ident>
          </dt>
          <dd>
            <pre className="payload" data-testid="flow-state-value">
              {asText}
            </pre>
          </dd>
        </div>
      ) : null}

      {write.isError ? <ErrorNote error={write.error} context="The value was not set" /> : null}
      {clear.isError ? <ErrorNote error={clear.error} context="The entry was not cleared" /> : null}

      <div className="acts">
        {maySet && flowId !== null ? (
          draft === null ? (
            <button
              className="btn sm"
              type="button"
              data-testid="flow-state-set"
              onClick={() => setDraft(asText === "" ? "null" : asText)}
            >
              Set value
            </button>
          ) : (
            <form
              className="stub-form"
              onSubmit={(event) => {
                event.preventDefault();
                write.mutate(
                  // The route's body is `{ value: <any json> }`, and the operator's text is the
                  // value — sent raw so their formatting survives, wrapped by hand rather than by
                  // `JSON.stringify` so the text is not re-encoded as a JSON *string*.
                  { port, flowId, key: entryKey, body: new RawJsonBody(`{"value":${draft}}`) },
                  { onSuccess: () => setDraft(null) },
                );
              }}
            >
              <label>
                <span className="eyebrow">Value for {entryKey} — any JSON, including null</span>
                <textarea
                  data-testid="flow-state-value-input"
                  rows={6}
                  value={draft}
                  onChange={(event) => setDraft(event.target.value)}
                />
              </label>
              <button
                className="btn sm primary"
                type="submit"
                data-testid="flow-state-value-save"
                disabled={write.isPending}
              >
                {write.isPending ? "Setting…" : "Set value"}
              </button>
              <button className="btn sm" type="button" onClick={() => setDraft(null)}>
                Cancel
              </button>
            </form>
          )
        ) : null}

        {mayClear && flowId !== null ? (
          <button
            className="btn sm danger"
            type="button"
            data-testid="flow-state-clear-key"
            disabled={clear.isPending}
            onClick={() => clear.mutate({ port, flowId, key: entryKey })}
          >
            Clear this key
          </button>
        ) : null}
      </div>
    </div>
  );
}

/** Everything on this screen is per-imposter, so with no port in the hash it asks which one. */
function ImposterPicker(): ReactNode {
  const imposters = useImposters();
  return (
    <section className="screen" data-testid="scenarios-screen">
      <header className="screen-head">
        <h1>Scenarios &amp; state</h1>
        <p className="muted">
          Choose an imposter to read its scenarios, spaces and flow state.
        </p>
      </header>
      {imposters.isError ? (
        <ErrorNote error={imposters.error} context="Could not read the imposter list" />
      ) : null}
      {imposters.isPending ? <p className="muted">Reading…</p> : null}
      {imposters.isSuccess && imposters.data.length === 0 ? (
        <Empty
          title="No imposters to inspect"
          body="Scenarios and spaces belong to an imposter. Create one and its state appears here."
        />
      ) : null}
      {imposters.isSuccess && imposters.data.length > 0 ? (
        <Card title="Choose an imposter" bleed>
          <div className="scroll-x">
            <table className="dense">
              <thead>
                <tr>
                  <th style={{ width: "12ch" }}>Port</th>
                  <th>Name</th>
                  <th style={{ width: "14ch" }} aria-label="Open" />
                </tr>
              </thead>
              <tbody>
                {imposters.data.map((imposter) => (
                  <tr key={imposter.port}>
                    <td>
                      <span className="port">{imposter.port}</span>
                    </td>
                    <td>
                      <Truncated value={imposter.name ?? "—"} />
                    </td>
                    <td>
                      <a className="btn sm" href={`#/scenarios/${imposter.port}`}>
                        Open state
                      </a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      ) : null}
    </section>
  );
}
