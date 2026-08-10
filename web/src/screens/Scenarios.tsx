import { type ReactNode, useState } from "react";

import { RawJsonBody } from "../api/client.ts";
import {
  useAddSpaceStub,
  useClearFlowState,
  useFlowStateEntry,
  useAllScenarios,
  useImposter,
  useImposters,
  useResetScenarios,
  useScenarios,
  useSetFlowStateEntry,
  useSetScenarioState,
  useSpace,
  useTeardownSpace,
} from "../app/queries.ts";
import { useSession } from "../app/session.tsx";
import { useHashQuery } from "../app/routing.ts";
import { Card, Confirm, Empty, ErrorNote, Ident, Truncated, UNNAMED } from "../components/primitives.tsx";
import { shadowingStubIndex } from "../features/requests/stubFromRequest.ts";
import { scenarioDefinitions } from "../features/scenarios/definitions.ts";
import { Pending, PendingPanel } from "../components/pending.tsx";
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
  if (port === null) return <AllFlows />;
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

  // In the hash query, so a tab is linkable and survives a reload — the same rule the imposter
  // list's filters and the imposter detail's tabs follow.
  const [search, setSearch] = useHashQuery();
  const requested = new URLSearchParams(search).get("tab");
  const tab: FlowTab = FLOW_TABS.find((entry) => entry.id === requested)?.id ?? "scenarios";
  const setTab = (next: FlowTab): void => {
    const params = new URLSearchParams(search);
    if (next === "scenarios") params.delete("tab");
    else params.set("tab", next);
    setSearch(params.toString());
  };

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

      {/*
       * Three tabs, as the design has them. They are not three views of one thing — they are three
       * different tiers, and the screen used to stack them so a reader scrolled past the per-flow
       * position to reach the machine that defines it:
       *
       *   Scenarios & KV      the position each flow currently sits at, plus the durable KV beside it
       *   Spaces              the flows themselves — what a scenario state is scoped to
       *   Scenario definitions the FSM the match gate reads, which is imposter config, not runtime
       *
       * The last is the one worth separating hardest: editing a definition is an ordinary clustered
       * write to the imposter document, while everything on the first tab is per-flow runtime that
       * a reset discards. Same screen, opposite durability.
       */}
      <FlowTabs current={tab} onPick={setTab} />

      {tab === "scenarios" ? (
        <>
          <ScenarioPanel port={port} flow={resolvedFlow} state={scenarios} />
          <FlowStatePanel port={port} flowId={resolvedFlow} />
          <OwnershipRules />
        </>
      ) : null}

      {tab === "spaces" ? (
        <>
          <SpacePanel port={port} flowId={resolvedFlow} />
          <ActiveSpaces />
        </>
      ) : null}

      {tab === "defs" ? <ScenarioDefinitions port={port} /> : null}
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
            {/* The design carries both beside the state. Which node owns this flow, and the epoch
                and generation a write against it is fenced with, are the two facts that decide
                whether a state you just set will survive — and neither is published (#359). */}
            <th style={{ width: "14ch" }}>Owner</th>
            <th style={{ width: "14ch" }}>Fence</th>
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
        <td>
          <Pending
            issue={359}
            reason="Which node owns this flow is not published. The ring's membership and epoch are; the assignment is not."
          />
        </td>
        <td>
          <Pending
            issue={359}
            reason="The epoch and ownership generation a write against this flow is fenced with are not exposed."
          />
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
        <div className="kv">
          <dt>Flow owner</dt>
          <dd data-testid="space-owner">
            {/* The node holding this flow's state (#359). A flow is the only thing the ring owns —
                imposters and stubs are replicated to every node — so this is the one place in the
                console where "owner" is a real question with a real answer.

                `—` when the fleet did not say, never a guess: the server omits the field when no
                membership is applied or the imposter's context scope could not be read, and a
                wrong owner sends an operator to the wrong node. */}
            {space.owner === null ? "—" : <Ident>{space.owner}</Ident>}
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
          {/*
            Issue #336's companion. A predicate-less stub matching everything is correct Mountebank
            semantics — it is a legitimate space-wide default — so this is a *warning*, not an error,
            and nothing here refuses it.

            What it must not be is invisible. Such a stub shadows every stub below it in this space,
            and the table above renders positions rather than ids, so "why is my other stub not
            answering" has no visible cause without this sentence. The imposter's own stub list
            already says this (`RequestLog`'s `stub-shadow-warning`); a space's list is exactly
            where it was missing, and is also where #336 could silently install one.
          */}
          {shadowingStubIndex(space.stubs) !== null ? (
            <div className="banner warn" data-testid="space-stub-shadow-warning" role="status">
              <span className="b-glyph" aria-hidden="true">
                &#9650;
              </span>
              <div>
                Stub <Ident>{shadowingStubIndex(space.stubs)}</Ident> declares no predicates, so it
                matches every request in this space and the stubs after it never answer. That is
                valid — an empty predicate list is a space-wide default — but if a later stub is not
                firing, this is why.
              </div>
            </div>
          ) : null}
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
/**
 * Every imposter's flow state, without asking which imposter first.
 *
 * The design's flow-state screen is fleet-wide: flows listed across imposters, with the imposter as
 * a **prefix on the flow id** rather than a choice to make before anything renders. This screen used
 * to open on a "choose an imposter" card that was pixel-for-pixel the request log's, which is both
 * a worse first screen and the wrong shape — the question is "what is the fleet's flow state", and
 * an operator who wanted one imposter can still click into it.
 *
 * What is listed is each imposter's DEFAULT flow. Nothing enumerates the others: a space is created
 * implicitly by whatever flow id a request carried, so there is no route that lists them (#374).
 * That limit is stated on the screen rather than left to be discovered — a table that silently
 * showed one flow per imposter would read as "these are the flows".
 */
function AllFlows(): ReactNode {
  const imposters = useImposters();
  const ports = (imposters.data?.imposters ?? []).flatMap((imposter) =>
    imposter.port === undefined ? [] : [imposter.port],
  );
  const named = new Map(
    (imposters.data?.imposters ?? []).map((imposter) => [imposter.port, imposter.name] as const),
  );
  const { rows, pending } = useAllScenarios(ports);

  return (
    <section className="screen" data-testid="scenarios-screen">
      <header className="screen-head">
        <h1>Scenarios &amp; state</h1>
        <p className="scope-label">
          Every imposter&rsquo;s default flow. A flow is created implicitly by whatever id a request
          carried, and no route lists them, so the others are reachable only by naming one.
        </p>
      </header>

      {imposters.isError ? (
        <ErrorNote error={imposters.error} context="Could not read the imposter list" />
      ) : null}
      {imposters.isPending || pending ? <p className="muted">Reading…</p> : null}

      {imposters.isSuccess && ports.length === 0 ? (
        <Empty
          title="No imposters to inspect"
          body="Scenarios and spaces belong to an imposter. Create one and its flow state appears here."
        />
      ) : null}

      {rows.length === 0 ? null : (
        <Card title="Scenario FSMs &amp; flow KV" bleed>
          <div className="scroll-x">
            <table className="dense">
              <thead>
                <tr>
                  <th>Flow id</th>
                  <th style={{ width: "22ch" }}>State</th>
                  <th style={{ width: "13ch" }}>Owner</th>
                  <th style={{ width: "13ch" }}>Fence</th>
                  <th style={{ width: "132px" }} aria-label="Open" />
                </tr>
              </thead>
              <tbody>
                {rows.map(({ port, state }) => (
                  <tr key={port} data-testid={`flow-row-${String(port)}`}>
                    <td>
                      <span className="id-cell">
                        {/* `i<port>:<flow>` — the design's own form, and it is the reason this
                            screen needs no picker: the imposter is part of the flow's name. */}
                        <span className="name">
                          <Ident>
                            i{port}:{state.kind === "scenarios" ? state.flowId : "?"}
                          </Ident>
                        </span>
                        <span className="meta">
                          {named.get(port) ?? UNNAMED}
                          {state.kind === "scenarios" && state.scenarios.length > 0
                            ? ` · ${state.scenarios.map((entry) => entry.name).join(", ")}`
                            : " · no scenarios"}
                        </span>
                      </span>
                    </td>
                    <td>
                      {state.kind === "unknown" ? (
                        <span className="status status-warn">
                          <span className="g" aria-hidden="true">
                            &#9650;
                          </span>
                          unread
                        </span>
                      ) : state.scenarios.length === 0 ? (
                        <span className="muted">&mdash;</span>
                      ) : (
                        <span className="flow-states">
                          {state.scenarios.map((entry) => (
                            <span key={entry.name} className="pill accent">
                              {entry.state}
                            </span>
                          ))}
                        </span>
                      )}
                    </td>
                    <td>
                      <Pending
                        issue={359}
                        reason="Which node owns this flow is not published. The ring's membership and epoch are; the assignment is not."
                      />
                    </td>
                    <td>
                      <Pending
                        issue={359}
                        reason="The epoch and ownership generation a write against this flow is fenced with are not exposed."
                      />
                    </td>
                    <td>
                      <a className="btn sm" href={`#/scenarios/${String(port)}`}>
                        Open state
                      </a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      <OwnershipRules />
    </section>
  );
}

const FLOW_TABS = [
  { id: "scenarios", label: "Scenarios & KV" },
  { id: "spaces", label: "Spaces" },
  { id: "defs", label: "Scenario definitions" },
] as const;

type FlowTab = (typeof FLOW_TABS)[number]["id"];

function FlowTabs({
  current,
  onPick,
}: {
  current: FlowTab;
  onPick: (tab: FlowTab) => void;
}): ReactNode {
  return (
    <div className="tabs" role="tablist" aria-label="Flow state sections">
      {FLOW_TABS.map((entry) => (
        <button
          key={entry.id}
          type="button"
          role="tab"
          data-testid={`flow-tab-${entry.id}`}
          aria-selected={entry.id === current}
          onClick={() => onPick(entry.id)}
        >
          {entry.label}
        </button>
      ))}
    </div>
  );
}

/**
 * The FSM each scenario declares.
 *
 * Derived from the imposter's own stubs rather than read from a definitions endpoint, because there
 * is no such endpoint and there does not need to be: the machine IS the stubs. Each one names the
 * state it requires and the state it moves to, so `scenarioDefinitions` reassembles exactly the
 * graph the match gate walks — and every edge points at the stub that drives it, which is what
 * makes this a reading rather than a drawing.
 */
function ScenarioDefinitions({ port }: { port: number }): ReactNode {
  const imposter = useImposter(port);
  const defs = scenarioDefinitions(imposter.data?.data.stubs);

  if (imposter.isPending) return <p className="muted">Reading…</p>;
  if (imposter.isError) {
    return <ErrorNote error={imposter.error} context="Could not read this imposter's stubs" />;
  }

  if (defs.length === 0) {
    return (
      <Empty
        testId="scenario-defs-empty"
        title="No scenario is declared on this imposter"
        body="A stub joins a machine by naming a scenario and the state it requires or moves to. Until one does, there is no FSM for the match gate to read."
      />
    );
  }

  return (
    <>
      <p className="muted">
        A scenario is the FSM the match gate reads before a stub is allowed to answer. It is part of
        the imposter document, so editing one is an ordinary clustered write — not flow state, which
        is the per-flow position inside it.
      </p>
      {defs.map((def) => (
        <Card key={def.name} title={def.name} bleed>
          <div className="fsm-states">
            {def.states.map((state) => (
              <span
                key={state}
                className={`fsm-state${state === def.initial ? " is-initial" : ""}`}
              >
                {state}
                {state === def.initial ? <span className="fsm-initial">initial</span> : null}
              </span>
            ))}
          </div>
          {def.transitions.length === 0 ? (
            <p className="hint">
              Every stub in this scenario answers without advancing it, so the machine has no
              transitions — it is a set of states the traffic never moves between.
            </p>
          ) : (
            <div className="scroll-x">
              <table className="dense">
                <thead>
                  <tr>
                    <th style={{ width: "4ch" }}>#</th>
                    <th style={{ width: "20ch" }}>From</th>
                    <th style={{ width: "20ch" }}>To</th>
                    <th>Stub</th>
                  </tr>
                </thead>
                <tbody>
                  {def.transitions.map((transition, index) => (
                    <tr key={`${transition.from}-${transition.to}-${String(index)}`}>
                      <td className="ident">{index + 1}</td>
                      <td className="ident">{transition.from}</td>
                      <td className="ident">&rarr; {transition.to}</td>
                      <td className="ident">
                        {transition.stub === null ? (
                          <span className="muted">this stub carries no id</span>
                        ) : (
                          <Truncated value={transition.stub} max={28} />
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>
      ))}
    </>
  );
}

/**
 * What a handoff does to each kind of state, and the rule that stops two owners overlapping.
 *
 * Both are statements about the cluster rather than readings from it, and both belong beside the
 * state they describe: an operator setting a scenario position is entitled to know that it survives
 * a handoff while the sequence cursor beside it does not.
 */
function OwnershipRules(): ReactNode {
  return (
    <div className="ownership-rules">
      <Card title="On ownership change">
        <dl className="kv-grid">
          <dt>Scenario FSM / KV</dt>
          <dd className="good-text">adopt highest (m_idx, v, origin)</dd>
          <dt>Sequence cursors</dt>
          {/* Deliberate, not a gap: a cursor is a position in a stream the new owner never saw. */}
          <dd className="warn-text">reset — deliberate</dd>
          <dt>proxyOnce Recorded</dt>
          <dd className="good-text">adopts</dd>
          <dt>proxyOnce Pending</dt>
          <dd className="crit-text">dies with the owner, re-claims</dd>
          <dt>Journal / counters</dt>
          <dd>no owner &middot; CRDT merge-on-read</dd>
        </dl>
      </Card>
      <Card title="Isolated-owner rule">
        <p className="muted">
          A node that has not heard a leader heartbeat within 3&times; the election timeout marks
          itself isolated and refuses owner-side stateful operations. The two serving windows cannot
          overlap by more than the heartbeat bound, and the fencing tuple mops up anything written
          inside it.
        </p>
      </Card>
    </div>
  );
}

/**
 * The spaces this imposter actually has.
 *
 * Not readable: `GET .../spaces/{flowId}` reads one space by id and nothing lists them. A space is
 * created implicitly by whatever flow id a request carried, so "which exist" is exactly the
 * question an operator arrives with — and today they infer it from the request log.
 */
function ActiveSpaces(): ReactNode {
  return (
    <Card title="Active spaces">
      <PendingPanel
        issue={374}
        reason="No endpoint lists an imposter's spaces — one can be read by id, but they are created implicitly by whatever flow id a request carried, so there is nothing to enumerate them."
      />
    </Card>
  );
}
