import { type ReactNode, useEffect, useState } from "react";

import { ApiError, RawJsonBody } from "../api/client.ts";
import { StubConflict, useAddStub, useDeleteStub, usePutStub } from "../app/queries.ts";
import { CodeEditor } from "../components/CodeEditor.tsx";
import { UnconfirmedNote } from "../components/primitives.tsx";
import { type Finding, lintStub } from "../features/stubs/lint.ts";
import { PredicateBuilder, describePredicates } from "../features/stubs/PredicateBuilder.tsx";
import { type PredicateItem, projectPredicates, renderPredicates } from "../features/stubs/predicates.ts";
import {
  STUB_FIELDS,
  type StubField,
  type StubForm,
  project,
  render,
} from "../features/stubs/projection.ts";
import { ResponseBuilder, describeResponseList } from "../features/stubs/ResponseBuilder.tsx";
import {
  type ResponseLabel,
  type ResponseModel,
  describeResponses,
  foreignBehaviorsOf,
  projectResponses,
  renderResponses,
} from "../features/stubs/responses.ts";

/**
 * The stub editor (RFC-006 C5, issue #188).
 *
 * Three rules shape everything below.
 *
 * **The form never silently drops a key.** `project` either understands the whole stub or refuses;
 * on a refusal this renders raw JSON with a banner naming what the form would have lost. There is no
 * middle state, because the middle state is the one that saves six of a stub's eight keys.
 *
 * **The text is the document.** Both views edit one string. The form writes into it through
 * `render`, and it is that string — the operator's own bytes — that is sent, not a reserialization
 * of a parse of it. Key order and whitespace an operator chose survive a save.
 *
 * **A refused write is a prompt, never a merge.** A `409` puts both versions on screen and stops.
 * Reapplying is a button the operator presses, and it retries against the state the *other* editor
 * left behind, with that state's token.
 */

/**
 * What this panel is editing. An append has no id yet; the by-id routes cannot address it.
 *
 * `seed` (issue #250) is a starting document for a new stub — derived from a request the journal
 * recorded. One optional field rather than a second entry path into the editor, so the projection,
 * the lint pane, the pinned revision, the If-Match save and the 409 rebase all apply to a seeded
 * stub exactly as they do to a hand-written one.
 */
export type StubTarget =
  | { kind: "existing"; stubId: string }
  | { kind: "new"; seed?: unknown };

const PRETTY_INDENT = 2;

/** JSON as the editor shows it. Only used for text this console generated, never for stored text. */
function pretty(value: unknown): string {
  return JSON.stringify(value, null, PRETTY_INDENT);
}

/**
 * Merge the three panes' edits back into one stub document.
 *
 * Safe precisely because the panes are only ever shown together when the stub is projectable by
 * *all three* projections (see `editable` below) — which means, by construction, every top-level
 * key the stub carries is covered by `render(form)`, by `predicates`, or by `responses`, and there
 * is nothing else to preserve. An empty predicate or response list omits its key entirely, the same
 * "null emits no key" convention `render` already applies to every field.
 */
function composeStubText(
  form: StubForm,
  predicateItems: PredicateItem[],
  responseItems: ResponseModel[],
): string {
  const rendered = render(form);
  const predicatesJson = renderPredicates(predicateItems);
  const responsesJson = renderResponses(responseItems);
  /*
   * Rebuilt in reading order — id, predicates, responses — rather than appending each pane's key to
   * what `render` produced. Appending would order the document by which projection happened to own
   * which key, so every form edit silently reordered it relative to what the presets wrote.
   * Reordering is not data loss, but it is a diff the operator did not ask for on a document they
   * may be reviewing side by side with a file.
   *
   * `rest` is what `render` produced beyond `id` — empty today, since `STUB_FIELDS` is down to that
   * one row, and spread anyway so a future row added to that table lands in the document without
   * needing a change here.
   */
  const { id, ...rest } = rendered;
  const stub: Record<string, unknown> = {};
  if (id !== undefined) stub.id = id;
  if (predicatesJson.length > 0) stub.predicates = predicatesJson;
  if (responsesJson.length > 0) stub.responses = responsesJson;
  return pretty({ ...stub, ...rest });
}

/**
 * An id for a stub the operator is creating.
 *
 * Seeded rather than left blank, because `id` being optional is a one-way door: a stub without one
 * has no by-id address, so `StubActions` correctly refuses to edit it — and offers no delete
 * either. Creating one through this form therefore produced a stub that could not be edited OR
 * removed from the stub table, recoverable only by rewriting the imposter's whole stub list. The
 * form is the one place that can stop that happening, and it costs a default the operator is free
 * to overwrite.
 *
 * Readable rather than a bare UUID: it goes in a URL, a `Delete <id>` button label and the
 * conflict diff, and `stub-3f9a2c` is something a person can say out loud.
 */
function newStubId(): string {
  const suffix =
    typeof crypto !== "undefined" && "randomUUID" in crypto
      ? crypto.randomUUID().slice(0, 6)
      : Math.floor(Math.random() * 0xff_ffff)
          .toString(16)
          .padStart(6, "0");
  return `stub-${suffix}`;
}

/**
 * A blank stub for the append case.
 *
 * Deliberately still predicate-less: guessing a path would put words in the operator's mouth, and
 * the editor says out loud what a predicate-less stub does (see `Summary`) rather than hiding the
 * consequence behind a plausible default. An id is NOT the same kind of guess — it names the stub
 * rather than deciding what it matches, and without one the stub is uneditable.
 */
function newStubText(): string {
  return pretty({ id: newStubId(), responses: [{ is: { statusCode: 200 } }] });
}

/**
 * Starting points for a new stub.
 *
 * Not templates in any clever sense — each one is a whole stub the operator then edits. They exist
 * because the distance from "Add stub" to "a stub that does something" was six empty boxes and a
 * knowledge of the mountebank shape, and the first one is the hardest to write.
 *
 * Every preset carries a method and path predicate, so picking one also moves off the
 * matches-everything state rather than leaving the operator in it.
 */
const PRESETS: readonly { label: string; note: string; stub: unknown }[] = [
  {
    label: "JSON 200",
    note: "A GET that returns a JSON body",
    stub: {
      predicates: [{ equals: { method: "GET", path: "/example" } }],
      responses: [
        {
          is: {
            statusCode: 200,
            headers: { "Content-Type": "application/json" },
            body: '{"ok":true}',
          },
        },
      ],
    },
  },
  {
    label: "Created 201",
    note: "A POST that accepts and echoes an id",
    stub: {
      predicates: [{ equals: { method: "POST", path: "/example" } }],
      responses: [
        {
          is: {
            statusCode: 201,
            headers: { "Content-Type": "application/json" },
            body: '{"id":1}',
          },
        },
      ],
    },
  },
  {
    label: "Not found 404",
    note: "A path that should answer as missing",
    stub: {
      predicates: [{ equals: { method: "GET", path: "/missing" } }],
      responses: [{ is: { statusCode: 404 } }],
    },
  },
  {
    label: "No content 204",
    note: "A DELETE that succeeds with no body",
    stub: {
      predicates: [{ equals: { method: "DELETE", path: "/example" } }],
      responses: [{ is: { statusCode: 204 } }],
    },
  },
];

function Presets({ onPick }: { onPick: (stub: unknown) => void }): ReactNode {
  return (
    <div className="presets" data-testid="stub-presets">
      <span className="eyebrow">Start from</span>
      {PRESETS.map((preset) => (
        <button
          key={preset.label}
          className="btn sm"
          type="button"
          title={preset.note}
          onClick={() => onPick(preset.stub)}
        >
          {preset.label}
        </button>
      ))}
    </div>
  );
}

type Parsed = { ok: true; value: unknown } | { ok: false; message: string };

/**
 * A stub's `responses` array, or an empty list for any document that has not got one.
 *
 * Guarded rather than cast. Every OTHER reader of this document — `projectResponses`,
 * `describeResponses` — copes with `responses` being absent, `null`, an object, or a scalar,
 * because the editor's whole job is to keep rendering while the operator types a document into
 * existence. A bare `as unknown[]` here re-introduced the one thing this screen must never do:
 * `{"id":"s-1","responses":{}}` threw during render and took the editor down with it.
 */
function responseListOf(value: unknown): unknown[] {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return [];
  const responses = (value as Record<string, unknown>).responses;
  return Array.isArray(responses) ? responses : [];
}

function parse(text: string): Parsed {
  try {
    return { ok: true, value: JSON.parse(text) as unknown };
  } catch (error) {
    return { ok: false, message: error instanceof Error ? error.message : String(error) };
  }
}

export function StubEditor({
  port,
  target,
  original,
  revision,
  onDone,
}: {
  port: number;
  target: StubTarget;
  /** The stub as the fleet currently has it — the text the raw editor starts from, byte for byte. */
  original: unknown;
  /** The token from the imposter read. `null` disables saving; see `NoRevisionNote`. */
  revision: string | null;
  onDone: () => void;
}): ReactNode {
  const [text, setText] = useState(() => {
    if (target.kind !== "new") return pretty(original);
    return target.seed === undefined ? newStubText() : pretty({ id: newStubId(), ...target.seed });
  });
  /*
   * Was this draft seeded from a request? Used only to swap the presets for a note explaining where
   * the response came from — there is no re-derivation to guard against here. The seed is a
   * one-shot snapshot taken before this editor mounts (see `StubRowAction` in `RequestLog.tsx`),
   * and from the first keystroke the draft is the operator's.
   */
  const seeded = target.kind === "new" && target.seed !== undefined;
  /*
   * The If-Match token this editor will save with, pinned at open. The `revision` prop tracks the
   * polled imposter query, and the poll (or a focus refetch — the second-tab workflow verbatim)
   * refreshes it while the operator types. A save closing over the LIVE token would then pass the
   * precondition against a table the draft has never seen, silently discarding the other editor's
   * write — the exact lost update this whole flow exists to refuse. Draft and token must age
   * together; only the 409 re-read, where the operator is shown both sides, hands out a newer one.
   */
  const [pinnedRevision] = useState(revision);
  const [conflict, setConflict] = useState<{ mine: string; theirs: string | null } | null>(null);
  const [findings, setFindings] = useState<Finding[] | "unavailable" | "pending">("pending");

  const put = usePutStub();
  const add = useAddStub();
  const write = target.kind === "new" ? add : put;

  const parsed = parse(text);
  const formProjection = parsed.ok ? project(parsed.value) : null;
  const predicateProjection = parsed.ok ? projectPredicates(parsed.value) : null;
  const responseProjection = parsed.ok ? projectResponses(parsed.value) : null;
  /*
   * The three projections are independent — #247 split predicates out of `formProjection` and #248
   * split responses out (see `projection.ts`'s `walk`, which now refuses to look at either) — so a
   * stub is form-editable only when *all three* succeed. Bundled into one object (rather than three
   * booleans) so every read site below narrows `.form`, `.predicateItems` and `.responseItems` from
   * a single null check, instead of re-deriving the joint condition at each call site.
   */
  const editable =
    formProjection?.kind === "form" &&
    predicateProjection?.kind === "predicates" &&
    responseProjection?.kind === "responses"
      ? {
          form: formProjection.form,
          predicateItems: predicateProjection.items,
          responseItems: responseProjection.items,
        }
      : null;
  // Any projection refusing means some key the three cannot jointly represent; the union of all
  // three key lists is what the banner names, so an operator sees the whole reason, not a third of it.
  const unmodelledKeys = [formProjection, predicateProjection, responseProjection].flatMap(
    (projection) => (projection?.kind === "rawOnly" ? projection.unmodelledKeys : []),
  );
  /*
   * Labels for every response, derived from the document rather than from the projection — which is
   * the whole point: they must still render in exactly the case the projection REFUSED (AC5). A
   * stub carrying a proxy response opens raw-only, and the operator still needs to see that it has
   * a proxy and where it points without reading the JSON.
   */
  const responseLabels = parsed.ok ? describeResponses(parsed.value) : [];
  /*
   * Per-response, the behaviours the form does not edit (#249 AC5). Kept beside the labels rather
   * than folded into them because it answers a different question: `kind` says what the response
   * IS, this says what else it runs on the way out.
   */
  const responseExtras: string[][] = responseListOf(parsed.ok ? parsed.value : null).map(
    foreignBehaviorsOf,
  );

  /*
   * Advisory lint, re-run as the document changes. Deliberately not gating the save button: the
   * server validates every write and its refusal is what an operator must act on. A local linter
   * that could block a save would eventually block a legitimate one on a rule the server does not
   * have.
   */
  useEffect(() => {
    let current = true;
    setFindings("pending");
    void lintStub(text).then((result) => {
      if (current) setFindings(result);
    });
    return () => {
      current = false;
    };
  }, [text]);

  const send = (ifMatch: string | null): void => {
    if (!parsed.ok || ifMatch === null) return;
    setConflict(null);
    const mine = text;
    /*
     * For an append the id is whatever the operator typed into the document — the route does not
     * take one, but a `409` re-read uses it to find the stub the conflict is *about*. Falling back
     * to `""` when the draft names no id is fine: the lookup then finds nothing and the panel says
     * the fleet has no stub with that id, which is exactly what is true.
     */
    const draftId = (parsed.value as { id?: unknown } | null)?.id;
    write.mutate(
      {
        port,
        stubId: target.kind === "new" ? (typeof draftId === "string" ? draftId : "") : target.stubId,
        body: new RawJsonBody(mine),
        revision: ifMatch,
      },
      {
        onError: (error) => {
          if (error instanceof StubConflict) {
            setConflict({ mine, theirs: error.theirs === null ? null : pretty(error.theirs) });
          }
        },
        onSuccess: (outcome) => {
          // An `unobservable` write was accepted but not watched land; the note below says so and
          // the panel stays open so the operator still has their text. Only a confirmed apply
          // closes it.
          if (outcome.kind === "applied") onDone();
        },
      },
    );
  };

  /** The token a save must quote: the opening pin, until the conflict re-read replaces it. */
  const conflictRevision = write.error instanceof StubConflict ? write.error.revision : null;
  const effectiveRevision = conflict === null ? pinnedRevision : conflictRevision;

  return (
    <section className="stub-editor" data-testid="stub-editor">
      <header className="screen-head">
        <h2>{target.kind === "new" ? "New stub" : `Stub ${target.stubId}`}</h2>
      </header>

      {conflict !== null ? (
        <div className="degraded" data-testid="stub-conflict" role="alert">
          <strong>This imposter changed while you were editing.</strong> Your edit has not been
          sent, and the other change is still in place. Nothing has been merged — reapply your edit
          on top of what is there now, or discard it.
          <div className="stub-diff">
            <div>
              <h3>Mine — the edit I tried to save</h3>
              <pre data-testid="stub-conflict-mine">{conflict.mine}</pre>
            </div>
            <div>
              <h3>Theirs — the stub as it is now</h3>
              <pre data-testid="stub-conflict-theirs">
                {/* True of both cases that reach here: a stub deleted out from under the edit, and
                    an append whose id the fleet does not carry. */}
                {conflict.theirs ?? "(the fleet has no stub with this id right now)"}
              </pre>
            </div>
          </div>
          <button
            className="btn primary"
            type="button"
            disabled={effectiveRevision === null}
            onClick={() => send(effectiveRevision)}
          >
            Reapply my edit
          </button>
          <button
            className="btn"
            type="button"
            onClick={() => {
              setText(conflict.theirs ?? "");
              setConflict(null);
            }}
          >
            Discard my edit
          </button>
        </div>
      ) : null}

      {write.data?.kind === "unobservable" ? <UnconfirmedNote reason={write.data.reason} /> : null}

      {write.isError && !(write.error instanceof StubConflict) ? (
        <p className="error" data-testid="stub-server-error" role="alert">
          {write.error instanceof ApiError ? write.error.body : write.error.message}
        </p>
      ) : null}

      {parsed.ok ? null : (
        <p className="error" data-testid="stub-json-error" role="alert">
          This is not valid JSON, so it has not been sent: {parsed.message}
        </p>
      )}

      {parsed.ok && editable === null ? (
        <div className="degraded" data-testid="stub-raw-banner" role="status">
          <strong>Raw JSON only.</strong> This stub carries fields the form does not model, so
          editing it through the form would drop them. Unmodelled: {unmodelledKeys.join(", ")}
          {responseLabels.length === 0 ? null : (
            <ResponseLabels labels={responseLabels} extras={responseExtras} />
          )}
        </div>
      ) : null}

      {seeded ? (
        <p className="hint" data-testid="stub-seed-note">
          Seeded from the recorded request. The journal records requests, not responses — so the
          response below is a starting point, not something this request actually returned.
        </p>
      ) : null}

      {target.kind === "new" && !seeded && editable !== null ? (
        <Presets
          // A preset is a whole stub the operator then edits, so it needs the same id a blank new
          // stub gets — otherwise picking one silently opts out of being editable later.
          onPick={(stub) => setText(pretty({ id: newStubId(), ...(stub as object) }))}
        />
      ) : null}

      {editable !== null ? (
        <Summary
          predicateItems={editable.predicateItems}
          responseItems={editable.responseItems}
        />
      ) : null}

      <div className="stub-panes">
        {editable !== null ? (
          <div className="stub-form-pane">
            <PredicateBuilder
              items={editable.predicateItems}
              onChange={(nextItems) =>
                setText(composeStubText(editable.form, nextItems, editable.responseItems))
              }
            />
            <ResponseBuilder
              items={editable.responseItems}
              onChange={(nextItems) =>
                setText(composeStubText(editable.form, editable.predicateItems, nextItems))
              }
            />
            <StubFields
              form={editable.form}
              onChange={(nextForm) =>
                setText(composeStubText(nextForm, editable.predicateItems, editable.responseItems))
              }
            />
          </div>
        ) : null}
        <CodeEditor value={text} onChange={setText} label="Stub JSON" testId="stub-json" />
      </div>

      <LintPane findings={findings} />
      <BodyPreview value={parsed.ok ? parsed.value : null} />

      {effectiveRevision === null ? <NoRevisionNote /> : null}

      <nav className="pager">
        <button
          className="btn primary"
          type="button"
          onClick={() => send(effectiveRevision)}
          disabled={!parsed.ok || effectiveRevision === null || write.isPending}
        >
          Save stub
        </button>
        <button className="btn" type="button" onClick={onDone}>
          Cancel
        </button>
      </nav>
    </section>
  );
}

/**
 * The modelled fields, rendered from `STUB_FIELDS` rather than written out.
 *
 * Widening the form (RFC-006 §12 Q2) is a row in that table; nothing here names a field.
 */
function StubFields({
  form,
  onChange,
}: {
  form: StubForm;
  onChange: (form: StubForm) => void;
}): ReactNode {
  /*
   * An empty box means "this stub does not carry that field", so it becomes `null` and `render`
   * emits no key — not an empty string, which would be a stub carrying that field set to "".
   *
   * There is no numeric coercion here any more: every row left in `STUB_FIELDS` holds a string,
   * because the one numeric field (`statusCode`) moved into `responses.ts` with #248. `StubField`
   * still declares `kind: "number"` for the next numeric row — and `StubForm` will have to widen to
   * admit one, which is precisely what the compiler will point at when that day comes.
   */
  const edit = (field: StubField, raw: string): void => {
    onChange({ ...form, [field.key]: raw === "" ? null : raw });
  };

  return (
    <div className="stub-form" data-testid="stub-form">
      {/*
        Widened to `StubField` deliberately. `STUB_FIELDS` is `as const`, so each entry narrows to
        its own literal type and an optional key absent from one entry is absent from the union —
        the renderer needs the declared shape, while `project`/`render` keep the literal one.
      */}
      {(STUB_FIELDS as readonly StubField[]).map((field) => {
        const value = form[field.key];
        const shown = value === null ? "" : String(value);
        const listId = field.suggest === undefined ? undefined : `suggest-${field.key}`;
        return (
          <div className="field" key={field.key}>
            <label htmlFor={`stub-${field.key}`}>{field.label}</label>
            {field.multiline === true ? (
              <textarea
                id={`stub-${field.key}`}
                rows={8}
                value={shown}
                onChange={(event) => edit(field, event.target.value)}
              />
            ) : (
              <input
                id={`stub-${field.key}`}
                type={field.kind === "number" ? "number" : "text"}
                list={listId}
                value={shown}
                onChange={(event) => edit(field, event.target.value)}
              />
            )}
            {field.suggest === undefined ? null : (
              <datalist id={listId}>
                {field.suggest.map((option) => (
                  <option key={option} value={option} />
                ))}
              </datalist>
            )}
            {field.hint === undefined ? null : <span className="field-hint">{field.hint}</span>}
          </div>
        );
      })}
    </div>
  );
}

/**
 * What this stub will actually do, in a sentence.
 *
 * The form is a handful of boxes and the JSON is a document; neither answers "so what does it
 * match?" at a glance. This is derived from both projections rather than from the text, so it
 * cannot claim anything the modelled fields do not say.
 */
function Summary({
  predicateItems,
  responseItems,
}: {
  predicateItems: PredicateItem[];
  responseItems: ResponseModel[];
}): ReactNode {
  const matchesEverything = predicateItems.length === 0;
  return (
    <div className={matchesEverything ? "banner warn" : "hint"} data-testid="stub-summary">
      {matchesEverything ? (
        <span className="b-glyph" aria-hidden="true">
          ▲
        </span>
      ) : null}
      <div>
        {matchesEverything ? (
          <>
            <strong>This stub matches every request.</strong>
            <p>
              It carries no predicates, so it answers anything reaching this imposter that no
              earlier stub claimed. That is occasionally what you want — a catch-all — and usually
              not.
            </p>
          </>
        ) : (
          <>
            Matches requests where <b>{describePredicates(predicateItems)}</b>, and{" "}
            <b>{describeResponseList(responseItems)}</b>.
          </>
        )}
      </div>
    </div>
  );
}

/**
 * Every response the stub carries, named by kind — shown beside the raw-only banner.
 *
 * This is the "recognised, but not editable" half of AC5. A stub with a `proxy` or `inject`
 * response cannot open in the form without the form pretending it can edit JavaScript or a proxy
 * rule, so it opens raw-only; that is honest, but on its own it leaves the operator to work out
 * from the JSON what kind of responses they are looking at. Read-only by construction — there is no
 * `onChange` here, so nothing this renders can write to the document.
 */
function ResponseLabels({
  labels,
  extras,
}: {
  labels: ResponseLabel[];
  extras: string[][];
}): ReactNode {
  return (
    <ul className="response-labels" data-testid="stub-response-labels">
      {labels.map((label) => {
        const runs = extras[label.index] ?? [];
        return (
          <li key={label.index}>
            Response {label.index + 1}: <b>{label.kind}</b>
            {label.detail === "" ? null : ` — ${label.detail}`}
            {runs.length === 0 ? null : <> · also runs: <b>{runs.join(", ")}</b></>}
          </li>
        );
      })}
    </ul>
  );
}

function LintPane({ findings }: { findings: Finding[] | "unavailable" | "pending" }): ReactNode {
  if (findings === "pending") {
    return (
      <div className="stub-lint" data-testid="stub-lint">
        <p className="muted">Linting…</p>
      </div>
    );
  }
  if (findings === "unavailable") {
    return (
      <div className="stub-lint" data-testid="stub-lint">
        <p className="muted">
          lint unavailable — the server still validates every save, and its refusal is what counts.
        </p>
      </div>
    );
  }
  return (
    <div className="stub-lint" data-testid="stub-lint">
      <p className="muted">
        Advisory only. The server validates every save and may refuse a stub this finds no fault
        with.
      </p>
      {findings.length === 0 ? (
        <p className="muted">No findings.</p>
      ) : (
        <ul>
          {findings.map((finding) => (
            <li key={`${finding.code}-${finding.location ?? ""}-${finding.message}`}>
              <strong>{finding.severity}</strong> {finding.code}: {finding.message}
              {finding.location === undefined ? null : ` (${finding.location})`}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

/**
 * The response body, shown as text.
 *
 * RFC-006 §9.1: a stub body is attacker-influenced data. It is rendered into a `<pre>` as a React
 * child, which escapes it — never through `innerHTML`, which lint and
 * `contract-traceability.test.ts` both ban.
 */
function BodyPreview({ value }: { value: unknown }): ReactNode {
  const projection = value === null ? null : projectResponses(value);
  if (projection?.kind !== "responses") return null;
  /*
   * The FIRST response's body. A cycling stub has several, and previewing only the first is the
   * honest simplification: the panel is a "what does this send" glance, and the response cards
   * above already show each body in full. Previewing a concatenation of all of them would show
   * something no single call ever returns.
   */
  const [first] = projection.items;
  if (first === undefined) return null;
  const body =
    first.body.kind === "text"
      ? first.body.text
      : first.body.kind === "json"
        ? pretty(first.body.value)
        : null;
  if (body === null) return null;
  return (
    <details className="stub-body">
      <summary>Response body</summary>
      <pre data-testid="stub-body-preview">{body}</pre>
    </details>
  );
}

function NoRevisionNote(): ReactNode {
  return (
    <p className="error" data-testid="stub-no-revision" role="alert">
      This read carried no <code>Rift-Cluster-Revision</code>, so there is no token to condition the
      write on — and without it this save could silently overwrite an edit made from another tab.
      Saving is disabled. Reload this screen; every imposter the fleet knows about answers with one.
    </p>
  );
}

/**
 * A per-stub delete, addressed by id and conditioned on the same token a save uses.
 *
 * Split out from the editor because deleting is not editing: it needs no draft, no projection and
 * no lint, and folding it in would make the editor's state machine answer for two operations.
 */
export /*
 * Unlike the editor, the delete button quotes the LIVE polled token on purpose. The editor pins
 * because its draft freezes at open while the world moves — draft and token must age together. This
 * row has no frozen draft: it renders the polled imposter, so the operator is looking at the very
 * state the live token names. Pinning here would do harm instead — the token is imposter-scoped,
 * so ANY stub write (including an unrelated colleague's) would turn a pinned delete into a false
 * 409 — while the live token still refuses the one race that matters, a delete quoting state older
 * than a write that landed between the poll and the click.
 */
function DeleteStubButton({
  port,
  stubId,
  revision,
}: {
  port: number;
  stubId: string;
  revision: string | null;
}): ReactNode {
  const remove = useDeleteStub();
  return (
    <>
      {/* `btn sm danger`, which it never carried: this button has been rendering with the browser's
          default chrome since it was written, sitting directly beside an `Edit` that has always been
          `btn sm`. Invisible while the console's own buttons were also grey and square; obvious the
          moment everything around it changed. `danger` because deleting a stub is destructive, which
          is the same reason the imposter list's Delete carries it. */}
      <button
        className="btn sm danger"
        type="button"
        disabled={revision === null || remove.isPending}
        onClick={() => remove.mutate({ port, stubId, revision })}
      >
        Delete {stubId}
      </button>
      {remove.isError ? (
        <p className="error" data-testid="stub-server-error" role="alert">
          {remove.error instanceof StubConflict
            ? "this imposter changed since it was read — reload and try again"
            : remove.error instanceof ApiError
              ? remove.error.body
              : remove.error.message}
        </p>
      ) : null}
      {remove.data?.kind === "unobservable" ? <UnconfirmedNote reason={remove.data.reason} /> : null}
    </>
  );
}
