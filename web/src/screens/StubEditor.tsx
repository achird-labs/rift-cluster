import { type ReactNode, useEffect, useState } from "react";

import { ApiError, RawJsonBody } from "../api/client.ts";
import { StubConflict, useAddStub, useDeleteStub, usePutStub } from "../app/queries.ts";
import { CodeEditor } from "../components/CodeEditor.tsx";
import { UnconfirmedNote } from "../components/primitives.tsx";
import { type Finding, lintStub } from "../features/stubs/lint.ts";
import { STUB_FIELDS, type StubForm, project, render } from "../features/stubs/projection.ts";

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

/** What this panel is editing. An append has no id yet; the by-id routes cannot address it. */
export type StubTarget = { kind: "existing"; stubId: string } | { kind: "new" };

const PRETTY_INDENT = 2;

/** JSON as the editor shows it. Only used for text this console generated, never for stored text. */
function pretty(value: unknown): string {
  return JSON.stringify(value, null, PRETTY_INDENT);
}

/** A blank stub for the append case: the fields the form models, all empty. */
const NEW_STUB_TEXT = pretty({ responses: [{ is: { statusCode: 200 } }] });

type Parsed = { ok: true; value: unknown } | { ok: false; message: string };

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
  const [text, setText] = useState(() =>
    target.kind === "new" ? NEW_STUB_TEXT : pretty(original),
  );
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
  const projection = parsed.ok ? project(parsed.value) : null;

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
            type="button"
            disabled={effectiveRevision === null}
            onClick={() => send(effectiveRevision)}
          >
            Reapply my edit
          </button>
          <button
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

      {projection?.kind === "rawOnly" ? (
        <div className="degraded" data-testid="stub-raw-banner" role="status">
          <strong>Raw JSON only.</strong> This stub carries fields the form does not model, so
          editing it through the form would drop them. Unmodelled:{" "}
          {projection.unmodelledKeys.join(", ")}
        </div>
      ) : null}

      <div className="stub-panes">
        {projection?.kind === "form" ? (
          <StubFields
            form={projection.form}
            onChange={(next) => setText(pretty(render(next)))}
          />
        ) : null}
        <CodeEditor value={text} onChange={setText} label="Stub JSON" testId="stub-json" />
      </div>

      <LintPane findings={findings} />
      <BodyPreview value={parsed.ok ? parsed.value : null} />

      {effectiveRevision === null ? <NoRevisionNote /> : null}

      <nav className="pager">
        <button
          type="button"
          onClick={() => send(effectiveRevision)}
          disabled={!parsed.ok || effectiveRevision === null || write.isPending}
        >
          Save stub
        </button>
        <button type="button" onClick={onDone}>
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
  return (
    <div className="stub-form" data-testid="stub-form">
      {STUB_FIELDS.map((field) => {
        const value = form[field.key];
        return (
          <label key={field.key}>
            <span>{field.label}</span>
            <input
              type={field.kind === "number" ? "number" : "text"}
              value={value === null ? "" : String(value)}
              onChange={(event) => {
                const raw = event.target.value;
                /*
                 * An empty box means "this stub does not carry that field", so it becomes `null`
                 * and `render` emits no key — not an empty string, which would be a stub with a
                 * `path` predicate matching "".
                 */
                const next =
                  raw === ""
                    ? null
                    : field.kind === "number"
                      ? Number.isFinite(Number(raw))
                        ? Number(raw)
                        : null
                      : raw;
                onChange({ ...form, [field.key]: next });
              }}
            />
          </label>
        );
      })}
    </div>
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
  const projection = value === null ? null : project(value);
  const body =
    projection?.kind === "form" && projection.form.body !== null ? projection.form.body : null;
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
      <button
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
