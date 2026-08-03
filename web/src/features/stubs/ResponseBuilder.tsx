import { type ReactNode, useId, useState } from "react";

import {
  DEFAULT_STATUS_CODE,
  type ResponseBody,
  type ResponseHeader,
  type ResponseModel,
  blankResponse,
} from "./responses.ts";

/**
 * The response-list editor for a stub's `responses` (issue #248), sibling to `PredicateBuilder.tsx`
 * and mounted below it in `StubEditor`.
 *
 * Controlled, like its sibling: this component holds no copy of the response list itself, so there
 * is never a moment where what's on screen and what `onChange` last proposed can drift apart. The
 * one thing it DOES hold locally is the JSON body textarea's raw text — see `JsonBodyField` — because
 * that field's whole job is to survive keystrokes the model cannot represent yet.
 */

function replaceAt<T>(list: readonly T[], index: number, value: T): T[] {
  return list.map((item, i) => (i === index ? value : item));
}

function removeAt<T>(list: readonly T[], index: number): T[] {
  return list.filter((_, i) => i !== index);
}

function isBodyKind(value: string): value is ResponseBody["kind"] {
  return value === "absent" || value === "text" || value === "json";
}

/** The sentence `StubEditor`'s `Summary` shows — exported so it stays derived from one place. */
export function describeResponseList(items: ResponseModel[]): string {
  const first = items[0];
  if (first === undefined) return "carries no responses";
  const statusCode = first.statusCode ?? DEFAULT_STATUS_CODE;
  if (items.length === 1) return `answers ${statusCode}`;
  const rest = items.length - 1;
  return `answers ${statusCode}, then cycles through ${rest} more`;
}

export function ResponseBuilder({
  items,
  onChange,
}: {
  items: ResponseModel[];
  onChange: (items: ResponseModel[]) => void;
}): ReactNode {
  const addResponse = (): void => {
    /*
     * The new response inherits the last one's wrapper shape. Appending an `is`-wrapped response to
     * a flat, recorded stub would produce a document mixing both spellings — engine-fine, but it
     * undercuts on the exact documents (#251 exports) that the whole `wrapped` carry exists to keep
     * diff-clean.
     */
    const previous = items[items.length - 1];
    const next = blankResponse();
    onChange([...items, previous === undefined ? next : { ...next, wrapped: previous.wrapped }]);
  };

  const removeResponse = (index: number): void => {
    onChange(removeAt(items, index));
  };

  const moveResponse = (index: number, direction: -1 | 1): void => {
    const target = index + direction;
    const here = items[index];
    const there = items[target];
    if (here === undefined || there === undefined) return;
    const next = items.slice();
    next[index] = there;
    next[target] = here;
    onChange(next);
  };

  return (
    <fieldset className="response-builder" data-testid="response-builder">
      <legend>Responses</legend>
      {items.length === 0 ? (
        <p className="muted">
          No responses. A stub with an empty response list matches a request and then has nothing to
          answer with — add one, or the stub does nothing useful.
        </p>
      ) : null}
      {items.map((item, index) => (
        <ResponseCard
          key={index}
          index={index}
          item={item}
          isFirst={index === 0}
          isLast={index === items.length - 1}
          onChange={(next) => onChange(replaceAt(items, index, next))}
          onRemove={() => removeResponse(index)}
          onMoveUp={() => moveResponse(index, -1)}
          onMoveDown={() => moveResponse(index, 1)}
        />
      ))}
      <div className="response-actions">
        <button type="button" className="btn sm" onClick={addResponse}>
          Add response
        </button>
      </div>
      {items.length >= 2 ? (
        <p className="muted" data-testid="response-cycling-note">
          The engine answers with these responses in order on successive calls to this stub, then
          cycles back to the first — this is what "cycles" means above.
        </p>
      ) : null}
    </fieldset>
  );
}

function ResponseCard({
  index,
  item,
  isFirst,
  isLast,
  onChange,
  onRemove,
  onMoveUp,
  onMoveDown,
}: {
  index: number;
  item: ResponseModel;
  isFirst: boolean;
  isLast: boolean;
  onChange: (item: ResponseModel) => void;
  onRemove: () => void;
  onMoveUp: () => void;
  onMoveDown: () => void;
}): ReactNode {
  const uid = useId();
  const n = index + 1;

  const onStatusCodeChange = (raw: string): void => {
    if (raw === "") {
      onChange({ ...item, statusCode: null });
      return;
    }
    const parsed = Number(raw);
    // `isFinite`, not `isNaN`: `Number("1e999")` is `Infinity`, which passes an isNaN guard and then
    // serializes as JSON `null` — flipping the whole stub to raw-only on a status code the operator
    // can actually type. This is the check the flat form it replaced used.
    if (!Number.isFinite(parsed)) return;
    onChange({ ...item, statusCode: parsed });
  };

  const addHeader = (): void => {
    onChange({ ...item, headers: [...item.headers, { name: "", value: "", multi: false }] });
  };

  const removeHeader = (headerIndex: number): void => {
    onChange({ ...item, headers: removeAt(item.headers, headerIndex) });
  };

  const updateHeader = (headerIndex: number, next: ResponseHeader): void => {
    onChange({ ...item, headers: replaceAt(item.headers, headerIndex, next) });
  };

  const onBodyKindChange = (kind: ResponseBody["kind"]): void => {
    const body: ResponseBody =
      kind === "text"
        ? { kind: "text", text: "" }
        : kind === "json"
          ? { kind: "json", value: null }
          : { kind: "absent" };
    onChange({ ...item, body });
  };

  return (
    <div className="response-card" data-testid={`response-card-${index}`}>
      <div className="response-card-header">
        <span className="eyebrow">Response {n}</span>
        <button
          type="button"
          className="btn sm"
          aria-label={`Move response ${n} up`}
          disabled={isFirst}
          onClick={onMoveUp}
        >
          ↑
        </button>
        <button
          type="button"
          className="btn sm"
          aria-label={`Move response ${n} down`}
          disabled={isLast}
          onClick={onMoveDown}
        >
          ↓
        </button>
        <button
          type="button"
          className="btn sm danger"
          aria-label={`Remove response ${n}`}
          onClick={onRemove}
        >
          Remove
        </button>
      </div>

      <div className="field">
        <label htmlFor={`${uid}-status`}>Status code for response {n}</label>
        <input
          id={`${uid}-status`}
          type="number"
          value={item.statusCode ?? ""}
          onChange={(event) => onStatusCodeChange(event.target.value)}
        />
      </div>

      <div className="response-headers">
        {item.headers.length === 0 ? null : (
          /*
           * Column headings rather than a `<label>` per input. The sibling `PredicateBuilder` labels
           * each row's controls individually, but its rows differ from one another; these are a
           * uniform two-column table, where repeating "Name"/"Value" on every row is noise. The
           * per-row `aria-label`s below still give each input its own accessible name, so this is
           * additive for sighted operators rather than a substitute.
           */
          <div className="response-header-heads" aria-hidden="true">
            <span className="eyebrow">Name</span>
            <span className="eyebrow">Value</span>
            <span />
          </div>
        )}
        {item.headers.map((header, headerIndex) => (
          <div className="field-row" key={headerIndex} data-testid={`response-header-row-${index}-${headerIndex}`}>
            <input
              type="text"
              aria-label={`Header ${headerIndex + 1} name for response ${n}`}
              value={header.name}
              onChange={(event) => updateHeader(headerIndex, { ...header, name: event.target.value })}
            />
            <input
              type="text"
              aria-label={`Header ${headerIndex + 1} value for response ${n}`}
              // A header value is carried verbatim as `unknown` (see responses.ts) — recorded mocks
              // can hold a number or boolean here. Editing always writes a string, but the box must
              // still be able to DISPLAY whatever was read in.
              value={typeof header.value === "string" ? header.value : String(header.value)}
              onChange={(event) => updateHeader(headerIndex, { ...header, value: event.target.value })}
            />
            <button
              type="button"
              className="btn sm"
              aria-label={`Remove header ${headerIndex + 1} from response ${n}`}
              onClick={() => removeHeader(headerIndex)}
            >
              Remove
            </button>
          </div>
        ))}
        <button type="button" className="btn sm" onClick={addHeader}>
          Add header to response {n}
        </button>
      </div>

      <div className="field">
        <label htmlFor={`${uid}-body-type`}>Body type for response {n}</label>
        <select
          id={`${uid}-body-type`}
          value={item.body.kind}
          onChange={(event) => {
            const value = event.target.value;
            if (isBodyKind(value)) onBodyKindChange(value);
          }}
        >
          <option value="absent">None</option>
          <option value="text">Text</option>
          <option value="json">JSON</option>
        </select>
      </div>

      {item.body.kind === "text" ? (
        <div className="field">
          <label htmlFor={`${uid}-body`}>Body for response {n}</label>
          <textarea
            id={`${uid}-body`}
            value={item.body.text}
            onChange={(event) => onChange({ ...item, body: { kind: "text", text: event.target.value } })}
          />
        </div>
      ) : null}

      {item.body.kind === "json" ? (
        <JsonBodyField
          index={index}
          responseNumber={n}
          value={item.body.value}
          onChange={(value) => onChange({ ...item, body: { kind: "json", value } })}
        />
      ) : null}
    </div>
  );
}

/**
 * The JSON body textarea, split out because it is the one field in this form that needs state of
 * its own.
 *
 * The model holds a parsed `unknown`, pretty-printed for display — but `{"ok":` is the normal state
 * of a half-typed object, and re-deriving the textarea's text from the model on every render would
 * erase exactly that keystroke the moment it stopped parsing. So the raw text the operator is
 * mid-typing is kept here, separate from the model, and only promoted to a real `onChange` once it
 * parses.
 *
 * **The resync signal is what this field last EMITTED, never object identity.** `StubEditor` renders
 * the whole stub to JSON text and re-parses it on every edit, so the `value` arriving here is a
 * structurally equal but referentially new object on each keystroke. A `value !== lastValue` check
 * therefore fires on every render and wipes the in-progress text the moment it stops parsing —
 * reintroducing the exact bug this local state exists to prevent (see the round-trip test in
 * `ResponseBuilder.test.tsx`). Comparing against the serialization this field itself last proposed
 * distinguishes the two cases that matter: our own edit coming back (keep the operator's text,
 * whitespace and all) versus the body being changed from outside, in the raw JSON pane (adopt it).
 */
function JsonBodyField({
  index,
  responseNumber,
  value,
  onChange,
}: {
  index: number;
  responseNumber: number;
  value: unknown;
  onChange: (value: unknown) => void;
}): ReactNode {
  const uid = useId();
  const [localText, setLocalText] = useState<string | null>(null);
  /** The compact serialization this field last proposed — the resync signal. See the doc comment. */
  const [lastEmitted, setLastEmitted] = useState(() => JSON.stringify(value));

  const incoming = JSON.stringify(value);
  if (incoming !== lastEmitted) {
    setLastEmitted(incoming);
    setLocalText(null);
  }

  const text = localText ?? JSON.stringify(value, null, 2);

  let parseError: string | null = null;
  if (localText !== null) {
    try {
      JSON.parse(localText);
    } catch (error) {
      parseError = error instanceof Error ? error.message : "Invalid JSON";
    }
  }

  const handleChange = (next: string): void => {
    setLocalText(next);
    let parsed: unknown;
    try {
      parsed = JSON.parse(next);
    } catch {
      // Invalid JSON is the normal mid-edit state, not a value to write back. Writing `next` itself
      // would silently turn this response into a TEXT body — changing what the mock returns from an
      // object into a quoted string — and re-proposing the old value would churn the document on
      // every keystroke for no gain. So nothing is proposed at all: the last good value stays in the
      // document, and `parseError` above is how the operator finds out why.
      return;
    }
    setLastEmitted(JSON.stringify(parsed));
    onChange(parsed);
  };

  return (
    <div className="field">
      <label htmlFor={`${uid}-body`}>Body for response {responseNumber}</label>
      <textarea id={`${uid}-body`} value={text} onChange={(event) => handleChange(event.target.value)} />
      {parseError !== null ? (
        <p className="error" data-testid={`response-body-error-${index}`}>
          {parseError}
        </p>
      ) : null}
    </div>
  );
}
