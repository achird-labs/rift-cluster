import { type ReactNode, useId, useState } from "react";

import {
  FAULT_KINDS,
  type BehaviorModel,
  type WaitModel,
  canonicalFaultKind,
} from "./behaviors.ts";
import {
  DEFAULT_STATUS_CODE,
  faultFiresAsRift,
  faultIsArmed,
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

function isWaitKind(value: string): value is WaitModel["kind"] {
  return value === "none" || value === "fixed" || value === "range";
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
    onChange([
      ...items,
      previous === undefined
        ? next
        : // `statusText` rides along with `wrapped` for the same reason: a stub read from the engine
          // spells every status as a string, and appending a number-spelled response beside them is
          // the mixed document the carry exists to avoid.
          { ...next, wrapped: previous.wrapped, statusText: previous.statusText },
    ]);
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

  /**
   * A header name no existing row already uses.
   *
   * The row has to arrive with a name, and that is not a style choice. This editor's JSON text is
   * the single source of truth and the form is a projection of it: `onChange` recomposes the JSON,
   * `renderHeaders` drops any header whose name is empty (an empty header name is not a header),
   * and the form is then re-derived from that JSON. A row added blank was therefore gone before it
   * could be typed into — the button did nothing at all, forever.
   *
   * The sibling `PredicateBuilder` has no such problem because `renderClauseJson` writes blank
   * values through, so its new rows survive the round trip. Headers are the one builder whose blank
   * row is unrepresentable, so it is the one that needs a representable starting value.
   *
   * Suffixed on collision so clicking twice gives two editable rows rather than one row and a
   * silent no-op — the same defect in miniature.
   */
  const nextHeaderName = (): string => {
    const taken = new Set(item.headers.map((header) => header.name));
    if (!taken.has("X-New-Header")) return "X-New-Header";
    for (let n = 2; ; n += 1) {
      const candidate = `X-New-Header-${n}`;
      if (!taken.has(candidate)) return candidate;
    }
  };

  const addHeader = (): void => {
    onChange({
      ...item,
      headers: [...item.headers, { name: nextHeaderName(), value: "", multi: false }],
    });
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

  // The current delay, defaulted rather than read straight off `item.behaviors` — a response with
  // only `repeat` set (or no `_behaviors` key at all) still has to show the delay select at "None".
  const wait: WaitModel = item.behaviors?.wait ?? { kind: "none" };

  const onDelayKindChange = (kind: WaitModel["kind"]): void => {
    if (kind === "none") {
      if (item.behaviors === null) return;
      // Mirrors the repeat field's own emptiness: a delay-less, repeat-less behaviours object is
      // nothing the source ever had, so it is not invented here either.
      if (item.behaviors.repeat === null) {
        onChange({ ...item, behaviors: null });
        return;
      }
      onChange({
        ...item,
        behaviors: {
          ...item.behaviors,
          wait: { kind: "none" },
          order: item.behaviors.order.filter((key) => key !== "wait"),
        },
      });
      return;
    }
    const nextWait: WaitModel = kind === "fixed" ? { kind: "fixed", ms: 0 } : { kind: "range", min: 0, max: 0 };
    // A newly created `_behaviors` object always uses the `_behaviors` spelling — the canonical one,
    // per behaviors.ts — since there is no source spelling to preserve for a key that did not exist.
    const base: BehaviorModel =
      item.behaviors ?? { spelling: "_behaviors", order: [], wait: { kind: "none" }, repeat: null };
    onChange({
      ...item,
      behaviors: {
        ...base,
        wait: nextWait,
        order: base.order.includes("wait") ? base.order : [...base.order, "wait"],
      },
    });
  };

  // `raw === ""` is a no-op rather than a write, for the fixed/range fields below: `WaitModel` has
  // no "empty" ms/min/max, so treating a mid-clear keystroke as 0 would fight the operator typing a
  // fresh number over a cleared box. Skipping the write leaves the DOM's own value alone until the
  // next keystroke parses, the same trick `JsonBodyField` uses for its textarea.
  const onFixedMsChange = (raw: string): void => {
    const behaviors = item.behaviors;
    if (behaviors === null || behaviors.wait.kind !== "fixed" || raw === "") return;
    const parsed = Number(raw);
    if (!Number.isFinite(parsed)) return;
    onChange({ ...item, behaviors: { ...behaviors, wait: { kind: "fixed", ms: parsed } } });
  };

  const onRangeBoundChange = (bound: "min" | "max", raw: string): void => {
    const behaviors = item.behaviors;
    if (behaviors === null || behaviors.wait.kind !== "range" || raw === "") return;
    const parsed = Number(raw);
    if (!Number.isFinite(parsed)) return;
    const current = behaviors.wait;
    const nextWait: WaitModel =
      bound === "min"
        ? { kind: "range", min: parsed, max: current.max }
        : { kind: "range", min: current.min, max: parsed };
    onChange({ ...item, behaviors: { ...behaviors, wait: nextWait } });
  };

  const onRepeatChange = (raw: string): void => {
    if (raw === "") {
      if (item.behaviors === null) return;
      onChange({
        ...item,
        behaviors: {
          ...item.behaviors,
          repeat: null,
          order: item.behaviors.order.filter((key) => key !== "repeat"),
        },
      });
      return;
    }
    const parsed = Number(raw);
    if (!Number.isFinite(parsed)) return;
    const base: BehaviorModel =
      item.behaviors ?? { spelling: "_behaviors", order: [], wait: { kind: "none" }, repeat: null };
    onChange({
      ...item,
      behaviors: {
        ...base,
        repeat: parsed,
        order: base.order.includes("repeat") ? base.order : [...base.order, "repeat"],
      },
    });
  };

  // Which fault form fires here — see `faultFiresAsRift` in responses.ts for the engine's two
  // opposite dispatch tests. It turns on the `is` KEY, not on whether there is a body.
  const firesAsRift = faultFiresAsRift(item);

  const onFaultKindChange = (raw: string): void => {
    if (raw === "") {
      onChange({ ...item, fault: null });
      return;
    }
    // Changing the kind keeps whichever form is already in the document — downgrading a working
    // `_rift` fault to the dead top-level key would silently switch off config that was correct.
    const current = item.fault;
    if (current !== null && current.form === "riftObject") {
      onChange({ ...item, fault: { ...current, kind: raw } });
      return;
    }
    const form = firesAsRift ? "riftString" : "responseKey";
    onChange({ ...item, fault: { form, kind: raw } });
  };

  const onFaultProbabilityChange = (raw: string): void => {
    const fault = item.fault;
    if (fault === null) return;
    if (raw === "") {
      // Clearing a probability that was actually set is itself an edit — but it must land on a form
      // that still fires. Collapsing to the top-level `fault` key here would switch the fault off
      // on any response with a body, which is every response the panel is normally open on.
      if (fault.form === "riftObject") {
        const form = firesAsRift ? "riftString" : "responseKey";
        onChange({ ...item, fault: { form, kind: fault.kind } });
      }
      return;
    }
    const parsed = Number(raw);
    /*
     * Range-checked here, not just via the input's `min`/`max`: those attributes do not stop the
     * operator typing `1.5`, and the engine — and `parseRiftTcpFault` with it — refuses a
     * probability outside 0..1. Writing one anyway would produce a document the form itself can no
     * longer read, so the panel would vanish into raw-only mid-keystroke. Skipping the write leaves
     * the box showing what they typed until it becomes valid.
     */
    if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) return;
    onChange({ ...item, fault: { form: "riftObject", kind: fault.kind, probability: parsed } });
  };

  const faultSelectValue = item.fault === null ? "" : (canonicalFaultKind(item.fault.kind) ?? "");

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

      {/*
       * Collapsed by default: a plain status/headers/body stub — the common case — never shows a
       * delay or a fault to worry about. `<details>` rather than a toggle button of our own so the
       * open/closed state needs no model and no state hook of its own.
       */}
      <details className="response-chaos" data-testid={`response-chaos-${index}`}>
        <summary>Latency & faults</summary>

        <div className="field">
          <label htmlFor={`${uid}-delay-kind`}>Delay for response {n}</label>
          <select
            id={`${uid}-delay-kind`}
            value={wait.kind}
            onChange={(event) => {
              const value = event.target.value;
              if (isWaitKind(value)) onDelayKindChange(value);
            }}
          >
            <option value="none">None</option>
            <option value="fixed">Fixed</option>
            <option value="range">Random range</option>
          </select>
        </div>

        {wait.kind === "fixed" ? (
          <div className="field">
            <label htmlFor={`${uid}-delay-ms`}>Delay milliseconds for response {n}</label>
            <input
              id={`${uid}-delay-ms`}
              type="number"
              value={wait.ms}
              onChange={(event) => onFixedMsChange(event.target.value)}
            />
          </div>
        ) : null}

        {wait.kind === "range" ? (
          <>
            <div className="field">
              <label htmlFor={`${uid}-delay-min`}>Minimum delay milliseconds for response {n}</label>
              <input
                id={`${uid}-delay-min`}
                type="number"
                value={wait.min}
                onChange={(event) => onRangeBoundChange("min", event.target.value)}
              />
            </div>
            <div className="field">
              <label htmlFor={`${uid}-delay-max`}>Maximum delay milliseconds for response {n}</label>
              <input
                id={`${uid}-delay-max`}
                type="number"
                value={wait.max}
                onChange={(event) => onRangeBoundChange("max", event.target.value)}
              />
            </div>
          </>
        ) : null}

        <div className="field">
          <label htmlFor={`${uid}-repeat`}>Repeat count for response {n}</label>
          <input
            id={`${uid}-repeat`}
            type="number"
            value={item.behaviors?.repeat ?? ""}
            onChange={(event) => onRepeatChange(event.target.value)}
          />
        </div>
        <p className="muted">
          This response is served this many times before the cycle advances to the next response.
        </p>

        <div className="field">
          <label htmlFor={`${uid}-fault-kind`}>Fault for response {n}</label>
          <select
            id={`${uid}-fault-kind`}
            value={faultSelectValue}
            onChange={(event) => onFaultKindChange(event.target.value)}
          >
            <option value="">None</option>
            {FAULT_KINDS.map((kind) => (
              <option key={kind} value={kind}>
                {kind}
              </option>
            ))}
          </select>
        </div>

        {item.fault !== null ? (
          <div className="field">
            <label htmlFor={`${uid}-fault-probability`}>Fault probability for response {n}</label>
            <input
              id={`${uid}-fault-probability`}
              type="number"
              step={0.01}
              min={0}
              max={1}
              value={item.fault.form === "riftObject" ? item.fault.probability : ""}
              onChange={(event) => onFaultProbabilityChange(event.target.value)}
            />
          </div>
        ) : null}

        {item.fault !== null ? (
          <div className="banner warn" data-testid={`response-fault-warning-${index}`}>
            <span className="b-glyph" aria-hidden="true">
              ▲
            </span>
            {!faultIsArmed(item) ? (
              /*
               * A document that is already in the dead shape — a top-level `fault` beside an `is`.
               * The engine dispatches `is` first and never reaches the fault, and drops the key on
               * the next read. The panel must not show this as an armed fault: saying "this replaces
               * the response" about a key that does nothing is worse than saying nothing at all.
               * The picker never writes this shape; only a hand-authored stub can arrive in it.
               */
              <div>
                <strong>This fault never fires.</strong> The engine reaches a different branch for
                a response spelled like this one, so the fault is inert — and{" "}
                {item.fault.form === "responseKey"
                  ? "it is dropped the next time this imposter is read"
                  : "the status and body are erased on the next read too"}
                . Pick the fault again to rewrite it in the form that does fire here:{" "}
                <code>{firesAsRift ? "_rift.fault.tcp" : "fault"}</code>.
              </div>
            ) : (
              <div>
                This response returns a connection-level fault instead of the status, headers and
                body above — not in addition to them.
              </div>
            )}
          </div>
        ) : null}
      </details>
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
