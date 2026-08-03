import { type ReactNode, useEffect, useId, useState } from "react";

import {
  PREDICATE_FIELDS,
  PREDICATE_OPERATORS,
  type PredicateClause,
  type PredicateEntry,
  type PredicateField,
  type PredicateItem,
  type PredicateOperator,
  type PredicateSelector,
  isPredicateField,
  isPredicateOperator,
} from "./predicates.ts";

/**
 * The row editor for a stub's `predicates` (issue #247), mounted above the response fields in
 * `StubEditor`.
 *
 * Each row is `[field] [operator] [value] [gear]`, matching one single-entry `PredicateClause` —
 * the shape this builder ever *writes*. A clause `predicates.ts` reads with more than one entry
 * (the two-field `equals` this console has always saved) is a shape this file only ever produced
 * historically through the old `method`/`path` rows; it is shown as a fixed summary rather than a
 * row this editor pretends to decompose, because there is no single field/operator pair that
 * describes it without inventing one.
 */

const OPERATOR_LABELS: Record<PredicateOperator, string> = {
  equals: "equals",
  deepEquals: "deep-equals",
  contains: "contains",
  startsWith: "starts with",
  endsWith: "ends with",
  matches: "matches (regex)",
  exists: "exists",
};

const FIELD_LABELS: Record<PredicateField, string> = {
  method: "Method",
  path: "Path",
  query: "Query",
  headers: "Headers",
  body: "Body",
};

function blankClause(): PredicateClause {
  return {
    operator: "equals",
    entries: [{ field: "path", key: null, value: "" }],
    caseSensitive: null,
    except: null,
    selector: null,
  };
}

function replaceAt<T>(list: readonly T[], index: number, value: T): T[] {
  return list.map((item, i) => (i === index ? value : item));
}

function removeAt<T>(list: readonly T[], index: number): T[] {
  return list.filter((_, i) => i !== index);
}

/**
 * `field entry`, or `field["key"] entry` — used for both the summary line and a group's read-out.
 *
 * Every qualifier the clause carries is named, because the summary's job is to say what the stub
 * matches and a narrower predicate described in broader words is worse than no description. A
 * `jsonpath`-narrowed body match reported as a whole-body match is the specific overclaim this
 * guards against — the editor's own note promises not to claim anything the fields do not say.
 */
function describeClause(clause: PredicateClause): string {
  const scope = clause.selector === null ? "" : ` at ${clause.selector.expression}`;
  const parts = clause.entries.map((entry) => {
    const label = entry.key === null ? FIELD_LABELS[entry.field] : `${FIELD_LABELS[entry.field]} "${entry.key}"`;
    if (clause.operator === "exists") {
      return `${label}${scope} ${entry.value === false ? "is absent" : "is present"}`;
    }
    return `${label}${scope} ${OPERATOR_LABELS[clause.operator]} ${JSON.stringify(entry.value)}`;
  });
  const joined = parts.join(" and ");
  const qualifiers = [
    clause.caseSensitive === true ? "case-sensitive" : null,
    clause.except === null ? null : `except ${clause.except}`,
  ].filter((qualifier): qualifier is string => qualifier !== null);
  return qualifiers.length === 0 ? joined : `${joined} (${qualifiers.join(", ")})`;
}

/** The sentence `StubEditor`'s `Summary` shows — exported so it stays derived from one place. */
export function describePredicates(items: PredicateItem[]): string {
  return items
    .map((item) => {
      if (item.kind === "clause") return describeClause(item.clause);
      if (item.op === "or") return `(${item.clauses.map(describeClause).join(" or ")})`;
      return `not (${item.clauses.map(describeClause).join(" and ")})`;
    })
    .join(" and ");
}

export function PredicateBuilder({
  items,
  onChange,
}: {
  items: PredicateItem[];
  onChange: (items: PredicateItem[]) => void;
}): ReactNode {
  const [selected, setSelected] = useState<ReadonlySet<number>>(new Set());

  /*
   * Selection is positional, and `items` can change under it from somewhere else entirely — the raw
   * JSON pane is bound per keystroke. A stale index does not point at "nothing", it points at a
   * *different* predicate, so acting on it would group or remove the wrong one. Clearing on any
   * change of the list identity is the cheap correct answer; the alternative is keying selection off
   * the clause objects, which buys nothing here because the list is short and re-selecting is one
   * click.
   */
  useEffect(() => {
    setSelected(new Set());
  }, [items]);

  const toggleSelected = (index: number): void => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(index)) next.delete(index);
      else next.add(index);
      return next;
    });
  };

  const addClause = (): void => {
    onChange([...items, { kind: "clause", clause: blankClause() }]);
  };

  const groupSelected = (op: "or" | "not"): void => {
    const indices = [...selected].sort((a, b) => a - b);
    if (indices.length < (op === "not" ? 1 : 2)) return;
    const clauses: PredicateClause[] = [];
    for (const index of indices) {
      const item = items[index];
      if (item !== undefined && item.kind === "clause") clauses.push(item.clause);
    }
    /*
     * All or nothing. `remaining` drops every selected index, but only `clause` items can become
     * members of the new group — so a selection that includes an existing group used to remove that
     * group and never re-add it, deleting a whole predicate from the document *before*
     * `renderPredicates` ran, where the projection could never see it.
     *
     * Nesting a group inside a group is out of scope (it would be the second level this builder
     * deliberately refuses), so the honest answer is to refuse the operation rather than apply a
     * part of it.
     */
    if (clauses.length !== indices.length) return;
    const remaining = items.filter((_, i) => !indices.includes(i));
    onChange([...remaining, { kind: "group", op, clauses }]);
    setSelected(new Set());
  };

  const removeItem = (index: number): void => {
    onChange(removeAt(items, index));
    setSelected(new Set());
  };

  const ungroup = (index: number): void => {
    const item = items[index];
    if (item === undefined || item.kind !== "group") return;
    const expanded: PredicateItem[] = item.clauses.map((clause) => ({ kind: "clause", clause }));
    onChange([...items.slice(0, index), ...expanded, ...items.slice(index + 1)]);
    setSelected(new Set());
  };

  return (
    <fieldset className="predicate-builder" data-testid="predicate-builder">
      <legend>Predicates</legend>
      {items.length === 0 ? (
        <p className="muted">No predicates. This stub matches every request that reaches it.</p>
      ) : null}
      {items.map((item, index) =>
        item.kind === "clause" ? (
          <ClauseRow
            key={index}
            index={index}
            clause={item.clause}
            selected={selected.has(index)}
            onToggleSelected={() => toggleSelected(index)}
            onChange={(clause) => onChange(replaceAt(items, index, { kind: "clause", clause }))}
            onRemove={() => removeItem(index)}
          />
        ) : (
          <GroupBlock
            key={index}
            item={item}
            onUngroup={() => ungroup(index)}
            onRemove={() => removeItem(index)}
          />
        ),
      )}
      <div className="predicate-actions">
        <button type="button" className="btn sm" onClick={addClause}>
          Add predicate
        </button>
        <button
          type="button"
          className="btn sm"
          disabled={selected.size < 2}
          onClick={() => groupSelected("or")}
        >
          Group selected as OR
        </button>
        <button
          type="button"
          className="btn sm"
          disabled={selected.size < 1}
          onClick={() => groupSelected("not")}
        >
          Group selected as NOT
        </button>
      </div>
    </fieldset>
  );
}

/**
 * A group, with the two ways out of it stated rather than implied.
 *
 * Ungrouping is **not** a neutral unwrap: the top-level list is an implicit `and`, so an `or` of
 * two path clauses becomes "both must match" — a stub that matches nothing — and a `not` becomes
 * its own opposite. It is also the only route to editing a clause inside a group, so an operator
 * reaching for it to fix a typo would invert what the stub matches with nothing on screen saying
 * so. Hence: it says what it will do, and takes a second click. `Remove group` exists so that
 * confirming a meaning change is not the only way to get rid of one.
 */
function GroupBlock({
  item,
  onUngroup,
  onRemove,
}: {
  item: Extract<PredicateItem, { kind: "group" }>;
  onUngroup: () => void;
  onRemove: () => void;
}): ReactNode {
  const [confirming, setConfirming] = useState(false);
  return (
    <div className="predicate-group" data-testid={`predicate-group-${item.op}`}>
      <span className="eyebrow">{item.op === "or" ? "Any of" : "None of"}</span>
      <ul>
        {item.clauses.map((clause, i) => (
          <li key={i}>{describeClause(clause)}</li>
        ))}
      </ul>
      <div className="acts">
        {confirming ? (
          <button
            type="button"
            className="btn sm danger"
            data-testid="ungroup-confirm"
            onClick={() => {
              setConfirming(false);
              onUngroup();
            }}
          >
            {item.op === "or"
              ? "Ungroup — every clause will have to match, not just one"
              : "Ungroup — these will have to match, instead of being excluded"}
          </button>
        ) : (
          <button type="button" className="btn sm" onClick={() => setConfirming(true)}>
            Ungroup
          </button>
        )}
        <button type="button" className="btn sm danger" onClick={onRemove}>
          Remove group
        </button>
      </div>
    </div>
  );
}

/**
 * One clause row. Reads `entries[0]` only — the shape this row ever writes back — and a clause
 * with more than one entry (the legacy two-field `equals`) is shown as a fixed summary instead, so
 * this editor never silently drops the second entry by only ever writing the first.
 */
function ClauseRow({
  index,
  clause,
  selected,
  onToggleSelected,
  onChange,
  onRemove,
}: {
  index: number;
  clause: PredicateClause;
  selected: boolean;
  onToggleSelected: () => void;
  onChange: (clause: PredicateClause) => void;
  onRemove: () => void;
}): ReactNode {
  const [expanded, setExpanded] = useState(false);
  const uid = useId();
  const entry = clause.entries[0];

  if (entry === undefined || clause.entries.length > 1) {
    return (
      <div className="predicate-row" data-testid="predicate-row-readonly">
        <span>{describeClause(clause)}</span>
        <label className="check">
          <input
            type="checkbox"
            checked={selected}
            onChange={onToggleSelected}
            aria-label={`Select predicate ${index + 1} for grouping`}
          />
          Select
        </label>
        <button type="button" className="btn sm" onClick={onRemove} aria-label={`Remove predicate ${index + 1}`}>
          Remove
        </button>
      </div>
    );
  }

  const setEntry = (next: PredicateEntry): void => onChange({ ...clause, entries: [next] });

  const onFieldChange = (field: PredicateField): void => {
    /*
     * `null`, never `""`. `renderClauseJson` only treats `null` as "no key", so an empty string
     * rendered as `{"equals":{"query":{"":"/x"}}}` — a predicate the engine can never match. Worse,
     * the *same* visually-empty Key box produced two different documents depending on whether it
     * had ever been typed into, because the input maps `""` back to `null`. One representation.
     */
    const key = field === "query" || field === "headers" ? entry.key : null;
    setEntry({ ...entry, field, key });
  };

  const onOperatorChange = (operator: PredicateOperator): void => {
    const value =
      operator === "exists"
        ? typeof entry.value === "boolean"
          ? entry.value
          : true
        : typeof entry.value === "boolean"
          ? ""
          : entry.value;
    onChange({ ...clause, operator, entries: [{ ...entry, value }] });
  };

  return (
    <div className="predicate-row" data-testid="predicate-row">
      <label className="check">
        <input
          type="checkbox"
          checked={selected}
          onChange={onToggleSelected}
          aria-label={`Select predicate ${index + 1} for grouping`}
        />
        Select
      </label>

      <div className="field-row">
        <div className="field">
          <label htmlFor={`${uid}-field`}>Field</label>
          <select
            id={`${uid}-field`}
            value={entry.field}
            onChange={(event) => {
              const value = event.target.value;
              if (isPredicateField(value)) onFieldChange(value);
            }}
          >
            {PREDICATE_FIELDS.map((field) => (
              <option key={field} value={field}>
                {FIELD_LABELS[field]}
              </option>
            ))}
          </select>
        </div>

        <div className="field">
          <label htmlFor={`${uid}-operator`}>Operator</label>
          <select
            id={`${uid}-operator`}
            value={clause.operator}
            onChange={(event) => {
              const value = event.target.value;
              if (isPredicateOperator(value)) onOperatorChange(value);
            }}
          >
            {PREDICATE_OPERATORS.map((operator) => (
              <option key={operator} value={operator}>
                {OPERATOR_LABELS[operator]}
              </option>
            ))}
          </select>
        </div>

        {entry.field === "query" || entry.field === "headers" ? (
          <div className="field">
            <label htmlFor={`${uid}-key`}>Key</label>
            <input
              id={`${uid}-key`}
              type="text"
              value={entry.key ?? ""}
              onChange={(event) => setEntry({ ...entry, key: event.target.value === "" ? null : event.target.value })}
            />
            {entry.key === null ? (
              <p className="error" data-testid="predicate-key-required">
                Name the {entry.field === "query" ? "query parameter" : "header"} this matches on. A{" "}
                {FIELD_LABELS[entry.field].toLowerCase()} clause with no key matches nothing.
              </p>
            ) : null}
          </div>
        ) : null}

        {clause.operator === "exists" ? (
          <div className="field">
            <label htmlFor={`${uid}-value`}>Value</label>
            <select
              id={`${uid}-value`}
              value={entry.value === false ? "false" : "true"}
              onChange={(event) => setEntry({ ...entry, value: event.target.value === "true" })}
            >
              <option value="true">Present</option>
              <option value="false">Absent</option>
            </select>
          </div>
        ) : (
          <div className="field">
            <label htmlFor={`${uid}-value`}>Value</label>
            <input
              id={`${uid}-value`}
              type="text"
              value={typeof entry.value === "string" ? entry.value : String(entry.value)}
              onChange={(event) => setEntry({ ...entry, value: event.target.value })}
            />
          </div>
        )}

        <button
          type="button"
          className="btn icon"
          aria-expanded={expanded}
          aria-label={`More options for predicate ${index + 1}`}
          onClick={() => setExpanded((current) => !current)}
        >
          ⚙
        </button>
      </div>

      {expanded ? (
        <div className="predicate-row-options field-row">
          <div className="field">
            <label htmlFor={`${uid}-case`}>Case sensitivity</label>
            <select
              id={`${uid}-case`}
              value={clause.caseSensitive === null ? "default" : clause.caseSensitive ? "sensitive" : "insensitive"}
              onChange={(event) => {
                const value = event.target.value;
                const caseSensitive = value === "default" ? null : value === "sensitive";
                onChange({ ...clause, caseSensitive });
              }}
            >
              <option value="default">Default (case-insensitive)</option>
              <option value="sensitive">Case sensitive</option>
              <option value="insensitive">Explicitly case-insensitive</option>
            </select>
          </div>
          <div className="field">
            <label htmlFor={`${uid}-except`}>Except (regex stripped before matching)</label>
            <input
              id={`${uid}-except`}
              type="text"
              value={clause.except ?? ""}
              onChange={(event) =>
                onChange({ ...clause, except: event.target.value === "" ? null : event.target.value })
              }
            />
          </div>
          {entry.field === "body" ? (
            <SelectorFields uid={uid} selector={clause.selector} onChange={(selector) => onChange({ ...clause, selector })} />
          ) : null}
        </div>
      ) : null}

      <button type="button" className="btn sm" onClick={onRemove} aria-label={`Remove predicate ${index + 1}`}>
        Remove
      </button>
    </div>
  );
}

/**
 * The optional JSONPath/XPath selector on a body predicate. Supports one namespace pair through
 * the UI — the common case (see the mountebank docs' own XPath example) — while `predicates.ts`
 * itself reads and preserves however many a hand-written document carries; only the *builder*
 * narrows, the same "builder simplifies, projection stays general" split `entries` makes.
 */
function SelectorFields({
  uid,
  selector,
  onChange,
}: {
  uid: string;
  selector: PredicateSelector | null;
  onChange: (selector: PredicateSelector | null) => void;
}): ReactNode {
  return (
    <div className="predicate-selector">
      <label className="check" htmlFor={`${uid}-selector-enabled`}>
        <input
          id={`${uid}-selector-enabled`}
          type="checkbox"
          checked={selector !== null}
          onChange={(event) =>
            onChange(event.target.checked ? { kind: "jsonpath", expression: "", ns: null } : null)
          }
        />
        Match a specific part of the body (JSONPath / XPath)
      </label>
      {selector === null ? null : (
        <div className="field-row">
          <div className="field">
            <label htmlFor={`${uid}-selector-kind`}>Selector kind</label>
            <select
              id={`${uid}-selector-kind`}
              value={selector.kind}
              onChange={(event) => {
                const value = event.target.value;
                if (value === "jsonpath" || value === "xpath") onChange({ ...selector, kind: value });
              }}
            >
              <option value="jsonpath">JSONPath</option>
              <option value="xpath">XPath</option>
            </select>
          </div>
          <div className="field">
            <label htmlFor={`${uid}-selector-expr`}>Selector expression</label>
            <input
              id={`${uid}-selector-expr`}
              type="text"
              value={selector.expression}
              onChange={(event) => onChange({ ...selector, expression: event.target.value })}
            />
          </div>
          {selector.kind === "xpath" ? (
            <NamespaceFields uid={uid} ns={selector.ns} onChange={(ns) => onChange({ ...selector, ns })} />
          ) : null}
        </div>
      )}
    </div>
  );
}

/** A single namespace prefix/URI pair — see `SelectorFields`'s comment on why only one. */
function NamespaceFields({
  uid,
  ns,
  onChange,
}: {
  uid: string;
  ns: Record<string, string> | null;
  onChange: (ns: Record<string, string> | null) => void;
}): ReactNode {
  const [prefix, uri] = Object.entries(ns ?? {})[0] ?? ["", ""];

  const update = (nextPrefix: string, nextUri: string): void => {
    onChange(nextPrefix === "" && nextUri === "" ? null : { [nextPrefix]: nextUri });
  };

  return (
    <>
      <div className="field">
        <label htmlFor={`${uid}-ns-prefix`}>Namespace prefix</label>
        <input
          id={`${uid}-ns-prefix`}
          type="text"
          value={prefix}
          onChange={(event) => update(event.target.value, uri)}
        />
      </div>
      <div className="field">
        <label htmlFor={`${uid}-ns-uri`}>Namespace URI</label>
        <input id={`${uid}-ns-uri`} type="text" value={uri} onChange={(event) => update(prefix, event.target.value)} />
      </div>
    </>
  );
}
