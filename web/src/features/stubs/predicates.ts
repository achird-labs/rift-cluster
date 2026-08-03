/**
 * The predicate form ⟷ JSON projection for one stub (issue #247), sibling to `projection.ts`.
 *
 * Same guarantee as the rest of the stub editor: this either understands the whole `predicates`
 * array or refuses the whole stub, naming every key it could not place. There is no form with a
 * predicate quietly missing.
 *
 * **The load-bearing decision: a clause, not a row, is the unit.** The obvious model — one
 * predicate object holds exactly one field — is wrong, because it is not what this console has
 * ever written. `STUB_FIELDS` used to render `{"equals":{"method":"GET","path":"/x"}}`: one
 * object, two fields. A one-entry-per-clause model would either refuse every stub saved that way
 * or silently split it into two predicate objects on the next save — the exact data loss this
 * whole module exists to prevent. A `PredicateClause` carries an `entries` array instead, so it
 * reads and re-renders a two-field `equals` unchanged. The **builder UI** (`PredicateBuilder.tsx`)
 * only ever constructs single-entry clauses; a multi-entry clause is a shape this file can *read*,
 * not one the row editor produces — see `predicates.test.ts`'s
 * "keeps reading the two-field equals this console has always written".
 *
 * Pure and free of React, for the same reason `projection.ts` is: the round-trip property is a
 * claim about `projectPredicates` and `renderPredicates` alone.
 */

export const PREDICATE_OPERATORS = [
  "equals",
  "deepEquals",
  "contains",
  "startsWith",
  "endsWith",
  "matches",
  "exists",
] as const;

export const PREDICATE_FIELDS = ["method", "path", "query", "headers", "body"] as const;

export type PredicateOperator = (typeof PREDICATE_OPERATORS)[number];
export type PredicateField = (typeof PREDICATE_FIELDS)[number];

/** Is `value` one of `PREDICATE_OPERATORS`? A type guard, not a cast, so callers narrow for free. */
export function isPredicateOperator(value: string): value is PredicateOperator {
  return (PREDICATE_OPERATORS as readonly string[]).includes(value);
}

/** Is `value` one of `PREDICATE_FIELDS`? A type guard, not a cast, so callers narrow for free. */
export function isPredicateField(value: string): value is PredicateField {
  return (PREDICATE_FIELDS as readonly string[]).includes(value);
}

/**
 * One `field`/`value` pair inside a clause. `key` names the sub-field for `query`/`headers`
 * (`headers["Authorization"]`); it is `null` for `method`, `path`, and `body`, which have no
 * sub-fields to name.
 */
export type PredicateEntry = { field: PredicateField; key: string | null; value: unknown };

export type PredicateSelector = {
  kind: "jsonpath" | "xpath";
  expression: string;
  ns: Record<string, string> | null;
};

export type PredicateClause = {
  operator: PredicateOperator;
  /** At least one, by construction: nothing places a clause with zero entries onto the array. */
  entries: PredicateEntry[];
  /** `null` means the key is ABSENT from the JSON, not that it is `false` — see `renderPredicates`. */
  caseSensitive: boolean | null;
  except: string | null;
  selector: PredicateSelector | null;
};

export type PredicateItem =
  | { kind: "clause"; clause: PredicateClause }
  | { kind: "group"; op: "or" | "not"; clauses: PredicateClause[] };

export type PredicateProjection =
  | { kind: "predicates"; items: PredicateItem[] }
  | { kind: "rawOnly"; unmodelledKeys: string[] };

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// ---------------------------------------------------------------------------------------------
// render: PredicateItem[] -> JSON
// ---------------------------------------------------------------------------------------------

/**
 * One clause's JSON, e.g. `{ jsonpath: { selector }, equals: { path: "/a" }, caseSensitive: false }`.
 *
 * Key order matters only for the property-based round-trip (it compares `JSON.stringify` output),
 * and that property compares two calls of this *same* function, so any deterministic order works.
 * The order chosen here — selector, then operator, then options — matches the engine's own
 * examples, which is a bonus, not a requirement.
 */
function renderClauseJson(clause: PredicateClause): Record<string, unknown> {
  const body: Record<string, unknown> = {};
  for (const entry of clause.entries) {
    if (entry.key === null) {
      body[entry.field] = entry.value;
      continue;
    }
    // A second entry for the same keyed field (two headers, say) accumulates into one object —
    // `{headers: {A: 1, B: 2}}`, not two competing `headers` keys.
    const existing = body[entry.field];
    const nested = isPlainObject(existing) ? { ...existing } : {};
    nested[entry.key] = entry.value;
    body[entry.field] = nested;
  }

  const rendered: Record<string, unknown> = {};
  if (clause.selector !== null) {
    const selectorBody: Record<string, unknown> = { selector: clause.selector.expression };
    if (clause.selector.ns !== null) selectorBody.ns = clause.selector.ns;
    rendered[clause.selector.kind] = selectorBody;
  }
  rendered[clause.operator] = body;
  // `null` omits the key entirely — an absent `caseSensitive` and an explicit `false` are
  // different documents, and only the operator's own default applies to the first.
  if (clause.caseSensitive !== null) rendered.caseSensitive = clause.caseSensitive;
  if (clause.except !== null) rendered.except = clause.except;
  return rendered;
}

/**
 * Render the builder's items back to the `predicates` array's JSON.
 *
 * `not` is documented as taking a single predicate object, never an array. A `not` group with more
 * than one clause therefore cannot render as `{not: [...]}`; it renders as `{not: {and: [...]}}`
 * instead — `and` takes an array, and wrapping it in `not` negates the conjunction, which is the
 * only reading "not all of these" has. A single-clause `not` skips the `and` wrapper and renders
 * the clause directly, matching the engine's own `not` examples exactly.
 */
export function renderPredicates(items: PredicateItem[]): unknown[] {
  return items.map((item) => {
    if (item.kind === "clause") return renderClauseJson(item.clause);
    if (item.op === "or") return { or: item.clauses.map(renderClauseJson) };

    const rendered = item.clauses.map(renderClauseJson);
    const only = rendered[0];
    if (rendered.length === 1 && only !== undefined) return { not: only };
    return { not: { and: rendered } };
  });
}

// ---------------------------------------------------------------------------------------------
// project: JSON -> PredicateItem[] (or a named refusal)
// ---------------------------------------------------------------------------------------------

type ParseResult<T> = { ok: true; value: T } | { ok: false; issues: string[] };

/**
 * Parse one predicate object — `{operator: {...}, caseSensitive?, except?, jsonpath|xpath?}` — into
 * a clause, or list every key that made it impossible.
 */
function parseClause(value: unknown, path: string): ParseResult<PredicateClause> {
  if (!isPlainObject(value)) return { ok: false, issues: [path] };
  const keys = Object.keys(value);

  const operatorKeys = keys.filter(isPredicateOperator);
  const selectorKeys = keys.filter((key): key is "jsonpath" | "xpath" => key === "jsonpath" || key === "xpath");

  if (operatorKeys.length === 0) {
    // Nothing here is a recognised operator. Name whatever isn't an option key (`caseSensitive`,
    // `except`) or a selector key, so an unknown operator like `soundsLike` is named explicitly —
    // and if there is truly nothing else odd (the nested-xpath variant, where the only top-level
    // key is `xpath` itself), fall back to naming the object as a whole.
    const suspects = keys.filter(
      (key) => key !== "caseSensitive" && key !== "except" && !selectorKeys.includes(key as "jsonpath" | "xpath"),
    );
    return { ok: false, issues: suspects.length > 0 ? suspects.map((key) => `${path}.${key}`) : [path] };
  }
  if (operatorKeys.length > 1) return { ok: false, issues: [path] };
  const [operator] = operatorKeys;
  if (operator === undefined) return { ok: false, issues: [path] };
  if (selectorKeys.length > 1) return { ok: false, issues: [path] };

  const allowedTopKeys = new Set<string>([operator, "caseSensitive", "except", ...selectorKeys]);
  const extraTopKeys = keys.filter((key) => !allowedTopKeys.has(key));
  if (extraTopKeys.length > 0) return { ok: false, issues: extraTopKeys.map((key) => `${path}.${key}`) };

  const bodyValue = value[operator];
  if (!isPlainObject(bodyValue)) return { ok: false, issues: [`${path}.${operator}`] };

  const entries: PredicateEntry[] = [];
  const issues: string[] = [];
  for (const [key, fieldValue] of Object.entries(bodyValue)) {
    if (!isPredicateField(key)) {
      issues.push(`${path}.${operator}.${key}`);
      continue;
    }
    if (key === "body") {
      // The body's value is opaque JSON — a string, a number, or a whole object per the engine's
      // own `equals`/`contains` examples — so it is carried as-is, never decomposed by key the way
      // `query`/`headers` are. A single-key object here still round-trips: nesting one key under a
      // field and assigning that field the object directly render to the identical JSON, so this
      // does not have to distinguish the two to stay lossless.
      entries.push({ field: key, key: null, value: fieldValue });
      continue;
    }
    // method, path, query, headers: an object value nests one entry per own key (`{headers:
    // {Authorization: "x"}}` -> one entry keyed `Authorization`); anything else is one entry whose
    // key is `null`. Uniform across all four, because the builder lets any of them carry a key (a
    // `method`/`path` predicate is never written keyed, but nothing in this shape forbids reading
    // one back if some other tool wrote it that way).
    if (isPlainObject(fieldValue)) {
      const subEntries = Object.entries(fieldValue);
      if (subEntries.length === 0) {
        issues.push(`${path}.${operator}.${key}`);
        continue;
      }
      for (const [subKey, subValue] of subEntries) {
        if (isPlainObject(subValue) || Array.isArray(subValue)) {
          issues.push(`${path}.${operator}.${key}.${subKey}`);
          continue;
        }
        entries.push({ field: key, key: subKey, value: subValue });
      }
      continue;
    }
    if (Array.isArray(fieldValue)) {
      issues.push(`${path}.${operator}.${key}`);
      continue;
    }
    entries.push({ field: key, key: null, value: fieldValue });
  }
  if (issues.length > 0) return { ok: false, issues };
  if (entries.length === 0) return { ok: false, issues: [`${path}.${operator}`] };

  let selector: PredicateSelector | null = null;
  if (selectorKeys.length === 1) {
    const [selectorKind] = selectorKeys;
    if (selectorKind === undefined) return { ok: false, issues: [path] };
    const selectorValue = value[selectorKind];
    if (!isPlainObject(selectorValue)) return { ok: false, issues: [`${path}.${selectorKind}`] };
    const selectorKeySet = Object.keys(selectorValue);
    const extraSelectorKeys = selectorKeySet.filter((key) => key !== "selector" && key !== "ns");
    if (extraSelectorKeys.length > 0) {
      // The nested-xpath variant (`{xpath: {selector, equals: "admin"}}`) lands here: `equals` is
      // an extra key inside the selector object, ambiguous against the sibling form this builder
      // claims, so it is refused rather than guessed at.
      return { ok: false, issues: extraSelectorKeys.map((key) => `${path}.${selectorKind}.${key}`) };
    }
    const expression = selectorValue.selector;
    if (typeof expression !== "string") return { ok: false, issues: [`${path}.${selectorKind}.selector`] };
    let ns: Record<string, string> | null = null;
    if ("ns" in selectorValue) {
      const nsValue = selectorValue.ns;
      if (!isPlainObject(nsValue) || Object.values(nsValue).some((v) => typeof v !== "string")) {
        return { ok: false, issues: [`${path}.${selectorKind}.ns`] };
      }
      ns = Object.fromEntries(Object.entries(nsValue)) as Record<string, string>;
    }
    selector = { kind: selectorKind, expression, ns };
  }

  let caseSensitive: boolean | null = null;
  if ("caseSensitive" in value) {
    const raw = value.caseSensitive;
    if (typeof raw !== "boolean") return { ok: false, issues: [`${path}.caseSensitive`] };
    caseSensitive = raw;
  }

  let except: string | null = null;
  if ("except" in value) {
    const raw = value.except;
    if (typeof raw !== "string") return { ok: false, issues: [`${path}.except`] };
    except = raw;
  }

  return { ok: true, value: { operator, entries, caseSensitive, except, selector } };
}

/** Parse every element of an `or`/`not`-wrapped array as a plain clause — no nested groups allowed. */
function parseClauseArray(raw: unknown, prefix: string): ParseResult<PredicateClause[]> {
  if (!Array.isArray(raw)) return { ok: false, issues: [prefix] };
  const clauses: PredicateClause[] = [];
  const issues: string[] = [];
  raw.forEach((element, index) => {
    const result = parseClause(element, `${prefix}[${index}]`);
    if (result.ok) clauses.push(result.value);
    else issues.push(...result.issues);
  });
  if (issues.length > 0) return { ok: false, issues };
  return { ok: true, value: clauses };
}

function parseNot(value: unknown, path: string): ParseResult<PredicateClause[]> {
  if (!isPlainObject(value)) return { ok: false, issues: [path] };
  const keys = Object.keys(value);
  if (keys.length === 1 && keys[0] === "and") {
    return parseClauseArray(value.and, `${path}.not.and`);
  }
  const single = parseClause(value, `${path}.not`);
  if (!single.ok) return single;
  return { ok: true, value: [single.value] };
}

/** Parse one element of the top-level `predicates` array: a clause, an `or` group, or a `not` group. */
function parseItem(value: unknown, index: number): ParseResult<PredicateItem> {
  const path = `predicates[${index}]`;
  if (!isPlainObject(value)) return { ok: false, issues: [path] };
  const keys = Object.keys(value);

  if (keys.length === 1 && keys[0] === "or") {
    const result = parseClauseArray(value.or, `${path}.or`);
    if (!result.ok) return result;
    return { ok: true, value: { kind: "group", op: "or", clauses: result.value } };
  }
  if (keys.length === 1 && keys[0] === "not") {
    const result = parseNot(value.not, path);
    if (!result.ok) return result;
    return { ok: true, value: { kind: "group", op: "not", clauses: result.value } };
  }
  if (keys.length === 1 && keys[0] === "and") {
    // A real mountebank shape, but not one this builder's group control ever writes (it only knows
    // `or`/`not`) — narrowing it to one of those would misrepresent what the stub actually matches.
    return { ok: false, issues: [path] };
  }

  const result = parseClause(value, path);
  if (!result.ok) return result;
  return { ok: true, value: { kind: "clause", clause: result.value } };
}

/**
 * Read a stub's `predicates` array into the builder's model — or refuse, naming every key that
 * made it impossible. An absent `predicates` key is an empty set, not a refusal: nothing about a
 * stub with no predicates is unmodelled, it just matches everything.
 */
export function projectPredicates(stub: unknown): PredicateProjection {
  const predicatesValue = isPlainObject(stub) ? stub.predicates : undefined;
  if (predicatesValue === undefined) return { kind: "predicates", items: [] };
  if (!Array.isArray(predicatesValue)) return { kind: "rawOnly", unmodelledKeys: ["predicates"] };

  const items: PredicateItem[] = [];
  const unmodelled: string[] = [];
  predicatesValue.forEach((raw, index) => {
    const result = parseItem(raw, index);
    if (result.ok) items.push(result.value);
    else unmodelled.push(...result.issues);
  });
  if (unmodelled.length > 0) return { kind: "rawOnly", unmodelledKeys: unmodelled };
  return { kind: "predicates", items };
}
