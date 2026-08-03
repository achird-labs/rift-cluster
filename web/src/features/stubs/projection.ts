/**
 * The form ⟷ JSON projection for one stub (RFC-006 §12 Q2, issue #188).
 *
 * The whole module exists to make one guarantee: **the form never silently drops a key.** A stub
 * carrying anything the form cannot hold does not get a partly-populated form — it gets no form at
 * all, and every key the model does not cover is named so the operator can see what the form would
 * have lost. That is why `project` returns a two-case result rather than a `StubForm` with holes in
 * it: a partial form is precisely the shape that lets a save quietly delete a `behaviors` block.
 *
 * Pure, and deliberately free of React: the round-trip property (AC3) is a claim about these two
 * functions, and it is worth nothing if it can only be exercised through a component.
 *
 * Widening the modelled set is an edit to `STUB_FIELDS` and nothing else — neither `project` nor
 * `render` names a field.
 */

/** The fields the form models. Widening the form means adding an entry here. */
export type FieldKey = "id";

/** A path into the stub JSON: object keys as strings, array indices as numbers. */
export type JsonPath = readonly (string | number)[];

export type StubField = {
  readonly key: FieldKey;
  readonly label: string;
  /** Where this field lives in the rendered stub. */
  readonly at: JsonPath;
  /** The JSON type the field holds. A value of any other type is unmodelled, never coerced. */
  readonly kind: "string" | "number";
  /**
   * Presentation only — none of the three below reaches `project` or `render`.
   *
   * They live here because this table is the one place a field is described, and splitting "what
   * the field is" from "how it is typed" across two files is how the two drift.
   */
  /** Render a textarea. A response body on one line is the single worst thing about this form. */
  readonly multiline?: boolean;
  /** Offered in a datalist. Suggestions, never a closed set: the API constrains none of these. */
  readonly suggest?: readonly string[];
  /** One line under the input, for a field whose meaning is not obvious from its label. */
  readonly hint?: string;
};

/**
 * What is left of the flat field table: a stub's stable id.
 *
 * Both of the interesting subtrees have since earned their own projection, for the same reason and
 * by the same rule — a subtree that needs a richer unit than one row moves out rather than being
 * half-modelled here:
 *
 * - **`predicates`** moved to `predicates.ts` in issue #247. Its unit is a clause, not a field.
 * - **`responses`** moved to `responses.ts` in issue #248. Its unit is a response, and there are N
 *   of them: this table modelled `responses[0].is` and nothing else, so a second response, a second
 *   header, or a JSON-object body each sent the whole stub to raw-only. That was honest but
 *   crippling — cycling responses are how you mock "202 accepted, then 200 with a result".
 *
 * `walk`, below, treats both subtrees as out of scope for exactly that reason.
 *
 * Everything else — scenarios, spaces, top-level behaviors — is deliberately *out*, and lands in
 * raw-only rather than being half-modelled. Widening is demand-driven and costs one row.
 */
export const STUB_FIELDS = [
  {
    key: "id",
    label: "Id",
    at: ["id"],
    kind: "string",
    hint: "Optional. A stable name this console and the API address the stub by.",
  },
] as const satisfies readonly StubField[];

/** A stub expressed in the modelled set. `null` means "this stub does not carry that field". */
export type StubForm = {
  id: string | null;
};

export function blankForm(): StubForm {
  return { id: null };
}

/**
 * The result of reading a stub into the form.
 *
 * `rawOnly` is not an error: it is the honest answer for a stub richer than the form, and the
 * editor's raw-JSON mode handles it perfectly well. What it is *not* is a form with fields missing.
 */
export type Projection =
  | { kind: "form"; form: StubForm }
  | { kind: "rawOnly"; unmodelledKeys: string[] };

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** `predicates[0].equals.method`, and `headers["X-Trace"]` for a key that is not an identifier. */
function describePath(path: JsonPath): string {
  return path.reduce<string>((rendered, segment) => {
    if (typeof segment === "number") return `${rendered}[${segment}]`;
    if (/^[A-Za-z_$][\w$]*$/.test(segment)) {
      return rendered === "" ? segment : `${rendered}.${segment}`;
    }
    return `${rendered}[${JSON.stringify(segment)}]`;
  }, "");
}

function samePath(a: JsonPath, b: JsonPath): boolean {
  return a.length === b.length && a.every((segment, index) => segment === b[index]);
}

/** Does `prefix` lead towards `path`? Used to allow an empty container on the way to a field. */
function isPrefixOf(prefix: JsonPath, path: JsonPath): boolean {
  return prefix.length <= path.length && prefix.every((segment, index) => segment === path[index]);
}

function matchesKind(value: unknown, kind: StubField["kind"]): boolean {
  return kind === "number" ? typeof value === "number" : typeof value === "string";
}

/**
 * Walk the stub, deciding for every leaf whether the model covers it.
 *
 * A *leaf* is a scalar, or an empty container. Scalars must land exactly on a modelled field, with
 * the type that field holds. An empty container is admitted only when it is on the way to a modelled
 * field — `predicates: []` carries nothing the form could drop, but `behaviors: []` is a key the
 * form has no home for and a save through the form would erase it.
 */
function walk(
  value: unknown,
  path: JsonPath,
  form: StubForm,
  unmodelled: string[],
): void {
  // `predicates` and `responses` are wholly owned by the sibling `predicates.ts` (#247) and
  // `responses.ts` (#248) projections now — see `STUB_FIELDS`'s comment above. Descending into
  // either here would mean two projections disagreeing about the same subtree — this one refusing
  // shapes it was never taught, the other accepting them — so this walk stops at the key and
  // reports nothing under it. The editor composes all three verdicts; a stub is form-editable only
  // when every one of them agrees.
  if (path[0] === "predicates" || path[0] === "responses") return;

  if (isPlainObject(value)) {
    const entries = Object.entries(value);
    if (entries.length === 0) {
      if (!STUB_FIELDS.some((field) => isPrefixOf(path, field.at))) unmodelled.push(describePath(path));
      return;
    }
    for (const [key, child] of entries) walk(child, [...path, key], form, unmodelled);
    return;
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      if (!STUB_FIELDS.some((field) => isPrefixOf(path, field.at))) unmodelled.push(describePath(path));
      return;
    }
    value.forEach((child, index) => walk(child, [...path, index], form, unmodelled));
    return;
  }

  const field = STUB_FIELDS.find((candidate) => samePath(candidate.at, path));
  if (field === undefined || !matchesKind(value, field.kind)) {
    unmodelled.push(describePath(path));
    return;
  }
  // The `as` is the one place this file trades a cast for the table being data: `STUB_FIELDS`
  // pairs each key with its kind, and `matchesKind` has just checked the value against that kind.
  (form[field.key] as unknown) = value;
}

/**
 * Read a stub into the form — or refuse, naming every key that made it impossible.
 *
 * Refusal is total by design. There is no "form for the parts we understand": that shape saves the
 * modelled fields and drops the rest, which is the silent data loss AC2 exists to prevent.
 */
export function project(stub: unknown): Projection {
  if (!isPlainObject(stub)) {
    // Not a JSON object at all. The raw editor can still hold it (and the server will refuse it),
    // but there is no key to name, so the root is what the banner reports.
    return { kind: "rawOnly", unmodelledKeys: ["(the stub is not a JSON object)"] };
  }
  const form = blankForm();
  const unmodelled: string[] = [];
  walk(stub, [], form, unmodelled);
  return unmodelled.length === 0 ? { kind: "form", form } : { kind: "rawOnly", unmodelledKeys: unmodelled };
}

/** Write `value` at `path`, creating the objects and arrays it passes through. */
function setAt(root: Record<string, unknown>, path: JsonPath, value: unknown): void {
  let cursor: Record<string, unknown> | unknown[] = root;
  for (let i = 0; i < path.length - 1; i += 1) {
    const segment = path[i] as string | number;
    const next = path[i + 1] as string | number;
    const container = (cursor as Record<string | number, unknown>)[segment];
    if (container === undefined) {
      const created: Record<string, unknown> | unknown[] = typeof next === "number" ? [] : {};
      (cursor as Record<string | number, unknown>)[segment] = created;
      cursor = created;
    } else {
      cursor = container as Record<string, unknown> | unknown[];
    }
  }
  (cursor as Record<string | number, unknown>)[path[path.length - 1] as string | number] = value;
}

/**
 * Render a form back to stub JSON.
 *
 * A `null` field emits no key — not `null`, and not an empty container on the way to it. "This stub
 * has no path predicate" and "this stub has a path predicate whose value is null" are different
 * stubs, and only the first is what an empty form field means.
 */
export function render(form: StubForm): Record<string, unknown> {
  const stub: Record<string, unknown> = {};
  for (const field of STUB_FIELDS) {
    const value = form[field.key];
    if (value === null) continue;
    setAt(stub, field.at, value);
  }
  return stub;
}
