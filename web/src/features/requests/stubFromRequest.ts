/**
 * Derive a stub from a request the journal recorded (issue #250).
 *
 * The request log is the moment a developer knows exactly which stub they need — they are looking
 * at the request that did not match. This turns that into a starting document.
 *
 * **The journal records requests, not responses.** `RecordedRequest` carries `requestFrom`,
 * `method`, `path`, `query`, `headers`, `body`, `_mode`, `timestamp` and `matchOutcome` — and no
 * response field at all. So the response half of the stub is a *default*, not a reconstruction, and
 * the editor says so out loud rather than letting the operator assume the console replayed
 * something it never saw.
 *
 * Pure and free of React, like the stub projections it feeds: the derivation is worth testing on
 * its own, and it is the only part of this slice with rules in it.
 */

import { DEFAULT_GENERATOR_FIELDS } from "../recording/state.ts";
import type { RecordedRequest } from "./source.ts";

/** Which parts of the request become predicates. Headers are opt-in one at a time. */
export type FieldSelection = {
  method: boolean;
  path: boolean;
  query: boolean;
  headers: ReadonlySet<string>;
  body: boolean;
};

/**
 * The starting selection, DERIVED from the recording flow's defaults rather than restated.
 *
 * The two paths that generate predicates from a real request — recording (#246) and this one —
 * have to agree about what a request's identity is, or the same traffic produces two different
 * stubs depending on which screen the operator happened to start from. Importing the constant is
 * what makes that a fact rather than a comment: a change to one is a change to both.
 *
 * (#250's text proposed `query: false` here, citing the engine's proxy `predicateGenerators`
 * defaults. That is not what the engine documents — `proxy.md`'s own `predicateGenerators` example
 * is `{method: true, path: true, query: true}` — and it is not what #246 shipped, for the reason
 * given beside `DEFAULT_GENERATOR_FIELDS`: `headers` and `body` vary far more than an operator
 * wants a stub keyed on, `query` does not. Following the engine and the shipped flow.)
 */
export function defaultSelection(): FieldSelection {
  const fields = new Set<string>(DEFAULT_GENERATOR_FIELDS);
  return {
    method: fields.has("method"),
    path: fields.has("path"),
    query: fields.has("query"),
    headers: new Set<string>(),
    body: fields.has("body"),
  };
}

/** Does this body parse as JSON? Absence of a parse is a domain answer here, not a failure. */
function parseJson(body: string): { ok: true; value: unknown } | { ok: false } {
  try {
    return { ok: true, value: JSON.parse(body) as unknown };
  } catch {
    return { ok: false };
  }
}

/**
 * Build the stub.
 *
 * Everything lands under a single `equals` predicate — one object with several fields — because
 * that is the shape this console has always written and the shape `predicates.ts` reads back
 * without splitting (see its header comment on why a clause carries an `entries` array).
 */
export function stubFromRequest(request: RecordedRequest, selection: FieldSelection): unknown {
  const equals: Record<string, unknown> = {};

  if (selection.method && request.method !== undefined) equals.method = request.method;
  if (selection.path && request.path !== undefined) equals.path = request.path;

  if (selection.query && request.query !== undefined && Object.keys(request.query).length > 0) {
    equals.query = { ...request.query };
  }

  if (selection.headers.size > 0 && request.headers !== undefined) {
    const headers: Record<string, string> = {};
    for (const [name, value] of Object.entries(request.headers)) {
      if (!selection.headers.has(name)) continue;
      /*
       * ONE value, even for a header the journal recorded twice — and specifically the LAST one.
       *
       * The `Vec<String>` shape belongs to the journal, not to the matcher. `header_map_to_hashmap`
       * collects hyper's per-value iterator into a `HashMap<String, String>`, so a duplicated header
       * collapses and the later value overwrites the earlier; `handler.rs` says so outright ("stays
       * the single-value view used for matching/context"). An array-valued expectation is then
       * routed to `compare_json_recursive`, which tries to parse that single header string as JSON
       * and returns false — so a `["a","b"]` predicate can NEVER match the request it was derived
       * from. Predicating on the value the matcher will actually see is the only form that fires.
       */
      headers[name] = Array.isArray(value) ? (value[value.length - 1] ?? "") : value;
    }
    if (Object.keys(headers).length > 0) equals.headers = headers;
  }

  if (selection.body && request.body !== undefined) {
    /*
     * `equals` on the PARSED value for a JSON body, not `deepEquals`.
     *
     * Verified against the engine rather than assumed: `compare_json_recursive` is reached for any
     * object-valued expectation, so `equals` already deep-compares a parsed JSON body field by
     * field. The only thing `deepEquals` adds is the extra-keys check (`expected_obj.len() !=
     * actual_obj.len()`), i.e. exact-key matching. For a stub derived from one observed request
     * that is too strict — the next request carrying an additional field is the same call as far as
     * the developer is concerned — so the subset semantics of `equals` is the better default, and
     * the operator can tighten it to `deepEquals` in the predicate builder.
     *
     * A body that is not JSON — including a base64 `_mode: "binary"` body — becomes an `equals` on
     * the string, which is exact-byte matching and is the only honest reading of those bytes.
     */
    const parsed = request._mode === "binary" ? { ok: false as const } : parseJson(request.body);
    /*
     * Only an OBJECT or ARRAY is substituted for the raw text.
     *
     * `check_string_field` routes to `compare_json_recursive` for those two shapes only; every other
     * parsed value falls through to a string comparison against the RAW body. So a body of `1.0`
     * would be re-serialized to `1` and compared as `"1"` vs `"1.0"` — no match. Same for `"hello"`
     * (a quoted JSON string re-emitted unquoted), for `  12  ` (whitespace lost), and for an integer
     * too large for a JS number (precision lost). Keeping the raw string for every scalar leaves
     * those byte-exact, which is what the engine actually compares.
     *
     * Known limitation, deliberately not solved here: an OBJECT body must be substituted (that is
     * the only form the engine deep-compares), so an integer beyond 2^53 inside one still loses
     * precision through `JSON.parse` and yields a predicate that will not match. Fixing it needs a
     * bigint-aware parser; the operator can correct the value in the predicate builder meanwhile.
     */
    const useParsed =
      parsed.ok && typeof parsed.value === "object" && parsed.value !== null;
    equals.body = useParsed && parsed.ok ? parsed.value : request.body;
  }

  const stub: Record<string, unknown> = {};
  if (Object.keys(equals).length > 0) stub.predicates = [{ equals }];
  // The response is a DEFAULT, not something the journal recorded. See the module comment.
  stub.responses = [
    { is: { statusCode: 200, headers: { "Content-Type": "application/json" }, body: "{}" } },
  ];
  return stub;
}

/**
 * Does this imposter already carry a stub that matches everything?
 *
 * Matching is first-match-wins and `useAddStub` appends, so a stub added below a catch-all can
 * never fire — the operator would save it, see no change, and have nothing on screen explaining
 * why. Same condition the editor's own `Summary` banner checks: a stub with no predicates at all
 * matches every request.
 */
export function hasCatchAll(stubs: unknown): boolean {
  // The container is guarded as well as each element: this list comes off the wire, and a `stubs`
  // that is `null` or an object would otherwise throw on `.some` and take the screen down.
  if (!Array.isArray(stubs)) return false;
  return stubs.some((stub) => {
    if (typeof stub !== "object" || stub === null || Array.isArray(stub)) return false;
    const predicates = (stub as Record<string, unknown>).predicates;
    if (predicates === undefined) return true;
    return Array.isArray(predicates) && predicates.length === 0;
  });
}

/**
 * The position of the first stub that actually **shadows** a later one, or `null` (issue #336).
 *
 * Two differences from [`hasCatchAll`], and both are the point:
 *
 * - It returns the **index**, because a space's stub table is addressed by position — there is no
 *   `by-id` route for that collection — so "stub 0 is swallowing your traffic" is the only form of
 *   the sentence an operator can act on.
 * - It requires something to actually be shadowed. A predicate-less stub that is *last* matches
 *   everything and shadows nothing; warning about it would be crying wolf on the single most
 *   ordinary shape there is, a space-wide default with nothing after it. `hasCatchAll` answers a
 *   different question — "would a stub appended *now* be shadowed" — where being last is exactly
 *   what matters, which is why this is a sibling rather than a rewrite of it.
 *
 * Same defensive container handling as `hasCatchAll`: the list comes off the wire.
 */
export function shadowingStubIndex(stubs: unknown): number | null {
  if (!Array.isArray(stubs)) return null;
  const matchesEverything = (stub: unknown): boolean => {
    if (typeof stub !== "object" || stub === null || Array.isArray(stub)) return false;
    const predicates = (stub as Record<string, unknown>).predicates;
    if (predicates === undefined) return true;
    return Array.isArray(predicates) && predicates.length === 0;
  };
  const at = stubs.findIndex(matchesEverything);
  // `at === stubs.length - 1` is the not-shadowing case above.
  return at !== -1 && at < stubs.length - 1 ? at : null;
}

/**
 * What the row's edit action should be, from the match outcome alone.
 *
 * A matched request's useful verb is not "make a new stub" — one already answered it — it is "open
 * the one that did". The exception is a winner with no `id`: the by-index routes are the documented
 * lost-update window, so the console does not edit by index and offers nothing rather than
 * something unsafe.
 */
export type RowAction =
  | { kind: "stub" }
  | { kind: "open"; stubId: string }
  | { kind: "none"; reason: "matched-without-id" | "unreadable" };

export function rowActionFor(outcome: unknown): RowAction {
  /*
   * `null` is folded into absence, exactly as `diagnostics.ts` does and for the reason it gives
   * there: the engine omits the field rather than emitting null, but a null carries the same
   * meaning any serializer that emits one intends. Reading `.matched` off it would throw and take
   * the request log down — this screen is the one an operator opens when something is ALREADY wrong.
   */
  if (outcome === null || outcome === undefined) return { kind: "stub" };
  if (typeof outcome !== "object" || Array.isArray(outcome)) return { kind: "none", reason: "unreadable" };

  const matched = (outcome as Record<string, unknown>).matched;
  /*
   * An outcome this console cannot read gets NO action, rather than a confident one. The
   * diagnostics panel directly above already says "unreadable — nothing here says whether the
   * request matched"; offering "Stub this" beside it would be the same screen asserting two
   * different things about one row.
   */
  if (typeof matched !== "boolean") return { kind: "none", reason: "unreadable" };
  if (!matched) return { kind: "stub" };

  const stubId = (outcome as Record<string, unknown>).stubId;
  if (typeof stubId === "string" && stubId !== "") return { kind: "open", stubId };
  return { kind: "none", reason: "matched-without-id" };
}
