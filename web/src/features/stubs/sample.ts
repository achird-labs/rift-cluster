import type { PredicateClause, PredicateItem } from "./predicates.ts";

/**
 * A concrete request derived from a stub's predicates — the request an operator would have to send
 * for that stub to match.
 *
 * The honesty problem is the whole design. A predicate is a *constraint*, and only some constraints
 * name a value: `equals` on `path` says exactly what the path must be, but `matches` gives a regex,
 * `contains` gives a fragment, and `exists` gives nothing at all. Inverting those into a concrete
 * request means guessing, and a guessed request that silently fails to match is worse than no
 * request — the operator concludes their stub is broken when it is the sample that is wrong.
 *
 * So every predicate is either honoured exactly or recorded as a caveat, and the caller shows the
 * caveats. Nothing here invents a value it cannot justify.
 */

/** A field this module can derive a concrete value for, and what it was derived from. */
export type Sample = {
  method: string;
  /** Path plus query string, ready to append to an origin. */
  target: string;
  headers: { name: string; value: string }[];
  body: string | null;
  /**
   * Predicates that could not be turned into a concrete value, in the operator's words.
   *
   * Empty means the sample satisfies every predicate the stub declares. Non-empty means the sample
   * is a starting point and may not match — which the UI has to say, or it is lying by omission.
   */
  caveats: string[];
};

/** Operators that name an exact value, so a request can be built from them without guessing. */
const EXACT = new Set(["equals", "deepEquals"]);

function asText(value: unknown): string | null {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return null;
}

/**
 * Flatten to the clauses a request can be built from.
 *
 * `or` groups are skipped rather than guessed at: satisfying one branch of an `or` is a choice
 * between alternatives the operator has not made, and picking the first silently would produce a
 * request that matches for a reason they did not choose. `not` groups are worse — the request must
 * *avoid* the clause, and "any value other than this" is not a value.
 */
function buildableClauses(items: readonly PredicateItem[]): {
  clauses: PredicateClause[];
  caveats: string[];
} {
  const clauses: PredicateClause[] = [];
  const caveats: string[] = [];
  for (const item of items) {
    if (item.kind === "clause") {
      clauses.push(item.clause);
      continue;
    }
    caveats.push(
      item.op === "or"
        ? "An `or` group was skipped: satisfying one branch is a choice between alternatives, and picking one silently would match for a reason you did not pick."
        : "A `not` group was skipped: it says which requests must NOT match, and “anything other than this” is not a value.",
    );
  }
  return { clauses, caveats };
}

/**
 * Derive a request from a stub's predicates.
 *
 * `items` is the projected predicate model, so this works on exactly what the form shows — a stub
 * whose predicates the form cannot model reaches the caller as raw-only and never gets here.
 */
export function sampleRequest(items: readonly PredicateItem[]): Sample {
  const { clauses, caveats } = buildableClauses(items);

  let method = "GET";
  let path = "/";
  const query: { key: string; value: string }[] = [];
  const headers: { name: string; value: string }[] = [];
  let body: string | null = null;

  for (const clause of clauses) {
    for (const entry of clause.entries) {
      const text = asText(entry.value);
      const exact = EXACT.has(clause.operator);

      if (entry.field === "method") {
        if (exact && text !== null) method = text.toUpperCase();
        else caveats.push(`Method uses \`${clause.operator}\`, so no exact method could be derived; GET is used.`);
        continue;
      }
      if (entry.field === "path") {
        // `startsWith` is the one inexact operator that still names a value the request can carry
        // verbatim — a path beginning with it satisfies the predicate by construction.
        if ((exact || clause.operator === "startsWith") && text !== null) path = text;
        else caveats.push(`Path uses \`${clause.operator}\`, so no exact path could be derived; \`/\` is used.`);
        continue;
      }
      if (entry.field === "query") {
        if (exact && entry.key !== null && text !== null) query.push({ key: entry.key, value: text });
        else caveats.push(`A query predicate uses \`${clause.operator}\`, so it was not added to the URL.`);
        continue;
      }
      if (entry.field === "headers") {
        if (exact && entry.key !== null && text !== null) headers.push({ name: entry.key, value: text });
        else caveats.push(`A header predicate uses \`${clause.operator}\`, so it was not added to the request.`);
        continue;
      }
      // body
      if (exact && text !== null) body = text;
      else if (exact && entry.value !== null && typeof entry.value === "object")
        body = JSON.stringify(entry.value);
      else caveats.push(`The body predicate uses \`${clause.operator}\`, so no exact body could be derived.`);
    }
  }

  const search = query
    .map(({ key, value }) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
    .join("&");

  return {
    method,
    target: search === "" ? path : `${path}?${search}`,
    headers,
    body,
    caveats,
  };
}

/** Shell-quote for `sh`: wrap in single quotes and escape any single quote the value contains. */
function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

/**
 * Render a sample as a `curl` an operator can paste into a terminal.
 *
 * `--include` rather than bare output: the whole point of trying a stub is seeing the status and
 * headers it produced, and a body-only response cannot tell a 200 from a 404 with the same text.
 *
 * Every interpolated value is shell-quoted. A path, a header value or a body is operator-authored
 * text that reaches a shell, and an unquoted `;` or `$(...)` there would execute rather than
 * transmit — this is generated for a human to run, so it must be safe to run.
 */
export function toCurl(sample: Sample, origin: string): string {
  const parts = ["curl --include"];
  if (sample.method !== "GET") parts.push(`--request ${sample.method}`);
  for (const header of sample.headers) {
    parts.push(`--header ${shellQuote(`${header.name}: ${header.value}`)}`);
  }
  if (sample.body !== null) parts.push(`--data ${shellQuote(sample.body)}`);
  parts.push(shellQuote(`${origin}${sample.target}`));
  // One line per argument: a stub with several headers produces a command that is read, edited and
  // re-run, not a single line that wraps unpredictably in a terminal.
  return parts.join(" \\\n  ");
}
