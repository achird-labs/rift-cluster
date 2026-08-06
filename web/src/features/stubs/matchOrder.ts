import type { components } from "../../api/schema.ts";
import { projectPredicates } from "./predicates.ts";
import { sampleRequest } from "./sample.ts";
import { describeResponses } from "./responses.ts";

type Stub = components["schemas"]["Stub"];

/** One row of the match-order column: what a stub matches, and what it answers. */
export type MatchOrderEntry = {
  /** Position in the imposter's stub list — the order the matcher walks them in. */
  index: number;
  id: string | null;
  /** `null` when the predicates do not pin a method, which is a real and common case. */
  method: string | null;
  /** Path plus query, as `sampleRequest` builds it. `null` when the predicates pin no path. */
  target: string | null;
  /** What the first response does, in `describeResponses`' own words — "200", "proxy", "fault". */
  answer: string | null;
  /** The first response's kind, so a proxy or a fault can be marked as what it is. */
  kind: string | null;
  /** How many responses the stub cycles through. `1` is the ordinary case. */
  responses: number;
  /** True when the stub carries no predicates at all, so it answers everything. */
  catchAll: boolean;
};

/**
 * Summarise each stub for the match-order column.
 *
 * Derived from the same projections the editor uses — `projectPredicates` then `sampleRequest` for
 * the request side, `describeResponses` for the answer — rather than reaching into the stub's shape
 * here. That matters because a stub is not obliged to be projectable: one the form cannot represent
 * still has to appear in this list, in its real position, or the list stops being the match order
 * and becomes "the stubs we understood".
 *
 * Everything is nullable for the same reason. A stub with no method predicate genuinely has no
 * method, and the column says so rather than defaulting to GET — which `sampleRequest` legitimately
 * does when it is building a request to *send*, a different job with a different obligation.
 */
export function matchOrder(stubs: readonly Stub[] | undefined): MatchOrderEntry[] {
  if (stubs === undefined) return [];

  return stubs.map((stub, index) => {
    const projection = projectPredicates(stub);
    const predicates = (stub as { predicates?: unknown }).predicates;
    const catchAll = !Array.isArray(predicates) || predicates.length === 0;

    let method: string | null = null;
    let target: string | null = null;
    if (projection.kind === "predicates" && projection.items.length > 0) {
      const sample = sampleRequest(projection.items);
      // `sampleRequest` fills its defaults so it can build a sendable request. Here a default would
      // be a claim about the stub, so only values the predicates actually pinned are shown.
      const pinned = JSON.stringify(projection.items);
      method = pinned.includes('"method"') ? sample.method : null;
      target = pinned.includes('"path"') ? sample.target : null;
    }

    const labels = describeResponses(stub);
    const first = labels[0];

    return {
      index,
      id: typeof stub.id === "string" && stub.id !== "" ? stub.id : null,
      method,
      target,
      answer: first?.detail ?? null,
      kind: first?.kind ?? null,
      responses: labels.length,
      catchAll,
    };
  });
}
