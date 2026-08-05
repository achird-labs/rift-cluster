import { describe, expect, it } from "vitest";

import type { PredicateClause, PredicateItem } from "./predicates.ts";
import { sampleRequest, toCurl } from "./sample.ts";

function clause(
  operator: PredicateClause["operator"],
  entries: PredicateClause["entries"],
): PredicateItem {
  return {
    kind: "clause",
    clause: { operator, entries, caseSensitive: null, except: null, selector: null },
  };
}

describe("sampleRequest", () => {
  it("builds the request a stub's exact predicates describe", () => {
    const sample = sampleRequest([
      clause("equals", [
        { field: "method", key: null, value: "post" },
        { field: "path", key: null, value: "/orders" },
      ]),
      clause("equals", [{ field: "headers", key: "Content-Type", value: "application/json" }]),
      clause("equals", [{ field: "query", key: "dry", value: "true" }]),
      clause("equals", [{ field: "body", key: null, value: '{"id":1}' }]),
    ]);

    // Upper-cased: a method is a token, and `curl --request post` is not what an operator means.
    expect(sample.method).toBe("POST");
    expect(sample.target).toBe("/orders?dry=true");
    expect(sample.headers).toEqual([{ name: "Content-Type", value: "application/json" }]);
    expect(sample.body).toBe('{"id":1}');
    expect(sample.caveats).toEqual([]);
  });

  it("defaults to GET / for a stub with no predicates, and says nothing is caveated", () => {
    // A predicate-less stub matches everything, so `GET /` genuinely reaches it — there is nothing
    // to warn about, and inventing a caveat would be as dishonest as inventing a path.
    const sample = sampleRequest([]);
    expect(sample.method).toBe("GET");
    expect(sample.target).toBe("/");
    expect(sample.caveats).toEqual([]);
  });

  it("carries a startsWith path verbatim, because that path satisfies the predicate", () => {
    const sample = sampleRequest([clause("startsWith", [{ field: "path", key: null, value: "/v1/" }])]);
    expect(sample.target).toBe("/v1/");
    expect(sample.caveats).toEqual([]);
  });

  it("refuses to invent a path from a regex, and says so", () => {
    /*
     * The case the whole module exists for. `matches` names a pattern, not a value; guessing one
     * produces a request that silently fails to match, and the operator concludes the STUB is
     * broken when it is the sample that is wrong.
     */
    const sample = sampleRequest([
      clause("matches", [{ field: "path", key: null, value: "^/orders/[0-9]+$" }]),
    ]);
    expect(sample.target).toBe("/");
    expect(sample.caveats).toHaveLength(1);
    expect(sample.caveats[0]).toContain("matches");
  });

  it("skips an or-group rather than picking a branch the operator did not choose", () => {
    const sample = sampleRequest([
      {
        kind: "group",
        op: "or",
        clauses: [
          { operator: "equals", entries: [{ field: "path", key: null, value: "/a" }], caseSensitive: null, except: null, selector: null },
          { operator: "equals", entries: [{ field: "path", key: null, value: "/b" }], caseSensitive: null, except: null, selector: null },
        ],
      },
    ]);
    expect(sample.target).toBe("/");
    expect(sample.caveats[0]).toContain("or");
  });

  it("drops an inexact header rather than sending a fragment as the whole value", () => {
    const sample = sampleRequest([
      clause("contains", [{ field: "headers", key: "Authorization", value: "Bearer " }]),
    ]);
    expect(sample.headers).toEqual([]);
    expect(sample.caveats).toHaveLength(1);
  });
});

describe("toCurl", () => {
  it("renders a GET without a redundant --request", () => {
    const curl = toCurl(sampleRequest([]), "http://localhost:6001");
    expect(curl).toContain("curl --include");
    expect(curl).not.toContain("--request");
    expect(curl).toContain("'http://localhost:6001/'");
  });

  it("includes the method, headers and body when there are any", () => {
    const sample = sampleRequest([
      clause("equals", [
        { field: "method", key: null, value: "POST" },
        { field: "path", key: null, value: "/orders" },
      ]),
      clause("equals", [{ field: "headers", key: "X-Trace", value: "1" }]),
      clause("equals", [{ field: "body", key: null, value: "hello" }]),
    ]);
    const curl = toCurl(sample, "http://localhost:6001");
    expect(curl).toContain("--request POST");
    expect(curl).toContain("--header 'X-Trace: 1'");
    expect(curl).toContain("--data 'hello'");
  });

  it("quotes values so operator text cannot execute in the shell", () => {
    /*
     * This is generated for a human to paste into a terminal, so it has to be safe to run. A path
     * or body is operator-authored text; unquoted, a `;` or `$(...)` in it would EXECUTE rather
     * than transmit.
     */
    const sample = sampleRequest([
      clause("equals", [{ field: "path", key: null, value: "/x;rm -rf /" }]),
      clause("equals", [{ field: "body", key: null, value: "$(whoami)" }]),
    ]);
    const curl = toCurl(sample, "http://localhost:6001");
    expect(curl).toContain("'http://localhost:6001/x;rm -rf /'");
    expect(curl).toContain("--data '$(whoami)'");
    // Nothing escapes its quotes.
    expect(curl).not.toMatch(/[^']\$\(whoami\)[^']/);
  });

  it("keeps a single quote inside a value from ending the quoting", () => {
    const sample = sampleRequest([clause("equals", [{ field: "body", key: null, value: "it's" }])]);
    expect(toCurl(sample, "http://x")).toContain(`--data 'it'\\''s'`);
  });
});
