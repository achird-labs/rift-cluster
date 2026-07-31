import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import {
  API_PATHS,
  fleetOpPath,
  imposterPath,
  lifecyclePath,
  requestsPath,
  stubByIdPath,
} from "../api/paths.ts";
import { FLEET_HEALTH_FIELDS, FLEET_MEMBER_FIELDS, IMPOSTER_COLUMNS } from "../app/contract.ts";

const SRC = new URL("..", import.meta.url).pathname;
const CONTRACT = readFileSync(join(SRC, "api", "schema.ts"), "utf8");

/**
 * The body of one `components["schemas"]` entry, sliced out by brace depth.
 *
 * A lazy `[\s\S]*?` over the whole file would find a key that is *not* in this schema by running
 * on into the next one — the test would then pass for exactly the field it exists to reject.
 */
function schemaBody(name: string): string {
  const start = CONTRACT.indexOf(`        ${name}: {`);
  if (start === -1) throw new Error(`the generated contract declares no ${name}`);
  let depth = 0;
  for (let i = CONTRACT.indexOf("{", start); i < CONTRACT.length; i += 1) {
    if (CONTRACT[i] === "{") depth += 1;
    if (CONTRACT[i] === "}") {
      depth -= 1;
      if (depth === 0) return CONTRACT.slice(start, i);
    }
  }
  throw new Error(`unterminated ${name} in the generated contract`);
}

/** Does the schema declare `key` as a property — as opposed to admitting it via `[key: string]`? */
function declares(name: string, key: string): boolean {
  return new RegExp(`^\\s{12}${key}\\??:`, "m").test(schemaBody(name));
}

/**
 * Source with its comments blanked out, so a scan bans a construct without banning the sentence
 * that explains why it is banned. Naive about `//` inside a string literal, which is fine for the
 * one scan that uses it: a false *positive* fails loudly and is fixed, never waved through.
 */
function withoutComments(source: string): string {
  return source.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:])\/\/.*$/gm, "$1");
}

function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((entry) => {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) return sourceFiles(path);
    return /\.tsx?$/.test(entry) && !/\.test\.tsx?$/.test(entry) ? [path] : [];
  });
}

/**
 * RFC-006 §11's exit criterion — "every displayed field is traceable to a schema'd endpoint" — has
 * two halves, and only one of them can be a type.
 *
 * The *keys* are checked by the compiler: `contract.ts` declares them as `keyof` the generated
 * schema type with its index signature stripped, so naming a field the contract does not declare
 * fails `tsc`, and the `& { [key: string]: unknown }` escape hatch on `Imposter` cannot be used to
 * smuggle one through. What the compiler cannot check is a screen that renders something without
 * going through those tables at all — hence the scans below.
 */
describe("every displayed field is traceable to the contract", () => {
  it("declares imposter columns the contract actually publishes", () => {
    expect(IMPOSTER_COLUMNS.length).toBeGreaterThan(0);
    for (const column of IMPOSTER_COLUMNS) {
      // The generated client is the contract rendered as TypeScript; the key must appear as a
      // declared property of `Imposter`, not merely be assignable to its index signature.
      expect([column.key, declares("Imposter", column.key)]).toEqual([column.key, true]);
    }
  });

  it("rejects a key that only the index signature would admit", () => {
    // Guards the guard: `Imposter` ends in `& { [key: string]: unknown }`, so any string is
    // assignable to it. If `declares` ever answered on that basis the test above would be vacuous.
    expect(declares("Imposter", "numberOfRequests")).toBe(false);
    expect(declares("Imposter", "enabled")).toBe(true);
  });

  it("declares fleet fields the contract actually publishes", () => {
    for (const field of FLEET_MEMBER_FIELDS) {
      expect([field.key, declares("FleetMembers", field.key)]).toEqual([field.key, true]);
    }
    for (const field of FLEET_HEALTH_FIELDS) {
      expect([field.key, declares("FleetHealth", field.key)]).toEqual([field.key, true]);
    }
  });

  it("declares the recorded-request fields the request log renders", () => {
    // The request log used to hand-write this shape, because the contract declared the response as
    // an untyped `object` and the generated client gave it nothing to import (#212). Deriving the
    // type only helps if the contract keeps carrying the fields — a regression to `type: object`
    // would leave `Partial<RecordedRequest>` as an empty type that still compiles everywhere.
    for (const field of ["requestFrom", "method", "path", "query", "headers", "timestamp"]) {
      expect([field, declares("RecordedRequest", field)]).toEqual([field, true]);
    }
    // The two the engine omits rather than nulls, which is why they are optional on the wire.
    expect(declares("RecordedRequest", "body")).toBe(true);
    expect(declares("RecordedRequest", "_mode")).toBe(true);
  });

  it("declares the match-diagnostics fields the request log renders (#208)", () => {
    // The panel reads four fields off a schema'd component rather than off the index signature. If
    // the contract ever regenerated without them, `describeOutcome` would keep compiling — its
    // input is validated at runtime, not by `tsc` — and every entry would render as "no match
    // diagnostics recorded", which is the one sentence this feature exists to stop being a lie.
    expect(declares("RecordedRequest", "matchOutcome")).toBe(true);
    for (const field of ["matched", "stubIndex", "stubId", "tried", "triedOmitted"]) {
      expect([field, declares("MatchOutcome", field)]).toEqual([field, true]);
    }
    for (const field of ["stubIndex", "stubId", "why"]) {
      expect([field, declares("TriedStub", field)]).toEqual([field, true]);
    }
    for (const field of ["reason", "predicateIndex"]) {
      expect([field, declares("TriedWhy", field)]).toEqual([field, true]);
    }
  });

  it("renders no field the prototype invented but the contract does not carry", () => {
    // `numberOfRequests` drove the prototype's one chart. It is not a declared `Imposter` property —
    // it would arrive only through the index signature — so C4 does not render it. This test is the
    // reason that stayed a decision rather than becoming an oversight.
    const keys = IMPOSTER_COLUMNS.map((c) => c.key as string);
    expect(keys).not.toContain("numberOfRequests");
  });
});

describe("the client is the only door to the network", () => {
  it("calls fetch from nowhere but api/client.ts", () => {
    // A screen calling `fetch` directly bypasses the tenant header, the CSRF header and the
    // non-2xx-becomes-an-error rule in one step, and none of the other tests would notice.
    const offenders = sourceFiles(SRC)
      .filter((path) => !path.endsWith(join("api", "client.ts")))
      .filter((path) => /(^|[^.\w])fetch\s*\(/.test(readFileSync(path, "utf8")));
    expect(offenders).toEqual([]);
  });

  it("asks only for paths the contract publishes", () => {
    // `API_PATHS` is typed as `ApiPath`, so for those this is belt-and-braces on the compiler. The
    // builders are where it earns its keep: they return `string`, so nothing but this checks that
    // the port is interpolated into a route the contract actually declares.
    const built = [
      imposterPath(4545),
      lifecyclePath(4545, true),
      lifecyclePath(4545, false),
      requestsPath(4545),
    ];
    for (const path of [...Object.values(API_PATHS), ...built]) {
      const template = path.replace(/\/\d+(?=\/|$)/, "/{port}");
      expect([path, CONTRACT.includes(`"${template}": {`)]).toEqual([path, true]);
    }
  });
});

describe("stubs are addressed by id and never by index (RFC-006 C5, AC9)", () => {
  it("builds a by-id stub path the contract declares", () => {
    const built = stubByIdPath(4545, "s-1");
    expect(built).toBe("/imposters/4545/stubs/by-id/s-1");
    expect(CONTRACT.includes('"/imposters/{port}/stubs/by-id/{stubId}": {')).toBe(true);
  });

  it("percent-encodes the stub id rather than splicing it in raw", () => {
    // An id with a slash must reach the server as one segment and 404, not address a route the
    // operator never asked for.
    expect(stubByIdPath(4545, "a/b")).toBe("/imposters/4545/stubs/by-id/a%2Fb");
  });

  it("names an index-addressed stub route nowhere in the console's source", () => {
    /*
     * The contract publishes `/imposters/{port}/stubs/{stubIndex}` and this console must never use
     * it. An index is a position: a concurrent edit that inserts or removes a stub shifts every
     * index after it, so an index-addressed write racing that edit replaces a *different* stub and
     * answers `200`. `If-Match` does not save it either — the revision would match, and the wrong
     * stub would still be the one overwritten.
     *
     * `schema.ts` is exempt because it is the contract rendered as TypeScript, not a call site: it
     * *declares* the route the rest of the source is forbidden to build.
     */
    const offenders = sourceFiles(SRC)
      .filter((path) => !path.endsWith(join("api", "schema.ts")))
      // Anchored on `/imposters/` so it matches a *route* and not the `features/stubs/` directory
      // the editor's own modules live in. A bare `/stubs/` scan flags every import of them.
      .filter((path) =>
        /\/imposters\/[^\n"'`]*\/stubs\/(?!by-id\/)/.test(withoutComments(readFileSync(path, "utf8"))),
      );
    expect(offenders).toEqual([]);
  });
});

describe("the op-status poll target is a published route", () => {
  it("builds a path the contract declares", () => {
    // `fleetOpPath` interpolates a uuid, not a number, so the numeric normalisation the test above
    // uses cannot reach it — it needs its own check or the one route #211 added would be the only
    // builder not covered by §11's "no field sourced from a UI-only endpoint".
    const built = fleetOpPath("11111111-1111-4111-8111-111111111111");
    expect(built.startsWith("/_fleet/ops/")).toBe(true);
    expect(CONTRACT.includes('"/_fleet/ops/{opId}": {')).toBe(true);
  });

  it("percent-encodes the op id rather than splicing it in raw", () => {
    // A malformed id must reach the server as one path segment and 404, not silently address a
    // different route.
    expect(fleetOpPath("a/b")).toBe("/_fleet/ops/a%2Fb");
  });
});

describe("XSS defense in depth (RFC-006 §9.1)", () => {
  it("uses dangerouslySetInnerHTML nowhere — the lint rule's assertion, restated as a test", () => {
    // `pnpm run lint` is the enforcing gate. This duplicates it on purpose: the lint step is a
    // separate CI invocation that a workflow edit could drop, and this criterion is the one whose
    // silent removal is least likely to be noticed.
    const offenders = sourceFiles(SRC).filter((path) =>
      readFileSync(path, "utf8").includes("dangerouslySetInnerHTML"),
    );
    expect(offenders).toEqual([]);
  });
});
