import { describe, expect, it } from "vitest";

import {
  cloneImposter,
  selectImposter,
  exportFilename,
  PROJECTION_OPTIONS,
  exportOptionsQuery,
  exportQuery,
  exportSetFilename,
  importPlan,
  parseImportDocument,
  renderSetDocument,
} from "./portable.ts";

const IMPOSTER = {
  port: 4545,
  protocol: "http",
  name: "billing",
  stubs: [{ predicates: [{ equals: { path: "/a" } }], responses: [{ is: { statusCode: 200 } }] }],
};

/** Only entries that parsed; throws loudly rather than letting a test assert on an error case. */
function entriesOf(text: string) {
  const parsed = parseImportDocument(text);
  if (parsed.kind !== "ok") throw new Error(`expected a document, got: ${parsed.message}`);
  return parsed.entries;
}

describe("export projections", () => {
  it("asks for the replay-ready projection by default, and the as-configured one on request", () => {
    // The difference is whether the importer keeps recording: `removeProxies` turns recorded proxy
    // responses into static stubs and drops the proxy stub itself.
    //
    // Every flag is now sent rather than omitted when false. The route parses values rather than
    // reading mere presence — `replayable=false` genuinely produces a different document — so this
    // is the same request, said out loud. It has to be: the export dialog shows the operator the
    // curl it is about to run, and a preview that hides a parameter is a preview of a different
    // command.
    //
    // `tls` is the design's third option and the route does not implement it yet
    // (`EXPORT_TLS_IS_INERT`). It is sent regardless so that the day the route learns it, this
    // works with no further change — and so the curl an operator copies out of the dialog is the
    // one the console ran.
    expect(exportQuery("replay-ready")).toBe(
      "?replayable=true&removeProxies=true&tls=false",
    );
    expect(exportQuery("as-configured")).toBe(
      "?replayable=true&removeProxies=false&tls=false",
    );
  });

  it("builds both projections through the one options builder", () => {
    // The presets and the dialog's checkboxes must not be able to produce different URLs for the
    // same intent: the drift would be silent, and the result is a file whose contents do not match
    // the projection its own filename claims.
    expect(exportQuery("replay-ready")).toBe(exportOptionsQuery(PROJECTION_OPTIONS["replay-ready"]));
    expect(exportQuery("as-configured")).toBe(
      exportOptionsQuery(PROJECTION_OPTIONS["as-configured"]),
    );
  });

  it("names the file by port, and by name when there is one", () => {
    expect(exportFilename(4545, "billing")).toBe("imposter-4545-billing.json");
    expect(exportFilename(4545, undefined)).toBe("imposter-4545.json");
    expect(exportSetFilename("acme")).toBe("imposters-acme.json");
    expect(exportSetFilename(null)).toBe("imposters.json");
  });

  it("keeps a name with awkward characters usable as a filename", () => {
    // Names come from the operator and end up in a downloads folder; a slash or a quote there is
    // at best confusing and at worst a path the browser refuses.
    expect(exportFilename(80, 'billing/v2 "prod"')).toBe("imposter-80-billing-v2-prod.json");
    expect(exportFilename(80, "   ")).toBe("imposter-80.json");
    expect(exportFilename(80, "...")).toBe("imposter-80.json");
  });
});

describe("selecting one imposter out of a set export", () => {
  /*
   * `GET /imposters/:port?replayable=true` cannot serve this: `handle_get` reads only
   * `removeProxies` and answers with the full `ImposterDetail` — `numberOfRequests`, the recorded
   * `requests` journal (headers and bodies included) and `_links` naming the serving node. Unstable
   * across exports, and it would carry captured credentials into a file the console tells the
   * operator to commit. So the set projection is fetched and the one entry selected here.
   */
  const SET = JSON.stringify({ imposters: [IMPOSTER, { ...IMPOSTER, port: 4546, name: "orders" }] });

  it("returns just the wanted imposter, deterministically serialized", () => {
    const selected = selectImposter(SET, 4546);
    expect(selected.kind).toBe("ok");
    if (selected.kind !== "ok") return;
    expect(JSON.parse(selected.text)).toEqual({ ...IMPOSTER, port: 4546, name: "orders" });
    // Two selections of an unchanged set must be byte-identical — that IS the diff stability.
    expect(selectImposter(SET, 4546)).toEqual(selected);
  });

  it("carries none of the per-node detail the single-imposter route would have included", () => {
    const noisy = JSON.stringify({
      imposters: [{ ...IMPOSTER, numberOfRequests: 12, requests: [{ headers: { authorization: "Bearer sk-live" } }], _links: { self: { href: "http://node-7:2525" } } }],
    });
    const selected = selectImposter(noisy, 4545);
    if (selected.kind !== "ok") throw new Error("expected a selection");
    /*
     * Defence in depth: the set projection renders `ImposterConfig` and never emits these, so this
     * strips nothing today. It is asserted anyway because the cost of being wrong is not a cosmetic
     * diff — `requests` is the journal, headers and bodies included, in a file the console tells the
     * operator to commit.
     */
    expect(selected.text).not.toContain("sk-live");
    expect(selected.text).not.toContain("_links");
    expect(selected.text).not.toContain("numberOfRequests");
  });

  it("says so when the fleet returned no imposter on that port", () => {
    const selected = selectImposter(SET, 9999);
    expect(selected.kind).toBe("error");
    if (selected.kind !== "error") return;
    expect(selected.message).toContain("9999");
  });

  it("reports a set document it could not read, rather than downloading nothing", () => {
    expect(selectImposter("{not json", 4545).kind).toBe("error");
  });
});

describe("an import accepts exactly what an export produces", () => {
  it("reads a single imposter object — what a one-imposter export downloads", () => {
    const entries = entriesOf(JSON.stringify(IMPOSTER));
    expect(entries).toHaveLength(1);
    expect(entries[0]?.port).toBe(4545);
    expect(entries[0]?.name).toBe("billing");
    expect(entries[0]?.imposter).toEqual(IMPOSTER);
  });

  it("reads an `{imposters: [...]}` document — what a whole-set export downloads", () => {
    const entries = entriesOf(JSON.stringify({ imposters: [IMPOSTER, { ...IMPOSTER, port: 4546 }] }));
    expect(entries.map((entry) => entry.port)).toEqual([4545, 4546]);
  });

  it("reads a bare array too, because that is what `GET /imposters` itself returns", () => {
    // Somebody will paste one. Refusing it would be a distinction without a reason.
    expect(entriesOf(JSON.stringify([IMPOSTER])).map((entry) => entry.port)).toEqual([4545]);
  });

  it("round-trips a set document back out unchanged", () => {
    const document = { imposters: [IMPOSTER, { ...IMPOSTER, port: 4546, name: "orders" }] };
    expect(renderSetDocument(entriesOf(JSON.stringify(document)))).toEqual(document);
  });
});

describe("an import that cannot be read says why, rather than failing at the server", () => {
  it("names the JSON error for a malformed paste", () => {
    const parsed = parseImportDocument("{not json");
    expect(parsed.kind).toBe("error");
    if (parsed.kind !== "error") return;
    expect(parsed.message).toMatch(/not valid JSON/i);
  });

  it("refuses an empty paste without pretending it imported nothing", () => {
    expect(parseImportDocument("   ").kind).toBe("error");
  });

  it("refuses a document that is not an imposter at all", () => {
    for (const text of ["42", '"a string"', "null", "true"]) {
      expect([text, parseImportDocument(text).kind]).toEqual([text, "error"]);
    }
  });

  it("names which item of a list is not an imposter, not merely that one is not", () => {
    const parsed = parseImportDocument(JSON.stringify([IMPOSTER, 42]));
    expect(parsed.kind).toBe("error");
    if (parsed.kind !== "error") return;
    expect(parsed.message).toMatch(/2/);
  });

  it("refuses an `imposters` key that is not a list", () => {
    expect(parseImportDocument(JSON.stringify({ imposters: {} })).kind).toBe("error");
  });

  it("carries an imposter with no usable port rather than dropping it silently", () => {
    // The server will refuse it; the pre-flight names it so the operator is not surprised.
    const entries = entriesOf(JSON.stringify({ protocol: "http" }));
    expect(entries[0]?.port).toBeNull();
  });
});

describe("the pre-flight plan, worked out before anything is written", () => {
  it("flags the ports the fleet already serves", () => {
    /*
     * The two modes fail differently and neither failure is visible from the document alone: `Add`
     * is refused per-imposter by the port check, while `Replace all` succeeds and destroys what was
     * there. Naming the overlap up front is what makes the choice informed.
     */
    const entries = entriesOf(JSON.stringify({ imposters: [IMPOSTER, { ...IMPOSTER, port: 9000 }] }));
    const plan = importPlan(entries, [4545, 7000]);
    expect(plan.collisions).toEqual([4545]);
    expect(plan.duplicates).toEqual([]);
    expect(plan.portless).toBe(0);
  });

  it("flags a port the document itself names twice", () => {
    // Not a fleet collision — the document contradicts itself, and only the last one would survive.
    const entries = entriesOf(JSON.stringify({ imposters: [IMPOSTER, { ...IMPOSTER }] }));
    const plan = importPlan(entries, []);
    expect(plan.duplicates).toEqual([4545]);
    expect(plan.collisions).toEqual([]);
  });

  it("treats a port outside 1..65535 as no port at all", () => {
    // Listing `-1` in the pre-flight as though it were a port promises something the server refuses.
    const entries = entriesOf(JSON.stringify({ imposters: [{ port: -1 }, { port: 999999 }, IMPOSTER] }));
    expect(entries.map((entry) => entry.port)).toEqual([null, null, 4545]);
  });

  it("counts imposters with no port", () => {
    const entries = entriesOf(JSON.stringify({ imposters: [{ protocol: "http" }, IMPOSTER] }));
    expect(importPlan(entries, []).portless).toBe(1);
  });

  it("reports each colliding port once, however many times it appears", () => {
    const entries = entriesOf(JSON.stringify({ imposters: [IMPOSTER, IMPOSTER, IMPOSTER] }));
    const plan = importPlan(entries, [4545]);
    expect(plan.collisions).toEqual([4545]);
    expect(plan.duplicates).toEqual([4545]);
  });
});

describe("clone", () => {
  it("carries everything across but the port and the name", () => {
    const result = cloneImposter(IMPOSTER, 4600, "billing-copy");
    expect(result.kind).toBe("ok");
    if (result.kind !== "ok") return;
    expect(result.imposter).toEqual({ ...IMPOSTER, port: 4600, name: "billing-copy" });
    // The stubs are the point of a duplicate — they must come along byte for byte.
    expect(result.imposter.stubs).toEqual(IMPOSTER.stubs);
  });

  it("drops the name entirely when none is given, rather than copying the original's", () => {
    // Two imposters called "billing" on different ports is the confusing outcome, not the helpful
    // one — and an absent name is a different document from an empty one.
    const result = cloneImposter(IMPOSTER, 4600, null);
    expect(result.kind).toBe("ok");
    if (result.kind !== "ok") return;
    expect("name" in result.imposter).toBe(false);
  });

  it("replaces a port spelled as a string with the numeric one the API requires", () => {
    const result = cloneImposter({ ...IMPOSTER, port: "4545" }, 4600, null);
    expect(result.kind).toBe("ok");
    if (result.kind !== "ok") return;
    expect(result.imposter.port).toBe(4600);
  });

  it("refuses a port outside the range, rather than letting the server answer for it", () => {
    for (const port of [0, -1, 65536, 1.5, Number.NaN]) {
      expect([port, cloneImposter(IMPOSTER, port, null).kind]).toEqual([port, "error"]);
    }
  });

  it("refuses a source that is not an imposter object", () => {
    for (const source of [null, 42, "x", [IMPOSTER]]) {
      expect([source, cloneImposter(source, 4600, null).kind]).toEqual([source, "error"]);
    }
  });

  it("does not carry the source's recorded journal into the duplicate", () => {
    // The dialog promises exactly this. `{ ...source }` alone would quietly break it.
    const result = cloneImposter(
      { ...IMPOSTER, requests: [{ headers: { authorization: "Bearer sk-live" } }], numberOfRequests: 9 },
      4600,
      null,
    );
    if (result.kind !== "ok") throw new Error("expected a clone");
    expect("requests" in result.imposter).toBe(false);
    expect("numberOfRequests" in result.imposter).toBe(false);
    expect(result.imposter.stubs).toEqual(IMPOSTER.stubs);
  });

  it("produces something an import would read straight back", () => {
    // The clone path and the import path have to agree about what an imposter document is.
    const result = cloneImposter(IMPOSTER, 4600, null);
    if (result.kind !== "ok") throw new Error("expected a clone");
    const entries = entriesOf(JSON.stringify(result.imposter));
    expect(entries[0]?.port).toBe(4600);
  });
});
