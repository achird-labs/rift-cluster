import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  cloneImposter,
  selectImposter,
  exportFilename,
  PROJECTION_OPTIONS,
  exportOptionsQuery,
  exportQuery,
  exportCurl,
  EXPORT_SET_FILENAME,
  renderImposterExport,
  renderSetExport,
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
    expect(exportQuery("replay-ready")).toBe("?replayable=true&removeProxies=true");
    expect(exportQuery("as-configured")).toBe("?replayable=true&removeProxies=false");
  });

  // Pins D-84: TLS material is kept or removed by the console, after the read. A `tls=` on the
  // wire told anyone reading the request that the server decided, and it never did.
  it("never sends tls, whichever way the option is set — the route has no such flag (D-84)", () => {
    expect(exportOptionsQuery({ replayable: true, removeProxies: false, tls: true })).toBe(
      "?replayable=true&removeProxies=false",
    );
    expect(exportOptionsQuery({ replayable: false, removeProxies: true, tls: false })).toBe(
      "?replayable=false&removeProxies=true",
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
    expect(EXPORT_SET_FILENAME).toBe("imposters.json");
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

/*
 * Pins D-84: the route's replayable projection is the imposter's config verbatim, so an https
 * imposter created with its own `cert` and `key` comes back with both. The console removes that pair
 * from a downloaded file unless the operator asked to keep it — and removes nothing else, because
 * `ca`, `mutualAuth` and `rejectUnauthorized` are public and change what the imposter does.
 */
describe("TLS material in an export (D-84)", () => {
  const HTTPS = {
    port: 4545,
    protocol: "https",
    name: "billing",
    cert: "-----BEGIN CERTIFICATE-----\nCERT\n-----END CERTIFICATE-----\n",
    key: "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----\n",
    mutualAuth: true,
    rejectUnauthorized: true,
    ca: ["-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n"],
    stubs: [],
  };
  // An https imposter with no material of its own serves the server default or a generated cert;
  // neither is in its config, so there is nothing to remove and nothing to report.
  const HTTPS_BARE = { port: 4546, protocol: "https", stubs: [] };
  const HTTP = { port: 4547, protocol: "http", stubs: [] };

  // What the route sends: `serde_json::to_string_pretty`, no trailing newline.
  const SET_TEXT = JSON.stringify({ imposters: [HTTPS, HTTPS_BARE, HTTP] }, null, 2);

  // Pins D-84: the pair goes, both halves; `ca`, `mutualAuth` and `rejectUnauthorized` stay.
  it("removes cert and key from every imposter, and keeps the client-auth settings", () => {
    const rendered = renderSetExport(SET_TEXT, false);
    expect(rendered).toEqual({
      kind: "ok",
      tlsPorts: [4545],
      tlsWithoutPort: 0,
      text: `{
  "imposters": [
    {
      "port": 4545,
      "protocol": "https",
      "name": "billing",
      "mutualAuth": true,
      "rejectUnauthorized": true,
      "ca": [
        "-----BEGIN CERTIFICATE-----\\nCA\\n-----END CERTIFICATE-----\\n"
      ],
      "stubs": []
    },
    {
      "port": 4546,
      "protocol": "https",
      "stubs": []
    },
    {
      "port": 4547,
      "protocol": "http",
      "stubs": []
    }
  ]
}
`,
    });
  });

  // Pins D-84: kept means the route's bytes.
  it("hands back the route's bytes untouched when the operator keeps TLS material", () => {
    // Deliberately not the indentation a re-serialization would produce: kept means kept.
    const raw = `{"imposters":[{"port":4545,"protocol":"https","cert":"C","key":"K"},{"port":4547,"protocol":"http"}]}`;
    expect(renderSetExport(raw, true)).toEqual({ kind: "ok", tlsPorts: [4545], tlsWithoutPort: 0, text: raw });
  });

  it("removes and reports half a pair — a lone key is still a private key", () => {
    const text = JSON.stringify({
      imposters: [
        { port: 1, protocol: "https", key: "K" },
        { port: 2, protocol: "https", cert: "C" },
      ],
    });
    const rendered = renderSetExport(text, false);
    expect(rendered.kind).toBe("ok");
    if (rendered.kind !== "ok") return;
    expect(rendered.tlsPorts).toEqual([1, 2]);
    expect(JSON.parse(rendered.text)).toEqual({
      imposters: [
        { port: 1, protocol: "https" },
        { port: 2, protocol: "https" },
      ],
    });
  });

  it("removes material from an imposter with no usable port, and counts the ones it cannot name", () => {
    const text = JSON.stringify({ imposters: [{ protocol: "https", cert: "C", key: "K" }] });
    const rendered = renderSetExport(text, false);
    expect(rendered).toEqual({
      kind: "ok",
      tlsPorts: [],
      tlsWithoutPort: 1,
      text: '{\n  "imposters": [\n    {\n      "protocol": "https"\n    }\n  ]\n}\n',
    });
  });

  it("hands back the route's bytes when there is nothing to remove", () => {
    // Nothing to take out means nothing to rewrite: the default export of a fleet with no inline
    // material is the route's document, digit for digit.
    const raw = '{"imposters":[{"port":4547,"protocol":"http","stubs":[{"responses":[{"is":{"body":{"id":1234567890123456789,"ratio":1.0}}}]}]}]}';
    expect(renderSetExport(raw, false)).toEqual({ kind: "ok", tlsPorts: [], tlsWithoutPort: 0, text: raw });
    expect(renderSetExport('{"imposters":[]}', false)).toEqual({
      kind: "ok",
      tlsPorts: [],
      tlsWithoutPort: 0,
      text: '{"imposters":[]}',
    });
  });

  // Pins D-84: removing the pair rewrites the document, and the rewrite changes nothing else — not
  // an integer past 2^53, not `1.0`, not `1e3`. `JSON.parse` alone would turn the first into
  // 1234567890123456800 and the others into 1 and 1000, with no error anywhere.
  it("keeps every number's own digits when it rewrites a document to remove the pair", () => {
    const raw =
      '{"imposters":[{"port":4545,"protocol":"https","cert":"C","key":"K","stubs":[{"responses":[{"is":{"body":{"id":1234567890123456789,"ratio":1.0,"big":1e3,"neg":-0,"plain":7}}}]}]}]}';
    expect(renderSetExport(raw, false)).toEqual({
      kind: "ok",
      tlsPorts: [4545],
      tlsWithoutPort: 0,
      text: `{
  "imposters": [
    {
      "port": 4545,
      "protocol": "https",
      "stubs": [
        {
          "responses": [
            {
              "is": {
                "body": {
                  "id": 1234567890123456789,
                  "ratio": 1.0,
                  "big": 1e3,
                  "neg": -0,
                  "plain": 7
                }
              }
            }
          ]
        }
      ]
    }
  ]
}
`,
    });
    expect(renderImposterExport(raw, 4545, false)).toMatchObject({ kind: "ok" });
    const one = renderImposterExport(raw, 4545, false);
    if (one.kind !== "ok") throw new Error("expected an export");
    expect(one.text).toContain('"id": 1234567890123456789,');
    expect(one.text).toContain('"ratio": 1.0,');
    // Duplicate reads through the same selection, and POSTs what it read.
    const selected = selectImposter(raw, 4545);
    if (selected.kind !== "ok") throw new Error("expected a selection");
    expect(selected.text).toContain('"id": 1234567890123456789,');
  });

  it("does not mistake a lone out-of-range number for an imposter", () => {
    expect(renderSetExport('{"imposters":[1.0]}', false).kind).toBe("error");
  });

  describe("in a browser that cannot parse JSON without rounding", () => {
    // `JSON.rawJSON` and the reviver's `source` shipped together (Chrome 114, Firefox 135,
    // Safari 18.4). Without them a rewrite could change a number and say nothing.
    let rawJSON: unknown;
    beforeEach(() => {
      rawJSON = Reflect.get(JSON, "rawJSON");
      Reflect.deleteProperty(JSON, "rawJSON");
    });
    afterEach(() => {
      Reflect.set(JSON, "rawJSON", rawJSON);
    });

    // Pins D-84: an export that would need a rewrite is refused, not rounded.
    it("refuses an export that would have to rewrite the document", () => {
      const rendered = renderSetExport(SET_TEXT, false);
      expect(rendered.kind).toBe("error");
      if (rendered.kind !== "error") return;
      expect(rendered.message).toMatch(/curl/);
      expect(renderImposterExport(SET_TEXT, 4546, false).kind).toBe("error");
    });

    it("still exports what needs no rewrite", () => {
      expect(renderSetExport(SET_TEXT, true)).toEqual({
        kind: "ok",
        tlsPorts: [4545],
        tlsWithoutPort: 0,
        text: SET_TEXT,
      });
      const bare = JSON.stringify({ imposters: [HTTP] });
      expect(renderSetExport(bare, false)).toEqual({ kind: "ok", tlsPorts: [], tlsWithoutPort: 0, text: bare });
    });

    it("still duplicates, as it did before exports learned to keep digits", () => {
      expect(selectImposter(SET_TEXT, 4545).kind).toBe("ok");
    });
  });

  it("refuses a document it cannot read, in both modes, rather than downloading it", () => {
    // Fail closed: with the option off, passing unreadable bytes through would be passing through
    // whatever key they hold.
    expect(renderSetExport("{not json", false).kind).toBe("error");
    expect(renderSetExport("{not json", true).kind).toBe("error");
    expect(renderSetExport('"a string"', false).kind).toBe("error");
  });

  it("strips one selected imposter the same way", () => {
    expect(renderImposterExport(SET_TEXT, 4545, false)).toEqual({
      kind: "ok",
      tlsPorts: [4545],
      tlsWithoutPort: 0,
      text: `{
  "port": 4545,
  "protocol": "https",
  "name": "billing",
  "mutualAuth": true,
  "rejectUnauthorized": true,
  "ca": [
    "-----BEGIN CERTIFICATE-----\\nCA\\n-----END CERTIFICATE-----\\n"
  ],
  "stubs": []
}
`,
    });
  });

  it("keeps one selected imposter's material when asked, and reports it", () => {
    const rendered = renderImposterExport(SET_TEXT, 4545, true);
    expect(rendered.kind).toBe("ok");
    if (rendered.kind !== "ok") return;
    expect(rendered.tlsPorts).toEqual([4545]);
    expect(JSON.parse(rendered.text)).toEqual(HTTPS);
  });

  it("reports nothing for a selected imposter with no material of its own", () => {
    expect(renderImposterExport(SET_TEXT, 4546, false)).toEqual({
      kind: "ok",
      tlsPorts: [],
      tlsWithoutPort: 0,
      text: '{\n  "port": 4546,\n  "protocol": "https",\n  "stubs": []\n}\n',
    });
  });

  it("says so when the selected port is not in the set", () => {
    const rendered = renderImposterExport(SET_TEXT, 9999, false);
    expect(rendered.kind).toBe("error");
    if (rendered.kind !== "error") return;
    expect(rendered.message).toContain("9999");
  });

  // Pins D-84: `selectImposter` feeds Duplicate. Stripping there would quietly turn a pinned-cert
  // copy into one serving the fleet default, in a fleet that already holds the key.
  it("leaves the duplicate path's selection alone — a clone in the same fleet keeps its key", () => {
    const selected = selectImposter(SET_TEXT, 4545);
    if (selected.kind !== "ok") throw new Error("expected a selection");
    expect(JSON.parse(selected.text)).toEqual(HTTPS);
  });

  it("shows a curl that produces the same file: a jq filter when material is removed", () => {
    const off = { replayable: true, removeProxies: true, tls: false };
    const on = { ...off, tls: true };
    expect(exportCurl({ kind: "all" }, off)).toBe(
      "curl -s '/imposters?replayable=true&removeProxies=true' | jq '.imposters[] |= del(.cert, .key)' > imposters.json",
    );
    expect(exportCurl({ kind: "all" }, on)).toBe(
      "curl -s '/imposters?replayable=true&removeProxies=true' > imposters.json",
    );
  });

  it("shows a one-imposter curl that selects from the set route, as the console does", () => {
    // `/imposters/:port` ignores `replayable` and answers with the request journal; the preview
    // must not name a route the console deliberately does not use.
    const off = { replayable: true, removeProxies: false, tls: false };
    expect(exportCurl({ kind: "one", port: 4545 }, off)).toBe(
      "curl -s '/imposters?replayable=true&removeProxies=false' | jq '.imposters[] | select(.port == 4545) | del(.cert, .key)' > imposter-4545.json",
    );
    expect(exportCurl({ kind: "one", port: 4545 }, { ...off, tls: true })).toBe(
      "curl -s '/imposters?replayable=true&removeProxies=false' | jq '.imposters[] | select(.port == 4545)' > imposter-4545.json",
    );
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

  it("imports a number exactly as the file spells it", () => {
    // A fixture's `1234567890123456789` must reach the fleet as that, not as the nearest double.
    const text = '{"imposters":[{"port":4545,"stubs":[{"responses":[{"is":{"body":{"id":1234567890123456789,"ratio":1.0}}}]}]}]}';
    expect(JSON.stringify(renderSetDocument(entriesOf(text)))).toBe(text);
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
