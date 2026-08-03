import { describe, expect, it } from "vitest";

import type { components } from "../../api/schema.ts";
import {
  EMPTY_QUERY,
  type ImposterQuery,
  actionablePorts,
  classifyRecording,
  decodeQuery,
  encodeQuery,
  filterImposters,
  isEmptyQuery,
  matchesText,
  sortImposters,
  sourceOwnedPorts,
  stubCount,
  unclassifiedCount,
  visibleImposters,
} from "./list.ts";

type Imposter = components["schemas"]["Imposter"];
type Stub = components["schemas"]["Stub"];

const PROXY_STUB: Stub = { responses: [{ proxy: { to: "https://upstream.example" } }] };
const STATIC_STUB: Stub = { responses: [{ is: { statusCode: 200 } }] };

/**
 * `undefined` here means "this field was ABSENT from the response", which under
 * `exactOptionalPropertyTypes` is a different thing from "not overridden" — and the difference is
 * most of what this suite tests. So the overrides accept an explicit `undefined` and the key is
 * then deleted, producing the object the API would actually have returned.
 */
type Overrides = { [K in keyof Imposter]?: Imposter[K] | undefined };

function imposter(over: Overrides = {}): Imposter {
  const merged: Record<string, unknown> = {
    port: 4545,
    protocol: "http",
    name: "checkout",
    recordRequests: false,
    enabled: true,
    ...over,
  };
  for (const [k, v] of Object.entries(merged)) if (v === undefined) delete merged[k];
  return merged as Imposter;
}

const query = (over: Partial<ImposterQuery> = {}): ImposterQuery => ({ ...EMPTY_QUERY, ...over });

describe("matchesText", () => {
  it("matches name, port and protocol case-insensitively", () => {
    const i = imposter({ name: "Checkout-API", port: 4545, protocol: "https" });
    expect(matchesText(i, "checkout")).toBe(true);
    expect(matchesText(i, "API")).toBe(true);
    expect(matchesText(i, "4545")).toBe(true);
    expect(matchesText(i, "HTTPS")).toBe(true);
    expect(matchesText(i, "billing")).toBe(false);
  });

  it("matches the port as a substring, so a half-typed port still finds it", () => {
    // The box is filtered on every keystroke; numeric equality would answer nothing until the
    // last digit, which is the same as having no filter for the whole time you are typing.
    expect(matchesText(imposter({ port: 4545 }), "45")).toBe(true);
    expect(matchesText(imposter({ port: 14500 }), "45")).toBe(true);
  });

  it("is true for empty or whitespace-only text", () => {
    expect(matchesText(imposter(), "")).toBe(true);
    expect(matchesText(imposter(), "   ")).toBe(true);
  });

  it("does not throw on an imposter missing every searchable field", () => {
    const bare = { recordRequests: false, enabled: true } as Imposter;
    expect(matchesText(bare, "anything")).toBe(false);
    expect(matchesText(bare, "")).toBe(true);
  });
});

describe("classifyRecording", () => {
  it("reads a proxy stub as a recording, via the shared derivation", () => {
    expect(classifyRecording(imposter({ stubs: [STATIC_STUB, PROXY_STUB] }))).toBe("has");
  });

  it("reads static-only stubs as none", () => {
    expect(classifyRecording(imposter({ stubs: [STATIC_STUB] }))).toBe("none");
    expect(classifyRecording(imposter({ stubs: [] }))).toBe("none");
  });

  it("reads an ABSENT stub list as unknown, not as none", () => {
    // The distinction the whole filter rests on: `stubs: undefined` means the response did not
    // carry them. Calling that "no recording" invents a fact about the imposter.
    expect(classifyRecording(imposter({ stubs: undefined }))).toBe("unknown");
  });
});

describe("stubCount", () => {
  it("is null when stubs are absent and a number when they are not", () => {
    expect(stubCount(imposter({ stubs: undefined }))).toBeNull();
    expect(stubCount(imposter({ stubs: [] }))).toBe(0);
    expect(stubCount(imposter({ stubs: [STATIC_STUB, PROXY_STUB] }))).toBe(2);
  });
});

describe("filterImposters", () => {
  const list = [
    imposter({ port: 4545, name: "checkout", enabled: true, stubs: [PROXY_STUB] }),
    imposter({ port: 4546, name: "billing", enabled: false, stubs: [STATIC_STUB] }),
    imposter({ port: 4547, name: "shipping", enabled: true, stubs: undefined }),
  ];

  it("narrows by text", () => {
    expect(filterImposters(list, query({ text: "bill" })).map((i) => i.port)).toEqual([4546]);
  });

  it("narrows by enabled state", () => {
    expect(filterImposters(list, query({ state: "enabled" })).map((i) => i.port)).toEqual([4545, 4547]);
    expect(filterImposters(list, query({ state: "disabled" })).map((i) => i.port)).toEqual([4546]);
  });

  it("narrows by recording, excluding rows it cannot classify from BOTH answers", () => {
    // 4547 appears in neither `has` nor `none`. That is the point of `unclassifiedCount` —
    // it is excluded because we do not know, and the screen has to say so.
    expect(filterImposters(list, query({ recording: "has" })).map((i) => i.port)).toEqual([4545]);
    expect(filterImposters(list, query({ recording: "none" })).map((i) => i.port)).toEqual([4546]);
  });

  it("combines filters conjunctively", () => {
    const result = filterImposters(list, query({ text: "c", state: "enabled", recording: "has" }));
    expect(result.map((i) => i.port)).toEqual([4545]);
  });

  it("returns everything for the empty query", () => {
    expect(filterImposters(list, EMPTY_QUERY)).toHaveLength(3);
  });
});

describe("owner filter", () => {
  const list = [
    imposter({ port: 4545, name: "from-git" }),
    imposter({ port: 4546, name: "hand-made" }),
    imposter({ port: 4547, name: "also-git" }),
  ];
  const owned = sourceOwnedPorts([{ ports: [4545] }, { ports: [4547] }]);

  it("unions every declared source's ports", () => {
    expect([...(owned ?? [])].sort()).toEqual([4545, 4547]);
  });

  it("is null when there is no reading of sources at all", () => {
    // Refused, unread or still loading — all the same fact, and all mean "cannot answer".
    expect(sourceOwnedPorts(undefined)).toBeNull();
  });

  it("is an empty set, NOT null, when the tenant declares no sources", () => {
    // A real reading of zero sources is knowledge: everything is hand-created. Collapsing it into
    // `null` would silently disable a filter that has a correct answer.
    expect(sourceOwnedPorts([])).toEqual(new Set());
  });

  it("narrows to source-owned and to hand-created", () => {
    expect(filterImposters(list, query({ owner: "source" }), owned).map((i) => i.port)).toEqual([4545, 4547]);
    expect(filterImposters(list, query({ owner: "hand" }), owned).map((i) => i.port)).toEqual([4546]);
  });

  it("is a NO-OP without a sources reading, never an answer of `all hand-created`", () => {
    // The failure this prevents: with nothing to join against, "hand-created" would match every
    // imposter — including the source-owned ones — and read as a confident, wrong answer.
    expect(filterImposters(list, query({ owner: "hand" }), null)).toHaveLength(3);
    expect(filterImposters(list, query({ owner: "source" }), null)).toHaveLength(3);
  });

  it("treats an imposter with no port as hand-created, since no source can own it", () => {
    const portless = [imposter({ port: undefined, name: "nameless" })];
    expect(filterImposters(portless, query({ owner: "hand" }), owned)).toHaveLength(1);
    expect(filterImposters(portless, query({ owner: "source" }), owned)).toHaveLength(0);
  });
});

describe("unclassifiedCount", () => {
  const list = [
    imposter({ port: 4545, stubs: [PROXY_STUB] }),
    imposter({ port: 4546, stubs: undefined }),
    imposter({ port: 4547, stubs: undefined, enabled: false }),
  ];

  it("is zero when no recording filter is applied", () => {
    expect(unclassifiedCount(list, EMPTY_QUERY)).toBe(0);
  });

  it("counts rows the recording filter could not classify", () => {
    expect(unclassifiedCount(list, query({ recording: "has" }))).toBe(2);
  });

  it("counts only rows that pass the TEXT filter", () => {
    const named = [
      imposter({ port: 4545, name: "billing", stubs: undefined }),
      imposter({ port: 4546, name: "checkout", stubs: undefined }),
    ];
    expect(unclassifiedCount(named, query({ recording: "has", text: "bill" }))).toBe(1);
  });

  it("counts only rows that pass the OWNER filter", () => {
    // The gap that shipped: an earlier version repeated the conjunction and left `owner` out, so a
    // row excluded because the operator asked for hand-created only was reported as "not shown
    // because we could not read its stubs" — the count that names the right reason, naming a wrong one.
    const mixed = [
      imposter({ port: 4545, stubs: undefined }),
      imposter({ port: 4546, stubs: undefined }),
    ];
    const owned = sourceOwnedPorts([{ ports: [4545] }]);
    expect(unclassifiedCount(mixed, query({ recording: "has", owner: "hand" }), owned)).toBe(1);
    expect(unclassifiedCount(mixed, query({ recording: "has", owner: "source" }), owned)).toBe(1);
    expect(unclassifiedCount(mixed, query({ recording: "has" }), owned)).toBe(2);
  });

  it("counts only rows that pass the OTHER filters", () => {
    // 4547 is disabled, so an enabled-only view never had it in scope — reporting it as
    // "hidden because we do not know" would be a second, wrong reason.
    expect(unclassifiedCount(list, query({ recording: "has", state: "enabled" }))).toBe(1);
  });
});

describe("sortImposters", () => {
  it("sorts by port ascending and descending", () => {
    const list = [imposter({ port: 4547 }), imposter({ port: 4545 }), imposter({ port: 4546 })];
    expect(sortImposters(list, "port", "asc").map((i) => i.port)).toEqual([4545, 4546, 4547]);
    expect(sortImposters(list, "port", "desc").map((i) => i.port)).toEqual([4547, 4546, 4545]);
  });

  it("sorts by name naturally, so port-suffixed names do not order lexically", () => {
    const list = [imposter({ name: "svc-10" }), imposter({ name: "svc-2" }), imposter({ name: "SVC-1" })];
    expect(sortImposters(list, "name", "asc").map((i) => i.name)).toEqual(["SVC-1", "svc-2", "svc-10"]);
  });

  it("sorts by stub count", () => {
    const list = [
      imposter({ port: 1, stubs: [STATIC_STUB, STATIC_STUB] }),
      imposter({ port: 2, stubs: [] }),
      imposter({ port: 3, stubs: [STATIC_STUB] }),
    ];
    expect(sortImposters(list, "stubs", "asc").map((i) => i.port)).toEqual([2, 3, 1]);
  });

  it("puts rows with no value LAST in both directions", () => {
    /*
     * Reversing the comparator would float the unknowns to the top of a descending sort.
     * "The ones we know least about, first" is never what a column header click meant.
     *
     * Sized and shaped deliberately. An earlier version of this test used three rows with the
     * missing one first, and a mutant that made absence sort FIRST still passed it: the mutation
     * makes the comparator self-contradictory (`a` before `b` AND `b` before `a`), and on a tiny
     * array V8's insertion sort happened to leave the expected order intact. Enough rows to reach
     * the real sort path, several missing values, and an assertion on the whole tail rather than on
     * one index, so no ordering accident can satisfy it.
     */
    const named = ["delta", "alpha", "echo", "bravo", "charlie", "foxtrot", "golf"];
    const list = [
      imposter({ port: 4600, name: undefined, stubs: undefined }),
      ...named.map((name, index) =>
        imposter({ port: 4500 + index, name, stubs: Array.from({ length: index }, () => STATIC_STUB) }),
      ),
      imposter({ port: 4601, name: undefined, stubs: undefined }),
      imposter({ port: 4602, name: undefined, stubs: undefined }),
    ];
    const missing = [4600, 4601, 4602];

    for (const key of ["name", "stubs"] as const) {
      for (const direction of ["asc", "desc"] as const) {
        const ports = sortImposters(list, key, direction).map((i) => i.port);
        expect(ports).toHaveLength(list.length);
        // The tail is exactly the rows with no value — every one of them, and nothing else.
        expect([...ports.slice(-missing.length)].sort()).toEqual(missing);
        // And the head is genuinely ordered, not merely "not missing".
        const head = sortImposters(list, key, direction).slice(0, named.length);
        const values =
          key === "name" ? head.map((i) => i.name ?? "") : head.map((i) => stubCount(i) ?? -1);
        const ordered = [...values].sort((a, b) =>
          typeof a === "number" && typeof b === "number"
            ? a - b
            : String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" }),
        );
        expect(values).toEqual(direction === "asc" ? ordered : [...ordered].reverse());
      }
    }
  });

  it("does not mutate its input", () => {
    const list = [imposter({ port: 4547 }), imposter({ port: 4545 })];
    sortImposters(list, "port", "asc");
    expect(list.map((i) => i.port)).toEqual([4547, 4545]);
  });
});

describe("visibleImposters", () => {
  it("filters then sorts", () => {
    const list = [
      imposter({ port: 4547, name: "svc-c", enabled: true, stubs: [] }),
      imposter({ port: 4545, name: "svc-a", enabled: false, stubs: [] }),
      imposter({ port: 4546, name: "svc-b", enabled: true, stubs: [] }),
    ];
    const result = visibleImposters(list, query({ state: "enabled", sort: "name", direction: "desc" }));
    expect(result.map((i) => i.port)).toEqual([4547, 4546]);
  });
});

describe("actionablePorts", () => {
  it("drops imposters with no port, because no admin call can address them", () => {
    const list = [imposter({ port: 4545 }), imposter({ port: undefined }), imposter({ port: 4546 })];
    expect(actionablePorts(list)).toEqual([4545, 4546]);
  });
});

describe("encodeQuery / decodeQuery", () => {
  it("writes nothing for the default view, so its URL stays plain", () => {
    expect(encodeQuery(EMPTY_QUERY)).toBe("");
    expect(decodeQuery("")).toEqual(EMPTY_QUERY);
  });

  it("round-trips every non-default field", () => {
    const full = query({ text: "checkout api", state: "disabled", recording: "has", owner: "source", sort: "stubs", direction: "desc" });
    expect(decodeQuery(encodeQuery(full))).toEqual(full);
  });

  it("round-trips text that needs escaping", () => {
    const odd = query({ text: "a&b=c d%25" });
    expect(decodeQuery(encodeQuery(odd))).toEqual(odd);
  });

  it("omits whitespace-only text, which filters nothing", () => {
    expect(encodeQuery(query({ text: "   " }))).toBe("");
  });

  it("falls back to the default for every unrecognised value", () => {
    // A stale or hand-edited bookmark is a normal thing to receive; the screen must still render.
    expect(decodeQuery("state=sideways&rec=maybe&owner=nobody&sort=colour&dir=widdershins")).toEqual(EMPTY_QUERY);
  });

  it("keeps the fields it can parse when others are junk", () => {
    expect(decodeQuery("q=bill&state=nonsense&sort=name")).toEqual(
      query({ text: "bill", sort: "name" }),
    );
  });
});

describe("isEmptyQuery", () => {
  it("recognises the default and rejects every single-field deviation", () => {
    expect(isEmptyQuery(EMPTY_QUERY)).toBe(true);
    expect(isEmptyQuery(query({ text: "x" }))).toBe(false);
    expect(isEmptyQuery(query({ state: "enabled" }))).toBe(false);
    expect(isEmptyQuery(query({ recording: "has" }))).toBe(false);
    expect(isEmptyQuery(query({ owner: "source" }))).toBe(false);
    expect(isEmptyQuery(query({ sort: "name" }))).toBe(false);
    expect(isEmptyQuery(query({ direction: "desc" }))).toBe(false);
  });
});
