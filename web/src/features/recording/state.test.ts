import { describe, expect, it } from "vitest";

import type { components } from "../../api/schema.ts";
import { DEFAULT_GENERATOR_FIELDS, GENERATOR_FIELDS, proxyStubFor, recordingState, responseFields } from "./state.ts";

type Stub = components["schemas"]["Stub"];

// The fleet's response is not bound by the contract's types, which is the point of the guard.
const stub = (value: unknown): Stub => value as Stub;

describe("recordingState", () => {
  it("is empty for an imposter with no stubs, loaded or not", () => {
    expect(recordingState(undefined)).toBe("empty");
    expect(recordingState([])).toBe("empty");
  });

  it("is replaying when no stub carries a proxy", () => {
    expect(
      recordingState([
        stub({ responses: [{ is: { statusCode: 200 } }] }),
        stub({ predicates: [{ equals: { path: "/p" } }] }),
      ]),
    ).toBe("replaying");
  });

  it("is recording when any response of any stub declares a proxy, even a malformed one", () => {
    expect(
      recordingState([
        stub({ responses: [{ is: {} }] }),
        stub({ responses: [{ is: {} }, { proxy: null }] }),
      ]),
    ).toBe("recording");
  });

  it("does not throw on a null or non-object response, and does not count it as a proxy", () => {
    // `"proxy" in null` throws; the guard is what keeps a detail load from crashing on it.
    expect(recordingState([stub({ responses: [null, "proxy", ["proxy"], 7] })])).toBe("replaying");
    expect(recordingState([stub({ responses: [null, { proxy: { to: "http://up" } }] })])).toBe(
      "recording",
    );
  });

  it("is replaying when a stub has no responses key at all", () => {
    expect(recordingState([stub({ id: "bare" })])).toBe("replaying");
  });
});

describe("proxyStubFor", () => {
  it("writes the selected match fields as a whitelist, never `false` for the rest", () => {
    expect(
      proxyStubFor({
        to: "http://up:8080",
        mode: "proxyOnce",
        fields: ["method", "path"],
        caseSensitive: false,
      }),
    ).toEqual({
      responses: [
        {
          proxy: {
            to: "http://up:8080",
            mode: "proxyOnce",
            predicateGenerators: [{ matches: { method: true, path: true }, caseSensitive: false }],
          },
        },
      ],
    });
  });

  it("defaults to method, path and query — a subset of the fields the engine can match", () => {
    expect(DEFAULT_GENERATOR_FIELDS).toEqual(["method", "path", "query"]);
    for (const field of DEFAULT_GENERATOR_FIELDS) expect(GENERATOR_FIELDS).toContain(field);
  });
});

describe("responseFields", () => {
  it("unwraps `is`, and prefers it when both forms are present", () => {
    expect(responseFields({ is: { statusCode: 201 } })).toEqual({ statusCode: 201 });
    expect(responseFields({ is: { statusCode: 201 }, statusCode: 500 })).toEqual({ statusCode: 201 });
  });

  it("returns the flat form, and anything that is not an object, unchanged", () => {
    expect(responseFields({ statusCode: 204 })).toEqual({ statusCode: 204 });
    expect(responseFields(null)).toBeNull();
  });
});
