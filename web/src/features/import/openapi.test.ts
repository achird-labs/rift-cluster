import { describe, expect, it } from "vitest";

import { ApiError } from "../../api/client.ts";
import { compileSpecPath } from "../../api/paths.ts";
import {
  MAX_SPEC_BYTES,
  type SpecCompileResult,
  compileFailureText,
  parsePort,
  preflight,
  specContentType,
  summarize,
} from "./openapi.ts";

describe("which media type an OpenAPI document is declared as", () => {
  it("trusts the filename when there is one", () => {
    expect(specContentType("{}", "petstore.json")).toBe("application/json");
    expect(specContentType("openapi: 3.0.3", "petstore.yaml")).toBe("application/yaml");
    expect(specContentType("openapi: 3.0.3", "PETSTORE.YML")).toBe("application/yaml");
  });

  it("sniffs a pasted document from its first significant character", () => {
    expect(specContentType('  \n{"openapi":"3.0.3"}')).toBe("application/json");
    expect(specContentType("[]")).toBe("application/json");
    expect(specContentType("openapi: 3.0.3\ninfo:\n  title: x")).toBe("application/yaml");
    expect(specContentType("# a comment first\nopenapi: 3.0.3")).toBe("application/yaml");
  });

  it("lets the filename win over the bytes, because the operator named it", () => {
    // A `.yaml` file whose body happens to be a JSON flow mapping is still YAML, and the header
    // should say what the file says. Declarative only — the server sniffs regardless.
    expect(specContentType('{"openapi":"3.0.3"}', "spec.yaml")).toBe("application/yaml");
  });
});

describe("the port field", () => {
  it("accepts a whole number in 1–65535 and nothing else", () => {
    expect(parsePort("4545")).toBe(4545);
    expect(parsePort(" 1 ")).toBe(1);
    expect(parsePort("65535")).toBe(65535);
    expect(parsePort("0")).toBeNull();
    expect(parsePort("65536")).toBeNull();
    expect(parsePort("45.5")).toBeNull();
    expect(parsePort("-1")).toBeNull();
    expect(parsePort("")).toBeNull();
    expect(parsePort("abc")).toBeNull();
  });
});

describe("the compile request's query", () => {
  it("always carries the port and carries the name only when one was given", () => {
    // The route requires `port` (D-72: nothing stored means nothing to infer a binding from) and
    // reads an absent and an empty `name` the same, so blank is left off rather than sent as `name=`.
    expect(compileSpecPath(4545, "")).toBe("/specs/compile?port=4545");
    expect(compileSpecPath(4545, "   ")).toBe("/specs/compile?port=4545");
    expect(compileSpecPath(4545, "pet store")).toBe("/specs/compile?port=4545&name=pet+store");
    expect(compileSpecPath(8080, "a&b=c")).toBe("/specs/compile?port=8080&name=a%26b%3Dc");
  });
});

describe("what the console refuses without a round trip", () => {
  it("refuses an empty box and a body past the route's cap, and nothing else", () => {
    expect(preflight("")).toMatch(/paste|choose a file/i);
    expect(preflight("   \n")).toMatch(/paste|choose a file/i);
    expect(preflight("openapi: 3.0.3")).toBeNull();
    // Not valid OpenAPI — but that is the compiler's refusal to make, verbatim, not this one's.
    expect(preflight("{ not even json")).toBeNull();
    expect(preflight("x".repeat(MAX_SPEC_BYTES))).toBeNull();
    expect(preflight("x".repeat(MAX_SPEC_BYTES + 1))).toMatch(/4\.0 MiB/);
  });

  it("measures bytes, not characters — the cap is the server's, and the server counts bytes", () => {
    // Two-byte characters: half the count reaches the cap.
    const twoByte = "é".repeat(MAX_SPEC_BYTES / 2 + 1);
    expect(preflight(twoByte)).not.toBeNull();
  });
});

describe("summarising a compile result", () => {
  const RESULT: SpecCompileResult = {
    imposter: {
      port: 4545,
      protocol: "http",
      name: "petstore",
      stubs: [{ responses: [] }, { responses: [] }, { responses: [] }],
    },
    operations: [
      { id: "listPets", method: "get", pathTemplate: "/pets", stubIds: ["s1", "s2"] },
      { id: "getPet", method: "get", pathTemplate: "/pets/{petId}", stubIds: ["s3"] },
    ],
  };

  it("reads the port, name, stub count and operations off the compiled imposter", () => {
    expect(summarize(RESULT)).toEqual({
      port: 4545,
      name: "petstore",
      stubCount: 3,
      operations: RESULT.operations,
    });
  });

  it("reports an unreadable stubs field as unknown, never as zero", () => {
    // `imposter` is opaque in the contract, so a compiler that emitted `stubs` in some other shape
    // would otherwise be summarised as "0 stubs" — a number the console invented.
    expect(summarize({ ...RESULT, imposter: { port: 4545 } }).stubCount).toBeNull();
    expect(summarize({ ...RESULT, imposter: { port: 4545, stubs: "three" } }).stubCount).toBeNull();
    expect(summarize({ ...RESULT, imposter: { port: 4545, stubs: [] } }).stubCount).toBe(0);
  });

  it("reports a missing or malformed port as none, which the review step refuses to create", () => {
    expect(summarize({ ...RESULT, imposter: { name: "x" } }).port).toBeNull();
    expect(summarize({ ...RESULT, imposter: { port: "4545" } }).port).toBeNull();
    expect(summarize({ ...RESULT, imposter: { port: 45.5 } }).port).toBeNull();
    expect(summarize({ ...RESULT, imposter: { port: 4545, name: "" } }).name).toBeNull();
  });
});

describe("the sentence a failed compile shows", () => {
  it("is the compiler's own refusal for a 400, verbatim", () => {
    // The route's contract: "the compiler's refusals … are this route's 400 verbatim". That text is
    // the diagnosis — an unsupported version, the external $ref, where the parse broke.
    expect(compileFailureText(new ApiError(400, "unsupported OpenAPI version 2.0"))).toBe(
      "unsupported OpenAPI version 2.0",
    );
  });

  it("names the size cap for a 413, with the server's words when it sent any", () => {
    expect(compileFailureText(new ApiError(413, ""))).toMatch(/too large.*4 MiB/);
    expect(compileFailureText(new ApiError(413, "body exceeds 4194304 bytes"))).toMatch(
      /too large.*body exceeds 4194304 bytes/,
    );
  });

  it("does not render a bodiless refusal as an empty string", () => {
    expect(compileFailureText(new ApiError(400, ""))).toMatch(/refused.*400/);
  });

  it("passes any other error through as its own message", () => {
    expect(compileFailureText(new TypeError("Failed to fetch"))).toBe("Failed to fetch");
    expect(compileFailureText("odd")).toBe("odd");
  });
});
