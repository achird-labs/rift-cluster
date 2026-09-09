import { describe, expect, it } from "vitest";

import { ApiError } from "../../api/client.ts";
import { compileSpecPath } from "../../api/paths.ts";
import {
  MAX_SPEC_BYTES,
  type SpecCompileResult,
  compileFailureText,
  fileProblem,
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

  it("looks past a byte-order mark and a YAML document start", () => {
    // Two prefixes an operator does not type and cannot see, and both of them land on the sniff.
    //
    // A BOM: an editor on Windows writes `﻿` in front of the document, and a naive
    // first-character read would call the JSON one YAML. `trimStart` drops it because U+FEFF is
    // WhiteSpace in the spec's own sense — pinned here because that is a fact about the language,
    // not about this function, and a hand-rolled trim would not have it.
    expect(specContentType('﻿{"openapi":"3.0.3"}')).toBe("application/json");
    expect(specContentType("﻿openapi: 3.0.3")).toBe("application/yaml");

    // `---` is YAML's document start, and the one document that begins with punctuation while
    // being emphatically not JSON. It must not be mistaken for one.
    expect(specContentType("---\nopenapi: 3.0.3\ninfo:\n  title: x")).toBe("application/yaml");
    expect(specContentType("﻿---\nopenapi: 3.0.3")).toBe("application/yaml");
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
  });

  it("percent-encodes the name, which is what the route decodes", () => {
    /*
     * `encodeURIComponent` output, and the contract says so: `?name=` is RFC 3986 percent-encoded
     * and `admin_front.rs::percent_decode` reverses exactly this at the read — `%XX` back to a
     * byte, and nothing else touched.
     *
     * These strings are the contract's two halves held together. The server read the value raw
     * until this PR, so `pet store` named an imposter literally called `pet%20store` and `a&b=c`
     * was cut at the `&` by the parameter split before the name was read at all. There is no
     * encoding that avoids both: an unescaped `&` cannot cross a query string at all, so the
     * route decodes and the console encodes. (`=` and `%` are escaped along with it because one
     * rule is easier to hold than three exceptions — a bare `=` would in fact survive the split,
     * and a bare `%` would reach the server and be refused as a malformed escape.)
     * `crates/rift-cluster-server/tests/spec_compile.rs`'s
     * `a_percent_encoded_name_reaches_the_compiled_imposter_verbatim` asserts the other end.
     */
    expect(compileSpecPath(4545, "pet store")).toBe("/specs/compile?port=4545&name=pet%20store");
    expect(compileSpecPath(8080, "a&b=c")).toBe("/specs/compile?port=8080&name=a%26b%3Dc");
    // A literal percent is escaped too, or `100%` would arrive as an escape eating what follows.
    expect(compileSpecPath(4545, "100% mock")).toBe("/specs/compile?port=4545&name=100%25%20mock");
    // Non-ASCII goes out one escape per UTF-8 byte.
    expect(compileSpecPath(4545, "café")).toBe("/specs/compile?port=4545&name=caf%C3%A9");
  });

  it("never spells a space as `+`, and escapes a real plus", () => {
    // The one place RFC 3986 and `x-www-form-urlencoded` disagree. `URLSearchParams` would send
    // `pet+store` for a space, which a server decoding by RFC 3986 takes literally — that was the
    // imposter named `Pet+Store`. And a `+` the operator typed must survive as one: `C++` is a
    // name, and it goes out as `%2B%2B`, never bare, so no decoder on either standard can read it
    // as spaces.
    expect(compileSpecPath(4545, "pet store")).not.toContain("+");
    expect(compileSpecPath(4545, "C++")).toBe("/specs/compile?port=4545&name=C%2B%2B");
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

/** A refusal exactly as the admin plane sends one — the `Error` schema, not a bare string. */
const envelope = (type: string, message: string): string =>
  JSON.stringify({ errors: [{ code: "400", type, message }] });

describe("the sentence a failed compile shows", () => {
  it("is the compiler's own refusal for a 400, unwrapped from the Error envelope", () => {
    // The route's contract: "the compiler's refusals … are this route's 400 verbatim". That text is
    // the diagnosis — an unsupported version, the external $ref, where the parse broke — and it
    // arrives inside the declared `Error` envelope, which is punctuation the operator does not
    // need in front of it.
    expect(
      compileFailureText(
        new ApiError(400, envelope("bad_data", "spec does not compile: unsupported version 2.0")),
      ),
    ).toBe("spec does not compile: unsupported version 2.0");
  });

  it("falls back to the raw body when it is not the declared envelope", () => {
    // A route that one day answers text/plain, or an envelope missing its message, must not have
    // its refusal swallowed into a generic sentence.
    expect(compileFailureText(new ApiError(400, "unsupported OpenAPI version 2.0"))).toBe(
      "unsupported OpenAPI version 2.0",
    );
    expect(compileFailureText(new ApiError(400, '{"errors":[]}'))).toBe('{"errors":[]}');
    expect(compileFailureText(new ApiError(400, '{"errors":[{"message":"  "}]}'))).toBe(
      '{"errors":[{"message":"  "}]}',
    );
  });

  it("names the size cap for a 413, with the server's words when it sent any", () => {
    expect(compileFailureText(new ApiError(413, ""))).toMatch(/too large.*4 MiB/);
    expect(
      compileFailureText(new ApiError(413, envelope("request_too_large", "spec exceeds 4194304 bytes"))),
    ).toBe("The document is too large for the fleet to compile: spec exceeds 4194304 bytes");
  });

  it("does not render a bodiless refusal as an empty string", () => {
    expect(compileFailureText(new ApiError(400, ""))).toMatch(/refused.*400/);
  });

  it("hands 401, 403 and 503 to the console's own guidance, not the envelope", () => {
    // None of the three is about the document: a lapsed session, a refusing front, a node that is
    // not ready. `describe` is what every other screen says about them, and this dialog must not
    // invent a second vocabulary for the same three facts — least of all one that renders a JSON
    // envelope where "Sign in again." belongs.
    expect(compileFailureText(new ApiError(401, envelope("unauthorized", "no session")))).toMatch(
      /401.*sign in again/i,
    );
    expect(compileFailureText(new ApiError(403, envelope("forbidden", "nope")))).toMatch(
      /403.*refused/i,
    );
    expect(compileFailureText(new ApiError(503, envelope("unavailable", "starting")))).toMatch(
      /503.*not ready/i,
    );
  });

  it("passes any other error through as its own message", () => {
    expect(compileFailureText(new TypeError("Failed to fetch"))).toBe("Failed to fetch");
    expect(compileFailureText("odd")).toBe("odd");
  });
});

describe("refusing a chosen file before it is read", () => {
  it("refuses on the file's own byte count, and only past the cap", () => {
    // `File.size` is known without opening the file, so this verdict costs nothing — the point of
    // having it at all rather than letting `preflight` reach the same one after the read.
    expect(fileProblem("petstore.yaml", 1024)).toBeNull();
    expect(fileProblem("petstore.yaml", MAX_SPEC_BYTES)).toBeNull();
    const refusal = fileProblem("petstore.yaml", MAX_SPEC_BYTES + 1);
    expect(refusal).toMatch(/petstore\.yaml/);
    expect(refusal).toMatch(/4\.0 MiB/);
  });
});
