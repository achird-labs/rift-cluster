import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

/**
 * The two stub-writing routes take different bodies, and the console sent the wrong one.
 *
 * `POST /imposters/:port/stubs` requires `{"stub": …}`; the by-id `PUT` takes a bare `Stub`. The
 * console sent the bare stub to both, so **appending a stub never worked** — every attempt answered
 * `400 missing field 'stub'`. It shipped in C5 and was found by hand.
 *
 * The reason no existing test caught it is worth stating, because it generalises: every other stub
 * test stubs `fetch`, so it asserts what the client *sends*. That is exactly the value that was
 * wrong. A mock cannot disagree with the client about the contract — only the contract can.
 *
 * So this reads `openapi-ee.yaml` directly and asserts the shape the console builds against the
 * shape the fleet publishes. It is deliberately a source-level check rather than a rendering test:
 * the mismatch lives in one line of `queries.ts`, and that is where it should fail.
 */

const WEB = new URL("..", import.meta.url).pathname;
const CONTRACT = readFileSync(join(WEB, "..", "..", "docs", "api", "openapi-ee.yaml"), "utf8");
const QUERIES = readFileSync(join(WEB, "app", "queries.ts"), "utf8");

/** The block of the contract for one path, sliced to the next top-level path key. */
function pathBlock(path: string): string {
  const start = CONTRACT.indexOf(`\n  ${path}:`);
  if (start === -1) throw new Error(`the contract publishes no ${path}`);
  const next = CONTRACT.slice(start + 1).search(/\n {2}\/[^\s:]*:/);
  return next === -1 ? CONTRACT.slice(start) : CONTRACT.slice(start, start + 1 + next);
}

describe("the stub write bodies match the contract", () => {
  it("addStub requires a { stub } envelope", () => {
    const block = pathBlock("/imposters/{port}/stubs");
    const post = block.slice(block.indexOf("post:"));
    expect(post).toMatch(/required:\s*\[stub\]/);
  });

  it("the console wraps the append body in that envelope, verbatim", () => {
    // Textual wrapping, not parse-and-reserialise: the operator's own bytes are the document, and a
    // round trip through JSON.parse would normalise the key order and whitespace they chose.
    expect(QUERIES).toContain('`{"stub":${body.text}}`');
  });

  it("replaceStubById takes a bare Stub, so the by-id write must not be wrapped", () => {
    const block = pathBlock("/imposters/{port}/stubs/by-id/{stubId}");
    const put = block.slice(block.indexOf("put:"), block.indexOf("responses:", block.indexOf("put:")));
    expect(put).toMatch(/\$ref: '#\/components\/schemas\/Stub'/);
    expect(put).not.toMatch(/required:\s*\[stub\]/);

    // And the console sends `write.body` straight through on that path.
    const byId = QUERIES.slice(QUERIES.indexOf("export function usePutStub"));
    expect(byId.slice(0, 400)).toContain("stubByIdPath(write.port, write.stubId), write.body");
  });
});
