import { ApiError } from "../../api/client.ts";
import type { components } from "../../api/schema.ts";

/**
 * The one-shot OpenAPI import (D-72, RFC-007 §3.1) — the pure half.
 *
 * The console hands an OpenAPI 3.0 document to `POST /specs/compile?port=…&name=…` and gets back
 * the imposter the compiler built, plus the operation index it built it from. Nothing about the
 * document is retained anywhere: the fleet stores no spec, mints no op and keeps no record. The
 * imposter reaches the log only when the operator takes the second step and the console
 * `POST /imposters` it through the same write path every other create takes.
 *
 * Free of React, like `features/imposters/portable.ts`: which media type a document is, what a
 * port field means, and what the compiler's answer amounts to are all worth deciding and testing
 * without a dialog attached.
 */

export type SpecCompileResult = components["schemas"]["SpecCompileResult"];
export type CompiledOperation = SpecCompileResult["operations"][number];

/** The two media types the route declares. The server sniffs the bytes; this is what we *say*. */
export type SpecContentType = "application/json" | "application/yaml";

/** The largest body the route accepts (`rift_cluster_spec::MAX_SPEC_BYTES`); a larger one answers `413`. */
export const MAX_SPEC_BYTES = 4 * 1024 * 1024;

/**
 * Which media type to declare for a document.
 *
 * The filename decides when there is one — an operator who saved `petstore.yaml` said what it is.
 * A pasted document has no name, so the bytes decide: JSON is the only one of the two whose first
 * significant character is `{` or `[` (a YAML document *can* start that way, as a flow mapping,
 * but such a document is also valid JSON in every case the compiler accepts). Everything else is
 * YAML, which is the superset.
 *
 * Declarative only: the server sniffs the bytes regardless (`openapi-ee.yaml`, `compileSpec`), so
 * a wrong answer here costs nothing on the wire. It is still worth getting right, because the
 * header is what a proxy log or a curious reviewer sees.
 */
export function specContentType(text: string, filename?: string): SpecContentType {
  const lower = (filename ?? "").toLowerCase();
  if (lower.endsWith(".json")) return "application/json";
  if (lower.endsWith(".yaml") || lower.endsWith(".yml")) return "application/yaml";
  const first = text.trimStart()[0];
  return first === "{" || first === "[" ? "application/json" : "application/yaml";
}

/**
 * The port field, as typed. `null` when it is not a whole number in 1–65535 — the same rule the
 * create wizard applies, and the one the route enforces (`minimum: 1`, `maximum: 65535`).
 */
export function parsePort(text: string): number | null {
  const trimmed = text.trim();
  if (!/^\d+$/.test(trimmed)) return null;
  const port = Number(trimmed);
  return Number.isInteger(port) && port >= 1 && port <= 65535 ? port : null;
}

/**
 * Whether a document is worth sending at all.
 *
 * Only the two refusals the console can see coming without a round trip: an empty box, and a body
 * past the route's size cap. Everything else — unsupported version, external `$ref`, a parse
 * failure — is the compiler's call, and its `400` carries the reason verbatim.
 */
export function preflight(text: string): string | null {
  if (text.trim().length === 0) return "Paste an OpenAPI document or choose a file.";
  const bytes = new TextEncoder().encode(text).byteLength;
  if (bytes > MAX_SPEC_BYTES) {
    return `This document is ${formatBytes(bytes)}; the fleet accepts at most ${formatBytes(MAX_SPEC_BYTES)}.`;
  }
  return null;
}

function formatBytes(bytes: number): string {
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(0)} KiB`;
  return `${String(bytes)} B`;
}

/** What the review step shows: the facts about the compiled imposter worth a glance before creating it. */
export type CompileSummary = {
  /** The port the compiled config binds — `null` if the compiler emitted none, which the create would refuse. */
  port: number | null;
  name: string | null;
  /** How many stubs the config carries; `null` when `stubs` is not an array, which is unknown rather than zero. */
  stubCount: number | null;
  operations: readonly CompiledOperation[];
};

/**
 * Read the facts off a compile result without trusting its shape.
 *
 * `imposter` is carried opaquely by the contract (a compiler that learns a field must not need the
 * schema changed first), so every read here is a check, not a cast. An absent or malformed
 * `stubs` is reported as unknown, never as `0`: a review step that said "0 stubs" for a config it
 * could not read would be inventing a number.
 */
export function summarize(result: SpecCompileResult): CompileSummary {
  const imposter = result.imposter;
  const port = imposter["port"];
  const name = imposter["name"];
  const stubs = imposter["stubs"];
  return {
    port: typeof port === "number" && Number.isInteger(port) ? port : null,
    name: typeof name === "string" && name.length > 0 ? name : null,
    stubCount: Array.isArray(stubs) ? stubs.length : null,
    operations: result.operations,
  };
}

/**
 * The sentence a failed compile shows.
 *
 * The compiler's refusals are the route's `400` verbatim, and that text is the diagnosis — an
 * unsupported version, the external `$ref` it will not follow, where the parse broke — so it is
 * shown as the server wrote it. `413` is the one status whose body may say nothing useful, so it
 * gets the console's own sentence with the server's appended when there is one. Anything else is
 * whatever the error says about itself.
 */
export function compileFailureText(error: unknown): string {
  if (error instanceof ApiError) {
    const body = error.body.trim();
    if (error.status === 413) {
      return body.length > 0
        ? `The document is too large for the fleet to compile: ${body}`
        : "The document is too large for the fleet to compile (the limit is 4 MiB).";
    }
    return body.length > 0 ? body : `The fleet refused the document (${String(error.status)}).`;
  }
  return error instanceof Error ? error.message : String(error);
}
