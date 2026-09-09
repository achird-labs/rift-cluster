import { ApiError } from "../../api/client.ts";
import type { components } from "../../api/schema.ts";
import { describe } from "../../components/primitives.tsx";

/**
 * The one-shot OpenAPI import (D-72, RFC-007 §3.1) — the pure half.
 *
 * The console hands an OpenAPI 3.0 document to `POST /specs/compile?port=…&name=…` and gets back
 * the imposter the compiler built, plus the operation index it built it from. Nothing about the
 * document is retained anywhere: the fleet stores no spec, mints no op and keeps no record. The
 * imposter reaches the log only when the operator takes the second step and the console
 * `POST /imposters` it through the same write path every other create takes.
 *
 * No components and no DOM, like `features/imposters/portable.ts`: which media type a document is,
 * what a port field means, and what the compiler's answer amounts to are all worth deciding and
 * testing without a dialog attached. (It borrows `describe` from `primitives.tsx` for the status
 * sentences, exactly as `features/imposters/bulk.ts` does, so the console has one voice for a 401.)
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

/**
 * Why a chosen file is refused before a byte of it is read, or `null` when it is worth reading.
 *
 * `File.size` is exactly the byte count the route caps, and the browser knows it without opening
 * the file — so a 300 MB document is refused for free, instead of after being pulled into a string
 * the dialog would immediately throw away. `preflight` would reach the same verdict; it just has
 * to have the text first.
 */
export function fileProblem(name: string, bytes: number): string | null {
  if (bytes > MAX_SPEC_BYTES) {
    return `${name} is ${formatBytes(bytes)}; the fleet accepts at most ${formatBytes(MAX_SPEC_BYTES)}.`;
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
 * The one sentence a refusal body actually carries, or `null` when the body is not one.
 *
 * Every refusal on this admin plane is the declared `Error` envelope —
 * `{"errors":[{"code","type","message"}]}` — and `message` is the whole diagnosis. Rendering the
 * envelope instead would put JSON punctuation on screen in front of the sentence the operator
 * needs, on the one surface whose error text *is* the feature.
 *
 * Every read is a check rather than a cast, and a body that does not parse or does not have the
 * shape returns `null` so the caller can fall back to the raw text: a route that one day answers
 * `text/plain` must not have its refusal swallowed into "the fleet refused the document".
 */
function envelopeMessage(body: string): string | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    // A domain-optional parse: plenty of things are not JSON, and the caller shows the raw body.
    return null;
  }
  const errors = (parsed as { errors?: unknown } | null)?.errors;
  if (!Array.isArray(errors)) return null;
  const message = (errors[0] as { message?: unknown } | undefined)?.message;
  return typeof message === "string" && message.trim().length > 0 ? message.trim() : null;
}

/**
 * The sentence a failed compile shows.
 *
 * The compiler's refusals are the route's `400`, and that text is the diagnosis — an unsupported
 * version, the external `$ref` it will not follow, where the parse broke — so it is shown as the
 * server wrote it, unwrapped from the `Error` envelope it arrives in. `413` is the one status
 * whose body may say nothing useful, so it gets the console's own sentence with the server's
 * appended when there is one.
 *
 * `401`, `403` and `503` are **not** about the document at all — a lapsed session, a refusing
 * front, a node that is not ready — and their envelope text says nothing an operator can act on.
 * Those go to `describe`, so this dialog says "sign in again" in the same words as every other
 * screen rather than inventing a second vocabulary for the same three facts.
 */
export function compileFailureText(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 401 || error.status === 403 || error.status === 503) {
      return describe(error);
    }
    const body = error.body.trim();
    const detail = envelopeMessage(body) ?? body;
    if (error.status === 413) {
      return detail.length > 0
        ? `The document is too large for the fleet to compile: ${detail}`
        : "The document is too large for the fleet to compile (the limit is 4 MiB).";
    }
    return detail.length > 0 ? detail : `The fleet refused the document (${String(error.status)}).`;
  }
  return describe(error);
}
