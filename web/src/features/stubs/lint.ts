/**
 * In-browser `rift-lint`, compiled to wasm and loaded lazily (RFC-006 §12 Q1, issue #188).
 *
 * **Advisory.** The server validates every save and its refusal is the authority; this pane exists
 * to tell an operator about a mistake before they send it, not to decide whether they may. When the
 * two disagree the server wins, and the write error path says so in the fleet's own words.
 *
 * The artifact is built by the release lane (`wasm-pack` → `web/public/lint/`) and is deliberately
 * absent from a dev checkout and from the test run. Its absence resolves to `"unavailable"` — a
 * value the pane renders as a sentence — rather than to an empty finding list, which would read as
 * "your stub is clean" on the strength of a linter that never ran.
 */

/** One `rift_lint::LintIssue`, as it serializes. */
export type Finding = {
  severity: "error" | "warning" | "info";
  code: string;
  message: string;
  location?: string;
  suggestion?: string;
};

/**
 * Where the built artifact is served from.
 *
 * `web/public/` is copied verbatim into `dist/`, and the console is served under `/console/`
 * (`vite.config.ts`'s `base`), so this is the URL the browser resolves. It is a *runtime* URL, not
 * a module specifier: Vite must not try to resolve it at build time, because on every build except
 * the release lane's the file does not exist yet.
 */
export const LINT_MODULE_URL = "/console/lint/rift_lint_wasm.js";

/** The shape `wasm-pack --target web` emits: a default init, plus the exported functions. */
type LintModule = {
  default: (input?: unknown) => Promise<unknown>;
  lint_stub: (json: string) => string;
};

type Loader = (url: string) => Promise<LintModule>;

const defaultLoader: Loader = (url) =>
  // `@vite-ignore` keeps this a runtime fetch of a `public/` asset rather than a build-time module
  // resolution that would fail every build the artifact is not present for.
  import(/* @vite-ignore */ url) as Promise<LintModule>;

/**
 * The load is attempted once per page. A second attempt would re-download the module on every
 * keystroke-triggered lint on a build that does not carry it.
 */
let loading: Promise<LintModule | null> | null = null;

/** Test seam: forget the cached load so a test can state its own outcome. */
export function resetLintModule(): void {
  loading = null;
}

/**
 * Decode the boundary's answer.
 *
 * `null` for anything unreadable, and the caller turns that into `"unavailable"`. Answering `[]`
 * here would be the swallow that matters: a linter whose output could not be parsed has classified
 * nothing, and reporting "no findings" would be the safe class chosen on no evidence.
 */
export function decodeFindings(text: string): Finding[] | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  if (!Array.isArray(parsed)) return null;
  const findings: Finding[] = [];
  for (const entry of parsed) {
    if (typeof entry !== "object" || entry === null) return null;
    const { severity, code, message, location, suggestion } = entry as Record<string, unknown>;
    if (severity !== "error" && severity !== "warning" && severity !== "info") return null;
    if (typeof code !== "string" || typeof message !== "string") return null;
    findings.push({
      severity,
      code,
      message,
      ...(typeof location === "string" ? { location } : {}),
      ...(typeof suggestion === "string" ? { suggestion } : {}),
    });
  }
  return findings;
}

/**
 * Lint one stub's JSON text, or report that the linter is not available on this build.
 *
 * `loader` is a seam for the tests only; production always resolves the bundled artifact.
 */
export async function lintStub(json: string, loader: Loader = defaultLoader): Promise<Finding[] | "unavailable"> {
  loading ??= loader(LINT_MODULE_URL)
    .then(async (module) => {
      await module.default();
      return module;
    })
    .catch(() => null);

  const module = await loading;
  if (module === null) return "unavailable";
  try {
    return decodeFindings(module.lint_stub(json)) ?? "unavailable";
  } catch {
    // A panic inside the wasm module is a bug in the linter, not a verdict on the stub.
    return "unavailable";
  }
}
