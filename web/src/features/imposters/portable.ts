/**
 * Moving a mock in and out of the fleet (issue #251) — export, import and clone.
 *
 * Pure and free of React, like the stub projections: what a document means, and what applying it
 * would do, is worth deciding and testing without a screen attached.
 *
 * **The load-bearing rule: an export must be STABLE — the same mock exports to the same file.** The
 * whole point is to commit the result beside the tests it supports, so a document that changes when
 * the mock has not is one a developer learns to distrust.
 *
 * Byte-preservation is how that is achieved for the whole-SET export, which is why `apiGetText`
 * exists: `GET /imposters?replayable=true` already emits clean `ImposterConfig`s, so re-indenting
 * them would only add churn.
 *
 * It is emphatically NOT how it is achieved for a single imposter, and this is the trap.
 * `GET /imposters/:port?replayable=true` looks like the obvious call and is wrong: `handle_get`
 * parses the query and then consults **only** `remove_proxies` — `replayable` is never read on that
 * route (`admin_api/handlers/imposters.rs`, where `handle_list` DOES branch on it). What comes back
 * is the full `ImposterDetail`: `numberOfRequests` and `requests` — the recorded journal, headers
 * and bodies included — plus `_links` carrying the serving node's own base URL. That document is
 * unstable by construction (the counts move with every request served) and it would carry captured
 * credentials into a file this console tells the operator to commit.
 *
 * So a single-imposter export is taken from the LIST projection and the one entry selected out of
 * it (`selectImposter`). That parses, and re-serializing is the honest cost of getting a clean,
 * stable document — determinism is what diff-stability actually needs, not literal bytes.
 */

/** The two shapes an export can take, both real projections the admin API already serves. */
export type ExportProjection = "replay-ready" | "as-configured";

/**
 * The query string for a projection.
 *
 * `replayable=true` renders the imposter in the form `PUT /imposters` accepts back.
 * `removeProxies=true` additionally turns recorded proxy responses into static stubs and drops the
 * proxy stubs themselves — the difference between "a mock of what the upstream said" and "a mock
 * that will go on recording at whoever imports it".
 */
export function exportQuery(projection: ExportProjection): string {
  return projection === "replay-ready"
    ? "?replayable=true&removeProxies=true"
    : "?replayable=true";
}

/** Indentation for the documents this module has to re-serialize. Deterministic is the requirement. */
const EXPORT_INDENT = 2;

/**
 * Pull one imposter out of a whole-set export.
 *
 * The single-imposter route cannot serve a replay-ready projection (see the module comment), so the
 * set projection is fetched and the wanted entry selected here. Re-serialized deterministically,
 * which is what keeps two exports of an unchanged mock identical.
 */
export function selectImposter(
  setText: string,
  port: number,
): { kind: "ok"; text: string } | { kind: "error"; message: string } {
  const parsed = parseImportDocument(setText);
  if (parsed.kind === "error") return { kind: "error", message: parsed.message };
  const entry = parsed.entries.find((candidate) => candidate.port === port);
  if (entry === undefined) {
    return { kind: "error", message: `The fleet returned no imposter on port ${port}.` };
  }
  return { kind: "ok", text: `${JSON.stringify(stripPerNode(entry.imposter), null, EXPORT_INDENT)}\n` };
}

/**
 * Keys that describe the imposter's state on one node rather than its configuration.
 *
 * The set projection does not emit these — it renders `ImposterConfig` — so this strips nothing in
 * practice today. It is here as defence in depth, because the cost of being wrong is not a cosmetic
 * diff: `requests` is the recorded journal, headers and bodies included, and an export is a file
 * this console tells the operator to commit. A route change that started including them should not
 * silently turn every export into a credential leak.
 */
const PER_NODE_KEYS = ["requests", "numberOfRequests", "_links"] as const;

function stripPerNode(imposter: Record<string, unknown>): Record<string, unknown> {
  const stripped: Record<string, unknown> = { ...imposter };
  for (const key of PER_NODE_KEYS) delete stripped[key];
  return stripped;
}

/**
 * Characters a filename cannot safely carry, collapsed rather than dropped so nothing runs together.
 *
 * Dots are trimmed from the ends as well as dashes: a name of `...` would otherwise produce
 * `imposter-80-....json`, and a leading dot makes the file hidden on unix — an export an operator
 * then cannot find is worse than one with a duller name.
 */
function slug(value: string): string {
  return value
    .trim()
    .replace(/[^A-Za-z0-9._-]+/g, "-")
    .replace(/^[-.]+|[-.]+$/g, "")
    .slice(0, 60);
}

/**
 * A filename carrying enough to tell two exports apart in a downloads folder.
 *
 * The port is always present because it is the imposter's identity; the name is added when it has
 * one, since `billing` is what a human recognises and `4545` is what the fleet does.
 */
export function exportFilename(port: number, name: string | undefined): string {
  const named = name === undefined ? "" : slug(name);
  return named === "" ? `imposter-${port}.json` : `imposter-${port}-${named}.json`;
}

export function exportSetFilename(tenant: string | null): string {
  const named = tenant === null ? "" : slug(tenant);
  return named === "" ? "imposters.json" : `imposters-${named}.json`;
}

// ---------------------------------------------------------------------------------------------
// import
// ---------------------------------------------------------------------------------------------

/** One imposter lifted out of an import document, with the port it claims. */
export type ImportEntry = {
  /** `null` when the document did not give this imposter a numeric port — the server will refuse it. */
  port: number | null;
  name: string | null;
  imposter: Record<string, unknown>;
};

export type ImportDocument =
  | { kind: "ok"; entries: ImportEntry[] }
  | { kind: "error"; message: string };

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function entryOf(imposter: Record<string, unknown>): ImportEntry {
  const port = imposter.port;
  const name = imposter.name;
  return {
    // Range-checked, not merely integer-checked: `-1` and `999999` are not ports, and listing them
    // in the pre-flight as though they were promises something the server will refuse.
    port: typeof port === "number" && Number.isInteger(port) && port >= 1 && port <= 65535 ? port : null,
    name: typeof name === "string" ? name : null,
    imposter,
  };
}

/**
 * Read an import document into the imposters it would create.
 *
 * Accepts both shapes the API deals in, because both are shapes THIS console produces: a single
 * imposter object (what a one-imposter export downloads) and a `{"imposters": [...]}` set document
 * (what a whole-tenant export downloads, and what `PUT /imposters` takes). Accepting only one would
 * mean an export this console wrote could not be imported by it.
 *
 * A bare array is accepted too — it is what `GET /imposters` itself returns for the list, so
 * somebody will paste one.
 */
export function parseImportDocument(text: string): ImportDocument {
  if (text.trim() === "") return { kind: "error", message: "There is nothing to import." };

  let parsed: unknown;
  try {
    parsed = JSON.parse(text) as unknown;
  } catch (error) {
    // Surfaced, never swallowed: a malformed paste is the single most likely thing to go wrong
    // here, and the parser's own message names the offset.
    return {
      kind: "error",
      message: `This is not valid JSON: ${error instanceof Error ? error.message : String(error)}`,
    };
  }

  if (Array.isArray(parsed)) {
    const bad = parsed.findIndex((entry) => !isPlainObject(entry));
    if (bad !== -1) {
      return { kind: "error", message: `Item ${bad + 1} of this list is not an imposter object.` };
    }
    return { kind: "ok", entries: parsed.filter(isPlainObject).map(entryOf) };
  }

  if (!isPlainObject(parsed)) {
    return { kind: "error", message: "An import must be an imposter, or a document of imposters." };
  }

  if ("imposters" in parsed) {
    const list = parsed.imposters;
    if (!Array.isArray(list)) {
      return { kind: "error", message: "`imposters` must be a list." };
    }
    const bad = list.findIndex((entry) => !isPlainObject(entry));
    if (bad !== -1) {
      return { kind: "error", message: `Imposter ${bad + 1} in this document is not an object.` };
    }
    return { kind: "ok", entries: list.filter(isPlainObject).map(entryOf) };
  }

  return { kind: "ok", entries: [entryOf(parsed)] };
}

/** What importing would do, worked out before anything is written. */
export type ImportPlan = {
  entries: ImportEntry[];
  /** Ports in the document that the fleet already serves — `Add` will be refused for these. */
  collisions: number[];
  /** Ports named more than once WITHIN the document itself. */
  duplicates: number[];
  /** Entries carrying no usable port; the server will refuse them. */
  portless: number;
};

/**
 * Work out what an import would do, before any of it is done.
 *
 * Collisions matter because the two modes fail differently and neither failure is obvious from the
 * document alone: `Add` is refused per-imposter by the port check, while `Replace all` succeeds and
 * destroys whatever was there. Naming the overlap up front is what makes that an informed choice
 * rather than a surprise.
 */
export function importPlan(entries: ImportEntry[], existingPorts: readonly number[]): ImportPlan {
  const existing = new Set(existingPorts);
  const seen = new Set<number>();
  const collisions: number[] = [];
  const duplicates: number[] = [];
  let portless = 0;

  for (const entry of entries) {
    if (entry.port === null) {
      portless += 1;
      continue;
    }
    if (existing.has(entry.port) && !collisions.includes(entry.port)) collisions.push(entry.port);
    if (seen.has(entry.port) && !duplicates.includes(entry.port)) duplicates.push(entry.port);
    seen.add(entry.port);
  }

  return { entries, collisions, duplicates, portless };
}

/** The document `PUT /imposters` takes, from the entries a plan covers. */
export function renderSetDocument(entries: ImportEntry[]): { imposters: Record<string, unknown>[] } {
  return { imposters: entries.map((entry) => entry.imposter) };
}

// ---------------------------------------------------------------------------------------------
// clone
// ---------------------------------------------------------------------------------------------

export type CloneResult =
  | { kind: "ok"; imposter: Record<string, unknown> }
  | { kind: "error"; message: string };

/**
 * Rewrite an exported imposter onto a new port, optionally renaming it.
 *
 * Everything else is carried across untouched — stubs, recorded responses, behaviours, the lot —
 * because the point of a duplicate is to try a variant against the same mock. What does NOT come
 * along is the request log: it is per-imposter journal state on the node, not part of the
 * configuration document, so a clone starts with an empty log. The dialog says so; this function
 * simply never had it.
 *
 * `port` is replaced rather than merged so a document that spells it as a string still ends up with
 * the numeric port the API requires.
 */
export function cloneImposter(
  source: unknown,
  port: number,
  name: string | null,
): CloneResult {
  if (!isPlainObject(source)) {
    return { kind: "error", message: "The imposter to duplicate could not be read." };
  }
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    return { kind: "error", message: "A port must be a whole number between 1 and 65535." };
  }

  // Same strip as the export path: a duplicate must not inherit the source's recorded journal, which
  // is what the dialog promises and what `{ ...source }` alone would quietly break.
  const imposter: Record<string, unknown> = { ...stripPerNode(source), port };
  if (name === null) delete imposter.name;
  else imposter.name = name;
  return { kind: "ok", imposter };
}
