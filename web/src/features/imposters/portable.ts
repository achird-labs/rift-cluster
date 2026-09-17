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
 * Byte-preservation is how that is achieved for a whole-SET export that keeps TLS material, which is
 * why `apiGetText` exists: `GET /imposters?replayable=true` already emits clean `ImposterConfig`s, so
 * re-indenting them would only add churn. Leaving TLS material out means taking fields out, which
 * means a deterministic re-serialization instead (`renderSetExport`, D-84).
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
 * What an export contains.
 *
 * `replayable` and `removeProxies` are the route's own flags. `replayable=true` renders the imposter
 * in the form `PUT /imposters` accepts back. `removeProxies=true` additionally turns recorded proxy
 * responses into static stubs and drops the proxy stubs themselves — the difference between "a mock
 * of what the upstream said" and "a mock that will go on recording at whoever imports it".
 *
 * `tls` is not a route flag, and is never sent (D-84). The replayable projection is the imposter's
 * config verbatim, so an https imposter created with its own `cert` and `key` always comes back with
 * both; `tls` decides whether the console keeps that pair in the downloaded file.
 */
export type ExportOptions = { replayable: boolean; removeProxies: boolean; tls: boolean };

/** What the export covers. The imposter list offers only the whole set; a detail screen offers both. */
export type ExportScope = { kind: "all" } | { kind: "one"; port: number };

export function exportOptionsQuery(options: ExportOptions): string {
  return `?replayable=${String(options.replayable)}&removeProxies=${String(options.removeProxies)}`;
}

/**
 * The fields that are an https imposter's own certificate (D-84).
 *
 * Both, always: the engine refuses an imposter carrying one without the other, so half a pair in a
 * file is no fixture, and a lone `key` is still a private key. `ca`, `mutualAuth` and
 * `rejectUnauthorized` are not here — they are public, and they change what the imposter does.
 */
const TLS_MATERIAL_FIELDS = ["cert", "key"] as const;

function carriesTlsMaterial(imposter: Record<string, unknown>): boolean {
  return TLS_MATERIAL_FIELDS.some((field) => field in imposter);
}

function withoutTlsMaterial(imposter: Record<string, unknown>): Record<string, unknown> {
  const stripped: Record<string, unknown> = { ...imposter };
  for (const field of TLS_MATERIAL_FIELDS) delete stripped[field];
  return stripped;
}

/**
 * A file ready to download.
 *
 * `tlsPorts` names the imposters that carried their own `cert`/`key` — kept in `text` when the
 * operator asked for TLS material, removed from it when not — so the screen can say which, rather
 * than warning about keys in general.
 */
export type ExportDocument =
  | {
      kind: "ok";
      text: string;
      tlsPorts: number[];
      /** Imposters with material of their own but no usable port to name them by. */
      tlsWithoutPort: number;
    }
  | { kind: "error"; message: string };

/**
 * Why an export that has to be rewritten is refused in a browser that would round its numbers.
 *
 * The file would look right and hold different numbers than the fleet serves. The curl the dialog
 * shows does the same removal in `jq`, which keeps digits.
 */
const CANNOT_KEEP_DIGITS =
  "This browser cannot take the key and cert out of the file without risking changes to numbers in it. " +
  "Use the curl command the export dialog shows, or a current browser.";

/**
 * The whole-set file, from the route's `GET /imposters?replayable=…` text.
 *
 * Kept TLS material, or none to remove, means the route's bytes, untouched. Removed means a
 * deterministic re-serialization — the only way to take fields out of a JSON document — that keeps
 * every number's digits, so two exports of an unchanged fleet are still identical and neither
 * differs from the fleet in anything but the pair. A document that does not parse is refused in
 * both modes: passing unreadable bytes through would pass through whatever key they hold.
 */
export function renderSetExport(setText: string, tls: boolean): ExportDocument {
  const parsed = parseImportDocument(setText);
  if (parsed.kind === "error") {
    return { kind: "error", message: `The fleet's export could not be read: ${parsed.message}` };
  }
  const carriers = parsed.entries.filter((entry) => carriesTlsMaterial(entry.imposter));
  const tlsPorts = carriers.flatMap((entry) => (entry.port === null ? [] : [entry.port]));
  const reported = { tlsPorts, tlsWithoutPort: carriers.length - tlsPorts.length };
  // Nothing kept or nothing to remove: the route's own bytes are already the file.
  if (tls || carriers.length === 0) return { kind: "ok", text: setText, ...reported };
  if (!keepsDigits()) return { kind: "error", message: CANNOT_KEEP_DIGITS };
  const imposters = parsed.entries.map((entry) => withoutTlsMaterial(entry.imposter));
  return {
    kind: "ok",
    text: `${JSON.stringify({ imposters }, null, EXPORT_INDENT)}\n`,
    ...reported,
  };
}

/** One imposter's file, selected out of the set text the same way `selectImposter` does. */
export function renderImposterExport(setText: string, port: number, tls: boolean): ExportDocument {
  // Always a rewrite — one imposter is cut out of the set — so digits must survive either way.
  if (!keepsDigits()) return { kind: "error", message: CANNOT_KEEP_DIGITS };
  const found = findImposter(setText, port);
  if (found.kind === "error") return found;
  const tlsPorts = carriesTlsMaterial(found.imposter) ? [port] : [];
  const imposter = tls ? found.imposter : withoutTlsMaterial(found.imposter);
  return {
    kind: "ok",
    text: `${JSON.stringify(imposter, null, EXPORT_INDENT)}\n`,
    tlsPorts,
    tlsWithoutPort: 0,
  };
}

/**
 * The command the export dialog shows: one that produces the same file the console downloads.
 *
 * Both scopes read the set route, as the console does. The jq filter does what `renderSetExport` and
 * `renderImposterExport` do — select the one imposter, and drop `cert`/`key` unless kept — so a
 * command copied out of the dialog cannot write a private key the dialog said it would leave out.
 * It does not repeat `stripPerNode`, which removes fields the set route never sends.
 */
export function exportCurl(scope: ExportScope, options: ExportOptions): string {
  const read = `curl -s '/imposters${exportOptionsQuery(options)}'`;
  const strip = `del(${TLS_MATERIAL_FIELDS.map((field) => `.${field}`).join(", ")})`;
  if (scope.kind === "all") {
    return options.tls
      ? `${read} > ${EXPORT_SET_FILENAME}`
      : `${read} | jq '.imposters[] |= ${strip}' > ${EXPORT_SET_FILENAME}`;
  }
  const select = `.imposters[] | select(.port == ${String(scope.port)})`;
  const filter = options.tls ? select : `${select} | ${strip}`;
  return `${read} | jq '${filter}' > ${exportFilename(scope.port, undefined)}`;
}

/**
 * The named projections, as options.
 *
 * One builder underneath, so the presets and the dialog's own checkboxes cannot produce different
 * URLs for the same intent — the drift would be silent, and it would be a file whose contents did
 * not match the projection its filename claims.
 */
export const PROJECTION_OPTIONS: Record<ExportProjection, ExportOptions> = {
  "replay-ready": { replayable: true, removeProxies: true, tls: false },
  "as-configured": { replayable: true, removeProxies: false, tls: false },
};

/** The query string for a named projection. */
export function exportQuery(projection: ExportProjection): string {
  return exportOptionsQuery(PROJECTION_OPTIONS[projection]);
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
  const found = findImposter(setText, port);
  if (found.kind === "error") return found;
  return { kind: "ok", text: `${JSON.stringify(found.imposter, null, EXPORT_INDENT)}\n` };
}

function findImposter(
  setText: string,
  port: number,
): { kind: "ok"; imposter: Record<string, unknown> } | { kind: "error"; message: string } {
  const parsed = parseImportDocument(setText);
  if (parsed.kind === "error") return parsed;
  const entry = parsed.entries.find((candidate) => candidate.port === port);
  if (entry === undefined) {
    return { kind: "error", message: `The fleet returned no imposter on port ${port}.` };
  }
  return { kind: "ok", imposter: stripPerNode(entry.imposter) };
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

/**
 * The filename a whole-set export downloads under.
 *
 * A constant since #550: it used to carry the tenant, which is the only thing that could have told
 * two whole-set exports apart. One fleet-wide set, one name.
 */
export const EXPORT_SET_FILENAME = "imposters.json";

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
  return (
    typeof value === "object" && value !== null && !Array.isArray(value) && !isRawNumber(value)
  );
}

/**
 * `JSON.rawJSON` and `JSON.isRawJSON`, which ES2022's lib does not declare. They shipped with the
 * reviver's `context.source` (Chrome 114, Firefox 135, Safari 18.4), so one presence check covers
 * all three.
 */
type RawJsonApi = {
  rawJSON?: (text: string) => unknown;
  isRawJSON?: (value: unknown) => boolean;
};
const RAW_JSON: RawJsonApi = JSON as RawJsonApi;

function isRawNumber(value: unknown): boolean {
  return RAW_JSON.isRawJSON?.(value) ?? false;
}

/**
 * Parse JSON without changing any number, where the browser can.
 *
 * A stub body is free-form JSON, so an imposter can hold `1234567890123456789` or `1.0`, and plain
 * `JSON.parse` turns those into `1234567890123456800` and `1` without a word. Every number whose
 * text a JavaScript number would not reproduce is kept as its original digits instead, and
 * `JSON.stringify` writes those digits back. Without the API this is plain `JSON.parse` — what the
 * console always did — and `keepsDigits` says so, for the callers that must refuse rather than
 * round (D-84).
 */
export function parseJson(text: string): unknown {
  const rawJSON = RAW_JSON.rawJSON;
  if (rawJSON === undefined) return JSON.parse(text);
  return JSON.parse(text, (_key, value: unknown, context?: { source?: string }) =>
    typeof value === "number" &&
    context?.source !== undefined &&
    String(value) !== context.source
      ? rawJSON(context.source)
      : value,
  );
}

function keepsDigits(): boolean {
  return RAW_JSON.rawJSON !== undefined;
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
 * (what a whole-set export downloads, and what `PUT /imposters` takes). Accepting only one would
 * mean an export this console wrote could not be imported by it.
 *
 * A bare array is accepted too — it is what `GET /imposters` itself returns for the list, so
 * somebody will paste one.
 */
export function parseImportDocument(text: string): ImportDocument {
  if (text.trim() === "") return { kind: "error", message: "There is nothing to import." };

  let parsed: unknown;
  try {
    parsed = parseJson(text);
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
