/**
 * The response-list form ⟷ JSON projection for one stub (issue #248), sibling to `projection.ts`
 * and `predicates.ts`.
 *
 * Same guarantee as the rest of the stub editor: this either understands the whole `responses`
 * array or refuses the whole stub, naming every key it could not place. There is no form with a
 * response quietly missing — that shape is the one that saves the first response and drops the
 * second, silently turning a cycling stub into a constant one.
 *
 * **The load-bearing decision: the shape a response ARRIVED in is the shape it leaves in.** The
 * engine accepts two spellings of the same response — `{is: {statusCode: 200}}` and the flat
 * `{statusCode: 200}` with no wrapper, which is what recorded and migrated mocks look like
 * (`vendor/rift/docs/mountebank/proxy.md:208-226`). Normalising one into the other would be
 * lossless as far as the *mock* is concerned and still wrong: it shows as a diff on every export
 * of a recorded imposter, which defeats #251 and trains an operator to distrust the console. So
 * `wrapped` is carried per response and `renderResponses` re-emits each one as it found it.
 *
 * Two further shapes this file refuses to simplify, both learned from the engine rather than
 * guessed (`vendor/rift/crates/rift-mock-core/src/imposter/types.rs`):
 *
 * - **Headers are multi-value.** They deserialize as `HashMap<String, Vec<String>>`, so
 *   `{"Set-Cookie": ["a", "b"]}` is two header lines, not one weird value. A row-per-value model
 *   is what lets the form hold that; an `Object.fromEntries` model would drop the second cookie.
 *   The single-element array `["a"]` is kept an array for the same round-trip reason as `wrapped`.
 * - **A header value is carried verbatim, not as a string.** Mountebank's recorders emit
 *   `"Content-Length": 124` and `"X-Flag": true`, and the engine tolerates both deliberately
 *   (upstream #754). Refusing them would send every recorded imposter to raw-only; stringifying
 *   them would rewrite the operator's document on the way through the form. Carrying the value as
 *   `unknown` is the same choice `predicates.ts` makes for `PredicateEntry.value`, for the same reason.
 *
 * Pure and free of React, for the same reason its two siblings are: the round-trip property is a
 * claim about `projectResponses` and `renderResponses` alone, and it is worth nothing if it can
 * only be exercised through a component.
 */

import {
  type BehaviorModel,
  type BehaviorSpelling,
  type FaultModel,
  parseBehaviors,
  parseResponseFault,
  parseRiftTcpFault,
  renderBehaviors,
  renderFault,
} from "./behaviors.ts";

/**
 * One header line. `value` is whatever JSON the document carried — usually a string, sometimes a
 * number or a boolean (see the module comment). `multi` records that this row came from a JSON
 * *array*, so a one-element array does not silently render back as a bare string.
 */
export type ResponseHeader = { name: string; value: unknown; multi: boolean };

/**
 * A response body in the three shapes the form can hold honestly.
 *
 * `absent` is not `text` with an empty string: a stub carrying no body and a stub answering with an
 * empty one are different documents, and only the second sends a `body` key.
 */
export type ResponseBody =
  | { kind: "absent" }
  | { kind: "text"; text: string }
  /** An object or array at `body`, edited as JSON and written back as a JSON *value*. */
  | { kind: "json"; value: unknown };

export type ResponseModel = {
  /** `false` for the flat, wrapper-less form recorded mocks use. See the module comment. */
  wrapped: boolean;
  /**
   * `_behaviors` — a delay and/or a repeat count hanging off this response (#249). `null` means the
   * response carries no behaviours key at all, which is different from carrying an empty one.
   */
  behaviors: BehaviorModel | null;
  /**
   * A connection fault (#249). Note this REPLACES the response rather than decorating it: `fault`
   * is its own `StubResponse` variant, dispatched after `is`/`proxy`/`inject`.
   */
  fault: FaultModel | null;
  /** `null` means the key is ABSENT, not that the status is zero — the engine then defaults to 200. */
  statusCode: number | null;
  /**
   * Did the source carry a `headers` key at all?
   *
   * Not the same question as "are there any headers". `IsResponseOut` has no
   * `skip_serializing_if` on its `headers` map, so the engine emits `"headers": {}` on every
   * response that happens to have none — which is most recorded ones. Without this flag an empty
   * map projects to zero rows and renders back to nothing, deleting the key from every recorded
   * stub that passes through the form. That is a silent rewrite of the operator's document, and on
   * an export (#251) it is a diff on almost every response.
   */
  headersPresent: boolean;
  headers: ResponseHeader[];
  body: ResponseBody;
};

export type ResponseProjection =
  | { kind: "responses"; items: ResponseModel[] }
  | { kind: "rawOnly"; unmodelledKeys: string[] };

/** What a response is, for the read-only label the operator sees even when the form refuses it. */
export type ResponseLabel = {
  index: number;
  kind: "is" | "proxy" | "inject" | "fault" | "_rift" | "other";
  detail: string;
};

/** The status the engine answers with when a response names none. */
export const DEFAULT_STATUS_CODE = 200;

/**
 * The keys an `is` response (wrapped or flat) may hold in the form. Anything else it carries —
 * `_mode` above all — is named as unmodelled and sends the stub to raw-only.
 */
const IS_KEYS = ["statusCode", "headers", "body"] as const;

/**
 * The response variants this form does not edit, in the order they are reported.
 *
 * `fault` left this list in #249 — it is now modelled by the latency-and-fault panel. `_rift` left
 * it too, but only part-way: `parseResponse` accepts a `_rift` whose sole content is `fault.tcp`
 * and names everything else it might carry (`script`, `templated`, the `latency`/`error` fault
 * kinds). It remains a `describeResponses` *label* kind, because a `_rift`-only response becomes a
 * `RiftScript` — a fifth kind of response entirely — and would otherwise be labelled as a plain 200,
 * telling the operator the stub answers a status when it actually runs a script.
 */
const FOREIGN_VARIANTS = ["proxy", "inject"] as const;

/**
 * Keys that live on the RESPONSE, beside `is`, rather than inside it.
 *
 * `StubResponseRaw` declares these at the same level as the flat form's `statusCode`/`headers`/
 * `body`, so in a flat response `_behaviors` and `statusCode` are genuinely adjacent and "is this
 * an is-body key or a response-level one" can only be decided by name. This list is that decision,
 * in one place, used by both branches of `parseResponse`.
 */
const RESPONSE_LEVEL_KEYS = ["is", "proxy", "inject", "fault", "_behaviors", "behaviors", "_rift"] as const;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** A header value the engine accepts on the wire: a string, or a scalar it coerces to one. */
function isHeaderScalar(value: unknown): boolean {
  return typeof value === "string" || typeof value === "number" || typeof value === "boolean";
}

/**
 * Which fault form actually FIRES on this response — and it turns on the `is` KEY alone.
 *
 * The engine has two separate dispatch tests, and they point opposite ways:
 *
 * - **With an `is` key**, `From<StubResponseRaw>` takes the `is` branch and hands `raw.rift` to
 *   `new_is(..., raw.rift)`, so `_rift.fault.tcp` fires — while the top-level `fault` is never
 *   reached, and `StubResponseOut`'s `Is` arm sets `fault: None` so the key does not even survive
 *   the next `GET /imposters`.
 * - **Without one** (the flat, recorded form), `raw.rift` is tested BEFORE the flat
 *   `statusCode`/`body`/`headers` branch, so a flat response carrying `_rift` becomes a
 *   `RiftScript` — `execute_stub_response_with_rift` returns `None` for that, `apply_rift_fault` is
 *   never called, and the request falls through to a default 200 with the status and body erased.
 *   There it is the top-level `fault` key that fires.
 *
 * So the predicate is exactly "does the document carry an `is` key", which is `wrapped`. An earlier
 * version of this asked "does it have a status, headers or a body" instead; that is true of a flat
 * recorded response, where the answer inverts — and recorded responses are the flat ones.
 */
export function faultFiresAsRift(response: ResponseModel): boolean {
  return response.wrapped;
}

/** Does the fault this response carries actually fire, given the dispatch above? */
export function faultIsArmed(response: ResponseModel): boolean {
  if (response.fault === null) return false;
  const isRiftForm = response.fault.form !== "responseKey";
  return faultFiresAsRift(response) === isRiftForm;
}

export function blankResponse(): ResponseModel {
  return {
    wrapped: true,
    statusCode: DEFAULT_STATUS_CODE,
    headersPresent: false,
    headers: [],
    body: { kind: "absent" },
    behaviors: null,
    fault: null,
  };
}

// ---------------------------------------------------------------------------------------------
// render: ResponseModel[] -> JSON
// ---------------------------------------------------------------------------------------------

/**
 * Group header rows back into the object the engine reads.
 *
 * Rows sharing a name accumulate into one array — that *is* a multi-value header — so two
 * `Set-Cookie` rows render as `{"Set-Cookie": ["a", "b"]}` and not as one key overwriting the other.
 * A lone row renders bare unless it was read from an array (`multi`), which keeps `["a"]` an array.
 *
 * **An unnamed row renders nothing at all.** The builder creates a row with an empty name and lets
 * the operator type into it, and this document is re-rendered and re-projected on every keystroke.
 * Were empty names emitted, clicking "Add header" twice would put two rows under the same `""` key,
 * they would merge into an array, and re-projection would mark BOTH `multi` — permanently rewriting
 * an untouched neighbour's `"1"` into `["1"]`. An empty header name also names no real header: the
 * engine would emit no line for it.
 */
function renderHeaders(headers: ResponseHeader[]): Record<string, unknown> {
  const grouped = new Map<string, { values: unknown[]; multi: boolean }>();
  for (const header of headers) {
    if (header.name === "") continue;
    const existing = grouped.get(header.name);
    if (existing === undefined) {
      grouped.set(header.name, { values: [header.value], multi: header.multi });
      continue;
    }
    // No `multi` update needed: two or more values already force the array branch below.
    existing.values.push(header.value);
  }

  /*
   * Built with `Object.fromEntries`, not by assigning into an object literal. Assignment invokes
   * inherited setters, and `__proto__` has one: `rendered["__proto__"] = v` would reassign the
   * object's prototype and create no own property, so a header genuinely named `__proto__` would
   * vanish from the document with nothing named. `Object.fromEntries` defines own properties
   * instead, so the name round-trips like any other.
   */
  return Object.fromEntries(
    [...grouped].map(([name, { values, multi }]) => [
      name,
      values.length === 1 && !multi ? values[0] : values,
    ]),
  );
}

/** The `is` body of one response — the same object whether or not it ends up wrapped. */
function renderIsBody(response: ResponseModel): Record<string, unknown> {
  const rendered: Record<string, unknown> = {};
  // Key order matches the engine's own examples. It matters only in that it must be deterministic:
  // the round-trip property compares this function's output against itself.
  if (response.statusCode !== null) rendered.statusCode = response.statusCode;
  // `headersPresent` keeps an empty `"headers": {}` — which the engine emits on every header-less
  // response — instead of deleting the key on the way through the form. See the type's comment.
  if (response.headersPresent || response.headers.length > 0) {
    rendered.headers = renderHeaders(response.headers);
  }
  if (response.body.kind === "text") rendered.body = response.body.text;
  else if (response.body.kind === "json") rendered.body = response.body.value;
  return rendered;
}

/** Render the builder's responses back to the `responses` array's JSON. */
export function renderResponses(items: ResponseModel[]): unknown[] {
  return items.map((response) => {
    const body = renderIsBody(response);
    /*
     * Response-level keys come AFTER the body in the flat form, so a document that was read as
     * `{statusCode, _behaviors}` is written back in that order rather than reshuffled. In the
     * wrapped form they are siblings of `is`, which is where `StubResponseRaw` declares them.
     */
    const level: Record<string, unknown> = {};
    if (response.behaviors !== null) {
      const rendered = renderBehaviors(response.behaviors);
      if (rendered !== null) level[rendered.key] = rendered.value;
    }
    if (response.fault !== null) {
      const rendered = renderFault(response.fault);
      level[rendered.key] = rendered.value;
    }
    return response.wrapped ? { is: body, ...level } : { ...body, ...level };
  });
}

// ---------------------------------------------------------------------------------------------
// project: JSON -> ResponseModel[] (or a named refusal)
// ---------------------------------------------------------------------------------------------

type ParseResult<T> = { ok: true; value: T } | { ok: false; issues: string[] };

/** The parts of a response that live inside `is` — everything `parseIsBody` is responsible for. */
type IsBodyFields = Pick<ResponseModel, "statusCode" | "headersPresent" | "headers" | "body">;

/**
 * Read a response's `headers` object into rows, appending to `headers` and returning the paths of
 * anything it could not place.
 *
 * Split out of `parseIsBody` because it is the only part of a response with real structure of its
 * own — one row per VALUE, so a multi-value header becomes several rows — and inlining it buried
 * the status and body cases under three levels of nesting.
 */
function parseHeaders(raw: unknown, path: string, headers: ResponseHeader[]): string[] {
  if (!isPlainObject(raw)) return [`${path}.headers`];
  const named = (name: string): string => `${path}.headers[${JSON.stringify(name)}]`;

  const issues: string[] = [];
  for (const [name, headerValue] of Object.entries(raw)) {
    if (name === "") {
      /*
       * A header with no name. `renderHeaders` deliberately drops unnamed rows (that is what keeps
       * the builder's freshly-added rows from merging), so accepting one here would mean a key that
       * is in the source document, visible as a row in the form, and silently gone from the next
       * save — with nothing named. Refusing keeps the two halves consistent.
       */
      issues.push(named(name));
      continue;
    }
    if (Array.isArray(headerValue)) {
      if (headerValue.some((element) => !isHeaderScalar(element))) {
        issues.push(named(name));
        continue;
      }
      if (headerValue.length === 0) {
        /*
         * A name mapped to zero values. The row model is one row PER VALUE, so this would project
         * to no rows at all and the name would vanish on the next save with nothing named — the one
         * outcome this module exists to prevent. It is also degenerate: the engine's serializer
         * skips such a key, so no recorded stub carries one, and refusing it costs a hand-written
         * document nothing but a trip through the raw editor.
         */
        issues.push(named(name));
        continue;
      }
      for (const element of headerValue) headers.push({ name, value: element, multi: true });
      continue;
    }
    if (!isHeaderScalar(headerValue)) {
      issues.push(named(name));
      continue;
    }
    headers.push({ name, value: headerValue, multi: false });
  }
  return issues;
}

/**
 * Parse the `is` body of one response — `{statusCode?, headers?, body?}` — naming every key that
 * made it impossible.
 *
 * `path` is where this object lives (`responses[0].is`, or `responses[0]` for the flat form), so a
 * refusal names the key an operator would actually go looking for.
 */
function parseIsBody(value: Record<string, unknown>, path: string): ParseResult<IsBodyFields> {
  const issues: string[] = [];

  // `_mode: "binary"` means the body is base64. The form has no way to edit that without
  // corrupting it, so it is named rather than modelled — as the issue asks.
  const allowed = new Set<string>(IS_KEYS);
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) issues.push(`${path}.${key}`);
  }

  let statusCode: number | null = null;
  if ("statusCode" in value) {
    const raw = value.statusCode;
    // A string status code is a real shape the engine coerces, but coercing it here would rewrite
    // the operator's document through the form. Named instead — `projection.ts` did the same.
    if (typeof raw !== "number") issues.push(`${path}.statusCode`);
    else statusCode = raw;
  }

  const headers: ResponseHeader[] = [];
  const headersPresent = "headers" in value;
  if (headersPresent) issues.push(...parseHeaders(value.headers, path, headers));

  // Any JSON is a legal body (`Option<serde_json::Value>`). A string is text the mock sends
  // verbatim; anything else is a JSON value, edited as JSON and written back as one.
  let body: ResponseBody = { kind: "absent" };
  if ("body" in value) {
    const raw = value.body;
    body = typeof raw === "string" ? { kind: "text", text: raw } : { kind: "json", value: raw };
  }

  if (issues.length > 0) return { ok: false, issues };
  return { ok: true, value: { statusCode, headersPresent, headers, body } };
}

/**
 * Read the response-level keys that sit BESIDE the response body — behaviours and faults.
 *
 * These are siblings of `is`, not fields inside it (`StubResponseRaw` declares them at the same
 * level), which is what makes the flat form fiddly: there, `_behaviors` and `statusCode` are
 * genuinely adjacent, so "is this an is-body key or a response-level one" has to be decided by
 * name rather than by depth. `RESPONSE_LEVEL_KEYS` is that decision, in one place.
 */
function parseResponseLevel(
  value: Record<string, unknown>,
  path: string,
): ParseResult<{ behaviors: BehaviorModel | null; fault: FaultModel | null }> {
  const issues: string[] = [];

  let behaviors: BehaviorModel | null = null;
  const spelling: BehaviorSpelling | null =
    "_behaviors" in value
      ? "_behaviors"
      : "behaviors" in value
        ? Array.isArray(value.behaviors)
          ? "behaviorsArray"
          : "behaviorsObject"
        : null;
  if (spelling !== null) {
    const key = spelling === "_behaviors" ? "_behaviors" : "behaviors";
    // Both spellings at once: the engine takes one and drops the other, and there is no honest
    // single model of "these two disagree", so it is named rather than silently resolved.
    if ("_behaviors" in value && "behaviors" in value) {
      issues.push(`${path}._behaviors`, `${path}.behaviors`);
    } else {
      const parsed = parseBehaviors(value[key], spelling, `${path}.${key}`);
      if (parsed.ok) behaviors = parsed.value;
      else issues.push(...parsed.issues);
    }
  }

  let fault: FaultModel | null = null;
  if ("fault" in value) {
    const parsed = parseResponseFault(value.fault, `${path}.fault`);
    if (parsed.ok) fault = parsed.value;
    else issues.push(...parsed.issues);
  }

  if ("_rift" in value) {
    const rift = value._rift;
    if (!isPlainObject(rift)) {
      issues.push(`${path}._rift`);
    } else {
      /*
       * Only `_rift.fault.tcp` is modelled. `script`, `templated`, and the `latency`/`error` fault
       * kinds are real engine features with their own shapes; naming them keeps them out of the
       * form without dropping them from the document.
       */
      const riftExtra = Object.keys(rift).filter((key) => key !== "fault");
      if (riftExtra.length > 0) issues.push(...riftExtra.map((key) => `${path}._rift.${key}`));
      if (Object.keys(rift).length === 0) {
        /*
         * A bare `_rift: {}`. There is nothing in it to model, and the model has no way to say "the
         * source carried an empty extension" — so rendering would drop the key unnamed. That is not
         * inert on a FLAT response: the engine checks `raw.rift` BEFORE the flat statusCode/body
         * branch, so the mere presence of `_rift` decides whether it builds a `RiftScript` or an
         * `Is`. Dropping it would change which response variant the engine constructs.
         */
        issues.push(`${path}._rift`);
      }
      if ("fault" in rift) {
        const riftFault = rift.fault;
        if (!isPlainObject(riftFault)) {
          issues.push(`${path}._rift.fault`);
        } else {
          const faultExtra = Object.keys(riftFault).filter((key) => key !== "tcp");
          if (faultExtra.length > 0) {
            issues.push(...faultExtra.map((key) => `${path}._rift.fault.${key}`));
          }
          // Same reasoning as the empty `_rift` above: nothing to model, and dropping it silently
          // would rewrite the document.
          if (Object.keys(riftFault).length === 0) issues.push(`${path}._rift.fault`);
          if ("tcp" in riftFault) {
            // Two faults at once — one on the response key, one under `_rift` — is a document the
            // form cannot re-emit without choosing which to keep.
            if (fault !== null) issues.push(`${path}._rift.fault.tcp`);
            else {
              const parsed = parseRiftTcpFault(riftFault.tcp, `${path}._rift.fault.tcp`);
              if (parsed.ok) fault = parsed.value;
              else issues.push(...parsed.issues);
            }
          }
        }
      }
    }
  }

  if (issues.length > 0) return { ok: false, issues };
  return { ok: true, value: { behaviors, fault } };
}

/** Parse one element of the `responses` array: an `is` response, wrapped or flat, or a refusal. */
function parseResponse(value: unknown, index: number): ParseResult<ResponseModel> {
  const path = `responses[${index}]`;
  if (!isPlainObject(value)) return { ok: false, issues: [path] };

  // A response carrying a variant this form does not edit is named, not modelled. `describeResponses`
  // still labels it, so "recognised" and "editable" stay different things.
  const foreign = FOREIGN_VARIANTS.filter((variant) => variant in value);
  if (foreign.length > 0) return { ok: false, issues: foreign.map((variant) => `${path}.${variant}`) };

  const level = parseResponseLevel(value, path);
  if (!level.ok) return level;

  if ("is" in value) {
    // Wrapped. Only response-level keys may ride alongside `is`; anything else is named, because a
    // form that quietly dropped it would erase something the operator configured.
    const extras = Object.keys(value).filter(
      (key) => !(RESPONSE_LEVEL_KEYS as readonly string[]).includes(key),
    );
    if (extras.length > 0) return { ok: false, issues: extras.map((key) => `${path}.${key}`) };
    const inner = value.is;
    if (!isPlainObject(inner)) return { ok: false, issues: [`${path}.is`] };
    const parsed = parseIsBody(inner, `${path}.is`);
    if (!parsed.ok) return parsed;
    return { ok: true, value: { wrapped: true, ...parsed.value, ...level.value } };
  }

  // Flat: everything that is not a response-level key belongs to the is-body.
  const flatBody = Object.fromEntries(
    Object.entries(value).filter(([key]) => !(RESPONSE_LEVEL_KEYS as readonly string[]).includes(key)),
  );
  const parsed = parseIsBody(flatBody, path);
  if (!parsed.ok) return parsed;
  return { ok: true, value: { wrapped: false, ...parsed.value, ...level.value } };
}

/**
 * Read a stub's `responses` array into the builder's model — or refuse, naming every key that made
 * it impossible.
 *
 * An absent `responses` key is an empty list, not a refusal: nothing about a response-less stub is
 * unmodelled, it just answers with the engine's default.
 */
export function projectResponses(stub: unknown): ResponseProjection {
  const responsesValue = isPlainObject(stub) ? stub.responses : undefined;
  if (responsesValue === undefined) return { kind: "responses", items: [] };
  if (!Array.isArray(responsesValue)) return { kind: "rawOnly", unmodelledKeys: ["responses"] };

  const items: ResponseModel[] = [];
  const unmodelled: string[] = [];
  responsesValue.forEach((raw, index) => {
    const result = parseResponse(raw, index);
    if (result.ok) items.push(result.value);
    else unmodelled.push(...result.issues);
  });
  if (unmodelled.length > 0) return { kind: "rawOnly", unmodelledKeys: unmodelled };
  return { kind: "responses", items };
}

// ---------------------------------------------------------------------------------------------
// describe: JSON -> read-only labels
// ---------------------------------------------------------------------------------------------

/**
 * Label every response, whether or not the form can edit it.
 *
 * This exists so "recognised" and "editable" can be different things (AC5). A stub carrying a proxy
 * response opens raw-only — but the operator still needs to see, without reading the JSON, that the
 * stub HAS a proxy and where it points. Deriving the labels from the document rather than from the
 * projection is what lets the editor show them in exactly the case the projection refused.
 */
export function describeResponses(stub: unknown): ResponseLabel[] {
  const responsesValue = isPlainObject(stub) ? stub.responses : undefined;
  if (!Array.isArray(responsesValue)) return [];

  return responsesValue.map((raw, index): ResponseLabel => {
    if (!isPlainObject(raw)) return { index, kind: "other", detail: "" };

    /*
     * Branch order mirrors the ENGINE's own precedence in `From<StubResponseRaw>` —
     * `is` > `proxy` > `inject` > `fault` > `_rift` — rather than the order this module happens to
     * refuse things in. A response carrying both `is` and `proxy` is rendered by the engine as its
     * `is`, so labelling it "proxy" would describe behaviour the stub does not have. Refusing to
     * EDIT such a document (which `projectResponses` does, naming the extra key) and describing
     * what it DOES are different questions, and only the second one is asked here.
     */
    if ("is" in raw) return { index, kind: "is", detail: describeStatus(raw.is) };

    if ("proxy" in raw) {
      const proxy = raw.proxy;
      const to = isPlainObject(proxy) ? proxy.to : undefined;
      return { index, kind: "proxy", detail: typeof to === "string" ? to : "" };
    }
    if ("inject" in raw) return { index, kind: "inject", detail: "" };
    if ("fault" in raw) {
      return { index, kind: "fault", detail: typeof raw.fault === "string" ? raw.fault : "" };
    }
    // A `_rift` response runs a script; labelling it by a status code it has not got would tell the
    // operator the opposite of what the stub does. Stays in step with `FOREIGN_VARIANTS`.
    if ("_rift" in raw) return { index, kind: "_rift", detail: "" };

    // No variant key at all: the flat, wrapper-less form.
    return { index, kind: "is", detail: describeStatus(raw) };
  });
}

/**
 * The behaviours a response runs that the form does not edit, named for the card (#249 AC5).
 *
 * Without this a response carrying `decorate` or a JS-function `wait` is labelled identically to a
 * plain one: the only trace is a dotted key buried in the generic "Unmodelled:" banner text. The AC
 * asks for the operator to be able to see, per response, that it runs something the form is not
 * showing them — "recognised" is exactly the half that is supposed to survive the refusal.
 */
export function foreignBehaviorsOf(raw: unknown): string[] {
  if (!isPlainObject(raw)) return [];
  const container = "_behaviors" in raw ? raw._behaviors : "behaviors" in raw ? raw.behaviors : undefined;
  if (container === undefined) return [];

  const entries: string[] = [];
  const collect = (value: unknown): void => {
    if (!isPlainObject(value)) return;
    for (const [key, inner] of Object.entries(value)) {
      // A string `wait` is a JS function the engine evaluates — recognised, never edited here.
      if (key === "wait" && typeof inner === "string") entries.push("wait (function)");
      else if (key !== "wait" && key !== "repeat") entries.push(key);
    }
  };
  if (Array.isArray(container)) for (const element of container) collect(element);
  else collect(container);
  return entries;
}

/**
 * The status an `is` response answers with, as text.
 *
 * Reads the string form as well as the number one, mirroring the engine's own
 * `parse_status_code_value`. That is not an exotic case: `IsResponseOut` serializes `statusCode`
 * **as a string**, so every response read back from the admin API carries one — and a string
 * `statusCode` is exactly what sends a stub to raw-only, which is the one place these labels are
 * the operator's only readout. Reporting the default `200` for all of them would be confidently
 * wrong precisely where it is most relied upon.
 */
function describeStatus(inner: unknown): string {
  const statusCode = isPlainObject(inner) ? inner.statusCode : undefined;
  if (typeof statusCode === "number") return String(statusCode);
  if (typeof statusCode === "string" && /^\d+$/.test(statusCode)) return statusCode;
  return String(DEFAULT_STATUS_CODE);
}
