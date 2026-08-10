import type { paths } from "./schema.ts";

/**
 * The thin fetch wrapper over the generated contract types (RFC-006 §5.1).
 *
 * Deliberately thin. The generated `schema.ts` carries the shapes; this file carries the three
 * things the schema cannot express and that every call must get right: the session cookie has to
 * ride along, mutations have to carry the CSRF header, and a non-2xx has to become a thrown error
 * rather than a `data` a screen renders as if it were a result.
 */

/** Every path the contract publishes. Keeps callers from inventing a route the front does not serve. */
export type ApiPath = keyof paths;

/**
 * The double-submit header from RFC-006 §5.3. The front rejects a cookie-authenticated mutation
 * without it; cross-origin HTML cannot set it without a preflight the front never answers.
 */
export const CSRF_HEADER = "X-Rift-CSRF";

/**
 * RFC-002 §8.1's tenant selector. It **selects among the principal's existing bindings; it never
 * grants one** — `admin_front.rs::requested_tenant` reads it and `authorize_action` only ever
 * intersects it against bindings already loaded from applied state.
 *
 * Sent only when a tenant is actually in view. An empty value would be a claim of a tenant named
 * `""`, which is a tenant the caller is not bound to and answers 404 (§8.4) — quite different from
 * omitting the header, which means "my default tenant".
 */
export const TENANT_HEADER = "X-Rift-Tenant";

/**
 * The optimistic-concurrency token the fleet stamps on a single-imposter read, and takes back as
 * `If-Match` on the write (contract: `default:<port>@<revision>`).
 *
 * Reading it is the whole point of `apiGetWithRevision`: a stub write sent without it is
 * last-writer-wins, which is the lost-update bug stated as a default.
 */
export const REVISION_HEADER = "Rift-Cluster-Revision";

/**
 * The header that makes a retried write safe to send (#371).
 *
 * The admin front derives a deterministic op id from it (`base_op_id`), and the Raft layer refuses
 * to apply an op id it has already applied — so two requests carrying the same key are one write,
 * however many times the network made us send it.
 */
export const IDEMPOTENCY_HEADER = "Idempotency-Key";

/** Per-call context the schema cannot express: the tenant in view, and the write's precondition. */
export type RequestOptions = {
  tenant?: string | null | undefined;
  /**
   * Sent as `If-Match`. Omit it and the write is unconditional — so callers that hold a token pass
   * it, and callers that do not hold one refuse to write rather than sending nothing.
   */
  ifMatch?: string | null | undefined;
  /**
   * Sent as `Idempotency-Key` on mutations (#371). Ignored on `GET`, which cannot double-apply.
   *
   * **Must be stable across retries of one intent and fresh for a new one** — `keyedAttempt` in
   * `features/writes/idempotency.ts` is what maintains that, and no call site should mint one
   * inline. A key that changes per attempt buys nothing; one that never changes is worse than
   * nothing, because a keyed retry of a `409` dedups back to that same `409` by design.
   */
  idempotencyKey?: string | null | undefined;
};

/**
 * A body the caller has already serialized, sent verbatim.
 *
 * The raw-JSON stub editor saves the operator's own text. Handing that text to `JSON.stringify` as
 * a string would send a JSON *string*; parsing and re-stringifying it would reorder keys and drop
 * their whitespace, producing a stored stub that differs from what they typed in ways they never
 * asked for. This is the only way to say "these exact bytes are the body".
 */
export class RawJsonBody {
  readonly text: string;

  constructor(text: string) {
    this.text = text;
  }
}

/** An admin response that was not 2xx, carrying enough to render a useful message. */
export class ApiError extends Error {
  readonly status: number;
  readonly body: string;

  constructor(status: number, body: string, options?: { cause: unknown }) {
    // `cause` is forwarded rather than dropped: when this wraps a `JSON.parse` failure, the original
    // `SyntaxError` says *where* the body broke, and that is the whole diagnosis.
    super(`${status}: ${body.slice(0, 500)}`, options);
    this.name = "ApiError";
    this.status = status;
    this.body = body;
  }
}

type Mutation = "POST" | "PUT" | "DELETE" | "PATCH";

/**
 * The header a terminated write's success response carries its op id in
 * (`decorate.rs::HEADER_OP_ID`). Only read as a fallback: the `202` body carries the ids too, and
 * for a multi-op mutation the body's `opIds` are the *only* pollable ones.
 */
export const OP_ID_HEADER = "Rift-Cluster-Op-Id";

/**
 * What a mutation actually did — the distinction a bare `2xx` throws away.
 *
 * `202 AcceptedParked` means the write was durably parked and is **still committing**; the contract
 * says in as many words to "poll the returned op id rather than assuming this response's absence of
 * a body reflects the final state". Returning it as a plain value is how the console came to report
 * a committing write as saved, so the two outcomes are separate constructors and every caller has
 * to say which one it is handling.
 */
export type SendResult<T> =
  | { kind: "applied"; data: T }
  | { kind: "parked"; opIds: readonly string[] };

/** The op ids a `202` body carries, preferring the derived ids of a multi-op mutation. */
function parkedOpIds(body: unknown, response: Response): string[] {
  const payload = (body ?? {}) as { opId?: unknown; opIds?: unknown };
  /*
   * `opIds` first, and not merely as a nicety: `admin_front.rs` parks only the *derived* ids of a
   * multi-op mutation and never the base, so a client polling the bare `opId` of a batch
   * `PUT /imposters` would 404 forever.
   *
   * Filter before testing for emptiness, not after. Testing the raw array and returning the
   * filtered one means a malformed `opIds` (entries that are not strings — a contract violation,
   * but one worth surviving) yields `[]` and skips the `opId` and header fallbacks that might each
   * still hold a usable id.
   */
  const derived = Array.isArray(payload.opIds)
    ? payload.opIds.filter((id): id is string => typeof id === "string")
    : [];
  if (derived.length > 0) return derived;
  if (typeof payload.opId === "string") return [payload.opId];
  /*
   * Best-effort last resort. The header carries the *base* id, which a multi-op mutation never
   * parks — polling it would 404 and settle as unobservable rather than wrong. Unreachable against
   * the current server, which always sends a body with the ids on the async path.
   */
  const header = response.headers.get(OP_ID_HEADER);
  return header === null ? [] : [header];
}

async function request(
  method: "GET" | Mutation,
  path: string,
  body?: unknown,
  options?: RequestOptions,
): Promise<{ response: Response; body: unknown }> {
  const headers: Record<string, string> = {};
  if (method !== "GET") {
    headers[CSRF_HEADER] = "1";
  }
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  const tenant = options?.tenant;
  if (tenant !== undefined && tenant !== null && tenant !== "") {
    headers[TENANT_HEADER] = tenant;
  }
  const ifMatch = options?.ifMatch;
  if (ifMatch !== undefined && ifMatch !== null && ifMatch !== "") {
    headers["If-Match"] = ifMatch;
  }
  const idempotencyKey = options?.idempotencyKey;
  // Never on `GET` (#371): a read cannot double-apply, and the admin front refuses the header on
  // one route outright (minting a principal), so sending it where it has no meaning invites a 400
  // for nothing.
  if (method !== "GET" && idempotencyKey !== undefined && idempotencyKey !== null && idempotencyKey !== "") {
    headers[IDEMPOTENCY_HEADER] = idempotencyKey;
  }

  const response = await fetch(path, {
    method,
    headers,
    // The session is an HttpOnly cookie (RFC-006 §5.3) — no script can read it, so it can only be
    // sent by asking the browser to attach it.
    credentials: "same-origin",
    ...(body === undefined
      ? {}
      : { body: body instanceof RawJsonBody ? body.text : JSON.stringify(body) }),
  });

  const text = await response.text();
  if (!response.ok) {
    throw new ApiError(response.status, text);
  }
  if (text.length === 0) {
    return { response, body: null };
  }
  // A 2xx whose body will not parse is a broken contract, not an empty result: surfacing it as
  // `null` here is exactly the swallow that shows up later as a blank screen with no server-side
  // trace to correlate.
  try {
    return { response, body: JSON.parse(text) as unknown };
  } catch (cause) {
    throw new ApiError(
      response.status,
      `admin front returned ${response.status} with a body that is not JSON: ${text.slice(0, 200)}`,
      { cause },
    );
  }
}

/**
 * Read a path and return the server's response text **verbatim**, unparsed.
 *
 * Exists for export (#251). Every other read goes through `apiGet`, which parses — and for an
 * export that parse is exactly the problem: `JSON.parse` followed by `JSON.stringify` reorders
 * nothing but re-indents everything and drops the server's own formatting, so a mock exported
 * twice from two console versions diffs against itself in a repository. What a developer commits
 * beside their tests should be the bytes the fleet actually returned.
 *
 * Errors are raised exactly as `request` raises them, so a 403 or a 404 still surfaces normally.
 */
export async function apiGetText(
  path: ApiPath | (string & {}),
  options?: RequestOptions,
): Promise<string> {
  const headers: Record<string, string> = { Accept: "application/json" };
  const tenant = options?.tenant;
  if (tenant !== undefined && tenant !== null && tenant !== "") headers[TENANT_HEADER] = tenant;

  const response = await fetch(path, {
    method: "GET",
    headers,
    credentials: "same-origin",
  });
  const text = await response.text();
  if (!response.ok) throw new ApiError(response.status, text);
  return text;
}

/**
 * `T` is an **assertion**, not a validation — nothing here checks the body against the schema.
 *
 * It defaults to `unknown` so an un-annotated call still forces the caller to narrow. Naming the
 * generated contract type at the call site is the honest middle ground: the shape comes from the
 * same document the server renders from, and the alternative (a cast at every call site) is the
 * identical unsafety written more loudly. Runtime validation is a separate decision, and would
 * belong here rather than in the screens.
 */
export function apiGet<T = unknown>(
  path: ApiPath | (string & {}),
  options?: RequestOptions,
): Promise<T> {
  return request("GET", path, undefined, options).then(({ body }) => body as T);
}

/** A read plus the optimistic-concurrency token the matching write has to quote back. */
export type RevisionedRead<T> = {
  data: T;
  /** From the `Rift-Cluster-Revision` response header; `null` when the response carried none. */
  revision: string | null;
};

/**
 * `apiGet`, plus the revision the response was stamped with.
 *
 * Beside `apiGet` rather than folded into it: every existing caller wants the body and nothing else,
 * and widening that return type would make ninety call sites pay for a token three of them use.
 * Only the reads that feed a conditioned write need this — today, the single-imposter read whose
 * revision becomes the stub editor's `If-Match`.
 *
 * A `null` revision is a real answer, not a default to paper over: it means this response carried no
 * token, and a caller holding `null` has nothing to condition a write on.
 */
export async function apiGetWithRevision<T = unknown>(
  path: ApiPath | (string & {}),
  options?: RequestOptions,
): Promise<RevisionedRead<T>> {
  const { response, body } = await request("GET", path, undefined, options);
  return { data: body as T, revision: response.headers.get(REVISION_HEADER) };
}

/**
 * The three headers a merge-on-read fan-out stamps on a fleet journal read (#147 D/H, #225):
 * `admin_front.rs::terminate_read_saved_requests` and `decorate.rs`'s `HEADER_*` constants are the
 * source of truth for the names and the additive-only convention documented on `MergedRead`.
 */
export const PARTIAL_HEADER = "Rift-Cluster-Partial";
export const NEXT_INDEX_HEADER = "x-rift-next-index";
export const TRUNCATED_HEADER = "x-rift-truncated";

/** A read of a fleet-wide merge, plus the three facts only the response headers carry. */
export type MergedRead<T> = {
  data: T;
  /**
   * From `Rift-Cluster-Partial`. The header is additive-only — stamped `true` or not stamped at
   * all, never `false` — because upstream's own clients already test against that shape, so
   * presence is the whole signal: a merge that reached every node in its budget carries no header.
   */
  partial: boolean;
  /**
   * From `x-rift-next-index`, the opaque vector token for the next `?since=` poll, verbatim.
   * `null` when the response carried none — a real answer meaning "no cursor offered", never a
   * fabricated default a caller could mistake for "resume from the start".
   */
  next: string | null;
  /**
   * From `x-rift-truncated`, additive-only like `partial`: retention evicted entries the reader's
   * cursor had not reached yet, so the merge this response describes is missing rows a slower poll
   * would have caught in time.
   */
  truncated: boolean;
};

/**
 * `apiGet`, plus the three merge-only facts a fleet-wide journal read stamps as headers rather than
 * folding into the body — the body stays the same bare `RecordedRequest[]` a single node always
 * served, so an older client (or a proxy that strips unknown headers) still gets a readable answer,
 * just without the coverage and paging information this type exists to carry.
 *
 * Beside `apiGetWithRevision` rather than folded into `apiGet` for the same reason that one is: the
 * one caller that needs merge facts (`useRequestLog`) is not the ninety that just want a body.
 */
export async function apiGetMerged<T = unknown>(
  path: ApiPath | (string & {}),
  options?: RequestOptions,
): Promise<MergedRead<T>> {
  const { response, body } = await request("GET", path, undefined, options);
  return {
    data: body as T,
    partial: response.headers.get(PARTIAL_HEADER) !== null,
    next: response.headers.get(NEXT_INDEX_HEADER),
    truncated: response.headers.get(TRUNCATED_HEADER) !== null,
  };
}

/**
 * Send a mutation, reporting **whether it actually landed**.
 *
 * The `SendResult` is deliberately not unwrapped here. Only 14 of the admin routes can answer
 * `202`, so most callers know a park is impossible — but "most" is the problem: when the return
 * type was a bare `T`, the three that *can* park read exactly like the seven that cannot, and all
 * ten reported success on a write still in flight. Making the caller name the case is what stops
 * the next mutation from inheriting that. Use `applied()` to assert a route cannot park.
 */
export function apiSend<T = unknown>(
  method: Mutation,
  path: ApiPath | (string & {}),
  body?: unknown,
  options?: RequestOptions,
): Promise<SendResult<T>> {
  return request(method, path, body, options).then(({ response, body: parsed }) =>
    response.status === 202
      ? { kind: "parked", opIds: parkedOpIds(parsed, response) }
      : { kind: "applied", data: parsed as T },
  );
}
