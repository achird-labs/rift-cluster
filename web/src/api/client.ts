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

/** Per-call context the schema cannot express. Currently just the tenant the screen is showing. */
export type RequestOptions = { tenant?: string | null | undefined };

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

  const response = await fetch(path, {
    method,
    headers,
    // The session is an HttpOnly cookie (RFC-006 §5.3) — no script can read it, so it can only be
    // sent by asking the browser to attach it.
    credentials: "same-origin",
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
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
