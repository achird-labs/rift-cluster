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

async function request(
  method: "GET" | Mutation,
  path: string,
  body?: unknown,
  options?: RequestOptions,
): Promise<unknown> {
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
    return null;
  }
  // A 2xx whose body will not parse is a broken contract, not an empty result: surfacing it as
  // `null` here is exactly the swallow that shows up later as a blank screen with no server-side
  // trace to correlate.
  try {
    return JSON.parse(text) as unknown;
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
  return request("GET", path, undefined, options) as Promise<T>;
}

export function apiSend<T = unknown>(
  method: Mutation,
  path: ApiPath | (string & {}),
  body?: unknown,
  options?: RequestOptions,
): Promise<T> {
  return request(method, path, body, options) as Promise<T>;
}
