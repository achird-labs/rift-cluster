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
): Promise<unknown> {
  const headers: Record<string, string> = {};
  if (method !== "GET") {
    headers[CSRF_HEADER] = "1";
  }
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
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

export function apiGet(path: ApiPath | (string & {})): Promise<unknown> {
  return request("GET", path);
}

export function apiSend(
  method: Mutation,
  path: ApiPath | (string & {}),
  body?: unknown,
): Promise<unknown> {
  return request(method, path, body);
}
