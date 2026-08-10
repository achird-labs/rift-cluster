import { ApiError } from "../../api/client.ts";

/**
 * Keys that make a retried admin write safe (#371).
 *
 * The admin API has always accepted `Idempotency-Key` — it derives a deterministic op id from it,
 * and the Raft layer refuses an op id it has already applied. The console never sent one, so every
 * write was exactly-once only by luck.
 *
 * **The bug this closes needs a timeout, not an error.** A write commits, its response is lost — a
 * dropped connection, a proxy giving up, a node going away mid-reply — and the console shows a
 * failure for a write that actually landed. The operator retries, and without a key that retry is a
 * *second* op. Some routes are protected by accident (`POST /imposters` on a fixed port would 409
 * the second time, because the port is taken); that is a coincidence of one route, not a design,
 * and it does not hold where the second apply is silent.
 *
 * ## Why a key cannot simply be per-attempt or per-intent-forever
 *
 * Both of the obvious policies are wrong, in opposite directions:
 *
 * - **A fresh key per attempt** is the same as sending none. The retry is a new op again.
 * - **One key that never rotates** is worse than sending none, and this is the subtle half. The
 *   admin front's own module doc: *"A keyed retry (same `Idempotency-Key`) of a `409` dedups to
 *   that same `409` by design — rebase and retry with a fresh key."* So a key held across a real
 *   refusal traps the operator in a permanent replay of it: they fix the conflict, press save, and
 *   are handed the stale `409` forever.
 *
 * ## The policy
 *
 * A key is held for as long as the **outcome is unknown**, and rotated the moment the fleet gives
 * a definitive answer of any kind:
 *
 * - the request threw without a response (timeout, dropped connection, DNS) → **keep the key**, so
 *   the retry is recognized as the same write;
 * - the fleet answered — 2xx, 4xx, 5xx alike — → **rotate**, because whatever happens next is a new
 *   intent, and reusing the key would replay the answer instead of acting on it.
 *
 * That is the whole of it. It protects exactly the case that was broken and stays out of the way of
 * the case the server explicitly warns about.
 *
 * ## Send it exactly where the contract declares it (#389)
 *
 * `openapi-ee.yaml` declares `Idempotency-Key` on fifteen routes — the ones reaching the admin
 * front's `build_and_run` (imposters, stubs, lifecycle, front-door routes, scenarios, flow state)
 * and its tenancy surface (tenants, principals, bindings). Those derive their op id from the header
 * via `base_op_id`, and those are the ones {@link keyedAttempt} is for.
 *
 * The routes in {@link UNDECLARED} do **not** declare the parameter, and the fleet does not read it
 * on them — the source writes and `try` return from `terminate` before the header is parsed, and
 * space teardown mints its own op id. An earlier revision of this console sent a key there anyway.
 * It was harmless on the wire and wrong as documentation: a reader seeing the header go out would
 * reasonably conclude those writes were retry-safe by key, when what actually makes them safe to
 * repeat is that each is convergent on its own terms — an upsert by id, a delete, a re-pull that
 * reapplies the same source, a monotone generation bump, and a `try` that writes no state at all.
 *
 * So the console now sends the key where it means something and not where it does not, and the
 * reason each of those routes is safe is recorded above rather than implied by a header.
 */

/**
 * Write routes that do **not** declare `Idempotency-Key`, and why repeating each is safe anyway.
 *
 * Exported so the rule is checkable rather than a convention: a test asserts the console sends no
 * key on these, which is what stops the next call site from quietly re-adding one.
 */
export const UNDECLARED: Readonly<Record<string, string>> = Object.freeze({
  // Keyed by the contract's own path templates, because that is what the test resolves them
  // against — `{sourceId}`, not the console's `{id}`.
  "POST /admin/sources": "an upsert by id — a repeat converges on the same record",
  "DELETE /admin/sources/{sourceId}": "a delete — absent is the same outcome as removed",
  "POST /admin/sources/{sourceId}/pull": "re-applies the same source content",
  "POST /admin/imposters/{port}/try":
    "writes no state; a repeat is a second probe, not a second write",
  "DELETE /imposters/{port}/spaces/{flowId}": "the journal generation bump is monotone",
});

/**
 * A fresh key.
 *
 * `crypto.randomUUID` is present in every browser this console supports and in jsdom, but it is
 * absent over plain HTTP on some older engines, where `crypto` exists and the method does not — so
 * the fallback is a real branch rather than defensive noise. It only has to be unique, not
 * unguessable: the key names an operation, and the operation is already authorized by the session.
 */
export function mintKey(): string {
  const source = globalThis.crypto;
  if (typeof source?.randomUUID === "function") return source.randomUUID();
  return `k-${String(Date.now())}-${Math.random().toString(36).slice(2, 12)}`;
}

/**
 * Whether an attempt's failure left the write's outcome **unknown**.
 *
 * An `ApiError` means the fleet answered, so the outcome is known even when it is a refusal. Any
 * other rejection — `TypeError: Failed to fetch`, an abort, a timeout — means the request may have
 * been applied without us hearing about it, which is precisely when the key must be kept.
 */
function outcomeIsUnknown(error: unknown): boolean {
  return !(error instanceof ApiError);
}

/**
 * Run `attempt` under a key that survives an unknown outcome and rotates after a known one.
 *
 * Hand the returned function to a `mutationFn`; it supplies the key to pass as
 * `RequestOptions.idempotencyKey`. One instance per mutation hook, so a screen's repeated saves
 * share the chain that a single logical write needs, and unrelated screens never do.
 *
 * ```ts
 * const keyed = keyedAttempt();
 * // inside mutationFn:
 * await keyed((idempotencyKey) => apiSend("PUT", path, body, { tenant, idempotencyKey }));
 * ```
 */
export function keyedAttempt(): <T>(attempt: (key: string) => Promise<T>) => Promise<T> {
  let held: string | null = null;

  return async <T>(attempt: (key: string) => Promise<T>): Promise<T> => {
    const key = held ?? mintKey();
    held = key;
    try {
      const value = await attempt(key);
      // The fleet answered and accepted. The next write through this hook is a new intent.
      held = null;
      return value;
    } catch (error) {
      // Rotate on a definitive refusal, hold on an unknown outcome — see this module's doc.
      if (!outcomeIsUnknown(error)) held = null;
      throw error;
    }
  };
}
