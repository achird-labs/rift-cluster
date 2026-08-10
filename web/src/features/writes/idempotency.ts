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
 * ## What this does and does not close, stated rather than assumed
 *
 * The key is honoured on the routes that flow through the admin front's `build_and_run` (imposters,
 * stubs, lifecycle, routes, scenarios, flow state) and through its tenancy surface (tenants,
 * principals, bindings) — those derive their op id from the header via `base_op_id`.
 *
 * Four routes accept the header and **ignore** it today, because they return from `terminate`
 * before it is read (sources: put, delete and pull; and `try`) or mint a fresh op id unconditionally
 * (space teardown). Sending the key there is harmless and costs nothing, and all four happen to be
 * idempotent or convergent by nature — an upsert by id, a delete, a re-pull that reapplies the same
 * source, a generation bump that is monotone, and a `try` that writes no state at all. So the
 * exposure is small, but it is not zero and it is not what a reader would assume from seeing the
 * header go out. Closing it is a server-side change and is tracked separately.
 */

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
