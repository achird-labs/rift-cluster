import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import type { ReactNode } from "react";

import { apiSend } from "../api/client.ts";
import { API_PATHS } from "../api/paths.ts";
import { applied } from "../features/writes/commit.ts";
import { describe } from "../components/primitives.tsx";

/**
 * Sign out — `DELETE /session` (C2, RFC-006 §5.3).
 *
 * The session is an `HttpOnly` cookie, which is what makes this control load-bearing rather than a
 * convenience: no script on this page can clear it, so without a sign-out the only way off a
 * session is to expire it or clear site data by hand. That also made the console untestable with
 * more than one principal, which is the shape of the gap that hid it — every screen was built and
 * reviewed under a single key.
 *
 * The server renders the clearing `Set-Cookie` (`Max-Age=0`); this only has to ask for it and then
 * stop trusting what it already read.
 */
export function SignOut(): ReactNode {
  const client = useQueryClient();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function signOut(): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      // `applied` because this route cannot park: it is terminated by `admin_front` and is not a
      // Raft write, so a `202` would be a contract violation rather than a case to handle.
      applied(await apiSend("DELETE", API_PATHS.session));

      /*
       * Two steps, and the order is the whole point.
       *
       * First drop every read *except* `whoami`, so the previous principal's imposters, tenants and
       * audit rows are gone rather than merely stale — invalidation would leave them on screen for
       * whoever signs in next, for as long as the refetches take.
       *
       * Then reset `whoami` specifically, which refetches it because `App` still has it mounted.
       * That 401 is what moves the console to the login screen. A bare `clear()` does not do this:
       * it removes the query while leaving the mounted observer holding its last successful result,
       * so nothing refetches and the console keeps rendering the signed-out principal's shell. That
       * bug shipped and was caught by hand, because the test asserted the cache had emptied instead
       * of asserting the operator reached the login screen — `signout-redirect.test.tsx` now pins
       * the behaviour rather than the mechanism.
       */
      client.removeQueries({ predicate: (query) => query.queryKey[0] !== "whoami" });
      await client.resetQueries({ queryKey: ["whoami"] });
    } catch (cause) {
      /*
       * Not a swallow, and specifically not a silent one: if the server refused to end the
       * session, the cookie is still live and the operator is still signed in. Saying so is the
       * difference between that and a console that looks signed out while every request it makes
       * still carries the old principal.
       */
      setError(describe(cause));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="signout">
      <button
        type="button"
        className="btn sm"
        data-testid="sign-out"
        disabled={busy}
        onClick={() => void signOut()}
      >
        {busy ? "Signing out…" : "Sign out"}
      </button>
      {error === null ? null : (
        <p className="error signout-error" role="alert">
          Still signed in — {error}
        </p>
      )}
    </div>
  );
}
