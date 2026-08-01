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
       * `clear()`, not `invalidateQueries()`. Invalidation refetches and leaves the previous
       * principal's data readable while those requests are in flight — so the console would show
       * one operator's imposters and tenants to whoever signs in next, for as long as the refetch
       * takes. Clearing drops the cache outright; `whoami` then refetches, answers 401, and `App`
       * renders the login screen.
       */
      client.clear();
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
