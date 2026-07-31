import { useState } from "react";
import type { FormEvent, ReactNode } from "react";

import { ApiError, apiSend } from "../api/client.ts";
import { API_PATHS } from "../api/paths.ts";
import { describe } from "../components/primitives.tsx";

/**
 * The API key → session cookie exchange (C2's `POST /session`, RFC-006 §5.3).
 *
 * §9.3: this is the one moment the long-lived API key transits the page, so it lives in component
 * state and nowhere else — never `localStorage`, never `sessionStorage`, never a URL — and is
 * dropped the moment the call returns. The session itself comes back as an `HttpOnly` cookie no
 * script on this page can read.
 */
export function Login({ onAuthenticated }: { onAuthenticated: () => void }): ReactNode {
  const [apiKey, setApiKey] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(event: FormEvent): Promise<void> {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await apiSend("POST", API_PATHS.session, { apiKey });
      setApiKey("");
      onAuthenticated();
    } catch (cause) {
      // A rejected key is the expected outcome of a typo, not a server fault, and saying so is the
      // difference between "check what you pasted" and "page the on-call".
      setApiKey("");
      setError(
        cause instanceof ApiError && cause.status === 401
          ? "That API key was not accepted."
          : describe(cause),
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="login">
      <h1>Rift Console</h1>
      <form onSubmit={(event) => void submit(event)}>
        <label htmlFor="api-key">API key</label>
        <input
          id="api-key"
          // `password`, so a shoulder-surfer and a screen-share see nothing. `off` on both
          // autocomplete and password managers: this is a fleet credential, not a login.
          type="password"
          autoComplete="off"
          value={apiKey}
          onChange={(event) => setApiKey(event.target.value)}
        />
        <button type="submit" disabled={busy || apiKey.length === 0}>
          Sign in
        </button>
      </form>
      {error === null ? null : (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      <p className="muted">
        The key is exchanged for a session cookie and is not stored by this page.
      </p>
    </main>
  );
}
