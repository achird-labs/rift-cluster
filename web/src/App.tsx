import { useQueryClient } from "@tanstack/react-query";
import type { ReactNode } from "react";

import { ApiError } from "./api/client.ts";
import { Shell } from "./app/Shell.tsx";
import { SESSION_KEY, useSession } from "./app/session.tsx";
import { ErrorNote } from "./components/primitives.tsx";
import { Login } from "./screens/Login.tsx";

/**
 * Sign-in, and nothing else.
 *
 * Since #550 there is one credential and one identity, so there is nothing left for this component
 * to resolve beyond "is there a session". A `401` is the login screen; anything else that fails is
 * an admin front we could not reach, and saying which is the difference between "paste your key
 * again" and "the fleet is down".
 */
export function App(): ReactNode {
  const client = useQueryClient();
  const session = useSession();

  if (session.isPending) return <p className="muted">Signing in…</p>;

  if (session.isError) {
    if (session.error instanceof ApiError && session.error.status === 401) {
      return <Login onAuthenticated={() => void client.invalidateQueries({ queryKey: SESSION_KEY })} />;
    }
    return (
      <main className="screen">
        <ErrorNote error={session.error} context="Could not reach the admin front" />
      </main>
    );
  }

  return <Shell />;
}
