import { useQuery } from "@tanstack/react-query";

import { apiGet } from "./api/client.ts";

/**
 * The scaffold shell.
 *
 * C3 (#186) delivers the embed pipeline, not the console: this renders enough to prove the bundle
 * loads, React mounts, and a generated-client call reaches the admin front through the same origin
 * the CSP restricts it to. The real information architecture (RFC-006 §4) arrives with C4 (#187).
 */
export function App() {
  const whoami = useQuery({
    queryKey: ["whoami"],
    queryFn: () => apiGet("/admin/whoami"),
  });

  return (
    <main className="shell">
      <h1>Rift Console</h1>
      <p className="tagline">
        Embed pipeline online. Screens land in C4.
      </p>
      <section>
        <h2>Admin front reachability</h2>
        {whoami.isPending && <p>Checking…</p>}
        {whoami.isError && (
          <p className="err">
            Not authenticated, or the admin front is unreachable: {whoami.error.message}
          </p>
        )}
        {whoami.isSuccess && (
          <pre className="whoami">{JSON.stringify(whoami.data, null, 2)}</pre>
        )}
      </section>
    </main>
  );
}
