// `vitest/config`'s re-export, not `vite`'s: it is the one that accepts the `test` block below.
// Importing `defineConfig` from `vite` type-errors on `test` rather than ignoring it.
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

/**
 * Every path prefix the console's API client calls, and which the dev server therefore has to
 * forward to a real admin front (RFC-006 §7, "Dev loop").
 *
 * Exported so `src/__tests__/vite-config.test.ts` can assert this table against the paths the
 * generated client actually publishes. A prefix missing here does not fail the build — it fails at
 * runtime, in the browser, as a 404 from Vite's own static handler that looks like a server bug.
 */
export const ADMIN_PROXY_PREFIXES = [
  "/imposters",
  "/admin",
  "/front-door",
  // RFC-004 S2 (issue #278): the spec import/deploy surface, EE-only and root-mounted like the
  // front door's route table.
  "/specs",
  "/session",
  "/_fleet",
  "/openapi.json",
  // Upstream's own surface, published through the front (`x-rift-origin: upstream`).
  "/health",
  "/config",
  "/logs",
  "/metrics",
  // Gateway traffic. Not an admin route, but the console links to imposter endpoints and a
  // relative link would otherwise resolve against the dev server.
  "/__rift",
] as const;

/**
 * Contract paths that must be forwarded but cannot be expressed as a prefix.
 *
 * Only `/` so far, and it is the reason this list exists at all: Vite matches a plain proxy key as a
 * *prefix*, so `"/"` would capture every request including `/console/` and the dev server would
 * forward its own app to the admin front. These become anchored regex keys instead.
 */
export const ADMIN_PROXY_EXACT = ["/"] as const;

/** Vite treats a key beginning with `^` as a regex; anchor both ends so it matches that path only. */
function exactKey(path: string): string {
  return `^${path.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`;
}

/**
 * Where `pnpm dev` forwards those prefixes. `127.0.0.1:2525` is a single node's admin API
 * (`deploy/Dockerfile:69`); the compose stack publishes node 1 on `12525`, so that case is the
 * reason this is an env var rather than a constant.
 */
export const DEV_ADMIN_URL = process.env.RIFT_ADMIN_URL ?? "http://127.0.0.1:2525";

export default defineConfig({
  // The binary serves the console under `/console/` (RFC-006 §7), so emitted asset URLs must be
  // absolute-from-`/console/`. With Vite's default `/` base every `<script src>` would point at the
  // admin API root and 404 through `proxy`.
  base: "/console/",
  plugins: [react()],
  build: {
    outDir: "dist",
    // `rust-embed` hard-fails a build whose folder is missing but is happy with a stale one, so the
    // release lane's `pnpm build` must leave nothing behind from a previous run.
    emptyOutDir: true,
    // No sourcemaps in the shipped bundle: they would be embedded into the binary and served, which
    // publishes the console's source to anyone who can reach the admin port.
    sourcemap: false,
  },
  server: {
    proxy: Object.fromEntries(
      [
        ...ADMIN_PROXY_PREFIXES.map((prefix) => prefix as string),
        ...ADMIN_PROXY_EXACT.map(exactKey),
      ].map((key) => [key, { target: DEV_ADMIN_URL, changeOrigin: false }]),
    ),
  },
  test: {
    // Node stays the default and the component tests opt into jsdom with a per-file
    // `@vitest-environment` docblock. The other way round does not work: under jsdom
    // `import.meta.url` is an `http:` URL, so the two tests that read repository files
    // (this config's contract check, and the traceability scan) fail on `fileURLToPath`.
    environment: "node",
    include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
    environmentOptions: {
      // An explicit, non-opaque origin. jsdom's default document is `about:blank`, whose origin is
      // opaque — and `localStorage` is unavailable on an opaque origin, so without this the tenant
      // selection cannot be exercised at all.
      jsdom: { url: "http://localhost/console/" },
    },
  },
});
