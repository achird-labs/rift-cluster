import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import viteConfig, {
  ADMIN_PROXY_EXACT,
  ADMIN_PROXY_PREFIXES,
  DEV_ADMIN_URL,
} from "../../vite.config.ts";

const CONTRACT = fileURLToPath(
  new URL("../../../docs/api/openapi-ee.yaml", import.meta.url),
);

/**
 * The path templates the contract publishes, read out of the YAML rather than the generated
 * `schema.ts` — `paths` there is a *type*, so it has no runtime value to enumerate.
 *
 * Deliberately a narrow reader, not a YAML parser: the only structure it needs is "two-space-indented
 * keys beginning with `/`, inside the top-level `paths:` block". It asserts it actually found the
 * block, so a restructured contract fails the test instead of silently yielding an empty set that
 * every assertion below would then vacuously pass.
 */
function contractPaths(): string[] {
  const lines = readFileSync(CONTRACT, "utf8").split("\n");
  const start = lines.findIndex((line) => line === "paths:");
  if (start === -1) {
    throw new Error(`no top-level 'paths:' block in ${CONTRACT}`);
  }
  const found: string[] = [];
  for (const line of lines.slice(start + 1)) {
    // A new top-level key ends the block.
    if (/^\S/.test(line)) break;
    const match = /^ {2}(\/\S*?):\s*$/.exec(line);
    if (match?.[1]) found.push(match[1]);
  }
  return found;
}

describe("dev server proxy", () => {
  it("declares a proxy entry for every admin prefix, pointed at the admin front", () => {
    const proxy = viteConfig.server?.proxy;
    expect(proxy, "vite config must configure server.proxy").toBeDefined();

    for (const prefix of ADMIN_PROXY_PREFIXES) {
      const entry = proxy?.[prefix];
      expect(entry, `no proxy entry for ${prefix}`).toBeDefined();
      expect(typeof entry === "object" ? entry.target : entry).toBe(DEV_ADMIN_URL);
    }
  });

  it("covers every path the published contract serves", () => {
    const paths = contractPaths();
    // The reader above is the kind of thing that quietly returns nothing. If it ever does, this
    // whole test degrades into asserting that an empty list is covered.
    expect(paths.length).toBeGreaterThan(10);

    const uncovered = paths.filter(
      (path) =>
        !ADMIN_PROXY_PREFIXES.some((prefix) => path.startsWith(prefix)) &&
        !ADMIN_PROXY_EXACT.some((exact) => path === exact),
    );
    expect(
      uncovered,
      `these contract paths would 404 against the dev server — add a prefix to ADMIN_PROXY_PREFIXES`,
    ).toEqual([]);
  });

  it("forwards the contract's root path without capturing the console itself", () => {
    // `/` is published by the contract but cannot be a prefix key: Vite prefix-matches, so a plain
    // `"/"` entry would forward `/console/` — the app's own URL — to the admin front, and `pnpm dev`
    // would serve nothing. The anchored regex is what keeps both true at once.
    const proxy = viteConfig.server?.proxy ?? {};
    expect(Object.keys(proxy)).toContain("^/$");
    expect(Object.keys(proxy)).not.toContain("/");
    expect(new RegExp("^/$").test("/console/")).toBe(false);
  });

  it("emits assets under the /console/ base the binary serves them from", () => {
    // With Vite's default base of `/`, every emitted `<script src>` points at the admin API root,
    // which proxies upstream and 404s. That failure only shows up in the embedded build.
    expect(viteConfig.base).toBe("/console/");
  });

  it("does not ship sourcemaps into the embedded bundle", () => {
    expect(viteConfig.build?.sourcemap).toBe(false);
  });
});
