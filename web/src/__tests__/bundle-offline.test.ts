import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync, rmSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

/**
 * RFC-006 §9.1 and the air-gap criterion, stated as something a machine can check.
 *
 * The console is served from a binary with `Content-Security-Policy: default-src 'self'`
 * (`console.rs`), frequently on a network with no route to the internet at all. So an asset that
 * fetches from a CDN does not degrade — it fails, at runtime, in an operator's browser, on a screen
 * that looked fine in review. C5 (#188) adds monaco, which is the single most likely way that
 * happens: the ecosystem's ergonomic wrappers (`@monaco-editor/react`, monaco's own AMD `loader.js`)
 * resolve from jsdelivr **by default**, and nothing else in this repo would notice.
 *
 * So the assertion is made against the bytes that ship — build the bundle and read every emitted
 * asset. Reviewing the imports would not catch a transitive dependency that inlines a URL.
 *
 * ## Why this is not "the string `https://` appears nowhere"
 *
 * That was the first draft, and it fails on the console as it stands today: React's warnings link to
 * react.dev, monaco's to code.visualstudio.com, monaco's JSON worker carries the `$id`s of every
 * JSON-Schema draft, and SVG and MathML rendering needs their namespace URIs — which are
 * identifiers, not addresses, and are never dereferenced. A gate that fails on all of those either
 * gets a growing allowlist of substrings, or gets deleted. Neither ends with the criterion enforced.
 *
 * What the criterion actually says is **no external subresource**: nothing the page *loads* from
 * another origin. So the scan looks for the constructs that load — a module import, a worker, an
 * `importScripts`, a stylesheet `@import` or `url()`, a `src`/`href` in the emitted HTML — with an
 * absolute URL in them. Plus a denylist of the package CDNs, which catches the specific failure this
 * gate exists for even if it arrives through a spelling not enumerated below.
 */

const WEB_ROOT = fileURLToPath(new URL("../..", import.meta.url));
const OUT_DIR = "dist-scan";
const OUT_PATH = join(WEB_ROOT, OUT_DIR);

/**
 * Not `dist/`: `pnpm build` writes there and the release lane embeds it, so a test that emptied it
 * could leave a developer's working bundle gone or half-written.
 */
function build(): void {
  execFileSync(
    join(WEB_ROOT, "node_modules", ".bin", "vite"),
    ["build", "--outDir", OUT_DIR, "--emptyOutDir", "--logLevel", "error"],
    { cwd: WEB_ROOT, stdio: "pipe" },
  );
}

function emittedAssets(): string[] {
  const walk = (dir: string): string[] =>
    readdirSync(dir).flatMap((entry) => {
      const path = join(dir, entry);
      if (statSync(path).isDirectory()) return walk(path);
      return /\.(js|css|html)$/.test(entry) ? [path] : [];
    });
  return walk(OUT_PATH);
}

const ABSOLUTE = String.raw`https?:\/\/[^"'\`)\s]+`;

/**
 * Every construct that would make the browser fetch from the URL inside it.
 *
 * Each is a *load*, not a mention. A URL in a thrown message, a namespace identifier passed to
 * `createElementNS`, or a JSON-Schema `$id` matches none of them, and correctly so.
 */
const SUBRESOURCE_FORMS: readonly { what: string; pattern: RegExp }[] = [
  { what: "static import", pattern: new RegExp(String.raw`\bfrom\s*["'\`]${ABSOLUTE}`, "g") },
  { what: "dynamic import", pattern: new RegExp(String.raw`\bimport\s*\(\s*["'\`]${ABSOLUTE}`, "g") },
  { what: "worker", pattern: new RegExp(String.raw`new\s+(?:Shared)?Worker\s*\(\s*["'\`]${ABSOLUTE}`, "g") },
  { what: "importScripts", pattern: new RegExp(String.raw`importScripts\s*\(\s*["'\`]${ABSOLUTE}`, "g") },
  { what: "fetch", pattern: new RegExp(String.raw`\bfetch\s*\(\s*["'\`]${ABSOLUTE}`, "g") },
  { what: "element src/href", pattern: new RegExp(String.raw`\b(?:src|href)\s*=\s*["'\`]${ABSOLUTE}`, "g") },
  { what: "css url()", pattern: new RegExp(String.raw`url\(\s*["'\`]?${ABSOLUTE}`, "g") },
  { what: "css @import", pattern: new RegExp(String.raw`@import[^;]*${ABSOLUTE}`, "g") },
];

/**
 * The hosts a JavaScript package is fetched from.
 *
 * A second, blunter net under the structural scan: monaco's loader building its URL by
 * concatenation, or a dependency inlining one in a form not listed above, would slip past every
 * pattern there and be caught here. A CDN hostname has no business in an air-gapped bundle in any
 * syntactic position.
 */
const PACKAGE_CDNS = [
  "cdn.jsdelivr.net",
  "unpkg.com",
  "cdnjs.cloudflare.com",
  "esm.sh",
  "cdn.skypack.dev",
  "ga.jspm.io",
  "fonts.googleapis.com",
  "fonts.gstatic.com",
  "ajax.googleapis.com",
];

describe("the built console loads nothing from outside itself", () => {
  beforeAll(() => {
    build();
  }, 600_000);

  afterAll(() => {
    if (existsSync(OUT_PATH)) rmSync(OUT_PATH, { recursive: true, force: true });
  });

  it("emits no asset that loads a subresource from an absolute URL", () => {
    const assets = emittedAssets();
    // A build that emitted nothing would make every assertion here vacuously true.
    expect(assets.length).toBeGreaterThan(0);

    const offenders = assets.flatMap((path) => {
      const source = readFileSync(path, "utf8");
      return SUBRESOURCE_FORMS.flatMap(({ what, pattern }) => {
        const hits = [...source.matchAll(pattern)].map((hit) => hit[0]);
        return hits.length === 0 ? [] : [`${path.slice(WEB_ROOT.length)} (${what}): ${hits.join(", ")}`];
      });
    });
    expect(offenders).toEqual([]);
  });

  it("names no package CDN anywhere in the shipped bytes", () => {
    const offenders = emittedAssets().flatMap((path) => {
      const source = readFileSync(path, "utf8");
      const found = PACKAGE_CDNS.filter((host) => source.includes(host));
      return found.length === 0 ? [] : [`${path.slice(WEB_ROOT.length)}: ${found.join(", ")}`];
    });
    expect(offenders).toEqual([]);
  });

  it("ships monaco from the bundle rather than from a CDN", () => {
    /*
     * Guards the guards. Both scans above pass trivially for a console that does not include monaco
     * at all — which is exactly what a mis-wired dynamic import produces: the editor falls back to
     * the textarea forever and nobody notices until an operator asks where it went.
     */
    const chunks = emittedAssets().filter((path) => path.endsWith(".js"));
    const monacoChunks = chunks.filter((path) =>
      readFileSync(path, "utf8").includes("monaco-editor"),
    );
    expect(monacoChunks.length).toBeGreaterThan(0);
    // And its JSON language worker, which is the part a CDN loader would have supplied.
    expect(chunks.some((path) => /json\.worker-[\w-]+\.js$/.test(path))).toBe(true);
  });
});
