import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = new URL("..", import.meta.url).pathname;
const STYLES = readFileSync(join(SRC, "styles.css"), "utf8");

/**
 * The `Confirm` primitive's three class names must actually be styled (#236).
 *
 * **What this test proves, and what it does not.** It proves the rules are *declared* — nothing
 * more. It cannot prove they have the intended effect, because jsdom parses CSS but never computes
 * the cascade or paints; that is the whole reason the browser layer exists, and
 * `e2e/smoke.spec.ts` asserts the geometry a real engine produces.
 *
 * It is still worth having, because the bug it guards against was not a rule that stopped working —
 * it was three rules that **vanished entirely** when #230 replaced the stylesheet with the design
 * prototype's system. `Confirm` went on rendering `.scrim`/`.confirm`/`.acts` against a sheet that
 * styled none of them, so the modal guarding every destructive act in the console became an
 * ordinary block in the page flow. Nothing failed: no test named these selectors, and the committed
 * visual baseline had captured the unstyled rendering as if it were correct.
 *
 * This runs in the fast suite that gates every PR, so a future rewrite that drops them again fails
 * in seconds rather than surviving into a baseline.
 */

/**
 * The sheet with comments blanked out.
 *
 * Every scan below runs against this rather than the raw text, because a declaration that has been
 * *commented out* must read as absent. Matching it would keep this file green through exactly the
 * edit it exists to catch — the same reasoning `contract-traceability.test.ts` gives for its own
 * comment-stripping scan.
 */
const CODE = STYLES.replace(/\/\*[\s\S]*?\*\//g, "");

/** One rule's declaration block, by exact selector — `null` when the sheet declares no such rule. */
function ruleBody(selector: string): string | null {
  // Anchored at a line start so `.confirm` cannot match inside `.confirm-something`, and the
  // selector must be followed by `{` or `,` so it matches a rule head rather than a mention
  // in prose.
  const pattern = new RegExp(`^\\${selector}\\s*[,{][^}]*}`, "m");
  const match = pattern.exec(CODE);
  return match === null ? null : match[0];
}

function declares(selector: string, property: string): boolean {
  const body = ruleBody(selector);
  return body !== null && new RegExp(`(^|[;{\\s])${property}\\s*:`, "m").test(body);
}

describe("the destructive-confirm modal is styled at all (#236)", () => {
  it("declares a rule for each of the three class names Confirm renders", () => {
    // `primitives.tsx::Confirm` renders exactly these, and every destructive act in the console
    // goes through it — delete imposter, clear request log, reset scenarios, tear down space,
    // clear flow state.
    for (const selector of [".scrim", ".confirm", ".confirm .acts"]) {
      expect([selector, ruleBody(selector) !== null]).toEqual([selector, true]);
    }
  });

  it("scopes the action row to the dialog, because `.acts` has another user", () => {
    /*
     * `.acts` is also the flow-state action area on the scenarios screen, where it wraps a textarea
     * form beside a danger button. A bare `.acts { display: flex; justify-content: flex-end }`
     * collapses that textarea to its intrinsic width and shoves it against the button — a screen
     * this change has no business touching. The prototype scopes it (`console-prototype.html:453`)
     * and so must this.
     */
    expect(ruleBody(".acts")).toBeNull();
    expect(ruleBody(".confirm .acts")).not.toBeNull();
  });

  it("gives the scrim the properties that make it an overlay rather than a block in the flow", () => {
    /*
     * `position: fixed` is the load-bearing one: without it the dialog scrolls with the page and
     * sits wherever the flow put it. `inset`/`z-index` are what make it cover the page rather than
     * appear beside it, and the flex centring is what puts the dialog in the middle.
     */
    for (const property of ["position", "inset", "z-index", "display"]) {
      expect([property, declares(".scrim", property)]).toEqual([property, true]);
    }
    expect(ruleBody(".scrim")).toMatch(/position:\s*fixed/);
  });

  it("gives the dialog a surface of its own", () => {
    // Without a background the dialog is transparent over the scrim, so the page shows through the
    // text of a confirmation someone is about to act on.
    for (const property of ["background", "border", "border-radius", "padding"]) {
      expect([property, declares(".confirm", property)]).toEqual([property, true]);
    }
  });

  it("lays the action row out as a row", () => {
    expect(declares(".confirm .acts", "display")).toBe(true);
    expect(ruleBody(".confirm .acts")).toMatch(/justify-content/);
  });

  it("builds the backdrop from a token rather than a literal", () => {
    /*
     * The one value that dims the whole page is named where the rest of the palette is. This used
     * to be about surviving a theme swap — a hardcoded `rgba(0,0,0,.5)` was legible on paper and
     * near-invisible over the old dark theme's ground — and the console ships one palette now, so
     * what is left is the plainer reason: a literal buried in a single rule is the value nobody
     * finds when the palette is next retuned.
     */
    expect(ruleBody(".scrim")).toMatch(/var\(--scrim\)/);
    expect(/:root\s*{[^}]*--scrim\s*:/.test(CODE)).toBe(true);
  });

  it("ships exactly one theme", () => {
    /*
     * Asserted rather than remembered.
     *
     * The console carried a dark theme nobody had designed against, so an operator on a dark
     * desktop saw a palette no mockup had been reviewed in — and every judgement about contrast,
     * about the ink-filled primary, about one cool accent among warm neutrals, was being made about
     * a set of colours half the readers never saw. Dropping it was a decision; a
     * `prefers-color-scheme` block reappearing would silently undo it, and nothing else in the
     * suite would notice.
     */
    expect(CODE).not.toMatch(/@media[^{]*prefers-color-scheme/);
    expect(ruleBody(":root")).toMatch(/color-scheme:\s*light\s*;/);
  });
});
