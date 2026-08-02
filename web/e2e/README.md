# `e2e/` — browser tests against the shipped console

These drive a real Chromium against the **binary serving `/console/`**, not the Vite dev server.
That is the point of them: `src/__tests__` already covers behaviour under jsdom, which parses CSS
but never computes the cascade or paints.

The bug that motivated this directory: `background-color` on an `input` suppresses a checkbox's
native checked indicator in Blink and makes `accent-color` a no-op, so **every** checkbox in the
console rendered as an empty square that never ticked. 375 green jsdom tests said nothing about it,
and it was found by a person clicking around. Running against the real artifact also puts the
shipped CSP, the real bundle and the bundled wasm linter under test rather than under assumption.

```sh
pnpm run e2e             # all three layers
pnpm run e2e:ui          # the Playwright UI, for debugging one spec
pnpm run e2e:update      # re-baseline the visual snapshots (read the diff first)
```

`playwright.config.ts` starts the fixture for you. To drive it by hand:

```sh
scripts/e2e-console.sh up      # one seeded node on :3525, keys in web/e2e/.fixture.json
scripts/e2e-console.sh down
```

## The three layers

| Spec | What it is for |
|---|---|
| `smoke.spec.ts` | Every screen loads as every role, with a clean browser console. |
| `visual.spec.ts` | Component screenshots, both themes, diffed against committed baselines. |
| `a11y.spec.ts` | axe over each screen; fails on `serious` and `critical` only. |

**Smoke** is where the console-error assertion lives, and it carries more weight than it looks:
`fixture.ts` fails any test that produced a browser console error or an uncaught rejection, so every
spec is also asserting no CSP violation, no failed chunk and no unhandled query rejection.

**Visual** is scoped to components, never pages. Full-page snapshots are why visual testing has a
reputation for flakiness — a row count, a timestamp or a poll landing mid-capture diffs the whole
image. Each baseline here is one component against fixture data that does not move, and anything
that legitimately advances (the applied index) is masked.

One test in that file commits no image at all: it screenshots a checkbox checked and unchecked and
asserts the bytes differ. A baseline can only catch a control that regressed *away* from a known
good state; that one catches a control that never rendered its state to begin with, which is what
actually happened.

**Accessibility** fails on `serious` and `critical` only. `moderate` and `minor` include advisory
rules (landmark structure, heading order) that would make this a style gate reviewers learn to skip
— the same reasoning behind the console's deliberately one-rule eslint config. It earns its place on
two things specifically: contrast, where the token set's "every status colour clears 4.5:1 in both
themes" was measured by hand once and is now re-measured against what the browser composites on
every run; and label association, which half a console of forms depends on.

## The fixture

`scripts/e2e-console.sh` starts **one** node, not three. Every console screen is per-node —
`/_fleet/*` is one node answering about itself — so one node exercises all eight screens and removes
the flakiness that leader election and voter convergence introduce. The cluster's own behaviour
belongs to `crates/rift-cluster/tests/cluster.rs` and the container chaos tier, which are built for
it. `--cluster-allow-solo` keeps it a real one-voter cluster, so the fleet screen renders its
single-node state rather than 404ing.

Everything it seeds is fixed — ports, tenants, imposters, stubs, the number of requests — because a
visual baseline diffed against a fixture that varies is a test that fails for reasons nobody changed.

`web/e2e/.fixture.json` holds the minted API keys and is **gitignored**, necessarily:
`createPrincipal` returns the raw key once and the fleet stores only an argon2id hash, so every
fixture run mints new ones. There is nothing stable to commit.

## Baselines

`e2e/**/-snapshots/` **is** committed — it is expected output, not a run artifact. Regenerate with
`pnpm run e2e:update`, and read the diff before you do: a baseline updated without looking is a
regression accepted in silence.

Baselines are platform-sensitive (font rasterisation differs between macOS and the CI container).
`maxDiffPixelRatio: 0.01` absorbs anti-aliasing but not a missing control. If local and CI disagree
on unchanged code, trust CI's — that is the platform the gate runs on.

A **new** baseline therefore cannot be produced on a macOS checkout at all: `pnpm run e2e:update`
writes only the `-darwin.png` half, and the gate runs on `-linux.png`. Add the `update-baselines`
label to the PR and `console-baselines.yml` generates the Linux image on CI and commits it back to
the branch. That is the only supported way to add one, and it is deliberately a labelled decision
rather than something a push does quietly.
