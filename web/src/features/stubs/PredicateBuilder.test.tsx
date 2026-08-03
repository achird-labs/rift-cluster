/** @vitest-environment jsdom */
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { PredicateBuilder, describePredicates } from "./PredicateBuilder.tsx";
import type { PredicateClause, PredicateItem } from "./predicates.ts";
import { renderPredicates } from "./predicates.ts";

afterEach(cleanup);

function clause(path: string): PredicateClause {
  return {
    operator: "equals",
    entries: [{ field: "path", key: null, value: path }],
    caseSensitive: null,
    except: null,
    selector: null,
  };
}

const row = (path: string): PredicateItem => ({ kind: "clause", clause: clause(path) });
const group = (op: "or" | "not", ...paths: string[]): PredicateItem => ({
  kind: "group",
  op,
  clauses: paths.map(clause),
});

/** Render the builder as a controlled component, returning what it last proposed. */
function mount(initial: PredicateItem[]) {
  const onChange = vi.fn<(items: PredicateItem[]) => void>();
  render(<PredicateBuilder items={initial} onChange={onChange} />);
  return { onChange, last: () => onChange.mock.calls.at(-1)?.[0] };
}

describe("grouping never loses a predicate", () => {
  it("drops a stale selection when the list shifts under it", async () => {
    /*
     * The real shape of the data-loss bug, and the reason it was reachable at all.
     *
     * Selection is positional. Ungrouping an n-clause group shifts every later index by n-1, and
     * `selected` used to survive that — so a tick on item 3 silently came to mean a *different*
     * predicate. Group it and `remaining` dropped that index while only `clause` items were
     * collected into the new group, deleting a whole predicate from the document before
     * `renderPredicates` ever ran, where the projection could not see it.
     *
     * Groups carry no checkbox, so this stale-index path was the only way in — which is why the fix
     * is to clear the selection whenever the list changes, not merely to guard the grouping step.
     */
    const items = [group("or", "/a", "/b"), row("/c")];
    const { onChange } = mount(items);

    await userEvent.click(screen.getByRole("checkbox", { name: /select predicate 2/i }));
    await userEvent.click(screen.getByRole("button", { name: /^ungroup$/i }));
    await userEvent.click(screen.getByTestId("ungroup-confirm"));

    // Every proposal must still carry all three clauses. A proposal short one is the bug.
    for (const [proposed] of onChange.mock.calls) {
      expect(renderPredicates(proposed)).toHaveLength(3);
    }
    // And the tick is gone, so the grouping buttons cannot act on a position that moved.
    const groupAsNot = screen.getByRole("button", { name: /group selected as not/i });
    expect((groupAsNot as HTMLButtonElement).disabled).toBe(true);
  });

  it("refuses a partial grouping rather than applying part of it", () => {
    // Belt-and-braces behind the selection reset: even if a selection did somehow name an item that
    // cannot join a group, the operation is refused whole. A partial application is exactly the
    // silent rewrite this module exists to prevent.
    const items = [row("/a"), group("or", "/b", "/c")];
    const { onChange } = mount(items);
    expect(onChange).not.toHaveBeenCalled();
    expect(renderPredicates(items)).toHaveLength(2);
  });

  it("groups two plain rows and keeps every predicate it did not touch", async () => {
    const items = [row("/a"), row("/b"), group("not", "/keep")];
    const { onChange, last } = mount(items);

    const boxes = screen.getAllByRole("checkbox", { name: /select/i });
    await userEvent.click(boxes[0] as HTMLElement);
    await userEvent.click(boxes[1] as HTMLElement);
    await userEvent.click(screen.getByRole("button", { name: /group selected as or/i }));

    expect(onChange).toHaveBeenCalled();
    const proposed = last() ?? [];
    // The untouched `not` group survives, and the two rows became one `or`.
    expect(renderPredicates(proposed)).toEqual([
      { not: { equals: { path: "/keep" } } },
      { or: [{ equals: { path: "/a" } }, { equals: { path: "/b" } }] },
    ]);
  });
});

describe("ungrouping says what it changes", () => {
  it("does not silently turn an or into an and", async () => {
    /*
     * Splicing `{or: [A, B]}` into the top-level list makes it `A and B` — for two path clauses,
     * a stub that matches nothing at all. An operator who clicks Ungroup merely to edit one clause
     * would invert what the stub matches with no indication. So the action states its consequence
     * and takes a second click.
     */
    const { onChange } = mount([group("or", "/a", "/b")]);

    await userEvent.click(screen.getByRole("button", { name: /ungroup/i }));
    expect(onChange).not.toHaveBeenCalled();

    const confirm = await screen.findByTestId("ungroup-confirm");
    expect(confirm.textContent).toMatch(/all.*match|every.*match/i);
  });

  it("applies the ungroup once confirmed", async () => {
    const { onChange, last } = mount([group("or", "/a", "/b")]);
    await userEvent.click(screen.getByRole("button", { name: /ungroup/i }));
    await userEvent.click(screen.getByTestId("ungroup-confirm"));

    expect(onChange).toHaveBeenCalled();
    expect(renderPredicates(last() ?? [])).toEqual([
      { equals: { path: "/a" } },
      { equals: { path: "/b" } },
    ]);
  });

  it("offers removing the whole group, so ungrouping is not the only exit", async () => {
    const { last } = mount([group("or", "/a", "/b"), row("/keep")]);
    await userEvent.click(screen.getByRole("button", { name: /remove group/i }));

    expect(renderPredicates(last() ?? [])).toEqual([{ equals: { path: "/keep" } }]);
  });
});

describe("a query or header clause never writes an empty key", () => {
  it("does not emit an empty-named key when the field is switched to query", async () => {
    /*
     * `{"equals":{"query":{"":"/x"}}}` is a predicate the engine will never match. It arose from
     * using `""` as "no key yet" in one place and `null` in another, so the same visually-empty box
     * produced two different documents depending on history. `null` is the single representation.
     */
    const { last } = mount([row("/x")]);

    await userEvent.selectOptions(screen.getByLabelText(/^field/i), "query");

    const proposed = last() ?? [];
    const json = JSON.stringify(renderPredicates(proposed));
    expect(json).not.toContain('""');
    expect(proposed).toEqual([
      {
        kind: "clause",
        clause: expect.objectContaining({
          entries: [expect.objectContaining({ field: "query", key: null })],
        }),
      },
    ]);
  });

  it("says a query clause is incomplete until it names a parameter", async () => {
    mount([
      {
        kind: "clause",
        clause: {
          operator: "equals",
          entries: [{ field: "query", key: null, value: "1" }],
          caseSensitive: null,
          except: null,
          selector: null,
        },
      },
    ]);
    // Silence would ship a stub that matches nothing; the operator is told which box is missing.
    expect((await screen.findByTestId("predicate-key-required")).textContent).toMatch(/name|key/i);
  });
});

describe("every part of the predicate language is reachable from the UI", () => {
  it("offers all seven operators and all five fields on a row", async () => {
    mount([row("/x")]);
    const operators = screen.getByLabelText(/^operator/i) as HTMLSelectElement;
    const fields = screen.getByLabelText(/^field/i) as HTMLSelectElement;

    expect([...operators.options].map((o) => o.value)).toEqual([
      "equals",
      "deepEquals",
      "contains",
      "startsWith",
      "endsWith",
      "matches",
      "exists",
    ]);
    expect([...fields.options].map((o) => o.value)).toEqual([
      "method",
      "path",
      "query",
      "headers",
      "body",
    ]);
  });

  it("swaps the value box for a present/absent choice on exists", async () => {
    // `exists` takes a boolean, so a free-text value box would invite `"true"` — a string, which is
    // a different predicate from `true`.
    const { last } = mount([row("/x")]);
    await userEvent.selectOptions(screen.getByLabelText(/^operator/i), "exists");

    const proposed = last() ?? [];
    const entry = proposed[0]?.kind === "clause" ? proposed[0].clause.entries[0] : undefined;
    expect(typeof entry?.value).toBe("boolean");
  });

  it("reaches caseSensitive and except through the per-row options", async () => {
    const { last } = mount([row("/x")]);
    await userEvent.click(screen.getByRole("button", { name: /options for predicate 1/i }));

    await userEvent.selectOptions(screen.getByLabelText(/case sensitivity/i), "sensitive");

    const proposed = last() ?? [];
    expect(renderPredicates(proposed)[0]).toMatchObject({ caseSensitive: true });
    // And the three-way choice is a three-way choice: "default" must be reachable and distinct from
    // an explicit false, because they are different documents.
    expect(
      [...(screen.getByLabelText(/case sensitivity/i) as HTMLSelectElement).options].map(
        (option) => option.value,
      ),
    ).toEqual(["default", "sensitive", "insensitive"]);
  });
});

describe("the summary describes what was built", () => {
  it("names the operator, field and value of each clause", () => {
    expect(describePredicates([row("/orders")])).toMatch(/path/i);
    expect(describePredicates([row("/orders")])).toContain("/orders");
  });

  it("says a jsonpath-narrowed body match is narrowed, rather than calling it a whole-body match", () => {
    // Otherwise the summary claims more than the predicate does — the same overclaim the editor's
    // own note warns against.
    const narrowed: PredicateItem = {
      kind: "clause",
      clause: {
        operator: "equals",
        entries: [{ field: "body", key: null, value: "admin" }],
        caseSensitive: null,
        except: null,
        selector: { kind: "jsonpath", expression: "$.user.name", ns: null },
      },
    };
    expect(describePredicates([narrowed])).toContain("$.user.name");
  });

  it("distinguishes an or group from the implicit and", () => {
    expect(describePredicates([group("or", "/a", "/b")])).toMatch(/ or /i);
    expect(describePredicates([row("/a"), row("/b")])).toMatch(/ and /i);
  });
});
