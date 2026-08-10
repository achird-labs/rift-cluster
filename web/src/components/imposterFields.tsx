import type { ReactNode } from "react";

import type { components } from "../api/schema.ts";
import type { ImposterColumn } from "../app/contract.ts";
import { Ident, Status, UNKNOWN } from "./primitives.tsx";

type Imposter = components["schemas"]["Imposter"];

/**
 * Refuses to compile when a `switch` gains an unhandled case.
 *
 * Needed because a render function returns `ReactNode`, which includes `undefined` — so TypeScript
 * accepts a `switch` that falls off the end and the missing column silently renders as nothing.
 * Narrowing to `never` is what actually makes "add a column here and you must give it a rendering"
 * true rather than merely intended.
 */
export function assertNever(value: never): never {
  throw new Error(`unhandled case: ${JSON.stringify(value)}`);
}

/**
 * How many stubs this imposter has, or `null` when the response did not say.
 *
 * Exported so the contract test can assert both projections resolve, and because "which field
 * carries the count" is exactly the kind of detail a second caller would get subtly wrong.
 */
export function stubCountOf(imposter: Imposter): number | null {
  if (imposter.stubs !== undefined) return imposter.stubs.length;
  return imposter.stubCount ?? null;
}

/**
 * One imposter field, rendered the same way wherever it appears.
 *
 * Shared by the list and the detail screen deliberately: two copies would let the same value read
 * as `0` in one place and `—` in the other, and it is exactly those absent-vs-zero distinctions
 * that this console is supposed to keep straight.
 *
 * `name` takes a render prop because the list links it to the detail screen and the detail screen
 * has nowhere to link to — the only difference between the two, so it is the only thing passed in.
 *
 * The prop receives `string | undefined` rather than `string`, and is consulted **before** the
 * absent-name placeholder. That ordering is the whole point: `name` is optional on the wire, and
 * this cell is the list's only route to the detail screen. Rendering `—` here without asking the
 * caller first is what stranded every nameless imposter — no stub editing, no recording panel, no
 * export, and a row that silently ignored clicks. A caller with nowhere to link still passes no
 * prop and still gets the placeholder.
 */
export function ImposterField({
  imposter,
  field,
  renderName,
}: {
  imposter: Imposter;
  field: ImposterColumn["key"];
  renderName?: (name: string | undefined) => ReactNode;
}): ReactNode {
  switch (field) {
    case "port":
      return <Ident>{imposter.port ?? UNKNOWN}</Ident>;
    case "host":
      return <Ident>{imposter.host ?? UNKNOWN}</Ident>;
    case "protocol":
      return imposter.protocol ?? UNKNOWN;
    case "name":
      if (renderName !== undefined) return renderName(imposter.name);
      return imposter.name === undefined ? <span className="muted">{UNKNOWN}</span> : imposter.name;
    /*
     * Both keys, one rendering. `ImposterField` is exhaustive over every field the schema declares
     * (that is what `assertNever` below enforces), so documenting `stubCount` in the contract makes
     * it a case this switch must answer even though `IMPOSTER_COLUMNS` names only `stubs`. Giving
     * it the same answer is the honest one: they are two encodings of a single fact, and a column
     * declared against either should show the same number.
     */
    case "stubCount":
    case "stubs":
      /*
       * Two fields carry this, and which one arrives depends on the response: a single-imposter
       * read sends `stubs`, the LIST projection omits the array and sends `stubCount`. Reading
       * only `stubs` therefore rendered `—` for every row on the list screen — the one screen the
       * column exists for — while the count sat unread in the same payload.
       *
       * `stubs.length` first, because when the array is present it is the thing itself rather than
       * a number about it. Absent both is still `—`, and that part is unchanged: "this response
       * did not include them" is not the same fact as an imposter with zero stubs.
       */
      return <Ident>{stubCountOf(imposter) ?? UNKNOWN}</Ident>;
    case "recordRequests":
      return imposter.recordRequests ? (
        <Status tone="ok" label="recording" />
      ) : (
        <Status tone="idle" label="off" />
      );
    case "enabled":
      return imposter.enabled ? (
        <Status tone="ok" label="enabled" />
      ) : (
        <Status tone="idle" label="disabled" />
      );
    /*
     * Issue #363. A **fleet** figure: the front rewrites each entry's count to the sum across every
     * node's slot for that port, so this is not what the answering node served on its own.
     *
     * `—` when the field is absent, never `0`. "This response did not carry a count" and "nothing
     * has ever hit this imposter" are the two facts an operator opens this screen to distinguish,
     * and a zero would answer the second when only the first is known — the same reasoning the
     * stub count above records.
     */
    case "numberOfRequests":
      return <Ident>{imposter.numberOfRequests ?? UNKNOWN}</Ident>;
    default:
      return assertNever(field);
  }
}
