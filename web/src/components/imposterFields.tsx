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
 * One imposter field, rendered the same way wherever it appears.
 *
 * Shared by the list and the detail screen deliberately: two copies would let the same value read
 * as `0` in one place and `—` in the other, and it is exactly those absent-vs-zero distinctions
 * that this console is supposed to keep straight.
 *
 * `name` takes a render prop because the list links it to the detail screen and the detail screen
 * has nowhere to link to — the only difference between the two, so it is the only thing passed in.
 */
export function ImposterField({
  imposter,
  field,
  renderName,
}: {
  imposter: Imposter;
  field: ImposterColumn["key"];
  renderName?: (name: string) => ReactNode;
}): ReactNode {
  switch (field) {
    case "port":
      return <Ident>{imposter.port ?? UNKNOWN}</Ident>;
    case "host":
      return <Ident>{imposter.host ?? UNKNOWN}</Ident>;
    case "protocol":
      return imposter.protocol ?? UNKNOWN;
    case "name":
      if (imposter.name === undefined) return <span className="muted">{UNKNOWN}</span>;
      return renderName === undefined ? imposter.name : renderName(imposter.name);
    case "stubs":
      // Absent `stubs` is "this response did not include them", which is not the same fact as an
      // imposter with zero stubs — so it renders as unknown rather than 0.
      return <Ident>{imposter.stubs === undefined ? UNKNOWN : imposter.stubs.length}</Ident>;
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
    default:
      return assertNever(field);
  }
}
