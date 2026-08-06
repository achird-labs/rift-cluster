import type { ReactNode } from "react";

/**
 * The marker for a value the design draws and the fleet does not publish.
 *
 * 2a uses `NOT SHIPPED YET` for two whole screens it has designed ahead of the build. This is the
 * same device doing a narrower job: the panel or column is built to the design, but no endpoint
 * backs the number that belongs in it.
 *
 * It is deliberately none of a dash, a zero, a spinner, or an empty cell. Every one of those reads
 * as a *measurement* — "the console asked the fleet, and this is the answer" — and this console's
 * entire contract is that what it shows is true of a live fleet (RFC-006 §3 rule 2, made mechanical
 * by `app/contract.ts`). A fabricated-looking zero next to `PARKED INTENTS` is worse than no panel
 * at all, because an operator would act on it.
 *
 * `title` carries the reason to a hover; `reason` renders it visibly on the panel form. Both are
 * required rather than optional, so nobody can add a marker without saying which endpoint is
 * missing — the note is how the next person knows whether the gap is "unbuilt", "not schema'd", or
 * "unreachable by construction".
 */
export function Unshipped({ reason }: { reason: string }): ReactNode {
  return (
    <span className="unshipped" title={reason}>
      <span className="visually-hidden">Not available: </span>
      no endpoint
    </span>
  );
}

/**
 * A whole panel the design specifies that has no data source behind it.
 *
 * Renders the marker plus the reason in full, because at panel scale there is room to say it and a
 * bare chip would leave the reader guessing whether the fleet is quiet or the console is blind.
 */
export function UnshippedPanel({ reason }: { reason: string }): ReactNode {
  return (
    <div className="unshipped-panel">
      <Unshipped reason={reason} />
      <p>{reason}</p>
    </div>
  );
}
