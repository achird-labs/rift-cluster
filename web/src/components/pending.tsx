import type { ReactNode } from "react";

import { ISSUE_URL } from "../app/pending.ts";

/**
 * The value of a panel the design draws and the fleet does not answer yet.
 *
 * An em dash and the issue number, borrowing the device the nav already uses for a screen that is
 * designed but unbuilt — "a visible roadmap, not a 404" (RFC-006 §4), applied one level down to a
 * field instead of a screen. The dash keeps the panel's shape exactly as the design draws it; the
 * chip answers the question the dash provokes.
 *
 * It is deliberately not a plausible figure. This console's contract is that what it shows is true
 * of a live fleet, and a fabricated "PARKED INTENTS · 3" is worse than an empty one because an
 * operator would act on it. It is equally not a bare blank: a blank says "zero, or loading, or
 * broken" and the reader cannot tell which.
 *
 * `reason` is required and lands on the hover, so nobody can add one of these without saying what
 * is missing — the note is how the next reader learns whether the gap is unbuilt, not schema'd, or
 * unreachable by construction.
 */
export function Pending({ issue, reason }: { issue: number; reason: string }): ReactNode {
  return (
    <span className="pending" title={reason}>
      <span aria-hidden="true">—</span>
      <a
        className="issue"
        href={ISSUE_URL(issue)}
        target="_blank"
        rel="noreferrer"
        title={`${reason} — tracked as issue #${String(issue)}`}
      >
        <span className="visually-hidden">Not published yet — tracked as issue </span>#{issue}
      </a>
    </span>
  );
}

/**
 * A whole panel whose data source does not exist.
 *
 * The panel keeps its heading and its box so the screen's composition is the design's; this fills
 * the body where rows would go. One sentence, because at panel scale there is room to say what is
 * missing and a bare dash would leave the reader deciding between "quiet fleet" and "blind console".
 */
export function PendingPanel({ issue, reason }: { issue: number; reason: string }): ReactNode {
  return (
    <div className="pending-panel">
      <p>{reason}</p>
      <a className="issue" href={ISSUE_URL(issue)} target="_blank" rel="noreferrer">
        #{issue}
      </a>
    </div>
  );
}
