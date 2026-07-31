import type { ReactNode } from "react";

import { ApiError } from "../api/client.ts";

/** Unknown is not zero. Every place a value may legitimately be absent renders this instead of 0. */
export const UNKNOWN = "—";

/**
 * A long value shown at a width a dense table can hold, with the whole value on the title.
 *
 * 34 characters is the prototype's measured cut: it clears a 40-character imposter name's
 * distinguishing head without letting one row set the column width for the other 199.
 */
export function Truncated({
  value,
  testId,
  max = 34,
}: {
  value: string;
  testId?: string;
  max?: number;
}): ReactNode {
  const clipped = value.length > max ? `${value.slice(0, max - 1)}…` : value;
  return (
    <span className="truncated" title={value} data-testid={testId}>
      {clipped}
    </span>
  );
}

/**
 * Status, triple-encoded: glyph shape, colour and word.
 *
 * Not decoration. Green↔red measures ΔE 5.8–7.2 apart under protanopia and deuteranopia, which no
 * hue tweak fixes inside a green/amber/red convention — so the shape and the word carry the
 * meaning and the colour only reinforces it.
 */
export type Tone = "ok" | "warn" | "bad" | "idle";

const GLYPH: Record<Tone, string> = { ok: "●", warn: "▲", bad: "■", idle: "○" };

export function Status({ tone, label }: { tone: Tone; label: string }): ReactNode {
  return (
    <span className={`status status-${tone}`}>
      <span aria-hidden="true">{GLYPH[tone]}</span> {label}
    </span>
  );
}

/** A monospace, tabular-figures identifier — a value an operator pastes into curl. */
export function Ident({ children }: { children: ReactNode }): ReactNode {
  return <span className="ident">{children}</span>;
}

/**
 * An error an operator can act on.
 *
 * `ApiError` carries the status, and the status is the diagnosis: 401 means the session lapsed,
 * 403 means the role refused it, 404 on a fleet-scoped route means the principal lacks the scope
 * (RFC-002 §8.4 renders that as "no such route", which must not become "no such cluster" on screen).
 */
export function ErrorNote({ error, context }: { error: unknown; context?: string }): ReactNode {
  return (
    <p className="error" role="alert">
      {context === undefined ? null : `${context}: `}
      {describe(error)}
    </p>
  );
}

/**
 * A write the fleet accepted but this session could not watch land.
 *
 * Deliberately `role="status"` and not `role="alert"`: nothing is wrong. The write was durably
 * parked and is committing; following it to completion needs fleet-admin scope, and `403`/`404` from
 * the op-status projection is what most principals get whatever the write did. Saying "saved" here
 * would be the bug this note exists to replace, and saying "failed" would be the same overclaim
 * pointing the other way.
 */
export function UnconfirmedNote({ reason }: { reason: string }): ReactNode {
  return (
    <p className="degraded" role="status" data-testid="write-unconfirmed">
      <strong>Accepted, not yet confirmed.</strong> {reason}. Re-read this screen in a moment to see
      where it landed.
    </p>
  );
}

export function describe(error: unknown): string {
  if (error instanceof ApiError) {
    switch (error.status) {
      case 401:
        return "401 — the session is not valid. Sign in again.";
      case 403:
        return "403 — this principal's role does not permit that.";
      case 503:
        return "503 — this node is not ready to answer.";
      default:
        return error.message;
    }
  }
  return error instanceof Error ? error.message : String(error);
}
