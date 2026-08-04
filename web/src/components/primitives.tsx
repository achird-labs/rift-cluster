import type { ReactNode } from "react";

import { ApiError } from "../api/client.ts";

/** Unknown is not zero. Every place a value may legitimately be absent renders this instead of 0. */
export const UNKNOWN = "—";

/**
 * An imposter that carries no name, which is a legal thing to be — `name` is optional on
 * `POST /imposters`.
 *
 * Distinct from [`UNKNOWN`] on purpose: `—` says "this response did not tell us", while this says
 * "the imposter genuinely has no name". The list needs the difference because it renders the cell
 * as the link to the detail screen, and a link labelled `—` is neither clickable-looking nor
 * announceable.
 */
export const UNNAMED = "(unnamed)";

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
      <span className="g" aria-hidden="true">
        {GLYPH[tone]}
      </span>
      {label}
    </span>
  );
}

/**
 * A titled panel. Every table and every grouped read on a screen sits in one, which is what makes
 * the page read as sections rather than as one long scroll of rules.
 *
 * `bleed` drops the body padding for the case the design intends: a table meets the card edge, so
 * its own cell padding is the only inset and the header row's background reaches the border.
 */
export function Card({
  title,
  actions,
  bleed = false,
  testId,
  children,
}: {
  title?: ReactNode;
  actions?: ReactNode;
  bleed?: boolean;
  testId?: string;
  children: ReactNode;
}): ReactNode {
  return (
    <section className="card" data-testid={testId}>
      {title === undefined && actions === undefined ? null : (
        <div className="card-head">
          {typeof title === "string" ? <h2>{title}</h2> : title}
          {actions === undefined ? null : <div className="spacer" />}
          {actions}
        </div>
      )}
      {bleed ? children : <div className="card-body">{children}</div>}
    </section>
  );
}

/**
 * One measured value.
 *
 * `tone` draws the severity stripe down the left edge — state encoded in form, so the tile does not
 * rely on its number's colour alone. `note` is where the caveat goes, and several of these screens
 * have one that matters more than the number does.
 */
export function Tile({
  label,
  value,
  unit,
  note,
  tone,
  plain = false,
  testId,
}: {
  label: string;
  value: ReactNode;
  unit?: string;
  note?: ReactNode;
  tone?: "good" | "warn" | "crit";
  /**
   * Render the value at body size rather than as a 25px figure.
   *
   * For a tile whose value is a status pill or a list of node ids rather than one number — a pill
   * set in a 25px line looks like a rendering fault, and a comma-separated voter list at that size
   * simply overflows.
   */
  plain?: boolean;
  testId?: string;
}): ReactNode {
  return (
    <div className={tone === undefined ? "tile" : `tile is-${tone}`} data-testid={testId}>
      <div className="eyebrow">{label}</div>
      <div className={plain ? "v-plain" : "v"}>
        {value}
        {unit === undefined ? null : <small> {unit}</small>}
      </div>
      {note === undefined ? null : <div className="note">{note}</div>}
    </div>
  );
}

/**
 * Nothing to show — and *why* there is nothing, which is the half that matters.
 *
 * "No imposters" and "cannot confirm this tenant is empty" are different facts, and the screens
 * that can tell them apart pass different `body` text here rather than sharing one message.
 */
export function Empty({
  mark = "○",
  title,
  body,
  children,
  testId,
}: {
  mark?: string;
  title: string;
  body?: ReactNode;
  children?: ReactNode;
  testId?: string;
}): ReactNode {
  return (
    <div className="empty" data-testid={testId}>
      <div className="mark" aria-hidden="true">
        {mark}
      </div>
      <h3>{title}</h3>
      {body === undefined ? null : <p>{body}</p>}
      {children}
    </div>
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

/**
 * A modal confirmation for a destructive act.
 *
 * Deleting an imposter takes its stubs, its recorded requests and its flow state with it, and
 * nothing in the fleet undoes that — so this exists to make the operator name the thing before it
 * goes, not to add ceremony. `confirmLabel` states the act ("Delete checkout-api") rather than
 * "OK", because the label is the last thing read before the click.
 *
 * `role="dialog"` + `aria-modal` and an Escape handler; the surrounding scrim is not clickable to
 * dismiss, deliberately — a stray click beside a destructive dialog should do nothing at all.
 */
export function Confirm({
  title,
  body,
  confirmLabel,
  busy = false,
  onConfirm,
  onCancel,
  testId,
}: {
  title: string;
  body: ReactNode;
  confirmLabel: string;
  busy?: boolean;
  onConfirm: () => void;
  onCancel: () => void;
  testId?: string;
}): ReactNode {
  return (
    <div
      className="scrim"
      onKeyDown={(event) => {
        if (event.key === "Escape") onCancel();
      }}
    >
      <div className="confirm" role="dialog" aria-modal="true" aria-label={title} data-testid={testId}>
        <h2>{title}</h2>
        <p>{body}</p>
        <div className="acts">
          <button className="btn" type="button" onClick={onCancel} disabled={busy}>
            Cancel
          </button>
          <button
            className="btn danger"
            type="button"
            onClick={onConfirm}
            disabled={busy}
            data-testid="confirm-destructive"
            autoFocus
          >
            {busy ? "Working…" : confirmLabel}
          </button>
        </div>
      </div>
    </div>
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
