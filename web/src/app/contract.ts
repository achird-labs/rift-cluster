import type { components } from "../api/schema.ts";

type Imposter = components["schemas"]["Imposter"];
type FleetMembers = components["schemas"]["FleetMembers"];
type FleetHealth = components["schemas"]["FleetHealth"];
type SourceRecord = components["schemas"]["SourceRecord"];

/**
 * `T` with its index signature removed.
 *
 * `Imposter` and `Stub` end in `& { [key: string]: unknown }` because the contract marks them
 * non-exhaustive, which makes `keyof Imposter` degenerate to `string` — every field name would
 * typecheck, including one the contract has never heard of. Stripping the index signature restores
 * `keyof` to the declared properties, which is what RFC-006 §11's "traceable to a schema'd
 * endpoint" actually means.
 */
type Declared<T> = {
  [K in keyof T as string extends K ? never : number extends K ? never : K]: T[K];
};

export type ImposterField = keyof Declared<Imposter>;

export type ImposterColumn = {
  key: ImposterField;
  label: string;
  /** Right-aligned, so numerals an operator scans down a column line up. */
  numeric: boolean;
};

/**
 * The imposter table, declared once. The screen maps over this rather than hand-writing cells, so
 * `key` being `keyof Declared<Imposter>` is load-bearing: a column for a field the contract does
 * not publish fails `tsc`, and there is no second place to add one.
 *
 * `numberOfRequests` — the prototype's one chart — is deliberately absent. It reaches the body only
 * through the non-exhaustive index signature, so rendering it would be exactly the client-side
 * guess §11 forbids. Its home is the request log (#189), whose backend carries it as a schema'd
 * field.
 */
export const IMPOSTER_COLUMNS = [
  { key: "port", label: "Port", numeric: true },
  { key: "protocol", label: "Protocol", numeric: false },
  { key: "name", label: "Name", numeric: false },
  { key: "stubs", label: "Stubs", numeric: true },
  { key: "recordRequests", label: "Recording", numeric: false },
  { key: "enabled", label: "State", numeric: false },
  // `as const` keeps the keys as literals, which is what lets `ImposterField`'s `assertNever`
  // default make a column with no rendering a compile error rather than a silently blank cell.
] as const satisfies readonly ImposterColumn[];

export type SourceField = keyof Declared<SourceRecord>;

export type SourceColumn = {
  key: SourceField;
  label: string;
  /** Right-aligned, so the one genuinely numeric column (poll cadence) lines up under itself. */
  numeric: boolean;
};

/**
 * The sources table, declared once so a column for a field the contract does not publish fails
 * `tsc` rather than rendering silently blank — the same discipline `IMPOSTER_COLUMNS` holds.
 *
 * `drifted` and `lastOutcome` are deliberately absent: neither is shown as a plain cell value.
 * `Sources.tsx` reads them together to decide one of three drift states (clean / drifted /
 * never-pulled), which a per-field cell mapping cannot express — see the screen's own note.
 *
 * `authRef` is absent for a different reason: it is a credential *name*, and the screen has no
 * question it answers. It is listed here as a comment rather than omitted silently, so the next
 * person can see it was considered.
 */
export const SOURCE_COLUMNS = [
  { key: "id", label: "Source", numeric: false },
  { key: "uri", label: "URI", numeric: false },
  { key: "mode", label: "Mode", numeric: false },
  { key: "ports", label: "Ports", numeric: false },
  // Absent on a `pinned` source — the screen renders it only for `tracking` rows, where it is the
  // cadence an operator checks when asking "why has this not updated".
  { key: "pollSecs", label: "Poll interval", numeric: true },
  { key: "onDrift", label: "On drift", numeric: false },
  /*
   * Provenance: what this source last produced, and at which log index. The issue asks for it
   * explicitly ("what it produced, at which revision"), and without it the screen lists sources
   * without answering the question an operator actually arrives with.
   *
   * `lastDigest` is deliberately not a column — it is a 64-character hash with no operator-facing
   * question behind it, and it would cost the table's width for something `lastVersion` and
   * `revision` already answer more legibly.
   */
  { key: "lastVersion", label: "Last version", numeric: false },
  { key: "lastPulledAtSecs", label: "Last pulled", numeric: false },
  { key: "revision", label: "Revision", numeric: true },
] as const satisfies readonly SourceColumn[];

export type FleetField<T> = {
  key: keyof Declared<T>;
  label: string;
  testId: string;
};

/** Every `/_fleet/members` field, in the order the screen presents them. */
export const FLEET_MEMBER_FIELDS = [
  { key: "node_id", label: "This node", testId: "fleet-node" },
  { key: "current_leader", label: "Leader", testId: "fleet-leader" },
  { key: "last_applied", label: "Applied index", testId: "fleet-applied" },
  { key: "voters", label: "Voters", testId: "fleet-voters" },
  { key: "is_leader", label: "Leading", testId: "fleet-is-leader" },
] as const satisfies readonly FleetField<FleetMembers>[];

/** Every `/_fleet/health` field. `ring` carries both the epoch and the ring membership. */
export const FLEET_HEALTH_FIELDS = [
  { key: "state", label: "Readiness", testId: "fleet-state" },
  { key: "ring", label: "Ring", testId: "fleet-ring-epoch" },
  { key: "pending_gates", label: "Pending gates", testId: "fleet-pending-gates" },
  { key: "isolated", label: "Isolation", testId: "fleet-isolated" },
] as const satisfies readonly FleetField<FleetHealth>[];
