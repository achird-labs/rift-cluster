import type { components } from "../../api/schema.ts";

type Stub = components["schemas"]["Stub"];

/**
 * Whether an imposter is actively recording, only replaying what it already has, or carries no
 * stubs at all — read straight from its stub list rather than from anything this console remembers.
 *
 * A proxy stub can be created outside the console entirely (curl, another operator, an imported
 * fixture), so there is no console-side "I started this" flag that can be trusted. The stub list is
 * the one thing every path that can create a recording is guaranteed to update, so it is the only
 * thing this reads — an imposter recorded before the console ever saw it still reads as Recording
 * on first load.
 */
export type RecordingState = "recording" | "replaying" | "empty";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * A stub's response carries a proxy when it declares the `proxy` key at all — its value is never
 * checked, because a malformed proxy block is still a recording someone started.
 *
 * `isRecord` first: the contract types `responses` as objects, but this runs on every imposter
 * detail load against whatever the fleet actually returned, and `"proxy" in null` throws.
 */
function isProxyResponse(response: unknown): boolean {
  return isRecord(response) && "proxy" in response;
}

export function recordingState(stubs: Stub[] | undefined): RecordingState {
  if (stubs === undefined || stubs.length === 0) return "empty";
  const recording = stubs.some((stub) => (stub.responses ?? []).some(isProxyResponse));
  return recording ? "recording" : "replaying";
}

/** The predicate-generator fields the engine can match a recorded request on. */
export const GENERATOR_FIELDS = ["method", "path", "query", "headers", "body"] as const;
export type GeneratorField = (typeof GENERATOR_FIELDS)[number];

/**
 * Selected by default when the start-recording form opens.
 *
 * `headers` and `body` are left off deliberately: both vary far more than an operator usually wants
 * a stub keyed on. A `Date` header or a JSON body carrying a millisecond timestamp turns every
 * request into a "unique" one, and `proxyAlways` in particular would then never replay anything it
 * has already recorded — it would just keep recording. `method`, `path` and `query` are the fields
 * mountebank's own docs use as the running example, and the fields most APIs actually vary their
 * responses on.
 */
export const DEFAULT_GENERATOR_FIELDS: GeneratorField[] = ["method", "path", "query"];

export type ProxyMode = "proxyOnce" | "proxyAlways" | "proxyTransparent";

/**
 * The three proxy modes the engine recognizes (`vendor/rift/docs/mountebank/proxy.md:36-79`), each
 * carrying what it does **and what it costs**. A picker that only named the modes would leave an
 * operator to guess which one duplicates recordings across a fleet — the prose exists so they do
 * not have to.
 */
export const PROXY_MODES: readonly { value: ProxyMode; label: string; description: string }[] = [
  {
    value: "proxyOnce",
    label: "Record once, then replay automatically",
    description:
      "Forwards the first matching request to the upstream, records the response as a stub, then answers every later match from that recording. One live call per unique request; replay is free after that.",
  },
  {
    value: "proxyAlways",
    label: "Forward every time, keep recording",
    description:
      "Continuously appends a new stub for every matching request, including ones already recorded, so the stub list keeps growing and accumulates duplicate recordings the longer this mode runs against a fleet.",
  },
  {
    value: "proxyTransparent",
    label: "Forward only, record nothing",
    description:
      "Unconditionally forwards every matching request to the upstream and records nothing at all, so there is nothing here to promote later. Zero recording cost, and zero replayable value once it stops.",
  },
];

/** What a start-recording form has filled in. */
export type ProxyStubForm = {
  to: string;
  mode: ProxyMode;
  fields: readonly GeneratorField[];
  caseSensitive: boolean;
};

/**
 * The single stub a recording start writes: one response carrying a `proxy` block, nothing else.
 *
 * `matches` is a **whitelist** (`vendor/rift/docs/mountebank/proxy.md:83-137`): only the selected
 * fields are written, never `false` for the rest, because `{body: false}` and an absent `body` mean
 * the same thing to the engine and only one of them says on screen that the operator did not ask
 * for it.
 */
export function proxyStubFor(form: ProxyStubForm): Stub {
  const matches: Partial<Record<GeneratorField, true>> = {};
  for (const field of form.fields) {
    matches[field] = true;
  }
  return {
    responses: [
      {
        proxy: {
          to: form.to,
          mode: form.mode,
          predicateGenerators: [{ matches, caseSensitive: form.caseSensitive }],
        },
      },
    ],
  };
}

/** A recorded stub's response read defensively, for a projection that is `unknown` beyond its shape. */
export function recordAt(value: unknown, key: string): unknown {
  return isRecord(value) ? value[key] : undefined;
}

export function stringAt(value: unknown, key: string): string | undefined {
  const child = recordAt(value, key);
  return typeof child === "string" ? child : undefined;
}

export function numberAt(value: unknown, key: string): number | undefined {
  const child = recordAt(value, key);
  return typeof child === "number" ? child : undefined;
}
