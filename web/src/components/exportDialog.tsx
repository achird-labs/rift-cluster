import { type ReactNode, useState } from "react";

import {
  type ExportOptions,
  exportOptionsQuery,
} from "../features/imposters/portable.ts";

/** What the export covers. The imposter list offers only the tenant; a detail screen offers both. */
export type ExportScope = { kind: "tenant" } | { kind: "one"; port: number };

/**
 * The export dialog.
 *
 * Two buttons on the screen used to be the whole affordance — "Replay-ready" and "As configured" —
 * which named the two projections but never said what either one puts in the file. That is the
 * question an operator actually arrives with, because the answer decides whether the document is a
 * fixture they can commit or a config dump that needs the upstreams reachable to be worth anything.
 *
 * So the choice moves into a dialog that shows the request it will run and lists what lands in the
 * file, item by item, changing as the options change.
 */
export function ExportDialog({
  scope,
  scopes,
  tenant,
  imposterCount,
  busy,
  onScope,
  onExport,
  onCancel,
}: {
  scope: ExportScope;
  /** Omitted when there is only one scope to offer, which is the imposter list's case. */
  scopes?: readonly ExportScope[];
  tenant: string | null;
  imposterCount: number;
  busy: boolean;
  onScope?: (scope: ExportScope) => void;
  onExport: (options: ExportOptions) => void;
  onCancel: () => void;
}): ReactNode {
  const [options, setOptions] = useState<ExportOptions>({
    replayable: true,
    removeProxies: true,
    tls: false,
  });

  const holes = options.removeProxies && !options.replayable;
  const path = scope.kind === "one" ? `/imposters/${String(scope.port)}` : "/imposters";
  const file =
    scope.kind === "one"
      ? `imposter-${String(scope.port)}.json`
      : `imposters-${tenant ?? "all"}.json`;

  return (
    <div
      className="scrim"
      onKeyDown={(event) => {
        if (event.key === "Escape") onCancel();
      }}
    >
      <div
        className="confirm export-dialog"
        role="dialog"
        aria-modal="true"
        aria-label="Export configuration"
        data-testid="export-dialog"
      >
        <h2>Export configuration</h2>
        <p className="muted">
          Reads through this node — a snapshot at its applied index, not a live stream.
        </p>

        {scopes === undefined || scopes.length < 2 ? null : (
          <div className="field">
            <span className="eyebrow">Scope</span>
            <div className="pill-filters export-scopes">
              {scopes.map((entry) => (
                <button
                  key={entry.kind === "one" ? `one-${String(entry.port)}` : "tenant"}
                  type="button"
                  className="pill-filter"
                  aria-pressed={entry.kind === scope.kind}
                  onClick={() => onScope?.(entry)}
                >
                  {entry.kind === "one"
                    ? `This imposter · ${String(entry.port)}`
                    : `Every imposter in ${tenant ?? "this tenant"} · ${String(imposterCount)}`}
                </button>
              ))}
            </div>
          </div>
        )}

        <div className="export-opts">
          <Opt
            id="replayable"
            label="replayable=true"
            checked={options.replayable}
            note="Inline every proxy recording as an ordinary stub, so the file replays with no upstream reachable. This is what makes an export a fixture instead of a config dump."
            onChange={(next) => setOptions({ ...options, replayable: next })}
          />
          <Opt
            id="removeProxies"
            label="removeProxies=true"
            checked={options.removeProxies}
            warn={holes}
            note={
              options.replayable
                ? "Drop the proxy stubs themselves once their recordings are inlined — nothing in the file can reach out."
                : "Without replayable the recordings are not inlined first, so removing the proxies leaves those paths answering nothing."
            }
            onChange={(next) => setOptions({ ...options, removeProxies: next })}
          />
          <Opt
            id="tls"
            label="include TLS material"
            checked={options.tls}
            warn={options.tls}
            note="https imposters carry key and cert in the document. Off by default — an export with this on is a private key in a file someone will commit."
            onChange={(next) => setOptions({ ...options, tls: next })}
          />
        </div>

        <div className="field">
          <span className="eyebrow">Request</span>
          <pre className="payload" data-testid="export-curl">
            {`curl -s '${path}${exportOptionsQuery(options)}' \\\n${
              tenant === null ? "" : `  -H 'X-Rift-Tenant: ${tenant}' \\\n`
            }  > ${file}`}
          </pre>
        </div>

        <div className="export-contents">
          <h3>What lands in the file</h3>
          <ul>
            <Line ok>
              {scope.kind === "one" ? "1 imposter" : `${String(imposterCount)} imposters`} — ports,
              protocols, stubs, predicates and scenario definitions
            </Line>
            <Line ok={options.replayable}>
              {options.replayable
                ? "Proxy recordings inlined as stubs — the file replays standalone"
                : "Proxy stubs left as proxies — replaying this needs the upstreams reachable"}
            </Line>
            <Line ok={options.tls}>
              {options.tls
                ? "TLS keys and certs in plaintext"
                : "No TLS material — https imposters import needing key and cert supplied again"}
            </Line>
            <Line ok={false}>
              No flow state, no scenario positions, no recorded requests — those are runtime, and an
              import starts every flow at its initial state
            </Line>
            <Line ok={false}>
              No provenance: source ids and revisions are fleet-local, so an import arrives
              hand-authored
            </Line>
          </ul>
        </div>

        {options.tls || holes ? (
          <div className="banner crit" data-testid="export-warning" role="status">
            <span className="b-glyph" aria-hidden="true">
              &#9650;
            </span>
            <div>
              {options.tls ? (
                <>
                  <strong>This file will contain private keys.</strong>
                  <p>
                    Treat it as a secret, or export without TLS material and supply key and cert at
                    import.
                  </p>
                </>
              ) : (
                <>
                  <strong>This produces a file with holes.</strong>
                  <p>
                    removeProxies without replayable drops the proxy stubs when their recordings
                    were never inlined, so those paths answer nothing at whoever imports it.
                  </p>
                </>
              )}
            </div>
          </div>
        ) : null}

        <div className="acts">
          <button className="btn" type="button" onClick={onCancel} disabled={busy}>
            Cancel
          </button>
          <button
            className="btn primary"
            type="button"
            data-testid="export-download"
            onClick={() => onExport(options)}
            disabled={busy}
          >
            {busy
              ? "Exporting…"
              : scope.kind === "one"
                ? "Download imposter"
                : `Download ${String(imposterCount)} imposter${imposterCount === 1 ? "" : "s"}`}
          </button>
        </div>
      </div>
    </div>
  );
}

/** One toggle, with the sentence that says what it does to the file. */
function Opt({
  id,
  label,
  checked,
  note,
  warn = false,
  onChange,
}: {
  id: string;
  label: string;
  checked: boolean;
  note: string;
  warn?: boolean;
  onChange: (next: boolean) => void;
}): ReactNode {
  return (
    <label className={`export-opt${warn ? " is-warn" : ""}`} htmlFor={`export-${id}`}>
      {/* A real checkbox, unstyled beyond `accent-color`: setting `background-color` on one
          suppresses the native checked indicator in Blink, which is how every checkbox in this
          console once rendered as an empty square. */}
      <input
        id={`export-${id}`}
        type="checkbox"
        checked={checked}
        data-testid={`export-opt-${id}`}
        onChange={(event) => onChange(event.target.checked)}
      />
      <div>
        <div className="export-opt-label">{label}</div>
        <p className="note">{note}</p>
      </div>
    </label>
  );
}

/** One line of "what lands in the file" — the glyph carries the state, not only the colour. */
function Line({ ok, children }: { ok: boolean; children: ReactNode }): ReactNode {
  return (
    <li className={ok ? "is-in" : "is-out"}>
      <span aria-hidden="true">{ok ? "✓" : "·"}</span>
      <span>
        <span className="visually-hidden">{ok ? "Included: " : "Not included: "}</span>
        {children}
      </span>
    </li>
  );
}
