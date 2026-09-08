import { type ChangeEvent, type ReactNode, useState } from "react";

import { useCompileSpec, useImportAddImposter } from "../../app/queries.ts";
import { ErrorNote, UnconfirmedNote } from "../../components/primitives.tsx";
import { useToast } from "../../components/toast.tsx";
import {
  type CompileSummary,
  type SpecCompileResult,
  compileFailureText,
  parsePort,
  preflight,
  specContentType,
  summarize,
} from "./openapi.ts";

/**
 * Import an OpenAPI document as an imposter (D-72, RFC-007 §3.1, #553) — WireMock Cloud's
 * *Import* button, beside *Record*.
 *
 * Two steps, and the seam between them is the design. **Compile** sends the document to
 * `POST /specs/compile`, which answers the imposter it built and the operations it built it from
 * and keeps nothing — no record, no op, no table read. **Create imposter** then sends that config
 * through `useImportAddImposter`, the same `POST /imposters` every other create takes, with the
 * same idempotency key and the same parked-write settling. So the review step is genuinely a
 * review: nothing has happened to the fleet until the second button is pressed, and closing the
 * dialog between the two leaves no trace anywhere.
 *
 * The compiler's refusals are shown as the server wrote them (`compileFailureText`): an unsupported
 * version, an external `$ref`, a parse failure — that text *is* the diagnosis, and the console has
 * no better one.
 */
export function OpenApiImport({ onClose }: { onClose: () => void }): ReactNode {
  const [text, setText] = useState("");
  const [filename, setFilename] = useState<string | undefined>(undefined);
  const [port, setPort] = useState("");
  const [name, setName] = useState("");
  const [compiled, setCompiled] = useState<SpecCompileResult | null>(null);
  const [unconfirmed, setUnconfirmed] = useState<string | null>(null);

  const compile = useCompileSpec();
  const add = useImportAddImposter();
  const toast = useToast();

  const busy = compile.isPending || add.isPending;
  const parsedPort = parsePort(port);
  const documentProblem = preflight(text);
  const portProblem =
    port.trim().length === 0 || parsedPort !== null
      ? null
      : "Port must be a whole number between 1 and 65535.";
  const canCompile = documentProblem === null && parsedPort !== null && !busy;

  async function handleFile(event: ChangeEvent<HTMLInputElement>): Promise<void> {
    const file = event.target.files?.[0];
    // Cleared unconditionally, so choosing the same file again after an edit still fires a change.
    event.target.value = "";
    if (file === undefined) return;
    setFilename(file.name);
    setText(await file.text());
    compile.reset();
  }

  function onTextChange(next: string): void {
    setText(next);
    // A pasted document has no filename; a file's name stops describing text that was then edited.
    setFilename(undefined);
    compile.reset();
  }

  function runCompile(): void {
    if (parsedPort === null) return;
    compile.mutate(
      { text, contentType: specContentType(text, filename), port: parsedPort, name },
      { onSuccess: (result) => setCompiled(result) },
    );
  }

  function runCreate(): void {
    if (compiled === null) return;
    add.mutate(compiled.imposter, {
      onSuccess: (outcome) => {
        if (outcome.kind === "unobservable") {
          // Not a success and not a failure: the fleet accepted the write and we could not watch it
          // land. Said in the dialog, in those words, rather than toasted as done.
          setUnconfirmed(outcome.reason);
          return;
        }
        const summary = summarize(compiled);
        toast({
          tone: "good",
          message: `Imported ${summary.name ?? `imposter ${String(summary.port ?? "")}`.trim()}`,
          meta: `${String(summary.operations.length)} operations`,
        });
        onClose();
      },
    });
  }

  const summary = compiled === null ? null : summarize(compiled);

  return (
    <div
      className="scrim"
      onKeyDown={(event) => {
        if (event.key === "Escape" && !busy) onClose();
      }}
    >
      <div
        className="confirm wizard"
        role="dialog"
        aria-modal="true"
        aria-label="Import OpenAPI"
        data-testid="openapi-import"
      >
        <header className="wizard-head">
          <div>
            <h2>Import OpenAPI</h2>
            <p className="muted">
              POST /specs/compile, then POST /imposters &mdash; the same replicated create
            </p>
          </div>
        </header>

        <div className="wizard-body">
          {summary === null ? (
            <DocumentStep
              text={text}
              port={port}
              name={name}
              portProblem={portProblem}
              busy={busy}
              onText={onTextChange}
              onFile={(event) => void handleFile(event)}
              onPort={setPort}
              onName={setName}
            />
          ) : (
            <ReviewStep summary={summary} />
          )}

          {compile.isError ? (
            <p className="error" role="alert" data-testid="openapi-compile-error">
              {compileFailureText(compile.error)}
            </p>
          ) : null}
          {add.isError ? (
            <ErrorNote error={add.error} context="The imposter was not created" />
          ) : null}
          {unconfirmed === null ? null : <UnconfirmedNote reason={unconfirmed} />}
        </div>

        <footer className="wizard-foot">
          <span className="muted">
            {summary === null
              ? "Compiled here and now; the fleet keeps no copy of the document."
              : "Nothing has been written yet."}
          </span>
          <div className="row">
            <button className="btn" type="button" onClick={onClose} disabled={busy}>
              {unconfirmed === null ? "Cancel" : "Close"}
            </button>
            {summary === null ? (
              <button
                key="compile"
                className="btn primary"
                type="button"
                data-testid="openapi-compile"
                disabled={!canCompile}
                onClick={runCompile}
              >
                {compile.isPending ? "Compiling…" : "Compile"}
              </button>
            ) : (
              <>
                <button
                  className="btn"
                  type="button"
                  data-testid="openapi-back"
                  disabled={busy || unconfirmed !== null}
                  onClick={() => {
                    setCompiled(null);
                    add.reset();
                  }}
                >
                  Back
                </button>
                {/*
                  Keyed apart from "Compile" for the reason the create wizard keys its buttons: the
                  two occupy the same slot, and a click in flight on one must not land on the other.
                */}
                <button
                  key="create"
                  className="btn primary"
                  type="button"
                  data-testid="openapi-create"
                  disabled={busy || unconfirmed !== null || summary.port === null}
                  onClick={runCreate}
                >
                  {add.isPending ? "Creating…" : "Create imposter"}
                </button>
              </>
            )}
          </div>
        </footer>
      </div>
    </div>
  );
}

function DocumentStep({
  text,
  port,
  name,
  portProblem,
  busy,
  onText,
  onFile,
  onPort,
  onName,
}: {
  text: string;
  port: string;
  name: string;
  portProblem: string | null;
  busy: boolean;
  onText: (text: string) => void;
  onFile: (event: ChangeEvent<HTMLInputElement>) => void;
  onPort: (port: string) => void;
  onName: (name: string) => void;
}): ReactNode {
  return (
    <>
      <p className="hint">
        The document is <b>compiled, not stored</b>: the fleet reads it once, hands back the
        imposter it describes, and retains nothing (D-72). Only the imposter you create in the next
        step reaches the log.
      </p>
      <div className="field">
        <label htmlFor="openapi-file">OpenAPI 3.0 document (JSON or YAML)</label>
        <input
          id="openapi-file"
          data-testid="openapi-file"
          type="file"
          accept=".json,.yaml,.yml,application/json,application/yaml"
          disabled={busy}
          onChange={onFile}
        />
      </div>
      <div className="field">
        <label htmlFor="openapi-text">Or paste it</label>
        <textarea
          id="openapi-text"
          data-testid="openapi-text"
          rows={8}
          value={text}
          disabled={busy}
          spellCheck={false}
          onChange={(event) => onText(event.target.value)}
          placeholder={'openapi: "3.0.3"\ninfo: …\npaths: …'}
        />
      </div>
      <div className="row">
        <div className="field">
          <label htmlFor="openapi-port">Port</label>
          <input
            id="openapi-port"
            data-testid="openapi-port"
            type="number"
            inputMode="numeric"
            min={1}
            max={65535}
            required
            value={port}
            disabled={busy}
            onChange={(event) => onPort(event.target.value)}
          />
          {/* Required by the route, and for a reason worth showing: nothing is stored, so there is
              no record to infer a binding from, and an imposter with no port cannot replicate. */}
          <span className={portProblem === null ? "muted" : "warn-text"} data-testid="openapi-port-hint">
            {portProblem ?? "Required — a compiled imposter binds a port on every node."}
          </span>
        </div>
        <div className="field">
          <label htmlFor="openapi-name">Name (optional)</label>
          <input
            id="openapi-name"
            data-testid="openapi-name"
            type="text"
            value={name}
            disabled={busy}
            autoComplete="off"
            onChange={(event) => onName(event.target.value)}
          />
        </div>
      </div>
    </>
  );
}

/** What the compiler produced, before any of it is written. */
function ReviewStep({ summary }: { summary: CompileSummary }): ReactNode {
  return (
    <div data-testid="openapi-review">
      <dl className="kv">
        <dt>Port</dt>
        <dd data-testid="openapi-review-port">
          {summary.port === null ? (
            <span className="warn-text">none — the compiler emitted no port, so this cannot be created</span>
          ) : (
            <code>{summary.port}</code>
          )}
        </dd>
        <dt>Name</dt>
        <dd data-testid="openapi-review-name">{summary.name ?? "—"}</dd>
        <dt>Stubs</dt>
        <dd data-testid="openapi-review-stubs">
          {/* Unknown is unknown: a config whose `stubs` is not an array is not one with zero. */}
          {summary.stubCount === null ? "unknown" : summary.stubCount}
        </dd>
        <dt>Operations</dt>
        <dd>{summary.operations.length}</dd>
      </dl>
      <table className="dense" data-testid="openapi-operations">
        <thead>
          <tr>
            <th scope="col">Method</th>
            <th scope="col">Path</th>
            <th scope="col">Operation</th>
            <th scope="col" className="numeric">
              Stubs
            </th>
          </tr>
        </thead>
        <tbody>
          {summary.operations.map((operation) => (
            <tr key={operation.id}>
              <td>
                <span className={`method ${operation.method.toLowerCase()}`}>
                  {operation.method.toUpperCase()}
                </span>
              </td>
              <td>
                <code>{operation.pathTemplate}</code>
              </td>
              <td>{operation.id}</td>
              <td className="numeric">{operation.stubIds.length}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
