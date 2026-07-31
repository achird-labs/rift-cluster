import { type ReactNode, useEffect, useRef, useState } from "react";

/**
 * The console's one code editor (RFC-006 §9.1, issue #188).
 *
 * Two properties matter more than anything it does on screen:
 *
 * 1. **Monaco is bundled, never fetched.** It arrives through `monaco-editor` in `package.json` and
 *    is emitted into `dist/` by Vite, including its JSON language worker — which is why the worker
 *    is wired explicitly below instead of through `@monaco-editor/react` or monaco's own AMD loader,
 *    both of which resolve from a CDN by default. `bundle-offline.test.ts` scans the built assets
 *    for an external URL and fails on one; the console's CSP (`console.rs`: `default-src 'self'`)
 *    would block it at runtime anyway, so a CDN dependency would not degrade — it would break.
 *
 * 2. **A plain `<textarea>` is the honest fallback, and the only path the tests take.** jsdom has no
 *    layout, no canvas metrics and no worker host, so monaco cannot be instantiated there. Rather
 *    than mock it, this component *degrades*: it renders the textarea until monaco has loaded, and
 *    stays on the textarea forever if loading or instantiation throws. That is also what an operator
 *    gets on a browser monaco refuses to run in — a working editor, not a blank panel.
 *
 * The component holds no editor logic at all. Projection, linting, validation, conflict handling and
 * saving all live outside it, so the whole feature is exercised through the textarea path.
 */

export type CodeEditorProps = {
  value: string;
  onChange: (value: string) => void;
  /** The accessible name; there is no visible label inside the editor surface. */
  label: string;
  testId: string;
  readOnly?: boolean;
};

export function CodeEditor({ value, onChange, label, testId, readOnly }: CodeEditorProps): ReactNode {
  const host = useRef<HTMLDivElement | null>(null);
  const [live, setLive] = useState(false);
  // Held in a ref so the monaco change handler always calls the current `onChange` without the
  // editor having to be torn down and rebuilt whenever the parent re-renders.
  const latest = useRef(onChange);
  latest.current = onChange;

  useEffect(() => {
    let disposed = false;
    let dispose: (() => void) | null = null;

    void (async () => {
      try {
        /*
         * The workers are imported with Vite's `?worker` suffix and loaded here, *before* the
         * editor exists, because `MonacoEnvironment.getWorker` is called synchronously by monaco
         * and has to hand back a `Worker` there and then.
         *
         * `?worker` is the spelling that keeps this console air-gapped: Vite compiles each worker
         * into its own same-origin chunk in `dist/` and the constructor points at it. The
         * alternatives all reach the network — `@monaco-editor/react` and monaco's own AMD
         * `loader.js` default to jsdelivr, and `new Worker(new URL("monaco-editor/…"))` does not
         * resolve a bare specifier at all. `bundle-offline.test.ts` is what keeps that true.
         */
        const [monaco, , jsonWorker, editorWorker] = await Promise.all([
          import("monaco-editor/editor/editor.api"),
          import("monaco-editor/language/json/monaco.contribution"),
          import("monaco-editor/language/json/json.worker?worker"),
          import("monaco-editor/editor/editor.worker?worker"),
        ]);
        (self as unknown as { MonacoEnvironment?: unknown }).MonacoEnvironment = {
          getWorker: (_id: string, workerLabel: string): Worker =>
            workerLabel === "json" ? new jsonWorker.default() : new editorWorker.default(),
        };
        if (disposed || host.current === null) return;
        const editor = monaco.editor.create(host.current, {
          value,
          language: "json",
          automaticLayout: true,
          minimap: { enabled: false },
          readOnly: readOnly === true,
          ariaLabel: label,
        });
        const subscription = editor.onDidChangeModelContent(() => {
          latest.current(editor.getValue());
        });
        dispose = () => {
          subscription.dispose();
          editor.dispose();
        };
        setLive(true);
      } catch {
        /*
         * Deliberately swallowed, and the one case where that is right: this is a *capability*
         * probe, not a data path. The failure has a visible, correct consequence — the textarea
         * below stays — and nothing about the operator's stub is lost or misreported by it. The
         * alternative, surfacing "monaco failed to load" as an error, would tell an operator whose
         * editor is working fine that something is broken.
         */
      }
    })();

    return () => {
      disposed = true;
      dispose?.();
    };
    /*
     * Mount-only, deliberately. Monaco owns its own buffer once it is live, and re-creating the
     * editor whenever `value` changed would move the caret to the end of the document on every
     * keystroke. `value` is read once, to seed it.
     */
  }, []);

  return (
    <>
      <div
        ref={host}
        className="code-editor"
        data-testid={testId}
        hidden={!live}
        aria-hidden={!live}
      />
      {live ? null : (
        <textarea
          className="code-editor-fallback"
          data-testid="code-editor-fallback"
          aria-label={label}
          spellCheck={false}
          readOnly={readOnly === true}
          value={value}
          onChange={(event) => onChange(event.target.value)}
        />
      )}
    </>
  );
}
