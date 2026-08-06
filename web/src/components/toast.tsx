import {
  type ReactNode,
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

/**
 * A transient confirmation that something happened.
 *
 * **Confirmations only — never errors.** A toast disappears, and an error the operator did not
 * happen to be looking at is an error they never received. Every failing path in this console
 * already renders a persistent note next to the thing that failed (`ErrorNote`, `UnconfirmedNote`,
 * `BulkReport`), and that is where failures stay. What has had nowhere to go is success: a file
 * downloads, a route table commits, and the screen looks exactly as it did a moment earlier.
 *
 * `warn` exists for a success that carries a caveat — "committed, but one node did not confirm" —
 * not for a failure wearing a softer colour.
 */
export type ToastTone = "good" | "warn";

export type Toast = {
  id: number;
  message: string;
  /** The detail under the message: a count, a filename, a revision. */
  meta?: string;
  tone: ToastTone;
};

type Raise = (toast: Omit<Toast, "id">) => void;

const ToastContext = createContext<Raise | null>(null);

/** How long a toast stays. Long enough to read a sentence twice, short enough not to stack up. */
const DWELL_MS = 5000;

export function ToastHost({ children }: { children: ReactNode }): ReactNode {
  const [toasts, setToasts] = useState<readonly Toast[]>([]);

  const raise = useCallback<Raise>((toast) => {
    // `Date.now()` would collide for two raised in the same millisecond, which is exactly what a
    // bulk action does. A counter cannot.
    setToasts((current) => [...current, { ...toast, id: nextId() }]);
  }, []);

  const value = useMemo(() => raise, [raise]);

  return (
    <ToastContext.Provider value={value}>
      {children}
      {/*
       * `aria-live="polite"` on the region rather than on each toast: a live region has to exist in
       * the DOM before content is inserted into it, or the insertion is not announced at all. So the
       * region is always here and only its contents change.
       */}
      <div className="toast-host" role="status" aria-live="polite">
        {toasts.map((toast) => (
          <ToastRow
            key={toast.id}
            toast={toast}
            onDone={() => setToasts((current) => current.filter((t) => t.id !== toast.id))}
          />
        ))}
      </div>
    </ToastContext.Provider>
  );
}

let counter = 0;
function nextId(): number {
  counter += 1;
  return counter;
}

function ToastRow({ toast, onDone }: { toast: Toast; onDone: () => void }): ReactNode {
  /*
   * The callback behind a ref, so the timer is armed once.
   *
   * `onDone` is a fresh closure on every parent render — and the parent re-renders whenever another
   * toast is raised or dismissed. Depending on it directly would clear and re-arm the timer each
   * time, so a toast raised during a busy moment would never actually leave.
   */
  const done = useRef(onDone);
  done.current = onDone;

  useEffect(() => {
    const timer = setTimeout(() => done.current(), DWELL_MS);
    return () => clearTimeout(timer);
  }, []);

  return (
    <div className={`toast is-${toast.tone}`} data-testid="toast">
      <span className="toast-dot" aria-hidden="true" />
      <div>
        <div className="toast-msg">{toast.message}</div>
        {toast.meta === undefined ? null : <div className="toast-meta">{toast.meta}</div>}
      </div>
      {/*
       * Dismissable, because five seconds is a guess. Anything that auto-disappears should also go
       * when asked — a reader who has read it should not have to wait for it.
       */}
      <button type="button" className="toast-close" aria-label="Dismiss" onClick={onDone}>
        &times;
      </button>
    </div>
  );
}

/**
 * Raise a toast.
 *
 * Returns a no-op outside a `ToastHost` rather than throwing: a component that confirms an action
 * should still perform the action when rendered in a test harness that has no host, and a thrown
 * error here would turn a missing decoration into a broken screen.
 */
export function useToast(): Raise {
  const raise = useContext(ToastContext);
  return raise ?? noop;
}

const noop: Raise = () => undefined;
