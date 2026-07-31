/**
 * Where a view preference is remembered, and what to do when there is nowhere.
 *
 * `window.localStorage` is not always reachable: Safari's private mode throws on access, an
 * embedded webview may block storage outright, and an opaque origin has none at all. None of that
 * is a reason for the console to fail to render — losing a remembered tenant is a much smaller
 * problem than a blank page — so the fallback is an in-memory store with the same shape. It is a
 * genuine terminal fallback: the payload is infallible by construction and nothing about
 * correctness depends on it, only on convenience across reloads.
 */
export type KeyValueStore = Pick<Storage, "getItem" | "setItem" | "removeItem">;

function memoryStore(): KeyValueStore {
  const map = new Map<string, string>();
  return {
    getItem: (key) => map.get(key) ?? null,
    setItem: (key, value) => void map.set(key, value),
    removeItem: (key) => void map.delete(key),
  };
}

/**
 * Probes with a real write: a `localStorage` that exists but throws on `setItem` (private mode,
 * quota exhausted) would otherwise pass a truthiness check and then throw on first use.
 */
function detect(): KeyValueStore {
  try {
    const candidate = globalThis.localStorage as Storage | undefined;
    if (candidate === undefined || candidate === null) return memoryStore();
    const probe = "rift-console.probe";
    candidate.setItem(probe, "1");
    candidate.removeItem(probe);
    return candidate;
  } catch {
    // Deliberately not reported: an unavailable store is an environment fact, not a fault, and it
    // costs the user only the tenant they had selected last time.
    return memoryStore();
  }
}

let store: KeyValueStore | null = null;

export function preferenceStore(): KeyValueStore {
  store ??= detect();
  return store;
}

/** Test seam: drop the memoised store so a test can install its own backing store. */
export function resetPreferenceStore(): void {
  store = null;
}
