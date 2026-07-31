/** @vitest-environment jsdom */
import { afterEach, describe, expect, it, vi } from "vitest";

import { preferenceStore, resetPreferenceStore } from "./storage.ts";

afterEach(() => {
  vi.unstubAllGlobals();
  resetPreferenceStore();
});

describe("preferenceStore", () => {
  it("uses a working browser store when there is one", () => {
    const backing = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => backing.get(key) ?? null,
      setItem: (key: string, value: string) => void backing.set(key, value),
      removeItem: (key: string) => void backing.delete(key),
    });
    resetPreferenceStore();

    preferenceStore().setItem("rift-console.tenant", "acme");
    expect(backing.get("rift-console.tenant")).toBe("acme");
  });

  it("falls back to memory when the store throws, instead of taking the console down", () => {
    // Safari's private mode throws on `setItem`, and an embedded webview can block storage
    // outright. Losing a remembered tenant is a much smaller problem than a blank page — but the
    // fallback has to actually engage, which is what this pins.
    vi.stubGlobal("localStorage", {
      getItem: () => null,
      setItem: () => {
        throw new DOMException("QuotaExceededError");
      },
      removeItem: () => undefined,
    });
    resetPreferenceStore();

    expect(() => preferenceStore().setItem("rift-console.tenant", "acme")).not.toThrow();
    expect(preferenceStore().getItem("rift-console.tenant")).toBe("acme");
  });

  it("falls back to memory when there is no store at all", () => {
    vi.stubGlobal("localStorage", undefined);
    resetPreferenceStore();

    expect(() => preferenceStore().setItem("rift-console.tenant", "globex")).not.toThrow();
    expect(preferenceStore().getItem("rift-console.tenant")).toBe("globex");
  });

  it("probes with a real write, so a store that only fails on use is caught up front", () => {
    // A `localStorage` that exists but throws on `setItem` would pass a truthiness check and then
    // throw on the first real write, from inside a render.
    const setItem = vi.fn(() => {
      throw new Error("blocked");
    });
    vi.stubGlobal("localStorage", { getItem: () => null, setItem, removeItem: () => undefined });
    resetPreferenceStore();

    preferenceStore();
    expect(setItem).toHaveBeenCalled();
  });
});
