import { describe, expect, it } from "vitest";

import { ApiError } from "../../api/client.ts";
import { keyedAttempt, mintKey } from "./idempotency.ts";

/**
 * #371. The two policies that look reasonable are both wrong, in opposite directions, and these
 * tests are written against exactly those two failure modes rather than against the implementation.
 */

describe("an idempotency key survives an unknown outcome (#371)", () => {
  // The bug as filed: a write commits, its response is lost, the operator retries. Without a
  // stable key that retry is a second op.
  it("reuses the key when the request threw without a response", async () => {
    const keyed = keyedAttempt();
    const seen: string[] = [];

    await expect(
      keyed((key) => {
        seen.push(key);
        // What `fetch` throws on a dropped connection — no response, so no ApiError.
        return Promise.reject(new TypeError("Failed to fetch"));
      }),
    ).rejects.toThrow("Failed to fetch");

    await keyed((key) => {
      seen.push(key);
      return Promise.resolve("ok");
    });

    expect(seen).toHaveLength(2);
    expect(seen[0]).toBe(seen[1]);
  });

  it("holds one key across several unknown outcomes, not just the first", async () => {
    const keyed = keyedAttempt();
    const seen: string[] = [];
    const drop = (key: string) => {
      seen.push(key);
      return Promise.reject(new TypeError("Failed to fetch"));
    };

    for (let i = 0; i < 3; i += 1) {
      await expect(keyed(drop)).rejects.toThrow();
    }

    expect(new Set(seen).size).toBe(1);
  });
});

describe("an idempotency key rotates once the fleet has answered (#371)", () => {
  /*
   * The subtle half, and the reason a never-rotating key is worse than none. The admin front:
   * "A keyed retry (same Idempotency-Key) of a 409 dedups to that same 409 by design — rebase and
   * retry with a fresh key." Hold the key across a refusal and the operator fixes the conflict,
   * presses save, and is handed the stale 409 forever.
   */
  it("rotates after a 409, so a rebased retry is not replayed into the same refusal", async () => {
    const keyed = keyedAttempt();
    const seen: string[] = [];

    await expect(
      keyed((key) => {
        seen.push(key);
        return Promise.reject(new ApiError(409, "revision moved"));
      }),
    ).rejects.toThrow();

    await keyed((key) => {
      seen.push(key);
      return Promise.resolve("ok");
    });

    expect(seen[0]).not.toBe(seen[1]);
  });

  it("rotates after a success, so the next write is a new intent", async () => {
    const keyed = keyedAttempt();
    const seen: string[] = [];
    const ok = (key: string) => {
      seen.push(key);
      return Promise.resolve("ok");
    };

    await keyed(ok);
    await keyed(ok);

    expect(seen[0]).not.toBe(seen[1]);
  });

  // A 5xx is still an answer: the fleet refused, and the retry is a fresh intent. Treating it as
  // unknown would reuse the key and dedup the retry into the stored refusal.
  it("rotates after a 503, because the fleet answered", async () => {
    const keyed = keyedAttempt();
    const seen: string[] = [];

    await expect(
      keyed((key) => {
        seen.push(key);
        return Promise.reject(new ApiError(503, "unavailable"));
      }),
    ).rejects.toThrow();
    await keyed((key) => {
      seen.push(key);
      return Promise.resolve("ok");
    });

    expect(seen[0]).not.toBe(seen[1]);
  });
});

describe("minted keys", () => {
  it("are unique", () => {
    const keys = new Set(Array.from({ length: 200 }, () => mintKey()));
    expect(keys.size).toBe(200);
  });

  it("are non-empty strings, so the client never sends a blank header", () => {
    expect(mintKey().length).toBeGreaterThan(0);
  });
});
