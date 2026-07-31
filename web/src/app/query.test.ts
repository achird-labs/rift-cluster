import { describe, expect, it } from "vitest";

import { ApiError } from "../api/client.ts";
import { POLLED, POLL_INTERVAL_MS, retryTransportFailures } from "./query.ts";

describe("retryTransportFailures", () => {
  it("does not re-ask a question the fleet has already answered", () => {
    for (const status of [400, 401, 403, 404, 409, 413]) {
      expect(retryTransportFailures(0, new ApiError(status, "no"))).toBe(false);
    }
  });

  it("retries a transport failure and a server-side error once", () => {
    expect(retryTransportFailures(0, new TypeError("network error"))).toBe(true);
    expect(retryTransportFailures(0, new ApiError(503, "not ready"))).toBe(true);
    expect(retryTransportFailures(1, new ApiError(503, "not ready"))).toBe(false);
  });
});

describe("the polling contract", () => {
  it("polls every 5 seconds and never in the background", () => {
    // Stated as an assertion as well as an option because the behavioural test that counts fetches
    // across a visibilitychange lives in `polling.test.tsx`; this pins the numbers RFC-006 §6 sets.
    expect(POLL_INTERVAL_MS).toBe(5_000);
    expect(POLLED.refetchInterval).toBe(POLL_INTERVAL_MS);
    expect(POLLED.refetchIntervalInBackground).toBe(false);
  });
});
