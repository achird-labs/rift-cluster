/** @vitest-environment jsdom */
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Admin } from "../screens/Admin.tsx";
import { renderInApp, stubFetch, whoamiWith } from "./harness.tsx";

const SINK = {
  uri: "s3://audit-bucket/rift",
  authRef: "audit-writer",
  batchMaxRows: 500,
  revision: 1282,
};

afterEach(() => vi.unstubAllGlobals());

describe("audit sink", () => {
  it("is refused to a tenant-admin, who may read their own audit rows but not redirect the fleet's", async () => {
    // `authz.rs` puts `AuditSinkRead` in the same ClusterAdmin arm as the writes, so *reading* this
    // screen is as privileged as changing it. A tenant-admin holds `audit.read` and still gets
    // nothing here.
    stubFetch({});
    renderInApp(<Admin tab="sink" tenant="acme" />, { whoami: whoamiWith("tenant-admin") });

    expect((await screen.findByTestId("sink-forbidden")).textContent).toMatch(/fleetadmin/i);
    // And it did not ask, so a guaranteed 404 is not sitting in the network log looking like a bug.
    expect(vi.mocked(fetch).mock.calls.some(([i]) => String(i).includes("/audit/sink"))).toBe(false);
  });

  it("reads a 404 as 'no sink declared' rather than an error", async () => {
    // The contract uses 404 for both "nothing declared" and "no fleet access" (RFC-002 §8.4). Only a
    // principal with the scope reaches this screen, so here it means the former.
    stubFetch({ "/admin/audit/sink": { status: 404, json: { errors: [] } } });
    renderInApp(<Admin tab="sink" tenant={null} />, { whoami: whoamiWith("fleet-admin") });

    expect(await screen.findByTestId("sink-none")).toBeTruthy();
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("says the status is unknown on a follower instead of rendering zeros", async () => {
    /*
     * Only the leader runs the exporter, so only the leader reports status — the contract is
     * explicit that a follower omits it "rather than a fabricated all-zero one". Rendering absent as
     * `0 rows shipped, not running` turns "this node cannot say" into "the export is broken".
     */
    stubFetch({ "/admin/audit/sink": { json: SINK } });
    renderInApp(<Admin tab="sink" tenant={null} />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("sink-status-unknown")).textContent).toMatch(/leader only/i);
    expect(screen.queryByTestId("sink-status")).toBeNull();
  });

  it("shows the export status when the node is the leader", async () => {
    stubFetch({
      "/admin/audit/sink": {
        json: { ...SINK, exportStatus: { running: true, lastError: null, shippedRows: 9120, consecutiveFailures: 0 } },
      },
    });
    renderInApp(<Admin tab="sink" tenant={null} />, { whoami: whoamiWith("fleet-admin") });

    expect((await screen.findByTestId("sink-status")).textContent).toMatch(/9120/);
    expect(screen.queryByTestId("sink-status-unknown")).toBeNull();
  });

  it("omits batchMaxRows when blank instead of sending zero", async () => {
    // The server default applies to an *omitted* field. A literal 0 is a batch size that ships
    // nothing, forever — so a blank box must not become one.
    stubFetch({
      "/admin/audit/sink": { status: 404, json: { errors: [] } },
    });
    renderInApp(<Admin tab="sink" tenant={null} />, { whoami: whoamiWith("fleet-admin") });

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: /declare a sink/i }));
    await user.type(screen.getByLabelText(/^uri$/i), "s3://bucket/rift");
    await user.click(screen.getByRole("button", { name: /save sink/i }));

    await waitFor(() =>
      expect(vi.mocked(fetch).mock.calls.some(([, init]) => init?.method === "PUT")).toBe(true),
    );
    const put = vi.mocked(fetch).mock.calls.find(([, init]) => init?.method === "PUT");
    const body = JSON.parse(String(put?.[1]?.body)) as Record<string, unknown>;
    expect(body).toEqual({ uri: "s3://bucket/rift" });
    expect(body).not.toHaveProperty("batchMaxRows");
  });

  it("refuses a zero batch size at the field", async () => {
    stubFetch({ "/admin/audit/sink": { status: 404, json: { errors: [] } } });
    renderInApp(<Admin tab="sink" tenant={null} />, { whoami: whoamiWith("fleet-admin") });

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: /declare a sink/i }));
    await user.type(screen.getByLabelText(/^uri$/i), "s3://bucket/rift");
    await user.type(screen.getByLabelText(/batch max rows/i), "0");
    await user.click(screen.getByRole("button", { name: /save sink/i }));

    expect((await screen.findByTestId("sink-invalid")).textContent).toMatch(/1 or more/i);
    expect(vi.mocked(fetch).mock.calls.some(([, init]) => init?.method === "PUT")).toBe(false);
  });
});
