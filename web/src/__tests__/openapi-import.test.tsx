/** @vitest-environment jsdom */
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MAX_SPEC_BYTES } from "../features/import/openapi.ts";
import { Imposters } from "../screens/Imposters.tsx";
import { renderInApp, stubFetch } from "./harness.tsx";

/**
 * The OpenAPI import (D-72, #553), walked from the Imposters screen: choose or paste a document,
 * give it a port, compile, review, create.
 *
 * What these pin is the seam: `POST /specs/compile` is a read of a pure function that stores
 * nothing, and the imposter reaches the fleet only through `POST /imposters` — the same write path,
 * with the same idempotency key, as every other create. A dialog that wrote on Compile, or that
 * created without a key, would pass a test that only checked the result rendered.
 */

const LISTED = {
  "/imposters": { json: { imposters: [] } },
  "/_fleet/members": { status: 404 },
  "/_fleet/health": { status: 404 },
};

const PETSTORE_YAML = `openapi: "3.0.3"
info:
  title: Petstore
  version: "1"
paths:
  /pets:
    get:
      operationId: listPets
      responses:
        "200":
          description: ok
`;

const COMPILED = {
  imposter: {
    port: 4545,
    protocol: "http",
    name: "petstore",
    stubs: [{ responses: [{ is: { statusCode: 200 } }] }, { responses: [] }],
  },
  operations: [
    { id: "listPets", method: "get", pathTemplate: "/pets", stubIds: ["s1"] },
    { id: "createPet", method: "post", pathTemplate: "/pets", stubIds: ["s2"] },
  ],
};

afterEach(() => vi.unstubAllGlobals());

/** Open the dialog, paste the document, fill the port. Stops short of Compile. */
async function openWithDocument(
  user: ReturnType<typeof userEvent.setup>,
  text: string,
  port: string,
): Promise<void> {
  await user.click(await screen.findByTestId("open-openapi-import"));
  await user.click(screen.getByTestId("openapi-text"));
  await user.paste(text);
  await user.type(screen.getByTestId("openapi-port"), port);
}

describe("importing an OpenAPI document", () => {
  it("compiles first and writes nothing until Create imposter is pressed", async () => {
    const { requests } = stubFetch({
      ...LISTED,
      "/specs/compile": { json: COMPILED },
    });
    renderInApp(<Imposters />);
    const user = userEvent.setup();

    await openWithDocument(user, PETSTORE_YAML, "4545");
    await user.type(screen.getByTestId("openapi-name"), "petstore");
    await user.click(screen.getByTestId("openapi-compile"));

    // The compile went out as the document was written, under the media type its bytes are, with
    // the CSRF header the cookie session requires — routed through the shared client, not a bare fetch.
    const compile = await waitFor(() => {
      const found = requests.find((r) => r.path.startsWith("/specs/compile"));
      expect(found).toBeDefined();
      return found;
    });
    expect(compile?.method).toBe("POST");
    expect(compile?.path).toBe("/specs/compile?port=4545&name=petstore");
    expect(compile?.body).toBe(PETSTORE_YAML);
    expect(compile?.headers["content-type"]).toBe("application/yaml");
    expect(compile?.headers["x-rift-csrf"]).toBeTruthy();
    // A compile is not a write: no idempotency key, because the route declares none and there is
    // nothing a key would dedupe.
    expect(compile?.headers["idempotency-key"]).toBeUndefined();

    // The review names what was built — port, stubs, and every operation with method and path.
    const review = await screen.findByTestId("openapi-review");
    expect(within(review).getByTestId("openapi-review-port").textContent).toBe("4545");
    expect(within(review).getByTestId("openapi-review-stubs").textContent).toBe("2");
    const rows = within(screen.getByTestId("openapi-operations")).getAllByRole("row").slice(1);
    expect(rows.map((row) => row.textContent)).toEqual([
      expect.stringMatching(/GET.*\/pets.*listPets.*1/),
      expect.stringMatching(/POST.*\/pets.*createPet.*1/),
    ]);

    // And the fleet has not been written to.
    expect(requests.some((r) => r.path === "/imposters" && r.method === "POST")).toBe(false);
  });

  it("creates the compiled imposter through the ordinary write path, keyed and settled", async () => {
    const { requests } = stubFetch({
      ...LISTED,
      "/specs/compile": { json: COMPILED },
      // Method-keyed: `/imposters` is both the listing this screen reads and the create this
      // dialog writes, and the create is the one being modelled here.
      "POST /imposters": { json: COMPILED.imposter },
    });
    renderInApp(<Imposters />);
    const user = userEvent.setup();

    await openWithDocument(user, JSON.stringify({ openapi: "3.0.3", paths: {} }), "4545");
    await user.click(screen.getByTestId("openapi-compile"));
    await screen.findByTestId("openapi-review");
    await user.click(screen.getByTestId("openapi-create"));

    const create = await waitFor(() => {
      const found = requests.find((r) => r.path === "/imposters" && r.method === "POST");
      expect(found).toBeDefined();
      return found;
    });
    // The body is the compiler's `imposter`, byte for byte what the contract says to send back.
    expect(JSON.parse(String(create?.body))).toEqual(COMPILED.imposter);
    // Same discipline as New imposter: a retry after a lost response must not double-apply.
    expect(create?.headers["idempotency-key"]).toBeTruthy();
    expect(create?.headers["x-rift-csrf"]).toBeTruthy();

    // A pasted JSON document is declared as JSON.
    const compile = requests.find((r) => r.path.startsWith("/specs/compile"));
    expect(compile?.headers["content-type"]).toBe("application/json");

    // Done: the dialog closes on an applied create.
    await waitFor(() => expect(screen.queryByTestId("openapi-import")).toBeNull());
  });

  it("polls a parked create to its commit before saying it is done", async () => {
    // `--cluster-admin-async` answers `202` the moment the write is parked. The dialog must not
    // call that done: `settle()` polls the op ids the body carries until one of them is terminal,
    // and only an `applied` op closes the dialog. Nothing else here exercises `pollCommit`.
    const { requests } = stubFetch({
      ...LISTED,
      "/specs/compile": { json: COMPILED },
      "POST /imposters": { status: 202, json: { opIds: ["op-77"] } },
      "/_fleet/ops/op-77": { json: { opId: "op-77", state: "applied", revision: 42 } },
    });
    renderInApp(<Imposters />);
    const user = userEvent.setup();

    await openWithDocument(user, PETSTORE_YAML, "4545");
    await user.click(screen.getByTestId("openapi-compile"));
    await screen.findByTestId("openapi-review");
    await user.click(screen.getByTestId("openapi-create"));

    // The op the `202` named was actually followed — a dialog that closed without this would be
    // reporting a write it never watched land.
    await waitFor(() =>
      expect(requests.some((r) => r.path === "/_fleet/ops/op-77" && r.method === "GET")).toBe(true),
    );
    await waitFor(() => expect(screen.queryByTestId("openapi-import")).toBeNull());
    expect(screen.queryByTestId("write-unconfirmed")).toBeNull();
  });

  it("says a parked create is unconfirmed when the op cannot be read, and stays open", async () => {
    // `GET /_fleet/ops/{id}` answers `404` for an op this node cannot resolve, and that is not
    // evidence the write did not land. The dialog says "accepted, not yet confirmed" in those
    // words and stays open, rather than toasting a create nobody observed or claiming a failure.
    stubFetch({
      ...LISTED,
      "/specs/compile": { json: COMPILED },
      "POST /imposters": { status: 202, json: { opIds: ["op-88"] } },
      "/_fleet/ops/op-88": { status: 404 },
    });
    renderInApp(<Imposters />);
    const user = userEvent.setup();

    await openWithDocument(user, PETSTORE_YAML, "4545");
    await user.click(screen.getByTestId("openapi-compile"));
    await screen.findByTestId("openapi-review");
    await user.click(screen.getByTestId("openapi-create"));

    const note = await screen.findByTestId("write-unconfirmed");
    expect(note.textContent).toMatch(/accepted, not yet confirmed/i);
    // Still open, and the create button is spent: pressing it again would be a second write for a
    // first one whose outcome is unknown.
    expect(screen.getByTestId("openapi-import")).toBeTruthy();
    expect((screen.getByTestId("openapi-create") as HTMLButtonElement).disabled).toBe(true);
  });

  it("shows the compiler's 400 in its own words and stays on the document", async () => {
    stubFetch({
      ...LISTED,
      "/specs/compile": {
        status: 400,
        // The refusal as the admin plane actually sends one — the declared `Error` envelope, not
        // a bare string. The dialog must show the sentence, not the JSON around it.
        json: {
          errors: [
            {
              code: "400",
              type: "bad_data",
              message: "external $ref not supported: ./common.yaml",
            },
          ],
        },
      },
    });
    renderInApp(<Imposters />);
    const user = userEvent.setup();

    await openWithDocument(user, PETSTORE_YAML, "4545");
    await user.click(screen.getByTestId("openapi-compile"));

    const alert = await screen.findByTestId("openapi-compile-error");
    expect(alert.textContent).toBe("external $ref not supported: ./common.yaml");
    // Unwrapped, not the envelope: no `errors`, no `type`, no braces in front of the diagnosis.
    expect(alert.textContent).not.toMatch(/errors|bad_data|[{}]/);
    // Still on the first step, document intact, so the operator can fix and retry.
    expect(screen.queryByTestId("openapi-review")).toBeNull();
    expect((screen.getByTestId("openapi-text") as HTMLTextAreaElement).value).toBe(PETSTORE_YAML);
  });

  it("names the size cap on a 413", async () => {
    stubFetch({ ...LISTED, "/specs/compile": { status: 413 } });
    renderInApp(<Imposters />);
    const user = userEvent.setup();

    await openWithDocument(user, PETSTORE_YAML, "4545");
    await user.click(screen.getByTestId("openapi-compile"));

    expect((await screen.findByTestId("openapi-compile-error")).textContent).toMatch(/too large/i);
  });

  it("will not compile without a valid port, because the route requires one", async () => {
    const { requests } = stubFetch(LISTED);
    renderInApp(<Imposters />);
    const user = userEvent.setup();

    await user.click(await screen.findByTestId("open-openapi-import"));
    await user.click(screen.getByTestId("openapi-text"));
    await user.paste(PETSTORE_YAML);
    const compileButton = screen.getByTestId("openapi-compile") as HTMLButtonElement;
    expect(compileButton.disabled).toBe(true);

    await user.type(screen.getByTestId("openapi-port"), "70000");
    expect(compileButton.disabled).toBe(true);
    expect(screen.getByTestId("openapi-port-hint").textContent).toMatch(/65535/);

    expect(requests.some((r) => r.path.startsWith("/specs/compile"))).toBe(false);
  });

  it("reads a chosen file and declares it by its extension", async () => {
    const { requests } = stubFetch({ ...LISTED, "/specs/compile": { json: COMPILED } });
    renderInApp(<Imposters />);
    const user = userEvent.setup();

    await user.click(await screen.findByTestId("open-openapi-import"));
    const file = new File([PETSTORE_YAML], "petstore.yml", { type: "" });
    await user.upload(screen.getByTestId("openapi-file"), file);
    await waitFor(() =>
      expect((screen.getByTestId("openapi-text") as HTMLTextAreaElement).value).toBe(PETSTORE_YAML),
    );
    await user.type(screen.getByTestId("openapi-port"), "4545");
    await user.click(screen.getByTestId("openapi-compile"));

    const compile = await waitFor(() => {
      const found = requests.find((r) => r.path.startsWith("/specs/compile"));
      expect(found).toBeDefined();
      return found;
    });
    expect(compile?.headers["content-type"]).toBe("application/yaml");
  });

  it("refuses an oversize file on its size alone, without reading it", async () => {
    // The route caps the body at 4 MiB and `File.size` is that same byte count, known before the
    // file is opened — so the refusal must come first. `text()` is rigged to throw: if the dialog
    // reads before it checks, this fails instead of quietly pulling a huge file into memory.
    const { requests } = stubFetch({ ...LISTED, "/specs/compile": { json: COMPILED } });
    renderInApp(<Imposters />);
    const user = userEvent.setup();

    await user.click(await screen.findByTestId("open-openapi-import"));
    await user.click(screen.getByTestId("openapi-text"));
    await user.paste(PETSTORE_YAML);

    const huge = new File(["x"], "huge.yaml", { type: "" });
    Object.defineProperty(huge, "size", { value: MAX_SPEC_BYTES + 1 });
    Object.defineProperty(huge, "text", {
      value: () => Promise.reject(new Error("must not be read")),
    });
    await user.upload(screen.getByTestId("openapi-file"), huge);

    const refusal = await screen.findByTestId("openapi-file-error");
    expect(refusal.textContent).toMatch(/huge\.yaml/);
    expect(refusal.textContent).toMatch(/4\.0 MiB/);
    // Nothing was sent, and the document already in the box is untouched.
    expect(requests.some((r) => r.path.startsWith("/specs/compile"))).toBe(false);
    expect((screen.getByTestId("openapi-text") as HTMLTextAreaElement).value).toBe(PETSTORE_YAML);
  });

  it("keeps the document it has when a file cannot be read, and says the read failed", async () => {
    // The bug this pins: `setFilename(file.name); setText(await file.text())` set the name first,
    // so a rejected read left the *previous* document on screen under the new file's name — and
    // pressing Compile then sent the old bytes with nothing saying so.
    stubFetch({ ...LISTED, "/specs/compile": { json: COMPILED } });
    renderInApp(<Imposters />);
    const user = userEvent.setup();

    await user.click(await screen.findByTestId("open-openapi-import"));
    await user.click(screen.getByTestId("openapi-text"));
    await user.paste(PETSTORE_YAML);

    const unreadable = new File(["…"], "moved.yaml", { type: "" });
    Object.defineProperty(unreadable, "text", {
      value: () => Promise.reject(new Error("NotReadableError: the file moved")),
    });
    await user.upload(screen.getByTestId("openapi-file"), unreadable);

    const refusal = await screen.findByTestId("openapi-file-error");
    expect(refusal.textContent).toMatch(/moved\.yaml/);
    expect(refusal.textContent).toMatch(/NotReadableError/);
    expect((screen.getByTestId("openapi-text") as HTMLTextAreaElement).value).toBe(PETSTORE_YAML);
  });

  it("says the document is compiled, not stored", async () => {
    stubFetch(LISTED);
    renderInApp(<Imposters />);

    await userEvent.setup().click(await screen.findByTestId("open-openapi-import"));
    // D-72 in the operator's view: the one fact about this flow that is not obvious from the form.
    expect(screen.getByTestId("openapi-import").textContent).toMatch(/compiled, not stored/i);
  });
});
