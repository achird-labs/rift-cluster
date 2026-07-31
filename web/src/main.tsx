import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClientProvider } from "@tanstack/react-query";

import { App } from "./App.tsx";
import { createQueryClient } from "./app/query.ts";
import "./styles.css";

const root = document.getElementById("root");
if (!root) {
  // Not a swallow: index.html ships `<div id="root">`, so a missing root means the served HTML is
  // not the one this bundle was built against. Failing loudly beats a blank page with a clean console.
  throw new Error("#root is missing from the served document");
}

// The same factory the tests render through, so a polling or retry setting cannot be true of the
// suite and false of the shipped bundle.
createRoot(root).render(
  <StrictMode>
    <QueryClientProvider client={createQueryClient()}>
      <App />
    </QueryClientProvider>
  </StrictMode>,
);
