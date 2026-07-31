import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { App } from "./App.tsx";
import "./styles.css";

const root = document.getElementById("root");
if (!root) {
  // Not a swallow: index.html ships `<div id="root">`, so a missing root means the served HTML is
  // not the one this bundle was built against. Failing loudly beats a blank page with a clean console.
  throw new Error("#root is missing from the served document");
}

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Admin state is edited by other operators and by the fleet itself, so a cached read is
      // stale the moment it lands. C4 tunes this per screen; the default stays honest.
      staleTime: 0,
      retry: 1,
    },
  },
});

createRoot(root).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </StrictMode>,
);
