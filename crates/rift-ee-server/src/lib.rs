//! The Rift Enterprise server, as a library.
//!
//! The binary is a thin caller over this crate — the same split the open-source
//! `rift` binary uses — so the composition can be driven in-process by tests and
//! by the container chaos harness without spawning a process and scraping stdout.
//!
//! The shape of the thing:
//!
//! * [`cli`] — the open-source CLI flattened into a `--cluster*` superset, plus
//!   the startup guards that refuse a fleet which would be quietly wrong.
//! * [`compose`] — the open-source [`ServerBuilder`](rift_ee::seams::ServerBuilder)
//!   wired to the cluster backends. With `--cluster` off it adds nothing at all.
//! * [`readiness`] — the startup latch behind `/readyz`, closed until every
//!   registered gate reports in.
//! * [`probes`] — the unauthenticated `/readyz` + `/healthz` listener.
//! * [`cluster_api`] — the authenticated `/_cluster/*` operator surface on the
//!   cluster port.

pub mod cli;
pub mod cluster_api;
pub mod compose;
pub mod probes;
pub mod readiness;

pub use cli::EeCli;
pub use compose::ComposedServer;
