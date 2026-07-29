//! The Rift Enterprise server, as a library.
//!
//! The binary is a thin caller over this crate — the same split the open-source
//! `rift` binary uses — so the composition can be driven in-process by tests and
//! by the container chaos harness without spawning a process and scraping stdout.
//!
//! The shape of the thing:
//!
//! * [`bootstrap`] — the pre-serve steps shared with the open-source binary:
//!   rcfile defaults, the PID file, and the `stop`/`restart`/`save` subcommands.
//! * [`cli`] — the open-source CLI flattened into a `--cluster*` superset, plus
//!   the startup guards that refuse a fleet which would be quietly wrong.
//! * [`compose`] — the open-source [`ServerBuilder`](rift_ee::seams::ServerBuilder)
//!   wired to the cluster backends. With `--cluster` off it adds nothing at all.
//! * [`readiness`] — the startup latch behind `/readyz`, closed until every
//!   registered gate reports in.
//! * [`probes`] — the unauthenticated `/readyz` + `/healthz` listener.
//! * [`cluster_api`] — the authenticated `/_cluster/*` operator surface on the
//!   cluster port.
//! * `tenancy` — RFC-002 §5's `/admin/tenants*` and `/admin/whoami` surface,
//!   terminated by [`admin_front`].

pub mod admin_front;
pub mod authorizer;
pub mod authz;
pub mod bootstrap;
pub mod cli;
pub mod cluster_api;
pub mod compose;
pub mod principal;
pub mod probes;
pub mod readiness;
mod tenancy;

pub use cli::EeCli;
pub use compose::ComposedServer;
