//! Audit export (issue #164): shipping committed audit rows to a
//! customer-owned sink.
//!
//! Two layers. [`sink`] is the transport: build a [`sink::SinkTransport`] for a
//! sink URI and ship one already-serialized batch over it. [`exporter`] is the
//! leader-only loop above it that decides *what* to ship and *when*, and
//! records how far it got — see that module for the at-least-once reasoning.

pub mod exporter;
pub mod sink;

pub use exporter::{AuditExporter, ExportContext, ExportStatus, ExportStatusSnapshot};
pub use sink::{Shipped, SinkTransport};
