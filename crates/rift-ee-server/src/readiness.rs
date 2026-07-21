//! The startup readiness latch behind `GET /readyz` (RFC-001 §11.1).
//!
//! Readiness is the load balancer's gate, and it answers a different question
//! from liveness: a node that is up but has not finished converging must not
//! receive traffic, yet restarting it would only delay the convergence. So the
//! latch is *closed until proven open* — it is constructed with the set of gates
//! this node is waiting on, and only reports ready once every one of them has
//! reported in.
//!
//! Each startup concern registers its own gate name, so adding one is additive
//! and a gate that is never satisfied fails visibly (the probe names what is
//! still pending) rather than by a node silently taking traffic early.

use std::sync::atomic::{AtomicBool, Ordering};

/// The node has joined the control plane and knows a leader.
pub const GATE_JOINED: &str = "cluster-joined";

/// What `/readyz` reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyState {
    /// Every gate has reported in; the balancer may send traffic.
    Ready,
    /// At least one gate is outstanding.
    NotReady,
    /// A graceful leave has begun; the node is finishing in-flight work.
    Draining,
}

impl ReadyState {
    /// The wire spelling reported to operators and probes.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::NotReady => "not-ready",
            Self::Draining => "draining",
        }
    }

    /// Whether a load balancer should route to this node.
    #[must_use]
    pub fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }
}

struct Gate {
    name: &'static str,
    satisfied: AtomicBool,
}

/// The set of startup gates a node is waiting on.
pub struct Readiness {
    gates: Vec<Gate>,
    draining: AtomicBool,
}

impl Readiness {
    /// A latch closed until each of `gates` is satisfied. An empty set is ready
    /// immediately — that is the un-clustered node, which has nothing to wait for.
    #[must_use]
    pub fn awaiting(gates: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            gates: gates
                .into_iter()
                .map(|name| Gate {
                    name,
                    satisfied: AtomicBool::new(false),
                })
                .collect(),
            draining: AtomicBool::new(false),
        }
    }

    /// Record that `gate` has reported in. Idempotent.
    ///
    /// A name that was never registered is a wiring bug — the gate it was meant
    /// to open is still holding the node closed — so it is reported rather than
    /// dropped.
    pub fn satisfy(&self, gate: &str) {
        match self.gates.iter().find(|g| g.name == gate) {
            Some(found) => found.satisfied.store(true, Ordering::Release),
            None => tracing::error!(
                gate,
                registered = ?self.gates.iter().map(|g| g.name).collect::<Vec<_>>(),
                "readiness gate satisfied but never registered"
            ),
        }
    }

    /// Begin a graceful leave: report not-ready from now on so the balancer
    /// sheds this node before any socket closes.
    ///
    /// Draining is terminal. A gate reporting in afterwards must not re-open the
    /// balancer onto a node that is on its way out.
    pub fn start_draining(&self) {
        self.draining.store(true, Ordering::Release);
    }

    /// Whether a graceful leave has begun.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    /// The gates still outstanding, in registration order.
    #[must_use]
    pub fn pending(&self) -> Vec<&'static str> {
        self.gates
            .iter()
            .filter(|g| !g.satisfied.load(Ordering::Acquire))
            .map(|g| g.name)
            .collect()
    }

    /// The current state.
    #[must_use]
    pub fn state(&self) -> ReadyState {
        if self.is_draining() {
            ReadyState::Draining
        } else if self.pending().is_empty() {
            ReadyState::Ready
        } else {
            ReadyState::NotReady
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_gates_is_ready_immediately() {
        let readiness = Readiness::awaiting([]);
        assert_eq!(readiness.state(), ReadyState::Ready);
        assert!(readiness.pending().is_empty());
    }

    #[test]
    fn every_gate_must_report_before_ready() {
        let readiness = Readiness::awaiting([GATE_JOINED, "second"]);
        assert_eq!(readiness.state(), ReadyState::NotReady);
        assert_eq!(readiness.pending(), [GATE_JOINED, "second"]);

        readiness.satisfy("second");
        assert_eq!(readiness.state(), ReadyState::NotReady);
        assert_eq!(readiness.pending(), [GATE_JOINED]);

        readiness.satisfy(GATE_JOINED);
        assert_eq!(readiness.state(), ReadyState::Ready);
    }

    #[test]
    fn satisfying_a_gate_twice_is_harmless() {
        let readiness = Readiness::awaiting([GATE_JOINED]);
        readiness.satisfy(GATE_JOINED);
        readiness.satisfy(GATE_JOINED);
        assert_eq!(readiness.state(), ReadyState::Ready);
    }

    #[test]
    fn an_unregistered_gate_does_not_open_the_latch() {
        let readiness = Readiness::awaiting([GATE_JOINED]);
        readiness.satisfy("typo-in-the-gate-name");
        assert_eq!(readiness.state(), ReadyState::NotReady);
        assert_eq!(readiness.pending(), [GATE_JOINED]);
    }

    #[test]
    fn draining_is_terminal() {
        let readiness = Readiness::awaiting([GATE_JOINED]);
        readiness.satisfy(GATE_JOINED);
        assert_eq!(readiness.state(), ReadyState::Ready);

        readiness.start_draining();
        assert_eq!(readiness.state(), ReadyState::Draining);
        assert!(!readiness.state().is_ready());

        readiness.satisfy(GATE_JOINED);
        assert_eq!(readiness.state(), ReadyState::Draining);
    }
}
