//! Pull-on-miss: a bounded catch-up wait on an already-failing request
//! (issue #49, #9 deliverable 8).
//!
//! A follower that is behind the leader can be asked for an imposter it has not
//! applied yet. Every other layer already narrows that window — the default
//! `--cluster-write-barrier=ready-nodes` makes a 2xx imply a fleet-wide apply,
//! and the `cluster-reconciled` readiness gate keeps a catching-up node out of
//! rotation — but neither closes it: a node that falls behind *after* going
//! Ready is still in rotation, and its answer would be a no-match.
//!
//! This is the safety net for exactly that case. On a genuine no-match, and
//! only then, it asks whether this node is behind the leader; if it is, it
//! waits briefly for the apply and asks the matcher to try once more.
//!
//! ## Why this is cheap
//!
//! It costs nothing on the hot path by construction: upstream consults the
//! [`NoMatchInterceptor`] seam only after matching has already returned no hit,
//! so a request that matched never reaches this code. The requests it does
//! touch were, without it, about to be answered with a miss anyway.
//!
//! It is not free for them, though, and the upstream seam is explicit about
//! why: `RetryMatch` re-runs the **whole** matching pass, so a predicate
//! `inject` script executes a second time and its persistent `state` mutations
//! and `logger` output are committed twice. The honest worst case for a
//! rescued request is therefore `PULL_ON_MISS_BUDGET` plus a second
//! `scriptEngine.timeoutMs`, not the budget alone — and an imposter whose
//! predicates mutate state through `inject` pays that duplication on every
//! rescue.
//!
//! ## Failing toward today's behaviour
//!
//! Every uncertainty resolves to [`NoMatchDirective::Proceed`]: no bound node,
//! no known leader, an unreachable leader, a budget that ran out before lag was
//! confirmed. It must never make a *working* request slower, and it must never
//! park a request on a leader that cannot answer.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use rift_cluster_base::seams::{NoMatchContext, NoMatchDirective, NoMatchInterceptor, annotate};

use tokio::sync::Mutex;

use crate::metrics;
use crate::raft::RaftNode;

/// The whole hook's wall-clock budget: leader lookup plus local-apply wait,
/// together (#9 deliverable 8). Deliberately not configurable — it bounds how
/// much slower a *failing* request can get, and a knob here would be a knob for
/// making misses arbitrarily slow.
const PULL_ON_MISS_BUDGET: Duration = Duration::from_millis(500);

/// How long a fetched leader target stays fresh.
///
/// Without it, a burst of misses on a lagging follower would fire one leader
/// RPC per request — turning a lag into an RPC storm at precisely the moment
/// the fleet is least able to absorb one.
const TARGET_TTL: Duration = Duration::from_millis(250);

/// The annotation key. `ClusterDecorator` turns this into the response header
/// `rift-cluster-pull-on-miss`, so a rescue is visible to a client and to the
/// integration test without any decorator change.
const ANNOTATION: &str = "cluster.pull_on_miss";

/// What the hook needs to know about the cluster, and nothing more.
///
/// A trait rather than a direct `RaftNode` dependency so the decision table
/// below is testable against a scripted view. The alternative — asserting on a
/// real Raft node — would mean racing an actual apply to observe a 500ms
/// budget, which is how timing-dependent tests become flaky tests.
///
/// Public only because it appears in [`PullOnMissInterceptor`]'s generic
/// parameter; it is an implementation seam, not an extension point, and carries
/// no openraft types across the boundary. Production has exactly one
/// implementation, [`RaftView`].
pub trait ClusterView: Send + Sync {
    /// A leader cannot lag itself.
    fn is_leader(&self) -> bool;
    /// This node's applied index, `None` before anything is applied.
    fn local_applied(&self) -> Option<u64>;
    /// The leader's applied index, or `None` if no leader is known or it could
    /// not be asked.
    fn leader_applied(&self) -> Pin<Box<dyn Future<Output = Option<u64>> + Send + '_>>;
    /// Wait until this node has applied at least `index`. `false` on timeout.
    fn wait_applied(
        &self,
        index: u64,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>>;
}

/// A `Weak<RaftNode>` seen through [`ClusterView`].
///
/// Weak because the hook lives inside the imposter manager, which outlives a
/// shutting-down node. The hazard is not a leak — it is a **delayed `Drop`**:
/// `RaftNode::Drop` is what aborts the server task and releases the redb lock
/// and the cluster port, so a strong handle held across an `.await` postpones
/// teardown by however long that await runs. That regression has already been
/// paid for once (`6c125d7`, which made the intent replayer hand out its
/// `Notify` rather than hold the node across a 250ms wait, after it broke
/// `departed_node_with_retained_state_dir_rejoins_on_restart`).
///
/// So the retention here is stated rather than assumed:
///
/// - [`is_leader`](ClusterView::is_leader) and
///   [`local_applied`](ClusterView::local_applied) upgrade for one synchronous
///   metrics read and drop immediately — the metrics sampler's discipline.
/// - [`wait_applied`](ClusterView::wait_applied) **polls**, re-upgrading each
///   time, so the long wait holds nothing between samples. Event-driven waiting
///   would mean holding the node for the whole wait, which is the bug above.
/// - [`leader_applied`](ClusterView::leader_applied) genuinely needs the node
///   for the duration of one RPC, so it does hold a strong handle — but only
///   for a call bounded by `LEADER_LOOKUP_TIMEOUT`, which is the one retention
///   this type cannot design away.
#[derive(Debug)]
pub struct RaftView(Weak<RaftNode>);

/// How often the catch-up wait re-checks the applied index.
///
/// Polling rather than `raft.wait(..)` is deliberate: see [`RaftView`]. At a
/// 500ms budget this is at most ~50 cheap metrics reads.
const APPLY_POLL: Duration = Duration::from_millis(10);

/// Bound on the one call that must hold the node across an await.
///
/// Without it the leader lookup could outlive the hook's own budget: the outer
/// backstop would cancel the future, but the RPC would already have held a
/// strong handle for however long it took.
const LEADER_LOOKUP_TIMEOUT: Duration = Duration::from_millis(250);

impl ClusterView for RaftView {
    fn is_leader(&self) -> bool {
        self.0.upgrade().is_some_and(|n| n.status().is_leader)
    }

    fn local_applied(&self) -> Option<u64> {
        self.0.upgrade()?.status().last_applied
    }

    fn leader_applied(&self) -> Pin<Box<dyn Future<Output = Option<u64>> + Send + '_>> {
        Box::pin(async move {
            let node = self.0.upgrade()?;
            tokio::time::timeout(LEADER_LOOKUP_TIMEOUT, node.leader_applied())
                .await
                .ok()
                .flatten()
        })
    }

    fn wait_applied(
        &self,
        index: u64,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        Box::pin(async move {
            let deadline = Instant::now() + timeout;
            loop {
                // Upgrade, read, drop -- all synchronous. Nothing is held
                // across the sleep below.
                match self.0.upgrade() {
                    Some(node) => {
                        if node.status().last_applied.unwrap_or(0) >= index {
                            return true;
                        }
                    }
                    // The node is gone; there is nothing left to catch up to.
                    None => return false,
                }
                if Instant::now() >= deadline {
                    return false;
                }
                tokio::time::sleep(APPLY_POLL.min(deadline - Instant::now())).await;
            }
        })
    }
}

/// The interceptor wired into the clustered imposter manager.
#[derive(Debug)]
pub struct PullOnMissInterceptor<V: ClusterView = RaftView> {
    /// Late-bound: the manager is constructed before the `RaftNode` exists, the
    /// same ordering problem `NodeSlot` solves for the operator surface. Before
    /// [`PullOnMissInterceptor::bind`], every request simply proceeds.
    view: OnceLock<V>,
    /// `(fetched_at, leader_applied)` — the burst cache guarded by `TARGET_TTL`.
    ///
    /// A `tokio` mutex rather than a `std` one because it is deliberately held
    /// **across the leader RPC**: that is what makes the cache single-flight.
    /// With a std mutex the lock could only cover the read and the write, so N
    /// concurrent misses would all find it empty and all fire their own RPC —
    /// one lookup per request in exactly the burst the cache exists to damp.
    target: Mutex<Option<(Instant, u64)>>,
}

impl PullOnMissInterceptor<RaftView> {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            view: OnceLock::new(),
            target: Mutex::new(None),
        })
    }

    /// Attach the node, once it exists. Binding twice is a no-op rather than an
    /// error: the second caller wanted the same thing the first one got.
    pub fn bind(&self, node: &Arc<RaftNode>) {
        let _ = self.view.set(RaftView(Arc::downgrade(node)));
    }
}

impl<V: ClusterView> PullOnMissInterceptor<V> {
    /// The leader's applied index, from cache when it is fresh enough.
    ///
    /// Single-flight: the lock is held across the fetch, so concurrent missers
    /// queue behind the first one and then read its result rather than each
    /// issuing their own lookup.
    async fn target(&self, view: &V) -> Option<u64> {
        let mut guard = self.target.lock().await;
        if let Some((fetched_at, target)) = *guard
            && fetched_at.elapsed() < TARGET_TTL
        {
            return Some(target);
        }
        let target = view.leader_applied().await?;
        *guard = Some((Instant::now(), target));
        Some(target)
    }

    /// The decision, minus the outer budget. Split out so the table is one
    /// readable function and so tests can drive it directly.
    async fn decide(&self, deadline: Instant) -> NoMatchDirective {
        // Not yet bound: the node is still coming up. Nothing to compare
        // against, so behave exactly as if the hook were not installed.
        let Some(view) = self.view.get() else {
            return NoMatchDirective::Proceed;
        };
        // A leader is by definition not behind the leader.
        if view.is_leader() {
            return NoMatchDirective::Proceed;
        }

        metrics::pull_on_miss_check();

        let Some(target) = self.target(view).await else {
            // No leader known, or it could not be asked. Never park a request
            // on an unreachable leader.
            return NoMatchDirective::Proceed;
        };
        let local = view.local_applied().unwrap_or(0);
        if local >= target {
            // Up to date. The miss is a real miss: this port genuinely has no
            // matching stub, and waiting would only make a correct answer slow.
            return NoMatchDirective::Proceed;
        }

        metrics::pull_on_miss_lagging();
        let remaining = deadline.saturating_duration_since(Instant::now());
        let caught_up = view.wait_applied(target, remaining).await;

        // Retry either way. The wait timing out does not mean nothing applied —
        // it means the whole gap did not close — and the retry costs one match
        // pass on a request that was already going to miss.
        annotate(
            ANNOTATION,
            if caught_up {
                "rescued-wait".to_owned()
            } else {
                "retry-after-timeout".to_owned()
            },
        );
        metrics::pull_on_miss_retry();
        NoMatchDirective::RetryMatch
    }
}

impl<V: ClusterView + 'static> NoMatchInterceptor for PullOnMissInterceptor<V> {
    fn on_no_match<'a>(
        &'a self,
        _ctx: NoMatchContext<'a>,
    ) -> Pin<Box<dyn Future<Output = NoMatchDirective> + Send + 'a>> {
        Box::pin(async move {
            let deadline = Instant::now() + PULL_ON_MISS_BUDGET;
            // The backstop, not the mechanism: `decide` bounds its own wait by
            // the remaining budget. If the budget runs out anyway -- a leader
            // RPC that hangs past it -- we have not confirmed lag, so the
            // honest answer is the one the fleet would have given without us.
            match tokio::time::timeout(PULL_ON_MISS_BUDGET, self.decide(deadline)).await {
                Ok(directive) => directive,
                Err(_) => NoMatchDirective::Proceed,
            }
        })
    }
}

/// What these cover, and what they deliberately do not.
///
/// Everything below drives the decision table through a scripted
/// [`ClusterView`], which is the right shape for branch coverage and the wrong
/// shape for proving the hook is *reached*: a fake view cannot tell you the
/// interceptor was built, bound to the node, and consulted by the serve loop.
/// That wiring is covered end to end by the container tier's
/// `c16_pull_on_miss_rescues_lagging_follower` (#102), which slows a real
/// follower's cluster link and asserts on the `rift-cluster-pull-on-miss`
/// response header — the rescue's only evidence, which is also why there is no
/// rescue counter (see `metrics.rs`).
///
/// The in-process two-node variant #49's acceptance criteria asked for stays
/// unwritten on purpose: making a follower lag in that harness means racing the
/// apply, and a timing-raced assertion is the exact shape the chaos tier's flake
/// policy exists to keep out of the tree.
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A scripted cluster view. Every field is what the decision table branches
    /// on, so a test states a situation rather than staging a real cluster.
    struct FakeView {
        is_leader: bool,
        local: Option<u64>,
        leader: Option<u64>,
        wait_succeeds: bool,
        leader_calls: AtomicUsize,
        wait_calls: AtomicUsize,
    }

    impl FakeView {
        fn lagging() -> Self {
            Self {
                is_leader: false,
                local: Some(5),
                leader: Some(9),
                wait_succeeds: true,
                leader_calls: AtomicUsize::new(0),
                wait_calls: AtomicUsize::new(0),
            }
        }
    }

    impl ClusterView for FakeView {
        fn is_leader(&self) -> bool {
            self.is_leader
        }
        fn local_applied(&self) -> Option<u64> {
            self.local
        }
        fn leader_applied(&self) -> Pin<Box<dyn Future<Output = Option<u64>> + Send + '_>> {
            self.leader_calls.fetch_add(1, Ordering::SeqCst);
            let leader = self.leader;
            Box::pin(async move { leader })
        }
        fn wait_applied(
            &self,
            _index: u64,
            _timeout: Duration,
        ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
            self.wait_calls.fetch_add(1, Ordering::SeqCst);
            let ok = self.wait_succeeds;
            Box::pin(async move { ok })
        }
    }

    fn with(view: FakeView) -> PullOnMissInterceptor<FakeView> {
        let hook = PullOnMissInterceptor {
            view: OnceLock::new(),
            target: Mutex::new(None),
        };
        let _ = hook.view.set(view);
        hook
    }

    fn deadline() -> Instant {
        Instant::now() + PULL_ON_MISS_BUDGET
    }

    #[tokio::test]
    async fn unbound_node_proceeds() {
        let hook: PullOnMissInterceptor<FakeView> = PullOnMissInterceptor {
            view: OnceLock::new(),
            target: Mutex::new(None),
        };
        assert_eq!(hook.decide(deadline()).await, NoMatchDirective::Proceed);
    }

    #[tokio::test]
    async fn a_leader_never_waits_on_itself() {
        let hook = with(FakeView {
            is_leader: true,
            ..FakeView::lagging()
        });
        assert_eq!(hook.decide(deadline()).await, NoMatchDirective::Proceed);
        assert_eq!(
            hook.view.get().unwrap().leader_calls.load(Ordering::SeqCst),
            0,
            "a leader must not even ask for the target"
        );
    }

    #[tokio::test]
    async fn an_unknown_leader_proceeds_rather_than_parking() {
        let hook = with(FakeView {
            leader: None,
            ..FakeView::lagging()
        });
        assert_eq!(hook.decide(deadline()).await, NoMatchDirective::Proceed);
        assert_eq!(
            hook.view.get().unwrap().wait_calls.load(Ordering::SeqCst),
            0,
            "with no target there is nothing to wait for"
        );
    }

    #[tokio::test]
    async fn a_caught_up_node_treats_the_miss_as_real() {
        let hook = with(FakeView {
            local: Some(9),
            leader: Some(9),
            ..FakeView::lagging()
        });
        assert_eq!(hook.decide(deadline()).await, NoMatchDirective::Proceed);
        assert_eq!(
            hook.view.get().unwrap().wait_calls.load(Ordering::SeqCst),
            0,
            "an up-to-date node must not delay a genuine miss"
        );
    }

    #[tokio::test]
    async fn a_node_that_has_applied_nothing_still_counts_as_lagging() {
        let hook = with(FakeView {
            local: None,
            ..FakeView::lagging()
        });
        assert_eq!(hook.decide(deadline()).await, NoMatchDirective::RetryMatch);
    }

    #[tokio::test]
    async fn a_lagging_node_waits_then_retries() {
        let hook = with(FakeView::lagging());
        assert_eq!(hook.decide(deadline()).await, NoMatchDirective::RetryMatch);
        assert_eq!(
            hook.view.get().unwrap().wait_calls.load(Ordering::SeqCst),
            1
        );
    }

    /// The retry happens even when the wait ran out — the point the design
    /// makes explicitly, because a partial catch-up can still turn the miss
    /// into a hit and the request was failing regardless.
    #[tokio::test]
    async fn a_timed_out_wait_still_retries() {
        let hook = with(FakeView {
            wait_succeeds: false,
            ..FakeView::lagging()
        });
        assert_eq!(hook.decide(deadline()).await, NoMatchDirective::RetryMatch);
    }

    /// A burst of misses must not become a burst of leader RPCs — and the
    /// requests must be genuinely **concurrent**, because that is the only
    /// regime where the claim is non-trivial.
    ///
    /// A sequential loop would pass against a cache with no single-flight at
    /// all: each call would find the previous call's value already stored. The
    /// interesting case is N callers arriving while the first lookup is still
    /// in flight, which is what a lagging follower under load actually does.
    #[tokio::test]
    async fn a_concurrent_burst_costs_one_leader_lookup() {
        struct SlowLeader {
            calls: AtomicUsize,
        }
        impl ClusterView for SlowLeader {
            fn is_leader(&self) -> bool {
                false
            }
            fn local_applied(&self) -> Option<u64> {
                Some(1)
            }
            fn leader_applied(&self) -> Pin<Box<dyn Future<Output = Option<u64>> + Send + '_>> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {
                    // Long enough that every other caller is waiting by the
                    // time this resolves.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Some(9)
                })
            }
            fn wait_applied(
                &self,
                _index: u64,
                _timeout: Duration,
            ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
                Box::pin(async { true })
            }
        }

        let hook = Arc::new(PullOnMissInterceptor {
            view: OnceLock::new(),
            target: Mutex::new(None),
        });
        let _ = hook.view.set(SlowLeader {
            calls: AtomicUsize::new(0),
        });

        let mut set = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let hook = Arc::clone(&hook);
            set.spawn(async move { hook.decide(Instant::now() + PULL_ON_MISS_BUDGET).await });
        }
        while let Some(joined) = set.join_next().await {
            assert_eq!(joined.expect("task ran"), NoMatchDirective::RetryMatch);
        }

        assert_eq!(
            hook.view.get().unwrap().calls.load(Ordering::SeqCst),
            1,
            "eight concurrent misses must share one leader lookup"
        );
    }

    /// A node whose `Weak` no longer upgrades proceeds instead of waiting.
    #[tokio::test]
    async fn a_departed_node_proceeds() {
        let node_view = {
            // A `RaftView` whose target is already gone: `Weak::new()` never
            // upgrades, which is exactly the post-shutdown state.
            RaftView(Weak::new())
        };
        assert!(!node_view.is_leader());
        assert_eq!(node_view.local_applied(), None);
        assert_eq!(node_view.leader_applied().await, None);
        assert!(
            !node_view.wait_applied(1, Duration::from_millis(5)).await,
            "there is nothing left to catch up to once the node is gone"
        );
    }

    /// `on_no_match` — the entry point the seam actually calls, including the
    /// deadline it computes and the outer backstop. Every other test drives
    /// `decide` directly, so without this the public path has no coverage.
    #[tokio::test]
    async fn on_no_match_retries_a_lagging_node() {
        let hook = with(FakeView::lagging());
        let ctx = NoMatchContext {
            port: 6001,
            method: "GET",
            path: "/anything",
        };
        assert_eq!(hook.on_no_match(ctx).await, NoMatchDirective::RetryMatch);
    }

    /// The backstop returns `Proceed` rather than hanging when the inner
    /// decision overruns the budget.
    #[tokio::test]
    async fn on_no_match_backstops_a_hanging_lookup() {
        struct Hangs;
        impl ClusterView for Hangs {
            fn is_leader(&self) -> bool {
                false
            }
            fn local_applied(&self) -> Option<u64> {
                Some(1)
            }
            fn leader_applied(&self) -> Pin<Box<dyn Future<Output = Option<u64>> + Send + '_>> {
                Box::pin(async {
                    // Longer than the budget, and never cancelled from inside.
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Some(9)
                })
            }
            fn wait_applied(
                &self,
                _index: u64,
                _timeout: Duration,
            ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
                Box::pin(async { true })
            }
        }
        let hook = PullOnMissInterceptor {
            view: OnceLock::new(),
            target: Mutex::new(None),
        };
        let _ = hook.view.set(Hangs);
        let ctx = NoMatchContext {
            port: 6001,
            method: "GET",
            path: "/anything",
        };
        let started = Instant::now();
        assert_eq!(hook.on_no_match(ctx).await, NoMatchDirective::Proceed);
        assert!(
            started.elapsed() < PULL_ON_MISS_BUDGET * 3,
            "the backstop must cap a hanging lookup, not wait it out"
        );
    }

    /// ...but the cache must expire    /// ...but the cache must expire, or a follower that caught up would keep
    /// retrying against a stale target forever.
    #[tokio::test]
    async fn the_cached_target_expires() {
        let hook = with(FakeView::lagging());
        let _ = hook.decide(deadline()).await;
        // Age the entry past the TTL rather than sleeping for it.
        {
            let mut guard = hook.target.lock().await;
            if let Some((_, target)) = *guard {
                *guard = Some((
                    Instant::now() - TARGET_TTL - Duration::from_millis(1),
                    target,
                ));
            }
        }
        let _ = hook.decide(deadline()).await;
        assert_eq!(
            hook.view.get().unwrap().leader_calls.load(Ordering::SeqCst),
            2,
            "a target older than TARGET_TTL must be re-fetched"
        );
    }

    /// The budget is spent, not ignored: a `decide` starting from an expired
    /// deadline asks for a zero-length wait rather than a full-length one.
    #[tokio::test]
    async fn an_exhausted_budget_does_not_extend_the_wait() {
        struct Recording(std::sync::Mutex<Option<Duration>>);
        impl ClusterView for Recording {
            fn is_leader(&self) -> bool {
                false
            }
            fn local_applied(&self) -> Option<u64> {
                Some(1)
            }
            fn leader_applied(&self) -> Pin<Box<dyn Future<Output = Option<u64>> + Send + '_>> {
                Box::pin(async { Some(9) })
            }
            fn wait_applied(
                &self,
                _index: u64,
                timeout: Duration,
            ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
                *self.0.lock().expect("record the timeout") = Some(timeout);
                Box::pin(async { false })
            }
        }
        let hook = PullOnMissInterceptor {
            view: OnceLock::new(),
            target: Mutex::new(None),
        };
        let _ = hook.view.set(Recording(std::sync::Mutex::new(None)));
        let past = Instant::now() - Duration::from_secs(1);
        let _ = hook.decide(past).await;
        let recorded = *hook.view.get().unwrap().0.lock().expect("read it");
        assert_eq!(
            recorded,
            Some(Duration::ZERO),
            "an expired budget must not hand the wait a fresh one"
        );
    }
}
