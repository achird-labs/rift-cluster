//! The sync→async bridge and the private cluster-io runtime (RFC-001 §7.7).
//!
//! The open-source engine's state seams are synchronous — they are called from
//! async request handlers *and* from the blocking script-pool threads — while
//! clustered implementations perform network RPC. Rather than async-ifying
//! those traits upstream (which would ripple through both scripting engines and
//! every call site), the enterprise side owns a small runtime and parks the
//! calling thread on a channel.
//!
//! Two properties make that safe:
//!
//! * **Parked callers are bounded.** Data-plane worker threads are the scarce
//!   resource, so their permits are capped at `max(2, workers/2)`: at least half
//!   the workers stay available to the stateless hot path even while an owner is
//!   black-holing. Script-pool threads are dedicated and blocking already, so
//!   they draw on a separate, larger pool and cannot starve the data plane.
//! * **The wait graph is acyclic.** Callers never execute cluster-io work and
//!   cluster-io never calls back into the data plane synchronously.

use std::future::Future;
use std::sync::Arc;
use std::sync::mpsc::{RecvTimeoutError, sync_channel};
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::metrics;
use crate::rpc::RpcError;

/// Which permit pool a caller draws from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallerClass {
    /// A tokio worker serving a data-plane request. Scarce: capped so the
    /// stateless path always has threads.
    DataPlane,
    /// A dedicated script-pool thread. Blocking there is already the norm.
    ScriptPool,
}

/// Bridge sizing.
#[derive(Debug, Clone, Copy)]
pub struct BridgeConfig {
    pub data_plane_permits: usize,
    pub script_pool_permits: usize,
    /// Worker threads for the private cluster-io runtime.
    pub io_threads: usize,
}

impl BridgeConfig {
    /// Derive sizing from the data plane's worker count.
    #[must_use]
    pub fn for_workers(worker_threads: usize) -> Self {
        Self {
            // Half the workers, floor 2: an owner outage may stall this many
            // threads for one deadline, and no more.
            data_plane_permits: (worker_threads / 2).max(2),
            // Script threads block by design, so they get room to queue.
            script_pool_permits: (worker_threads * 4).max(16),
            io_threads: 2,
        }
    }
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self::for_workers(std::thread::available_parallelism().map_or(4, std::num::NonZero::get))
    }
}

/// Owns the cluster-io runtime and the permit pools.
pub struct Bridge {
    runtime: tokio::runtime::Runtime,
    data_plane: Arc<Semaphore>,
    script_pool: Arc<Semaphore>,
}

impl Bridge {
    /// Start the cluster-io runtime.
    pub fn start(config: BridgeConfig) -> std::io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.io_threads)
            .thread_name("cluster-io")
            .enable_all()
            .build()?;
        Ok(Self {
            runtime,
            data_plane: Arc::new(Semaphore::new(config.data_plane_permits)),
            script_pool: Arc::new(Semaphore::new(config.script_pool_permits)),
        })
    }

    /// Handle for spawning onto cluster-io directly (servers, background tasks).
    #[must_use]
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.runtime.handle().clone()
    }

    fn permits(&self, class: CallerClass) -> &Arc<Semaphore> {
        match class {
            CallerClass::DataPlane => &self.data_plane,
            CallerClass::ScriptPool => &self.script_pool,
        }
    }

    /// Free permits in a class — the gauge behind `rift_cluster_bridge_inflight`.
    #[must_use]
    pub fn available_permits(&self, class: CallerClass) -> usize {
        self.permits(class).available_permits()
    }

    /// Run `op` on cluster-io and block the calling thread until it resolves or
    /// `deadline` elapses.
    ///
    /// Parks on `std::sync::mpsc` rather than a tokio channel deliberately:
    /// `blocking_recv` panics when called from inside an async context, and this
    /// is reached from both async handlers and blocking script threads.
    pub fn call<F, T>(&self, class: CallerClass, deadline: Duration, op: F) -> Result<T, RpcError>
    where
        F: Future<Output = Result<T, RpcError>> + Send + 'static,
        T: Send + 'static,
    {
        let Ok(permit) = Arc::clone(self.permits(class)).try_acquire_owned() else {
            // Shed immediately: queueing here would convert an owner outage
            // into data-plane thread exhaustion, which is the failure this
            // bound exists to prevent.
            metrics::bridge_rejected();
            return Err(RpcError::Shed);
        };
        metrics::bridge_inflight_inc();

        // Capacity 1 and a matching receiver: the sender never blocks, so a
        // caller that has already timed out cannot wedge the cluster-io task.
        let (tx, rx) = sync_channel::<Result<T, RpcError>>(1);
        self.runtime.spawn(async move {
            // The deadline bounds the *permit*, not just the caller. Releasing
            // it only when `op` finishes would let a slow peer hold data-plane
            // capacity far longer than anyone waited for it — an op that
            // retries internally can outlive its caller by many seconds — and
            // the pool would sit empty while every new call is shed, which is
            // precisely the collapse the permit bound exists to prevent.
            let outcome = match tokio::time::timeout(deadline, op).await {
                Ok(outcome) => outcome,
                Err(_) => Err(RpcError::Timeout),
            };
            // Release the permit *before* handing the result back, not after:
            // the op is done, so its capacity is free now, and dropping first
            // establishes a happens-before so a caller that has received its
            // result also sees the permit returned (a caller that timed out is
            // already gone, and the op simply finishes and frees the permit
            // whenever it completes — the slow-op capacity bound is unchanged).
            drop(permit);
            let _ = tx.send(outcome);
        });

        let result = match rx.recv_timeout(deadline) {
            Ok(outcome) => outcome,
            Err(RecvTimeoutError::Timeout) => Err(RpcError::Timeout),
            // The task was dropped without sending — treat as transport loss
            // rather than panicking a request thread.
            Err(RecvTimeoutError::Disconnected) => Err(RpcError::Transport(
                "cluster-io task ended without a result".into(),
            )),
        };
        metrics::bridge_inflight_dec();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bridge(data_plane_permits: usize) -> Bridge {
        Bridge::start(BridgeConfig {
            data_plane_permits,
            script_pool_permits: 16,
            io_threads: 2,
        })
        .expect("cluster-io runtime starts")
    }

    #[test]
    fn bridge_round_trip_from_sync_caller() {
        let bridge = bridge(2);
        let out = bridge
            .call(CallerClass::DataPlane, Duration::from_secs(5), async {
                Ok(41 + 1)
            })
            .expect("op resolves");
        assert_eq!(out, 42);
    }

    #[test]
    fn bridge_propagates_op_errors() {
        let bridge = bridge(2);
        let err = bridge
            .call(CallerClass::DataPlane, Duration::from_secs(5), async {
                Err::<(), _>(RpcError::Handler("upstream said no".into()))
            })
            .unwrap_err();
        assert!(matches!(err, RpcError::Handler(m) if m == "upstream said no"));
    }

    #[test]
    fn bridge_times_out_without_hanging() {
        let bridge = bridge(2);
        let started = std::time::Instant::now();
        let err = bridge
            .call(CallerClass::DataPlane, Duration::from_millis(50), async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(err, RpcError::Timeout);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "did not honour the deadline"
        );
    }

    #[test]
    fn bridge_sheds_beyond_permit_bound() {
        let bridge = Arc::new(bridge(1));
        let (release_tx, release_rx) = sync_channel::<()>(0);
        let release_rx = Arc::new(parking_lot::Mutex::new(release_rx));

        // Occupy the single data-plane permit with an op that will not finish
        // until the test lets it.
        let holder = {
            let bridge = Arc::clone(&bridge);
            let release_rx = Arc::clone(&release_rx);
            std::thread::spawn(move || {
                bridge.call(
                    CallerClass::DataPlane,
                    Duration::from_secs(10),
                    async move {
                        tokio::task::spawn_blocking(move || {
                            let _ = release_rx.lock().recv();
                        })
                        .await
                        .map_err(|e| RpcError::Transport(e.to_string()))
                    },
                )
            })
        };

        // Wait for the permit to actually be taken before asserting the shed.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while bridge.available_permits(CallerClass::DataPlane) > 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "permit was never acquired"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        let started = std::time::Instant::now();
        let err = bridge
            .call(CallerClass::DataPlane, Duration::from_secs(10), async {
                Ok(())
            })
            .unwrap_err();
        assert_eq!(err, RpcError::Shed);
        // Shedding must be immediate — the point is not to park the caller.
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "shed was not immediate"
        );

        drop(release_tx);
        holder
            .join()
            .expect("holder thread joins")
            .expect("holder op resolves");
    }

    #[test]
    fn bridge_script_pool_isolated_from_data_plane() {
        let bridge = Arc::new(
            Bridge::start(BridgeConfig {
                data_plane_permits: 1,
                script_pool_permits: 4,
                io_threads: 2,
            })
            .expect("cluster-io runtime starts"),
        );
        let (release_tx, release_rx) = sync_channel::<()>(0);
        let release_rx = Arc::new(parking_lot::Mutex::new(release_rx));

        let holder = {
            let bridge = Arc::clone(&bridge);
            let release_rx = Arc::clone(&release_rx);
            std::thread::spawn(move || {
                bridge.call(
                    CallerClass::DataPlane,
                    Duration::from_secs(10),
                    async move {
                        tokio::task::spawn_blocking(move || {
                            let _ = release_rx.lock().recv();
                        })
                        .await
                        .map_err(|e| RpcError::Transport(e.to_string()))
                    },
                )
            })
        };

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while bridge.available_permits(CallerClass::DataPlane) > 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "permit was never acquired"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        // The data plane is saturated; a script-pool caller still gets through.
        let out = bridge
            .call(CallerClass::ScriptPool, Duration::from_secs(5), async {
                Ok(7)
            })
            .expect("script pool has its own permits");
        assert_eq!(out, 7);

        drop(release_tx);
        holder
            .join()
            .expect("holder thread joins")
            .expect("holder op resolves");
    }

    #[test]
    fn permit_sizing_keeps_half_the_data_plane_free() {
        assert_eq!(BridgeConfig::for_workers(16).data_plane_permits, 8);
        assert_eq!(BridgeConfig::for_workers(8).data_plane_permits, 4);
        // Floor of 2 so a 1- or 2-worker runtime can still make progress.
        assert_eq!(BridgeConfig::for_workers(1).data_plane_permits, 2);
        assert_eq!(BridgeConfig::for_workers(2).data_plane_permits, 2);
        // Script callers always get strictly more room than the data plane.
        for workers in [1, 2, 8, 16, 64] {
            let c = BridgeConfig::for_workers(workers);
            assert!(
                c.script_pool_permits > c.data_plane_permits,
                "workers={workers}"
            );
        }
    }

    #[test]
    fn permits_are_released_when_a_call_times_out() {
        // The safety property is that a stalled peer cannot hold data-plane
        // capacity beyond the caller's deadline. A permit that outlives the
        // timeout would drain the pool under sustained peer slowness while
        // every test here still passed.
        let bridge = bridge(1);
        let err = bridge
            .call(CallerClass::DataPlane, Duration::from_millis(50), async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(err, RpcError::Timeout);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while bridge.available_permits(CallerClass::DataPlane) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "permit was still held long after the caller's deadline"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        // And the freed permit is genuinely usable again.
        assert_eq!(
            bridge.call(CallerClass::DataPlane, Duration::from_secs(5), async {
                Ok(1)
            }),
            Ok(1)
        );
    }

    #[test]
    fn permits_are_released_after_each_call() {
        let bridge = bridge(2);
        for _ in 0..10 {
            let _ = bridge.call(CallerClass::DataPlane, Duration::from_secs(5), async {
                Ok(())
            });
        }
        assert_eq!(bridge.available_permits(CallerClass::DataPlane), 2);
    }
}
