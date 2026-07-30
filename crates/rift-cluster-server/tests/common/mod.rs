//! Helpers shared by this crate's integration-test binaries.
//!
//! Each binary compiles its own copy, so anything a given binary does not use
//! is dead code there — that is inherent to `mod common`, not an oversight.
#![allow(dead_code)]

/// Port allocation for this crate's integration tests (issue #110).
///
/// Every test file here used to reserve a port by binding `127.0.0.1:0`, reading
/// the port the kernel picked, and *dropping the listener* before the server
/// that actually wants it binds. That gap is a time-of-check/time-of-use race
/// with two possible winners:
///
/// - **Another thread of the same binary.** The tests inside one binary run on a
///   thread pool, so two of them can sit between "read the port" and "start the
///   server" at once, and the kernel is free to hand the second the port the
///   first just released. This is the demonstrated one: `nologfile_beats_log`
///   failed with `Address already in use` under `cargo test --workspace` — where
///   the extra load widens the window — while passing on its own.
/// - **Another `cargo test` invocation.** Test *binaries* within one invocation
///   run strictly one after another (measured: `clustered`, `oss_bootstrap` and
///   `write_path` each leased the same block in turn, which they could not have
///   done concurrently), so a cross-process collision needs a second run — which
///   the worktree-per-issue workflow in `CLAUDE.md` produces routinely.
///
/// A counter alone would close the first and not the second. So a process
/// **leases a whole block** of ports by binding the block's base address and
/// holding that listener for its lifetime, then hands out ports from inside the
/// block with a counter. The kernel arbitrates the block — a second process
/// asking for the same base is refused — and the lease needs no lock file,
/// because the OS reclaims the socket on any exit, SIGKILL included.
///
/// The blocks sit below the ephemeral ranges both CI (Linux, 32768+) and dev
/// machines (macOS, 49152+) allocate from, so the kernel never hands one of
/// these out as a source port behind our back. A container configured with a
/// wide `ip_local_port_range` (some set `1024 65535`) would break that
/// assumption; nothing the suite runs on today does.
///
/// Residual risk, stated rather than hidden: a process outside this suite could
/// squat inside a leased block — including one binding `0.0.0.0:<base>`, which
/// BSD accepts alongside our `127.0.0.1:<base>` and so would not be refused the
/// lease. The trade is deliberate: that surfaces as a loud bind failure naming
/// the port, instead of a silent cross-test collision that reads as a flake.
pub mod ports {
    use std::net::TcpListener;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU16, Ordering};

    /// Ports per lease. One block per test binary, and no binary comes close to
    /// spending one.
    pub const BLOCK_SIZE: u16 = 1000;

    /// Test binaries run one at a time, so a single `cargo test` needs only the
    /// four `port_alloc.rs` claims at once while it proves the exclusion works.
    /// The rest is headroom for concurrent runs across worktrees.
    ///
    /// The gaps are deliberate — every one of these is a port something in this
    /// repo really binds:
    ///
    /// - 18000: `raft/store.rs`'s unit tests drive real `ImposterManager` binds
    ///   on 127.0.0.1:18081-18092 (`bind_failure_does_not_fail_apply` and
    ///   friends).
    /// - 22000, 26000, 29000: host ports the container chaos tier publishes
    ///   (22525/22526, 26300, 29090). A dev machine can be running both.
    ///
    /// And nothing at or above 32768, which Linux hands out as ephemeral source
    /// ports.
    const BLOCK_BASES: [u16; 9] = [
        20000, 21000, 23000, 24000, 25000, 27000, 28000, 30000, 31000,
    ];

    /// A claim on one block, held open. Dropping it releases the block; the
    /// process-wide lease is never dropped, which is the point.
    pub struct Lease {
        /// Held, never read: being bound *is* the lease.
        _held: TcpListener,
        base: u16,
        next: AtomicU16,
    }

    impl Lease {
        #[must_use]
        pub fn base(&self) -> u16 {
            self.base
        }

        /// The next port in this block. Never returns the same port twice, and
        /// never returns the base — that one is holding the lease open.
        #[must_use]
        pub fn next_port(&self) -> u16 {
            let offset = self.next.fetch_add(1, Ordering::Relaxed);
            assert!(
                offset < BLOCK_SIZE,
                "port block {} exhausted after {} ports; wrapping would hand out a \
                 port that is already serving something",
                self.base,
                BLOCK_SIZE - 1
            );
            self.base + offset
        }
    }

    /// Claim the first free block, or `None` if every block is taken.
    ///
    /// The `.ok()` is a domain-optional probe, not a swallowed error: a refused
    /// bind is precisely the signal "another process holds this block", which is
    /// the answer this function exists to compute.
    #[must_use]
    pub fn claim_block() -> Option<Lease> {
        BLOCK_BASES.iter().find_map(|&base| {
            TcpListener::bind(("127.0.0.1", base))
                .ok()
                .map(|held| Lease {
                    _held: held,
                    base,
                    next: AtomicU16::new(1),
                })
        })
    }

    static PROCESS_LEASE: OnceLock<Lease> = OnceLock::new();

    fn process_lease() -> &'static Lease {
        PROCESS_LEASE.get_or_init(|| {
            claim_block().unwrap_or_else(|| {
                panic!(
                    "all {} port blocks are leased; either more test binaries are \
                     running than there are blocks, or something outside the suite \
                     is bound to one of {BLOCK_BASES:?}",
                    BLOCK_BASES.len()
                )
            })
        })
    }

    /// The base of this process's lease. Exposed so a test can assert the block
    /// really is held.
    #[must_use]
    pub fn lease_base() -> u16 {
        process_lease().base()
    }

    /// A port no other test process can be handed.
    ///
    /// Unlike the bind-and-release idiom it replaces, this does **not** verify
    /// the port is free right now. It does not need to: within the block, this
    /// process is the only allocator. If a stranger has squatted on the port
    /// anyway, the caller's own bind fails loudly and names it.
    #[must_use]
    pub fn reserve_port() -> u16 {
        process_lease().next_port()
    }

    /// `N` distinct ports, for a test that must know several before the server
    /// that binds them exists.
    #[must_use]
    pub fn reserve_ports<const N: usize>() -> [u16; N] {
        std::array::from_fn(|_| reserve_port())
    }

    /// A reserved port as a loopback address, for the CLI flags that want one.
    #[must_use]
    pub fn reserve_addr() -> String {
        format!("127.0.0.1:{}", reserve_port())
    }
}

/// What a response actually was, kept around so a failed assertion can say so.
///
/// The failure that opened issue #110 printed `left: 404, right: 201` and
/// nothing else, which is exactly why the issue had to be filed with the cause
/// unknown. The cluster headers distinguish the candidate explanations from each
/// other — `rift-cluster-warnings` means the engine refused the write,
/// `rift-cluster-revision` alone means the write committed and the re-read still
/// missed, and neither means the request never reached the cluster-terminated
/// route at all — so a status assertion should never throw them away.
pub mod seen {
    use reqwest::header::HeaderMap;
    use std::fmt;

    pub struct Seen {
        pub status: u16,
        pub headers: HeaderMap,
        pub body: String,
    }

    impl Seen {
        /// Bodies are for diagnosis, not for reading a config dump in a panic.
        pub const BODY_LIMIT: usize = 512;

        /// Consume a response into something that survives into a panic message.
        pub async fn of(response: reqwest::Response) -> Self {
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            // A body that will not read is itself worth reporting; this is a
            // diagnostic path, so the fallback text is the correct answer rather
            // than a swallowed failure.
            let body = response
                .text()
                .await
                .unwrap_or_else(|e| format!("<body could not be read: {e}>"));
            Self {
                status,
                headers,
                body,
            }
        }

        #[must_use]
        pub fn header(&self, name: &str) -> Option<&str> {
            self.headers.get(name).and_then(|v| v.to_str().ok())
        }

        /// The body as JSON, with the whole response in the panic message when
        /// it is not JSON at all.
        #[must_use]
        pub fn json(&self) -> serde_json::Value {
            serde_json::from_str(&self.body)
                .unwrap_or_else(|e| panic!("expected a JSON body ({e}) but saw {self}"))
        }
    }

    impl fmt::Display for Seen {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "HTTP {}", self.status)?;

            let mut cluster: Vec<String> = self
                .headers
                .iter()
                .filter(|(name, _)| name.as_str().starts_with("rift-"))
                .map(|(name, value)| format!("{name}: {}", value.to_str().unwrap_or("<non-utf8>")))
                .collect();
            cluster.sort();

            if cluster.is_empty() {
                write!(
                    f,
                    "; no rift-* headers (the request may never have reached the \
                     cluster-terminated route)"
                )?;
            } else {
                write!(f, "; {}", cluster.join(", "))?;
            }

            if self.body.is_empty() {
                return write!(f, "; empty body");
            }
            let shown: String = self.body.chars().take(Self::BODY_LIMIT).collect();
            if shown.len() < self.body.len() {
                write!(f, "; body: {shown}… (truncated)")
            } else {
                write!(f, "; body: {shown}")
            }
        }
    }
}
