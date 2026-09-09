# `rift-cluster-server` integration tests

These binaries drive `compose::start` and speak plain HTTP to a real admin listener. A test here
is not a unit test with a mock behind it: it binds real localhost ports, and in the multi-node
cases it stands up a Raft fleet with real elections.

## Fleet tests are serialized by `common::TEST_LOCK`

Every `#[tokio::test]` in `write_path.rs` opens with:

```rust
let _serial = TEST_LOCK.lock().await;
```

**Why.** libtest runs as many tests at once as the runner has cores. Each fleet test holds one to
three nodes, each with its own tokio runtime plus a two-worker bridge runtime, so a handful of
concurrent tests oversubscribe a CI runner several times over. Raft's timers are fixed at a
150–300 ms election timeout against a 50 ms heartbeat (D-42, pinned by
`raft::node::tests`), so a follower that misses a scheduling slot for 300 ms campaigns, the leader
steps down, and any write issued in that gap answers `503 no quorum / leader unreachable`.

That is issue #561: eleven `write_path` tests failing that way on loaded runners and on `master`
itself, each of them passing in isolation. It is not a property of any one test's bound — the
failing *set* moves between identical runs, which is the signature of scheduling starvation rather
than a regression.

`crates/rift-cluster/tests/cluster.rs` has serialized its harness this way since it hit the same
thing; see `crates/rift-cluster/tests/README.md`. The rule is the same here: **you do not need
`--test-threads=1`; the lock enforces it.** That matters because CI's `build` job runs an
unmodified workspace-wide test command, and issue #495 closed with "no `--test-threads` flag added
to `build`" as an acceptance criterion.

The rule is enforced, not just written down: `every_test_in_this_file_serialises_its_fleet` in
`write_path.rs` parses the file's own source and fails naming any `#[tokio::test]` that does not
open with the guard. It also rejects `let _ = TEST_LOCK.lock().await`, which reads as the guard
while dropping it immediately.

**Scope.** The lock is `pub static` in `tests/common/mod.rs`, and each test binary compiles its own
copy of `common` — so it is one lock per binary, which is the right scope: test *binaries* already
run one at a time within a single test invocation. Only `write_path.rs` takes it today.
`clustered.rs` (19 tests, 39 `compose::start` sites, 14 multi-node) and `fleet_session.rs` have the
same latent hazard and are the next candidates if the 503 signature shows up in them; neither
appears in #561's roster, so neither is serialized on suspicion.

**Do not take the lock inside a helper, and do not drop it early.** `tokio::sync::Mutex` is not
reentrant, so a helper that locked as well would deadlock against the test that called it — and a
deadlock is a CI *hang*, not a red test. The structural gate above checks this too: no line outside
a test's opening guard may call `TEST_LOCK.lock()`. An explicit `drop(_serial)` mid-test would
likewise defeat the point, and is the one way to evade the gate; normal scope-end drop already
gives you what you want. The lock does not poison, so a panicking test releases it and the next one
proceeds.

**What is not covered.** Three properties here are correct by construction and not exercised by any
test, because forcing them costs a fleet that never elects: that the retry budget in
`barrier_none_does_not_wait_on_an_unreachable_peer` fails loudly rather than hanging when it is
exhausted; that `await_stable_leader` panics rather than spinning when no leader ever settles; and
that a barrier genuinely waiting on a dead peer surfaces as a `504` and so falls through the retry
to a red assertion. The first two are single bounded loops that check their deadline on every
iteration; the third is pinned indirectly by `only_a_leaderless_503_is_retried`.

## Ports

`common::ports` leases a whole block of ports per process and hands them out with a counter, rather
than binding `:0` and dropping the listener. `common/mod.rs` documents why; the short version is
that the bind-and-drop pattern is a time-of-check/time-of-use race with two possible winners, and a
counter alone closes only one of them.
