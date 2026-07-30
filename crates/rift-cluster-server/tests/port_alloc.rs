//! The gate for issue #110's shared test helpers.
//!
//! These are the properties the rest of the suite depends on but cannot check
//! for itself: a port handed to one test binary is never handed to another, and
//! a status assertion that fails says why. Both are cross-cutting, so they get
//! their own binary rather than riding along in whichever suite noticed the
//! problem first.

mod common;

use common::ports;
use common::seen::Seen;
use reqwest::header::{HeaderMap, HeaderValue};
use std::collections::BTreeSet;
use std::net::TcpListener;

/// The property the whole design rests on: the lease base stays bound for the
/// life of the process, so a second leaser cannot pick the same block. If this
/// ever passes, the mutual exclusion is imaginary and the allocator is back to
/// hoping.
#[test]
fn the_lease_base_stays_bound_for_the_life_of_the_process() {
    let base = ports::lease_base();
    let second = TcpListener::bind(("127.0.0.1", base));
    assert!(
        second.is_err(),
        "port {base} was rebindable, so nothing stops another test process from \
         leasing this block too"
    );
}

/// Two leases are what two concurrent test binaries look like to the kernel —
/// the arbiter is the OS, so holding both in one process exercises the same
/// exclusion a second process would hit.
#[test]
fn two_leases_never_share_a_block() {
    let first = ports::claim_block().expect("a free block");
    let second = ports::claim_block().expect("a second free block");
    assert_ne!(
        first.base(),
        second.base(),
        "both leases claimed the same block, so their ports would collide"
    );
}

#[test]
fn every_port_is_distinct_and_inside_this_process_lease() {
    let base = ports::lease_base();
    let mut handed_out = BTreeSet::new();

    for _ in 0..64 {
        let port = ports::reserve_port();
        assert!(
            port > base && port < base + ports::BLOCK_SIZE,
            "{port} is outside this process's lease {base}..{}",
            base + ports::BLOCK_SIZE
        );
        assert!(handed_out.insert(port), "{port} was handed out twice");
    }
}

/// A handed-out port must be free *and* stay free while its neighbours are
/// taken — holding them all at once is what a suite of concurrent tests does.
#[test]
fn handed_out_ports_are_bindable_together() {
    let held: Vec<TcpListener> = (0..8)
        .map(|_| {
            let port = ports::reserve_port();
            TcpListener::bind(("127.0.0.1", port))
                .unwrap_or_else(|e| panic!("port {port} was handed out but is not free: {e}"))
        })
        .collect();
    assert_eq!(held.len(), 8);
}

/// Exhaustion is the one case where silence would be worse than failure:
/// wrapping the counter would hand out a live port a second time.
#[test]
#[should_panic(expected = "exhausted")]
fn an_exhausted_block_panics_rather_than_reusing_a_port() {
    let lease = ports::claim_block().expect("a free block");
    for _ in 0..ports::BLOCK_SIZE {
        let _ = lease.next_port();
    }
}

#[test]
fn a_seen_response_reports_status_cluster_headers_and_body() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "rift-cluster-revision",
        HeaderValue::from_static("default:1@7"),
    );
    headers.insert("content-type", HeaderValue::from_static("application/json"));

    let rendered = Seen {
        status: 404,
        headers,
        body: "{\"error\":\"not found\"}".to_owned(),
    }
    .to_string();

    assert!(rendered.contains("404"), "{rendered}");
    assert!(
        rendered.contains("rift-cluster-revision: default:1@7"),
        "the header that names the failure mode must survive into the message: {rendered}"
    );
    assert!(rendered.contains("not found"), "{rendered}");
    assert!(
        !rendered.contains("content-type"),
        "unrelated headers are noise: {rendered}"
    );
}

/// "No cluster headers at all" is itself a diagnosis — the request never
/// reached the terminated-route handler — so it has to be stated, not implied
/// by an empty line.
#[test]
fn a_seen_response_says_so_when_no_cluster_headers_are_present() {
    let rendered = Seen {
        status: 404,
        headers: HeaderMap::new(),
        body: String::new(),
    }
    .to_string();

    assert!(
        rendered.contains("no rift-* headers"),
        "an absent header set must be reported explicitly: {rendered}"
    );
}

#[test]
fn a_long_body_is_truncated_but_says_that_it_was() {
    let rendered = Seen {
        status: 500,
        headers: HeaderMap::new(),
        body: "x".repeat(Seen::BODY_LIMIT * 2),
    }
    .to_string();

    assert!(rendered.len() < Seen::BODY_LIMIT * 2, "{}", rendered.len());
    assert!(rendered.contains("truncated"), "truncation must be visible");
}
