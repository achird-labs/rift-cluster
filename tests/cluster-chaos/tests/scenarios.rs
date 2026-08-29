//! Container-tier chaos scenarios (issue #11).
//!
//! Every one of these needs a real process to die, so none of them can live in
//! the in-process harness. Run with:
//!
//! ```sh
//! cargo test -p cluster-chaos -- --ignored --test-threads=1
//! ```
//!
//! `--ignored` because they need a container runtime; `--test-threads=1`
//! because the compose file publishes fixed host ports, so two stacks cannot
//! coexist. The harness holds a process-wide lock as well, so forgetting the
//! flag costs time rather than correctness.
//!
//! House rules, inherited from the issue's design bars:
//! - assertions read the admin API and Prometheus metrics, **never** log output;
//! - convergence is polled against a real surface, never slept-and-hoped;
//! - a scenario that fails is a bug to file, not a flake to retry.

use std::time::Duration;

use tokio::task::JoinSet;

use cluster_chaos::{
    CONVERGE_TIMEOUT, Cluster, FLOW_STATE_HOST_PORTS, FLOW_STATE_IMPOSTER_PORT,
    FRONT_DOOR_HOST_PORTS, FRONT_PORT, NODES, PULL_ON_MISS_HOST_PORTS, PULL_ON_MISS_IMPOSTER_PORT,
    SEQUENCING_HOST_PORTS, SEQUENCING_IMPOSTER_PORT, SOURCES_CLUSTER_HOST_PORTS,
    SOURCES_ORIGIN_BASE_URL, TENANCY_A_HOST_PORTS, TENANCY_A_IMPOSTER_PORT, TENANCY_B_HOST_PORTS,
    TENANCY_B_IMPOSTER_PORT, TENANCY_FLEET_KEY, add_toxic, admin_as, admin_with_key, append_stub,
    backend_failing_health_check, clear_toxics, cluster_config, cluster_imposters,
    committed_config, config_revision, create_tenant, declare_source, exec_probe, get_data_plane,
    get_data_plane_with, get_json, imposter_ports, metric, mint_principal, origin_publish,
    origin_republish, origin_request_count, probe, provenance_of, published_host_ports,
    pull_source, put_imposter, put_imposter_config, put_imposter_with_key, put_routes, put_stubs,
    read_source, source_document, toxic_count, wait_admin_reachable, wait_admin_reachable_with_key,
    wait_backend_ejected, wait_converged, wait_converged_on, wait_converged_with_key,
    wait_origin_ready, wait_ports_free_in, wait_revisions_agree, wait_revisions_agree_on,
    wait_single_leader, wait_sources_reachable, wait_voters,
};

/// The imposter port a scenario configures. Inside the container network
/// nothing else binds it, and each scenario gets a fresh stack.
const IMPOSTER_PORT: u16 = 6001;

/// How long the fleet may take to converge with the write barrier off.
///
/// From the issue's normative table. It is a *product* bound, not a harness
/// tolerance: replication is a Raft append plus an apply, so 5s is generous by
/// an order of magnitude on a healthy LAN. If this starts failing, the question
/// is what got slow, not whether the number should go up.
const UNBARRIERED_CONVERGE_BOUND: Duration = Duration::from_secs(5);

/// Writes in C14's storm, per the issue's normative table.
///
/// Each one binds a distinct imposter port from `IMPOSTER_PORT` upward, so the
/// range must stay clear of the ports other scenarios use.
const C14_STORM_WRITES: u16 = 100;

/// How long the fleet may take to accept writes again after its leader is
/// killed — the operator-visible form of the issue's "new leader <= 3s".
///
/// **Why this and not the leader gauge or `/_cluster/members`.** The gauge
/// (`rift_cluster_members{state="leader"}`) is resampled on a ~5s timer and so
/// cannot resolve a three-second bound at all; reading a quantity coarser than
/// the bound is the mistake #94 fixed in C6. `GET /_cluster/members` *does*
/// serve openraft's live metrics, but it rides the **cluster port** behind the
/// HMAC credential (docs/rift-cluster-server.md, "These ride the cluster port"), so
/// the harness cannot reach it. A write is what is left, and it is also what a
/// client actually experiences.
///
/// **Derived, and deliberately larger than 3s.** A post-kill write pays the
/// election *and* the write barrier: with the default `ready-nodes` barrier the
/// new leader waits on the dead node's applied index until
/// `--cluster-write-barrier-timeout` (2s) expires, then answers 201 with a
/// warnings header. So the client-visible budget is election (<= 3s per the
/// issue) + barrier timeout (2s) = 5s. Timing the write against a bare 3s would
/// fail a perfectly healthy fleet for doing exactly what it is configured to
/// do. The 3s the issue names is the election component, and it is the part
/// that would have to change for this bound to change.
const FAILOVER_WRITE_BOUND: Duration = Duration::from_secs(5);

/// Poll until an admin write is accepted again, returning how long it took.
///
/// `503`/`504` are the documented answers while no leader is available, so they
/// are retry signals here rather than failures. Any other non-201 is returned
/// as an error rather than retried, so a permanently broken request fails fast
/// and says what it saw instead of spinning out the whole budget.
async fn time_until_writes_resume(admin: u16, port: u16, body: &str) -> Result<Duration, String> {
    let started = std::time::Instant::now();
    let deadline = started + FAILOVER_WRITE_BOUND * 3;
    let mut last = String::from("no attempt completed");
    while std::time::Instant::now() < deadline {
        match put_imposter(admin, port, body).await {
            Ok(201) => return Ok(started.elapsed()),
            Ok(503 | 504) => last = "503/504 (no leader yet)".to_owned(),
            Ok(other) => {
                return Err(format!(
                    "write answered {other}, which is not a failover state"
                ));
            }
            Err(e) => last = format!("transport error: {e}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!(
        "no write accepted within {:?}; last: {last}",
        deadline - started
    ))
}

/// Rungs in `test_graceful_leave`'s write ladder, each on its own port.
const LADDER_RUNGS: u16 = 20;

/// First port of the ladder. Clear of `IMPOSTER_PORT` and of C14's storm range.
const LADDER_BASE_PORT: u16 = 6200;

/// Ceiling on observed leadership transitions in C6's 60s toxic window.
///
/// Derived, not tuned. C6 injects 100±100ms each direction against openraft's
/// randomized election timeout (150ms to `ELECTION_TIMEOUT_MAX_MS` = 300ms,
/// heartbeat 50ms, all in `rift-cluster`'s `raft/node.rs`), so heartbeat
/// arrival gaps routinely exceed a timeout draw from the low half of that
/// range: occasional elections are *in spec* for a correct fleet under these
/// toxics, not evidence of a fault. What separates correct from flapping is the
/// rate.
///
/// The leader gauge resamples on a ~5s timer, so the 60s window yields at most
/// ~12 samples and therefore at most 11 observable transitions. A fleet
/// re-electing continuously shows a different leader in nearly every sample --
/// 8-11 -- while near-threshold elections under these toxics show 0-4. This
/// bound is the top of the in-spec regime, which leaves it a wide margin below
/// the flapping floor and none above: a 5th election in one window is treated
/// as flapping, deliberately.
///
/// If `node.rs`'s timeouts or C6's toxics change, re-derive from the new
/// arithmetic; do not nudge it upward to silence a failure.
///
/// D-42: C6 bounds an election *rate*, never a count, and the election timers
/// it is derived from are fixed in `raft/node.rs` rather than exposed as a
/// `NodeConfig` knob — widening them so a count bound holds was the rejected fix.
const C6_MAX_LEADER_TRANSITIONS: usize = 4;

/// Observed leadership transitions in a sequence of distinct leader samples.
///
/// Shared by C6 and its bound test so the two cannot drift: a change to how a
/// transition is counted is felt by the gate, not only by the container tier.
fn leader_transitions(samples: &[usize]) -> usize {
    samples.len().saturating_sub(1)
}

/// The C6 bound admits a correct fleet's near-threshold elections and still
/// rejects a flapping one.
///
/// Runs in ordinary CI: C6 itself needs a container runtime, so without this
/// the bound's arithmetic would only ever be exercised by the nightly tier.
///
/// Pins D-42: the bound is a rate over the ~5 s gauge resolution — a fleet
/// showing 0–4 transitions in the 60 s window (near-threshold elections under
/// the toxics) passes, one showing a new leader in nearly every sample fails,
/// and a count-of-zero assertion would reject the correct fleet.
#[test]
fn c6_bound_admits_near_threshold_but_rejects_flapping() {
    // The sequence C6 actually failed on in PR #92 (run 29973215820, attempt 1),
    // which attempt 2 passed on the same SHA -- a healthy fleet, not a fault.
    let near_threshold = [0, 1, 2, 1];
    assert_eq!(leader_transitions(&near_threshold), 3);
    assert!(
        leader_transitions(&near_threshold) <= C6_MAX_LEADER_TRANSITIONS,
        "the observed same-SHA-passing sequence must not fail the bound"
    );

    // A fleet re-electing continuously: a different leader in every ~5s sample
    // across the 60s window, which is the most the sampler can observe.
    let flapping: Vec<usize> = (0..12).map(|i| i % 3).collect();
    assert!(
        leader_transitions(&flapping) > C6_MAX_LEADER_TRANSITIONS,
        "a leader change in nearly every sample must still fail the bound"
    );

    // Pin the threshold itself, so an off-by-one edit to the constant or to the
    // comparison is caught rather than absorbed by the gap between 3 and 11.
    let at_bound: Vec<usize> = (0..=C6_MAX_LEADER_TRANSITIONS).collect();
    assert_eq!(leader_transitions(&at_bound), C6_MAX_LEADER_TRANSITIONS);
    assert!(leader_transitions(&at_bound) <= C6_MAX_LEADER_TRANSITIONS);

    let one_over: Vec<usize> = (0..=C6_MAX_LEADER_TRANSITIONS + 1).collect();
    assert!(leader_transitions(&one_over) > C6_MAX_LEADER_TRANSITIONS);
}

/// The barrier-none overlay really does turn the barrier off, on every node.
///
/// `test_config_sync_converges_without_barrier` passes identically against a
/// fleet still running the default `ready-nodes` barrier — that fleet converges
/// well inside 5s too. So a typo'd key, a renamed env var, or a deleted service
/// block would leave the scenario green while it silently stopped testing the
/// thing it is named for. Checking the overlay's text is cheap and catches all
/// three; it runs un-ignored because it needs no container.
#[test]
fn barrier_none_overlay_disables_the_barrier_fleet_wide() {
    let overlay = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/compose/barrier-none.overlay.yml"
    ))
    .expect("read the barrier-none overlay");

    for node in &NODES {
        let block = overlay
            .split(&format!("{}:", node.name))
            .nth(1)
            .unwrap_or_else(|| panic!("overlay has no block for {}", node.name));
        let env = block
            .split_once("RIFT_CLUSTER_WRITE_BARRIER")
            .map(|(_, rest)| rest)
            .unwrap_or_else(|| {
                panic!(
                    "overlay does not set RIFT_CLUSTER_WRITE_BARRIER for {} -- \
                     the scenario that uses it would silently run with the \
                     default barrier and still pass",
                    node.name
                )
            });
        assert!(
            env.trim_start().starts_with(": \"none\""),
            "{} sets RIFT_CLUSTER_WRITE_BARRIER to something other than \"none\"",
            node.name
        );
    }
}

/// The port barrier must fail *naming the port*, not time out anonymously.
///
/// This is issue #117's failure reproduced deterministically: a held host port
/// is exactly what `compose up` hits, and docker's own message says only
/// `address already in use` with no clue which stack held it. The barrier exists
/// to answer that, so a silent timeout would be no improvement.
///
/// The squatter binds `0.0.0.0`, the same address docker publishes on, because
/// BSD accepts `0.0.0.0:P` alongside `127.0.0.1:P` — a loopback squatter would
/// make this pass on macOS while proving nothing.
#[test]
fn the_port_barrier_names_the_port_that_is_still_held() {
    // A port this test owns, not one from the published set: waiting on the
    // published set would fail on any machine with the `deploy/compose` demo
    // stack up, which says nothing about the barrier.
    let squatter = std::net::TcpListener::bind(("0.0.0.0", 0)).expect("take a free port");
    let port = squatter.local_addr().expect("addr").port();

    let err = wait_ports_free_in(&[port], Duration::from_millis(300))
        .expect_err("a held port must fail the barrier");
    let message = err.to_string();
    assert!(
        message.contains(&port.to_string()),
        "the barrier must name the port it is stuck on, got: {message}"
    );

    drop(squatter);
    wait_ports_free_in(&[port], Duration::from_secs(5))
        .expect("the barrier clears once the port is released");
}

/// The barrier must wait on exactly what compose publishes — read out of the
/// compose files, not out of the constants it is checking.
///
/// Deriving both sides from `NODES` and friends would only prove the constants
/// agree with themselves: a `ports:` entry added to an overlay with no matching
/// constant would sail past, unwaited-on and unreserved, which is precisely the
/// hole #117 came through. Set equality in both directions, so an unpublished
/// constant is caught too.
#[test]
fn the_barrier_covers_exactly_what_compose_publishes() {
    let mut published: Vec<u16> = compose_files()
        .iter()
        .flat_map(|path| host_ports_in(&read_compose(path)))
        .collect();
    published.sort_unstable();
    published.dedup();

    let mut waited = published_host_ports();
    waited.sort_unstable();
    waited.dedup();

    assert_eq!(
        published, waited,
        "the ports compose publishes and the ports the barrier waits on have \
         diverged. Left-only: a published port nobody waits on or reserves -- the \
         #117 hole. Right-only: a constant naming a port nothing publishes."
    );

    // If the scraper silently matched nothing, set equality could still hold
    // against an empty list. Pin the shape so a broken scraper is loud.
    assert!(
        published.len() >= NODES.len() * 3,
        "scraped only {} host ports from the compose files; the scraper is \
         broken, not the topology",
        published.len()
    );
}

/// Every compose file in this repo that can publish a host port: the shipped
/// base file, plus **every** overlay in `compose/`.
///
/// Enumerated from the directory rather than listed, because a hardcoded list
/// is a coverage check that silently stops covering the moment someone adds an
/// overlay — which is exactly the fail-open shape the port check exists to
/// prevent. Found while adding the flow-state overlay: the new file would have
/// escaped the check entirely.
fn compose_files() -> Vec<std::path::PathBuf> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = vec![root.join("../../deploy/compose/docker-compose.yml")];

    let overlays = root.join("compose");
    let entries =
        std::fs::read_dir(&overlays).unwrap_or_else(|e| panic!("read {}: {e}", overlays.display()));
    for entry in entries {
        let path = entry.expect("dir entry").path();
        if path.extension().is_some_and(|ext| ext == "yml") {
            files.push(path);
        }
    }
    // Deterministic order so a failure message reads the same on every machine.
    files.sort();
    assert!(
        files.len() > 2,
        "found {} compose files; the overlay directory scan is broken, not the \
         topology",
        files.len()
    );
    files
}

/// Every compose `build:` against `deploy/Dockerfile` must pin a `target:`
/// (issue #228). The Dockerfile's last stage is now the chaos suite's
/// `runtime-faketime` — it builds `FROM runtime`, so it cannot precede it —
/// which means an untargeted build silently produces the clock-lying flavor
/// instead of the production image. That flip actually shipped once, one file
/// away from the compose that was fixed; this is the tripwire that makes
/// appending a stage safe forever.
///
/// The check is a per-file count heuristic — `target:` lines at least as
/// numerous as `dockerfile: deploy/Dockerfile` lines — rather than a YAML
/// parse, matching the workflow-literal style of the sibling meta-tests; a
/// false pass would need a file to pair an untargeted build with an unrelated
/// surplus `target:`, which no honest compose file has a reason to contain.
#[test]
fn every_dockerfile_build_site_pins_a_target() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = compose_files();
    let demos = root.join("../../deploy/compose");
    for entry in std::fs::read_dir(&demos).unwrap_or_else(|e| panic!("read deploy/compose: {e}")) {
        let path = entry.expect("dir entry").path();
        if path.extension().is_some_and(|ext| ext == "yml") && !files.contains(&path) {
            files.push(path);
        }
    }
    for path in files {
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let builds = text.matches("dockerfile: deploy/Dockerfile").count();
        let targets = text.matches("target:").count();
        assert!(
            targets >= builds,
            "{}: {builds} build(s) against deploy/Dockerfile but only {targets} `target:` \
             pin(s) — an untargeted build now produces the faketime flavor, not the \
             production image",
            path.display()
        );
    }
}

/// Every compile-time file reference must resolve inside a root the builder copies.
///
/// D-62 replaced the builder's `COPY . .` with named roots, so that `cook`'s
/// output survives into the build layer instead of being dirtied by a blanket
/// copy. The cost of naming roots is that one can be missing, and this is the
/// guard for that.
///
/// It is not hypothetical. `crates/rift-cluster-server/src/openapi.rs` does
/// `include_str!("../../../docs/api/openapi-ee.yaml")` — the OpenAPI contract is
/// compiled into the binary, so `docs/` is a build input rather than
/// documentation, and a list written from "what a Rust build obviously needs"
/// omits it. The failure would be a broken image build, which is loud but lands
/// on whoever adds the next `include_str!` rather than on whoever shortened the
/// copy list.
///
/// Roots are read from the Dockerfile rather than restated here: a list in this
/// test that had to be kept in step with the one in the Dockerfile would be the
/// same rot one file over.
#[test]
fn the_builder_copies_every_root_the_crates_compile_in() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let dockerfile = std::fs::read_to_string(root.join("deploy/Dockerfile"))
        .unwrap_or_else(|e| panic!("read deploy/Dockerfile: {e}"));

    // The builder stage, and within it everything the cook step leaves behind.
    let builder = dockerfile
        .split("\nFROM chef AS builder")
        .nth(1)
        .expect("Dockerfile has no `FROM chef AS builder` stage");
    let copied: std::collections::BTreeSet<String> = builder
        .lines()
        .map(str::trim)
        .filter_map(|l| l.strip_prefix("COPY "))
        // `--from=<stage>` copies out of another stage, not out of the context.
        .filter(|l| !l.starts_with("--from="))
        .filter_map(|l| {
            let mut parts: Vec<&str> = l.split_whitespace().collect();
            parts.pop()?; // destination
            Some(parts)
        })
        .flatten()
        .map(|s| s.trim_end_matches('/').to_owned())
        .collect();

    assert!(
        !copied.is_empty(),
        "parsed no COPY sources out of the builder stage — the parser has drifted \
         from the Dockerfile, which would make this test pass vacuously"
    );

    let mut checked = 0usize;
    for entry in walk_rs(&root.join("crates")) {
        let text = std::fs::read_to_string(&entry)
            .unwrap_or_else(|e| panic!("read {}: {e}", entry.display()));
        for macro_name in ["include_str!(\"", "include_bytes!(\""] {
            for (idx, _) in text.match_indices(macro_name) {
                let rest = &text[idx + macro_name.len()..];
                let Some(end) = rest.find('"') else { continue };
                let literal = &rest[..end];
                // Relative to the file that names it, then back to repo-relative.
                let resolved = entry
                    .parent()
                    .expect("a .rs file has a parent")
                    .join(literal);
                let rel = resolved
                    .strip_prefix(&root)
                    .map(std::path::Path::to_path_buf)
                    .unwrap_or(resolved.clone());
                let normalised = normalise(&rel);
                let top = normalised
                    .split('/')
                    .next()
                    .expect("a path has a first component");
                assert!(
                    copied.contains(top) || copied.contains(&normalised),
                    "{} compiles in `{literal}` (-> `{normalised}`), whose root `{top}` is \
                     not copied by the builder stage. The builder copies {copied:?}. Add the \
                     root to `deploy/Dockerfile`, or the image build fails on a missing file.",
                    entry.display()
                );
                checked += 1;
            }
        }
    }
    assert!(
        checked > 0,
        "found no compile-time file references at all under crates/ — the scan is \
         broken, and a missing root would sail past it"
    );
}

/// Every `.rs` file under `dir`, recursively.
fn walk_rs(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            // `target/` holds build output, not source, and is enormous.
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            out.extend(walk_rs(&path));
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
    out
}

/// Collapse `a/b/../c` to `a/c` textually — the paths here are literals from
/// source, never symlinks, so a lexical fold is the right resolution and does
/// not need the file to exist.
fn normalise(path: &std::path::Path) -> String {
    let text = path.to_string_lossy().into_owned();
    let mut parts: Vec<&str> = Vec::new();
    for component in text.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    parts.join("/")
}

fn read_compose(path: &std::path::Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Host ports from the `ports:` blocks of a compose file (`"12525:2525"` -> 12525).
///
/// Deliberately a literal scrape rather than a YAML parse: the value of this
/// check is that it reads the shipped text the way a person would, with no
/// dependency that could normalise away the thing being checked.
fn host_ports_in(compose: &str) -> Vec<u16> {
    let mut ports = Vec::new();
    let mut in_ports = false;
    for line in compose.lines() {
        let trimmed = line.trim();
        if trimmed == "ports:" {
            in_ports = true;
            continue;
        }
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if !trimmed.starts_with('-') {
            in_ports = false;
            continue;
        }
        if !in_ports {
            continue;
        }
        let spec = trimmed.trim_start_matches('-').trim();
        let spec = spec.split('#').next().unwrap_or(spec).trim();
        let spec = spec.trim_matches('"').trim_matches('\'');
        if let Some((host, _container)) = spec.split_once(':')
            && let Ok(port) = host.trim().parse::<u16>()
        {
            ports.push(port);
        }
    }
    ports
}

/// Linux hands out 32768-60999 as ephemeral source ports, so any published port
/// in that window can be transiently held by an unrelated outbound connection —
/// including the harness's own polling — and `compose up` then fails to bind it.
/// CI reserves exactly those ports from the ephemeral pool.
///
/// This test is what keeps that list honest: publish a new port in the ephemeral
/// range without reserving it and this fails, naming the port. Without it the
/// sysctl is a comment that rots the first time the topology grows.
#[test]
fn ci_reserves_every_published_port_that_linux_could_hand_out() {
    const EPHEMERAL: std::ops::RangeInclusive<u16> = 32768..=60999;

    let vulnerable: Vec<u16> = published_host_ports()
        .into_iter()
        .filter(|p| EPHEMERAL.contains(p))
        .collect();
    assert!(
        !vulnerable.is_empty(),
        "if no published port is in the ephemeral range any more, delete the \
         sysctl step and this test rather than leaving both to rot"
    );

    for workflow in ["ci.yml", "nightly-chaos.yml"] {
        let path =
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../.github/workflows/").to_owned() + workflow;
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let sets = reserved_port_sets(&text);
        assert!(
            !sets.is_empty(),
            "{workflow} sets no net.ipv4.ip_local_reserved_ports, so the chaos \
             tier's published ports stay in the pool Linux allocates ephemeral \
             source ports from"
        );

        for reserved in &sets {
            for port in &vulnerable {
                assert!(
                    reserved.contains(port),
                    "{workflow} has a reservation that omits {port}, which is \
                     published and inside Linux's ephemeral range — an outbound \
                     connection can hold it and `compose up` will fail to bind it"
                );
            }
        }
    }
}

/// Every `a,b-c,d` value assigned to `net.ipv4.ip_local_reserved_ports` in a
/// workflow, expanded. Empty when the workflow sets it nowhere.
///
/// *Every* occurrence, not the first: the reservation step is already duplicated
/// across two workflows, so a second one in the same file is a matter of time,
/// and pinning only the first would let the rest drift unwatched.
fn reserved_port_sets(workflow: &str) -> Vec<Vec<u16>> {
    workflow
        .split("ip_local_reserved_ports=")
        .skip(1)
        .filter_map(|rest| {
            let value = rest.split_whitespace().next()?;
            let mut ports: Vec<u16> = Vec::new();
            for part in value.split(',') {
                match part.split_once('-') {
                    Some((lo, hi)) => {
                        let lo: u16 = lo.parse().ok()?;
                        let hi: u16 = hi.parse().ok()?;
                        ports.extend(lo..=hi);
                    }
                    None => ports.push(part.parse().ok()?),
                }
            }
            Some(ports)
        })
        .collect()
}

/// `list` must emit one token per line, so its caller can read it into an array.
///
/// Driven from a fixture because nothing is quarantined today: run against the
/// real scenarios file this would assert on empty output and pass whatever the
/// format was. The shape only matters when there *is* a quarantine, which is
/// exactly when nobody is looking at it.
#[test]
fn quarantine_list_emits_one_argument_per_line() {
    // The tag word is assembled at runtime, never written literally, because
    // `chaos-quarantine.sh` scans THIS file too: a literal tag here registers as
    // a real quarantine, and `check` then demands that issues #101 and #102 be
    // open. (It did, on the first draft — "2 quarantined" against a tree with
    // none.) Nothing enforces this but the comment, so: do not inline it.
    let tag = "quarantin";
    let fixture_text = format!(
        "#[tokio::test]\n\
         #[ignore = \"{tag}ed: #101 -- flaky under load\"]\n\
         async fn alpha_scenario() {{}}\n\
         \n\
         #[tokio::test]\n\
         #[ignore = \"needs a container runtime\"]\n\
         async fn not_quarantined() {{}}\n\
         \n\
         #[tokio::test]\n\
         #[ignore = \"{tag}ed: #102 -- known failing\"]\n\
         async fn beta_scenario() {{}}\n"
    );

    let dir = std::env::temp_dir().join("rift-116-quarantine-fixture");
    std::fs::create_dir_all(&dir).expect("fixture dir");
    let fixture = dir.join("scenarios.rs");
    std::fs::write(&fixture, &fixture_text).expect("write fixture");

    let out = std::process::Command::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../scripts/chaos-quarantine.sh"
    ))
    .args(["list", &fixture.to_string_lossy()])
    .output()
    .expect("run chaos-quarantine.sh list");
    assert!(out.status.success(), "list failed: {out:?}");

    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let tokens: Vec<&str> = stdout.lines().collect();

    assert_eq!(
        tokens,
        ["--skip", "alpha_scenario", "--skip", "beta_scenario"],
        "one token per line, quarantined scenarios only"
    );
    assert!(
        !tokens.iter().any(|t| t.contains(' ')),
        "a token containing a space collapses back into the single-string form \
         that issue #116 is about: {tokens:?}"
    );
}

/// The runner must pass its scenario names as an **array**, never as one string.
///
/// `cargo test -- ... "$names"` hands libtest one argument, which it reads as a
/// single filter matching nothing. Issue #116 is the same hazard one argument
/// over: there the collapsed token began with `--` and libtest rejected it
/// outright (exit 101), loudly. Here it would be silent — a filter matching
/// nothing is a green run of zero scenarios — and the only thing between that
/// and a required check certifying it is the floor asserted below.
///
/// Under D-58 the array holds scenario names rather than `--skip` flags: the
/// shard partition drops quarantined scenarios, so they never reach the command
/// line. The unquoted-expansion hazard SC2086 keeps pointing at is unchanged,
/// which is why this still pins the shape rather than the flag.
#[test]
fn the_chaos_runner_expands_its_scenarios_as_an_array() {
    let ci = read_workflow("ci.yml");
    let runner = ci
        .split("Container chaos scenarios")
        .nth(1)
        .expect("ci.yml has no chaos runner step");

    assert!(
        runner.contains("\"${scenarios[@]}\""),
        "the runner must expand the scenario names as a quoted array"
    );
    assert!(
        !runner.contains("--exact ${scenarios[@]}") && !runner.contains("--exact $scenarios"),
        "the unquoted form is back; word-splitting is what makes it appear to \
         work until a name changes"
    );
    assert!(
        runner.contains("mapfile") || runner.contains("readarray"),
        "the names have to be read into an array for the quoted expansion above \
         to mean anything"
    );
}

/// The runner must select its scenarios through the shard partition.
///
/// Two things ride on that call, and neither is visible in the workflow once it
/// is gone. It is what drops **quarantined** scenarios now that the runner
/// passes names instead of `--skip` flags — bypass it and a scenario ignored
/// behind an open issue runs anyway, which is the quarantine convention silently
/// undone. And it is what makes each shard run a *quarter* of the tier: with no
/// partition every shard runs everything, four times over, and passes.
#[test]
fn the_chaos_runner_selects_through_the_shard_partition() {
    let ci = read_workflow("ci.yml");
    let runner = ci
        .split("Container chaos scenarios")
        .nth(1)
        .expect("ci.yml has no chaos runner step");

    assert!(
        runner.contains("chaos-shard.sh"),
        "the runner must partition through chaos-shard.sh — it is what drops \
         quarantined scenarios and what makes a shard a quarter of the tier"
    );
    assert!(
        runner.contains("--ignored --list") || runner.contains("--list"),
        "the scenario names must be DERIVED from libtest's own listing; a list \
         written into the workflow rots the way the nightly's matrix has"
    );
    // libtest reads an empty filter list as "run every test", so an empty array
    // is not an empty shard -- it is the whole tier, in every shard, green. The
    // partition script refuses to emit one, but `mapfile` reading a process
    // substitution does not see its exit status, so the caller has to re-check.
    assert!(
        runner.contains("${#scenarios[@]}\" -eq 0") || runner.contains("${#scenarios[@]} -eq 0"),
        "the runner must refuse an empty scenario array; libtest reads no \
         filters as 'run everything', so an empty shard would silently run the \
         whole tier and still pass"
    );
    assert!(
        runner.contains("--assert \"${#scenarios[@]}\""),
        "the floor must be the shard's OWN count, not 1 — the expected number is \
         known here, so a scenario that was selected and then did not run has to \
         fail rather than quietly shrink the shard"
    );
}

/// The required check must actually judge the shards (D-58).
///
/// `cluster-smoke` is the context `.github/rulesets/master.json` pins, and since
/// D-58 it runs no scenarios of its own — it reads the prepare job's filter
/// verdict and the shards' aggregate result and decides. Replace that call with
/// anything unconditional and the required check passes on a run where every
/// shard was skipped, which is #93 and #101's shared failure shape reached by a
/// third route.
#[test]
fn the_required_check_runs_the_gate() {
    let ci = read_workflow("ci.yml");
    let gate = ci
        .split("\n  cluster-smoke:")
        .nth(1)
        .expect("ci.yml has no cluster-smoke gate job");

    assert!(
        gate.contains("cluster-smoke-gate.sh"),
        "the required check must run the gate; without it the name certifies \
         nothing about whether the tier ran"
    );
    for input in [
        "--want",
        "needs.cluster-smoke-prepare.outputs.run",
        "--shards",
        "needs.cluster-smoke-shard.result",
    ] {
        assert!(
            gate.contains(input),
            "the gate needs `{input}` to tell 'skipped because nothing changed' \
             from 'skipped because something broke'"
        );
    }
}

/// Both tiers must refuse a run in which no scenario actually ran.
///
/// libtest exits 0 when a filter matches nothing, so every way of losing the
/// scenarios — bad skips, a renamed `--exact` target, a broken runner edit —
/// otherwise lands on a green check. Since #104 made `cluster-smoke` required,
/// that green is worse than a failure: the ruleset then certifies what did not
/// run.
#[test]
fn both_tiers_refuse_a_run_that_tested_nothing() {
    for workflow in ["ci.yml", "nightly-chaos.yml"] {
        let text = read_workflow(workflow);
        assert!(
            text.contains("assert-scenarios-ran.sh"),
            "{workflow} does not assert that any scenario ran, so a filter \
             matching nothing would report success"
        );
    }
}

fn read_workflow(name: &str) -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../.github/workflows/").to_owned() + name;
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

/// An all-window leaderless fleet must not pass the transition bound vacuously.
#[test]
fn c6_bound_is_vacuous_on_a_leaderless_fleet() {
    assert_eq!(
        leader_transitions(&[]),
        0,
        "zero samples yields zero transitions, which clears the bound -- C6 \
         therefore has to assert the sequence is non-empty separately"
    );
}

/// A write accepted by one node is servable by every node.
///
/// This is R1, the whole point of config-sync: with the default write barrier a
/// 2xx means the fleet has it, not merely that the leader does.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_config_sync_converges() {
    let _cluster = Cluster::up().await.expect("fleet comes up");

    let (status, headers, _) = put_imposter_with_key(
        NODES[0].admin,
        IMPOSTER_PORT,
        "converged",
        "converge-at-2xx",
    )
    .await
    .expect("admin write");
    assert_eq!(status, 201, "the write must be accepted by rift-1");

    // A barrier that *timed out* also answers 201 -- with a Rift-Cluster-Warnings
    // header naming the nodes that had not applied. Without this check a slow
    // fleet would fail the no-retry assertion below as a bare "did not serve
    // it", which reads as a lost write rather than as a slow barrier. Asserting
    // the header's absence turns that into the precise failure it actually is.
    assert!(
        !headers.contains_key("rift-cluster-warnings"),
        "the write barrier timed out (Rift-Cluster-Warnings: {:?}); the fleet is \
         slow rather than broken, but a 201 no longer means every node applied",
        headers.get("rift-cluster-warnings")
    );

    // At 2xx-return, not eventually. Polling with a timeout here would pass on
    // a fleet whose barrier does nothing at all, because convergence would
    // arrive on its own a moment later -- the scenario would then be asserting
    // eventual consistency while claiming to prove read-your-write. So every
    // node is asked exactly once, with no retry: the only thing between the
    // 201 and the question is one HTTP round trip.
    let want = u64::from(IMPOSTER_PORT);
    for node in &NODES {
        let ports = imposter_ports(node.admin)
            .await
            .unwrap_or_else(|e| panic!("read imposters from {}: {e}", node.name));
        assert!(
            ports.contains(&want),
            "{} did not serve {want} at the moment the write returned 201 -- \
             with --cluster-write-barrier=ready-nodes a 2xx means the fleet has \
             it, not merely that the leader does (R1)",
            node.name
        );
    }
}

/// R1's other half: with the barrier off, a 2xx promises less — so convergence
/// has to be *fast* instead of immediate.
///
/// Separated from the scenario above rather than folded into it, because the
/// two assert different contracts. With the barrier on, "eventually" is a bug;
/// with it off, "eventually" is the contract and the only question is the
/// bound. Running both is what stops the barrier from being a no-op that
/// nothing would notice.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_config_sync_converges_without_barrier() {
    let _cluster = Cluster::up_with_barrier_none()
        .await
        .expect("fleet comes up with the write barrier off");

    let started = std::time::Instant::now();
    let status = put_imposter(NODES[0].admin, IMPOSTER_PORT, "unbarriered")
        .await
        .expect("admin write");
    assert_eq!(status, 201, "the write must be accepted by rift-1");

    wait_converged(u64::from(IMPOSTER_PORT), UNBARRIERED_CONVERGE_BOUND)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "with --cluster-write-barrier=none the fleet must still converge \
                 within {UNBARRIERED_CONVERGE_BOUND:?}, measured from the write: {e}"
            )
        });

    let elapsed = started.elapsed();
    assert!(
        elapsed <= UNBARRIERED_CONVERGE_BOUND,
        "converged, but in {elapsed:?} -- past the {UNBARRIERED_CONVERGE_BOUND:?} bound"
    );
}

/// A node killed outright rejoins and catches up on what it missed.
///
/// `kill -9`, not a stop: the node gets no chance to leave, so this is the
/// dead-peer path rather than the graceful one.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_node_rejoin() {
    let cluster = Cluster::up().await.expect("fleet comes up");
    let survivors: Vec<_> = NODES.iter().filter(|n| n.name != "rift-2").collect();

    cluster.kill("rift-2").expect("kill rift-2");

    // The survivors keep taking writes while it is gone — a dead follower must
    // not cost the cluster its quorum.
    let status = put_imposter(NODES[0].admin, IMPOSTER_PORT, "written-while-down")
        .await
        .expect("admin write survives a dead follower");
    assert_eq!(status, 201);
    wait_converged_on(&survivors, u64::from(IMPOSTER_PORT), CONVERGE_TIMEOUT)
        .await
        .expect("the two survivors converge without the third");

    cluster.start("rift-2").expect("restart rift-2");
    cluster
        .wait_all_ready(Duration::from_secs(90))
        .await
        .expect("the killed node comes back ready");

    wait_converged(u64::from(IMPOSTER_PORT), CONVERGE_TIMEOUT)
        .await
        .expect("the rejoined node catches up on the write it missed");
}

/// SIGTERM removes the node from the membership, not just from the balancer.
///
/// This is the container proof of issue #6: the graceful leave has to be
/// answered by a real signal handler in a real process, and the *survivors*
/// have to observe the voter set shrink. In-process tests cannot show that the
/// signal path is wired at all.
// multi_thread because the write ladder below must be polled while the main
// task is blocked inside a synchronous `docker compose stop`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a container runtime"]
async fn test_graceful_leave() {
    let cluster = Cluster::up().await.expect("fleet comes up");
    let survivors: Vec<_> = NODES.iter().filter(|n| n.name != "rift-3").collect();

    wait_voters(&NODES[0], 3.0, CONVERGE_TIMEOUT)
        .await
        .expect("three voters before the leave, or the assertion after proves nothing");

    // Drive writes *across* the leave rather than after it: a leave drops an
    // acknowledged write, if it drops one at all, in the window where
    // membership is changing. A scenario that writes once the dust has settled
    // cannot see the failure it exists to catch.
    //
    // Each rung takes its own port, so "was this acknowledged write kept?" is
    // answered per write by asking whether the port is served. The config
    // revision cannot answer it: `rift_cluster_config_revision` is the Raft log
    // index that last wrote a config -- global and monotone, already past the
    // ladder's length from bootstrap and bumped by the leave's own membership
    // change -- so `revision >= acked` would hold even if most rungs vanished.
    let writer = tokio::spawn(async move {
        let mut acked = Vec::new();
        let mut errors = Vec::new();
        for rung in 0..LADDER_RUNGS {
            let port = LADDER_BASE_PORT + rung;
            match put_imposter_with_key(
                NODES[0].admin,
                port,
                "rung",
                &format!("leave-ladder-{rung}"),
            )
            .await
            {
                Ok((201, _, _)) => acked.push(u64::from(port)),
                // 503/504 with an op-id is the documented degraded answer while
                // membership changes: the write was never acknowledged, so it
                // is not something the fleet promised to keep.
                Ok((503 | 504, _, _)) => {}
                Ok((other, _, _)) => errors.push(format!("rung {rung}: status {other}")),
                Err(e) => errors.push(format!("rung {rung}: {e}")),
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        (acked, errors)
    });

    // Stop mid-ladder so the leave lands while writes are in flight.
    //
    // `stop` is synchronous -- it shells out to `docker compose` and does not
    // return until the drain completes. On the default current-thread runtime
    // that would block the whole runtime, so the ladder could not be polled
    // during exactly the window this scenario exists to cover, and the writes
    // would all land before or after the leave. The `multi_thread` flavour on
    // this test is what keeps the writer on another worker; it is load-bearing,
    // not decoration.
    tokio::time::sleep(Duration::from_millis(600)).await;
    cluster.stop("rift-3").expect("SIGTERM rift-3");

    wait_voters(&NODES[0], 2.0, CONVERGE_TIMEOUT)
        .await
        .expect("a graceful leave must shrink the voter set the survivors see");

    let (acked, errors) = writer.await.expect("the write ladder task ran");
    assert!(
        errors.is_empty(),
        "survivors returned data-plane errors across a graceful leave: {errors:?}"
    );
    assert!(
        acked.len() >= LADDER_RUNGS as usize / 2,
        "only {} of {LADDER_RUNGS} rungs were acknowledged; the ladder did not \
         really run across the leave",
        acked.len()
    );

    // Every acknowledged write is still served by both survivors. This is the
    // "zero lost acknowledged writes" the table asks for, per write.
    for port in &acked {
        wait_converged_on(&survivors, *port, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| {
                panic!("write to port {port} was acknowledged and then lost across the leave: {e}")
            });
    }
    wait_revisions_agree_on(&survivors, acked[0], CONVERGE_TIMEOUT)
        .await
        .expect("survivors disagree on the config revision after the leave");
}

/// A full-fleet restart restores configuration from disk.
///
/// The redb state directories survive the stop, so this proves durability
/// through real process exit and re-open — not through an in-process handle
/// that was never actually closed.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_cold_start() {
    let cluster = Cluster::up().await.expect("fleet comes up");

    let status = put_imposter(NODES[0].admin, IMPOSTER_PORT, "durable")
        .await
        .expect("admin write");
    assert_eq!(status, 201);
    wait_converged(u64::from(IMPOSTER_PORT), CONVERGE_TIMEOUT)
        .await
        .expect("converges before the restart");

    for node in &NODES {
        cluster.kill(node.name).expect("kill the whole fleet");
    }
    for node in &NODES {
        cluster.start(node.name).expect("restart the fleet");
    }
    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the fleet comes back from cold");

    wait_converged(u64::from(IMPOSTER_PORT), CONVERGE_TIMEOUT)
        .await
        .expect("configuration survives a full-cluster restart");
}

/// The other half of cold start: a fleet whose state directories are empty
/// comes back **empty**, not with yesterday's config.
///
/// Without this, `test_cold_start` is nearly vacuous — it would pass just the
/// same if the config were being restored from the image, from a stray
/// `--datadir` write-through, or from anything else that outlives a container.
/// Wiping the volumes is what makes the restore in that scenario attributable
/// to redb: same restart, same fleet, only the durable state removed, and the
/// config must then be gone.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn empty_state_dirs_cold_start_empty() {
    let want = u64::from(IMPOSTER_PORT);

    {
        let cluster = Cluster::up().await.expect("fleet comes up");
        let status = put_imposter(NODES[0].admin, IMPOSTER_PORT, "should-not-survive")
            .await
            .expect("admin write");
        assert_eq!(status, 201);
        wait_converged(want, CONVERGE_TIMEOUT)
            .await
            .expect("converges before the wipe, or the wipe proves nothing");
        drop(cluster);
    }

    // What wipes the state is container *destruction*: the compose file
    // declares no volumes, so each node's state dir lives in its container's
    // writable layer and `down` takes it with the container. That is precisely
    // the difference from `test_cold_start`, which kills and restarts the same
    // containers and so keeps its state dirs.
    let _cluster = Cluster::up().await.expect("fleet comes back up empty");

    for node in &NODES {
        let ports = imposter_ports(node.admin)
            .await
            .unwrap_or_else(|e| panic!("read imposters from {}: {e}", node.name));
        assert!(
            !ports.contains(&want),
            "{} served {want} after its state directory was wiped -- the config \
             came from somewhere other than redb, which means test_cold_start \
             was not proving durability",
            node.name
        );
    }
}

/// C14: killing the leader mid-write loses no acknowledged write.
///
/// Every write that returned 2xx before the kill must still be present after a
/// new leader settles — an acknowledgement the cluster later forgets is the
/// worst failure this system can have.
// multi_thread because the storm must keep being polled while the main task is
// blocked inside a synchronous `docker kill`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a container runtime"]
async fn c14_leader_kill_keeps_every_acknowledged_write() {
    let cluster = Cluster::up().await.expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("exactly one leader");

    // A 100-write storm that is genuinely *in flight* when the leader dies.
    //
    // Issuing the writes sequentially and awaiting each would settle every one
    // through the barrier before the kill, so the storm would only ever
    // exercise the quiet path -- 100 settled writes where there used to be 5,
    // and still nothing in the window that matters. Driving them concurrently
    // from a spawned task, and killing partway through, is what puts writes
    // mid-commit when the leader goes away.
    let leader_admin = NODES[leader].admin;
    let storm = tokio::spawn(async move {
        let mut inflight = JoinSet::new();
        for offset in 0..C14_STORM_WRITES {
            let port = IMPOSTER_PORT + offset;
            inflight.spawn(async move { (port, put_imposter(leader_admin, port, "storm").await) });
            // A trickle rather than a thundering herd: 100 simultaneous
            // connections would mostly measure the admin listener's accept
            // queue rather than the write path.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut acked = Vec::new();
        while let Some(joined) = inflight.join_next().await {
            if let Ok((port, Ok(201))) = joined {
                acked.push(u64::from(port));
            }
        }
        acked
    });

    // Kill partway through the storm, not after it.
    tokio::time::sleep(Duration::from_millis(700)).await;
    cluster
        .kill(NODES[leader].name)
        .expect("kill the leader outright");

    let survivors: Vec<_> = NODES
        .iter()
        .filter(|n| n.name != NODES[leader].name)
        .collect();
    let survivor_admin = survivors[0].admin;

    // Failover speed, as a client experiences it. See FAILOVER_WRITE_BOUND for
    // why this is a write rather than the leader gauge or /_cluster/members,
    // and why the budget is larger than the issue's bare 3s.
    let resumed = time_until_writes_resume(
        survivor_admin,
        IMPOSTER_PORT + C14_STORM_WRITES + 1,
        "post-kill",
    )
    .await
    .unwrap_or_else(|e| {
        panic!("the fleet never accepted a write after the leader was killed: {e}")
    });
    assert!(
        resumed <= FAILOVER_WRITE_BOUND,
        "writes resumed only after {resumed:?} following a leader kill, past the \
         {FAILOVER_WRITE_BOUND:?} budget (election + barrier timeout) -- this is \
         the window in which the front door sheds traffic"
    );

    let acknowledged = storm.await.expect("the storm task ran");
    assert!(
        !acknowledged.is_empty(),
        "no write in the storm was acknowledged, so the scenario proves nothing"
    );

    // Every write acknowledged before or during the kill is still there.
    tokio::time::timeout(CONVERGE_TIMEOUT, async {
        loop {
            if let Ok(ports) = imposter_ports(survivor_admin).await
                && acknowledged.iter().all(|p| ports.contains(p))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "a write acknowledged around the leader's death was lost: {} ports \
             were acknowledged and the survivor never listed them all",
            acknowledged.len()
        )
    });

    // The table's "zero duplicates" is discharged by construction rather than
    // by an assertion here: the admin API renders imposters from a port-keyed
    // map, so one port appearing twice is unrepresentable in the response and
    // a count would always find exactly one -- it could never fail. What *can*
    // show a double-apply is the revision: a replayed intent applied twice
    // would leave the survivors disagreeing on a stormed port.
    wait_revisions_agree_on(&survivors, acknowledged[0], CONVERGE_TIMEOUT)
        .await
        .expect("survivors disagree on a stormed port's revision after failover");

    for node in &survivors {
        assert_eq!(
            probe(node.probe, "/healthz")
                .await
                .expect("probe reachable"),
            200,
            "{} stopped serving after the leader died",
            node.name
        );
    }
}

/// C15: `kill -9` the whole fleet under load; nothing acknowledged is lost.
///
/// The difference from `test_cold_start` is the absence of any cooperation —
/// no drain, no leave, no flush. Whatever was acknowledged was durable at the
/// moment it was acknowledged, or it was never really acknowledged.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c15_hard_kill_of_the_whole_fleet_keeps_acknowledged_writes() {
    let cluster = Cluster::up().await.expect("fleet comes up");

    let mut acknowledged = Vec::new();
    for offset in 0..5 {
        let port = IMPOSTER_PORT + offset;
        if put_imposter(NODES[0].admin, port, "pre-hard-kill")
            .await
            .is_ok_and(|s| s == 201)
        {
            acknowledged.push(u64::from(port));
        }
    }
    assert!(!acknowledged.is_empty(), "nothing was acknowledged");

    for node in &NODES {
        cluster.kill(node.name).expect("hard-kill the fleet");
    }
    for node in &NODES {
        cluster.start(node.name).expect("restart the fleet");
    }
    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the fleet comes back after a hard kill");

    for port in &acknowledged {
        wait_converged(*port, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("imposter {port} was acknowledged and then lost: {e}"));
    }
}

/// C5: a rolling SIGTERM restart keeps the cluster serving throughout.
///
/// This is the shape a real deploy takes, and the one the graceful leave from
/// issue #6 exists for: each node leaves, restarts, and rejoins while the other
/// two hold quorum. The bar is that a write is accepted at every point in the
/// roll — a window where the fleet takes nothing is an outage, however brief.
///
/// This scenario was committed failing, as the reproduction for issue #72: a
/// node that gracefully left could not rejoin when it restarted with its state
/// directory intact, because `join_or_bootstrap` resumed on `is_initialized()`
/// alone. Fixed by the departure marker and the membership check, so it now
/// guards that fix rather than reporting the bug.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c5_rolling_restart_never_stops_accepting_writes() {
    let cluster = Cluster::up().await.expect("fleet comes up");

    for (i, rolled) in NODES.iter().enumerate() {
        // Whether this roll takes the leader down decides what the timing
        // below means, so establish it before the stop rather than inferring
        // it after.
        let leader_before = wait_single_leader(CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("no settled leader before rolling {}: {e}", rolled.name));
        let rolling_the_leader = NODES[leader_before].name == rolled.name;

        cluster.stop(rolled.name).expect("SIGTERM the node");

        // Write through a node that is NOT the one being rolled, so this
        // measures the cluster's availability rather than one node's.
        let other = &NODES[(i + 1) % NODES.len()];
        let port = IMPOSTER_PORT + u16::try_from(i).expect("three nodes fit in a u16");
        // Asserted as zero interruption rather than as a recovery bound. A
        // graceful leave hands leadership over *during* the drain, and the
        // drain happens inside the synchronous `stop` above -- so by the time
        // any timer here could start, a leader already exists and a "recovered
        // within N seconds" bound would be satisfied by one HTTP round trip no
        // matter how bad the handover was. The stronger and actually
        // measurable claim is that the very first write after the leave is
        // accepted, with no retry at all.
        let status = put_imposter(other.admin, port, "mid-roll")
            .await
            .unwrap_or_else(|e| panic!("no write accepted while {} was down: {e}", rolled.name));
        assert_eq!(
            status,
            201,
            "the first write after {} left was answered {status}; a graceful \
             leave transfers leadership before the process exits, so the fleet \
             should never have stopped accepting writes at all{}",
            rolled.name,
            if rolling_the_leader {
                " (this roll took the leader)"
            } else {
                " (this roll took a follower, which should be invisible)"
            }
        );

        cluster.start(rolled.name).expect("bring the node back");
        cluster
            .wait_all_ready(Duration::from_secs(90))
            .await
            .unwrap_or_else(|e| panic!("{} did not rejoin after its roll: {e}", rolled.name));

        wait_voters(other, 3.0, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("voter set did not recover after {}: {e}", rolled.name));
    }

    // Everything written during the roll is present everywhere at the end.
    for i in 0..NODES.len() {
        let port = u64::from(IMPOSTER_PORT) + i as u64;
        wait_converged(port, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("a write taken mid-roll was lost: {e}"));
    }
}

/// Issues #69 and #72 together: a graceful stop of the whole fleet, then a cold
/// start, converges on its own.
///
/// This is the composed invariant the two issues share, and neither one proves
/// it alone. A `docker compose stop` SIGTERMs every node, so each one in turn
/// tries to leave: #69's voter floor is what stops that walking the membership
/// down to a single authoritative volume, and #72's marker-and-rejoin is what
/// gets the node that *did* depart back in afterwards. Get either half wrong
/// and the fleet either cold-starts on one voter or never re-forms at all.
///
/// Deliberately a graceful stop rather than C15's hard kill: a kill leaves the
/// membership untouched, so it exercises none of this.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn whole_fleet_sigterm_then_cold_start_converges() {
    let cluster = Cluster::up().await.expect("fleet comes up");

    let port = IMPOSTER_PORT;
    let status = put_imposter(NODES[0].admin, port, "pre-teardown")
        .await
        .expect("write before the teardown");
    assert_eq!(status, 201, "the pre-teardown write must be acknowledged");
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the pre-teardown write converges");

    // SIGTERM every node, in order, exactly as `docker compose stop` does.
    for node in &NODES {
        cluster.stop(node.name).expect("SIGTERM the node");
    }
    for node in &NODES {
        cluster.start(node.name).expect("restart the node");
    }

    // Generous: a node whose departure landed rejoins through its seeds, and it
    // may need a restart-policy retry if it boots before any quorum exists.
    cluster
        .wait_all_ready(Duration::from_secs(180))
        .await
        .expect("the whole fleet must come back after a graceful stop");

    for node in &NODES {
        wait_voters(node, 3.0, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("{} did not converge on 3 voters: {e}", node.name));
    }
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the write taken before the teardown must survive it");
}

// ---------------------------------------------------------------------------
// Slice 2 (issue #73): the scenarios that need degraded links, a real network
// partition, and a front door. All of these run on the chaos overlay, so every
// cluster link transits toxiproxy and every node also sits on `mgmt`.
// ---------------------------------------------------------------------------

/// C4 — a partitioned minority parks its writes and replays them on heal.
///
/// The property is that a write to a node that cannot reach a leader is neither
/// served nor lost: it is refused with a receipt (`rift-cluster-op-id`), parked
/// durably, and replayed when a leader comes back — and a duplicate of it that
/// reached the majority meanwhile collapses instead of applying twice.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c4_partition_parks_minority_writes_and_replays_on_heal() {
    let cluster = Cluster::up_with_chaos().await.expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles");

    // Partition a follower. Isolating the leader would be a different scenario
    // (the majority elects a new one); this one is about the minority side.
    let minority = NODES
        .iter()
        .enumerate()
        .find(|(i, _)| *i != leader)
        .map(|(_, n)| n)
        .expect("a non-leader exists");
    let majority: Vec<_> = NODES.iter().filter(|n| n.name != minority.name).collect();

    let first = 6001_u16;
    assert_eq!(
        put_imposter(majority[0].admin, first, "before")
            .await
            .expect("admin write"),
        201,
        "the pre-partition write must be accepted"
    );
    wait_converged(u64::from(first), CONVERGE_TIMEOUT)
        .await
        .expect("the fleet converges before anything is broken");

    cluster
        .partition(minority.name)
        .expect("cut the minority off");

    // The whole scenario depends on this: `mgmt` must keep the isolated node
    // reachable from the host, or none of the assertions below can be made at
    // all. Fail loudly and specifically rather than as a confusing timeout.
    wait_admin_reachable(minority.admin_via_mgmt, Duration::from_secs(30))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "{} must stay assertable over `mgmt` while partitioned ({e})",
                minority.name
            )
        });

    let parked = 6002_u16;
    let key = "c4-duplicate-key";
    let (status, headers, envelope) =
        put_imposter_with_key(minority.admin_via_mgmt, parked, "parked", key)
            .await
            .expect("the minority answers rather than hanging");
    // Both are correct parks: no reachable quorum answers 503 immediately, and
    // a forward that hangs to the write deadline answers 504. Either way the
    // intent is durable and the receipt is the proof.
    assert!(
        status == 503 || status == 504,
        "a minority write must be refused, got {status}"
    );
    assert!(
        headers.contains_key("rift-cluster-op-id"),
        "a refused write must carry the op-id that proves it was parked"
    );
    let slug = envelope["errors"][0]["type"]
        .as_str()
        .or_else(|| envelope["type"].as_str())
        .unwrap_or_default()
        .to_owned();
    assert!(
        slug == "unavailable" || slug == "timeout",
        "the error envelope must name the typed slug, got {slug:?} from {envelope}"
    );

    let pending = metric(minority.metrics_via_mgmt, "rift_cluster_intents_pending")
        .await
        .expect("the intent gauge is published");
    assert!(
        pending >= 1.0,
        "the parked intent must be pending, got {pending}"
    );

    // The same write, same key, through the majority: this is the duplicate
    // that dedup has to collapse when the parked copy replays.
    let (dup_status, _, _) = put_imposter_with_key(majority[0].admin, parked, "parked", key)
        .await
        .expect("the majority is still writable");
    assert_eq!(dup_status, 201, "the majority must still accept writes");

    cluster.heal(minority).expect("heal the partition");

    wait_converged(u64::from(parked), CONVERGE_TIMEOUT)
        .await
        .expect("the parked write must be present fleet-wide after heal");

    // The parked copy drains, and the fleet agrees on one applied revision for
    // the port -- which is what "the duplicate collapsed" looks like from
    // outside. Two applications would leave different revisions behind.
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let pending = metric(minority.metrics_via_mgmt, "rift_cluster_intents_pending")
            .await
            .unwrap_or(f64::INFINITY);
        if pending == 0.0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the parked intent never drained: pending={pending}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let dedup = metric(NODES[leader].metrics, "rift_cluster_dedup_hits_total")
        .await
        .expect("the dedup counter is published");
    assert!(
        dedup >= 1.0,
        "the replayed duplicate must have been collapsed, dedup_hits={dedup}"
    );

    for port in [first, parked] {
        wait_revisions_agree(u64::from(port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("nodes disagree on port {port} after heal: {e}"));
    }

    // Exactly one imposter for the deduplicated port, on every node.
    for node in &NODES {
        let ports = imposter_ports(node.admin).await.expect("list imposters");
        let copies = ports.iter().filter(|p| **p == u64::from(parked)).count();
        assert_eq!(
            copies, 1,
            "{} has {copies} copies of the deduplicated write",
            node.name
        );
    }
}

/// C6 — heavy latency, jitter and connection resets do not flap membership or
/// lose an acknowledged write.
///
/// Toxiproxy is an L4 proxy and cannot drop packets, so "lossy" is modelled as
/// latency+jitter on every byte plus `reset_peer` on a third of connections —
/// the TCP-level analogue of loss bursts.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c6_loss_and_jitter_do_not_flap_or_lose_writes() {
    let cluster = Cluster::up_with_chaos().await.expect("fleet comes up");
    wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles before the links degrade");

    for node in &NODES {
        add_toxic(
            node.proxy,
            serde_json::json!({
                "type": "latency",
                "stream": "upstream",
                "toxicity": 1.0,
                "attributes": { "latency": 100, "jitter": 100 }
            }),
        )
        .await
        .expect("add upstream latency");
        add_toxic(
            node.proxy,
            serde_json::json!({
                "type": "latency",
                "stream": "downstream",
                "toxicity": 1.0,
                "attributes": { "latency": 100, "jitter": 100 }
            }),
        )
        .await
        .expect("add downstream latency");
        add_toxic(
            node.proxy,
            serde_json::json!({
                "type": "reset_peer",
                "stream": "upstream",
                "toxicity": 0.3,
                "attributes": { "timeout": 0 }
            }),
        )
        .await
        .expect("add connection resets");
    }

    // Confirm the links are actually degraded before concluding anything from
    // the stability that follows. Without this the scenario passes just as
    // happily against a cluster nobody perturbed -- "no flapping under load"
    // asserted over an untouched fleet.
    for node in &NODES {
        let attached = toxic_count(node.proxy)
            .await
            .unwrap_or_else(|e| panic!("read toxics on {}: {e}", node.proxy));
        assert_eq!(
            attached, 3,
            "{} should carry latency-up, latency-down and reset_peer; the toxic \
             window means nothing if they did not land",
            node.proxy
        );
    }

    // Every port whose write was acknowledged. Only these are asserted on: a
    // write refused with a park receipt is allowed to be absent until it
    // replays, and asserting on it here would be asserting the wrong contract.
    let mut acked: Vec<u16> = Vec::new();
    let window = Duration::from_secs(60);
    let started = std::time::Instant::now();
    let mut next_write = std::time::Instant::now();
    let mut leader_samples: Vec<usize> = Vec::new();

    while started.elapsed() < window {
        for node in &NODES {
            let voters = metric(node.metrics, r#"rift_cluster_members{state="voter"}"#)
                .await
                .unwrap_or(f64::NAN);
            assert_eq!(
                voters, 3.0,
                "{} saw the voter set change to {voters} under load -- membership \
                 must not flap just because links are slow",
                node.name
            );
        }

        // Who currently claims leadership. The gauge is resampled on a 5s timer,
        // so this bounds resolution rather than catching every transition; it is
        // enough to catch a fleet that is re-electing continuously.
        let mut leaders = Vec::new();
        for (i, node) in NODES.iter().enumerate() {
            if metric(node.metrics, r#"rift_cluster_members{state="leader"}"#)
                .await
                .is_ok_and(|v| v == 1.0)
            {
                leaders.push(i);
            }
        }
        if let [only] = leaders[..]
            && leader_samples.last() != Some(&only)
        {
            leader_samples.push(only);
        }

        if std::time::Instant::now() >= next_write {
            let port = 6100 + acked.len() as u16;
            if let Ok((status, _, _)) =
                put_imposter_with_key(NODES[0].admin, port, "under-load", &format!("c6-{port}"))
                    .await
                && status == 201
            {
                acked.push(port);
            }
            next_write = std::time::Instant::now() + Duration::from_secs(5);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Ordered before the rate bound: zero samples yields zero transitions, so a
    // fleet that never had a leader at all would clear the bound vacuously.
    assert!(
        !leader_samples.is_empty(),
        "no node ever reported leadership during the toxic window -- the \
         transition bound would pass vacuously over a leaderless fleet"
    );

    let transitions = leader_transitions(&leader_samples);
    assert!(
        transitions <= C6_MAX_LEADER_TRANSITIONS,
        "leadership changed {transitions} times in the toxic window (sequence \
         {leader_samples:?}); occasional near-threshold elections are in spec \
         under C6's jitter, but at ~5s sampling a continuously re-electing \
         fleet shows 8+ -- this rate means the fleet is flapping"
    );
    assert!(
        !acked.is_empty(),
        "no write was acknowledged during the toxic window -- the scenario proved nothing"
    );

    for node in &NODES {
        clear_toxics(node.proxy).await.expect("clear toxics");
    }

    for port in &acked {
        wait_converged(u64::from(*port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("acknowledged write {port} was lost: {e}"));
        wait_revisions_agree(u64::from(*port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("nodes disagree on port {port} after healing: {e}"));
    }

    for node in &NODES {
        // A scrape failure is not evidence of zero. `unwrap_or(0.0)` under an `== 0.0` assertion
        // makes an unreachable node — crashed, partitioned — read as a clean pass, which is the
        // one direction this check must never fail in.
        let pending = metric(node.metrics, "rift_cluster_intents_pending")
            .await
            .unwrap_or_else(|e| panic!("{}: metrics endpoint did not answer: {e}", node.name));
        assert_eq!(pending, 0.0, "{} still has parked intents", node.name);
    }
    drop(cluster);
}

/// C7 — a node joining with an empty state directory serves nothing until its
/// reconciliation gate opens, then serves exactly what everyone else does.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c7_joining_node_serves_nothing_until_reconciled() {
    let cluster = Cluster::up_with_chaos().await.expect("fleet comes up");

    let port = 6200_u16;
    assert_eq!(
        put_imposter(NODES[0].admin, port, "reconciled")
            .await
            .expect("admin write"),
        201
    );
    // Several more, so the joiner has real reconciliation work to do. With a
    // single imposter the gated window can close faster than it can be
    // observed, and the scenario then fails on its own "never saw the gate"
    // guard rather than on anything about the product.
    for extra in 1..5 {
        assert_eq!(
            put_imposter(NODES[0].admin, port + extra, "reconciled")
                .await
                .expect("admin write"),
            201
        );
    }
    wait_converged(u64::from(port + 4), CONVERGE_TIMEOUT)
        .await
        .expect("the fleet converges before the join");

    let joiner = &NODES[2];
    cluster
        .recreate(joiner.name)
        .expect("replace the node with an empty one");

    // Catch it before its gate opens. The observation is required rather than
    // best-effort: if the gated state is never seen, the scenario never watched
    // a join and would pass just as happily against a node with no gate at all.
    //
    // Two things keep that window catchable. The poll is cheap and tight --
    // reading `/readyz` only -- because a `docker exec` costs a few hundred ms
    // and doing one per poll burns the very window being watched. And the probe
    // runs once, on the first gated reading, rather than every time.
    // What must never happen is a node answering the data plane out of state it
    // does not have. Note this is NOT the same as "does not serve until ready":
    // a node legitimately binds its imposters *before* flipping the readiness
    // gate -- that ordering is the safe one, since the reverse would have a load
    // balancer routing to ports that are not listening yet. Asserting on the
    // gate alone therefore fails intermittently on correct behaviour.
    //
    // `rift_cluster_config_revision{port}` is the precise question: it is absent
    // until this node has applied that config. Serving while it is absent means
    // answering out of empty state, which is the actual defect C7 guards.
    let mut saw_gated = false;
    let mut served_unapplied = false;
    let mut probed = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    while std::time::Instant::now() < deadline {
        match get_json(joiner.probe, "/readyz").await {
            Ok((503, body)) if body["pending"].to_string().contains("cluster-reconciled") => {
                saw_gated = true;
                let unapplied = config_revision(joiner.metrics, u64::from(port))
                    .await
                    .is_err();
                if unapplied && !probed {
                    probed = true;
                    let served = exec_probe(joiner.name, &format!("http://127.0.0.1:{port}/"));
                    // Re-read afterwards: the probe costs a few hundred ms, long
                    // enough for the node to apply the config underneath it, and
                    // a node that applied mid-probe is allowed to serve.
                    let still_unapplied = config_revision(joiner.metrics, u64::from(port))
                        .await
                        .is_err();
                    served_unapplied = served && still_unapplied;
                }
            }
            Ok((200, _)) => break,
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !served_unapplied,
        "{} answered on the data plane for a config it had not applied -- a \
         joining node must not serve out of empty state",
        joiner.name
    );
    assert!(
        saw_gated,
        "never observed {} gated on `cluster-reconciled` -- the scenario did not \
         witness a join and would pass even if the gate did not exist",
        joiner.name
    );

    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the joiner becomes ready");

    assert!(
        exec_probe(joiner.name, &format!("http://127.0.0.1:{port}/")),
        "{} must serve the imposter once reconciled",
        joiner.name
    );
    wait_revisions_agree(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the joiner lands on the same applied revision as everyone else");
}

/// Applying a config must not rebuild imposters it did not change.
///
/// Recorded requests are the visible proxy for that: they live in the running
/// imposter, so a sibling write that recreated it would reset the count to
/// zero. This is the incrementality property, asserted from outside.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_reconcile_preserves_state() {
    let cluster = Cluster::up_with_chaos().await.expect("fleet comes up");
    let port = 6300_u16;

    let body = serde_json::json!({
        "port": port,
        "protocol": "http",
        "recordRequests": true,
        "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": "recorded" } }] }]
    });
    let status = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/imposters", NODES[0].admin))
        .timeout(Duration::from_secs(30))
        .json(&body)
        .send()
        .await
        .expect("create the recording imposter")
        .status()
        .as_u16();
    assert_eq!(status, 201);
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the recording imposter converges");

    assert!(
        exec_probe(NODES[0].name, &format!("http://127.0.0.1:{port}/")),
        "the imposter must answer on the data plane"
    );

    let recorded = |admin: u16| async move {
        get_json(admin, &format!("/imposters/{port}"))
            .await
            .map(|(_, b)| b["numberOfRequests"].as_u64().unwrap_or(0))
    };
    let before = recorded(NODES[0].admin).await.expect("read the imposter");
    assert_eq!(before, 1, "the data-plane request must have been recorded");

    // A sibling write: it changes the config set, but not this imposter.
    let sibling = 6301_u16;
    assert_eq!(
        put_imposter(NODES[1].admin, sibling, "sibling")
            .await
            .expect("sibling write"),
        201
    );
    wait_converged(u64::from(sibling), CONVERGE_TIMEOUT)
        .await
        .expect("the sibling converges");

    let after = recorded(NODES[0].admin).await.expect("read the imposter");
    assert_eq!(
        after, before,
        "the sibling write recreated the untouched imposter -- recorded requests \
         went from {before} to {after}"
    );

    for node in &NODES {
        // `unwrap_or(0.0)` is correct here and must stay — unlike the `intents_pending` check
        // above, which reads an unlabelled `Gauge` that is always emitted. `bind_failures` is a
        // `GaugeVec{port}`, and `observe_apply_failures` `reset()`s it before setting the failing
        // ports, so a healthy node publishes **no series at all** for `port="0"` and `metric()`
        // reports the family as absent. Absence is the domain-optional "no failures" answer, not a
        // swallowed error. (Treating it as one was tried, and turned this into a hard failure on a
        // perfectly healthy fleet.)
        let failures = metric(node.metrics, r#"rift_cluster_bind_failures{port="0"}"#)
            .await
            .unwrap_or(0.0);
        assert_eq!(failures, 0.0, "{} reported a bind failure", node.name);
    }
    drop(cluster);
}

/// Reordering an imposter's stubs propagates fleet-wide and preserves state.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_reconcile_reorder() {
    let cluster = Cluster::up_with_chaos().await.expect("fleet comes up");
    let port = 6400_u16;

    let body = serde_json::json!({
        "port": port,
        "protocol": "http",
        "recordRequests": true,
        "stubs": [
            { "responses": [{ "is": { "statusCode": 200, "body": "first" } }] },
            { "responses": [{ "is": { "statusCode": 201, "body": "second" } }] }
        ]
    });
    let status = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/imposters", NODES[0].admin))
        .timeout(Duration::from_secs(30))
        .json(&body)
        .send()
        .await
        .expect("create the two-stub imposter")
        .status()
        .as_u16();
    assert_eq!(status, 201);
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the imposter converges");
    assert!(exec_probe(
        NODES[0].name,
        &format!("http://127.0.0.1:{port}/")
    ));

    let reversed = serde_json::json!([
        { "responses": [{ "is": { "statusCode": 201, "body": "second" } }] },
        { "responses": [{ "is": { "statusCode": 200, "body": "first" } }] }
    ]);
    let status = put_stubs(NODES[0].admin, port, reversed)
        .await
        .expect("reorder the stubs");
    assert_eq!(status, 200, "the stub replacement must be accepted");

    // Every node must show the new order -- read from the admin API, so this is
    // the committed config rather than one node's local memory.
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut agreed = 0;
        for node in &NODES {
            if let Ok((_, body)) = get_json(node.admin, &format!("/imposters/{port}")).await
                && body["stubs"][0]["responses"][0]["is"]["body"].as_str() == Some("second")
            {
                agreed += 1;
            }
        }
        if agreed == NODES.len() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "only {agreed}/{} nodes show the reordered stubs",
            NODES.len()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let (_, body) = get_json(NODES[0].admin, &format!("/imposters/{port}"))
        .await
        .expect("read the imposter");
    assert_eq!(
        body["numberOfRequests"].as_u64().unwrap_or(0),
        1,
        "reordering stubs must not rebuild the imposter and lose its recorded requests"
    );
    wait_revisions_agree(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the reorder lands at the same revision everywhere");
    drop(cluster);
}

/// A node whose seeds are unreachable stays live but never ready, and never
/// accepts a write.
///
/// The failure this guards against is a node that gives up joining and quietly
/// founds a cluster of one — which would look healthy and serve divergent state.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_no_seeds_not_ready() {
    // rift-2 seeds from rift-1, which is not running.
    let _cluster = Cluster::up_isolated("rift-2")
        .await
        .expect("start one node");
    let node = &NODES[1];

    // Give it time to boot far enough to answer at all.
    let boot = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < boot {
        if probe(node.probe, "/healthz").await.is_ok_and(|s| s == 200) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let observe = std::time::Instant::now() + Duration::from_secs(30);
    let mut checks = 0;
    while std::time::Instant::now() < observe {
        let health = probe(node.probe, "/healthz").await;
        assert_eq!(
            health.ok(),
            Some(200),
            "a seedless node must stay live -- it is running, just not joined"
        );
        let ready = probe(node.probe, "/readyz").await;
        assert_eq!(
            ready.ok(),
            Some(503),
            "a node that never reached its seeds must never report ready"
        );
        let write = put_imposter(node.admin, 6500, "must-not-apply").await;
        assert_ne!(
            write.ok(),
            Some(201),
            "a node that never joined must not accept a write -- doing so means \
             it founded a cluster of one"
        );
        checks += 1;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    assert!(
        checks >= 5,
        "only {checks} observations made; the window did not run"
    );
}

/// The front door stops routing to a backend that is no longer ready.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn test_front_routes_around_an_unready_node() {
    let cluster = Cluster::up_with_chaos().await.expect("fleet comes up");
    let port = 6600_u16;

    let status = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{FRONT_PORT}/imposters"))
        .timeout(Duration::from_secs(30))
        .json(&serde_json::json!({
            "port": port,
            "protocol": "http",
            "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": "front" } }] }]
        }))
        .send()
        .await
        .expect("write through the front")
        .status()
        .as_u16();
    assert_eq!(
        status, 201,
        "the front must accept a write while all backends are up"
    );
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the write converges");

    let dead = &NODES[1];
    cluster.kill(dead.name).expect("kill a backend");

    // Wait for the front to actually eject it. Round-robin keeps offering the
    // dead backend its share until the health check trips, so writing before
    // this would measure Envoy's check interval, not its routing.
    wait_backend_ejected(dead.ip, Duration::from_secs(60))
        .await
        .expect("envoy must notice the dead backend");

    for i in 0..10 {
        let status = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{FRONT_PORT}/imposters"))
            .timeout(Duration::from_secs(30))
            .json(&serde_json::json!({
                "port": 6610 + i,
                "protocol": "http",
                "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": "f" } }] }]
            }))
            .send()
            .await
            .expect("write through the front")
            .status()
            .as_u16();
        assert_eq!(
            status, 201,
            "write {i} through the front hit the ejected backend"
        );
    }

    cluster.start(dead.name).expect("restart the backend");
    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the fleet comes back");

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if !backend_failing_health_check(dead.ip).await.unwrap_or(true) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "envoy never marked the restarted backend healthy again"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the fleet reconverges after the backend returns");
}

/// C16 — the pull-on-miss safety net rescues a lagging follower (#49, #102).
///
/// #49 shipped the safety net with an exhaustive unit-level decision table and
/// no end-to-end proof: the *logic* was covered, the *wiring* — manager
/// construction, `bind` on the node, the seam actually being consulted — was
/// not. This is that proof.
///
/// Two things make it deterministic rather than raced:
///
/// **The lag is injected, not hoped for.** A 250 ms latency toxic on the
/// follower's inbound cluster link puts a floor under how far behind it is,
/// while the hook's own budget (500 ms, deliberately not configurable) puts a
/// ceiling on how long it will wait. Floor below ceiling means the rescue
/// happens by construction. Constant latency with **zero jitter** on purpose:
/// jitter creates gaps between heartbeats and would risk the elections C6 exists
/// to bound, whereas a constant shift preserves the heartbeat *rate* and so
/// leaves leadership alone.
///
/// **The evidence is self-proving.** `rift-cluster-pull-on-miss: rescued-wait`
/// is only ever set on the path where the node found itself behind the leader
/// and then caught up — so the header *is* the assertion that it lagged. A
/// separate "is it lagging yet?" precondition would be both redundant and racy
/// (the node could apply between the check and the request), so there is not
/// one.
///
/// The imposter is created and converged fleet-wide **first**, and only a
/// *stub* is left in flight. A node that has not applied a missing imposter has
/// no port bound at all: the request is refused at the socket and never reaches
/// the no-match hook this net hangs on. That case is C7's, not this one.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c16_pull_on_miss_rescues_lagging_follower() {
    let cluster = Cluster::up_with_overlays(&[
        "chaos.overlay.yml",
        "barrier-none.overlay.yml",
        "pull-on-miss.overlay.yml",
    ])
    .await
    .expect("fleet comes up");
    cluster
        .wait_all_ready(CONVERGE_TIMEOUT)
        .await
        .expect("all three ready");

    // The node that lags must be a follower — a leader is never behind itself.
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("exactly one leader");
    let lagging_idx = (leader + 1) % NODES.len();
    let lagging = &NODES[lagging_idx];
    let writer = &NODES[leader];
    let port = PULL_ON_MISS_IMPOSTER_PORT;

    // The imposter, fleet-wide, before anything is slowed down.
    assert_eq!(
        put_imposter(writer.admin, port, "base")
            .await
            .expect("admin write"),
        201
    );
    // Scope the base stub to its own path. `put_imposter` creates a stub with no
    // predicates, which matches *everything* — including the path this scenario
    // needs to miss on. With a catch-all in place the request never reaches the
    // no-match hook at all, and the scenario passes on the base body while
    // proving nothing (it did exactly that on the first run).
    assert_eq!(
        put_stubs(
            writer.admin,
            port,
            serde_json::json!([{
                "predicates": [{ "equals": { "path": "/base" } }],
                "responses": [{ "is": { "statusCode": 200, "body": "base" } }]
            }]),
        )
        .await
        .expect("scope the base stub"),
        200
    );
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the fleet converges before the lag is injected");

    let retries_before = metric(
        lagging.metrics_via_mgmt,
        "rift_cluster_pull_on_miss_retries_total",
    )
    .await
    .unwrap_or(0.0);

    // Inbound cluster traffic to the follower now arrives 250 ms late, so its
    // apply trails a commit the other two reach immediately.
    add_toxic(
        lagging.proxy,
        serde_json::json!({
            "type": "latency",
            "stream": "upstream",
            "toxicity": 1.0,
            "attributes": { "latency": 250, "jitter": 0 }
        }),
    )
    .await
    .expect("latency toxic on the follower's cluster link");

    // A stub append is a config write. Under barrier=none it answers as soon as
    // it is committed and applied *here*, without waiting for the follower.
    assert_eq!(
        append_stub(writer.admin, port, "/rescued", "rescued-body")
            .await
            .expect("stub append"),
        200
    );

    // The request that must be rescued: it lands on the follower inside the
    // window where the stub is committed but not yet applied there.
    let (status, headers, body) = get_data_plane(PULL_ON_MISS_HOST_PORTS[lagging_idx], "/rescued")
        .await
        .expect("data-plane request to the lagging follower");

    let rescue = headers
        .get("rift-cluster-pull-on-miss")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("<absent>")
        .to_string();

    assert_eq!(
        status, 200,
        "the follower answered {status} for a stub that was committed before \
         the request arrived; the safety net did not rescue it (header {rescue}, \
         body {body})"
    );
    assert_eq!(
        body, "rescued-body",
        "rescued with the wrong stub: {body:?} (header {rescue}). The header is what \
         separates the readings: `rescued-wait` means the rescue completed and still \
         served the wrong body — a real defect; `retry-after-timeout` means the 250 ms \
         lag outran the 500 ms budget — tier noise; `<absent>` means the no-match hook \
         never fired — wiring"
    );
    assert_eq!(
        rescue, "rescued-wait",
        "expected the follower to report a catch-up rescue; got {rescue:?}. \
         `retry-after-timeout` would mean a 250 ms lag outran the 500 ms budget, \
         and an absent header would mean the hook never fired — the wiring, not \
         the decision table, is what this scenario covers"
    );

    // Metrics corroborate the header rather than substitute for it: there is
    // deliberately no rescue counter, so these say "went down the lagging path",
    // not "rescued".
    let retries_after = metric(
        lagging.metrics_via_mgmt,
        "rift_cluster_pull_on_miss_retries_total",
    )
    .await
    .expect("retries metric on the lagging node");
    assert!(
        retries_after > retries_before,
        "the follower served a rescue header but its retry counter did not \
         move ({retries_before} -> {retries_after})"
    );

    clear_toxics(lagging.proxy).await.expect("clear the toxic");
    wait_revisions_agree(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the fleet reconverges once the link is healthy");
}

// ---------------------------------------------------------------------------
// C15-flow: the durable flow-state tier, end to end (#121, the last child of
// the #16 epic).
// ---------------------------------------------------------------------------

/// The imposter this scenario configures: one scripted stub that increments a
/// per-flow counter and answers with it.
///
/// `durability: "sync"` is the whole point — the counter must be fsynced before
/// the response is acknowledged, so a full-fleet stop cannot lose it.
/// `flowIdSource: "header:X-Flow-Id"` splits one imposter into several
/// independent flows, which is what makes "per-flow isolation survived" a
/// thing this scenario can assert rather than assume.
///
/// `readConsistency` is left at its `strong` default deliberately: the whole
/// claim is that a counter is correct *however* the requests were spread, and
/// only owner-answered reads make that true without a staleness window to
/// tolerate. A `local` variant would need slack in the assertions and would be
/// asserting a weaker property.
fn flow_counter_imposter(port: u16) -> serde_json::Value {
    serde_json::json!({
        "port": port,
        "protocol": "http",
        "_rift": {
            "flowState": {
                "durability": "sync",
                "flowIdSource": "header:X-Flow-Id",
            }
        },
        "stubs": [{
            "predicates": [{ "equals": { "path": "/step" } }],
            "responses": [{
                "_rift": {
                    "script": {
                        "engine": "javascript",
                        "code": "function respond(ctx) { return http(200, String(ctx.state.incr('count'))); }",
                    }
                }
            }],
        }],
    })
}

/// Take one step of `flow`, through the node at `node_idx`, and return the
/// counter the fleet answered with.
async fn flow_step(node_idx: usize, flow: &str) -> anyhow::Result<i64> {
    let (status, _headers, body) = get_data_plane_with(
        FLOW_STATE_HOST_PORTS[node_idx],
        "/step",
        &[("X-Flow-Id", flow)],
    )
    .await?;
    anyhow::ensure!(
        status == 200,
        "flow {flow} step on {} answered {status}: {body}",
        NODES[node_idx].name
    );
    body.trim()
        .parse::<i64>()
        .map_err(|e| anyhow::anyhow!("flow {flow} step answered {body:?}, not a counter: {e}"))
}

/// **C15 (flow state): a scenario mid-flight survives a full-cluster restart.**
///
/// This is #16's Gap E as a user sees it. Several flows advance through a
/// scripted imposter, each request deliberately landing on a *different* node —
/// a hand-rolled round robin, which is the honest stand-in for a load balancer
/// and is what makes the result a statement about the cluster rather than about
/// one process. Then the whole fleet stops and starts, and every flow must
/// resume at exactly the next integer.
///
/// Why the assertions are exact rather than "nonzero": a counter that resets to
/// 1 is the bug this exists to catch, and a counter that comes back at some
/// *other* value is torn state — worse than a reset, because it looks plausible.
/// `strong` reads are owner-answered, so there is no replication window to
/// tolerate and no polling to do; anything but the exact successor is a failure.
///
/// SIGTERM (`compose stop`), not `kill -9`: the graceful path is what a rolling
/// deploy and a `kubectl delete pod` both take, and `sync` durability means the
/// counters were on disk before each response was acknowledged either way. The
/// hard-kill variant of full-fleet restart is already covered for the *config*
/// plane by `c15_hard_kill_of_the_whole_fleet_keeps_acknowledged_writes`, and
/// for flow state at process level by rift-cluster's SIGKILL suite (#119) —
/// which is the layer where a kill can be timed precisely enough to mean
/// something.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c15_flow_state_survives_a_full_cluster_restart() {
    let cluster = Cluster::up_with_overlays(&["flow-state.overlay.yml"])
        .await
        .expect("fleet comes up");
    let port = FLOW_STATE_IMPOSTER_PORT;

    let (status, body) = put_imposter_config(NODES[0].admin, &flow_counter_imposter(port))
        .await
        .expect("admin write");
    assert_eq!(
        status, 201,
        "the scripted flow-state imposter was refused: {body}"
    );
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the imposter binds on every node before any flow starts");

    // Four flows, five steps each, every step on a different node than the one
    // before it. If ownership routing were broken, two nodes would keep
    // independent counters and the sequence would repeat a value here — before
    // the restart is even reached.
    let flows = ["alpha", "beta", "gamma", "delta"];
    let mut expected: std::collections::BTreeMap<&str, i64> = std::collections::BTreeMap::new();
    for step in 0..5 {
        for (i, flow) in flows.iter().enumerate() {
            // Stagger by flow as well as by step, so no flow sees the same node
            // twice in a row and the flows do not move in lockstep.
            let node_idx = (step + i) % NODES.len();
            let seen = flow_step(node_idx, flow)
                .await
                .unwrap_or_else(|e| panic!("pre-restart step: {e}"));
            let want = i64::try_from(step).expect("small") + 1;
            assert_eq!(
                seen, want,
                "flow {flow} answered {seen} on step {want} via {} — the fleet is \
                 not serializing this flow through one owner",
                NODES[node_idx].name
            );
            expected.insert(flow, seen);
        }
    }

    // The restart: every node down, then every node up.
    for node in &NODES {
        cluster.stop(node.name).expect("SIGTERM the node");
    }
    for node in &NODES {
        cluster.start(node.name).expect("restart the node");
    }
    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the fleet comes back");
    cluster
        .wait_cluster_formed(Duration::from_secs(120))
        .await
        .expect("the fleet re-forms a cluster");
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the imposter is rebound after the restart");

    // Every flow resumes at exactly the next integer, read through a node that
    // is (for most flows) not the one that took its last step.
    for (i, flow) in flows.iter().enumerate() {
        let node_idx = (i + 1) % NODES.len();
        let resumed = flow_step(node_idx, flow)
            .await
            .unwrap_or_else(|e| panic!("post-restart step: {e}"));
        let before = expected[flow];
        assert_eq!(
            resumed,
            before + 1,
            "flow {flow} stood at {before} before the restart and resumed at \
             {resumed} via {}. A reset to 1 means the durable tier lost the \
             flow; any other value means it came back torn. Per-flow state at \
             the restart was {expected:?}",
            NODES[node_idx].name
        );
    }

    // Recovery is observable per node, not only through the counters: the
    // replay counter is what an operator would look at to answer "did this node
    // come back with its state or without it?".
    let replayed: Vec<f64> = {
        let mut seen = Vec::new();
        for node in &NODES {
            seen.push(
                metric(node.metrics, "rift_cluster_flow_replay_entries_total")
                    .await
                    .unwrap_or(0.0),
            );
        }
        seen
    };
    assert!(
        replayed.iter().any(|&n| n > 0.0),
        "no node reported replaying a single flow entry from disk after the \
         restart ({replayed:?}) — the counters above would then be passing on \
         something other than durability"
    );
}

// ---------------------------------------------------------------------------
// C17-C19: the front door's route table, and the imposter map it dispatches
// through, end to end (#132/#143, closing #19's cluster-level acceptance
// list).
// ---------------------------------------------------------------------------

/// C17's two imposters, each answering with a distinct body so a dispatched
/// request's body says which one it reached.
const C17_IMPOSTER_A: u16 = 6500;
const C17_IMPOSTER_B: u16 = 6501;

/// **C17: a route write converges the moment its 2xx returns — R1 for the
/// front door.**
///
/// This is `test_config_sync_converges` reprised for the route table, with
/// the stronger check issue #131's correction (and #132's, following it)
/// calls for: there is no `/front-door/resolve` to read, so "the fleet has
/// it" is proven by a REAL request through the OTHER two nodes' own front
/// doors — never an admin-API lookup. `--cluster-write-barrier=ready-nodes`
/// (the default, and what `front-door.overlay.yml` does not override) means
/// the leader's write does not return 200 until every ready node has applied
/// the entry, so if either dispatch below needed even one retry, the barrier
/// would already be broken.
///
/// Two writes, not one: the second retargets the SAME route id to a
/// different imposter, so this is a genuine test of "a *change* converges",
/// not merely "the first write landed" — which a node that starts with an
/// empty table would pass by accident even with no barrier at all.
///
/// No polling anywhere in this scenario, on purpose: polling with a timeout
/// would pass against a front door that recompiles its table eventually,
/// which is a materially weaker claim than the barrier's contract.
///
/// Mutation: commenting out the `ArcSwap` store in
/// `RedbStateMachine::drive_engine`'s `EngineAction::SyncRoutes` arm
/// (`crates/rift-cluster/src/raft/store.rs`) turns this red, because no node —
/// leader included — ever applies a route again; both dispatches below then
/// answer the front door's `no-route` 404 instead of the target imposter's
/// body. Verified: see the chaos README's C17 entry for the actual failure
/// message.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c17_routes_converge() {
    let _cluster = Cluster::up_with_overlays(&["front-door.overlay.yml"])
        .await
        .expect("fleet comes up");

    for (port, body) in [(C17_IMPOSTER_A, "from-a"), (C17_IMPOSTER_B, "from-b")] {
        let (status, resp_body) = put_imposter_config(
            NODES[0].admin,
            &serde_json::json!({
                "port": port,
                "protocol": "http",
                "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": body } }] }],
            }),
        )
        .await
        .expect("admin write");
        assert_eq!(
            status, 201,
            "imposter on port {port} was refused: {resp_body}"
        );
    }
    for port in [C17_IMPOSTER_A, C17_IMPOSTER_B] {
        wait_converged(u64::from(port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("imposter {port} did not bind fleet-wide: {e}"));
    }

    // First write: an empty table becoming one route. Every node other than
    // the writer starts with NO route at all, so this already exercises real
    // convergence rather than a no-op re-apply.
    let (status, body) = put_routes(
        NODES[0].admin,
        &serde_json::json!({
            "routes": [{
                "id": "svc",
                "match": { "path_prefix": "/svc" },
                "target": { "port": C17_IMPOSTER_A },
            }],
        }),
    )
    .await
    .expect("put routes");
    assert_eq!(status, 200, "route write was refused: {body}");

    for (idx, node) in NODES.iter().enumerate().skip(1) {
        let (dispatch_status, _headers, dispatch_body) =
            get_data_plane_with(FRONT_DOOR_HOST_PORTS[idx], "/svc/anything", &[])
                .await
                .unwrap_or_else(|e| panic!("dispatch through {}: {e}", node.name));
        assert_eq!(
            dispatch_status, 200,
            "{} did not route /svc the moment the write returned 200 -- with \
             --cluster-write-barrier=ready-nodes a 2xx means the fleet has the \
             new table, not merely that the leader does (R1)",
            node.name
        );
        assert_eq!(
            dispatch_body, "from-a",
            "{} routed /svc to the wrong imposter",
            node.name
        );
    }

    // Second write: retarget the same route id. Proves a CHANGE converges,
    // not just an initial write landing on an empty table.
    let (status, body) = put_routes(
        NODES[0].admin,
        &serde_json::json!({
            "routes": [{
                "id": "svc",
                "match": { "path_prefix": "/svc" },
                "target": { "port": C17_IMPOSTER_B },
            }],
        }),
    )
    .await
    .expect("put routes (retarget)");
    assert_eq!(status, 200, "route retarget was refused: {body}");

    for (idx, node) in NODES.iter().enumerate().skip(1) {
        let (dispatch_status, _headers, dispatch_body) =
            get_data_plane_with(FRONT_DOOR_HOST_PORTS[idx], "/svc/anything", &[])
                .await
                .unwrap_or_else(|e| panic!("dispatch through {}: {e}", node.name));
        assert_eq!(
            dispatch_status, 200,
            "{} did not pick up the retarget the moment the write returned 200",
            node.name
        );
        assert_eq!(
            dispatch_body, "from-b",
            "{} still routes /svc to the pre-retarget imposter after a \
             converged write",
            node.name
        );
    }
}

/// C18's three imposters, one per route shape, each answering with a distinct
/// body so a dispatched request's body says which one it reached.
const C18_IMPOSTER_EXACT: u16 = 6510;
const C18_IMPOSTER_WILD: u16 = 6511;
const C18_IMPOSTER_PREFIX: u16 = 6512;

/// The table C18 writes before the restart: one route per shape the issue
/// names — an exact host, a wildcard host, and a path prefix — each targeting
/// a different imposter.
fn c18_route_table() -> serde_json::Value {
    serde_json::json!({
        "routes": [
            {
                "id": "exact-host",
                "match": { "host": "payments.c18.test" },
                "target": { "port": C18_IMPOSTER_EXACT },
            },
            {
                "id": "wild-host",
                "match": { "host": "*.search.c18.test" },
                "target": { "port": C18_IMPOSTER_WILD },
            },
            {
                "id": "prefix",
                "match": { "path_prefix": "/api" },
                "target": { "port": C18_IMPOSTER_PREFIX },
            },
        ],
    })
}

/// Route ids from a `GET /front-door/routes` body, sorted — so a comparison
/// asserts the SET of routes came back, independent of whatever order the
/// state machine's table iterator happens to yield them in.
fn route_ids(routes_response: &serde_json::Value) -> Vec<String> {
    let mut ids: Vec<String> = routes_response["routes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|route| route["id"].as_str().map(str::to_owned))
        .collect();
    ids.sort();
    ids
}

/// Dispatch all three of C18's route shapes through one node's own front
/// door, asserting each answers 200 and returning the three bodies (exact,
/// wildcard, prefix) for the caller to check against the right imposter.
async fn c18_probe_all_shapes(front_door_port: u16, node_name: &str) -> (String, String, String) {
    let (exact_status, _headers, exact_body) =
        get_data_plane_with(front_door_port, "/", &[("Host", "payments.c18.test")])
            .await
            .unwrap_or_else(|e| panic!("{node_name}: exact-host dispatch: {e}"));
    assert_eq!(
        exact_status, 200,
        "{node_name}: exact-host route did not dispatch"
    );

    let (wild_status, _headers, wild_body) =
        get_data_plane_with(front_door_port, "/", &[("Host", "api.search.c18.test")])
            .await
            .unwrap_or_else(|e| panic!("{node_name}: wildcard-host dispatch: {e}"));
    assert_eq!(
        wild_status, 200,
        "{node_name}: wildcard-host route did not dispatch"
    );

    let (prefix_status, _headers, prefix_body) =
        get_data_plane_with(front_door_port, "/api/anything", &[])
            .await
            .unwrap_or_else(|e| panic!("{node_name}: path-prefix dispatch: {e}"));
    assert_eq!(
        prefix_status, 200,
        "{node_name}: path-prefix route did not dispatch"
    );

    (exact_body, wild_body, prefix_body)
}

/// **C18: the front door's route table — and real dispatch through it —
/// survives a full-cluster restart.**
///
/// Same discipline as C15 (`c15_flow_state_survives_a_full_cluster_restart`):
/// a full-fleet SIGTERM/restart (`stop` then `start` every node, never
/// `recreate` — the state directory persisting across it is exactly what is
/// under test), `wait_all_ready` + `wait_cluster_formed` before asserting
/// anything.
///
/// Two checks per node, not one, because they exercise different mechanisms:
///
/// - `GET /front-door/routes` reads `sm_routes` directly — durability of the
///   *stored* table. There is no `resolve` endpoint upstream to assert
///   against instead (issue #131's correction, inherited by #132).
/// - Real dispatch through each node's own front door exercises the
///   *in-memory* `ArcSwap` a restarted node has to rebuild from that stored
///   table before it can serve anything again
///   (`RedbStateMachine::reconcile_engine`, the same cold-start projection
///   `test_cold_start` exercises for imposter configs — a fresh process's
///   `ArcSwap<CompiledRoutes>` starts empty regardless of what is on disk).
///   A node could pass the first check and still 404 every request if that
///   rebuild were skipped, which is exactly the failure this second check
///   exists to catch.
///
/// Three routes, three shapes, three imposters — an exact host, a wildcard
/// host, and a path prefix, each targeting a different imposter — so a
/// mismatched dispatch after the restart is visible in the response body,
/// not just the status.
///
/// **Correction to the issue's premise:** the issue named `build_snapshot` /
/// `install_snapshot` as the mutation target ("rides the snapshot"). Measured
/// against this exact restart pattern, it does not: `Cluster::stop`/`start`
/// keeps every node's own state directory intact and does not fall behind the
/// log, so openraft never calls `install_snapshot` on any of the three nodes
/// — that RPC exists to bring a LAGGING peer's state machine up to date over
/// the wire, which nothing here is. Durability across *this* restart runs
/// through `reconcile_engine`'s post-join re-derivation from `sm_routes`
/// instead (see `crates/rift-cluster/src/raft/node.rs`'s `reconcile_engine`,
/// called from the readiness reconciler `compose.rs` spawns to satisfy
/// `GATE_RECONCILED`), so that is the line this scenario is mutation-proven
/// against: dropping the `routes_action` from the vec `reconcile_engine`
/// hands to `drive_engine` (`crates/rift-cluster/src/raft/store.rs`) turns
/// this scenario red post-restart — `GET /front-door/routes` still matches
/// (the stored table is untouched on disk), but every dispatch 404s with
/// `x-rift-front-door: no-route`, because the front door of a freshly
/// restarted process starts from `CompiledRoutes::default()` and nothing
/// repopulates it. Verified: see the chaos README's C18 entry for the actual
/// failure message.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c18_routes_survive_a_full_cluster_restart() {
    let cluster = Cluster::up_with_overlays(&["front-door.overlay.yml"])
        .await
        .expect("fleet comes up");

    for (port, body) in [
        (C18_IMPOSTER_EXACT, "exact-host-service"),
        (C18_IMPOSTER_WILD, "wildcard-host-service"),
        (C18_IMPOSTER_PREFIX, "path-prefix-service"),
    ] {
        let status = put_imposter(NODES[0].admin, port, body)
            .await
            .expect("admin write");
        assert_eq!(status, 201, "imposter on port {port} was refused");
    }
    for port in [C18_IMPOSTER_EXACT, C18_IMPOSTER_WILD, C18_IMPOSTER_PREFIX] {
        wait_converged(u64::from(port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("imposter {port} did not bind fleet-wide: {e}"));
    }

    let (status, body) = put_routes(NODES[0].admin, &c18_route_table())
        .await
        .expect("put routes");
    assert_eq!(status, 200, "route write was refused: {body}");

    let expected_ids = ["exact-host", "prefix", "wild-host"];

    // Pre-restart: every node's own front door already serves all three
    // shapes (C17's write-barrier guarantee), and every node's stored table
    // already matches.
    for (idx, node) in NODES.iter().enumerate() {
        let (_status, routes_before) = get_json(node.admin, "/front-door/routes")
            .await
            .unwrap_or_else(|e| panic!("{}: get routes: {e}", node.name));
        assert_eq!(
            route_ids(&routes_before),
            expected_ids,
            "{}: pre-restart route table",
            node.name
        );

        let (exact, wild, prefix) =
            c18_probe_all_shapes(FRONT_DOOR_HOST_PORTS[idx], node.name).await;
        assert_eq!(
            exact, "exact-host-service",
            "{}: pre-restart exact-host body",
            node.name
        );
        assert_eq!(
            wild, "wildcard-host-service",
            "{}: pre-restart wildcard-host body",
            node.name
        );
        assert_eq!(
            prefix, "path-prefix-service",
            "{}: pre-restart path-prefix body",
            node.name
        );
    }

    // The restart: every node down, then every node up.
    for node in &NODES {
        cluster.stop(node.name).expect("SIGTERM the node");
    }
    for node in &NODES {
        cluster.start(node.name).expect("restart the node");
    }
    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the fleet comes back");
    cluster
        .wait_cluster_formed(Duration::from_secs(120))
        .await
        .expect("the fleet re-forms a cluster");
    for port in [C18_IMPOSTER_EXACT, C18_IMPOSTER_WILD, C18_IMPOSTER_PREFIX] {
        wait_converged(u64::from(port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("imposter {port} did not rebind after the restart: {e}"));
    }

    // Post-restart: both checks, on EVERY node.
    for (idx, node) in NODES.iter().enumerate() {
        let (_status, routes_after) = get_json(node.admin, "/front-door/routes")
            .await
            .unwrap_or_else(|e| panic!("{}: get routes after restart: {e}", node.name));
        assert_eq!(
            route_ids(&routes_after),
            expected_ids,
            "{}: route table after restart does not match the pre-restart table",
            node.name
        );

        let (exact, wild, prefix) =
            c18_probe_all_shapes(FRONT_DOOR_HOST_PORTS[idx], node.name).await;
        assert_eq!(
            exact, "exact-host-service",
            "{}: exact-host dispatch after restart -- the stored table survived \
             (checked above) but the front door's in-memory compiled table was \
             not rebuilt from it",
            node.name
        );
        assert_eq!(
            wild, "wildcard-host-service",
            "{}: wildcard-host dispatch after restart",
            node.name
        );
        assert_eq!(
            prefix, "path-prefix-service",
            "{}: path-prefix dispatch after restart",
            node.name
        );
    }
}

/// The imposter port `bind-squat.overlay.yml` squats inside rift-2's network
/// namespace only. Not published to the host -- see that overlay's header for
/// why. 6520 continues the numbering C17/C18 (6500-6512) and C20-C23 (6610-6640) already use in
/// this file. C26 also uses 6520-6522, which is safe only because this tier runs `--test-threads=1`
/// and every scenario brings up and tears down its own stack — the overlays differ, so the two
/// never coexist. Reusing a number across scenarios is fine *for that reason*, not because the
/// number is unused; anything that made these run concurrently would have to revisit it.
const C19_IMPOSTER_PORT: u16 = 6520;

/// Poll `docker inspect` until `name`'s healthcheck reports `healthy`, or bail
/// at the deadline.
///
/// This is what makes the squat in `bind-squat.overlay.yml` provable rather
/// than assumed. Compose cannot express "rift-2 waits on the squatter": the
/// squatter needs rift-2's network namespace to attach to
/// (`network_mode: "service:rift-2"`), which only exists once rift-2's own
/// container has started, so the compose-graph dependency has to run
/// rift-2 -> squatter, not the other way -- rift-2 waiting on the squatter
/// would be a cycle. Gating the *test* on the squatter's own healthcheck (a
/// real connect to the port it just bound) closes the loop from the other
/// side: nothing here writes the imposter config until this returns, so the
/// squat is confirmed held, not merely scheduled, before the write that
/// depends on it.
async fn wait_container_healthy(name: &str, timeout: Duration) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(output) = std::process::Command::new("docker")
            .args(["inspect", "--format", "{{.State.Health.Status}}", name])
            .output()
            && output.status.success()
            && String::from_utf8_lossy(&output.stdout).trim() == "healthy"
        {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("{name} never reported healthy within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// **C19: a node whose explicit bind fails still serves the imposter through
/// its own front door -- the container-tier proof of the issue #143 /
/// RFC-001 §7.4.6 dividend.**
///
/// `crates/rift-cluster-server/tests/bind_divergence.rs` already proves the
/// in-process half of this: there, the squat is a plain listener inside the
/// test's own process. This is the same collision for real, across a real
/// three-node stack: `bind-squat.overlay.yml` runs an `alpine/socat` sidecar
/// *inside rift-2's network namespace* (`network_mode: "service:rift-2"`), so
/// the port is held by a process outside rift-2 entirely, the way an
/// unrelated deployment on the same host would hold it -- and only inside
/// rift-2's namespace, so rift-1 and rift-3 bind the same port cleanly.
///
/// `wait_container_healthy` is what stops this from being a race that passes
/// by luck: it blocks on the squatter's own healthcheck before the imposter
/// write below, so the port is provably held before the write that depends on
/// it rather than merely started first in the compose graph.
///
/// The write itself is the first assertion that matters: `201`, not the `404`
/// a bind-failed node used to answer before #143, because every node
/// constructs the imposter and claims the port in its map regardless of
/// whether the local bind succeeded (`with_serve_unbound(true)`, set only by
/// the cluster cluster composition). `wait_converged` reads exactly that
/// map (`GET /imposters`), so it converges fleet-wide despite the squat --
/// convergence of the *config*, not of the bind.
///
/// The bind failure is still reported, not hidden: rift-2's own
/// `rift_cluster_bind_failures` gauge for this port is checked before the
/// dividend, so a scenario run against a node that silently stopped reporting
/// degraded state would fail here rather than being masked by the dispatch
/// passing anyway.
///
/// Two dispatch checks, not one, because divergence is the claim: rift-2 (the
/// squatted node, `FRONT_DOOR_HOST_PORTS[1]`) must serve the imposter
/// in-process despite never holding the socket, and rift-1
/// (`FRONT_DOOR_HOST_PORTS[0]`, bind succeeded) must keep serving it
/// normally. Only the first check would leave open the possibility that the
/// whole stack degraded uniformly rather than diverged.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c19_front_door_routes_around_bind_divergence() {
    let _cluster = Cluster::up_with_overlays(&["front-door.overlay.yml", "bind-squat.overlay.yml"])
        .await
        .expect("fleet comes up");

    wait_container_healthy("bind-squat", Duration::from_secs(60))
        .await
        .expect("the squatter holds rift-2's imposter port before the write below");

    let (status, body) = put_imposter_config(
        NODES[0].admin,
        &serde_json::json!({
            "port": C19_IMPOSTER_PORT,
            "protocol": "http",
            "stubs": [{
                "responses": [{ "is": { "statusCode": 200, "body": "served-while-unbound" } }],
            }],
        }),
    )
    .await
    .expect("admin write");
    assert_eq!(
        status, 201,
        "the imposter must exist cluster-wide regardless of rift-2's local bind \
         -- not the 404 a bind-failed node answered before #143: {body}"
    );

    wait_converged(u64::from(C19_IMPOSTER_PORT), CONVERGE_TIMEOUT)
        .await
        .unwrap_or_else(|e| {
            panic!("imposter {C19_IMPOSTER_PORT} did not converge despite the squat: {e}")
        });

    // Polled, not read once. `wait_converged` proves the *config* reached every node's map, which
    // is a different event from the local apply having recorded its bind outcome, and this tier's
    // single-shot timing assertions are its main flake source. A bounded poll asserts the same
    // thing without pinning the order of two events nothing guarantees the order of.
    //
    // A scrape error is distinguished from a genuine `0`: collapsing both into `0.0` would let a
    // fleet that never exposed the gauge at all fail with "did not report the bind failure", which
    // sends the next reader looking in the wrong place.
    // Polled, not read once. `wait_converged` proves the *config* reached every node's map, which
    // is a different event from the local apply having recorded its bind outcome, and this tier's
    // single-shot timing assertions are its main flake source.
    //
    // An absent family counts as "not yet", not as an error: `bind_failures` is a `GaugeVec{port}`
    // that `observe_apply_failures` `reset()`s, so before the failure is recorded this series does
    // not exist at all. Failing on absence would abort the poll on exactly the state it is waiting
    // to leave.
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    let family = format!(r#"rift_cluster_bind_failures{{port="{C19_IMPOSTER_PORT}"}}"#);
    loop {
        if metric(NODES[1].metrics, &family).await.unwrap_or(0.0) == 1.0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "rift-2 never reported the bind failure the squat should have caused"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let (status, body) = put_routes(
        NODES[0].admin,
        &serde_json::json!({
            "routes": [{
                "id": "svc",
                "match": { "path_prefix": "/svc" },
                "target": { "port": C19_IMPOSTER_PORT },
            }],
        }),
    )
    .await
    .expect("put routes");
    assert_eq!(status, 200, "route write was refused: {body}");

    // The dividend: rift-2's OWN front door -- the node whose bind failed --
    // serves the imposter anyway, dispatched in-process.
    let (dispatch_status, _headers, dispatch_body) =
        get_data_plane_with(FRONT_DOOR_HOST_PORTS[1], "/svc/anything", &[])
            .await
            .expect("dispatch through rift-2's own front door");
    assert_eq!(
        dispatch_status, 200,
        "rift-2 did not serve the imposter it could not bind"
    );
    assert_eq!(
        dispatch_body, "served-while-unbound",
        "rift-2 served the wrong body for the imposter it could not bind"
    );

    // The other in-process route §7.4.6 promises. The front door resolves a route table first;
    // the gateway addresses the port directly. Both land in `get_imposter`, but they are distinct
    // paths to it, so the front-door assertion above does not prove this one -- break the gateway
    // lookup for an unbound imposter and only this check goes red.
    let (gateway_status, _headers, gateway_body) = get_data_plane_with(
        NODES[1].admin,
        &format!("/__rift/{C19_IMPOSTER_PORT}/anything"),
        &[],
    )
    .await
    .expect("dispatch through rift-2's gateway prefix");
    assert_eq!(
        gateway_status, 200,
        "rift-2's gateway route did not serve the imposter it could not bind"
    );
    assert_eq!(
        gateway_body, "served-while-unbound",
        "rift-2's gateway route served the wrong body"
    );

    // Divergence, not uniform breakage: rift-1's bind succeeded, and it must
    // still serve the same imposter normally through its own front door.
    let (healthy_status, _headers, healthy_body) =
        get_data_plane_with(FRONT_DOOR_HOST_PORTS[0], "/svc/anything", &[])
            .await
            .expect("dispatch through rift-1's own front door");
    assert_eq!(
        healthy_status, 200,
        "rift-1, whose bind succeeded, must still serve the imposter normally"
    );
    assert_eq!(
        healthy_body, "served-while-unbound",
        "rift-1 served the wrong body for the imposter"
    );
}

// ---------------------------------------------------------------------------
// C20-C23: imposter sources, end to end (#134/#135, closing #20's cluster
// acceptance list). Every one of these rides `sources.overlay.yml`, which
// publishes each node's cluster port (where `/admin/sources*` lives) and adds
// the counting origin the fleet fetches from.
//
// The origin is a fourth `rift-cluster-server`, un-clustered, whose imposter's
// response body *is* the config document. That is what makes "fetched once
// fleet-wide" an equality against `numberOfRequests` — a first-class admin-API
// value — rather than a log scrape. See the overlay's header for why it is a
// rift container and not a static-file image.
// ---------------------------------------------------------------------------

/// The path C20's document is served on, and the two imposters it declares.
const C20_DOC_PATH: &str = "/gh-mocks.json";
const C20_PORT_A: u16 = 6610;
const C20_PORT_B: u16 = 6611;

/// How long a source's control-plane surface gets to start answering.
///
/// Longer than [`CONVERGE_TIMEOUT`] because it covers a different thing:
/// `/readyz` going 200 says the node is up, not that the source puller has been
/// bound to it, and an unbound puller answers "cluster node is not available
/// yet". This is composition order, not convergence.
const SOURCES_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// **C20: a source pull converges fleet-wide and fetches the source exactly
/// once.**
///
/// Two claims, and the second is the one the whole design turns on.
///
/// *Converges.* A pull is an ordinary control op: the receiving node fetches,
/// canonicalizes, hashes, and submits `SourcePullResult` through the leader like
/// any other write. So every node must end up serving the imposters the document
/// declared, at the same applied revision — asserted with `wait_converged` plus
/// `wait_revisions_agree`, which is strictly stronger (two nodes can both serve
/// a port while one is on an older config for it).
///
/// Polled rather than asserted at 2xx-return, and deliberately so: the write
/// barrier (`--cluster-write-barrier`) is a property of the **admin front**, and
/// `/admin/sources/:id/pull` rides the **cluster port**, which has no such
/// barrier — `SourcePuller::pull` awaits only *this* node's local apply (#99).
/// Claiming read-your-write across the fleet here would be asserting a contract
/// the code does not offer. C17 is where the barrier's own contract is pinned.
///
/// *Fetches once.* The counter equality is `== 1`, never `>= 1`. "Followers
/// never fetch, they apply" is the reason the fetch can be non-deterministic
/// I/O at all (two nodes fetching the same URI a second apart can legitimately
/// get different bytes; a fleet that applied *different* configs from the same
/// op would have diverged with nothing to point at). An inequality would pass
/// against a fleet that had quietly gone back to per-node fetching, which is
/// exactly the regression worth catching.
///
/// The second pull is not decoration. It pins the other half of the contract:
/// a pull **always** fetches — it has to, to find out whether anything changed —
/// and it is the *write* the digest short circuit removes. So the counter moves
/// by exactly one again while `unchanged: true` and nothing is applied.
///
/// Mutation: making `SourcePuller::pull` fetch once per voter instead of once
/// (`crates/rift-cluster/src/sources/mod.rs`) — a stand-in for the fetch-in-apply
/// design #134 rejected — turns this red on the counter, and only on the
/// counter: every convergence assertion still passes, which is the point. See
/// the chaos README's C20 entry for the actual failure message.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c20_source_pull_converges_and_fetches_once() {
    let _cluster = Cluster::up_with_overlays(&["sources.overlay.yml"])
        .await
        .expect("fleet comes up");
    wait_origin_ready(SOURCES_READY_TIMEOUT)
        .await
        .expect("the counting origin's admin API answers");

    let (status, body) = origin_publish(&[(
        C20_DOC_PATH,
        source_document(&[(C20_PORT_A, "gh-v1-a"), (C20_PORT_B, "gh-v1-b")]),
    )])
    .await
    .expect("publish the source document");
    assert_eq!(status, 201, "the origin refused the document: {body}");

    for host in SOURCES_CLUSTER_HOST_PORTS {
        wait_sources_reachable(host, SOURCES_READY_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("cluster port :{host}: {e}"));
    }

    let (status, body) = declare_source(
        SOURCES_CLUSTER_HOST_PORTS[0],
        &serde_json::json!({
            "id": "gh-mocks",
            "uri": format!("{SOURCES_ORIGIN_BASE_URL}{C20_DOC_PATH}"),
            "onDrift": "overwrite",
        }),
    )
    .await
    .expect("declare the source");
    assert_eq!(status, 200, "declaring the source was refused: {body}");

    // Declaring is not fetching: in pinned mode a pull is an explicit act. This
    // also makes the counter reading below unambiguous — the one request it
    // sees can only be the pull's.
    let before = origin_request_count().await.expect("origin request count");
    assert_eq!(
        before, 0,
        "declaring a source fetched it; the counter below could then not \
         attribute its request to the pull"
    );

    let (status, report) = pull_source(SOURCES_CLUSTER_HOST_PORTS[0], "gh-mocks")
        .await
        .expect("pull the source");
    assert_eq!(status, 200, "the pull was refused: {report}");
    assert_eq!(report["unchanged"], false, "first pull: {report}");
    assert_eq!(report["skipped"], false, "first pull: {report}");
    assert_eq!(
        report["changed"],
        serde_json::json!([C20_PORT_A, C20_PORT_B]),
        "the pull must name the ports it created: {report}"
    );

    for port in [C20_PORT_A, C20_PORT_B] {
        wait_converged(u64::from(port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("source imposter {port} did not reach every node: {e}"));
        wait_revisions_agree(u64::from(port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("nodes serve {port} but do not agree on its revision: {e}"));
    }

    // Every node stamps the same provenance on the ports the pull created —
    // read from each node's OWN applied state, which is what makes comparing
    // them a convergence check rather than three reads of one answer.
    for (idx, host) in SOURCES_CLUSTER_HOST_PORTS.iter().enumerate() {
        let (status, config) = cluster_config(*host)
            .await
            .unwrap_or_else(|e| panic!("{}: /_cluster/config: {e}", NODES[idx].name));
        assert_eq!(status, 200, "{}: {config}", NODES[idx].name);
        for port in [C20_PORT_A, C20_PORT_B] {
            let stamped = provenance_of(&config, port);
            assert_eq!(
                stamped.as_ref().map(|(id, _)| id.as_str()),
                Some("gh-mocks"),
                "{}: port {port} is not stamped with the source that created it: {config}",
                NODES[idx].name
            );
        }
    }

    let after = origin_request_count().await.expect("origin request count");
    assert_eq!(
        after - before,
        1,
        "the fleet fetched the source {} times for one pull. Exactly one is the \
         contract: the receiving node fetches and submits the bytes, and every \
         other node applies them. Anything more means a node fetched on the \
         apply path, where two fetches a second apart can legitimately disagree \
         and the fleet diverges with nothing to point at.",
        after - before
    );

    // A pull always fetches — that is how it finds out whether anything changed.
    // What the digest short circuit removes is the *write*.
    let (status, report) = pull_source(SOURCES_CLUSTER_HOST_PORTS[0], "gh-mocks")
        .await
        .expect("re-pull the source");
    assert_eq!(status, 200, "the second pull was refused: {report}");
    assert_eq!(
        report["unchanged"], true,
        "identical content must short-circuit: {report}"
    );
    assert_eq!(report["changed"], serde_json::json!([]));
    let after_second = origin_request_count().await.expect("origin request count");
    assert_eq!(
        after_second - after,
        1,
        "an unchanged pull must still fetch exactly once — it cannot know the \
         content is unchanged without asking"
    );
}

/// C21's document path, the imposter it starts with, and the one a content
/// change adds.
const C21_DOC_PATH: &str = "/tracked.json";
const C21_PORT: u16 = 6620;
const C21_PORT_ADDED: u16 = 6621;

/// C21's poll cadence: the enforced floor (`MIN_POLL_SECS`), because the
/// scenario's cost is dominated by how long it must watch and a slower cadence
/// buys nothing.
const C21_POLL_SECS: u64 = 5;

/// How long each rate measurement watches the origin's counter.
const C21_WINDOW: Duration = Duration::from_secs(40);

/// Bounds on fetches observed in one [`C21_WINDOW`], for a fleet with **one**
/// poller.
///
/// Derived, not tuned. The poll loop sleeps `C21_POLL_SECS` ±10% (the scheduler
/// jitters so sources declared together do not burst), so one poller completes
/// between `40/5.5 ≈ 7.3` and `40/4.5 ≈ 8.9` sleeps in the window, and the
/// window's edges can clip one at either end: 6-10 on an idle box.
///
/// The bounds are widened from that, in one direction each and for one reason
/// each:
///
/// * **down to 3**, because a CI runner under load can stretch sleeps and,
///   after a failover, the second window starts with no poller at all until the
///   election resolves. Three still fails a fleet that is not polling: zero,
///   one or two in forty seconds is not a five-second cadence.
/// * **up to 12**, which is ~50% of headroom above the ideal ceiling and still
///   nowhere near what a second poller costs. Three pollers at this cadence
///   produce `3 × 7 = 21` at an absolute minimum — so the bound separates
///   "one poller, on a slow box" from "more than one poller" with room to
///   spare, which is the only distinction it is asked to make.
///
/// If this fails, the question is how many nodes are polling, not whether the
/// numbers should move.
const C21_MIN_FETCHES: u64 = 3;
const C21_MAX_FETCHES: u64 = 12;

/// **C21: only the leader polls a tracking source, and that survives losing the
/// leader.**
///
/// A `tracking` source is re-fetched on an interval with nobody asking. The
/// whole difficulty is the word *fleet*: three nodes each running a timer would
/// fetch three times per interval and undo C20's fetch-once property with the
/// very thing meant to drive it. So the scheduler is leader-only, grounded on
/// the same Raft leadership watch the forward-to-leader write path reads.
///
/// Measured as a **rate over a window**, not as a single count, because a
/// single count cannot tell a slow box from a second poller. The bound is
/// derived on `C21_MIN_FETCHES`/`C21_MAX_FETCHES` and is deliberately loose
/// enough to survive a stretched sleep while staying far below what a second
/// poller would cost — and the floor keeps it from passing vacuously against a
/// fleet that has stopped polling altogether.
///
/// The failover half is the part that makes this more than a startup check:
/// leadership is the *only* thing gating the poller, so a fleet that lost its
/// leader must resume polling at one node's cadence — not zero (the survivors
/// never took it up) and not two (the dead node's tasks were inherited as well
/// as started). A content change afterwards proves the resumed poller is doing
/// real work and not merely burning requests.
///
/// `rift_cluster_source_polls_total` is asserted alongside the origin's counter
/// on purpose: only the leader increments it, so the fleet-wide sum is an
/// independent second opinion on the same claim, read from the product's own
/// observability rather than from the thing being polled.
///
/// Mutation: deleting the `if !is_leader { … }` arm from
/// `SourceScheduler::supervise` (`crates/rift-cluster/src/sources/scheduler.rs`),
/// so every node reconciles and polls, turns this red on the first rate
/// assertion. See the chaos README's C21 entry for the actual failure message.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c21_tracking_poll_is_leader_only_and_survives_failover() {
    let cluster = Cluster::up_with_overlays(&["sources.overlay.yml"])
        .await
        .expect("fleet comes up");
    wait_origin_ready(SOURCES_READY_TIMEOUT)
        .await
        .expect("the counting origin's admin API answers");

    let (status, body) =
        origin_publish(&[(C21_DOC_PATH, source_document(&[(C21_PORT, "tracked-v1")]))])
            .await
            .expect("publish the source document");
    assert_eq!(status, 201, "the origin refused the document: {body}");

    for host in SOURCES_CLUSTER_HOST_PORTS {
        wait_sources_reachable(host, SOURCES_READY_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("cluster port :{host}: {e}"));
    }

    let (status, body) = declare_source(
        SOURCES_CLUSTER_HOST_PORTS[0],
        &serde_json::json!({
            "id": "tracked",
            "uri": format!("{SOURCES_ORIGIN_BASE_URL}{C21_DOC_PATH}"),
            "mode": "tracking",
            "pollSecs": C21_POLL_SECS,
            "onDrift": "overwrite",
        }),
    )
    .await
    .expect("declare the tracking source");
    assert_eq!(status, 200, "declaring the source was refused: {body}");

    // The first poll applies the document, which is also the proof the
    // scheduler picked the source up at all — without it the rate below could
    // be measuring a fleet that never started.
    wait_converged(u64::from(C21_PORT), Duration::from_secs(60))
        .await
        .expect("the tracking source's first poll reaches every node");

    let first_leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("exactly one leader before the kill");

    let (fetched, counted) = c21_measure_window().await;
    assert!(
        (C21_MIN_FETCHES..=C21_MAX_FETCHES).contains(&fetched),
        "the origin saw {fetched} fetches in {C21_WINDOW:?} at a \
         {C21_POLL_SECS}s cadence. One poller is {C21_MIN_FETCHES}-\
         {C21_MAX_FETCHES}; three would be 21 or more. Fleet-wide \
         rift_cluster_source_polls_total moved by {counted} over the same \
         window."
    );
    assert_eq!(
        counted, fetched,
        "the fleet counted {counted} polls while the origin served {fetched} \
         fetches. Only the leader increments the counter, so a disagreement \
         means either a follower fetched without counting or a poll fetched \
         more than once"
    );

    // Kill the leader outright: no drain, no leave, no chance to hand the
    // scheduler over. Two of three is still a quorum, so the fleet must elect
    // and resume on its own.
    let dead = &NODES[first_leader];
    cluster.kill(dead.name).expect("kill the leader");
    let survivors: Vec<&cluster_chaos::Node> = NODES
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != first_leader)
        .map(|(_, node)| node)
        .collect();
    let survivor_hosts: Vec<u16> = NODES
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != first_leader)
        .map(|(i, _)| SOURCES_CLUSTER_HOST_PORTS[i])
        .collect();

    // Wait for the election before measuring: the window would otherwise start
    // in the leaderless gap and measure how fast the fleet re-elects, which is
    // C14's question, not this one.
    let new_leader = c21_wait_surviving_leader(first_leader).await;
    assert_ne!(
        new_leader, first_leader,
        "the killed node is still reporting itself leader"
    );

    let (fetched, counted) = c21_measure_window().await;
    assert!(
        (C21_MIN_FETCHES..=C21_MAX_FETCHES).contains(&fetched),
        "after failover the origin saw {fetched} fetches in {C21_WINDOW:?}. \
         Below {C21_MIN_FETCHES} means the survivors never took the schedule \
         up; above {C21_MAX_FETCHES} means more than one of them did. \
         Fleet-wide rift_cluster_source_polls_total moved by {counted}."
    );

    // The resumed poller does real work, not just requests: a content change
    // has to reach the whole surviving fleet without anyone asking for it.
    let (status, body) = origin_republish(&[(
        C21_DOC_PATH,
        source_document(&[(C21_PORT, "tracked-v2"), (C21_PORT_ADDED, "tracked-added")]),
    )])
    .await
    .expect("republish the source document");
    assert_eq!(status, 200, "the origin refused the new document: {body}");

    wait_converged_on(
        &survivors,
        u64::from(C21_PORT_ADDED),
        // Generous against the cadence: the change can land just after a poll,
        // so the fleet may legitimately wait a full interval before noticing.
        Duration::from_secs(90),
    )
    .await
    .expect("the tracking poll carries a content change to every surviving node");

    for host in survivor_hosts {
        let (status, record) = read_source(host, "tracked")
            .await
            .unwrap_or_else(|e| panic!("read the source on :{host}: {e}"));
        assert_eq!(status, 200, ":{host}: {record}");
        assert_eq!(
            record["lastOutcome"], "applied",
            ":{host}: the poll that carried the change is not recorded as \
             applied: {record}"
        );
    }
}

/// Watch the origin's counter and the fleet's own poll counter across one
/// window, returning `(fetches the origin served, polls the fleet counted)`.
///
/// Both numbers, not one: the origin's counter is the external truth and the
/// metric is the product's own account of it, so a scenario that reports them
/// together can say *which* of the two is wrong when they disagree.
async fn c21_measure_window() -> (u64, u64) {
    let fetches_before = origin_request_count().await.expect("origin request count");
    let polls_before = c21_fleet_poll_count().await;
    tokio::time::sleep(C21_WINDOW).await;
    let fetches_after = origin_request_count().await.expect("origin request count");
    let polls_after = c21_fleet_poll_count().await;
    (
        fetches_after.saturating_sub(fetches_before),
        polls_after.saturating_sub(polls_before),
    )
}

/// `rift_cluster_source_polls_total` summed over every outcome and every node
/// that is still answering.
///
/// Summed fleet-wide because only the leader increments it, so the sum counts
/// each poll exactly once — and a fleet that grew a second poller shows up here
/// as double counting. A node that is down (C21 kills one) or has never polled
/// contributes zero rather than failing the read: its absence is expected, and
/// treating it as an error would turn the kill into a harness failure.
async fn c21_fleet_poll_count() -> u64 {
    let mut total = 0.0;
    for node in &NODES {
        for outcome in ["applied", "unchanged", "skipped", "error"] {
            let family = format!(r#"rift_cluster_source_polls_total{{outcome="{outcome}"}}"#);
            total += metric(node.metrics, &family).await.unwrap_or(0.0);
        }
    }
    total as u64
}

/// Poll until exactly one of the *surviving* nodes reports itself leader.
///
/// [`wait_single_leader`] cannot serve here: it reads all three nodes, and the
/// killed one stops answering rather than reporting zero, so the scenario would
/// be waiting on a metrics endpoint that is gone.
async fn c21_wait_surviving_leader(dead: usize) -> usize {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut leaders = Vec::new();
        for (i, node) in NODES.iter().enumerate() {
            if i == dead {
                continue;
            }
            if metric(node.metrics, r#"rift_cluster_members{state="leader"}"#)
                .await
                .is_ok_and(|v| v == 1.0)
            {
                leaders.push(i);
            }
        }
        if leaders.len() == 1 {
            return leaders[0];
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the surviving nodes reported {leaders:?} as leader; expected \
             exactly one within 60s of the kill"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// C22's two documents and the ports each declares. Two sources, not one,
/// because the scenario has to assert two things a single source cannot hold at
/// once: a drift flag that survives, and a digest that still short-circuits
/// (drift deliberately defeats the short circuit, so the same source cannot
/// carry both claims).
const C22_CLEAN_PATH: &str = "/c22-clean.json";
const C22_DRIFTED_PATH: &str = "/c22-drifted.json";
const C22_CLEAN_PORT: u16 = 6630;
const C22_DRIFTED_PORT: u16 = 6631;

/// **C22: sources, their provenance and their drift flags survive a full
/// cluster restart.**
///
/// The C15/C18 harness pattern exactly: `stop` then `start` every node (never
/// `recreate` — each node keeping its own state directory across the restart is
/// precisely what is under test), then `wait_all_ready` + `wait_cluster_formed`
/// before asserting anything.
///
/// Three properties, checked on **every** node from that node's own applied
/// state:
///
/// - the source records themselves — uri, mode, `onDrift`, and the whole `last…`
///   block a pull wrote;
/// - the provenance stamped on each config, which is what `/_cluster/config`
///   reports and what an operator compares across nodes;
/// - the replicated `drifted` flag, which is not a node's opinion: it is
///   computed from provenance that lives in the state machine, so every replica
///   flips it at the same log index for the same reason. A fleet that came back
///   having forgotten a hand edit would silently re-apply a source over an
///   operator's deliberate change under `onDrift: skip`.
///
/// Then the sharper check the issue asks for: a post-restart pull of the
/// **clean** source must still short-circuit on the unchanged digest. That is
/// the only assertion here that distinguishes "the source row came back" from
/// "the source's *last applied digest* came back" — a record restored without
/// its `last` block would re-apply identical content on every pull forever, and
/// every other check above would still pass.
///
/// A separate clean source is needed for it because drift deliberately defeats
/// the short circuit (`!record.drifted && applied_digest() == digest`): an
/// operator who hand-edited an imposter and then pulls is doing the ordinary
/// repair, and answering "unchanged" there would make drift unfixable except by
/// editing the document upstream.
///
/// **Correction to the issue's named mutation.** The issue names "`sm_sources`
/// omitted from the snapshot". Measured, that mutant cannot be killed at this
/// tier and it is not this scenario's fault: openraft's snapshot policy here is
/// the default `LogEntries(5000)` (`crates/rift-cluster/src/raft/node.rs` builds
/// `Config { .. ..Default::default() }`), and a chaos stack commits a few dozen
/// entries — so no snapshot is ever built, and `stop`/`start` restores from each
/// node's own redb rather than over the wire. This is the same correction C18
/// carries. The snapshot round trip is gated in-process instead, by
/// `provenance_is_reported_and_survives_snapshot_restore` in
/// `crates/rift-cluster/src/raft/store.rs`, which drives `build_snapshot` /
/// `install_snapshot` directly. What *does* kill this scenario is the restart
/// path it actually exercises: see the chaos README's C22 entry.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c22_sources_survive_a_full_cluster_restart() {
    let cluster = Cluster::up_with_overlays(&["sources.overlay.yml"])
        .await
        .expect("fleet comes up");
    wait_origin_ready(SOURCES_READY_TIMEOUT)
        .await
        .expect("the counting origin's admin API answers");

    let (status, body) = origin_publish(&[
        (
            C22_CLEAN_PATH,
            source_document(&[(C22_CLEAN_PORT, "clean-v1")]),
        ),
        (
            C22_DRIFTED_PATH,
            source_document(&[(C22_DRIFTED_PORT, "drifted-v1")]),
        ),
    ])
    .await
    .expect("publish the source documents");
    assert_eq!(status, 201, "the origin refused the documents: {body}");

    for host in SOURCES_CLUSTER_HOST_PORTS {
        wait_sources_reachable(host, SOURCES_READY_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("cluster port :{host}: {e}"));
    }

    for (id, path) in [("clean", C22_CLEAN_PATH), ("drifted", C22_DRIFTED_PATH)] {
        let (status, body) = declare_source(
            SOURCES_CLUSTER_HOST_PORTS[0],
            &serde_json::json!({
                "id": id,
                "uri": format!("{SOURCES_ORIGIN_BASE_URL}{path}"),
                // `skip` on the drifted source, so a stray pull cannot quietly
                // repair the drift this scenario needs to survive the restart.
                "onDrift": if id == "drifted" { "skip" } else { "overwrite" },
            }),
        )
        .await
        .expect("declare a source");
        assert_eq!(status, 200, "declaring {id} was refused: {body}");

        let (status, report) = pull_source(SOURCES_CLUSTER_HOST_PORTS[0], id)
            .await
            .expect("pull a source");
        assert_eq!(status, 200, "pulling {id} was refused: {report}");
        assert_eq!(report["unchanged"], false, "{id}: {report}");
    }
    for port in [C22_CLEAN_PORT, C22_DRIFTED_PORT] {
        wait_converged(u64::from(port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("source imposter {port} did not reach every node: {e}"));
    }

    // The hand edit that drifts one source: an ordinary admin write against a
    // port the source owns, exactly as an operator debugging in production
    // would make it.
    let status = put_stubs(NODES[0].admin, C22_DRIFTED_PORT, hand_edited_stubs())
        .await
        .expect("hand-edit a source-owned imposter");
    assert_eq!(status, 200, "the hand edit was refused");
    wait_drift(
        &SOURCES_CLUSTER_HOST_PORTS,
        "drifted",
        true,
        CONVERGE_TIMEOUT,
    )
    .await
    .expect("the hand edit is visible as drift on every node before the restart");

    // The record as it stood before the restart, read from the node that made
    // every write. Compared field for field afterwards, on every node.
    let (status, before) = read_source(SOURCES_CLUSTER_HOST_PORTS[0], "clean")
        .await
        .expect("read the clean source");
    assert_eq!(status, 200, "{before}");
    assert_eq!(before["lastOutcome"], "applied", "{before}");
    assert!(
        before["lastDigest"].is_string(),
        "a pull that applied must have recorded a digest: {before}"
    );

    // The restart: every node down, then every node up. The origin container is
    // untouched, so the document is still there to be re-fetched — which is
    // what makes the short-circuit check below a statement about the fleet's
    // memory rather than about the origin's availability.
    for node in &NODES {
        cluster.stop(node.name).expect("SIGTERM the node");
    }
    for node in &NODES {
        cluster.start(node.name).expect("restart the node");
    }
    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the fleet comes back");
    cluster
        .wait_cluster_formed(Duration::from_secs(120))
        .await
        .expect("the fleet re-forms a cluster");
    for port in [C22_CLEAN_PORT, C22_DRIFTED_PORT] {
        wait_converged(u64::from(port), CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| {
                panic!("source imposter {port} did not rebind after the restart: {e}")
            });
    }
    for host in SOURCES_CLUSTER_HOST_PORTS {
        wait_sources_reachable(host, SOURCES_READY_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("cluster port :{host} after the restart: {e}"));
    }

    for (idx, host) in SOURCES_CLUSTER_HOST_PORTS.iter().enumerate() {
        let name = NODES[idx].name;

        let (status, after) = read_source(*host, "clean")
            .await
            .unwrap_or_else(|e| panic!("{name}: read the clean source: {e}"));
        assert_eq!(status, 200, "{name}: {after}");
        for field in [
            "uri",
            "mode",
            "onDrift",
            "lastDigest",
            "lastOutcome",
            "ports",
            "drifted",
        ] {
            assert_eq!(
                after[field], before[field],
                "{name}: the clean source's {field} did not survive the restart. \
                 Before: {before}. After: {after}"
            );
        }

        let (status, drifted) = read_source(*host, "drifted")
            .await
            .unwrap_or_else(|e| panic!("{name}: read the drifted source: {e}"));
        assert_eq!(status, 200, "{name}: {drifted}");
        assert_eq!(
            drifted["drifted"], true,
            "{name}: the fleet came back having forgotten a hand edit. Under \
             onDrift: skip that means the next pull silently overwrites an \
             operator's deliberate change: {drifted}"
        );

        let (status, config) = cluster_config(*host)
            .await
            .unwrap_or_else(|e| panic!("{name}: /_cluster/config: {e}",));
        assert_eq!(status, 200, "{name}: {config}");
        for (port, source_id) in [(C22_CLEAN_PORT, "clean"), (C22_DRIFTED_PORT, "drifted")] {
            assert_eq!(
                provenance_of(&config, port)
                    .as_ref()
                    .map(|(id, _)| id.as_str()),
                Some(source_id),
                "{name}: port {port} lost the provenance that says which source \
                 owns it — which is what the drift flag is computed from: {config}"
            );
        }
    }

    // The digest survived, not merely the row: an identical pull writes nothing
    // at all. `last_applied` standing still is what makes that exact — a
    // re-apply would move it on every node.
    let (status, config_before) = cluster_config(SOURCES_CLUSTER_HOST_PORTS[0])
        .await
        .expect("/_cluster/config before the post-restart pull");
    assert_eq!(status, 200, "{config_before}");
    let applied_before = config_before["last_applied"].clone();

    let (status, report) = pull_source(SOURCES_CLUSTER_HOST_PORTS[0], "clean")
        .await
        .expect("re-pull the clean source after the restart");
    assert_eq!(status, 200, "the post-restart pull was refused: {report}");
    assert_eq!(
        report["unchanged"], true,
        "the fleet re-applied content it already held. The source row came back \
         from disk but its last applied digest did not, so every pull from here \
         on writes a log entry for nothing: {report}"
    );
    assert_eq!(report["changed"], serde_json::json!([]), "{report}");

    let (status, config_after) = cluster_config(SOURCES_CLUSTER_HOST_PORTS[0])
        .await
        .expect("/_cluster/config after the post-restart pull");
    assert_eq!(status, 200, "{config_after}");
    assert_eq!(
        config_after["last_applied"], applied_before,
        "an unchanged pull moved the applied index, so it wrote a log entry \
         after all"
    );
}

/// The stub list a hand edit leaves behind: one catch-all answering
/// `hand-edited`, so the edit is visible in committed content and not only in a
/// flag.
///
/// A stub replacement (`PUT /imposters/:port/stubs`) rather than a whole-imposter
/// `POST`, because it is what an operator debugging in production actually
/// reaches for — and because the two travel different paths into the state
/// machine (`PatchStubs` vs `PutImposter`), only one of which can be exercised
/// per scenario. Both mark drift; this is the one an operator uses.
fn hand_edited_stubs() -> serde_json::Value {
    serde_json::json!([
        { "responses": [{ "is": { "statusCode": 200, "body": "hand-edited" } }] }
    ])
}

/// Poll every node's source surface until `id` reports the drift flag `want`.
///
/// Polled rather than read once: `drifted` is set by an ordinary committed op
/// (the hand edit), so a node that has not applied it yet is behind, not wrong.
/// Returns the last reading on failure, so an assertion says what it saw.
async fn wait_drift(hosts: &[u16], id: &str, want: bool, timeout: Duration) -> Result<(), String> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let mut seen = Vec::with_capacity(hosts.len());
        for host in hosts {
            match read_source(*host, id).await {
                Ok((200, record)) => seen.push(record["drifted"].clone()),
                Ok((status, body)) => {
                    seen.push(serde_json::json!({ "status": status, "body": body }))
                }
                Err(e) => seen.push(serde_json::json!({ "error": e.to_string() })),
            }
        }
        if seen.iter().all(|v| *v == serde_json::Value::Bool(want)) {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "source {id:?} did not report drifted={want} on every node \
                 within {timeout:?}; last reading: {seen:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// C23's document path and the imposter the source owns.
const C23_DOC_PATH: &str = "/c23.json";
const C23_PORT: u16 = 6640;

/// **C23: a hand edit shows as drift fleet-wide, and the next pull overwrites
/// it.**
///
/// Drift is what makes a source-backed fleet honest about the difference
/// between what an operator declared and what the fleet is actually serving.
/// The flag is *replicated* state, not a node's opinion — it is computed from
/// provenance that lives in the state machine — so it is asserted on every
/// node, not on the one that took the edit.
///
/// **Only the `overwrite` arm runs here, deliberately.** `on_drift` has three
/// arms and all three are already covered in-process, over real HTTP, by #134's
/// suite in `crates/rift-cluster-server/tests/sources.rs`
/// (`a_skipped_pull_does_not_short_circuit_the_pull_that_resolves_it` for
/// `skip`, and the state machine's own `drifted_source_fails_when_asked` for
/// `fail`). What containers add is process death and the operator-facing
/// surface, neither of which differs between the arms — so triplicating a
/// multi-minute scenario would buy a third copy of the same evidence and spend
/// the tier's budget on it. `overwrite` is the arm run here because it is the
/// default, and because it is the only one whose effect is visible in committed
/// content rather than only in a report field.
///
/// The repair is asserted on the committed config, not on the pull's report: a
/// report saying `applied` while the fleet still holds the hand edit is exactly
/// the failure worth catching, and `/_cluster/imposters` is where each node's
/// own committed body can be read.
///
/// Mutation: making `RedbStateMachine::mark_drifted` a no-op
/// (`crates/rift-cluster/src/raft/store.rs`) — the drift flag never raised —
/// turns this red on the post-edit check, with all three nodes reporting
/// `false`. Note what still passes under it: the repair pull, and the committed
/// content afterwards. Drift is what the operator *sees*, and a fleet that
/// silently loses it keeps working right up until someone sets `onDrift: skip`
/// and finds their deliberate edit overwritten anyway. See the chaos README's
/// C23 entry for the actual failure message.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c23_drift_flags_and_pull_overwrites() {
    let _cluster = Cluster::up_with_overlays(&["sources.overlay.yml"])
        .await
        .expect("fleet comes up");
    wait_origin_ready(SOURCES_READY_TIMEOUT)
        .await
        .expect("the counting origin's admin API answers");

    let (status, body) =
        origin_publish(&[(C23_DOC_PATH, source_document(&[(C23_PORT, "declared-v1")]))])
            .await
            .expect("publish the source document");
    assert_eq!(status, 201, "the origin refused the document: {body}");

    for host in SOURCES_CLUSTER_HOST_PORTS {
        wait_sources_reachable(host, SOURCES_READY_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("cluster port :{host}: {e}"));
    }

    let (status, body) = declare_source(
        SOURCES_CLUSTER_HOST_PORTS[0],
        &serde_json::json!({
            "id": "c23-mocks",
            "uri": format!("{SOURCES_ORIGIN_BASE_URL}{C23_DOC_PATH}"),
            "onDrift": "overwrite",
        }),
    )
    .await
    .expect("declare the source");
    assert_eq!(status, 200, "declaring the source was refused: {body}");

    let (status, report) = pull_source(SOURCES_CLUSTER_HOST_PORTS[0], "c23-mocks")
        .await
        .expect("pull the source");
    assert_eq!(status, 200, "the pull was refused: {report}");
    wait_converged(u64::from(C23_PORT), CONVERGE_TIMEOUT)
        .await
        .expect("the source's imposter reaches every node");
    wait_drift(
        &SOURCES_CLUSTER_HOST_PORTS,
        "c23-mocks",
        false,
        CONVERGE_TIMEOUT,
    )
    .await
    .expect("a freshly pulled source is not drifted");

    // The hand edit: an ordinary admin write against a port the source owns.
    let status = put_stubs(NODES[0].admin, C23_PORT, hand_edited_stubs())
        .await
        .expect("hand-edit a source-owned imposter");
    assert_eq!(status, 200, "the hand edit was refused");
    wait_drift(
        &SOURCES_CLUSTER_HOST_PORTS,
        "c23-mocks",
        true,
        CONVERGE_TIMEOUT,
    )
    .await
    .expect("the hand edit is visible as drift on EVERY node, not only the one that took it");
    c23_assert_committed_body(C23_PORT, "hand-edited").await;

    // The repair. The document has not changed at all — only the fleet moved —
    // so a pull that answered "unchanged" here would make drift unrepairable
    // except by editing the document upstream.
    let (status, report) = pull_source(SOURCES_CLUSTER_HOST_PORTS[0], "c23-mocks")
        .await
        .expect("pull to repair the drift");
    assert_eq!(status, 200, "the repair pull was refused: {report}");
    assert_eq!(
        report["unchanged"], false,
        "the fleet no longer matches the source, so there IS something to do: {report}"
    );
    assert_eq!(
        report["skipped"], false,
        "onDrift: overwrite must apply, not record a decision to hold off: {report}"
    );
    assert_eq!(
        report["changed"],
        serde_json::json!([C23_PORT]),
        "the repair must name the port it rewrote: {report}"
    );

    wait_drift(
        &SOURCES_CLUSTER_HOST_PORTS,
        "c23-mocks",
        false,
        CONVERGE_TIMEOUT,
    )
    .await
    .expect("overwrite resolves the drift on every node");
    c23_assert_committed_body(C23_PORT, "declared-v1").await;
}

/// Assert every node's *committed* config for `port` carries `marker`.
///
/// Read from `/_cluster/imposters` rather than the admin API's own `/imposters`
/// listing: the latter answers with what this node's engine has bound, which is
/// a different question from what the fleet agreed to hold — and "applied" that
/// never reached the log is precisely the failure C23 exists to catch.
async fn c23_assert_committed_body(port: u16, marker: &str) {
    for (idx, host) in SOURCES_CLUSTER_HOST_PORTS.iter().enumerate() {
        let name = NODES[idx].name;
        let (status, imposters) = cluster_imposters(*host)
            .await
            .unwrap_or_else(|e| panic!("{name}: /_cluster/imposters: {e}"));
        assert_eq!(status, 200, "{name}: {imposters}");
        let config = committed_config(&imposters, port)
            .unwrap_or_else(|| panic!("{name}: no committed config for port {port}: {imposters}"));
        let rendered = config.to_string();
        assert!(
            rendered.contains(marker),
            "{name}: the committed config for port {port} does not carry \
             {marker:?}: {rendered}"
        );
    }
}

// ---------------------------------------------------------------------------
// Issue #165 — C24-C27: the tenancy/RBAC/audit boundary, at the container tier.
//
// These four are the only scenarios in this tier that run against a *closed*
// admin plane. Every other one relies on RFC-002 §3.4 leaving the plane open
// while the fleet holds no principal, which is why they send no credential.
// `tenancy.overlay.yml` boots these with `MB_APIKEY`, so a fleet-admin
// credential exists from the first request — see that file's header for why
// the scenario cannot bootstrap one over HTTP itself.
// ---------------------------------------------------------------------------

/// Poll every node until `GET /imposters/{port}` (sent as `credential`, acting
/// explicitly as `tenant`) answers `200` — C24 and C27's convergence gate for
/// an imposter owned by a non-default tenant.
///
/// Not [`wait_converged_with_key`]: that reads `GET /imposters`, the
/// collection listing, which issue #182 filters to the caller's own tenant —
/// and the fleet admin's list defaults to `default` when no `X-Rift-Tenant` is
/// sent, so it can never observe a port owned by `acme`, `alpha` or `beta`. A
/// single-port read carries the tenant explicitly and is filtered by the
/// per-resource ownership gate, not the list filter, so it sees exactly the
/// resource asked for.
///
/// Doubles as the binding-convergence check when `credential` is the tenant's
/// own principal rather than the fleet admin's: a `200` here requires both the
/// imposter *and* the principal's binding to have replicated to that node.
/// Probed this way rather than `GET /admin/whoami`, which classifies no
/// action and would answer `200` to anyone who authenticates — going green on
/// a node that replicated the principal row but not the binding, the exact
/// race this gate exists to exclude. (C25 lost a container run to that
/// mistake; see `c25_probe`.)
async fn wait_imposter_visible_as(port: u16, credential: &str, tenant: &str, timeout: Duration) {
    for node in &NODES {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let (status, body) = admin_as(
                node.admin,
                "GET",
                &format!("/imposters/{port}"),
                None,
                Some(credential),
                Some(tenant),
            )
            .await
            .unwrap_or_else(|e| panic!("{}: read {port} as {tenant}: {e}", node.name));
            if status == 200 || std::time::Instant::now() > deadline {
                assert_eq!(
                    status, 200,
                    "{}: {port} never became visible to {tenant}: {body}",
                    node.name
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// One probe in C24's matrix: what to ask, and how to ask it.
struct MatrixProbe {
    label: &'static str,
    method: &'static str,
    path: String,
    body: Option<serde_json::Value>,
}

/// The §4.1 action matrix C24 drives through every node.
///
/// Deliberately spans three shapes, because the claim is about the *whole*
/// decision surface and not one route: a read the role holds, writes it does
/// not, and fleet-scoped routes it is not bound to at all (the `403`/`404`
/// split — refused-because-not-permitted versus invisible-because-not-yours).
fn c24_matrix(port: u16) -> Vec<MatrixProbe> {
    vec![
        // A read of a resource the caller's own tenant owns. `GET /imposters`
        // (the collection) is deliberately not the probe here: issue #182
        // filters that list to the caller's own tenant, so it would also
        // answer `200` — but through `tenant_owned_ports`, a different code
        // path from the per-port ownership gate in `authorize_action` that is
        // the actual thing this issue added. Addressing the port directly
        // exercises that gate itself, which is the sharper claim.
        MatrixProbe {
            label: "imposter.read",
            method: "GET",
            path: format!("/imposters/{port}"),
            body: None,
        },
        MatrixProbe {
            label: "imposter.write",
            method: "POST",
            path: "/imposters".to_owned(),
            body: Some(serde_json::json!({
                "port": port,
                "protocol": "http",
                "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": "nope" } }] }]
            })),
        },
        MatrixProbe {
            label: "imposter.delete",
            method: "DELETE",
            path: format!("/imposters/{port}"),
            body: None,
        },
        MatrixProbe {
            label: "audit.read",
            method: "GET",
            path: "/admin/audit?since=0&limit=5".to_owned(),
            body: None,
        },
        MatrixProbe {
            label: "tenant.manage",
            method: "GET",
            path: "/admin/tenants".to_owned(),
            body: None,
        },
        MatrixProbe {
            label: "cluster.admin",
            method: "GET",
            path: "/admin/audit/sink".to_owned(),
            body: None,
        },
    ]
}

/// Replace a node's own admin authority in a response body with a fixed token,
/// so C24 can compare bodies across nodes byte-for-byte.
///
/// `GET /imposters/:port` renders `_links.self.href` as
/// `http://127.0.0.1:<that node's admin port>/imposters/:port`. Three nodes
/// therefore return three different bodies for an identical, identically-decided
/// request — a self-referential URL, not a divergence.
///
/// **Narrow on purpose.** This substitutes only the authority the request was
/// *sent to*; it does not touch any other host, port or field. So a body that
/// named a *different* node — a link leaking the leader's address, say — would
/// survive canonicalisation and still fail the comparison, which is the kind of
/// divergence this scenario exists to catch. Blanket-stripping `_links` would
/// have hidden it.
fn c24_canonical(admin: u16, body: serde_json::Value) -> serde_json::Value {
    let canonical = body
        .to_string()
        .replace(&format!("127.0.0.1:{admin}"), "NODE");
    // Not `unwrap_or(body)`: falling back to the un-canonicalised body on a
    // parse failure would compare the wrong thing and report it as a fleet
    // divergence. The substitution is inside JSON string literals and
    // introduces no escapes, so a failure here is a harness bug and should say
    // so rather than quietly change what is being asserted.
    serde_json::from_str(&canonical)
        .unwrap_or_else(|e| panic!("canonicalised body is not valid JSON ({e}): {canonical}"))
}

/// C24 — one principal, one role, the full action matrix through all three
/// nodes, and every verdict identical **including the body**.
///
/// The point is not "authorization works". It is that authorization gives the
/// *same answer everywhere*, which is what consensus is being paid for: the
/// binding was accepted by one node and every node must decide from it.
///
/// Bodies are compared, not just statuses. A fleet where one node refuses with
/// a different reason — or a different `403`/`404` classification — has diverged
/// in exactly the way that is invisible to a status-only assertion.
///
/// **Runs in a non-default tenant (`acme`), not `default`.** This used to be
/// unrunnable: `admin_front::authorize_action`'s fail-closed guard (issue
/// #161, blockers B2/B3) answered RFC-002 §8.4's 404 for every non-`default`
/// tenant, because `raft::store`'s `desired_configs`/`desired_routes` skipped
/// non-default tenants when binding the local engine — running the matrix in
/// `acme` made **every** probe 404 regardless of role, so authorization was
/// never the thing being measured, and the vacuity assertions at the foot of
/// this scenario are what caught it.
///
/// Issue #182 replaced that blanket guard with a narrower per-resource
/// ownership gate (see `authorize_action`'s doc for both halves: the read/sync
/// paths becoming tenant-aware, and the gate that had to land before the old
/// guard could come off). A tenant other than `default` is now genuinely
/// served, and running the matrix there is the stronger claim of the two —
/// agreement across nodes for a tenant that used to be unreachable, rather
/// than for the one every route quietly fell back to. The role still
/// genuinely discriminates in `acme`: the `imposter.read`/`write`/`delete`
/// probes come from the viewer's tenant-scoped binding, and the 404 half of
/// the split comes from the fleet-scoped routes (`GET /admin/tenants`,
/// `GET /admin/audit/sink` both scope to `FLEET_SCOPE`), which a tenant-bound
/// principal holds no binding for regardless of which tenant it is bound to.
///
/// Every request the viewer sends below carries an explicit `X-Rift-Tenant:
/// acme`: unlike `default`, `acme` is not what an omitted header resolves to
/// (`requested_tenant` defaults to `default`), and the viewer holds no
/// binding in `default` at all — an omitted header would deny every probe
/// with `NotBoundToTenant` before the matrix measured anything.
///
/// *Mutant:* an authorizer reading bindings from a per-node cache, or only from
/// the leader, must go red on the node that did not accept the binding write.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c24_rbac_enforcement_is_identical_through_any_node() {
    let _cluster = Cluster::up_with_overlays(&["tenancy.overlay.yml"])
        .await
        .expect("fleet comes up");
    wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles");

    // One tenant, one principal, one role — created once, through one node.
    // `acme`, not `default` (see this scenario's doc comment): this is now the
    // non-vacuous case.
    const C24_TENANT: &str = "acme";
    let (status, body) = create_tenant(NODES[0].admin, C24_TENANT, TENANCY_FLEET_KEY)
        .await
        .expect("create tenant");
    assert!(
        (200..300).contains(&status),
        "the fleet admin must be able to create a tenant: {status} {body}"
    );
    let (_viewer_id, viewer) =
        mint_principal(NODES[0].admin, C24_TENANT, "viewer", TENANCY_FLEET_KEY)
            .await
            .expect("mint a viewer in acme");

    // One imposter the viewer's own tenant owns, created by the fleet admin
    // acting explicitly *as* `acme` — the fleet admin's own binding is the
    // fleet scope, not `acme`, so an omitted `X-Rift-Tenant` would create in
    // `default` instead. The matrix needs a resource the viewer is genuinely
    // entitled to read; without it every probe answers 403/404 and agreement
    // across nodes proves nothing — which is exactly what the vacuity
    // assertions at the end of this scenario caught the first time it ran.
    const C24_PORT: u16 = 6510;
    let (status, body) = admin_as(
        NODES[0].admin,
        "POST",
        "/imposters",
        Some(&serde_json::json!({
            "port": C24_PORT,
            "protocol": "http",
            "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": "acme" } }] }]
        })),
        Some(TENANCY_FLEET_KEY),
        Some(C24_TENANT),
    )
    .await
    .expect("seed an imposter in acme");
    assert!(
        (200..300).contains(&status),
        "the fleet admin must be able to create in acme: {status} {body}"
    );

    // Every node must have applied both the imposter *and* the viewer's
    // binding before either can be asked about — or the scenario would be
    // racing consensus and calling the race a divergence. See
    // `wait_imposter_visible_as`'s doc for why `wait_converged_with_key`
    // cannot be used here, and for why polling as the viewer also covers the
    // binding.
    wait_imposter_visible_as(C24_PORT, TENANCY_FLEET_KEY, C24_TENANT, CONVERGE_TIMEOUT).await;
    wait_imposter_visible_as(C24_PORT, &viewer, C24_TENANT, CONVERGE_TIMEOUT).await;

    let mut verdicts: Vec<(&'static str, Vec<(u16, serde_json::Value)>)> = Vec::new();
    for probe in c24_matrix(C24_PORT) {
        let mut per_node = Vec::new();
        for node in &NODES {
            // Explicit `X-Rift-Tenant: acme` — see this scenario's doc comment
            // for why an omitted header would deny every probe instead of
            // measuring anything.
            let seen = admin_as(
                node.admin,
                probe.method,
                &probe.path,
                probe.body.as_ref(),
                Some(&viewer),
                Some(C24_TENANT),
            )
            .await
            .unwrap_or_else(|e| panic!("{} on {}: {e}", probe.label, node.name));
            per_node.push((seen.0, c24_canonical(node.admin, seen.1)));
        }
        verdicts.push((probe.label, per_node));
    }

    for (label, per_node) in &verdicts {
        let first = &per_node[0];
        for (i, seen) in per_node.iter().enumerate() {
            assert_eq!(
                seen, first,
                "{label}: {} answered {seen:?} where {} answered {first:?} — the same \
                 principal, the same action, a different verdict. Authorization must be a \
                 property of the fleet, not of whichever node was asked",
                NODES[i].name, NODES[0].name
            );
        }
    }

    // The matrix must actually have exercised a mix, or "every node agreed"
    // would be satisfied by a fleet that refused everything identically —
    // including one where authorization was switched off and every route 404'd.
    let statuses: std::collections::BTreeSet<u16> = verdicts.iter().map(|(_, v)| v[0].0).collect();
    // Per-probe, so a failure names which action produced which verdict rather
    // than only the set — the set alone cannot tell you what to fix.
    let seen: Vec<String> = verdicts
        .iter()
        .map(|(label, v)| format!("{label}={} {}", v[0].0, v[0].1))
        .collect();
    assert!(
        statuses.contains(&200),
        "the matrix must include something the viewer may do, or agreement proves nothing: \
         {statuses:?}\n{seen:#?}"
    );
    assert!(
        statuses.contains(&403),
        "the matrix must include a refusal inside the caller's own tenant (403): {statuses:?}"
    );
    assert!(
        statuses.contains(&404),
        "the matrix must include a fleet-scoped route the caller is not bound to, which is \
         invisible (404) rather than forbidden (403) — RFC-002 §8.4: {statuses:?}"
    );
    // The body comparison above is only evidence if there are bodies. The
    // harness renders an unparseable payload as `Null`, and three nodes all
    // returning `Null` compare equal — agreement that proves nothing, on the one
    // assertion this scenario exists to make.
    assert!(
        verdicts
            .iter()
            .all(|(_, per_node)| per_node.iter().all(|(_, body)| !body.is_null())),
        "every probe must have returned a real body, or comparing bodies across nodes is \
         vacuous:\n{seen:#?}"
    );
}

/// C25 — revocation across a partition.
///
/// The obvious assertion here is wrong, and stating why is the point. A
/// **partitioned minority replica has not applied the revocation**, so it will
/// still allow: that is inherent to consensus, not a defect. RFC-002 §3.1's
/// guarantee is against *replication lag in a healthy fleet*, never against a
/// replica that cannot see the commit. So this asserts the two things that are
/// actually claimed:
///
/// (a) the minority node cannot itself perform an authorization write, and
/// (b) the **very first** request through the previously-minority node after the
///     heal is refused, with the convergence window measured and bounded.
///
/// *Settled here, and recorded in `docs/architecture/08-tenancy-security.md`:*
/// a stale minority node **serves reads from its own applied state** rather than
/// refusing outright. Refusing would make a partition indistinguishable from a
/// misconfiguration and would take the whole read surface down on a node that is
/// merely behind; the fleet already answers "is this node current" through the
/// M3 staleness signal. What is not acceptable — and what (b) pins — is serving
/// stale *authority* after the node can see the commit again.
///
/// *Mutant:* any TTL cache over authorization data must go red post-heal, because
/// the first request through the healed node would still be allowed until it
/// expired.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c25_key_revocation_survives_a_partition() {
    // Both overlays, and the chaos one is not optional: it is what routes every
    // cluster link through toxiproxy (so `partition` can cut one) *and* what puts
    // each node on the `mgmt` network (so the isolated node stays assertable from
    // the host). With `tenancy` alone the partition is unmakeable and
    // `admin_via_mgmt` answers nothing — which is exactly how this first failed.
    let cluster = Cluster::up_with_overlays(&["chaos.overlay.yml", "tenancy.overlay.yml"])
        .await
        .expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles");

    create_tenant(NODES[0].admin, "acme", TENANCY_FLEET_KEY)
        .await
        .expect("create tenant");
    let (admin_id, tenant_admin) =
        mint_principal(NODES[0].admin, "acme", "tenant-admin", TENANCY_FLEET_KEY)
            .await
            .expect("mint a tenant admin in acme");

    // The binding is live on every node before anything is cut.
    for node in &NODES {
        let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
        loop {
            let (status, _) = c25_probe(node.admin, &tenant_admin).await;
            if status == 200 || std::time::Instant::now() > deadline {
                assert_eq!(status, 200, "node {} never applied the binding", node.name);
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    let minority = NODES
        .iter()
        .enumerate()
        .find(|(i, _)| *i != leader)
        .map(|(_, n)| n)
        .expect("a non-leader");
    let majority: Vec<_> = NODES.iter().filter(|n| n.name != minority.name).collect();

    cluster
        .partition(minority.name)
        .expect("cut the minority off");
    wait_admin_reachable_with_key(
        minority.admin_via_mgmt,
        Duration::from_secs(30),
        Some(TENANCY_FLEET_KEY),
    )
    .await
    .expect("the minority stays reachable over the mgmt path");

    // (a) The minority cannot perform an authorization write. It has no quorum,
    // so the write parks or times out rather than being applied locally — which
    // is what stops a partitioned node minting its own authority.
    let (status, body) = admin_with_key(
        minority.admin_via_mgmt,
        "POST",
        "/admin/tenants/acme/principals",
        Some(&serde_json::json!({ "displayName": "smuggled", "role": "editor" })),
        Some(TENANCY_FLEET_KEY),
    )
    .await
    .expect("the minority answers");
    assert!(
        status == 503 || status == 504,
        "a partitioned node must not apply an authorization write on its own: {status} {body}"
    );

    // Revoke on the majority side, where quorum is.
    let (status, body) = admin_with_key(
        majority[0].admin,
        "DELETE",
        &format!("/admin/tenants/acme/bindings/{admin_id}"),
        None,
        Some(TENANCY_FLEET_KEY),
    )
    .await
    .expect("revoke on the majority");
    assert!(
        (200..300).contains(&status),
        "the majority must commit the revocation: {status} {body}"
    );

    // The majority refuses immediately, and with the §8.4 404 rather than a 403:
    // the principal still authenticates — revoking a *binding* does not delete
    // the key — so what must change is that it is now bound to nothing and the
    // tenant is invisible to it.
    let (status, body) = c25_probe(majority[0].admin, &tenant_admin).await;
    assert_eq!(
        status, 404,
        "the side that committed the revocation must refuse the revoked key at once: {body}"
    );

    cluster.heal(minority).expect("heal the partition");

    // (b) The first request through the healed node, measured. Polled only for
    // the node to catch up — the assertion is that once it answers at all, it
    // answers *refused*, never once allowed.
    let started = std::time::Instant::now();
    let deadline = started + CONVERGE_TIMEOUT;
    let window;
    loop {
        let (status, body) = c25_probe(minority.admin, &tenant_admin).await;
        if status == 200 && std::time::Instant::now() < deadline {
            // Still behind: it has not applied the revocation yet. Keep
            // waiting, but this is the state the mutant would never leave.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        assert_eq!(
            status, 404,
            "the previously-minority node still honours a revoked key after the heal, past the \
             convergence window: {status} {body}"
        );
        window = started.elapsed();
        break;
    }
    assert!(
        window < CONVERGE_TIMEOUT,
        "the revocation must reach the healed node inside the convergence window, took {window:?}"
    );
}

/// C25's revocation probe: a route that needs a **grant**, not merely a valid key.
///
/// `GET /admin/whoami` was the obvious probe and it is the wrong one — it kept
/// answering `200` after the revocation and cost a container run to understand.
/// Revoking a *binding* does not delete the principal, so the key still
/// authenticates; `whoami` classifies no action (RFC-002 §4.3's `None` case) and
/// so returns `200` for anyone who authenticates at all. A scenario built on it
/// would go green against a fleet that had revoked nothing.
///
/// This lists a tenant's principals: `Action::TenantManage`, scoped by the path
/// to `acme`. Held by the `tenant-admin` binding (`200`) and by nothing at all
/// once that binding is gone — `decide` finds no binding for `acme` and renders
/// §8.4's `404`. So the before/after signal is `200` → `404`, which no amount of
/// "the key is still a key" can fake.
async fn c25_probe(admin: u16, key: &str) -> (u16, serde_json::Value) {
    admin_with_key(
        admin,
        "GET",
        "/admin/tenants/acme/principals",
        None,
        Some(key),
    )
    .await
    .unwrap_or_else(|e| panic!("principal list on :{admin}: {e}"))
}

/// How long C26's lagging node gets to catch up over `install_snapshot` once
/// it answers again. Generous relative to `CONVERGE_TIMEOUT`: a snapshot
/// transfer plus a full table-by-table apply is more work than the ordinary
/// per-write convergence that bound is sized for.
const SNAPSHOT_CATCHUP_TIMEOUT: Duration = Duration::from_secs(90);

/// C26 — the audit chain survives a full-fleet stop/start, and survives a
/// single lagging node's catch-up over a real `install_snapshot`.
///
/// Two phases, because they guard two different regressions this scenario has
/// caught before, in different code paths:
///
/// **Phase 1 — force the wire path.** A full-cluster restart alone never
/// exercises `install_snapshot`: every node restores from its own redb and
/// needs nothing from a peer, which is why this scenario's mutation target
/// (the issue's "`audit` table omitted from `SnapshotPayload`") used to
/// survive here — the same correction the README used to carry three times
/// over, once each for C18/C22/C26, now consolidated into one explanation
/// there. `RIFT_CLUSTER_SNAPSHOT_LOG_ENTRIES=10` (`chaos.overlay.yml`) is what
/// makes a lagging node unable to avoid the wire path: this phase stops one
/// follower, commits more than 10 entries through the other two (each an
/// `imposter.write`, so the audit chain keeps growing), and restarts it. With
/// `snapshot_policy = LogsSinceLast(10)` and `max_in_snapshot_log_to_keep = 0`
/// (`NodeConfig::snapshot_log_entries`, `crates/rift-cluster/src/raft/node.rs`,
/// pinned there by two unit tests), the leader purges the entries the
/// follower missed as soon as it snapshots, so the only way back is a real
/// `install_snapshot` — a real socket, real serialize/deserialize of
/// `SnapshotPayload`. The post-restart convergence check is what goes red if
/// the audit table is dropped from it.
///
/// *The evidence that this is what actually happened is second-order, not
/// direct, and that gap is deliberate rather than missed.* This tier has no
/// admin-API or Prometheus signal that fires specifically on
/// `install_snapshot` — checked, not assumed: `crates/rift-cluster/src/metrics.rs`
/// has no such family, `/_cluster/members` reports only `last_applied`, and
/// this file's own house rule (top of module) rules out a log line for it
/// even if one exists upstream in openraft. So instead of observing the RPC,
/// this phase asserts its *precondition*, from data the harness already reads:
/// the live nodes' own last committed revision, taken after the extra writes,
/// is checked to be more than 10 past the lagging node's revision from the
/// moment it stopped — which is exactly what `LogsSinceLast(10)` +
/// `max_in_snapshot_log_to_keep = 0` need to have already purged the entries
/// it is missing (that arithmetic is what the two unit tests above pin). A
/// regression that broke only the purge, leaving ordinary replication to
/// quietly cover for it, would not trip this assertion. Closing that
/// remaining gap needs a counter on `RedbStateMachine::install_snapshot`
/// itself, which is a `crates/` change out of scope for this worktree.
///
/// **Phase 2 — the full-fleet restart, kept rather than replaced.** Its own
/// mutant story is a different bug in a different place: "clearing `sm_audit`
/// whenever the store is opened" — the ordinary cold-start path, no snapshot
/// involved at all — went red only here, at `node rift-1 lost or reordered
/// audit rows across the restart`. Phase 1's install-snapshot path does not
/// subsume it: a node that never restarted a second time never re-opens its
/// store from cold, which is exactly the path that mutant lives on. Every
/// node's `(revision, action, resource)` projection must still be
/// byte-identical to its own pre-restart one and to every other node's.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c26_audit_chain_survives_a_full_cluster_restart() {
    let cluster = Cluster::up_with_overlays(&[
        "chaos.overlay.yml",
        "tenancy.overlay.yml",
        // Only this scenario stacks the snapshot knob. It purges the log as soon as a snapshot
        // covers it, which changes how *every* lagging node catches up — putting it in the shared
        // `chaos.overlay.yml` turned C4, C6 and C7 red. See that overlay's header.
        "snapshot-install.overlay.yml",
    ])
    .await
    .expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles");

    // A session of writes, spread across nodes so the audit stream is not one
    // node's local view of its own work. A `tenant.manage` write leads, so the
    // session spans two action kinds rather than only `imposter.write`.
    //
    // The imposters go to the **default** tenant. Not a limitation any more — issue #182 made
    // resource routes servable in every tenant — but this scenario is about the *audit chain*
    // surviving a restart, and `default` keeps that the only variable under test. `acme` is still
    // created below because the session under audit spans tenant writes too.
    create_tenant(NODES[0].admin, "acme", TENANCY_FLEET_KEY)
        .await
        .expect("create tenant");
    for (i, node) in NODES.iter().enumerate() {
        let port = 6520 + i as u16;
        let (status, body) = admin_with_key(
            node.admin,
            "POST",
            "/imposters",
            Some(&serde_json::json!({
                "port": port,
                "protocol": "http",
                "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": "audited" } }] }]
            })),
            Some(TENANCY_FLEET_KEY),
        )
        .await
        .expect("write");
        assert!(
            (200..300).contains(&status),
            "write through {}: {status} {body}",
            node.name
        );
        wait_converged_with_key(u64::from(port), CONVERGE_TIMEOUT, TENANCY_FLEET_KEY)
            .await
            .expect("converged");
    }

    let baseline = c26_audit_on_every_node(TENANCY_FLEET_KEY).await;
    assert!(
        baseline[0].len() >= NODES.len(),
        "the session must have produced rows to lose: {:?}",
        baseline[0].len()
    );
    for (i, rows) in baseline.iter().enumerate() {
        assert_eq!(
            rows, &baseline[0],
            "before any restart, node {} already disagrees with {}",
            NODES[i].name, NODES[0].name
        );
    }

    // ---- Phase 1: a real install_snapshot ----------------------------------

    let lagging_idx = NODES
        .iter()
        .enumerate()
        .find(|(i, _)| *i != leader)
        .map(|(i, _)| i)
        .expect("a non-leader");
    let lagging = &NODES[lagging_idx];
    let live: Vec<_> = NODES.iter().filter(|n| n.name != lagging.name).collect();
    assert_eq!(live.len(), 2, "exactly two nodes stay up under the lag");

    // The lagging node's own last committed revision at the instant it stops
    // -- captured from `baseline`, read moments earlier while all three still
    // agreed, so nothing commits between the read and the stop below.
    let lagging_revision_at_stop = baseline[lagging_idx]
        .iter()
        .map(|(revision, _, _)| *revision)
        .max()
        .expect("baseline has rows");

    cluster
        .stop(lagging.name)
        .expect("SIGTERM the lagging node");

    // n, matching RIFT_CLUSTER_SNAPSHOT_LOG_ENTRIES (chaos.overlay.yml) and the
    // LogsSinceLast(n) policy it drives (NodeConfig::snapshot_log_entries).
    const SNAPSHOT_LOG_ENTRIES: u64 = 10;
    // More than n committed while it's down. n alone would already guarantee
    // at least one full LogsSinceLast(n) window closes entirely after it
    // stopped (worst case, the window was freshly opened and needs exactly n
    // more entries); this is comfortably past that floor.
    const ENTRIES_WHILE_DOWN: usize = 15;
    for i in 0..ENTRIES_WHILE_DOWN {
        let via = live[i % live.len()];
        let (status, body) = admin_with_key(
            via.admin,
            "PUT",
            "/imposters/6520/stubs",
            Some(&serde_json::json!({
                "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": format!("audited-{i}") } }] }]
            })),
            Some(TENANCY_FLEET_KEY),
        )
        .await
        .unwrap_or_else(|e| panic!("write {i} while {} is down: {e}", lagging.name));
        assert!(
            (200..300).contains(&status),
            "write {i} via {}: {status} {body}",
            via.name
        );
    }

    // The two live nodes must still agree with each other -- otherwise the
    // "more than n past the lagging node" check below would be comparing
    // against a number that is not actually the fleet's.
    let live_rows_0 = c26_audit_rows(live[0].admin, live[0].name, TENANCY_FLEET_KEY).await;
    let live_rows_1 = c26_audit_rows(live[1].admin, live[1].name, TENANCY_FLEET_KEY).await;
    assert_eq!(
        live_rows_1, live_rows_0,
        "the two live nodes must agree with each other while the third is down"
    );
    let live_revision_after_lag_writes = live_rows_0
        .iter()
        .map(|(revision, _, _)| *revision)
        .max()
        .expect("the extra writes produced rows");

    // The precondition `install_snapshot` needs: see the doc comment above for
    // why this is the strongest evidence available at this tier without a new
    // metric or a log line.
    assert!(
        live_revision_after_lag_writes > lagging_revision_at_stop + SNAPSHOT_LOG_ENTRIES,
        "the lag must clear a full LogsSinceLast({SNAPSHOT_LOG_ENTRIES}) window: live revision \
         {live_revision_after_lag_writes}, {} stopped at revision \
         {lagging_revision_at_stop}",
        lagging.name
    );

    cluster
        .start(lagging.name)
        .expect("restart the lagging node");
    wait_admin_reachable_with_key(
        lagging.admin,
        Duration::from_secs(120),
        Some(TENANCY_FLEET_KEY),
    )
    .await
    .expect("the restarted node answers again");

    // Polled with a deadline, never slept-and-hoped: catch-up over
    // install_snapshot has no gauge of its own to wait on, so this polls the
    // one surface that only reaches parity once it has actually happened.
    let deadline = std::time::Instant::now() + SNAPSHOT_CATCHUP_TIMEOUT;
    let caught_up = loop {
        let rows = c26_audit_rows(lagging.admin, lagging.name, TENANCY_FLEET_KEY).await;
        if rows == live_rows_0 {
            break rows;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "{} never caught up to the live nodes' audit chain within \
                 {SNAPSHOT_CATCHUP_TIMEOUT:?}: got {} rows, wanted {}",
                lagging.name,
                rows.len(),
                live_rows_0.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    // (preferred evidence) — this is what goes red if a table is dropped from
    // `SnapshotPayload`: the lagging node comes back missing that table's rows.
    assert_eq!(
        caught_up, live_rows_0,
        "{} must converge to byte-identical audit rows after install_snapshot",
        lagging.name
    );

    // …and it really was `install_snapshot`, not replication that happened to work.
    //
    // Without this the scenario proves only the *precondition*: the fleet advanced far enough that
    // a snapshot install *should* have been required. A regression that broke only the purge —
    // leaving the log the lagging node needs still present — would let it catch up by ordinary
    // replication, converge to the same rows, and pass. The whole point of this scenario is that
    // the wire path is exercised, so the wire path is what is asserted.
    //
    // `rift_cluster_snapshots_installed_total` is a plain `IntCounter` registered at startup, so it
    // is always published and an absent family is a genuine scrape failure rather than a zero.
    let installed = metric(lagging.metrics, "rift_cluster_snapshots_installed_total")
        .await
        .unwrap_or_else(|e| panic!("{}'s metrics endpoint did not answer: {e}", lagging.name));
    assert!(
        installed >= 1.0,
        "{} converged, but never installed a snapshot ({installed}) — it caught up by log \
         replication, so this scenario is no longer exercising the wire path it exists for",
        lagging.name
    );

    // ---- Phase 2: the full-fleet restart ------------------------------------

    let before = c26_audit_on_every_node(TENANCY_FLEET_KEY).await;
    for (i, rows) in before.iter().enumerate() {
        assert_eq!(
            rows, &before[0],
            "before the full-fleet restart, node {} already disagrees with {}",
            NODES[i].name, NODES[0].name
        );
    }

    for node in &NODES {
        cluster.stop(node.name).expect("SIGTERM the node");
    }
    for node in &NODES {
        cluster.start(node.name).expect("restart the node");
    }
    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the fleet comes back");
    cluster
        .wait_cluster_formed(Duration::from_secs(120))
        .await
        .expect("the fleet re-forms a cluster");

    let after = c26_audit_on_every_node(TENANCY_FLEET_KEY).await;
    for (i, rows) in after.iter().enumerate() {
        assert_eq!(
            rows, &before[i],
            "node {} lost or reordered audit rows across the full-fleet restart",
            NODES[i].name
        );
    }
    for (i, rows) in after.iter().enumerate() {
        assert_eq!(
            rows, &after[0],
            "after the full-fleet restart, node {} disagrees with {}",
            NODES[i].name, NODES[0].name
        );
    }
}

/// `(revision, action, resource)` for one node's audit projection — the
/// ordered projection C26 compares. Full rows would drag in per-read fields;
/// this is the part that must be identical everywhere.
async fn c26_audit_rows(admin: u16, name: &str, key: &str) -> Vec<(u64, String, String)> {
    let (status, body) = admin_with_key(
        admin,
        "GET",
        "/admin/audit?since=0&limit=1000",
        None,
        Some(key),
    )
    .await
    .unwrap_or_else(|e| panic!("audit read on {name}: {e}"));
    assert_eq!(status, 200, "audit read on {name}: {body}");
    // `GET /admin/audit` answers a bare JSON array, not a `{"rows": [...]}`
    // envelope. Asserted rather than defaulted: an empty projection here would
    // make "every node agrees" trivially true and the whole scenario vacuous,
    // so a shape this does not recognise must stop the test.
    body.as_array()
        .unwrap_or_else(|| panic!("audit on {name} is not an array: {body}"))
        .iter()
        .map(|r| {
            let revision = r["revision"]
                .as_u64()
                .unwrap_or_else(|| panic!("row without a revision on {name}: {r}"));
            let action = r["action"]
                .as_str()
                .unwrap_or_else(|| panic!("row without an action on {name}: {r}"));
            let resource = r["resource"]
                .as_str()
                .unwrap_or_else(|| panic!("row without a resource on {name}: {r}"));
            (revision, action.to_owned(), resource.to_owned())
        })
        .collect()
}

/// `(revision, action, resource)` per row, per node — see [`c26_audit_rows`].
async fn c26_audit_on_every_node(key: &str) -> Vec<Vec<(u64, String, String)>> {
    let mut out = Vec::new();
    for node in &NODES {
        out.push(c26_audit_rows(node.admin, node.name, key).await);
    }
    out
}

/// C27 — tenancy isolates *ownership*, not the data plane.
///
/// Two tenants, one imposter each — the issue's own shape, and constructible
/// as of issue #182 (see C24's doc comment for what changed: the read/sync
/// paths are tenant-aware now, gated by a per-resource ownership check rather
/// than a blanket default-only refusal). `alpha`'s Editor can read and manage
/// its own imposter; so can `beta`'s. Neither can see the other's, and the
/// refusal is a **404** — byte-identical to a resource that does not exist, so
/// the surface cannot be used to enumerate ports a caller is not entitled to
/// (RFC-002 §8.4). The same boundary is asserted on the tenancy surface, where
/// tenants are genuinely served per-tenant: `alpha`'s Editor cannot list
/// `beta`'s principals, and `beta`'s Editor cannot list `alpha`'s.
///
/// **And the imposters answer unauthenticated traffic — through every node,
/// with no credential at all, even a wrong one.** That is RFC-002 §7's stated
/// non-goal asserted in anger, so nobody later "fixes" it into a breaking
/// change for every system under test: the data plane is the thing being
/// mocked, and putting a credential in front of it would break every caller
/// the mock exists to serve. This half of the claim is untouched by issue
/// #182: ownership governs who may *configure* a mock, exactly as much as
/// before, and still never who may call it.
///
/// *Mutant:* authenticating the data plane must go red. So must rendering the
/// cross-tenant refusal as `403`, or as a body distinguishable from the
/// ghost's, or a `beta` credential reaching `alpha`'s imposter (or the
/// reverse).
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c27_tenancy_isolates_ownership_but_not_the_data_plane() {
    let _cluster = Cluster::up_with_overlays(&["tenancy.overlay.yml"])
        .await
        .expect("fleet comes up");
    wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles");

    for tenant in ["alpha", "beta"] {
        create_tenant(NODES[0].admin, tenant, TENANCY_FLEET_KEY)
            .await
            .unwrap_or_else(|e| panic!("create {tenant}: {e}"));
    }
    let (_a_id, a_editor) = mint_principal(NODES[0].admin, "alpha", "editor", TENANCY_FLEET_KEY)
        .await
        .expect("mint alpha editor");
    let (_b_id, b_editor) = mint_principal(NODES[0].admin, "beta", "editor", TENANCY_FLEET_KEY)
        .await
        .expect("mint beta editor");

    // One imposter per tenant, each created by the fleet admin acting
    // explicitly *as* that tenant — the fleet admin's own binding is the
    // fleet scope, not `alpha` or `beta`, so an omitted `X-Rift-Tenant` would
    // create in `default` instead. Genuinely owned by `alpha` and `beta`
    // respectively, not both parked in `default` the way issue #161's guard
    // used to force.
    for (tenant, port) in [
        ("alpha", TENANCY_A_IMPOSTER_PORT),
        ("beta", TENANCY_B_IMPOSTER_PORT),
    ] {
        let (status, body) = admin_as(
            NODES[0].admin,
            "POST",
            "/imposters",
            Some(&serde_json::json!({
                "port": port,
                "protocol": "http",
                "stubs": [{ "responses": [{ "is": { "statusCode": 200, "body": tenant } }] }]
            })),
            Some(TENANCY_FLEET_KEY),
            Some(tenant),
        )
        .await
        .expect("create imposter");
        assert!(
            (200..300).contains(&status),
            "the fleet admin must be able to create in {tenant}: {status} {body}"
        );
    }
    // `wait_converged_with_key` cannot be used here — see
    // `wait_imposter_visible_as`'s doc. Polling as each tenant's own editor
    // also proves that editor's binding replicated, not just the imposter.
    wait_imposter_visible_as(
        TENANCY_A_IMPOSTER_PORT,
        &a_editor,
        "alpha",
        CONVERGE_TIMEOUT,
    )
    .await;
    wait_imposter_visible_as(TENANCY_B_IMPOSTER_PORT, &b_editor, "beta", CONVERGE_TIMEOUT).await;

    // Each tenant's Editor can read and manage its own imposter: a read, and
    // a write. `AddStub` is the write chosen because — unlike delete or a
    // whole-imposter replace — it does not remove the imposter this
    // scenario's later assertions still need.
    for (tenant, editor, port) in [
        ("alpha", &a_editor, TENANCY_A_IMPOSTER_PORT),
        ("beta", &b_editor, TENANCY_B_IMPOSTER_PORT),
    ] {
        let (status, body) = admin_as(
            NODES[0].admin,
            "GET",
            &format!("/imposters/{port}"),
            None,
            Some(editor),
            Some(tenant),
        )
        .await
        .expect("own-tenant read");
        assert_eq!(
            status, 200,
            "{tenant}'s editor must be able to read its own imposter: {body}"
        );

        let (status, body) = admin_as(
            NODES[0].admin,
            "POST",
            &format!("/imposters/{port}/stubs"),
            Some(&serde_json::json!({
                "stub": {
                    "predicates": [{ "equals": { "path": "/c27-managed" } }],
                    "responses": [{ "is": { "statusCode": 200, "body": "managed" } }]
                }
            })),
            Some(editor),
            Some(tenant),
        )
        .await
        .expect("own-tenant manage");
        assert!(
            (200..300).contains(&status),
            "{tenant}'s editor must be able to manage its own imposter: {status} {body}"
        );
    }

    // Ownership is isolated, and invisibly so: 404, never 403. Each editor
    // acts explicitly *as its own tenant* (`X-Rift-Tenant` matches its only
    // binding) and addresses the *other* tenant's port, so `decide` allows
    // the action and it is `authorize_action`'s ownership gate (issue #182)
    // that refuses it — a genuine cross-tenant attempt, not an accident of an
    // omitted header the way a bound-but-wrong-tenant request would be.
    let mut refusals = Vec::new();
    for (label, editor, tenant, method, other_port) in [
        (
            "alpha reads beta's",
            &a_editor,
            "alpha",
            "GET",
            TENANCY_B_IMPOSTER_PORT,
        ),
        (
            "alpha deletes beta's",
            &a_editor,
            "alpha",
            "DELETE",
            TENANCY_B_IMPOSTER_PORT,
        ),
        (
            "beta reads alpha's",
            &b_editor,
            "beta",
            "GET",
            TENANCY_A_IMPOSTER_PORT,
        ),
        (
            "beta deletes alpha's",
            &b_editor,
            "beta",
            "DELETE",
            TENANCY_A_IMPOSTER_PORT,
        ),
    ] {
        let (status, body) = admin_as(
            NODES[0].admin,
            method,
            &format!("/imposters/{other_port}"),
            None,
            Some(editor),
            Some(tenant),
        )
        .await
        .expect("cross-tenant attempt");
        assert_eq!(
            status, 404,
            "{label} imposter must be 404 — a 403 would confirm the port exists and turn this \
             into an enumeration oracle: {body}"
        );
        refusals.push((label, status, body));
    }

    // A nonexistent port must be indistinguishable from one a caller may not
    // see — status *and* body, since a differing body is an oracle just as
    // surely as a differing status.
    let (ghost_status, ghost_body) = admin_as(
        NODES[0].admin,
        "GET",
        "/imposters/6599",
        None,
        Some(&a_editor),
        Some("alpha"),
    )
    .await
    .expect("ghost read");
    assert_eq!(
        ghost_status, 404,
        "if a nonexistent port answered differently from one the caller may not see, the pair \
         would still be an enumeration oracle"
    );
    for (label, status, body) in &refusals {
        assert_eq!(
            (*status, body),
            (ghost_status, &ghost_body),
            "the {label} refusal differs from the refusal of a port that does not exist — that \
             difference is the oracle RFC-002 §8.4 forbids"
        );
    }

    // The same boundary on the tenancy surface, which *is* served per tenant:
    // neither editor may enumerate the other's principals.
    for (label, editor, other_tenant) in [
        ("alpha lists beta's", &a_editor, "beta"),
        ("beta lists alpha's", &b_editor, "alpha"),
    ] {
        let (status, body) = admin_with_key(
            NODES[0].admin,
            "GET",
            &format!("/admin/tenants/{other_tenant}/principals"),
            None,
            Some(editor),
        )
        .await
        .expect("cross-tenant principal list");
        assert_eq!(
            status, 404,
            "{label} principals must be 404, not 403: {body}"
        );
    }

    // …and the data plane answers everybody, through every node, with no
    // credential at all — and would even with a wrong one, since the data
    // plane does not check, rather than merely not being asked.
    for (tenant, host_ports) in [
        ("alpha", TENANCY_A_HOST_PORTS),
        ("beta", TENANCY_B_HOST_PORTS),
    ] {
        for (i, host_port) in host_ports.iter().enumerate() {
            let (status, _, body) = get_data_plane(*host_port, "/")
                .await
                .unwrap_or_else(|e| panic!("{tenant} data plane via {}: {e}", NODES[i].name));
            assert_eq!(
                status, 200,
                "{tenant}'s imposter must answer unauthenticated traffic through {} — RFC-002 \
                 §7: tenancy governs who may *configure* a mock, never who may call it",
                NODES[i].name
            );
            assert_eq!(
                body, tenant,
                "{tenant}'s imposter served the wrong body through {}",
                NODES[i].name
            );

            let (status, _, body) =
                get_data_plane_with(*host_port, "/", &[("authorization", "not-a-real-key")])
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "{tenant} data plane (bad credential) via {}: {e}",
                            NODES[i].name
                        )
                    });
            assert_eq!(
                status, 200,
                "{tenant}'s imposter must ignore a bogus credential through {} — the data plane \
                 has no authentication to fail, not merely none presented",
                NODES[i].name
            );
            assert_eq!(
                body, tenant,
                "{tenant}'s imposter served the wrong body (bad credential) through {}",
                NODES[i].name
            );
        }
    }
}
/// The journal imposter's port, and the front-door prefix that reaches it.
/// 6700 sits clear of every other scenario's data port (6300 pull-on-miss, 6400
/// flow-state, 6500/6501 tenancy, 6600 sources origin, 6001/6002 C4).
const JOURNAL_IMPOSTER_PORT: u16 = 6700;
const JOURNAL_ROUTE_PREFIX: &str = "/journal";

/// A merged `savedRequests` read: the set of recorded paths, and whether the
/// response was stamped `Rift-Cluster-Partial: true`.
///
/// `get_data_plane` rather than `get_json` because the header *is* half the
/// assertion and `get_json` drops headers. It is pointed at the admin port here
/// rather than a data port — the helper is a plain GET-with-headers against any
/// published port, and the merged read is an admin call.
async fn merged_saved_requests(
    admin: u16,
) -> anyhow::Result<(std::collections::BTreeSet<String>, bool)> {
    let (status, headers, body) = get_data_plane(
        admin,
        &format!("/imposters/{JOURNAL_IMPOSTER_PORT}/savedRequests"),
    )
    .await?;
    if status != 200 {
        anyhow::bail!("merged savedRequests answered {status}: {body}");
    }
    // Upstream answers a bare array; the cluster front may wrap it. The set of
    // paths is the contract either way.
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    let entries = parsed
        .as_array()
        .cloned()
        .or_else(|| {
            parsed
                .get("requests")
                .and_then(serde_json::Value::as_array)
                .cloned()
        })
        .unwrap_or_default();
    let paths = entries
        .iter()
        .filter_map(|entry| entry.get("path").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
        .collect();
    let partial = headers
        .get("rift-cluster-partial")
        .and_then(|value| value.to_str().ok())
        == Some("true");
    Ok((paths, partial))
}

/// The fleet-wide `numberOfRequests` for the journal imposter, as one node
/// reports it (issue #223: a per-node G-counter slot summed on read).
async fn journal_number_of_requests(admin: u16) -> anyhow::Result<u64> {
    let (status, body) = get_json(admin, &format!("/imposters/{JOURNAL_IMPOSTER_PORT}")).await?;
    if status != 200 {
        anyhow::bail!("imposter read answered {status}");
    }
    body.get("numberOfRequests")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| anyhow::anyhow!("imposter body carries no numberOfRequests: {body}"))
}

/// Drive `count` requests through node `index`'s **own** front door.
///
/// Per-node front doors are what make the shards genuinely distinct. Every node
/// serves imposter 6700 in-process (the front door dispatches to the local
/// `ImposterManager`, never a socket — RFC-001 §7.4.6), so a request through
/// rift-2's listener is recorded in rift-2's writer shard and nowhere else.
/// Spraying one published port instead would put every entry on one node and
/// the merge would be asserted against a single shard.
async fn drive_journal_traffic(index: usize, tag: &str, count: usize) -> anyhow::Result<()> {
    for n in 0..count {
        let (status, _, body) = get_data_plane(
            FRONT_DOOR_HOST_PORTS[index],
            &format!("{JOURNAL_ROUTE_PREFIX}/{tag}-{n}"),
        )
        .await?;
        if status != 200 {
            anyhow::bail!("node {index} front door answered {status} for {tag}-{n}: {body}");
        }
    }
    Ok(())
}

/// The C28–C30/C12 journal fixture: the recording imposter, the front-door
/// route, and the wait until every node's own front door dispatches it —
/// shared because four scenarios repeat it verbatim. (The #223 smoke scenario
/// that pioneered this setup was deleted when C29 landed, exactly as its own
/// README note directed: C29 is the same property measured properly.)
async fn journal_fixture(admin: u16) {
    let (status, body) = put_imposter_config(
        admin,
        &serde_json::json!({
            "port": JOURNAL_IMPOSTER_PORT,
            "protocol": "http",
            "recordRequests": true,
            "stubs": [{
                "responses": [{ "is": { "statusCode": 200, "body": "journal" } }]
            }]
        }),
    )
    .await
    .expect("admin write");
    assert_eq!(status, 201, "the journal imposter must be created: {body}");
    wait_converged(u64::from(JOURNAL_IMPOSTER_PORT), CONVERGE_TIMEOUT)
        .await
        .expect("the imposter converges fleet-wide before any traffic");

    let (status, body) = put_routes(
        admin,
        &serde_json::json!({
            "routes": [{
                "id": "journal",
                "match": { "path_prefix": JOURNAL_ROUTE_PREFIX },
                "target": { "port": JOURNAL_IMPOSTER_PORT },
            }],
        }),
    )
    .await
    .expect("route write");
    assert_eq!(status, 200, "the front-door route must be accepted: {body}");

    for (index, front_door) in FRONT_DOOR_HOST_PORTS.iter().enumerate() {
        let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
        loop {
            if let Ok((200, _, _)) = get_data_plane(
                *front_door,
                &format!("{JOURNAL_ROUTE_PREFIX}/ready-{index}"),
            )
            .await
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node {index}'s front door never began dispatching the replicated route"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

/// The paths [`journal_fixture`]'s own readiness probes record, present in
/// every scenario's expected set. Kept as a function of the same loop rather
/// than a magic `3`: the probes ARE recorded requests (`recordRequests` is on),
/// and a scenario that forgot them would chase a phantom off-by-three.
fn fixture_probe_paths() -> std::collections::BTreeSet<String> {
    (0..NODES.len())
        .map(|index| format!("{JOURNAL_ROUTE_PREFIX}/ready-{index}"))
        .collect()
}

/// C28 (#228): the fleet journal is exact under node kill —
/// `test_journal_merge_exact` in anger, against the "exact under node kill"
/// exit bar.
///
/// Refinement against the implemented merge semantics (issue #223): the merge's
/// roster is the **applied membership**, so a SIGKILLed node — which by
/// definition never left — keeps every merged read honestly stamped
/// `Rift-Cluster-Partial: true`: its replica-cached entries still merge in, but
/// the cache *could* be stale and the stamp says so. "No partial once the
/// roster settles" therefore means: once the node is back. The scenario
/// asserts all three phases:
/// 1. kill a follower → every survivor still answers **exactly N** (the dead
///    writer's entries served from replica caches), stamped partial;
/// 2. fleet `numberOfRequests == N` on every survivor throughout;
/// 3. restart the node → the stamp clears; the survivors keep the exact N,
///    and the victim converges on N minus its own lost shard — its journal
///    was memory and merged reads never hand a writer its shard back. Pinned
///    as-is with the honesty gap (short answer, no stamp) filed as #349.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c28_fleet_journal_is_exact_under_node_kill() {
    let cluster = Cluster::up_with_overlays(&["chaos.overlay.yml", "front-door.overlay.yml"])
        .await
        .expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles");
    journal_fixture(NODES[leader].admin).await;

    // Uneven, distinctly tagged spray through every node's own front door —
    // the worst LB (no affinity), and the shape that catches a merge serving
    // one shard three times.
    let sprays = [(0usize, "a", 4usize), (1, "b", 7), (2, "c", 2)];
    for (index, tag, count) in sprays {
        drive_journal_traffic(index, tag, count)
            .await
            .unwrap_or_else(|e| panic!("driving node {index}'s front door: {e}"));
    }
    let mut expected = fixture_probe_paths();
    for (_, tag, count) in sprays {
        for n in 0..count {
            expected.insert(format!("{JOURNAL_ROUTE_PREFIX}/{tag}-{n}"));
        }
    }

    // Wait until the spray is fully merged and unstamped **on every node** —
    // the healthy-fleet standing gate, and the cache warmer: a node's replica
    // cache of its peers' shards fills as a side effect of its own
    // merge-on-read pulls, so polling only one node would leave the others'
    // caches cold and the kill would (correctly!) expose them as missing the
    // dead shard.
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut settled = true;
        let mut last = None;
        for node in &NODES {
            let read = merged_saved_requests(node.admin).await;
            settled &= matches!(&read, Ok((paths, false)) if *paths == expected);
            last = Some((node.name, read));
        }
        if settled {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the healthy fleet never converged on the sprayed set everywhere: {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Kill a follower with SIGKILL: no drain, no leave — its shard now exists
    // only in the survivors' replica caches.
    let victim_index = (0..NODES.len())
        .find(|index| *index != leader)
        .expect("a non-leader exists");
    let victim = &NODES[victim_index];
    cluster.kill(victim.name).expect("kill the follower");
    let survivors: Vec<usize> = (0..NODES.len())
        .filter(|index| *index != victim_index)
        .collect();

    // Every survivor answers exactly N — dead shard included, from cache —
    // and says partial, because a cached shard is honest about possibly
    // lagging its unreachable writer. The fleet count is polled in the same
    // loop rather than asserted one-shot after it: the summed G-counter can
    // converge a beat behind the merged set, and "set exact" and "count
    // exact" are one claim about the same moment.
    let expected_count = expected.len() as u64;
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut settled = true;
        let mut last = None;
        for index in survivors.iter().copied() {
            let read = merged_saved_requests(NODES[index].admin).await;
            let count = journal_number_of_requests(NODES[index].admin).await;
            settled &= matches!(&read, Ok((paths, true)) if *paths == expected)
                && count.as_ref().is_ok_and(|count| *count == expected_count);
            last = Some((index, read, count));
        }
        if settled {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a survivor never served the exact set and count from cache under kill: {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // The roster settles the only way it can after a SIGKILL: the node comes
    // back (`start` — retained disk, though the journal is memory and dies
    // with the process). The stamp clears everywhere; the *survivors* keep
    // the exact set, because their replica caches hold the victim's lost
    // shard. The victim itself converges on the set minus its own pre-kill
    // entries: a merged read asks each peer for that peer's OWN writer shard,
    // never for the asker's lost shard back, so entries whose writer crashed
    // live on only in the survivors' caches — Ch.7's volatility contract
    // applied to one node.
    //
    // The victim's short answer is STAMPED PARTIAL (#349). When this scenario
    // was written the answer was short and unstamped, and it pinned that as a
    // documented honesty gap rather than papering over it; #349 closed the gap
    // by having a peer report the lowest seq it still caches of the ASKER's
    // shard, which the asker compares against the durable boot floor #351
    // gave it. So the two halves of this phase now assert different things on
    // purpose: the survivors are complete and unstamped, the victim is short
    // and says so. A victim that answered short and unstamped would be the
    // original defect; one that answered the full set would mean the entries
    // came back, which the volatility contract says they do not.
    cluster.start(victim.name).expect("restart the victim");
    for node in &NODES {
        wait_voters(node, 3.0, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("{} did not reconverge on 3 voters: {e}", node.name));
    }
    let victim_lost: std::collections::BTreeSet<String> = {
        let (_, tag, count) = sprays[victim_index];
        let mut lost: std::collections::BTreeSet<String> = (0..count)
            .map(|n| format!("{JOURNAL_ROUTE_PREFIX}/{tag}-{n}"))
            .collect();
        lost.insert(format!("{JOURNAL_ROUTE_PREFIX}/ready-{victim_index}"));
        lost
    };
    let victim_expected: std::collections::BTreeSet<String> =
        expected.difference(&victim_lost).cloned().collect();
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let reads: Vec<_> = {
            let mut collected = Vec::new();
            for (index, node) in NODES.iter().enumerate() {
                collected.push((index, merged_saved_requests(node.admin).await));
            }
            collected
        };
        let settled = reads.iter().all(|(index, read)| {
            let victim = *index == victim_index;
            let want = if victim { &victim_expected } else { &expected };
            // The victim stamps partial and the survivors do not — see the note
            // above. Asserting the flag per node rather than a single shared
            // value is the whole point: `false` everywhere was the pre-#349
            // defect, and `true` everywhere would mean the survivors had gone
            // degraded too, which nothing here should cause.
            matches!(read, Ok((paths, partial)) if paths == want && *partial == victim)
        });
        if settled {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "after restart the survivors must answer the exact set unstamped, and the \
             victim the set minus its lost shard stamped partial (#349): {reads:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// C12 (#228, reserved in Ch.12): clears are exact under ±5 s clock skew —
/// `test_journal_clear`'s adversarial form, on the `faketime.overlay.yml`
/// fleet (rift-1 believes +5 s, rift-3 believes −5 s).
///
/// The invariant is *zero timestamp consultation*: a clear is a replicated
/// generation bump (issue #224), so which node issued it and what that node's
/// clock believed must be irrelevant. Three probes, each of which a
/// timestamp-based clear would fail under this spread:
/// 1. a clear issued from the **fast** node erases everything, fleet-wide —
///    including entries whose recorded timestamps are "in the future" of the
///    slow node's clock;
/// 2. every post-clear append survives everywhere, counts exact — a
///    time-window clear would eat appends stamped "before" the skewed clear;
/// 3. clears racing from the two most-skewed nodes converge (generations
///    compose by max; both mean "ignore everything before me").
///
/// The scenario first proves the skew is real — the two extreme nodes' `Date`
/// headers must disagree by most of the 10 s spread — so a broken overlay
/// fails loudly instead of letting every probe pass vacuously on a
/// synchronized fleet.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c12_clears_are_exact_under_clock_skew() {
    let _cluster = Cluster::up_with_overlays(&[
        "chaos.overlay.yml",
        "front-door.overlay.yml",
        "faketime.overlay.yml",
    ])
    .await
    .expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles");

    // The vacuity guard: rift-1 (+5) and rift-3 (−5) must actually disagree.
    let date_secs = |headers: &reqwest::header::HeaderMap| -> Option<i64> {
        let raw = headers.get("date")?.to_str().ok()?;
        let parsed = httpdate::parse_http_date(raw).ok()?;
        parsed
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|since| since.as_secs() as i64)
    };
    let (_, fast_headers, _) = get_data_plane(NODES[0].admin, "/imposters")
        .await
        .expect("the fast node answers");
    let (_, slow_headers, _) = get_data_plane(NODES[2].admin, "/imposters")
        .await
        .expect("the slow node answers");
    let (fast, slow) = match (date_secs(&fast_headers), date_secs(&slow_headers)) {
        (Some(fast), Some(slow)) => (fast, slow),
        other => panic!("both nodes must answer a parseable Date header: {other:?}"),
    };
    assert!(
        fast - slow >= 8,
        "the faketime overlay must skew the extremes ~10 s apart; \
         observed {}s — a synchronized fleet would pass every probe vacuously",
        fast - slow
    );
    println!(
        "c12 artifact: observed clock spread between extremes = {}s",
        fast - slow
    );

    journal_fixture(NODES[leader].admin).await;
    let clear = |admin: u16| async move {
        reqwest::Client::new()
            .delete(format!(
                "http://127.0.0.1:{admin}/imposters/{JOURNAL_IMPOSTER_PORT}/savedRequests"
            ))
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map(|response| response.status().as_u16())
    };

    // Probe 1+2: spray, clear from the fast node, assert empty fleet-wide,
    // then spray again and assert every post-clear append survives.
    for (index, tag, count) in [(0usize, "pre", 3usize), (1, "pre2", 3), (2, "pre3", 3)] {
        drive_journal_traffic(index, tag, count)
            .await
            .unwrap_or_else(|e| panic!("spraying node {index}: {e}"));
    }
    let status = clear(NODES[0].admin)
        .await
        .expect("the fast-node clear answers");
    assert_eq!(status, 200, "the clear must be accepted");

    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut all_empty = true;
        let mut last = None;
        for node in &NODES {
            let read = merged_saved_requests(node.admin).await;
            all_empty &= matches!(&read, Ok((paths, false)) if paths.is_empty());
            last = Some((node.name, read));
        }
        if all_empty {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a skewed clear must erase fleet-wide with no clock consulted: {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let mut survivors = std::collections::BTreeSet::new();
    for (index, tag, count) in [(0usize, "post", 2usize), (1, "post2", 4), (2, "post3", 3)] {
        drive_journal_traffic(index, tag, count)
            .await
            .unwrap_or_else(|e| panic!("post-clear spraying node {index}: {e}"));
        for n in 0..count {
            survivors.insert(format!("{JOURNAL_ROUTE_PREFIX}/{tag}-{n}"));
        }
    }
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut settled = true;
        let mut last = None;
        for node in &NODES {
            let read = merged_saved_requests(node.admin).await;
            settled &= matches!(&read, Ok((paths, false)) if *paths == survivors);
            last = Some((node.name, read));
        }
        let count_exact = journal_number_of_requests(NODES[leader].admin)
            .await
            .is_ok_and(|count| count == survivors.len() as u64);
        if settled && count_exact {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "every post-clear append must survive everywhere, counts exact: {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Probe 3: clears racing from the two clock extremes converge.
    let (fast_clear, slow_clear) = tokio::join!(clear(NODES[0].admin), clear(NODES[2].admin));
    assert_eq!(fast_clear.expect("fast racing clear answers"), 200);
    assert_eq!(slow_clear.expect("slow racing clear answers"), 200);
    let mut round_c = std::collections::BTreeSet::new();
    for (index, tag, count) in [(0usize, "race", 2usize), (2, "race2", 2)] {
        drive_journal_traffic(index, tag, count)
            .await
            .unwrap_or_else(|e| panic!("post-race spraying node {index}: {e}"));
        for n in 0..count {
            round_c.insert(format!("{JOURNAL_ROUTE_PREFIX}/{tag}-{n}"));
        }
    }
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut settled = true;
        let mut last = None;
        for node in &NODES {
            let read = merged_saved_requests(node.admin).await;
            // Exact equality on EVERY node, like probes 1 and 2: a subset
            // check is satisfied by an empty answer, and a slow-clock node
            // that applied the racing clear late and ate its own post-race
            // append is precisely the timestamp-sensitivity this scenario
            // exists to detect.
            settled &= matches!(&read, Ok((paths, false)) if *paths == round_c);
            last = Some((node.name, read));
        }
        if settled {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "racing skewed clears must converge exactly on every node (nothing \
             pre-race survives, everything post-race does): {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The proxy-recording fixture (#228, C10/C11): a counting origin on the
/// standalone `source-origin` server (in-network by service name, its own
/// single-node admin published at 46525 — an already-reserved port, so no new
/// compose surface), plus a proxyOnce and a proxyAlways imposter on the fleet
/// routed through the front doors.
const PROXY_ORIGIN_IMPOSTER_PORT: u16 = 6810;
const PROXY_ONCE_IMPOSTER_PORT: u16 = 6811;
const PROXY_ALWAYS_IMPOSTER_PORT: u16 = 6812;
const PROXY_ORIGIN_ADMIN: u16 = 46525;
const PROXY_ORIGIN_URL: &str = "http://source-origin:6810";

async fn proxy_fixture(admin: u16, origin_wait_ms: u64) {
    // The origin counts by recording: every upstream call the fleet makes is a
    // saved request on the un-clustered origin, per path — a single-node truth
    // no merge semantics can blur. `wait` keeps claims Pending long enough for
    // C10 to kill their owners mid-flight; C11 passes 0.
    let (status, body) = put_imposter_config(
        PROXY_ORIGIN_ADMIN,
        &serde_json::json!({
            "port": PROXY_ORIGIN_IMPOSTER_PORT,
            "protocol": "http",
            "recordRequests": true,
            "stubs": [{
                "responses": [{
                    "is": { "statusCode": 200, "body": "from-origin" },
                    "behaviors": [{ "wait": origin_wait_ms }]
                }]
            }]
        }),
    )
    .await
    .expect("origin admin write");
    assert_eq!(status, 201, "the counting origin must be created: {body}");

    for (port, mode) in [
        (PROXY_ONCE_IMPOSTER_PORT, "proxyOnce"),
        (PROXY_ALWAYS_IMPOSTER_PORT, "proxyAlways"),
    ] {
        let (status, body) = put_imposter_config(
            admin,
            &serde_json::json!({
                "port": port,
                "protocol": "http",
                "stubs": [{
                    "responses": [{
                        "proxy": {
                            "to": PROXY_ORIGIN_URL,
                            "mode": mode,
                            "predicateGenerators": [{ "matches": { "path": true } }]
                        }
                    }]
                }]
            }),
        )
        .await
        .expect("fleet admin write");
        assert_eq!(status, 201, "the {mode} imposter must be created: {body}");
        wait_converged(u64::from(port), CONVERGE_TIMEOUT)
            .await
            .expect("the proxy imposter converges fleet-wide");
    }

    let (status, body) = put_routes(
        admin,
        &serde_json::json!({
            "routes": [
                {
                    "id": "once",
                    "match": { "path_prefix": "/once" },
                    "target": { "port": PROXY_ONCE_IMPOSTER_PORT },
                },
                {
                    "id": "always",
                    "match": { "path_prefix": "/always" },
                    "target": { "port": PROXY_ALWAYS_IMPOSTER_PORT },
                },
            ],
        }),
    )
    .await
    .expect("route write");
    assert_eq!(status, 200, "the proxy routes must be accepted: {body}");

    // Route dispatch readiness, per node. The probes go to the proxy
    // imposters and therefore reach the origin — their paths are excluded
    // from every per-path count below by using a dedicated prefix.
    for (index, front_door) in FRONT_DOOR_HOST_PORTS.iter().enumerate() {
        let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
        loop {
            if let Ok((200, _, _)) =
                get_data_plane(*front_door, &format!("/once/ready/{index}")).await
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node {index}'s front door never dispatched the proxy routes"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

/// Upstream calls the origin has served, counted per path — read from the
/// un-clustered origin's own admin, so it is a plain array with single-node
/// semantics.
async fn origin_calls_per_path() -> anyhow::Result<std::collections::BTreeMap<String, usize>> {
    let (status, body) = get_json(
        PROXY_ORIGIN_ADMIN,
        &format!("/imposters/{PROXY_ORIGIN_IMPOSTER_PORT}"),
    )
    .await?;
    if status != 200 {
        anyhow::bail!("origin imposter read answered {status}");
    }
    let mut counts = std::collections::BTreeMap::new();
    for entry in body
        .get("requests")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(path) = entry.get("path").and_then(serde_json::Value::as_str) {
            *counts.entry(path.to_owned()).or_insert(0) += 1;
        }
    }
    Ok(counts)
}

/// The recorded (non-proxy) stubs a node's applied config holds for `port`.
async fn recorded_stub_paths(admin: u16, port: u16) -> anyhow::Result<Vec<String>> {
    let (status, body) = get_json(admin, &format!("/imposters/{port}")).await?;
    if status != 200 {
        anyhow::bail!("imposter read answered {status}");
    }
    let mut paths = Vec::new();
    for stub in body
        .get("stubs")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        let is_proxy = stub
            .get("responses")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|responses| responses.iter().any(|r| r.get("proxy").is_some()));
        if is_proxy {
            continue;
        }
        if let Some(path) = stub
            .pointer("/predicates/0/equals/path")
            .and_then(serde_json::Value::as_str)
        {
            paths.push(path.to_owned());
        }
    }
    Ok(paths)
}

/// Fire one data-plane GET through node `index`'s front door, tolerating
/// transport errors (a killed node's listener dying mid-request is part of the
/// chaos, not a failure of the scenario).
async fn fire(index: usize, path: &str) -> Option<u16> {
    get_data_plane(FRONT_DOOR_HOST_PORTS[index], path)
        .await
        .ok()
        .map(|(status, _, _)| status)
}

/// C11 (#228, reserved in Ch.12): concurrent recording loses nothing.
///
/// Sharpened against upstream's own claim contract: a concurrent `InFlight`
/// loser **forwards without recording by design**, so "exactly one upstream
/// call per signature" is unpinnable during the racing window even on a single
/// node. What is pinnable, and what this asserts:
/// 1. exactly **one recorded stub** per proxyOnce signature, identical on
///    every node (zero lost, zero duplicated recordings);
/// 2. once Recorded, **zero further upstream calls** — a full post-settle
///    round from every node adds nothing to the origin's count;
/// 3. proxyAlways loses nothing either: every post-settle request still
///    reaches the origin (never replayed), and every signature's responses
///    merge into one stub per signature fleet-wide.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c11_concurrent_recording_loses_nothing() {
    let _cluster = Cluster::up_with_overlays(&[
        "chaos.overlay.yml",
        "front-door.overlay.yml",
        "sources.overlay.yml",
    ])
    .await
    .expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles");
    proxy_fixture(NODES[leader].admin, 0).await;

    const SIGS: usize = 6;
    // The racing window: every node fires every signature concurrently.
    let mut tasks = tokio::task::JoinSet::new();
    for sig in 0..SIGS {
        for node in 0..NODES.len() {
            tasks.spawn(async move {
                let once = fire(node, &format!("/once/p/{sig}")).await;
                let always = fire(node, &format!("/always/p/{sig}")).await;
                (once, always)
            });
        }
    }
    while let Some(joined) = tasks.join_next().await {
        let (once, always) = joined.expect("hammer task");
        assert_eq!(
            once,
            Some(200),
            "a raced proxyOnce request must still answer"
        );
        assert_eq!(
            always,
            Some(200),
            "a raced proxyAlways request must still answer"
        );
    }

    // Settle: every proxyOnce signature ends Recorded — one stub each, on
    // every node. The fixture's readiness probes went through the same
    // proxyOnce imposter, so they are recorded signatures too — three more
    // stubs, expected rather than mysterious.
    let expected: std::collections::BTreeSet<String> = (0..SIGS)
        .map(|sig| format!("/once/p/{sig}"))
        .chain((0..NODES.len()).map(|index| format!("/once/ready/{index}")))
        .collect();
    // A raced winner's publication can legitimately fail and release its
    // claim (the retryable contract) — and a released signature stays
    // unrecorded until *someone* asks again, which the finished hammer never
    // will. So the settle loop refires any signature recorded nowhere yet,
    // exactly as a real client's next request would; the invariant stays
    // "exactly one stub each, everywhere", and duplicates would still fail
    // the set comparison.
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT * 2;
    loop {
        let mut settled = true;
        let mut last = None;
        let mut recorded_anywhere: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        for node in &NODES {
            let paths = recorded_stub_paths(node.admin, PROXY_ONCE_IMPOSTER_PORT).await;
            if let Ok(paths) = &paths {
                recorded_anywhere.extend(paths.iter().cloned());
            }
            let as_set = paths
                .as_ref()
                .map(|p| p.iter().cloned().collect::<std::collections::BTreeSet<_>>());
            settled &= paths.as_ref().is_ok_and(|p| p.len() == expected.len())
                && as_set.as_ref().is_ok_and(|s| *s == expected);
            last = Some((node.name, paths));
        }
        if settled {
            break;
        }
        for sig in 0..SIGS {
            let path = format!("/once/p/{sig}");
            if !recorded_anywhere.contains(&path) {
                let _ = fire(sig % NODES.len(), &path).await;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "every node must hold exactly one recorded stub per signature: {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Post-settle: proxyOnce replays (origin frozen), proxyAlways still
    // proxies (origin grows by exactly one per request).
    let before = origin_calls_per_path().await.expect("origin answers");
    for sig in 0..SIGS {
        for node in 0..NODES.len() {
            assert_eq!(
                fire(node, &format!("/once/p/{sig}")).await,
                Some(200),
                "a recorded signature must replay"
            );
            assert_eq!(
                fire(node, &format!("/always/p/{sig}")).await,
                Some(200),
                "proxyAlways must keep proxying"
            );
        }
    }
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let after = origin_calls_per_path().await.expect("origin answers");
        let once_frozen = (0..SIGS).all(|sig| {
            let path = format!("/once/p/{sig}");
            after.get(&path) == before.get(&path)
        });
        let always_grew = (0..SIGS).all(|sig| {
            let path = format!("/always/p/{sig}");
            after.get(&path).copied().unwrap_or(0)
                == before.get(&path).copied().unwrap_or(0) + NODES.len()
        });
        if once_frozen && always_grew {
            println!(
                "c11 artifact: per-signature origin calls during the racing window: {:?}",
                (0..SIGS)
                    .map(|sig| before.get(&format!("/once/p/{sig}")).copied().unwrap_or(0))
                    .collect::<Vec<_>>()
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "post-settle: proxyOnce must freeze the origin and proxyAlways must not; \
             before={before:?} after={after:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// C10 (#228, reserved in Ch.12): proxyOnce's two critical moments, measured.
///
/// Moment one kills a **claim owner** while claims are Pending (a slow origin
/// holds them open); moment two kills the **Raft leader** while publications
/// are racing commit. Both phases assert the same invariants:
/// - zero wedged signatures — every signature ends Recorded (a stub on every
///   node) and replaying (a final round adds nothing at the origin);
/// - the duplicate-upstream bound is *measured* and printed, per Ch.12's
///   philosophy, and asserted as one origin call per fire (initial + counted
///   degraded-window refires): the contract bounds *recordings* at
///   1 + ownership changes, while degraded forwards during the owner outage
///   are by-design and tracked by the scenario itself — anything beyond its
///   own fire count is genuine duplication.
/// - "recorded but stub-less" is asserted through its observable consequence:
///   any signature the fleet replays must show its recorded stub in every
///   node's applied config (no marker-inspection endpoint exists, and with
///   predicate generators configured a recording always carries its stub).
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c10_proxy_once_survives_owner_and_leader_kills() {
    let cluster = Cluster::up_with_overlays(&[
        "chaos.overlay.yml",
        "front-door.overlay.yml",
        "sources.overlay.yml",
    ])
    .await
    .expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles");
    proxy_fixture(NODES[leader].admin, 3000).await;

    let mut phase_sig_base = 0usize;
    for phase in ["owner-kill", "leader-kill"] {
        const SIGS: usize = 8;
        let sigs: Vec<String> = (phase_sig_base..phase_sig_base + SIGS)
            .map(|sig| format!("/once/q/{sig}"))
            .collect();
        phase_sig_base += SIGS;

        let leader_now = wait_single_leader(CONVERGE_TIMEOUT)
            .await
            .expect("a leader settles before the phase");
        let victim_index = match phase {
            "owner-kill" => (0..NODES.len())
                .find(|index| *index != leader_now)
                .expect("a non-leader exists"),
            _ => leader_now,
        };

        // Fire every signature concurrently through the two nodes that will
        // survive, so requests outlive the victim; the slow origin keeps
        // their claims Pending while the victim dies.
        let firers: Vec<usize> = (0..NODES.len())
            .filter(|index| *index != victim_index)
            .collect();
        let mut tasks = tokio::task::JoinSet::new();
        for (n, path) in sigs.iter().cloned().enumerate() {
            let node = firers[n % firers.len()];
            tasks.spawn(async move { fire(node, &path).await });
        }
        tokio::time::sleep(Duration::from_millis(600)).await;
        cluster
            .kill(NODES[victim_index].name)
            .unwrap_or_else(|e| panic!("kill the {phase} victim: {e}"));
        // The vacuity guard for "mid-flight": the 600 ms pre-kill sleep is a
        // timing choice, not a proof. The origin records a request on arrival,
        // *before* its 3 s wait, so at kill time it must already have seen at
        // least one of this phase's signatures — otherwise the kill landed
        // before any claim was in flight and the phase silently degraded to
        // "kill, then fire", which asserts much less than it claims to.
        let at_kill = origin_calls_per_path()
            .await
            .unwrap_or_else(|e| panic!("{phase}: the origin answers at kill time: {e}"));
        assert!(
            sigs.iter().any(|sig| at_kill.contains_key(sig)),
            "{phase}: the kill landed before any claim was in flight — the mid-flight \
             premise is vacuous (origin had seen none of {sigs:?})"
        );
        while let Some(joined) = tasks.join_next().await {
            // The response itself may be an error if its claim RPC raced the
            // kill; the engine's contract is degrade-don't-wedge, asserted
            // below by every signature ending Recorded.
            let _ = joined.expect("hammer task");
        }

        cluster
            .start(NODES[victim_index].name)
            .unwrap_or_else(|e| panic!("restart the {phase} victim: {e}"));
        for node in &NODES {
            wait_voters(node, 3.0, CONVERGE_TIMEOUT)
                .await
                .unwrap_or_else(|e| panic!("{} did not reconverge: {e}", node.name));
        }

        // Zero wedged: retry each not-yet-recorded signature until every one
        // is Recorded on every node. The budget is deliberately its own —
        // one retry pass costs up to `sigs × 3 s` at the slow origin alone
        // (a still-unrecorded signature proxies, and the origin's wait is
        // what held claims open for the kill), so the standard 45 s
        // convergence timeout affords barely two passes around a
        // kill-and-rejoin. Double it: the assertion is "eventually Recorded,
        // never wedged", and starving the loop measures the harness, not the
        // claim machinery.
        let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT * 2;
        let mut refires: std::collections::BTreeMap<String, usize> =
            sigs.iter().map(|sig| (sig.clone(), 0)).collect();
        loop {
            let mut recorded_on: std::collections::BTreeMap<&str, usize> =
                sigs.iter().map(|sig| (sig.as_str(), 0)).collect();
            for node in &NODES {
                let paths = recorded_stub_paths(node.admin, PROXY_ONCE_IMPOSTER_PORT)
                    .await
                    .unwrap_or_default();
                for sig in &sigs {
                    if paths.contains(sig) {
                        *recorded_on.entry(sig.as_str()).or_insert(0) += 1;
                    }
                }
            }
            if recorded_on.values().all(|nodes| *nodes == NODES.len()) {
                break;
            }
            for (n, path) in sigs.iter().enumerate() {
                // A signature recorded *somewhere* is committed and only
                // replicating; refiring it would replay, not record. Only the
                // genuinely unrecorded ones need another upstream round — and
                // each refire is counted, because while a claim's owner is
                // dead every retry legitimately forwards without recording
                // (degrade-don't-wedge), so the origin-call bound below is a
                // function of exactly this counter.
                if recorded_on.get(path.as_str()) == Some(&0) {
                    *refires.entry(path.clone()).or_insert(0) += 1;
                    let _ = fire(n % NODES.len(), path).await;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{phase}: a signature wedged — not Recorded fleet-wide within the deadline"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Exactly one recorded stub per signature, not merely at-least-one:
        // the Ch.12 row promises a duplicate bound, and `contains` alone
        // would wave a double recording through.
        for node in &NODES {
            let paths = recorded_stub_paths(node.admin, PROXY_ONCE_IMPOSTER_PORT)
                .await
                .unwrap_or_else(|e| panic!("{phase}: {} answers stubs: {e}", node.name));
            for sig in &sigs {
                let copies = paths.iter().filter(|path| *path == sig).count();
                assert_eq!(
                    copies, 1,
                    "{phase}: {} holds {copies} recorded stubs for {sig}",
                    node.name
                );
            }
        }

        // Replay steady-state: a full round adds nothing at the origin.
        let before = origin_calls_per_path().await.expect("origin answers");
        for path in &sigs {
            for node in 0..NODES.len() {
                assert_eq!(
                    fire(node, path).await,
                    Some(200),
                    "{phase}: a recorded signature must replay from every node"
                );
            }
        }
        let after = origin_calls_per_path().await.expect("origin answers");
        for path in &sigs {
            assert_eq!(
                after.get(path),
                before.get(path),
                "{phase}: replay must not call the origin for {path}"
            );
        }

        // The measured duplicate bound — the Ch.12 artifact. The contract
        // bounds *recordings* at 1 + ownership changes (Ch.6/Ch.7); upstream
        // *calls* additionally include every degraded forward the outage
        // window produced, and this scenario knows exactly how many of those
        // it manufactured: its own refires. So the sound per-signature bound
        // is `1 initial fire + refires` — one origin call per fire, at most —
        // and anything above it is genuine duplication (a double recording or
        // a replay that proxied), which is what the assertion exists to catch.
        let max_calls = sigs
            .iter()
            .map(|path| after.get(path).copied().unwrap_or(0))
            .max()
            .unwrap_or(0);
        let max_refires = refires.values().copied().max().unwrap_or(0);
        println!(
            "c10 artifact ({phase}): max upstream calls for one signature = {max_calls} \
             (max degraded-window refires = {max_refires}; recordings bound: 1 + ownership changes)"
        );
        for path in &sigs {
            let calls = after.get(path).copied().unwrap_or(0);
            let fired = 1 + refires.get(path).copied().unwrap_or(0);
            assert!(
                calls <= fired,
                "{phase}: {path} reached the origin {calls} times from only {fired} fires — \
                 a request that should have replayed proxied instead"
            );
        }
    }
}

/// One page of a vector-cursor walk (issue #225): the recorded paths this
/// page delivered, the `x-rift-next-index` token to continue from, and whether
/// `x-rift-truncated` was stamped.
async fn cursor_page(
    admin: u16,
    token: Option<&str>,
) -> anyhow::Result<(Vec<String>, String, bool)> {
    let path = match token {
        Some(token) => format!("/imposters/{JOURNAL_IMPOSTER_PORT}/savedRequests?since={token}"),
        None => format!("/imposters/{JOURNAL_IMPOSTER_PORT}/savedRequests"),
    };
    let (status, headers, body) = get_data_plane(admin, &path).await?;
    if status != 200 {
        anyhow::bail!("cursor read answered {status}: {body}");
    }
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    let entries = parsed.as_array().cloned().unwrap_or_default();
    let paths = entries
        .iter()
        .filter_map(|entry| entry.get("path").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
        .collect();
    let next = headers
        .get("x-rift-next-index")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("a merged cursor read must answer x-rift-next-index"))?;
    let truncated = headers
        .get("x-rift-truncated")
        .and_then(|value| value.to_str().ok())
        == Some("true");
    Ok((paths, next, truncated))
}

/// C30 (#228): the vector-cursor walk is gapless and duplicate-free across a
/// node kill and its return — `test_journal_cursor_merge` in anger.
///
/// Delivery is tallied by unique request path rather than `(node_id, seq)`:
/// every sprayed request is distinctly tagged, so "no path is ever delivered
/// twice" is duplicate-freedom and "the walked union equals the sprayed set"
/// is gaplessness — the same invariants, asserted through the public surface.
///
/// The SSE variant the issue sketches is deliberately absent: the merged SSE
/// tail was **not** shipped by #225 — `07-verification-plane.md` records
/// `.../savedRequests/stream` as still proxying per-node, with the cursor
/// token designed so the terminator is additive when it lands. Tailing a
/// per-node proxy here would assert single-node behavior and rot the moment
/// the merged terminator arrives.
///
/// Truncation is proven positively and negatively: a token far below a
/// shard's eviction watermark answers `x-rift-truncated: true`; a fresh
/// baseline read of the same evicting port does not.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c30_vector_cursor_walk_survives_membership_change() {
    let cluster = Cluster::up_with_overlays(&["chaos.overlay.yml", "front-door.overlay.yml"])
        .await
        .expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles");
    journal_fixture(NODES[leader].admin).await;

    let mut sprayed = fixture_probe_paths();
    let mut delivered: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let spray = |round: &str, per_node: [usize; 3]| -> Vec<(usize, String, usize)> {
        let mut plan = Vec::new();
        for (index, count) in per_node.into_iter().enumerate() {
            plan.push((index, format!("{round}-{index}"), count));
        }
        plan
    };

    // Round 1: a base set through every node, walked from a baseline read.
    for (index, tag, count) in spray("r1", [3, 4, 2]) {
        drive_journal_traffic(index, &tag, count)
            .await
            .unwrap_or_else(|e| panic!("spraying node {index}: {e}"));
        for n in 0..count {
            sprayed.insert(format!("{JOURNAL_ROUTE_PREFIX}/{tag}-{n}"));
        }
    }
    let walker = NODES[leader].admin;
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    let mut token = loop {
        // A transport hiccup is "not yet", never a panic: the loop's own
        // deadline is the failure path, and one dropped read out of a
        // 3-node poll must not kill a 25-iteration nightly run (issue #228
        // review, N1).
        let Ok((paths, next, truncated)) = cursor_page(walker, None).await else {
            assert!(
                std::time::Instant::now() < deadline,
                "the baseline page never answered"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };
        assert!(
            !truncated,
            "a baseline read is a snapshot and never truncated"
        );
        if paths
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            == sprayed
        {
            delivered.extend(paths);
            break next;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the baseline walk never converged on the sprayed set"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    // Round 2: more traffic everywhere, then kill a follower mid-walk. The
    // dead node's fresh entries must still arrive — from the survivors'
    // replica caches — and nothing already delivered may repeat.
    let victim_index = (0..NODES.len())
        .find(|index| *index != leader)
        .expect("a non-leader exists");
    for (index, tag, count) in spray("r2", [2, 3, 3]) {
        drive_journal_traffic(index, &tag, count)
            .await
            .unwrap_or_else(|e| panic!("spraying node {index}: {e}"));
        for n in 0..count {
            sprayed.insert(format!("{JOURNAL_ROUTE_PREFIX}/{tag}-{n}"));
        }
    }
    // Warm every node's replica cache with the victim's round-2 entries
    // before the kill — a node caches its peers' shards through its own
    // merge-on-read pulls, so every node is polled, not just the walker.
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut settled = true;
        for node in &NODES {
            let read = merged_saved_requests(node.admin).await;
            settled &= matches!(&read, Ok((paths, false)) if *paths == sprayed);
        }
        if settled {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "round 2 never fully merged everywhere before the kill"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    cluster
        .kill(NODES[victim_index].name)
        .expect("kill mid-walk");

    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    token = loop {
        let Ok((paths, next, _)) = cursor_page(walker, Some(&token)).await else {
            assert!(
                std::time::Instant::now() < deadline,
                "the walk never answered past the kill"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };
        for path in &paths {
            assert!(
                delivered.insert(path.clone()),
                "duplicate delivery of {path} after the kill"
            );
        }
        if delivered == sprayed {
            break next;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the walk never became gapless after the kill: missing {:?}",
            sprayed.difference(&delivered).collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    // Round 3: the victim returns, and the walk continues over the
    // **survivors'** new traffic.
    //
    // The defect this scenario found on its first in-anger run — a
    // crash-restart reusing sequence numbers from 1, colliding with its own
    // cached pre-kill history in the walk's `(node_id, seq)` identity, and
    // delivering a survivor-era path twice — was filed as #351 and **is
    // fixed**: a restarted writer now resumes above a durable per-port seq
    // floor, so its post-restart seqs sit strictly above every position a
    // held cursor could carry. This comment previously described that as an
    // open defect; it is not, as of `75a8692`.
    //
    // What is asserted below is still scoped to the survivors, and that is
    // now a *conservative* assertion rather than a forced one. Extending it
    // to require the victim's post-restart entries to arrive on the same walk
    // is the natural follow-up, and needs its own compose run to land safely
    // rather than being widened here on inference.
    cluster
        .start(NODES[victim_index].name)
        .expect("restart the victim");
    for node in &NODES {
        wait_voters(node, 3.0, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("{} did not reconverge: {e}", node.name));
    }
    for (index, tag, count) in spray("r3", [2, 2, 2]) {
        if index == victim_index {
            continue;
        }
        drive_journal_traffic(index, &tag, count)
            .await
            .unwrap_or_else(|e| panic!("spraying node {index}: {e}"));
        for n in 0..count {
            sprayed.insert(format!("{JOURNAL_ROUTE_PREFIX}/{tag}-{n}"));
        }
    }
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    token = loop {
        let Ok((paths, next, _)) = cursor_page(walker, Some(&token)).await else {
            assert!(
                std::time::Instant::now() < deadline,
                "the walk never answered past the restart"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };
        for path in &paths {
            assert!(
                delivered.insert(path.clone()),
                "duplicate delivery of {path} after the restart"
            );
        }
        if delivered == sprayed {
            break next;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the walk never caught the survivors' round 3: missing {:?}",
            sprayed.difference(&delivered).collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    // Truncation, both directions. The per-shard cap is max(500, 10_000/N) =
    // 3334 entries for a 3-voter fleet: overflow one shard well past it so
    // eviction demonstrably advanced its watermark past our token's position.
    // One shared client for the whole overflow: `drive_journal_traffic`
    // builds a client per request, which at 3,500 requests means 3,500 TCP
    // handshakes in ~20 s on the runner — ~100× any other scenario's volume.
    // A single keep-alive client is both faster and kinder to the ephemeral
    // port range the CI sysctl reserves around this suite.
    const OVERFLOW: usize = 3500;
    let overflow_client = reqwest::Client::new();
    for n in 0..OVERFLOW {
        let response = overflow_client
            .get(format!(
                "http://127.0.0.1:{}{JOURNAL_ROUTE_PREFIX}/evict-{n}",
                FRONT_DOOR_HOST_PORTS[leader]
            ))
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .unwrap_or_else(|e| panic!("overflow spray request {n}: {e}"));
        assert_eq!(
            response.status().as_u16(),
            200,
            "overflow spray request {n} must be recorded"
        );
    }
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let (Ok((_, _, truncated)), Ok((_, _, baseline_truncated))) = (
            cursor_page(walker, Some(&token)).await,
            cursor_page(walker, None).await,
        ) else {
            assert!(
                std::time::Instant::now() < deadline,
                "the truncation probes never answered"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };
        if truncated && !baseline_truncated {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "truncation must appear iff a presented position predates a watermark \
             (stale token: {truncated}, baseline: {baseline_truncated})"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// C29 (#228): partial honesty under partition, *measured* — the adversarial
/// tier over #223's smoke-level partition assertion (which this scenario
/// replaced, per that scenario's own deletion note): the stamps on both
/// sides, plus the **budget** (a partitioned merged read answers within the
/// documented 2 s peer budget, never hangs) and
/// the **metric** (`rift_cluster_journal_partial_reads_total` moves — headers
/// without metrics would leave operators blind to what tests can see).
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c29_partial_reads_answer_within_budget_and_count_themselves() {
    let cluster = Cluster::up_with_overlays(&["chaos.overlay.yml", "front-door.overlay.yml"])
        .await
        .expect("fleet comes up");
    let leader = wait_single_leader(CONVERGE_TIMEOUT)
        .await
        .expect("a leader settles");
    journal_fixture(NODES[leader].admin).await;

    for (index, tag, count) in [(0usize, "a", 3usize), (1, "b", 3), (2, "c", 3)] {
        drive_journal_traffic(index, tag, count)
            .await
            .unwrap_or_else(|e| panic!("driving node {index}'s front door: {e}"));
    }

    // The standing gate, asserted before anything is broken: a healthy fleet's
    // merged read is complete and NOT stamped, on every node — the same
    // pre-fault convergence loop C28/C30/C12 run, and the one this scenario
    // (the family's partial-stamp specialist) must not skip.
    let mut healthy = fixture_probe_paths();
    for (_, tag, count) in [(0usize, "a", 3usize), (1, "b", 3), (2, "c", 3)] {
        for n in 0..count {
            healthy.insert(format!("{JOURNAL_ROUTE_PREFIX}/{tag}-{n}"));
        }
    }
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let mut settled = true;
        let mut last = None;
        for node in &NODES {
            let read = merged_saved_requests(node.admin).await;
            settled &= matches!(&read, Ok((paths, false)) if *paths == healthy);
            last = Some((node.name, read));
        }
        if settled {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a healthy fleet must answer the complete set unstamped everywhere: {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let minority_index = (0..NODES.len())
        .find(|index| *index != leader)
        .expect("a non-leader exists");
    let minority = &NODES[minority_index];
    let majority = (0..NODES.len())
        .find(|index| *index != minority_index)
        .expect("a majority node exists");

    let partial_before = metric(
        NODES[majority].metrics,
        "rift_cluster_journal_partial_reads_total",
    )
    .await
    .unwrap_or(0.0);

    cluster
        .partition(minority.name)
        .expect("cut the minority off");
    wait_admin_reachable(minority.admin_via_mgmt, Duration::from_secs(30))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "{} must stay assertable over `mgmt` while partitioned ({e})",
                minority.name
            )
        });

    // Both sides answer stamped — and the majority side's answer is *timed*.
    // The peer budget is 2 s; 4 s of headroom separates "budgeted and
    // degraded" from "hung on the unreachable peer" without inviting flakes
    // on a loaded runner. The measured value is printed as the scenario
    // artifact Ch.12 asks for.
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    let measured = loop {
        let started = std::time::Instant::now();
        let majority_side = merged_saved_requests(NODES[majority].admin).await;
        let elapsed = started.elapsed();
        let minority_side = merged_saved_requests(minority.admin_via_mgmt).await;
        if let (Ok((_, true)), Ok((_, true))) = (&majority_side, &minority_side) {
            assert!(
                elapsed < Duration::from_secs(6),
                "a partitioned merged read must answer within the peer budget \
                 plus headroom, not hang: took {elapsed:?}"
            );
            break elapsed;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "both sides must answer stamped partial; \
             majority={majority_side:?} minority={minority_side:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    println!("c29 artifact: partitioned merged read answered in {measured:?} (peer budget 2s)");

    // Metrics honesty: the stamped reads counted themselves.
    let partial_after = metric(
        NODES[majority].metrics,
        "rift_cluster_journal_partial_reads_total",
    )
    .await
    .unwrap_or_else(|e| panic!("the partial-reads family must be scrapeable: {e}"));
    assert!(
        partial_after > partial_before,
        "rift_cluster_journal_partial_reads_total must move when reads are \
         stamped: before={partial_before} after={partial_after}"
    );

    // Heal and re-assert the standing gate: stamp gone, sets identical.
    cluster.heal(minority).expect("heal the partition");
    for node in &NODES {
        wait_voters(node, 3.0, CONVERGE_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("{} did not reconverge on 3 voters: {e}", node.name));
    }
    let deadline = std::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let reads: Vec<_> = {
            let mut collected = Vec::new();
            for node in &NODES {
                collected.push(merged_saved_requests(node.admin).await);
            }
            collected
        };
        let settled = reads.iter().all(|read| matches!(read, Ok((_, false))))
            && reads
                .iter()
                .filter_map(|read| read.as_ref().ok())
                .map(|(paths, _)| paths)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == 1;
        if settled {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "after heal every node must answer the same unstamped set: {reads:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The observability runtime lane's assertions are opt-in, and the opt-in is a
/// single workflow line (issue #316).
///
/// `verify.sh` gates its Prometheus and Grafana assertions on
/// `[ "${RIFT_OBSERVABILITY:-0}" = "1" ]` and otherwise runs the plain 3-node
/// smoke and exits 0. So dropping that `env:` line — or typo'ing the value to
/// `"true"` — leaves a job that still passes, on the same PRs, having asserted
/// nothing about Prometheus or Grafana. That is precisely the fail-green shape
/// #316 was filed to remove, relocated from "the script is never invoked" to
/// "the script is invoked with its assertions switched off", and nothing else
/// in the tree would notice.
///
/// Pinned the same way `the_chaos_runner_expands_skips_as_an_array` pins the
/// chaos runner: read the workflow text and assert the load-bearing literal.
#[test]
fn the_observability_runtime_lane_actually_enables_its_assertions() {
    let ci = read_workflow("ci.yml");
    let job = ci
        .split("observability-runtime:")
        .nth(1)
        .expect("ci.yml has no observability-runtime job");

    assert!(
        job.contains("deploy/compose/verify.sh"),
        "the lane must invoke verify.sh — it is where the runtime assertions live"
    );
    assert!(
        job.contains(r#"RIFT_OBSERVABILITY: "1""#),
        "verify.sh checks `${{RIFT_OBSERVABILITY:-0}} = 1`; without exactly that \
         value the lane runs the plain smoke and asserts nothing about \
         Prometheus or Grafana while still going green"
    );
    assert!(
        job.contains("--job observability"),
        "the lane must use its own watched set; defaulting to cluster-smoke's \
         would run it on every cluster source change"
    );
}

/// The sequencing imposter C33 drives: three responses, cycled fleet-wide.
///
/// `_rift.sequencing.mode: "owner"` is the whole opt-in (D-47). Without it the
/// default is per-process cursors (D-10), and every node would answer `A` — the
/// behaviour this scenario exists to prove is gone.
fn sequencing_imposter(port: u16) -> serde_json::Value {
    serde_json::json!({
        "port": port,
        "protocol": "http",
        "_rift": { "sequencing": { "mode": "owner" } },
        "stubs": [{
            "predicates": [{ "equals": { "path": "/cycle" } }],
            "responses": [
                { "is": { "statusCode": 200, "body": "A" } },
                { "is": { "statusCode": 200, "body": "B" } },
                { "is": { "statusCode": 200, "body": "C" } },
            ],
        }],
    })
}

/// One request to the sequencing imposter through the node at `node_idx`,
/// returning `(status, body, fleet-ordered?)`.
///
/// The third element is the `rift-cluster-sequence` header's absence: the
/// decorator sets it to `local-fallback` exactly when the decision degraded
/// (`ClusteredSequencer::route`), so no header means the ring answered.
async fn c33_cycle(node_idx: usize) -> anyhow::Result<(u16, String, bool)> {
    let (status, headers, body) = get_data_plane(SEQUENCING_HOST_PORTS[node_idx], "/cycle").await?;
    let fleet_ordered = !headers.contains_key("rift-cluster-sequence");
    Ok((status, body, fleet_ordered))
}

/// The fleet's total `rift_cluster_sequence_fallbacks_total`, over the nodes
/// named in `live`.
///
/// Summed rather than read per node because the counter lives on whichever node
/// *served* the degraded request, and the spray deliberately does not control
/// which that is.
///
/// `unwrap_or(0.0)`, like C29's partial-reads baseline, and for the same
/// reason rather than as a shrug: a `lazy_static` Prometheus counter registers
/// on first use, so on a fleet that has never fallen back the family is
/// **absent from `/metrics`** and `metric` reports "not present" — which is the
/// number 0, not a failure. It cannot mask a dead node either, because every
/// index passed here has just served a 200 in [`c33_spray`], or is about to.
async fn c33_fallbacks(live: &[usize]) -> f64 {
    let mut total = 0.0;
    for &i in live {
        total += metric(NODES[i].metrics, "rift_cluster_sequence_fallbacks_total")
            .await
            .unwrap_or(0.0);
    }
    total
}

/// Spray `rounds * 3` serial requests round-robin across `live`, returning the
/// bodies in order.
///
/// **Serial, one request at a time.** Concurrent clients cannot assert strict
/// order: the cursor advances per decision, so two in-flight requests may be
/// answered in either order and `A, C, B` would be a correct fleet-wide cycle
/// reported as a failure. A load balancer's *spraying* is what this reproduces,
/// not its concurrency — and the spray is the part the claim is about.
async fn c33_spray(live: &[usize], rounds: usize) -> anyhow::Result<Vec<String>> {
    let mut bodies = Vec::with_capacity(rounds * live.len());
    for round in 0..rounds * live.len() {
        let node_idx = live[round % live.len()];
        let (status, body, _) = c33_cycle(node_idx).await?;
        anyhow::ensure!(
            status == 200,
            "sequencing answered {status} via {} — D-47 makes every cluster \
             failure on this path a degraded 200, never an error",
            NODES[node_idx].name
        );
        bodies.push(body);
    }
    Ok(bodies)
}

/// C33 — owner-mode sequencing under an LB-shaped spray, the owner's death, and
/// its return.
///
/// The in-process gate (`crates/rift-cluster/tests/sequencer.rs`) already covers
/// fleet-wide cycling, the untouched `local` default, fallback-on-owner-loss,
/// non-replication, reset fan-out and the RPC count. What only the container
/// tier can answer is whether that survives real traffic sprayed across a real
/// three-node fleet by something that has never heard of cursor ownership.
///
/// **The fallback counter is the assertion, not the index.** By design every
/// cluster failure here degrades to a local cursor (D-10: sequencing is the one
/// stateful op where availability wins), so a broken fleet still returns
/// plausible-looking indices — three nodes each cycling their own copy answer
/// `A, A, A`, and after the kill a healthy-looking `A, B, C` proves nothing
/// about ownership. `rift_cluster_sequence_fallbacks_total` and the
/// `rift-cluster-sequence` header are what distinguish a fleet-ordered answer
/// from a locally cycled one, and they are checked at every phase.
///
/// **Finding the owner by killing.** Ownership is HRW over committed membership
/// under `KeyClass::Sequence`, keyed by a private wire key built from the
/// engine's `stub_key` — the chaos crate cannot recompute it, and adding surface
/// to expose it would be inventing an API for a test. So the owner is found the
/// way an operator would find it: SIGKILL does not change membership (a killed
/// node does not leave), so killing a *non-owner* leaves the cursor's owner
/// intact and nothing degrades. The node whose death makes the survivors'
/// fallback counter move is the owner. At most two kills are needed to identify
/// it among three, and the kills that turn out to hit a bystander are not waste:
/// they are the "killing a non-owner changes nothing" claim, asserted.
#[tokio::test]
#[ignore = "needs a container runtime"]
async fn c33_owner_mode_sequencing_cycles_fleet_wide_and_degrades_on_owner_kill() {
    let cluster = Cluster::up_with_overlays(&["sequencing.overlay.yml"])
        .await
        .expect("fleet comes up");
    let port = SEQUENCING_IMPOSTER_PORT;
    let all: Vec<usize> = (0..NODES.len()).collect();

    let (status, body) = put_imposter_config(NODES[0].admin, &sequencing_imposter(port))
        .await
        .expect("admin write");
    assert_eq!(
        status, 201,
        "the owner-mode sequencing imposter was refused: {body}"
    );
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the imposter binds on every node before any request is sprayed");

    // ---- Phase 1: a healthy fleet cycles once, however the LB spreads it. ----
    let before = c33_fallbacks(&all).await;
    let bodies = c33_spray(&all, 3).await.expect("healthy spray");
    let want: Vec<String> = "ABCABCABC".chars().map(|c| c.to_string()).collect();
    assert_eq!(
        bodies, want,
        "three nodes sharing one owned cursor must produce ONE cycle. `A, A, A` \
         means each node cycled its own copy — the pre-#466 behaviour, and what \
         a load balancer in front of an un-clustered fleet still does"
    );
    let healthy = c33_fallbacks(&all).await - before;
    assert_eq!(
        healthy, 0.0,
        "a healthy fleet must answer every decision from the ring; {healthy} \
         fallback(s) means the cycle above was assembled locally and the \
         indices happened to line up"
    );

    // ---- Phase 2: find the owner by killing, asserting the bystanders. ----
    // Kill candidates in turn. A non-owner's death must change nothing; the
    // owner's death must move the counter. The loop stops at the first mover.
    let mut owner: Option<usize> = None;
    for (candidate, node) in NODES.iter().enumerate() {
        let live: Vec<usize> = all.iter().copied().filter(|&i| i != candidate).collect();
        let before = c33_fallbacks(&live).await;
        cluster.kill(node.name).expect("SIGKILL the node");

        let sprayed = 3 * live.len();
        let bodies = c33_spray(&live, 3).await.expect("spray past the dead node");
        for (i, body) in bodies.iter().enumerate() {
            assert!(
                matches!(body.as_str(), "A" | "B" | "C"),
                "request {i} answered {body:?} with {} dead — D-47 keeps this \
                 path answering, degraded if it must, never with an error body",
                node.name
            );
        }
        let moved = c33_fallbacks(&live).await - before;

        // The owner is dead ⇒ **every** decision degrades, because SIGKILL left
        // membership (and so the ring) untouched and the key is still homed on a
        // node that cannot answer. Classifying on `moved == sprayed` rather than
        // `moved > 0` is what keeps this robust: a single stray fallback from
        // some unrelated blip would otherwise be read as "found the owner" and
        // send the rest of the scenario after the wrong node.
        #[allow(clippy::cast_precision_loss)]
        let all_degraded = moved == sprayed as f64;
        if all_degraded {
            // The owner. Every response must still be a 2xx (asserted in the
            // spray) and the degradation must be *stated*, not merely counted:
            // the header is what a client sees.
            let (status, _, fleet_ordered) = c33_cycle(live[0]).await.expect("probe after kill");
            assert_eq!(
                status, 200,
                "an unreachable cursor owner must degrade, never fail the request"
            );
            assert!(
                !fleet_ordered,
                "with the owner dead the response must carry \
                 `rift-cluster-sequence: local-fallback`. A degraded answer that \
                 does not say so is the failure mode D-10's flagged-degradation \
                 rule exists to prevent — the counter moved, so the client was \
                 served a local cursor and was not told"
            );
            owner = Some(candidate);
            break;
        }

        // A bystander: nothing degraded, so the cursor's owner is still alive
        // and still answering. Restore the fleet and try the next candidate.
        assert_eq!(
            moved, 0.0,
            "killing {} — which is not the cursor's owner — moved the fallback \
             counter by {moved} over {sprayed} decisions. A non-owner's death \
             must be invisible to sequencing: SIGKILL is not a leave, so \
             committed membership and the ring are unchanged and the owner is \
             still answering. A small non-zero here would mean a decision \
             degraded for some reason other than the owner being gone — worth \
             an issue, not a wider tolerance",
            node.name
        );
        cluster.start(node.name).expect("restart the bystander");
        cluster
            .wait_all_ready(Duration::from_secs(120))
            .await
            .expect("the bystander comes back");
        cluster
            .wait_cluster_formed(Duration::from_secs(120))
            .await
            .expect("the fleet re-forms after the bystander returns");
        wait_converged(u64::from(port), CONVERGE_TIMEOUT)
            .await
            .expect("the imposter is rebound on the returned bystander");
    }
    let owner = owner.expect(
        "one of the three nodes owns the cursor, so one of the kills above must \
         have moved the fallback counter",
    );

    // ---- Phase 3: the owner returns and the fleet re-orders. ----
    cluster.start(NODES[owner].name).expect("restart the owner");
    cluster
        .wait_all_ready(Duration::from_secs(120))
        .await
        .expect("the owner comes back");
    cluster
        .wait_cluster_formed(Duration::from_secs(120))
        .await
        .expect("the fleet re-forms with the owner back");
    wait_converged(u64::from(port), CONVERGE_TIMEOUT)
        .await
        .expect("the imposter is rebound on the returned owner");

    let before = c33_fallbacks(&all).await;
    let bodies = c33_spray(&all, 3).await.expect("spray after recovery");
    let after = c33_fallbacks(&all).await - before;
    assert_eq!(
        after, 0.0,
        "with every node back the ring must answer every decision again; \
         {after} fallback(s) means the returned owner never re-took the key"
    );

    // Cycling, not continuity. D-8 makes a cursor reset the documented price of
    // not replicating every advance, so the sequence after the owner's return
    // may start anywhere in `A, B, C` — asserting it resumed where it left off
    // would be asserting the opposite of the design.
    let start = "ABC"
        .find(bodies[0].as_str())
        .unwrap_or_else(|| panic!("first response after recovery was {:?}", bodies[0]));
    let want: Vec<String> = (0..bodies.len())
        .map(|i| "ABC".as_bytes()[(start + i) % 3] as char)
        .map(|c| c.to_string())
        .collect();
    assert_eq!(
        bodies, want,
        "after the owner's return the fleet must cycle strictly again from \
         wherever the new cursor started (D-8 permits the reset, not a stall or \
         a repeat)"
    );
}
