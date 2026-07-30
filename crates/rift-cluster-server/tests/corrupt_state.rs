//! On-disk corruption of an authorization record must **fail closed** at the HTTP boundary.
//!
//! `crates/rift-cluster/src/raft/store.rs` pins the storage half — a corrupt row reads back as an
//! `Err`, never as `None`. This pins the half that actually matters to a caller: that the error
//! reaches the wire as a `500` and is never mistaken for "this credential resolves to nobody".
//!
//! The distinction is the whole point. `None` is an ordinary, expected answer on the authentication
//! path — it is what an unknown key produces — and `principal::should_bypass` turns it into an
//! *open admin plane* on a fleet that has no `--api-key` and no principals. So a corrupt row that
//! read back as `None` would quietly convert disk corruption into an authorization decision. A
//! `500` is the honest answer: the node cannot tell who this is, so it refuses to guess.

use std::time::Duration;

use clap::Parser;
use redb::{Database, TableDefinition};
use rift_cluster::control::{FLEET_SCOPE, Role};
use rift_cluster::{ControlOp, ControlRequest, RaftNode, TenantId};
use rift_cluster_server::cli::EeCli;
use rift_cluster_server::compose::{self, ComposedServer};
use tempfile::TempDir;

mod common;

use common::seen::Seen;

const SECRET: &str = "corrupt-state-secret";

/// The state machine's principal table, named exactly as `store.rs` declares it. Duplicated here
/// rather than exported: a test reaching into another crate's on-disk schema should have to say so
/// out loud, and widening the real constant's visibility purely for a test would invite production
/// code to depend on it.
const SM_PRINCIPALS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("sm_principals");

fn cluster_cli(state: &TempDir, extra: &[&str]) -> EeCli {
    let mut args = vec![
        "rift-cluster-server".to_owned(),
        "--port".to_owned(),
        "0".to_owned(),
        "--metrics-port".to_owned(),
        "0".to_owned(),
        "--cluster".to_owned(),
        "--cluster-allow-solo".to_owned(),
        "--cluster-bind".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--cluster-probe-bind".to_owned(),
        "127.0.0.1:0".to_owned(),
        "--cluster-secret".to_owned(),
        SECRET.to_owned(),
        "--cluster-state-dir".to_owned(),
        state.path().to_string_lossy().into_owned(),
    ];
    args.extend(extra.iter().map(|s| (*s).to_owned()));
    EeCli::try_parse_from(args).expect("parses")
}

async fn wait_ready(server: &ComposedServer) {
    let probes = server.probe_addr().expect("probes bound under --cluster");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) = reqwest::get(format!("http://{probes}/readyz")).await
            && response.status().as_u16() == 200
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "node never became ready"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn seed(node: &RaftNode, op_id: u128, op: ControlOp) {
    let response = node
        .write(ControlRequest {
            op_id: uuid::Uuid::from_u128(op_id),
            principal: None,
            issued_at_secs: 0,
            expected_revision: None,
            op,
        })
        .await
        .expect("seed op commits");
    assert_eq!(response.outcome, rift_cluster::ControlOutcome::Applied);
}

/// Find the node's redb file under its state dir. The layout is the node's own business, so it is
/// discovered rather than assumed — a rename should fail this test loudly, not silently stop
/// corrupting anything and leave it passing for the wrong reason.
fn state_machine_db(state: &TempDir) -> std::path::PathBuf {
    fn find(dir: &std::path::Path) -> Option<std::path::PathBuf> {
        for entry in std::fs::read_dir(dir).ok()? {
            let path = entry.ok()?.path();
            if path.is_dir() {
                if let Some(found) = find(&path) {
                    return Some(found);
                }
            } else if path.extension().is_some_and(|e| e == "redb") {
                return Some(path);
            }
        }
        None
    }
    find(state.path()).expect("the node wrote a .redb file under its state dir")
}

/// A committed principal row corrupted on disk must answer `500`, never `401` and never a bypass.
///
/// The node is stopped before the row is rewritten and restarted afterwards, so this exercises the
/// same path a real operator would hit: corruption that was already on disk when the process came
/// up, discovered on the first request that needs the record.
#[tokio::test]
async fn a_corrupt_principal_record_fails_closed_at_the_http_boundary() {
    let state = TempDir::new().expect("tempdir");
    let key = "corrupt-principal-key";

    // 1. A fleet with exactly one real principal.
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("solo cluster starts");
    wait_ready(&server).await;
    let node = server.node().expect("clustered");
    let principal = rift_cluster::control::Principal {
        id: rift_cluster::control::api_key_principal_id(key),
        display_name: "victim".to_owned(),
        auth: rift_cluster::control::AuthSource::ApiKey {
            hash: rift_cluster::control::hash_api_key(key),
        },
        disabled: false,
    };
    let principal_id = principal.id.clone();
    seed(
        node,
        1,
        ControlOp::PrincipalPut {
            tenant: TenantId::default(),
            principal,
        },
    )
    .await;
    seed(
        node,
        2,
        ControlOp::BindingPut {
            tenant: TenantId::new(FLEET_SCOPE),
            principal_id: principal_id.clone(),
            role: Role::FleetAdmin,
        },
    )
    .await;

    // It authenticates before the corruption — otherwise a later 500 could just mean "bad key".
    let admin = server.admin_addr().to_string();
    let seen = Seen::of(
        reqwest::Client::new()
            .get(format!("http://{admin}/admin/whoami"))
            .header("authorization", key)
            .send()
            .await
            .expect("whoami"),
    )
    .await;
    assert_eq!(
        seen.status, 200,
        "the key must work before corruption: {seen}"
    );

    server.shutdown().await;

    // 2. Corrupt the committed row in place.
    let db_path = state_machine_db(&state);
    {
        let db = Database::open(&db_path).expect("open state machine db");
        let write = db.begin_write().expect("write txn");
        {
            let mut table = write
                .open_table(SM_PRINCIPALS_TABLE)
                .expect("sm_principals exists — if this fails the schema was renamed");
            table
                .insert(principal_id.as_str(), "{ not a principal record }")
                .expect("overwrite the row");
        }
        write.commit().expect("commit");
    }

    // 3. Restart on the same state dir and present the same key.
    let server = compose::start(cluster_cli(&state, &[]))
        .await
        .expect("cluster restarts over the corrupt row");
    wait_ready(&server).await;
    let admin = server.admin_addr().to_string();

    let seen = Seen::of(
        reqwest::Client::new()
            .get(format!("http://{admin}/admin/whoami"))
            .header("authorization", key)
            .send()
            .await
            .expect("whoami after corruption"),
    )
    .await;
    assert_eq!(
        seen.status, 500,
        "a corrupt principal record must fail closed as a 500. A 401 would mean the read error \
         was flattened into \"no such principal\", which is the same answer an unknown key gets — \
         and on a fleet with no --api-key that path reaches should_bypass and opens the admin \
         plane: {seen}"
    );

    // And an anonymous request must not be let in either — the corruption must not be readable as
    // "this fleet has no principals".
    let seen = Seen::of(
        reqwest::get(format!("http://{admin}/admin/whoami"))
            .await
            .expect("anonymous whoami"),
    )
    .await;
    assert_ne!(
        seen.status, 200,
        "an anonymous request was served while a principal record was corrupt — disk corruption \
         became an authorization bypass: {seen}"
    );

    server.shutdown().await;
}
