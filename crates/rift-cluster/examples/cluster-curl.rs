//! `curl` for the cluster port: a signed request, printed.
//!
//! The cluster port carries the operator surface (`/_cluster/*`) and the
//! imposter-source surface (`/admin/sources*`) — see `docs/rift-ee-server.md`.
//! Both are authenticated with the **cluster credential**, not the admin API
//! key: every request has to carry an HMAC over a canonical encoding of its
//! timestamp, nonce, method, path and body (RFC-001 §11.2). Plain `curl` cannot
//! produce that, which leaves the endpoints documented and unreachable for
//! anyone without a client.
//!
//! This is that client, in the crate that defines the format, so it cannot
//! drift from the server it talks to. It is deliberately the smallest thing
//! that works — one request, no subcommands, no config file — because it exists
//! to make the source demo (`deploy/compose/sources-demo.yml`) runnable as
//! documented, not to become a CLI.
//!
//! ```sh
//! cargo run -q -p rift-cluster --example cluster-curl -- \
//!     --secret local-development-cluster-secret \
//!     GET http://127.0.0.1:14790/admin/sources
//!
//! cargo run -q -p rift-cluster --example cluster-curl -- \
//!     --secret local-development-cluster-secret \
//!     POST http://127.0.0.1:14790/admin/sources/my-mocks/pull
//! ```
//!
//! `--secret` may be omitted when `RIFT_CLUSTER_SECRET` is set, which is how a
//! real deployment already injects it.
//!
//! Exit status is the point as much as the output: a refusal exits non-zero and
//! prints the status the node answered with, so this composes into a script.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use rift_cluster::rpc::{AlwaysHealthy, RpcClient, RpcClientConfig, Signer};

fn main() -> anyhow::Result<()> {
    let args = Args::parse(std::env::args().skip(1))?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(args))
}

async fn run(args: Args) -> anyhow::Result<()> {
    let client = RpcClient::new(
        Some(Signer::new(&args.secret)),
        Arc::new(AlwaysHealthy),
        RpcClientConfig {
            connect_timeout: Duration::from_secs(5),
            // A source pull does a real fetch against a foreign host and then a
            // Raft round trip, so the 2s peer-RPC default is far too short for
            // an operator command.
            request_timeout: Duration::from_secs(60),
            // Never retried. A retried `POST .../pull` fetches the source a
            // second time, and this tool must do exactly what it was asked to
            // do once — a failure is the operator's to see and repeat.
            max_retries: 0,
        },
    );

    match client
        .call(args.peer, &args.method, &args.path, args.body)
        .await
    {
        Ok(body) => {
            println!("{}", String::from_utf8_lossy(&body));
            Ok(())
        }
        Err(e) => anyhow::bail!("{} {} -> {} ({e})", args.method, args.path, e.status()),
    }
}

struct Args {
    peer: SocketAddr,
    method: String,
    path: String,
    body: Vec<u8>,
    secret: String,
}

impl Args {
    fn parse(argv: impl Iterator<Item = String>) -> anyhow::Result<Self> {
        let mut secret = std::env::var("RIFT_CLUSTER_SECRET").ok();
        let mut body: Option<String> = None;
        let mut positional: Vec<String> = Vec::new();

        let mut argv = argv.peekable();
        while let Some(arg) = argv.next() {
            match arg.as_str() {
                "--secret" => secret = argv.next(),
                "--data" | "-d" => body = argv.next(),
                "-h" | "--help" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                _ => positional.push(arg),
            }
        }

        let [method, url] = positional.as_slice() else {
            anyhow::bail!("expected METHOD and URL\n\n{USAGE}");
        };
        let secret = secret.ok_or_else(|| {
            anyhow::anyhow!("no cluster secret: pass --secret or set RIFT_CLUSTER_SECRET")
        })?;

        // Split the URL by hand rather than pulling in a URL crate for it: the
        // signed string is the path *and query* exactly as it goes on the wire,
        // so anything that might normalise it is a liability here.
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| anyhow::anyhow!("the cluster port speaks plain http; got {url:?}"))?;
        let (authority, path) = match rest.find('/') {
            Some(at) => (&rest[..at], &rest[at..]),
            None => (rest, "/"),
        };
        let peer = authority
            .to_socket_addrs()
            .map_err(|e| anyhow::anyhow!("resolving {authority:?}: {e}"))?
            .next()
            .ok_or_else(|| anyhow::anyhow!("{authority:?} resolved to no address"))?;

        Ok(Self {
            peer,
            method: method.to_uppercase(),
            path: path.to_owned(),
            body: body.unwrap_or_default().into_bytes(),
            secret,
        })
    }
}

const USAGE: &str = "\
usage: cluster-curl [--secret SECRET] [--data BODY] METHOD URL

  --secret  the cluster credential (default: $RIFT_CLUSTER_SECRET)
  --data    request body, sent as application/json

  cluster-curl GET  http://127.0.0.1:14790/_cluster/members
  cluster-curl POST http://127.0.0.1:14790/admin/sources/my-mocks/pull";
