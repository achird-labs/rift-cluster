//! The clustered admin front (issue #9, Ch. 4): the thin listener that owns the
//! public admin address when `--cluster` is on.
//!
//! Upstream's `AdminApiServer` builds its router privately — there is no public
//! router or middleware seam (verified at v0.15.0) — so interception happens a
//! listener earlier instead: the OSS admin binds loopback, this front binds the
//! public address, **terminates** the config-mutating routes into
//! [`ControlOp`]s on the Raft log, and reverse-proxies everything else to the
//! OSS admin byte-for-byte. With `--cluster` off this module is never
//! constructed and the OSS admin binds the public address itself — the parity
//! bar (#37) rides on that path having zero new code.
//!
//! What terminates here is exactly the *replicated-config* surface: imposter
//! create/replace/delete, stub CRUD, and enable/disable (config since
//! upstream #817 — a pause must survive restarts and converge fleet-wide,
//! #15). Runtime-state mutations (scenarios, spaces, recorded-request
//! deletes) stay proxied to the local engine — node-local today by design,
//! tracked by #16.
//!
//! Mutation responses are rendered by re-reading the just-applied state through
//! the loopback admin (`GET /imposters/:port` after the barrier), so the body
//! shape is upstream's own — no parallel projection code to drift.
//!
//! Concurrency: index-addressed and list-replace stub edits are a
//! read-modify-write of this node's applied state committed as a full
//! `PutImposter` — two concurrent writers to the same imposter are
//! last-writer-wins, and a lagging follower can base its write on stale state.
//! An expected-revision precondition (`If-Match` on `Rift-Cluster-Revision`)
//! is the planned fix and is tracked as follow-up work; until then, serialize
//! writers per imposter.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

use http_body_util::{BodyExt, Full, Limited, combinators::BoxBody};
use hyper::body::{Bytes, Incoming};
use hyper::header::{HeaderName, HeaderValue};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rift_cluster::control::{self, ControlOp, ControlRequest, StubEdit, StubEditScript};
use rift_cluster::decorate::{HEADER_OP_ID, HEADER_REVISION, HEADER_WARNINGS};
use rift_cluster::{ControlOutcome, ControlResponse, NodeError, RaftNode, TenantId};
use rift_ee::seams::{
    ErrorKind, ImposterConfig, RiftScriptConfig, ScriptBaseDir, Stub, config_uses_script_surface,
    error_response_typed, resolve_scripts, resolve_stub_scripts,
};
use serde::Deserialize;
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::cli::WriteBarrier;

/// Largest admin request body the front accepts on a terminated route. The
/// proxied path streams and is not subject to this.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// How long a terminated write may take to commit (forwarding included) before
/// the client gets the `timeout` error shape. Distinct from the barrier
/// timeout, which begins after the commit and degrades to a warning.
const WRITE_DEADLINE: Duration = Duration::from_secs(10);

type FrontBody = BoxBody<Bytes, hyper::Error>;

/// Everything the front needs besides the node itself.
pub struct FrontConfig {
    /// The public admin address to bind (what the operator pointed clients at);
    /// a `host:port` string because the OSS CLI accepts hostnames.
    pub public_addr: String,
    /// The loopback address the OSS admin actually bound.
    pub upstream_admin: SocketAddr,
    /// The admin API key, when one is configured. Terminated routes enforce it
    /// here (the same constant-time whole-header comparison upstream uses);
    /// proxied routes carry the header through and upstream enforces it.
    pub api_key: Option<String>,
    /// Whether `--allowInjection` is on. Terminated writes are gated on the
    /// same classifier the OSS admin applies before storing.
    pub allow_injection: bool,
    /// Resolution base for `_rift.script` `file:` refs on terminated writes
    /// (upstream #356); absent ⇒ any `file:` ref is refused.
    pub scripts_dir: Option<PathBuf>,
    pub barrier: WriteBarrier,
    pub barrier_timeout: Duration,
    /// `--cluster-admin-async`: answer 202 + op id right after parking, and
    /// let the submit run in the background.
    pub admin_async: bool,
}

/// A bound, serving admin front.
pub struct AdminFront {
    local_addr: SocketAddr,
    task: JoinHandle<()>,
}

impl AdminFront {
    /// The address actually bound (resolves an ephemeral `:0` request).
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stop serving and release the port.
    pub async fn shutdown(self) {
        self.task.abort();
        if let Err(e) = self.task.await
            && !e.is_cancelled()
        {
            tracing::error!(error = %e, "admin front accept task ended abnormally");
        }
    }
}

/// Per-request context, shared by clone into each connection task.
struct FrontState {
    /// `Weak` for the same reason as [`crate::cluster_api::NodeSlot`]: the node
    /// must never be kept alive by the surfaces that serve it.
    node: Weak<RaftNode>,
    upstream_admin: SocketAddr,
    api_key: Option<String>,
    allow_injection: bool,
    scripts_dir: Option<PathBuf>,
    barrier: WriteBarrier,
    barrier_timeout: Duration,
    admin_async: bool,
    /// Streams proxied requests through unchanged (SSE included).
    proxy: Client<hyper_util::client::legacy::connect::HttpConnector, Incoming>,
    /// Issues the internal re-reads mutation responses are rendered from.
    fetch: Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>,
}

/// Bind the public admin address and start serving.
pub async fn bind(config: FrontConfig, node: &Arc<RaftNode>) -> std::io::Result<AdminFront> {
    let listener = TcpListener::bind(config.public_addr.as_str()).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!(
        %local_addr,
        upstream = %config.upstream_admin,
        "clustered admin front listening (config mutations replicate, the rest proxies)"
    );

    let state = Arc::new(FrontState {
        node: Arc::downgrade(node),
        upstream_admin: config.upstream_admin,
        api_key: config.api_key,
        allow_injection: config.allow_injection,
        scripts_dir: config.scripts_dir,
        barrier: config.barrier,
        barrier_timeout: config.barrier_timeout,
        admin_async: config.admin_async,
        proxy: Client::builder(TokioExecutor::new()).build_http(),
        fetch: Client::builder(TokioExecutor::new()).build_http(),
    });

    let task = tokio::spawn(async move {
        // Same accept-loop shape as the probe listener: back off on systemic
        // accept failure, reap finished connections, never orphan a task.
        let mut backoff = Duration::from_millis(1);
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let accepted = tokio::select! {
                Some(joined) = connections.join_next(), if !connections.is_empty() => {
                    if let Err(e) = joined
                        && e.is_panic()
                    {
                        tracing::error!(error = %e, "admin front connection task panicked");
                    }
                    continue;
                }
                accepted = listener.accept() => accepted,
            };
            let stream = match accepted {
                Ok((stream, _)) => {
                    backoff = Duration::from_millis(1);
                    stream
                }
                Err(e) => {
                    tracing::warn!(error = %e, ?backoff, "admin front accept failed; backing off");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                    continue;
                }
            };
            let state = Arc::clone(&state);
            connections.spawn(async move {
                let service = service_fn(move |req| {
                    let state = Arc::clone(&state);
                    async move { Ok::<_, std::convert::Infallible>(handle(state, req).await) }
                });
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    // Routine for dropped keep-alives; the client saw its
                    // responses or its own error either way.
                    tracing::debug!(error = %e, "admin front connection ended with an error");
                }
            });
        }
    });

    Ok(AdminFront { local_addr, task })
}

/// The config-mutating routes the front terminates. Everything else proxies.
#[derive(Debug)]
enum Terminated {
    Create,
    ReplaceAllImposters,
    DeleteAllImposters,
    DeleteImposter(u16),
    AddStub(u16),
    ReplaceStubs(u16),
    ReplaceStubAt(u16, usize),
    DeleteStubAt(u16, usize),
    ReplaceStubById(u16, String),
    DeleteStubById(u16, String),
    SetEnabled(u16, bool),
}

fn classify(method: &Method, path: &str) -> Option<Terminated> {
    if path == "/imposters" {
        return match *method {
            Method::POST => Some(Terminated::Create),
            Method::PUT => Some(Terminated::ReplaceAllImposters),
            Method::DELETE => Some(Terminated::DeleteAllImposters),
            _ => None,
        };
    }
    let rest = path.strip_prefix("/imposters/")?;
    let segments: Vec<&str> = rest.split('/').collect();
    let port: u16 = segments.first()?.parse().ok()?;
    match segments.as_slice() {
        [_] if *method == Method::DELETE => Some(Terminated::DeleteImposter(port)),
        [_, "enable"] if *method == Method::POST => Some(Terminated::SetEnabled(port, true)),
        [_, "disable"] if *method == Method::POST => Some(Terminated::SetEnabled(port, false)),
        [_, "stubs"] => match *method {
            Method::POST => Some(Terminated::AddStub(port)),
            Method::PUT => Some(Terminated::ReplaceStubs(port)),
            _ => None,
        },
        [_, "stubs", "by-id", id] if !id.is_empty() => match *method {
            Method::PUT => Some(Terminated::ReplaceStubById(port, (*id).to_owned())),
            Method::DELETE => Some(Terminated::DeleteStubById(port, (*id).to_owned())),
            _ => None,
        },
        [_, "stubs", index] => {
            let index: usize = index.parse().ok()?;
            match *method {
                Method::PUT => Some(Terminated::ReplaceStubAt(port, index)),
                Method::DELETE => Some(Terminated::DeleteStubAt(port, index)),
                _ => None,
            }
        }
        _ => None,
    }
}

async fn handle(state: Arc<FrontState>, req: Request<Incoming>) -> Response<FrontBody> {
    let path = req.uri().path().to_owned();
    match classify(req.method(), &path) {
        Some(kind) => terminate(state, req, kind).await,
        None => proxy(state, req).await,
    }
}

// ---------------------------------------------------------------------------
// Proxy path
// ---------------------------------------------------------------------------

/// Forward `req` to the loopback admin unchanged and stream the response back.
async fn proxy(state: Arc<FrontState>, req: Request<Incoming>) -> Response<FrontBody> {
    let (mut parts, body) = req.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or("/", |paq| paq.as_str())
        .to_owned();
    let uri: Uri = match format!("http://{}{}", state.upstream_admin, path_and_query).parse() {
        Ok(uri) => uri,
        Err(e) => {
            return typed_error(
                StatusCode::BAD_REQUEST,
                ErrorKind::BadData,
                &format!("request target does not re-target: {e}"),
            );
        }
    };
    parts.uri = uri;
    match state.proxy.request(Request::from_parts(parts, body)).await {
        // The response body streams through as-is — buffering here would break
        // the admin SSE streams.
        Ok(response) => response.map(BodyExt::boxed),
        Err(e) => typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            &format!("local admin backend unreachable: {e}"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Terminated path
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct AddStubBody {
    stub: Stub,
    #[serde(default)]
    index: Option<usize>,
}

#[derive(Deserialize)]
struct ReplaceStubsBody {
    stubs: Vec<Stub>,
}

#[derive(Deserialize)]
struct ReplaceAllBody {
    #[serde(default)]
    imposters: Vec<ImposterConfig>,
}

async fn terminate(
    state: Arc<FrontState>,
    req: Request<Incoming>,
    kind: Terminated,
) -> Response<FrontBody> {
    let Some(node) = state.node.upgrade() else {
        return typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            "cluster node is shutting down",
        );
    };

    // The same whole-header constant-time comparison the OSS admin performs;
    // terminated requests never reach it, so its gate is enforced here.
    let auth = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    if let Some(expected) = &state.api_key {
        let presented = auth.as_deref().unwrap_or("");
        if !bool::from(presented.as_bytes().ct_eq(expected.as_bytes())) {
            return unauthorized();
        }
    }

    let host = req.headers().get("host").cloned();
    let idempotency = req
        .headers()
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let body = match Limited::new(req.into_body(), MAX_BODY_BYTES)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return typed_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorKind::RequestTooLarge,
                &format!("admin request body refused: {e}"),
            );
        }
    };

    match build_and_run(
        &state,
        &node,
        kind,
        &body,
        auth.as_deref(),
        host.as_ref(),
        idempotency.as_deref(),
    )
    .await
    {
        Ok(response) => response,
        Err(response) => response,
    }
}

/// One terminated mutation: what to commit, and how to answer afterwards.
struct Mutation {
    ops: Vec<ControlOp>,
    /// The port label for the revision header; `None` for collection-wide ops.
    port: Option<u16>,
    /// How the success response is rendered.
    render: Render,
}

enum Render {
    /// `GET` this loopback path after the barrier and answer with its body.
    FetchAfter { path: String, status: StatusCode },
    /// Answer with a body captured *before* the ops committed (deletes answer
    /// with what was removed).
    Captured {
        body: Bytes,
        content_type: Option<HeaderValue>,
        status: StatusCode,
    },
}

/// Build the mutation for `kind`, pre-validate it, commit it op by op, run the
/// barrier, and render the response. Errors are already client-shaped.
async fn build_and_run(
    state: &Arc<FrontState>,
    node: &Arc<RaftNode>,
    kind: Terminated,
    body: &[u8],
    auth: Option<&str>,
    host: Option<&HeaderValue>,
    idempotency: Option<&str>,
) -> Result<Response<FrontBody>, Response<FrontBody>> {
    let is_batch = matches!(kind, Terminated::ReplaceAllImposters);
    let mut mutation = build_mutation(state, node, kind, body, auth, host).await?;

    // Pre-validate every op before committing any: a multi-op mutation (PUT
    // /imposters) must not tear half the fleet's config down and then refuse
    // the other half. The state machine re-runs the same checks on apply.
    for op in &mutation.ops {
        if let Err(reason) = control::validate(op) {
            return Err(refusal_response(&reason));
        }
    }

    if !state.allow_injection {
        for op in &mutation.ops {
            if op_uses_script_surface(op) {
                return Err(injection_disallowed());
            }
        }
    }

    // Resolve `_rift.script` file:/ref: sources (upstream #356) after the
    // gate and before parking: nothing unresolved is ever parked, replayed,
    // or replicated, and a gated request never touches the filesystem.
    let script_base = front_script_base(state.scripts_dir.as_deref());
    let mut put_index = 0usize;
    for op in &mut mutation.ops {
        let batch_index = if is_batch && matches!(op, ControlOp::PutImposter { .. }) {
            let index = put_index;
            put_index += 1;
            Some(index)
        } else {
            None
        };
        resolve_op_scripts(op, node, &script_base, batch_index)?;
    }

    // Mint deterministically from the client's Idempotency-Key (when given),
    // then park every op durably BEFORE submitting any (R4): once parked, the
    // op survives a crash and the replay loop finishes what this request
    // cannot — including the tail of a multi-op sequence.
    let base = base_op_id(idempotency);
    let total = mutation.ops.len();
    let requests: Vec<ControlRequest> = mutation
        .ops
        .into_iter()
        .enumerate()
        .map(|(index, op)| mint(op_id_for(base, index, total), op))
        .collect();
    for request in &requests {
        if let Err(e) = node.park_intent(request) {
            // Refusing is the only honest answer: R4's promise is exactly that
            // an accepted op is durable, and this one could not be made so.
            return Err(typed_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorKind::InternalError,
                &format!("cannot durably accept the write: {e}"),
            ));
        }
    }

    if state.admin_async {
        let node = Arc::clone(node);
        let op_ids: Vec<Uuid> = requests.iter().map(|request| request.op_id).collect();
        let background = requests;
        tokio::spawn(async move {
            for request in background {
                let op_id = request.op_id;
                match node.submit(request).await {
                    Ok(_) => {
                        if let Err(e) = node.unpark_intent(&op_id) {
                            tracing::error!(%op_id, error = %e, "applied but could not unpark");
                        }
                    }
                    Err(e) => {
                        // The replay loop owns it from here.
                        tracing::warn!(%op_id, error = %e, "async submit failed; intent stays parked");
                        return;
                    }
                }
            }
        });
        // `opIds` is what `GET /_cluster/ops/:id` can actually answer for: a
        // multi-op mutation parks only the derived ids, never the base — a
        // client polling the bare base of a PUT /imposters would 404 forever.
        let body = serde_json::json!({
            "opId": base.to_string(),
            "opIds": op_ids.iter().map(Uuid::to_string).collect::<Vec<_>>(),
        })
        .to_string();
        let mut response =
            buffered_response(StatusCode::ACCEPTED, Bytes::from(body), json_content_type())?;
        set_header(&mut response, HEADER_OP_ID, &base.to_string());
        return Ok(response);
    }

    let mut last: Option<(Uuid, ControlResponse)> = None;
    for request in requests {
        let op_id = request.op_id;
        let submitted = tokio::time::timeout(WRITE_DEADLINE, node.submit(request)).await;
        let response = match submitted {
            Err(_) => {
                // Parked, so not lost: the replay loop retries it. Tell the
                // client which op to poll.
                let mut response = typed_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    ErrorKind::Timeout,
                    "write did not commit within the deadline; parked for replay",
                );
                set_header(&mut response, HEADER_OP_ID, &base.to_string());
                return Err(response);
            }
            Ok(Err(NodeError::Unavailable(detail))) => {
                // R4: refused only AFTER parking — the op is durable here and
                // the replay loop applies it once a quorum returns.
                let mut response = typed_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    ErrorKind::Unavailable,
                    &format!("no quorum / leader unreachable (parked for replay): {detail}"),
                );
                response
                    .headers_mut()
                    .insert("retry-after", HeaderValue::from_static("1"));
                set_header(&mut response, HEADER_OP_ID, &base.to_string());
                return Err(response);
            }
            Ok(Err(e)) => {
                let mut response = typed_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorKind::InternalError,
                    &e.to_string(),
                );
                set_header(&mut response, HEADER_OP_ID, &base.to_string());
                return Err(response);
            }
            Ok(Ok(response)) => response,
        };
        // Terminal either way (a Failed outcome replays to the identical
        // refusal), so the intent retires now.
        if let Err(e) = node.unpark_intent(&op_id) {
            tracing::error!(%op_id, error = %e, "op terminal but could not unpark");
        }
        if let ControlOutcome::Failed { reason } = &response.outcome {
            return Err(refusal_response(reason));
        }
        last = Some((op_id, response));
    }
    let Some((op_id, committed)) = last else {
        return Err(typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            "nothing to apply",
        ));
    };

    let unapplied = match state.barrier {
        WriteBarrier::None => Vec::new(),
        WriteBarrier::ReadyNodes => {
            node.await_applied(committed.revision, state.barrier_timeout)
                .await
        }
    };

    let mut response = match mutation.render {
        Render::Captured {
            body,
            content_type,
            status,
        } => buffered_response(status, body, content_type)?,
        Render::FetchAfter { path, status } => {
            let (fetched, content_type, body) = fetch(state, &path, auth, host).await?;
            // The commit is real either way, but the render must not dress a
            // non-2xx re-read (barrier timed out, or `--cluster-write-barrier
            // none` outrunning the local apply) in the success code — a 201
            // wrapping a 404 body would claim a state this node cannot show.
            // The cluster headers below still carry the committed revision.
            let status = if fetched.is_success() {
                status
            } else {
                tracing::warn!(%path, status = %fetched, "post-commit render read did not confirm the write");
                fetched
            };
            buffered_response(status, body, content_type)?
        }
    };

    let revision = match mutation.port {
        Some(port) => format!("{}:{port}@{}", TenantId::default(), committed.revision),
        None => format!("{}@{}", TenantId::default(), committed.revision),
    };
    set_header(&mut response, HEADER_REVISION, &revision);
    set_header(&mut response, HEADER_OP_ID, &op_id.to_string());
    let mut warnings = Vec::new();
    if !unapplied.is_empty() {
        let nodes = unapplied
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        warnings.push(format!("unapplied={nodes}"));
    }
    // The commit is fleet truth, but THIS node's engine may still have failed
    // to realize it (a bind, a refused toggle): §7.4.6 — success with a named
    // warning, never a silent divergence the client cannot see.
    if let Some(port) = mutation.port
        && let Some(failure) = node.apply_failures().get(&port)
    {
        warnings.push(format!("local-engine={failure}"));
    }
    if !warnings.is_empty() {
        set_header(&mut response, HEADER_WARNINGS, &warnings.join(","));
    }
    Ok(response)
}

/// Translate one terminated route into ops + a render plan. Reads that inform
/// the mutation (current stubs for index-addressed edits, capture-before-delete
/// bodies) come from the local applied state / loopback admin.
async fn build_mutation(
    state: &FrontState,
    node: &Arc<RaftNode>,
    kind: Terminated,
    body: &[u8],
    auth: Option<&str>,
    host: Option<&HeaderValue>,
) -> Result<Mutation, Response<FrontBody>> {
    match kind {
        Terminated::Create => {
            let config: ImposterConfig = parse(body)?;
            let Some(port) = config.port else {
                return Err(typed_error(
                    StatusCode::BAD_REQUEST,
                    ErrorKind::BadData,
                    "a clustered imposter needs an explicit port: auto-assigned ports \
                     cannot replicate",
                ));
            };
            Ok(Mutation {
                ops: vec![ControlOp::PutImposter {
                    tenant: TenantId::default(),
                    config: Box::new(config),
                }],
                port: Some(port),
                render: Render::FetchAfter {
                    path: format!("/imposters/{port}"),
                    status: StatusCode::CREATED,
                },
            })
        }
        Terminated::ReplaceAllImposters => {
            let replace: ReplaceAllBody = parse(body)?;
            // Upsert the new set first, then prune the leftovers — never a
            // DeleteAll up front. The ops commit as separate Raft entries, so a
            // mid-sequence loss of quorum tears the sequence; torn this way the
            // fleet keeps a superset (new configs plus stale leftovers) that a
            // retry heals, instead of an empty fleet that lost everything.
            let mut keep = std::collections::BTreeSet::new();
            let mut ops = Vec::new();
            for config in replace.imposters {
                let Some(port) = config.port else {
                    return Err(typed_error(
                        StatusCode::BAD_REQUEST,
                        ErrorKind::BadData,
                        "a clustered imposter needs an explicit port: auto-assigned ports \
                         cannot replicate",
                    ));
                };
                keep.insert(port);
                ops.push(ControlOp::PutImposter {
                    tenant: TenantId::default(),
                    config: Box::new(config),
                });
            }
            if ops.is_empty() {
                ops.push(ControlOp::DeleteAll {
                    tenant: TenantId::default(),
                });
            } else {
                let existing = node
                    .configured_ports()
                    .map_err(|e| internal(&e.to_string()))?;
                for port in existing {
                    if !keep.contains(&port) {
                        ops.push(ControlOp::DeleteImposter {
                            tenant: TenantId::default(),
                            port,
                        });
                    }
                }
            }
            Ok(Mutation {
                ops,
                port: None,
                render: Render::FetchAfter {
                    path: "/imposters".to_owned(),
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::DeleteAllImposters => {
            let (_, content_type, captured) = fetch(state, "/imposters", auth, host).await?;
            Ok(Mutation {
                ops: vec![ControlOp::DeleteAll {
                    tenant: TenantId::default(),
                }],
                port: None,
                render: Render::Captured {
                    body: captured,
                    content_type,
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::DeleteImposter(port) => {
            let (status, content_type, captured) =
                fetch(state, &format!("/imposters/{port}"), auth, host).await?;
            if status == StatusCode::NOT_FOUND {
                // Mirror upstream: deleting an absent imposter is a 404, and
                // committing nothing keeps the log free of no-ops.
                return Err(
                    match buffered_response(StatusCode::NOT_FOUND, captured, content_type) {
                        Ok(response) | Err(response) => response,
                    },
                );
            }
            Ok(Mutation {
                ops: vec![ControlOp::DeleteImposter {
                    tenant: TenantId::default(),
                    port,
                }],
                port: Some(port),
                render: Render::Captured {
                    body: captured,
                    content_type,
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::AddStub(port) => {
            let add: AddStubBody = parse(body)?;
            Ok(Mutation {
                ops: vec![ControlOp::PatchStubs {
                    tenant: TenantId::default(),
                    port,
                    edit: StubEditScript(vec![StubEdit::Add {
                        stub: add.stub,
                        index: add.index,
                    }]),
                }],
                port: Some(port),
                render: Render::FetchAfter {
                    path: format!("/imposters/{port}"),
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::ReplaceStubs(port) => {
            let replace: ReplaceStubsBody = parse(body)?;
            let mut config = stored_config(node, port)?;
            config.stubs = replace.stubs;
            Ok(put_config_mutation(port, config))
        }
        Terminated::ReplaceStubAt(port, index) => {
            let stub: Stub = parse(body)?;
            let mut config = stored_config(node, port)?;
            if index >= config.stubs.len() {
                return Err(stub_index_missing(index));
            }
            config.stubs[index] = stub;
            Ok(put_config_mutation(port, config))
        }
        Terminated::DeleteStubAt(port, index) => {
            let mut config = stored_config(node, port)?;
            if index >= config.stubs.len() {
                return Err(stub_index_missing(index));
            }
            config.stubs.remove(index);
            Ok(put_config_mutation(port, config))
        }
        Terminated::ReplaceStubById(port, id) => {
            let stub: Stub = parse(body)?;
            Ok(Mutation {
                ops: vec![ControlOp::PatchStubs {
                    tenant: TenantId::default(),
                    port,
                    edit: StubEditScript(vec![StubEdit::ReplaceById { id, stub }]),
                }],
                port: Some(port),
                render: Render::FetchAfter {
                    path: format!("/imposters/{port}"),
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::SetEnabled(port, enabled) => {
            let state = if enabled { "enabled" } else { "disabled" };
            Ok(Mutation {
                ops: vec![ControlOp::SetEnabled {
                    tenant: TenantId::default(),
                    port,
                    enabled,
                }],
                port: Some(port),
                // Upstream's own response shape, byte-identical — no re-read
                // needed for a message body.
                render: Render::Captured {
                    body: Bytes::from(
                        serde_json::json!({ "message": format!("Imposter {state}") }).to_string(),
                    ),
                    content_type: json_content_type(),
                    status: StatusCode::OK,
                },
            })
        }
        Terminated::DeleteStubById(port, id) => Ok(Mutation {
            ops: vec![ControlOp::PatchStubs {
                tenant: TenantId::default(),
                port,
                edit: StubEditScript(vec![StubEdit::DeleteById { id }]),
            }],
            port: Some(port),
            render: Render::FetchAfter {
                path: format!("/imposters/{port}"),
                status: StatusCode::OK,
            },
        }),
    }
}

/// Index-addressed stub edits and whole-list replacement have no by-id spelling
/// in the op set, so they commit as a full `PutImposter` of the stored config
/// with the stub list edited — the engine's #316 diff still patches only the
/// touched stubs in place.
fn put_config_mutation(port: u16, config: ImposterConfig) -> Mutation {
    Mutation {
        ops: vec![ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: Box::new(config),
        }],
        port: Some(port),
        render: Render::FetchAfter {
            path: format!("/imposters/{port}"),
            status: StatusCode::OK,
        },
    }
}

/// The committed config for `port` from the local applied state, parsed.
// The Err IS the client response (the early-return channel this module
// uses everywhere); boxing it would just move the bytes to every call site.
#[allow(clippy::result_large_err)]
fn stored_config(node: &Arc<RaftNode>, port: u16) -> Result<ImposterConfig, Response<FrontBody>> {
    let stored = node
        .get_imposter(port)
        .map_err(|e| internal(&e.to_string()))?;
    let Some(stored) = stored else {
        return Err(typed_error(
            StatusCode::NOT_FOUND,
            ErrorKind::NoSuchResource,
            &format!("no imposter on port {port}"),
        ));
    };
    serde_json::from_str(&stored).map_err(|e| internal(&format!("stored config for {port}: {e}")))
}

fn mint(op_id: Uuid, op: ControlOp) -> ControlRequest {
    // Pre-epoch clocks mint 0: only this op's dedup TTL weakens, never its
    // response (same reasoning as the node's own mint site).
    let issued_at_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    ControlRequest {
        op_id,
        principal: None,
        issued_at_secs,
        op,
    }
}

/// Fixed namespace for deriving op ids from `Idempotency-Key` values that are
/// not themselves UUIDs. Changing it would break every in-flight client key,
/// so: never.
const IDEMPOTENCY_NAMESPACE: Uuid = Uuid::from_u128(0x52_49_46_54_2d_45_45_2d_49_44_45_4d_50_4f_54);

/// The mutation's base op id: the client's `Idempotency-Key` verbatim when it
/// is a UUID, a v5 derivation of it otherwise, or a fresh v4 when absent.
fn base_op_id(idempotency: Option<&str>) -> Uuid {
    match idempotency.map(str::trim) {
        Some(key) if !key.is_empty() => key
            .parse()
            .unwrap_or_else(|_| Uuid::new_v5(&IDEMPOTENCY_NAMESPACE, key.as_bytes())),
        _ => Uuid::new_v4(),
    }
}

/// Per-op ids for a multi-op mutation, derived deterministically from the base
/// so a retried Idempotency-Key dedups every op in the sequence, not just the
/// first.
///
/// Stability caveat: ids shift if the same key later yields a different op
/// COUNT (a prune set that changed flips `base` ↔ `v5(base, 0)`). That cannot
/// double-apply today because every op that appears in a multi-op mutation
/// (Put/Delete/DeleteAll) is idempotent — the one non-idempotent op
/// (`PatchStubs` append) is always single-op. Keep it that way.
fn op_id_for(base: Uuid, index: usize, total: usize) -> Uuid {
    if total == 1 {
        base
    } else {
        Uuid::new_v5(&base, &index.to_be_bytes())
    }
}

fn json_content_type() -> Option<HeaderValue> {
    Some(HeaderValue::from_static("application/json"))
}

/// Whether a terminated op would introduce a scripting surface — the same
/// classifier the OSS admin gates on, applied to the incoming payload.
fn op_uses_script_surface(op: &ControlOp) -> bool {
    match op {
        ControlOp::PutImposter { config, .. } => config_uses_script_surface(config),
        ControlOp::PatchStubs { edit, .. } => {
            let stubs: Vec<Stub> = edit
                .0
                .iter()
                .filter_map(|step| match step {
                    StubEdit::Add { stub, .. } | StubEdit::ReplaceById { stub, .. } => {
                        Some(stub.clone())
                    }
                    StubEdit::DeleteById { .. } | StubEdit::Move { .. } => None,
                })
                .collect();
            if stubs.is_empty() {
                return false;
            }
            let scratch = ImposterConfig {
                stubs,
                ..ImposterConfig::default()
            };
            config_uses_script_surface(&scratch)
        }
        _ => false,
    }
}

/// Mirrors upstream's private `admin_script_base`: `--scripts-dir` when
/// configured, else every `file:` ref is refused.
fn front_script_base(scripts_dir: Option<&Path>) -> ScriptBaseDir {
    match scripts_dir {
        Some(dir) => ScriptBaseDir::ScriptsDir(dir.to_path_buf()),
        None => ScriptBaseDir::Unconfigured,
    }
}

/// The target imposter's already-resolved `_rift.scripts`, from applied
/// state; empty when the imposter is absent (resolve → then not-found
/// ordering: an unknown ref against an empty registry still fails with
/// `UnknownRef`, the same observable order upstream produces for a genuinely
/// missing imposter).
#[allow(clippy::result_large_err)]
fn stored_script_registry(
    node: &Arc<RaftNode>,
    port: u16,
) -> Result<HashMap<String, RiftScriptConfig>, Response<FrontBody>> {
    // An absent imposter is the domain-optional empty registry: an unknown ref
    // then fails as UnknownRef, upstream's resolve-then-not-found order. A
    // storage or parse failure is a real fault and must not masquerade as
    // "unknown script ref" — it propagates as 500, same as stored_config.
    let Some(stored) = node
        .get_imposter(port)
        .map_err(|e| internal(&e.to_string()))?
    else {
        return Ok(HashMap::new());
    };
    let config: ImposterConfig = serde_json::from_str(&stored)
        .map_err(|e| internal(&format!("stored config for {port}: {e}")))?;
    Ok(config.rift.map(|rift| rift.scripts).unwrap_or_default())
}

/// Resolve one terminated op in place; `Err` is the client-shaped 400 with
/// upstream's exact message. `batch_index` is `Some` for `PUT /imposters`
/// ops.
#[allow(clippy::result_large_err)]
fn resolve_op_scripts(
    op: &mut ControlOp,
    node: &Arc<RaftNode>,
    base: &ScriptBaseDir,
    batch_index: Option<usize>,
) -> Result<(), Response<FrontBody>> {
    match op {
        ControlOp::PutImposter { config, .. } => resolve_scripts(config, base).map_err(|e| {
            let message = match batch_index {
                Some(idx) => format!(
                    "Script resolution failed in imposter[{idx}] (port {:?}): {e}",
                    config.port
                ),
                None => format!("Script resolution failed: {e}"),
            };
            typed_error(StatusCode::BAD_REQUEST, ErrorKind::BadData, &message)
        }),
        ControlOp::PatchStubs { port, edit, .. } => {
            let needs_registry = edit
                .0
                .iter()
                .any(|step| matches!(step, StubEdit::Add { .. } | StubEdit::ReplaceById { .. }));
            if !needs_registry {
                return Ok(());
            }
            let registry = stored_script_registry(node, *port)?;
            for step in &mut edit.0 {
                if let StubEdit::Add { stub, .. } | StubEdit::ReplaceById { stub, .. } = step {
                    resolve_stub_scripts(std::slice::from_mut(stub), &registry, base).map_err(
                        |e| {
                            typed_error(
                                StatusCode::BAD_REQUEST,
                                ErrorKind::BadData,
                                &format!("Script resolution failed: {e}"),
                            )
                        },
                    )?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// `GET` a loopback admin path, forwarding the caller's authorization header.
/// Returns status, content type, and the collected body.
async fn fetch(
    state: &FrontState,
    path: &str,
    auth: Option<&str>,
    host: Option<&HeaderValue>,
) -> Result<(StatusCode, Option<HeaderValue>, Bytes), Response<FrontBody>> {
    let uri: Uri = format!("http://{}{path}", state.upstream_admin)
        .parse()
        .map_err(|e| internal(&format!("render path: {e}")))?;
    let mut request = Request::builder().method(Method::GET).uri(uri);
    if let Some(auth) = auth {
        request = request.header("authorization", auth);
    }
    // The client's own Host, so the HATEOAS links upstream builds from it
    // carry the public authority rather than the loopback one.
    if let Some(host) = host {
        request = request.header("host", host);
    }
    let request = request
        .body(Full::new(Bytes::new()))
        .map_err(|e| internal(&e.to_string()))?;
    let response = state.fetch.request(request).await.map_err(|e| {
        typed_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            &format!("local admin backend unreachable: {e}"),
        )
    })?;
    let status = response.status();
    let content_type = response.headers().get("content-type").cloned();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|e| internal(&format!("render read: {e}")))?
        .to_bytes();
    Ok((status, content_type, body))
}

// ---------------------------------------------------------------------------
// Response shapes
// ---------------------------------------------------------------------------

// The Err IS the client response (the early-return channel this module
// uses everywhere); boxing it would just move the bytes to every call site.
#[allow(clippy::result_large_err)]
fn parse<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, Response<FrontBody>> {
    serde_json::from_slice(body).map_err(|e| {
        typed_error(
            StatusCode::BAD_REQUEST,
            ErrorKind::BadData,
            &format!("invalid request JSON: {e}"),
        )
    })
}

/// Assemble a buffered-body response with an optional upstream content type.
#[allow(clippy::result_large_err)]
fn buffered_response(
    status: StatusCode,
    body: Bytes,
    content_type: Option<HeaderValue>,
) -> Result<Response<FrontBody>, Response<FrontBody>> {
    let mut response = Response::builder()
        .status(status)
        .body(Full::new(body).map_err(|never| match never {}).boxed())
        .map_err(|e| internal(&e.to_string()))?;
    if let Some(content_type) = content_type {
        response.headers_mut().insert("content-type", content_type);
    }
    Ok(response)
}

/// Map a committed (or pre-validated) refusal reason to the client shape:
/// absent targets are 404s, everything else is bad data.
fn refusal_response(reason: &str) -> Response<FrontBody> {
    if reason.contains("no imposter on port") || reason.contains("no stub with id") {
        typed_error(StatusCode::NOT_FOUND, ErrorKind::NoSuchResource, reason)
    } else {
        typed_error(StatusCode::BAD_REQUEST, ErrorKind::BadData, reason)
    }
}

fn stub_index_missing(index: usize) -> Response<FrontBody> {
    typed_error(
        StatusCode::NOT_FOUND,
        ErrorKind::NoSuchResource,
        &format!("no stub at index {index}"),
    )
}

fn typed_error(status: StatusCode, kind: ErrorKind, message: &str) -> Response<FrontBody> {
    error_response_typed(status, kind, message)
        .map(|body| body.map_err(|never| match never {}).boxed())
}

fn internal(message: &str) -> Response<FrontBody> {
    typed_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        ErrorKind::InternalError,
        message,
    )
}

/// The OSS admin's own 401 shape, byte-for-byte.
fn unauthorized() -> Response<FrontBody> {
    let body = r#"{"errors":[{"code":"unauthorized","type":"unauthorized","message":"Invalid authorization token"}]}"#;
    let mut response = Response::new(
        Full::new(Bytes::from_static(body.as_bytes()))
            .map_err(|never| match never {})
            .boxed(),
    );
    *response.status_mut() = StatusCode::UNAUTHORIZED;
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/json"));
    response
}

/// The injection-disallowed 400, mirroring upstream's shape so a client cannot
/// tell which listener refused it.
fn injection_disallowed() -> Response<FrontBody> {
    let body = serde_json::json!({
        "errors": [{
            "code": ErrorKind::InvalidInjection.slug(),
            "type": ErrorKind::InvalidInjection.slug(),
            "message": "inject requires --allowInjection to be set. See \
                        http://www.mbtest.org/docs/api/injection for more information.",
        }]
    });
    let mut response = Response::new(
        Full::new(Bytes::from(body.to_string()))
            .map_err(|never| match never {})
            .boxed(),
    );
    *response.status_mut() = StatusCode::BAD_REQUEST;
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/json"));
    response
}

fn set_header(response: &mut Response<FrontBody>, name: &'static str, value: &str) {
    match HeaderValue::from_str(value) {
        Ok(value) => {
            response
                .headers_mut()
                .insert(HeaderName::from_static(name), value);
        }
        Err(e) => {
            // Purely informational headers; a value that cannot be spelled must
            // not fail the write it describes.
            tracing::warn!(header = name, error = %e, "dropping unspellable cluster header");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_terminates_exactly_the_config_surface() {
        let terminated = [
            (Method::POST, "/imposters"),
            (Method::PUT, "/imposters"),
            (Method::DELETE, "/imposters"),
            (Method::DELETE, "/imposters/4545"),
            (Method::POST, "/imposters/4545/stubs"),
            (Method::PUT, "/imposters/4545/stubs"),
            (Method::PUT, "/imposters/4545/stubs/0"),
            (Method::DELETE, "/imposters/4545/stubs/2"),
            (Method::PUT, "/imposters/4545/stubs/by-id/a"),
            (Method::DELETE, "/imposters/4545/stubs/by-id/a"),
            (Method::POST, "/imposters/4545/enable"),
            (Method::POST, "/imposters/4545/disable"),
        ];
        for (method, path) in terminated {
            assert!(
                classify(&method, path).is_some(),
                "{method} {path} must terminate"
            );
        }

        // Runtime-state mutations and every read stay proxied: replicating
        // them is #15/#16 territory, and reads must hit the live engine.
        let proxied = [
            (Method::GET, "/imposters"),
            (Method::GET, "/imposters/4545"),
            (Method::DELETE, "/imposters/4545/savedRequests"),
            (Method::DELETE, "/imposters/4545/requests"),
            (Method::POST, "/imposters/4545/verify"),
            (Method::PUT, "/imposters/4545/scenarios/checkout/state"),
            (Method::POST, "/imposters/4545/scenarios/reset"),
            (Method::DELETE, "/imposters/4545/spaces/flow-1"),
            (Method::GET, "/config"),
            (Method::GET, "/metrics"),
            (Method::POST, "/_reload"),
        ];
        for (method, path) in proxied {
            assert!(
                classify(&method, path).is_none(),
                "{method} {path} must proxy"
            );
        }

        // An unparseable port is not this surface's route at all.
        assert!(classify(&Method::DELETE, "/imposters/not-a-port").is_none());
    }

    #[test]
    fn op_ids_derive_deterministically_from_the_idempotency_key() {
        // A UUID key is used verbatim; a non-UUID key derives stably; absent
        // keys mint fresh (and therefore differ).
        let uuid_key = "0189dcf0-0454-4e0b-a10c-8a8f8dccce1f";
        assert_eq!(
            base_op_id(Some(uuid_key)),
            uuid_key.parse::<Uuid>().expect("uuid"),
        );
        assert_eq!(base_op_id(Some("my-key")), base_op_id(Some("  my-key  ")));
        assert_ne!(base_op_id(Some("my-key")), base_op_id(Some("other-key")));
        assert_ne!(base_op_id(None), base_op_id(None));
        assert_ne!(
            base_op_id(Some("")),
            base_op_id(Some("")),
            "an empty key is no key"
        );

        // Single-op mutations use the base verbatim (the pollable id); multi-op
        // sequences derive per-index ids that never collide with the base.
        let base = base_op_id(Some("my-key"));
        assert_eq!(op_id_for(base, 0, 1), base);
        assert_ne!(op_id_for(base, 0, 2), base);
        assert_ne!(op_id_for(base, 0, 2), op_id_for(base, 1, 2));
        assert_eq!(op_id_for(base, 1, 3), op_id_for(base, 1, 3));
    }

    #[test]
    fn front_script_base_maps_the_flag() {
        assert!(matches!(
            front_script_base(None),
            ScriptBaseDir::Unconfigured
        ));

        let dir = PathBuf::from("/tmp/rift-test-scripts");
        match front_script_base(Some(dir.as_path())) {
            ScriptBaseDir::ScriptsDir(got) => assert_eq!(got, dir),
            other => panic!("expected ScriptsDir, got {other:?}"),
        }
    }

    /// A `RaftNode` with empty applied state — real enough for `get_imposter`
    /// (which "does not require leadership", per its own doc comment) without
    /// paying for `cluster_init`/election. The `TempDir` must outlive the node.
    async fn test_node() -> (Arc<RaftNode>, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let config = rift_cluster::NodeConfig {
            node_id: 1,
            bind: "127.0.0.1:0".parse().expect("bind addr"),
            advertise: None,
            data_dir: dir.path().to_path_buf(),
            secret: Some("admin-front-test-secret".to_owned()),
            routes: rift_cluster::Router::new(),
            engine: None,
        };
        let node = RaftNode::start(config).await.expect("node starts");
        (Arc::new(node), dir)
    }

    async fn body_text(response: Response<FrontBody>) -> String {
        let collected = response.into_body().collect().await.expect("collect body");
        String::from_utf8(collected.to_bytes().to_vec()).expect("utf8 body")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resolve_op_scripts_leaves_non_script_ops_untouched() {
        let (node, _dir) = test_node().await;
        let mut op = ControlOp::DeleteImposter {
            tenant: TenantId::default(),
            port: 4545,
        };

        let result = resolve_op_scripts(&mut op, &node, &ScriptBaseDir::Unconfigured, None);
        assert!(result.is_ok());
        assert!(matches!(op, ControlOp::DeleteImposter { port: 4545, .. }));

        let mut op = ControlOp::SetEnabled {
            tenant: TenantId::default(),
            port: 4545,
            enabled: false,
        };
        assert!(resolve_op_scripts(&mut op, &node, &ScriptBaseDir::Unconfigured, None).is_ok());

        // Move/DeleteById steps carry no stub payload, so no registry read and
        // no resolution — even against a port that has no imposter at all.
        let mut op = ControlOp::PatchStubs {
            tenant: TenantId::default(),
            port: 4545,
            edit: StubEditScript(vec![
                StubEdit::Move { from: 1, to: 0 },
                StubEdit::DeleteById { id: "a".to_owned() },
            ]),
        };
        assert!(resolve_op_scripts(&mut op, &node, &ScriptBaseDir::Unconfigured, None).is_ok());

        node.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_resolution_error_names_the_imposter_index_and_port() {
        let (node, _dir) = test_node().await;
        let config: ImposterConfig = serde_json::from_value(serde_json::json!({
            "port": 4545,
            "protocol": "http",
            "stubs": [{
                "responses": [{ "_rift": { "script": { "file": "greet.rhai" } } }],
            }],
        }))
        .expect("config parses");
        let mut op = ControlOp::PutImposter {
            tenant: TenantId::default(),
            config: Box::new(config),
        };

        let err = resolve_op_scripts(&mut op, &node, &ScriptBaseDir::Unconfigured, Some(1))
            .expect_err("an Unconfigured base must refuse a file: ref");
        let text = body_text(err).await;
        assert!(
            text.contains("Script resolution failed in imposter[1] (port Some(4545)):"),
            "{text}"
        );

        node.shutdown().await.expect("shutdown");
    }
}
