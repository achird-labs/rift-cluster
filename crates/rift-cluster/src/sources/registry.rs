//! `registry:` — imposters assembled from a small internal service registry
//! (issue #136).
//!
//! ## URI shape
//!
//! `registry://<service-id>[,<service-id>…]`. For each id, in the order
//! written, this issues `GET {endpoint}/{service-id}` against
//! [`RegistryConfig::endpoint`] (a trailing `/` on the endpoint is trimmed
//! before joining, so `http://host/` and `http://host` behave identically).
//!
//! ## Response shape
//!
//! Each response is mapped through [`RegistryConfig::imposters_pointer`], an
//! RFC 6901 JSON pointer (e.g. `/data/imposters`) into wherever the registry's
//! own response envelope keeps the imposters array — this provider has no
//! opinion about that envelope beyond the one pointer. A pointer that resolves
//! to nothing, or to a value that is not an array, is a **configuration
//! error** naming the pointer, never treated as an empty imposter list: an
//! empty list is indistinguishable from "delete every imposter this source
//! owns", which is exactly the wrong thing to do quietly on a misconfigured
//! pointer or a registry that changed its response shape.
//!
//! The matched array is re-encoded to JSON text and handed to
//! [`parse_remote_document`] like any other provider's bytes — this module
//! never constructs an [`rift_ee::seams::ImposterConfig`] itself.
//!
//! ## Version
//!
//! `version` is a SHA-256 hex digest of the concatenated raw response bodies,
//! in service order. Deterministic for unchanged content is the whole point:
//! it is what makes #134's short circuit fire even against a registry that
//! serves no `ETag` at all.
//!
//! ## Auth
//!
//! A resolved credential is sent as `Authorization: Bearer <token>` on every
//! per-service request. No `auth_ref` means an anonymous fetch; a named ref
//! that fails to resolve is always an error, never a fallback to anonymous —
//! see [`super::auth`]'s fail-closed rule.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context as _;
use rift_ee::seams::{
    FetchedImposters, LoadedConfig, SourceMeta, SourceRef, parse_remote_document,
};
use sha2::{Digest, Sha256};

use super::CredentialedSource;
use super::auth::{self, CredentialResolver};
use super::common::{self, hex_encode};

/// Whole-request budget per service: connect, headers and body.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Where the registry lives, and where in each response the imposters are.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// Joined with `/<service-id>` for each request; a trailing `/` is
    /// trimmed first.
    pub endpoint: String,
    /// An RFC 6901 JSON pointer into each response, e.g. `/data/imposters`.
    pub imposters_pointer: String,
}

/// `registry:` imposter source.
pub struct RegistrySource {
    resolver: Arc<dyn CredentialResolver>,
    config: RegistryConfig,
    client: reqwest::Client,
}

impl std::fmt::Debug for RegistrySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrySource")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl RegistrySource {
    /// # Errors
    /// If the underlying HTTP client cannot be built.
    pub fn new(
        resolver: Arc<dyn CredentialResolver>,
        config: RegistryConfig,
    ) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("building the registry source's HTTP client")?;
        Ok(Self {
            resolver,
            config,
            client,
        })
    }
}

impl CredentialedSource for RegistrySource {
    fn schemes(&self) -> &'static [&'static str] {
        &["registry"]
    }

    fn fetch_with_auth<'a>(
        &'a self,
        r: &'a SourceRef,
        auth_ref: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<FetchedImposters>> + Send + 'a>> {
        Box::pin(async move {
            // Fail closed, before any request is issued: a named credential
            // that does not resolve must never fall through to an anonymous
            // fetch. Resolved off the async worker thread: the resolver can
            // do blocking file I/O under the secrets-directory step.
            let credential = auth::resolve_off_thread(&self.resolver, auth_ref).await?;

            let ids = parse_registry_uri(&r.uri)?;
            let mut hasher = Sha256::new();
            let mut documents: Vec<(String, LoadedConfig)> = Vec::with_capacity(ids.len());

            for id in &ids {
                let url = format!("{}/{id}", self.config.endpoint.trim_end_matches('/'));
                let mut request = self.client.get(&url);
                if let Some(credential) = &credential {
                    request = request.header(
                        reqwest::header::AUTHORIZATION,
                        format!("Bearer {}", credential.expose()),
                    );
                }

                let response = request
                    .send()
                    .await
                    .map_err(|e| anyhow::anyhow!("fetching registry service {id:?}: {e}"))?;

                let status = response.status();
                if !status.is_success() {
                    // The body is never read on this path. A registry that
                    // echoes the request back in an error body (a hostile or
                    // merely chatty one) must not be able to put the
                    // Authorization header's token into our error string.
                    anyhow::bail!("registry service {id:?} returned HTTP {status}");
                }

                let body =
                    common::read_capped(response, &format!("registry response from {url}")).await?;
                hasher.update(body.as_bytes());

                let value: serde_json::Value = serde_json::from_str(&body).map_err(|e| {
                    anyhow::anyhow!("registry service {id:?} did not return JSON: {e}")
                })?;
                let pointer = &self.config.imposters_pointer;
                let imposters = value.pointer(pointer).filter(|v| v.is_array()).ok_or_else(
                    || {
                        anyhow::anyhow!(
                            "registry service {id:?} has nothing at pointer {pointer:?}, or it is \
                             not an array; refusing to treat that as an empty imposter list"
                        )
                    },
                )?;
                let array_text = serde_json::to_string(imposters).map_err(|e| {
                    anyhow::anyhow!("registry service {id:?} imposters do not re-encode: {e}")
                })?;
                let loaded = parse_remote_document(&array_text, &format!("{url}{pointer}"))
                    .map_err(|e| anyhow::anyhow!("registry service {id:?}: {e}"))?;
                documents.push((id.clone(), loaded));
            }

            let merged = common::merge_documents(documents, "registry service")?;
            let digest = hex_encode(&hasher.finalize());

            Ok(FetchedImposters {
                configs: merged.imposters,
                intercept: merged.intercept,
                routes: merged.routes,
                meta: SourceMeta {
                    version: Some(digest),
                    fetched_at: SystemTime::now(),
                },
                unchanged: false,
            })
        })
    }
}

fn parse_registry_uri(uri: &str) -> anyhow::Result<Vec<String>> {
    let rest = uri
        .strip_prefix("registry://")
        .ok_or_else(|| anyhow::anyhow!("source uri {uri:?} is not a registry:// uri"))?;
    let ids: Vec<String> = rest.split(',').map(str::to_owned).collect();
    if ids.iter().any(|id| id.is_empty()) {
        anyhow::bail!(
            "source uri {uri:?} is not written `registry://<service-id>[,<service-id>…]`"
        );
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_registry_uri_splits_ids_in_order() {
        assert_eq!(
            parse_registry_uri("registry://svc-a,svc-b").unwrap(),
            vec!["svc-a".to_owned(), "svc-b".to_owned()]
        );
        assert_eq!(
            parse_registry_uri("registry://svc-a").unwrap(),
            vec!["svc-a".to_owned()]
        );
        assert!(parse_registry_uri("registry://svc-a,,svc-b").is_err());
    }
}
