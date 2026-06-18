use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_core::anthropic_compat::{
    CLAUDE_CODE_STABLE_VERSION_FALLBACK, CLAUDE_CODE_STABLE_VERSION_KEY, claude_code_user_agent,
};
use cc_lb_core::{AuditPayload, MetadataHookHandle, MetadataHookRequest};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::worker::{EntityJob, SchedulerBackend};
use cc_lb_signer_anthropic_oauth::{LazyRefreshError, LazyRefreshHandle};
use cc_lb_storage_api::{AuditEntry, AuditStore, StorageError, StorageResult, UpstreamRecord};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderValue, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dynamic_view_builder::Stores;

const LAZY_REFRESH_CLAIM_TTL_SECS: u64 = 60;
const LAZY_REFRESH_CONTENTION_WAIT_SECS: u64 = 60;
const LAZY_REFRESH_POLL_INTERVAL_SECS: u64 = 1;

type HyperTokenClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

#[derive(Clone)]
struct TokenHttpClient {
    client: HyperTokenClient,
    timeout: Duration,
}

impl TokenHttpClient {
    fn new(timeout: Duration) -> Self {
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self { client, timeout }
    }
}

#[derive(Clone)]
pub struct LazyRefresher {
    stores: Arc<Stores>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    replica_id: Uuid,
    http: TokenHttpClient,
    metadata_hook: Option<MetadataHookHandle>,
    cancel: CancellationToken,
    claim_guard: Arc<dyn LazyRefreshClaimGuard>,
    contention: LazyRefreshContentionConfig,
    apalis_handle: SchedulerBackend,
}

#[derive(Clone, Copy)]
pub struct LazyRefreshContentionConfig {
    claim_ttl_secs: u64,
    wait_timeout: Duration,
    poll_interval: Duration,
}

impl LazyRefreshContentionConfig {
    const fn production() -> Self {
        Self {
            claim_ttl_secs: LAZY_REFRESH_CLAIM_TTL_SECS,
            wait_timeout: Duration::from_secs(LAZY_REFRESH_CONTENTION_WAIT_SECS),
            poll_interval: Duration::from_secs(LAZY_REFRESH_POLL_INTERVAL_SECS),
        }
    }

    #[cfg(test)]
    const fn for_tests(wait_timeout: Duration, poll_interval: Duration) -> Self {
        Self {
            claim_ttl_secs: LAZY_REFRESH_CLAIM_TTL_SECS,
            wait_timeout,
            poll_interval,
        }
    }
}

#[async_trait]
pub trait LazyRefreshClaimGuard: Send + Sync {
    async fn try_acquire(
        &self,
        upstream_id: Uuid,
        holder: &str,
        ttl_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<bool>;

    async fn complete_and_bump_generation(
        &self,
        upstream_id: Uuid,
        holder: &str,
        generation: u64,
    ) -> StorageResult<bool>;

    async fn release_if_holder(&self, upstream_id: Uuid, holder: &str) -> StorageResult<bool>;
}

struct LegacyRefreshLeaseGuard {
    upstreams: Arc<dyn cc_lb_storage_api::UpstreamStore>,
    replica_id: Uuid,
}

#[async_trait]
impl LazyRefreshClaimGuard for LegacyRefreshLeaseGuard {
    async fn try_acquire(
        &self,
        upstream_id: Uuid,
        _holder: &str,
        ttl_secs: u64,
        _now_unix_secs: u64,
    ) -> StorageResult<bool> {
        self.upstreams
            .claim_refresh_lease(upstream_id, self.replica_id, ttl_secs)
            .await
    }

    async fn complete_and_bump_generation(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _generation: u64,
    ) -> StorageResult<bool> {
        Ok(true)
    }

    async fn release_if_holder(&self, _upstream_id: Uuid, _holder: &str) -> StorageResult<bool> {
        Ok(true)
    }
}

#[cfg(feature = "sqlite")]
#[async_trait]
impl LazyRefreshClaimGuard
    for cc_lb_scheduler::idempotency::OAuthRefreshClaimsStore<scheduler_sqlx::Sqlite>
{
    async fn try_acquire(
        &self,
        upstream_id: Uuid,
        holder: &str,
        ttl_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        self.try_acquire(upstream_id, holder, ttl_secs, now_unix_secs)
            .await
            .map_err(scheduler_claim_error)
    }

    async fn complete_and_bump_generation(
        &self,
        upstream_id: Uuid,
        holder: &str,
        generation: u64,
    ) -> StorageResult<bool> {
        self.complete_and_bump_generation(upstream_id, holder, generation)
            .await
            .map_err(scheduler_claim_error)
    }

    async fn release_if_holder(&self, upstream_id: Uuid, holder: &str) -> StorageResult<bool> {
        self.release_if_holder(upstream_id, holder)
            .await
            .map_err(scheduler_claim_error)
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl LazyRefreshClaimGuard
    for cc_lb_scheduler::idempotency::OAuthRefreshClaimsStore<scheduler_sqlx::Postgres>
{
    async fn try_acquire(
        &self,
        upstream_id: Uuid,
        holder: &str,
        ttl_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<bool> {
        self.try_acquire(upstream_id, holder, ttl_secs, now_unix_secs)
            .await
            .map_err(scheduler_claim_error)
    }

    async fn complete_and_bump_generation(
        &self,
        upstream_id: Uuid,
        holder: &str,
        generation: u64,
    ) -> StorageResult<bool> {
        self.complete_and_bump_generation(upstream_id, holder, generation)
            .await
            .map_err(scheduler_claim_error)
    }

    async fn release_if_holder(&self, upstream_id: Uuid, holder: &str) -> StorageResult<bool> {
        self.release_if_holder(upstream_id, holder)
            .await
            .map_err(scheduler_claim_error)
    }
}

impl LazyRefresher {
    pub fn new(
        stores: Arc<Stores>,
        aead: Arc<AeadService>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        replica_id: Uuid,
        metadata_hook: Option<MetadataHookHandle>,
        cancel: CancellationToken,
        apalis_handle: SchedulerBackend,
    ) -> Self {
        let claim_guard = Arc::new(LegacyRefreshLeaseGuard {
            upstreams: stores.upstreams.clone(),
            replica_id,
        });
        Self::new_with_claim_guard(
            LazyRefresherDeps {
                stores,
                aead,
                oauth_cfg,
            },
            replica_id,
            metadata_hook,
            cancel,
            claim_guard,
            apalis_handle,
        )
    }

    pub fn new_with_claim_guard(
        deps: LazyRefresherDeps,
        replica_id: Uuid,
        metadata_hook: Option<MetadataHookHandle>,
        cancel: CancellationToken,
        claim_guard: Arc<dyn LazyRefreshClaimGuard>,
        apalis_handle: SchedulerBackend,
    ) -> Self {
        Self::new_with_claim_guard_and_config(
            deps,
            replica_id,
            metadata_hook,
            cancel,
            claim_guard,
            LazyRefreshContentionConfig::production(),
            apalis_handle,
        )
    }

    #[cfg(test)]
    fn new_with_claim_guard_for_tests(
        deps: LazyRefresherDeps,
        replica_id: Uuid,
        metadata_hook: Option<MetadataHookHandle>,
        cancel: CancellationToken,
        claim_guard: Arc<dyn LazyRefreshClaimGuard>,
        contention: LazyRefreshContentionConfig,
        apalis_handle: SchedulerBackend,
    ) -> Self {
        Self::new_with_claim_guard_and_config(
            deps,
            replica_id,
            metadata_hook,
            cancel,
            claim_guard,
            contention,
            apalis_handle,
        )
    }

    fn new_with_claim_guard_and_config(
        deps: LazyRefresherDeps,
        replica_id: Uuid,
        metadata_hook: Option<MetadataHookHandle>,
        cancel: CancellationToken,
        claim_guard: Arc<dyn LazyRefreshClaimGuard>,
        contention: LazyRefreshContentionConfig,
        apalis_handle: SchedulerBackend,
    ) -> Self {
        let http = TokenHttpClient::new(Duration::from_secs(30));
        Self {
            stores: deps.stores,
            aead: deps.aead,
            oauth_cfg: deps.oauth_cfg,
            replica_id,
            http,
            metadata_hook,
            cancel,
            claim_guard,
            contention,
            apalis_handle,
        }
    }

    async fn wait_for_token_generation(
        &self,
        upstream_id: Uuid,
        starting_generation: u64,
    ) -> Result<(), LazyRefreshError> {
        let deadline = tokio::time::Instant::now() + self.contention.wait_timeout;
        loop {
            let Some(upstream) = self
                .stores
                .upstreams
                .get_by_id(upstream_id)
                .await
                .map_err(lazy_error)?
            else {
                return Err(LazyRefreshError::Failed {
                    reason: "oauth upstream not found".to_owned(),
                });
            };
            if upstream.oauth_token_generation > starting_generation {
                return Ok(());
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                metrics::counter!("cclb_scheduler_lazy_refresh_timeout_total").increment(1);
                return Err(LazyRefreshError::Failed {
                    reason: "oauth refresh timed out waiting for another holder".to_owned(),
                });
            }
            let sleep_for = self.contention.poll_interval.min(deadline - now);
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    return Err(LazyRefreshError::Failed {
                        reason: "oauth refresh cancelled".to_owned(),
                    });
                }
                _ = tokio::time::sleep(sleep_for) => {}
            }
        }
    }
}

#[derive(Clone)]
pub struct LazyRefresherDeps {
    pub stores: Arc<Stores>,
    pub aead: Arc<AeadService>,
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
}

#[async_trait]
impl LazyRefreshHandle for LazyRefresher {
    async fn refresh_one(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        let upstream = self
            .stores
            .upstreams
            .get_by_id(upstream_id)
            .await
            .map_err(lazy_error)?
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "oauth upstream not found".to_owned(),
            })?;
        let holder = lazy_refresh_holder(self.replica_id);
        let claimed = self
            .claim_guard
            .try_acquire(
                upstream_id,
                &holder,
                self.contention.claim_ttl_secs,
                now_unix_secs(),
            )
            .await
            .map_err(lazy_error)?;
        if !claimed {
            return self
                .wait_for_token_generation(upstream_id, upstream.oauth_token_generation)
                .await;
        }
        let result = refresh_flow(
            &self.stores,
            self.stores.audit.as_deref(),
            &self.aead,
            &self.oauth_cfg,
            self.replica_id,
            &self.http,
            self.metadata_hook.as_ref(),
            &self.cancel,
            upstream,
        )
        .await;
        match result {
            Ok(()) => {
                let generation = self
                    .stores
                    .upstreams
                    .read_oauth_token_generation(upstream_id)
                    .await
                    .map_err(lazy_error)?
                    .ok_or_else(|| LazyRefreshError::Failed {
                        reason: "oauth upstream not found".to_owned(),
                    })?;
                let completed = self
                    .claim_guard
                    .complete_and_bump_generation(upstream_id, &holder, generation)
                    .await
                    .map_err(lazy_error)?;
                if !completed {
                    return Err(LazyRefreshError::Failed {
                        reason: "oauth refresh claim was not held at completion".to_owned(),
                    });
                }
                self.apalis_handle
                    .push_job(EntityJob::MetadataRefresh(MetadataRefreshJob::new(
                        upstream_id,
                        generation,
                    )))
                    .await
                    .map_err(lazy_scheduler_error)?;
                Ok(())
            }
            Err(error) => {
                if let Err(release_error) = self
                    .claim_guard
                    .release_if_holder(upstream_id, &holder)
                    .await
                {
                    tracing::warn!(error = %release_error, upstream_id = %upstream_id, "oauth refresh claim release failed");
                }
                Err(LazyRefreshError::Failed {
                    reason: error.to_string(),
                })
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RefreshError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("missing oauth credentials")]
    MissingCredentials,
    #[error("oauth decrypt failed")]
    Decrypt,
    #[error("oauth encrypt failed")]
    Encrypt,
    #[error("oauth token endpoint returned {0}")]
    Status(StatusCode),
    #[error("oauth token endpoint request failed: {0}")]
    Http(String),
    #[error("oauth token response parse failed: {0}")]
    Parse(serde_json::Error),
    #[error("oauth refresh cancelled")]
    Cancelled,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: u64,
    scope: Option<String>,
}

#[allow(clippy::too_many_arguments)]
async fn refresh_flow(
    stores: &Stores,
    audit: Option<&dyn AuditStore>,
    aead: &AeadService,
    oauth_cfg: &AnthropicOAuthConfig,
    replica_id: Uuid,
    http: &TokenHttpClient,
    metadata_hook: Option<&MetadataHookHandle>,
    cancel: &CancellationToken,
    upstream: UpstreamRecord,
) -> Result<(), RefreshError> {
    let previous = upstream
        .oauth_credentials
        .as_ref()
        .ok_or(RefreshError::MissingCredentials)?
        .decrypt(aead, upstream.id.as_bytes())
        .map_err(|_| RefreshError::Decrypt)?;
    let token = request_refresh(http, oauth_cfg, cancel, &previous.refresh_token).await;
    match token {
        Ok(response) => {
            let now = now_unix_secs();
            let scopes = response
                .scope
                .as_deref()
                .map(|scope| scope.split_whitespace().map(ToOwned::to_owned).collect())
                .unwrap_or(previous.scopes);
            let bundle = OAuthTokenBundle {
                access_token: response.access_token,
                refresh_token: response.refresh_token.unwrap_or(previous.refresh_token),
                expires_at_unix_secs: now.saturating_add(response.expires_in),
                scopes,
            };
            let fingerprint = access_token_fingerprint(&bundle.access_token);
            let expires_at = bundle.expires_at_unix_secs;
            let encrypted = EncryptedOAuthTokens::encrypt(aead, &bundle, upstream.id.as_bytes())
                .map_err(|_| RefreshError::Encrypt)?;
            stores
                .upstreams
                .complete_refresh(upstream.id, replica_id, encrypted)
                .await?;
            if let Some(metadata_hook) = metadata_hook {
                enqueue_metadata_hook(
                    stores,
                    metadata_hook,
                    upstream.id,
                    bundle.access_token.clone(),
                )
                .await;
            }
            increment_metric(&upstream.name, "success");
            emit_audit(
                audit,
                AuditPayload::UpstreamOauthRefreshSuccess {
                    upstream_id: upstream.id.to_string(),
                    upstream_name: upstream.name.clone(),
                    expires_at_unix_secs: expires_at,
                    access_token_fingerprint: fingerprint,
                    replica_id,
                },
                &upstream,
                200,
            )
            .await;
            Ok(())
        }
        Err(error) => {
            increment_metric(&upstream.name, "failure");
            let reason = reason_for(&error);
            let release_result = stores
                .upstreams
                .release_lease_on_failure(upstream.id, replica_id, reason.clone())
                .await;
            emit_audit(
                audit,
                AuditPayload::UpstreamOauthRefreshFailure {
                    upstream_id: upstream.id.to_string(),
                    upstream_name: upstream.name.clone(),
                    reason,
                    replica_id,
                },
                &upstream,
                500,
            )
            .await;
            release_result?;
            Err(error)
        }
    }
}

async fn enqueue_metadata_hook(
    stores: &Stores,
    metadata_hook: &MetadataHookHandle,
    upstream_id: Uuid,
    access_token: String,
) {
    let version = stores
        .anthropic_compatibility_kv
        .get_compatibility_kv(CLAUDE_CODE_STABLE_VERSION_KEY)
        .await
        .map(|record| record.map(|record| record.value))
        .unwrap_or(None)
        .unwrap_or_else(|| CLAUDE_CODE_STABLE_VERSION_FALLBACK.to_owned());
    metadata_hook.enqueue(MetadataHookRequest {
        upstream_id,
        access_token,
        user_agent: claude_code_user_agent(&version),
    });
}

async fn request_refresh(
    http: &TokenHttpClient,
    oauth_cfg: &AnthropicOAuthConfig,
    cancel: &CancellationToken,
    refresh_token: &str,
) -> Result<TokenResponse, RefreshError> {
    let body = refresh_form_body(oauth_cfg.client_id.as_str(), refresh_token);
    let content_length = HeaderValue::from_str(&body.len().to_string())
        .map_err(|error| RefreshError::Http(error.to_string()))?;
    let request = Request::post(oauth_cfg.token_url.as_str())
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header(CONTENT_LENGTH, content_length)
        .body(Full::new(Bytes::from(body)))
        .map_err(|error| RefreshError::Http(error.to_string()))?;

    let response = tokio::select! {
        _ = cancel.cancelled() => return Err(RefreshError::Cancelled),
        response = tokio::time::timeout(http.timeout, http.client.request(request)) => {
            response
                .map_err(|_| RefreshError::Http("request timed out".to_owned()))?
                .map_err(|error| RefreshError::Http(error.to_string()))?
        }
    };
    let status = response.status();
    if !status.is_success() {
        return Err(RefreshError::Status(status));
    }
    let bytes = tokio::select! {
        _ = cancel.cancelled() => return Err(RefreshError::Cancelled),
        bytes = tokio::time::timeout(http.timeout, response.into_body().collect()) => {
            bytes
                .map_err(|_| RefreshError::Http("response body timed out".to_owned()))?
                .map_err(|error| RefreshError::Http(error.to_string()))?
                .to_bytes()
        }
    };
    serde_json::from_slice(&bytes).map_err(RefreshError::Parse)
}

fn refresh_form_body(client_id: &str, refresh_token: &str) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("grant_type", "refresh_token");
    serializer.append_pair("client_id", client_id);
    serializer.append_pair("refresh_token", refresh_token);
    serializer.finish()
}

fn access_token_fingerprint(access_token: &str) -> String {
    let digest = Sha256::digest(access_token.as_bytes());
    to_hex(&digest[..4])
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn increment_metric(upstream: &str, outcome: &'static str) {
    metrics::counter!(
        "cclb_oauth_refresh_total",
        "upstream" => upstream.to_owned(),
        "outcome" => outcome
    )
    .increment(1);
}

async fn emit_audit(
    audit: Option<&dyn AuditStore>,
    payload: AuditPayload,
    upstream: &UpstreamRecord,
    status: u16,
) {
    let Some(audit) = audit else {
        return;
    };
    let now = now_unix_secs();
    let entry = AuditEntry {
        ts: now,
        request_id: format!("oauth-refresh-{}-{now}", upstream.id),
        principal_id: String::new(),
        route: "oauth_refresh".to_owned(),
        upstream: upstream.id.to_string(),
        model: None,
        status,
        input_tokens: None,
        output_tokens: None,
        duration_ms: 0,
        agent_label: None,
        api_key_id: None,
        cost_usd_micros: None,
        limit_violation: None,
        admin_action: Some(payload.to_string()),
        actor: Some("system".to_owned()),
        kind: Some(payload.to_string()),
        payload: None,
    };
    if let Err(error) = audit.append_audit(&entry).await {
        tracing::warn!(error = %error, upstream_id = %upstream.id, "oauth refresh audit append failed");
    }
}

fn reason_for(error: &RefreshError) -> String {
    match error {
        RefreshError::Status(status) => format!("status_{}", status.as_u16()),
        RefreshError::Http(_) => "network".to_owned(),
        RefreshError::Parse(_) => "parse".to_owned(),
        RefreshError::Cancelled => "cancelled".to_owned(),
        RefreshError::MissingCredentials => "missing_credentials".to_owned(),
        RefreshError::Decrypt => "decrypt".to_owned(),
        RefreshError::Encrypt => "encrypt".to_owned(),
        RefreshError::Storage(_) => "storage".to_owned(),
    }
}

fn lazy_error(error: StorageError) -> LazyRefreshError {
    LazyRefreshError::Failed {
        reason: error.to_string(),
    }
}

fn lazy_scheduler_error(error: cc_lb_scheduler::error::SchedulerError) -> LazyRefreshError {
    LazyRefreshError::Failed {
        reason: error.to_string(),
    }
}

fn lazy_refresh_holder(replica_id: Uuid) -> String {
    format!("cc-lb-server:lazy-refresher:{replica_id}")
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn scheduler_claim_error(error: cc_lb_scheduler::error::SchedulerError) -> StorageError {
    StorageError::Unavailable {
        message: error.to_string(),
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
    use cc_lb_config::AnthropicOAuthConfig;
    use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
    use cc_lb_storage_api::upstream::UpstreamKind;
    use cc_lb_storage_api::{BackendKind, MetaStore, StorageResult, UpstreamCreate, UpstreamStore};
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;
    use url::Url;
    use uuid::Uuid;

    use super::{
        LazyRefreshClaimGuard, LazyRefreshContentionConfig, LazyRefresher, LazyRefresherDeps,
        SchedulerBackend,
    };

    #[tokio::test]
    async fn release_lease_on_failure_clears_refresh_lease() {
        let storage = cc_lb_storage_sqlite::open_sqlite("sqlite::memory:")
            .await
            .expect("storage opens");
        storage.initialize(BackendKind::Sqlite).await.unwrap();
        let record = storage
            .create(UpstreamCreate {
                name: "failure-clear".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                next_warmup_at: None,
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
                warmup_dialect_plugin: None,
            })
            .await
            .expect("upstream created");
        let holder = Uuid::new_v4();
        assert!(
            storage
                .claim_refresh_lease(record.id, holder, 60)
                .await
                .expect("lease claimed")
        );

        storage
            .release_lease_on_failure(record.id, holder, "status_401".to_owned())
            .await
            .expect("failure marker written");

        let stored = storage
            .get_by_id(record.id)
            .await
            .expect("read upstream")
            .expect("upstream exists");
        assert_eq!(stored.refresh_lease_holder, None);
        assert_eq!(stored.refresh_lease_until_unix_secs, None);
        assert_eq!(stored.last_apply_error.as_deref(), Some("status_401"));
        assert!(stored.last_apply_at_unix_secs.is_some());
    }

    #[tokio::test]
    async fn lazy_refresher_single_flight_under_concurrency() {
        let fixture = LazyRefreshFixture::new(Duration::from_millis(50)).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let claims = Arc::new(TestOAuthRefreshClaims::single_winner());
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_millis(500),
            Duration::from_millis(5),
        );
        let first = fixture.lazy_refresher(claims.clone(), config);
        let second = fixture.lazy_refresher(claims.clone(), config);

        let (left, right) = tokio::join!(
            first.refresh_one(upstream_id),
            second.refresh_one(upstream_id)
        );

        left.expect("first lazy refresh succeeds");
        right.expect("second lazy refresh joins");
        assert_eq!(fixture.refresh_call_count(), 1);
        assert_eq!(claims.completed_generation(), Some(1));
    }

    #[tokio::test]
    async fn lazy_refresher_timeout_when_contended_generation_does_not_advance() {
        let fixture = LazyRefreshFixture::new(Duration::from_millis(0)).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let claims = Arc::new(TestOAuthRefreshClaims::always_contended());
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_millis(20),
            Duration::from_millis(5),
        );
        let refresher = fixture.lazy_refresher(claims, config);

        let error = refresher
            .refresh_one(upstream_id)
            .await
            .expect_err("contended lazy refresh times out");

        assert!(error.to_string().contains("timed out"));
        assert_eq!(fixture.refresh_call_count(), 0);
    }

    struct LazyRefreshFixture {
        _dir: tempfile::TempDir,
        storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
        stores: Arc<crate::dynamic_view_builder::Stores>,
        aead: Arc<AeadService>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        refresh_calls: Arc<AtomicUsize>,
        scheduler_backend: SchedulerBackend,
    }

    impl LazyRefreshFixture {
        async fn new(refresh_delay: Duration) -> Self {
            let refresh_calls = Arc::new(AtomicUsize::new(0));
            let token_url =
                spawn_lazy_refresh_token_server(refresh_calls.clone(), refresh_delay).await;
            let dir = tempfile::tempdir().expect("tempdir");
            let database_url = format!(
                "sqlite://{}",
                dir.path().join("lazy-refresh.sqlite").display()
            );
            let storage = Arc::new(
                cc_lb_storage_sqlite::open_sqlite(&database_url)
                    .await
                    .expect("storage opens"),
            );
            storage
                .initialize(BackendKind::Sqlite)
                .await
                .expect("storage initializes");
            let scheduler_db_url =
                format!("sqlite://{}", dir.path().join("scheduler.sqlite").display());
            use std::str::FromStr as _;
            let scheduler_options =
                scheduler_sqlx::sqlite::SqliteConnectOptions::from_str(&scheduler_db_url)
                    .expect("parse url")
                    .create_if_missing(true);
            let scheduler_pool = scheduler_sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(5)
                .connect_with(scheduler_options)
                .await
                .expect("scheduler storage opens");
            apalis_sqlite::SqliteStorage::setup(&scheduler_pool)
                .await
                .expect("scheduler storage initializes");
            let scheduler_backend =
                SchedulerBackend::Sqlite(cc_lb_scheduler::worker::SqliteSchedulerStorage {
                    pool: scheduler_pool.clone(),
                    storage: apalis_sqlite::SqliteStorage::new_in_queue(
                        &scheduler_pool,
                        cc_lb_scheduler::worker::ENTITY_QUEUE,
                    ),
                });
            let stores = Arc::new(crate::dynamic_view_builder::Stores {
                upstreams: storage.clone(),
                principals: storage.clone(),
                plugin_registry: storage.clone(),
                upstream_rate_limits: storage.clone(),
                upstream_subscription_quotas: storage.clone(),
                prompt_cache_observations: storage.clone(),
                anthropic_compatibility_kv: storage.clone(),
                audit: Some(storage.clone()),
                plugin_registry_repo: None,
            });
            let aead = Arc::new(AeadService::from_master_key([42; 32]));
            let oauth_cfg = Arc::new(AnthropicOAuthConfig {
                client_id: "lazy-client".to_owned(),
                auth_url: Url::parse("http://127.0.0.1/oauth/authorize").expect("auth url"),
                token_url,
                redirect_uri: Url::parse("http://127.0.0.1/callback").expect("redirect url"),
                scopes: vec!["messages".to_owned()],
            });
            Self {
                _dir: dir,
                storage,
                stores,
                aead,
                oauth_cfg,
                refresh_calls,
                scheduler_backend,
            }
        }

        async fn create_oauth_upstream(&self) -> Uuid {
            let record = self
                .storage
                .create(UpstreamCreate {
                    name: "lazy-guard".to_owned(),
                    kind: UpstreamKind::AnthropicOauth,
                    base_url: None,
                    api_key_ciphertext: None,
                    oauth_token_generation: None,
                    warmup_enabled: false,
                    next_warmup_at: None,
                    last_warmup_cycle_key: None,
                    warmup_lease_holder: None,
                    warmup_lease_until_unix_secs: None,
                    warmup_dialect_plugin: None,
                })
                .await
                .expect("upstream created");
            let encrypted = EncryptedOAuthTokens::encrypt(
                self.aead.as_ref(),
                &OAuthTokenBundle {
                    access_token: "sk-ant-oat01-old".to_owned(),
                    refresh_token: "sk-ant-ort01-old".to_owned(),
                    expires_at_unix_secs: 1,
                    scopes: vec!["messages".to_owned()],
                },
                record.id.as_bytes(),
            )
            .expect("tokens encrypt");
            self.storage
                .store_oauth_tokens(record.id, record.revision, encrypted)
                .await
                .expect("tokens stored");
            record.id
        }

        fn lazy_refresher(
            &self,
            claims: Arc<TestOAuthRefreshClaims>,
            config: LazyRefreshContentionConfig,
        ) -> LazyRefresher {
            LazyRefresher::new_with_claim_guard_for_tests(
                LazyRefresherDeps {
                    stores: self.stores.clone(),
                    aead: self.aead.clone(),
                    oauth_cfg: self.oauth_cfg.clone(),
                },
                Uuid::new_v4(),
                None,
                CancellationToken::new(),
                claims,
                config,
                self.scheduler_backend.clone(),
            )
        }

        fn refresh_call_count(&self) -> usize {
            self.refresh_calls.load(Ordering::SeqCst)
        }
    }

    async fn spawn_lazy_refresh_token_server(
        refresh_calls: Arc<AtomicUsize>,
        refresh_delay: Duration,
    ) -> Url {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("token listener binds");
        let addr = listener.local_addr().expect("token listener addr");
        let app = Router::new()
            .route("/oauth/token", post(lazy_refresh_token_response))
            .with_state(TokenServerState {
                refresh_calls,
                refresh_delay,
            });
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("token server runs");
        });
        Url::parse(&format!("http://{addr}/oauth/token")).expect("token url")
    }

    #[derive(Clone)]
    struct TokenServerState {
        refresh_calls: Arc<AtomicUsize>,
        refresh_delay: Duration,
    }

    async fn lazy_refresh_token_response(
        State(state): State<TokenServerState>,
    ) -> Json<serde_json::Value> {
        state.refresh_calls.fetch_add(1, Ordering::SeqCst);
        if !state.refresh_delay.is_zero() {
            tokio::time::sleep(state.refresh_delay).await;
        }
        Json(serde_json::json!({
            "access_token": "sk-ant-oat01-new",
            "refresh_token": "sk-ant-ort01-new",
            "expires_in": 3600,
            "scope": "messages"
        }))
    }

    #[derive(Debug)]
    struct TestOAuthRefreshClaims {
        first_acquire_wins: bool,
        acquired: AtomicBool,
        completed_generation: Mutex<Option<u64>>,
    }

    impl TestOAuthRefreshClaims {
        fn single_winner() -> Self {
            Self {
                first_acquire_wins: true,
                acquired: AtomicBool::new(false),
                completed_generation: Mutex::new(None),
            }
        }

        fn always_contended() -> Self {
            Self {
                first_acquire_wins: false,
                acquired: AtomicBool::new(true),
                completed_generation: Mutex::new(None),
            }
        }

        fn completed_generation(&self) -> Option<u64> {
            *self.completed_generation.lock().expect("completed lock")
        }
    }

    #[async_trait::async_trait]
    impl LazyRefreshClaimGuard for TestOAuthRefreshClaims {
        async fn try_acquire(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _ttl_secs: u64,
            _now_unix_secs: u64,
        ) -> StorageResult<bool> {
            Ok(self.first_acquire_wins && !self.acquired.swap(true, Ordering::SeqCst))
        }

        async fn complete_and_bump_generation(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            generation: u64,
        ) -> StorageResult<bool> {
            *self.completed_generation.lock().expect("completed lock") = Some(generation);
            Ok(true)
        }

        async fn release_if_holder(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
        ) -> StorageResult<bool> {
            Ok(true)
        }
    }
}
