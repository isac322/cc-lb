use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_core::AuditPayload;
use cc_lb_signer_anthropic_oauth::{LazyRefreshError, LazyRefreshHandle};
use cc_lb_storage_api::{AuditEntry, AuditStore, StorageError, StorageResult, UpstreamRecord};
use cc_lb_storage_api::upstream::UpstreamKind;
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

const SWEEP_INTERVAL_SECS: u64 = 60;
const LOOKAHEAD_SECS: u64 = 300;
const LEASE_TTL_SECS: u64 = 90;
const PAGE_SIZE: usize = 100;

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

pub struct OAuthRefresher {
    pub stores: Arc<Stores>,
    pub aead: Arc<AeadService>,
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub replica_id: Uuid,
    http: TokenHttpClient,
    pub cancel: CancellationToken,
}

impl OAuthRefresher {
    pub fn new(
        stores: Arc<Stores>,
        aead: Arc<AeadService>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        replica_id: Uuid,
        cancel: CancellationToken,
    ) -> Self {
        let http = TokenHttpClient::new(Duration::from_secs(30));
        Self {
            stores,
            aead,
            oauth_cfg,
            replica_id,
            http,
            cancel,
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_secs(SWEEP_INTERVAL_SECS));
        interval.tick().await;
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = interval.tick() => {}
            }

            let jitter = tokio::time::sleep(Duration::from_millis(rand::random_range(0..=10_000)));
            tokio::pin!(jitter);
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = &mut jitter => {}
            }

            if let Err(error) = self.sweep_once().await {
                tracing::warn!(error = %error, "oauth refresh sweep failed");
            }
        }
    }

    pub async fn sweep_once(&self) -> StorageResult<()> {
        let candidates = self.candidates().await?;
        for upstream in candidates {
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            let claimed = self
                .stores
                .upstreams
                .claim_refresh_lease(upstream.id, self.replica_id, LEASE_TTL_SECS)
                .await?;
            if !claimed {
                continue;
            }
            self.refresh_claimed(upstream).await;
        }
        Ok(())
    }

    async fn candidates(&self) -> StorageResult<Vec<UpstreamRecord>> {
        let now = now_unix_secs();
        let mut after = None;
        let mut candidates = Vec::new();
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(candidates),
                page = self.stores.upstreams.list(after, PAGE_SIZE) => {
                    let page = page?;
                    if page.is_empty() {
                        break;
                    }
                    after = page.last().map(|record| record.id);
                    for record in page {
                        if is_refresh_candidate(&record, &self.aead, now) {
                            candidates.push(record);
                        }
                    }
                }
            }
        }
        Ok(candidates)
    }

    async fn refresh_claimed(&self, upstream: UpstreamRecord) {
        let result = refresh_flow(
            &self.stores,
            self.stores.audit.as_deref(),
            &self.aead,
            &self.oauth_cfg,
            self.replica_id,
            &self.http,
            &self.cancel,
            upstream.clone(),
        )
        .await;
        if let Err(error) = result {
            tracing::error!(
                upstream_id = %upstream.id,
                upstream_name = upstream.name.as_str(),
                error = %error,
                "oauth refresh failed"
            );
        }
    }
}

#[derive(Clone)]
pub struct LazyRefresher {
    stores: Arc<Stores>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    replica_id: Uuid,
    http: TokenHttpClient,
    cancel: CancellationToken,
}

impl LazyRefresher {
    pub fn new(
        stores: Arc<Stores>,
        aead: Arc<AeadService>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        replica_id: Uuid,
        cancel: CancellationToken,
    ) -> Self {
        let http = TokenHttpClient::new(Duration::from_secs(30));
        Self {
            stores,
            aead,
            oauth_cfg,
            replica_id,
            http,
            cancel,
        }
    }
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
        let claimed = self
            .stores
            .upstreams
            .claim_refresh_lease(upstream_id, self.replica_id, LEASE_TTL_SECS)
            .await
            .map_err(lazy_error)?;
        if !claimed {
            return Err(LazyRefreshError::Failed {
                reason: "oauth refresh lease is held".to_owned(),
            });
        }
        refresh_flow(
            &self.stores,
            self.stores.audit.as_deref(),
            &self.aead,
            &self.oauth_cfg,
            self.replica_id,
            &self.http,
            &self.cancel,
            upstream,
        )
        .await
        .map_err(|error| LazyRefreshError::Failed {
            reason: error.to_string(),
        })
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

fn is_refresh_candidate(record: &UpstreamRecord, aead: &AeadService, now: u64) -> bool {
    if record.kind != UpstreamKind::AnthropicOauth
        || !record.enabled
        || record.deleted_at_unix_secs.is_some()
    {
        return false;
    }
    let Some(encrypted) = &record.oauth_credentials else {
        return false;
    };
    encrypted
        .decrypt(aead, record.id.as_bytes())
        .map(|bundle| bundle.expires_at_unix_secs < now.saturating_add(LOOKAHEAD_SECS))
        .unwrap_or(false)
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

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
