use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_core::AuditPayload;
use cc_lb_observability::{OAuthRefreshOutcome, record_oauth_refresh, set_oauth_refresh_lag};
use cc_lb_signer_anthropic_oauth::{LazyRefreshError, LazyRefreshHandle};
use cc_lb_storage_api::{AuditEntry, AuditStore, StorageError, StorageResult, UpstreamRecord};
use cc_lb_storage_api::upstream::UpstreamKind;
use reqwest::StatusCode;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dynamic_view_builder::Stores;

const SWEEP_INTERVAL_SECS: u64 = 60;
const LOOKAHEAD_SECS: u64 = 300;
const LEASE_TTL_SECS: u64 = 90;
const PAGE_SIZE: usize = 100;

pub struct OAuthRefresher {
    pub stores: Arc<Stores>,
    pub aead: Arc<AeadService>,
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub replica_id: Uuid,
    pub http: reqwest::Client,
    pub cancel: CancellationToken,
}

impl OAuthRefresher {
    pub fn new(
        stores: Arc<Stores>,
        aead: Arc<AeadService>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        replica_id: Uuid,
        cancel: CancellationToken,
    ) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self {
            stores,
            aead,
            oauth_cfg,
            replica_id,
            http,
            cancel,
        })
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
                        if let Some(lag_seconds) = refresh_lag_seconds(&record, &self.aead, now) {
                            set_oauth_refresh_lag(&record.name, lag_seconds);
                            if lag_seconds > 0.0 {
                                candidates.push(record);
                            }
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
    http: reqwest::Client,
    cancel: CancellationToken,
}

impl LazyRefresher {
    pub fn new(
        stores: Arc<Stores>,
        aead: Arc<AeadService>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        replica_id: Uuid,
        cancel: CancellationToken,
    ) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self {
            stores,
            aead,
            oauth_cfg,
            replica_id,
            http,
            cancel,
        })
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
    Http(reqwest::Error),
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
    http: &reqwest::Client,
    cancel: &CancellationToken,
    upstream: UpstreamRecord,
) -> Result<(), RefreshError> {
    let started = Instant::now();
    let previous = match upstream.oauth_credentials.as_ref() {
        Some(credentials) => match credentials.decrypt(aead, upstream.id.as_bytes()) {
            Ok(previous) => previous,
            Err(_) => {
                record_oauth_refresh(
                    &upstream.name,
                    OAuthRefreshOutcome::Network,
                    started.elapsed(),
                );
                return Err(RefreshError::Decrypt);
            }
        },
        None => {
            record_oauth_refresh(
                &upstream.name,
                OAuthRefreshOutcome::Network,
                started.elapsed(),
            );
            return Err(RefreshError::MissingCredentials);
        }
    };
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
            let encrypted =
                match EncryptedOAuthTokens::encrypt(aead, &bundle, upstream.id.as_bytes()) {
                    Ok(encrypted) => encrypted,
                    Err(_) => {
                        record_oauth_refresh(
                            &upstream.name,
                            OAuthRefreshOutcome::Network,
                            started.elapsed(),
                        );
                        return Err(RefreshError::Encrypt);
                    }
                };
            if let Err(error) = stores
                .upstreams
                .complete_refresh(upstream.id, replica_id, encrypted)
                .await
            {
                record_oauth_refresh(
                    &upstream.name,
                    OAuthRefreshOutcome::Network,
                    started.elapsed(),
                );
                return Err(error.into());
            }
            record_oauth_refresh(
                &upstream.name,
                OAuthRefreshOutcome::Success,
                started.elapsed(),
            );
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
            record_oauth_refresh(&upstream.name, refresh_outcome(&error), started.elapsed());
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
    http: &reqwest::Client,
    oauth_cfg: &AnthropicOAuthConfig,
    cancel: &CancellationToken,
    refresh_token: &str,
) -> Result<TokenResponse, RefreshError> {
    let request = http
        .post(oauth_cfg.token_url.clone())
        .form(&[
            ("grant_type", "refresh_token"),
            ("client_id", oauth_cfg.client_id.as_str()),
            ("refresh_token", refresh_token),
        ])
        .send();
    let response = tokio::select! {
        _ = cancel.cancelled() => return Err(RefreshError::Cancelled),
        response = request => response.map_err(RefreshError::Http)?,
    };
    let status = response.status();
    if !status.is_success() {
        return Err(RefreshError::Status(status));
    }
    let bytes = tokio::select! {
        _ = cancel.cancelled() => return Err(RefreshError::Cancelled),
        bytes = response.bytes() => bytes.map_err(RefreshError::Http)?,
    };
    serde_json::from_slice(&bytes).map_err(RefreshError::Parse)
}

fn refresh_lag_seconds(record: &UpstreamRecord, aead: &AeadService, now: u64) -> Option<f64> {
    if record.kind != UpstreamKind::AnthropicOauth
        || !record.enabled
        || record.deleted_at_unix_secs.is_some()
    {
        return None;
    }
    let encrypted = record.oauth_credentials.as_ref()?;
    let bundle = encrypted.decrypt(aead, record.id.as_bytes()).ok()?;
    let window_opened = bundle.expires_at_unix_secs.saturating_sub(LOOKAHEAD_SECS);
    Some(now.saturating_sub(window_opened) as f64)
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

fn refresh_outcome(error: &RefreshError) -> OAuthRefreshOutcome {
    match error {
        RefreshError::Status(_) => OAuthRefreshOutcome::HttpError,
        RefreshError::Parse(_) => OAuthRefreshOutcome::Parse,
        RefreshError::Http(_)
        | RefreshError::Cancelled
        | RefreshError::MissingCredentials
        | RefreshError::Decrypt
        | RefreshError::Encrypt
        | RefreshError::Storage(_) => OAuthRefreshOutcome::Network,
    }
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
