use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use cc_lb_storage_api::{OAuthCredentials, StorageError};
use dashmap::DashMap;
use http::StatusCode;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use thiserror::Error;
use url::Url;

use crate::http_client::{OAuthHttpClient, OAuthTokenRequest};

pub const REFRESH_BUFFER_SECS: u64 = 60;
pub const BREAKER_FAILURE_THRESHOLD: u32 = 3;

pub type BreakerMap = Arc<DashMap<String, Arc<CircuitBreakerState>>>;

#[derive(Debug, Default)]
pub struct CircuitBreakerState {
    consecutive_failures: AtomicU32,
}

impl CircuitBreakerState {
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }

    pub fn is_open(&self) -> bool {
        self.consecutive_failures() >= BREAKER_FAILURE_THRESHOLD
    }

    pub fn record_failure(&self) -> u32 {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn reset(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Error)]
pub enum RefreshError {
    #[error("oauth credentials are missing")]
    MissingCredentials,
    #[error("oauth refresh circuit breaker is open")]
    CircuitOpen,
    #[error("oauth token endpoint returned status {status}")]
    TokenEndpoint { status: StatusCode },
    #[error("oauth token endpoint request failed: {reason}")]
    Http { reason: String },
    #[error("oauth token response json failed: {reason}")]
    Json { reason: String },
    #[error("oauth storage operation failed: {source}")]
    Storage {
        #[from]
        source: StorageError,
    },
}

#[derive(Debug, Deserialize)]
struct TokenEndpointJson {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: u64,
    scope: Option<String>,
}

pub fn new_breaker_map() -> BreakerMap {
    Arc::new(DashMap::new())
}

pub fn breaker_for(
    breakers: &BreakerMap,
    principal_id: &str,
    provider: &str,
) -> Arc<CircuitBreakerState> {
    breakers
        .entry(crate::single_flight::single_flight_key(
            principal_id,
            provider,
        ))
        .or_insert_with(|| Arc::new(CircuitBreakerState::default()))
        .clone()
}

pub fn is_expiring(creds: &OAuthCredentials, now_epoch_secs: u64) -> bool {
    now_epoch_secs >= creds.expires_at.saturating_sub(REFRESH_BUFFER_SECS)
}

pub async fn refresh_credentials(
    http: &dyn OAuthHttpClient,
    token_url: &Url,
    client_id: &str,
    existing: &OAuthCredentials,
    now_epoch_secs: u64,
) -> Result<OAuthCredentials, RefreshError> {
    let body = form_body(client_id, &existing.refresh_token, None);
    let response = http
        .post_token(OAuthTokenRequest {
            endpoint: token_url.clone(),
            form_body: body,
        })
        .await
        .map_err(|source| RefreshError::Http {
            reason: source.to_string(),
        })?;

    if !response.status.is_success() {
        return Err(RefreshError::TokenEndpoint {
            status: response.status,
        });
    }

    parse_token_response(response.body, existing, now_epoch_secs)
}

pub async fn exchange_pkce_code(
    http: &dyn OAuthHttpClient,
    token_url: &Url,
    client_id: &str,
    auth_code: &str,
    code_verifier: &SecretString,
    redirect_uri: &Url,
    now_epoch_secs: u64,
) -> Result<OAuthCredentials, RefreshError> {
    let body = form_body(
        client_id,
        code_verifier.expose_secret(),
        Some((auth_code, redirect_uri)),
    );
    let response = http
        .post_token(OAuthTokenRequest {
            endpoint: token_url.clone(),
            form_body: body,
        })
        .await
        .map_err(|source| RefreshError::Http {
            reason: source.to_string(),
        })?;

    if !response.status.is_success() {
        return Err(RefreshError::TokenEndpoint {
            status: response.status,
        });
    }

    let placeholder = OAuthCredentials {
        access_token: String::new(),
        refresh_token: String::new(),
        expires_at: now_epoch_secs,
        scopes: Vec::new(),
    };
    parse_token_response(response.body, &placeholder, now_epoch_secs)
}

pub fn increment_refresh_metric(principal: &str, provider: &str, outcome: &'static str) {
    metrics::counter!(
        "cc_lb_oauth_refresh_total",
        "principal" => principal.to_owned(),
        "provider" => provider.to_owned(),
        "outcome" => outcome
    )
    .increment(1);
}

fn parse_token_response(
    body: Bytes,
    existing: &OAuthCredentials,
    now_epoch_secs: u64,
) -> Result<OAuthCredentials, RefreshError> {
    let parsed: TokenEndpointJson =
        serde_json::from_slice(&body).map_err(|source| RefreshError::Json {
            reason: source.to_string(),
        })?;
    let scopes = parsed
        .scope
        .map(|scope| scope.split_whitespace().map(str::to_owned).collect())
        .unwrap_or_else(|| existing.scopes.clone());

    Ok(OAuthCredentials {
        access_token: parsed.access_token,
        refresh_token: parsed
            .refresh_token
            .unwrap_or_else(|| existing.refresh_token.clone()),
        expires_at: now_epoch_secs.saturating_add(parsed.expires_in),
        scopes,
    })
}

fn form_body(client_id: &str, token: &str, pkce: Option<(&str, &Url)>) -> SecretString {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    match pkce {
        Some((auth_code, redirect_uri)) => {
            serializer.append_pair("grant_type", "authorization_code");
            serializer.append_pair("code", auth_code);
            serializer.append_pair("code_verifier", token);
            serializer.append_pair("redirect_uri", redirect_uri.as_str());
        }
        None => {
            serializer.append_pair("grant_type", "refresh_token");
            serializer.append_pair("refresh_token", token);
        }
    }
    serializer.append_pair("client_id", client_id);
    SecretString::new(serializer.finish().into_boxed_str())
}
