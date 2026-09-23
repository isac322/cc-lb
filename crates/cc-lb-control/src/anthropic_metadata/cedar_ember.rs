//! Cedar Ember (subscription limit-reset) provider calls.
//!
//! Anthropic exposes the reset program through the OAuth API surface:
//! `GET /api/oauth/usage?cedar_ember=1&skip_spend=1` carries the grant status
//! block and `POST /api/organizations/{org}/reset_rate_limits` consumes a
//! grant. All calls are keyed off the upstream's configured `base_url` so the
//! same code path serves production (`https://api.anthropic.com`) and any
//! operator-configured gateway.
//!
//! The claim POST is deliberately not retried: a timeout or mid-flight
//! transport failure leaves the outcome indeterminate (the provider may have
//! consumed the grant), so callers must surface "unknown" rather than assume
//! failure. Only a connect failure — the request never left this process —
//! is reported as a definite non-delivery.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use http::{HeaderValue, Request};
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::fetchers::{MetadataHttpClient, ProfileResponse};

const PROFILE_PATH: &str = "api/oauth/profile";
const CEDAR_EMBER_USAGE_PATH: &str = "api/oauth/usage?cedar_ember=1&skip_spend=1";
const RESET_RATE_LIMITS_PATH_PREFIX: &str = "api/organizations/";
const RESET_RATE_LIMITS_PATH_SUFFIX: &str = "/reset_rate_limits";
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
/// The observed client allows 25s for the claim POST; keep the same bound.
const CLAIM_TIMEOUT: Duration = Duration::from_secs(25);
const CLAIM_PROGRAM: &str = "cedar_ember";
/// Provider payloads are small JSON documents; a hard cap keeps a
/// pathological response from buffering unboundedly.
const MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;

/// Provider-side grant id shape (`^[a-z0-9_-]{1,40}$`); enforced before any
/// claim is dispatched so malformed ids never reach the provider.
pub fn is_valid_grant_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 40
        && value
            .bytes()
            .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'))
}

#[derive(Debug, Error)]
pub enum CedarEmberError {
    /// The request never reached the provider (connect refused, DNS, TLS
    /// handshake). For a claim this is a definite non-delivery.
    #[error("provider connect failed: {0}")]
    Connect(String),
    /// The request may have reached the provider but the response was lost.
    /// For a claim the outcome is indeterminate.
    #[error("provider transport failed: {0}")]
    Transport(String),
    /// No response within the timeout; for a claim the outcome is
    /// indeterminate.
    #[error("provider request timed out")]
    Timeout,
    #[error("provider request cancelled")]
    Cancelled,
    /// The provider answered with a non-success status. For a claim only a
    /// 4xx is a definite rejection; a 5xx may still have consumed the grant
    /// before the response was lost, so callers must treat it as unknown.
    #[error("provider returned status {0}")]
    ProviderStatus(u16),
    /// The provider answered 2xx but the body did not match the contract.
    #[error("provider response malformed: {0}")]
    Parse(String),
    /// The request could not be built locally (bad base URL, header value).
    #[error("provider request build failed: {0}")]
    Request(String),
}

/// `cedar_ember` block of the usage response. `grants` retains the provider's
/// own window names (`five_hour`, `seven_day`, …); callers map them to the
/// cc-lb short names at the API boundary.
#[derive(Debug, Clone, Deserialize)]
pub struct CedarEmberStatus {
    pub eligible: bool,
    #[serde(default)]
    pub ineligible_reason: Option<String>,
    #[serde(default)]
    pub at_limit: Option<bool>,
    #[serde(default)]
    pub exhausted: Option<Vec<String>>,
    /// Required when the block is present: a missing/null array is malformed,
    /// not an empty grant list.
    pub grants: Vec<CedarEmberGrant>,
    #[serde(default)]
    pub next_grant_id: Option<String>,
    #[serde(default)]
    pub weekly_resets_at: Option<String>,
    #[serde(default)]
    pub cooldown_until: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CedarEmberGrant {
    pub id: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub resets_total: Option<u32>,
    /// Consumption is provider-owned; a missing value fails the parse rather
    /// than fabricating a count.
    pub resets_left: u32,
    #[serde(default)]
    pub starts_at: Option<String>,
    #[serde(default)]
    pub ends_at: Option<String>,
    #[serde(default)]
    pub clears: Option<Vec<String>>,
    #[serde(default)]
    pub paused: Option<bool>,
    #[serde(default)]
    pub usable_now: Option<bool>,
    #[serde(default)]
    pub use_requires_limit: Option<bool>,
    #[serde(default)]
    pub percent_used: Option<BTreeMap<String, Value>>,
    #[serde(default)]
    pub blocking: Option<Vec<String>>,
}

/// Provider answer to `reset_rate_limits`. The result vocabulary is
/// normalized to the known outcomes; anything unexpected becomes `Unknown`
/// so callers never present an arbitrary provider string as a definite
/// rejection.
#[derive(Debug, Clone, Deserialize)]
pub struct CedarEmberClaimResult {
    pub result: CedarEmberClaimOutcome,
    #[serde(default, deserialize_with = "sanitize_reason")]
    pub reason: Option<String>,
    #[serde(default)]
    pub resets_left: Option<u32>,
    #[serde(default)]
    pub cleared: Option<Vec<String>>,
    #[serde(default)]
    pub weekly_resets_at: Option<String>,
    #[serde(default)]
    pub cooldown_until: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CedarEmberClaimOutcome {
    Reset,
    AlreadyUsed,
    NotLimited,
    Cooldown,
    Ineligible,
    Unavailable,
    #[serde(other)]
    Unknown,
}

/// Provider-side reason vocabulary (`zn` in the observed client). Unknown
/// strings collapse to "unknown" rather than leaking arbitrary text.
const KNOWN_CLAIM_REASONS: &[&str] = &[
    "config_off",
    "tier",
    "seat",
    "mobile",
    "surface",
    "cli_version",
    "no_grant",
    "tenure",
    "other_experiment",
    "unavailable",
    "unknown",
    "paused",
    "expired",
    "unknown_grant",
    "not_next_grant",
    "grant_id_required",
    "not_limited",
    "already_used",
    "cooldown",
    "stamp_indeterminate",
    "reset_unconfirmed",
];

fn sanitize_reason<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match Option::<String>::deserialize(deserializer)? {
        Some(reason) if KNOWN_CLAIM_REASONS.contains(&reason.as_str()) => Some(reason),
        Some(_) => Some("unknown".to_owned()),
        None => None,
    })
}

#[derive(Deserialize)]
struct CedarEmberUsageEnvelope {
    #[serde(default)]
    cedar_ember: Option<CedarEmberStatus>,
}

/// Live profile fetch keyed off the upstream's base URL. The claim flow needs
/// the *current* OAuth identity (account/org can change on reauthorization),
/// so this never reads the cached metadata records.
pub async fn fetch_oauth_profile_at(
    client: &MetadataHttpClient,
    base_url: &Url,
    access_token: &str,
    user_agent: &str,
    cancel: &CancellationToken,
) -> Result<ProfileResponse, CedarEmberError> {
    let url = join_url(base_url, PROFILE_PATH)?;
    let body = send_request(
        client,
        Request::get(url.as_str()),
        access_token,
        user_agent,
        Full::new(Bytes::new()),
        FETCH_TIMEOUT,
        cancel,
    )
    .await?;
    serde_json::from_slice(&body).map_err(|error| CedarEmberError::Parse(error.to_string()))
}

/// Returns the `cedar_ember` status block, or `None` when the provider omits
/// it (account not enrolled). A present-but-malformed block is an error.
pub async fn fetch_cedar_ember_status(
    client: &MetadataHttpClient,
    base_url: &Url,
    access_token: &str,
    user_agent: &str,
    cancel: &CancellationToken,
) -> Result<Option<CedarEmberStatus>, CedarEmberError> {
    let url = join_url(base_url, CEDAR_EMBER_USAGE_PATH)?;
    let body = send_request(
        client,
        Request::get(url.as_str()),
        access_token,
        user_agent,
        Full::new(Bytes::new()),
        FETCH_TIMEOUT,
        cancel,
    )
    .await?;
    let envelope: CedarEmberUsageEnvelope =
        serde_json::from_slice(&body).map_err(|error| CedarEmberError::Parse(error.to_string()))?;
    Ok(envelope.cedar_ember)
}

/// Dispatches the claim exactly once. `grant_id` is the caller's choice —
/// forwarded verbatim, never substituted with `next_grant_id`.
pub async fn claim_cedar_ember_reset(
    client: &MetadataHttpClient,
    base_url: &Url,
    access_token: &str,
    user_agent: &str,
    org_uuid: &str,
    grant_id: &str,
    request_id: &str,
    cancel: &CancellationToken,
) -> Result<CedarEmberClaimResult, CedarEmberError> {
    let url = join_url(
        base_url,
        &format!("{RESET_RATE_LIMITS_PATH_PREFIX}{org_uuid}{RESET_RATE_LIMITS_PATH_SUFFIX}"),
    )?;
    let body = serde_json::to_vec(&json!({
        "program": CLAIM_PROGRAM,
        "grant_id": grant_id,
        "request_id": request_id,
    }))
    .map_err(|error| CedarEmberError::Request(error.to_string()))?;
    let body = send_request(
        client,
        Request::post(url.as_str()).header(CONTENT_TYPE, "application/json"),
        access_token,
        user_agent,
        Full::new(Bytes::from(body)),
        CLAIM_TIMEOUT,
        cancel,
    )
    .await?;
    serde_json::from_slice(&body).map_err(|error| CedarEmberError::Parse(error.to_string()))
}

fn join_url(base_url: &Url, path: &str) -> Result<Url, CedarEmberError> {
    base_url
        .join(path)
        .map_err(|error| CedarEmberError::Request(format!("invalid provider base URL: {error}")))
}

fn bearer_header(access_token: &str) -> Result<HeaderValue, CedarEmberError> {
    HeaderValue::from_str(&format!("Bearer {access_token}"))
        .map_err(|error| CedarEmberError::Request(error.to_string()))
}

async fn send_request(
    client: &MetadataHttpClient,
    builder: http::request::Builder,
    access_token: &str,
    user_agent: &str,
    body: Full<Bytes>,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<Bytes, CedarEmberError> {
    let request = builder
        .header(AUTHORIZATION, bearer_header(access_token)?)
        .header(
            USER_AGENT,
            HeaderValue::from_str(user_agent)
                .map_err(|error| CedarEmberError::Request(error.to_string()))?,
        )
        .header(ACCEPT, "application/json")
        // OAuth-API calls carry the same beta header as the warmup path.
        .header("anthropic-beta", "oauth-2025-04-20")
        .body(body)
        .map_err(|error| CedarEmberError::Request(error.to_string()))?;

    // One deadline covers headers AND body: two sequential timeouts would
    // silently double the worst-case claim latency.
    tokio::select! {
        _ = cancel.cancelled() => Err(CedarEmberError::Cancelled),
        result = tokio::time::timeout(timeout, exchange(client, request)) => {
            result.map_err(|_| CedarEmberError::Timeout)?
        }
    }
}

async fn exchange(
    client: &MetadataHttpClient,
    request: Request<Full<Bytes>>,
) -> Result<Bytes, CedarEmberError> {
    let response = client.request(request).await.map_err(|error| {
        if error.is_connect() {
            CedarEmberError::Connect(error.to_string())
        } else {
            CedarEmberError::Transport(error.to_string())
        }
    })?;
    let status = response.status();
    if !status.is_success() {
        return Err(CedarEmberError::ProviderStatus(status.as_u16()));
    }
    Limited::new(response.into_body(), MAX_RESPONSE_BODY_BYTES)
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .map_err(|error| {
            if error.downcast_ref::<LengthLimitError>().is_some() {
                CedarEmberError::Parse("provider response exceeded size limit".to_owned())
            } else {
                CedarEmberError::Transport(error.to_string())
            }
        })
}
