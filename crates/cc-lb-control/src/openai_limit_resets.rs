//! Banked Codex rate-limit reset credits, using the provider's real protocol.
//!
//! Protocol: <https://github.com/openai/codex/blob/main/codex-rs/backend-client/src/client/rate_limit_resets.rs>
//! and its `types.rs`. Inventory is account-bound; `available_count` need not
//! equal the length of the provider's capped credit list. Unknown reset types
//! and statuses remain raw provider values, not inferred eligibility.
//!
//! Consume is dispatched once, with an explicitly selected credit. Transport,
//! timeout, cancellation, malformed success, and server errors can all leave
//! its outcome uncertain. A successful reset still requires a fresh usage
//! observation before callers can consider routing quota state updated.

use std::time::Duration;

use bytes::Bytes;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use http::{HeaderValue, Request};
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::anthropic_metadata::MetadataHttpClient;

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const CONSUME_TIMEOUT: Duration = Duration::from_secs(25);
const MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;
const EPOCH_FENCE_PREFIX: &str = "pending:";

/// Recovery is deliberately longer than a consume and subsequent usage refresh.
pub const OPENAI_LIMIT_RESET_FENCE_RECOVERY_AFTER_MILLIS: u64 = 5 * 60 * 1_000;

/// MetaStore inventory key; no separate database table is needed.
pub fn openai_limit_reset_meta_key(upstream_id: Uuid) -> String {
    format!("openai_limit_reset:{upstream_id}")
}

/// Compare-and-put epoch key shared by inventory polling and consumption.
pub fn openai_limit_reset_epoch_meta_key(upstream_id: Uuid) -> String {
    format!("openai_limit_reset_epoch:{upstream_id}")
}

/// Fence consumption before dispatch, then settle only the owning attempt's fence.
pub fn openai_limit_reset_epoch_fence(attempt: Uuid, started_at_unix_millis: u64) -> String {
    format!("{EPOCH_FENCE_PREFIX}{attempt}:{started_at_unix_millis}")
}

/// Only well-formed pending fences may be recovered after the recovery interval.
pub fn openai_limit_reset_fence_started_at_millis(raw: &str) -> Option<u64> {
    let (attempt, started) = raw.strip_prefix(EPOCH_FENCE_PREFIX)?.split_once(':')?;
    Uuid::parse_str(attempt).ok()?;
    started.parse().ok()
}

/// `Some(None)` is an initial epoch, `Some(Some(_))` settled, and `None`
/// pending or malformed. Callers must fail closed on pending/malformed epochs.
pub fn settled_openai_limit_reset_epoch(raw: Option<&str>) -> Option<Option<Uuid>> {
    match raw {
        None => Some(None),
        Some(value) => Uuid::parse_str(value).ok().map(Some),
    }
}

/// A snapshot is servable only for this credential/account and a matching
/// settled epoch. Read the epoch before dispatching the inventory request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAiLimitResetPollRecord {
    pub credential_fingerprint: u64,
    pub oauth_token_generation: u64,
    pub account_id: String,
    pub epoch: Option<Uuid>,
    pub observed_at_unix_millis: u64,
    /// `None` explicitly invalidates previously observed inventory on failure.
    pub inventory: Option<OpenAiLimitResetInventory>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenAiLimitResetInventory {
    /// Required even when empty; missing/malformed inventory is not zero credits.
    pub credits: Vec<OpenAiLimitResetCredit>,
    /// Provider total; the returned list may be capped.
    pub available_count: i64,
    /// Preserve provider applicability when present; missing means unknown.
    pub applicable_available_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenAiLimitResetCredit {
    pub id: String,
    pub reset_type: String,
    /// Provider eligibility for this plan. Missing means unknown, not permission.
    pub is_supported_by_plan: Option<bool>,
    pub status: String,
    pub granted_at: String,
    pub expires_at: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
}

/// Borrowed request identity. Deliberately not `Debug`: contains a bearer token.
#[derive(Clone, Copy)]
pub struct OpenAiLimitResetAuth<'a> {
    pub access_token: &'a str,
    pub account_id: &'a str,
    pub user_agent: &'a str,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct OpenAiLimitResetConsumeRequest<'a> {
    pub redeem_request_id: &'a str,
    /// Required selection: never substitute a different credit or omit this ID.
    pub credit_id: &'a str,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OpenAiLimitResetConsumeOutcome {
    Reset,
    NothingToReset,
    NoCredit,
    AlreadyRedeemed,
}

/// The raw snake-case provider code, not an invented success/rejection boolean.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenAiLimitResetConsumeResult {
    pub code: OpenAiLimitResetConsumeOutcome,
    pub windows_reset: Option<i64>,
}

#[derive(Debug, Error)]
pub enum OpenAiLimitResetError {
    /// DNS/connect/TLS failure: the request did not reach the provider.
    #[error("provider connect failed: {0}")]
    Connect(String),
    #[error("provider transport failed: {0}")]
    Transport(String),
    #[error("provider request timed out")]
    Timeout,
    #[error("provider request cancelled")]
    Cancelled,
    #[error("provider returned status {0}")]
    ProviderStatus(u16),
    /// Includes unknown consume codes: never report those as definite rejection.
    #[error("provider response malformed: {0}")]
    Parse(String),
    #[error("provider request build failed: {0}")]
    Request(String),
}

impl OpenAiLimitResetError {
    /// A 4xx rejects the request; 5xx and unreadable/lost replies may have
    /// consumed the credit. Never retry consumption based on this distinction.
    pub fn consume_outcome_is_uncertain(&self) -> bool {
        match self {
            Self::Connect(_) | Self::Request(_) => false,
            Self::ProviderStatus(status) => !(400..500).contains(status),
            Self::Transport(_) | Self::Timeout | Self::Cancelled | Self::Parse(_) => true,
        }
    }
}

/// Uses the same base URL semantics as the scheduler's Codex usage request:
/// configured `/backend-api/codex` becomes `/backend-api/wham`, while custom
/// Codex API gateways retain their prefix and use `/api/codex`.
pub fn openai_limit_reset_credits_url(base_url: &Url) -> Url {
    let mut url = base_url.clone();
    let base_path = base_url.path().trim_end_matches('/');
    let backend_path = base_path.strip_suffix("/codex").unwrap_or(base_path);
    let path = if backend_path.contains("/backend-api") {
        format!("{backend_path}/wham/rate-limit-reset-credits")
    } else if matches!(url.host_str(), Some("chatgpt.com" | "chat.openai.com")) {
        format!("{backend_path}/backend-api/wham/rate-limit-reset-credits")
    } else {
        format!("{base_path}/api/codex/rate-limit-reset-credits")
    };
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    url
}

pub async fn list_openai_limit_reset_credits(
    client: &MetadataHttpClient,
    base_url: &Url,
    auth: OpenAiLimitResetAuth<'_>,
    cancel: &CancellationToken,
) -> Result<OpenAiLimitResetInventory, OpenAiLimitResetError> {
    let url = openai_limit_reset_credits_url(base_url);
    let body = send_request(
        client,
        Request::get(url.as_str()),
        auth,
        Bytes::new(),
        FETCH_TIMEOUT,
        cancel,
    )
    .await?;
    serde_json::from_slice(&body).map_err(|error| OpenAiLimitResetError::Parse(error.to_string()))
}

/// Dispatches exactly once. A reset response confirms the provider operation,
/// not that the caller has refreshed its usage or routing cache.
pub async fn consume_openai_limit_reset_credit(
    client: &MetadataHttpClient,
    base_url: &Url,
    auth: OpenAiLimitResetAuth<'_>,
    request: OpenAiLimitResetConsumeRequest<'_>,
    cancel: &CancellationToken,
) -> Result<OpenAiLimitResetConsumeResult, OpenAiLimitResetError> {
    if request.credit_id.is_empty() || request.redeem_request_id.is_empty() {
        return Err(OpenAiLimitResetError::Request(
            "credit_id and redeem_request_id are required".to_owned(),
        ));
    }
    let mut url = openai_limit_reset_credits_url(base_url);
    url.set_path(&format!("{}/consume", url.path()));
    let body = serde_json::to_vec(&request)
        .map_err(|error| OpenAiLimitResetError::Request(error.to_string()))?;
    let body = send_request(
        client,
        Request::post(url.as_str()).header(CONTENT_TYPE, "application/json"),
        auth,
        Bytes::from(body),
        CONSUME_TIMEOUT,
        cancel,
    )
    .await?;
    serde_json::from_slice(&body).map_err(|error| OpenAiLimitResetError::Parse(error.to_string()))
}

async fn send_request(
    client: &MetadataHttpClient,
    builder: http::request::Builder,
    auth: OpenAiLimitResetAuth<'_>,
    body: Bytes,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<Bytes, OpenAiLimitResetError> {
    if auth.account_id.is_empty() {
        return Err(OpenAiLimitResetError::Request(
            "bound account_id is required".to_owned(),
        ));
    }
    let bearer = HeaderValue::from_str(&format!("Bearer {}", auth.access_token))
        .map_err(|error| OpenAiLimitResetError::Request(error.to_string()))?;
    let request = builder
        .header(AUTHORIZATION, bearer)
        .header(USER_AGENT, auth.user_agent)
        .header("originator", "codex_cli_rs")
        .header("ChatGPT-Account-Id", auth.account_id)
        .header(ACCEPT, "application/json")
        .body(Full::new(body))
        .map_err(|error| OpenAiLimitResetError::Request(error.to_string()))?;

    // One deadline bounds headers and body together; no retry, including for POST.
    tokio::select! {
        _ = cancel.cancelled() => Err(OpenAiLimitResetError::Cancelled),
        result = tokio::time::timeout(timeout, exchange(client, request)) => {
            result.map_err(|_| OpenAiLimitResetError::Timeout)?
        }
    }
}

async fn exchange(
    client: &MetadataHttpClient,
    request: Request<Full<Bytes>>,
) -> Result<Bytes, OpenAiLimitResetError> {
    let response = client.request(request).await.map_err(|error| {
        if error.is_connect() {
            OpenAiLimitResetError::Connect(error.to_string())
        } else {
            OpenAiLimitResetError::Transport(error.to_string())
        }
    })?;
    let status = response.status();
    if !status.is_success() {
        return Err(OpenAiLimitResetError::ProviderStatus(status.as_u16()));
    }
    Limited::new(response.into_body(), MAX_RESPONSE_BODY_BYTES)
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .map_err(|error| {
            if error.downcast_ref::<LengthLimitError>().is_some() {
                OpenAiLimitResetError::Parse("provider response exceeded size limit".to_owned())
            } else {
                OpenAiLimitResetError::Transport(error.to_string())
            }
        })
}
