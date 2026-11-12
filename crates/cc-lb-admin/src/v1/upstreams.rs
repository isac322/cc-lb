use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{ETAG, IF_MATCH},
    },
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use cc_lb_aead::{AeadEncryptedField, OAuthTokenBundle};
use cc_lb_clock::Clock;
use cc_lb_control::AuditPayload;
use cc_lb_control::anthropic_api_key::{RemediationErrorBody, RemediationErrorInner};
use cc_lb_control::anthropic_compat::{
    CLAUDE_CODE_STABLE_VERSION_FALLBACK, CLAUDE_CODE_STABLE_VERSION_KEY, claude_code_user_agent,
};
use cc_lb_control::anthropic_metadata::make_metadata_http_client;
use cc_lb_control::subscription_metadata_hook::{
    fetch_metadata_only, store_organization_metadata, store_subscription_metadata,
};
use cc_lb_oauth_protocol::{OAuthAccountIdentity, account_id_from_access_token};
use cc_lb_quota::{
    UnifiedQuotaObservation, build_subscription_quota_samples, parse_anthropic_unified_headers,
};
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerPushTask};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamWarmupDialectPlugin};
use cc_lb_storage_api::{
    OAuthTokensCreate, OrganizationMetadataRecord, RemediationCode, Storage, StorageError,
    SubscriptionQuotaSample, SubscriptionQuotaWindow, UpstreamApiKeyDetailsRecord,
    UpstreamApiKeyStore, UpstreamCreate, UpstreamRecord, UpstreamStatusUpdate, UpstreamStore,
    UpstreamSubscriptionMetadataRecord, UpstreamUpdate, WarmupAttemptOutcome, WarmupDispatchKind,
    WarmupLane, WarmupPermanentFailureReason, WarmupSkipReason, WarmupTransientFailureReason,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::add_dynamic_rebind_headers;
use super::api_key::access::{ApiKeyAccessResponse, api_key_access_response};
use super::api_key::poll_enqueue::enqueue_usage_poll;
use super::api_key::preflight::{
    ApiKeyUpdateRequest, KeyCheck, PREFLIGHT_ROUTE, no_store, prepare_api_key_create,
    prepare_api_key_update, validate_base_url_input,
};
use super::post_commit::{PostCommitAborted, complete_after_commit, rebuild_after_audit_failure};
use crate::ports::{WarmupAttemptInput, WarmupAttemptResult};
use crate::{
    AdminState, WarmupDialectDispatchErrorKind, WarmupNativeDispatchOutcome,
    audit::{AdminAuditEvent, record_admin_audit},
    auth::AdminIdentity,
};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const STORE_PAGE_LIMIT: usize = 1000;
const METADATA_REFRESH_LOOKAHEAD_SECS: u64 = 60;
const METADATA_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);
const FIRE_NOW_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com/";
const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/";
const DEFAULT_GEMINI_OAUTH_BASE_URL: &str = "https://daily-cloudcode-pa.googleapis.com/";
const DEFAULT_GEMINI_BASE_URL: &str = "https://generativelanguage.googleapis.com/";

fn prepare_imported_oauth_bundle(
    kind: UpstreamKind,
    bundle: Option<OAuthTokenBundle>,
    request_project_id: Option<&str>,
    now_unix_secs: u64,
) -> Result<Option<OAuthTokenBundle>, UpstreamError> {
    let Some(mut bundle) = bundle else {
        if kind == UpstreamKind::GeminiOauth && request_project_id.is_some() {
            return Err(UpstreamError::BadRequest {
                error: "missing_oauth_tokens",
                detail: "Gemini OAuth requires oauth_tokens".to_owned(),
            });
        }
        if kind == UpstreamKind::OpenaiOauth && request_project_id.is_some() {
            return Err(UpstreamError::BadRequest {
                error: "unexpected_project_id",
                detail: "OpenAI OAuth does not accept project_id".to_owned(),
            });
        }
        if matches!(kind, UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth) {
            return Err(UpstreamError::BadRequest {
                error: "missing_oauth_tokens",
                detail: if kind == UpstreamKind::OpenaiOauth {
                    "OpenAI OAuth requires oauth_tokens".to_owned()
                } else {
                    "Gemini OAuth requires oauth_tokens".to_owned()
                },
            });
        }
        return Ok(None);
    };

    if bundle.access_token.trim().is_empty() {
        return Err(UpstreamError::BadRequest {
            error: "invalid_oauth_tokens",
            detail: "access_token must be non-empty".to_owned(),
        });
    }
    let access_only = matches!(kind, UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth)
        && bundle.never_refresh;
    if access_only {
        if !bundle.refresh_token.is_empty() || bundle.refresh_token_expires_at_unix_secs.is_some() {
            return Err(UpstreamError::BadRequest {
                error: "invalid_oauth_tokens",
                detail: "access-only OAuth imports must not include a refresh token or refresh-token expiry"
                    .to_owned(),
            });
        }
    } else {
        if bundle.refresh_token.trim().is_empty() {
            return Err(UpstreamError::BadRequest {
                error: "invalid_oauth_tokens",
                detail: "refresh_token must be non-empty".to_owned(),
            });
        }
        if bundle
            .refresh_token_expires_at_unix_secs
            .is_some_and(|deadline| deadline <= now_unix_secs)
        {
            return Err(UpstreamError::BadRequest {
                error: "refresh_token_expired",
                detail: "refresh_token_expires_at_unix_secs must be in the future".to_owned(),
            });
        }
    }

    match kind {
        UpstreamKind::OpenaiOauth => {
            if request_project_id.is_some() || bundle.project_id.is_some() {
                return Err(UpstreamError::BadRequest {
                    error: "unexpected_project_id",
                    detail: "OpenAI OAuth does not accept project_id".to_owned(),
                });
            }
            if bundle
                .account_id
                .as_deref()
                .is_none_or(|account_id| account_id.trim().is_empty())
            {
                bundle.account_id = account_id_from_access_token(&bundle.access_token);
            }
        }
        UpstreamKind::GeminiOauth => {
            let request_project_id_was_present = request_project_id.is_some();
            let request_project_id = request_project_id
                .map(str::trim)
                .filter(|id| !id.is_empty());
            if request_project_id_was_present && request_project_id.is_none() {
                return Err(UpstreamError::BadRequest {
                    error: "missing_project_id",
                    detail: "Gemini OAuth project_id must be non-empty".to_owned(),
                });
            }
            let bundle_project_id = bundle
                .project_id
                .as_deref()
                .map(str::trim)
                .filter(|id| !id.is_empty());
            if bundle.project_id.is_some() && bundle_project_id.is_none() {
                return Err(UpstreamError::BadRequest {
                    error: "missing_project_id",
                    detail: "Gemini OAuth project_id must be non-empty".to_owned(),
                });
            }
            if let (Some(requested), Some(from_bundle)) = (request_project_id, bundle_project_id)
                && requested != from_bundle
            {
                return Err(UpstreamError::BadRequest {
                    error: "project_id_mismatch",
                    detail: "request project_id does not match oauth_tokens.project_id".to_owned(),
                });
            }
            let project_id = request_project_id.or(bundle_project_id).ok_or_else(|| {
                UpstreamError::BadRequest {
                    error: "missing_project_id",
                    detail: "Gemini OAuth requires a non-empty project_id".to_owned(),
                }
            })?;
            bundle.project_id = Some(project_id.to_owned());
        }
        _ => {
            return Err(UpstreamError::BadRequest {
                error: "unexpected_oauth_tokens",
                detail: "OAuth token import is supported only for OpenAI and Gemini OAuth"
                    .to_owned(),
            });
        }
    }
    Ok(Some(bundle))
}

async fn write_warmup_status_after_success(
    storage: &dyn Storage,
    upstream_id: Uuid,
    clock: &dyn Clock,
) {
    let status = UpstreamStatusUpdate {
        last_warmup_at_unix_secs: Some(Some(cc_lb_clock::unix_secs(clock.now()))),
        ..UpstreamStatusUpdate::default()
    };
    if let Err(error) = UpstreamStore::set_status(storage, upstream_id, status).await {
        tracing::warn!(target: "warmup", upstream_id = %upstream_id, %error, "fire_now writeback failed");
    }
}

pub fn router() -> Router<AdminState> {
    Router::new()
        .route(
            "/admin/v1/upstreams",
            post(create_upstream).get(list_upstreams),
        )
        .route(
            PREFLIGHT_ROUTE,
            post(super::api_key::preflight::preflight_candidate),
        )
        .route(
            "/admin/v1/upstreams/limit-resets",
            get(super::limit_resets::list_upstream_limit_resets),
        )
        .route(
            "/admin/v1/upstreams/{id}",
            get(get_upstream)
                .put(update_upstream)
                .patch(update_upstream)
                .delete(delete_upstream),
        )
        .route(
            "/admin/v1/upstreams/{id}/subscription-metadata",
            get(get_upstream_subscription_metadata),
        )
        .route(
            "/admin/v1/upstreams/{id}/subscription-metadata/refresh",
            post(refresh_upstream_subscription_metadata),
        )
        .route(
            "/admin/v1/upstreams/{id}/warmup/fire-now",
            post(fire_now_upstream_warmup),
        )
        .route(
            "/admin/v1/upstreams/{id}/limit-resets",
            get(super::limit_resets::get_upstream_limit_resets),
        )
        .route(
            "/admin/v1/upstreams/{id}/limit-resets/history",
            get(super::limit_resets::get_upstream_limit_reset_history),
        )
        .route(
            "/admin/v1/upstreams/{id}/limit-resets/claim",
            post(super::limit_resets::claim_upstream_limit_reset),
        )
        .route(
            "/admin/v1/upstreams/{id}/warmup",
            get(super::upstream_warmup::get_upstream_warmup),
        )
        .route(
            "/admin/v1/upstreams/{id}/warmup/attempts",
            get(super::upstream_warmup::list_upstream_warmup_attempts),
        )
        .route(
            "/admin/v1/upstreams/{id}/warmup-dialect-plugin",
            delete(delete_upstream_warmup_dialect_plugin),
        )
        .route("/admin/v1/upstreams/{id}/enable", post(enable_upstream))
        .route("/admin/v1/upstreams/{id}/disable", post(disable_upstream))
        .route(
            "/admin/v1/upstreams/{id}/preflight",
            post(super::api_key::recheck::recheck_upstream_api_key),
        )
        .route(
            "/admin/v1/upstreams/{id}/workspaces",
            get(super::api_key::workspaces::list_upstream_workspaces),
        )
        .route(
            "/admin/v1/upstreams/{id}/usage",
            get(super::api_key::usage::get_upstream_usage),
        )
        .merge(super::api_key::reporting::router())
}

#[derive(Deserialize)]
struct UpstreamCreateBody {
    name: String,
    kind: UpstreamKind,
    #[serde(default)]
    api_key_value: Option<String>,
    base_url: Option<Url>,
    /// OAuth token bundle for non-interactive credential import.
    #[serde(default, alias = "oauth_tokens")]
    oauth_token_bundle: Option<OAuthTokenBundle>,
    /// Gemini Code Assist project used in the request envelope.
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    warmup_enabled: Option<bool>,
    #[serde(default)]
    warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
    /// Anthropic workspace for an org-scoped API key.
    #[serde(default)]
    workspace_id: Option<String>,
    /// Store the API key unverified without probing it. The local prefix
    /// gate still applies.
    #[serde(default)]
    skip_preflight: bool,
}

#[derive(Deserialize)]
struct UpstreamUpdateBody {
    #[serde(default)]
    name: Option<String>,
    /// Missing or explicit `null` preserves the override; a non-null
    /// value sets it. Clearing requires `clear_base_url`.
    base_url: Option<Url>,
    api_key_value: Option<String>,
    /// Explicitly clears the `base_url` override. Mutually exclusive
    /// with a non-null `base_url`.
    #[serde(default)]
    clear_base_url: bool,
    /// OAuth token bundle for non-interactive credential replacement.
    #[serde(default, alias = "oauth_tokens")]
    oauth_token_bundle: Option<OAuthTokenBundle>,
    /// Gemini Code Assist project used in the request envelope.
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    warmup_enabled: Option<bool>,
    #[serde(default)]
    warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
    /// Missing or `null` preserves the workspace; a non-null value sets it.
    /// Clearing requires `clear_workspace_id`.
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    clear_workspace_id: bool,
    /// Store a new key, workspace, or host unverified without probing.
    #[serde(default)]
    skip_preflight: bool,
}

/// `Some("[REDACTED]")` for a present secret, so `Debug` shows presence only.
fn redacted_presence(value: &Option<String>) -> Option<&'static str> {
    value.as_ref().map(|_| "[REDACTED]")
}

impl std::fmt::Debug for UpstreamCreateBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            name,
            kind,
            base_url,
            api_key_value,
            oauth_token_bundle,
            project_id,
            warmup_enabled,
            warmup_dialect_plugin,
            workspace_id,
            skip_preflight,
        } = self;
        f.debug_struct("UpstreamCreateBody")
            .field("name", name)
            .field("kind", kind)
            .field("base_url", base_url)
            .field("api_key_value", &redacted_presence(api_key_value))
            .field(
                "oauth_token_bundle",
                &oauth_token_bundle.as_ref().map(|_| "[REDACTED]"),
            )
            .field("project_id", project_id)
            .field("warmup_dialect_plugin", warmup_dialect_plugin)
            .field("warmup_enabled", warmup_enabled)
            .field("workspace_id", workspace_id)
            .field("skip_preflight", skip_preflight)
            .finish()
    }
}

impl std::fmt::Debug for UpstreamUpdateBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            name,
            base_url,
            clear_base_url,
            api_key_value,
            oauth_token_bundle,
            project_id,
            enabled,
            warmup_enabled,
            warmup_dialect_plugin,
            workspace_id,
            clear_workspace_id,
            skip_preflight,
        } = self;
        f.debug_struct("UpstreamUpdateBody")
            .field("name", name)
            .field("base_url", base_url)
            .field("clear_base_url", clear_base_url)
            .field("api_key_value", &redacted_presence(api_key_value))
            .field(
                "oauth_token_bundle",
                &oauth_token_bundle.as_ref().map(|_| "[REDACTED]"),
            )
            .field("project_id", project_id)
            .field("enabled", enabled)
            .field("warmup_enabled", warmup_enabled)
            .field("warmup_dialect_plugin", warmup_dialect_plugin)
            .field("workspace_id", workspace_id)
            .field("clear_workspace_id", clear_workspace_id)
            .field("skip_preflight", skip_preflight)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct UpstreamResponse {
    id: String,
    name: String,
    kind: UpstreamKind,
    base_url: Option<Url>,
    enabled: bool,
    warmup_enabled: bool,
    warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
    spec_revision: u64,
    status: UpstreamStatusResponse,
    /// API-key upstreams with a details row only.
    #[serde(skip_serializing_if = "Option::is_none")]
    api_key_access: Option<ApiKeyAccessResponse>,
    /// Recognizable fragment of the stored key; never the key itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    key_hint: Option<String>,
}

#[derive(Debug, Serialize)]
struct UpstreamStatusResponse {
    last_apply_error: Option<String>,
    last_apply_at_unix_secs: Option<u64>,
    last_warmup_at_unix_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct SubscriptionMetadataResponse {
    upstream_id: String,
    subscription_metadata: Option<UpstreamSubscriptionMetadataRecord>,
    organization_metadata: Option<OrganizationMetadataRecord>,
    #[serde(flatten)]
    account_identity: OAuthAccountIdentity,
}

#[derive(Debug, Serialize)]
struct FireNowFiredResponse {
    fired: bool,
    lane: WarmupLane,
    cycle_key: i64,
    outcome: WarmupAttemptOutcome,
}

#[derive(Debug, Serialize)]
struct FireNowNotFiredResponse {
    fired: bool,
    lane: WarmupLane,
    #[serde(flatten)]
    outcome: WarmupAttemptOutcome,
}

#[derive(Debug, Deserialize)]
struct FireNowQuery {
    lane: Option<String>,
}

#[derive(Debug)]
pub(crate) enum UpstreamError {
    StorageUnavailable,
    NotFound,
    BadRequest {
        error: &'static str,
        detail: String,
    },
    MissingIfMatch,
    StaleRevision {
        current_revision: u64,
    },
    InvalidInput {
        field: String,
        reason: String,
    },
    NameConflict {
        name: String,
        existing_id: Uuid,
    },
    Conflict {
        detail: String,
    },
    NotOauthUpstream,
    CredentialDecrypt,
    /// Failure carrying a remediation code; renders the nested
    /// `{"error":{"code","message"}}` body.
    Remediation {
        status: StatusCode,
        code: RemediationCode,
        message: String,
    },
    NotApiKeyUpstream,
    /// The stored API-key credential could not be decrypted.
    ApiKeyCredentialDecrypt,
    RefreshUnavailable,
    RefreshFailed {
        detail: String,
    },
    MetadataRefreshTimeout,
    WarmupUnavailable,
    /// Claim body account/org no longer matches the live OAuth identity.
    StaleIdentity,
    /// Provider rejected the claim with a 4xx — a definite non-consumption.
    ProviderRejected {
        status: u16,
    },
    /// Provider answered a non-success status on a read path.
    ProviderError {
        status: u16,
    },
    /// The request never reached the provider.
    ProviderUnreachable {
        detail: String,
    },
    /// Provider answered 2xx with a body outside the contract.
    ProviderMalformed {
        detail: String,
    },
    ProviderTimeout,
    /// The claim may have been applied but the outcome never arrived.
    ClaimOutcomeUnknown,
    Internal {
        detail: String,
    },
    AuditWriteFailed,
    Storage(StorageError),
}

impl UpstreamError {
    /// HTTP status this error maps to; mirrors `into_response` so callers
    /// (e.g. audit recording) can log the outcome without consuming it.
    pub(crate) fn status(&self) -> StatusCode {
        match self {
            Self::StorageUnavailable => StatusCode::NOT_IMPLEMENTED,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::BadRequest { .. } | Self::InvalidInput { .. } => StatusCode::BAD_REQUEST,
            Self::MissingIfMatch => StatusCode::PRECONDITION_REQUIRED,
            Self::StaleRevision { .. }
            | Self::NameConflict { .. }
            | Self::Conflict { .. }
            | Self::StaleIdentity => StatusCode::CONFLICT,
            Self::NotOauthUpstream => StatusCode::BAD_REQUEST,
            Self::CredentialDecrypt | Self::ApiKeyCredentialDecrypt => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::Remediation { status, .. } => *status,
            Self::NotApiKeyUpstream => StatusCode::BAD_REQUEST,
            Self::RefreshUnavailable | Self::WarmupUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::RefreshFailed { .. }
            | Self::ProviderError { .. }
            | Self::ProviderUnreachable { .. }
            | Self::ProviderMalformed { .. } => StatusCode::BAD_GATEWAY,
            Self::MetadataRefreshTimeout | Self::ProviderTimeout | Self::ClaimOutcomeUnknown => {
                StatusCode::GATEWAY_TIMEOUT
            }
            Self::ProviderRejected { status } => {
                StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_REQUEST)
            }
            Self::Internal { .. } | Self::AuditWriteFailed | Self::Storage(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    /// Stable machine-readable error code matching the `"error"` field in
    /// `into_response`; safe for audit payloads (no provider/user content).
    pub(crate) fn error_code(&self) -> &'static str {
        match self {
            Self::StorageUnavailable => "storage_unavailable",
            Self::NotFound => "upstream_not_found",
            Self::BadRequest { error, .. } => error,
            Self::MissingIfMatch => "if_match_required",
            Self::StaleRevision { .. } => "stale_revision",
            Self::InvalidInput { .. } => "invalid_input",
            Self::NameConflict { .. } => "upstream_name_conflict",
            Self::Conflict { .. } => "conflict",
            Self::NotOauthUpstream => "not_oauth_upstream",
            Self::CredentialDecrypt => "oauth_credential_decrypt_failed",
            Self::Remediation { code, .. } => code.as_str(),
            Self::NotApiKeyUpstream => "not_api_key_upstream",
            Self::ApiKeyCredentialDecrypt => "api_key_credential_decrypt_failed",
            Self::RefreshUnavailable => "oauth_refresh_unavailable",
            Self::RefreshFailed { .. } => "oauth_refresh_failed",
            Self::MetadataRefreshTimeout => "metadata_refresh_timeout",
            Self::WarmupUnavailable => "warmup_unavailable",
            Self::StaleIdentity => "stale_identity",
            Self::ProviderRejected { .. } => "provider_rejected",
            Self::ProviderError { .. } => "provider_error",
            Self::ProviderUnreachable { .. } => "provider_unreachable",
            Self::ProviderMalformed { .. } => "provider_malformed_response",
            Self::ProviderTimeout => "provider_timeout",
            Self::ClaimOutcomeUnknown => "claim_outcome_unknown",
            Self::Internal { .. } => "internal_error",
            Self::AuditWriteFailed => "audit_write_failed",
            Self::Storage(_) => "storage_error",
        }
    }
}

impl IntoResponse for UpstreamError {
    fn into_response(self) -> Response {
        match self {
            Self::StorageUnavailable => (
                StatusCode::NOT_IMPLEMENTED,
                Json(json!({ "error": "storage_unavailable" })),
            )
                .into_response(),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "upstream_not_found" })),
            )
                .into_response(),
            Self::BadRequest { error, detail } => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": error, "detail": detail })),
            )
                .into_response(),
            Self::MissingIfMatch => (
                StatusCode::PRECONDITION_REQUIRED,
                Json(json!({ "error": "if_match_required" })),
            )
                .into_response(),
            Self::StaleRevision { current_revision } => (
                StatusCode::CONFLICT,
                Json(json!({ "error": "stale_revision", "current_revision": current_revision })),
            )
                .into_response(),
            Self::InvalidInput { field, reason } => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "invalid_input", "field": field, "reason": reason })),
            )
                .into_response(),
            Self::NameConflict { name, existing_id } => (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "upstream_name_conflict",
                    "name": name,
                    "existing_upstream_id": existing_id,
                    "detail": "An active upstream already uses this name."
                })),
            )
                .into_response(),
            Self::Conflict { detail } => (
                StatusCode::CONFLICT,
                Json(json!({ "error": "conflict", "detail": detail })),
            )
                .into_response(),
            Self::NotOauthUpstream => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "not_oauth_upstream" })),
            )
                .into_response(),
            Self::CredentialDecrypt => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "oauth_credential_decrypt_failed" })),
            )
                .into_response(),
            Self::Remediation {
                status,
                code,
                message,
            } => (
                status,
                Json(RemediationErrorBody {
                    error: RemediationErrorInner { code, message },
                }),
            )
                .into_response(),
            Self::NotApiKeyUpstream => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "not_api_key_upstream" })),
            )
                .into_response(),
            Self::ApiKeyCredentialDecrypt => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "api_key_credential_decrypt_failed" })),
            )
                .into_response(),
            Self::RefreshUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "oauth_refresh_unavailable" })),
            )
                .into_response(),
            Self::RefreshFailed { detail } => (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "oauth_refresh_failed", "detail": detail })),
            )
                .into_response(),
            Self::MetadataRefreshTimeout => (
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({ "error": "metadata_refresh_timeout" })),
            )
                .into_response(),
            Self::WarmupUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "warmup_unavailable" })),
            )
                .into_response(),
            Self::StaleIdentity => (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "stale_identity",
                    "detail": "submitted account_id/organization_id no longer match the live OAuth identity; refetch limit-resets"
                })),
            )
                .into_response(),
            Self::ProviderRejected { status } => (
                StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST),
                Json(json!({ "error": "provider_rejected", "provider_status": status })),
            )
                .into_response(),
            Self::ProviderError { status } => (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "provider_error", "provider_status": status })),
            )
                .into_response(),
            Self::ProviderUnreachable { detail } => (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "provider_unreachable", "detail": detail })),
            )
                .into_response(),
            Self::ProviderMalformed { detail } => (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "provider_malformed_response", "detail": detail })),
            )
                .into_response(),
            Self::ProviderTimeout => (
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({ "error": "provider_timeout" })),
            )
                .into_response(),
            Self::ClaimOutcomeUnknown => (
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({
                    "error": "claim_outcome_unknown",
                    "detail": "the claim may have been applied but the outcome never arrived; recheck limit-resets before retrying"
                })),
            )
                .into_response(),
            Self::Internal { detail } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "internal_error", "detail": detail })),
            )
                .into_response(),
            Self::AuditWriteFailed => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "audit_write_failed" })),
            )
                .into_response(),
            Self::Storage(error) => {
                tracing::error!(error = %error, "admin v1 upstream storage operation failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

impl From<StorageError> for UpstreamError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Conflict { message } => Self::Conflict { detail: message },
            StorageError::InvalidInput { field, reason } => Self::InvalidInput { field, reason },
            other => Self::Storage(other),
        }
    }
}

impl From<SchedulerError> for UpstreamError {
    fn from(error: SchedulerError) -> Self {
        Self::Internal {
            detail: error.to_string(),
        }
    }
}

impl From<PostCommitAborted> for UpstreamError {
    fn from(_: PostCommitAborted) -> Self {
        Self::Internal {
            detail: "admin post-commit continuation aborted".to_owned(),
        }
    }
}

async fn validate_warmup_plugin(
    storage: &dyn Storage,
    kind: UpstreamKind,
    plugin: Option<&UpstreamWarmupDialectPlugin>,
) -> Result<(), UpstreamError> {
    let Some(plugin) = plugin else {
        return Ok(());
    };
    // Legacy Anthropic validation remains in storage. Other providers require
    // their own shape export, not merely a response hook in the shape slot.
    let hook = match kind {
        UpstreamKind::OpenaiOauth => "shape_openai",
        UpstreamKind::GeminiOauth => "shape_gemini",
        _ => return Ok(()),
    };
    let registry = storage
        .get_registry_entry_by_id(plugin.wasm_registry_id)
        .await?
        .ok_or_else(|| UpstreamError::BadRequest {
            error: "unknown_registry_entry",
            detail: format!("warmup plugin {} does not exist", plugin.wasm_registry_id),
        })?;
    if !registry
        .supported_slots
        .contains(&cc_lb_storage_api::PluginSlotKind::Shape)
        || !registry.hook_metadata.contains_key(hook)
    {
        return Err(UpstreamError::BadRequest {
            error: "warmup_plugin_unsupported_provider",
            detail: format!("warmup plugin {} must export cc_lb_{hook}", registry.name),
        });
    }
    Ok(())
}

async fn create_upstream(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Json(body): Json<UpstreamCreateBody>,
) -> Result<Response, UpstreamError> {
    let storage = storage(&state)?;
    if let Some(base_url) = body.base_url.as_ref() {
        validate_base_url_input(base_url)?;
    }
    // The id is allocated up front so the credential — sealed with AAD = id —
    // and the spec row can be committed in one storage transaction (#852).
    let id = Uuid::new_v4();
    let name = body.name.clone();
    let kind = body.kind;
    if !matches!(
        kind,
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
    ) && (body.warmup_enabled == Some(true) || body.warmup_dialect_plugin.is_some())
    {
        return Err(UpstreamError::BadRequest {
            error: "warmup_unsupported_kind",
            detail: "warmup is supported only for OAuth upstreams".to_owned(),
        });
    }
    validate_warmup_plugin(storage, kind, body.warmup_dialect_plugin.as_ref()).await?;
    let oauth_bundle = match kind {
        UpstreamKind::GeminiOauth if body.oauth_token_bundle.is_none() => {
            if body.api_key_value.is_some() || body.workspace_id.is_some() {
                return Err(UpstreamError::BadRequest {
                    error: "invalid_gemini_oauth_credential",
                    detail: "Gemini OAuth accepts only oauth_tokens and project_id".to_owned(),
                });
            }
            return Err(UpstreamError::BadRequest {
                error: "missing_oauth_tokens",
                detail: "Gemini OAuth requires oauth_tokens".to_owned(),
            });
        }
        UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth => {
            if body.api_key_value.is_some() {
                if kind == UpstreamKind::GeminiOauth {
                    return Err(UpstreamError::BadRequest {
                        error: "invalid_gemini_oauth_credential",
                        detail: "Gemini OAuth accepts only oauth_tokens and project_id".to_owned(),
                    });
                }
                return Err(UpstreamError::BadRequest {
                    error: "unexpected_api_key",
                    detail: format!("{} upstreams do not accept api_key_value", kind.as_str()),
                });
            }
            if body.workspace_id.is_some() {
                return Err(if kind == UpstreamKind::GeminiOauth {
                    UpstreamError::BadRequest {
                        error: "invalid_gemini_oauth_credential",
                        detail: "Gemini OAuth accepts only oauth_tokens and project_id".to_owned(),
                    }
                } else {
                    UpstreamError::NotApiKeyUpstream
                });
            }
            prepare_imported_oauth_bundle(
                kind,
                body.oauth_token_bundle.clone(),
                body.project_id.as_deref(),
                cc_lb_clock::unix_secs(state.clock.now()),
            )?
        }
        _ => {
            if body.oauth_token_bundle.is_some()
                || (body.project_id.is_some()
                    && !matches!(
                        kind,
                        UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey
                    ))
            {
                return Err(UpstreamError::BadRequest {
                    error: "unexpected_oauth_tokens",
                    detail: "OAuth token import is supported only for OpenAI and Gemini OAuth"
                        .to_owned(),
                });
            }
            None
        }
    };
    let api_key = match kind {
        UpstreamKind::AnthropicApiKey | UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey => {
            // A taken name is refused before the key is probed.
            if let Some(existing) = UpstreamStore::get_by_name(storage, &name).await? {
                return Err(UpstreamError::NameConflict {
                    name,
                    existing_id: existing.id,
                });
            }
            Some(
                prepare_api_key_create(
                    &state,
                    kind,
                    body.api_key_value.as_deref(),
                    body.base_url.as_ref(),
                    body.workspace_id.as_deref(),
                    body.project_id.as_deref(),
                    body.skip_preflight,
                )
                .await?,
            )
        }
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth => {
            if body.api_key_value.is_some() {
                return Err(UpstreamError::BadRequest {
                    error: "unexpected_api_key",
                    detail: format!("{} upstreams do not accept api_key_value", kind.as_str()),
                });
            }
            if body.workspace_id.is_some() {
                return Err(UpstreamError::NotApiKeyUpstream);
            }
            None
        }
    };
    let warmup_enabled = body.warmup_enabled.unwrap_or(matches!(
        kind,
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
    ));
    // The api key is sealed under the preallocated id so the spec row and its
    // credential land in one storage transaction; no reader ever observes an
    // api-key upstream without its key.
    let api_key_ciphertext = api_key
        .as_ref()
        .map(|prepared| encrypt_plaintext_value(&state, prepared.key.as_str(), id.as_bytes()))
        .transpose()?;
    let oauth_tokens = oauth_bundle
        .as_ref()
        .map(|bundle| {
            AeadEncryptedField::<OAuthTokenBundle>::encrypt(&state.aead, bundle, id.as_bytes()).map(
                |tokens| OAuthTokensCreate {
                    tokens,
                    never_refresh: bundle.never_refresh,
                },
            )
        })
        .transpose()
        .map_err(|_| UpstreamError::CredentialDecrypt)?;
    let record = match UpstreamStore::create(
        storage,
        UpstreamCreate {
            id,
            name: body.name,
            kind,
            base_url: body.base_url,
            api_key_ciphertext,
            oauth_tokens,
            warmup_enabled,
            warmup_dialect_plugin: body.warmup_dialect_plugin,
            workspace_id: api_key
                .as_ref()
                .and_then(|prepared| prepared.check.workspace_id.clone()),
            api_key_access: api_key
                .as_ref()
                .map(|prepared| prepared.check.access.clone()),
        },
    )
    .await
    {
        Ok(record) => record,
        Err(error @ StorageError::Conflict { .. }) => {
            match UpstreamStore::get_by_name(storage, &name).await? {
                Some(existing) => {
                    return Err(UpstreamError::NameConflict {
                        name,
                        existing_id: existing.id,
                    });
                }
                None => return Err(error.into()),
            }
        }
        Err(error) => return Err(error.into()),
    };
    let check = api_key.map(|prepared| prepared.check);
    let imported_oauth = oauth_bundle.is_some();
    let storage = storage_arc(&state)?;
    complete_after_commit(async move {
        if imported_oauth
            && let Err(error) = super::oauth::seed_oauth_bootstrap_tasks(&state, &record).await
        {
            super::oauth::rebuild_after_committed_failure(&state, "oauth import scheduler seeding")
                .await;
            return Err(UpstreamError::from(error));
        }
        let payload = AuditPayload::UpstreamCreate {
            upstream_id: record.id.to_string(),
            kind: record.kind.as_str().to_owned(),
            key_hint: check.as_ref().and_then(KeyCheck::key_hint),
            verdict: check.as_ref().and_then(KeyCheck::verdict_label),
            remediation: check
                .as_ref()
                .map(KeyCheck::remediation_labels)
                .unwrap_or_default(),
        };
        if let Err(error) = audit_upstream_action(
            &state,
            &identity,
            &payload,
            "/admin/v1/upstreams",
            Some(&record.name),
            StatusCode::CREATED,
        )
        .await
        {
            rebuild_after_audit_failure(&state).await;
            return Err(error);
        }
        if let Some(checked_at) = check.as_ref().and_then(KeyCheck::usage_reported_at) {
            enqueue_usage_poll(&state, record.id, record.revision, checked_at).await;
        }
        let details = api_key_details_after_commit(storage.as_ref(), &record).await;
        let location = match HeaderValue::from_str(&format!("/admin/v1/upstreams/{}", record.id)) {
            Ok(location) => location,
            Err(error) => {
                return Err(UpstreamError::BadRequest {
                    error: "invalid_location",
                    detail: error.to_string(),
                });
            }
        };
        let mut response = (
            StatusCode::CREATED,
            Json(upstream_response(&record, details.as_ref())),
        )
            .into_response();
        response
            .headers_mut()
            .insert(axum::http::header::LOCATION, location);
        add_dynamic_rebind_headers(&mut response, &state).await;
        Ok(key_handling_response(&record, response))
    })
    .await?
}

async fn list_upstreams(
    State(state): State<AdminState>,
    Query(query): Query<ListQuery>,
) -> Result<Response, UpstreamError> {
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let all = list_all_upstreams(&state).await?;
    let total = all.len();
    let page = all
        .into_iter()
        .filter(|record| {
            query
                .after
                .as_ref()
                .is_none_or(|after| record.id.to_string() > *after)
        })
        .take(limit)
        .collect::<Vec<_>>();
    let api_key_ids = page
        .iter()
        .filter(|record| {
            matches!(
                record.kind,
                UpstreamKind::AnthropicApiKey
                    | UpstreamKind::OpenaiApiKey
                    | UpstreamKind::GeminiApiKey
            )
        })
        .map(|record| record.id)
        .collect::<Vec<_>>();
    let details = if api_key_ids.is_empty() {
        Vec::new()
    } else {
        UpstreamApiKeyStore::list_api_key_details(storage(&state)?, &api_key_ids).await?
    };
    let page = page
        .iter()
        .map(|record| {
            let details = details
                .iter()
                .find(|details| details.upstream_id == record.id);
            upstream_response(record, details)
        })
        .collect::<Vec<_>>();

    let mut response = Json(json!({ "upstreams": page })).into_response();
    response.headers_mut().insert(
        "X-Total-Count",
        HeaderValue::from_str(&total.to_string()).map_err(|error| UpstreamError::BadRequest {
            error: "invalid_total_count",
            detail: error.to_string(),
        })?,
    );
    Ok(response)
}

async fn get_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Response, UpstreamError> {
    let record = find_upstream(&state, &id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    let details = api_key_details(storage(&state)?, &record).await?;
    let mut response = Json(upstream_response(&record, details.as_ref())).into_response();
    response
        .headers_mut()
        .insert(axum::http::header::ETAG, etag_value(record.revision)?);
    Ok(response)
}

async fn get_upstream_subscription_metadata(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<SubscriptionMetadataResponse>, UpstreamError> {
    let storage = storage(&state)?;
    let upstream_id = id
        .parse::<Uuid>()
        .map_err(|error| UpstreamError::BadRequest {
            error: "invalid_upstream_id",
            detail: error.to_string(),
        })?;
    let mut response = subscription_metadata_response(storage, upstream_id).await?;
    if let Some(upstream) = UpstreamStore::get_by_id(storage, upstream_id).await?
        && matches!(
            upstream.kind,
            UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
        )
        && let Ok(bundle) = decrypt_oauth_bundle(&state, &upstream)
    {
        response.account_identity =
            super::oauth::provider_account_identity(storage, &upstream, &bundle).await;
    }
    Ok(Json(response))
}

async fn refresh_upstream_subscription_metadata(
    State(state): State<AdminState>,
    Path(upstream_id): Path<Uuid>,
    Extension(identity): Extension<AdminIdentity>,
) -> Result<Json<SubscriptionMetadataResponse>, UpstreamError> {
    let storage = storage_arc(&state)?;
    let upstream = UpstreamStore::get_by_id(storage.as_ref(), upstream_id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if upstream.kind != UpstreamKind::AnthropicOauth || upstream.oauth_credentials.is_none() {
        return Err(UpstreamError::NotOauthUpstream);
    }

    let bundle = decrypt_oauth_bundle(&state, &upstream)?;
    let access_token =
        fresh_enough_access_token(&state, storage.clone(), &upstream, bundle).await?;
    let user_agent = metadata_user_agent(storage.as_ref()).await?;
    let client = make_metadata_http_client();
    let cancel = CancellationToken::new();
    // One deadline covers the fetch and both metadata writes, as before the
    // writes were split around the post-commit boundary.
    let deadline = tokio::time::Instant::now() + METADATA_REFRESH_TIMEOUT;
    let records = tokio::time::timeout_at(deadline, async {
        let records = fetch_metadata_only(
            &client,
            upstream_id,
            &access_token,
            &user_agent,
            &cancel,
            &*state.clock,
        )
        .await;
        store_subscription_metadata(storage.as_ref(), &records)
            .await
            .map(|()| records)
    })
    .await
    .map_err(|_| UpstreamError::MetadataRefreshTimeout)?
    .map_err(|error| UpstreamError::RefreshFailed {
        detail: error.to_string(),
    })?;

    // The subscription record is committed: the organization write, the
    // audit and the response read outlive a dropped request.
    complete_after_commit(async move {
        match tokio::time::timeout_at(
            deadline,
            store_organization_metadata(storage.as_ref(), &records),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err(UpstreamError::RefreshFailed {
                    detail: error.to_string(),
                });
            }
            Err(_) => return Err(UpstreamError::MetadataRefreshTimeout),
        }
        let action = "upstream_subscription_metadata_refresh";
        let route = format!("/admin/v1/upstreams/{upstream_id}/subscription-metadata/refresh");
        if let Err(error) = record_admin_audit(
            &state,
            AdminAuditEvent {
                identity: Some(&identity),
                system_component: None,
                action,
                route: &route,
                target_principal_id: None,
                target_upstream: Some(&upstream.name),
                api_key_id: None,
                status: StatusCode::OK.as_u16(),
                payload: None,
            },
        )
        .await
        {
            tracing::error!(error = %error, action, "admin audit write failed");
            return Err(UpstreamError::AuditWriteFailed);
        }

        subscription_metadata_response(storage.as_ref(), upstream_id)
            .await
            .map(Json)
    })
    .await?
}

async fn fire_now_upstream_warmup(
    State(state): State<AdminState>,
    Path(upstream_id): Path<Uuid>,
    Query(query): Query<FireNowQuery>,
    Extension(identity): Extension<AdminIdentity>,
) -> Result<Response, UpstreamError> {
    let lane = match query.lane.as_deref() {
        None => WarmupLane::Primary,
        Some(value) => super::upstream_warmup::parse_lane(value).map_err(|detail| {
            UpstreamError::BadRequest {
                error: "invalid_warmup_lane",
                detail,
            }
        })?,
    };
    let storage = storage_arc(&state)?;
    let upstream = UpstreamStore::get_by_id(storage.as_ref(), upstream_id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    let target_upstream = upstream.name.clone();
    let stage =
        fire_now_upstream_warmup_inner(&state, storage.clone(), upstream_id, upstream, lane)
            .await?;
    // Every stage has committed a warmup attempt row (or needs no write);
    // the remaining persistence and the audit outlive a dropped request.
    complete_after_commit(async move {
        let response = match stage {
            FireNowStage::Finished(response) => response,
            FireNowStage::Attempted(attempted) => {
                match finish_fire_now_attempt(
                    &state,
                    storage.as_ref(),
                    upstream_id,
                    lane,
                    attempted,
                )
                .await
                {
                    Ok(response) => response,
                    Err(error) => return Err(error),
                }
            }
        };
        let status = response.status().as_u16();
        let action = "upstream_warmup_fire_now";
        let route = format!("/admin/v1/upstreams/{upstream_id}/warmup/fire-now");
        if let Err(error) = record_admin_audit(
            &state,
            AdminAuditEvent {
                identity: Some(&identity),
                system_component: None,
                action,
                route: &route,
                target_principal_id: None,
                target_upstream: Some(&target_upstream),
                api_key_id: None,
                status,
                payload: None,
            },
        )
        .await
        {
            tracing::error!(error = %error, action, "admin audit write failed");
            return Err(UpstreamError::AuditWriteFailed);
        }
        Ok(response)
    })
    .await?
}

/// Where a fire-now request stands once the request future hands off.
enum FireNowStage {
    /// The final response is known; any attempt row is already written.
    Finished(Response),
    /// The dispatch attempt row is committed; quota observations, warmup
    /// status and the response still follow.
    Attempted(FireNowAttempted),
}

struct FireNowAttempted {
    holder: String,
    candidate_cycle_key: i64,
    dispatch_attempt: FireNowDispatchAttempt,
    record: crate::ports::WarmupAttemptOutcomeSnapshot,
}

async fn fire_now_upstream_warmup_inner(
    state: &AdminState,
    storage: Arc<dyn Storage>,
    upstream_id: Uuid,
    upstream: UpstreamRecord,
    lane: WarmupLane,
) -> Result<FireNowStage, UpstreamError> {
    let kind_lanes = WarmupLane::lanes_for_kind(upstream.kind);
    if kind_lanes.is_empty() {
        return Ok(FireNowStage::Finished(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "warmup_unsupported_for_kind",
                    "kind": upstream.kind.as_str(),
                })),
            )
                .into_response(),
        ));
    }
    if !kind_lanes.contains(&lane) {
        return Ok(FireNowStage::Finished(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "warmup_lane_unsupported",
                    "kind": upstream.kind.as_str(),
                    "lane": lane.as_str(),
                })),
            )
                .into_response(),
        ));
    }
    let now_unix_secs = unix_now_secs_i64(&*state.clock)?;
    if !upstream.warmup_enabled {
        let _ = record_fire_now_skip(
            state,
            storage.as_ref(),
            &upstream,
            lane,
            now_unix_secs,
            WarmupSkipReason::UpstreamDisabled,
            None,
        )
        .await?;
        return Ok(FireNowStage::Finished(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "warmup_disabled" })),
            )
                .into_response(),
        ));
    }
    if upstream.oauth_credentials.is_none() {
        let outcome = record_fire_now_failure(
            state,
            storage.as_ref(),
            &upstream,
            lane,
            now_unix_secs,
            now_unix_secs,
            None,
            WarmupPermanentFailureReason::OauthCredentialsMissing,
            WarmupDispatchKind::NotDispatched,
            None,
            None,
        )
        .await?;
        return Ok(FireNowStage::Finished(fire_now_not_fired_response(
            StatusCode::BAD_REQUEST,
            lane,
            outcome,
        )));
    }

    let holder = format!("fire-now:{}", Uuid::new_v4());
    tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, lane = lane.as_str(), action = "dispatch_requested");

    // Only non-primary lanes depend on the credential profile; the primary
    // lane never needs the plaintext credential here.
    let bundle = if lane == WarmupLane::Primary {
        None
    } else {
        match decrypt_oauth_bundle(state, &upstream) {
            Ok(bundle) => Some(bundle),
            Err(_) => {
                let outcome = record_fire_now_failure(
                    state,
                    storage.as_ref(),
                    &upstream,
                    lane,
                    now_unix_secs,
                    now_unix_secs,
                    Some(holder.as_str()),
                    WarmupPermanentFailureReason::CredentialDecryptFailed,
                    WarmupDispatchKind::NotDispatched,
                    Some(StatusCode::BAD_GATEWAY),
                    Some("upstream oauth credential decrypt failed"),
                )
                .await?;
                return Ok(FireNowStage::Finished(fire_now_not_fired_response(
                    StatusCode::BAD_GATEWAY,
                    lane,
                    outcome,
                )));
            }
        }
    };
    let latest_quota = storage
        .list_latest_subscription_quota_for_upstreams(&[upstream_id])
        .await?;
    let reported_windows =
        super::upstream_warmup::lane_reported_windows(upstream.kind, lane, &latest_quota);
    if let Some(reason) = super::upstream_warmup::lane_ineligible_reason(
        upstream.kind,
        lane,
        bundle.as_ref(),
        &reported_windows,
    ) {
        let detail = match reason {
            super::upstream_warmup::WarmupLaneIneligibleReason::ProfileUnsupported => {
                "credential profile does not serve this quota group"
            }
            super::upstream_warmup::WarmupLaneIneligibleReason::QuotaGroupAbsent => {
                "every target window of this quota group is reported absent"
            }
        };
        tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, lane = lane.as_str(), reason = detail, action = "dispatch_skipped");
        let outcome = record_fire_now_skip(
            state,
            storage.as_ref(),
            &upstream,
            lane,
            now_unix_secs,
            WarmupSkipReason::QuotaGroupUnavailable,
            Some(detail),
        )
        .await?;
        return Ok(FireNowStage::Finished(fire_now_not_fired_response(
            StatusCode::ACCEPTED,
            lane,
            outcome,
        )));
    }
    let candidate_cycle_key = lane
        .target_windows(upstream.kind)
        .first()
        .and_then(|window| latest_window_quota(&latest_quota, *window))
        .and_then(cycle_key_from_latest_observation)
        .unwrap_or(now_unix_secs);
    if upstream.warmup_dialect_plugin.is_some()
        && (state.runtime.is_none()
            || state.data_dir.is_none()
            || state.warmup_dialect_dispatcher.is_none())
    {
        tracing::warn!(target: "warmup", upstream_id = %upstream_id, holder = %holder, action = "cycle_abandoned", reason = "dialect_plugin_failed");
        let outcome = record_fire_now_failure(
            state,
            storage.as_ref(),
            &upstream,
            lane,
            now_unix_secs,
            candidate_cycle_key,
            Some(holder.as_str()),
            WarmupPermanentFailureReason::DialectPluginFailed,
            WarmupDispatchKind::NotDispatched,
            Some(StatusCode::BAD_GATEWAY),
            Some("warmup dialect dispatcher unavailable"),
        )
        .await?;
        return Ok(FireNowStage::Finished(fire_now_not_fired_response(
            StatusCode::BAD_GATEWAY,
            lane,
            outcome,
        )));
    }
    let dialect_dispatch_bundle = upstream.warmup_dialect_plugin.as_ref().and_then(|_| {
        let runtime = state.runtime.as_ref()?;
        let data_dir = state.data_dir.as_deref()?;
        let dispatcher = state.warmup_dialect_dispatcher.as_deref()?;
        Some((runtime, data_dir, dispatcher))
    });
    let dialect_plugin_used = dialect_dispatch_bundle.is_some();
    tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, lane = lane.as_str(), cycle_key = %candidate_cycle_key, dialect_plugin_used = dialect_plugin_used, action = "dispatch_start");
    let dispatch_attempt = if let Some((runtime, data_dir, dispatcher)) = dialect_dispatch_bundle {
        match tokio::time::timeout(
            Duration::from_secs(FIRE_NOW_REQUEST_TIMEOUT_SECS),
            dispatcher.dispatch_warmup_with_dialect(runtime, data_dir, &upstream, lane),
        )
        .await
        {
            Ok(Ok(outcome)) => fire_now_response_attempt(
                outcome.status,
                outcome.headers,
                WarmupDispatchKind::DialectPlugin,
                upstream.kind,
            ),
            Ok(Err(error)) => match error.kind {
                WarmupDialectDispatchErrorKind::Transient => {
                    FireNowDispatchAttempt::TransientFailure {
                        reason: WarmupTransientFailureReason::DialectPluginTransient,
                        dispatch_kind: WarmupDispatchKind::DialectPlugin,
                        status: StatusCode::SERVICE_UNAVAILABLE,
                        error_detail: error.detail,
                    }
                }
                WarmupDialectDispatchErrorKind::Permanent => {
                    FireNowDispatchAttempt::PermanentFailure {
                        reason: WarmupPermanentFailureReason::DialectPluginFailed,
                        dispatch_kind: WarmupDispatchKind::DialectPlugin,
                        status: StatusCode::BAD_GATEWAY,
                        error_detail: error.detail,
                    }
                }
            },
            Err(_) => FireNowDispatchAttempt::TransientFailure {
                reason: WarmupTransientFailureReason::RequestTimeout,
                dispatch_kind: WarmupDispatchKind::DialectPlugin,
                status: StatusCode::SERVICE_UNAVAILABLE,
                error_detail: "warmup dispatch timed out".to_owned(),
            },
        }
    } else if let Some(dispatcher) = state.warmup_dialect_dispatcher.as_deref() {
        match tokio::time::timeout(
            Duration::from_secs(FIRE_NOW_REQUEST_TIMEOUT_SECS),
            dispatcher.dispatch_warmup_native(&upstream, lane),
        )
        .await
        {
            Ok(Ok(outcome)) => fire_now_attempt_from_native_outcome(outcome),
            Ok(Err(error)) => FireNowDispatchAttempt::PermanentFailure {
                reason: WarmupPermanentFailureReason::RequestBuildFailed,
                dispatch_kind: WarmupDispatchKind::NotDispatched,
                status: StatusCode::BAD_GATEWAY,
                error_detail: error.detail,
            },
            Err(_) => FireNowDispatchAttempt::TransientFailure {
                reason: WarmupTransientFailureReason::RequestTimeout,
                dispatch_kind: WarmupDispatchKind::Http,
                status: StatusCode::SERVICE_UNAVAILABLE,
                error_detail: "warmup dispatch timed out".to_owned(),
            },
        }
    } else {
        return Err(UpstreamError::WarmupUnavailable);
    };
    let record = record_warmup_attempt(
        state,
        WarmupAttemptInput {
            storage: storage.as_ref(),
            upstream: &upstream,
            lane,
            scheduled_for_unix_secs: now_unix_secs,
            lease_holder: Some(holder.as_str()),
            expected_cycle_key: Some(candidate_cycle_key),
            attempted_at_unix_secs: now_unix_secs,
            completed_at_unix_secs: Some(unix_now_secs_i64(&*state.clock)?),
            dispatch_kind: fire_now_attempt_dispatch_kind(&dispatch_attempt),
            result: execution_result_from_fire_now_attempt(&dispatch_attempt),
        },
    )
    .await?;
    Ok(FireNowStage::Attempted(FireNowAttempted {
        holder,
        candidate_cycle_key,
        dispatch_attempt,
        record,
    }))
}

/// Post-commit half of a dispatched fire-now: persists the response's quota
/// observations and the warmup status, then builds the response.
async fn finish_fire_now_attempt(
    state: &AdminState,
    storage: &dyn Storage,
    upstream_id: Uuid,
    lane: WarmupLane,
    FireNowAttempted {
        holder,
        candidate_cycle_key,
        dispatch_attempt,
        record,
    }: FireNowAttempted,
) -> Result<Response, UpstreamError> {
    if let FireNowDispatchAttempt::Response {
        headers,
        observations,
        ..
    } = &dispatch_attempt
    {
        if !headers.is_empty() {
            record_fire_now_subscription_quota_observations(state, storage, upstream_id, headers)
                .await?;
        } else {
            record_fire_now_unified_quota_observations(state, storage, upstream_id, observations)
                .await?;
        }
    }
    let status = fire_now_attempt_status(&dispatch_attempt);
    tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, status = %status, outcome = ?record.outcome, dispatch_error = record.error_detail.as_deref(), action = "dispatch_result");

    match record.outcome {
        WarmupAttemptOutcome::Success(_) => {
            let response_cycle_key = record.cycle_key.unwrap_or(candidate_cycle_key);
            tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, cycle_key = %candidate_cycle_key, response_cycle_key = %response_cycle_key, action = "dispatch_succeeded");
            write_warmup_status_after_success(storage, upstream_id, &*state.clock).await;
            Ok(Json(FireNowFiredResponse {
                fired: true,
                lane,
                cycle_key: response_cycle_key,
                outcome: record.outcome,
            })
            .into_response())
        }
        WarmupAttemptOutcome::PermanentFailure(reason) => {
            tracing::warn!(target: "warmup", upstream_id = %upstream_id, reason = ?reason, action = "cycle_abandoned");
            Ok(fire_now_not_fired_response(
                StatusCode::BAD_GATEWAY,
                lane,
                record.outcome,
            ))
        }
        WarmupAttemptOutcome::TransientFailure(_) => Ok(fire_now_not_fired_response(
            StatusCode::SERVICE_UNAVAILABLE,
            lane,
            record.outcome,
        )),
        WarmupAttemptOutcome::Skipped(_) => Ok(fire_now_not_fired_response(
            StatusCode::ACCEPTED,
            lane,
            record.outcome,
        )),
    }
}

fn fire_now_not_fired_response(
    status: StatusCode,
    lane: WarmupLane,
    outcome: WarmupAttemptOutcome,
) -> Response {
    (
        status,
        Json(FireNowNotFiredResponse {
            fired: false,
            lane,
            outcome,
        }),
    )
        .into_response()
}

fn latest_window_quota(
    latest_quota: &[SubscriptionQuotaSample],
    window: SubscriptionQuotaWindow,
) -> Option<&SubscriptionQuotaSample> {
    latest_quota
        .iter()
        .filter(|record| record.window == window)
        .max_by_key(|record| record.observed_at_unix_millis)
}

fn cycle_key_from_latest_observation(latest: &SubscriptionQuotaSample) -> Option<i64> {
    latest
        .resets_at_unix_secs
        .and_then(|resets_at| i64::try_from(resets_at).ok())
}

async fn record_fire_now_subscription_quota_observations(
    state: &AdminState,
    storage: &dyn Storage,
    upstream_id: Uuid,
    headers: &HeaderMap,
) -> Result<(), UpstreamError> {
    if headers.is_empty() {
        return Ok(());
    }
    let observed_at_unix_millis = unix_now_millis(&*state.clock)?;
    if let Some(quota_ingestion) = state
        .lifecycle
        .as_ref()
        .and_then(|ports| ports.subscription_quota_ingestion.as_deref())
    {
        let observed_at = UNIX_EPOCH
            .checked_add(Duration::from_millis(observed_at_unix_millis))
            .ok_or_else(|| invalid_warmup_state("warmup observed timestamp overflow"))?;
        quota_ingestion.ingest_subscription_quota_headers(headers, upstream_id, observed_at);
        return Ok(());
    }
    let records = build_subscription_quota_samples(headers, upstream_id, observed_at_unix_millis);
    if !records.is_empty() {
        storage.record_subscription_quota_samples(&records).await?;
    }
    Ok(())
}
async fn record_fire_now_unified_quota_observations(
    state: &AdminState,
    storage: &dyn Storage,
    upstream_id: Uuid,
    observations: &[UnifiedQuotaObservation],
) -> Result<(), UpstreamError> {
    let observed_at_unix_millis = unix_now_millis(&*state.clock)?;
    if let Some(quota_ingestion) = state
        .lifecycle
        .as_ref()
        .and_then(|ports| ports.subscription_quota_ingestion.as_deref())
    {
        let observed_at = UNIX_EPOCH
            .checked_add(Duration::from_millis(observed_at_unix_millis))
            .ok_or_else(|| invalid_warmup_state("warmup observed timestamp overflow"))?;
        quota_ingestion.ingest_subscription_quota_observations(
            observations,
            upstream_id,
            observed_at,
        );
        return Ok(());
    }
    let records = observations
        .iter()
        .cloned()
        .map(|observation| {
            cc_lb_quota::unified_observation_to_sample(
                upstream_id,
                observation,
                observed_at_unix_millis,
            )
        })
        .collect::<Vec<_>>();
    if !records.is_empty() {
        storage.record_subscription_quota_samples(&records).await?;
    }
    Ok(())
}

enum FireNowDispatchAttempt {
    Response {
        status: StatusCode,
        dispatch_kind: WarmupDispatchKind,
        headers: HeaderMap,
        observations: Vec<UnifiedQuotaObservation>,
        error_detail: Option<String>,
    },
    TransientFailure {
        reason: WarmupTransientFailureReason,
        dispatch_kind: WarmupDispatchKind,
        status: StatusCode,
        error_detail: String,
    },
    PermanentFailure {
        reason: WarmupPermanentFailureReason,
        dispatch_kind: WarmupDispatchKind,
        status: StatusCode,
        error_detail: String,
    },
}

fn fire_now_attempt_from_native_outcome(
    outcome: WarmupNativeDispatchOutcome,
) -> FireNowDispatchAttempt {
    match outcome {
        WarmupNativeDispatchOutcome::Response {
            status,
            dispatch_kind,
            observations,
            error_detail,
        } => FireNowDispatchAttempt::Response {
            status,
            dispatch_kind,
            headers: HeaderMap::new(),
            observations,
            error_detail,
        },
        WarmupNativeDispatchOutcome::TransientFailure {
            dispatch_kind,
            reason,
            status,
            error_detail,
        } => FireNowDispatchAttempt::TransientFailure {
            reason,
            dispatch_kind,
            status,
            error_detail,
        },
        WarmupNativeDispatchOutcome::PermanentFailure {
            dispatch_kind,
            reason,
            status,
            error_detail,
        } => FireNowDispatchAttempt::PermanentFailure {
            reason,
            dispatch_kind,
            status,
            error_detail,
        },
    }
}

fn fire_now_response_attempt(
    status: StatusCode,
    headers: HeaderMap,
    dispatch_kind: WarmupDispatchKind,
    kind: UpstreamKind,
) -> FireNowDispatchAttempt {
    let observations = match kind {
        UpstreamKind::OpenaiOauth => cc_lb_quota::parse_openai_codex_quota_headers(&headers),
        UpstreamKind::GeminiOauth => Vec::new(),
        _ => parse_anthropic_unified_headers(&headers),
    };
    FireNowDispatchAttempt::Response {
        status,
        dispatch_kind,
        headers,
        observations,
        error_detail: None,
    }
}

fn execution_result_from_fire_now_attempt(
    attempt: &FireNowDispatchAttempt,
) -> WarmupAttemptResult<'_> {
    match attempt {
        FireNowDispatchAttempt::Response {
            status,
            observations,
            error_detail,
            ..
        } => WarmupAttemptResult::Response {
            status: *status,
            observations,
            error_detail: error_detail.as_deref(),
        },
        FireNowDispatchAttempt::TransientFailure {
            reason,
            status,
            error_detail,
            ..
        } => WarmupAttemptResult::TransientFailure {
            reason: *reason,
            http_status: Some(*status),
            error_detail: Some(error_detail.as_str()),
        },
        FireNowDispatchAttempt::PermanentFailure {
            reason,
            status,
            error_detail,
            ..
        } => WarmupAttemptResult::PermanentFailure {
            reason: *reason,
            http_status: Some(*status),
            error_detail: Some(error_detail.as_str()),
        },
    }
}

fn fire_now_attempt_status(attempt: &FireNowDispatchAttempt) -> StatusCode {
    match attempt {
        FireNowDispatchAttempt::Response { status, .. }
        | FireNowDispatchAttempt::TransientFailure { status, .. }
        | FireNowDispatchAttempt::PermanentFailure { status, .. } => *status,
    }
}

fn fire_now_attempt_dispatch_kind(attempt: &FireNowDispatchAttempt) -> WarmupDispatchKind {
    match attempt {
        FireNowDispatchAttempt::Response { dispatch_kind, .. }
        | FireNowDispatchAttempt::TransientFailure { dispatch_kind, .. }
        | FireNowDispatchAttempt::PermanentFailure { dispatch_kind, .. } => *dispatch_kind,
    }
}

async fn record_fire_now_skip(
    state: &AdminState,
    storage: &dyn Storage,
    upstream: &UpstreamRecord,
    lane: WarmupLane,
    now_unix_secs: i64,
    reason: WarmupSkipReason,
    error_detail: Option<&str>,
) -> Result<WarmupAttemptOutcome, UpstreamError> {
    record_warmup_attempt(
        state,
        WarmupAttemptInput {
            storage,
            upstream,
            lane,
            scheduled_for_unix_secs: now_unix_secs,
            lease_holder: None,
            expected_cycle_key: None,
            attempted_at_unix_secs: now_unix_secs,
            completed_at_unix_secs: Some(now_unix_secs),
            dispatch_kind: WarmupDispatchKind::NotDispatched,
            result: WarmupAttemptResult::Skipped {
                reason,
                cycle_key: None,
                error_detail,
            },
        },
    )
    .await
    .map(|record| record.outcome)
}

#[allow(clippy::too_many_arguments)]
async fn record_fire_now_failure(
    state: &AdminState,
    storage: &dyn Storage,
    upstream: &UpstreamRecord,
    lane: WarmupLane,
    now_unix_secs: i64,
    candidate_cycle_key: i64,
    holder: Option<&str>,
    reason: WarmupPermanentFailureReason,
    dispatch_kind: WarmupDispatchKind,
    status: Option<StatusCode>,
    error_detail: Option<&str>,
) -> Result<WarmupAttemptOutcome, UpstreamError> {
    record_warmup_attempt(
        state,
        WarmupAttemptInput {
            storage,
            upstream,
            lane,
            scheduled_for_unix_secs: now_unix_secs,
            lease_holder: holder,
            expected_cycle_key: Some(candidate_cycle_key),
            attempted_at_unix_secs: now_unix_secs,
            completed_at_unix_secs: Some(now_unix_secs),
            dispatch_kind,
            result: WarmupAttemptResult::PermanentFailure {
                reason,
                http_status: status,
                error_detail,
            },
        },
    )
    .await
    .map(|record| record.outcome)
}

async fn record_warmup_attempt(
    state: &AdminState,
    input: WarmupAttemptInput<'_>,
) -> Result<crate::ports::WarmupAttemptOutcomeSnapshot, UpstreamError> {
    let warmup_port = state
        .lifecycle
        .as_ref()
        .and_then(|ports| ports.warmup.as_deref())
        .ok_or(UpstreamError::WarmupUnavailable)?;

    Ok(warmup_port.record_attempt(input).await)
}

pub(super) fn upstream_base_url(upstream: &UpstreamRecord) -> Result<Url, UpstreamError> {
    if let Some(base_url) = upstream.base_url.as_ref() {
        return Ok(base_url.clone());
    }
    let default = match upstream.kind {
        UpstreamKind::AnthropicApiKey | UpstreamKind::AnthropicOauth => DEFAULT_ANTHROPIC_BASE_URL,
        UpstreamKind::OpenaiApiKey | UpstreamKind::OpenaiOauth => DEFAULT_OPENAI_BASE_URL,
        UpstreamKind::GeminiApiKey => DEFAULT_GEMINI_BASE_URL,
        UpstreamKind::GeminiOauth => DEFAULT_GEMINI_OAUTH_BASE_URL,
    };
    Url::parse(default)
        .map_err(|error| invalid_warmup_state(&format!("invalid default warmup URL: {error}")))
}

/// The configured `base_url` override for preflight requests, or the provider default host.
pub(super) fn effective_base_url(
    base_url: Option<&Url>,
    kind: UpstreamKind,
) -> Result<Url, UpstreamError> {
    if let Some(base_url) = base_url {
        return Ok(base_url.clone());
    }
    let default = match kind {
        UpstreamKind::AnthropicApiKey | UpstreamKind::AnthropicOauth => DEFAULT_ANTHROPIC_BASE_URL,
        UpstreamKind::OpenaiApiKey | UpstreamKind::OpenaiOauth => DEFAULT_OPENAI_BASE_URL,
        UpstreamKind::GeminiApiKey => DEFAULT_GEMINI_BASE_URL,
        UpstreamKind::GeminiOauth => DEFAULT_GEMINI_OAUTH_BASE_URL,
    };
    Url::parse(default)
        .map_err(|error| invalid_warmup_state(&format!("invalid default preflight URL: {error}")))
}

fn invalid_warmup_state(message: &str) -> UpstreamError {
    UpstreamError::Storage(StorageError::Fatal {
        message: message.to_owned(),
    })
}

async fn update_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Extension(identity): Extension<AdminIdentity>,
    Json(body): Json<UpstreamUpdateBody>,
) -> Result<Response, UpstreamError> {
    let expected_revision = parse_if_match(&headers)?;
    let current = find_upstream(&state, &id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if current.revision != expected_revision {
        return Err(UpstreamError::StaleRevision {
            current_revision: current.revision,
        });
    }
    if body.clear_base_url && body.base_url.is_some() {
        return Err(UpstreamError::BadRequest {
            error: "conflicting_base_url",
            detail: "provide either base_url or clear_base_url, not both".to_owned(),
        });
    }
    if body.clear_workspace_id && body.workspace_id.is_some() {
        return Err(UpstreamError::InvalidInput {
            field: "workspace_id".to_owned(),
            reason: "provide either workspace_id or clear_workspace_id, not both".to_owned(),
        });
    }
    if let Some(base_url) = body.base_url.as_ref() {
        validate_base_url_input(base_url)?;
    }
    if !matches!(
        current.kind,
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
    ) && (body.warmup_enabled == Some(true) || body.warmup_dialect_plugin.is_some())
    {
        return Err(UpstreamError::BadRequest {
            error: "warmup_unsupported_kind",
            detail: "warmup is supported only for OAuth upstreams".to_owned(),
        });
    }
    if body.warmup_enabled == Some(true)
        && matches!(
            current.kind,
            UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
        )
        && current.oauth_credentials.is_none()
    {
        return Err(UpstreamError::BadRequest {
            error: "warmup_requires_oauth_credentials",
            detail: "complete OAuth before enabling warmup".to_owned(),
        });
    }
    let storage = storage(&state)?;
    validate_warmup_plugin(storage, current.kind, body.warmup_dialect_plugin.as_ref()).await?;
    let imported_oauth_bundle = match current.kind {
        UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth => {
            if body.api_key_value.is_some() {
                if current.kind == UpstreamKind::GeminiOauth {
                    return Err(UpstreamError::BadRequest {
                        error: "invalid_gemini_oauth_credential",
                        detail: "Gemini OAuth accepts only oauth_tokens and project_id".to_owned(),
                    });
                }
                return Err(UpstreamError::BadRequest {
                    error: "unexpected_api_key",
                    detail: format!(
                        "{} upstreams do not accept api_key_value",
                        current.kind.as_str()
                    ),
                });
            }
            if body.workspace_id.is_some() || body.clear_workspace_id {
                return Err(if current.kind == UpstreamKind::GeminiOauth {
                    UpstreamError::BadRequest {
                        error: "invalid_gemini_oauth_credential",
                        detail: "Gemini OAuth accepts only oauth_tokens and project_id".to_owned(),
                    }
                } else {
                    UpstreamError::NotApiKeyUpstream
                });
            }
            if body.oauth_token_bundle.is_some() {
                prepare_imported_oauth_bundle(
                    current.kind,
                    body.oauth_token_bundle.clone(),
                    body.project_id.as_deref(),
                    cc_lb_clock::unix_secs(state.clock.now()),
                )?
            } else if current.kind == UpstreamKind::OpenaiOauth {
                if body.project_id.is_some() {
                    return Err(UpstreamError::BadRequest {
                        error: "unexpected_project_id",
                        detail: "OpenAI OAuth does not accept project_id".to_owned(),
                    });
                }
                None
            } else if let Some(raw_project_id) = body.project_id.as_deref() {
                let project_id = raw_project_id.trim();
                if project_id.is_empty() {
                    return Err(UpstreamError::BadRequest {
                        error: "missing_project_id",
                        detail: "Gemini OAuth project_id must be non-empty".to_owned(),
                    });
                }
                let mut bundle = current
                    .oauth_credentials
                    .as_ref()
                    .ok_or(UpstreamError::CredentialDecrypt)?
                    .decrypt(&state.aead, current.id.as_bytes())
                    .map_err(|_| UpstreamError::CredentialDecrypt)?;
                bundle.project_id = Some(project_id.to_owned());
                Some(bundle)
            } else {
                None
            }
        }
        _ => {
            if body.oauth_token_bundle.is_some()
                || (body.project_id.is_some()
                    && !matches!(
                        current.kind,
                        UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey
                    ))
            {
                return Err(UpstreamError::BadRequest {
                    error: "unexpected_oauth_tokens",
                    detail: "OAuth token import is supported only for OpenAI and Gemini OAuth"
                        .to_owned(),
                });
            }
            None
        }
    };
    if let Some(bundle) = imported_oauth_bundle {
        let mut fields_changed = changed_fields(&body);
        if body.oauth_token_bundle.is_some() {
            fields_changed.push("oauth_tokens");
        }
        if current.kind == UpstreamKind::GeminiOauth
            && (body.project_id.is_some() || body.oauth_token_bundle.is_some())
            && !fields_changed.contains(&"project_id")
        {
            fields_changed.push("project_id");
        }
        let target_base_url = if body.clear_base_url {
            None
        } else {
            body.base_url.clone().or_else(|| current.base_url.clone())
        };
        let base_url_written = body.base_url.is_some() || body.clear_base_url;
        let (old_host, new_host) = if base_url_written {
            (
                current.base_url.as_ref().map(audit_host),
                target_base_url.as_ref().map(audit_host),
            )
        } else {
            (None, None)
        };
        let encrypted = AeadEncryptedField::<OAuthTokenBundle>::encrypt(
            &state.aead,
            &bundle,
            current.id.as_bytes(),
        )
        .map_err(|_| UpstreamError::CredentialDecrypt)?;
        let updated = match UpstreamStore::update(
            storage,
            current.id,
            expected_revision,
            UpstreamUpdate {
                name: body.name,
                base_url: if body.clear_base_url {
                    Some(None)
                } else {
                    body.base_url.map(Some)
                },
                api_key_ciphertext: None,
                oauth_tokens: Some(OAuthTokensCreate {
                    tokens: encrypted,
                    never_refresh: bundle.never_refresh,
                }),
                oauth_token_generation: None,
                enabled: body.enabled,
                warmup_enabled: body.warmup_enabled,
                warmup_dialect_plugin: body.warmup_dialect_plugin,
                workspace_id: None,
                api_key_access: None,
                reset_api_key_poll_state: false,
            },
        )
        .await
        {
            Ok(updated) => updated,
            Err(StorageError::Conflict { message }) => {
                return stale_or_conflict(&state, &id, expected_revision, message).await;
            }
            Err(error) => return Err(error.into()),
        };
        let payload = AuditPayload::UpstreamUpdate {
            upstream_id: updated.id.to_string(),
            fields_changed,
            key_hint: None,
            verdict: None,
            remediation: Vec::new(),
            old_host,
            new_host,
        };
        let mut response = respond_with_etag(&updated, None);
        add_dynamic_rebind_headers(&mut response, &state).await;
        complete_after_commit(async move {
            if let Err(error) = super::oauth::seed_oauth_bootstrap_tasks(&state, &updated).await {
                super::oauth::rebuild_after_committed_failure(
                    &state,
                    "oauth import scheduler seeding",
                )
                .await;
                return Err(UpstreamError::from(error));
            }
            audit_upstream_action(
                &state,
                &identity,
                &payload,
                &format!("/admin/v1/upstreams/{}", updated.id),
                Some(&updated.name),
                StatusCode::OK,
            )
            .await?;
            Ok(response)
        })
        .await?
    } else {
        let target_base_url = if body.clear_base_url {
            None
        } else {
            body.base_url.clone().or_else(|| current.base_url.clone())
        };
        let api_key = match current.kind {
            UpstreamKind::AnthropicApiKey
            | UpstreamKind::OpenaiApiKey
            | UpstreamKind::GeminiApiKey => {
                if current.kind == UpstreamKind::AnthropicApiKey && body.project_id.is_some() {
                    return Err(UpstreamError::BadRequest {
                        error: "unexpected_oauth_tokens",
                        detail: "OAuth token import is supported only for OpenAI and Gemini OAuth"
                            .to_owned(),
                    });
                }
                let existing_details = match storage.get_api_key_details(current.id).await {
                    Ok(details) => details,
                    Err(error) => {
                        tracing::warn!(
                            upstream_id = %current.id,
                            ?error,
                            "admin upstream update proceeds without api-key details after a failed read"
                        );
                        None
                    }
                };
                prepare_api_key_update(
                    &state,
                    &current,
                    ApiKeyUpdateRequest {
                        api_key_value: body.api_key_value.as_deref(),
                        workspace_id: body.workspace_id.as_deref(),
                        clear_workspace_id: body.clear_workspace_id,
                        project_id: body.project_id.as_deref(),
                        existing_details: existing_details.as_ref(),
                        base_url: target_base_url.as_ref(),
                        skip_preflight: body.skip_preflight,
                    },
                )
                .await?
            }
            UpstreamKind::AnthropicOauth => {
                if body.api_key_value.is_some() {
                    return Err(UpstreamError::BadRequest {
                        error: "unexpected_api_key",
                        detail: format!(
                            "{} upstreams do not accept api_key_value",
                            current.kind.as_str()
                        ),
                    });
                }
                if body.workspace_id.is_some() || body.clear_workspace_id {
                    return Err(UpstreamError::NotApiKeyUpstream);
                }
                Default::default()
            }
            UpstreamKind::OpenaiOauth => {
                if body.api_key_value.is_some() {
                    return Err(UpstreamError::BadRequest {
                        error: "unexpected_api_key",
                        detail: format!(
                            "{} upstreams do not accept api_key_value",
                            current.kind.as_str()
                        ),
                    });
                }
                if body.workspace_id.is_some() || body.clear_workspace_id {
                    return Err(UpstreamError::NotApiKeyUpstream);
                }
                Default::default()
            }
            UpstreamKind::GeminiOauth => {
                if body.api_key_value.is_some() || body.workspace_id.is_some() {
                    return Err(UpstreamError::BadRequest {
                        error: "invalid_gemini_oauth_credential",
                        detail: "Gemini OAuth accepts only oauth_tokens and project_id".to_owned(),
                    });
                }
                Default::default()
            }
        };

        let mut fields_changed = changed_fields(&body);
        if api_key.workspace_id.is_some() && !fields_changed.contains(&"workspace_id") {
            fields_changed.push("workspace_id");
        }
        let api_key_ciphertext = api_key
            .new_key
            .as_ref()
            .map(|key| encrypt_plaintext_value(&state, key.as_str(), current.id.as_bytes()))
            .transpose()?;
        let base_url_written = body.base_url.is_some() || body.clear_base_url;
        let (old_host, new_host) = if base_url_written {
            (
                current.base_url.as_ref().map(audit_host),
                target_base_url.as_ref().map(audit_host),
            )
        } else {
            (None, None)
        };
        let check = api_key.check;
        let result = UpstreamStore::update(
            storage,
            current.id,
            expected_revision,
            UpstreamUpdate {
                name: body.name,
                base_url: if body.clear_base_url {
                    Some(None)
                } else {
                    body.base_url.map(Some)
                },
                api_key_ciphertext,
                oauth_tokens: None,
                oauth_token_generation: None,
                enabled: body.enabled,
                warmup_enabled: body.warmup_enabled,
                warmup_dialect_plugin: body.warmup_dialect_plugin,
                workspace_id: api_key.workspace_id,
                api_key_access: check.as_ref().map(|check| check.access.clone()),
                reset_api_key_poll_state: api_key.reset_poll_state,
            },
        )
        .await;
        match result {
            Ok(updated) => {
                let storage = storage_arc(&state)?;
                complete_after_commit(async move {
                    if let Err(error) = seed_warmup_if_toggled(&state, &current, &updated).await {
                        // The update is committed even though seeding failed;
                        // keep routing in step before returning the seed error.
                        if let Err(rebuild_error) =
                            super::apply_dynamic_view_after_mutation(&state).await
                        {
                            tracing::error!(
                                error = %rebuild_error,
                                "admin v1 dynamic view rebind deferred after warmup seeding failure"
                            );
                        }
                        return Err(error);
                    }
                    let payload = AuditPayload::UpstreamUpdate {
                        upstream_id: updated.id.to_string(),
                        fields_changed,
                        key_hint: check.as_ref().and_then(KeyCheck::key_hint),
                        verdict: check.as_ref().and_then(KeyCheck::verdict_label),
                        remediation: check
                            .as_ref()
                            .map(KeyCheck::remediation_labels)
                            .unwrap_or_default(),
                        old_host,
                        new_host,
                    };
                    let route = format!("/admin/v1/upstreams/{}", updated.id);
                    if let Err(error) = audit_upstream_action(
                        &state,
                        &identity,
                        &payload,
                        &route,
                        Some(&updated.name),
                        StatusCode::OK,
                    )
                    .await
                    {
                        rebuild_after_audit_failure(&state).await;
                        return Err(error);
                    }
                    if let Some(checked_at) = check.as_ref().and_then(KeyCheck::usage_reported_at) {
                        enqueue_usage_poll(&state, updated.id, updated.revision, checked_at).await;
                    }
                    let details = api_key_details_after_commit(storage.as_ref(), &updated).await;
                    let mut response = respond_with_etag(&updated, details.as_ref());
                    add_dynamic_rebind_headers(&mut response, &state).await;
                    Ok(key_handling_response(&updated, response))
                })
                .await?
            }
            Err(StorageError::Conflict { message }) => {
                stale_or_conflict(&state, &id, expected_revision, message).await
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Create and update answers for an API-key upstream follow a key check, so
/// they are never cacheable, like the preflight and re-check answers.
fn key_handling_response(record: &UpstreamRecord, response: Response) -> Response {
    if matches!(
        record.kind,
        UpstreamKind::AnthropicApiKey | UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey
    ) {
        no_store(response)
    } else {
        response
    }
}

/// Host part of a `base_url` for audit rows; never the path or userinfo.
fn audit_host(base_url: &Url) -> String {
    match (base_url.host_str(), base_url.port()) {
        (Some(host), Some(port)) => format!("{}://{host}:{port}", base_url.scheme()),
        (Some(host), None) => format!("{}://{host}", base_url.scheme()),
        (None, _) => base_url.scheme().to_owned(),
    }
}

async fn seed_warmup_if_toggled(
    state: &AdminState,
    before: &UpstreamRecord,
    after: &UpstreamRecord,
) -> Result<(), UpstreamError> {
    let toggled_on = !before.warmup_enabled && after.warmup_enabled;
    if !toggled_on {
        return Ok(());
    }
    if WarmupLane::lanes_for_kind(after.kind).is_empty() || after.oauth_credentials.is_none() {
        return Ok(());
    }
    let seed_secs = cc_lb_clock::unix_secs(state.clock.now());
    push_warmup_bootstrap_tasks(state, after, seed_secs)
        .await
        .map_err(Into::into)
}

/// Seeds one bootstrap warm-up task per lane the upstream's credential
/// supports. Conflicts mean the task already exists and are not errors.
pub(super) async fn push_warmup_bootstrap_tasks(
    state: &AdminState,
    upstream: &UpstreamRecord,
    seed_secs: u64,
) -> Result<(), SchedulerError> {
    let Some(scheduler) = state.scheduler.as_ref() else {
        return Ok(());
    };
    let lanes = WarmupLane::lanes_for_kind(upstream.kind);
    let bundle = if lanes.iter().any(|lane| *lane != WarmupLane::Primary) {
        super::upstream_warmup::decrypt_lane_bundle(state, upstream)
    } else {
        None
    };
    for task in warmup_bootstrap_tasks(upstream.kind, upstream.id, bundle.as_ref(), seed_secs) {
        match scheduler.push_adaptive_task(task).await {
            Ok(()) | Err(SchedulerError::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Bootstrap tasks for every lane of `kind` that `bundle` supports; a missing
/// or undecryptable credential seeds the primary lane alone.
fn warmup_bootstrap_tasks(
    kind: UpstreamKind,
    upstream_id: Uuid,
    bundle: Option<&OAuthTokenBundle>,
    seed_secs: u64,
) -> Vec<SchedulerPushTask<AdaptiveJob>> {
    WarmupLane::lanes_for_kind(kind)
        .iter()
        .copied()
        .filter(|lane| super::upstream_warmup::lane_supported_by_credential(kind, *lane, bundle))
        .map(|lane| {
            let job = UpstreamWarmupJob::new_for_lane(upstream_id, seed_secs, lane);
            SchedulerPushTask {
                args: AdaptiveJob::Warmup(job),
                idempotency_key: Some(job.bootstrap_idempotency_key(seed_secs)),
                run_at_unix_secs: Some(seed_secs),
                max_attempts: None,
            }
        })
        .collect()
}

async fn enable_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Extension(identity): Extension<AdminIdentity>,
) -> Result<Response, UpstreamError> {
    set_enabled(state, id, headers, true, identity).await
}

async fn disable_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Extension(identity): Extension<AdminIdentity>,
) -> Result<Response, UpstreamError> {
    set_enabled(state, id, headers, false, identity).await
}

async fn delete_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Extension(identity): Extension<AdminIdentity>,
) -> Result<Response, UpstreamError> {
    let expected_revision = parse_if_match(&headers)?;
    let current = find_upstream(&state, &id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if current.revision != expected_revision {
        return Err(UpstreamError::StaleRevision {
            current_revision: current.revision,
        });
    }

    let storage = storage(&state)?;
    let result = UpstreamStore::soft_delete(storage, current.id, expected_revision).await;
    match result {
        Ok(()) => {
            complete_after_commit(async move {
                let payload = AuditPayload::UpstreamDelete {
                    upstream_id: current.id.to_string(),
                };
                let action = payload.to_string();
                let route = format!("/admin/v1/upstreams/{}", current.id);
                if let Err(error) = record_admin_audit(
                    &state,
                    AdminAuditEvent {
                        identity: Some(&identity),
                        system_component: None,
                        action: &action,
                        route: &route,
                        target_principal_id: None,
                        target_upstream: Some(&current.name),
                        api_key_id: None,
                        status: StatusCode::NO_CONTENT.as_u16(),
                        payload: None,
                    },
                )
                .await
                {
                    tracing::error!(error = %error, action = %action, "admin audit write failed");
                    rebuild_after_audit_failure(&state).await;
                    return Err(UpstreamError::AuditWriteFailed);
                }
                let mut response = StatusCode::NO_CONTENT.into_response();
                add_dynamic_rebind_headers(&mut response, &state).await;
                Ok(response)
            })
            .await?
        }
        Err(StorageError::Conflict { message }) => {
            stale_or_conflict(&state, &id, expected_revision, message).await
        }
        Err(error) => Err(error.into()),
    }
}

async fn delete_upstream_warmup_dialect_plugin(
    State(state): State<AdminState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Extension(identity): Extension<AdminIdentity>,
) -> Result<Response, UpstreamError> {
    let expected_revision = parse_if_match(&headers)?;
    let storage = storage(&state)?;
    UpstreamStore::get_by_id(storage, id)
        .await?
        .ok_or(UpstreamError::NotFound)?;

    let cleared = storage
        .clear_warmup_dialect_plugin(id, expected_revision)
        .await?;
    match cleared {
        Some(updated) => {
            let storage = storage_arc(&state)?;
            complete_after_commit(async move {
                let payload = AuditPayload::UpstreamUpdate {
                    upstream_id: updated.id.to_string(),
                    fields_changed: vec!["warmup_dialect_plugin"],
                    key_hint: None,
                    verdict: None,
                    remediation: Vec::new(),
                    old_host: None,
                    new_host: None,
                };
                let action = payload.to_string();
                let route = format!("/admin/v1/upstreams/{}/warmup-dialect-plugin", updated.id);
                if let Err(error) = record_admin_audit(
                    &state,
                    AdminAuditEvent {
                        identity: Some(&identity),
                        system_component: None,
                        action: &action,
                        route: &route,
                        target_principal_id: None,
                        target_upstream: Some(&updated.name),
                        api_key_id: None,
                        status: StatusCode::OK.as_u16(),
                        payload: None,
                    },
                )
                .await
                {
                    tracing::error!(error = %error, action = %action, "admin audit write failed");
                    rebuild_after_audit_failure(&state).await;
                    return Err(UpstreamError::AuditWriteFailed);
                }
                let details = api_key_details_after_commit(storage.as_ref(), &updated).await;
                let mut response = respond_with_etag(&updated, details.as_ref());
                add_dynamic_rebind_headers(&mut response, &state).await;
                Ok(response)
            })
            .await?
        }
        None => {
            stale_or_conflict(
                &state,
                &id.to_string(),
                expected_revision,
                "warmup dialect plugin clear conflicted".to_owned(),
            )
            .await
        }
    }
}

async fn set_enabled(
    state: AdminState,
    id: String,
    headers: HeaderMap,
    enabled: bool,
    identity: AdminIdentity,
) -> Result<Response, UpstreamError> {
    let storage = storage(&state)?;
    let expected_revision = parse_if_match(&headers)?;
    let Ok(id) = id.parse() else {
        return Err(UpstreamError::BadRequest {
            error: "invalid_upstream_id",
            detail: "invalid uuid".to_owned(),
        });
    };

    let updated = match UpstreamStore::set_enabled(storage, id, expected_revision, enabled).await {
        Ok(updated) => updated,
        Err(StorageError::Conflict { message }) => {
            return stale_or_conflict(&state, &id.to_string(), expected_revision, message).await;
        }
        Err(error) => return Err(error.into()),
    };

    let payload = if enabled {
        AuditPayload::UpstreamEnable {
            upstream_id: updated.id.to_string(),
        }
    } else {
        AuditPayload::UpstreamDisable {
            upstream_id: updated.id.to_string(),
        }
    };
    let action = payload.to_string();
    let route = if enabled {
        format!("/admin/v1/upstreams/{}/enable", updated.id)
    } else {
        format!("/admin/v1/upstreams/{}/disable", updated.id)
    };
    let storage = storage_arc(&state)?;
    complete_after_commit(async move {
        if let Err(error) = record_admin_audit(
            &state,
            AdminAuditEvent {
                identity: Some(&identity),
                system_component: None,
                action: &action,
                route: &route,
                target_principal_id: None,
                target_upstream: Some(&updated.name),
                api_key_id: None,
                status: StatusCode::OK.as_u16(),
                payload: None,
            },
        )
        .await
        {
            tracing::error!(error = %error, action = %action, "admin audit write failed");
            rebuild_after_audit_failure(&state).await;
            return Err(UpstreamError::AuditWriteFailed);
        }

        let details = api_key_details_after_commit(storage.as_ref(), &updated).await;
        let mut response = respond_with_etag(&updated, details.as_ref());
        crate::v1::add_dynamic_rebind_headers(&mut response, &state).await;
        Ok(response)
    })
    .await?
}

async fn stale_or_conflict(
    state: &AdminState,
    id: &str,
    expected_revision: u64,
    detail: String,
) -> Result<Response, UpstreamError> {
    let current = find_upstream(state, id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if current.revision != expected_revision {
        Err(UpstreamError::StaleRevision {
            current_revision: current.revision,
        })
    } else {
        Err(UpstreamError::Conflict { detail })
    }
}

fn respond_with_etag(
    record: &UpstreamRecord,
    details: Option<&UpstreamApiKeyDetailsRecord>,
) -> Response {
    let mut response = Json(upstream_response(record, details)).into_response();
    if let Ok(etag) = etag_value(record.revision) {
        response.headers_mut().insert(ETAG, etag);
    }
    response
}

pub(super) fn storage(state: &AdminState) -> Result<&dyn Storage, UpstreamError> {
    state
        .storage
        .as_deref()
        .ok_or(UpstreamError::StorageUnavailable)
}

/// The details row of an API-key upstream; `None` for OAuth upstreams.
async fn api_key_details(
    storage: &dyn Storage,
    record: &UpstreamRecord,
) -> Result<Option<UpstreamApiKeyDetailsRecord>, UpstreamError> {
    if !matches!(
        record.kind,
        UpstreamKind::AnthropicApiKey | UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey
    ) {
        return Ok(None);
    }
    Ok(UpstreamApiKeyStore::get_api_key_details(storage, record.id).await?)
}

/// Post-commit read of the details row. The mutation and its audit row are
/// already durable, so a transient read failure drops only the display-only
/// `api_key_access`/`key_hint` fields of this response; the response is still
/// built and the serving-view rebuild still runs.
async fn api_key_details_after_commit(
    storage: &dyn Storage,
    record: &UpstreamRecord,
) -> Option<UpstreamApiKeyDetailsRecord> {
    match api_key_details(storage, record).await {
        Ok(details) => details,
        Err(error) => {
            tracing::warn!(
                upstream_id = %record.id,
                ?error,
                "admin upstream response omits api-key details after a failed read"
            );
            None
        }
    }
}

/// Writes the audit row of an admin upstream action. `payload` is stored
/// both as the action text and as JSON; neither carries key material.
pub(super) async fn audit_upstream_action(
    state: &AdminState,
    identity: &AdminIdentity,
    payload: &AuditPayload,
    route: &str,
    target_upstream: Option<&str>,
    status: StatusCode,
) -> Result<(), UpstreamError> {
    let action = payload.to_string();
    let payload_json = serde_json::to_value(payload).ok();
    if let Err(error) = record_admin_audit(
        state,
        AdminAuditEvent {
            identity: Some(identity),
            system_component: None,
            action: &action,
            route,
            target_principal_id: None,
            target_upstream,
            api_key_id: None,
            status: status.as_u16(),
            payload: payload_json,
        },
    )
    .await
    {
        tracing::error!(error = %error, action = %action, "admin audit write failed");
        return Err(UpstreamError::AuditWriteFailed);
    }
    Ok(())
}

pub(super) fn storage_arc(state: &AdminState) -> Result<Arc<dyn Storage>, UpstreamError> {
    state
        .storage
        .clone()
        .ok_or(UpstreamError::StorageUnavailable)
}

async fn subscription_metadata_response(
    storage: &dyn Storage,
    upstream_id: Uuid,
) -> Result<SubscriptionMetadataResponse, UpstreamError> {
    let subscription_metadata = storage
        .get_upstream_subscription_metadata(upstream_id)
        .await?;
    let organization_metadata = match subscription_metadata
        .as_ref()
        .and_then(|metadata| metadata.organization_uuid.as_deref())
    {
        Some(organization_uuid) => storage.get_organization_metadata(organization_uuid).await?,
        None => None,
    };
    Ok(SubscriptionMetadataResponse {
        upstream_id: upstream_id.to_string(),
        // Provider display identity is attached only after binding it to the
        // current decrypted credential in the read handler.
        account_identity: OAuthAccountIdentity::default(),
        subscription_metadata,
        organization_metadata,
    })
}

pub(super) async fn fresh_enough_access_token(
    state: &AdminState,
    storage: Arc<dyn Storage>,
    upstream: &UpstreamRecord,
    bundle: OAuthTokenBundle,
) -> Result<String, UpstreamError> {
    let now = cc_lb_clock::unix_secs(state.clock.now());
    if bundle.never_refresh {
        // Long-lived (365-day) credentials must never be refreshed: Anthropic
        // revokes the long-lived grant and returns a short-lived token, so the
        // stored refresh token is retained but never used. An expired
        // long-lived credential can only be recovered by reauthorizing.
        return if bundle.expires_at_unix_secs > now {
            Ok(bundle.access_token)
        } else {
            Err(UpstreamError::RefreshFailed {
                detail: "long-lived oauth credential expired; reauthorization is required"
                    .to_owned(),
            })
        };
    }
    if bundle.expires_at_unix_secs > now.saturating_add(METADATA_REFRESH_LOOKAHEAD_SECS) {
        return Ok(bundle.access_token);
    }
    let refresher = state
        .lazy_refresher
        .as_ref()
        .ok_or(UpstreamError::RefreshUnavailable)?;
    refresher
        .refresh_one(upstream.id)
        .await
        .map_err(|error| UpstreamError::RefreshFailed {
            detail: error.to_string(),
        })?;
    let upstream = UpstreamStore::get_by_id(storage.as_ref(), upstream.id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    Ok(decrypt_oauth_bundle(state, &upstream)?.access_token)
}

pub(super) fn decrypt_oauth_bundle(
    state: &AdminState,
    upstream: &UpstreamRecord,
) -> Result<OAuthTokenBundle, UpstreamError> {
    upstream
        .oauth_credentials
        .as_ref()
        .ok_or(UpstreamError::NotOauthUpstream)?
        .decrypt(state.aead.as_ref(), upstream.id.as_bytes())
        .map_err(|_| UpstreamError::CredentialDecrypt)
}

pub(super) async fn metadata_user_agent(storage: &dyn Storage) -> Result<String, UpstreamError> {
    let version = storage
        .get_compatibility_kv(CLAUDE_CODE_STABLE_VERSION_KEY)
        .await?
        .map(|record| record.value)
        .unwrap_or_else(|| CLAUDE_CODE_STABLE_VERSION_FALLBACK.to_owned());
    Ok(claude_code_user_agent(&version))
}

fn upstream_response(
    record: &UpstreamRecord,
    details: Option<&UpstreamApiKeyDetailsRecord>,
) -> UpstreamResponse {
    let api_key_access = api_key_access_response(record, details);
    let key_hint = api_key_access
        .as_ref()
        .and(details)
        .map(|details| details.key_hint.clone())
        .filter(|hint| !hint.is_empty());
    UpstreamResponse {
        id: record.id.to_string(),
        name: record.name.clone(),
        kind: record.kind,
        base_url: record.base_url.clone(),
        enabled: record.enabled,
        warmup_enabled: record.warmup_enabled,
        warmup_dialect_plugin: record.warmup_dialect_plugin.clone(),
        spec_revision: record.revision,
        status: UpstreamStatusResponse {
            last_apply_error: record.last_apply_error.clone(),
            last_apply_at_unix_secs: record.last_apply_at_unix_secs,
            last_warmup_at_unix_secs: record.last_warmup_at_unix_secs,
        },
        api_key_access,
        key_hint,
    }
}

fn parse_if_match(headers: &HeaderMap) -> Result<u64, UpstreamError> {
    let value = headers.get(IF_MATCH).ok_or(UpstreamError::MissingIfMatch)?;
    let raw = value.to_str().map_err(|error| UpstreamError::BadRequest {
        error: "invalid_if_match",
        detail: error.to_string(),
    })?;
    let trimmed = raw.trim();
    let revision = trimmed
        .strip_prefix("W/\"")
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            trimmed
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
        })
        .unwrap_or(trimmed);
    revision
        .parse::<u64>()
        .map_err(|error| UpstreamError::BadRequest {
            error: "invalid_if_match",
            detail: error.to_string(),
        })
}

fn etag_value(revision: u64) -> Result<HeaderValue, UpstreamError> {
    HeaderValue::from_str(&format!("W/\"{revision}\"")).map_err(|error| UpstreamError::BadRequest {
        error: "invalid_etag",
        detail: error.to_string(),
    })
}

fn encrypt_plaintext_value(
    state: &AdminState,
    plaintext: &str,
    aad: &[u8],
) -> Result<Vec<u8>, UpstreamError> {
    state
        .aead
        .encrypt(plaintext.as_bytes(), aad)
        .map_err(|error| UpstreamError::BadRequest {
            error: "credential_encryption_failed",
            detail: error.to_string(),
        })
}

fn changed_fields(body: &UpstreamUpdateBody) -> Vec<&'static str> {
    let mut fields = Vec::new();
    if body.name.is_some() {
        fields.push("name");
    }
    if body.base_url.is_some() || body.clear_base_url {
        fields.push("base_url");
    }
    if body.api_key_value.is_some() {
        fields.push("api_key_ciphertext");
    }
    if body.project_id.is_some() {
        fields.push("project_id");
    }
    if body.enabled.is_some() {
        fields.push("enabled");
    }
    if body.warmup_enabled.is_some() {
        fields.push("warmup_enabled");
    }
    if body.warmup_dialect_plugin.is_some() {
        fields.push("warmup_dialect_plugin");
    }
    fields
}

async fn find_upstream(
    state: &AdminState,
    id: &str,
) -> Result<Option<UpstreamRecord>, UpstreamError> {
    Ok(list_all_upstreams(state)
        .await?
        .into_iter()
        .find(|record| record.id.to_string() == id))
}

pub(super) async fn list_all_upstreams(
    state: &AdminState,
) -> Result<Vec<UpstreamRecord>, UpstreamError> {
    let storage = storage(state)?;
    let mut after = None;
    let mut all = Vec::new();
    loop {
        let page = UpstreamStore::list(storage, after, STORE_PAGE_LIMIT).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|record| record.id);
        let page_len = page.len();
        all.extend(page);
        if page_len < STORE_PAGE_LIMIT {
            break;
        }
    }
    Ok(all)
}

fn unix_now_secs_i64(clock: &dyn Clock) -> Result<i64, UpstreamError> {
    i64::try_from(cc_lb_clock::unix_secs(clock.now()))
        .map_err(|_| invalid_warmup_state("current timestamp overflow"))
}

fn unix_now_millis(clock: &dyn Clock) -> Result<u64, UpstreamError> {
    cc_lb_clock::unix_secs(clock.now())
        .checked_mul(1_000)
        .ok_or_else(|| invalid_warmup_state("current timestamp millis overflow"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use axum::body::{Body as AxumBody, to_bytes};
    use axum::http::{Method, Request};
    use cc_lb_aead::{AeadEncryptedField, AeadService};
    use cc_lb_config::Config;
    use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
    use cc_lb_control::api_keys::limit_engine::LimitEngine;
    use cc_lb_control::api_keys::principal_view::PrincipalView;
    use cc_lb_control::{
        DynamicView, DynamicViewBuilder, DynamicViewHolder, UpstreamStatusSnapshot,
    };
    use cc_lb_domain::Upstream;
    use cc_lb_storage_api::{
        MetaStore, OAuthTokensCreate, SubscriptionQuotaSampleKind, UpstreamStore,
        UpstreamSubscriptionQuotaStore,
    };
    use cc_lb_upstream::{
        ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError,
        SignerFactory, SigningCapability, UpstreamError,
    };
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    use super::*;
    use serde_json::Value;

    type TestStorage = cc_lb_storage_sqlite::SqliteStorage;

    async fn error_response_json(error: super::UpstreamError) -> (StatusCode, Value) {
        let response = error.into_response();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("error body collects");
        (
            status,
            serde_json::from_slice(&bytes).expect("error body is JSON"),
        )
    }

    #[tokio::test]
    async fn remediation_error_renders_nested_body_with_its_status() {
        let error = super::UpstreamError::Remediation {
            status: StatusCode::BAD_REQUEST,
            code: RemediationCode::WorkspaceRequired,
            message: "choose a workspace".to_owned(),
        };
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.error_code(), "workspace_required");
        assert_eq!(
            error_response_json(error).await,
            (
                StatusCode::BAD_REQUEST,
                json!({ "error": { "code": "workspace_required", "message": "choose a workspace" } })
            )
        );
    }

    #[tokio::test]
    async fn api_key_upstream_errors_render_flat_bodies() {
        let not_api_key = super::UpstreamError::NotApiKeyUpstream;
        assert_eq!(not_api_key.error_code(), "not_api_key_upstream");
        assert_eq!(
            error_response_json(not_api_key).await,
            (
                StatusCode::BAD_REQUEST,
                json!({ "error": "not_api_key_upstream" })
            )
        );
        let decrypt = super::UpstreamError::ApiKeyCredentialDecrypt;
        assert_eq!(decrypt.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(decrypt.error_code(), "api_key_credential_decrypt_failed");
        assert_eq!(
            error_response_json(decrypt).await,
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "api_key_credential_decrypt_failed" })
            )
        );
    }

    /// SEC-09: Debug of the create/update bodies never prints the key.
    #[test]
    fn sec_09_debug_redacts_upstream_bodies() {
        let update: UpstreamUpdateBody =
            serde_json::from_value(json!({ "api_key_value": "sk-ant-api03-SECRETVALUE1234" }))
                .expect("update body parses");
        let create: UpstreamCreateBody = serde_json::from_value(json!({
            "name": "primary",
            "kind": "anthropic_api_key",
            "api_key_value": "sk-ant-api03-SECRETVALUE1234",
        }))
        .expect("create body parses");
        for rendered in [format!("{update:?}"), format!("{create:?}")] {
            assert!(!rendered.contains("SECRETVALUE"), "{rendered}");
            assert!(rendered.contains("[REDACTED]"), "{rendered}");
        }
    }

    struct TestContext {
        _dir: tempfile::TempDir,
        state: AdminState,
        storage: Arc<TestStorage>,
        aead: Arc<AeadService>,
        /// `(lane, outcome)` of every attempt recorded through the warmup port.
        recorded_attempts: Arc<std::sync::Mutex<Vec<(WarmupLane, WarmupAttemptOutcome)>>>,
        /// Lane of every provider dispatch (native or dialect).
        dispatched_lanes: Arc<std::sync::Mutex<Vec<WarmupLane>>>,
    }

    struct TestSignerFactory;

    impl ApiKeyAwareSignerFactory for TestSignerFactory {
        fn with_router_choice(
            &self,
            _router_chosen_upstream_name: String,
        ) -> Arc<dyn SignerFactory> {
            Arc::new(TestSignerFactory)
        }
    }

    #[async_trait]
    impl SignerFactory for TestSignerFactory {
        async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
            Ok(Arc::new(TestSigner))
        }
    }

    struct TestSigner;

    #[async_trait]
    impl Signer for TestSigner {
        async fn sign(
            &self,
            shaped: ShapedRequest,
            capability: &mut SigningCapability,
        ) -> Result<SignedRequest, SignerError> {
            Ok(SignedRequest::from_shaped(shaped, capability))
        }

        async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
            RetryDecision::Fail
        }
    }

    #[derive(Default)]
    struct TestWarmupPort {
        recorded: Arc<std::sync::Mutex<Vec<(WarmupLane, WarmupAttemptOutcome)>>>,
    }

    #[async_trait]
    impl crate::ports::WarmupPort for TestWarmupPort {
        async fn record_attempt(
            &self,
            input: crate::ports::WarmupAttemptInput<'_>,
        ) -> crate::ports::WarmupAttemptOutcomeSnapshot {
            let (outcome, cycle_key, error_detail) = match input.result {
                crate::ports::WarmupAttemptResult::Response {
                    status,
                    error_detail,
                    ..
                } if status.is_success() => (
                    WarmupAttemptOutcome::Success(
                        cc_lb_storage_api::WarmupSuccessReason::CycleAdvanced,
                    ),
                    input.expected_cycle_key,
                    error_detail.map(str::to_owned),
                ),
                crate::ports::WarmupAttemptResult::Response {
                    status,
                    error_detail,
                    ..
                } if status == StatusCode::UNAUTHORIZED => (
                    WarmupAttemptOutcome::PermanentFailure(
                        WarmupPermanentFailureReason::AuthFailed,
                    ),
                    input.expected_cycle_key,
                    error_detail.map(str::to_owned),
                ),
                crate::ports::WarmupAttemptResult::Response { error_detail, .. } => (
                    WarmupAttemptOutcome::TransientFailure(
                        WarmupTransientFailureReason::Upstream5xx,
                    ),
                    input.expected_cycle_key,
                    error_detail.map(str::to_owned),
                ),
                crate::ports::WarmupAttemptResult::TransientFailure {
                    reason,
                    error_detail,
                    ..
                } => (
                    WarmupAttemptOutcome::TransientFailure(reason),
                    input.expected_cycle_key,
                    error_detail.map(str::to_owned),
                ),
                crate::ports::WarmupAttemptResult::PermanentFailure {
                    reason,
                    error_detail,
                    ..
                } => (
                    WarmupAttemptOutcome::PermanentFailure(reason),
                    input.expected_cycle_key,
                    error_detail.map(str::to_owned),
                ),
                crate::ports::WarmupAttemptResult::Skipped {
                    reason,
                    cycle_key,
                    error_detail,
                } => (
                    WarmupAttemptOutcome::Skipped(reason),
                    cycle_key,
                    error_detail.map(str::to_owned),
                ),
            };
            self.recorded
                .lock()
                .expect("recorded attempts lock")
                .push((input.lane, outcome));
            crate::ports::WarmupAttemptOutcomeSnapshot {
                outcome,
                cycle_key,
                error_detail,
            }
        }
    }

    struct TestNativeDispatcher {
        status: StatusCode,
        lanes: Arc<std::sync::Mutex<Vec<WarmupLane>>>,
    }

    #[async_trait]
    impl crate::WarmupDialectDispatcher for TestNativeDispatcher {
        async fn dispatch_warmup_with_dialect(
            &self,
            _runtime: &Arc<cc_lb_runtime_wasmtime::WasmtimeRuntime>,
            _data_dir: &std::path::Path,
            _upstream: &UpstreamRecord,
            lane: WarmupLane,
        ) -> Result<crate::WarmupDialectDispatchOutcome, crate::WarmupDialectDispatchError>
        {
            self.lanes.lock().expect("dispatched lanes lock").push(lane);
            Ok(crate::WarmupDialectDispatchOutcome {
                status: self.status,
                headers: HeaderMap::new(),
            })
        }

        async fn dispatch_warmup_native(
            &self,
            upstream: &UpstreamRecord,
            lane: WarmupLane,
        ) -> Result<WarmupNativeDispatchOutcome, crate::WarmupDialectDispatchError> {
            assert!(matches!(
                upstream.kind,
                UpstreamKind::AnthropicOauth
                    | UpstreamKind::OpenaiOauth
                    | UpstreamKind::GeminiOauth
            ));
            self.lanes.lock().expect("dispatched lanes lock").push(lane);
            Ok(WarmupNativeDispatchOutcome::Response {
                status: self.status,
                dispatch_kind: WarmupDispatchKind::Http,
                observations: Vec::new(),
                error_detail: None,
            })
        }
    }

    #[tokio::test]
    async fn fire_now_native_dispatches_openai_oauth_through_bridge() {
        let context = test_context_with_dispatcher(StatusCode::OK).await;
        let mut upstream =
            create_oauth_upstream(context.storage.as_ref(), context.aead.as_ref(), true, None)
                .await;
        upstream.kind = UpstreamKind::OpenaiOauth;
        let stage = fire_now_upstream_warmup_inner(
            &context.state,
            context.storage.clone(),
            upstream.id,
            upstream,
            WarmupLane::Primary,
        )
        .await
        .expect("native bridge dispatch");
        assert!(matches!(stage, FireNowStage::Attempted(_)));
    }

    #[tokio::test]
    async fn fire_now_rejects_non_oauth_400() {
        let context = test_context().await;
        let upstream = create_api_key_upstream(context.storage.as_ref(), true).await;

        let (status, body) = fire_now_response(context.state, upstream.id).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "warmup_unsupported_for_kind");
        assert_eq!(body["kind"], "anthropic_api_key");
    }

    #[tokio::test]
    async fn fire_now_rejects_warmup_disabled_400() {
        let context = test_context().await;
        let upstream =
            create_oauth_upstream(context.storage.as_ref(), context.aead.as_ref(), false, None)
                .await;

        let (status, body) = fire_now_response(context.state, upstream.id).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "warmup_disabled");
    }

    #[tokio::test]
    async fn fire_now_returns_503_without_dispatcher() {
        let context = test_context().await;
        let upstream =
            create_oauth_upstream(context.storage.as_ref(), context.aead.as_ref(), true, None)
                .await;
        let (status, body) = fire_now_response(context.state, upstream.id).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "warmup_unavailable");
    }

    #[tokio::test]
    async fn fire_now_returns_200_on_success() {
        let context = test_context_with_dispatcher(StatusCode::OK).await;
        let upstream =
            create_oauth_upstream(context.storage.as_ref(), context.aead.as_ref(), true, None)
                .await;

        let (status, body) = fire_now_response(context.state, upstream.id).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["fired"], true);
        assert_eq!(body["lane"], "primary");
        assert!(body["cycle_key"].as_i64().is_some());
        assert_eq!(
            *context.dispatched_lanes.lock().expect("lock"),
            vec![WarmupLane::Primary]
        );
        assert_eq!(
            context.recorded_attempts.lock().expect("lock")[0].0,
            WarmupLane::Primary
        );
    }

    #[tokio::test]
    async fn fire_now_rejects_unknown_lane_400() {
        let context = test_context_with_dispatcher(StatusCode::OK).await;
        let upstream =
            create_oauth_upstream(context.storage.as_ref(), context.aead.as_ref(), true, None)
                .await;

        let (status, body) =
            fire_now_response_for_lane(context.state.clone(), upstream.id, Some("gpt_oss")).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "invalid_warmup_lane");
        assert!(context.dispatched_lanes.lock().expect("lock").is_empty());
        assert!(context.recorded_attempts.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn fire_now_rejects_third_party_lane_for_anthropic_400() {
        let context = test_context_with_dispatcher(StatusCode::OK).await;
        let upstream =
            create_oauth_upstream(context.storage.as_ref(), context.aead.as_ref(), true, None)
                .await;

        let (status, body) =
            fire_now_response_for_lane(context.state.clone(), upstream.id, Some("third_party"))
                .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "warmup_lane_unsupported");
        assert_eq!(body["kind"], "anthropic_oauth");
        assert_eq!(body["lane"], "third_party");
        assert!(context.dispatched_lanes.lock().expect("lock").is_empty());
        assert!(context.recorded_attempts.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn fire_now_third_party_lane_skips_gemini_cli_profile_without_dispatch() {
        let context = test_context_with_dispatcher(StatusCode::OK).await;
        let upstream = create_gemini_oauth_upstream(
            context.storage.as_ref(),
            context.aead.as_ref(),
            cc_lb_oauth_protocol::GeminiOAuthProfile::GeminiCli,
        )
        .await;

        let (status, body) =
            fire_now_response_for_lane(context.state.clone(), upstream.id, Some("third_party"))
                .await;

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body["fired"], false);
        assert_eq!(body["lane"], "third_party");
        assert_eq!(body["status"], "skipped");
        assert_eq!(body["reason"], "quota_group_unavailable");
        assert!(context.dispatched_lanes.lock().expect("lock").is_empty());
        assert_eq!(
            *context.recorded_attempts.lock().expect("lock"),
            vec![(
                WarmupLane::ThirdParty,
                WarmupAttemptOutcome::Skipped(WarmupSkipReason::QuotaGroupUnavailable)
            )]
        );
    }

    #[tokio::test]
    async fn fire_now_third_party_lane_skips_when_every_target_window_absent() {
        let context = test_context_with_dispatcher(StatusCode::OK).await;
        let upstream = create_gemini_oauth_upstream(
            context.storage.as_ref(),
            context.aead.as_ref(),
            cc_lb_oauth_protocol::GeminiOAuthProfile::Antigravity,
        )
        .await;
        context
            .storage
            .record_subscription_quota_samples(&[
                quota_sample(
                    upstream.id,
                    SubscriptionQuotaWindow::ThirdPartyFiveHour,
                    SubscriptionQuotaSampleKind::Absent,
                ),
                quota_sample(
                    upstream.id,
                    SubscriptionQuotaWindow::ThirdPartySevenDay,
                    SubscriptionQuotaSampleKind::Absent,
                ),
            ])
            .await
            .expect("quota samples record");

        let (status, body) =
            fire_now_response_for_lane(context.state.clone(), upstream.id, Some("third_party"))
                .await;

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body["reason"], "quota_group_unavailable");
        assert!(context.dispatched_lanes.lock().expect("lock").is_empty());

        // The primary lane is unaffected by third-party absence.
        let (status, body) =
            fire_now_response_for_lane(context.state.clone(), upstream.id, Some("primary")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["lane"], "primary");
        assert_eq!(
            *context.dispatched_lanes.lock().expect("lock"),
            vec![WarmupLane::Primary]
        );
    }

    #[tokio::test]
    async fn fire_now_third_party_lane_dispatches_antigravity_with_lane() {
        let context = test_context_with_dispatcher(StatusCode::OK).await;
        let upstream = create_gemini_oauth_upstream(
            context.storage.as_ref(),
            context.aead.as_ref(),
            cc_lb_oauth_protocol::GeminiOAuthProfile::Antigravity,
        )
        .await;
        // One reported window keeps the group eligible.
        context
            .storage
            .record_subscription_quota_samples(&[quota_sample(
                upstream.id,
                SubscriptionQuotaWindow::ThirdPartySevenDay,
                SubscriptionQuotaSampleKind::Absent,
            )])
            .await
            .expect("quota samples record");

        let (status, body) =
            fire_now_response_for_lane(context.state.clone(), upstream.id, Some("third_party"))
                .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["fired"], true);
        assert_eq!(body["lane"], "third_party");
        assert_eq!(
            *context.dispatched_lanes.lock().expect("lock"),
            vec![WarmupLane::ThirdParty]
        );
        assert_eq!(
            context.recorded_attempts.lock().expect("lock")[0].0,
            WarmupLane::ThirdParty
        );
    }

    #[tokio::test]
    async fn fire_now_third_party_cycle_key_uses_lane_five_hour_reset() {
        let context = test_context_with_dispatcher(StatusCode::OK).await;
        let upstream = create_gemini_oauth_upstream(
            context.storage.as_ref(),
            context.aead.as_ref(),
            cc_lb_oauth_protocol::GeminiOAuthProfile::Antigravity,
        )
        .await;
        let mut gemini = quota_sample(
            upstream.id,
            SubscriptionQuotaWindow::GeminiFiveHour,
            SubscriptionQuotaSampleKind::Sample,
        );
        gemini.resets_at_unix_secs = Some(4_000_000_000);
        let mut third_party = quota_sample(
            upstream.id,
            SubscriptionQuotaWindow::ThirdPartyFiveHour,
            SubscriptionQuotaSampleKind::Sample,
        );
        third_party.resets_at_unix_secs = Some(4_000_000_777);
        context
            .storage
            .record_subscription_quota_samples(&[gemini, third_party])
            .await
            .expect("quota samples record");

        let (status, body) =
            fire_now_response_for_lane(context.state.clone(), upstream.id, Some("third_party"))
                .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["cycle_key"], 4_000_000_777i64);

        let (status, body) =
            fire_now_response_for_lane(context.state.clone(), upstream.id, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["lane"], "primary");
        assert_eq!(body["cycle_key"], 4_000_000_000i64);
    }

    #[tokio::test]
    async fn fire_now_anthropic_dispatches_even_when_both_windows_absent() {
        let context = test_context_with_dispatcher(StatusCode::OK).await;
        let upstream =
            create_oauth_upstream(context.storage.as_ref(), context.aead.as_ref(), true, None)
                .await;
        context
            .storage
            .record_subscription_quota_samples(&[
                quota_sample(
                    upstream.id,
                    SubscriptionQuotaWindow::FiveHour,
                    SubscriptionQuotaSampleKind::Absent,
                ),
                quota_sample(
                    upstream.id,
                    SubscriptionQuotaWindow::SevenDay,
                    SubscriptionQuotaSampleKind::Absent,
                ),
            ])
            .await
            .expect("quota samples record");

        let (status, body) = fire_now_response(context.state.clone(), upstream.id).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["fired"], true);
        assert_eq!(body["lane"], "primary");
        assert_eq!(
            *context.dispatched_lanes.lock().expect("lock"),
            vec![WarmupLane::Primary]
        );
    }

    async fn create_gemini_oauth_upstream(
        storage: &TestStorage,
        aead: &AeadService,
        profile: cc_lb_oauth_protocol::GeminiOAuthProfile,
    ) -> UpstreamRecord {
        let id = Uuid::new_v4();
        let bundle = OAuthTokenBundle {
            oauth_profile: profile,
            access_token: "test-access-token".to_owned(),
            refresh_token: "test-refresh-token".to_owned(),
            expires_at_unix_secs: u64::MAX / 2,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: Some("test-project".to_owned()),
            scopes: Vec::new(),
            never_refresh: false,
        };
        let tokens = AeadEncryptedField::<OAuthTokenBundle>::encrypt(aead, &bundle, id.as_bytes())
            .expect("token encryption succeeds");
        storage
            .create(UpstreamCreate {
                id,
                name: format!("gemini-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::GeminiOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: Some(OAuthTokensCreate {
                    tokens,
                    never_refresh: false,
                }),
                warmup_enabled: true,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            })
            .await
            .expect("upstream create succeeds")
    }

    fn quota_sample(
        upstream_id: Uuid,
        window: SubscriptionQuotaWindow,
        sample_kind: SubscriptionQuotaSampleKind,
    ) -> SubscriptionQuotaSample {
        let observed_at_unix_millis =
            u64::try_from(cc_lb_clock::unix_millis(std::time::SystemTime::now()))
                .expect("now fits u64");
        SubscriptionQuotaSample {
            upstream_id,
            window,
            source: cc_lb_storage_api::SubscriptionQuotaSource::Api,
            sample_kind,
            observed_at_unix_millis,
            sample_id: Uuid::new_v4(),
            utilization: (sample_kind == SubscriptionQuotaSampleKind::Sample).then_some(0.0),
            status: None,
            resets_at_unix_secs: None,
            surpassed_threshold: None,
            representative_claim: None,
            fallback_percentage: None,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            credits: None,
            ingested_at_unix_millis: observed_at_unix_millis,
        }
    }

    #[tokio::test]
    async fn fire_now_returns_502_on_permanent_abandon() {
        let context = test_context_with_dispatcher(StatusCode::UNAUTHORIZED).await;
        let upstream =
            create_oauth_upstream(context.storage.as_ref(), context.aead.as_ref(), true, None)
                .await;

        let (status, body) = fire_now_response(context.state, upstream.id).await;

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body["fired"], false);
        assert_eq!(body["reason"], "auth_failed");
    }

    #[tokio::test]
    async fn fire_now_returns_503_on_transient() {
        let context = test_context_with_dispatcher(StatusCode::INTERNAL_SERVER_ERROR).await;
        let upstream =
            create_oauth_upstream(context.storage.as_ref(), context.aead.as_ref(), true, None)
                .await;

        let (status, body) = fire_now_response(context.state, upstream.id).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["fired"], false);
        assert_eq!(body["reason"], "upstream_5xx");
    }

    async fn test_context_with_dispatcher(status: StatusCode) -> TestContext {
        let mut context = test_context().await;
        context.state.warmup_dialect_dispatcher = Some(Arc::new(TestNativeDispatcher {
            status,
            lanes: context.dispatched_lanes.clone(),
        }));
        context
    }

    async fn test_context() -> TestContext {
        let dir = tempfile::tempdir().expect("storage dir");
        let database_url = format!("sqlite://{}", dir.path().join("upstreams.sqlite").display());
        let storage =
            cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
                .await
                .expect("storage opens");
        storage.initialize().await.expect("storage initializes");
        let storage = Arc::new(storage);
        let aead = Arc::new(AeadService::from_master_key([8; 32]));
        let warmup_port = TestWarmupPort::default();
        let recorded_attempts = warmup_port.recorded.clone();
        let state = AdminState {
            config_path: None,
            build: crate::AdminBuildInfo {
                git_sha: "test-git-sha",
                rustc: "test-rustc",
                profile: "test-profile",
                target: "test-target",
            },
            startup_config_overrides: Default::default(),
            storage: Some(storage.clone()),
            key_store: None,
            aead: aead.clone(),
            limit_engine: LimitEngine::new(
                Arc::new(KeyConcurrencyManager::new()),
                Arc::new(cc_lb_clock::SystemClock),
            ),
            lifecycle: Some(crate::AdminPorts {
                warmup: Some(Arc::new(warmup_port)),
                ..crate::AdminPorts::default()
            }),
            lazy_refresher: None,
            runtime: None,
            data_dir: None,
            warmup_dialect_dispatcher: None,
            limit_reset_usage_refresh: None,
            dynamic_view: Arc::new(DynamicViewHolder::new(test_view())),
            config: Arc::new(Config::default()),
            dynamic_view_rebinder: None,
            scheduler: None,
            admin_auth: Arc::new(crate::auth::AdminAuthenticator::new(Vec::new())),
            start_time: std::time::Instant::now(),
            event_bus: None,
            storage_tail: crate::events::storage_tail_channel(),
            clock: Arc::new(cc_lb_clock::SystemClock),
            anthropic_org_api: cc_lb_control::anthropic_api_key::AnthropicOrgApi::production()
                .expect("production Anthropic origin parses"),
        };
        TestContext {
            _dir: dir,
            state,
            storage,
            aead,
            recorded_attempts,
            dispatched_lanes: Arc::default(),
        }
    }

    fn test_view() -> Arc<DynamicView> {
        let principal_view = Arc::new(PrincipalView::from_db(&[], HashMap::new()));
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(TestSignerFactory))
            .principal_view(principal_view)
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .build()
    }

    async fn create_api_key_upstream(
        storage: &TestStorage,
        warmup_enabled: bool,
    ) -> UpstreamRecord {
        storage
            .create(UpstreamCreate {
                id: Uuid::new_v4(),
                name: format!("api-key-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: Some(vec![1, 2, 3]),
                oauth_tokens: None,
                warmup_enabled,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            })
            .await
            .expect("upstream create succeeds")
    }

    async fn create_oauth_upstream(
        storage: &TestStorage,
        aead: &AeadService,
        warmup_enabled: bool,
        base_url: Option<Url>,
    ) -> UpstreamRecord {
        create_oauth_upstream_with_plugin(storage, aead, warmup_enabled, base_url, None).await
    }

    async fn create_oauth_upstream_with_plugin(
        storage: &TestStorage,
        aead: &AeadService,
        warmup_enabled: bool,
        base_url: Option<Url>,
        warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
    ) -> UpstreamRecord {
        let id = Uuid::new_v4();
        let bundle = OAuthTokenBundle {
            oauth_profile: Default::default(),
            access_token: "test-access-token".to_owned(),
            refresh_token: "test-refresh-token".to_owned(),
            expires_at_unix_secs: u64::MAX / 2,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: None,
            scopes: Vec::new(),
            never_refresh: false,
        };
        let tokens = AeadEncryptedField::<OAuthTokenBundle>::encrypt(aead, &bundle, id.as_bytes())
            .expect("token encryption succeeds");
        storage
            .create(UpstreamCreate {
                id,
                name: format!("oauth-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicOauth,
                base_url,
                api_key_ciphertext: None,
                oauth_tokens: Some(OAuthTokensCreate {
                    tokens,
                    never_refresh: bundle.never_refresh,
                }),
                warmup_enabled,
                warmup_dialect_plugin,
                workspace_id: None,
                api_key_access: None,
            })
            .await
            .expect("upstream create succeeds")
    }

    fn test_admin_identity() -> AdminIdentity {
        AdminIdentity {
            authority: "test".to_owned(),
            subject: "upstream-unit-tests".to_owned(),
            kind: crate::auth::AdminActorKind::Service,
            provider_id: "test".to_owned(),
            email: None,
            display_name: None,
            groups: Vec::new(),
            expires_at_unix_secs: None,
        }
    }

    async fn fire_now_response(state: AdminState, upstream_id: Uuid) -> (StatusCode, Value) {
        fire_now_response_for_lane(state, upstream_id, None).await
    }

    async fn fire_now_response_for_lane(
        state: AdminState,
        upstream_id: Uuid,
        lane: Option<&str>,
    ) -> (StatusCode, Value) {
        let uri = match lane {
            Some(lane) => format!("/admin/v1/upstreams/{upstream_id}/warmup/fire-now?lane={lane}"),
            None => format!("/admin/v1/upstreams/{upstream_id}/warmup/fire-now"),
        };
        let response = router()
            .with_state(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(uri)
                    .extension(test_admin_identity())
                    .body(AxumBody::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request completes");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read succeeds");
        let body = serde_json::from_slice(&bytes).expect("response body is json");
        (status, body)
    }

    async fn create_upstream_response(context: &TestContext, body: Value) -> (StatusCode, Value) {
        let response = router()
            .with_state(context.state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/admin/v1/upstreams")
                    .header("content-type", "application/json")
                    .extension(test_admin_identity())
                    .body(AxumBody::from(body.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request completes");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read succeeds");
        let body = serde_json::from_slice(&bytes).expect("response is json");
        (status, body)
    }

    fn oauth_create_body(kind: &str, warmup_enabled: Option<bool>) -> Value {
        let mut body = json!({
            "name": format!("oauth-warmup-{kind}-{}", Uuid::new_v4().simple()),
            "kind": kind,
        });
        if matches!(kind, "openai_oauth" | "gemini_oauth") {
            body["oauth_token_bundle"] = json!({
                "access_token": "test-access-token",
                "refresh_token": "test-refresh-token",
                "expires_at_unix_secs": 4_000_000_000u64,
                "scopes": [],
            });
        }
        if kind == "gemini_oauth" {
            body["project_id"] = json!("test-project");
        }
        if let Some(warmup_enabled) = warmup_enabled {
            body["warmup_enabled"] = json!(warmup_enabled);
        }
        body
    }

    fn api_key_create_body(kind: &str, warmup_enabled: Option<bool>) -> Value {
        let mut body = json!({
            "name": format!("api-key-warmup-{kind}-{}", Uuid::new_v4().simple()),
            "kind": kind,
            "api_key_value": "test-api-key",
            "skip_preflight": true,
        });
        if let Some(warmup_enabled) = warmup_enabled {
            body["warmup_enabled"] = json!(warmup_enabled);
        }
        body
    }

    async fn spawn_warmup_server(status: StatusCode) -> (Url, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/v1/messages",
            post(move || async move { status.into_response() }),
        );
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener binds");
        let addr = listener.local_addr().expect("local addr is available");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server runs");
        });
        let base_url = Url::parse(&format!("http://{addr}/")).expect("base URL parses");
        (base_url, server)
    }

    #[tokio::test]
    async fn fire_now_with_dialect_plugin_returns_502_when_runtime_unavailable() {
        let context = test_context().await;
        let (base_url, server) = spawn_warmup_server(StatusCode::OK).await;
        let plugin = UpstreamWarmupDialectPlugin {
            wasm_registry_id: Uuid::new_v4(),
            config: serde_json::Value::Null,
            wire_version: Some(1),
        };
        let upstream = create_oauth_upstream_with_plugin(
            context.storage.as_ref(),
            context.aead.as_ref(),
            true,
            Some(base_url),
            Some(plugin),
        )
        .await;

        let (status, body) = fire_now_response(context.state, upstream.id).await;

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body["fired"], false);
        assert_eq!(body["reason"], "dialect_plugin_failed");
        server.abort();
    }

    #[tokio::test]
    async fn create_upstream_defaults_warmup_enabled_for_all_oauth_kinds() {
        for kind in ["anthropic_oauth", "openai_oauth", "gemini_oauth"] {
            let context = test_context().await;
            let (status, body) =
                create_upstream_response(&context, oauth_create_body(kind, None)).await;

            assert_eq!(status, StatusCode::CREATED, "{kind}");
            assert_eq!(body["warmup_enabled"], true, "{kind}");
            let upstream_id: Uuid = body["id"]
                .as_str()
                .expect("id is a string")
                .parse()
                .expect("id is a uuid");
            let stored = context
                .storage
                .get_by_id(upstream_id)
                .await
                .expect("read succeeds")
                .expect("upstream exists");
            assert!(stored.warmup_enabled, "{kind}");
            if kind == "anthropic_oauth" {
                assert!(
                    stored.oauth_credentials.is_none(),
                    "Anthropic direct create keeps its no-credential shape"
                );
            } else {
                assert!(stored.oauth_credentials.is_some(), "{kind}");
            }
        }
    }

    #[tokio::test]
    async fn create_upstream_preserves_explicitly_disabled_warmup_for_all_oauth_kinds() {
        for kind in ["anthropic_oauth", "openai_oauth", "gemini_oauth"] {
            let context = test_context().await;
            let (status, body) =
                create_upstream_response(&context, oauth_create_body(kind, Some(false))).await;

            assert_eq!(status, StatusCode::CREATED, "{kind}");
            assert_eq!(body["warmup_enabled"], false, "{kind}");
            let upstream_id: Uuid = body["id"]
                .as_str()
                .expect("id is a string")
                .parse()
                .expect("id is a uuid");
            let stored = context
                .storage
                .get_by_id(upstream_id)
                .await
                .expect("read succeeds")
                .expect("upstream exists");
            assert!(!stored.warmup_enabled, "{kind}");
        }
    }

    #[tokio::test]
    async fn create_upstream_keeps_warmup_disabled_for_all_api_key_kinds_when_omitted_or_false() {
        for warmup_enabled in [None, Some(false)] {
            for kind in ["anthropic_api_key", "openai_api_key", "gemini_api_key"] {
                let context = test_context().await;
                let (status, body) =
                    create_upstream_response(&context, api_key_create_body(kind, warmup_enabled))
                        .await;

                assert_eq!(status, StatusCode::CREATED, "{kind} {warmup_enabled:?}");
                assert_eq!(body["warmup_enabled"], false, "{kind} {warmup_enabled:?}");
                let upstream_id: Uuid = body["id"]
                    .as_str()
                    .expect("id is a string")
                    .parse()
                    .expect("id is a uuid");
                let stored = context
                    .storage
                    .get_by_id(upstream_id)
                    .await
                    .expect("read succeeds")
                    .expect("upstream exists");
                assert!(!stored.warmup_enabled, "{kind} {warmup_enabled:?}");
            }
        }
    }

    #[tokio::test]
    async fn create_upstream_rejects_explicitly_enabled_warmup_for_all_api_key_kinds() {
        for kind in ["anthropic_api_key", "openai_api_key", "gemini_api_key"] {
            let context = test_context().await;
            let body = json!({
                "name": format!("api-key-warmup-rejected-{kind}"),
                "kind": kind,
                "warmup_enabled": true,
            });
            let (status, response_body) = create_upstream_response(&context, body).await;

            assert_eq!(status, StatusCode::BAD_REQUEST, "{kind}");
            assert_eq!(response_body["error"], "warmup_unsupported_kind", "{kind}");
        }
    }

    #[tokio::test]
    async fn create_api_key_upstream_binds_ciphertext_to_upstream_id() {
        let context = test_context().await;
        let body = serde_json::json!({
            "name": "api-key-aad-bound",
            "kind": "anthropic_api_key",
            "api_key_value": "sk-ant-UPSTREAM-SECRET",
            "skip_preflight": true,
        });
        let response = router()
            .with_state(context.state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/admin/v1/upstreams")
                    .header("content-type", "application/json")
                    .extension(test_admin_identity())
                    .body(AxumBody::from(body.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request completes");

        assert_eq!(response.status(), StatusCode::CREATED);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read succeeds");
        let body: Value = serde_json::from_slice(&bytes).expect("response is json");
        let upstream_id: Uuid = body["id"]
            .as_str()
            .expect("id is a string")
            .parse()
            .expect("id is a uuid");
        let stored = context
            .storage
            .get_by_id(upstream_id)
            .await
            .expect("read succeeds")
            .expect("upstream exists");
        assert_eq!(body["spec_revision"].as_u64(), Some(stored.revision));
        let ciphertext = stored
            .api_key_ciphertext
            .as_deref()
            .expect("api-key ciphertext stored");
        let plaintext = context
            .aead
            .decrypt(ciphertext, upstream_id.as_bytes())
            .expect("ciphertext decrypts under the upstream id");
        assert_eq!(plaintext, b"sk-ant-UPSTREAM-SECRET");
        assert!(
            context
                .aead
                .decrypt(ciphertext, stored.name.as_bytes())
                .is_err(),
            "ciphertext must not decrypt under the upstream name"
        );
    }

    #[tokio::test]
    async fn create_api_key_upstream_reports_null_last_apply_error() {
        let context = test_context().await;
        let body = serde_json::json!({
            "name": "api-key-clean-status",
            "kind": "anthropic_api_key",
            "api_key_value": "sk-ant-CLEAN-STATUS",
            "skip_preflight": true,
        });
        let response = router()
            .with_state(context.state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/admin/v1/upstreams")
                    .header("content-type", "application/json")
                    .extension(test_admin_identity())
                    .body(AxumBody::from(body.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request completes");

        assert_eq!(response.status(), StatusCode::CREATED);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read succeeds");
        let body: Value = serde_json::from_slice(&bytes).expect("response is json");
        assert!(body["status"]["last_apply_error"].is_null());
        assert!(body["status"]["last_apply_at_unix_secs"].is_null());
    }

    #[test]
    fn upstream_response_preserves_provider_oauth_reconnect_status() {
        for kind in [UpstreamKind::OpenaiOauth, UpstreamKind::GeminiOauth] {
            let mut record = UpstreamRecord {
                kind,
                last_apply_error: Some("status_401".to_owned()),
                last_apply_at_unix_secs: Some(1234),
                last_warmup_at_unix_secs: Some(5678),
                ..UpstreamRecord::default()
            };
            let response = upstream_response(&record, None);
            assert_eq!(
                response.status.last_apply_error.as_deref(),
                Some("status_401"),
                "{kind:?}"
            );
            assert_eq!(response.status.last_apply_at_unix_secs, Some(1234));
            assert_eq!(response.status.last_warmup_at_unix_secs, Some(5678));

            record.last_apply_error = Some("network".to_owned());
            let transient = upstream_response(&record, None);
            assert_eq!(
                transient.status.last_apply_error.as_deref(),
                Some("network")
            );
        }
    }

    #[tokio::test]

    async fn create_api_key_upstream_missing_secret_writes_no_row() {
        let context = test_context().await;
        let body = serde_json::json!({
            "name": "api-key-missing-secret",
            "kind": "anthropic_api_key",
        });
        let response = router()
            .with_state(context.state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/admin/v1/upstreams")
                    .header("content-type", "application/json")
                    .extension(test_admin_identity())
                    .body(AxumBody::from(body.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request completes");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read succeeds");
        let body: Value = serde_json::from_slice(&bytes).expect("response is json");
        assert_eq!(body["error"], "missing_api_key");
        assert!(
            context
                .storage
                .get_by_name("api-key-missing-secret")
                .await
                .expect("lookup succeeds")
                .is_none(),
            "invalid create must not leave an upstream row"
        );
    }

    /// The removed `api_key_env` field is not part of the request contract:
    /// upstream bodies ignore unknown fields like every other admin v1 DTO,
    /// so an env-only create is simply a create without `api_key_value` and
    /// never reads the server environment.
    #[tokio::test]
    async fn create_api_key_upstream_ignores_removed_api_key_env_field() {
        let context = test_context().await;
        let body = serde_json::json!({
            "name": "api-key-removed-env-field",
            "kind": "anthropic_api_key",
            "api_key_env": "ANTHROPIC_API_KEY",
        });
        let response = router()
            .with_state(context.state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/admin/v1/upstreams")
                    .header("content-type", "application/json")
                    .extension(test_admin_identity())
                    .body(AxumBody::from(body.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request completes");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read succeeds");
        let body: Value = serde_json::from_slice(&bytes).expect("response is json");
        assert_eq!(body["error"], "missing_api_key");
        assert!(
            context
                .storage
                .get_by_name("api-key-removed-env-field")
                .await
                .expect("lookup succeeds")
                .is_none(),
            "create carrying only the removed field must not leave an upstream row"
        );
    }

    #[tokio::test]
    async fn update_warmup_enabled_bootstraps_when_currently_null() {
        let context = test_context().await;
        let upstream =
            create_oauth_upstream(context.storage.as_ref(), context.aead.as_ref(), false, None)
                .await;

        let response = router()
            .with_state(context.state)
            .oneshot(
                Request::builder()
                    .method(Method::PATCH)
                    .uri(format!("/admin/v1/upstreams/{}", upstream.id))
                    .header("content-type", "application/json")
                    .header("if-match", format!("W/\"{}\"", upstream.revision))
                    .extension(test_admin_identity())
                    .body(AxumBody::from(r#"{"warmup_enabled":true}"#.to_owned()))
                    .expect("request builds"),
            )
            .await
            .expect("request completes");

        assert_eq!(response.status(), StatusCode::OK);
        let stored = context
            .storage
            .get_by_id(upstream.id)
            .await
            .expect("read succeeds")
            .expect("upstream exists");
        assert!(stored.warmup_enabled);
    }

    fn bootstrap_bundle(profile: cc_lb_oauth_protocol::GeminiOAuthProfile) -> OAuthTokenBundle {
        OAuthTokenBundle {
            oauth_profile: profile,
            access_token: "test-access-token".to_owned(),
            refresh_token: "test-refresh-token".to_owned(),
            expires_at_unix_secs: u64::MAX / 2,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: None,
            scopes: Vec::new(),
            never_refresh: false,
        }
    }

    fn bootstrap_task_summary(
        tasks: &[SchedulerPushTask<AdaptiveJob>],
    ) -> Vec<(String, WarmupLane, u64, Option<u64>)> {
        tasks
            .iter()
            .map(|task| {
                let AdaptiveJob::Warmup(job) = &task.args else {
                    panic!("expected warmup job");
                };
                (
                    task.idempotency_key.clone().expect("bootstrap key"),
                    job.lane,
                    job.cycle_key,
                    task.run_at_unix_secs,
                )
            })
            .collect()
    }

    #[test]
    fn warmup_bootstrap_tasks_seed_primary_only_for_single_lane_kinds() {
        let upstream_id =
            Uuid::parse_str("12345678-1234-5678-1234-567812345678").expect("uuid parses");
        let seed_secs = 1_800_000_000;
        let antigravity = bootstrap_bundle(cc_lb_oauth_protocol::GeminiOAuthProfile::Antigravity);

        for kind in [UpstreamKind::AnthropicOauth, UpstreamKind::OpenaiOauth] {
            for bundle in [None, Some(&antigravity)] {
                let tasks = warmup_bootstrap_tasks(kind, upstream_id, bundle, seed_secs);
                assert_eq!(
                    bootstrap_task_summary(&tasks),
                    vec![(
                        "adaptive:warmup:12345678-1234-5678-1234-567812345678:bootstrap:1800000000"
                            .to_owned(),
                        WarmupLane::Primary,
                        seed_secs,
                        Some(seed_secs),
                    )],
                    "{kind:?}"
                );
            }
        }
        assert!(
            warmup_bootstrap_tasks(UpstreamKind::AnthropicApiKey, upstream_id, None, seed_secs)
                .is_empty()
        );
    }

    #[test]
    fn warmup_bootstrap_tasks_seed_third_party_only_for_antigravity_gemini() {
        let upstream_id =
            Uuid::parse_str("12345678-1234-5678-1234-567812345678").expect("uuid parses");
        let seed_secs = 1_800_000_000;
        let primary = (
            "adaptive:warmup:12345678-1234-5678-1234-567812345678:bootstrap:1800000000".to_owned(),
            WarmupLane::Primary,
            seed_secs,
            Some(seed_secs),
        );
        let third_party = (
            "adaptive:warmup_third_party:12345678-1234-5678-1234-567812345678:bootstrap:1800000000"
                .to_owned(),
            WarmupLane::ThirdParty,
            seed_secs,
            Some(seed_secs),
        );

        let antigravity = bootstrap_bundle(cc_lb_oauth_protocol::GeminiOAuthProfile::Antigravity);
        let tasks = warmup_bootstrap_tasks(
            UpstreamKind::GeminiOauth,
            upstream_id,
            Some(&antigravity),
            seed_secs,
        );
        assert_eq!(
            bootstrap_task_summary(&tasks),
            vec![primary.clone(), third_party]
        );

        let gemini_cli = bootstrap_bundle(cc_lb_oauth_protocol::GeminiOAuthProfile::GeminiCli);
        let tasks = warmup_bootstrap_tasks(
            UpstreamKind::GeminiOauth,
            upstream_id,
            Some(&gemini_cli),
            seed_secs,
        );
        assert_eq!(bootstrap_task_summary(&tasks), vec![primary.clone()]);

        // An undecryptable credential never seeds the third-party lane.
        let tasks = warmup_bootstrap_tasks(UpstreamKind::GeminiOauth, upstream_id, None, seed_secs);
        assert_eq!(bootstrap_task_summary(&tasks), vec![primary]);
    }

    #[tokio::test]
    async fn update_warmup_enabled_rejects_when_oauth_credentials_missing() {
        let context = test_context().await;
        let upstream = context
            .storage
            .create(UpstreamCreate {
                id: Uuid::new_v4(),
                name: format!("oauth-no-creds-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            })
            .await
            .expect("create succeeds");
        assert!(upstream.oauth_credentials.is_none());

        let response = router()
            .with_state(context.state)
            .oneshot(
                Request::builder()
                    .method(Method::PATCH)
                    .uri(format!("/admin/v1/upstreams/{}", upstream.id))
                    .header("content-type", "application/json")
                    .header("if-match", format!("W/\"{}\"", upstream.revision))
                    .extension(test_admin_identity())
                    .body(AxumBody::from(r#"{"warmup_enabled":true}"#.to_owned()))
                    .expect("request builds"),
            )
            .await
            .expect("request completes");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read succeeds");
        let body: Value = serde_json::from_slice(&bytes).expect("response is json");
        assert_eq!(body["error"], "warmup_requires_oauth_credentials");
        let stored = context
            .storage
            .get_by_id(upstream.id)
            .await
            .expect("read succeeds")
            .expect("upstream exists");
        assert!(!stored.warmup_enabled);
    }

    #[tokio::test]
    async fn fire_now_returns_400_when_oauth_credentials_missing() {
        let context = test_context().await;
        let upstream = context
            .storage
            .create(UpstreamCreate {
                id: Uuid::new_v4(),
                name: format!("oauth-fire-no-creds-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: true,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            })
            .await
            .expect("create succeeds");
        assert!(upstream.oauth_credentials.is_none());

        let response = router()
            .with_state(context.state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!(
                        "/admin/v1/upstreams/{}/warmup/fire-now",
                        upstream.id
                    ))
                    .extension(test_admin_identity())
                    .body(AxumBody::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request completes");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read succeeds");
        let body: Value = serde_json::from_slice(&bytes).expect("response is json");
        assert_eq!(body["fired"], false);
        assert_eq!(body["reason"], "oauth_credentials_missing");
    }
}
