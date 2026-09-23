use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderValue, Method, Request, StatusCode,
        header::{ETAG, IF_MATCH},
    },
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use bytes::Bytes;
use cc_lb_aead::OAuthTokenBundle;
use cc_lb_clock::Clock;
use cc_lb_control::anthropic_compat::{
    CLAUDE_CODE_STABLE_VERSION_FALLBACK, CLAUDE_CODE_STABLE_VERSION_KEY, claude_code_user_agent,
};
use cc_lb_control::anthropic_metadata::make_metadata_http_client;
use cc_lb_control::{AuditPayload, run_metadata_refresh};
use cc_lb_quota::{
    UnifiedQuotaObservation, build_subscription_quota_samples, parse_anthropic_unified_headers,
};
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerPushTask};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamWarmupDialectPlugin};
use cc_lb_storage_api::{
    OrganizationMetadataRecord, Storage, StorageError, SubscriptionQuotaLatestRecord,
    SubscriptionQuotaWindow, UpstreamCreate, UpstreamRecord, UpstreamStatusUpdate, UpstreamStore,
    UpstreamSubscriptionMetadataRecord, UpstreamUpdate, WarmupAttemptOutcome, WarmupAttemptTrigger,
    WarmupDispatchKind, WarmupPermanentFailureReason, WarmupSkipReason,
    WarmupTransientFailureReason,
};
use http_body_util::Full;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::add_dynamic_rebind_headers;
use crate::ports::{WarmupAttemptInput, WarmupAttemptResult};
use crate::{
    AdminState, WarmupDialectDispatchErrorKind,
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
const WARMUP_MODEL: &str = "claude-haiku-4-5-20251001";
const WARMUP_MAX_TOKENS: u32 = 1;
const WARMUP_ANTHROPIC_BETA: &str = "oauth-2025-04-20";

type WarmupHttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

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
}

#[derive(Debug, Deserialize)]
struct UpstreamCreateBody {
    name: String,
    kind: UpstreamKind,
    #[serde(default)]
    base_url: Option<Url>,
    #[serde(default)]
    api_key_env: Option<String>,
    #[serde(default)]
    api_key_value: Option<String>,
    #[serde(default)]
    warmup_enabled: Option<bool>,
    #[serde(default)]
    warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
}

#[derive(Debug, Deserialize)]
struct UpstreamUpdateBody {
    #[serde(default)]
    name: Option<String>,
    /// Missing or explicit `null` preserves the override; a non-null
    /// value sets it. Clearing requires `clear_base_url`.
    #[serde(default)]
    base_url: Option<Url>,
    /// Explicitly clears the `base_url` override. Mutually exclusive
    /// with a non-null `base_url`.
    #[serde(default)]
    clear_base_url: bool,
    #[serde(default)]
    api_key_env: Option<String>,
    #[serde(default)]
    api_key_value: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    warmup_enabled: Option<bool>,
    #[serde(default)]
    warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
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
}

#[derive(Debug, Serialize)]
struct FireNowFiredResponse {
    fired: bool,
    cycle_key: i64,
    outcome: WarmupAttemptOutcome,
}

#[derive(Debug, Serialize)]
struct FireNowNotFiredResponse {
    fired: bool,
    #[serde(flatten)]
    outcome: WarmupAttemptOutcome,
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
            Self::CredentialDecrypt => StatusCode::INTERNAL_SERVER_ERROR,
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

async fn create_upstream(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Json(body): Json<UpstreamCreateBody>,
) -> Result<Response, UpstreamError> {
    let storage = storage(&state)?;
    let api_key_plaintext = api_key_plaintext_for_create(&body)?;
    let name = body.name.clone();
    let kind = body.kind;
    let warmup_enabled = body
        .warmup_enabled
        .unwrap_or(kind == UpstreamKind::AnthropicOauth);
    let created = match UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: body.name,
            kind,
            base_url: body.base_url,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled,
            warmup_dialect_plugin: body.warmup_dialect_plugin,
        },
    )
    .await
    {
        Ok(created) => created,
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
    let record = match api_key_plaintext {
        Some(plaintext) => store_api_key_credential(&state, storage, &created, &plaintext).await?,
        None => created,
    };
    let payload = AuditPayload::UpstreamCreate {
        upstream_id: record.id.to_string(),
        kind: record.kind.as_str().to_owned(),
    };
    let action = payload.to_string();
    if let Err(error) = record_admin_audit(
        &state,
        AdminAuditEvent {
            identity: Some(&identity),
            system_component: None,
            action: &action,
            route: "/admin/v1/upstreams",
            target_principal_id: None,
            target_upstream: Some(&record.name),
            api_key_id: None,
            status: StatusCode::CREATED.as_u16(),
            payload: None,
        },
    )
    .await
    {
        tracing::error!(error = %error, action = %action, "admin audit write failed");
        return Err(UpstreamError::AuditWriteFailed);
    }
    let mut response = (StatusCode::CREATED, Json(upstream_response(&record))).into_response();
    response.headers_mut().insert(
        axum::http::header::LOCATION,
        HeaderValue::from_str(&format!("/admin/v1/upstreams/{}", record.id)).map_err(|error| {
            UpstreamError::BadRequest {
                error: "invalid_location",
                detail: error.to_string(),
            }
        })?,
    );
    add_dynamic_rebind_headers(&mut response, &state).await;
    Ok(response)
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
        .map(|record| upstream_response(&record))
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
    let mut response = Json(upstream_response(&record)).into_response();
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
    subscription_metadata_response(storage, upstream_id)
        .await
        .map(Json)
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
    tokio::time::timeout(
        METADATA_REFRESH_TIMEOUT,
        run_metadata_refresh(
            storage.clone(),
            &client,
            upstream_id,
            &access_token,
            &user_agent,
            &cancel,
            &*state.clock,
        ),
    )
    .await
    .map_err(|_| UpstreamError::MetadataRefreshTimeout)?
    .map_err(|error| UpstreamError::RefreshFailed {
        detail: error.to_string(),
    })?;

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
}

async fn fire_now_upstream_warmup(
    State(state): State<AdminState>,
    Path(upstream_id): Path<Uuid>,
    Extension(identity): Extension<AdminIdentity>,
) -> Result<Response, UpstreamError> {
    let storage = storage_arc(&state)?;
    let upstream = UpstreamStore::get_by_id(storage.as_ref(), upstream_id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    let target_upstream = upstream.name.clone();
    let response = fire_now_upstream_warmup_inner(&state, storage, upstream_id, upstream).await?;
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
}

async fn fire_now_upstream_warmup_inner(
    state: &AdminState,
    storage: Arc<dyn Storage>,
    upstream_id: Uuid,
    upstream: UpstreamRecord,
) -> Result<Response, UpstreamError> {
    if upstream.kind != UpstreamKind::AnthropicOauth {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "warmup_unsupported_for_kind",
                "kind": upstream.kind.as_str(),
            })),
        )
            .into_response());
    }
    let now_unix_secs = unix_now_secs_i64(&*state.clock)?;
    if !upstream.warmup_enabled {
        let _ = record_fire_now_skip(
            state,
            storage.as_ref(),
            &upstream,
            now_unix_secs,
            WarmupSkipReason::UpstreamDisabled,
            None,
        )
        .await?;
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "warmup_disabled" })),
        )
            .into_response());
    }
    if upstream.oauth_credentials.is_none() {
        let outcome = record_fire_now_failure(
            state,
            storage.as_ref(),
            &upstream,
            now_unix_secs,
            now_unix_secs,
            None,
            WarmupPermanentFailureReason::OauthCredentialsMissing,
            WarmupDispatchKind::NotDispatched,
            None,
            None,
        )
        .await?;
        return Ok(fire_now_not_fired_response(
            StatusCode::BAD_REQUEST,
            outcome,
        ));
    }

    let holder = format!("fire-now:{}", Uuid::new_v4());
    tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, action = "dispatch_requested");

    let latest = latest_five_hour_quota(storage.as_ref(), upstream_id).await?;
    let candidate_cycle_key = latest
        .as_ref()
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
            now_unix_secs,
            candidate_cycle_key,
            Some(holder.as_str()),
            WarmupPermanentFailureReason::DialectPluginFailed,
            WarmupDispatchKind::NotDispatched,
            Some(StatusCode::BAD_GATEWAY),
            Some("warmup dialect dispatcher unavailable"),
        )
        .await?;
        return Ok(fire_now_not_fired_response(
            StatusCode::BAD_GATEWAY,
            outcome,
        ));
    }
    let dialect_dispatch_bundle = upstream.warmup_dialect_plugin.as_ref().and_then(|_| {
        let runtime = state.runtime.as_ref()?;
        let data_dir = state.data_dir.as_deref()?;
        let dispatcher = state.warmup_dialect_dispatcher.as_deref()?;
        Some((runtime, data_dir, dispatcher))
    });
    let dialect_plugin_used = dialect_dispatch_bundle.is_some();
    tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, cycle_key = %candidate_cycle_key, dialect_plugin_used = dialect_plugin_used, action = "dispatch_start");
    let dispatch_attempt = if let Some((runtime, data_dir, dispatcher)) = dialect_dispatch_bundle {
        match tokio::time::timeout(
            Duration::from_secs(FIRE_NOW_REQUEST_TIMEOUT_SECS),
            dispatcher.dispatch_warmup_with_dialect(runtime, data_dir, &upstream),
        )
        .await
        {
            Ok(Ok(outcome)) => fire_now_response_attempt(
                outcome.status,
                outcome.headers,
                WarmupDispatchKind::DialectPlugin,
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
    } else {
        let bundle = match decrypt_oauth_bundle(state, &upstream) {
            Ok(bundle) => bundle,
            Err(error) => {
                let _ = record_fire_now_failure(
                    state,
                    storage.as_ref(),
                    &upstream,
                    now_unix_secs,
                    candidate_cycle_key,
                    Some(holder.as_str()),
                    WarmupPermanentFailureReason::CredentialDecryptFailed,
                    WarmupDispatchKind::NotDispatched,
                    None,
                    Some("oauth credential decrypt failed"),
                )
                .await;
                return Err(error);
            }
        };
        let access_token =
            match fresh_enough_access_token(state, storage.clone(), &upstream, bundle).await {
                Ok(access_token) => access_token,
                Err(error) => {
                    let _ = record_fire_now_failure(
                        state,
                        storage.as_ref(),
                        &upstream,
                        now_unix_secs,
                        candidate_cycle_key,
                        Some(holder.as_str()),
                        WarmupPermanentFailureReason::OauthRefreshFailed,
                        WarmupDispatchKind::NotDispatched,
                        None,
                        Some("oauth refresh failed"),
                    )
                    .await;
                    return Err(error);
                }
            };
        let base_url = match upstream_base_url(&upstream) {
            Ok(base_url) => base_url,
            Err(error) => {
                let _ = record_fire_now_failure(
                    state,
                    storage.as_ref(),
                    &upstream,
                    now_unix_secs,
                    candidate_cycle_key,
                    Some(holder.as_str()),
                    WarmupPermanentFailureReason::RequestBuildFailed,
                    WarmupDispatchKind::NotDispatched,
                    None,
                    Some("warmup base url resolution failed"),
                )
                .await;
                return Err(error);
            }
        };
        let client = warmup_http_client();
        match tokio::time::timeout(
            Duration::from_secs(FIRE_NOW_REQUEST_TIMEOUT_SECS),
            dispatch_fire_now_warmup(&client, &access_token, &base_url, &holder),
        )
        .await
        {
            Ok(attempt) => attempt,
            Err(_) => FireNowDispatchAttempt::TransientFailure {
                reason: WarmupTransientFailureReason::RequestTimeout,
                dispatch_kind: WarmupDispatchKind::Http,
                status: StatusCode::SERVICE_UNAVAILABLE,
                error_detail: "warmup dispatch timed out".to_owned(),
            },
        }
    };
    let record = record_warmup_attempt(
        state,
        WarmupAttemptInput {
            storage: storage.as_ref(),
            upstream: &upstream,
            scheduled_for_unix_secs: now_unix_secs,
            trigger: WarmupAttemptTrigger::Manual,
            replica_id: None,
            lease_holder: Some(holder.as_str()),
            expected_cycle_key: Some(candidate_cycle_key),
            attempted_at_unix_secs: now_unix_secs,
            completed_at_unix_secs: Some(unix_now_secs_i64(&*state.clock)?),
            dispatch_kind: fire_now_attempt_dispatch_kind(&dispatch_attempt),
            result: execution_result_from_fire_now_attempt(&dispatch_attempt),
        },
    )
    .await?;
    if let FireNowDispatchAttempt::Response { headers, .. } = &dispatch_attempt {
        record_fire_now_subscription_quota_observations(
            state,
            storage.as_ref(),
            upstream_id,
            headers,
        )
        .await?;
    }
    let status = fire_now_attempt_status(&dispatch_attempt);
    tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, status = %status, outcome = ?record.outcome, dispatch_error = record.error_detail.as_deref(), action = "dispatch_result");

    match record.outcome {
        WarmupAttemptOutcome::Success(_) => {
            let response_cycle_key = record.cycle_key.unwrap_or(candidate_cycle_key);
            tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, cycle_key = %candidate_cycle_key, response_cycle_key = %response_cycle_key, action = "dispatch_succeeded");
            write_warmup_status_after_success(storage.as_ref(), upstream_id, &*state.clock).await;
            Ok(Json(FireNowFiredResponse {
                fired: true,
                cycle_key: response_cycle_key,
                outcome: record.outcome,
            })
            .into_response())
        }
        WarmupAttemptOutcome::PermanentFailure(reason) => {
            tracing::warn!(target: "warmup", upstream_id = %upstream_id, reason = ?reason, action = "cycle_abandoned");
            Ok(fire_now_not_fired_response(
                StatusCode::BAD_GATEWAY,
                record.outcome,
            ))
        }
        WarmupAttemptOutcome::TransientFailure(_) => Ok(fire_now_not_fired_response(
            StatusCode::SERVICE_UNAVAILABLE,
            record.outcome,
        )),
        WarmupAttemptOutcome::Skipped(_) => Ok(fire_now_not_fired_response(
            StatusCode::ACCEPTED,
            record.outcome,
        )),
    }
}

fn fire_now_not_fired_response(status: StatusCode, outcome: WarmupAttemptOutcome) -> Response {
    (
        status,
        Json(FireNowNotFiredResponse {
            fired: false,
            outcome,
        }),
    )
        .into_response()
}

async fn latest_five_hour_quota(
    storage: &dyn Storage,
    upstream_id: Uuid,
) -> Result<Option<SubscriptionQuotaLatestRecord>, UpstreamError> {
    let latest = storage
        .list_latest_subscription_quota_for_upstreams(&[upstream_id])
        .await?;
    Ok(latest
        .into_iter()
        .filter(|record| record.window == SubscriptionQuotaWindow::FiveHour)
        .max_by_key(|record| record.observed_at_unix_millis))
}

fn cycle_key_from_latest_observation(latest: &SubscriptionQuotaLatestRecord) -> Option<i64> {
    latest
        .resets_at_unix_secs
        .and_then(|resets_at| i64::try_from(resets_at).ok())
}

async fn dispatch_fire_now_warmup(
    client: &WarmupHttpClient,
    access_token: &str,
    base_url: &Url,
    holder: &str,
) -> FireNowDispatchAttempt {
    let request = match build_fire_now_warmup_request(access_token, base_url, holder) {
        Ok(request) => request,
        Err(error) => {
            return FireNowDispatchAttempt::PermanentFailure {
                reason: WarmupPermanentFailureReason::RequestBuildFailed,
                dispatch_kind: WarmupDispatchKind::NotDispatched,
                status: StatusCode::INTERNAL_SERVER_ERROR,
                error_detail: error,
            };
        }
    };
    match client.request(request).await {
        Ok(response) => fire_now_response_attempt(
            response.status(),
            response.headers().clone(),
            WarmupDispatchKind::Http,
        ),
        Err(error) => FireNowDispatchAttempt::TransientFailure {
            reason: WarmupTransientFailureReason::NetworkError,
            dispatch_kind: WarmupDispatchKind::Http,
            status: StatusCode::BAD_GATEWAY,
            error_detail: error.to_string(),
        },
    }
}

fn build_fire_now_warmup_request(
    access_token: &str,
    base_url: &Url,
    _holder: &str,
) -> Result<Request<Full<Bytes>>, String> {
    let url = base_url
        .join("v1/messages")
        .map_err(|error| format!("failed to build warmup URL: {error}"))?;
    let body_bytes = warmup_body_bytes()?;
    Request::builder()
        .method(Method::POST)
        .uri(url.as_str())
        .header("authorization", format!("Bearer {access_token}"))
        .header("content-type", "application/json")
        .header("anthropic-version", "2023-06-01")
        .header("anthropic-beta", WARMUP_ANTHROPIC_BETA)
        .body(Full::new(Bytes::from(body_bytes)))
        .map_err(|error| format!("failed to build warmup request: {error}"))
}

fn warmup_body_bytes() -> Result<Vec<u8>, String> {
    serde_json::to_string(&warmup_body())
        .map(String::into_bytes)
        .map_err(|error| format!("failed to serialize warmup body: {error}"))
}

fn warmup_body() -> Value {
    json!({
        "model": WARMUP_MODEL,
        "max_tokens": WARMUP_MAX_TOKENS,
        "messages": [{"role": "user", "content": "."}]
    })
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

enum FireNowDispatchAttempt {
    Response {
        status: StatusCode,
        dispatch_kind: WarmupDispatchKind,
        headers: HeaderMap,
        observations: Vec<UnifiedQuotaObservation>,
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

fn fire_now_response_attempt(
    status: StatusCode,
    headers: HeaderMap,
    dispatch_kind: WarmupDispatchKind,
) -> FireNowDispatchAttempt {
    let observations = parse_anthropic_unified_headers(&headers);
    FireNowDispatchAttempt::Response {
        status,
        dispatch_kind,
        headers,
        observations,
    }
}

fn execution_result_from_fire_now_attempt(
    attempt: &FireNowDispatchAttempt,
) -> WarmupAttemptResult<'_> {
    match attempt {
        FireNowDispatchAttempt::Response {
            status,
            observations,
            ..
        } => WarmupAttemptResult::Response {
            status: *status,
            observations,
            error_detail: None,
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
    now_unix_secs: i64,
    reason: WarmupSkipReason,
    error_detail: Option<&str>,
) -> Result<WarmupAttemptOutcome, UpstreamError> {
    record_warmup_attempt(
        state,
        WarmupAttemptInput {
            storage,
            upstream,
            scheduled_for_unix_secs: now_unix_secs,
            trigger: WarmupAttemptTrigger::Manual,
            replica_id: None,
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
            scheduled_for_unix_secs: now_unix_secs,
            trigger: WarmupAttemptTrigger::Manual,
            replica_id: None,
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
    match upstream.base_url.clone() {
        Some(base_url) => Ok(base_url),
        None => Url::parse(DEFAULT_ANTHROPIC_BASE_URL)
            .map_err(|error| invalid_warmup_state(&format!("invalid default warmup URL: {error}"))),
    }
}

fn warmup_http_client() -> WarmupHttpClient {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    Client::builder(TokioExecutor::new()).build(connector)
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

    let fields_changed = changed_fields(&body);
    let api_key_ciphertext = api_key_ciphertext_for_update(&state, &current, &body)?;
    let storage = storage(&state)?;
    if body.warmup_enabled == Some(true)
        && current.kind == UpstreamKind::AnthropicOauth
        && current.oauth_credentials.is_none()
    {
        return Err(UpstreamError::BadRequest {
            error: "warmup_requires_oauth_credentials",
            detail: "complete OAuth before enabling warmup".to_owned(),
        });
    }
    match UpstreamStore::update(
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
            oauth_token_generation: None,
            enabled: body.enabled,
            warmup_enabled: body.warmup_enabled,
            warmup_dialect_plugin: body.warmup_dialect_plugin,
        },
    )
    .await
    {
        Ok(updated) => {
            seed_warmup_if_toggled(&state, &current, &updated).await?;
            let payload = AuditPayload::UpstreamUpdate {
                upstream_id: updated.id.to_string(),
                fields_changed,
            };
            let action = payload.to_string();
            let route = format!("/admin/v1/upstreams/{}", updated.id);
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
                return Err(UpstreamError::AuditWriteFailed);
            }
            let mut response = respond_with_etag(&updated);
            add_dynamic_rebind_headers(&mut response, &state).await;
            Ok(response)
        }
        Err(StorageError::Conflict { message }) => {
            stale_or_conflict(&state, &id, expected_revision, message).await
        }
        Err(error) => Err(error.into()),
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
    if after.kind != UpstreamKind::AnthropicOauth || after.oauth_credentials.is_none() {
        return Ok(());
    }
    let Some(scheduler) = state.scheduler.as_ref() else {
        return Ok(());
    };
    let seed_secs = cc_lb_clock::unix_secs(state.clock.now());
    match scheduler
        .push_adaptive_task(warmup_bootstrap_task(after.id, seed_secs))
        .await
    {
        Ok(()) | Err(SchedulerError::Conflict(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn warmup_bootstrap_task(
    upstream_id: Uuid,
    seed_secs: u64,
) -> SchedulerPushTask<AdaptiveJob> {
    let job = UpstreamWarmupJob::new(upstream_id, seed_secs);
    SchedulerPushTask {
        args: AdaptiveJob::Warmup(job),
        idempotency_key: Some(format!(
            "adaptive:warmup:{upstream_id}:bootstrap:{seed_secs}"
        )),
        run_at_unix_secs: Some(seed_secs),
        max_attempts: None,
    }
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
    match UpstreamStore::soft_delete(storage, current.id, expected_revision).await {
        Ok(()) => {
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
                return Err(UpstreamError::AuditWriteFailed);
            }
            let mut response = StatusCode::NO_CONTENT.into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            Ok(response)
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

    match storage
        .clear_warmup_dialect_plugin(id, expected_revision)
        .await?
    {
        Some(updated) => {
            let payload = AuditPayload::UpstreamUpdate {
                upstream_id: updated.id.to_string(),
                fields_changed: vec!["warmup_dialect_plugin"],
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
                return Err(UpstreamError::AuditWriteFailed);
            }
            let mut response = respond_with_etag(&updated);
            add_dynamic_rebind_headers(&mut response, &state).await;
            Ok(response)
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
        return Err(UpstreamError::AuditWriteFailed);
    }

    let mut response = respond_with_etag(&updated);
    crate::v1::add_dynamic_rebind_headers(&mut response, &state).await;
    Ok(response)
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

fn respond_with_etag(record: &UpstreamRecord) -> Response {
    let mut response = Json(upstream_response(record)).into_response();
    if let Ok(etag) = etag_value(record.revision) {
        response.headers_mut().insert(ETAG, etag);
    }
    response
}

fn storage(state: &AdminState) -> Result<&dyn Storage, UpstreamError> {
    state
        .storage
        .as_deref()
        .ok_or(UpstreamError::StorageUnavailable)
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

fn upstream_response(record: &UpstreamRecord) -> UpstreamResponse {
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

fn api_key_plaintext_for_create(
    body: &UpstreamCreateBody,
) -> Result<Option<String>, UpstreamError> {
    let plaintext = body
        .api_key_value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let env_name = body
        .api_key_env
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match body.kind {
        UpstreamKind::AnthropicApiKey => match (plaintext, env_name) {
            (Some(_), Some(_)) => Err(UpstreamError::BadRequest {
                error: "conflicting_api_key",
                detail: "provide either api_key_value or api_key_env, not both".to_owned(),
            }),
            (Some(value), None) => Ok(Some(value.to_owned())),
            (None, Some(env)) => {
                let value = std::env::var(env).map_err(|error| UpstreamError::BadRequest {
                    error: "missing_api_key_env_value",
                    detail: format!("{env}: {error}"),
                })?;
                Ok(Some(value))
            }
            (None, None) => Err(UpstreamError::BadRequest {
                error: "missing_api_key",
                detail: "anthropic_api_key upstreams require api_key_value or api_key_env"
                    .to_owned(),
            }),
        },
        UpstreamKind::AnthropicOauth => {
            if plaintext.is_some() || env_name.is_some() {
                return Err(UpstreamError::BadRequest {
                    error: "unexpected_api_key",
                    detail: "anthropic_oauth upstreams do not accept api_key_value or api_key_env"
                        .to_owned(),
                });
            }
            Ok(None)
        }
    }
}

/// Best-effort removal of the upstream created moments ago when persisting
/// its api-key credential fails. The row is already committed, so a
/// concurrent view rebuild — or a crash or failed `soft_delete` here — can
/// leave a credential-less api-key upstream behind. That is safe: such a row
/// fails closed via `last_apply_error` and a terminal 502 instead of falling
/// back to the caller's key.
async fn rollback_api_key_create(
    storage: &dyn Storage,
    created: &UpstreamRecord,
    error: impl std::fmt::Debug,
) {
    tracing::error!(upstream_id = %created.id, ?error, "rolling back api-key upstream create");
    if let Err(rollback_error) =
        UpstreamStore::soft_delete(storage, created.id, created.revision).await
    {
        tracing::error!(upstream_id = %created.id, error = %rollback_error, "api-key upstream rollback delete failed");
    }
}

/// Encrypts the operator-provided api key under the upstream id and stores it
/// on the row committed moments ago, then clears the transient apply error a
/// concurrent view rebuild may have written while the row was still
/// credential-less. The clear is conditional and best-effort: it only runs when
/// the stored record still carries an error (an unconditional clear would also
/// stamp `last_apply_at` on every api-key create), and a failure to clear is
/// logged rather than failing an otherwise successful create — the stored
/// credential is already correct and the next rebuild resolves the status.
async fn store_api_key_credential(
    state: &AdminState,
    storage: &dyn Storage,
    created: &UpstreamRecord,
    plaintext: &str,
) -> Result<UpstreamRecord, UpstreamError> {
    let ciphertext = match encrypt_plaintext_value(state, plaintext, created.id.as_bytes()) {
        Ok(ciphertext) => ciphertext,
        Err(error) => {
            rollback_api_key_create(storage, created, &error).await;
            return Err(error);
        }
    };
    let record = match storage
        .update_api_key_secret(created.id, Some(ciphertext))
        .await
    {
        Ok(record) => record,
        Err(error) => {
            rollback_api_key_create(storage, created, &error).await;
            return Err(UpstreamError::Internal {
                detail: "failed to store upstream api-key credential".to_owned(),
            });
        }
    };
    if record.last_apply_error.is_none() {
        return Ok(record);
    }
    if let Err(error) = UpstreamStore::set_last_apply_error(storage, record.id, None).await {
        tracing::warn!(
            upstream_id = %record.id,
            error = %error,
            "failed to clear transient apply error after storing upstream api-key credential"
        );
        return Ok(record);
    }
    match UpstreamStore::get_by_id(storage, record.id).await {
        Ok(Some(record)) => Ok(record),
        Ok(None) => {
            tracing::warn!(
                upstream_id = %record.id,
                "upstream vanished while clearing transient apply error"
            );
            Ok(UpstreamRecord {
                last_apply_error: None,
                ..record
            })
        }
        Err(error) => {
            tracing::warn!(
                upstream_id = %record.id,
                error = %error,
                "failed to re-read upstream after clearing transient apply error"
            );
            Ok(UpstreamRecord {
                last_apply_error: None,
                ..record
            })
        }
    }
}

fn api_key_ciphertext_for_update(
    state: &AdminState,
    current: &UpstreamRecord,
    body: &UpstreamUpdateBody,
) -> Result<Option<Vec<u8>>, UpstreamError> {
    let plaintext = body
        .api_key_value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let env_name = body
        .api_key_env
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match (plaintext, env_name) {
        (Some(_), Some(_)) => Err(UpstreamError::BadRequest {
            error: "conflicting_api_key",
            detail: "provide either api_key_value or api_key_env, not both".to_owned(),
        }),
        (None, None) => Ok(None),
        (value_opt, env_opt) => {
            if !matches!(current.kind, UpstreamKind::AnthropicApiKey) {
                return Err(UpstreamError::BadRequest {
                    error: "unexpected_api_key",
                    detail: format!(
                        "{} upstreams do not accept api_key_value or api_key_env",
                        current.kind.as_str()
                    ),
                });
            }
            if let Some(value) = value_opt {
                encrypt_plaintext_value(state, value, current.id.as_bytes()).map(Some)
            } else if let Some(env) = env_opt {
                encrypt_env_value(state, env, current.id.as_bytes()).map(Some)
            } else {
                Ok(None)
            }
        }
    }
}

fn encrypt_env_value(
    state: &AdminState,
    env_name: &str,
    aad: &[u8],
) -> Result<Vec<u8>, UpstreamError> {
    let value = std::env::var(env_name).map_err(|error| UpstreamError::BadRequest {
        error: "missing_api_key_env_value",
        detail: format!("{env_name}: {error}"),
    })?;
    encrypt_plaintext_value(state, &value, aad)
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
    if body.api_key_env.is_some() || body.api_key_value.is_some() {
        fields.push("api_key_ciphertext");
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

async fn list_all_upstreams(state: &AdminState) -> Result<Vec<UpstreamRecord>, UpstreamError> {
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
    use cc_lb_aead::{AeadEncryptedField, AeadService};
    use cc_lb_config::Config;
    use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
    use cc_lb_control::api_keys::limit_engine::LimitEngine;
    use cc_lb_control::api_keys::principal_view::PrincipalView;
    use cc_lb_control::{
        DynamicView, DynamicViewBuilder, DynamicViewHolder, RouteDecision, RouteError,
        RouterPlugin, RoutingContext, UpstreamStatusSnapshot,
    };
    use cc_lb_domain::{Principal, Upstream, UpstreamCandidate};
    use cc_lb_observability::{ObservabilityError, ObservabilityHook, ObserveEvent};
    use cc_lb_storage_api::{BackendKind, MetaStore, UpstreamStore};
    use cc_lb_upstream::{
        ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError,
        SignerFactory, SigningCapability, UpstreamError,
    };
    use http_body_util::BodyExt;
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    use super::*;

    type TestStorage = cc_lb_storage_sqlite::SqliteStorage;

    const TEST_FIRE_NOW_HOLDER: &str = "fire-now:00000000-0000-4000-8000-000000000000";

    fn fire_now_warmup_request(holder: &str) -> Request<Full<Bytes>> {
        let base_url = Url::parse("https://api.anthropic.com/").expect("valid URL");
        build_fire_now_warmup_request("test-token", &base_url, holder)
            .expect("request builds successfully")
    }

    async fn fire_now_warmup_body_from_request(holder: &str) -> Value {
        let bytes = fire_now_warmup_request(holder)
            .into_body()
            .collect()
            .await
            .expect("body collects")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("valid json body")
    }

    #[test]
    fn fire_now_request_shape_includes_minimal_headers() {
        let request = fire_now_warmup_request(TEST_FIRE_NOW_HOLDER);

        assert_eq!(request.uri().path(), "/v1/messages");
        assert_eq!(request.method(), Method::POST);
        assert_eq!(
            request.headers().get("anthropic-version"),
            Some(&HeaderValue::from_static("2023-06-01"))
        );
        assert_eq!(
            request.headers().get("anthropic-beta"),
            Some(&HeaderValue::from_static(WARMUP_ANTHROPIC_BETA))
        );
    }

    #[tokio::test]
    async fn fire_now_request_shape_has_no_system_or_tools_or_stream() {
        let body = fire_now_warmup_body_from_request(TEST_FIRE_NOW_HOLDER).await;

        assert!(body.get("system").is_none());
        assert_eq!(body.get("max_tokens"), Some(&json!(1)));
        assert!(body.get("tools").is_none());
        assert!(body.get("stream").is_none());
    }

    #[tokio::test]
    async fn fire_now_request_shape_has_no_metadata() {
        let body = fire_now_warmup_body_from_request(TEST_FIRE_NOW_HOLDER).await;

        assert!(body.get("metadata").is_none());
    }

    #[test]
    fn fire_now_request_body_matches_locked_json_byte_for_byte() {
        let expected = br#"{"max_tokens":1,"messages":[{"content":".","role":"user"}],"model":"claude-haiku-4-5-20251001"}"#;

        assert_eq!(
            warmup_body_bytes().expect("body serializes"),
            expected.to_vec()
        );
    }

    struct TestContext {
        _dir: tempfile::TempDir,
        state: AdminState,
        storage: Arc<TestStorage>,
        aead: Arc<AeadService>,
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

    struct TestRouter;

    impl RouterPlugin for TestRouter {
        fn route(
            &self,
            _ctx: &RoutingContext,
            _principal: &Principal,
            _candidates: &[UpstreamCandidate],
        ) -> Result<RouteDecision, RouteError> {
            Err(RouteError::NoRoute {
                reason: "test router has no route".to_owned(),
            })
        }
    }

    struct TestHook;

    impl ObservabilityHook for TestHook {
        fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
            Ok(())
        }
    }

    struct TestWarmupPort;

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
                crate::ports::WarmupAttemptResult::PreflightActiveWindow { cycle_key } => (
                    WarmupAttemptOutcome::Success(
                        cc_lb_storage_api::WarmupSuccessReason::WindowAlreadyActive,
                    ),
                    Some(cycle_key),
                    None,
                ),
            };
            crate::ports::WarmupAttemptOutcomeSnapshot {
                outcome,
                cycle_key,
                error_detail,
            }
        }
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
    async fn fire_now_returns_200_on_success() {
        let context = test_context().await;
        let (base_url, server) = spawn_warmup_server(StatusCode::OK).await;
        let upstream = create_oauth_upstream(
            context.storage.as_ref(),
            context.aead.as_ref(),
            true,
            Some(base_url),
        )
        .await;

        let (status, body) = fire_now_response(context.state, upstream.id).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["fired"], true);
        assert!(body["cycle_key"].as_i64().is_some());
        server.abort();
    }

    #[tokio::test]
    async fn fire_now_returns_502_on_permanent_abandon() {
        let context = test_context().await;
        let (base_url, server) = spawn_warmup_server(StatusCode::UNAUTHORIZED).await;
        let upstream = create_oauth_upstream(
            context.storage.as_ref(),
            context.aead.as_ref(),
            true,
            Some(base_url),
        )
        .await;

        let (status, body) = fire_now_response(context.state, upstream.id).await;

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body["fired"], false);
        assert_eq!(body["reason"], "auth_failed");
        server.abort();
    }

    #[tokio::test]
    async fn fire_now_returns_503_on_transient() {
        let context = test_context().await;
        let (base_url, server) = spawn_warmup_server(StatusCode::INTERNAL_SERVER_ERROR).await;
        let upstream = create_oauth_upstream(
            context.storage.as_ref(),
            context.aead.as_ref(),
            true,
            Some(base_url),
        )
        .await;

        let (status, body) = fire_now_response(context.state, upstream.id).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["fired"], false);
        assert_eq!(body["reason"], "upstream_5xx");
        server.abort();
    }

    async fn test_context() -> TestContext {
        let dir = tempfile::tempdir().expect("storage dir");
        let database_url = format!("sqlite://{}", dir.path().join("upstreams.sqlite").display());
        let storage =
            cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
                .await
                .expect("storage opens");
        storage
            .initialize(BackendKind::Sqlite)
            .await
            .expect("storage initializes");
        let storage = Arc::new(storage);
        let aead = Arc::new(AeadService::from_master_key([8; 32]));
        let state = AdminState {
            config_path: None,
            startup_config_overrides: Default::default(),
            storage: Some(storage.clone()),
            key_store: None,
            aead: aead.clone(),
            limit_engine: LimitEngine::new(
                Arc::new(KeyConcurrencyManager::new()),
                Arc::new(cc_lb_clock::SystemClock),
            ),
            lifecycle: Some(crate::AdminPorts {
                warmup: Some(Arc::new(TestWarmupPort)),
                ..crate::AdminPorts::default()
            }),
            subscription_metadata_hook: None,
            lazy_refresher: None,
            runtime: None,
            data_dir: None,
            warmup_dialect_dispatcher: None,
            dynamic_view: Arc::new(DynamicViewHolder::new(test_view())),
            config: Arc::new(Config::default()),
            dynamic_view_rebinder: None,
            scheduler: None,
            admin_auth: Arc::new(crate::auth::AdminAuthenticator::new(Vec::new())),
            start_time: std::time::Instant::now(),
            event_bus: None,
            storage_tail: crate::events::storage_tail_channel(),
            clock: Arc::new(cc_lb_clock::SystemClock),
        };
        TestContext {
            _dir: dir,
            state,
            storage,
            aead,
        }
    }

    fn test_view() -> Arc<DynamicView> {
        let principal_view = Arc::new(PrincipalView::from_db(&[], HashMap::new()));
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(TestSignerFactory))
            .global_router(Arc::new(TestRouter))
            .global_observability_hooks(vec![Arc::new(TestHook)])
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
                name: format!("api-key-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: Some(vec![1, 2, 3]),
                oauth_token_generation: None,
                warmup_enabled,
                warmup_dialect_plugin: None,
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
        let created = storage
            .create(UpstreamCreate {
                name: format!("oauth-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicOauth,
                base_url,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled,
                warmup_dialect_plugin,
            })
            .await
            .expect("upstream create succeeds");
        let bundle = OAuthTokenBundle {
            access_token: "test-access-token".to_owned(),
            refresh_token: "test-refresh-token".to_owned(),
            expires_at_unix_secs: u64::MAX / 2,
            refresh_token_expires_at_unix_secs: None,
            scopes: Vec::new(),
            never_refresh: false,
        };
        let encrypted =
            AeadEncryptedField::<OAuthTokenBundle>::encrypt(aead, &bundle, created.id.as_bytes())
                .expect("token encryption succeeds");
        storage
            .store_oauth_tokens(
                created.id,
                created.revision,
                encrypted,
                bundle.never_refresh,
            )
            .await
            .expect("token store succeeds")
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
        let response = router()
            .with_state(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/admin/v1/upstreams/{upstream_id}/warmup/fire-now"))
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
    async fn create_upstream_defaults_warmup_enabled_for_anthropic_oauth_kind() {
        let context = test_context().await;
        let body = serde_json::json!({
            "name": "oauth-warmup-default",
            "kind": "anthropic_oauth",
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
        assert_eq!(body["warmup_enabled"], true);
    }

    #[tokio::test]
    async fn create_upstream_defaults_warmup_disabled_for_api_key_kind() {
        let context = test_context().await;
        let body = serde_json::json!({
            "name": "api-key-warmup-default",
            "kind": "anthropic_api_key",
            "api_key_value": "test-api-key",
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
        assert_eq!(body["warmup_enabled"], false);
    }

    #[tokio::test]
    async fn create_api_key_upstream_binds_ciphertext_to_upstream_id() {
        let context = test_context().await;
        let body = serde_json::json!({
            "name": "api-key-aad-bound",
            "kind": "anthropic_api_key",
            "api_key_value": "sk-ant-UPSTREAM-SECRET",
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

    #[tokio::test]
    async fn create_api_key_upstream_clears_raced_apply_error() {
        let context = test_context().await;
        let created = context
            .storage
            .create(UpstreamCreate {
                name: format!("api-key-raced-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            })
            .await
            .expect("upstream create succeeds");
        // Simulate a view rebuild landing between the credential-less commit
        // and the credential store: it marks the upstream errored.
        context
            .storage
            .set_last_apply_error(
                created.id,
                Some("anthropic api-key upstream missing api_key_ciphertext".to_owned()),
            )
            .await
            .expect("status seed succeeds");

        let record = store_api_key_credential(
            &context.state,
            context.storage.as_ref(),
            &created,
            "sk-ant-RACED-SECRET",
        )
        .await
        .expect("credential store succeeds");

        let body = serde_json::to_value(upstream_response(&record)).expect("response serializes");
        assert!(body["status"]["last_apply_error"].is_null());
        let stored = context
            .storage
            .get_by_id(created.id)
            .await
            .expect("read succeeds")
            .expect("upstream exists");
        assert!(stored.last_apply_error.is_none());
        assert!(stored.api_key_ciphertext.is_some());
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

    #[test]
    fn warmup_bootstrap_task_uses_seed_suffix_as_key_and_cycle() {
        let upstream_id =
            Uuid::parse_str("12345678-1234-5678-1234-567812345678").expect("uuid parses");
        let seed_secs = 1_800_000_000;

        let task = warmup_bootstrap_task(upstream_id, seed_secs);

        assert_eq!(
            task.idempotency_key.as_deref(),
            Some("adaptive:warmup:12345678-1234-5678-1234-567812345678:bootstrap:1800000000")
        );
        assert_eq!(task.run_at_unix_secs, Some(seed_secs));
        let cc_lb_scheduler::worker::AdaptiveJob::Warmup(job) = task.args else {
            panic!("expected warmup job");
        };
        assert_eq!(job.upstream_id, upstream_id);
        assert_eq!(job.cycle_key, seed_secs);
    }

    #[tokio::test]
    async fn update_warmup_enabled_rejects_when_oauth_credentials_missing() {
        let context = test_context().await;
        let upstream = context
            .storage
            .create(UpstreamCreate {
                name: format!("oauth-no-creds-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
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
                name: format!("oauth-fire-no-creds-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: true,
                warmup_dialect_plugin: None,
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
