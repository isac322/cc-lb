use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderValue, Method, Request, StatusCode,
        header::{ETAG, IF_MATCH},
    },
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use bytes::Bytes;
use cc_lb_aead::{AeadEncryptedField, OAuthTokenBundle};
use cc_lb_core::anthropic_compat::{
    CLAUDE_CODE_STABLE_VERSION_FALLBACK, CLAUDE_CODE_STABLE_VERSION_KEY, claude_code_user_agent,
};
use cc_lb_core::warmup_attempts::{
    WarmupAttemptExecution, WarmupAttemptExecutionResult, execute_warmup_attempt,
};
use cc_lb_core::{
    AuditEntry, AuditPayload, Clock, UnifiedQuotaObservation, make_metadata_http_client,
    observe_subscription_quota_headers, parse_anthropic_unified_headers, run_metadata_refresh,
};
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerPushTask};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamWarmupDialectPlugin};
use cc_lb_storage_api::{
    OrganizationMetadataRecord, Storage, StorageError, SubscriptionQuotaLatestRecord,
    SubscriptionQuotaWindow, UpstreamCreate, UpstreamRecord, UpstreamStatusUpdate, UpstreamStore,
    UpstreamSubscriptionMetadataRecord, UpstreamUpdate, WarmupAttemptOutcome, WarmupAttemptReason,
    WarmupAttemptTrigger,
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
use crate::{AdminState, WarmupDialectDispatchErrorKind};

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
        last_warmup_at_unix_secs: Some(Some(cc_lb_core::clock::unix_secs(clock.now()))),
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
    warmup_enabled: bool,
    #[serde(default)]
    warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
}

#[derive(Debug, Deserialize)]
struct UpstreamUpdateBody {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    base_url: Option<Url>,
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

enum UpstreamError {
    StorageUnavailable,
    NotFound,
    BadRequest { error: &'static str, detail: String },
    MissingIfMatch,
    StaleRevision { current_revision: u64 },
    Conflict { detail: String },
    NotOauthUpstream,
    CredentialDecrypt,
    RefreshUnavailable,
    RefreshFailed { detail: String },
    MetadataRefreshTimeout,
    Internal { detail: String },
    Storage(StorageError),
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
            Self::Internal { detail } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "internal_error", "detail": detail })),
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
    Json(body): Json<UpstreamCreateBody>,
) -> Result<Response, UpstreamError> {
    let storage = storage(&state)?;
    let api_key_ciphertext = api_key_ciphertext_for_create(&state, &body)?;
    let kind = body.kind;
    // AnthropicOauth creates carry no credentials; warmup_due_candidate would silently skip.
    if body.warmup_enabled && kind == UpstreamKind::AnthropicOauth {
        return Err(UpstreamError::BadRequest {
            error: "warmup_requires_oauth_credentials",
            detail: "complete OAuth before enabling warmup".to_owned(),
        });
    }
    let created = UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: body.name,
            kind,
            base_url: body.base_url,
            api_key_ciphertext,
            oauth_token_generation: None,
            warmup_enabled: body.warmup_enabled,
            warmup_dialect_plugin: body.warmup_dialect_plugin,
        },
    )
    .await?;
    enqueue_upstream_audit(
        &state,
        &created,
        AuditPayload::UpstreamCreate {
            upstream_id: created.id.to_string(),
            kind: created.kind.as_str().to_owned(),
        },
    );

    let mut response = (StatusCode::CREATED, Json(upstream_response(&created))).into_response();
    response.headers_mut().insert(
        axum::http::header::LOCATION,
        HeaderValue::from_str(&format!("/admin/v1/upstreams/{}", created.id)).map_err(|error| {
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

    subscription_metadata_response(storage.as_ref(), upstream_id)
        .await
        .map(Json)
}

async fn fire_now_upstream_warmup(
    State(state): State<AdminState>,
    Path(upstream_id): Path<Uuid>,
) -> Result<Response, UpstreamError> {
    let storage = storage_arc(&state)?;
    let upstream = UpstreamStore::get_by_id(storage.as_ref(), upstream_id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
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
        record_fire_now_skip(
            storage.as_ref(),
            &upstream,
            now_unix_secs,
            WarmupAttemptReason::UpstreamDisabled,
            None,
        )
        .await;
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "warmup_disabled" })),
        )
            .into_response());
    }
    if upstream.oauth_credentials.is_none() {
        record_fire_now_skip(
            storage.as_ref(),
            &upstream,
            now_unix_secs,
            WarmupAttemptReason::OauthCredentialsMissing,
            None,
        )
        .await;
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({ "fired": false, "reason": "oauth_credentials_missing" })),
        )
            .into_response());
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
        record_fire_now_failure(
            storage.as_ref(),
            &upstream,
            now_unix_secs,
            candidate_cycle_key,
            &holder,
            WarmupAttemptReason::DialectPluginFailed,
            Some(StatusCode::BAD_GATEWAY),
            Some("warmup dialect dispatcher unavailable"),
        )
        .await;
        return Ok((
            StatusCode::BAD_GATEWAY,
            Json(json!({ "fired": false, "reason": "dialect_plugin_failed" })),
        )
            .into_response());
    }
    let dialect_dispatch_bundle = upstream.warmup_dialect_plugin.as_ref().and_then(|_| {
        let runtime = state.runtime.as_deref()?;
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
            Ok(Ok(outcome)) => fire_now_response_attempt(outcome.status, outcome.headers),
            Ok(Err(error)) => match error.kind {
                WarmupDialectDispatchErrorKind::Transient => {
                    FireNowDispatchAttempt::TransientFailure {
                        reason: WarmupAttemptReason::DialectPluginTransient,
                        status: StatusCode::SERVICE_UNAVAILABLE,
                        error_detail: error.detail,
                    }
                }
                WarmupDialectDispatchErrorKind::Permanent => {
                    FireNowDispatchAttempt::PermanentFailure {
                        reason: WarmupAttemptReason::DialectPluginFailed,
                        status: StatusCode::BAD_GATEWAY,
                        error_detail: error.detail,
                    }
                }
            },
            Err(_) => FireNowDispatchAttempt::TransientFailure {
                reason: WarmupAttemptReason::RequestTimeout,
                status: StatusCode::SERVICE_UNAVAILABLE,
                error_detail: "warmup dispatch timed out".to_owned(),
            },
        }
    } else {
        let bundle = match decrypt_oauth_bundle(&state, &upstream) {
            Ok(bundle) => bundle,
            Err(error) => {
                record_fire_now_failure(
                    storage.as_ref(),
                    &upstream,
                    now_unix_secs,
                    candidate_cycle_key,
                    &holder,
                    WarmupAttemptReason::CredentialDecryptFailed,
                    None,
                    Some("oauth credential decrypt failed"),
                )
                .await;
                return Err(error);
            }
        };
        let access_token =
            match fresh_enough_access_token(&state, storage.clone(), &upstream, bundle).await {
                Ok(access_token) => access_token,
                Err(error) => {
                    record_fire_now_failure(
                        storage.as_ref(),
                        &upstream,
                        now_unix_secs,
                        candidate_cycle_key,
                        &holder,
                        WarmupAttemptReason::OauthRefreshFailed,
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
                record_fire_now_failure(
                    storage.as_ref(),
                    &upstream,
                    now_unix_secs,
                    candidate_cycle_key,
                    &holder,
                    WarmupAttemptReason::RequestBuildFailed,
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
                reason: WarmupAttemptReason::RequestTimeout,
                status: StatusCode::SERVICE_UNAVAILABLE,
                error_detail: "warmup dispatch timed out".to_owned(),
            },
        }
    };
    let record = execute_warmup_attempt(WarmupAttemptExecution {
        storage: storage.as_ref(),
        upstream: &upstream,
        scheduled_for_unix_secs: now_unix_secs,
        trigger: WarmupAttemptTrigger::Manual,
        replica_id: None,
        lease_holder: Some(holder.as_str()),
        expected_cycle_key: Some(candidate_cycle_key),
        attempted_at_unix_secs: now_unix_secs,
        completed_at_unix_secs: Some(unix_now_secs_i64(&*state.clock)?),
        result: execution_result_from_fire_now_attempt(&dispatch_attempt),
    })
    .await;
    if let FireNowDispatchAttempt::Response { headers, .. } = &dispatch_attempt {
        record_fire_now_subscription_quota_observations(
            &state,
            storage.as_ref(),
            upstream_id,
            headers,
        )
        .await?;
    }
    let status = fire_now_attempt_status(&dispatch_attempt);
    tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, status = %status, outcome = ?record.outcome, dispatch_error = record.error_detail.as_deref(), action = "dispatch_result");

    match record.outcome {
        WarmupAttemptOutcome::SuccessFresh | WarmupAttemptOutcome::SuccessRedundant => {
            let response_cycle_key = record.cycle_key.unwrap_or(candidate_cycle_key);
            tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, cycle_key = %candidate_cycle_key, response_cycle_key = %response_cycle_key, action = "dispatch_succeeded");
            write_warmup_status_after_success(storage.as_ref(), upstream_id, &*state.clock).await;
            Ok(Json(json!({ "fired": true, "cycle_key": response_cycle_key })).into_response())
        }
        WarmupAttemptOutcome::PermanentFailure => {
            let reason = record
                .reason
                .unwrap_or(WarmupAttemptReason::DialectPluginFailed);
            tracing::warn!(target: "warmup", upstream_id = %upstream_id, reason = %warmup_attempt_reason_str(reason), action = "cycle_abandoned");
            Ok((
                StatusCode::BAD_GATEWAY,
                Json(json!({ "fired": false, "reason": warmup_attempt_reason_str(reason) })),
            )
                .into_response())
        }
        WarmupAttemptOutcome::TransientFailure => Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "fired": false, "reason": "transient" })),
        )
            .into_response()),
        WarmupAttemptOutcome::Skipped => {
            let reason = record
                .reason
                .expect("Skipped warm-up attempt always carries a reason");
            Ok((
                StatusCode::ACCEPTED,
                Json(json!({ "fired": false, "reason": warmup_attempt_reason_str(reason) })),
            )
                .into_response())
        }
    }
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
                reason: WarmupAttemptReason::RequestBuildFailed,
                status: StatusCode::INTERNAL_SERVER_ERROR,
                error_detail: error,
            };
        }
    };
    match client.request(request).await {
        Ok(response) => fire_now_response_attempt(response.status(), response.headers().clone()),
        Err(error) => FireNowDispatchAttempt::TransientFailure {
            reason: WarmupAttemptReason::NetworkError,
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
    if let Some(lifecycle) = &state.lifecycle {
        let observed_at = UNIX_EPOCH
            .checked_add(Duration::from_millis(observed_at_unix_millis))
            .ok_or_else(|| invalid_warmup_state("warmup observed timestamp overflow"))?;
        lifecycle.record_subscription_quota_observations(headers, upstream_id, observed_at);
        return Ok(());
    }
    let records = observe_subscription_quota_headers(headers, upstream_id, observed_at_unix_millis);
    if !records.is_empty() {
        storage.put_subscription_quota_batch(&records).await?;
    }
    Ok(())
}

enum FireNowDispatchAttempt {
    Response {
        status: StatusCode,
        headers: HeaderMap,
        observations: Vec<UnifiedQuotaObservation>,
    },
    TransientFailure {
        reason: WarmupAttemptReason,
        status: StatusCode,
        error_detail: String,
    },
    PermanentFailure {
        reason: WarmupAttemptReason,
        status: StatusCode,
        error_detail: String,
    },
}

fn fire_now_response_attempt(status: StatusCode, headers: HeaderMap) -> FireNowDispatchAttempt {
    let observations = parse_anthropic_unified_headers(&headers);
    FireNowDispatchAttempt::Response {
        status,
        headers,
        observations,
    }
}

fn execution_result_from_fire_now_attempt(
    attempt: &FireNowDispatchAttempt,
) -> WarmupAttemptExecutionResult<'_> {
    match attempt {
        FireNowDispatchAttempt::Response {
            status,
            observations,
            ..
        } => WarmupAttemptExecutionResult::Response {
            status: *status,
            observations,
            error_detail: None,
        },
        FireNowDispatchAttempt::TransientFailure {
            reason,
            status,
            error_detail,
        } => WarmupAttemptExecutionResult::TransientFailure {
            reason: *reason,
            http_status: Some(*status),
            error_detail: Some(error_detail.as_str()),
        },
        FireNowDispatchAttempt::PermanentFailure {
            reason,
            status,
            error_detail,
        } => WarmupAttemptExecutionResult::PermanentFailure {
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

async fn record_fire_now_skip(
    storage: &dyn Storage,
    upstream: &UpstreamRecord,
    now_unix_secs: i64,
    reason: WarmupAttemptReason,
    error_detail: Option<&str>,
) {
    execute_warmup_attempt(WarmupAttemptExecution {
        storage,
        upstream,
        scheduled_for_unix_secs: now_unix_secs,
        trigger: WarmupAttemptTrigger::Manual,
        replica_id: None,
        lease_holder: None,
        expected_cycle_key: None,
        attempted_at_unix_secs: now_unix_secs,
        completed_at_unix_secs: Some(now_unix_secs),
        result: WarmupAttemptExecutionResult::Skipped {
            reason,
            cycle_key: None,
            error_detail,
        },
    })
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn record_fire_now_failure(
    storage: &dyn Storage,
    upstream: &UpstreamRecord,
    now_unix_secs: i64,
    candidate_cycle_key: i64,
    holder: &str,
    reason: WarmupAttemptReason,
    status: Option<StatusCode>,
    error_detail: Option<&str>,
) {
    execute_warmup_attempt(WarmupAttemptExecution {
        storage,
        upstream,
        scheduled_for_unix_secs: now_unix_secs,
        trigger: WarmupAttemptTrigger::Manual,
        replica_id: None,
        lease_holder: Some(holder),
        expected_cycle_key: Some(candidate_cycle_key),
        attempted_at_unix_secs: now_unix_secs,
        completed_at_unix_secs: Some(now_unix_secs),
        result: WarmupAttemptExecutionResult::PermanentFailure {
            reason,
            http_status: status,
            error_detail,
        },
    })
    .await;
}

const fn warmup_attempt_reason_str(reason: WarmupAttemptReason) -> &'static str {
    match reason {
        WarmupAttemptReason::WindowAlreadyActive => "window_already_active",
        WarmupAttemptReason::SevenDayQuotaExhausted => "seven_day_quota_exhausted",
        WarmupAttemptReason::Http429MissingCycleKey => "http_429_missing_cycle_key",
        WarmupAttemptReason::Upstream5xx => "upstream_5xx",
        WarmupAttemptReason::NetworkError => "network_error",
        WarmupAttemptReason::RequestTimeout => "request_timeout",
        WarmupAttemptReason::RequestBuildFailed => "request_build_failed",
        WarmupAttemptReason::OauthRefreshFailed => "oauth_refresh_failed",
        WarmupAttemptReason::CredentialDecryptFailed => "credential_decrypt_failed",
        WarmupAttemptReason::AuthFailed => "auth_failed",
        WarmupAttemptReason::Forbidden => "forbidden",
        WarmupAttemptReason::BadRequest => "bad_request",
        WarmupAttemptReason::NotFound => "not_found",
        WarmupAttemptReason::DialectPluginFailed => "dialect_plugin_failed",
        WarmupAttemptReason::DialectPluginTransient => "dialect_plugin_transient",
        WarmupAttemptReason::OauthCredentialsMissing => "oauth_credentials_missing",
        WarmupAttemptReason::LeaseHeld => "lease_held",
        WarmupAttemptReason::UpstreamDisabled => "upstream_disabled",
        WarmupAttemptReason::UpstreamDeleted => "upstream_deleted",
    }
}

fn upstream_base_url(upstream: &UpstreamRecord) -> Result<Url, UpstreamError> {
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
            base_url: body.base_url,
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
            enqueue_upstream_audit(
                &state,
                &updated,
                AuditPayload::UpstreamUpdate {
                    upstream_id: updated.id.to_string(),
                    fields_changed,
                },
            );
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
    let seed_secs = cc_lb_core::clock::unix_secs(state.clock.now());
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
    }
}

async fn enable_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, UpstreamError> {
    set_enabled(state, id, headers, true).await
}

async fn disable_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, UpstreamError> {
    set_enabled(state, id, headers, false).await
}

async fn delete_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
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
            enqueue_upstream_audit(
                &state,
                &current,
                AuditPayload::UpstreamDelete {
                    upstream_id: current.id.to_string(),
                },
            );
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
            enqueue_upstream_audit(
                &state,
                &updated,
                AuditPayload::UpstreamUpdate {
                    upstream_id: updated.id.to_string(),
                    fields_changed: vec!["warmup_dialect_plugin"],
                },
            );
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

    enqueue_upstream_audit(
        &state,
        &updated,
        if enabled {
            AuditPayload::UpstreamEnable {
                upstream_id: updated.id.to_string(),
            }
        } else {
            AuditPayload::UpstreamDisable {
                upstream_id: updated.id.to_string(),
            }
        },
    );

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

fn storage_arc(state: &AdminState) -> Result<Arc<dyn Storage>, UpstreamError> {
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

async fn fresh_enough_access_token(
    state: &AdminState,
    storage: Arc<dyn Storage>,
    upstream: &UpstreamRecord,
    bundle: OAuthTokenBundle,
) -> Result<String, UpstreamError> {
    let now = cc_lb_core::clock::unix_secs(state.clock.now());
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

fn decrypt_oauth_bundle(
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

async fn metadata_user_agent(storage: &dyn Storage) -> Result<String, UpstreamError> {
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

fn api_key_ciphertext_for_create(
    state: &AdminState,
    body: &UpstreamCreateBody,
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

    match body.kind {
        UpstreamKind::AnthropicApiKey => match (plaintext, env_name) {
            (Some(_), Some(_)) => Err(UpstreamError::BadRequest {
                error: "conflicting_api_key",
                detail: "provide either api_key_value or api_key_env, not both".to_owned(),
            }),
            (Some(value), None) => {
                encrypt_plaintext_value(state, value, body.name.as_bytes()).map(Some)
            }
            (None, Some(env)) => encrypt_env_value(state, env, body.name.as_bytes()).map(Some),
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
    let value = plaintext.to_owned();
    let encrypted = AeadEncryptedField::<String>::encrypt(state.aead.as_ref(), &value, aad)
        .map_err(|error| UpstreamError::BadRequest {
            error: "credential_encryption_failed",
            detail: error.to_string(),
        })?;
    Ok(encrypted.ciphertext().to_vec())
}

fn changed_fields(body: &UpstreamUpdateBody) -> Vec<&'static str> {
    let mut fields = Vec::new();
    if body.name.is_some() {
        fields.push("name");
    }
    if body.base_url.is_some() {
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

fn enqueue_upstream_audit(state: &AdminState, upstream: &UpstreamRecord, payload: AuditPayload) {
    let Some(audit_sink) = &state.audit_sink else {
        return;
    };
    let ts = cc_lb_core::clock::unix_secs(state.clock.now());
    let action = payload.to_string();
    let mut entry: AuditEntry = payload.into();
    entry.ts = ts;
    entry.request_id = format!("admin-upstream-{}-{ts}", upstream.id);
    entry.principal_id = "admin".to_owned();
    entry.route = "admin_v1_upstreams".to_owned();
    entry.upstream = upstream.name.clone();
    entry.status = 200;
    entry.duration_ms = 0;
    entry.actor = Some("admin".to_owned());
    entry.admin_action = Some(action);
    let _ = audit_sink.try_enqueue(entry);
}

fn unix_now_secs_i64(clock: &dyn Clock) -> Result<i64, UpstreamError> {
    i64::try_from(cc_lb_core::clock::unix_secs(clock.now()))
        .map_err(|_| invalid_warmup_state("current timestamp overflow"))
}

fn unix_now_millis(clock: &dyn Clock) -> Result<u64, UpstreamError> {
    cc_lb_core::clock::unix_secs(clock.now())
        .checked_mul(1_000)
        .ok_or_else(|| invalid_warmup_state("current timestamp millis overflow"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use axum::body::{Body as AxumBody, to_bytes};
    use cc_lb_aead::AeadService;
    use cc_lb_config::Config;
    use cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager;
    use cc_lb_core::api_keys::limit_engine::LimitEngine;
    use cc_lb_core::api_keys::principal_view::PrincipalView;
    use cc_lb_core::{
        Body as CoreBody, DispatchError, DynamicView, DynamicViewBuilder, DynamicViewHolder,
        ErrorNormalizer, UpstreamDispatch, UpstreamStatusSnapshot,
    };
    use cc_lb_plugin_api::{
        ApiKeyAwareSignerFactory, ObservabilityError, ObservabilityHook, ObserveEvent, Principal,
        RequestContext, RouteDecision, RouteError, RouterPlugin, ShapedRequest, SignedRequest,
        Signer, SignerError, SignerFactory, SigningCapability, Upstream, UpstreamCandidate,
    };
    use cc_lb_storage_api::{BackendKind, MetaStore, UpstreamStore};
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
            _api_key: String,
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

        async fn on_unauthorized(
            &self,
            _err: &cc_lb_plugin_api::UpstreamError,
        ) -> cc_lb_plugin_api::RetryDecision {
            cc_lb_plugin_api::RetryDecision::Fail
        }
    }

    struct TestRouter;

    impl RouterPlugin for TestRouter {
        fn route(
            &self,
            _ctx: &RequestContext,
            _principal: &Principal,
            _candidates: &[UpstreamCandidate],
        ) -> Result<RouteDecision, RouteError> {
            Err(RouteError::NoRoute {
                reason: "test router has no route".to_owned(),
            })
        }
    }

    struct TestDispatcher;

    #[async_trait]
    impl UpstreamDispatch for TestDispatcher {
        async fn dispatch(
            &self,
            _request: SignedRequest,
        ) -> Result<axum::http::Response<CoreBody>, DispatchError> {
            Ok(axum::http::Response::builder()
                .status(StatusCode::OK)
                .body(CoreBody::from(Bytes::new()))
                .expect("test response builds"))
        }
    }

    struct TestHook;

    impl ObservabilityHook for TestHook {
        fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
            Ok(())
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
        assert_eq!(body["reason"], "transient");
        server.abort();
    }

    async fn test_context() -> TestContext {
        let dir = tempfile::tempdir().expect("storage dir");
        let database_url = format!("sqlite://{}", dir.path().join("upstreams.sqlite").display());
        let storage =
            cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_core::SystemClock))
                .await
                .expect("storage opens");
        storage
            .initialize(BackendKind::Sqlite)
            .await
            .expect("storage initializes");
        let storage = Arc::new(storage);
        let aead = Arc::new(AeadService::from_master_key([8; 32]));
        let state = AdminState {
            storage: Some(storage.clone()),
            key_store: None,
            aead: aead.clone(),
            limit_engine: LimitEngine::new(
                Arc::new(KeyConcurrencyManager::new()),
                Arc::new(cc_lb_core::SystemClock),
            ),
            lifecycle: None,
            subscription_metadata_hook: None,
            lazy_refresher: None,
            runtime: None,
            data_dir: None,
            warmup_dialect_dispatcher: None,
            audit_sink: None,
            dynamic_view: Arc::new(DynamicViewHolder::new(test_view())),
            config: Arc::new(Config::default()),
            scheduler: None,
            admin_token: None,
            start_time: std::time::Instant::now(),
            event_bus: None,
            clock: Arc::new(cc_lb_core::SystemClock),
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
            .dispatcher(Arc::new(TestDispatcher))
            .global_observability_hooks(vec![Arc::new(TestHook)])
            .error_normalizer(Arc::new(ErrorNormalizer::new()))
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
            scopes: Vec::new(),
        };
        let encrypted =
            AeadEncryptedField::<OAuthTokenBundle>::encrypt(aead, &bundle, created.id.as_bytes())
                .expect("token encryption succeeds");
        storage
            .store_oauth_tokens(created.id, created.revision, encrypted)
            .await
            .expect("token store succeeds")
    }

    async fn fire_now_response(state: AdminState, upstream_id: Uuid) -> (StatusCode, Value) {
        let response = router()
            .with_state(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/admin/v1/upstreams/{upstream_id}/warmup/fire-now"))
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
    async fn create_upstream_with_warmup_enabled_rejects_anthropic_oauth_kind() {
        let context = test_context().await;
        let body = serde_json::json!({
            "name": "oauth-warmup-precreate",
            "kind": "anthropic_oauth",
            "warmup_enabled": true,
        });
        let response = router()
            .with_state(context.state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/admin/v1/upstreams")
                    .header("content-type", "application/json")
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
        assert_eq!(body["error"], "warmup_requires_oauth_credentials");
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
