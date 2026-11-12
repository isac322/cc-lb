use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cc_lb_aead::OAuthTokenBundle;
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_storage_api::upstream::{
    UpstreamKind, UpstreamRecord, UpstreamStore, UpstreamWarmupDialectPlugin,
};
use cc_lb_storage_api::warmup_attempts::{
    WarmupAttemptCursor, WarmupAttemptListFilters, WarmupAttemptRecord, WarmupAttemptStatus,
    WarmupLane,
};
use cc_lb_storage_api::{
    Storage, StorageError, SubscriptionQuotaSample, SubscriptionQuotaSampleKind,
    SubscriptionQuotaWindow,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::AdminState;

const RECENT_ATTEMPT_LIMIT: u32 = 10;
const ATTEMPT_DEFAULT_LIMIT: u32 = 50;
const ATTEMPT_MAX_LIMIT: u32 = 200;

#[derive(Debug, Deserialize)]
pub(crate) struct WarmupAttemptsQuery {
    limit: Option<u32>,
    before: Option<String>,
    status: Option<WarmupAttemptStatus>,
    lane: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct WarmupSummaryResponse {
    upstream_id: Uuid,
    dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
    lanes: Vec<WarmupLaneSummary>,
}

#[derive(Debug, Serialize)]
struct WarmupLaneSummary {
    lane: WarmupLane,
    eligible: bool,
    ineligible_reason: Option<WarmupLaneIneligibleReason>,
    target_windows: Vec<SubscriptionQuotaWindow>,
    next_scheduled_at_unix_secs: Option<i64>,
    last_attempt: Option<WarmupAttemptRecord>,
    recent_attempts: Vec<WarmupAttemptRecord>,
}

/// Why a lane cannot be warmed right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WarmupLaneIneligibleReason {
    /// The credential profile does not serve the lane's quota group
    /// (e.g. a Gemini CLI grant has no Claude/GPT-OSS quota).
    ProfileUnsupported,
    /// Every lane target window's latest stored quota sample is `Absent`.
    QuotaGroupAbsent,
}

#[derive(Debug, Serialize)]
pub(crate) struct WarmupAttemptsResponse {
    attempts: Vec<WarmupAttemptRecord>,
    next_cursor: Option<String>,
}

#[derive(Debug)]
pub(crate) enum WarmupReadError {
    StorageUnavailable,
    NotFound,
    BadRequest { error: &'static str, detail: String },
    Internal { detail: String },
    Storage(StorageError),
}

impl IntoResponse for WarmupReadError {
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
            Self::Internal { detail } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "internal_error", "detail": detail })),
            )
                .into_response(),
            Self::Storage(error) => {
                tracing::error!(error = %error, "admin v1 upstream warmup storage operation failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

impl From<StorageError> for WarmupReadError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<SchedulerError> for WarmupReadError {
    fn from(error: SchedulerError) -> Self {
        Self::Internal {
            detail: error.to_string(),
        }
    }
}

pub(crate) async fn get_upstream_warmup(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<WarmupSummaryResponse>, WarmupReadError> {
    let storage = storage(&state)?;
    let upstream_id = parse_upstream_id(&id)?;
    let upstream = load_upstream(storage, upstream_id).await?;
    let lanes = WarmupLane::lanes_for_kind(upstream.kind);
    let bundle = if lanes.iter().any(|lane| *lane != WarmupLane::Primary) {
        decrypt_lane_bundle(&state, &upstream)
    } else {
        None
    };
    let latest_quota = if lanes.is_empty() {
        Vec::new()
    } else {
        storage
            .list_latest_subscription_quota_for_upstreams(&[upstream_id])
            .await?
    };

    let mut summaries = Vec::with_capacity(lanes.len());
    for &lane in lanes {
        let target_windows = lane_reported_windows(upstream.kind, lane, &latest_quota);
        let ineligible_reason =
            lane_ineligible_reason(upstream.kind, lane, bundle.as_ref(), &target_windows);
        let recent_attempts = storage
            .list_warmup_attempts_for_upstream(
                upstream_id,
                WarmupAttemptListFilters {
                    limit: Some(RECENT_ATTEMPT_LIMIT),
                    before: None,
                    status: None,
                    lane: Some(lane),
                },
            )
            .await?;
        summaries.push(WarmupLaneSummary {
            lane,
            eligible: ineligible_reason.is_none(),
            ineligible_reason,
            target_windows,
            next_scheduled_at_unix_secs: next_scheduled_at_unix_secs(&state, upstream_id, lane)
                .await?,
            // Attempts are ordered newest first, so the latest attempt is the head.
            last_attempt: recent_attempts.first().cloned(),
            recent_attempts,
        });
    }

    Ok(Json(WarmupSummaryResponse {
        upstream_id,
        dialect_plugin: upstream.warmup_dialect_plugin,
        lanes: summaries,
    }))
}

pub(crate) async fn list_upstream_warmup_attempts(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Query(query): Query<WarmupAttemptsQuery>,
) -> Result<Json<WarmupAttemptsResponse>, WarmupReadError> {
    let storage = storage(&state)?;
    let upstream_id = parse_upstream_id(&id)?;
    load_upstream(storage, upstream_id).await?;
    let lane = query
        .lane
        .as_deref()
        .map(parse_lane)
        .transpose()
        .map_err(|detail| WarmupReadError::BadRequest {
            error: "invalid_warmup_lane",
            detail,
        })?;
    let limit = normalized_limit(query.limit);
    let before = decode_before(query.before.as_deref())?;
    let attempts = storage
        .list_warmup_attempts_for_upstream(
            upstream_id,
            WarmupAttemptListFilters {
                limit: Some(limit),
                before,
                status: query.status,
                lane,
            },
        )
        .await?;
    let limit_len = usize::try_from(limit).map_err(|_| WarmupReadError::Internal {
        detail: "warmup attempt limit is outside usize".to_owned(),
    })?;
    let next_cursor = if attempts.len() == limit_len {
        attempts.last().map(encode_cursor)
    } else {
        None
    };

    Ok(Json(WarmupAttemptsResponse {
        attempts,
        next_cursor,
    }))
}

fn storage(state: &AdminState) -> Result<&dyn Storage, WarmupReadError> {
    state
        .storage
        .as_deref()
        .ok_or(WarmupReadError::StorageUnavailable)
}

fn parse_upstream_id(id: &str) -> Result<Uuid, WarmupReadError> {
    Uuid::parse_str(id).map_err(|_| WarmupReadError::NotFound)
}

async fn load_upstream(
    storage: &dyn Storage,
    upstream_id: Uuid,
) -> Result<UpstreamRecord, WarmupReadError> {
    UpstreamStore::get_by_id(storage, upstream_id)
        .await?
        .ok_or(WarmupReadError::NotFound)
}

async fn next_scheduled_at_unix_secs(
    state: &AdminState,
    upstream_id: Uuid,
    lane: WarmupLane,
) -> Result<Option<i64>, WarmupReadError> {
    match state.scheduler.as_ref() {
        Some(scheduler) => Ok(scheduler
            .next_run_for_upstream(upstream_id, lane.job_kind())
            .await?),
        None => Ok(None),
    }
}

/// Parses a `lane` query value; the error is the 400 detail.
pub(crate) fn parse_lane(value: &str) -> Result<WarmupLane, String> {
    WarmupLane::parse(value).ok_or_else(|| {
        format!("unknown warmup lane `{value}`; expected `primary` or `third_party`")
    })
}

/// Decrypted OAuth credential used only to decide lane eligibility. A missing
/// or undecryptable credential supports the primary lane alone; a decrypt
/// failure is logged so the resulting `profile_unsupported` is never silent.
pub(crate) fn decrypt_lane_bundle(
    state: &AdminState,
    upstream: &UpstreamRecord,
) -> Option<OAuthTokenBundle> {
    match upstream
        .oauth_credentials
        .as_ref()?
        .decrypt(state.aead.as_ref(), upstream.id.as_bytes())
    {
        Ok(bundle) => Some(bundle),
        Err(error) => {
            tracing::warn!(
                target: "warmup",
                upstream_id = %upstream.id,
                error = %error,
                "oauth credential decrypt failed; non-primary warmup lanes treated as profile_unsupported"
            );
            None
        }
    }
}

/// Whether the credential can serve `lane` for `kind`.
pub(crate) fn lane_supported_by_credential(
    kind: UpstreamKind,
    lane: WarmupLane,
    bundle: Option<&OAuthTokenBundle>,
) -> bool {
    match bundle {
        Some(bundle) => lane.credential_supports(kind, bundle),
        None => lane == WarmupLane::Primary && WarmupLane::lanes_for_kind(kind).contains(&lane),
    }
}

/// Lane target windows whose latest stored quota sample is not `Absent`.
/// Missing, unobserved, or stale windows still count as reported.
pub(crate) fn lane_reported_windows(
    kind: UpstreamKind,
    lane: WarmupLane,
    latest_quota: &[SubscriptionQuotaSample],
) -> Vec<SubscriptionQuotaWindow> {
    lane.target_windows(kind)
        .iter()
        .copied()
        .filter(|window| !window_reported_absent(latest_quota, *window))
        .collect()
}

fn window_reported_absent(
    latest_quota: &[SubscriptionQuotaSample],
    window: SubscriptionQuotaWindow,
) -> bool {
    latest_quota
        .iter()
        .filter(|sample| sample.window == window)
        .max_by_key(|sample| sample.observed_at_unix_millis)
        .is_some_and(|sample| sample.sample_kind == SubscriptionQuotaSampleKind::Absent)
}

/// `None` when the lane can be warmed. `reported_windows` comes from
/// [`lane_reported_windows`].
///
/// `quota_group_absent` applies only to `gemini_oauth`, whose quota groups can
/// be missing from an account; Anthropic/OpenAI primary lanes stay eligible
/// even when both windows are reported absent.
pub(crate) fn lane_ineligible_reason(
    kind: UpstreamKind,
    lane: WarmupLane,
    bundle: Option<&OAuthTokenBundle>,
    reported_windows: &[SubscriptionQuotaWindow],
) -> Option<WarmupLaneIneligibleReason> {
    if !lane_supported_by_credential(kind, lane, bundle) {
        return Some(WarmupLaneIneligibleReason::ProfileUnsupported);
    }
    if kind == UpstreamKind::GeminiOauth
        && !lane.target_windows(kind).is_empty()
        && reported_windows.is_empty()
    {
        return Some(WarmupLaneIneligibleReason::QuotaGroupAbsent);
    }
    None
}

fn normalized_limit(limit: Option<u32>) -> u32 {
    match limit {
        Some(0) | None => ATTEMPT_DEFAULT_LIMIT,
        Some(limit) => limit.min(ATTEMPT_MAX_LIMIT),
    }
}

fn decode_before(value: Option<&str>) -> Result<Option<WarmupAttemptCursor>, WarmupReadError> {
    value
        .map(WarmupAttemptCursor::decode)
        .transpose()
        .map_err(|detail| WarmupReadError::BadRequest {
            error: "invalid_warmup_cursor",
            detail,
        })
}

fn encode_cursor(attempt: &WarmupAttemptRecord) -> String {
    WarmupAttemptCursor {
        attempted_at_unix_secs: attempt.attempted_at_unix_secs,
        id: attempt.id,
    }
    .encode()
}
