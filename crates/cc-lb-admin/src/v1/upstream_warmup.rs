use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_storage_api::upstream::{UpstreamRecord, UpstreamStore, UpstreamWarmupDialectPlugin};
use cc_lb_storage_api::warmup_attempts::{
    WarmupAttemptCursor, WarmupAttemptListFilters, WarmupAttemptRecord, WarmupAttemptStatus,
    WarmupAttemptSummary,
};
use cc_lb_storage_api::{Storage, StorageError};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::AdminState;

const RECENT_ATTEMPT_LIMIT: u32 = 10;
const ATTEMPT_DEFAULT_LIMIT: u32 = 50;
const ATTEMPT_MAX_LIMIT: u32 = 200;
const RECENT_SUMMARY_WINDOW_SECS: i64 = 7 * 86_400;
const WARMUP_JOB_KIND: &str = "warmup";

#[derive(Debug, Deserialize)]
pub(crate) struct WarmupAttemptsQuery {
    limit: Option<u32>,
    before: Option<String>,
    status: Option<WarmupAttemptStatus>,
}

#[derive(Debug, Serialize)]
pub(crate) struct WarmupSummaryResponse {
    upstream_id: Uuid,
    last_attempt: Option<WarmupAttemptRecord>,
    recent_attempts: Vec<WarmupAttemptRecord>,
    next_scheduled_at_unix_secs: Option<i64>,
    recent_summary_7d: WarmupAttemptSummary,
    dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
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
    let last_attempt = storage
        .latest_warmup_attempt_for_upstream(upstream_id)
        .await?;
    let recent_attempts = storage
        .list_warmup_attempts_for_upstream(
            upstream_id,
            WarmupAttemptListFilters {
                limit: Some(RECENT_ATTEMPT_LIMIT),
                before: None,
                status: None,
            },
        )
        .await?;
    let next_scheduled_at_unix_secs = next_scheduled_at_unix_secs(&state, upstream_id).await?;
    let now_unix_secs = i64::try_from(cc_lb_clock::unix_secs(state.clock.now())).map_err(|_| {
        WarmupReadError::Internal {
            detail: "clock now overflowed i64 unix seconds".to_owned(),
        }
    })?;
    let cutoff_unix_secs = now_unix_secs
        .checked_sub(RECENT_SUMMARY_WINDOW_SECS)
        .ok_or_else(|| WarmupReadError::Internal {
            detail: "recent summary cutoff overflow".to_owned(),
        })?;
    let recent_summary_7d = storage
        .summarize_recent_warmup_attempts(upstream_id, cutoff_unix_secs)
        .await?;

    Ok(Json(WarmupSummaryResponse {
        upstream_id,
        last_attempt,
        recent_attempts,
        next_scheduled_at_unix_secs,
        recent_summary_7d,
        dialect_plugin: upstream.warmup_dialect_plugin,
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
    let limit = normalized_limit(query.limit);
    let before = decode_before(query.before.as_deref())?;
    let attempts = storage
        .list_warmup_attempts_for_upstream(
            upstream_id,
            WarmupAttemptListFilters {
                limit: Some(limit),
                before,
                status: query.status,
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
) -> Result<Option<i64>, WarmupReadError> {
    match state.scheduler.as_ref() {
        Some(scheduler) => Ok(scheduler
            .next_run_for_upstream(upstream_id, WARMUP_JOB_KIND)
            .await?),
        None => Ok(None),
    }
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
