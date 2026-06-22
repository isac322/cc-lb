use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use cc_lb_scheduler::admin::{SchedulerFailure, SchedulerRecurringJobStatus};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::AdminState;

const DEFAULT_FAILURE_LIMIT: u32 = 100;
const MAX_FAILURE_LIMIT: u32 = 500;
const SCHEDULE_VERSION: &str = match option_env!("CC_LB_GIT_SHA") {
    Some(value) => value,
    None => "unknown",
};

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/scheduler/status", get(status))
        .route("/admin/scheduler/failures", get(failures))
}

#[derive(Debug, Serialize)]
struct SchedulerStatusResponse {
    schedule_version: &'static str,
    leader_status: &'static str,
    recurring_jobs: Vec<SchedulerRecurringJobStatus>,
    pool_in_use: u32,
    pool_idle: u32,
}

#[derive(Debug, Deserialize)]
struct SchedulerFailuresQuery {
    limit: Option<u32>,
    offset: Option<u32>,
    job_type: Option<String>,
}

#[derive(Debug, Serialize)]
struct SchedulerFailuresResponse {
    failures: Vec<SchedulerFailure>,
    limit: u32,
    offset: u32,
}

async fn status(State(state): State<AdminState>) -> Response {
    let Some(scheduler) = state.scheduler.as_ref() else {
        return scheduler_unavailable();
    };
    let config = state.config.current_config();
    match scheduler.status(&config.scheduler).await {
        Ok(snapshot) => Json(SchedulerStatusResponse {
            schedule_version: SCHEDULE_VERSION,
            leader_status: snapshot.leader_status,
            recurring_jobs: snapshot.recurring_jobs,
            pool_in_use: snapshot.pool_in_use,
            pool_idle: snapshot.pool_idle,
        })
        .into_response(),
        Err(error) => scheduler_error(error),
    }
}

async fn failures(
    State(state): State<AdminState>,
    Query(query): Query<SchedulerFailuresQuery>,
) -> Response {
    let Some(scheduler) = state.scheduler.as_ref() else {
        return scheduler_unavailable();
    };
    let limit = query
        .limit
        .unwrap_or(DEFAULT_FAILURE_LIMIT)
        .min(MAX_FAILURE_LIMIT);
    let offset = query.offset.unwrap_or(0);
    match scheduler
        .list_failures(query.job_type.as_deref(), limit, offset)
        .await
    {
        Ok(failures) => Json(SchedulerFailuresResponse {
            failures,
            limit,
            offset,
        })
        .into_response(),
        Err(error) => scheduler_error(error),
    }
}

fn scheduler_unavailable() -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({ "error": "scheduler_unavailable" })),
    )
        .into_response()
}

fn scheduler_error(error: cc_lb_scheduler::error::SchedulerError) -> Response {
    tracing::error!(%error, "scheduler admin operation failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "scheduler_error" })),
    )
        .into_response()
}
