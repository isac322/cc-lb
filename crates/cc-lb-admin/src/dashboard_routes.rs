use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::AdminState;
use crate::dashboard::{
    DashboardBuildError, auto_step, build_dashboard_summary, build_dashboard_usage_checked,
    parse_group_by, parse_range, parse_step,
};

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/dashboard/summary", get(handle_dashboard_summary))
        .route("/admin/dashboard/usage", get(handle_dashboard_usage))
        .route("/admin/usage", get(handle_dashboard_usage))
}

#[derive(Debug, Deserialize)]
pub(crate) struct SummaryQuery {
    range: String,
}

pub(crate) async fn handle_dashboard_summary(
    State(state): State<AdminState>,
    Query(query): Query<SummaryQuery>,
) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return service_unavailable("storage_unavailable");
    };
    let range = match parse_range(&query.range) {
        Ok(range) => range,
        Err(_) => return bad_request("invalid_range"),
    };
    match build_dashboard_summary(
        storage.as_ref(),
        range,
        cc_lb_core::clock::unix_secs(state.clock.now()),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => {
            tracing::error!(%error, "dashboard summary failed");
            internal_error("storage_error")
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct UsageQuery {
    range: String,
    #[serde(default)]
    group_by: Option<String>,
    #[serde(default)]
    step: Option<String>,
    #[serde(default)]
    upstream_id: Option<String>,
}

pub(crate) async fn handle_dashboard_usage(
    State(state): State<AdminState>,
    Query(query): Query<UsageQuery>,
) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return service_unavailable("storage_unavailable");
    };
    let range = match parse_range(&query.range) {
        Ok(range) => range,
        Err(_) => return bad_request("invalid_range"),
    };
    let step = match query.step.as_deref() {
        Some(value) => match parse_step(value) {
            Ok(step) => step,
            Err(error) => return bad_request(error.as_str()),
        },
        None => auto_step(range),
    };
    let group_by = match query.group_by.as_deref() {
        Some(value) => match parse_group_by(value) {
            Ok(group_by) => group_by,
            Err(error) => return bad_request(error.as_str()),
        },
        None => crate::dashboard::UsageGroupBy::None,
    };
    let upstream_id = match query.upstream_id.as_deref() {
        Some(value) => match Uuid::parse_str(value) {
            Ok(upstream_id) => Some(upstream_id),
            Err(_) => return bad_request("invalid_upstream_id"),
        },
        None => None,
    };
    match build_dashboard_usage_checked(
        storage.as_ref(),
        range,
        step,
        group_by,
        upstream_id,
        cc_lb_core::clock::unix_secs(state.clock.now()),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(DashboardBuildError::Query(error)) => bad_request(error.as_str()),
        Err(DashboardBuildError::Storage(error)) => {
            tracing::error!(%error, "dashboard usage failed");
            internal_error("storage_error")
        }
    }
}

fn bad_request(error: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": error }))).into_response()
}

fn service_unavailable(error: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "error": error })),
    )
        .into_response()
}

fn internal_error(error: &str) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": error })),
    )
        .into_response()
}
