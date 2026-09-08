use std::sync::{Arc, LazyLock};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::HeaderMap,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::AdminState;
use crate::dashboard::{
    DashboardBuildError, DashboardUsageResponse, UsageProjection, auto_step,
    build_dashboard_summary, build_dashboard_usage_projected_checked, build_window_for_step,
    parse_group_by, parse_range, parse_step, parse_usage_projection, validate_step_for_range,
};
use crate::response_cache::{
    PRINCIPAL_TOTALS_CACHE_TTL, ShortTtlSingleFlightCache, apply_private_revalidation,
    matches_if_none_match, not_modified_response,
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
    headers: HeaderMap,
    Query(query): Query<SummaryQuery>,
) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return service_unavailable("storage_unavailable");
    };
    let range = match parse_range(&query.range) {
        Ok(range) => range,
        Err(_) => return bad_request("invalid_range"),
    };
    let now_unix_secs = cc_lb_clock::unix_secs(state.clock.now());
    let (_, window_end_unix_secs) = build_window_for_step(range, auto_step(range), now_unix_secs);
    let etag = dashboard_checkpoint(storage.as_ref())
        .await
        .map(|checkpoint| {
            format!(
                "W/\"dashboard:summary:{}:{checkpoint}:{window_end_unix_secs}\"",
                range.as_str()
            )
        });
    if let Some(etag) = &etag
        && matches_if_none_match(&headers, etag)
    {
        return not_modified_response(etag);
    }
    match build_dashboard_summary(storage.as_ref(), range, now_unix_secs).await {
        Ok(response) => apply_private_revalidation(Json(response).into_response(), etag.as_deref()),
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
    #[serde(default)]
    projection: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct PrincipalTotalsCacheKey {
    range: &'static str,
    step: &'static str,
    group_by: &'static str,
    upstream_id: Option<Uuid>,
    projection: &'static str,
    window_start_unix_secs: u64,
    window_end_unix_secs: u64,
}

type PrincipalTotalsCache = ShortTtlSingleFlightCache<
    PrincipalTotalsCacheKey,
    DashboardUsageResponse,
    DashboardBuildError,
    dyn cc_lb_storage_api::Storage,
>;

static PRINCIPAL_TOTALS_CACHE: LazyLock<PrincipalTotalsCache> =
    LazyLock::new(|| ShortTtlSingleFlightCache::new(PRINCIPAL_TOTALS_CACHE_TTL));

pub(crate) async fn handle_dashboard_usage(
    State(state): State<AdminState>,
    headers: HeaderMap,
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
    if let Err(error) = validate_step_for_range(range, step) {
        return bad_request(error.as_str());
    }
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
    let projection = match parse_usage_projection(query.projection.as_deref()) {
        Ok(projection) => projection,
        Err(_) => return bad_request("invalid_projection"),
    };
    let now_unix_secs = cc_lb_clock::unix_secs(state.clock.now());
    let (window_start_unix_secs, window_end_unix_secs) =
        build_window_for_step(range, step, now_unix_secs);
    let projection_name = match projection {
        UsageProjection::Full => "full",
        UsageProjection::Totals => "totals",
    };
    if group_by == crate::dashboard::UsageGroupBy::Principal
        && projection == UsageProjection::Totals
    {
        let cache_key = PrincipalTotalsCacheKey {
            range: range.as_str(),
            step: step.as_str(),
            group_by: group_by.as_str(),
            upstream_id,
            projection: projection_name,
            window_start_unix_secs,
            window_end_unix_secs,
        };
        let storage_for_build = Arc::clone(storage);
        let result = PRINCIPAL_TOTALS_CACHE
            .get_or_build(storage, cache_key, async move {
                build_dashboard_usage_projected_checked(
                    storage_for_build.as_ref(),
                    range,
                    step,
                    group_by,
                    upstream_id,
                    now_unix_secs,
                    projection,
                )
                .await
            })
            .await;
        return match result {
            Ok(response) => {
                apply_private_revalidation(Json(response.as_ref()).into_response(), None)
            }
            Err(error) => dashboard_usage_error(error.as_ref()),
        };
    }
    let etag = if group_by == crate::dashboard::UsageGroupBy::Principal {
        None
    } else {
        dashboard_checkpoint(storage.as_ref())
            .await
            .map(|checkpoint| {
                format!(
                    "W/\"dashboard:usage:{}:{}:{}:{}:{projection_name}:{checkpoint}:{window_end_unix_secs}\"",
                    range.as_str(),
                    step.as_str(),
                    group_by.as_str(),
                    upstream_id
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "all".to_owned()),
                )
            })
    };
    if let Some(etag) = &etag
        && matches_if_none_match(&headers, etag)
    {
        return not_modified_response(etag);
    }
    match build_dashboard_usage_projected_checked(
        storage.as_ref(),
        range,
        step,
        group_by,
        upstream_id,
        now_unix_secs,
        projection,
    )
    .await
    {
        Ok(response) => apply_private_revalidation(Json(response).into_response(), etag.as_deref()),
        Err(error) => dashboard_usage_error(&error),
    }
}

fn dashboard_usage_error(error: &DashboardBuildError) -> Response {
    match error {
        DashboardBuildError::Query(error) => bad_request((*error).as_str()),
        DashboardBuildError::Storage(error) => {
            tracing::error!(%error, "dashboard usage failed");
            internal_error("storage_error")
        }
    }
}

async fn dashboard_checkpoint(storage: &dyn cc_lb_storage_api::Storage) -> Option<u64> {
    match storage.usage_rollup_checkpoint().await {
        Ok(checkpoint) => checkpoint,
        Err(error) => {
            tracing::warn!(%error, "dashboard ETag checkpoint read failed");
            None
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
