mod query;
pub(crate) mod view;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use cc_lb_storage_api::{CacheKeepaliveSessionReadStore, PrincipalStore, Storage, StorageError};
use serde::Deserialize;

use self::{
    query::{parse_horizon, parse_query},
    view::{
        CacheKeepaliveViewContext, FIVE_MINUTES_MS, detail_for_item, list_upstream_names,
        load_activity_batch, row_for_item,
    },
};
use crate::AdminState;
use crate::cache_keepalive_view::economics::{rates_from_catalog, renewal_avoided_micros};

pub fn router() -> Router<AdminState> {
    Router::new()
        .route(
            "/admin/v1/principals/{principal_id}/cache-keepalive",
            get(list_cache_keepalive),
        )
        .route(
            "/admin/v1/principals/{principal_id}/cache-keepalive/detail",
            get(get_cache_keepalive_detail),
        )
}

#[derive(Debug, Deserialize)]
struct CacheKeepaliveDetailQuery {
    id: String,
    horizon: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawCacheKeepaliveQuery {
    limit: Option<String>,
    cursor: Option<String>,
    horizon: Option<String>,
    status: Option<String>,
    error: Option<String>,
}

async fn list_cache_keepalive(
    State(state): State<AdminState>,
    Path(principal_id): Path<String>,
    Query(raw_query): Query<RawCacheKeepaliveQuery>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let principal = match active_principal(storage, &principal_id).await {
        Ok(principal) => principal,
        Err(error) => return error.into_response(),
    };
    let now_ms = cc_lb_clock::unix_secs(state.clock.now()).saturating_mul(1_000);
    let query = match parse_query(raw_query, &principal_id, now_ms) {
        Ok(query) => query,
        Err(error) => return error.into_response(),
    };
    let max_attempts = principal
        .cache_keepalive
        .as_ref()
        .map_or(0, |config| config.max_refreshes_per_session);
    let context = CacheKeepaliveViewContext {
        storage,
        catalog: cc_lb_pricing::global_catalog().as_ref(),
        now_ms,
        max_attempts,
        horizon_start_ms: query.storage.horizon_start_ms,
    };
    let page = if query.limit == 0 {
        None
    } else {
        match CacheKeepaliveSessionReadStore::list_cache_keepalive_sessions(storage, &query.storage)
            .await
        {
            Ok(page) => Some(page),
            Err(error) => return storage_error(error),
        }
    };
    let aggregate =
        match CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_aggregate_for_horizon(
            storage,
            &principal_id,
            now_ms.saturating_sub(FIVE_MINUTES_MS),
            query.storage.horizon_start_ms,
        )
        .await
        {
            Ok(aggregate) => aggregate,
            Err(error) => return storage_error(error),
        };
    let mut avoided_micros = 0_i64;
    let mut pricing_complete = aggregate.unknown_costs == 0;
    for group in &aggregate.groups {
        let Some(rates) = rates_from_catalog(
            cc_lb_pricing::global_catalog().as_ref(),
            &group.model,
            group.ttl,
        ) else {
            pricing_complete = false;
            continue;
        };
        // Every renewal in a group read the same token count; price one renewal
        // and multiply so the header rounds exactly like the per-row sum.
        let per_renewal_tokens = group
            .avoided_cache_read_tokens
            .checked_div(group.renewals)
            .unwrap_or(0);
        let group_avoided = renewal_avoided_micros(per_renewal_tokens, rates)
            .saturating_mul(i64::try_from(group.renewals).unwrap_or(i64::MAX));
        avoided_micros = avoided_micros.saturating_add(group_avoided);
    }
    let total_spent = aggregate.total_spent_micros as f64 / 1_000_000.0;
    let total_avoided = pricing_complete.then(|| avoided_micros as f64 / 1_000_000.0);
    let cost_saved = pricing_complete.then(|| {
        (avoided_micros.saturating_sub(aggregate.total_spent_micros)) as f64 / 1_000_000.0
    });
    let summary = view::CacheKeepaliveSummaryResponse {
        renewing_now: aggregate.renewing_now,
        sessions_last_5m: aggregate.sessions_last_5m,
        retained_successful_hits: aggregate.successful_hits,
        cost_saved,
        total_spent,
        total_avoided,
        unlisted_sessions: aggregate.unlisted_sessions,
        unlisted_renewals: aggregate.unlisted_renewals,
        unlisted_spent: aggregate.unlisted_spent_micros as f64 / 1_000_000.0,
        evidence_status: aggregate.evidence_status,
        unknown_cycles: aggregate.unknown_cycles,
        retained_from_ms: aggregate.retained_from_ms,
    };
    let Some(page) = page else {
        return Json(view::CacheKeepaliveListResponse {
            summary,
            rows: Vec::new(),
            next_cursor: None,
        })
        .into_response();
    };
    let activity_batch = match load_activity_batch(storage, &principal_id, page.rows.iter()).await {
        Ok(batch) => batch,
        Err(error) => return storage_error(error),
    };
    let upstream_names = match list_upstream_names(storage, &page.rows).await {
        Ok(upstream_names) => upstream_names,
        Err(error) => return storage_error(error),
    };
    let mut rows = Vec::with_capacity(page.rows.len());
    for item in &page.rows {
        match row_for_item(&context, item, &activity_batch, &upstream_names) {
            Ok(row) => rows.push(row),
            Err(error) => return storage_error(error),
        }
    }
    let next_cursor = match page.next_cursor {
        Some(cursor) => match query::encode_cursor(&cursor, query.horizon) {
            Ok(cursor) => Some(cursor),
            Err(error) => return error.into_response(),
        },
        None => None,
    };
    Json(view::CacheKeepaliveListResponse {
        summary,
        rows,
        next_cursor,
    })
    .into_response()
}

async fn get_cache_keepalive_detail(
    State(state): State<AdminState>,
    Path(principal_id): Path<String>,
    Query(query): Query<CacheKeepaliveDetailQuery>,
) -> axum::response::Response {
    let Some(storage) = state.storage.as_deref() else {
        return storage_unavailable();
    };
    let principal = match active_principal(storage, &principal_id).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let item = match CacheKeepaliveSessionReadStore::get_cache_keepalive_list_item(
        storage,
        &principal_id,
        &query.id,
    )
    .await
    {
        Ok(Some(item)) => item,
        Ok(None) => {
            return error_response(StatusCode::NOT_FOUND, "unknown_cache_keepalive_entry");
        }
        Err(error) => return storage_error(error),
    };
    let now_ms = cc_lb_clock::unix_secs(state.clock.now()).saturating_mul(1_000);
    let horizon = match query.horizon.as_deref() {
        Some(value) => match parse_horizon(Some(value)) {
            Ok(horizon) => horizon,
            Err(error) => return error.into_response(),
        },
        None => query::CacheKeepaliveHorizon::All,
    };
    let max_attempts = principal
        .cache_keepalive
        .as_ref()
        .map_or(0, |config| config.max_refreshes_per_session);
    let context = CacheKeepaliveViewContext {
        storage,
        catalog: cc_lb_pricing::global_catalog().as_ref(),
        now_ms,
        max_attempts,
        horizon_start_ms: horizon.start_ms(now_ms),
    };
    match detail_for_item(&context, &item).await {
        Ok(detail) => Json(detail).into_response(),
        Err(error) => storage_error(error),
    }
}

#[allow(clippy::result_large_err)]
async fn active_principal(
    storage: &dyn Storage,
    principal_id: &str,
) -> Result<cc_lb_storage_api::PrincipalRecord, axum::response::Response> {
    let id = principal_id
        .parse()
        .map_err(|_| error_response(StatusCode::BAD_REQUEST, "invalid_principal_id"))?;
    match PrincipalStore::get_by_id(storage, id).await {
        Ok(Some(record)) if record.deleted_at_unix_secs.is_none() => Ok(record),
        Ok(_) => Err(error_response(StatusCode::NOT_FOUND, "unknown_principal")),
        Err(error) => Err(storage_error(error)),
    }
}

fn storage_error(storage_error: StorageError) -> axum::response::Response {
    match storage_error {
        StorageError::InvalidInput { field, reason } => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid_input", "field": field, "reason": reason })),
        )
            .into_response(),
        StorageError::Conflict { message } => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "storage_conflict", "message": message })),
        )
            .into_response(),
        source => {
            tracing::error!(error = %source, "admin cache keepalive read failed");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "storage_error")
        }
    }
}

fn storage_unavailable() -> axum::response::Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, "1")],
        Json(serde_json::json!({ "error": "storage_unavailable" })),
    )
        .into_response()
}

fn error_response(status: StatusCode, code: &str) -> axum::response::Response {
    (status, Json(serde_json::json!({ "error": code }))).into_response()
}
