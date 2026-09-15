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

use crate::AdminState;

use self::{
    query::parse_query,
    view::{
        CacheKeepaliveViewContext, FIVE_MINUTES_MS, detail_for_item, list_upstream_names,
        load_activity_batch, row_for_item, summary_for_items,
    },
};

pub fn router() -> Router<AdminState> {
    Router::new()
        .route(
            "/admin/v1/principals/{principal_id}/cache-keepalive",
            get(list_cache_keepalive),
        )
        .route(
            "/admin/v1/principals/{principal_id}/cache-keepalive/{session_or_decision_id}",
            get(get_cache_keepalive_detail),
        )
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
    };
    let summary_input = match CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input(
        storage,
        &principal_id,
        now_ms.saturating_sub(FIVE_MINUTES_MS),
    )
    .await
    {
        Ok(input) => input,
        Err(error) => return storage_error(error),
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
    let activity_items = summary_input
        .sessions
        .iter()
        .chain(page.as_ref().into_iter().flat_map(|page| page.rows.iter()));
    let activity_batch = match load_activity_batch(storage, &principal_id, activity_items).await {
        Ok(batch) => batch,
        Err(error) => return storage_error(error),
    };
    let mut summary = match summary_for_items(
        context.catalog,
        context.now_ms,
        &summary_input.sessions,
        &activity_batch,
    ) {
        Ok(summary) => summary,
        Err(error) => return storage_error(error),
    };
    summary.sessions_last_5m = summary
        .sessions_last_5m
        .saturating_add(summary_input.recent_decisions);
    let Some(page) = page else {
        return Json(view::CacheKeepaliveListResponse {
            summary,
            rows: Vec::new(),
            next_cursor: None,
        })
        .into_response();
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
    Path((principal_id, session_or_decision_id)): Path<(String, String)>,
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
        &session_or_decision_id,
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
    let max_attempts = principal
        .cache_keepalive
        .as_ref()
        .map_or(0, |config| config.max_refreshes_per_session);
    let context = CacheKeepaliveViewContext {
        storage,
        catalog: cc_lb_pricing::global_catalog().as_ref(),
        now_ms,
        max_attempts,
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
