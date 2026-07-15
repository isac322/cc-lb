use axum::{
    Router,
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::get,
};

use crate::AdminState;
use crate::events::{EventsError, fetch_request_event_detail};

pub fn router() -> Router<AdminState> {
    Router::new().route(
        "/admin/v1/events/detail/{event_id}",
        get(handle_event_detail),
    )
}

pub async fn handle_event_detail(
    State(state): State<AdminState>,
    Path(event_id): Path<String>,
) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return service_unavailable();
    };
    match fetch_request_event_detail(storage.as_ref(), &event_id).await {
        Ok(Some(event)) => axum::Json(event).into_response(),
        Ok(None) => not_found(),
        Err(EventsError::Storage(error)) => {
            tracing::error!(%error, "request event detail query failed");
            internal_error()
        }
        Err(_) => internal_error(),
    }
}

fn not_found() -> Response {
    (
        axum::http::StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({ "error": "not_found" })),
    )
        .into_response()
}

fn service_unavailable() -> Response {
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(serde_json::json!({ "error": "storage_unavailable" })),
    )
        .into_response()
}

fn internal_error() -> Response {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        axum::Json(serde_json::json!({ "error": "storage_error" })),
    )
        .into_response()
}
