use axum::{
    Extension, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde_json::json;

use crate::{
    AdminState,
    audit::{AdminAuditEvent, record_admin_audit},
    auth::{AdminAction, AdminIdentity, authorize},
    events::{EventsError, fetch_request_event_detail},
};

pub fn router() -> Router<AdminState> {
    Router::new().route("/admin/v1/events/detail", get(handle_event_detail))
}

#[derive(Debug, Deserialize)]
struct EventDetailQuery {
    id: String,
}

async fn handle_event_detail(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Query(query): Query<EventDetailQuery>,
) -> Response {
    if authorize(&identity, AdminAction::SensitiveRead).is_err() {
        return forbidden();
    }

    let Some(storage) = state.storage.as_ref() else {
        return service_unavailable();
    };
    match fetch_request_event_detail(storage.as_ref(), &query.id).await {
        Ok(Some(event)) => {
            if let Err(error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action: "request_event_detail_read",
                    route: "/admin/v1/events/detail",
                    target_principal_id: None,
                    target_upstream: None,
                    api_key_id: None,
                    status: 200,
                    payload: Some(json!({ "event_id": query.id })),
                },
            )
            .await
            {
                tracing::error!(%error, action = "request_event_detail_read", "admin audit write failed");
                return audit_write_failed();
            }
            axum::Json(event).into_response()
        }
        Ok(None) => not_found(),
        Err(EventsError::Storage(error)) => {
            tracing::error!(%error, "request event detail query failed");
            internal_error()
        }
        Err(_) => internal_error(),
    }
}

fn forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        axum::Json(json!({ "error": "forbidden" })),
    )
        .into_response()
}

fn audit_write_failed() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        axum::Json(json!({ "error": "audit_write_failed" })),
    )
        .into_response()
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
