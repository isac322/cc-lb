use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

use crate::ports::RetainedPartialPort;

const CLUSTER_TOKEN_HEADER: &str = "x-cluster-token";

#[derive(Clone)]
pub struct InternalPartialsState {
    pub retention: std::sync::Arc<dyn RetainedPartialPort>,
    pub cluster_token: String,
}

pub fn router(state: InternalPartialsState) -> Router {
    Router::new()
        .route(
            "/internal/v1/partials/{event_id}",
            get(handle_internal_partial_fetch),
        )
        .with_state(state)
}

pub async fn handle_internal_partial_fetch(
    Path(event_id): Path<String>,
    State(state): State<InternalPartialsState>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&headers, &state.cluster_token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match state.retention.retained_partial(&event_id) {
        Some(snapshot) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(snapshot.payload))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn authorized(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get(CLUSTER_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|actual| actual == expected)
}
