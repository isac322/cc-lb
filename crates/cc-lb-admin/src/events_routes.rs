use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use cc_lb_storage_api::Storage;
use serde_json::json;
use tokio::time::sleep;

use crate::AdminState;
use crate::events::{
    EventsError, apply_filters_to_event, build_recent_events_payload, parse_recent_params,
    parse_stream_filters,
};

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/events/recent", get(handle_recent_events))
        .route("/admin/events/stream", get(handle_events_stream))
}

async fn handle_recent_events(
    State(state): State<AdminState>,
    Query(map): Query<HashMap<String, String>>,
) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return service_unavailable("storage_unavailable");
    };
    let params = match parse_recent_params(&map) {
        Ok(params) => params,
        Err(EventsError::Storage(error)) => {
            tracing::error!(%error, "parse recent params storage error");
            return internal_error("storage_error");
        }
        Err(error) => return bad_request(error.as_str()),
    };
    match build_recent_events_payload(storage.as_ref(), &params).await {
        Ok(payload) => Json(payload).into_response(),
        Err(EventsError::Storage(error)) => {
            tracing::error!(%error, "recent events query failed");
            internal_error("storage_error")
        }
        Err(error) => bad_request(error.as_str()),
    }
}

async fn handle_events_stream(
    State(state): State<AdminState>,
    Query(map): Query<HashMap<String, String>>,
) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return service_unavailable("storage_unavailable");
    };
    let filters = match parse_stream_filters(&map) {
        Ok(filters) => filters,
        Err(error) => return bad_request(error.as_str()),
    };

    let storage: Arc<dyn Storage> = storage.clone();
    let initial_since = now_unix_secs();

    let stream = async_stream::stream! {
        // First body byte unblocks the client; without it the UI stays
        // "disconnected" until the 15s keep-alive (or first event).
        let initial: Result<Event, std::convert::Infallible> =
            Ok(Event::default().comment("connected"));
        yield initial;

        let mut since = initial_since;
        loop {
            sleep(Duration::from_millis(1_000)).await;
            let until = now_unix_secs().saturating_add(1);
            let events = match storage
                .query_request_events(since, until, 500)
                .await
            {
                Ok(events) => events,
                Err(error) => {
                    tracing::warn!(%error, "event stream poll failed");
                    continue;
                }
            };
            let mut emitted: Vec<_> = events
                .into_iter()
                .filter(|event| event.ts >= since)
                .filter(|event| apply_filters_to_event(event, &filters))
                .collect();
            emitted.sort_by(|left, right| {
                left.ts
                    .cmp(&right.ts)
                    .then_with(|| left.request_id.cmp(&right.request_id))
            });
            let mut max_ts = since;
            for event in &emitted {
                if event.ts > max_ts {
                    max_ts = event.ts;
                }
                let payload = match serde_json::to_string(event) {
                    Ok(payload) => payload,
                    Err(error) => {
                        tracing::warn!(%error, "event stream serialize failed");
                        continue;
                    }
                };
                let item: Result<Event, std::convert::Infallible> =
                    Ok(Event::default().data(payload));
                yield item;
            }
            since = max_ts.saturating_add(1).max(since);
        }
    };

    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
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

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
