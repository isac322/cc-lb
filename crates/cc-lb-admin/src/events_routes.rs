use std::collections::HashMap;
use std::convert::Infallible;
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
use cc_lb_core::{BusReceiver, record_dashboard_sse_lagged};
use serde_json::json;
use tokio::sync::broadcast::error::RecvError;

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

pub async fn handle_recent_events(
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

pub async fn handle_events_stream(
    State(state): State<AdminState>,
    Query(map): Query<HashMap<String, String>>,
) -> Response {
    let Some(bus) = state.event_bus.as_ref() else {
        return service_unavailable("event_bus_unavailable");
    };
    let filters = match parse_stream_filters(&map) {
        Ok(filters) => filters,
        Err(error) => return bad_request(error.as_str()),
    };

    let receiver = bus.subscribe();
    let stream = async_stream::stream! {
        let initial: Result<Event, Infallible> = Ok(Event::default().comment("connected"));
        yield initial;

        match receiver {
            BusReceiver::InMemory(mut rx) => loop {
                match rx.recv().await {
                    Ok(update) => {
                        if !apply_filters_to_event(&update.event, &filters) {
                            continue;
                        }
                        let payload = match serde_json::to_string(&update) {
                            Ok(s) => s,
                            Err(error) => {
                                tracing::warn!(%error, "request event update serialize failed");
                                continue;
                            }
                        };
                        let item: Result<Event, Infallible> =
                            Ok(Event::default().data(payload));
                        yield item;
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        record_dashboard_sse_lagged(skipped);
                        let resync: Result<Event, Infallible> =
                            Ok(Event::default().comment(format!("lagged {skipped}")));
                        yield resync;
                    }
                    Err(RecvError::Closed) => break,
                }
            },
            BusReceiver::Remote(mut rx) => {
                while let Some(update) = rx.recv().await {
                    if !apply_filters_to_event(&update.event, &filters) {
                        continue;
                    }
                    let payload = match serde_json::to_string(&update) {
                        Ok(s) => s,
                        Err(error) => {
                            tracing::warn!(%error, "request event update serialize failed");
                            continue;
                        }
                    };
                    let item: Result<Event, Infallible> = Ok(Event::default().data(payload));
                    yield item;
                }
            }
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
