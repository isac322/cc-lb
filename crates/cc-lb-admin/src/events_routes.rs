use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, Sse},
    },
    routing::get,
};
use cc_lb_control::BusReceiver;
use cc_lb_control::record_dashboard_sse_lagged;
use cc_lb_engine::ResetReason;
use cc_lb_request_log::{RequestEventPartial, RequestEventUpdate};
use cc_lb_storage_api::RequestEvent;
use serde_json::json;
use tokio::{sync::broadcast::error::RecvError, time::Duration};

use crate::AdminState;
use crate::events::{
    BACKFILL_MAX_EVENTS, EventsError, StorageTailUpdate, StreamFilters, apply_filters_to_event,
    build_delta_events_payload, build_recent_events_payload, parse_delta_query,
    parse_last_event_id, parse_recent_params, parse_stream_filters, sse_id_from_cursor,
};

const MAX_EMITTED_EVENT_IDS: usize = 10_000;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/events/recent", get(handle_recent_events))
        .route("/admin/events/delta", get(handle_events_delta))
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

pub async fn handle_events_delta(
    State(state): State<AdminState>,
    Query(map): Query<HashMap<String, String>>,
) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return service_unavailable("storage_unavailable");
    };
    let query = match parse_delta_query(&map) {
        Ok(query) => query,
        Err(error) => return bad_request(error.as_str()),
    };
    match build_delta_events_payload(storage.as_ref(), &query).await {
        Ok(payload) => Json(payload).into_response(),
        Err(EventsError::Storage(error)) => {
            tracing::error!(%error, "events delta query failed");
            internal_error("storage_error")
        }
        Err(error) => bad_request(error.as_str()),
    }
}

pub async fn handle_events_stream(
    State(state): State<AdminState>,
    Query(map): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let Some(bus) = state.event_bus.as_ref() else {
        return service_unavailable("event_bus_unavailable");
    };
    let Some(storage) = state.storage.as_ref() else {
        return service_unavailable("storage_unavailable");
    };
    let filters = match parse_stream_filters(&map) {
        Ok(filters) => filters,
        Err(error) => return bad_request(error.as_str()),
    };

    record_sse_reconnect();

    let last_event_id = parse_last_event_id(
        headers
            .get("last-event-id")
            .and_then(|value| value.to_str().ok()),
    );
    let storage = storage.clone();
    let mut bus_rx = bus.subscribe();
    let mut storage_tail_rx = state.storage_tail.subscribe();

    let stream = async_stream::stream! {
        yield Ok::<Event, Infallible>(Event::default().comment("connected"));

        let cursor_hi = match storage.current_request_event_cursor().await {
            Ok(cursor) => cursor,
            Err(error) => {
                tracing::error!(%error, "sse current cursor query failed");
                yield Ok::<Event, Infallible>(reset_event(0, ResetReason::StorageError));
                return;
            }
        };
        let start_cursor = last_event_id.unwrap_or(cursor_hi);
        let mut stream_state = StreamEmitState::new(start_cursor);
        let storage_filters = filters.storage_filters();

        match storage
            .query_request_events_between_cursors(
                start_cursor,
                cursor_hi,
                BACKFILL_MAX_EVENTS,
                &storage_filters,
            )
            .await
        {
            Ok(rows) => {
                record_sse_backfill(rows.len());
                let reached_page_cap = rows.len() >= BACKFILL_MAX_EVENTS;
                for (cursor, event) in rows {
                    if let Some(sse) = stream_state.storage_final_event(&event, cursor) {
                        yield Ok::<Event, Infallible>(sse);
                    }
                }
                if reached_page_cap {
                    yield Ok::<Event, Infallible>(reset_event(stream_state.last_finalized_cursor, ResetReason::BackfillCap));
                    return;
                }
                stream_state.advance_bookmark(cursor_hi);
                yield Ok::<Event, Infallible>(cursor_event(cursor_hi));
            }
            Err(error) => {
                tracing::error!(%error, "sse backfill cursor query failed");
                yield Ok::<Event, Infallible>(reset_event(stream_state.last_finalized_cursor, ResetReason::StorageError));
                return;
            }
        }

        let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
        let mut storage_tail_active = true;
        loop {
            tokio::select! {
                bus_result = recv_bus_update(&mut bus_rx) => {
                    match bus_result {
                        BusUpdateResult::Update(update) => {
                            if !apply_filters_to_update(&update, &filters) {
                                continue;
                            }
                            if let Some(sse) = stream_state.bus_event(&update) {
                                yield Ok::<Event, Infallible>(sse);
                            }
                        }
                        BusUpdateResult::Lagged(skipped) => {
                            record_dashboard_sse_lagged(skipped);
                            yield Ok::<Event, Infallible>(reset_event(stream_state.last_finalized_cursor, ResetReason::BusLagged));
                            return;
                        }
                        BusUpdateResult::Closed => return,
                    }
                }
                storage_result = storage_tail_rx.recv(), if storage_tail_active => {
                    match storage_result {
                        Ok(update) => {
                            if !apply_filters_to_event(&update.event, &filters) {
                                stream_state.advance_bookmark(update.cursor);
                                continue;
                            }
                            if let Some(sse) = stream_state.storage_tail_event(&update) {
                                yield Ok::<Event, Infallible>(sse);
                            }
                        }
                        Err(RecvError::Lagged(skipped)) => {
                            tracing::warn!(skipped, "sse storage tail receiver lagged");
                            yield Ok::<Event, Infallible>(reset_event(stream_state.last_finalized_cursor, ResetReason::StorageError));
                            return;
                        }
                        Err(RecvError::Closed) => {
                            storage_tail_active = false;
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    yield Ok::<Event, Infallible>(heartbeat_event(stream_state.last_finalized_cursor));
                }
            };
        }
    };

    Sse::new(stream).into_response()
}

// `Update` holds an inline `RequestEventUpdate` for the same reason the enum
// itself keeps its payload inline (see `RequestEventUpdate` in cc-lb-contract):
// this value is stack-only per recv, boxing would trade an alloc-per-message
// for no memory ceiling win.
#[allow(clippy::large_enum_variant)]
enum BusUpdateResult {
    Update(RequestEventUpdate),
    Lagged(u64),
    Closed,
}

async fn recv_bus_update(receiver: &mut BusReceiver) -> BusUpdateResult {
    match receiver {
        BusReceiver::InMemory(rx) => match rx.recv().await {
            Ok(update) => BusUpdateResult::Update(update),
            Err(RecvError::Lagged(skipped)) => BusUpdateResult::Lagged(skipped),
            Err(RecvError::Closed) => BusUpdateResult::Closed,
        },
        BusReceiver::Remote(rx) => match rx.recv().await {
            Some(update) => BusUpdateResult::Update(update),
            None => BusUpdateResult::Closed,
        },
    }
}

struct StreamEmitState {
    last_finalized_cursor: u64,
    max_emitted_cursor: u64,
    max_emitted_by_id: HashMap<String, u64>,
    emitted_order: VecDeque<String>,
}

impl StreamEmitState {
    fn new(last_finalized_cursor: u64) -> Self {
        Self {
            last_finalized_cursor,
            max_emitted_cursor: last_finalized_cursor,
            max_emitted_by_id: HashMap::new(),
            emitted_order: VecDeque::new(),
        }
    }

    fn storage_final_event(&mut self, event: &RequestEvent, cursor: u64) -> Option<Event> {
        let sse = self.final_message_event(event, cursor, cursor)?;
        self.advance_bookmark(cursor);
        Some(sse)
    }

    fn storage_tail_event(&mut self, update: &StorageTailUpdate) -> Option<Event> {
        let event_id = final_event_id(&update.event)?;
        let should_emit = self
            .max_emitted_by_id
            .get(event_id)
            .is_none_or(|emitted| *emitted < update.cursor);
        let sse = if should_emit {
            self.final_message_event(&update.event, update.cursor, update.cursor)
        } else if update.cursor > self.last_finalized_cursor {
            Some(cursor_event(update.cursor))
        } else {
            None
        };
        self.advance_bookmark(update.cursor);
        sse
    }

    fn bus_event(&mut self, update: &RequestEventUpdate) -> Option<Event> {
        match update {
            RequestEventUpdate::Final(final_update) => {
                let event_id = final_event_id(&final_update.event)?;
                if self
                    .max_emitted_by_id
                    .get(event_id)
                    .is_some_and(|emitted| *emitted >= final_update.cursor)
                {
                    return None;
                }
                self.final_message_event(
                    &final_update.event,
                    final_update.cursor,
                    self.last_finalized_cursor,
                )
            }
            RequestEventUpdate::Partial(partial) => {
                if self.partial_is_finalized(partial) {
                    return None;
                }
                self.partial_message_event(partial)
            }
        }
    }

    fn final_message_event(
        &mut self,
        event: &RequestEvent,
        payload_cursor: u64,
        sse_cursor: u64,
    ) -> Option<Event> {
        let event_id = final_event_id(event)?.to_owned();
        let update = RequestEventUpdate::final_(event.clone(), payload_cursor);
        let sse = message_event(&update, sse_cursor)?;
        self.remember_event_id(event_id, payload_cursor);
        self.max_emitted_cursor = self.max_emitted_cursor.max(sse_cursor);
        Some(sse)
    }

    fn partial_message_event(&mut self, partial: &RequestEventPartial) -> Option<Event> {
        let update = RequestEventUpdate::partial(partial.clone());
        let sse = message_event(&update, self.last_finalized_cursor)?;
        self.remember_event_id(partial.event_id.clone(), self.last_finalized_cursor);
        Some(sse)
    }

    fn partial_is_finalized(&self, partial: &RequestEventPartial) -> bool {
        self.max_emitted_by_id
            .get(&partial.event_id)
            .is_some_and(|cursor| *cursor > self.last_finalized_cursor)
    }

    fn advance_bookmark(&mut self, cursor: u64) {
        self.last_finalized_cursor = self.last_finalized_cursor.max(cursor);
        self.max_emitted_cursor = self.max_emitted_cursor.max(cursor);
    }

    fn remember_event_id(&mut self, event_id: String, cursor: u64) {
        if event_id.is_empty() {
            return;
        }
        if !self.max_emitted_by_id.contains_key(&event_id) {
            while self.max_emitted_by_id.len() >= MAX_EMITTED_EVENT_IDS {
                let Some(oldest) = self.emitted_order.pop_front() else {
                    break;
                };
                self.max_emitted_by_id.remove(&oldest);
            }
            self.emitted_order.push_back(event_id.clone());
        }
        let entry = self.max_emitted_by_id.entry(event_id).or_insert(0);
        *entry = (*entry).max(cursor);
    }
}

fn message_event(update: &RequestEventUpdate, cursor: u64) -> Option<Event> {
    let payload = match serde_json::to_string(update) {
        Ok(payload) => payload,
        Err(error) => {
            tracing::warn!(%error, "request event update serialize failed");
            record_sse_malformed_frame();
            return None;
        }
    };
    Some(
        Event::default()
            .event("message")
            .id(sse_id_from_cursor(cursor))
            .data(payload),
    )
}

fn apply_filters_to_update(update: &RequestEventUpdate, filters: &StreamFilters) -> bool {
    match update {
        RequestEventUpdate::Final(final_update) => {
            apply_filters_to_event(&final_update.event, filters)
        }
        RequestEventUpdate::Partial(partial) => apply_filters_to_partial(partial, filters),
    }
}

fn apply_filters_to_partial(partial: &RequestEventPartial, filters: &StreamFilters) -> bool {
    if let Some(principal_id) = filters.principal_id.as_deref()
        && partial.principal_id.as_deref() != Some(principal_id)
    {
        return false;
    }
    if let Some(model) = filters.model.as_deref()
        && partial.model.as_deref() != Some(model)
    {
        return false;
    }
    if let Some(upstream) = filters.upstream
        && partial.upstream != Some(upstream)
    {
        return false;
    }
    if let Some(upstream_id) = filters.upstream_id
        && partial.upstream_id != Some(upstream_id)
    {
        return false;
    }
    true
}

fn reset_event(cursor: u64, reason: ResetReason) -> Event {
    record_sse_reset(reason);
    let reason = reason.as_str();
    Event::default()
        .event("reset")
        .id(sse_id_from_cursor(cursor))
        .data(json!({ "reason": reason }).to_string())
}

fn cursor_event(cursor: u64) -> Event {
    Event::default()
        .event("cursor")
        .id(sse_id_from_cursor(cursor))
        .data("{}")
}

fn heartbeat_event(cursor: u64) -> Event {
    Event::default()
        .event("heartbeat")
        .id(sse_id_from_cursor(cursor))
        .data("{}")
}

fn final_event_id(event: &RequestEvent) -> Option<&str> {
    let request_id_fallback = if event.request_id.is_empty() {
        None
    } else {
        Some(event.request_id.as_str())
    };
    event.event_id.as_deref().or(request_id_fallback)
}

fn record_sse_reconnect() {
    metrics::counter!("sse_reconnects_total").increment(1);
}

fn record_sse_backfill(rows: usize) {
    metrics::counter!("sse_backfill_pages_total").increment(1);
    metrics::counter!("sse_backfill_rows_total").increment(rows as u64);
}

fn record_sse_reset(reason: ResetReason) {
    metrics::counter!("sse_reset_events_sent_total", "reason" => reason.as_str()).increment(1);
    if reason == ResetReason::BusLagged {
        metrics::counter!("sse_lagged_resync_total").increment(1);
    }
}

fn record_sse_malformed_frame() {
    metrics::counter!("sse_malformed_frames_total").increment(1);
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
