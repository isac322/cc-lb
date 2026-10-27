//! Responses WebSocket admission and per-response ownership.
//!
//! Transport I/O lives in the server. This single-owner state machine keeps
//! connection-local continuation separate from durable response ownership.
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_control::api_keys::builtin_authn::BuiltinAuthError;
use cc_lb_control::api_keys::limit_engine::{LimitEngine, Reservation};
use cc_lb_lifecycle::{LifecycleEvent, UsageSnapshot, UsageSource};
use http::{HeaderMap, StatusCode};
use serde_json::Value;
use sonic_rs::JsonValueTrait as _;
use tracing::Span;
use url::Url;
use uuid::Uuid;

use crate::authn_rail::{Authenticated, reject_unauthenticated};
use crate::lifecycle::Lifecycle;
use crate::terminal_observer::{InternalFailure, LifecycleContext, UpstreamErrorCode};
use crate::usage_parser::{UsageCounts, observe_non_stream_json_value};

#[derive(Clone, Debug)]
pub struct WebSocketError {
    pub status: u16,
    pub code: &'static str,
    pub native_code: Option<String>,
    pub message: String,
    pub stream_id: Option<String>,
}

impl WebSocketError {
    pub fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            native_code: None,
            message: message.into(),
            stream_id: None,
        }
    }

    pub fn with_native_code(mut self, code: impl Into<String>) -> Self {
        self.native_code = Some(code.into());
        self
    }

    pub fn with_stream(mut self, stream_id: Option<String>) -> Self {
        self.stream_id = stream_id;
        self
    }

    pub fn to_bytes(&self) -> Bytes {
        let code = self.native_code.as_deref().unwrap_or(self.code);
        let mut error = serde_json::json!({
            "type": "error",
            "status": self.status,
            "error": {
                "type": if self.code == "authentication_error" { "authentication_error" }
                    else if self.status == 429 { "rate_limit_error" }
                    else if self.status < 500 { "invalid_request_error" }
                    else { "server_error" },
                "code": code,
                "message": self.message,
                "param": null,
            },
        });
        if let Some(stream_id) = self.stream_id.as_ref() {
            error["stream_id"] = Value::String(stream_id.clone());
        }
        Bytes::from(serde_json::to_vec(&error).expect("WebSocket error contains only JSON values"))
    }
}

impl fmt::Display for WebSocketError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for WebSocketError {}

#[derive(Clone, Debug)]
pub struct WebSocketCandidate {
    pub upstream_id: Uuid,
    pub upstream_name: String,
}

/// Borrowed transport inputs shared by one parsed logical turn.
#[derive(Clone, Copy)]
pub(crate) struct WebSocketIngress<'a> {
    pub headers: &'a HeaderMap,
    pub frame: &'a Bytes,
    pub query: Option<&'a str>,
}

pub(crate) struct PreparedWebSocketTurn {
    pub candidates: Vec<WebSocketCandidate>,
    pub observer: Option<LifecycleContext>,
    pub reservation: Option<Reservation>,
    pub limit_engine: Option<Arc<LimitEngine>>,
    pub principal_id: String,
    pub key_id: String,
    pub model: String,
    pub store: bool,
    pub span: Span,
    pub route_info: cc_lb_lifecycle::RouteInfo,
}

#[derive(Clone, Debug)]
pub struct WebSocketDispatch {
    pub turn_id: u64,
    pub frame: Bytes,
    pub connection_id: Option<u64>,
    pub candidates: Vec<WebSocketCandidate>,
    pub stream_id: Option<String>,
}

#[derive(Default)]
pub struct WebSocketEventResult {
    pub forward: bool,
    pub dispatches: Vec<WebSocketDispatch>,
    pub errors: Vec<WebSocketError>,
}

#[derive(Clone, Debug)]
struct ResponseOwner {
    connection_id: u64,
    upstream_id: Uuid,
    turn_id: u64,
    principal_id: String,
    body: Arc<Value>,
    lane: Option<String>,
}

#[derive(Default)]
struct Lane {
    active: Option<u64>,
    queued: VecDeque<u64>,
}

struct Turn {
    auth: Authenticated,
    prepared: PreparedWebSocketTurn,
    body: Arc<Value>,
    frame: Bytes,
    lane: Option<String>,
    connection_id: Option<u64>,
    upstream_id: Option<Uuid>,
    pinned: Option<(u64, Uuid)>,
    response_id: Option<String>,
    previous_response_id: Option<String>,
    steer_parent: Option<String>,
    steer_submissions: usize,
    accepted_steers: HashSet<String>,
    controls: VecDeque<Bytes>,
    sent: bool,
    attempted: bool,
    charged_bytes: usize,
    usage: Option<UsageCounts>,
    usage_complete: bool,
}

/// A session owns no authenticated proof until a fresh client check succeeds.
/// In-flight turns retain their original proof; a later revocation affects new
/// dispatches, not already admitted provider output.
pub struct ResponsesWebSocketGateway {
    lifecycle: Arc<Lifecycle>,
    headers: HeaderMap,
    query: Option<String>,
    byte_cap: usize,
    slot_cap: usize,
    used_bytes: usize,
    control_slots: usize,
    next_turn_id: u64,
    lanes: HashMap<Option<String>, Lane>,
    turns: HashMap<u64, Turn>,
    owners: HashMap<String, ResponseOwner>,
    recent: HashMap<(u64, Option<String>), String>,
    connections: HashMap<u64, Uuid>,
    successors: HashMap<String, u64>,
}

impl ResponsesWebSocketGateway {
    pub fn new(
        lifecycle: Arc<Lifecycle>,
        _authenticated: &Authenticated,
        original_headers: HeaderMap,
        budget_bytes: usize,
        slots: usize,
    ) -> Self {
        Self {
            lifecycle,
            headers: original_headers,
            query: None,
            byte_cap: budget_bytes,
            slot_cap: slots,
            used_bytes: 0,
            control_slots: 0,
            next_turn_id: 0,
            lanes: HashMap::new(),
            turns: HashMap::new(),
            owners: HashMap::new(),
            recent: HashMap::new(),
            connections: HashMap::new(),
            successors: HashMap::new(),
        }
    }

    /// Preserve the downstream upgrade query after removing API-key parameters.
    pub fn with_downstream_query(mut self, query: Option<String>) -> Self {
        self.query = query.and_then(|query| match cc_lb_upstream::scrub_query_api_key(&query) {
            std::borrow::Cow::Borrowed(_) => Some(query),
            std::borrow::Cow::Owned(clean) if clean.is_empty() => None,
            std::borrow::Cow::Owned(clean) => Some(clean),
        });
        self
    }

    fn successor_for_parent(&self, parent: &str) -> Option<u64> {
        self.successors.get(parent).copied().or_else(|| {
            self.turns.iter().find_map(|(id, turn)| {
                (turn.steer_parent.as_deref() == Some(parent)).then_some(*id)
            })
        })
    }
    pub async fn authenticate(&self) -> Result<Authenticated, BuiltinAuthError> {
        self.lifecycle.authenticate(&self.headers).await
    }

    pub fn reject_authentication(&self, error: &BuiltinAuthError) -> WebSocketError {
        let (observer, _) = self.lifecycle.websocket_observer();
        if let Some(observer) = observer.as_ref() {
            observer.mark_authn_reached();
        }
        let _ = reject_unauthenticated(error, observer.as_ref());
        WebSocketError::new(
            error.http_status(),
            "authentication_error",
            error.to_string(),
        )
    }

    pub fn reject_protocol(&self, error: &WebSocketError) {
        let (observer, _) = self.lifecycle.websocket_observer();
        if let Some(observer) = observer.as_ref() {
            observer.mark_authn_reached();
            terminate_error(observer, error);
        }
    }

    pub async fn client_message(
        &mut self,
        auth: Authenticated,
        frame: Bytes,
    ) -> Result<Option<WebSocketDispatch>, WebSocketError> {
        let body = parse_client_frame(&auth, &frame, self.byte_cap);
        let repeated_steer = body.as_ref().ok().is_some_and(|body| {
            body.get("type").and_then(Value::as_str) == Some("response.steer")
                && body
                    .get("previous_response_id")
                    .and_then(Value::as_str)
                    .is_some_and(|parent| self.successor_for_parent(parent).is_some())
        });
        let (observer, request_id) = self.lifecycle.websocket_observer();
        let mut auth_info = observer
            .as_ref()
            .map(|_| self.lifecycle.websocket_auth_info(&auth));
        if !repeated_steer
            && let (Some(observer), Some(info)) = (observer.as_ref(), auth_info.take())
        {
            observe_authenticated_frame(observer, info);
            self.lifecycle
                .record_authentication_completed(&auth, observer);
        }
        let result = match body {
            Ok(body) => {
                self.admit_client(
                    auth,
                    frame,
                    body,
                    if repeated_steer {
                        None
                    } else {
                        observer.clone()
                    },
                    &request_id,
                )
                .await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = result.as_ref()
            && let Some(observer) = observer.as_ref()
        {
            if let Some(info) = auth_info.take() {
                observe_authenticated_frame(observer, info);
            }
            terminate_error(observer, error);
        }
        result
    }

    async fn admit_client(
        &mut self,
        auth: Authenticated,
        frame: Bytes,
        body: Value,
        observer: Option<LifecycleContext>,
        request_id: &str,
    ) -> Result<Option<WebSocketDispatch>, WebSocketError> {
        let kind = body.get("type").and_then(Value::as_str).ok_or_else(|| {
            WebSocketError::new(
                400,
                "invalid_request_error",
                "WebSocket client message requires a type",
            )
        })?;
        if kind == "response.steer" {
            return self
                .admit_steer(auth, frame, body, observer, request_id)
                .await;
        }
        if kind != "response.create" {
            return Err(WebSocketError::new(
                400,
                "invalid_request_error",
                "Unsupported Responses WebSocket client event",
            ));
        }
        let lane = stream_id(&body)?;
        let previous = body
            .get("previous_response_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let owner = previous
            .as_ref()
            .and_then(|id| self.owners.get(id))
            .cloned();
        if owner
            .as_ref()
            .is_some_and(|owner| owner.principal_id != auth.principal_id())
        {
            return Err(WebSocketError::new(
                400,
                "previous_response_not_found",
                "The previous response is not owned by this client",
            )
            .with_stream(lane));
        }
        let charge = frame
            .len()
            .saturating_add(json_owned_bytes(&body))
            .saturating_add(std::mem::size_of::<Turn>())
            .saturating_add(lane.as_ref().map_or(0, String::len))
            .saturating_add(previous.as_ref().map_or(0, String::len))
            .saturating_add(auth.principal_id().len())
            .saturating_add(auth.key_id().len())
            .saturating_add(
                body.get("model")
                    .and_then(Value::as_str)
                    .map_or(0, str::len),
            );
        self.check_admission(&lane, charge)?;

        // A required tool result is an explicit create with its own settings.
        // Retire the never-created successor before reserving this explicit one.
        if let Some(parent) = previous.as_ref()
            && let Some(successor) = self.successors.remove(parent)
        {
            let error = WebSocketError::new(
                409,
                "steering_requires_explicit_response",
                "Steering will continue through the explicit response.create",
            );
            self.finish_turn(successor, Some(&error));
        }
        let body = Arc::new(body);
        let prepared = self
            .lifecycle
            .prepare_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: &self.headers,
                    frame: &frame,
                    query: self.query.as_deref(),
                },
                body.clone(),
                owner.as_ref().map(|owner| owner.upstream_id),
                observer,
                request_id,
            )
            .await
            .map_err(|error| error.with_stream(lane.clone()))?;
        if let Some(stream_id) = lane.as_deref() {
            prepared.span.record("cc_lb.responses.stream_id", stream_id);
        }
        self.next_turn_id = self.next_turn_id.checked_add(1).ok_or_else(|| {
            WebSocketError::new(
                503,
                "overloaded_error",
                "WebSocket turn identifier space is exhausted",
            )
        })?;
        let turn_id = self.next_turn_id;
        self.used_bytes = self.used_bytes.saturating_add(charge);
        self.turns.insert(
            turn_id,
            Turn {
                auth,
                prepared,
                body,
                frame,
                lane: lane.clone(),
                connection_id: owner.as_ref().map(|owner| owner.connection_id),
                upstream_id: owner.as_ref().map(|owner| owner.upstream_id),
                pinned: owner
                    .as_ref()
                    .map(|owner| (owner.connection_id, owner.upstream_id)),
                response_id: None,
                previous_response_id: previous,
                steer_parent: None,
                attempted: false,
                steer_submissions: 0,
                accepted_steers: HashSet::new(),
                sent: false,
                controls: VecDeque::new(),
                charged_bytes: charge,
                usage: None,
                usage_complete: false,
            },
        );
        if !self.lanes.contains_key(&lane) {
            self.used_bytes = self.used_bytes.saturating_add(lane_bytes(&lane));
        }
        let lane_state = self.lanes.entry(lane).or_default();
        lane_state.queued.push_back(turn_id);
        if lane_state.active.is_none() {
            let next = lane_state
                .queued
                .pop_front()
                .expect("newly queued turn exists");
            lane_state.active = Some(next);
            Ok(self.dispatch(next))
        } else {
            Ok(None)
        }
    }

    fn check_admission(&self, lane: &Option<String>, bytes: usize) -> Result<(), WebSocketError> {
        let lane_bytes = if self.lanes.contains_key(lane) {
            0
        } else {
            std::mem::size_of::<Lane>().saturating_add(lane.as_ref().map_or(0, String::len))
        };
        if self.slot_cap == 0
            || self.turns.len() >= self.slot_cap
            || (!self.lanes.contains_key(lane) && self.lanes.len() >= self.slot_cap)
            || bytes.saturating_add(lane_bytes) > self.byte_cap.saturating_sub(self.used_bytes)
        {
            return Err(WebSocketError::new(
                503,
                "overloaded_error",
                "WebSocket session admission budget is full",
            )
            .with_stream(lane.clone()));
        }
        Ok(())
    }

    async fn admit_steer(
        &mut self,
        auth: Authenticated,
        frame: Bytes,
        control: Value,
        observer: Option<LifecycleContext>,
        request_id: &str,
    ) -> Result<Option<WebSocketDispatch>, WebSocketError> {
        let parent = control
            .get("previous_response_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                WebSocketError::new(
                    400,
                    "invalid_request_error",
                    "response.steer requires previous_response_id",
                )
            })?
            .to_owned();
        let owner = self.owners.get(&parent).cloned().ok_or_else(|| {
            WebSocketError::new(
                400,
                "response_not_found",
                "The steering target is not available on this WebSocket connection",
            )
        })?;
        if owner.principal_id != auth.principal_id() {
            return Err(WebSocketError::new(
                400,
                "response_not_found",
                "The steering target is not owned by this client",
            ));
        }
        let lane = owner.lane.clone();
        if let Some(successor) = self.successor_for_parent(&parent) {
            let Some(turn) = self.turns.get_mut(&successor) else {
                return Err(WebSocketError::new(
                    502,
                    "invalid_upstream_response",
                    "Steering successor ownership is unavailable",
                ));
            };
            let charge = queued_control_bytes(&frame);
            if charge > self.byte_cap.saturating_sub(self.used_bytes)
                || (!turn.sent && self.control_slots >= self.slot_cap)
            {
                return Err(WebSocketError::new(
                    503,
                    "overloaded_error",
                    "WebSocket steering budget is full",
                )
                .with_stream(lane));
            }
            turn.steer_submissions = turn.steer_submissions.saturating_add(1);
            if !turn.sent {
                self.control_slots += 1;
                self.used_bytes = self.used_bytes.saturating_add(charge);
                turn.charged_bytes = turn.charged_bytes.saturating_add(charge);
                turn.controls.push_back(frame);
                return Ok(None);
            }
            return Ok(Some(WebSocketDispatch {
                turn_id: successor,
                frame,
                connection_id: Some(owner.connection_id),
                candidates: Vec::new(),
                stream_id: lane,
            }));
        }
        let body = owner.body.clone();
        let charge = frame
            .len()
            .saturating_add(std::mem::size_of::<Turn>())
            .saturating_add(parent.len().saturating_mul(3))
            .saturating_add(lane.as_ref().map_or(0, String::len))
            .saturating_add(auth.principal_id().len())
            .saturating_add(auth.key_id().len());
        self.check_admission(&lane, charge)?;
        let prepared = self
            .lifecycle
            .prepare_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: &self.headers,
                    frame: &frame,
                    query: self.query.as_deref(),
                },
                body.clone(),
                Some(owner.upstream_id),
                observer,
                request_id,
            )
            .await
            .map_err(|error| error.with_stream(lane.clone()))?;
        if let Some(stream_id) = lane.as_deref() {
            prepared.span.record("cc_lb.responses.stream_id", stream_id);
        }
        self.next_turn_id = self.next_turn_id.checked_add(1).ok_or_else(|| {
            WebSocketError::new(
                503,
                "overloaded_error",
                "WebSocket turn identifier space is exhausted",
            )
        })?;
        let turn_id = self.next_turn_id;
        self.used_bytes = self.used_bytes.saturating_add(charge);
        self.successors.insert(parent.clone(), turn_id);
        self.turns.insert(
            turn_id,
            Turn {
                auth,
                prepared,
                body,
                frame,
                lane: lane.clone(),
                connection_id: Some(owner.connection_id),
                upstream_id: Some(owner.upstream_id),
                response_id: None,
                pinned: Some((owner.connection_id, owner.upstream_id)),
                previous_response_id: Some(parent.clone()),
                steer_parent: Some(parent),
                attempted: false,
                steer_submissions: 1,
                accepted_steers: HashSet::new(),
                sent: false,
                controls: VecDeque::new(),
                charged_bytes: charge,
                usage: None,
                usage_complete: false,
            },
        );
        let lane_state = self.lanes.entry(lane).or_default();
        match lane_state.active {
            None => {
                lane_state.active = Some(turn_id);
                Ok(self.dispatch(turn_id))
            }
            Some(active) if active == owner.turn_id => Ok(self.dispatch(turn_id)),
            Some(_) => {
                lane_state.queued.push_back(turn_id);
                Ok(None)
            }
        }
    }

    fn dispatch(&mut self, turn_id: u64) -> Option<WebSocketDispatch> {
        let turn = self.turns.get_mut(&turn_id)?;
        Some(WebSocketDispatch {
            turn_id,
            frame: turn.frame.clone(),
            connection_id: turn.connection_id,
            candidates: std::mem::take(&mut turn.prepared.candidates),
            stream_id: turn.lane.clone(),
        })
    }

    pub async fn prepare_connection(
        &self,
        dispatch: &WebSocketDispatch,
        candidate: &WebSocketCandidate,
    ) -> Result<(Url, HeaderMap), WebSocketError> {
        let auth = self.authenticate().await.map_err(|error| {
            WebSocketError::new(
                error.http_status(),
                "authentication_error",
                error.to_string(),
            )
        })?;
        let turn = self.turns.get(&dispatch.turn_id).ok_or_else(|| {
            WebSocketError::new(
                409,
                "response_not_found",
                "The admitted response is no longer available",
            )
        })?;
        if auth.principal_id() != turn.prepared.principal_id
            || auth.key_id() != turn.prepared.key_id
        {
            return Err(WebSocketError::new(
                401,
                "authentication_error",
                "The client identity changed before connection",
            ));
        }
        if turn
            .prepared
            .reservation
            .as_ref()
            .is_some_and(|reservation| !reservation.is_active())
        {
            return Err(WebSocketError::new(
                503,
                "reservation_expired",
                "The queued response reservation is no longer active",
            ));
        }
        self.lifecycle
            .validate_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: &self.headers,
                    frame: &turn.frame,
                    query: self.query.as_deref(),
                },
                &turn.body,
                candidate.upstream_id,
            )
            .await?;
        self.lifecycle
            .sign_websocket_connection(&auth, &self.headers, candidate, self.query.as_deref())
            .await
    }

    pub fn assign_connection(
        &mut self,
        turn_id: u64,
        connection_id: u64,
        upstream_id: Uuid,
        upstream_name: &str,
    ) {
        self.connections.entry(connection_id).or_insert(upstream_id);
        if let Some(turn) = self.turns.get_mut(&turn_id) {
            turn.connection_id = Some(connection_id);
            turn.upstream_id = Some(upstream_id);
            turn.prepared
                .span
                .record("cc_lb.responses.connection_id", connection_id);
            turn.prepared
                .span
                .record("cc_lb.upstream.id", tracing::field::display(upstream_id));
            if turn.prepared.route_info.upstream_id != upstream_id {
                turn.prepared.route_info.upstream_id = upstream_id;
                turn.prepared.route_info.upstream_name = upstream_name.to_owned();
                turn.prepared.route_info.routing_trace = None;
                turn.prepared.route_info.upstream_kind = None;
                turn.prepared.route_info.route_ms = None;
                turn.prepared.route_info.predicted_cache_read_tokens = None;
                turn.prepared.route_info.matched_v3_cache_key = None;
                turn.prepared.route_info.breakpoint_content_block_index = None;
                turn.prepared.route_info.matched_content_block_index = None;
                turn.prepared.route_info.lookback_distance = None;
                turn.prepared.route_info.predicted_cache_creation_tokens_5m = None;
                turn.prepared.route_info.predicted_cache_creation_tokens_1h = None;
                turn.prepared.route_info.token_estimate_source = None;
                turn.prepared.route_info.cache_value_micros = None;
                turn.prepared.route_info.formula_winner_upstream_id = None;
                turn.prepared.route_info.kept_upstream_id = None;
                turn.prepared.route_info.quota_urgency_5h = None;
                turn.prepared.route_info.quota_urgency_7d = None;
                turn.prepared.route_info.quota_urgency_combined = None;
                turn.prepared.route_info.quota_warning_multiplier = None;
                if let Some(observer) = turn.prepared.observer.as_ref() {
                    observer.emit_lifecycle(LifecycleEvent::RouteCompleted {
                        event_id: observer.event_id().to_owned(),
                        result: Ok(turn.prepared.route_info.clone()),
                        routing_trace: None,
                    });
                }
            }
        }
    }

    pub async fn validate_send(&mut self, turn_id: u64) -> Result<(), WebSocketError> {
        let Some(turn) = self.turns.get(&turn_id) else {
            return Err(WebSocketError::new(
                409,
                "response_not_found",
                "The queued response is no longer available",
            ));
        };
        let auth = self.authenticate().await.map_err(|error| {
            // The already-owned turn reports this authentication failure. Do not
            // construct another observer, which would duplicate the request row.
            WebSocketError::new(
                error.http_status(),
                "authentication_error",
                error.to_string(),
            )
            .with_stream(turn.lane.clone())
        })?;
        if auth.principal_id() != turn.prepared.principal_id
            || auth.key_id() != turn.prepared.key_id
        {
            return Err(WebSocketError::new(
                401,
                "authentication_error",
                "The client identity changed before dispatch",
            )
            .with_stream(turn.lane.clone()));
        }
        let upstream_id = turn.upstream_id.ok_or_else(|| {
            WebSocketError::new(
                502,
                "upstream_unavailable",
                "No upstream owns this dispatch",
            )
        })?;
        if turn
            .connection_id
            .and_then(|connection| self.connections.get(&connection))
            != Some(&upstream_id)
        {
            return Err(WebSocketError::new(
                503,
                "upstream_affinity_unavailable",
                "The selected provider connection is no longer owned by this upstream",
            )
            .with_stream(turn.lane.clone()));
        }
        if turn.pinned.is_some_and(|(connection, upstream)| {
            turn.connection_id != Some(connection) || upstream_id != upstream
        }) {
            return Err(WebSocketError::new(
                503,
                "upstream_affinity_unavailable",
                "A continuation cannot move to another provider connection",
            )
            .with_stream(turn.lane.clone()));
        }
        self.lifecycle
            .validate_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: &self.headers,
                    frame: &turn.frame,
                    query: self.query.as_deref(),
                },
                &turn.body,
                upstream_id,
            )
            .await
            .map_err(|error| error.with_stream(turn.lane.clone()))?;
        let turn = self.turns.get_mut(&turn_id).ok_or_else(|| {
            WebSocketError::new(
                409,
                "response_not_found",
                "The admitted response is no longer available",
            )
        })?;
        if turn
            .prepared
            .reservation
            .as_ref()
            .is_some_and(|reservation| !reservation.is_active())
        {
            return Err(WebSocketError::new(
                503,
                "reservation_expired",
                "The queued response reservation is no longer active",
            )
            .with_stream(turn.lane.clone()));
        }
        if !turn.attempted {
            turn.attempted = true;
            if let Some(observer) = turn.prepared.observer.as_ref() {
                observer.emit_lifecycle(LifecycleEvent::UpstreamAttempt {
                    event_id: observer.event_id().to_owned(),
                    attempt_num: 1,
                    upstream_id,
                });
            }
        }
        Ok(())
    }

    pub fn sent(&mut self, turn_id: u64) -> Vec<WebSocketDispatch> {
        let Some(turn) = self.turns.get_mut(&turn_id) else {
            return Vec::new();
        };
        turn.sent = true;
        let mut ready = Vec::with_capacity(turn.controls.len());
        while let Some(frame) = turn.controls.pop_front() {
            let charge = queued_control_bytes(&frame);
            self.used_bytes = self.used_bytes.saturating_sub(charge);
            turn.charged_bytes = turn.charged_bytes.saturating_sub(charge);
            self.control_slots = self.control_slots.saturating_sub(1);
            ready.push(WebSocketDispatch {
                turn_id,
                frame,
                connection_id: turn.connection_id,
                candidates: Vec::new(),
                stream_id: turn.lane.clone(),
            });
        }
        ready
    }

    pub async fn provider_message(
        &mut self,
        connection_id: u64,
        frame: &Bytes,
    ) -> Result<WebSocketEventResult, WebSocketError> {
        // A borrowed lazy discriminator is the only work for deltas. It does
        // not construct a Value, copy payload bytes, or record observations.
        let kind = {
            let event_type = sonic_rs::get_from_slice(frame, &["type"]).map_err(|_| {
                WebSocketError::new(
                    502,
                    "invalid_upstream_response",
                    "Upstream WebSocket event has no valid type",
                )
            })?;
            match event_type.as_str() {
                Some("response.created") => "response.created",
                Some("response.queued") => "response.queued",
                Some("response.in_progress") => "response.in_progress",
                Some("response.completed") => "response.completed",
                Some("response.failed") => "response.failed",
                Some("response.incomplete") => "response.incomplete",
                Some("error") => "error",
                Some("response.steer.accepted") => "response.steer.accepted",
                Some("response.steer.pending") => "response.steer.pending",
                Some("response.steer.failed") => "response.steer.failed",
                Some(_) => {
                    return Ok(WebSocketEventResult {
                        forward: true,
                        ..Default::default()
                    });
                }
                None => {
                    return Err(WebSocketError::new(
                        502,
                        "invalid_upstream_response",
                        "Upstream event type is not a string",
                    ));
                }
            }
        };
        if frame.len() > self.byte_cap {
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream control exceeds the configured message cap",
            ));
        }
        let value: Value = sonic_rs::from_slice(frame).map_err(|_| {
            WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream control is not valid JSON",
            )
        })?;
        let lane = stream_id(&value).map_err(|_| {
            WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream control carries an invalid stream_id",
            )
        })?;
        if kind == "error"
            && value.pointer("/error/code").and_then(Value::as_str)
                == Some("websocket_connection_limit_reached")
        {
            return Err(WebSocketError::new(
                400,
                "websocket_connection_limit_reached",
                value
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("The provider WebSocket reached its connection limit"),
            ));
        }
        if kind.starts_with("response.steer.") {
            return self.steering_event(connection_id, kind, &value, lane);
        }
        let response = value.get("response");
        let response_id = response
            .and_then(|response| response.get("id"))
            .and_then(Value::as_str)
            .or_else(|| value.get("response_id").and_then(Value::as_str));
        let terminal = matches!(
            kind,
            "response.completed" | "response.failed" | "response.incomplete" | "error"
        );
        if terminal && kind != "error" && response_id.is_none() {
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream terminal has no response identity",
            )
            .with_stream(lane));
        }
        let mut turn_id = response_id
            .and_then(|id| self.owners.get(id))
            .map(|owner| owner.turn_id);
        if turn_id.is_none() && !matches!(kind, "error") {
            let active = self.lanes.get(&lane).and_then(|lane| lane.active);
            let parent = response
                .and_then(|response| response.get("previous_response_id"))
                .and_then(Value::as_str);
            turn_id = parent
                .and_then(|parent| self.successor_for_parent(parent))
                .or_else(|| {
                    active
                        .and_then(|active| self.turns.get(&active))
                        .and_then(|active| active.response_id.as_deref())
                        .and_then(|parent| self.successor_for_parent(parent))
                })
                .or(active);
        }
        if kind == "error" && turn_id.is_none() {
            turn_id = self
                .lanes
                .get(&lane)
                .and_then(|lane| lane.active)
                .filter(|id| {
                    self.turns
                        .get(id)
                        .is_some_and(|turn| turn.connection_id == Some(connection_id) && turn.sent)
                });
        }
        let Some(turn_id) = turn_id else {
            // Never open a second settlement from an unowned or replayed id.
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream event cannot be attributed to an admitted response",
            )
            .with_stream(lane));
        };
        let Some(turn) = self.turns.get(&turn_id) else {
            let duplicate = terminal
                && response_id
                    .and_then(|id| self.owners.get(id))
                    .is_some_and(|owner| {
                        owner.turn_id == turn_id
                            && owner.connection_id == connection_id
                            && owner.lane == lane
                    });
            if duplicate {
                return Ok(WebSocketEventResult::default());
            }
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream response is already terminal",
            )
            .with_stream(lane));
        };
        if turn.connection_id != Some(connection_id) || turn.lane != lane || !turn.sent {
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream response does not match its connection and lane",
            )
            .with_stream(lane));
        }
        if let Some(id) = response_id {
            if turn.response_id.as_deref().is_some_and(|bound| bound != id) {
                return Err(WebSocketError::new(
                    502,
                    "invalid_upstream_response",
                    "Upstream changed an admitted response id",
                )
                .with_stream(lane));
            }
            if turn.response_id.is_none() {
                if !matches!(
                    kind,
                    "response.queued" | "response.created" | "response.in_progress"
                ) {
                    return Err(WebSocketError::new(
                        502,
                        "invalid_upstream_response",
                        "Upstream terminal arrived before response ownership was established",
                    )
                    .with_stream(lane));
                }
                let upstream_id = turn.upstream_id.ok_or_else(|| {
                    WebSocketError::new(
                        502,
                        "upstream_unavailable",
                        "Response upstream is unavailable",
                    )
                })?;
                let response = response.ok_or_else(|| {
                    WebSocketError::new(
                        502,
                        "invalid_upstream_response",
                        "Response identity has no response object",
                    )
                })?;
                let stored = turn.prepared.store
                    && response
                        .get("store")
                        .and_then(Value::as_bool)
                        .unwrap_or(true);
                self.lifecycle
                    .bind_websocket_response(&turn.auth, upstream_id, response, stored)
                    .await?;
                let principal_id = turn.auth.principal_id().to_owned();
                let body = turn.body.clone();
                let previous = turn.previous_response_id.clone();
                if let Some(turn) = self.turns.get_mut(&turn_id) {
                    turn.previous_response_id = None;
                }
                self.collect_owner(previous.as_deref());
                self.bind_owner(
                    id,
                    ResponseOwner {
                        connection_id,
                        upstream_id,
                        turn_id,
                        principal_id,
                        body,
                        lane: lane.clone(),
                    },
                    &lane,
                )?;
                let turn = self
                    .turns
                    .get_mut(&turn_id)
                    .expect("admitted turn remains owned during binding");
                turn.response_id = Some(id.to_owned());
                turn.prepared.span.record("gen_ai.response.id", id);
                if let Some(parent) = turn.steer_parent.as_ref() {
                    self.successors.remove(parent);
                    let lane = self.lanes.entry(lane.clone()).or_default();
                    if lane.active.is_none() || lane.active == Some(turn_id) {
                        lane.active = Some(turn_id);
                    }
                }
            }
        }
        if let Some(response) = response
            && let Some((usage, complete)) = observed_response_usage(response)
        {
            let turn = self.turns.get_mut(&turn_id).expect("bound turn exists");
            turn.usage = Some(usage);
            turn.usage_complete = terminal && complete;
        }
        let mut result = WebSocketEventResult {
            forward: true,
            ..Default::default()
        };
        if terminal {
            let failure =
                (kind != "response.completed").then(|| native_terminal_error(&value, &lane));
            self.finish_turn(turn_id, failure.as_ref());
            if let Some(dispatch) = self.next_in_lane(&lane) {
                result.dispatches.push(dispatch);
            }
        }
        Ok(result)
    }

    fn bind_owner(
        &mut self,
        id: &str,
        owner: ResponseOwner,
        lane: &Option<String>,
    ) -> Result<(), WebSocketError> {
        if let Some(existing) = self.owners.get(id) {
            if existing.turn_id == owner.turn_id && existing.connection_id == owner.connection_id {
                return Ok(());
            }
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Response id is already owned by another turn",
            )
            .with_stream(lane.clone()));
        }
        let recent_key = (owner.connection_id, lane.clone());
        let old = self.recent.remove(&recent_key);
        self.collect_owner(old.as_deref());
        let cost = owner_bytes(id, &owner);
        if self.owners.len() >= self.slot_cap
            || cost > self.byte_cap.saturating_sub(self.used_bytes)
        {
            return Err(WebSocketError::new(
                503,
                "overloaded_error",
                "WebSocket response ownership budget is full",
            )
            .with_stream(lane.clone()));
        }
        self.used_bytes = self.used_bytes.saturating_add(cost);
        self.recent.insert(recent_key, id.to_owned());
        self.owners.insert(id.to_owned(), owner);
        Ok(())
    }

    fn collect_owner(&mut self, id: Option<&str>) {
        let Some(id) = id else { return };
        let referenced = self.turns.values().any(|turn| {
            turn.response_id.as_deref() == Some(id)
                || turn.previous_response_id.as_deref() == Some(id)
        }) || self.recent.values().any(|recent| recent == id);
        if !referenced && let Some(owner) = self.owners.remove(id) {
            self.used_bytes = self.used_bytes.saturating_sub(owner_bytes(id, &owner));
        }
    }

    fn steering_event(
        &mut self,
        connection_id: u64,
        kind: &str,
        value: &Value,
        lane: Option<String>,
    ) -> Result<WebSocketEventResult, WebSocketError> {
        let parent = value
            .get("steer")
            .and_then(|steer| steer.get("previous_response_id"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                WebSocketError::new(
                    502,
                    "invalid_upstream_response",
                    "Steering control has no parent response",
                )
            })?;
        let turn_id = self.successor_for_parent(parent).ok_or_else(|| {
            WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Steering control has no reserved successor",
            )
        })?;
        let turn = self.turns.get_mut(&turn_id).ok_or_else(|| {
            WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Steering successor is unavailable",
            )
        })?;
        if turn.connection_id != Some(connection_id) || turn.lane != lane {
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Steering control changed its owning connection or lane",
            ));
        }
        let steer_id = value
            .get("steer")
            .and_then(|steer| steer.get("id"))
            .and_then(Value::as_str);
        let mut result = WebSocketEventResult {
            forward: true,
            ..Default::default()
        };
        if kind == "response.steer.accepted" {
            if let Some(id) = steer_id {
                let charge = steering_id_bytes(id);
                if !turn.accepted_steers.contains(id) {
                    if self.control_slots >= self.slot_cap
                        || charge > self.byte_cap.saturating_sub(self.used_bytes)
                    {
                        return Err(WebSocketError::new(
                            503,
                            "overloaded_error",
                            "WebSocket steering ownership budget is full",
                        ));
                    }
                    turn.accepted_steers.insert(id.to_owned());
                    self.control_slots += 1;
                    self.used_bytes = self.used_bytes.saturating_add(charge);
                    turn.charged_bytes = turn.charged_bytes.saturating_add(charge);
                }
            }
        } else if kind == "response.steer.failed" {
            if let Some(id) = steer_id
                && turn.accepted_steers.remove(id)
            {
                let charge = steering_id_bytes(id);
                self.control_slots = self.control_slots.saturating_sub(1);
                self.used_bytes = self.used_bytes.saturating_sub(charge);
                turn.charged_bytes = turn.charged_bytes.saturating_sub(charge);
            }
            turn.steer_submissions = turn.steer_submissions.saturating_sub(1);
            if turn.steer_submissions == 0 && turn.response_id.is_none() {
                let error = WebSocketError::new(
                    400,
                    "steering_failed",
                    "The provider did not commit the steering successor",
                )
                .with_stream(lane.clone());
                self.finish_turn(turn_id, Some(&error));
                if let Some(dispatch) = self.next_in_lane(&lane) {
                    result.dispatches.push(dispatch);
                }
            }
        }
        Ok(result)
    }

    fn finish_turn(&mut self, turn_id: u64, error: Option<&WebSocketError>) {
        let Some(mut turn) = self.turns.remove(&turn_id) else {
            return;
        };
        self.used_bytes = self.used_bytes.saturating_sub(turn.charged_bytes);
        self.control_slots = self.control_slots.saturating_sub(
            turn.controls
                .len()
                .saturating_add(turn.accepted_steers.len()),
        );
        if let Some(parent) = turn.steer_parent.as_ref()
            && self.successors.get(parent) == Some(&turn_id)
        {
            self.successors.remove(parent);
        }
        let successor = turn
            .response_id
            .as_ref()
            .and_then(|id| self.successor_for_parent(id))
            .filter(|id| {
                self.turns
                    .get(id)
                    .is_some_and(|successor| successor.sent && successor.lane == turn.lane)
            });
        if let Some(lane) = self.lanes.get_mut(&turn.lane) {
            if lane.active == Some(turn_id) {
                lane.active = successor.filter(|successor| !lane.queued.contains(successor));
            }
            lane.queued.retain(|id| *id != turn_id);
        }
        if let Some(observer) = turn.prepared.observer.as_ref() {
            if let Some(usage) = turn.usage.take() {
                observer.set_usage_counts(&usage, error.is_none() && turn.usage_complete);
                observer.emit_lifecycle(LifecycleEvent::UsageObserved {
                    event_id: observer.event_id().to_owned(),
                    usage: usage_snapshot(usage),
                    source: UsageSource::NonStreamBody,
                });
            }
            // The subscriber owns settlement from this point, including an
            // interrupted response with measured usage. Drop must not refund it.
            if let Some(reservation) = turn.prepared.reservation.take() {
                reservation.handoff_after_response();
            }
            if let Some(error) = error {
                if let Some(native_code) = error.native_code.as_deref() {
                    observer.emit_lifecycle(LifecycleEvent::StreamCompleted {
                        event_id: observer.event_id().to_owned(),
                        result: Err(cc_lb_lifecycle::StreamError {
                            error_type: native_code.to_owned(),
                            error_message: error.message.clone(),
                        }),
                    });
                    observer.set_upstream_error(
                        StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY),
                        UpstreamErrorCode::StreamError,
                    );
                }
                terminate_error(observer, error);
            } else {
                observer.set_success_status(http::StatusCode::OK);
                observer.finish();
            }
        } else if let Some(reservation) = turn.prepared.reservation.take() {
            // No event bus exists for this embedded caller. Settle business
            // counters directly; normal server traffic always uses the worker.
            if let (Some(engine), Some(usage)) =
                (turn.prepared.limit_engine.as_ref(), turn.usage.as_ref())
                && let Some(cost) = cc_lb_pricing::virtual_cost_micros_full_with_context(
                    &turn.prepared.model,
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.cache_creation_input_tokens,
                    usage.cache_creation_input_tokens_5m,
                    usage.cache_creation_input_tokens_1h,
                    usage.cache_read_input_tokens,
                    usage.service_tier.as_deref(),
                )
            {
                engine.reconcile(
                    reservation,
                    usage.input_tokens,
                    usage.output_tokens,
                    cost.total_micros,
                );
            }
            // Otherwise the reservation's existing RAII owner refunds it.
        }
        self.collect_owner(turn.previous_response_id.as_deref());
        self.collect_owner(turn.response_id.as_deref());
        // Retain only bounded, connection-local most-recent control metadata.
        // Other stored ids resolve through the existing durable affinity store.
        self.prune_empty_lanes();
    }

    fn prune_empty_lanes(&mut self) {
        let mut released = 0_usize;
        self.lanes.retain(|id, lane| {
            let keep = lane.active.is_some()
                || !lane.queued.is_empty()
                || self.recent.keys().any(|(_, recent_lane)| recent_lane == id);
            if !keep {
                released = released.saturating_add(lane_bytes(id));
            }
            keep
        });
        self.used_bytes = self.used_bytes.saturating_sub(released);
    }

    fn next_in_lane(&mut self, lane_id: &Option<String>) -> Option<WebSocketDispatch> {
        let lane = self.lanes.get_mut(lane_id)?;
        if lane.active.is_some() {
            return None;
        }
        let next = lane.queued.pop_front()?;
        lane.active = Some(next);
        self.dispatch(next)
    }

    pub async fn fail_dispatch(
        &mut self,
        turn_id: u64,
        error: &WebSocketError,
    ) -> Vec<WebSocketDispatch> {
        let lane = self.turns.get(&turn_id).map(|turn| turn.lane.clone());
        self.finish_turn(turn_id, Some(error));
        lane.and_then(|lane| self.next_in_lane(&lane))
            .into_iter()
            .collect()
    }

    pub fn connection_closed(&mut self, connection_id: u64, reason: &str) -> WebSocketEventResult {
        self.connections.remove(&connection_id);
        let turn_ids: Vec<_> = self
            .turns
            .iter()
            .filter_map(|(id, turn)| (turn.connection_id == Some(connection_id)).then_some(*id))
            .collect();
        let mut result = WebSocketEventResult::default();
        for id in turn_ids {
            let lane = self.turns.get(&id).map(|turn| turn.lane.clone());
            let error = WebSocketError::new(502, "upstream_connection_closed", reason)
                .with_stream(lane.clone().flatten());
            self.finish_turn(id, Some(&error));
            result.errors.push(error);
        }
        let dead_owners: Vec<_> = self
            .owners
            .iter()
            .filter_map(|(id, owner)| (owner.connection_id == connection_id).then_some(id.clone()))
            .collect();
        self.recent
            .retain(|(connection, _), _| *connection != connection_id);
        for id in dead_owners {
            self.collect_owner(Some(&id));
        }
        let lanes: Vec<_> = self.lanes.keys().cloned().collect();
        for lane in lanes {
            if let Some(dispatch) = self.next_in_lane(&lane) {
                result.dispatches.push(dispatch);
            }
        }
        self.prune_empty_lanes();
        result
    }

    pub fn drain(&mut self) {
        let error = WebSocketError::new(
            503,
            "draining",
            "The gateway is draining active WebSocket responses",
        );
        self.finish_session(&error);
    }

    pub fn close(&mut self) {
        let error = WebSocketError::new(
            499,
            "client_closed_request",
            "The client WebSocket closed before the response completed",
        );
        self.finish_session(&error);
    }

    fn finish_session(&mut self, error: &WebSocketError) {
        let turn_ids: Vec<_> = self.turns.keys().copied().collect();
        for turn_id in turn_ids {
            self.finish_turn(turn_id, Some(error));
        }
        self.lanes.clear();
        self.owners.clear();
        self.recent.clear();
        self.connections.clear();
        self.successors.clear();
        self.used_bytes = 0;
        self.control_slots = 0;
    }
}

impl Drop for ResponsesWebSocketGateway {
    fn drop(&mut self) {
        self.close();
    }
}

fn native_terminal_error(value: &Value, lane: &Option<String>) -> WebSocketError {
    let status = value
        .get("status")
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .unwrap_or(502);
    let native_code = value
        .pointer("/response/error/code")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .pointer("/response/error/type")
                .and_then(Value::as_str)
        });
    let message = value
        .pointer("/response/error/message")
        .and_then(Value::as_str)
        .unwrap_or("The provider response did not complete successfully");
    let mut error =
        WebSocketError::new(status, "upstream_stream_error", message).with_stream(lane.clone());
    if let Some(code) = native_code {
        error = error.with_native_code(code.to_owned());
    }
    error
}

fn stream_id(value: &Value) -> Result<Option<String>, WebSocketError> {
    match value.get("stream_id") {
        None => Ok(None),
        Some(Value::String(id))
            if !id.is_empty()
                && id.len() <= 256
                && id.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
                }) =>
        {
            Ok(Some(id.clone()))
        }
        _ => Err(WebSocketError::new(
            400,
            "invalid_stream_id",
            "stream_id must contain 1-256 letters, numbers, underscores, hyphens, or periods",
        )),
    }
}

fn json_owned_bytes(value: &Value) -> usize {
    let own = std::mem::size_of::<Value>();
    match value {
        Value::String(value) => own.saturating_add(value.len()),
        Value::Array(values) => values.iter().fold(own, |total, value| {
            total.saturating_add(json_owned_bytes(value))
        }),
        Value::Object(values) => values.iter().fold(own, |total, (key, value)| {
            total
                .saturating_add(key.len())
                .saturating_add(json_owned_bytes(value))
                .saturating_add(4 * std::mem::size_of::<usize>())
        }),
        _ => own,
    }
}

fn terminate_error(observer: &LifecycleContext, error: &WebSocketError) {
    observer.terminate_failure(InternalFailure {
        status: StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY),
        error_code: error.code,
        error: cc_lb_domain::InternalError {
            stage: if error.code == "authentication_error" {
                cc_lb_domain::InternalErrorStage::Authn
            } else {
                cc_lb_domain::InternalErrorStage::Relay
            },
            kind: if error.status < 500 {
                cc_lb_domain::InternalErrorKind::InvalidInput
            } else {
                cc_lb_domain::InternalErrorKind::Unavailable
            },
            message: Some(error.message.clone()),
        },
    });
}

fn usage_snapshot(usage: UsageCounts) -> UsageSnapshot {
    UsageSnapshot {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_creation_input_tokens: usage.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: usage.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: usage.cache_creation_input_tokens_1h,
        cache_read_input_tokens: usage.cache_read_input_tokens,
        thinking_tokens: usage.thinking_tokens,
        web_search_requests: usage.web_search_requests,
        web_fetch_requests: usage.web_fetch_requests,
        service_tier: usage.service_tier,
        inference_geo: usage.inference_geo,
        iterations: usage.iterations,
    }
}

fn lane_bytes(lane: &Option<String>) -> usize {
    std::mem::size_of::<Lane>().saturating_add(lane.as_ref().map_or(0, String::len))
}

fn owner_bytes(id: &str, owner: &ResponseOwner) -> usize {
    std::mem::size_of::<ResponseOwner>()
        .saturating_add(owner.principal_id.len())
        .saturating_add(std::mem::size_of::<(u64, Option<String>)>())
        .saturating_add(id.len().saturating_mul(3))
        .saturating_add(owner.lane.as_ref().map_or(0, String::len).saturating_mul(2))
        .saturating_add(json_owned_bytes(&owner.body))
}

fn parse_client_frame(
    _authenticated: &Authenticated,
    frame: &Bytes,
    cap: usize,
) -> Result<Value, WebSocketError> {
    if frame.len() > cap {
        return Err(WebSocketError::new(
            413,
            "request_too_large",
            "WebSocket message exceeds the configured body cap",
        ));
    }
    sonic_rs::from_slice(frame).map_err(|_| {
        WebSocketError::new(
            400,
            "invalid_json",
            "WebSocket client message must be valid JSON",
        )
    })
}

fn observe_authenticated_frame(observer: &LifecycleContext, info: cc_lb_lifecycle::AuthInfo) {
    observer.mark_authn_reached();
    observer.emit_request_started(true);
    observer.emit_lifecycle(LifecycleEvent::AuthCompleted {
        event_id: observer.event_id().to_owned(),
        result: Ok(info),
    });
}

fn queued_control_bytes(frame: &Bytes) -> usize {
    frame.len().saturating_add(std::mem::size_of::<Bytes>())
}

fn steering_id_bytes(id: &str) -> usize {
    id.len()
        .saturating_add(std::mem::size_of::<String>())
        .saturating_add(2 * std::mem::size_of::<usize>())
}

fn observed_response_usage(response: &Value) -> Option<(UsageCounts, bool)> {
    let reported = response
        .get("usage")
        .or_else(|| {
            response
                .get("response")
                .and_then(|value| value.get("usage"))
        })?
        .as_object()?;
    let input = reported.get("input_tokens").and_then(Value::as_u64);
    let output = reported.get("output_tokens").and_then(Value::as_u64);
    if input.is_none() && output.is_none() {
        return None;
    }
    let mut usage = observe_non_stream_json_value(response).usage;
    if let Some(tier) = response
        .get("service_tier")
        .and_then(Value::as_str)
        .or_else(|| {
            response
                .get("response")
                .and_then(|value| value.get("service_tier"))
                .and_then(Value::as_str)
        })
    {
        usage.service_tier = Some(tier.to_owned());
    }
    Some((usage, input.is_some() && output.is_some()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_null_and_empty_usage_remain_unknown_not_zero() {
        for value in [
            serde_json::json!({}),
            serde_json::json!({"usage": null}),
            serde_json::json!({"usage": {}}),
            serde_json::json!({"usage": {"input_tokens": null, "output_tokens": null}}),
        ] {
            assert!(observed_response_usage(&value).is_none());
        }
        let (zero, complete) = observed_response_usage(
            &serde_json::json!({"usage": {"input_tokens": 0, "output_tokens": 0}}),
        )
        .expect("explicit numeric zero is a real observation");
        assert!(zero.present && complete);
        assert_eq!((zero.input_tokens, zero.output_tokens), (0, 0));
        let (partial, complete) =
            observed_response_usage(&serde_json::json!({"usage": {"input_tokens": 7}}))
                .expect("measured input is retained on interruption");
        assert_eq!(partial.input_tokens, 7);
        assert!(
            !complete,
            "an unreported output count is not a complete zero snapshot"
        );
        let (tiered, _) = observed_response_usage(
            &serde_json::json!({"service_tier": "priority", "usage": {"input_tokens": 7, "output_tokens": 11}}),
        ).unwrap();
        assert_eq!(
            tiered.service_tier.as_deref(),
            Some("priority"),
            "pricing uses the provider's resolved tier"
        );
    }

    #[test]
    fn named_and_default_lanes_remain_distinct() {
        assert_eq!(stream_id(&serde_json::json!({})).unwrap(), None);
        assert_eq!(
            stream_id(&serde_json::json!({"stream_id":"agent.main-1"}))
                .unwrap()
                .as_deref(),
            Some("agent.main-1")
        );
        for value in [
            Value::Null,
            Value::String(String::new()),
            Value::String("a/b".to_owned()),
        ] {
            assert_eq!(
                stream_id(&serde_json::json!({"stream_id":value}))
                    .unwrap_err()
                    .code,
                "invalid_stream_id"
            );
        }
    }

    #[test]
    fn response_failed_uses_native_error_details() {
        let error = native_terminal_error(
            &serde_json::json!({
                "type": "response.failed",
                "status": 200,
                "response": {
                    "error": {
                        "code": "server_error",
                        "message": "native failure"
                    }
                }
            }),
            &Some("main".to_owned()),
        );
        assert_eq!(error.code, "upstream_stream_error");
        assert_eq!(error.native_code.as_deref(), Some("server_error"));
        assert_eq!(error.message, "native failure");
        assert_eq!(error.stream_id.as_deref(), Some("main"));
    }

    #[test]
    fn protocol_error_keeps_provider_error_shape_and_default_lane_absent() {
        let error = WebSocketError::new(503, "overloaded_error", "configured budget is full");
        let value: Value = sonic_rs::from_slice(&error.to_bytes()).unwrap();
        assert_eq!(value["type"], "error");
        assert_eq!(value["status"], 503);
        assert_eq!(value["error"]["code"], "overloaded_error");
        assert!(value.get("stream_id").is_none());
        let value: Value =
            sonic_rs::from_slice(&error.with_stream(Some("main".to_owned())).to_bytes()).unwrap();
        assert_eq!(value["stream_id"], "main");
    }

    #[test]
    fn borrowed_delta_discriminator_does_not_construct_or_rewrite_payload() {
        let raw = Bytes::from_static(
            br#"{"type":"response.output_text.delta","delta":"opaque payload","stream_id":"main"}"#,
        );
        let pointer = raw.as_ptr();
        let event_type = sonic_rs::get_from_slice(&raw, &["type"]).unwrap();
        assert_eq!(event_type.as_str(), Some("response.output_text.delta"));
        assert_eq!(raw.as_ptr(), pointer);
        assert_eq!(
            raw.as_ref(),
            br#"{"type":"response.output_text.delta","delta":"opaque payload","stream_id":"main"}"#
        );
    }
}
