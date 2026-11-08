//! Responses WebSocket admission and per-response ownership.
//!
//! Transport I/O lives in the server. This single-owner state machine keeps
//! connection-local continuation separate from durable response ownership.
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
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
use crate::completion_observer::{StreamCompletionLog, StreamLatency};
use crate::downstream_stream_drop_guard::record_stream_termination_metric;
use crate::lifecycle::Lifecycle;
use crate::response_transform::ResponseTransformContext;
use crate::terminal_observer::{
    InternalFailure, LifecycleContext, StreamTerminationCause, StreamTerminationOutcome,
    UpstreamErrorCode,
};
use crate::usage_parser::{UsageCounts, observe_non_stream_json_value};
use cc_lb_upstream::{SseEvent, TransformSseEventRequest, TransformSseEventResult};

#[derive(Clone, Debug)]
pub struct WebSocketError {
    pub status: u16,
    pub code: &'static str,
    pub native_code: Option<String>,
    pub message: String,
    pub stream_id: Option<String>,
    provider_headers: Vec<(String, String)>,
}

impl WebSocketError {
    pub fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            native_code: None,
            message: message.into(),
            stream_id: None,
            provider_headers: Vec::new(),
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

    /// Retain only bounded, non-sensitive provider response headers.
    pub fn with_provider_headers(mut self, headers: &HeaderMap) -> Self {
        for (name, value) in headers {
            if self.provider_headers.len() >= 32 {
                break;
            }
            let name = name.as_str().to_ascii_lowercase();
            if name != "retry-after"
                && !name.starts_with("x-ratelimit-")
                && !name.starts_with("anthropic-ratelimit-")
                && !name.starts_with("ratelimit-")
            {
                continue;
            }
            let Ok(value) = value.to_str() else {
                continue;
            };
            if value.len() > 1024 {
                continue;
            }
            self.provider_headers.push((name, value.to_owned()));
        }
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
        if !self.provider_headers.is_empty() {
            let headers = self
                .provider_headers
                .iter()
                .map(|(name, value)| (name.clone(), Value::String(value.clone())))
                .collect::<BTreeMap<_, _>>();
            error["headers"] = serde_json::to_value(headers)
                .expect("serialized provider headers contain only JSON values");
        }
        Bytes::from(serde_json::to_vec(&error).expect("WebSocket error contains only JSON values"))
    }
    pub fn copy_provider_headers(&self, headers: &mut http::HeaderMap) {
        for (name, value) in &self.provider_headers {
            if let (Ok(name), Ok(value)) = (
                http::HeaderName::from_bytes(name.as_bytes()),
                http::HeaderValue::from_str(value),
            ) {
                headers.append(name, value);
            }
        }
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
    pub completion_observer: Arc<crate::completion_observer::CompletionObserver>,
    pub reservation: Option<Reservation>,
    pub limit_engine: Option<Arc<LimitEngine>>,
    pub principal_id: String,
    pub key_id: String,
    pub model: String,
    pub store: bool,
    pub span: Span,
    pub route_info: cc_lb_lifecycle::RouteInfo,
    pub request_id: String,
}

pub(crate) struct WebSocketPluginRequest {
    pub frame: Bytes,
    pub headers: HeaderMap,
    pub context: ResponseTransformContext,
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
    /// Replacement text frames, in order; `None` retains the upstream frame.
    pub frames: Option<Vec<Bytes>>,
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

#[derive(Debug)]
struct ResponsesStreamMetrics {
    started: std::time::Instant,
    first_output_ms: Option<u64>,
    last_output_ms: Option<u64>,
    event_count: u64,
    total_bytes: u64,
    provider_session_id: Option<String>,
}

impl ResponsesStreamMetrics {
    fn new() -> Self {
        Self {
            started: std::time::Instant::now(),
            first_output_ms: None,
            last_output_ms: None,
            event_count: 0,
            total_bytes: 0,
            provider_session_id: None,
        }
    }

    fn observe(&mut self, frame: &[u8]) {
        let elapsed = self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        self.first_output_ms.get_or_insert(elapsed);
        self.last_output_ms = Some(elapsed);
        self.event_count = self.event_count.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(frame.len() as u64);
        if self.provider_session_id.is_none() {
            self.provider_session_id = sonic_rs::get_from_slice(frame, &["response", "id"])
                .ok()
                .or_else(|| sonic_rs::get_from_slice(frame, &["response_id"]).ok())
                .and_then(|value| value.as_str().map(str::to_owned));
        }
    }

    fn latency(&self, status: u16) -> StreamLatency {
        let stream_total_ms = self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let inter_output_avg_ms = match (self.first_output_ms, self.last_output_ms) {
            (Some(first), Some(last)) if self.event_count > 1 => {
                Some(last.saturating_sub(first) / (self.event_count - 1))
            }
            _ => None,
        };
        StreamLatency {
            status,
            stream_first_chunk_ms: self.first_output_ms,
            stream_message_start_ms: None,
            stream_content_block_start_ms: None,
            stream_first_content_delta_ms: None,
            stream_last_content_delta_ms: None,
            stream_message_stop_ms: None,
            stream_last_chunk_ms: self.last_output_ms,
            stream_first_output_ms: self.first_output_ms,
            stream_last_output_ms: self.last_output_ms,
            stream_event_count: self.event_count,
            stream_inter_output_avg_ms: inter_output_avg_ms,
            provider_session_id: self.provider_session_id.clone(),
            provider_connection_id: None,
            stream_total_ms,
            sse_event_count: 0,
            content_delta_count: 0,
            ping_count: 0,
            inter_token_avg_ms: None,
            total_bytes: self.total_bytes,
        }
    }
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
    plugin_request: Option<(Uuid, WebSocketPluginRequest)>,
    metrics: ResponsesStreamMetrics,
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
        let (observer, _) = self.lifecycle.websocket_observer("/v1/responses");
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
        let (observer, _) = self.lifecycle.websocket_observer("/v1/responses");
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
        let (observer, request_id) = self.lifecycle.websocket_observer("/v1/responses");
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
                plugin_request: None,
                metrics: ResponsesStreamMetrics::new(),
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
                plugin_request: None,
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
        &mut self,
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
        let (turn_frame, turn_body) = {
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
            (turn.frame.clone(), turn.body.clone())
        };
        self.lifecycle
            .validate_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: &self.headers,
                    frame: &turn_frame,
                    query: self.query.as_deref(),
                },
                &turn_body,
                candidate.upstream_id,
            )
            .await?;
        self.prepare_plugin_request(dispatch.turn_id, candidate.upstream_id)?;
        let headers = self
            .turns
            .get(&dispatch.turn_id)
            .and_then(|turn| turn.plugin_request.as_ref())
            .map_or(&self.headers, |(_, request)| &request.headers);
        self.lifecycle
            .sign_websocket_connection(&auth, headers, candidate, self.query.as_deref())
            .await
    }

    fn prepare_plugin_request(
        &mut self,
        turn_id: u64,
        upstream_id: Uuid,
    ) -> Result<(), WebSocketError> {
        let (auth, frame, request_id, observer, old_charge) = {
            let turn = self.turns.get(&turn_id).ok_or_else(|| {
                WebSocketError::new(
                    409,
                    "response_not_found",
                    "The admitted response is no longer available",
                )
            })?;
            if turn
                .plugin_request
                .as_ref()
                .is_some_and(|(id, _)| *id == upstream_id)
            {
                return Ok(());
            }
            (
                turn.auth.clone(),
                turn.frame.clone(),
                turn.prepared.request_id.clone(),
                turn.prepared.observer.clone(),
                turn.plugin_request
                    .as_ref()
                    .map_or(0, |(_, request)| request.frame.len()),
            )
        };
        let request = self.lifecycle.shape_websocket_frame(
            &auth,
            WebSocketIngress {
                headers: &self.headers,
                frame: &frame,
                query: self.query.as_deref(),
            },
            upstream_id,
            &request_id,
            observer.as_ref(),
        )?;
        let new_charge = request.as_ref().map_or(0, |request| request.frame.len());
        if new_charge
            > self
                .byte_cap
                .saturating_sub(self.used_bytes)
                .saturating_add(old_charge)
        {
            return Err(WebSocketError::new(
                503,
                "overloaded_error",
                "Shaped WebSocket frame exceeds the session budget",
            ));
        }
        self.used_bytes = self
            .used_bytes
            .saturating_sub(old_charge)
            .saturating_add(new_charge);
        let turn = self.turns.get_mut(&turn_id).expect("turn remains admitted");
        turn.charged_bytes = turn
            .charged_bytes
            .saturating_sub(old_charge)
            .saturating_add(new_charge);
        turn.plugin_request = request.map(|request| (upstream_id, request));
        Ok(())
    }

    /// Shape each create once for the actual connection owner. Steering controls
    /// keep their native wire format; their successor retains the response hook.
    pub fn frame_for_send(
        &mut self,
        dispatch: &WebSocketDispatch,
    ) -> Result<Bytes, WebSocketError> {
        let (upstream_id, steer) = {
            let turn = self.turns.get(&dispatch.turn_id).ok_or_else(|| {
                WebSocketError::new(
                    409,
                    "response_not_found",
                    "The admitted response is no longer available",
                )
            })?;
            (
                turn.upstream_id.ok_or_else(|| {
                    WebSocketError::new(
                        502,
                        "upstream_unavailable",
                        "No upstream owns this dispatch",
                    )
                })?,
                turn.steer_parent.is_some(),
            )
        };
        if steer {
            return Ok(dispatch.frame.clone());
        }
        self.prepare_plugin_request(dispatch.turn_id, upstream_id)?;
        let turn = self
            .turns
            .get(&dispatch.turn_id)
            .expect("turn remains admitted");
        Ok(turn.plugin_request.as_ref().map_or_else(
            || dispatch.frame.clone(),
            |(_, request)| request.frame.clone(),
        ))
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

    fn observe_provider_metrics(&mut self, connection_id: u64, frame: &Bytes) {
        if let Some(turn) = self
            .turns
            .values_mut()
            .find(|turn| turn.connection_id == Some(connection_id) && turn.sent)
        {
            turn.metrics.observe(frame);
        }
    }

    pub async fn provider_message(
        &mut self,
        connection_id: u64,
        frame: &Bytes,
    ) -> Result<WebSocketEventResult, WebSocketError> {
        self.observe_provider_metrics(connection_id, frame);
        let transformed = self.transform_provider_frame(connection_id, frame);
        let mut result = self
            .observe_provider_message(connection_id, frame, transformed.as_ref().err())
            .await?;
        match transformed {
            Ok(Some(frames)) => {
                result.forward = false;
                result.frames = Some(frames);
            }
            Ok(None) => {}
            Err(error) => {
                result.forward = false;
                if let Some(lane) = error.stream_id.as_ref()
                    && let Some(turn_id) = self
                        .lanes
                        .get(&Some(lane.clone()))
                        .and_then(|lane| lane.active)
                {
                    self.finish_turn(turn_id, Some(&error));
                    if let Some(dispatch) = self.next_in_lane(&Some(lane.clone())) {
                        result.dispatches.push(dispatch);
                    }
                }
                result.errors.push(error);
            }
        }
        Ok(result)
    }

    fn transform_provider_frame(
        &self,
        connection_id: u64,
        frame: &Bytes,
    ) -> Result<Option<Vec<Bytes>>, WebSocketError> {
        // No JSON work, allocation, or guest call for connections without hooks.
        if !self.turns.values().any(|turn| {
            turn.connection_id == Some(connection_id)
                && turn.plugin_request.as_ref().is_some_and(|(_, request)| {
                    request.context.dialect.sse_event_transform_hook().is_some()
                })
        }) {
            return Ok(None);
        }
        let lane = sonic_rs::get_from_slice(frame, &["stream_id"])
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned));
        let response_id = sonic_rs::get_from_slice(frame, &["response_id"])
            .ok()
            .or_else(|| sonic_rs::get_from_slice(frame, &["response", "id"]).ok());
        let turn = response_id
            .as_ref()
            .and_then(|id| id.as_str())
            .and_then(|id| self.owners.get(id))
            .and_then(|owner| self.turns.get(&owner.turn_id))
            .or_else(|| {
                self.lanes
                    .get(&lane)
                    .and_then(|lane| lane.active)
                    .and_then(|id| self.turns.get(&id))
            })
            .filter(|turn| turn.connection_id == Some(connection_id) && turn.sent);
        let Some(turn) = turn else {
            return Ok(None);
        };
        let Some((_, request)) = turn.plugin_request.as_ref() else {
            return Ok(None);
        };
        transform_websocket_event(
            &request.context,
            &turn.prepared.request_id,
            &turn.prepared.model,
            frame,
            self.byte_cap,
        )
        .map_err(|error| error.with_stream(lane))
    }

    async fn observe_provider_message(
        &mut self,
        connection_id: u64,
        frame: &Bytes,
        transform_error: Option<&WebSocketError>,
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
            let failure = transform_error.cloned().or_else(|| {
                (kind != "response.completed").then(|| native_terminal_error(&value, &lane))
            });
            self.finish_turn(turn_id, failure.as_ref());
            if let Some(dispatch) = self.next_in_lane(&lane) {
                result.dispatches.push(dispatch);
            }
        }
        if let Some(error) = transform_error
            && !terminal
        {
            self.finish_turn(turn_id, Some(error));
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
        let complete = error.is_none();
        let (outcome, cause) = match error {
            None => (
                StreamTerminationOutcome::Completed,
                StreamTerminationCause::None,
            ),
            Some(error) if error.code == "client_closed_request" => (
                StreamTerminationOutcome::ClientCancelled,
                StreamTerminationCause::None,
            ),
            Some(error)
                if error.native_code.is_some() || error.code == "upstream_connection_closed" =>
            {
                (
                    StreamTerminationOutcome::UpstreamError,
                    StreamTerminationCause::ProviderError,
                )
            }
            Some(_) => (
                StreamTerminationOutcome::ProxyError,
                StreamTerminationCause::TransportError,
            ),
        };
        record_stream_termination_metric(
            outcome,
            cause,
            turn.prepared
                .candidates
                .first()
                .map(|candidate| candidate.upstream_name.as_str())
                .unwrap_or("unknown"),
            Some(&turn.prepared.model),
        );
        let latency = turn
            .metrics
            .latency(StatusCode::SWITCHING_PROTOCOLS.as_u16());
        latency.record_on_span(&turn.prepared.span);
        if let Some(observer) = turn.prepared.observer.as_ref() {
            let usage = turn.usage.take();
            if let Some(usage) = usage.as_ref() {
                observer.set_usage_counts(usage, complete);
                observer.emit_lifecycle(LifecycleEvent::UsageObserved {
                    event_id: observer.event_id().to_owned(),
                    usage: usage_snapshot(usage.clone()),
                    source: UsageSource::NonStreamBody,
                });
            }
            observer.emit_lifecycle(LifecycleEvent::StreamCompleted {
                event_id: observer.event_id().to_owned(),
                result: if complete {
                    Ok(cc_lb_lifecycle::StreamSuccess {
                        usage: usage
                            .as_ref()
                            .map(|usage| usage_snapshot(usage.clone()))
                            .unwrap_or_default(),
                        usage_present: usage.is_some(),
                        sse_event_count: 0,
                        body_bytes: Some(latency.total_bytes),
                        body_chunk_count: Some(latency.stream_event_count),
                        first_body_chunk_ms: latency.stream_first_output_ms,
                        stream_last_chunk_ms: latency.stream_last_output_ms,
                        stream_total_ms: Some(latency.stream_total_ms),
                        stream_inter_output_avg_ms: latency.stream_inter_output_avg_ms,
                        provider_session_id: latency.provider_session_id.clone(),
                        provider_connection_id: latency.provider_connection_id.clone(),
                        ..Default::default()
                    })
                } else {
                    Err(cc_lb_lifecycle::StreamError {
                        error_type: error
                            .map(|error| {
                                error
                                    .native_code
                                    .as_deref()
                                    .unwrap_or(error.code)
                                    .to_owned()
                            })
                            .unwrap_or_else(|| "responses_stream_incomplete".to_owned()),
                        error_message: error.map(|error| error.message.clone()).unwrap_or_else(
                            || "Responses WebSocket stream ended before completion".to_owned(),
                        ),
                    })
                },
            });
            // The subscriber owns settlement from this point, including an
            // interrupted response with measured usage. Drop must not refund it.
            if let Some(reservation) = turn.prepared.reservation.take() {
                reservation.handoff_after_response();
            }
            match (error, complete) {
                (Some(error), _) if error.code == "client_closed_request" => {
                    observer.set_client_closed(StatusCode::SWITCHING_PROTOCOLS);
                }
                (Some(error), _)
                    if error.native_code.is_some()
                        || error.code == "upstream_connection_closed" =>
                {
                    observer.set_upstream_error(
                        StatusCode::SWITCHING_PROTOCOLS,
                        UpstreamErrorCode::StreamError,
                    );
                }
                (Some(_), _) => observer.set_failure(InternalFailure {
                    status: StatusCode::SWITCHING_PROTOCOLS,
                    error_code: UpstreamErrorCode::StreamError.as_str(),
                    error: cc_lb_domain::InternalError {
                        stage: cc_lb_domain::InternalErrorStage::Relay,
                        kind: cc_lb_domain::InternalErrorKind::Unavailable,
                        message: Some("Responses WebSocket relay failed".to_owned()),
                    },
                }),
                (None, true) => observer.set_success_status(StatusCode::SWITCHING_PROTOCOLS),
                (None, false) => observer.set_upstream_error(
                    StatusCode::SWITCHING_PROTOCOLS,
                    UpstreamErrorCode::StreamError,
                ),
            }
            observer.finish();
            if let Some(dispatch) = tracing::enabled!(
                target: "cc_lb_engine::lifecycle",
                tracing::Level::INFO
            )
            .then(|| tracing::dispatcher::get_default(|dispatch| dispatch.clone()))
            {
                let _ = turn
                    .prepared
                    .completion_observer
                    .enqueue(StreamCompletionLog::new(
                        turn.prepared.request_id.clone(),
                        &turn.prepared.span,
                        latency,
                        dispatch,
                    ));
            }
        } else if let Some(reservation) = turn.prepared.reservation.take() {
            // No event bus exists for this embedded caller. Settle business
            // counters directly; normal server traffic always uses the worker.
            if let (Some(engine), Some(usage)) =
                (turn.prepared.limit_engine.as_ref(), turn.usage.as_ref())
            {
                let cost = cc_lb_pricing::virtual_cost_micros_full_with_context_and_images(
                    &turn.prepared.model,
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.input_image_tokens.unwrap_or(0),
                    usage.output_image_tokens.unwrap_or(0),
                    usage.cache_creation_input_tokens,
                    usage.cache_creation_input_tokens_5m,
                    usage.cache_creation_input_tokens_1h,
                    usage.cache_read_input_tokens,
                    usage.service_tier.as_deref(),
                )
                .unwrap_or_else(cc_lb_pricing::ComputedCostBreakdown::unknown);
                engine.reconcile(
                    reservation,
                    usage.input_tokens,
                    usage.output_tokens,
                    cost.total_micros,
                );
            }
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

    fn activate_next_turn(lane: &mut Lane) -> Option<u64> {
        if lane.active.is_some() {
            return None;
        }
        let next = lane.queued.pop_front()?;
        lane.active = Some(next);
        Some(next)
    }

    fn next_in_lane(&mut self, lane_id: &Option<String>) -> Option<WebSocketDispatch> {
        let lane = self.lanes.get_mut(lane_id)?;
        let next = Self::activate_next_turn(lane)?;
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

fn transform_websocket_event(
    context: &ResponseTransformContext,
    request_id: &str,
    model: &str,
    frame: &Bytes,
    cap: usize,
) -> Result<Option<Vec<Bytes>>, WebSocketError> {
    let Some(hook) = context.dialect.sse_event_transform_hook() else {
        return Ok(None);
    };
    let error = |message: String| WebSocketError::new(502, "response_transform_error", message);
    let kind = sonic_rs::get_from_slice(frame, &["type"])
        .map_err(|_| error("Upstream event has no valid type".to_owned()))?;
    let name = kind
        .as_str()
        .ok_or_else(|| error("Upstream event type is not a string".to_owned()))?;
    match hook
        .transform_sse_event(TransformSseEventRequest {
            request_id: request_id.to_owned(),
            principal: context.principal.clone(),
            upstream: context.upstream.clone(),
            request_method: context.request_method.clone(),
            request_path: context.request_path.clone(),
            canonical_model_id: crate::model_resolution::canonical_model_id(model).to_owned(),
            response_status: StatusCode::OK,
            response_headers: HeaderMap::new(),
            event: SseEvent {
                event: name.to_owned(),
                data: frame.clone(),
            },
        })
        .map_err(|source| error(source.to_string()))?
    {
        TransformSseEventResult::Unchanged => Ok(None),
        TransformSseEventResult::Drop => Ok(Some(Vec::new())),
        TransformSseEventResult::Replace { events } => {
            let mut frames = Vec::with_capacity(events.len());
            let mut bytes = 0usize;
            for event in events {
                let mut value: Value = sonic_rs::from_slice(&event.data).map_err(|_| {
                    error("Plugin returned an invalid WebSocket JSON event".to_owned())
                })?;
                let object = value.as_object_mut().ok_or_else(|| {
                    error("Plugin returned a non-object WebSocket event".to_owned())
                })?;
                if !event.event.is_empty() {
                    object.insert("type".to_owned(), Value::String(event.event));
                }
                if object.get("type").and_then(Value::as_str).is_none() {
                    return Err(error(
                        "Plugin returned a WebSocket event without a type".to_owned(),
                    ));
                }
                let frame = Bytes::from(
                    serde_json::to_vec(&value).map_err(|source| error(source.to_string()))?,
                );
                bytes = bytes.saturating_add(frame.len());
                if bytes > cap {
                    return Err(error(
                        "Plugin WebSocket output exceeds the message budget".to_owned(),
                    ));
                }
                frames.push(frame);
            }
            Ok(Some(frames))
        }
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
        input_image_tokens: usage.input_image_tokens,
        output_image_tokens: usage.output_image_tokens,
        cache_creation_input_tokens: usage.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: usage.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: usage.cache_creation_input_tokens_1h,
        cache_read_input_tokens: usage.cache_read_input_tokens,
        thinking_tokens: usage.thinking_tokens,
        tool_use_prompt_tokens: usage.tool_use_prompt_tokens,
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

    struct TestWebSocketTransformDialect {
        result: TransformSseEventResult,
    }

    impl cc_lb_upstream::UpstreamDialect for TestWebSocketTransformDialect {
        fn shape(
            &self,
            _context: &cc_lb_upstream::DialectShapeContext,
            _upstream: &cc_lb_domain::Upstream,
            _principal: &cc_lb_domain::Principal,
            _builder: &mut cc_lb_upstream::ShapedRequestBuilder,
        ) -> Result<cc_lb_upstream::ShapedRequest, cc_lb_upstream::DialectError> {
            unreachable!("the websocket transform test does not shape a request")
        }

        fn sse_event_transform_hook(&self) -> Option<&dyn cc_lb_upstream::SseEventTransformHook> {
            Some(self)
        }
    }

    impl cc_lb_upstream::SseEventTransformHook for TestWebSocketTransformDialect {
        fn transform_sse_event(
            &self,
            _request: cc_lb_upstream::TransformSseEventRequest,
        ) -> Result<TransformSseEventResult, cc_lb_upstream::ResponseTransformError> {
            Ok(self.result.clone())
        }
    }

    fn websocket_transform_context(result: TransformSseEventResult) -> ResponseTransformContext {
        ResponseTransformContext {
            principal: cc_lb_domain::Principal {
                id: "principal".to_owned(),
                kind: cc_lb_domain::PrincipalKind::InternalKey,
            },
            upstream: cc_lb_domain::Upstream::OpenaiDirect { base_url: None },
            request_method: http::Method::POST,
            request_path: "/v1/responses".to_owned(),
            dialect: Arc::new(TestWebSocketTransformDialect { result }),
            gemini_oauth: false,
        }
    }

    #[test]
    fn websocket_sse_transform_maps_replace_drop_and_event_type() {
        let replacement = transform_websocket_event(
            &websocket_transform_context(TransformSseEventResult::Replace {
                events: vec![SseEvent {
                    event: "response.output_text.delta".to_owned(),
                    data: Bytes::from_static(br#"{"delta":"rewritten"}"#),
                }],
            }),
            "request",
            "gpt-test",
            &Bytes::from_static(br#"{"type":"response.output_text.delta","delta":"raw"}"#),
            1024,
        )
        .expect("replacement succeeds")
        .expect("replacement emits frames");
        let value: Value = sonic_rs::from_slice(&replacement[0]).expect("frame is JSON");
        assert_eq!(value["type"], "response.output_text.delta");
        assert_eq!(value["delta"], "rewritten");

        let dropped = transform_websocket_event(
            &websocket_transform_context(TransformSseEventResult::Drop),
            "request",
            "gpt-test",
            &Bytes::from_static(br#"{"type":"response.completed"}"#),
            1024,
        )
        .expect("drop succeeds");
        assert_eq!(dropped, Some(Vec::new()));
    }

    #[test]
    fn terminal_transform_failure_advances_named_and_default_lanes() {
        for lane_id in [None, Some("main".to_owned())] {
            let mut lane = Lane {
                active: None,
                queued: VecDeque::from([11, 12]),
            };
            assert_eq!(
                ResponsesWebSocketGateway::activate_next_turn(&mut lane),
                Some(11)
            );
            assert_eq!(lane.active, Some(11));
            lane.active = None;
            assert_eq!(
                ResponsesWebSocketGateway::activate_next_turn(&mut lane),
                Some(12)
            );
            assert_eq!(lane.active, Some(12));
            assert!(lane_id.is_none() || lane_id.as_deref() == Some("main"));
        }
    }

    #[test]
    fn responses_metrics_keep_provider_neutral_timing_and_identity() {
        let mut metrics = ResponsesStreamMetrics::new();
        metrics.observe(br#"{"response":{"id":"resp_123"}}"#);
        metrics.observe(br#"{"type":"response.output_text.delta","delta":"ok"}"#);
        let latency = metrics.latency(101);
        assert_eq!(latency.stream_event_count, 2);
        assert!(latency.stream_first_output_ms.is_some());
        assert!(latency.stream_last_output_ms.is_some());
        assert_eq!(latency.provider_session_id.as_deref(), Some("resp_123"));
        assert_eq!(latency.sse_event_count, 0);
        assert_eq!(latency.content_delta_count, 0);
    }
}
