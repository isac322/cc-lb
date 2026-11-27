use super::*;

pub(super) async fn responses_websocket_handler(
    State(state): State<ProxyState>,
    OriginalUri(uri): OriginalUri,
    mut headers: HeaderMap,
    provider_scope: Option<Extension<ProviderScope>>,
    codex_backend: Option<Extension<cc_lb_engine::CodexBackendSurface>>,
    websocket: WebSocketUpgrade,
) -> Response<Body> {
    // WebSocket routes intentionally bypass the normal drain middleware.
    // Rejecting before authentication keeps drain probes silent.
    let Some(guard) = state.drain_controller.try_track() else {
        let mut response = Response::new(Body::from("draining"));
        *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
        response
            .headers_mut()
            .insert(RETRY_AFTER, HeaderValue::from_static("60"));
        response
            .extensions_mut()
            .insert(TerminalClassification::DRAIN_REJECTED);
        return response;
    };

    cc_lb_engine::strip_hop_by_hop(&mut headers);
    let auth = match state.lifecycle.authenticate(&headers).await {
        Ok(auth) => auth,
        Err(error) => {
            return state
                .lifecycle
                .reject_responses_websocket_authentication_for_provider(
                    &error,
                    uri.path(),
                    provider_scope.as_ref().map(|scope| scope.0.0),
                );
        }
    };
    let slots: usize = state
        .websocket_bulkhead_config
        .semaphore_permits
        .try_into()
        .unwrap_or(usize::MAX);
    let gateway = ResponsesWebSocketGateway::new(
        state.lifecycle.clone(),
        &auth,
        headers,
        state.body_caps.messages,
        slots,
    )
    .with_provider(provider_scope.as_ref().map(|scope| scope.0.0))
    .with_codex_backend(codex_backend.is_some())
    .with_downstream_query(uri.query().and_then(|query| {
        let scrubbed = scrub_query_api_key(query);
        (!scrubbed.is_empty()).then_some(scrubbed.into_owned())
    }));
    websocket
        .max_message_size(state.body_caps.messages)
        .max_frame_size(state.body_caps.messages)
        .on_upgrade(move |downstream| async move {
            let _guard = guard;
            relay_responses_websocket(downstream, state, gateway).await;
        })
}

type WsProviderSink =
    futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, UpstreamMessage>;

pub(super) struct WsProviderConnection {
    upstream_id: uuid::Uuid,
    upstream_name: String,
    sink: WsProviderSink,
    task: Option<JoinHandle<()>>,
    lane: Option<String>,
    _bulkhead_guard: cc_lb_engine::OwnedBulkheadGuard,
}

impl Drop for WsProviderConnection {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub(super) enum WsProviderEvent {
    Frame {
        connection_id: u64,
        bytes: Bytes,
        text: bool,
    },
    Ping {
        connection_id: u64,
        bytes: Bytes,
    },
    Closed {
        connection_id: u64,
        reason: String,
    },
}

fn provider_websocket_error_from_http(response: http::Response<Option<Vec<u8>>>) -> WebSocketError {
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    WebSocketError::new(
        status,
        "upstream_unavailable",
        "provider WebSocket handshake rejected",
    )
    .with_provider_headers(&headers)
}

pub(super) fn websocket_breaker_key(upstream_id: uuid::Uuid) -> String {
    format!("websocket:{upstream_id}")
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct WsConnectTimings {
    pub(super) dns_ms: Option<u64>,
    pub(super) connect_ms: Option<u64>,
}

fn acquire_websocket_breaker_permit(
    state: &ProxyState,
    upstream_id: uuid::Uuid,
) -> Result<cc_lb_engine::Permit, WebSocketError> {
    let breaker = state.breaker_registry.breaker(
        websocket_breaker_key(upstream_id),
        state.breaker_config,
        state.clock.clone(),
    );
    breaker.permit().map_err(|_| {
        WebSocketError::new(
            502,
            "upstream_unavailable",
            "provider WebSocket connection is unavailable",
        )
    })
}

pub(super) const DEFAULT_WS_PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
pub(super) const WS_PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
pub(super) const WS_DISPATCH_CONNECT_BUDGET: Duration = Duration::from_secs(30);
pub(super) const WS_MAX_UPSTREAM_ATTEMPTS: usize = 3;
const MAX_WS_EVENT_DRAIN_BATCH: usize = 64;

pub(super) async fn connect_provider_websocket(
    state: &ProxyState,
    upstream_id: uuid::Uuid,
    request: http::Request<()>,
    bulkhead_guard: cc_lb_engine::OwnedBulkheadGuard,
) -> Result<
    (
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        HeaderMap,
        cc_lb_engine::OwnedBulkheadGuard,
        WsConnectTimings,
    ),
    WebSocketError,
> {
    let permit = acquire_websocket_breaker_permit(state, upstream_id)?;
    connect_provider_websocket_with_permit(
        permit,
        state.body_caps.messages,
        state.dns_resolver.clone(),
        request,
        bulkhead_guard,
        DEFAULT_WS_PROVIDER_CONNECT_TIMEOUT.min(state.upstream_connect_timeout),
    )
    .await
}

async fn connect_provider_websocket_with_permit(
    permit: cc_lb_engine::Permit,
    message_cap: usize,
    dns_resolver: cc_lb_engine::MetricDnsResolver,
    request: http::Request<()>,
    bulkhead_guard: cc_lb_engine::OwnedBulkheadGuard,
    connect_timeout: Duration,
) -> Result<
    (
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        HeaderMap,
        cc_lb_engine::OwnedBulkheadGuard,
        WsConnectTimings,
    ),
    WebSocketError,
> {
    let Some(host) = request.uri().host().map(str::to_owned) else {
        drop(permit);
        return Err(WebSocketError::new(
            502,
            "upstream_unavailable",
            "provider WebSocket connection failed",
        ));
    };
    let port = request.uri().port_u16().unwrap_or_else(|| {
        if request.uri().scheme_str() == Some("wss") {
            443
        } else {
            80
        }
    });
    let ws_config = responses_websocket_config(message_cap);
    let Some(dial_deadline) = tokio::time::Instant::now().checked_add(connect_timeout) else {
        drop(permit);
        return Err(WebSocketError::new(
            502,
            "upstream_unavailable",
            "provider WebSocket connect timeout exceeds representable deadline",
        ));
    };
    let result = tokio::time::timeout_at(dial_deadline, async move {
        let mut connect_timings = WsConnectTimings::default();
        let ip_literal = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .ok();
        let addrs = match ip_literal {
            Some(ip) => {
                connect_timings.dns_ms = Some(0);
                cc_lb_engine::dns_cache::ProgressiveSocketAddrs {
                    initial: vec![std::net::SocketAddr::new(ip, port)],
                    pending_peer: None,
                }
            }
            None => {
                let dns_started = std::time::Instant::now();
                let resolved = dns_resolver
                    .resolve_host_progressive(&host, port)
                    .await
                    .map_err(|err| {
                        tokio_tungstenite::tungstenite::Error::Io(io::Error::other(err.to_string()))
                    })?;
                connect_timings.dns_ms =
                    Some(dns_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
                resolved
            }
        };
        let connect_started = std::time::Instant::now();
        let tcp = cc_lb_engine::dns_cache::connect_tcp_with_progressive_fallback(
            addrs,
            Some(dial_deadline),
        )
        .await
        .map_err(tokio_tungstenite::tungstenite::Error::Io)?;
        let (stream, response) =
            client_async_tls_with_config(request, tcp, Some(ws_config), None).await?;
        connect_timings.connect_ms = Some(
            connect_started
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        );
        Ok::<_, tokio_tungstenite::tungstenite::Error>((stream, response, connect_timings))
    })
    .await;
    match result {
        Ok(Ok((stream, response, connect_timings))) => {
            let (response_parts, _) = response.into_parts();
            permit.record_success();
            Ok((
                stream,
                response_parts.headers,
                bulkhead_guard,
                connect_timings,
            ))
        }
        Ok(Err(tokio_tungstenite::tungstenite::Error::Http(response))) => {
            let error = provider_websocket_error_from_http(*response);
            if (500..=599).contains(&error.status) {
                permit.record_failure();
            } else if error.status == 429 {
                drop(permit);
            } else {
                permit.record_success();
            }
            Err(error)
        }
        Ok(Err(tokio_tungstenite::tungstenite::Error::Url(_))) => {
            drop(permit);
            Err(WebSocketError::new(
                502,
                "upstream_unavailable",
                "provider WebSocket connection failed",
            ))
        }
        Ok(Err(_)) => {
            permit.record_failure();
            Err(WebSocketError::new(
                502,
                "upstream_unavailable",
                "provider WebSocket connection failed",
            ))
        }
        Err(_) => {
            permit.record_failure();
            Err(WebSocketError::new(
                504,
                "upstream_unavailable",
                "provider WebSocket connection timed out",
            ))
        }
    }
}

type UpstreamWsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn spawn_responses_provider_reader(
    connection_id: u64,
    mut stream: futures_util::stream::SplitStream<UpstreamWsStream>,
    events: tokio::sync::mpsc::Sender<WsProviderEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let reason = loop {
            let event = match stream.next().await {
                Some(Ok(UpstreamMessage::Text(frame))) => WsProviderEvent::Frame {
                    connection_id,
                    bytes: frame.into(),
                    text: true,
                },
                Some(Ok(UpstreamMessage::Binary(frame))) => WsProviderEvent::Frame {
                    connection_id,
                    bytes: frame,
                    text: false,
                },
                Some(Ok(UpstreamMessage::Ping(frame))) => WsProviderEvent::Ping {
                    connection_id,
                    bytes: frame,
                },
                Some(Ok(UpstreamMessage::Pong(_))) | Some(Ok(UpstreamMessage::Frame(_))) => {
                    continue;
                }
                Some(Ok(UpstreamMessage::Close(_))) | None => {
                    break "provider closed".to_owned();
                }
                Some(Err(error)) => break format!("provider read failed: {error}"),
            };
            if events.send(event).await.is_err() {
                return;
            }
        };
        let _ = events
            .send(WsProviderEvent::Closed {
                connection_id,
                reason,
            })
            .await;
    })
}

pub(super) fn responses_websocket_config(message_cap: usize) -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(message_cap);
    config.max_frame_size = Some(message_cap);
    config
}

pub(super) fn responses_websocket_request(
    upstream_url: url::Url,
    mut upstream_headers: HeaderMap,
) -> Result<http::Request<()>, String> {
    let mut request = upstream_url
        .as_str()
        .into_client_request()
        .map_err(|error| format!("upstream WebSocket request could not be built: {error}"))?;
    cc_lb_engine::strip_hop_by_hop(&mut upstream_headers);
    for (name, value) in &upstream_headers {
        if name == HOST
            || name == CONTENT_LENGTH
            || name == CONNECTION
            || name == UPGRADE
            || name == SEC_WEBSOCKET_KEY
            || name == SEC_WEBSOCKET_PROTOCOL
            || name == SEC_WEBSOCKET_VERSION
            || name.as_str() == "sec-websocket-extensions"
        {
            continue;
        }
        request.headers_mut().append(name.clone(), value.clone());
    }
    Ok(request)
}

pub(super) fn websocket_error_message(error: &WebSocketError) -> Message {
    Message::Text(
        error
            .to_bytes()
            .try_into()
            .expect("serialized WebSocket errors are UTF-8"),
    )
}

async fn write_dispatched_frame(
    gateway: &mut ResponsesWebSocketGateway,
    dispatch: WebSocketDispatch,
    connection_id: u64,
    connections: &mut HashMap<u64, WsProviderConnection>,
    dispatch_timings: cc_lb_engine::responses_websocket::ResponsesDispatchTimings,
) -> (Vec<WebSocketDispatch>, Vec<WebSocketError>) {
    let Some(connection) = connections.get_mut(&connection_id) else {
        let result = gateway.connection_closed(connection_id, "provider connection is unavailable");
        return (result.dispatches, result.errors);
    };
    let frame = match gateway.frame_for_send(&dispatch) {
        Ok(frame) => match String::from_utf8(frame.to_vec()) {
            Ok(frame) => UpstreamMessage::Text(frame.into()),
            Err(_) => {
                let error = WebSocketError::new(
                    502,
                    "response_transform_error",
                    "Shape plugin returned a non-UTF-8 WebSocket frame",
                )
                .with_stream(dispatch.stream_id.clone());
                let ready = gateway.fail_dispatch(dispatch.turn_id, &error).await;
                return (ready, vec![error]);
            }
        },
        Err(error) => {
            let ready = gateway.fail_dispatch(dispatch.turn_id, &error).await;
            return (ready, vec![error]);
        }
    };
    let send_started = std::time::Instant::now();
    // After the first write is attempted, failure may have reached the
    // provider. Never retry or replay it on a different connection.
    if connection.sink.send(frame).await.is_err() {
        if let Some(mut connection) = connections.remove(&connection_id)
            && let Some(task) = connection.task.take()
        {
            task.abort();
        }
        let result = gateway.connection_closed(connection_id, "provider write failed");
        return (result.dispatches, result.errors);
    }
    (
        gateway.sent_with_timings(dispatch.turn_id, dispatch_timings, send_started),
        Vec::new(),
    )
}

async fn commit_and_send_responses_dispatch(
    gateway: &mut ResponsesWebSocketGateway,
    dispatch: WebSocketDispatch,
    connection_id: u64,
    connections: &mut HashMap<u64, WsProviderConnection>,
    dispatch_timings: cc_lb_engine::responses_websocket::ResponsesDispatchTimings,
) -> (Vec<WebSocketDispatch>, Vec<WebSocketError>) {
    let Some(connection) = connections.get(&connection_id) else {
        let result = gateway.connection_closed(connection_id, "provider connection is unavailable");
        return (result.dispatches, result.errors);
    };
    gateway.assign_connection(
        dispatch.turn_id,
        connection_id,
        connection.upstream_id,
        &connection.upstream_name,
    );
    gateway.record_dispatch_timings(dispatch.turn_id, dispatch_timings);
    gateway.commit_send_attempt(dispatch.turn_id);
    write_dispatched_frame(
        gateway,
        dispatch,
        connection_id,
        connections,
        dispatch_timings,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn fail_dispatch_with_recovery_budget(
    gateway: &mut ResponsesWebSocketGateway,
    turn_id: u64,
    failed_upstream_id: Option<uuid::Uuid>,
    error: &WebSocketError,
    connections: &HashMap<u64, WsProviderConnection>,
    pending_connects: &HashMap<u64, PendingConnect>,
    queued_dispatches: &VecDeque<WebSocketDispatch>,
    deferred_for_session_owner: &VecDeque<PendingDispatchState>,
    carried_budgets: &mut HashMap<u64, CarriedConnectBudget>,
    budget: CarriedConnectBudget,
) -> (Vec<WebSocketDispatch>, Vec<WebSocketError>) {
    let mut pool = ws_relay_pool_snapshot(
        connections,
        pending_connects,
        queued_dispatches,
        deferred_for_session_owner,
    );
    pool.excluded_upstreams = budget.attempted_upstreams.clone();
    pool.dial_attempts_exhausted = budget.attempts >= budget.max_attempts;
    pool.connect_budget_expired = tokio::time::Instant::now() >= budget.deadline;
    let (ready, emitted_error) = gateway
        .fail_dispatch_with_recovery_and_pool(turn_id, failed_upstream_id, error, &pool)
        .await;
    if emitted_error.is_none() && ready.iter().any(|next| next.turn_id == turn_id) {
        carried_budgets.insert(turn_id, budget);
    }
    (ready, emitted_error.into_iter().collect())
}

#[allow(clippy::too_many_arguments)]
async fn send_responses_dispatch(
    gateway: &mut ResponsesWebSocketGateway,
    mut dispatch: WebSocketDispatch,
    connection_id: u64,
    connections: &mut HashMap<u64, WsProviderConnection>,
    pending_connects: &HashMap<u64, PendingConnect>,
    queued_dispatches: &VecDeque<WebSocketDispatch>,
    deferred_for_session_owner: &VecDeque<PendingDispatchState>,
    carried_budgets: &mut HashMap<u64, CarriedConnectBudget>,
    attempts: usize,
    max_attempts: usize,
    attempted_upstreams: &std::collections::HashSet<uuid::Uuid>,
    dispatch_timings: cc_lb_engine::responses_websocket::ResponsesDispatchTimings,
    deadline: tokio::time::Instant,
    drain_controller: &DrainController,
) -> (Vec<WebSocketDispatch>, Vec<WebSocketError>) {
    let Some(connection) = connections.get_mut(&connection_id) else {
        let mut result =
            gateway.connection_closed(connection_id, "provider connection is unavailable");
        if dispatch.control && result.errors.is_empty() {
            result.errors.push(
                WebSocketError::new(
                    400,
                    "response_not_found",
                    "The interrupt target connection is no longer available",
                )
                .with_stream(dispatch.stream_id),
            );
        }
        return (result.dispatches, result.errors);
    };
    if dispatch.control {
        if !gateway.owns_connection(connection_id, connection.upstream_id) {
            return (
                Vec::new(),
                vec![
                    WebSocketError::new(
                        400,
                        "response_not_found",
                        "The interrupt target connection is no longer available",
                    )
                    .with_stream(dispatch.stream_id),
                ],
            );
        }
        let frame = match gateway.frame_for_send(&dispatch) {
            Ok(frame) => match String::from_utf8(frame.to_vec()) {
                Ok(frame) => UpstreamMessage::Text(frame.into()),
                Err(_) => {
                    return (
                        Vec::new(),
                        vec![
                            WebSocketError::new(
                                400,
                                "invalid_request_error",
                                "WebSocket control message must be valid UTF-8",
                            )
                            .with_stream(dispatch.stream_id),
                        ],
                    );
                }
            },
            Err(error) => return (Vec::new(), vec![error]),
        };
        if connection.sink.send(frame).await.is_err() {
            if let Some(mut connection) = connections.remove(&connection_id)
                && let Some(task) = connection.task.take()
            {
                task.abort();
            }
            let mut result = gateway.connection_closed(connection_id, "provider write failed");
            if result.errors.is_empty() {
                result.errors.push(
                    WebSocketError::new(502, "upstream_connection_closed", "provider write failed")
                        .with_stream(dispatch.stream_id),
                );
            }
            return (result.dispatches, result.errors);
        }
        return (Vec::new(), Vec::new());
    }
    let upstream_id = connection.upstream_id;
    if let Err(error) = gateway.finalize_dispatch_reasoning(&mut dispatch, true) {
        let error = error.with_stream(dispatch.stream_id.clone());
        let ready = gateway.fail_dispatch(dispatch.turn_id, &error).await;
        return (ready, vec![error]);
    }
    let validation = tokio::select! {
        biased;
        _ = drain_controller.wait_for_draining(), if !dispatch.control => {
            return finish_draining_dispatch(gateway, dispatch).await;
        }
        result = tokio::time::timeout_at(
            deadline,
            gateway.validate_candidate_for_send(
                dispatch.turn_id,
                upstream_id,
                Some(connection_id),
                false,
            ),
        ) => result,
    };
    let validation_error = match validation {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(_) => Some(
            WebSocketError::new(
                504,
                "upstream_unavailable",
                "provider WebSocket connection timed out",
            )
            .with_stream(dispatch.stream_id.clone()),
        ),
    };
    if let Some(error) = validation_error {
        let mut attempted = attempted_upstreams.clone();
        attempted.insert(upstream_id);
        return fail_dispatch_with_recovery_budget(
            gateway,
            dispatch.turn_id,
            Some(upstream_id),
            &error,
            connections,
            pending_connects,
            queued_dispatches,
            deferred_for_session_owner,
            carried_budgets,
            CarriedConnectBudget {
                attempts,
                max_attempts,
                deadline,
                attempted_upstreams: attempted,
            },
        )
        .await;
    }
    if drain_controller.is_draining() {
        return finish_draining_dispatch(gateway, dispatch).await;
    }
    commit_and_send_responses_dispatch(
        gateway,
        dispatch,
        connection_id,
        connections,
        dispatch_timings,
    )
    .await
}

fn has_remaining_ws_candidates(
    pending: &VecDeque<cc_lb_engine::responses_websocket::WebSocketCandidate>,
    connections: &HashMap<u64, WsProviderConnection>,
    stream_id: Option<&str>,
    attempts: usize,
    max_attempts: usize,
) -> bool {
    !pending.is_empty()
        && (attempts < max_attempts
            || pending.iter().any(|next| {
                connections.values().any(|conn| {
                    conn.upstream_id == next.upstream_id
                        && (!next.socket_per_lane || conn.lane.as_deref() == stream_id)
                })
            }))
}

const MAX_FALLBACK_DEADLINE: Duration = Duration::from_secs(365 * 24 * 60 * 60);

fn bounded_deadline(now: tokio::time::Instant, duration: Duration) -> tokio::time::Instant {
    now.checked_add(duration)
        .or_else(|| now.checked_add(MAX_FALLBACK_DEADLINE))
        .unwrap_or(now)
}

struct CarriedConnectBudget {
    attempts: usize,
    max_attempts: usize,
    deadline: tokio::time::Instant,
    attempted_upstreams: std::collections::HashSet<uuid::Uuid>,
}

struct PendingDispatchState {
    dispatch: WebSocketDispatch,
    remaining: VecDeque<cc_lb_engine::responses_websocket::WebSocketCandidate>,
    attempts: usize,
    max_attempts: usize,
    deadline: tokio::time::Instant,
    per_attempt_timeout: Duration,
    last_error: Option<WebSocketError>,
    last_attempted_upstream_id: Option<uuid::Uuid>,
    attempted_upstreams: std::collections::HashSet<uuid::Uuid>,
    pinned_to_single_owner: bool,
}

impl PendingDispatchState {
    #[cfg(test)]
    fn new(
        dispatch: WebSocketDispatch,
        per_attempt_timeout: Duration,
        connect_budget: Duration,
        max_attempts: usize,
    ) -> Self {
        Self::with_carried_budget(
            dispatch,
            per_attempt_timeout,
            connect_budget,
            max_attempts,
            None,
        )
    }

    fn with_carried_budget(
        dispatch: WebSocketDispatch,
        per_attempt_timeout: Duration,
        connect_budget: Duration,
        max_attempts: usize,
        carried: Option<CarriedConnectBudget>,
    ) -> Self {
        let remaining = dispatch.candidates.iter().cloned().collect();
        let pinned_to_single_owner = dispatch.candidates.len() <= 1;
        let (attempts, max_attempts, deadline, attempted_upstreams) = match carried {
            Some(carried) => (
                carried.attempts,
                carried.max_attempts,
                carried.deadline,
                carried.attempted_upstreams,
            ),
            None => (
                0,
                max_attempts,
                bounded_deadline(tokio::time::Instant::now(), connect_budget),
                std::collections::HashSet::new(),
            ),
        };
        Self {
            dispatch,
            remaining,
            attempts,
            max_attempts,
            deadline,
            per_attempt_timeout,
            last_error: None,
            last_attempted_upstream_id: None,
            attempted_upstreams,
            pinned_to_single_owner,
        }
    }
}

struct PendingConnect {
    primary: PendingDispatchState,
    active_candidate: cc_lb_engine::responses_websocket::WebSocketCandidate,
    active_upstream_id: uuid::Uuid,
    active_socket_per_lane: bool,
    establishes_oauth_owner: bool,
    waiters: Vec<PendingDispatchState>,
}

struct ConnectedAttempt {
    upstream: UpstreamWsStream,
    bulkhead_guard: cc_lb_engine::OwnedBulkheadGuard,
    upstream_id: uuid::Uuid,
    upstream_name: String,
    lane: Option<String>,
    signer: Arc<dyn cc_lb_upstream::Signer>,
    prepare_signer_ms: f64,
    sign_ms: u64,
    dispatch_timings: cc_lb_engine::responses_websocket::ResponsesDispatchTimings,
}

enum ConnectAttemptError {
    Prepare(WebSocketError),
    Dial(WebSocketError),
}

struct ConnectAttemptOutcome {
    connection_id: u64,
    result: Result<ConnectedAttempt, ConnectAttemptError>,
}

fn is_fatal_ws_connect_error(error: &WebSocketError) -> bool {
    matches!(
        error.code,
        "authentication_error" | "reservation_expired" | "response_not_found" | "limit_rejected"
    )
}

fn next_ws_candidate(
    remaining: &mut VecDeque<cc_lb_engine::responses_websocket::WebSocketCandidate>,
    attempts: usize,
    max_attempts: usize,
    prefer_api_key_on_final_attempt: bool,
) -> Option<cc_lb_engine::responses_websocket::WebSocketCandidate> {
    if remaining.is_empty() {
        return None;
    }
    let reserve_api_key =
        max_attempts > 1 && attempts + 1 == max_attempts && prefer_api_key_on_final_attempt;
    if reserve_api_key
        && !remaining[0].is_api_key
        && let Some(pos) = remaining.iter().position(|candidate| candidate.is_api_key)
    {
        remaining.remove(pos)
    } else {
        remaining.pop_front()
    }
}

fn ws_relay_pool_snapshot(
    connections: &HashMap<u64, WsProviderConnection>,
    pending_connects: &HashMap<u64, PendingConnect>,
    queued_dispatches: &VecDeque<WebSocketDispatch>,
    deferred_for_session_owner: &VecDeque<PendingDispatchState>,
) -> cc_lb_engine::responses_websocket::WsRelayPoolSnapshot {
    let mut coalescable_upstreams = std::collections::HashSet::new();
    for entry in pending_connects.values() {
        if !entry.active_socket_per_lane {
            coalescable_upstreams.insert(entry.active_upstream_id);
        }
    }
    let has_existing_or_coalescable =
        |candidate: &cc_lb_engine::responses_websocket::WebSocketCandidate,
         stream_id: &Option<String>| {
            connections.values().any(|conn| {
                conn.upstream_id == candidate.upstream_id
                    && (!candidate.socket_per_lane || &conn.lane == stream_id)
            }) || (!candidate.socket_per_lane
                && coalescable_upstreams.contains(&candidate.upstream_id))
        };
    let mut planned_dials = 0_usize;
    for dispatch in queued_dispatches {
        if dispatch.control || dispatch.connection_id.is_some() {
            continue;
        }
        let Some(candidate) = dispatch.candidates.first() else {
            continue;
        };
        if !has_existing_or_coalescable(candidate, &dispatch.stream_id) {
            planned_dials = planned_dials.saturating_add(1);
        }
    }
    for deferred in deferred_for_session_owner {
        if deferred.dispatch.control
            || deferred.dispatch.connection_id.is_some()
            || deferred.attempts >= deferred.max_attempts
        {
            continue;
        }
        let Some(candidate) = deferred.remaining.front() else {
            continue;
        };
        if !has_existing_or_coalescable(candidate, &deferred.dispatch.stream_id) {
            planned_dials = planned_dials.saturating_add(1);
        }
    }
    cc_lb_engine::responses_websocket::WsRelayPoolSnapshot {
        pending_connects: pending_connects.len().saturating_add(planned_dials),
        coalescable_upstreams,
        ..Default::default()
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish_failed_dispatch(
    gateway: &mut ResponsesWebSocketGateway,
    pending: PendingDispatchState,
    connections: &HashMap<u64, WsProviderConnection>,
    pending_connects: &HashMap<u64, PendingConnect>,
    queued_dispatches: &VecDeque<WebSocketDispatch>,
    deferred_for_session_owner: &VecDeque<PendingDispatchState>,
    carried_budgets: &mut HashMap<u64, CarriedConnectBudget>,
) -> (Vec<WebSocketDispatch>, Vec<WebSocketError>) {
    let turn_id = pending.dispatch.turn_id;
    let last_attempted_upstream_id = pending.last_attempted_upstream_id;
    let budget = CarriedConnectBudget {
        attempts: pending.attempts,
        max_attempts: pending.max_attempts,
        deadline: pending.deadline,
        attempted_upstreams: pending.attempted_upstreams,
    };
    let error = pending
        .last_error
        .unwrap_or_else(|| {
            WebSocketError::new(
                502,
                "upstream_unavailable",
                "No Responses WebSocket upstream is available",
            )
        })
        .with_stream(pending.dispatch.stream_id);
    fail_dispatch_with_recovery_budget(
        gateway,
        turn_id,
        last_attempted_upstream_id,
        &error,
        connections,
        pending_connects,
        queued_dispatches,
        deferred_for_session_owner,
        carried_budgets,
        budget,
    )
    .await
}

async fn finish_draining_dispatch(
    gateway: &mut ResponsesWebSocketGateway,
    dispatch: WebSocketDispatch,
) -> (Vec<WebSocketDispatch>, Vec<WebSocketError>) {
    let error =
        WebSocketError::new(503, "draining", "server is draining").with_stream(dispatch.stream_id);
    let ready = gateway.fail_dispatch(dispatch.turn_id, &error).await;
    (ready, vec![error])
}

#[allow(clippy::too_many_arguments)]
async fn advance_one_dispatch(
    gateway: &mut ResponsesWebSocketGateway,
    mut pending: PendingDispatchState,
    state: &ProxyState,
    connections: &mut HashMap<u64, WsProviderConnection>,
    events: &tokio::sync::mpsc::Sender<WsProviderEvent>,
    next_connection_id: &mut u64,
    connection_capacity: usize,
    pending_connects: &mut HashMap<u64, PendingConnect>,
    connect_tasks: &mut tokio::task::JoinSet<ConnectAttemptOutcome>,
    deferred_for_session_owner: &mut VecDeque<PendingDispatchState>,
    queued_dispatches: &VecDeque<WebSocketDispatch>,
    carried_budgets: &mut HashMap<u64, CarriedConnectBudget>,
) -> (Vec<WebSocketDispatch>, Vec<WebSocketError>) {
    if state.drain_controller.is_draining() && !pending.dispatch.control {
        return finish_draining_dispatch(gateway, pending.dispatch).await;
    }

    if let Some(connection_id) = pending.dispatch.connection_id {
        return send_responses_dispatch(
            gateway,
            pending.dispatch,
            connection_id,
            connections,
            pending_connects,
            queued_dispatches,
            deferred_for_session_owner,
            carried_budgets,
            pending.attempts,
            pending.max_attempts,
            &pending.attempted_upstreams,
            cc_lb_engine::responses_websocket::ResponsesDispatchTimings::reused(),
            pending.deadline,
            &state.drain_controller,
        )
        .await;
    }

    while let Some(candidate) = next_ws_candidate(
        &mut pending.remaining,
        pending.attempts,
        pending.max_attempts,
        pending.dispatch.prefer_api_key_on_final_attempt,
    ) {
        let existing = connections.iter().find_map(|(id, connection)| {
            (connection.upstream_id == candidate.upstream_id
                && (!candidate.socket_per_lane || connection.lane == pending.dispatch.stream_id))
                .then_some(*id)
        });
        if let Some(connection_id) = existing {
            pending.last_attempted_upstream_id = Some(candidate.upstream_id);
            if pending
                .deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .is_zero()
            {
                pending.last_error.get_or_insert_with(|| {
                    WebSocketError::new(
                        504,
                        "upstream_unavailable",
                        "provider WebSocket connection timed out",
                    )
                });
                break;
            }
            let has_remaining_candidates = has_remaining_ws_candidates(
                &pending.remaining,
                connections,
                pending.dispatch.stream_id.as_deref(),
                pending.attempts,
                pending.max_attempts,
            );
            let pinned_to_single_owner = pending.pinned_to_single_owner
                || (!pending.dispatch.has_explicit_affinity
                    && gateway.session_upstream() == Some(candidate.upstream_id));
            if let Err(error) =
                gateway.finalize_dispatch_reasoning(&mut pending.dispatch, pinned_to_single_owner)
            {
                pending.attempted_upstreams.insert(candidate.upstream_id);
                pending.last_error = Some(error.with_stream(pending.dispatch.stream_id.clone()));
                return finish_failed_dispatch(
                    gateway,
                    pending,
                    connections,
                    pending_connects,
                    queued_dispatches,
                    deferred_for_session_owner,
                    carried_budgets,
                )
                .await;
            }
            let validation = tokio::select! {
                biased;
                _ = state.drain_controller.wait_for_draining(), if !pending.dispatch.control => {
                    return finish_draining_dispatch(gateway, pending.dispatch).await;
                }
                result = tokio::time::timeout_at(
                    pending.deadline,
                    gateway.validate_candidate_for_send(
                        pending.dispatch.turn_id,
                        candidate.upstream_id,
                        Some(connection_id),
                        has_remaining_candidates,
                    ),
                ) => result,
            };
            match validation {
                Ok(Ok(())) => {
                    if state.drain_controller.is_draining() {
                        return finish_draining_dispatch(gateway, pending.dispatch).await;
                    }
                    return commit_and_send_responses_dispatch(
                        gateway,
                        pending.dispatch,
                        connection_id,
                        connections,
                        cc_lb_engine::responses_websocket::ResponsesDispatchTimings::reused(),
                    )
                    .await;
                }
                Ok(Err(error)) => {
                    if state.drain_controller.is_draining() {
                        return finish_draining_dispatch(gateway, pending.dispatch).await;
                    }
                    let fatal = is_fatal_ws_connect_error(&error);
                    pending.attempted_upstreams.insert(candidate.upstream_id);
                    pending.last_error = Some(error);
                    if fatal {
                        break;
                    }
                    continue;
                }
                Err(_) => {
                    pending.attempted_upstreams.insert(candidate.upstream_id);
                    pending.last_error = Some(WebSocketError::new(
                        504,
                        "upstream_unavailable",
                        "provider WebSocket connection timed out",
                    ));
                    break;
                }
            }
        }

        // Pin all initial OAuth lanes to the same account: if another lane is already
        // performing the initial unpinned OAuth handshake, wait before dialing or routing
        // a competing OAuth candidate.
        let other_establishes_oauth_owner = gateway.session_upstream().is_none()
            && pending_connects
                .values()
                .any(|entry| entry.establishes_oauth_owner);
        if !candidate.is_api_key && other_establishes_oauth_owner {
            pending.remaining.push_front(candidate);
            deferred_for_session_owner.push_back(pending);
            return (Vec::new(), Vec::new());
        }
        let pinned_to_single_owner = pending.pinned_to_single_owner
            || (!pending.dispatch.has_explicit_affinity
                && gateway.session_upstream() == Some(candidate.upstream_id));
        if let Err(error) =
            gateway.finalize_dispatch_reasoning(&mut pending.dispatch, pinned_to_single_owner)
        {
            pending.last_attempted_upstream_id = Some(candidate.upstream_id);
            pending.attempted_upstreams.insert(candidate.upstream_id);
            pending.last_error = Some(error.with_stream(pending.dispatch.stream_id.clone()));
            return finish_failed_dispatch(
                gateway,
                pending,
                connections,
                pending_connects,
                queued_dispatches,
                deferred_for_session_owner,
                carried_budgets,
            )
            .await;
        }

        // Coalesce concurrent shareable API-key lanes onto an in-flight dial instead of
        // opening duplicate sockets and consuming extra bulkhead permits.
        if !candidate.socket_per_lane
            && let Some(in_flight) = pending_connects.values_mut().find(|entry| {
                entry.active_upstream_id == candidate.upstream_id && !entry.active_socket_per_lane
            })
        {
            in_flight.waiters.push(pending);
            return (Vec::new(), Vec::new());
        }

        if pending.attempts >= pending.max_attempts {
            continue;
        }
        if pending
            .deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .is_zero()
        {
            pending.last_error.get_or_insert_with(|| {
                WebSocketError::new(
                    504,
                    "upstream_unavailable",
                    "provider WebSocket connection timed out",
                )
            });
            break;
        }
        pending.last_attempted_upstream_id = Some(candidate.upstream_id);

        while connections.len() + pending_connects.len() >= connection_capacity {
            let Some(retired_id) = gateway.reclaim_idle_connection_for_failover(
                candidate.upstream_id,
                candidate.socket_per_lane,
                &pending.dispatch.stream_id,
            ) else {
                break;
            };
            connections.remove(&retired_id);
        }
        if connections.len() + pending_connects.len() >= connection_capacity {
            pending.last_error = Some(WebSocketError::new(
                503,
                "overloaded_error",
                "WebSocket provider connection budget is full",
            ));
            continue;
        }
        // Skip open circuit breakers without burning a retry slot or bulkhead/signing work.
        let permit = match acquire_websocket_breaker_permit(state, candidate.upstream_id) {
            Ok(permit) => permit,
            Err(error) => {
                pending.attempted_upstreams.insert(candidate.upstream_id);
                pending.last_error = Some(error);
                continue;
            }
        };
        // Recheck fresh quota and routing eligibility before burning an attempt
        // slot or acquiring the bulkhead so an unpinned fallback whose quota just
        // exhausted skips cleanly to the next eligible upstream.
        let fallback_validation = tokio::select! {
            biased;
            _ = state.drain_controller.wait_for_draining(), if !pending.dispatch.control => {
                drop(permit);
                return finish_draining_dispatch(gateway, pending.dispatch).await;
            }
            result = tokio::time::timeout_at(
                pending.deadline,
                gateway.validate_fallback_candidate(&pending.dispatch, &candidate),
            ) => result,
        };
        match fallback_validation {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(permit);
                let fatal = is_fatal_ws_connect_error(&error);
                pending.attempted_upstreams.insert(candidate.upstream_id);
                pending.last_error = Some(error);
                if fatal {
                    break;
                }
                continue;
            }
            Err(_) => {
                drop(permit);
                pending.attempted_upstreams.insert(candidate.upstream_id);
                pending.last_error = Some(WebSocketError::new(
                    504,
                    "upstream_unavailable",
                    "provider WebSocket connection timed out",
                ));
                break;
            }
        }
        pending.attempts += 1;
        pending.attempted_upstreams.insert(candidate.upstream_id);
        let attempt_deadline =
            bounded_deadline(tokio::time::Instant::now(), pending.per_attempt_timeout)
                .min(pending.deadline);
        let prepare_job = match gateway.begin_connection_prepare(&pending.dispatch, &candidate) {
            Ok(job) => job,
            Err(error) => {
                drop(permit);
                let fatal = is_fatal_ws_connect_error(&error);
                pending.last_error = Some(error);
                if fatal {
                    break;
                }
                continue;
            }
        };
        let Some(next_id) = next_connection_id.checked_add(1) else {
            drop(permit);
            pending.last_error = Some(WebSocketError::new(
                503,
                "overloaded_error",
                "WebSocket connection identifiers are exhausted",
            ));
            break;
        };
        if state.drain_controller.is_draining() && !pending.dispatch.control {
            drop(permit);
            return finish_draining_dispatch(gateway, pending.dispatch).await;
        }
        let connection_id = *next_connection_id;
        *next_connection_id = next_id;

        let upstream_id = candidate.upstream_id;
        let upstream_name = candidate.upstream_name.clone();
        let lane = pending.dispatch.stream_id.clone();
        let establishes_oauth_owner = gateway.session_upstream().is_none() && !candidate.is_api_key;
        let active_socket_per_lane = candidate.socket_per_lane;
        pending_connects.insert(
            connection_id,
            PendingConnect {
                primary: pending,
                active_candidate: candidate,
                active_upstream_id: upstream_id,
                active_socket_per_lane,
                establishes_oauth_owner,
                waiters: Vec::new(),
            },
        );
        let state = state.clone();
        let _events_keepalive = events;
        connect_tasks.spawn(async move {
            if state.websocket_bulkhead_config.semaphore_permits == 0 {
                drop(permit);
                return ConnectAttemptOutcome {
                    connection_id,
                    result: Err(ConnectAttemptError::Dial(WebSocketError::new(
                        503,
                        "overloaded_error",
                        "WebSocket connection budget is full",
                    ))),
                };
            }
            let bulkhead = state.websocket_bulkheads.semaphore_only_bulkhead(
                websocket_breaker_key(upstream_id),
                state.websocket_bulkhead_config,
            );
            let bulkhead_started = std::time::Instant::now();
            let bulkhead_guard =
                match tokio::time::timeout_at(attempt_deadline, bulkhead.acquire_owned()).await {
                    Ok(Ok(guard)) => guard,
                    Ok(Err(error)) => {
                        drop(permit);
                        return ConnectAttemptOutcome {
                            connection_id,
                            result: Err(ConnectAttemptError::Dial(WebSocketError::new(
                                503,
                                "overloaded_error",
                                error.to_string(),
                            ))),
                        };
                    }
                    Err(_) => {
                        drop(permit);
                        return ConnectAttemptOutcome {
                            connection_id,
                            result: Err(ConnectAttemptError::Dial(WebSocketError::new(
                                504,
                                "upstream_unavailable",
                                "provider WebSocket connection timed out",
                            ))),
                        };
                    }
                };
            let bulkhead_wait_ms = bulkhead_started
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64;

            // External auth / quota checks and OAuth token refresh run off the relay
            // loop inside the bounded connect task.
            let signed = match tokio::time::timeout_at(attempt_deadline, prepare_job.run()).await {
                Ok(Ok(signed)) => signed,
                Ok(Err(error)) => {
                    drop(bulkhead_guard);
                    drop(permit);
                    return ConnectAttemptOutcome {
                        connection_id,
                        result: Err(ConnectAttemptError::Prepare(error)),
                    };
                }
                Err(_) => {
                    drop(bulkhead_guard);
                    drop(permit);
                    return ConnectAttemptOutcome {
                        connection_id,
                        result: Err(ConnectAttemptError::Prepare(WebSocketError::new(
                            504,
                            "upstream_unavailable",
                            "provider WebSocket connection timed out",
                        ))),
                    };
                }
            };
            let request = match responses_websocket_request(signed.url, signed.headers) {
                Ok(request) => request,
                Err(error) => {
                    drop(bulkhead_guard);
                    drop(permit);
                    return ConnectAttemptOutcome {
                        connection_id,
                        result: Err(ConnectAttemptError::Dial(WebSocketError::new(
                            502,
                            "upstream_unavailable",
                            error,
                        ))),
                    };
                }
            };
            let remaining_attempt =
                attempt_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining_attempt.is_zero() {
                drop(bulkhead_guard);
                drop(permit);
                return ConnectAttemptOutcome {
                    connection_id,
                    result: Err(ConnectAttemptError::Dial(WebSocketError::new(
                        504,
                        "upstream_unavailable",
                        "provider WebSocket connection timed out",
                    ))),
                };
            }
            let result = connect_provider_websocket_with_permit(
                permit,
                state.body_caps.messages,
                state.dns_resolver.clone(),
                request,
                bulkhead_guard,
                remaining_attempt.min(state.upstream_connect_timeout),
            )
            .await
            .map_err(ConnectAttemptError::Dial)
            .map(
                |(upstream, _headers, bulkhead_guard, connect_timings)| ConnectedAttempt {
                    upstream,
                    bulkhead_guard,
                    upstream_id,
                    upstream_name,
                    lane,
                    signer: signed.signer,
                    prepare_signer_ms: signed.prepare_signer_ms,
                    sign_ms: signed.sign_ms,
                    dispatch_timings: cc_lb_engine::responses_websocket::ResponsesDispatchTimings {
                        connection_reused: Some(false),
                        bulkhead_wait_ms: Some(bulkhead_wait_ms),
                        dns_ms: connect_timings.dns_ms,
                        connect_ms: connect_timings.connect_ms,
                    },
                },
            );
            ConnectAttemptOutcome {
                connection_id,
                result,
            }
        });
        return (Vec::new(), Vec::new());
    }

    finish_failed_dispatch(
        gateway,
        pending,
        connections,
        pending_connects,
        queued_dispatches,
        deferred_for_session_owner,
        carried_budgets,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_connect_outcome(
    gateway: &mut ResponsesWebSocketGateway,
    outcome: ConnectAttemptOutcome,
    state: &ProxyState,
    connections: &mut HashMap<u64, WsProviderConnection>,
    events: &tokio::sync::mpsc::Sender<WsProviderEvent>,
    next_connection_id: &mut u64,
    connection_capacity: usize,
    pending_connects: &mut HashMap<u64, PendingConnect>,
    connect_tasks: &mut tokio::task::JoinSet<ConnectAttemptOutcome>,
    deferred_for_session_owner: &mut VecDeque<PendingDispatchState>,
    dispatches: &mut VecDeque<WebSocketDispatch>,
    carried_budgets: &mut HashMap<u64, CarriedConnectBudget>,
) -> Vec<WebSocketError> {
    let Some(pending) = pending_connects.remove(&outcome.connection_id) else {
        return Vec::new();
    };
    let mut errors = Vec::new();
    let mut resumed = VecDeque::new();

    match outcome.result {
        Ok(attempt) => {
            let upstream_id = attempt.upstream_id;
            let mut pending_attempt = Some(attempt);
            let mut candidates = Vec::with_capacity(1 + pending.waiters.len());
            candidates.push(pending.primary);
            candidates.extend(pending.waiters);

            if state.drain_controller.is_draining() {
                for entry in candidates {
                    let (ready, errs) = finish_draining_dispatch(gateway, entry.dispatch).await;
                    dispatches.extend(ready);
                    errors.extend(errs);
                }
                return errors;
            }

            for mut entry in candidates {
                if state.drain_controller.is_draining() {
                    let (ready, errs) = finish_draining_dispatch(gateway, entry.dispatch).await;
                    dispatches.extend(ready);
                    errors.extend(errs);
                    continue;
                }
                let now = tokio::time::Instant::now();
                if now >= entry.deadline {
                    entry.last_error = Some(WebSocketError::new(
                        504,
                        "upstream_unavailable",
                        "provider WebSocket connection timed out",
                    ));
                    let (ready, errs) = finish_failed_dispatch(
                        gateway,
                        entry,
                        connections,
                        pending_connects,
                        dispatches,
                        deferred_for_session_owner,
                        carried_budgets,
                    )
                    .await;
                    dispatches.extend(ready);
                    errors.extend(errs);
                    continue;
                }
                let adopted = pending_attempt.is_none();
                if adopted && !connections.contains_key(&outcome.connection_id) {
                    entry.last_error = Some(WebSocketError::new(
                        502,
                        "upstream_connection_closed",
                        "provider write failed",
                    ));
                    resumed.push_back(entry);
                    continue;
                }
                let existing_conn = adopted.then_some(outcome.connection_id);
                let has_remaining_candidates = has_remaining_ws_candidates(
                    &entry.remaining,
                    connections,
                    entry.dispatch.stream_id.as_deref(),
                    entry.attempts,
                    entry.max_attempts,
                );
                let pinned_to_single_owner = entry.pinned_to_single_owner
                    || (!entry.dispatch.has_explicit_affinity
                        && gateway.session_upstream() == Some(upstream_id));
                if let Err(error) =
                    gateway.finalize_dispatch_reasoning(&mut entry.dispatch, pinned_to_single_owner)
                {
                    entry.last_attempted_upstream_id = Some(upstream_id);
                    entry.attempted_upstreams.insert(upstream_id);
                    entry.last_error = Some(error.with_stream(entry.dispatch.stream_id.clone()));
                    let (ready, errs) = finish_failed_dispatch(
                        gateway,
                        entry,
                        connections,
                        pending_connects,
                        dispatches,
                        deferred_for_session_owner,
                        carried_budgets,
                    )
                    .await;
                    dispatches.extend(ready);
                    errors.extend(errs);
                    continue;
                }
                let validation = tokio::select! {
                    biased;
                    _ = state.drain_controller.wait_for_draining(), if !entry.dispatch.control => {
                        let (ready, errs) = finish_draining_dispatch(gateway, entry.dispatch).await;
                        dispatches.extend(ready);
                        errors.extend(errs);
                        continue;
                    }
                    result = tokio::time::timeout_at(
                        entry.deadline,
                        gateway.validate_candidate_for_send(
                            entry.dispatch.turn_id,
                            upstream_id,
                            existing_conn,
                            has_remaining_candidates,
                        ),
                    ) => result,
                };
                match validation {
                    Ok(Ok(())) => {
                        if state.drain_controller.is_draining() {
                            let (ready, errs) =
                                finish_draining_dispatch(gateway, entry.dispatch).await;
                            dispatches.extend(ready);
                            errors.extend(errs);
                            continue;
                        }
                        let timings = if let Some(attempt) = pending_attempt.take() {
                            // Register ownership BEFORE spawning the reader so no early provider
                            // frame (such as `codex.rate_limits`) can arrive ahead of registration.
                            gateway.record_prepared_signer(
                                entry.dispatch.turn_id,
                                attempt.upstream_id,
                                attempt.signer,
                                attempt.prepare_signer_ms,
                                attempt.sign_ms,
                            );
                            let (sink, stream) = attempt.upstream.split();
                            connections.insert(
                                outcome.connection_id,
                                WsProviderConnection {
                                    upstream_id: attempt.upstream_id,
                                    upstream_name: attempt.upstream_name,
                                    lane: attempt.lane,
                                    sink,
                                    task: None,
                                    _bulkhead_guard: attempt.bulkhead_guard,
                                },
                            );
                            let reader_task = spawn_responses_provider_reader(
                                outcome.connection_id,
                                stream,
                                events.clone(),
                            );
                            connections
                                .get_mut(&outcome.connection_id)
                                .expect("registered connection exists")
                                .task = Some(reader_task);
                            attempt.dispatch_timings
                        } else {
                            cc_lb_engine::responses_websocket::ResponsesDispatchTimings::reused()
                        };
                        let (ready, errs) = commit_and_send_responses_dispatch(
                            gateway,
                            entry.dispatch,
                            outcome.connection_id,
                            connections,
                            timings,
                        )
                        .await;
                        dispatches.extend(ready);
                        errors.extend(errs);
                    }
                    Ok(Err(error)) => {
                        if state.drain_controller.is_draining() {
                            let (ready, errs) =
                                finish_draining_dispatch(gateway, entry.dispatch).await;
                            dispatches.extend(ready);
                            errors.extend(errs);
                            continue;
                        }
                        let fatal = is_fatal_ws_connect_error(&error);
                        entry.last_attempted_upstream_id = Some(upstream_id);
                        entry.attempted_upstreams.insert(upstream_id);
                        entry.last_error = Some(error);
                        if fatal {
                            let (ready, errs) = finish_failed_dispatch(
                                gateway,
                                entry,
                                connections,
                                pending_connects,
                                dispatches,
                                deferred_for_session_owner,
                                carried_budgets,
                            )
                            .await;
                            dispatches.extend(ready);
                            errors.extend(errs);
                        } else {
                            resumed.push_back(entry);
                        }
                    }
                    Err(_) => {
                        entry.last_attempted_upstream_id = Some(upstream_id);
                        entry.attempted_upstreams.insert(upstream_id);
                        entry.last_error = Some(WebSocketError::new(
                            504,
                            "upstream_unavailable",
                            "provider WebSocket connection timed out",
                        ));
                        let (ready, errs) = finish_failed_dispatch(
                            gateway,
                            entry,
                            connections,
                            pending_connects,
                            dispatches,
                            deferred_for_session_owner,
                            carried_budgets,
                        )
                        .await;
                        dispatches.extend(ready);
                        errors.extend(errs);
                    }
                }
            }
        }
        Err(ConnectAttemptError::Prepare(error)) => {
            if state.drain_controller.is_draining() {
                for entry in std::iter::once(pending.primary).chain(pending.waiters) {
                    let (ready, errs) = finish_draining_dispatch(gateway, entry.dispatch).await;
                    dispatches.extend(ready);
                    errors.extend(errs);
                }
                return errors;
            }
            let now = tokio::time::Instant::now();
            let mut primary = pending.primary;
            primary.last_error = Some(error.clone());
            primary.last_attempted_upstream_id = Some(pending.active_upstream_id);
            primary
                .attempted_upstreams
                .insert(pending.active_upstream_id);
            if is_fatal_ws_connect_error(&error) || now >= primary.deadline {
                let (ready, errs) = finish_failed_dispatch(
                    gateway,
                    primary,
                    connections,
                    pending_connects,
                    dispatches,
                    deferred_for_session_owner,
                    carried_budgets,
                )
                .await;
                dispatches.extend(ready);
                errors.extend(errs);
            } else {
                resumed.push_back(primary);
            }
            // Waiters must not inherit a turn-specific preparation failure from the
            // primary; restore the coalesced candidate so each waiter revalidates it.
            for mut entry in pending.waiters {
                if now >= entry.deadline {
                    entry.last_error.get_or_insert_with(|| {
                        WebSocketError::new(
                            504,
                            "upstream_unavailable",
                            "provider WebSocket connection timed out",
                        )
                    });
                    let (ready, errs) = finish_failed_dispatch(
                        gateway,
                        entry,
                        connections,
                        pending_connects,
                        dispatches,
                        deferred_for_session_owner,
                        carried_budgets,
                    )
                    .await;
                    dispatches.extend(ready);
                    errors.extend(errs);
                } else {
                    entry.remaining.push_front(pending.active_candidate.clone());
                    resumed.push_back(entry);
                }
            }
        }
        Err(ConnectAttemptError::Dial(error)) => {
            if error.status == 429 {
                let mut headers = http::HeaderMap::new();
                error.copy_provider_headers(&mut headers);
                let retry_after_secs = headers
                    .get(http::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|raw| raw.trim().parse::<u64>().ok());
                gateway.record_session_rate_limited_with_retry_after(
                    pending.active_upstream_id,
                    retry_after_secs,
                );
                if !headers.is_empty() {
                    state.lifecycle.ingest_subscription_quota_headers(
                        &headers,
                        pending.active_upstream_id,
                        state.clock.now(),
                    );
                }
            }
            if state.drain_controller.is_draining() {
                for entry in std::iter::once(pending.primary).chain(pending.waiters) {
                    let (ready, errs) = finish_draining_dispatch(gateway, entry.dispatch).await;
                    dispatches.extend(ready);
                    errors.extend(errs);
                }
                return errors;
            }
            let now = tokio::time::Instant::now();
            for mut entry in std::iter::once(pending.primary).chain(pending.waiters) {
                entry.last_error = Some(error.clone());
                entry.last_attempted_upstream_id = Some(pending.active_upstream_id);
                entry.attempted_upstreams.insert(pending.active_upstream_id);
                if is_fatal_ws_connect_error(&error) || now >= entry.deadline {
                    let (ready, errs) = finish_failed_dispatch(
                        gateway,
                        entry,
                        connections,
                        pending_connects,
                        dispatches,
                        deferred_for_session_owner,
                        carried_budgets,
                    )
                    .await;
                    dispatches.extend(ready);
                    errors.extend(errs);
                } else {
                    resumed.push_back(entry);
                }
            }
        }
    }

    if let Some(owner_id) = gateway.session_upstream() {
        while let Some(mut deferred) = deferred_for_session_owner.pop_front() {
            if tokio::time::Instant::now() >= deferred.deadline {
                deferred.last_error.get_or_insert_with(|| {
                    WebSocketError::new(
                        504,
                        "upstream_unavailable",
                        "provider WebSocket connection timed out",
                    )
                });
                let (ready, errs) = finish_failed_dispatch(
                    gateway,
                    deferred,
                    connections,
                    pending_connects,
                    dispatches,
                    deferred_for_session_owner,
                    carried_budgets,
                )
                .await;
                dispatches.extend(ready);
                errors.extend(errs);
                continue;
            }
            if !deferred.dispatch.has_explicit_affinity {
                deferred
                    .remaining
                    .retain(|candidate| candidate.upstream_id == owner_id);
                if deferred.remaining.is_empty() {
                    deferred.last_error = Some(WebSocketError::new(
                        409,
                        "upstream_affinity_unavailable",
                        "The session is pinned to another OAuth account",
                    ));
                    let (ready, errs) = finish_failed_dispatch(
                        gateway,
                        deferred,
                        connections,
                        pending_connects,
                        dispatches,
                        deferred_for_session_owner,
                        carried_budgets,
                    )
                    .await;
                    dispatches.extend(ready);
                    errors.extend(errs);
                    continue;
                }
                deferred.pinned_to_single_owner = true;
            }
            resumed.push_back(deferred);
        }
    } else if !pending_connects
        .values()
        .any(|entry| entry.establishes_oauth_owner)
    {
        while let Some(deferred) = deferred_for_session_owner.pop_front() {
            resumed.push_back(deferred);
        }
    }

    while let Some(state_item) = resumed.pop_front() {
        let (ready, errs) = advance_one_dispatch(
            gateway,
            state_item,
            state,
            connections,
            events,
            next_connection_id,
            connection_capacity,
            pending_connects,
            connect_tasks,
            deferred_for_session_owner,
            dispatches,
            carried_budgets,
        )
        .await;
        dispatches.extend(ready);
        errors.extend(errs);
    }

    errors
}

#[allow(clippy::too_many_arguments)]
async fn expire_deferred_for_session_owner(
    gateway: &mut ResponsesWebSocketGateway,
    deferred_for_session_owner: &mut VecDeque<PendingDispatchState>,
    connections: &HashMap<u64, WsProviderConnection>,
    pending_connects: &HashMap<u64, PendingConnect>,
    dispatches: &mut VecDeque<WebSocketDispatch>,
    carried_budgets: &mut HashMap<u64, CarriedConnectBudget>,
) -> Vec<WebSocketError> {
    let mut errors = Vec::new();
    let now = tokio::time::Instant::now();
    let mut keep = VecDeque::with_capacity(deferred_for_session_owner.len());
    while let Some(mut entry) = deferred_for_session_owner.pop_front() {
        if entry.deadline <= now {
            entry.last_error.get_or_insert_with(|| {
                WebSocketError::new(
                    504,
                    "upstream_unavailable",
                    "provider WebSocket connection timed out",
                )
            });
            let (ready, errs) = finish_failed_dispatch(
                gateway,
                entry,
                connections,
                pending_connects,
                dispatches,
                deferred_for_session_owner,
                carried_budgets,
            )
            .await;
            dispatches.extend(ready);
            errors.extend(errs);
        } else {
            keep.push_back(entry);
        }
    }
    *deferred_for_session_owner = keep;
    errors
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn dispatch_responses_websocket_with_limits(
    gateway: &mut ResponsesWebSocketGateway,
    dispatch: WebSocketDispatch,
    state: &ProxyState,
    connections: &mut HashMap<u64, WsProviderConnection>,
    events: &tokio::sync::mpsc::Sender<WsProviderEvent>,
    next_connection_id: &mut u64,
    connection_capacity: usize,
    per_attempt_timeout: Duration,
    connect_budget: Duration,
    max_attempts: usize,
) -> (Vec<WebSocketDispatch>, Vec<WebSocketError>) {
    let mut pending_connects: HashMap<u64, PendingConnect> = HashMap::new();
    let mut connect_tasks: tokio::task::JoinSet<ConnectAttemptOutcome> =
        tokio::task::JoinSet::new();
    let mut deferred_for_session_owner: VecDeque<PendingDispatchState> = VecDeque::new();
    let mut ready_dispatches = VecDeque::new();
    let empty_dispatches = VecDeque::new();
    let mut carried_budgets = HashMap::new();
    let (ready, mut errors) = advance_one_dispatch(
        gateway,
        PendingDispatchState::new(dispatch, per_attempt_timeout, connect_budget, max_attempts),
        state,
        connections,
        events,
        next_connection_id,
        connection_capacity,
        &mut pending_connects,
        &mut connect_tasks,
        &mut deferred_for_session_owner,
        &empty_dispatches,
        &mut carried_budgets,
    )
    .await;
    ready_dispatches.extend(ready);
    while !pending_connects.is_empty()
        || !connect_tasks.is_empty()
        || !deferred_for_session_owner.is_empty()
    {
        let deferred_deadline = deferred_for_session_owner
            .iter()
            .map(|entry| entry.deadline)
            .min();
        tokio::select! {
            Some(joined) = connect_tasks.join_next(), if !connect_tasks.is_empty() => {
                match joined {
                    Ok(outcome) => {
                        let errs = handle_connect_outcome(
                            gateway,
                            outcome,
                            state,
                            connections,
                            events,
                            next_connection_id,
                            connection_capacity,
                            &mut pending_connects,
                            &mut connect_tasks,
                            &mut deferred_for_session_owner,
                            &mut ready_dispatches,
                            &mut carried_budgets,
                        )
                        .await;
                        errors.extend(errs);
                    }
                    Err(join_error) if join_error.is_cancelled() => {}
                    Err(join_error) => {
                        tracing::error!(%join_error, "responses provider connect task panicked");
                    }
                }
            }
            _ = async {
                if let Some(deadline) = deferred_deadline {
                    tokio::time::sleep_until(deadline).await;
                }
            }, if deferred_deadline.is_some() => {
                let errs = expire_deferred_for_session_owner(
                    gateway,
                    &mut deferred_for_session_owner,
                    connections,
                    &pending_connects,
                    &mut ready_dispatches,
                    &mut carried_budgets,
                )
                .await;
                errors.extend(errs);
            }
        }
    }
    (ready_dispatches.into_iter().collect(), errors)
}

#[allow(clippy::too_many_arguments)]
async fn handle_ws_provider_event(
    provider: WsProviderEvent,
    gateway: &mut ResponsesWebSocketGateway,
    connections: &mut HashMap<u64, WsProviderConnection>,
    pending_connects: &HashMap<u64, PendingConnect>,
    deferred_for_session_owner: &VecDeque<PendingDispatchState>,
    dispatches: &mut VecDeque<WebSocketDispatch>,
    downstream_sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> bool {
    match provider {
        WsProviderEvent::Frame {
            connection_id,
            bytes,
            text,
        } => {
            if !connections.contains_key(&connection_id) {
                return true;
            }
            let pool = ws_relay_pool_snapshot(
                connections,
                pending_connects,
                dispatches,
                deferred_for_session_owner,
            );
            match gateway
                .provider_message_with_pool(connection_id, &bytes, &pool)
                .await
            {
                Ok(result) => {
                    let sending_downstream =
                        result.frames.is_some() || result.forward || !result.errors.is_empty();
                    if sending_downstream {
                        gateway.begin_downstream_send(connection_id);
                    }
                    if let Some(frames) = result.frames {
                        for frame in frames {
                            let message = Message::Text(
                                frame.try_into().expect("transformed event is UTF-8"),
                            );
                            if downstream_sink.send(message).await.is_err() {
                                return false;
                            }
                        }
                    } else if result.forward {
                        let message = if text {
                            Message::Text(bytes.try_into().expect("provider text is UTF-8"))
                        } else {
                            Message::Binary(bytes)
                        };
                        if downstream_sink.send(message).await.is_err() {
                            return false;
                        }
                    }
                    for error in result.errors {
                        if downstream_sink
                            .send(websocket_error_message(&error))
                            .await
                            .is_err()
                        {
                            return false;
                        }
                    }
                    if sending_downstream {
                        gateway.finish_downstream_send(connection_id);
                    }
                    for retire_id in result.retire_connections {
                        connections.remove(&retire_id);
                    }
                    dispatches.extend(result.dispatches);
                }
                Err(error) => {
                    for pending_error in gateway.take_pending_errors() {
                        if downstream_sink
                            .send(websocket_error_message(&pending_error))
                            .await
                            .is_err()
                        {
                            return false;
                        }
                    }
                    let native_connection_error = matches!(
                        error.code,
                        "websocket_connection_limit_reached" | "unattributed_upstream_error"
                    );
                    let affinity_error = error.code == "upstream_affinity_unavailable";
                    if native_connection_error {
                        let message = if text {
                            Message::Text(bytes.try_into().expect("provider text is UTF-8"))
                        } else {
                            Message::Binary(bytes)
                        };
                        if downstream_sink.send(message).await.is_err() {
                            return false;
                        }
                    }
                    if affinity_error
                        && downstream_sink
                            .send(websocket_error_message(&error))
                            .await
                            .is_err()
                    {
                        return false;
                    }
                    connections.remove(&connection_id);
                    let result = gateway.connection_closed(connection_id, &error.message);
                    // A native connection-level error already
                    // represents all affected turns on the wire.
                    if !native_connection_error {
                        if result.errors.is_empty() {
                            if !affinity_error
                                && downstream_sink
                                    .send(websocket_error_message(&error))
                                    .await
                                    .is_err()
                            {
                                return false;
                            }
                        } else {
                            let mut suppress_affinity_duplicate = affinity_error;
                            for closed_error in result.errors {
                                if suppress_affinity_duplicate
                                    && closed_error.stream_id == error.stream_id
                                {
                                    suppress_affinity_duplicate = false;
                                    continue;
                                }
                                if downstream_sink
                                    .send(websocket_error_message(&closed_error))
                                    .await
                                    .is_err()
                                {
                                    return false;
                                }
                            }
                        }
                    }
                    dispatches.extend(result.dispatches);
                }
            }
        }
        WsProviderEvent::Ping {
            connection_id,
            bytes,
        } => {
            if !connections.contains_key(&connection_id) {
                return true;
            }
            gateway.observe_provider_ping(connection_id);
            if let Some(connection) = connections.get_mut(&connection_id)
                && connection
                    .sink
                    .send(UpstreamMessage::Pong(bytes))
                    .await
                    .is_err()
            {
                if let Some(mut connection) = connections.remove(&connection_id)
                    && let Some(task) = connection.task.take()
                {
                    task.abort();
                }
                let result = gateway.connection_closed(connection_id, "provider pong failed");
                for error in result.errors {
                    if downstream_sink
                        .send(websocket_error_message(&error))
                        .await
                        .is_err()
                    {
                        return false;
                    }
                }
                dispatches.extend(result.dispatches);
            }
        }
        WsProviderEvent::Closed {
            connection_id,
            reason,
        } => {
            if connections.remove(&connection_id).is_none() {
                return true;
            }
            let result = gateway.connection_closed(connection_id, &reason);
            for error in result.errors {
                if downstream_sink
                    .send(websocket_error_message(&error))
                    .await
                    .is_err()
                {
                    return false;
                }
            }
            dispatches.extend(result.dispatches);
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn flush_pending_ws_dispatches(
    gateway: &mut ResponsesWebSocketGateway,
    state: &ProxyState,
    connections: &mut HashMap<u64, WsProviderConnection>,
    events: &tokio::sync::mpsc::Sender<WsProviderEvent>,
    next_connection_id: &mut u64,
    capacity: usize,
    pending_connects: &mut HashMap<u64, PendingConnect>,
    connect_tasks: &mut tokio::task::JoinSet<ConnectAttemptOutcome>,
    deferred_for_session_owner: &mut VecDeque<PendingDispatchState>,
    dispatches: &mut VecDeque<WebSocketDispatch>,
    carried_budgets: &mut HashMap<u64, CarriedConnectBudget>,
    downstream_sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> bool {
    while let Some(dispatch) = dispatches.pop_front() {
        let carried = carried_budgets.remove(&dispatch.turn_id);
        let (ready, errors) = advance_one_dispatch(
            gateway,
            PendingDispatchState::with_carried_budget(
                dispatch,
                WS_PROVIDER_CONNECT_TIMEOUT,
                WS_DISPATCH_CONNECT_BUDGET,
                WS_MAX_UPSTREAM_ATTEMPTS,
                carried,
            ),
            state,
            connections,
            events,
            next_connection_id,
            capacity,
            pending_connects,
            connect_tasks,
            deferred_for_session_owner,
            dispatches,
            carried_budgets,
        )
        .await;
        dispatches.extend(ready);
        for error in errors {
            if downstream_sink
                .send(websocket_error_message(&error))
                .await
                .is_err()
            {
                return false;
            }
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn drain_buffered_ws_events_snapshot(
    event_rx: &mut tokio::sync::mpsc::Receiver<WsProviderEvent>,
    gateway: &mut ResponsesWebSocketGateway,
    connections: &mut HashMap<u64, WsProviderConnection>,
    pending_connects: &HashMap<u64, PendingConnect>,
    deferred_for_session_owner: &VecDeque<PendingDispatchState>,
    dispatches: &mut VecDeque<WebSocketDispatch>,
    downstream_sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    max_events: usize,
) -> bool {
    let mut buffered = Vec::with_capacity(max_events.min(event_rx.max_capacity()));
    while buffered.len() < max_events {
        match event_rx.try_recv() {
            Ok(event) => buffered.push(event),
            Err(_) => break,
        }
    }
    for provider in buffered {
        if !handle_ws_provider_event(
            provider,
            gateway,
            connections,
            pending_connects,
            deferred_for_session_owner,
            dispatches,
            downstream_sink,
        )
        .await
        {
            return false;
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn drain_buffered_ws_events_before_expiry_with_pool(
    event_rx: &mut tokio::sync::mpsc::Receiver<WsProviderEvent>,
    gateway: &mut ResponsesWebSocketGateway,
    connections: &mut HashMap<u64, WsProviderConnection>,
    pending_connects: &HashMap<u64, PendingConnect>,
    deferred_for_session_owner: &VecDeque<PendingDispatchState>,
    dispatches: &mut VecDeque<WebSocketDispatch>,
    downstream_sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    now: std::time::Instant,
) -> bool {
    let snapshot_limit = event_rx.len();
    if snapshot_limit > 0
        && !drain_buffered_ws_events_snapshot(
            event_rx,
            gateway,
            connections,
            pending_connects,
            deferred_for_session_owner,
            dispatches,
            downstream_sink,
            snapshot_limit,
        )
        .await
    {
        return false;
    }
    for error in gateway.expire_pending_error_terminals(now) {
        if downstream_sink
            .send(websocket_error_message(&error))
            .await
            .is_err()
        {
            return false;
        }
    }
    true
}

#[cfg(test)]
pub(super) async fn drain_buffered_ws_events_before_expiry(
    event_rx: &mut tokio::sync::mpsc::Receiver<WsProviderEvent>,
    gateway: &mut ResponsesWebSocketGateway,
    connections: &mut HashMap<u64, WsProviderConnection>,
    dispatches: &mut VecDeque<WebSocketDispatch>,
    downstream_sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    now: std::time::Instant,
) -> bool {
    let pending_connects = HashMap::new();
    let deferred_for_session_owner = VecDeque::new();
    drain_buffered_ws_events_before_expiry_with_pool(
        event_rx,
        gateway,
        connections,
        &pending_connects,
        &deferred_for_session_owner,
        dispatches,
        downstream_sink,
        now,
    )
    .await
}

pub(super) struct WsDownstreamReader {
    pub(super) task: JoinHandle<(bool, futures_util::stream::SplitStream<WebSocket>)>,
}

impl Drop for WsDownstreamReader {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) async fn flush_responses_peer_close(
    sink: futures_util::stream::SplitSink<WebSocket, Message>,
    stream: futures_util::stream::SplitStream<WebSocket>,
    deadline: tokio::time::Instant,
) {
    // A cancelled send can leave application data in SplitSink's slot. Reunite
    // drops that unsent slot so it cannot block the automatically queued reply.
    let mut socket = sink.reunite(stream).expect("matching downstream halves");
    let _ = tokio::time::timeout_at(deadline, socket.flush()).await;
}

pub(super) async fn close_responses_downstream(
    mut sink: futures_util::stream::SplitSink<WebSocket, Message>,
    mut reader: WsDownstreamReader,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let _ = tokio::time::timeout_at(deadline, sink.send(Message::Close(None))).await;
    // The reader may consume a peer close while the shutdown send is pending.
    // Use its result even when the send failed with SendAfterClosing.
    match tokio::time::timeout_at(deadline, &mut reader.task).await {
        Ok(Ok((true, stream))) => {
            flush_responses_peer_close(sink, stream, deadline).await;
        }
        Ok(_) => {}
        Err(_) => {
            reader.task.abort();
            let _ = (&mut reader.task).await;
        }
    }
}

async fn relay_responses_websocket(
    downstream: WebSocket,
    state: ProxyState,
    mut gateway: ResponsesWebSocketGateway,
) {
    let (mut downstream_sink, mut downstream_stream) = downstream.split();
    let slots: usize = state
        .websocket_bulkhead_config
        .semaphore_permits
        .try_into()
        .unwrap_or(usize::MAX);
    let capacity = slots.max(1);
    let (events, mut event_rx) = tokio::sync::mpsc::channel(capacity);
    let (client_tx, mut client_rx) = tokio::sync::mpsc::channel(capacity);
    let client_closed = CancellationToken::new();
    let reader_closed = client_closed.clone();
    let mut client_reader = WsDownstreamReader {
        task: tokio::spawn(async move {
            let mut pending = VecDeque::new();
            let peer_closed = loop {
                if pending.is_empty() {
                    let Some(result) = downstream_stream.next().await else {
                        break false;
                    };
                    let message = match result {
                        Ok(Message::Close(_)) => break true,
                        Err(_) => break false,
                        Ok(message) => message,
                    };
                    match client_tx.try_send(message) {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(message)) => {
                            pending.push_back(message);
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break false,
                    }
                } else {
                    tokio::select! {
                        permit = client_tx.reserve() => {
                            let Ok(permit) = permit else {
                                break false;
                            };
                            if let Some(message) = pending.pop_front() {
                                permit.send(message);
                            }
                        }
                        result = downstream_stream.next() => {
                            let Some(result) = result else {
                                break false;
                            };
                            match result {
                                Ok(Message::Close(_)) => break true,
                                Err(_) => break false,
                                Ok(message) => {
                                    if pending.len() >= capacity {
                                        break false;
                                    }
                                    pending.push_back(message);
                                }
                            }
                        }
                    }
                }
            };
            reader_closed.cancel();
            // Hold the read half until the owner flushes the queued close reply.
            (peer_closed, downstream_stream)
        }),
    };
    let mut connections = HashMap::new();
    let mut pending_connects: HashMap<u64, PendingConnect> = HashMap::new();
    let mut connect_tasks: tokio::task::JoinSet<ConnectAttemptOutcome> =
        tokio::task::JoinSet::new();
    let mut deferred_for_session_owner: VecDeque<PendingDispatchState> = VecDeque::new();
    let mut dispatches = VecDeque::new();
    let mut carried_budgets: HashMap<u64, CarriedConnectBudget> = HashMap::new();
    let mut next_connection_id = 1_u64;
    let heartbeat_period = cc_lb_lifecycle::DEFAULT_STREAM_ACTIVITY_HEARTBEAT_INTERVAL;
    let mut activity_heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + heartbeat_period,
        heartbeat_period,
    );
    activity_heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut drain_handled = false;
    let body = async {
        'relay: loop {
            let dispatched_any = !dispatches.is_empty();
            if !flush_pending_ws_dispatches(
                &mut gateway,
                &state,
                &mut connections,
                &events,
                &mut next_connection_id,
                capacity,
                &mut pending_connects,
                &mut connect_tasks,
                &mut deferred_for_session_owner,
                &mut dispatches,
                &mut carried_budgets,
                &mut downstream_sink,
            )
            .await
            {
                break 'relay;
            }
            if dispatched_any {
                // Drain a finite snapshot of provider events already buffered
                // before parking in `select!`, so sustained provider traffic
                // never starves client controls, heartbeats, or expiry timers.
                let snapshot_limit = event_rx.len().min(MAX_WS_EVENT_DRAIN_BATCH);
                if snapshot_limit > 0
                    && !drain_buffered_ws_events_snapshot(
                        &mut event_rx,
                        &mut gateway,
                        &mut connections,
                        &pending_connects,
                        &deferred_for_session_owner,
                        &mut dispatches,
                        &mut downstream_sink,
                        snapshot_limit,
                    )
                    .await
                {
                    break 'relay;
                }
                if !flush_pending_ws_dispatches(
                    &mut gateway,
                    &state,
                    &mut connections,
                    &events,
                    &mut next_connection_id,
                    capacity,
                    &mut pending_connects,
                    &mut connect_tasks,
                    &mut deferred_for_session_owner,
                    &mut dispatches,
                    &mut carried_budgets,
                    &mut downstream_sink,
                )
                .await
                {
                    break 'relay;
                }
            }

            let pending_deadline = gateway
                .next_pending_error_deadline()
                .map(tokio::time::Instant::from_std);
            let deferred_deadline = deferred_for_session_owner
                .iter()
                .map(|entry| entry.deadline)
                .min();
            if state.drain_controller.is_draining()
                && !gateway.has_active_turns()
                && pending_deadline.is_none()
                && pending_connects.is_empty()
                && deferred_for_session_owner.is_empty()
                && dispatches.is_empty()
            {
                break 'relay;
            }

            tokio::select! {
                _ = state.drain_controller.wait_for_draining(), if !drain_handled => {
                    drain_handled = true;
                    connect_tasks.abort_all();
                    while connect_tasks.join_next().await.is_some() {}
                    for (_, pending) in pending_connects.drain() {
                        for entry in std::iter::once(pending.primary).chain(pending.waiters) {
                            let error =
                                WebSocketError::new(503, "draining", "server is draining")
                                    .with_stream(entry.dispatch.stream_id);
                            let ready =
                                gateway.fail_dispatch(entry.dispatch.turn_id, &error).await;
                            dispatches.extend(ready);
                            if downstream_sink
                                .send(websocket_error_message(&error))
                                .await
                                .is_err()
                            {
                                break 'relay;
                            }
                        }
                    }
                    for entry in deferred_for_session_owner.drain(..) {
                        let error = WebSocketError::new(503, "draining", "server is draining")
                            .with_stream(entry.dispatch.stream_id);
                        let ready = gateway.fail_dispatch(entry.dispatch.turn_id, &error).await;
                        dispatches.extend(ready);
                        if downstream_sink
                            .send(websocket_error_message(&error))
                            .await
                            .is_err()
                        {
                            break 'relay;
                        }
                    }
                    if !gateway.has_active_turns()
                        && gateway.next_pending_error_deadline().is_none()
                        && dispatches.is_empty()
                    {
                        break 'relay;
                    }
                }
                _ = activity_heartbeat.tick(), if gateway.has_active_turns() => {
                    gateway.observe_active_turns_heartbeat();
                }
                _ = async {
                    if let Some(deadline) = pending_deadline {
                        tokio::time::sleep_until(deadline).await;
                    }
                }, if pending_deadline.is_some() => {
                    if !drain_buffered_ws_events_before_expiry_with_pool(
                        &mut event_rx,
                        &mut gateway,
                        &mut connections,
                        &pending_connects,
                        &deferred_for_session_owner,
                        &mut dispatches,
                        &mut downstream_sink,
                        std::time::Instant::now(),
                    )
                    .await
                    {
                        break 'relay;
                    }
                }
                _ = async {
                    if let Some(deadline) = deferred_deadline {
                        tokio::time::sleep_until(deadline).await;
                    }
                }, if deferred_deadline.is_some() => {
                    let errors = expire_deferred_for_session_owner(
                        &mut gateway,
                        &mut deferred_for_session_owner,
                        &connections,
                        &pending_connects,
                        &mut dispatches,
                        &mut carried_budgets,
                    )
                    .await;
                    for error in errors {
                        if downstream_sink
                            .send(websocket_error_message(&error))
                            .await
                            .is_err()
                        {
                            break 'relay;
                        }
                    }
                }
                Some(joined) = connect_tasks.join_next(), if !connect_tasks.is_empty() => {
                    match joined {
                        Ok(outcome) => {
                            let errors = handle_connect_outcome(
                                &mut gateway,
                                outcome,
                                &state,
                                &mut connections,
                                &events,
                                &mut next_connection_id,
                                capacity,
                                &mut pending_connects,
                                &mut connect_tasks,
                                &mut deferred_for_session_owner,
                                &mut dispatches,
                                &mut carried_budgets,
                            )
                            .await;
                            for error in errors {
                                if downstream_sink
                                    .send(websocket_error_message(&error))
                                    .await
                                    .is_err()
                                {
                                    break 'relay;
                                }
                            }
                        }
                        Err(join_error) if join_error.is_cancelled() => {}
                        Err(join_error) => {
                            tracing::error!(%join_error, "responses provider connect task panicked");
                        }
                    }
                }
                client = client_rx.recv() => {
                    let Some(message) = client else { break };
                    match message {
                        Message::Text(frame) => {
                            let auth = match gateway.authenticate().await {
                                Ok(auth) => auth,
                                Err(error) => {
                                    let websocket_error = gateway.reject_authentication(&error);
                                    if downstream_sink.send(websocket_error_message(&websocket_error)).await.is_err() {
                                        break;
                                    }
                                    continue;
                                }
                            };
                            if state.drain_controller.is_draining()
                                && let Ok(parsed) = sonic_rs::from_str::<serde_json::Value>(&frame)
                                && parsed.get("type").and_then(serde_json::Value::as_str)
                                    == Some("response.create")
                            {
                                let stream_id = parsed
                                    .get("stream_id")
                                    .and_then(serde_json::Value::as_str)
                                    .map(str::to_owned);
                                let error = WebSocketError::new(
                                    503,
                                    "draining",
                                    "server is draining",
                                )
                                .with_stream(stream_id);
                                if downstream_sink
                                    .send(websocket_error_message(&error))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                                continue;
                            }
                            match gateway.client_message(auth, frame.into()).await {
                                Ok(Some(dispatch)) => dispatches.push_back(dispatch),
                                Ok(None) => {}
                                Err(error) => {
                                    if downstream_sink.send(websocket_error_message(&error)).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        Message::Binary(_) => {
                            let error = match gateway.authenticate().await {
                                Ok(_auth) => {
                                    let error = WebSocketError::new(
                                        400,
                                        "invalid_request_error",
                                        "Binary WebSocket messages are not supported",
                                    );
                                    gateway.reject_protocol(&error);
                                    error
                                }
                                Err(error) => gateway.reject_authentication(&error),
                            };
                            if downstream_sink.send(websocket_error_message(&error)).await.is_err() {
                                break;
                            }
                        }
                        Message::Ping(frame) => {
                            if downstream_sink.send(Message::Pong(frame)).await.is_err() {
                                break;
                            }
                        }
                        Message::Pong(_) => {}
                        Message::Close(_) => break,
                    }
                }
                provider = event_rx.recv() => {
                    let Some(provider) = provider else { break };
                    if !handle_ws_provider_event(
                        provider,
                        &mut gateway,
                        &mut connections,
                        &pending_connects,
                        &deferred_for_session_owner,
                        &mut dispatches,
                        &mut downstream_sink,
                    )
                    .await
                    {
                        break 'relay;
                    }
                }
            }
        }
    };
    let force_closed =
        run_responses_websocket_until_force_closed(body, &client_closed, &state.drain_controller)
            .await;

    connect_tasks.abort_all();
    while connect_tasks.join_next().await.is_some() {}
    pending_connects.clear();
    if force_closed || state.drain_controller.is_draining() {
        gateway.drain();
    } else {
        gateway.close();
    }
    drop(client_rx);
    drop(dispatches);
    if client_closed.is_cancelled() {
        if let Ok((peer_closed, stream)) = (&mut client_reader.task).await
            && peer_closed
        {
            flush_responses_peer_close(
                downstream_sink,
                stream,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await;
        }
    } else {
        close_responses_downstream(downstream_sink, client_reader).await;
    }
    for (_, mut connection) in connections.drain() {
        if let Some(task) = connection.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

#[cfg(all(test, feature = "admin"))]
pub(super) async fn run_responses_websocket_until_cancelled<F>(
    body: F,
    client_closed: &CancellationToken,
    drain_controller: &DrainController,
) -> bool
where
    F: Future<Output = ()>,
{
    tokio::pin!(body);
    let drain_wait = drain_controller.wait_for_draining();
    tokio::pin!(drain_wait);
    tokio::select! {
        _ = &mut body => false,
        _ = client_closed.cancelled() => false,
        _ = &mut drain_wait => true,
    }
}

async fn run_responses_websocket_until_force_closed<F>(
    body: F,
    client_closed: &CancellationToken,
    drain_controller: &DrainController,
) -> bool
where
    F: Future<Output = ()>,
{
    tokio::pin!(body);
    let force_close_wait = drain_controller.wait_for_force_close();
    tokio::pin!(force_close_wait);
    tokio::select! {
        _ = &mut body => false,
        _ = client_closed.cancelled() => false,
        _ = &mut force_close_wait => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn responses_websocket_prepare_timeout_isolates_coalesced_waiter() {
        use cc_lb_upstream::{
            ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer,
            SignerError, SignerFactory, SigningCapability, UpstreamError,
        };
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        #[derive(Clone)]
        struct TimeoutFirstSignerFactory {
            calls: Arc<AtomicUsize>,
            first_started: Arc<tokio::sync::Notify>,
        }

        struct TestWsSigner;

        #[async_trait::async_trait]
        impl Signer for TestWsSigner {
            async fn sign(
                &self,
                shaped: ShapedRequest,
                capability: &mut SigningCapability,
            ) -> Result<SignedRequest, SignerError> {
                Ok(SignedRequest::from_shaped(shaped, capability))
            }

            async fn on_unauthorized(&self, _error: &UpstreamError) -> RetryDecision {
                RetryDecision::Fail
            }
        }

        impl ApiKeyAwareSignerFactory for TimeoutFirstSignerFactory {
            fn with_router_choice(
                &self,
                _router_chosen_upstream_name: String,
            ) -> Arc<dyn SignerFactory> {
                Arc::new(self.clone())
            }
        }

        #[async_trait::async_trait]
        impl SignerFactory for TimeoutFirstSignerFactory {
            async fn build(
                &self,
                _upstream: &cc_lb_domain::Upstream,
            ) -> Result<Arc<dyn Signer>, SignerError> {
                if self.calls.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                    self.first_started.notify_one();
                    std::future::pending::<()>().await;
                }
                Ok(Arc::new(TestWsSigner))
            }
        }

        let (sent_tx, mut sent_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let accept_count = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test upstream");
        let addr = listener.local_addr().unwrap();
        let accepts = Arc::clone(&accept_count);
        let up_task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                accepts.fetch_add(1, AtomicOrdering::SeqCst);
                let tx = sent_tx.clone();
                tokio::spawn(async move {
                    if let Ok(mut ws) = tokio_tungstenite::accept_async(socket).await
                        && let Some(Ok(UpstreamMessage::Text(text))) = ws.next().await
                    {
                        let _ = tx.send(text.to_string());
                    }
                });
            }
        });

        let up = crate::app::tests::websocket_upstream(
            1,
            "openai-prepare-timeout",
            url::Url::parse(&format!("http://{addr}/")).unwrap(),
        );
        let first_started = Arc::new(tokio::sync::Notify::new());
        let signer_factory = Arc::new(TimeoutFirstSignerFactory {
            calls: Arc::new(AtomicUsize::new(0)),
            first_started: Arc::clone(&first_started),
        });
        let state = crate::app::tests::websocket_test_state(
            vec![up.clone()],
            cc_lb_domain::TerminalStrategy::FirstPick,
            DrainController::new(),
            signer_factory,
            Arc::new(crate::app::tests::FixtureKeyStore),
            vec!["*".to_owned()],
        )
        .await;

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(crate::app::tests::TEST_KEY.plaintext.as_str()).unwrap(),
        );
        let auth = state.lifecycle.authenticate(&headers).await.unwrap();
        let mut gateway = ResponsesWebSocketGateway::new(
            state.lifecycle.clone(),
            &auth,
            headers,
            state.body_caps.messages,
            8,
        );
        let d1 = gateway
            .client_message(
                auth.clone(),
                Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-4.1","stream_id":"lane-1","store":false,"input":[]}"#,
                ),
            )
            .await
            .unwrap()
            .unwrap();
        let d2 = gateway
            .client_message(
                auth,
                Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-5","stream_id":"lane-2","store":false,"input":[]}"#,
                ),
            )
            .await
            .unwrap()
            .unwrap();

        let (events, _rx) = tokio::sync::mpsc::channel(16);
        let mut connections = HashMap::new();
        let mut next_connection_id = 1;
        let mut pending_connects = HashMap::new();
        let mut connect_tasks = tokio::task::JoinSet::new();
        let mut deferred = VecDeque::new();
        let mut ready = VecDeque::new();
        let mut carried_budgets = HashMap::new();

        // Start Lane 1 with a tight 80ms prepare timeout so prepare_job.run() times out.
        let (r1, e1) = advance_one_dispatch(
            &mut gateway,
            PendingDispatchState::new(d1, Duration::from_millis(80), Duration::from_millis(80), 1),
            &state,
            &mut connections,
            &events,
            &mut next_connection_id,
            8,
            &mut pending_connects,
            &mut connect_tasks,
            &mut deferred,
            &ready,
            &mut carried_budgets,
        )
        .await;
        assert!(r1.is_empty() && e1.is_empty());
        tokio::time::timeout(Duration::from_secs(2), first_started.notified())
            .await
            .expect("lane-1 entered prepare_job.run()");

        // Coalesce Lane 2 (with plenty of budget) onto Lane 1's PendingConnect.
        let (r2, e2) = advance_one_dispatch(
            &mut gateway,
            PendingDispatchState::new(d2, Duration::from_secs(5), Duration::from_secs(5), 1),
            &state,
            &mut connections,
            &events,
            &mut next_connection_id,
            8,
            &mut pending_connects,
            &mut connect_tasks,
            &mut deferred,
            &ready,
            &mut carried_budgets,
        )
        .await;
        assert!(r2.is_empty() && e2.is_empty());
        assert_eq!(pending_connects.values().next().unwrap().waiters.len(), 1);

        // Wait for Lane 1's prepare_job.run() timeout outcome; verify ConnectAttemptError::Prepare
        // fails only Lane 1 (504) while restoring candidate to Lane 2 and re-dispatching it cleanly.
        let mut errs = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline
            && (!pending_connects.is_empty() || !connect_tasks.is_empty())
        {
            let outcome = tokio::time::timeout(Duration::from_secs(2), connect_tasks.join_next())
                .await
                .expect("connect task completes")
                .expect("some connect task")
                .expect("join ok");
            let new_errs = handle_connect_outcome(
                &mut gateway,
                outcome,
                &state,
                &mut connections,
                &events,
                &mut next_connection_id,
                8,
                &mut pending_connects,
                &mut connect_tasks,
                &mut deferred,
                &mut ready,
                &mut carried_budgets,
            )
            .await;
            errs.extend(new_errs);
        }

        assert_eq!(errs.len(), 1, "only timed-out lane-1 must error: {errs:?}");
        assert_eq!(errs[0].status, 504);
        assert_eq!(errs[0].stream_id.as_deref(), Some("lane-1"));
        assert!(
            ready.is_empty(),
            "direct lane-2 dispatch does not queue follow-ups: {ready:?}"
        );
        assert_eq!(
            connections.len(),
            1,
            "lane-2 must establish its own connection"
        );
        assert_eq!(accept_count.load(AtomicOrdering::SeqCst), 1);
        let sent_frame = tokio::time::timeout(Duration::from_secs(2), sent_rx.recv())
            .await
            .expect("lane-2 frame delivered to upstream")
            .expect("frame present");
        assert!(
            sent_frame.contains("\"lane-2\""),
            "expected lane-2 frame on upstream socket, got {sent_frame}"
        );
        up_task.abort();
    }

    #[derive(Clone)]
    struct BlockingValidateKeyStore {
        armed: Arc<std::sync::atomic::AtomicBool>,
        entered: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl cc_lb_storage_api::ManagedKeyStore for BlockingValidateKeyStore {
        async fn issue(
            &self,
            principal_id: &str,
            key_id: &str,
            params: cc_lb_storage_api::IssueParams,
        ) -> cc_lb_storage_api::StorageResult<cc_lb_storage_api::StoredApiKeyRecord> {
            crate::app::tests::FixtureKeyStore
                .issue(principal_id, key_id, params)
                .await
        }

        async fn get(
            &self,
            principal_id: &str,
            key_id: &str,
        ) -> cc_lb_storage_api::StorageResult<Option<cc_lb_storage_api::StoredApiKeyRecord>>
        {
            crate::app::tests::FixtureKeyStore
                .get(principal_id, key_id)
                .await
        }

        async fn lookup_by_index_hash(
            &self,
            index_hash: &[u8; 32],
        ) -> cc_lb_storage_api::StorageResult<
            Option<(String, String, cc_lb_storage_api::StoredApiKeyRecord)>,
        > {
            if self.armed.load(std::sync::atomic::Ordering::SeqCst) {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            crate::app::tests::FixtureKeyStore
                .lookup_by_index_hash(index_hash)
                .await
        }

        async fn list_by_principal(
            &self,
            principal_id: &str,
        ) -> cc_lb_storage_api::StorageResult<Vec<cc_lb_storage_api::StoredApiKeyRecord>> {
            crate::app::tests::FixtureKeyStore
                .list_by_principal(principal_id)
                .await
        }

        async fn list_all(
            &self,
        ) -> cc_lb_storage_api::StorageResult<
            Vec<(String, String, cc_lb_storage_api::StoredApiKeyRecord)>,
        > {
            crate::app::tests::FixtureKeyStore.list_all().await
        }

        async fn update(
            &self,
            principal_id: &str,
            key_id: &str,
            mutation: cc_lb_storage_api::ApiKeyMutation,
        ) -> cc_lb_storage_api::StorageResult<()> {
            crate::app::tests::FixtureKeyStore
                .update(principal_id, key_id, mutation)
                .await
        }

        async fn revoke_zero_secrets(
            &self,
            principal_id: &str,
            key_id: &str,
        ) -> cc_lb_storage_api::StorageResult<()> {
            crate::app::tests::FixtureKeyStore
                .revoke_zero_secrets(principal_id, key_id)
                .await
        }
    }

    #[tokio::test]
    async fn responses_websocket_fallback_validation_observes_drain_without_dialing() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

        let dial_accepts = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fallback upstream");
        let addr = listener.local_addr().unwrap();
        let accepts_task = Arc::clone(&dial_accepts);
        let up_task = tokio::spawn(async move {
            while let Ok((_socket, _)) = listener.accept().await {
                accepts_task.fetch_add(1, AtomicOrdering::SeqCst);
            }
        });

        let up1 = crate::app::tests::websocket_upstream(
            1,
            "primary-upstream",
            url::Url::parse("http://127.0.0.1:1/").unwrap(),
        );
        let up2 = crate::app::tests::websocket_upstream(
            2,
            "fallback-upstream",
            url::Url::parse(&format!("http://{addr}/")).unwrap(),
        );
        let armed = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let key_store = Arc::new(BlockingValidateKeyStore {
            armed: Arc::clone(&armed),
            entered: Arc::clone(&entered),
        });
        let drain = DrainController::new();
        let state = crate::app::tests::websocket_test_state(
            vec![up1, up2],
            cc_lb_domain::TerminalStrategy::FirstPick,
            drain.clone(),
            Arc::new(crate::app::tests::WebSocketTestSignerFactory),
            key_store,
            vec!["*".to_owned()],
        )
        .await;

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(crate::app::tests::TEST_KEY.plaintext.as_str()).unwrap(),
        );
        let auth = state.lifecycle.authenticate(&headers).await.unwrap();
        let mut gateway = ResponsesWebSocketGateway::new(
            state.lifecycle.clone(),
            &auth,
            headers,
            state.body_caps.messages,
            8,
        );
        let d1 = gateway
            .client_message(
                auth,
                Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-4.1","stream_id":"fallback-lane","store":false,"input":[]}"#,
                ),
            )
            .await
            .unwrap()
            .unwrap();
        let mut pending =
            PendingDispatchState::new(d1, Duration::from_secs(5), Duration::from_secs(5), 2);
        // Simulate exhausted primary so `advance_one_dispatch` enters `validate_fallback_candidate`
        // for `up2` (`reject_exhausted_fallback == true`, triggering `self.authenticate()`).
        let first = pending.remaining.pop_front().expect("primary candidate");
        assert_eq!(first.upstream_name, "primary-upstream");
        assert_eq!(pending.remaining.len(), 1);

        let (events, _rx) = tokio::sync::mpsc::channel(16);
        let mut connections = HashMap::new();
        let mut next_connection_id = 1;
        let mut pending_connects = HashMap::new();
        let mut connect_tasks = tokio::task::JoinSet::new();
        let mut deferred = VecDeque::new();
        let empty_dispatches = VecDeque::new();
        let mut carried_budgets = HashMap::new();

        armed.store(true, AtomicOrdering::SeqCst);
        let drain_for_trigger = drain.clone();
        let trigger_task = tokio::spawn(async move {
            entered.notified().await;
            drain_for_trigger.trigger();
        });
        let (ready, errs) = tokio::time::timeout(
            Duration::from_secs(2),
            advance_one_dispatch(
                &mut gateway,
                pending,
                &state,
                &mut connections,
                &events,
                &mut next_connection_id,
                8,
                &mut pending_connects,
                &mut connect_tasks,
                &mut deferred,
                &empty_dispatches,
                &mut carried_budgets,
            ),
        )
        .await
        .expect("advance_one_dispatch cancels blocked fallback validation on drain");
        trigger_task.await.expect("trigger task completes");

        assert!(ready.is_empty());
        assert_eq!(errs.len(), 1, "expected 503 draining error, got: {errs:?}");
        assert_eq!(errs[0].status, 503);
        assert_eq!(errs[0].code, "draining");
        assert!(pending_connects.is_empty());
        assert!(connect_tasks.is_empty());
        assert_eq!(dial_accepts.load(AtomicOrdering::SeqCst), 0);
        up_task.abort();
    }

    #[tokio::test]
    async fn responses_websocket_pinned_send_validation_observes_drain() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind provider socket");
        let addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while ws.next().await.is_some() {}
        });

        let up1 = crate::app::tests::websocket_upstream(
            1,
            "primary-upstream",
            url::Url::parse(&format!("http://{addr}/")).unwrap(),
        );
        let armed = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let key_store = Arc::new(BlockingValidateKeyStore {
            armed: Arc::clone(&armed),
            entered: Arc::clone(&entered),
        });
        let drain = DrainController::new();
        let state = crate::app::tests::websocket_test_state(
            vec![up1.clone()],
            cc_lb_domain::TerminalStrategy::FirstPick,
            drain.clone(),
            Arc::new(crate::app::tests::WebSocketTestSignerFactory),
            key_store,
            vec!["*".to_owned()],
        )
        .await;

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(crate::app::tests::TEST_KEY.plaintext.as_str()).unwrap(),
        );
        let auth = state.lifecycle.authenticate(&headers).await.unwrap();
        let mut gateway = ResponsesWebSocketGateway::new(
            state.lifecycle.clone(),
            &auth,
            headers,
            state.body_caps.messages,
            8,
        );
        let dispatch = gateway
            .client_message(
                auth,
                Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-4.1","stream_id":"pinned-lane","store":false,"input":[]}"#,
                ),
            )
            .await
            .unwrap()
            .unwrap();
        gateway.assign_connection(dispatch.turn_id, 1, up1.id, "primary-upstream");

        let (ws_client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/v1/responses"))
            .await
            .expect("connect mock provider socket");
        let (sink, _stream) = ws_client.split();
        let bulkhead = state
            .websocket_bulkheads
            .semaphore_only_bulkhead(
                websocket_breaker_key(up1.id),
                state.websocket_bulkhead_config,
            )
            .acquire_owned()
            .await
            .expect("bulkhead permit");
        let mut connections = HashMap::new();
        connections.insert(
            1,
            WsProviderConnection {
                upstream_id: up1.id,
                upstream_name: "primary-upstream".to_owned(),
                sink,
                task: None,
                lane: Some("pinned-lane".to_owned()),
                _bulkhead_guard: bulkhead,
            },
        );

        armed.store(true, AtomicOrdering::SeqCst);
        let drain_for_trigger = drain.clone();
        let trigger_task = tokio::spawn(async move {
            entered.notified().await;
            drain_for_trigger.trigger();
        });
        let empty_pending_connects = HashMap::new();
        let empty_dispatches = VecDeque::new();
        let empty_deferred = VecDeque::new();
        let mut carried_budgets = HashMap::new();
        let empty_attempted = std::collections::HashSet::new();
        let (ready, errs) = tokio::time::timeout(
            Duration::from_secs(2),
            send_responses_dispatch(
                &mut gateway,
                dispatch,
                1,
                &mut connections,
                &empty_pending_connects,
                &empty_dispatches,
                &empty_deferred,
                &mut carried_budgets,
                0,
                WS_MAX_UPSTREAM_ATTEMPTS,
                &empty_attempted,
                cc_lb_engine::responses_websocket::ResponsesDispatchTimings::reused(),
                tokio::time::Instant::now() + Duration::from_secs(30),
                &drain,
            ),
        )
        .await
        .expect("send_responses_dispatch cancels blocked send validation on drain");
        trigger_task.await.expect("trigger task completes");

        assert!(ready.is_empty());
        assert_eq!(errs.len(), 1, "expected 503 draining error, got: {errs:?}");
        assert_eq!(errs[0].status, 503);
        assert_eq!(errs[0].code, "draining");
        server_task.abort();
    }

    async fn spawn_test_downstream_sink() -> (
        futures_util::stream::SplitSink<WebSocket, Message>,
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        JoinHandle<()>,
    ) {
        use futures_util::StreamExt as _;

        let (sink_tx, sink_rx) = tokio::sync::oneshot::channel();
        let sink_tx = Arc::new(std::sync::Mutex::new(Some(sink_tx)));
        let app = axum::Router::new().route(
            "/ws",
            axum::routing::get(move |ws: axum::extract::ws::WebSocketUpgrade| {
                let sink_tx = Arc::clone(&sink_tx);
                async move {
                    ws.on_upgrade(move |socket| async move {
                        let (sink, _stream) = socket.split();
                        if let Some(tx) = sink_tx.lock().unwrap().take() {
                            let _ = tx.send(sink);
                        }
                        std::future::pending::<()>().await;
                    })
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test downstream sink");
        let addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let (client_ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
            .await
            .expect("connect test downstream client");
        let downstream_sink = sink_rx.await.expect("capture downstream sink");
        (downstream_sink, client_ws, server_task)
    }

    async fn spawn_502_handshake_listener() -> (
        std::net::SocketAddr,
        Arc<std::sync::atomic::AtomicUsize>,
        JoinHandle<()>,
    ) {
        use tokio::io::AsyncWriteExt as _;

        let accepts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepts_task = Arc::clone(&accepts);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind 502 handshake listener");
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                accepts_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = tcp
                    .write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
                    .await;
                let _ = tcp.shutdown().await;
            }
        });
        (addr, accepts, task)
    }

    async fn ws_affinity_test_state(
        records: Vec<UpstreamRecord>,
        principal_id: &str,
        cipher_owners: &[(&str, uuid::Uuid)],
    ) -> ProxyState {
        use sha2::Digest as _;

        struct UnusedWsHttpDispatcher;
        #[async_trait::async_trait]
        impl cc_lb_engine::UpstreamDispatch for UnusedWsHttpDispatcher {
            async fn dispatch(
                &self,
                _request: cc_lb_upstream::SignedRequest,
            ) -> Result<http::Response<cc_lb_engine::Body>, cc_lb_engine::DispatchError>
            {
                unreachable!("unused")
            }
        }

        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let sqlite = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .expect("sqlite memory opens"),
        );
        cc_lb_storage_api::MetaStore::initialize(&*sqlite)
            .await
            .expect("sqlite initializes");
        for record in &records {
            cc_lb_storage_api::UpstreamStore::create(
                &*sqlite,
                cc_lb_storage_api::UpstreamCreate {
                    id: record.id,
                    name: record.name.clone(),
                    kind: record.kind,
                    base_url: record.base_url.clone(),
                    api_key_ciphertext: record.api_key_ciphertext.clone(),
                    oauth_tokens: None,
                    warmup_enabled: record.warmup_enabled,
                    warmup_dialect_plugin: record.warmup_dialect_plugin.clone(),
                    workspace_id: record.workspace_id.clone(),
                    vertex: record.vertex.clone(),
                    vertex_credential_type: record.vertex_credential_type,
                    vertex_quota_project_id: record.vertex_quota_project_id.clone(),
                    api_key_access: None,
                },
            )
            .await
            .expect("seed upstream record");
        }
        let now_secs = cc_lb_clock::unix_secs(clock.now());
        let bindings: Vec<_> = cipher_owners
            .iter()
            .map(
                |(cipher, owner)| cc_lb_storage_api::UpstreamAffinityBinding {
                    key: cc_lb_storage_api::UpstreamAffinityKey {
                        principal_id: principal_id.to_owned(),
                        provider: "openai".to_owned(),
                        kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                        value_sha256: sha2::Sha256::digest(cipher.as_bytes()).into(),
                    },
                    upstream_id: *owner,
                    owner_epoch: 0,
                    observed_at_unix_secs: now_secs,
                    expires_at_unix_secs: None,
                },
            )
            .collect();
        cc_lb_storage_api::UpstreamAffinityStore::bind_upstream_affinities(
            &*sqlite, &bindings, now_secs, 86_400,
        )
        .await
        .expect("seed affinity bindings");

        let mut state = crate::app::tests::websocket_test_state(
            records,
            cc_lb_domain::TerminalStrategy::FirstPick,
            DrainController::new(),
            Arc::new(crate::app::tests::WebSocketTestSignerFactory),
            Arc::new(crate::app::tests::FixtureKeyStore),
            vec!["*".to_owned()],
        )
        .await;
        let keys = Arc::new(cc_lb_control::api_keys::key_store::KeyStore::new(Arc::new(
            crate::app::tests::FixtureKeyStore,
        )));
        let builtin_authn = Arc::new(cc_lb_control::api_keys::builtin_authn::BuiltinAuthn::new(
            keys,
            clock.clone(),
        ));
        state.lifecycle = Arc::new(
            cc_lb_engine::Lifecycle::new_with_dynamic_view(
                builtin_authn,
                Arc::clone(&state.dynamic_view),
                Arc::new(UnusedWsHttpDispatcher),
                cc_lb_engine::LifecycleConfig::default(),
                clock,
            )
            .with_upstream_affinity_store(
                Arc::clone(&sqlite) as Arc<dyn cc_lb_storage_api::UpstreamAffinityStore>
            ),
        );
        state.storage = sqlite as Arc<dyn cc_lb_storage_api::Storage>;
        state
    }

    #[tokio::test]
    async fn responses_websocket_pre_dispatch_owner_recovery_preserves_connect_budget() {
        use futures_util::StreamExt as _;
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        // Subcase A: 1 initial dial attempt on owner `up1` + pre-dispatch recovery onto `[up2, up3, up4]`
        // hits the shared 3-attempt dial cap on `up3` so `up4` receives zero dials.
        let (addr1, accepts1, task1) = spawn_502_handshake_listener().await;
        let (addr2, accepts2, task2) = spawn_502_handshake_listener().await;
        let (addr3, accepts3, task3) = spawn_502_handshake_listener().await;
        let accepts4 = Arc::new(AtomicUsize::new(0));
        let accepts4_task = Arc::clone(&accepts4);
        let listener4 = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind healthy 4th upstream");
        let addr4 = listener4.local_addr().unwrap();
        let task4 = tokio::spawn(async move {
            while let Ok((tcp, _)) = listener4.accept().await {
                accepts4_task.fetch_add(1, AtomicOrdering::SeqCst);
                let _ = tokio_tungstenite::accept_async(tcp).await;
            }
        });

        let up1 = crate::app::tests::websocket_upstream(
            1,
            "owner-up1",
            url::Url::parse(&format!("http://{addr1}/")).unwrap(),
        );
        let up2 = crate::app::tests::websocket_upstream(
            2,
            "up2",
            url::Url::parse(&format!("http://{addr2}/")).unwrap(),
        );
        let up3 = crate::app::tests::websocket_upstream(
            3,
            "up3",
            url::Url::parse(&format!("http://{addr3}/")).unwrap(),
        );
        let up4 = crate::app::tests::websocket_upstream(
            4,
            "up4",
            url::Url::parse(&format!("http://{addr4}/")).unwrap(),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(crate::app::tests::TEST_KEY.plaintext.as_str()).unwrap(),
        );
        let cipher_up1 = "gAAAAAB_pre_dispatch_budget_up1";
        let state_seed = crate::app::tests::websocket_test_state(
            vec![up1.clone()],
            cc_lb_domain::TerminalStrategy::FirstPick,
            DrainController::new(),
            Arc::new(crate::app::tests::WebSocketTestSignerFactory),
            Arc::new(crate::app::tests::FixtureKeyStore),
            vec!["*".to_owned()],
        )
        .await;
        let auth = state_seed.lifecycle.authenticate(&headers).await.unwrap();
        let state = ws_affinity_test_state(
            vec![up1.clone(), up2.clone(), up3.clone(), up4.clone()],
            auth.principal_id(),
            &[(cipher_up1, up1.id)],
        )
        .await;

        let mut gateway = ResponsesWebSocketGateway::new(
            state.lifecycle.clone(),
            &auth,
            headers.clone(),
            state.body_caps.messages,
            8,
        );
        let initial_dispatch = gateway
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "input": [
                            {"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": cipher_up1},
                            {"role": "user", "content": "budget check"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(initial_dispatch.candidates.len(), 1);
        assert_eq!(initial_dispatch.candidates[0].upstream_id, up1.id);

        let (mut downstream_sink, mut client_ws, sink_server) = spawn_test_downstream_sink().await;
        let (events, _event_rx) = tokio::sync::mpsc::channel(16);
        let mut connections = HashMap::new();
        let mut next_connection_id = 1;
        let mut pending_connects = HashMap::new();
        let mut connect_tasks = tokio::task::JoinSet::new();
        let mut deferred = VecDeque::new();
        let mut dispatches = VecDeque::from([initial_dispatch]);
        let mut carried_budgets = HashMap::new();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    flush_pending_ws_dispatches(
                        &mut gateway,
                        &state,
                        &mut connections,
                        &events,
                        &mut next_connection_id,
                        8,
                        &mut pending_connects,
                        &mut connect_tasks,
                        &mut deferred,
                        &mut dispatches,
                        &mut carried_budgets,
                        &mut downstream_sink,
                    )
                    .await
                );
                if pending_connects.is_empty() && connect_tasks.is_empty() && dispatches.is_empty()
                {
                    break;
                }
                let outcome = connect_tasks
                    .join_next()
                    .await
                    .expect("connect task present")
                    .expect("join ok");
                let errs = handle_connect_outcome(
                    &mut gateway,
                    outcome,
                    &state,
                    &mut connections,
                    &events,
                    &mut next_connection_id,
                    8,
                    &mut pending_connects,
                    &mut connect_tasks,
                    &mut deferred,
                    &mut dispatches,
                    &mut carried_budgets,
                )
                .await;
                for err in errs {
                    downstream_sink
                        .send(websocket_error_message(&err))
                        .await
                        .unwrap();
                }
            }
        })
        .await
        .expect("pre-dispatch recovery loop completes within timeout");

        let client_frame = tokio::time::timeout(Duration::from_secs(2), client_ws.next())
            .await
            .expect("client receives 502 error")
            .unwrap()
            .unwrap();
        let client_json: serde_json::Value =
            serde_json::from_str(client_frame.to_text().unwrap()).unwrap();
        assert_eq!(client_json["type"], "error");
        assert_eq!(client_json["status"], 502);
        assert_eq!(accepts1.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(accepts2.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(accepts3.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            accepts4.load(AtomicOrdering::SeqCst),
            0,
            "shared 3-attempt dial cap must prevent dialing 4th upstream after owner recovery"
        );
        assert!(carried_budgets.is_empty());

        sink_server.abort();
        task1.abort();
        task2.abort();
        task3.abort();
        task4.abort();

        // Subcase B: an expired connect deadline must block pre-dispatch owner recovery instead of
        // minting a fresh connect budget for the replacement upstream.
        let (hold_tx, _hold_rx) = tokio::sync::mpsc::channel(4);
        let stall_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalling listener");
        let stall_addr = stall_listener.local_addr().unwrap();
        let stall_task = tokio::spawn(async move {
            while let Ok((tcp, _)) = stall_listener.accept().await {
                let _ = hold_tx.try_send(tcp);
            }
        });
        let accepts_sub_b = Arc::new(AtomicUsize::new(0));
        let accepts_sub_b_task = Arc::clone(&accepts_sub_b);
        let healthy_b_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind subcase B replacement listener");
        let healthy_b_addr = healthy_b_listener.local_addr().unwrap();
        let healthy_b_task = tokio::spawn(async move {
            while let Ok((tcp, _)) = healthy_b_listener.accept().await {
                accepts_sub_b_task.fetch_add(1, AtomicOrdering::SeqCst);
                let _ = tokio_tungstenite::accept_async(tcp).await;
            }
        });
        let up_stall = crate::app::tests::websocket_upstream(
            11,
            "stall-up",
            url::Url::parse(&format!("http://{stall_addr}/")).unwrap(),
        );
        let up_healthy_b = crate::app::tests::websocket_upstream(
            12,
            "healthy-b",
            url::Url::parse(&format!("http://{healthy_b_addr}/")).unwrap(),
        );
        let cipher_b = "gAAAAAB_pre_dispatch_expired_b";
        let state_b = ws_affinity_test_state(
            vec![up_stall.clone(), up_healthy_b],
            auth.principal_id(),
            &[(cipher_b, up_stall.id)],
        )
        .await;
        let auth_b = state_b.lifecycle.authenticate(&headers).await.unwrap();
        let mut gateway_b = ResponsesWebSocketGateway::new(
            state_b.lifecycle.clone(),
            &auth_b,
            headers,
            state_b.body_caps.messages,
            8,
        );
        let dispatch_b = gateway_b
            .client_message(
                auth_b,
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "input": [
                            {"type": "reasoning", "id": "rs_b", "summary": [], "encrypted_content": cipher_b},
                            {"role": "user", "content": "expired budget check"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();

        let (events_b, _event_rx_b) = tokio::sync::mpsc::channel(16);
        let mut connections_b = HashMap::new();
        let mut next_conn_id_b = 1;
        let mut pending_b = HashMap::new();
        let mut connect_tasks_b = tokio::task::JoinSet::new();
        let mut deferred_b = VecDeque::new();
        let mut ready_b = VecDeque::new();
        let mut carried_b = HashMap::new();
        let (r0, e0) = advance_one_dispatch(
            &mut gateway_b,
            PendingDispatchState::new(
                dispatch_b,
                Duration::from_millis(30),
                Duration::from_millis(30),
                3,
            ),
            &state_b,
            &mut connections_b,
            &events_b,
            &mut next_conn_id_b,
            8,
            &mut pending_b,
            &mut connect_tasks_b,
            &mut deferred_b,
            &ready_b,
            &mut carried_b,
        )
        .await;
        assert!(r0.is_empty() && e0.is_empty());
        let outcome_b = tokio::time::timeout(Duration::from_secs(2), connect_tasks_b.join_next())
            .await
            .expect("stalled connect times out")
            .expect("connect task joined")
            .expect("join ok");
        let errs_b = handle_connect_outcome(
            &mut gateway_b,
            outcome_b,
            &state_b,
            &mut connections_b,
            &events_b,
            &mut next_conn_id_b,
            8,
            &mut pending_b,
            &mut connect_tasks_b,
            &mut deferred_b,
            &mut ready_b,
            &mut carried_b,
        )
        .await;
        assert!(
            ready_b.is_empty(),
            "expired connect budget must block recovery dispatch: {ready_b:?}"
        );
        assert_eq!(errs_b.len(), 1);
        assert_eq!(errs_b[0].status, 504);
        assert_eq!(errs_b[0].code, "upstream_unavailable");
        assert!(carried_b.is_empty());
        assert_eq!(accepts_sub_b.load(AtomicOrdering::SeqCst), 0);

        stall_task.abort();
        healthy_b_task.abort();
    }

    #[tokio::test]
    async fn responses_websocket_buffered_429_batch_does_not_double_promise_single_free_slot() {
        use futures_util::StreamExt as _;
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        let spawn_ws_sink_server = || async {
            let accepts = Arc::new(AtomicUsize::new(0));
            let accepts_task = Arc::clone(&accepts);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind ws server");
            let addr = listener.local_addr().unwrap();
            let task = tokio::spawn(async move {
                while let Ok((tcp, _)) = listener.accept().await {
                    accepts_task.fetch_add(1, AtomicOrdering::SeqCst);
                    tokio::spawn(async move {
                        if let Ok(mut ws) = tokio_tungstenite::accept_async(tcp).await {
                            while let Some(Ok(_)) = ws.next().await {}
                        }
                    });
                }
            });
            (addr, accepts, task)
        };

        let (addr_a, _acc_a, task_a) = spawn_ws_sink_server().await;
        let (addr_b, _acc_b, task_b) = spawn_ws_sink_server().await;
        let (addr_c, acc_c, task_c) = spawn_ws_sink_server().await;
        let (addr_d, acc_d, task_d) = spawn_ws_sink_server().await;

        let mk_up = |id: u128, name: &str, addr: std::net::SocketAddr, enabled: bool| {
            let mut rec = crate::app::tests::websocket_upstream(
                id,
                name,
                url::Url::parse(&format!("http://{addr}/")).unwrap(),
            );
            rec.enabled = enabled;
            rec
        };
        let up_a = mk_up(21, "api-a", addr_a, true);
        let up_b = mk_up(22, "api-b", addr_b, true);
        let up_c = mk_up(23, "api-c", addr_c, true);
        let up_d = mk_up(24, "api-d", addr_d, true);

        let state = crate::app::tests::websocket_test_state(
            vec![up_a.clone(), up_b.clone(), up_c.clone(), up_d.clone()],
            cc_lb_domain::TerminalStrategy::FirstPick,
            DrainController::new(),
            Arc::new(crate::app::tests::WebSocketTestSignerFactory),
            Arc::new(crate::app::tests::FixtureKeyStore),
            vec!["*".to_owned()],
        )
        .await;
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(crate::app::tests::TEST_KEY.plaintext.as_str()).unwrap(),
        );
        let auth = state.lifecycle.authenticate(&headers).await.unwrap();
        let mut gateway = ResponsesWebSocketGateway::new(
            state.lifecycle.clone(),
            &auth,
            headers,
            state.body_caps.messages,
            3,
        );

        async fn connect_existing(
            state: &ProxyState,
            upstream_id: uuid::Uuid,
            upstream_name: &str,
            addr: std::net::SocketAddr,
        ) -> WsProviderConnection {
            let (ws_client, _) =
                tokio_tungstenite::connect_async(format!("ws://{addr}/v1/responses"))
                    .await
                    .expect("connect existing mock socket");
            let (sink, _stream) = ws_client.split();
            let bulkhead = state
                .websocket_bulkheads
                .semaphore_only_bulkhead(
                    websocket_breaker_key(upstream_id),
                    state.websocket_bulkhead_config,
                )
                .acquire_owned()
                .await
                .expect("bulkhead permit");
            WsProviderConnection {
                upstream_id,
                upstream_name: upstream_name.to_owned(),
                sink,
                task: None,
                lane: None,
                _bulkhead_guard: bulkhead,
            }
        }
        let mut connections = HashMap::new();
        connections.insert(1, connect_existing(&state, up_a.id, "api-a", addr_a).await);
        connections.insert(2, connect_existing(&state, up_b.id, "api-b", addr_b).await);

        let swap_records = |records: Vec<UpstreamRecord>| {
            let view = state.dynamic_view.load();
            state.dynamic_view.store(
                cc_lb_control::DynamicViewBuilder::from_view(&view)
                    .upstream_records(records)
                    .build(),
            );
        };

        // Turn 1 (`conn 1`, `api-a`) has replacement `[api-c]`; seed an owner on `conn 1` so it stays non-idle.
        swap_records(vec![
            mk_up(21, "api-a", addr_a, true),
            mk_up(22, "api-b", addr_b, false),
            mk_up(23, "api-c", addr_c, true),
            mk_up(24, "api-d", addr_d, false),
        ]);
        let seed1 = gateway
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "stream_id": "lane-1",
                        "store": false,
                        "input": []
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        gateway.assign_connection(seed1.turn_id, 1, up_a.id, "api-a");
        gateway.commit_send_attempt(seed1.turn_id);
        gateway.sent(seed1.turn_id);
        gateway
            .provider_message(
                1,
                &Bytes::from_static(br#"{"type":"response.created","stream_id":"lane-1","response":{"id":"resp_seed_1","status":"in_progress","store":false}}"#),
            )
            .await
            .unwrap();
        gateway
            .provider_message(
                1,
                &Bytes::from_static(br#"{"type":"response.completed","stream_id":"lane-1","response":{"id":"resp_seed_1","status":"completed","store":false}}"#),
            )
            .await
            .unwrap();
        let t1 = gateway
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "stream_id": "lane-1",
                        "store": false,
                        "input": [{"role": "user", "content": "turn 1"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            t1.candidates
                .iter()
                .map(|c| c.upstream_id)
                .collect::<Vec<_>>(),
            vec![up_a.id, up_c.id]
        );
        gateway.assign_connection(t1.turn_id, 1, up_a.id, "api-a");
        gateway.commit_send_attempt(t1.turn_id);
        gateway.sent(t1.turn_id);

        // Turn 2 (`conn 2`, `api-b`) has replacement `[api-d]`; seed an owner on `conn 2` so it stays non-idle.
        swap_records(vec![
            mk_up(21, "api-a", addr_a, false),
            mk_up(22, "api-b", addr_b, true),
            mk_up(23, "api-c", addr_c, false),
            mk_up(24, "api-d", addr_d, true),
        ]);
        let seed2 = gateway
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "stream_id": "lane-2",
                        "store": false,
                        "input": []
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        gateway.assign_connection(seed2.turn_id, 2, up_b.id, "api-b");
        gateway.commit_send_attempt(seed2.turn_id);
        gateway.sent(seed2.turn_id);
        gateway
            .provider_message(
                2,
                &Bytes::from_static(br#"{"type":"response.created","stream_id":"lane-2","response":{"id":"resp_seed_2","status":"in_progress","store":false}}"#),
            )
            .await
            .unwrap();
        gateway
            .provider_message(
                2,
                &Bytes::from_static(br#"{"type":"response.completed","stream_id":"lane-2","response":{"id":"resp_seed_2","status":"completed","store":false}}"#),
            )
            .await
            .unwrap();
        let t2 = gateway
            .client_message(
                auth,
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "stream_id": "lane-2",
                        "store": false,
                        "input": [{"role": "user", "content": "turn 2"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            t2.candidates
                .iter()
                .map(|c| c.upstream_id)
                .collect::<Vec<_>>(),
            vec![up_b.id, up_d.id]
        );
        gateway.assign_connection(t2.turn_id, 2, up_b.id, "api-b");
        gateway.commit_send_attempt(t2.turn_id);
        gateway.sent(t2.turn_id);

        // Re-enable all 4 upstreams so slot capacity is the only constraint when both 429 frames arrive in one batch.
        swap_records(vec![up_a, up_b, up_c.clone(), up_d.clone()]);
        acc_c.store(0, AtomicOrdering::SeqCst);
        acc_d.store(0, AtomicOrdering::SeqCst);

        let (events, mut event_rx) = tokio::sync::mpsc::channel(16);
        events
            .send(WsProviderEvent::Frame {
                connection_id: 1,
                bytes: Bytes::from_static(br#"{"type":"error","stream_id":"lane-1","error":{"code":"rate_limit_exceeded","message":"429 from api-a"}}"#),
                text: true,
            })
            .await
            .unwrap();
        events
            .send(WsProviderEvent::Frame {
                connection_id: 2,
                bytes: Bytes::from_static(br#"{"type":"error","stream_id":"lane-2","error":{"code":"rate_limit_exceeded","message":"429 from api-b"}}"#),
                text: true,
            })
            .await
            .unwrap();

        let (mut downstream_sink, mut client_ws, sink_server) = spawn_test_downstream_sink().await;
        let mut next_connection_id = 3;
        let mut pending_connects = HashMap::new();
        let mut connect_tasks = tokio::task::JoinSet::new();
        let deferred = VecDeque::new();
        let mut dispatches = VecDeque::new();
        let mut carried_budgets = HashMap::new();

        assert!(
            drain_buffered_ws_events_snapshot(
                &mut event_rx,
                &mut gateway,
                &mut connections,
                &pending_connects,
                &deferred,
                &mut dispatches,
                &mut downstream_sink,
                2,
            )
            .await
        );
        assert_eq!(
            dispatches.len(),
            1,
            "single free slot must be reserved by Turn 1 only, not double-promised to Turn 2"
        );
        assert_eq!(dispatches[0].stream_id.as_deref(), Some("lane-1"));
        assert_eq!(dispatches[0].candidates[0].upstream_id, up_c.id);

        let forwarded = tokio::time::timeout(Duration::from_secs(2), client_ws.next())
            .await
            .expect("Turn 2 original 429 is forwarded immediately")
            .unwrap()
            .unwrap();
        let forwarded_json: serde_json::Value =
            serde_json::from_str(forwarded.to_text().unwrap()).unwrap();
        assert_eq!(forwarded_json["type"], "error");
        assert_eq!(forwarded_json["stream_id"], "lane-2");
        assert_eq!(forwarded_json["error"]["code"], "rate_limit_exceeded");
        assert_eq!(forwarded_json["error"]["message"], "429 from api-b");

        let mut deferred_mut = deferred;
        assert!(
            flush_pending_ws_dispatches(
                &mut gateway,
                &state,
                &mut connections,
                &events,
                &mut next_connection_id,
                3,
                &mut pending_connects,
                &mut connect_tasks,
                &mut deferred_mut,
                &mut dispatches,
                &mut carried_budgets,
                &mut downstream_sink,
            )
            .await
        );
        let outcome = tokio::time::timeout(Duration::from_secs(2), connect_tasks.join_next())
            .await
            .expect("Turn 1 connects to api-c")
            .expect("connect task joined")
            .expect("join ok");
        let errs = handle_connect_outcome(
            &mut gateway,
            outcome,
            &state,
            &mut connections,
            &events,
            &mut next_connection_id,
            3,
            &mut pending_connects,
            &mut connect_tasks,
            &mut deferred_mut,
            &mut dispatches,
            &mut carried_budgets,
        )
        .await;
        assert!(errs.is_empty(), "Turn 1 recovery must not fail: {errs:?}");
        assert_eq!(acc_c.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(acc_d.load(AtomicOrdering::SeqCst), 0);

        sink_server.abort();
        task_a.abort();
        task_b.abort();
        task_c.abort();
        task_d.abort();
    }
}
