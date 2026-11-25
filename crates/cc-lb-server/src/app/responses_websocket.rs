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

async fn send_responses_dispatch(
    gateway: &mut ResponsesWebSocketGateway,
    mut dispatch: WebSocketDispatch,
    connection_id: u64,
    connections: &mut HashMap<u64, WsProviderConnection>,
    dispatch_timings: cc_lb_engine::responses_websocket::ResponsesDispatchTimings,
    deadline: tokio::time::Instant,
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
    match tokio::time::timeout_at(
        deadline,
        gateway.validate_candidate_for_send(
            dispatch.turn_id,
            upstream_id,
            Some(connection_id),
            false,
        ),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let ready = gateway.fail_dispatch(dispatch.turn_id, &error).await;
            return (ready, vec![error]);
        }
        Err(_) => {
            let error = WebSocketError::new(
                504,
                "upstream_unavailable",
                "provider WebSocket connection timed out",
            )
            .with_stream(dispatch.stream_id.clone());
            let ready = gateway.fail_dispatch(dispatch.turn_id, &error).await;
            return (ready, vec![error]);
        }
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

struct PendingDispatchState {
    dispatch: WebSocketDispatch,
    remaining: VecDeque<cc_lb_engine::responses_websocket::WebSocketCandidate>,
    attempts: usize,
    max_attempts: usize,
    deadline: tokio::time::Instant,
    per_attempt_timeout: Duration,
    last_error: Option<WebSocketError>,
    pinned_to_single_owner: bool,
}

impl PendingDispatchState {
    fn new(
        dispatch: WebSocketDispatch,
        per_attempt_timeout: Duration,
        connect_budget: Duration,
        max_attempts: usize,
    ) -> Self {
        let remaining = dispatch.candidates.iter().cloned().collect();
        let pinned_to_single_owner = dispatch.candidates.len() <= 1;
        Self {
            dispatch,
            remaining,
            attempts: 0,
            max_attempts,
            deadline: bounded_deadline(tokio::time::Instant::now(), connect_budget),
            per_attempt_timeout,
            last_error: None,
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

async fn finish_failed_dispatch(
    gateway: &mut ResponsesWebSocketGateway,
    pending: PendingDispatchState,
) -> (Vec<WebSocketDispatch>, Vec<WebSocketError>) {
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
    let ready = gateway
        .fail_dispatch(pending.dispatch.turn_id, &error)
        .await;
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
) -> (Vec<WebSocketDispatch>, Vec<WebSocketError>) {
    if let Some(connection_id) = pending.dispatch.connection_id {
        return send_responses_dispatch(
            gateway,
            pending.dispatch,
            connection_id,
            connections,
            cc_lb_engine::responses_websocket::ResponsesDispatchTimings::reused(),
            pending.deadline,
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
                pending.last_error = Some(error.with_stream(pending.dispatch.stream_id.clone()));
                return finish_failed_dispatch(gateway, pending).await;
            }
            match tokio::time::timeout_at(
                pending.deadline,
                gateway.validate_candidate_for_send(
                    pending.dispatch.turn_id,
                    candidate.upstream_id,
                    Some(connection_id),
                    has_remaining_candidates,
                ),
            )
            .await
            {
                Ok(Ok(())) => {
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
                    let fatal = is_fatal_ws_connect_error(&error);
                    pending.last_error = Some(error);
                    if fatal {
                        break;
                    }
                    continue;
                }
                Err(_) => {
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
            pending.last_error = Some(error.with_stream(pending.dispatch.stream_id.clone()));
            return finish_failed_dispatch(gateway, pending).await;
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

        if connections.len() + pending_connects.len() >= connection_capacity {
            pending.last_error = Some(WebSocketError::new(
                503,
                "overloaded_error",
                "WebSocket provider connection budget is full",
            ));
            break;
        }
        // Skip open circuit breakers without burning a retry slot or bulkhead/signing work.
        let permit = match acquire_websocket_breaker_permit(state, candidate.upstream_id) {
            Ok(permit) => permit,
            Err(error) => {
                pending.last_error = Some(error);
                continue;
            }
        };
        if pending.attempts >= pending.max_attempts {
            drop(permit);
            break;
        }
        if pending
            .deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .is_zero()
        {
            drop(permit);
            pending.last_error.get_or_insert_with(|| {
                WebSocketError::new(
                    504,
                    "upstream_unavailable",
                    "provider WebSocket connection timed out",
                )
            });
            break;
        }
        // Recheck fresh quota and routing eligibility before burning an attempt
        // slot or acquiring the bulkhead so an unpinned fallback whose quota just
        // exhausted skips cleanly to the next eligible upstream.
        match tokio::time::timeout_at(
            pending.deadline,
            gateway.validate_fallback_candidate(&pending.dispatch, &candidate),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(permit);
                let fatal = is_fatal_ws_connect_error(&error);
                pending.last_error = Some(error);
                if fatal {
                    break;
                }
                continue;
            }
            Err(_) => {
                drop(permit);
                pending.last_error = Some(WebSocketError::new(
                    504,
                    "upstream_unavailable",
                    "provider WebSocket connection timed out",
                ));
                break;
            }
        }
        pending.attempts += 1;
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

    finish_failed_dispatch(gateway, pending).await
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

            for mut entry in candidates {
                let now = tokio::time::Instant::now();
                if now >= entry.deadline {
                    entry.last_error = Some(WebSocketError::new(
                        504,
                        "upstream_unavailable",
                        "provider WebSocket connection timed out",
                    ));
                    let (ready, errs) = finish_failed_dispatch(gateway, entry).await;
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
                    entry.last_error = Some(error.with_stream(entry.dispatch.stream_id.clone()));
                    let (ready, errs) = finish_failed_dispatch(gateway, entry).await;
                    dispatches.extend(ready);
                    errors.extend(errs);
                    continue;
                }
                match tokio::time::timeout_at(
                    entry.deadline,
                    gateway.validate_candidate_for_send(
                        entry.dispatch.turn_id,
                        upstream_id,
                        existing_conn,
                        has_remaining_candidates,
                    ),
                )
                .await
                {
                    Ok(Ok(())) => {
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
                        let fatal = is_fatal_ws_connect_error(&error);
                        entry.last_error = Some(error);
                        if fatal {
                            let (ready, errs) = finish_failed_dispatch(gateway, entry).await;
                            dispatches.extend(ready);
                            errors.extend(errs);
                        } else {
                            resumed.push_back(entry);
                        }
                    }
                    Err(_) => {
                        entry.last_error = Some(WebSocketError::new(
                            504,
                            "upstream_unavailable",
                            "provider WebSocket connection timed out",
                        ));
                        let (ready, errs) = finish_failed_dispatch(gateway, entry).await;
                        dispatches.extend(ready);
                        errors.extend(errs);
                    }
                }
            }
        }
        Err(ConnectAttemptError::Prepare(error)) => {
            let now = tokio::time::Instant::now();
            let mut primary = pending.primary;
            primary.last_error = Some(error.clone());
            if is_fatal_ws_connect_error(&error) || now >= primary.deadline {
                let (ready, errs) = finish_failed_dispatch(gateway, primary).await;
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
                    let (ready, errs) = finish_failed_dispatch(gateway, entry).await;
                    dispatches.extend(ready);
                    errors.extend(errs);
                } else {
                    entry.remaining.push_front(pending.active_candidate.clone());
                    resumed.push_back(entry);
                }
            }
        }
        Err(ConnectAttemptError::Dial(error)) => {
            let now = tokio::time::Instant::now();
            for mut entry in std::iter::once(pending.primary).chain(pending.waiters) {
                entry.last_error = Some(error.clone());
                if is_fatal_ws_connect_error(&error) || now >= entry.deadline {
                    let (ready, errs) = finish_failed_dispatch(gateway, entry).await;
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
                let (ready, errs) = finish_failed_dispatch(gateway, deferred).await;
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
                        503,
                        "upstream_affinity_unavailable",
                        "The session is pinned to another OAuth account",
                    ));
                    let (ready, errs) = finish_failed_dispatch(gateway, deferred).await;
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
        )
        .await;
        dispatches.extend(ready);
        errors.extend(errs);
    }

    errors
}

async fn expire_deferred_for_session_owner(
    gateway: &mut ResponsesWebSocketGateway,
    deferred_for_session_owner: &mut VecDeque<PendingDispatchState>,
    dispatches: &mut VecDeque<WebSocketDispatch>,
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
            let (ready, errs) = finish_failed_dispatch(gateway, entry).await;
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
                    &mut ready_dispatches,
                )
                .await;
                errors.extend(errs);
            }
        }
    }
    (ready_dispatches.into_iter().collect(), errors)
}

async fn handle_ws_provider_event(
    provider: WsProviderEvent,
    gateway: &mut ResponsesWebSocketGateway,
    connections: &mut HashMap<u64, WsProviderConnection>,
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
            match gateway.provider_message(connection_id, &bytes).await {
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
    downstream_sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> bool {
    while let Some(dispatch) = dispatches.pop_front() {
        let (ready, errors) = advance_one_dispatch(
            gateway,
            PendingDispatchState::new(
                dispatch,
                WS_PROVIDER_CONNECT_TIMEOUT,
                WS_DISPATCH_CONNECT_BUDGET,
                WS_MAX_UPSTREAM_ATTEMPTS,
            ),
            state,
            connections,
            events,
            next_connection_id,
            capacity,
            pending_connects,
            connect_tasks,
            deferred_for_session_owner,
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

async fn drain_buffered_ws_events_snapshot(
    event_rx: &mut tokio::sync::mpsc::Receiver<WsProviderEvent>,
    gateway: &mut ResponsesWebSocketGateway,
    connections: &mut HashMap<u64, WsProviderConnection>,
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
        if !handle_ws_provider_event(provider, gateway, connections, dispatches, downstream_sink)
            .await
        {
            return false;
        }
    }
    true
}

pub(super) async fn drain_buffered_ws_events_before_expiry(
    event_rx: &mut tokio::sync::mpsc::Receiver<WsProviderEvent>,
    gateway: &mut ResponsesWebSocketGateway,
    connections: &mut HashMap<u64, WsProviderConnection>,
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
    let mut next_connection_id = 1_u64;
    let heartbeat_period = cc_lb_lifecycle::DEFAULT_STREAM_ACTIVITY_HEARTBEAT_INTERVAL;
    let mut activity_heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + heartbeat_period,
        heartbeat_period,
    );
    activity_heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

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
            tokio::select! {
                _ = activity_heartbeat.tick(), if gateway.has_active_turns() => {
                    gateway.observe_active_turns_heartbeat();
                }
                _ = async {
                    if let Some(deadline) = pending_deadline {
                        tokio::time::sleep_until(deadline).await;
                    }
                }, if pending_deadline.is_some() => {
                    if !drain_buffered_ws_events_before_expiry(
                        &mut event_rx,
                        &mut gateway,
                        &mut connections,
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
                        &mut dispatches,
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
    let draining =
        run_responses_websocket_until_cancelled(body, &client_closed, &state.drain_controller)
            .await;

    connect_tasks.abort_all();
    while connect_tasks.join_next().await.is_some() {}
    pending_connects.clear();
    if draining {
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
        )
        .await;
        assert!(r2.is_empty() && e2.is_empty());
        assert_eq!(pending_connects.values().next().unwrap().waiters.len(), 1);

        // Wait for Lane 1's prepare_job.run() timeout outcome; verify ConnectAttemptError::Prepare
        // fails only Lane 1 (504) while restoring candidate to Lane 2 and re-dispatching it cleanly.
        let mut ready = VecDeque::new();
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
}
