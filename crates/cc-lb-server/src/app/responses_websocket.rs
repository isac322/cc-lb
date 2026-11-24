use super::*;

pub(super) async fn responses_websocket_handler(
    State(state): State<ProxyState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
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
    task: JoinHandle<()>,
    lane: Option<String>,
    _bulkhead_guard: cc_lb_engine::OwnedBulkheadGuard,
}

impl Drop for WsProviderConnection {
    fn drop(&mut self) {
        self.task.abort();
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
        request,
        bulkhead_guard,
        DEFAULT_WS_PROVIDER_CONNECT_TIMEOUT,
    )
    .await
}

async fn connect_provider_websocket_with_permit(
    permit: cc_lb_engine::Permit,
    message_cap: usize,
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
    let result = tokio::time::timeout(connect_timeout, async move {
        let mut connect_timings = WsConnectTimings::default();
        let ip_literal = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .ok();
        let addrs: Vec<std::net::SocketAddr> = match ip_literal {
            Some(ip) => {
                connect_timings.dns_ms = Some(0);
                vec![std::net::SocketAddr::new(ip, port)]
            }
            None => {
                let dns_started = std::time::Instant::now();
                let resolved = tokio::net::lookup_host((host.as_str(), port))
                    .await
                    .map_err(tokio_tungstenite::tungstenite::Error::Io)?;
                connect_timings.dns_ms =
                    Some(dns_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
                resolved.collect()
            }
        };
        if addrs.is_empty() {
            return Err(tokio_tungstenite::tungstenite::Error::Io(
                std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "DNS resolution returned no socket addresses",
                ),
            ));
        }
        let connect_started = std::time::Instant::now();
        let tcp = TcpStream::connect(addrs.as_slice())
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
pub(super) const WS_DISPATCH_CONNECT_BUDGET: Duration = Duration::from_secs(30);
pub(super) const WS_MAX_UPSTREAM_ATTEMPTS: usize = 3;

struct ResponsesProviderParams {
    message_cap: usize,
    permit: cc_lb_engine::Permit,
    connect_timeout: Duration,
    request: http::Request<()>,
    connection_id: u64,
    upstream_id: uuid::Uuid,
    lane: Option<String>,
    upstream_name: String,
    bulkhead_guard: cc_lb_engine::OwnedBulkheadGuard,
    events: tokio::sync::mpsc::Sender<WsProviderEvent>,
}

async fn start_responses_provider(
    params: ResponsesProviderParams,
) -> Result<(WsProviderConnection, WsConnectTimings), WebSocketError> {
    let ResponsesProviderParams {
        message_cap,
        permit,
        connect_timeout,
        request,
        connection_id,
        upstream_id,
        upstream_name,
        lane,
        bulkhead_guard,
        events,
    } = params;
    let (upstream, _response_headers, bulkhead_guard, connect_timings) =
        connect_provider_websocket_with_permit(
            permit,
            message_cap,
            request,
            bulkhead_guard,
            connect_timeout,
        )
        .await?;
    let (sink, mut stream) = upstream.split();
    let task = tokio::spawn(async move {
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
    });
    Ok((
        WsProviderConnection {
            upstream_id,
            upstream_name,
            lane,
            sink,
            task,
            _bulkhead_guard: bulkhead_guard,
        },
        connect_timings,
    ))
}

pub(super) fn responses_websocket_config(message_cap: usize) -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(message_cap);
    config.max_frame_size = Some(message_cap);
    config
}

pub(super) fn responses_websocket_request(
    upstream_url: url::Url,
    upstream_headers: HeaderMap,
) -> Result<http::Request<()>, String> {
    let mut request = upstream_url
        .as_str()
        .into_client_request()
        .map_err(|error| format!("upstream WebSocket request could not be built: {error}"))?;
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
        if let Some(connection) = connections.remove(&connection_id) {
            connection.task.abort();
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
    dispatch: WebSocketDispatch,
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
            if let Some(connection) = connections.remove(&connection_id) {
                connection.task.abort();
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

async fn dispatch_responses_websocket(
    gateway: &mut ResponsesWebSocketGateway,
    dispatch: WebSocketDispatch,
    state: &ProxyState,
    connections: &mut HashMap<u64, WsProviderConnection>,
    events: &tokio::sync::mpsc::Sender<WsProviderEvent>,
    next_connection_id: &mut u64,
    connection_capacity: usize,
) -> (Vec<WebSocketDispatch>, Vec<WebSocketError>) {
    dispatch_responses_websocket_with_limits(
        gateway,
        dispatch,
        state,
        connections,
        events,
        next_connection_id,
        connection_capacity,
        WS_PROVIDER_CONNECT_TIMEOUT,
        WS_DISPATCH_CONNECT_BUDGET,
        WS_MAX_UPSTREAM_ATTEMPTS,
    )
    .await
}

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
    let mut last_error = None;
    let mut attempts = 0usize;
    let deadline = tokio::time::Instant::now() + connect_budget;
    if let Some(connection_id) = dispatch.connection_id {
        return send_responses_dispatch(
            gateway,
            dispatch,
            connection_id,
            connections,
            cc_lb_engine::responses_websocket::ResponsesDispatchTimings::reused(),
            deadline,
        )
        .await;
    }

    let mut pending: VecDeque<cc_lb_engine::responses_websocket::WebSocketCandidate> =
        dispatch.candidates.iter().cloned().collect();
    while !pending.is_empty() {
        let reserve_api_key = max_attempts > 1
            && attempts + 1 == max_attempts
            && dispatch.prefer_api_key_on_final_attempt;
        let candidate = if reserve_api_key && !pending[0].is_api_key {
            if let Some(pos) = pending.iter().position(|candidate| candidate.is_api_key) {
                pending.remove(pos).expect("api-key candidate exists")
            } else {
                pending.pop_front().expect("pending queue is non-empty")
            }
        } else {
            pending.pop_front().expect("pending queue is non-empty")
        };
        let existing = connections.iter().find_map(|(id, connection)| {
            (connection.upstream_id == candidate.upstream_id
                && (!candidate.socket_per_lane || connection.lane == dispatch.stream_id))
                .then_some(*id)
        });
        if let Some(connection_id) = existing {
            if deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .is_zero()
            {
                last_error.get_or_insert_with(|| {
                    WebSocketError::new(
                        504,
                        "upstream_unavailable",
                        "provider WebSocket connection timed out",
                    )
                });
                break;
            }
            let has_remaining_candidates = has_remaining_ws_candidates(
                &pending,
                connections,
                dispatch.stream_id.as_deref(),
                attempts,
                max_attempts,
            );
            match tokio::time::timeout_at(
                deadline,
                gateway.validate_candidate_for_send(
                    dispatch.turn_id,
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
                        dispatch,
                        connection_id,
                        connections,
                        cc_lb_engine::responses_websocket::ResponsesDispatchTimings::reused(),
                    )
                    .await;
                }
                Ok(Err(error)) => {
                    let fatal = matches!(
                        error.code,
                        "authentication_error" | "reservation_expired" | "response_not_found"
                    );
                    last_error = Some(error);
                    if fatal {
                        break;
                    }
                    continue;
                }
                Err(_) => {
                    last_error = Some(WebSocketError::new(
                        504,
                        "upstream_unavailable",
                        "provider WebSocket connection timed out",
                    ));
                    break;
                }
            }
        }
        if connections.len() >= connection_capacity {
            last_error = Some(WebSocketError::new(
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
                last_error = Some(error);
                continue;
            }
        };
        if attempts >= max_attempts {
            drop(permit);
            break;
        }
        if deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .is_zero()
        {
            drop(permit);
            last_error.get_or_insert_with(|| {
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
            deadline,
            gateway.validate_fallback_candidate(&dispatch, &candidate),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(permit);
                let fatal = matches!(
                    error.code,
                    "authentication_error" | "reservation_expired" | "response_not_found"
                );
                last_error = Some(error);
                if fatal {
                    break;
                }
                continue;
            }
            Err(_) => {
                drop(permit);
                last_error = Some(WebSocketError::new(
                    504,
                    "upstream_unavailable",
                    "provider WebSocket connection timed out",
                ));
                break;
            }
        }
        attempts += 1;
        let attempt_deadline = (tokio::time::Instant::now() + per_attempt_timeout).min(deadline);
        let mut bulkhead_config = state.websocket_bulkhead_config;
        bulkhead_config.semaphore_permits = bulkhead_config.max_conns_per_upstream;
        let bulkhead = state
            .websocket_bulkheads
            .bulkhead(candidate.upstream_name.clone(), bulkhead_config);
        let bulkhead_started = std::time::Instant::now();
        let bulkhead_guard =
            match tokio::time::timeout_at(attempt_deadline, bulkhead.acquire_owned()).await {
                Ok(Ok(guard)) => guard,
                Ok(Err(error)) => {
                    drop(permit);
                    last_error = Some(WebSocketError::new(
                        503,
                        "overloaded_error",
                        error.to_string(),
                    ));
                    break;
                }
                Err(_) => {
                    drop(permit);
                    last_error = Some(WebSocketError::new(
                        504,
                        "upstream_unavailable",
                        "provider WebSocket connection timed out",
                    ));
                    if tokio::time::Instant::now() >= deadline {
                        break;
                    }
                    continue;
                }
            };
        let bulkhead_wait_ms = bulkhead_started
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let prepared = match tokio::time::timeout_at(
            attempt_deadline,
            gateway.prepare_connection(&dispatch, &candidate),
        )
        .await
        {
            Ok(Ok(prepared)) => prepared,
            Ok(Err(error)) => {
                drop(bulkhead_guard);
                drop(permit);
                let authentication_failed = error.code == "authentication_error";
                last_error = Some(error);
                if authentication_failed {
                    break;
                }
                continue;
            }
            Err(_) => {
                drop(bulkhead_guard);
                drop(permit);
                last_error = Some(WebSocketError::new(
                    504,
                    "upstream_unavailable",
                    "provider WebSocket connection timed out",
                ));
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                continue;
            }
        };
        let request = match responses_websocket_request(prepared.0, prepared.1) {
            Ok(request) => request,
            Err(error) => {
                drop(bulkhead_guard);
                drop(permit);
                last_error = Some(WebSocketError::new(502, "upstream_unavailable", error));
                continue;
            }
        };
        let attempt_timeout =
            attempt_deadline.saturating_duration_since(tokio::time::Instant::now());
        if attempt_timeout.is_zero() {
            drop(bulkhead_guard);
            drop(permit);
            last_error = Some(WebSocketError::new(
                504,
                "upstream_unavailable",
                "provider WebSocket connection timed out",
            ));
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            continue;
        }
        let Some(next_id) = next_connection_id.checked_add(1) else {
            drop(bulkhead_guard);
            drop(permit);
            last_error = Some(WebSocketError::new(
                503,
                "overloaded_error",
                "WebSocket connection identifiers are exhausted",
            ));
            break;
        };
        let connection_id = *next_connection_id;
        *next_connection_id = next_id;
        let (connection, connect_timings) =
            match start_responses_provider(ResponsesProviderParams {
                message_cap: state.body_caps.messages,
                permit,
                connect_timeout: attempt_timeout,
                request,
                connection_id,
                upstream_id: candidate.upstream_id,
                lane: dispatch.stream_id.clone(),
                upstream_name: candidate.upstream_name.clone(),
                bulkhead_guard,
                events: events.clone(),
            })
            .await
            {
                Ok(connection) => connection,
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };
        let dispatch_timings = cc_lb_engine::responses_websocket::ResponsesDispatchTimings {
            connection_reused: Some(false),
            bulkhead_wait_ms: Some(bulkhead_wait_ms),
            dns_ms: connect_timings.dns_ms,
            connect_ms: connect_timings.connect_ms,
        };
        let has_remaining_candidates = has_remaining_ws_candidates(
            &pending,
            connections,
            dispatch.stream_id.as_deref(),
            attempts,
            max_attempts,
        );
        match tokio::time::timeout_at(
            attempt_deadline,
            gateway.validate_candidate_for_send(
                dispatch.turn_id,
                candidate.upstream_id,
                None,
                has_remaining_candidates,
            ),
        )
        .await
        {
            Ok(Ok(())) => {
                connections.insert(connection_id, connection);
                return commit_and_send_responses_dispatch(
                    gateway,
                    dispatch,
                    connection_id,
                    connections,
                    dispatch_timings,
                )
                .await;
            }
            Ok(Err(error)) => {
                drop(connection);
                let fatal = matches!(
                    error.code,
                    "authentication_error" | "reservation_expired" | "response_not_found"
                );
                last_error = Some(error);
                if fatal {
                    break;
                }
                continue;
            }
            Err(_) => {
                drop(connection);
                last_error = Some(WebSocketError::new(
                    504,
                    "upstream_unavailable",
                    "provider WebSocket connection timed out",
                ));
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                continue;
            }
        }
    }
    let error = last_error
        .unwrap_or_else(|| {
            WebSocketError::new(
                502,
                "upstream_unavailable",
                "No Responses WebSocket upstream is available",
            )
        })
        .with_stream(dispatch.stream_id);
    let ready = gateway.fail_dispatch(dispatch.turn_id, &error).await;
    (ready, vec![error])
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
                if let Some(connection) = connections.remove(&connection_id) {
                    connection.task.abort();
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
    dispatches: &mut VecDeque<WebSocketDispatch>,
    downstream_sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> bool {
    while let Some(dispatch) = dispatches.pop_front() {
        let (ready, errors) = dispatch_responses_websocket(
            gateway,
            dispatch,
            state,
            connections,
            events,
            next_connection_id,
            capacity,
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
    let mut buffered = Vec::with_capacity(max_events.min(MAX_WS_EVENT_DRAIN_BATCH));
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
    let snapshot_limit = event_rx.max_capacity().clamp(1, MAX_WS_EVENT_DRAIN_BATCH);
    if !drain_buffered_ws_events_snapshot(
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
                let snapshot_limit = capacity.clamp(1, MAX_WS_EVENT_DRAIN_BATCH);
                if !drain_buffered_ws_events_snapshot(
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
        connection.task.abort();
        let _ = (&mut connection.task).await;
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
