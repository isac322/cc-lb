use super::*;
use tracing::instrument::WithSubscriber as _;

fn downstream_close_frame(
    frame: axum::extract::ws::CloseFrame,
) -> tokio_tungstenite::tungstenite::protocol::CloseFrame {
    let code = frame.code;
    tokio_tungstenite::tungstenite::protocol::CloseFrame {
        code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(code),
        reason: frame.reason.to_string().into(),
    }
}
fn downstream_close_termination(
    frame: Option<&axum::extract::ws::CloseFrame>,
) -> cc_lb_engine::realtime_websocket::RealtimeWebSocketTermination {
    match frame {
        Some(frame) if frame.code == 1000 => {
            cc_lb_engine::realtime_websocket::RealtimeWebSocketTermination::DownstreamClosed
        }
        _ => cc_lb_engine::realtime_websocket::RealtimeWebSocketTermination::DownstreamCancelled,
    }
}

fn upstream_close_frame(
    frame: tokio_tungstenite::tungstenite::protocol::CloseFrame,
) -> axum::extract::ws::CloseFrame {
    let code: u16 = frame.code.into();
    axum::extract::ws::CloseFrame {
        code,
        reason: frame.reason.to_string().into(),
    }
}
fn realtime_payload_kind(payload: &[u8], binary: bool) -> cc_lb_upstream::RealtimeFrameKind {
    if binary {
        cc_lb_upstream::RealtimeFrameKind::Binary
    } else if sonic_rs::from_slice::<serde_json::Value>(payload).is_ok() {
        cc_lb_upstream::RealtimeFrameKind::Json
    } else {
        cc_lb_upstream::RealtimeFrameKind::Text
    }
}

fn realtime_data_frame(message: &UpstreamMessage) -> bool {
    matches!(
        message,
        UpstreamMessage::Text(_) | UpstreamMessage::Binary(_)
    )
}

fn transform_realtime_message(
    session: &RealtimeWebSocketSession,
    direction: cc_lb_upstream::RealtimeFrameDirection,
    message: UpstreamMessage,
) -> Result<Option<UpstreamMessage>, ()> {
    if !session.has_frame_transform() {
        return Ok(Some(message));
    }
    let (kind, payload) = match &message {
        UpstreamMessage::Text(frame) => (
            realtime_payload_kind(frame.as_bytes(), false),
            Bytes::from(frame.clone()),
        ),
        UpstreamMessage::Binary(frame) => (realtime_payload_kind(frame, true), frame.clone()),
        _ => return Ok(Some(message)),
    };
    match session
        .transform_frame(direction, kind, payload)
        .map_err(|_| ())?
    {
        cc_lb_upstream::TransformRealtimeFrameResult::Unchanged => Ok(Some(message)),
        cc_lb_upstream::TransformRealtimeFrameResult::Drop => Ok(None),
        cc_lb_upstream::TransformRealtimeFrameResult::Replace { kind, payload } => {
            Ok(Some(match kind {
                cc_lb_upstream::RealtimeFrameKind::Binary => UpstreamMessage::Binary(payload),
                cc_lb_upstream::RealtimeFrameKind::Json
                | cc_lb_upstream::RealtimeFrameKind::Text => UpstreamMessage::Text(
                    String::from_utf8(payload.to_vec()).map_err(|_| ())?.into(),
                ),
            }))
        }
    }
}

type RealtimeProvider = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn websocket_http_error(error: &WebSocketError) -> Response<Body> {
    let mut response = Response::new(Body::from(error.to_bytes()));
    *response.status_mut() = StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY);
    error.copy_provider_headers(response.headers_mut());
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/json"));
    response
}
fn spawn_realtime_usage_worker(
    kind: RealtimeWebSocketKind,
    observer: Option<cc_lb_engine::realtime_websocket::RealtimeUsageObserver>,
) -> (
    tokio::sync::mpsc::UnboundedSender<Bytes>,
    JoinHandle<RealtimeUsage>,
) {
    let (usage_tx, mut usage_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    let usage_worker = tokio::spawn(async move {
        let mut usage = observer
            .map(RealtimeUsage::with_observer)
            .unwrap_or_default();
        while let Some(frame) = usage_rx.recv().await {
            usage.observe(kind, &frame);
        }
        usage
    });
    (usage_tx, usage_worker)
}

async fn connect_realtime_provider(
    state: &ProxyState,
    session: &RealtimeWebSocketSession,
) -> Result<
    (
        RealtimeProvider,
        HeaderMap,
        cc_lb_engine::OwnedBulkheadGuard,
    ),
    WebSocketError,
> {
    if state.websocket_bulkhead_config.semaphore_permits == 0 {
        return Err(WebSocketError::new(
            503,
            "overloaded_error",
            "WebSocket connection budget is full",
        ));
    }
    let mut config = state.websocket_bulkhead_config;
    config.semaphore_permits = config.max_conns_per_upstream;
    let operation = async {
        let guard = state
            .websocket_bulkheads
            .bulkhead(session.candidate.upstream_name.clone(), config)
            .acquire_owned()
            .await
            .map_err(|_| {
                WebSocketError::new(
                    503,
                    "overloaded_error",
                    "WebSocket upstream connection budget is full",
                )
            })?;
        let mut request = responses_websocket_request(session.url.clone(), session.headers.clone())
            .map_err(|_| {
                WebSocketError::new(
                    502,
                    "upstream_unavailable",
                    "could not build provider WebSocket request",
                )
            })?;
        session.inject_trace_context(request.headers_mut());
        connect_provider_websocket(state, session.candidate.upstream_id, request, guard).await
    };
    tokio::select! {
        result = operation => result,
        _ = state.drain_controller.wait_for_draining() => Err(WebSocketError::new(503, "draining", "server is draining")),
    }
}

pub(super) async fn openai_realtime_websocket_handler(
    State(state): State<ProxyState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    provider_scope: Option<Extension<ProviderScope>>,
    websocket: WebSocketUpgrade,
) -> Response<Body> {
    let Some(guard) = state.drain_controller.try_track() else {
        return websocket_http_error(&WebSocketError::new(503, "draining", "server is draining"));
    };
    let auth = match state.lifecycle.authenticate(&headers).await {
        Ok(auth) => auth,
        Err(error) => {
            return state
                .lifecycle
                .reject_responses_websocket_authentication_for_provider(
                    &error,
                    "/v1/realtime",
                    provider_scope.as_ref().map(|scope| scope.0.0),
                );
        }
    };
    let model = uri.query().and_then(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == "model")
            .map(|(_, value)| value.into_owned())
    });
    let observation = state
        .lifecycle
        .begin_realtime_websocket(&auth, "/v1/realtime");
    let mut session = match state
        .lifecycle
        .prepare_realtime_websocket(
            &auth,
            observation,
            RealtimeWebSocketKind::Openai,
            &headers,
            RealtimeWebSocketTarget {
                path: "/v1/realtime",
                query: uri.query(),
                model: model.as_deref(),
            },
            Bytes::new(),
        )
        .await
    {
        Ok(session) => session,
        Err(error) => return websocket_http_error(&error),
    };
    let (upstream, response_headers, bulkhead) =
        match connect_realtime_provider(&state, &session).await {
            Ok(connection) => connection,
            Err(error) => {
                let status = StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY);
                session.finish_handshake(status);
                return websocket_http_error(&error);
            }
        };
    session.connected(&response_headers);
    let request_dispatch = tracing::dispatcher::get_default(Clone::clone);
    websocket
        .max_message_size(state.body_caps.messages)
        .max_frame_size(state.body_caps.messages)
        .on_upgrade(move |downstream| {
            async move {
                let (_guard, _bulkhead) = (guard, bulkhead);
                relay_realtime_websocket(
                    &auth,
                    downstream,
                    upstream,
                    &state,
                    session,
                    RealtimeWebSocketKind::Openai,
                )
                .await;
            }
            .with_subscriber(request_dispatch)
        })
}

pub(super) async fn gemini_live_websocket_handler(
    State(state): State<ProxyState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    provider_scope: Option<Extension<ProviderScope>>,
    websocket: WebSocketUpgrade,
) -> Response<Body> {
    let Some(guard) = state.drain_controller.try_track() else {
        return websocket_http_error(&WebSocketError::new(503, "draining", "server is draining"));
    };
    let path = uri
        .path()
        .strip_prefix("/gemini")
        .unwrap_or(uri.path())
        .to_owned();
    let auth = match state.lifecycle.authenticate(&headers).await {
        Ok(auth) => auth,
        Err(error) => {
            return state
                .lifecycle
                .reject_responses_websocket_authentication_for_provider(
                    &error,
                    &path,
                    provider_scope.as_ref().map(|scope| scope.0.0),
                );
        }
    };
    let observation = state.lifecycle.begin_realtime_websocket(&auth, &path);
    if uri.path()
        != "/gemini/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent"
    {
        return observation.reject_unsupported_operation(cc_lb_domain::Provider::Gemini, &path);
    }
    // The setup frame supplies the model, but cannot make a disabled, unbound,
    // or OAuth-only upstream eligible for Live. Reject that structural absence
    // as HTTP; leave the full model-specific routing pipeline after setup.
    let candidates = cc_lb_engine::build_candidates(
        &state.dynamic_view.load(),
        auth.principal_id(),
        cc_lb_engine::RequestKind::GeminiGenerateContent,
        &[],
        &HashMap::new(),
        state.clock.as_ref(),
    );
    if !candidates
        .iter()
        .any(|candidate| candidate.kind == cc_lb_domain::UpstreamKind::GeminiApiKey)
    {
        return observation.reject_no_eligible_upstream(cc_lb_domain::Provider::Gemini);
    }
    let request_span = tracing::Span::current();
    let request_dispatch = tracing::dispatcher::get_default(Clone::clone);
    websocket
        .max_message_size(state.body_caps.messages)
        .max_frame_size(state.body_caps.messages)
        .on_upgrade(move |mut downstream| {
            async move {
                let _guard = guard;
                let first = async {
                    loop {
                        match downstream.next().await {
                            Some(Ok(Message::Text(frame))) => {
                                break Some(UpstreamMessage::Text(
                                    Bytes::from(frame).try_into().expect("valid client text"),
                                ));
                            }
                            Some(Ok(Message::Binary(frame))) => {
                                break Some(UpstreamMessage::Binary(frame));
                            }
                            Some(Ok(Message::Ping(frame))) => {
                                if downstream.send(Message::Pong(frame)).await.is_err() {
                                    break None;
                                }
                            }
                            Some(Ok(Message::Pong(_))) => {}
                            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break None,
                        }
                    }
                };
                let first = tokio::select! {
                    first = first => first,
                    _ = state.drain_controller.wait_for_draining() => {
                        observation.finish_draining();
                        let _ = downstream.send(Message::Close(None)).now_or_never();
                        return;
                    }
                };
                let Some(first) = first else {
                    // Authentication and lifecycle admission have already
                    // completed, but no provider setup was received. Record this
                    // as a client cancellation rather than a successful upgrade.
                    observation.finish_client_closed();
                    let _ = downstream.send(Message::Close(None)).now_or_never();
                    return;
                };
                let setup = match &first {
                    UpstreamMessage::Text(frame) => Bytes::from(frame.clone()),
                    UpstreamMessage::Binary(frame) => frame.clone(),
                    _ => unreachable!("first message is a data frame"),
                };
                let model = gemini_setup_model(&auth, &setup);
                let mut session = match state
                    .lifecycle
                    .prepare_realtime_websocket(
                        &auth,
                        observation,
                        RealtimeWebSocketKind::Gemini,
                        &headers,
                        RealtimeWebSocketTarget {
                            path: &path,
                            query: uri.query(),
                            model: model.as_deref(),
                        },
                        setup,
                    )
                    .await
                {
                    Ok(session) => session,
                    Err(error) => {
                        let _ = downstream.send(websocket_error_message(&error)).await;
                        let _ = downstream.send(Message::Close(None)).now_or_never();
                        return;
                    }
                };
                let (mut upstream, response_headers, _bulkhead) =
                    match connect_realtime_provider(&state, &session).await {
                        Ok(connection) => connection,
                        Err(error) => {
                            let status = StatusCode::from_u16(error.status)
                                .unwrap_or(StatusCode::BAD_GATEWAY);
                            session.finish_handshake(status);
                            let _ = downstream.send(websocket_error_message(&error)).await;
                            let _ = downstream.send(Message::Close(None)).now_or_never();
                            return;
                        }
                    };
                session.connected(&response_headers);
                let first = match transform_realtime_message(
                    &session,
                    cc_lb_upstream::RealtimeFrameDirection::Setup,
                    first,
                ) {
                    Ok(Some(first)) => first,
                    Ok(None) | Err(()) => {
                        session.finish(
                            RealtimeUsage::default(),
                            RealtimeStreamMetrics::new(),
                            RealtimeWebSocketTermination::ProxyRelayError,
                            cc_lb_engine::realtime_websocket::RealtimeWebSocketDiagnostics::default(
                            ),
                        );
                        let _ = downstream.send(Message::Close(None)).now_or_never();
                        return;
                    }
                };
                let sent = tokio::select! {
                    sent = upstream.send(first) => Some(sent),
                    _ = state.drain_controller.wait_for_draining() => None,
                };
                match sent {
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        session.finish(
                        RealtimeUsage::default(),
                        RealtimeStreamMetrics::new(),
                        RealtimeWebSocketTermination::ProxyRelayError,
                        cc_lb_engine::realtime_websocket::RealtimeWebSocketDiagnostics::default()
                            .with_relay_error(&error),
                    );
                        return;
                    }
                    None => {
                        session.finish(
                            RealtimeUsage::default(),
                            RealtimeStreamMetrics::new(),
                            RealtimeWebSocketTermination::DownstreamCancelled,
                            cc_lb_engine::realtime_websocket::RealtimeWebSocketDiagnostics::default(
                            ),
                        );
                        return;
                    }
                }
                relay_realtime_websocket(
                    &auth,
                    downstream,
                    upstream,
                    &state,
                    session,
                    RealtimeWebSocketKind::Gemini,
                )
                .await;
            }
            .instrument(request_span)
            .with_subscriber(request_dispatch)
        })
}

async fn relay_realtime_websocket(
    _auth: &cc_lb_engine::Authenticated,
    mut downstream: WebSocket,
    mut upstream: RealtimeProvider,
    state: &ProxyState,
    session: RealtimeWebSocketSession,
    kind: RealtimeWebSocketKind,
) {
    // Usage frames are already marker-filtered, so an unbounded handoff keeps
    // accounting lossless without ever blocking the media relay task.
    let (usage_tx, usage_worker) =
        spawn_realtime_usage_worker(kind, Some(session.usage_observer()));
    let mut metrics = RealtimeStreamMetrics::new();
    let mut setup_pending = matches!(kind, RealtimeWebSocketKind::Openai);
    let relay = async {
        let mut diagnostics =
            cc_lb_engine::realtime_websocket::RealtimeWebSocketDiagnostics::default();
        loop {
            tokio::select! {
                message = downstream.next() => {
                    let message = match message {
                        Some(Ok(Message::Text(frame))) => UpstreamMessage::Text(Bytes::from(frame).try_into().expect("valid client text")),
                        Some(Ok(Message::Binary(frame))) => UpstreamMessage::Binary(frame),
                        Some(Ok(Message::Ping(frame))) => UpstreamMessage::Ping(frame),
                        Some(Ok(Message::Pong(frame))) => UpstreamMessage::Pong(frame),
                        Some(Ok(Message::Close(frame))) => {
                            if let Some(close) = frame.as_ref() {
                                diagnostics = diagnostics.with_close(
                                    close.code,
                                    close.reason.as_str(),
                                );
                            }
                            let termination = downstream_close_termination(frame.as_ref());
                            let frame = frame.map(downstream_close_frame);
                            let _ = upstream.send(UpstreamMessage::Close(frame)).await;
                            let _ = downstream.flush().await;
                            return (termination, diagnostics);
                        }
                        Some(Err(error)) => {
                            diagnostics = diagnostics.with_relay_error(&error);
                            return (RealtimeWebSocketTermination::DownstreamCancelled, diagnostics);
                        }
                        None => return (RealtimeWebSocketTermination::DownstreamCancelled, diagnostics),
                    };
                    let is_data = realtime_data_frame(&message);
                    let direction = if setup_pending {
                        cc_lb_upstream::RealtimeFrameDirection::Setup
                    } else {
                        cc_lb_upstream::RealtimeFrameDirection::ClientToProvider
                    };
                    let message = match transform_realtime_message(
                        &session,
                        direction,
                        message,
                    ) {
                        Ok(Some(message)) => message,
                        Ok(None) => continue,
                        Err(()) => return (RealtimeWebSocketTermination::ProxyRelayError, diagnostics),
                    };
                    if let Err(error) = upstream.send(message).await {
                        diagnostics = diagnostics.with_relay_error(&error);
                        return (RealtimeWebSocketTermination::ProxyRelayError, diagnostics);
                    }
                    // Ping/Pong control frames are relayed but do not
                    // consume OpenAI's first-data-frame setup state.
                    if is_data {
                        setup_pending = false;
                    }
                }
                message = upstream.next() => {
                    let message = match message {
                        Some(Ok(message @ UpstreamMessage::Text(_))) => {
                            if let UpstreamMessage::Text(frame) = &message {
                                metrics.observe_frame(frame.as_bytes());
                                metrics.observe_turn_boundary(kind, frame.as_bytes());
                                metrics.observe_provider_metadata(kind, frame.as_bytes());
                                if kind.may_report_usage(frame.as_bytes()) {
                                    let _ = usage_tx.send(Bytes::from(frame.clone()));
                                }
                            }
                            message
                        }
                        Some(Ok(message @ UpstreamMessage::Binary(_))) => {
                            if let UpstreamMessage::Binary(frame) = &message {
                                metrics.observe_frame(frame);
                                metrics.observe_turn_boundary(kind, frame);
                                metrics.observe_provider_metadata(kind, frame);
                                if kind.may_report_usage(frame) {
                                    let _ = usage_tx.send(frame.clone());
                                }
                            }
                            message
                        }
                        Some(Ok(message @ UpstreamMessage::Ping(_))) => {
                            metrics.observe_ping();
                            message
                        }
                        Some(Ok(message @ UpstreamMessage::Pong(_))) => message,
                        Some(Ok(UpstreamMessage::Close(frame))) => {
                            if let Some(close) = frame.as_ref() {
                                diagnostics = diagnostics.with_close(
                                    close.code.into(),
                                    close.reason.as_str(),
                                );
                            }
                            let frame = frame.map(upstream_close_frame);
                            let _ = downstream.send(Message::Close(frame)).await;
                            let _ = upstream.flush().await;
                            return (RealtimeWebSocketTermination::ProviderClosed, diagnostics);
                        }
                        Some(Ok(UpstreamMessage::Frame(_))) => continue,
                        Some(Err(error)) => {
                            diagnostics = diagnostics.with_relay_error(&error);
                            return (RealtimeWebSocketTermination::ProxyRelayError, diagnostics);
                        }
                        None => return (RealtimeWebSocketTermination::ProxyRelayError, diagnostics),
                    };
                    let message = match transform_realtime_message(
                        &session,
                        cc_lb_upstream::RealtimeFrameDirection::ProviderToClient,
                        message,
                    ) {
                        Ok(Some(message)) => message,
                        Ok(None) => continue,
                        Err(()) => return (RealtimeWebSocketTermination::ProxyRelayError, diagnostics),
                    };
                    let message = match message {
                        UpstreamMessage::Text(frame) => {
                            Message::Text(Bytes::from(frame).try_into().expect("valid provider text"))
                        }
                        UpstreamMessage::Binary(frame) => Message::Binary(frame),
                        UpstreamMessage::Ping(frame) => Message::Ping(frame),
                        UpstreamMessage::Pong(frame) => Message::Pong(frame),
                        UpstreamMessage::Close(_) | UpstreamMessage::Frame(_) => continue,
                    };
                    if let Err(error) = downstream.send(message).await {
                        diagnostics = diagnostics.with_relay_error(&error);
                        return (RealtimeWebSocketTermination::DownstreamCancelled, diagnostics);
                    }
                }
            }
        }
    };
    let (termination, diagnostics) = tokio::select! {
        result = relay => result,
        _ = state.drain_controller.wait_for_draining() => (
            RealtimeWebSocketTermination::DownstreamCancelled,
            cc_lb_engine::realtime_websocket::RealtimeWebSocketDiagnostics::default(),
        ),
    };
    let _ = upstream.send(UpstreamMessage::Close(None)).now_or_never();
    let _ = downstream.send(Message::Close(None)).now_or_never();
    drop(usage_tx);
    let usage = usage_worker.await.unwrap_or_default();
    session.finish(usage, metrics, termination, diagnostics);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn realtime_usage_worker_accepts_burst_without_drops() {
        let (usage_tx, usage_worker) =
            spawn_realtime_usage_worker(RealtimeWebSocketKind::Openai, None);
        let frame = Bytes::from_static(
            br#"{"type":"response.done","response":{"usage":{"input_tokens":1,"output_tokens":1}}}"#,
        );
        for _ in 0..64 {
            usage_tx
                .send(frame.clone())
                .expect("usage frame must be accepted");
        }
        drop(usage_tx);
        usage_worker.await.expect("usage worker must drain");
    }

    #[test]
    fn realtime_control_frames_do_not_consume_openai_setup_state() {
        assert!(!realtime_data_frame(&UpstreamMessage::Ping(Bytes::new())));
        assert!(!realtime_data_frame(&UpstreamMessage::Pong(Bytes::new())));
        assert!(realtime_data_frame(&UpstreamMessage::Text(
            "response.create".to_owned().into()
        )));
        assert!(realtime_data_frame(&UpstreamMessage::Binary(Bytes::new())));
    }

    #[test]
    fn only_explicit_normal_downstream_closes_are_completion_candidates() {
        let normal = axum::extract::ws::CloseFrame {
            code: 1000,
            reason: "".into(),
        };
        let abnormal = axum::extract::ws::CloseFrame {
            code: 1001,
            reason: "".into(),
        };
        assert_eq!(
            downstream_close_termination(Some(&normal)),
            RealtimeWebSocketTermination::DownstreamClosed
        );
        assert_eq!(
            downstream_close_termination(Some(&abnormal)),
            RealtimeWebSocketTermination::DownstreamCancelled
        );
        assert_eq!(
            downstream_close_termination(None),
            RealtimeWebSocketTermination::DownstreamCancelled
        );
    }
}
