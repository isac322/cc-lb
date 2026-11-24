use super::*;
use crate::realtime_websocket::{
    RealtimeWebSocketKind, RealtimeWebSocketObservation, RealtimeWebSocketSession,
    RealtimeWebSocketTarget,
};

impl Lifecycle {
    pub fn begin_realtime_websocket(
        &self,
        auth: &crate::Authenticated,
        path: &str,
    ) -> RealtimeWebSocketObservation {
        let (observer, request_id) = self.websocket_observer(path);
        if let Some(observer) = observer.as_ref() {
            observer.mark_authn_reached();
            observer.set_protocol(cc_lb_request_log::RequestProtocol::Websocket);
            observer.emit_request_started(true);
            observer.set_success_status(StatusCode::SWITCHING_PROTOCOLS);
            observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::AuthCompleted {
                event_id: observer.event_id().to_owned(),
                result: Ok(self.websocket_auth_info(auth)),
            });
            self.record_authentication_completed(auth, observer);
        }
        RealtimeWebSocketObservation {
            observer,
            completion_observer: Arc::clone(&self.completion_observer),
            request_id,
        }
    }

    /// Route, reserve and sign exactly one provider API-key connection.
    pub async fn prepare_realtime_websocket(
        &self,
        auth: &crate::Authenticated,
        observation: RealtimeWebSocketObservation,
        kind: RealtimeWebSocketKind,
        headers: &HeaderMap,
        target: RealtimeWebSocketTarget<'_>,
        setup: Bytes,
    ) -> Result<RealtimeWebSocketSession, WebSocketError> {
        let setup_started = Instant::now();
        let RealtimeWebSocketTarget {
            path,
            query,
            model,
            codex_backend,
        } = target;
        let public_query = query.map(str::to_owned);
        let call_id = path
            .strip_prefix("/v1/live/")
            .map(str::trim)
            .filter(|call_id| !call_id.is_empty())
            .map(str::to_owned)
            .or_else(|| {
                query.and_then(|query| {
                    url::form_urlencoded::parse(query.as_bytes())
                        .find(|(key, _)| key == "call_id")
                        .map(|(_, value)| value.into_owned())
                        .filter(|call_id| !call_id.trim().is_empty())
                })
            });
        let has_intent = query.is_some_and(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .any(|(key, value)| key == "intent" && !value.trim().is_empty())
        });
        let RealtimeWebSocketObservation {
            observer,
            completion_observer,
            request_id,
        } = observation;
        // Realtime setup frames are provider control payloads, not the
        // request-body formats handled by the shared metadata fallbacks.
        // Header identity is still parsed after authentication, with the
        // canonical twelve-header precedence and 512-byte bounds.
        let identity = request_identity_metadata(headers, None, None);
        let thread_id = identity.thread_id.clone();
        let observed_session_id = identity.observed_session_id.clone();
        let client_app = identity.client_app.clone();
        let session_id_source = identity.session_id_source.clone();
        let request_kind = Some(
            match kind {
                RealtimeWebSocketKind::Openai => "realtime",
                RealtimeWebSocketKind::Gemini => "live",
            }
            .to_owned(),
        );
        if let Some(observer) = observer.as_ref() {
            observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::ParseCompleted {
                event_id: observer.event_id().to_owned(),
                result: Ok(cc_lb_lifecycle::ParseInfo {
                    path: path.to_owned(),
                    method: "GET".to_owned(),
                    model: model.map(str::to_owned),
                    stream: true,
                    body_bytes: setup.len() as u64,
                    thinking_budget_tokens: None,
                    reasoning_effort: None,
                    thinking_level: None,
                    cache_breakpoints: Vec::new(),
                    cache_prefix_hash: None,
                    thread_id: thread_id.clone(),
                    observed_session_id,
                    request_kind,
                    // These fields describe Claude's request ancestry. Live
                    // protocols must not inherit values from similarly named
                    // headers supplied by a non-Claude client.
                    claude_agent_id: None,
                    claude_parent_agent_id: None,
                    parent_session_id: None,
                    client_app,
                    session_id_source,
                    message_id: None,
                    message_index: None,
                    message_count: None,
                    cache_control_message_indices: Vec::new(),
                    native_cache: None,
                    keepalive_session_key_hash: None,
                }),
            });
        }
        let result = async {
            let view = self.dynamic_view.load();
            use cc_lb_control::api_keys::principal_view::PrincipalStatus;
            if view.principal_view.principal_status(auth.principal_id()) != PrincipalStatus::Active
            {
                return Err(WebSocketError::new(
                    403,
                    "principal_unavailable",
                    "authenticated principal is unavailable",
                ));
            }
            let requested_model = model.map(str::trim).filter(|model| !model.is_empty());
            if let Some(requested_model) = requested_model
                && !view
                    .principal_view
                    .is_model_allowed(auth.principal_id(), requested_model)
            {
                return Err(WebSocketError::new(
                    403,
                    "model_not_allowed",
                    "model is not allowed for this principal",
                ));
            }
            let allow_modelless = matches!(kind, RealtimeWebSocketKind::Openai)
                && (call_id.is_some()
                    || ((path == "/v1/live" || (codex_backend && has_intent))
                        && !view.principal_view.has_model_allowlist(auth.principal_id())));
            if requested_model.is_none() && !allow_modelless {
                return Err(WebSocketError::new(
                    400,
                    "invalid_request_error",
                    "realtime WebSocket requests require a model",
                ));
            }
            let cached = view
                .principal_view
                .get(auth.principal_id())
                .ok_or_else(|| {
                    WebSocketError::new(
                        403,
                        error_codes::PRINCIPAL_MISSING,
                        "authenticated principal is unavailable",
                    )
                })?;
            let pipeline = cached.resolved_pipeline(None);
            if let Some(error) = pipeline.instantiation_error.as_deref() {
                return Err(WebSocketError::new(
                    502,
                    error_codes::ROUTER_PIPELINE_UNAVAILABLE,
                    error,
                ));
            }
            let sideband_upstream_id = if matches!(kind, RealtimeWebSocketKind::Openai) {
                if let Some(call_id) = call_id.as_deref() {
                    let key = UpstreamAffinityKey {
                        principal_id: auth.principal_id().to_owned(),
                        provider: "openai".to_owned(),
                        kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiRealtimeCall,
                        value_sha256: Sha256::digest(call_id.as_bytes()).into(),
                    };
                    match self.resolve_request_affinities(&[key]).await {
                        Ok(Some(resolution)) => Some(resolution.target_upstream_id),
                        Ok(None) => None,
                        Err(error)
                            if error.error_type == "api_error"
                                && error.message == UPSTREAM_AFFINITY_NOT_ESTABLISHED_MESSAGE =>
                        {
                            return Err(WebSocketError::new(
                                404,
                                "realtime_call_not_found",
                                "realtime call is not bound to an available upstream",
                            ));
                        }
                        Err(error) => {
                            return Err(WebSocketError::new(
                                error.status.as_u16(),
                                error.error_type,
                                error.message,
                            ));
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            };
            let mut clean_headers = headers.clone();
            strip_hop_by_hop(&mut clean_headers);
            let query = query
                .map(cc_lb_upstream::scrub_query_api_key)
                .filter(|query| !query.is_empty())
                .map(|query| query.into_owned());
            let ctx = RequestContext::builder()
                .request_id(request_id)
                .thread_id(thread_id)
                .downstream_headers(clean_headers)
                .method(http::Method::GET)
                .path(path.to_owned())
                .query(query)
                .body_bytes(setup)
                .canonical_model_id(
                    canonical_model_id(kind.provider(), model.unwrap_or_default()).to_owned(),
                )
                .codex_backend(codex_backend)
                .build();
            let principal = Principal {
                id: auth.principal_id().to_owned(),
                kind: PrincipalKind::InternalKey,
            };
            let routing_request_kind = match kind {
                RealtimeWebSocketKind::Openai => RequestKind::OpenaiResponses,
                RealtimeWebSocketKind::Gemini => RequestKind::GeminiGenerateContent,
            };
            let mut candidates = build_candidates(
                &view,
                auth.principal_id(),
                routing_request_kind,
                &[],
                &HashMap::new(),
                &*self.clock,
            );
            let openai_kind_eligible = |candidate_kind: CandidateUpstreamKind| {
                if codex_backend {
                    candidate_kind == CandidateUpstreamKind::OpenaiOauth
                } else {
                    matches!(
                        candidate_kind,
                        CandidateUpstreamKind::OpenaiApiKey | CandidateUpstreamKind::OpenaiOauth
                    )
                }
            };
            if let Some(target) = sideband_upstream_id {
                candidates.retain(|candidate| {
                    candidate.upstream_id == target && openai_kind_eligible(candidate.kind)
                });
            } else {
                match kind {
                    RealtimeWebSocketKind::Openai => {
                        candidates.retain(|candidate| openai_kind_eligible(candidate.kind))
                    }
                    RealtimeWebSocketKind::Gemini => {
                        let records = view.upstreams_snapshot();
                        candidates.retain(|candidate| {
                            records
                                .iter()
                                .find(|record| record.id == candidate.upstream_id)
                                .is_some_and(live_eligible)
                        });
                    }
                }
            }
            let started = Instant::now();
            let mut filtered = execute_filter_pipeline(
                pipeline.filters_for(kind.provider()),
                &ctx,
                &principal,
                candidates,
            );
            if let Some(observer) = observer.as_ref() {
                for error in filtered.internal_errors.drain(..) {
                    observer.record_internal_error(error);
                }
            }
            if let Some(target) = sideband_upstream_id {
                filtered.candidates.retain(|candidate| {
                    candidate.upstream_id == target && openai_kind_eligible(candidate.kind)
                });
            }
            let terminal =
                self.select_terminal_upstream(pipeline.terminal.clone(), &filtered.candidates);
            let first_id = terminal.upstream_id.ok_or_else(|| {
                if sideband_upstream_id.is_some() {
                    realtime_call_not_found()
                } else {
                    WebSocketError::new(
                        503,
                        error_codes::ROUTE_NO_UPSTREAM_AFTER_FILTER,
                        if codex_backend {
                            "no eligible OpenAI OAuth upstream"
                        } else {
                            "no eligible provider API-key upstream"
                        },
                    )
                }
            })?;
            let body =
                Arc::new(serde_json::json!({"model": model.unwrap_or_default(), "stream": true}));
            let body_view = RequestBodyView {
                parsed: Ok(body),
                json_parse_ms: 0.0,
            };
            let mut attempted_id = None;
            let mut last_signing_error = None;
            let (routed, signed, url, prepare_signer_ms, sign_ms) = loop {
                let id = match attempted_id {
                    None => first_id,
                    Some(failed_id) => filtered
                        .candidates
                        .iter()
                        .find(|candidate| candidate.upstream_id != failed_id)
                        .map(|candidate| candidate.upstream_id)
                        .ok_or_else(|| {
                            last_signing_error.unwrap_or_else(|| {
                                WebSocketError::new(
                                    502,
                                    "upstream_signing_failed",
                                    "could not sign provider WebSocket request",
                                )
                            })
                        })?,
                };
                attempted_id = Some(id);
                let record = view
                    .upstreams_snapshot()
                    .iter()
                    .find(|record| record.id == id)
                    .ok_or_else(|| {
                        if sideband_upstream_id.is_some() {
                            return realtime_call_not_found();
                        }
                        WebSocketError::new(
                            503,
                            error_codes::ROUTE_NOT_CONFIGURED,
                            "selected upstream is unavailable",
                        )
                    })?;
                let (route, base_url) = route_for_record(record, cached).map_err(|message| {
                    WebSocketError::new(502, error_codes::ROUTE_NOT_CONFIGURED, message)
                })?;
                let routed = RoutedUpstream::new(
                    record,
                    route,
                    base_url,
                    &filtered,
                    TerminalDecision {
                        upstream_id: Some(id),
                        strategy: terminal.strategy.clone(),
                    },
                    None,
                    duration_to_ms(started.elapsed()),
                );
                let upstream = upstream_for_record(record).map_err(|message| {
                    WebSocketError::new(502, error_codes::ROUTE_NOT_CONFIGURED, message)
                })?;
                let shaped = raw_passthrough_request(
                    record.base_url.as_ref(),
                    &ctx,
                    &principal,
                    &upstream,
                    false,
                )
                .map_err(|_| {
                    WebSocketError::new(
                        502,
                        "upstream_signing_failed",
                        "could not shape provider WebSocket request",
                    )
                })?;
                let factory = view.signer_factory.with_router_choice(record.name.clone());
                let signer_started = Instant::now();
                let signer = match factory.build(&upstream).await {
                    Ok(signer) => signer,
                    Err(_) => {
                        let error = WebSocketError::new(
                            502,
                            "upstream_signing_failed",
                            "could not load provider credentials",
                        );
                        if attempted_id == Some(first_id)
                            && filtered
                                .candidates
                                .iter()
                                .any(|candidate| candidate.upstream_id != id)
                        {
                            last_signing_error = Some(error);
                            continue;
                        }
                        return Err(error);
                    }
                };
                let prepare_signer_ms = signer_started.elapsed().as_secs_f64() * 1_000.0;
                let sign_started = Instant::now();
                let signed = match sign_request(signer.as_ref(), shaped).await {
                    Ok(signed) => signed,
                    Err(_) => {
                        let error = WebSocketError::new(
                            502,
                            "upstream_signing_failed",
                            "could not sign provider WebSocket request",
                        );
                        if attempted_id == Some(first_id)
                            && filtered
                                .candidates
                                .iter()
                                .any(|candidate| candidate.upstream_id != id)
                        {
                            last_signing_error = Some(error);
                            continue;
                        }
                        return Err(error);
                    }
                };
                let sign_ms = duration_to_ms(sign_started.elapsed());
                let mut url = signed.url().clone();
                if matches!(kind, RealtimeWebSocketKind::Gemini)
                    && matches!(
                        record.kind,
                        StorageUpstreamKind::VertexApiKey | StorageUpstreamKind::VertexCredential
                    )
                {
                    let cfg = record.vertex.as_ref().ok_or_else(|| {
                        WebSocketError::new(
                            502,
                            error_codes::ROUTE_NOT_CONFIGURED,
                            "Vertex upstream is missing target configuration",
                        )
                    })?;
                    url = cc_lb_signer_vertex::vertex_live_target(cfg, record.base_url.as_ref())
                        .map_err(|_| {
                            WebSocketError::new(
                                502,
                                "invalid_upstream_url",
                                "invalid Vertex Live URL",
                            )
                        })?;
                } else {
                    if record.kind == StorageUpstreamKind::OpenaiOauth {
                        url = self.config.openai_realtime_origin.clone();
                        url.set_path(path);
                        url.set_query(public_query.as_deref());
                    }
                    let scheme = match url.scheme() {
                        "https" | "wss" => "wss",
                        "http" | "ws" => "ws",
                        _ => {
                            return Err(WebSocketError::new(
                                502,
                                "invalid_upstream_url",
                                "unsupported WebSocket URL scheme",
                            ));
                        }
                    };
                    url.set_scheme(scheme).map_err(|_| {
                        WebSocketError::new(502, "invalid_upstream_url", "invalid WebSocket URL")
                    })?;
                }
                break (routed, signed, url, prepare_signer_ms, sign_ms);
            };
            if let Some(observer) = observer.as_ref() {
                let info = route_info_for_routed(&routed, model);
                observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
                    event_id: observer.event_id().to_owned(),
                    routing_trace: info.routing_trace.clone(),
                    result: Ok(info),
                });
            }
            let record = routed.record;
            let id = routed.route.upstream_id;
            let reserve_started = Instant::now();
            let mut limit = match self
                .reserve_limit(
                    &view.principal_view,
                    &ctx,
                    &principal,
                    record,
                    auth.success(),
                    || {
                        let use_image_rates = realtime_use_image_rates(kind);
                        let capability = classify_provider_capability(
                            &ctx.method,
                            &ctx.path,
                            ctx.provider_scope,
                        );
                        body_view.limit_request(
                            &ctx.canonical_model_id,
                            capability,
                            &[],
                            use_image_rates || use_image_rates_for_admission(capability, &ctx.path),
                        )
                    },
                )
                .await
            {
                Ok(limit) => limit,
                Err(LimitRejectionErr { response, info }) => {
                    if let Some(observer) = observer.as_ref() {
                        observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::LimitDecision {
                            event_id: observer.event_id().to_owned(),
                            decision: cc_lb_lifecycle::LimitDecisionKind::Rejected {
                                reason: info.reason_label.clone(),
                                subject: info.subject,
                                request_summary: info.request_summary,
                                route_summary: info.route_summary,
                                limit_violation: info.limit_violation,
                            },
                        });
                    }
                    return Err(WebSocketError::new(
                        response.status().as_u16(),
                        error_codes::LIMIT_REJECTED,
                        info.reason_label,
                    ));
                }
            };
            let reservation_id = limit
                .as_ref()
                .and_then(|limit| limit.reservation.as_ref())
                .map(|reservation| reservation.id().to_owned())
                .unwrap_or_default();
            if let Some(observer) = observer.as_ref() {
                if let Some(engine) = self.limit_engine.as_ref()
                    && !reservation_id.is_empty()
                {
                    observer.attach_limit_reservation(Arc::clone(engine), reservation_id.clone());
                }
                observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::LimitDecision {
                    event_id: observer.event_id().to_owned(),
                    decision: cc_lb_lifecycle::LimitDecisionKind::Reserved {
                        reservation_id,
                        amount: limit.as_ref().map_or(0, |limit| {
                            limit.request.max_tokens.unwrap_or(0).max(0) as u64
                        }),
                        limit_reserve_ms: Some(duration_to_ms(reserve_started.elapsed())),
                    },
                });
            }
            let mut signed_headers = signed.headers().clone();
            strip_hop_by_hop(&mut signed_headers);
            // The provider joins a sideband only when its `OpenAI-Alpha` matches
            // the live call, and subscription calls are created with the
            // quicksilver protocol. Public clients never send it; Codex does,
            // so a client-supplied value is kept.
            if record.kind == StorageUpstreamKind::OpenaiOauth
                && (call_id.is_some() || path == "/v1/live")
            {
                let (name, value) = crate::openai_compat::realtime::REALTIME_ALPHA_HEADER;
                signed_headers
                    .entry(name)
                    .or_insert(http::HeaderValue::from_static(value));
            }
            Ok(RealtimeWebSocketSession {
                candidate: WebSocketCandidate {
                    upstream_id: id,
                    upstream_name: record.name.clone(),
                    socket_per_lane: false,
                    is_api_key: matches!(
                        record.kind,
                        StorageUpstreamKind::OpenaiApiKey
                            | StorageUpstreamKind::AnthropicApiKey
                            | StorageUpstreamKind::GeminiApiKey
                            | StorageUpstreamKind::VertexApiKey
                    ),
                },
                url,
                headers: signed_headers,
                observer: observer.clone(),
                completion_observer: Arc::clone(&completion_observer),
                stream_span: tracing::info_span!(
                    "proxy.realtime_stream",
                    upstream_id = %id,
                    upstream_name = %record.name,
                    provider = ?kind.provider(),
                    principal_id = %principal.id,
                    model = %model.unwrap_or_default(),
                    otel.status_code = tracing::field::Empty,
                    http.response.status_code = tracing::field::Empty,
                    cc_lb.request.outcome = tracing::field::Empty,
                    cc_lb.request.duration_ms = tracing::field::Empty,
                    cc_lb.request.finalize_ms = tracing::field::Empty,
                    cc_lb.request.unaccounted_ms = tracing::field::Empty,
                    stream.outcome = tracing::field::Empty,
                    error.cause = tracing::field::Empty,
                    error.chain = tracing::field::Empty,
                    error.io.kind = tracing::field::Empty,
                    error.io.os_error = tracing::field::Empty,
                    error.type = tracing::field::Empty,
                    stream_first_output_ms = tracing::field::Empty,
                    stream_last_output_ms = tracing::field::Empty,
                    stream_event_count = tracing::field::Empty,
                    stream_total_ms = tracing::field::Empty,
                    ping_count = tracing::field::Empty,
                    total_bytes = tracing::field::Empty,
                    stream_inter_output_avg_ms = tracing::field::Empty,
                    provider_session_id = tracing::field::Empty,
                    provider_connection_id = tracing::field::Empty,
                    websocket.close.code = tracing::field::Empty,
                    websocket.close.reason = tracing::field::Empty,
                ),
                reservation: limit.as_mut().and_then(|limit| limit.reservation.take()),
                limit_engine: self.limit_engine.clone(),
                model: model.unwrap_or_default().to_owned(),
                principal: principal.clone(),
                vertex_config: record.vertex.clone(),
                upstream: routed.route.upstream.clone(),
                path: path.to_owned(),
                dialect: routed.route.dialect.clone(),
                provider_request_id: None,
                proxy_setup_ms: Some(duration_to_ms(setup_started.elapsed())),
                setup_timings: cc_lb_lifecycle::RequestSetupTimings {
                    prepare_signer_ms: Some(prepare_signer_ms),
                    ..Default::default()
                },
                sign_ms: Some(sign_ms),
                connection_timings: Default::default(),
                connected_at: None,
                first_frame_observed: false,
            })
        }
        .await;
        if let (Err(error), Some(observer)) = (&result, observer.as_ref()) {
            observer.terminate_failure(InternalFailure {
                status: StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY),
                error_code: error.code,
                error: InternalError {
                    stage: InternalErrorStage::Router,
                    kind: InternalErrorKind::Unavailable,
                    message: Some(error.message.clone()),
                },
            });
        }
        result
    }
}

/// Returns whether a stored upstream may receive a Gemini Live connection.
///
/// Vertex API keys are excluded: the Live endpoint requires OAuth credentials
/// that assert a principal, just like the cache and model-catalog RPCs.
fn realtime_call_not_found() -> WebSocketError {
    WebSocketError::new(
        404,
        "realtime_call_not_found",
        "realtime call is not bound to an available upstream",
    )
}

pub fn live_eligible(record: &UpstreamRecord) -> bool {
    matches!(
        record.kind,
        StorageUpstreamKind::GeminiApiKey | StorageUpstreamKind::VertexCredential
    )
}

fn realtime_use_image_rates(kind: RealtimeWebSocketKind) -> bool {
    matches!(kind, RealtimeWebSocketKind::Openai)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn realtime_admission_uses_image_rates_only_for_openai() {
        assert!(realtime_use_image_rates(RealtimeWebSocketKind::Openai));
        assert!(!realtime_use_image_rates(RealtimeWebSocketKind::Gemini));
    }

    #[test]
    fn realtime_identity_uses_shared_bounded_header_precedence() {
        let mut headers = HeaderMap::new();
        headers.insert("x-session-id", HeaderValue::from_static("fallback"));
        headers.insert("x-opencode-session", HeaderValue::from_static("opencode"));
        headers.insert("x-claude-session-id", HeaderValue::from_static("claude"));
        headers.insert("x-app", HeaderValue::from_static("voice-client"));
        headers.insert(
            "x-claude-code-agent-id",
            HeaderValue::from_static("not-claude"),
        );

        let identity = request_identity_metadata(&headers, None, None);
        assert_eq!(identity.thread_id.as_deref(), Some("claude"));
        assert_eq!(identity.observed_session_id.as_deref(), Some("claude"));
        assert_eq!(
            identity.session_id_source.as_deref(),
            Some("x-claude-session-id")
        );
        assert_eq!(identity.client_app.as_deref(), Some("voice-client"));
        assert_eq!(identity.claude_agent_id.as_deref(), Some("not-claude"));

        headers.insert(
            "x-claude-session-id",
            HeaderValue::from_str(&"x".repeat(MAX_IDENTITY_VALUE_BYTES + 1)).unwrap(),
        );
        let identity = request_identity_metadata(&headers, None, None);
        assert_eq!(identity.thread_id.as_deref(), Some("opencode"));
        assert_eq!(
            identity.session_id_source.as_deref(),
            Some("x-opencode-session")
        );
    }
}
