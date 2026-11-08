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
        let RealtimeWebSocketTarget { path, query, model } = target;
        let RealtimeWebSocketObservation {
            observer,
            request_id,
        } = observation;
        if let Some(observer) = observer.as_ref() {
            observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::ParseCompleted {
                event_id: observer.event_id().to_owned(),
                result: Ok(cc_lb_lifecycle::ParseInfo {
                    path: path.to_owned(),
                    method: "GET".to_owned(),
                    model: model.map(str::to_owned),
                    stream: true,
                    body_bytes: setup.len() as u64,
                    cache_control_block_count: None,
                    thinking_budget_tokens: None,
                    reasoning_effort: None,
                    cache_breakpoints: Vec::new(),
                    cache_prefix_hash: None,
                    thread_id: None,
                    observed_session_id: None,
                    request_kind: None,
                    claude_agent_id: None,
                    claude_parent_agent_id: None,
                    parent_session_id: None,
                    client_app: None,
                    session_id_source: None,
                    message_id: None,
                    message_index: None,
                    message_count: None,
                    cache_control_message_indices: Vec::new(),
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
            let requested_model = model
                .map(str::trim)
                .filter(|model| !model.is_empty())
                .ok_or_else(|| {
                    WebSocketError::new(
                        400,
                        "invalid_request_error",
                        "realtime WebSocket requests require a model",
                    )
                })?;
            if !view
                .principal_view
                .is_model_allowed(auth.principal_id(), requested_model)
            {
                return Err(WebSocketError::new(
                    403,
                    "model_not_allowed",
                    "model is not allowed for this principal",
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
            let mut clean_headers = headers.clone();
            strip_hop_by_hop(&mut clean_headers);
            let query = query
                .map(cc_lb_upstream::scrub_query_api_key)
                .filter(|query| !query.is_empty())
                .map(|query| query.into_owned());
            let ctx = RequestContext::builder()
                .request_id(request_id)
                .downstream_headers(clean_headers)
                .method(http::Method::GET)
                .path(path.to_owned())
                .query(query)
                .body_bytes(setup)
                .canonical_model_id(canonical_model_id(model.unwrap_or_default()).to_owned())
                .build();
            let principal = Principal {
                id: auth.principal_id().to_owned(),
                kind: PrincipalKind::InternalKey,
            };
            let request_kind = match kind {
                RealtimeWebSocketKind::Openai => RequestKind::OpenaiResponses,
                RealtimeWebSocketKind::Gemini => RequestKind::GeminiGenerateContent,
            };
            let required_kind = match kind {
                RealtimeWebSocketKind::Openai => CandidateUpstreamKind::OpenaiApiKey,
                RealtimeWebSocketKind::Gemini => CandidateUpstreamKind::GeminiApiKey,
            };
            let mut candidates = build_candidates(
                &view,
                auth.principal_id(),
                request_kind,
                &[],
                &HashMap::new(),
                &*self.clock,
            );
            candidates.retain(|candidate| candidate.kind == required_kind);
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
            let terminal =
                self.select_terminal_upstream(pipeline.terminal.clone(), &filtered.candidates);
            let id = terminal.upstream_id.ok_or_else(|| {
                WebSocketError::new(
                    503,
                    error_codes::ROUTE_NO_UPSTREAM_AFTER_FILTER,
                    "no eligible provider API-key upstream",
                )
            })?;
            let record = view
                .upstreams_snapshot()
                .iter()
                .find(|record| record.id == id)
                .ok_or_else(|| {
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
                terminal,
                None,
                duration_to_ms(started.elapsed()),
            );
            if let Some(observer) = observer.as_ref() {
                let info = route_info_for_routed(&routed, model);
                observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
                    event_id: observer.event_id().to_owned(),
                    routing_trace: info.routing_trace.clone(),
                    result: Ok(info),
                });
            }
            let body =
                Arc::new(serde_json::json!({"model": model.unwrap_or_default(), "stream": true}));
            let body_view = RequestBodyView {
                parsed: Ok(body),
                json_parse_ms: 0.0,
            };
            let mut limit = self
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
                .map_err(|error| {
                    WebSocketError::new(
                        error.response.status().as_u16(),
                        error_codes::LIMIT_REJECTED,
                        error.info.reason_label,
                    )
                })?;
            if let Some(observer) = observer.as_ref() {
                observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::LimitDecision {
                    event_id: observer.event_id().to_owned(),
                    decision: cc_lb_lifecycle::LimitDecisionKind::Reserved {
                        reservation_id: limit
                            .as_ref()
                            .and_then(|limit| limit.reservation.as_ref())
                            .map(|reservation| reservation.id().to_owned())
                            .unwrap_or_default(),
                        amount: limit.as_ref().map_or(0, |limit| {
                            limit.request.max_tokens.unwrap_or(0).max(0) as u64
                        }),
                        limit_reserve_ms: None,
                    },
                });
            }
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
            let signer = factory.build(&upstream).await.map_err(|_| {
                WebSocketError::new(
                    502,
                    "upstream_signing_failed",
                    "could not load provider credentials",
                )
            })?;
            let signed = sign_request(signer.as_ref(), shaped).await.map_err(|_| {
                WebSocketError::new(
                    502,
                    "upstream_signing_failed",
                    "could not sign provider WebSocket request",
                )
            })?;
            let mut url = signed.url().clone();
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
            let mut signed_headers = signed.headers().clone();
            strip_hop_by_hop(&mut signed_headers);
            Ok(RealtimeWebSocketSession {
                candidate: WebSocketCandidate {
                    upstream_id: id,
                    upstream_name: record.name.clone(),
                },
                url,
                headers: signed_headers,
                observer: observer.clone(),
                reservation: limit.as_mut().and_then(|limit| limit.reservation.take()),
                limit_engine: self.limit_engine.clone(),
                model: model.unwrap_or_default().to_owned(),
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
}
