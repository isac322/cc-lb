use super::*;

#[derive(Clone, Serialize)]
pub(super) struct HealthBody {
    pub(super) status: &'static str,
    pub(super) uptime_seconds: u64,
    pub(super) build: BuildMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) ready: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reason: Option<String>,
}
pub(super) fn app_router(state: ProxyState, timeout_secs: u64) -> Router {
    health_router(state.clone()).merge(proxy_router(state, timeout_secs))
}

pub(super) fn health_router(state: ProxyState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state)
}

pub(super) fn proxy_route_template(path: &str) -> Option<&'static str> {
    match path {
        "/v1/messages" => Some("/v1/messages"),
        "/v1/messages/count_tokens" => Some("/v1/messages/count_tokens"),
        "/v1/responses" => Some("/v1/responses"),
        "/v1/models" => Some("/v1/models"),
        "/v1/files" => Some("/v1/files"),
        _ if path.starts_with("/v1/models/") => Some("/v1/models/{id}"),
        _ if path.starts_with("/v1/files/") && path.ends_with("/content") => {
            Some("/v1/files/{id}/content")
        }
        _ if path.starts_with("/v1/files/") => Some("/v1/files/{id}"),
        _ if path.starts_with("/v1beta/models/") && path.ends_with(":generateContent") => {
            Some("/v1beta/models/{model}:generateContent")
        }
        _ if path.starts_with("/v1beta/models/") && path.ends_with(":streamGenerateContent") => {
            Some("/v1beta/models/{model}:streamGenerateContent")
        }
        _ if path.starts_with("/v1beta/models/") && path.ends_with(":countTokens") => {
            Some("/v1beta/models/{model}:countTokens")
        }
        "/api/oauth/usage" => Some("/api/oauth/usage"),
        _ if path.starts_with("/api/") => Some("/api/{*path}"),
        _ if path.starts_with("/v1beta/") => Some("/v1beta/{*path}"),
        _ if path.starts_with("/v1/") => Some("/v1/{*path}"),
        _ => None,
    }
}

async fn rewrite_codex_backend_request(mut request: Request<Body>, next: Next) -> Response<Body> {
    let path = request.uri().path();
    let rewritten_path = format!("/v1{path}");
    let rewritten_uri = match request.uri().query() {
        Some(query) => format!("{rewritten_path}?{query}"),
        None => rewritten_path,
    }
    .parse::<Uri>();
    let Ok(rewritten_uri) = rewritten_uri else {
        return provider_scope_error_response(
            cc_lb_domain::Provider::Openai,
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid backend API path",
        );
    };
    *request.uri_mut() = rewritten_uri;
    request
        .extensions_mut()
        .insert(cc_lb_engine::CodexBackendSurface);
    next.run(request).await
}

async fn backend_api_not_found(
    State(state): State<ProxyState>,
    request: Request<Body>,
) -> Response<Body> {
    let observer = request
        .extensions()
        .get::<cc_lb_engine::LifecycleContext>()
        .cloned();
    if let Some(observer) = observer.as_ref() {
        observer.mark_authn_reached();
    }
    let auth = match state.lifecycle.authenticate(request.headers()).await {
        Ok(auth) => auth,
        Err(error) => {
            return cc_lb_engine::reject_unauthenticated_for_route(
                &error,
                request.method(),
                request.uri().path(),
                Some(cc_lb_domain::Provider::Openai),
                observer.as_ref(),
            );
        }
    };
    if let Some(observer) = observer.as_ref() {
        state
            .lifecycle
            .record_authentication_completed(&auth, observer);
        observer.terminate_local(TerminalClassification::OPERATION_NOT_SUPPORTED);
    }
    provider_scope_error_response(
        cc_lb_domain::Provider::Openai,
        StatusCode::NOT_FOUND,
        "operation_not_supported",
        "requested operation is not supported",
    )
}

fn codex_backend_http_routes(
    state: ProxyState,
    request_id_state: RequestIdState,
    timeout_secs: u64,
) -> Router {
    let routes = Router::new()
        .route("/responses", post(lifecycle_handler))
        .route("/models", get(lifecycle_handler))
        .route("/alpha/search", post(lifecycle_handler))
        .route("/images/generations", post(lifecycle_handler))
        .route("/images/edits", post(lifecycle_handler))
        .route("/realtime/calls", post(lifecycle_handler))
        .route("/realtime/calls/{call_id}/hangup", post(lifecycle_handler))
        .with_state(state.clone());
    wrap_scoped_proxy_routes(
        routes,
        state,
        request_id_state,
        timeout_secs,
        Some(cc_lb_domain::Provider::Openai),
    )
    .layer(middleware::from_fn(rewrite_codex_backend_request))
}

pub(super) fn proxy_router(state: ProxyState, timeout_secs: u64) -> Router {
    let _ = cc_lb_providers::builtin_provider_registry();
    let request_id_state = RequestIdState::uuid_v7("req_server_");
    let responses_ws_route = || {
        get(responses_websocket_handler).layer(cc_lb_observability::trace_layer().make_span_with(
            cc_lb_observability::ProxyMakeSpan::with_route_template(proxy_route_template),
        ))
    };
    let websocket_routes = Router::new()
        .route("/v1/responses", responses_ws_route())
        .nest(
            "/openai",
            Router::new()
                .route("/v1/responses", responses_ws_route())
                .route(
                    "/v1/realtime",
                    get(openai_realtime_websocket_handler).layer(
                        cc_lb_observability::trace_layer().make_span_with(
                            cc_lb_observability::ProxyMakeSpan::with_route_template(
                                proxy_route_template,
                            ),
                        ),
                    ),
                )
                .route(
                    "/v1/live",
                    get(openai_live_websocket_handler).layer(
                        cc_lb_observability::trace_layer().make_span_with(
                            cc_lb_observability::ProxyMakeSpan::with_route_template(
                                proxy_route_template,
                            ),
                        ),
                    ),
                )
                .route(
                    "/v1/live/{call_id}",
                    get(openai_live_call_websocket_handler).layer(
                        cc_lb_observability::trace_layer().make_span_with(
                            cc_lb_observability::ProxyMakeSpan::with_route_template(
                                proxy_route_template,
                            ),
                        ),
                    ),
                )
                .nest(
                    "/backend-api/codex",
                    Router::new()
                        .route(
                            "/",
                            any(codex_backend_root_handler).layer(
                                cc_lb_observability::trace_layer().make_span_with(
                                    cc_lb_observability::ProxyMakeSpan::with_route_template(
                                        proxy_route_template,
                                    ),
                                ),
                            ),
                        )
                        .route("/responses", responses_ws_route())
                        .route(
                            "/realtime",
                            get(openai_realtime_websocket_handler).layer(
                                cc_lb_observability::trace_layer().make_span_with(
                                    cc_lb_observability::ProxyMakeSpan::with_route_template(
                                        proxy_route_template,
                                    ),
                                ),
                            ),
                        )
                        .route(
                            "/live",
                            get(openai_live_websocket_handler).layer(
                                cc_lb_observability::trace_layer().make_span_with(
                                    cc_lb_observability::ProxyMakeSpan::with_route_template(
                                        proxy_route_template,
                                    ),
                                ),
                            ),
                        )
                        .route(
                            "/live/{call_id}",
                            get(openai_live_call_websocket_handler).layer(
                                cc_lb_observability::trace_layer().make_span_with(
                                    cc_lb_observability::ProxyMakeSpan::with_route_template(
                                        proxy_route_template,
                                    ),
                                ),
                            ),
                        )
                        .layer(middleware::from_fn(rewrite_codex_backend_request)),
                )
                .layer(Extension(ProviderScope(cc_lb_domain::Provider::Openai))),
        )
        .nest(
            "/gemini",
            Router::new()
                .route(
                    "/ws/{*path}",
                    get(gemini_live_websocket_handler).layer(
                        cc_lb_observability::trace_layer().make_span_with(
                            cc_lb_observability::ProxyMakeSpan::with_route_template(
                                proxy_route_template,
                            ),
                        ),
                    ),
                )
                .layer(Extension(ProviderScope(cc_lb_domain::Provider::Gemini))),
        )
        .with_state(state.clone());
    Router::new()
        .merge(scoped_proxy_routes(
            state.clone(),
            request_id_state.clone(),
            timeout_secs,
            None,
            true,
        ))
        .nest(
            "/anthropic",
            scoped_proxy_routes(
                state.clone(),
                request_id_state.clone(),
                timeout_secs,
                Some(cc_lb_domain::Provider::Anthropic),
                true,
            ),
        )
        .nest(
            "/openai",
            scoped_proxy_routes(
                state.clone(),
                request_id_state.clone(),
                timeout_secs,
                Some(cc_lb_domain::Provider::Openai),
                false,
            ),
        )
        .nest(
            "/openai/backend-api/codex",
            codex_backend_http_routes(state.clone(), request_id_state.clone(), timeout_secs),
        )
        .nest(
            "/openai/backend-api",
            wrap_scoped_proxy_routes(
                Router::new()
                    .route("/", any(backend_api_not_found))
                    .route("/{*path}", any(backend_api_not_found))
                    .with_state(state.clone()),
                state.clone(),
                request_id_state.clone(),
                timeout_secs,
                Some(cc_lb_domain::Provider::Openai),
            ),
        )
        .nest(
            "/gemini",
            scoped_proxy_routes(
                state,
                request_id_state,
                timeout_secs,
                Some(cc_lb_domain::Provider::Gemini),
                false,
            ),
        )
        .merge(websocket_routes)
}

fn scoped_proxy_routes(
    state: ProxyState,
    request_id_state: RequestIdState,
    timeout_secs: u64,
    provider: Option<cc_lb_domain::Provider>,
    include_oauth_usage: bool,
) -> Router {
    let mut routes = Router::new()
        .route("/v1/messages", post(lifecycle_handler))
        .route("/v1/messages/count_tokens", post(lifecycle_handler))
        .route("/v1/responses", post(lifecycle_handler))
        .route("/v1/models", get(lifecycle_handler))
        .route("/v1/models/{id}", get(lifecycle_handler))
        .route("/v1/files", post(lifecycle_handler).get(lifecycle_handler))
        .route(
            "/v1/files/{id}",
            get(lifecycle_handler).delete(lifecycle_handler),
        )
        .route("/v1/files/{id}/content", get(lifecycle_handler))
        .route("/api/{*path}", any(lifecycle_handler))
        .route("/v1/{*path}", any(lifecycle_handler))
        .route("/v1beta/{*path}", any(lifecycle_handler));
    if include_oauth_usage {
        routes = routes.route("/api/oauth/usage", get(oauth_usage_handler));
    }
    wrap_scoped_proxy_routes(
        routes.with_state(state.clone()),
        state,
        request_id_state,
        timeout_secs,
        provider,
    )
}

pub(super) fn wrap_scoped_proxy_routes(
    routes: Router,
    state: ProxyState,
    request_id_state: RequestIdState,
    timeout_secs: u64,
    provider: Option<cc_lb_domain::Provider>,
) -> Router {
    let chaos = crate::chaos::ChaosLayer::from_env();
    let timeout_layer = middleware::from_fn_with_state(
        ProxyTimeoutState {
            timeout: Duration::from_secs(timeout_secs.max(1)),
            provider,
        },
        proxy_timeout_middleware,
    );
    let routes = if chaos.is_enabled() {
        routes.layer(ServiceBuilder::new().layer(chaos).layer(timeout_layer))
    } else {
        routes.layer(timeout_layer)
    };
    let outer_sb = ServiceBuilder::new()
        .layer(middleware::from_fn_with_state(
            request_id_state,
            request_id_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            lifecycle_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            state.drain_controller.clone(),
            cc_lb_engine::proxy_drain_middleware,
        ))
        .layer(HopByHopStripLayer::new())
        .layer(cc_lb_observability::trace_layer().make_span_with(
            cc_lb_observability::ProxyMakeSpan::with_route_template(proxy_route_template),
        ));
    let protected = Router::new()
        .merge(routes)
        .fallback(proxy_not_found)
        .method_not_allowed_fallback(proxy_method_not_allowed)
        .layer(outer_sb);
    match provider {
        Some(provider) => protected.layer(Extension(ProviderScope(provider))),
        None => protected,
    }
}

async fn proxy_not_found(request: Request<Body>) -> Response<Body> {
    let mut response = match request
        .extensions()
        .get::<ProviderScope>()
        .map(|scope| scope.0)
    {
        Some(provider) => provider_scope_error_response(
            provider,
            StatusCode::NOT_FOUND,
            "not_found",
            "requested proxy path was not found",
        ),
        None => anthropic_error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            "requested proxy path was not found",
        ),
    };
    response
        .extensions_mut()
        .insert(TerminalClassification::ROUTE_NOT_FOUND);
    response
}

async fn proxy_method_not_allowed(request: Request<Body>) -> Response<Body> {
    let mut response = match request
        .extensions()
        .get::<ProviderScope>()
        .map(|scope| scope.0)
    {
        Some(provider) => provider_scope_error_response(
            provider,
            StatusCode::METHOD_NOT_ALLOWED,
            "not_found",
            "method is not allowed for this proxy path",
        ),
        None => anthropic_error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "not_found",
            "method is not allowed for this proxy path",
        ),
    };
    response
        .extensions_mut()
        .insert(TerminalClassification::METHOD_NOT_ALLOWED);
    response
}

pub(super) async fn healthz(State(state): State<ProxyState>) -> Json<HealthBody> {
    Json(HealthBody {
        status: "ok",
        uptime_seconds: state.start_time.elapsed().as_secs(),
        build: BuildMeta::current(),
        ready: None,
        reason: None,
    })
}

pub(super) async fn readyz(State(state): State<ProxyState>) -> (StatusCode, Json<HealthBody>) {
    let readiness = readiness(&state);
    let ready = matches!(readiness, Readiness::Ready { .. });
    let reason = match readiness {
        Readiness::Ready { degraded } => degraded,
        Readiness::NotReady(blocker) => Some(blocker),
    };

    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(HealthBody {
            status: if ready { "ok" } else { "not_ready" },
            uptime_seconds: state.start_time.elapsed().as_secs(),
            build: BuildMeta::current(),
            ready: Some(ready),
            reason: reason.map(str::to_owned),
        }),
    )
}

pub(super) enum Readiness {
    /// Serving traffic. `degraded` names a non-fatal problem, such as an
    /// OAuth upstream whose credentials cannot be decrypted, that routing
    /// works around.
    Ready {
        degraded: Option<&'static str>,
    },
    NotReady(&'static str),
}

pub(super) fn readiness(state: &ProxyState) -> Readiness {
    if state.drain_controller.is_draining() {
        return Readiness::NotReady("draining");
    }
    if state.server_state.current() != ServerState::Ready {
        return Readiness::NotReady("startup_not_ready");
    }

    let view = state.dynamic_view.load();
    if !view.principal_view.has_any_active_principal() {
        return Readiness::NotReady("no_ready_principal");
    }
    let aead = state.aead.as_ref();
    if !view
        .upstreams_snapshot()
        .iter()
        .any(|upstream| upstream_usable(&view, aead, upstream))
    {
        return Readiness::NotReady("no_ready_upstream");
    }
    let oauth_credentials_broken = view.upstreams_snapshot().iter().any(|upstream| {
        upstream_live(upstream)
            && matches!(
                upstream.kind,
                UpstreamKind::AnthropicOauth
                    | UpstreamKind::OpenaiOauth
                    | UpstreamKind::GeminiOauth
            )
            && !oauth_credentials_decryptable(aead, upstream)
    });

    Readiness::Ready {
        degraded: oauth_credentials_broken.then_some("oauth_credentials_not_decryptable"),
    }
}

fn upstream_live(upstream: &UpstreamRecord) -> bool {
    upstream.enabled && upstream.deleted_at_unix_secs.is_none()
}

/// An upstream routing can select: applied, and for OAuth, holding
/// decryptable credentials that do not need an operator reconnect.
fn upstream_usable(view: &DynamicView, aead: &AeadService, upstream: &UpstreamRecord) -> bool {
    let applied = view
        .upstream_status_snapshot
        .entries
        .get(&upstream.name)
        .is_some_and(|entry| entry.status == cc_lb_control::ApplyStatus::Active);
    if !upstream_live(upstream) || !applied {
        return false;
    }
    match upstream.kind {
        UpstreamKind::AnthropicApiKey | UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey => {
            true
        }
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth => {
            !cc_lb_oauth_protocol::refresh_requires_reconnect(upstream.last_apply_error.as_deref())
                && oauth_credentials_decryptable(aead, upstream)
        }
        UpstreamKind::VertexApiKey => true,
        UpstreamKind::VertexCredential => {
            !cc_lb_domain::vertex::vertex_credential_requires_replacement(
                upstream.last_apply_error.as_deref(),
            ) && upstream.api_key_ciphertext.is_some()
        }
    }
}

fn oauth_credentials_decryptable(aead: &AeadService, upstream: &UpstreamRecord) -> bool {
    upstream
        .oauth_credentials
        .as_ref()
        .is_some_and(|credentials| credentials.decrypt(aead, upstream.id.as_bytes()).is_ok())
}

#[derive(Clone, Copy)]
pub(super) struct ProxyTimeoutState {
    pub(super) timeout: Duration,
    pub(super) provider: Option<cc_lb_domain::Provider>,
}

pub(super) async fn proxy_timeout_middleware(
    State(timeout_state): State<ProxyTimeoutState>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let is_gemini_http = match timeout_state.provider {
        Some(cc_lb_domain::Provider::Gemini) => true,
        Some(_) => false,
        None => cc_lb_engine::is_unscoped_gemini_http_route(request.method(), request.uri().path()),
    };
    match tokio::time::timeout(timeout_state.timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => {
            let mut response = if is_gemini_http {
                provider_scope_error_response(
                    cc_lb_domain::Provider::Gemini,
                    StatusCode::GATEWAY_TIMEOUT,
                    "timeout_error",
                    "request timed out",
                )
            } else {
                let mut response = Response::new(Body::from("request timed out"));
                *response.status_mut() = StatusCode::GATEWAY_TIMEOUT;
                response
            };
            response
                .extensions_mut()
                .insert(cc_lb_engine::InternalFailure::tower_timeout(
                    cc_lb_domain::InternalError {
                        stage: cc_lb_domain::InternalErrorStage::Relay,
                        kind: cc_lb_domain::InternalErrorKind::Timeout,
                        message: Some("request timed out".to_owned()),
                    },
                ));
            response
        }
    }
}
