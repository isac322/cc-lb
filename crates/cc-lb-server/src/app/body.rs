use super::*;

/// Response extension carrying the terminal classification for a response
/// produced outside the lifecycle handler. `lifecycle_middleware` reads it
/// and finalizes the request's `LifecycleContext`; unmarked responses are
/// left to the context's `Drop` backstop. Responses that terminate on a
/// known internal failure carry `cc_lb_engine::InternalFailure` instead,
/// which the middleware consumes first.
pub(super) type TerminalClassification = cc_lb_engine::TerminalClassification;

#[derive(Clone, Copy)]
pub(super) struct RequestBodyCaps {
    pub(super) messages: usize,
    pub(super) files: usize,
}

impl RequestBodyCaps {
    pub(super) fn for_path(self, path: &str) -> usize {
        if path.starts_with("/v1/files") || cc_lb_engine::lifecycle::is_multipart_operation(path) {
            self.files
        } else {
            self.messages
        }
    }
}

#[derive(Debug)]
pub(super) enum RequestBodyReadError {
    TooLarge,
    Read(tower::BoxError),
}

pub(super) struct RequestBodyTimingGuard {
    pub(super) observer: Option<cc_lb_engine::LifecycleContext>,
    pub(super) started: Instant,
    pub(super) first_chunk_received: Option<Instant>,
    pub(super) accumulated_wait: Duration,
    pub(super) active_wait_started: Option<Instant>,
    pub(super) process_started: Instant,
    pub(super) process: Duration,
    pub(super) chunk_count: u64,
    pub(super) published: bool,
}

impl RequestBodyTimingGuard {
    pub(super) fn new(observer: Option<cc_lb_engine::LifecycleContext>) -> Self {
        let started = Instant::now();
        Self {
            observer,
            started,
            first_chunk_received: None,
            accumulated_wait: Duration::ZERO,
            active_wait_started: None,
            process_started: started,
            process: Duration::ZERO,
            chunk_count: 0,
            published: false,
        }
    }

    pub(super) fn begin_wait(&mut self) {
        debug_assert!(self.active_wait_started.is_none());
        let started = Instant::now();
        self.process = self
            .process
            .saturating_add(started.saturating_duration_since(self.process_started));
        self.active_wait_started = Some(started);
    }

    pub(super) fn end_wait(&mut self, ended: Instant) {
        let Some(started) = self.active_wait_started.take() else {
            debug_assert!(false, "request body wait ended without a start");
            return;
        };
        self.accumulated_wait = self
            .accumulated_wait
            .saturating_add(ended.saturating_duration_since(started));
        self.process_started = ended;
    }

    pub(super) fn observe_data(&mut self, received: Instant) {
        self.first_chunk_received.get_or_insert(received);
        self.chunk_count = self.chunk_count.saturating_add(1);
    }

    pub(super) fn snapshot(&self, ended: Instant) -> cc_lb_lifecycle::RequestIoTimings {
        let (wait, process) = if let Some(started) = self.active_wait_started {
            (
                self.accumulated_wait
                    .saturating_add(ended.saturating_duration_since(started)),
                self.process,
            )
        } else {
            (
                self.accumulated_wait,
                self.process
                    .saturating_add(ended.saturating_duration_since(self.process_started)),
            )
        };
        cc_lb_lifecycle::RequestIoTimings {
            request_body_first_chunk_ms: self
                .first_chunk_received
                .map(|received| duration_ms(received.saturating_duration_since(self.started))),
            request_body_receive_ms: self
                .first_chunk_received
                .map(|received| duration_ms(ended.saturating_duration_since(received))),
            request_body_wait_ms: Some(duration_ms(wait)),
            request_body_process_ms: Some(duration_ms(process)),
            request_body_chunk_count: Some(self.chunk_count),
            ..Default::default()
        }
    }

    pub(super) fn finish(mut self) {
        let timings = self.snapshot(Instant::now());
        self.published = true;
        self.publish(timings);
    }

    fn publish(&self, timings: cc_lb_lifecycle::RequestIoTimings) {
        if let Some(observer) = self.observer.as_ref() {
            observer.set_io_timings(timings);
        }
    }
}

impl Drop for RequestBodyTimingGuard {
    fn drop(&mut self) {
        if !self.published {
            self.publish(self.snapshot(Instant::now()));
        }
    }
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

pub(super) struct PricingLimitCostEstimator {
    pub(super) catalog: Arc<cc_lb_pricing::PriceCatalog>,
}

impl cc_lb_engine::LimitCostEstimator for PricingLimitCostEstimator {
    fn estimate_max(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.catalog
            .estimate_max(model, max_input, max_output, service_tier)
            .map(|cost| cost.try_into().unwrap_or(i64::MAX))
    }

    fn estimate_max_with_cache(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        estimated_cache_tokens: u64,
        can_cross_cache_cliff: bool,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.catalog
            .estimate_max_with_cache(
                model,
                max_input,
                max_output,
                estimated_cache_tokens,
                can_cross_cache_cliff,
                service_tier,
            )
            .map(|cost| cost.try_into().unwrap_or(i64::MAX))
    }

    fn estimate_max_with_cache_and_images(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        estimated_cache_tokens: u64,
        can_cross_cache_cliff: bool,
        use_image_rates: bool,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.catalog
            .estimate_max_with_cache_and_images(
                model,
                max_input,
                max_output,
                estimated_cache_tokens,
                can_cross_cache_cliff,
                use_image_rates,
                service_tier,
            )
            .map(|cost| cost.try_into().unwrap_or(i64::MAX))
    }
    fn estimate_max_with_local_cache(
        &self,
        model: &str,
        input: cc_lb_pricing::LocalCacheInputEstimate,
        max_output: u64,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.catalog
            .estimate_max_with_local_cache(model, input, max_output, service_tier)
            .map(|cost| cost.try_into().unwrap_or(i64::MAX))
    }
    fn estimate_max_with_local_cache_and_images(
        &self,
        model: &str,
        input: cc_lb_pricing::LocalCacheInputEstimate,
        max_output: u64,
        use_image_rates: bool,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.catalog
            .estimate_max_with_local_cache_and_images(
                model,
                input,
                max_output,
                use_image_rates,
                service_tier,
            )
            .map(|cost| cost.try_into().unwrap_or(i64::MAX))
    }
}
pub(super) async fn lifecycle_handler(
    State(state): State<ProxyState>,
    request: Request<Body>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let observer = parts
        .extensions
        .get::<cc_lb_engine::LifecycleContext>()
        .cloned();
    // Authentication comes before any request work: an unauthenticated
    // caller must not be able to make this process buffer its body.
    if let Some(observer) = observer.as_ref() {
        observer.mark_authn_reached();
    }
    let auth = match state.lifecycle.authenticate(&parts.headers).await {
        Ok(auth) => auth,
        Err(error) => {
            let provider = parts.extensions.get::<ProviderScope>().map(|scope| scope.0);
            return cc_lb_engine::reject_unauthenticated_for_route(
                &error,
                &parts.method,
                parts.uri.path(),
                provider,
                observer.as_ref(),
            );
        }
    };
    // Record the authenticated principal before the body is read: requests
    // that terminate on a body-read error or the size cap still carry their
    // real principal in metrics. The event is metrics-only; `Lifecycle::handle`
    // re-emits it idempotently for non-HTTP callers.
    if let Some(observer) = observer.as_ref() {
        state
            .lifecycle
            .record_authentication_completed(&auth, observer);
    }
    let cap = state.body_caps.for_path(parts.uri.path());
    let body_read_started = Instant::now();
    let body_read_span = tracing::info_span!(
        "proxy.read_request_body",
        "cc_lb.request_body_read_ms" = tracing::field::Empty,
        "http.request.body.size" = tracing::field::Empty,
        outcome = tracing::field::Empty,
    );
    let body_result = read_request_body(&parts.headers, body, cap, observer.clone(), &auth)
        .instrument(body_read_span.clone())
        .await;
    let body_read_ms = body_read_started
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    if let Ok(body_read_ms) = i64::try_from(body_read_ms) {
        body_read_span.record("cc_lb.request_body_read_ms", body_read_ms);
    } else {
        body_read_span.record("cc_lb.request_body_read_ms", body_read_ms);
    }

    let body = match body_result {
        Ok(body) => {
            let body_bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);
            if let Ok(body_bytes) = i64::try_from(body_bytes) {
                body_read_span.record("http.request.body.size", body_bytes);
            } else {
                body_read_span.record("http.request.body.size", body_bytes);
            }
            body_read_span.record("outcome", "success");
            drop(body_read_span);
            if let Some(observer) = observer.as_ref() {
                observer.set_request_body_timing(body_read_ms, Some(body_bytes));
            }
            body
        }
        Err(RequestBodyReadError::TooLarge) => {
            body_read_span.record("outcome", "too_large");
            drop(body_read_span);
            if let Some(observer) = observer.as_ref() {
                observer.set_request_body_timing(body_read_ms, None);
                observer.terminate_body_too_large(cap as u64);
            }
            return body_too_large_response();
        }
        Err(RequestBodyReadError::Read(source)) => {
            body_read_span.record("outcome", "read_error");
            drop(body_read_span);
            if let Some(observer) = observer.as_ref() {
                observer.set_request_body_timing(body_read_ms, None);
            }
            let mut response = Response::new(Body::from(format!("body read failed: {source}")));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            response
                .extensions_mut()
                .insert(cc_lb_engine::InternalFailure::body_read_failed(
                    cc_lb_domain::InternalError {
                        stage: cc_lb_domain::InternalErrorStage::Ingress,
                        kind: cc_lb_domain::InternalErrorKind::InvalidInput,
                        message: Some(source.to_string()),
                    },
                ));
            return response;
        }
    };
    let request = Request::from_parts(parts, body);
    let request = match responses_request_body::normalize(request, cap, &auth, observer.as_ref()) {
        Ok(request) => request,
        Err(error) => return error.into_response(),
    };
    match state.lifecycle.handle(request, &auth).await {
        Ok(response) => response,
        Err(source) => {
            let mut response = Response::new(Body::from(source.to_string()));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        }
    }
}

/// Buffers the request body under `cap`.
///
/// `_auth` is the proof that the request was already authenticated: the body
/// must never be buffered for a caller that has not authenticated. The
/// parameter is unused here on purpose — it exists so that moving this call
/// ahead of authentication fails to compile instead of silently reopening
/// the pre-auth buffering hole.
pub(super) async fn read_request_body(
    headers: &HeaderMap,
    body: Body,
    cap: usize,
    observer: Option<cc_lb_engine::LifecycleContext>,
    _auth: &cc_lb_engine::Authenticated,
) -> Result<Bytes, RequestBodyReadError> {
    let mut timings = RequestBodyTimingGuard::new(observer);
    let content_length_too_large = content_length_exceeds_cap(headers, cap);
    if content_length_too_large {
        timings.finish();
        return Err(RequestBodyReadError::TooLarge);
    }

    let result = collect_limited_body(body, cap, &mut timings)
        .await
        .map_err(|source| {
            if source.is::<LengthLimitError>() {
                RequestBodyReadError::TooLarge
            } else {
                RequestBodyReadError::Read(source)
            }
        });
    timings.finish();
    result
}

fn content_length_exceeds_cap(headers: &HeaderMap, cap: usize) -> bool {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| usize::try_from(length).map_or(true, |length| length > cap))
}

async fn collect_limited_body(
    body: Body,
    cap: usize,
    timings: &mut RequestBodyTimingGuard,
) -> Result<Bytes, tower::BoxError> {
    let mut body = Limited::new(body, cap);
    let mut first: Option<Bytes> = None;
    let mut combined: Option<BytesMut> = None;

    loop {
        timings.begin_wait();
        let frame = body.frame().await;
        let received = Instant::now();
        timings.end_wait(received);
        let Some(frame) = frame else {
            break;
        };

        let frame = frame?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if data.is_empty() {
            continue;
        }

        timings.observe_data(received);
        if let Some(buffer) = combined.as_mut() {
            buffer.extend_from_slice(&data);
        } else if let Some(initial) = first.take() {
            let mut buffer = BytesMut::with_capacity(initial.len().saturating_add(data.len()));
            buffer.extend_from_slice(&initial);
            buffer.extend_from_slice(&data);
            combined = Some(buffer);
        } else {
            first = Some(data);
        }
    }

    Ok(combined.map(BytesMut::freeze).or(first).unwrap_or_default())
}

pub(super) fn body_too_large_response() -> Response<Body> {
    anthropic_error_response(
        StatusCode::PAYLOAD_TOO_LARGE,
        "body_too_large",
        "request body exceeds configured cap",
    )
}

pub(super) async fn oauth_usage_handler(
    State(state): State<ProxyState>,
    request: Request<Body>,
) -> Response<Body> {
    let authn = &state.builtin_authn;
    let dynamic_view = state.dynamic_view.load();
    // The request reached the authentication attempt: replay buffered
    // pre-auth lifecycle events so the request-log row is complete.
    if let Some(ctx) = request.extensions().get::<cc_lb_engine::LifecycleContext>() {
        ctx.mark_authn_reached();
    }
    if let Err(error) =
        cc_lb_engine::authenticate_first(authn, request.headers(), &dynamic_view.principal_view)
            .await
    {
        let status = StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::UNAUTHORIZED);
        let mut response = json_response(
            status,
            serde_json::json!({ "error": "authentication_error", "message": error.to_string() }),
        );
        response
            .extensions_mut()
            .insert(TerminalClassification::local(status));
        return response;
    }

    let mut response = match cc_lb_control::subscription_quotas::build_cc_lb_oauth_usage_response(
        state.storage.as_ref(),
        &state.dynamic_view,
        &*state.clock,
    )
    .await
    {
        Ok(response) => json_response(StatusCode::OK, response),
        Err(error) => {
            tracing::error!(%error, "cc-lb oauth usage response build failed");
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({ "error": "usage_unavailable" }),
            )
        }
    };
    let status = response.status();
    response
        .extensions_mut()
        .insert(TerminalClassification::local(status));
    response
}

pub(super) fn json_response(status: StatusCode, value: impl Serialize) -> Response<Body> {
    match serde_json::to_vec(&value) {
        Ok(bytes) => Response::builder()
            .status(status)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(bytes))
            .unwrap_or_else(|_| Response::new(Body::empty())),
        Err(error) => {
            tracing::error!(%error, "json response serialization failed");
            let mut response = Response::new(Body::from("json serialization failed"));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        }
    }
}

pub(super) async fn lifecycle_middleware(
    State(state): State<ProxyState>,
    mut request: Request<Body>,
    next: Next,
) -> Response<Body> {
    if let Some(scope) = request.extensions().get::<ProviderScope>().copied() {
        let provider = match scope.0 {
            cc_lb_domain::Provider::Anthropic => "anthropic",
            cc_lb_domain::Provider::Openai => "openai",
            cc_lb_domain::Provider::Gemini => "gemini",
        };
        tracing::Span::current().record("cc_lb.provider_scope", provider);
    }
    let ctx: Option<cc_lb_engine::LifecycleContext> = state.lifecycle.event_bus().map(|bus| {
        let request_id = request
            .headers()
            .get("request-id")
            .and_then(|v| v.to_str().ok())
            .expect("request_id_middleware sets valid UTF-8 request-id")
            .to_owned();
        let context = cc_lb_engine::LifecycleContext::new(request_id, bus, &state.clock);
        context.set_event_kind(cc_lb_request_log::RequestEventKind::from_path(
            request.uri().path(),
        ));
        context
    });
    if let Some(c) = ctx.as_ref() {
        request.extensions_mut().insert(c.clone());
    }
    let mut response = next.run(request).await;
    // Only a response carrying an explicit terminal marker is finalized here.
    // A streaming proxy response returns headers long before its body
    // finishes, so terminating unconditionally would finalize every stream at
    // header time; unmarked responses fall through to the context's Drop
    // backstop instead. `InternalFailure` (typed internal error) is checked
    // before `TerminalClassification` (local status-only outcome).
    if let Some(c) = ctx.as_ref() {
        if let Some(failure) = response
            .extensions_mut()
            .remove::<cc_lb_engine::InternalFailure>()
        {
            c.terminate_failure(failure);
        } else if let Some(marker) = response.extensions().get::<TerminalClassification>() {
            c.terminate_local(*marker);
        }
    }
    response
}
