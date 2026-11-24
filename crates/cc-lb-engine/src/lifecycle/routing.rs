use super::*;

pub fn observe_rate_limits(
    headers: &HeaderMap,
    upstream_id: Uuid,
    observed_at: u64,
) -> Vec<UpstreamRateLimitObservationRecord> {
    let mut observations = parse_anthropic_rate_limit_headers(headers)
        .into_iter()
        .chain(parse_openai_rate_limit_headers(headers))
        .chain(parse_standard_rate_limit_headers(headers))
        .map(|snapshot| UpstreamRateLimitObservationRecord {
            upstream_id,
            window: snapshot.window,
            kind: snapshot.kind,
            limit: snapshot.limit,
            remaining: snapshot.remaining,
            reset: snapshot.reset,
            observed_at_unix_secs: observed_at,
        })
        .collect::<Vec<_>>();
    if observations.is_empty() && headers.get(RETRY_AFTER).is_some() {
        observations.extend(
            cc_lb_quota::parse_retry_after_rate_limit_headers(headers)
                .into_iter()
                .map(|snapshot| UpstreamRateLimitObservationRecord {
                    upstream_id,
                    window: snapshot.window,
                    kind: snapshot.kind,
                    limit: snapshot.limit,
                    remaining: snapshot.remaining,
                    reset: snapshot.reset,
                    observed_at_unix_secs: observed_at,
                }),
        );
    }
    observations
}

pub(super) struct FilterPipelineResult {
    pub(super) candidates: Vec<UpstreamCandidate>,
    pub(super) stages: Vec<StageDecision>,
    pub(super) internal_errors: Vec<InternalError>,
}

impl FilterPipelineResult {
    pub(super) fn routing_trace(&self, terminal_decision: TerminalDecision) -> RoutingTrace {
        RoutingTrace {
            stages: self.stages.clone(),
            terminal_decision: Some(terminal_decision),
        }
    }
}

pub(super) fn subscription_preference_trace(
    routing_trace: &RoutingTrace,
) -> Option<&cc_lb_domain::SubscriptionPreferenceTrace> {
    routing_trace
        .stages
        .iter()
        .find_map(|stage| stage.subscription_preference.as_ref())
}

pub(crate) fn resolved_candidate_urgency(
    routing_trace: &RoutingTrace,
    resolved_upstream_id: Uuid,
) -> Option<&cc_lb_domain::CandidateUrgency> {
    subscription_preference_trace(routing_trace).and_then(|trace| {
        trace
            .candidates
            .iter()
            .find(|candidate| candidate.upstream_id == resolved_upstream_id)
    })
}

pub(super) fn execute_filter_pipeline(
    filters: &[Arc<dyn FilterPlugin>],
    ctx: &RequestContext,
    principal: &Principal,
    candidates: Vec<UpstreamCandidate>,
) -> FilterPipelineResult {
    let routing_context = ctx.routing_context();
    let mut current = candidates;
    let mut stages = Vec::with_capacity(filters.len());
    let mut internal_errors = Vec::new();

    for (stage_index, filter) in filters.iter().enumerate() {
        let stage_name = filter.plugin_name().to_owned();
        let stage_started = Instant::now();
        match filter.filter(&routing_context, principal, &current) {
            Ok(output) => {
                let stage_elapsed = stage_started.elapsed();
                let duration_ms = duration_to_ms(stage_elapsed);
                let stage_idx = u8::try_from(stage_index).unwrap_or(u8::MAX);
                let output = match validate_filter_output(
                    &output,
                    &current,
                    stage_idx,
                    &mut internal_errors,
                ) {
                    Ok(output) => output,
                    Err(error) => {
                        let message = format!("filter output validation failed: {error}");
                        tracing::warn!(
                            stage = stage_name.as_str(),
                            duration_ms,
                            error = message.as_str(),
                            "router filter returned invalid output; passing candidates through"
                        );
                        stages.push(StageDecision {
                            stage_name,
                            upstream_id: current.first().map(|candidate| candidate.upstream_id),
                            reason: Some(message.clone()),
                            duration_us: duration_to_us(stage_elapsed),
                            subscription_preference: None,
                        });
                        internal_errors.push(InternalError {
                            stage: InternalErrorStage::RouterFilter,
                            kind: InternalErrorKind::InvalidOutput,
                            message: Some(message),
                        });
                        continue;
                    }
                };
                tracing::debug!(
                    stage = stage_name.as_str(),
                    duration_ms,
                    input_candidates = current.len(),
                    kept_candidates = output.kept_upstream_ids.len(),
                    "router filter stage completed"
                );
                stages.push(StageDecision {
                    stage_name,
                    upstream_id: output.kept_upstream_ids.first().copied(),
                    reason: Some(output.reason.clone()),
                    duration_us: duration_to_us(stage_elapsed),
                    subscription_preference: output.subscription_preference.clone(),
                });
                current = keep_filter_candidates(&current, &output.kept_upstream_ids);
            }
            Err(error @ (FilterError::Trap { .. } | FilterError::Runtime { .. })) => {
                let stage_elapsed = stage_started.elapsed();
                let duration_ms = duration_to_ms(stage_elapsed);
                let message = error.to_string();
                tracing::warn!(
                    stage = stage_name.as_str(),
                    duration_ms,
                    error = message.as_str(),
                    "router filter stage failed; passing candidates through"
                );
                stages.push(StageDecision {
                    stage_name,
                    upstream_id: current.first().map(|candidate| candidate.upstream_id),
                    reason: Some(message.clone()),
                    duration_us: duration_to_us(stage_elapsed),
                    subscription_preference: None,
                });
                internal_errors.push(InternalError {
                    stage: InternalErrorStage::Router,
                    kind: InternalErrorKind::PluginError,
                    message: Some(message),
                });
            }
        }
    }

    FilterPipelineResult {
        candidates: current,
        stages,
        internal_errors,
    }
}

pub(super) struct ValidatedOutput {
    pub(super) kept_upstream_ids: Vec<Uuid>,
    pub(super) reason: String,
    pub(super) subscription_preference: Option<cc_lb_domain::SubscriptionPreferenceTrace>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(super) enum ValidationError {
    #[error("unknown upstream id {unknown_id}")]
    Unknown { unknown_id: Uuid },
    #[error("duplicate upstream id {id}")]
    Duplicate { id: Uuid },
    #[error("kept output length {kept_len} exceeds input length {input_len}")]
    Superset { kept_len: usize, input_len: usize },
}

pub(super) fn validate_filter_output(
    out: &FilterOutput,
    input: &[UpstreamCandidate],
    stage_idx: u8,
    internal_errors: &mut Vec<InternalError>,
) -> Result<ValidatedOutput, ValidationError> {
    let _ = stage_idx;
    let mut seen = HashSet::with_capacity(out.kept_upstream_ids.len());
    for id in &out.kept_upstream_ids {
        if !seen.insert(*id) {
            return Err(ValidationError::Duplicate { id: *id });
        }
    }

    if out.kept_upstream_ids.len() > input.len() {
        return Err(ValidationError::Superset {
            kept_len: out.kept_upstream_ids.len(),
            input_len: input.len(),
        });
    }

    let input_ids = input
        .iter()
        .map(|candidate| candidate.upstream_id)
        .collect::<HashSet<_>>();
    for id in &out.kept_upstream_ids {
        if !input_ids.contains(id) {
            return Err(ValidationError::Unknown { unknown_id: *id });
        }
    }

    let mut per_candidate_reasons = out.per_candidate_reasons.clone();
    if !per_candidate_reasons.is_empty() && per_candidate_reasons.len() != input.len() {
        if per_candidate_reasons.len() > input.len() {
            per_candidate_reasons.truncate(input.len());
        }
        internal_errors.push(InternalError {
            stage: InternalErrorStage::RouterFilter,
            kind: InternalErrorKind::InvalidOutput,
            message: Some("per_candidate_reasons sanitized".to_owned()),
        });
    }

    Ok(ValidatedOutput {
        kept_upstream_ids: out.kept_upstream_ids.clone(),
        reason: out.reason.clone(),
        subscription_preference: out.subscription_preference.clone(),
    })
}

pub(super) fn keep_filter_candidates(
    candidates: &[UpstreamCandidate],
    kept_upstream_ids: &[Uuid],
) -> Vec<UpstreamCandidate> {
    candidates
        .iter()
        .filter(|candidate| kept_upstream_ids.contains(&candidate.upstream_id))
        .cloned()
        .collect()
}

pub(super) fn terminal_candidates(
    candidates: &[UpstreamCandidate],
    terminal_decision: &TerminalDecision,
) -> Vec<UpstreamCandidate> {
    terminal_decision
        .upstream_id
        .and_then(|upstream_id| {
            candidates
                .iter()
                .find(|candidate| candidate.upstream_id == upstream_id)
        })
        .cloned()
        .into_iter()
        .collect()
}

pub(super) fn filter_codex_models_catalog(
    body: &[u8],
    principal_view: &cc_lb_control::api_keys::principal_view::PrincipalView,
    principal_id: &str,
) -> Result<Bytes, ()> {
    let mut catalog: Value = sonic_rs::from_slice(body).map_err(|_| ())?;
    let models = catalog
        .get_mut("models")
        .and_then(Value::as_array_mut)
        .ok_or(())?;
    models.retain(|model| {
        model
            .get("slug")
            .and_then(Value::as_str)
            .is_some_and(|slug| principal_view.is_model_allowed(principal_id, slug))
    });
    serde_json::to_vec(&catalog)
        .map(Bytes::from)
        .map_err(|_| ())
}

pub(super) fn upstream_for_record(record: &UpstreamRecord) -> Result<Upstream, String> {
    let base_url = match record.kind {
        StorageUpstreamKind::VertexApiKey | StorageUpstreamKind::VertexCredential => None,
        _ => record
            .base_url
            .clone()
            .or_else(|| Some(default_base_url(record.kind))),
    };
    match record.kind {
        StorageUpstreamKind::AnthropicApiKey | StorageUpstreamKind::AnthropicOauth => {
            Ok(Upstream::AnthropicDirect { base_url })
        }
        StorageUpstreamKind::OpenaiOauth | StorageUpstreamKind::OpenaiApiKey => {
            Ok(Upstream::OpenaiDirect { base_url })
        }
        StorageUpstreamKind::GeminiApiKey | StorageUpstreamKind::GeminiOauth => {
            Ok(Upstream::GeminiDirect { base_url })
        }
        StorageUpstreamKind::VertexApiKey | StorageUpstreamKind::VertexCredential => {
            let Some(config) = record.vertex.as_ref() else {
                return Err("Vertex upstream is missing its configuration".to_owned());
            };
            let base_url = record
                .base_url
                .as_ref()
                .map(origin_only)
                .transpose()
                .map_err(|error| format!("invalid Vertex base URL: {error}"))?
                .or_else(|| Url::parse(&vertex_origin(&config.target)).ok());
            Ok(Upstream::GeminiDirect { base_url })
        }
    }
}

pub(super) fn origin_only(url: &Url) -> Result<Url, url::ParseError> {
    let mut origin = Url::parse(url.as_str())?;
    origin.set_path("");
    origin.set_query(None);
    origin.set_fragment(None);
    Ok(origin)
}

pub(super) fn default_base_url(kind: StorageUpstreamKind) -> Url {
    let value = match kind {
        StorageUpstreamKind::AnthropicApiKey | StorageUpstreamKind::AnthropicOauth => {
            DEFAULT_ANTHROPIC_BASE_URL
        }
        StorageUpstreamKind::OpenaiOauth => DEFAULT_OPENAI_CODEX_BASE_URL,
        StorageUpstreamKind::OpenaiApiKey => DEFAULT_OPENAI_BASE_URL,
        StorageUpstreamKind::GeminiApiKey => DEFAULT_GEMINI_BASE_URL,
        StorageUpstreamKind::GeminiOauth => DEFAULT_GEMINI_CODE_ASSIST_BASE_URL,
        // Kind-only fallback; Vertex records normally resolve their host from
        // `VertexUpstreamConfig` via `vertex_origin`.
        StorageUpstreamKind::VertexApiKey | StorageUpstreamKind::VertexCredential => {
            DEFAULT_VERTEX_BASE_URL
        }
    };
    Url::parse(value).expect("provider base URL parses")
}

pub(super) fn upstream_kind_for_candidate(kind: StorageUpstreamKind) -> CandidateUpstreamKind {
    match kind {
        StorageUpstreamKind::AnthropicApiKey => CandidateUpstreamKind::AnthropicApiKey,
        StorageUpstreamKind::AnthropicOauth => CandidateUpstreamKind::AnthropicOauth,
        StorageUpstreamKind::OpenaiOauth => CandidateUpstreamKind::OpenaiOauth,
        StorageUpstreamKind::OpenaiApiKey => CandidateUpstreamKind::OpenaiApiKey,
        StorageUpstreamKind::GeminiApiKey => CandidateUpstreamKind::GeminiApiKey,
        StorageUpstreamKind::GeminiOauth => CandidateUpstreamKind::GeminiOauth,
        StorageUpstreamKind::VertexApiKey => CandidateUpstreamKind::VertexApiKey,
        StorageUpstreamKind::VertexCredential => CandidateUpstreamKind::VertexCredential,
    }
}

pub(super) fn storage_upstream_kind_label(kind: StorageUpstreamKind) -> &'static str {
    match kind {
        StorageUpstreamKind::AnthropicApiKey => "anthropic_key",
        StorageUpstreamKind::AnthropicOauth => "anthropic_oauth",
        StorageUpstreamKind::OpenaiOauth => "openai_oauth",
        StorageUpstreamKind::OpenaiApiKey => "openai_api_key",
        StorageUpstreamKind::GeminiApiKey => "gemini_api_key",
        StorageUpstreamKind::GeminiOauth => "gemini_oauth",
        StorageUpstreamKind::VertexApiKey => "vertex_api_key",
        StorageUpstreamKind::VertexCredential => "vertex_credential",
    }
}
/// Distinct upstreams one request may try, counting the first. The
/// same-upstream 401 refresh retry does not count against it.
pub(super) const MAX_UPSTREAM_ATTEMPTS: u8 = 3;

/// Why an attempt moved to another upstream. Every reason is a failure that
/// happened before the upstream processed the request, so retrying elsewhere
/// cannot duplicate a generation.
#[derive(Clone, Copy, Debug)]
pub(super) enum FailoverReason {
    SignerBuild,
    Sign,
    Unauthorized,
    RateLimited,
}

impl FailoverReason {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::SignerBuild => "signer_build_failed",
            Self::Sign => "sign_failed",
            Self::Unauthorized => "upstream_401",
            Self::RateLimited => "upstream_429",
        }
    }
}

/// One routing decision: the selected upstream and the trace data that
/// `RouteCompleted` reports for it.
pub(super) struct RoutedUpstream<'v> {
    pub(super) record: &'v UpstreamRecord,
    pub(super) route: RouteDecision,
    pub(super) raw_passthrough_base_url: Option<Url>,
    pub(super) routing_trace: RoutingTrace,
    pub(super) selected_cache_score: Option<CacheScore>,
    pub(super) selected_cache_match: Option<SelectedCacheMatch>,
    pub(super) cache_bundle: PromptCacheBundle,
    pub(super) route_ms: u64,
}

impl<'v> RoutedUpstream<'v> {
    pub(super) fn new(
        record: &'v UpstreamRecord,
        route: RouteDecision,
        raw_passthrough_base_url: Option<Url>,
        pipeline_result: &FilterPipelineResult,
        terminal_decision: TerminalDecision,
        selected_cache_match: Option<SelectedCacheMatch>,
        route_ms: u64,
    ) -> Self {
        let selected_cache_score = pipeline_result
            .candidates
            .iter()
            .find(|candidate| candidate.upstream_id == route.upstream_id)
            .and_then(|candidate| candidate.cache_score.clone());
        Self {
            record,
            route,
            raw_passthrough_base_url,
            routing_trace: pipeline_result.routing_trace(terminal_decision),
            selected_cache_score,
            selected_cache_match,
            cache_bundle: PromptCacheBundle::default(),
            route_ms,
        }
    }

    pub(super) fn with_cache_bundle(mut self, cache_bundle: PromptCacheBundle) -> Self {
        self.cache_bundle = cache_bundle;
        self
    }
}

pub(super) fn route_for_record(
    record: &UpstreamRecord,
    cached: &PrincipalSpecCached,
) -> Result<(RouteDecision, Option<Url>), String> {
    let raw_passthrough_base_url = record.base_url.clone().or_else(|| {
        (record.kind == StorageUpstreamKind::GeminiOauth).then(|| default_base_url(record.kind))
    });
    if matches!(
        record.kind,
        StorageUpstreamKind::VertexApiKey | StorageUpstreamKind::VertexCredential
    ) && raw_passthrough_base_url.is_none()
        && record
            .vertex
            .as_ref()
            .and_then(|config| Url::parse(&vertex_origin(&config.target)).ok())
            .is_none()
    {
        return Err("Vertex route has no configured origin".to_owned());
    }
    let upstream = upstream_for_record(record)?;
    let dialect = cached
        .resolved_dialect(record.id)
        .cloned()
        .unwrap_or_else(|| native_route_dialect(record));
    Ok((
        RouteDecision {
            upstream_id: record.id,
            upstream,
            dialect,
        },
        raw_passthrough_base_url,
    ))
}

/// Construct the native HTTP dialect outside request execution when composing
/// a response-only provider plugin. OAuth path handling matches ordinary routes.
pub fn native_route_dialect(record: &UpstreamRecord) -> Arc<dyn UpstreamDialect> {
    Arc::new(RawPassthroughDialect {
        base_url: match record.kind {
            StorageUpstreamKind::VertexApiKey | StorageUpstreamKind::VertexCredential => record
                .base_url
                .clone()
                .or_else(|| {
                    record
                        .vertex
                        .as_ref()
                        .and_then(|config| Url::parse(&vertex_origin(&config.target)).ok())
                })
                .unwrap_or_else(|| default_base_url(record.kind)),
            _ => record
                .base_url
                .clone()
                .unwrap_or_else(|| default_base_url(record.kind)),
        },
        openai_oauth: record.kind == StorageUpstreamKind::OpenaiOauth,
    })
}

/// Emit `RouteCompleted` for `routed`. A failover emits it again for the next
/// upstream; the request-event assembler keeps the last one.
pub(super) fn emit_route_completed(
    observer: Option<&LifecycleContext>,
    routed: &RoutedUpstream<'_>,
    model: Option<&str>,
) {
    let Some(o) = observer else {
        return;
    };
    o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
        event_id: o.event_id().to_owned(),
        result: Ok(route_info_for_routed(routed, model)),
        routing_trace: Some(routed.routing_trace.clone()),
    });
}

pub(super) fn route_info_for_routed(
    routed: &RoutedUpstream<'_>,
    model: Option<&str>,
) -> cc_lb_lifecycle::RouteInfo {
    let upstream_id = routed.route.upstream_id;
    let selected_cache_score = routed.selected_cache_score.as_ref();
    let selected_quota_candidate = resolved_candidate_urgency(&routed.routing_trace, upstream_id);
    let subscription_trace = subscription_preference_trace(&routed.routing_trace);
    cc_lb_lifecycle::RouteInfo {
        upstream_id,
        upstream_name: routed.record.name.clone(),
        model: model.map(ToOwned::to_owned),
        upstream_kind: Some(storage_upstream_kind_label(routed.record.kind).to_owned()),
        route_ms: Some(routed.route_ms),
        routing_trace: Some(routed.routing_trace.clone()),
        predicted_cache_read_tokens: Some(
            selected_cache_score.map_or(0, |score| score.predicted_cache_read_tokens),
        ),
        matched_v3_cache_key: selected_cache_score
            .and_then(|score| score.matched_v3_cache_key.clone()),
        breakpoint_content_block_index: selected_cache_score
            .and_then(|score| score.breakpoint_content_block_index),
        matched_content_block_index: selected_cache_score
            .and_then(|score| score.matched_content_block_index),
        lookback_distance: selected_cache_score.and_then(|score| score.lookback_distance),
        predicted_cache_creation_tokens_5m: selected_cache_score
            .map(|score| score.predicted_cache_creation_tokens_5m),
        predicted_cache_creation_tokens_1h: selected_cache_score
            .map(|score| score.predicted_cache_creation_tokens_1h),
        token_estimate_source: selected_cache_score
            .and_then(|score| score.token_estimate_source.clone()),
        cache_breakpoints: Some(routed.cache_bundle.request_breakpoints.clone()),
        cache_prefix_hash: routed.cache_bundle.cache_prefix_hash.clone(),
        cache_value_micros: subscription_trace.and_then(|trace| {
            trace
                .candidates
                .iter()
                .find(|candidate| candidate.upstream_id == upstream_id)
                .and_then(|candidate| candidate.cache_value_micros)
        }),
        formula_winner_upstream_id: subscription_trace
            .and_then(|trace| trace.formula_winner_upstream_id),
        kept_upstream_id: subscription_trace.and_then(|trace| trace.kept_upstream_id),
        quota_urgency_5h: selected_quota_candidate.and_then(|candidate| candidate.quota_urgency_5h),
        quota_urgency_7d: selected_quota_candidate.and_then(|candidate| candidate.quota_urgency_7d),
        quota_urgency_combined: selected_quota_candidate
            .and_then(|candidate| candidate.quota_urgency_combined),
        quota_warning_multiplier: selected_quota_candidate
            .map(|candidate| candidate.warning_multiplier),
    }
}

pub(super) fn vertex_scope(record: &UpstreamRecord) -> Option<(String, String)> {
    match record.vertex.as_ref()?.target {
        cc_lb_domain::VertexTarget::Regional {
            ref project_id,
            ref location,
        } => Some((project_id.to_string(), location.to_string())),
        cc_lb_domain::VertexTarget::Express => None,
    }
}
/// Re-routes a request after a failover-eligible failure. Re-runs the
/// principal's filter pipeline over fresh candidates minus every upstream
/// already tried, so one routing convention covers the first pick and each
/// fallback.
pub(super) struct FailoverRouter<'v, 'a> {
    pub(super) view: &'v DynamicView,
    pub(super) cached: &'a PrincipalSpecCached,
    pub(super) router_pipeline: &'a RouterPipelineCache,
    pub(super) ctx: &'a RequestContext,
    pub(super) principal: &'a Principal,
    pub(super) warm_entries: &'a HashMap<Uuid, Vec<WarmCacheEntry>>,
    pub(super) cache_bundles: &'a PromptCacheBundles,
    pub(super) request_kind: RequestKind,
    pub(super) compat_api_key_only_features:
        &'a [crate::openai_compat::request::ApiKeyOnlyFeature],
    pub(super) feature_pinned_api_key: bool,
    pub(super) transcription_oauth_disabled: bool,
    pub(super) observer: Option<&'a LifecycleContext>,
    pub(super) handle_span: &'a tracing::Span,
    /// False when request affinity pins the upstream (web-search continuity
    /// is account-bound).
    pub(super) enabled: bool,
    pub(super) excluded: Vec<Uuid>,
    /// Vertex scope keys excluded after Vertex rate-limit observations.
    pub(super) vertex_rate_limited_scopes: HashSet<(String, String)>,
    pub(super) upstream_attempts: u8,
}

impl<'v, 'a> FailoverRouter<'v, 'a> {
    /// Routes to the next upstream, or returns `None` when the caller must
    /// relay the current failure. The caller announces the new route.
    pub(super) fn next(
        &mut self,
        lifecycle: &Lifecycle,
        failed: &RoutedUpstream<'_>,
        reason: FailoverReason,
    ) -> Option<RoutedUpstream<'v>> {
        if !self.enabled || self.upstream_attempts >= MAX_UPSTREAM_ATTEMPTS {
            return None;
        }
        let failed_id = failed.route.upstream_id;
        if !self.excluded.contains(&failed_id) {
            self.excluded.push(failed_id);
        }
        let route_start = Instant::now();
        let (mut candidates, mut selected_cache_matches) =
            build_candidates_with_matches_for_bundles(
                self.view,
                &self.principal.id,
                self.request_kind,
                &self.ctx.cache_breakpoints,
                self.warm_entries,
                Some(self.cache_bundles),
                &*lifecycle.clock,
            );
        candidates.retain(|candidate| !self.excluded.contains(&candidate.upstream_id));
        if self.ctx.codex_backend {
            candidates
                .retain(|candidate| candidate.kind == cc_lb_domain::UpstreamKind::OpenaiOauth);
        }
        if matches!(reason, FailoverReason::RateLimited)
            && matches!(
                failed.record.kind,
                StorageUpstreamKind::VertexApiKey | StorageUpstreamKind::VertexCredential
            )
            && let Some(scope) = vertex_scope(failed.record)
        {
            self.vertex_rate_limited_scopes.insert(scope);
        }
        if !self.vertex_rate_limited_scopes.is_empty() {
            candidates.retain(|candidate| {
                self.view
                    .upstreams_snapshot()
                    .iter()
                    .find(|record| record.id == candidate.upstream_id)
                    .and_then(vertex_scope)
                    .is_none_or(|candidate_scope| {
                        !self.vertex_rate_limited_scopes.contains(&candidate_scope)
                    })
            });
        }
        if self.feature_pinned_api_key {
            candidates
                .retain(|candidate| candidate.kind == cc_lb_domain::UpstreamKind::OpenaiApiKey);
        } else {
            retain_api_key_candidates(&mut candidates, self.compat_api_key_only_features);
        }
        if self.transcription_oauth_disabled {
            candidates
                .retain(|candidate| candidate.kind != cc_lb_domain::UpstreamKind::OpenaiOauth);
        }
        // The last allowed attempt goes to an API key when one remains, so the
        // attempt cap never strands a request on exhausted OAuth accounts.
        let last_attempt =
            !self.ctx.codex_backend && self.upstream_attempts + 1 == MAX_UPSTREAM_ATTEMPTS;
        if last_attempt
            && candidates
                .iter()
                .any(|candidate| candidate.kind.credential_kind() == CredentialKind::ApiKey)
        {
            candidates
                .retain(|candidate| candidate.kind.credential_kind() == CredentialKind::ApiKey);
        }
        if candidates.is_empty() {
            return None;
        }
        let filters = self
            .router_pipeline
            .filters_for(self.request_kind.provider());
        let mut pipeline_result =
            execute_filter_pipeline(filters, self.ctx, self.principal, candidates);
        if let Some(o) = self.observer {
            for error in pipeline_result.internal_errors.drain(..) {
                o.record_internal_error(error);
            }
        }
        let terminal_decision = lifecycle.select_terminal_upstream(
            self.router_pipeline.terminal.clone(),
            &pipeline_result.candidates,
        );
        let selected = terminal_decision.upstream_id.and_then(|upstream_id| {
            pipeline_result
                .candidates
                .iter()
                .find(|candidate| candidate.upstream_id == upstream_id)
        })?;
        // The subscription filter keeps a lone survivor even when its quota is
        // proven exhausted; on failover that would revive an account already
        // known to be spent, so relay the current failure instead.
        if filters
            .iter()
            .any(|filter| filter.plugin_id() == BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
            && subscription_hard_blocked(self.ctx.canonical_model_id.as_str(), selected)
        {
            return None;
        }
        let upstream_id = selected.upstream_id;
        let record = self
            .view
            .upstreams_snapshot()
            .iter()
            .find(|record| record.id == upstream_id)?;
        let (route, raw_passthrough_base_url) = match route_for_record(record, self.cached) {
            Ok(resolved) => resolved,
            Err(error) => {
                tracing::warn!(
                    upstream = %record.name,
                    %error,
                    "failover target has no upstream route; relaying the current failure"
                );
                return None;
            }
        };
        self.upstream_attempts += 1;
        let routed = RoutedUpstream::new(
            record,
            route,
            raw_passthrough_base_url,
            &pipeline_result,
            terminal_decision,
            selected_cache_matches.remove(&upstream_id),
            duration_to_ms(route_start.elapsed()),
        )
        .with_cache_bundle(self.cache_bundles.for_upstream(record).clone());
        tracing::warn!(
            from_upstream = %failed.record.name,
            from_upstream_id = %failed_id,
            to_upstream = %record.name,
            to_upstream_id = %upstream_id,
            reason = reason.as_str(),
            "failing over to another upstream"
        );
        metrics::counter!(
            "cc_lb_upstream_failover_total",
            "reason" => reason.as_str(),
            "from_kind" => storage_upstream_kind_label(failed.record.kind),
            "to_kind" => storage_upstream_kind_label(record.kind)
        )
        .increment(1);
        self.handle_span
            .record("cc_lb.upstream.name", record.name.as_str());
        self.handle_span
            .record("cc_lb.upstream.id", tracing::field::display(upstream_id));
        Some(routed)
    }
}

/// Start the next attempt: clear the previous attempt's observations (the
/// assembler resets on `UpstreamAttempt` with `attempt_num > 1`), then
/// announce the upstream. The first dispatch stays the TTFB origin.
pub(super) fn announce_attempt(
    observer: Option<&LifecycleContext>,
    attempt_num: &mut u32,
    upstream_id: Uuid,
    attempt_timings: &mut AttemptTimings,
    first_dispatch_started: Option<Instant>,
) {
    if *attempt_num > 0 {
        attempt_timings.reset_attempt_stages();
        if let Some(o) = observer {
            o.reset_attempt_timings();
            if let Some(dispatch_started) = first_dispatch_started {
                o.set_io_timings(cc_lb_lifecycle::RequestIoTimings {
                    retry_overhead_ms: Some(dispatch_started.elapsed().as_secs_f64() * 1_000.0),
                    ..Default::default()
                });
            }
        }
    }
    *attempt_num += 1;
    if let Some(o) = observer {
        o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UpstreamAttempt {
            event_id: o.event_id().to_owned(),
            attempt_num: *attempt_num,
            upstream_id,
        });
    }
}
