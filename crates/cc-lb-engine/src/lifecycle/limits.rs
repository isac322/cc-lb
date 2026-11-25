use super::cache::request_service_tier;
use super::*;

pub(super) fn use_image_rates_for_admission(
    capability: Option<ProviderCapability>,
    path: &str,
) -> bool {
    matches!(
        capability,
        Some(ProviderCapability::OpenaiResponses | ProviderCapability::OpenaiChatCompletions)
    ) || (capability == Some(ProviderCapability::OpenaiImages)
        && matches!(path, "/v1/images/edits" | "/v1/images/variations"))
}
pub(super) fn admission_fanout_multiplier(
    upstream_kind: StorageUpstreamKind,
    capability: Option<ProviderCapability>,
    requested: i64,
) -> i64 {
    if upstream_kind == StorageUpstreamKind::OpenaiOauth
        && capability == Some(ProviderCapability::OpenaiChatCompletions)
    {
        requested
    } else {
        1
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LimitRequest {
    pub(super) model: String,
    pub(super) max_tokens: Option<i64>,
    pub(super) input_tokens_estimate: i64,
    pub(super) local_cache_input: Option<cc_lb_pricing::LocalCacheInputEstimate>,
    pub(super) estimated_cache_tokens: u64,
    pub(super) can_cross_cache_cliff: bool,
    pub(super) use_image_rates: bool,
    pub(super) stream: bool,
    pub(super) service_tier: Option<String>,
    pub(super) fanout_multiplier: i64,
}
impl LimitRequest {
    /// Native Gemini operations use the normalized canonical model. Other
    /// operations prefer the body's model and use the canonical fallback only
    /// when that field is absent. Catalog/default-moderation operations have no
    /// model; required-model operations retain the existing `unknown` sentinel.
    pub(super) fn from_value(
        value: Option<&Value>,
        fallback_model: &str,
        capability: Option<ProviderCapability>,
        breakpoints: &[CacheBreakpoint],
        use_image_rates: bool,
    ) -> Self {
        let anthropic_input = value
            .filter(|_| capability == Some(ProviderCapability::AnthropicMessages))
            .map(|value| estimate_anthropic_input(value, breakpoints));
        let body_model = value
            .and_then(|v| v.get("model"))
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty());
        let native_gemini = matches!(
            capability,
            Some(
                ProviderCapability::GeminiGenerateContent
                    | ProviderCapability::GeminiCountTokens
                    | ProviderCapability::GeminiCachedContentsCreate
                    | ProviderCapability::GeminiEmbedContent
                    | ProviderCapability::GeminiPredict
            )
        );
        let model_optional = matches!(
            capability,
            Some(
                ProviderCapability::AnthropicModels
                    | ProviderCapability::OpenaiModels
                    | ProviderCapability::GeminiModels
                    | ProviderCapability::GeminiOpenaiModels
                    | ProviderCapability::OpenaiModerations
                    | ProviderCapability::OpenaiCodexAuxiliary
            )
        );
        let fallback = (!fallback_model.is_empty()).then_some(fallback_model);
        let model = if native_gemini {
            fallback.or(body_model)
        } else {
            body_model.or(fallback)
        }
        .unwrap_or(if model_optional { "" } else { "unknown" });
        let model = if capability
            .is_some_and(|capability| capability.provider() == cc_lb_domain::Provider::Gemini)
        {
            canonical_gemini_model_id(model)
        } else {
            model
        };
        let max_tokens = match capability {
            Some(ProviderCapability::OpenaiResponses) => value
                .and_then(|v| v.get("max_output_tokens"))
                .and_then(Value::as_i64),
            Some(ProviderCapability::AnthropicMessages) => value
                .and_then(|v| v.get("max_tokens"))
                .and_then(Value::as_i64),
            Some(ProviderCapability::AnthropicComplete) => value
                .and_then(|v| v.get("max_tokens_to_sample"))
                .and_then(Value::as_i64),
            Some(
                ProviderCapability::OpenaiChatCompletions
                | ProviderCapability::GeminiOpenaiChatCompletions,
            ) => value.and_then(|v| {
                ["max_completion_tokens", "max_tokens"]
                    .into_iter()
                    .filter_map(|key| v.get(key).and_then(Value::as_i64))
                    .max()
            }),
            Some(ProviderCapability::OpenaiCompletions) => value
                .and_then(|v| v.get("max_tokens"))
                .and_then(Value::as_i64),
            Some(ProviderCapability::GeminiGenerateContent) => value
                .and_then(|v| v.get("generationConfig"))
                .and_then(|config| config.get("maxOutputTokens"))
                .and_then(Value::as_i64),
            _ => Some(0),
        };
        let fanout_multiplier = if capability == Some(ProviderCapability::OpenaiChatCompletions) {
            value
                .and_then(|value| value.get("n"))
                .and_then(Value::as_u64)
                .and_then(|n| u32::try_from(n.min(8)).ok())
                .filter(|n| *n > 0)
                .unwrap_or(1)
        } else {
            1
        };
        let max_tokens =
            max_tokens.map(|tokens| tokens.saturating_mul(i64::from(fanout_multiplier)));
        let input_tokens_estimate = anthropic_input
            .map(|input| {
                let whole = input
                    .uncached_input_tokens
                    .saturating_add(input.cache_prefix_5m_tokens)
                    .saturating_add(input.cache_prefix_1h_tokens);
                i64::try_from(whole).unwrap_or(i64::MAX)
            })
            .or_else(|| value.and_then(|value| estimate_provider_input(value, model, capability)))
            .or_else(|| value.map(estimate_input_tokens))
            .unwrap_or(if model_optional {
                0
            } else {
                DEFAULT_MAX_INPUT_ESTIMATE
            });
        let local_cache_input = anthropic_input.filter(|_| !breakpoints.is_empty());
        Self {
            model: model.to_owned(),
            max_tokens,
            input_tokens_estimate,
            local_cache_input,
            estimated_cache_tokens: breakpoints
                .iter()
                .map(|bp| bp.prefix_bytes.as_token_estimate())
                .max()
                .unwrap_or(0),
            can_cross_cache_cliff: breakpoints.is_empty()
                && value.is_some_and(request_can_cross_cache_cliff),
            use_image_rates,
            stream: value
                .and_then(|v| v.get("stream"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            service_tier: value
                .and_then(|value| request_service_tier(value, capability))
                .map(str::to_owned),
            fanout_multiplier: i64::from(fanout_multiplier),
        }
    }
}

/// Estimates gross prompt tokens (uncached input + cached prefix) from the
/// already-parsed request JSON and cache breakpoints so routing pricing can
/// resolve prompt-length threshold bands without re-parsing the body.
pub(super) fn estimate_routing_gross_prompt_tokens(
    value: Option<&Value>,
    model: &str,
    capability: Option<ProviderCapability>,
    breakpoints: &[CacheBreakpoint],
) -> Option<u64> {
    let value = value?;
    if capability == Some(ProviderCapability::AnthropicMessages) {
        let input = estimate_anthropic_input(value, breakpoints);
        return Some(
            input
                .uncached_input_tokens
                .saturating_add(input.cache_prefix_5m_tokens)
                .saturating_add(input.cache_prefix_1h_tokens),
        );
    }
    let estimated = estimate_provider_input(value, model, capability)
        .unwrap_or_else(|| estimate_input_tokens(value))
        .max(0) as u64;
    Some(estimated)
}

pub(crate) const INPUT_ESTIMATE_MAX_NODES: usize = 16_384;
pub(crate) const INPUT_ESTIMATE_MAX_BYTES: usize = 64 * 1024 * 1024;

pub(super) fn estimate_input_tokens(value: &Value) -> i64 {
    let mut nodes = 0;
    let mut bytes = 0;
    for key in [
        "tools",
        "system",
        "messages",
        "instructions",
        "input",
        "systemInstruction",
        "contents",
    ] {
        if let Some(selected) = value.get(key) {
            collect_input_bytes(selected, &mut nodes, &mut bytes);
        }
    }
    if let Some(request) = value.get("generateContentRequest") {
        for key in ["tools", "contents", "systemInstruction"] {
            if let Some(selected) = request.get(key) {
                collect_input_bytes(selected, &mut nodes, &mut bytes);
            }
        }
    }
    i64::try_from(bytes / 4)
        .unwrap_or(i64::MAX)
        .max(DEFAULT_MAX_INPUT_ESTIMATE)
}

pub(super) fn request_can_cross_cache_cliff(value: &Value) -> bool {
    if [
        "previous_response_id",
        "conversation",
        "cachedContent",
        "container",
        "prompt_cache_key",
        "prompt_cache_options",
        "prompt_cache_retention",
    ]
    .iter()
    .any(|key| value.get(*key).is_some_and(|v| !v.is_null()))
    {
        return true;
    }
    if value
        .get("generateContentRequest")
        .and_then(|req| req.get("cachedContent"))
        .is_some_and(|v| !v.is_null())
    {
        return true;
    }
    let mut nodes = 0;
    value_contains_cache_marker(value, &mut nodes)
}

pub(super) fn value_contains_cache_marker(value: &Value, nodes: &mut usize) -> bool {
    if *nodes >= INPUT_ESTIMATE_MAX_NODES {
        return false;
    }
    *nodes += 1;
    match value {
        Value::Object(map) => {
            if map.contains_key("cache_control") || map.contains_key("prompt_cache_breakpoint") {
                return true;
            }
            map.values()
                .any(|child| value_contains_cache_marker(child, nodes))
        }
        Value::Array(items) => items
            .iter()
            .any(|child| value_contains_cache_marker(child, nodes)),
        _ => false,
    }
}

pub(super) fn collect_input_bytes(value: &Value, nodes: &mut usize, bytes: &mut usize) {
    if *nodes >= INPUT_ESTIMATE_MAX_NODES || *bytes >= INPUT_ESTIMATE_MAX_BYTES {
        return;
    }
    *nodes = (*nodes).saturating_add(1);
    match value {
        Value::String(text) => {
            *bytes = bytes
                .saturating_add(text.len())
                .min(INPUT_ESTIMATE_MAX_BYTES);
        }
        Value::Array(values) => {
            for value in values {
                collect_input_bytes(value, nodes, bytes);
                if *nodes >= INPUT_ESTIMATE_MAX_NODES || *bytes >= INPUT_ESTIMATE_MAX_BYTES {
                    break;
                }
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_input_bytes(value, nodes, bytes);
                if *nodes >= INPUT_ESTIMATE_MAX_NODES || *bytes >= INPUT_ESTIMATE_MAX_BYTES {
                    break;
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

pub(super) struct ActiveLimit {
    pub(super) subject: AuthLimitSubject,
    pub(super) request: LimitRequest,
    pub(super) fanout_multiplier: i64,
    pub(super) service_tier: Option<String>,
    pub(super) reservation: Option<LimitReservation>,
}
pub(super) fn attach_limit_headers_from_engine(
    headers: &mut HeaderMap,
    limit_engine: &LimitEngine,
    provider: Provider,
    key_id: &str,
    principal_id: &str,
) {
    for (name, value) in limit_engine.headers_for_provider(provider, key_id, principal_id) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            headers.insert(name, value);
        }
    }
}

pub(super) fn attach_limit_headers_from_record(
    headers: &mut HeaderMap,
    limit_engine: &LimitEngine,
    provider: Provider,
    key_id: &str,
    record: &StoredApiKeyRecord,
    default_limits: &[Limit],
) {
    for (name, value) in
        limit_engine.headers_for_provider_record(provider, key_id, record, default_limits)
    {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            headers.insert(name, value);
        }
    }
}

pub(super) fn limit_rejection_response(
    capability: ProviderCapability,
    reason: RejectReason,
    model: &str,
    principal_id: &str,
    retry_after_seconds: Option<u64>,
) -> Response<Body> {
    let should_retry = match &reason {
        RejectReason::Unsatisfiable { .. } => Some("false"),
        RejectReason::RequestsRateLimit
        | RejectReason::TokenRateLimit { .. }
        | RejectReason::CostRateLimit
        | RejectReason::ConcurrentRateLimit
        | RejectReason::ConcurrentLimitUnavailable => Some("true"),
        _ => None,
    };
    let (status, error_type, message, limit_kind) = match reason {
        RejectReason::ModelNotAllowed => (
            StatusCode::FORBIDDEN,
            "forbidden",
            format!("model {model} not allowed for principal {principal_id}"),
            None,
        ),
        RejectReason::Expired => (
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "api key expired".to_owned(),
            None,
        ),
        RejectReason::KeyRevoked => (
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "api key revoked".to_owned(),
            None,
        ),
        RejectReason::PrincipalDisabled => (
            StatusCode::FORBIDDEN,
            "forbidden",
            "principal disabled".to_owned(),
            None,
        ),
        RejectReason::PrincipalMissing => (
            StatusCode::FORBIDDEN,
            "forbidden",
            "principal missing".to_owned(),
            None,
        ),
        RejectReason::RequestsRateLimit => (
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "requests/window cap exceeded".to_owned(),
            Some("requests"),
        ),
        RejectReason::TokenRateLimit { kind } => (
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "token/window cap exceeded".to_owned(),
            Some(limit_kind_name(kind)),
        ),
        RejectReason::CostRateLimit => (
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "cost/window cap exceeded".to_owned(),
            Some("cost_usd"),
        ),
        RejectReason::ConcurrentRateLimit => (
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "concurrent request cap exceeded".to_owned(),
            Some("concurrent"),
        ),
        RejectReason::ConcurrentLimitUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            "concurrent request limit backend unavailable".to_owned(),
            Some("concurrent"),
        ),
        RejectReason::Unsatisfiable {
            kind,
            cap,
            requested,
        } => (
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            format!(
                "requested {} reservation {requested} cannot fit configured cap {cap}",
                limit_kind_name(kind)
            ),
            Some(limit_kind_name(kind)),
        ),
        RejectReason::CostUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            format!("cost limits unavailable for model {model}"),
            None,
        ),
        RejectReason::UnsupportedLimitWindow { window_secs } => (
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            format!("API key limit window {window_secs}s is unsupported"),
            None,
        ),
        RejectReason::InvalidOutputCap { requested } => (
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!("requested output token bound {requested} must be zero or greater"),
            None,
        ),
        RejectReason::OutputCapExceeded { cap, requested } => (
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!("max_tokens {requested} exceeds key output limit {cap}"),
            None,
        ),
        RejectReason::OutputCapRequired { cap } => (
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!("provider output limit is required for configured cap {cap}"),
            None,
        ),
    };

    let mut response = Response::new(Body::from(provider_error_body_with_retry_after(
        capability,
        status,
        error_type,
        &message,
        limit_kind,
        retry_after_seconds,
    )));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    if let Some(value) = should_retry {
        response
            .headers_mut()
            .insert("x-should-retry", HeaderValue::from_static(value));
    }
    if matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    ) && let Some(value) =
        retry_after_seconds.and_then(|sec| HeaderValue::from_str(&sec.to_string()).ok())
    {
        response.headers_mut().insert(RETRY_AFTER, value);
    }
    response
}

pub(super) fn limit_retry_after_secs(reason: RejectReason) -> Option<u64> {
    match reason {
        RejectReason::ConcurrentRateLimit | RejectReason::ConcurrentLimitUnavailable => Some(1),
        RejectReason::RequestsRateLimit
        | RejectReason::TokenRateLimit { .. }
        | RejectReason::CostRateLimit => Some(60),
        RejectReason::PrincipalMissing
        | RejectReason::PrincipalDisabled
        | RejectReason::KeyRevoked
        | RejectReason::Expired
        | RejectReason::ModelNotAllowed
        | RejectReason::CostUnavailable
        | RejectReason::UnsupportedLimitWindow { .. }
        | RejectReason::InvalidOutputCap { .. }
        | RejectReason::OutputCapExceeded { .. }
        | RejectReason::OutputCapRequired { .. }
        | RejectReason::Unsatisfiable { .. } => None,
    }
}

pub(super) fn limit_violation_name(reason: &RejectReason) -> Option<&'static str> {
    match reason {
        RejectReason::RequestsRateLimit => Some("Requests"),
        RejectReason::TokenRateLimit { kind } | RejectReason::Unsatisfiable { kind, .. } => {
            Some(audit_limit_kind_name(*kind))
        }
        RejectReason::CostRateLimit => Some("CostUsd"),
        RejectReason::ConcurrentRateLimit => Some("Concurrent"),
        RejectReason::ConcurrentLimitUnavailable => Some("Concurrent"),
        RejectReason::PrincipalMissing
        | RejectReason::PrincipalDisabled
        | RejectReason::KeyRevoked
        | RejectReason::Expired
        | RejectReason::ModelNotAllowed
        | RejectReason::CostUnavailable
        | RejectReason::UnsupportedLimitWindow { .. }
        | RejectReason::InvalidOutputCap { .. }
        | RejectReason::OutputCapExceeded { .. }
        | RejectReason::OutputCapRequired { .. } => None,
    }
}

pub(super) fn audit_limit_kind_name(kind: LimitKind) -> &'static str {
    match kind {
        LimitKind::InputTokens => "InputTokens",
        LimitKind::OutputTokens => "OutputTokens",
        LimitKind::TotalTokens => "TotalTokens",
        LimitKind::Requests => "Requests",
        LimitKind::CostUsd => "CostUsd",
        LimitKind::Concurrent => "Concurrent",
    }
}

pub(super) fn limit_kind_name(kind: LimitKind) -> &'static str {
    match kind {
        LimitKind::InputTokens => "input_tokens",
        LimitKind::OutputTokens => "output_tokens",
        LimitKind::TotalTokens => "total_tokens",
        LimitKind::Requests => "requests",
        LimitKind::CostUsd => "cost_usd",
        LimitKind::Concurrent => "concurrent",
    }
}

#[cfg(test)]
mod limit_request_estimate_tests {
    use super::{
        LimitRequest, ProviderCapability, RequestBodyView, StorageUpstreamKind,
        admission_fanout_multiplier,
    };
    use bytes::Bytes;
    use serde_json::json;
    #[test]
    fn admission_fanout_scales_oauth_only() {
        assert_eq!(
            admission_fanout_multiplier(
                StorageUpstreamKind::OpenaiOauth,
                Some(ProviderCapability::OpenaiChatCompletions),
                3,
            ),
            3
        );
        assert_eq!(
            admission_fanout_multiplier(
                StorageUpstreamKind::OpenaiApiKey,
                Some(ProviderCapability::OpenaiChatCompletions),
                3,
            ),
            1
        );
    }

    #[test]
    fn image_admission_rates_are_limited_to_image_capable_routes() {
        assert!(super::use_image_rates_for_admission(
            Some(ProviderCapability::OpenaiResponses),
            "/v1/responses",
        ));
        assert!(super::use_image_rates_for_admission(
            Some(ProviderCapability::OpenaiChatCompletions),
            "/v1/chat/completions",
        ));
        assert!(super::use_image_rates_for_admission(
            Some(ProviderCapability::OpenaiImages),
            "/v1/images/edits",
        ));
        assert!(super::use_image_rates_for_admission(
            Some(ProviderCapability::OpenaiImages),
            "/v1/images/variations",
        ));
        assert!(!super::use_image_rates_for_admission(
            Some(ProviderCapability::OpenaiImages),
            "/v1/images/generations",
        ));
        assert!(!super::use_image_rates_for_admission(
            Some(ProviderCapability::GeminiOpenaiImages),
            "/v1beta/openai/images/generations",
        ));
    }

    #[test]
    fn output_caps_are_capability_aware() {
        let cases = [
            (
                ProviderCapability::AnthropicMessages,
                json!({"max_tokens": 0, "max_output_tokens": 99}),
                Some(0),
            ),
            (
                ProviderCapability::AnthropicMessages,
                json!({"max_tokens": -1, "max_output_tokens": 99}),
                Some(-1),
            ),
            (
                ProviderCapability::AnthropicComplete,
                json!({"max_tokens": 99, "max_tokens_to_sample": 37}),
                Some(37),
            ),
            (
                ProviderCapability::OpenaiResponses,
                json!({"max_tokens": 0, "max_output_tokens": 99}),
                Some(99),
            ),
            (
                ProviderCapability::OpenaiResponses,
                json!({"max_output_tokens": -2, "max_tokens": 99}),
                Some(-2),
            ),
            (
                ProviderCapability::OpenaiChatCompletions,
                json!({"max_completion_tokens": -2, "max_tokens": -3}),
                Some(-2),
            ),
            (
                ProviderCapability::OpenaiChatCompletions,
                json!({"max_completion_tokens": 40, "max_tokens": 99}),
                Some(99),
            ),
            (
                ProviderCapability::GeminiGenerateContent,
                json!({"generationConfig":{"maxOutputTokens": 88}, "max_tokens": 99}),
                Some(88),
            ),
            (
                ProviderCapability::GeminiGenerateContent,
                json!({"generationConfig":{"maxOutputTokens": -3}, "max_tokens": 99}),
                Some(-3),
            ),
        ];
        for (capability, value, expected) in cases {
            let request =
                LimitRequest::from_value(Some(&value), "test-model", Some(capability), &[], false);
            assert_eq!(request.max_tokens, expected, "{capability:?}");
        }
        for capability in [
            ProviderCapability::OpenaiResponses,
            ProviderCapability::AnthropicMessages,
            ProviderCapability::AnthropicComplete,
            ProviderCapability::OpenaiChatCompletions,
            ProviderCapability::OpenaiCompletions,
            ProviderCapability::GeminiGenerateContent,
        ] {
            let request = LimitRequest::from_value(
                Some(&json!({"max_tokens": "wrong", "max_output_tokens": null})),
                "test-model",
                Some(capability),
                &[],
                false,
            );
            assert_eq!(request.max_tokens, None, "{capability:?}");
        }
    }

    #[test]
    fn zero_and_negative_output_caps_remain_visible_for_fail_closed_rejection() {
        for (capability, value, expected) in [
            (
                ProviderCapability::AnthropicMessages,
                json!({"max_tokens": 0, "max_completion_tokens": 99, "max_output_tokens": 99}),
                Some(0),
            ),
            (
                ProviderCapability::AnthropicMessages,
                json!({"max_tokens": -1, "max_completion_tokens": 99, "max_output_tokens": 99}),
                Some(-1),
            ),
            (
                ProviderCapability::OpenaiResponses,
                json!({"max_output_tokens": -2, "max_tokens": 99}),
                Some(-2),
            ),
        ] {
            let request =
                LimitRequest::from_value(Some(&value), "test-model", Some(capability), &[], false);
            assert_eq!(request.max_tokens, expected, "{capability:?}");
        }
    }

    #[test]
    fn non_generation_operations_reserve_zero_output_tokens() {
        for capability in [
            ProviderCapability::AnthropicCountTokens,
            ProviderCapability::GeminiCountTokens,
            ProviderCapability::OpenaiEmbeddings,
            ProviderCapability::OpenaiModerations,
            ProviderCapability::OpenaiModels,
            ProviderCapability::GeminiModels,
        ] {
            let request = LimitRequest::from_value(
                Some(&json!({"max_tokens": 99, "max_output_tokens": 88})),
                "test-model",
                Some(capability),
                &[],
                false,
            );
            assert_eq!(request.max_tokens, Some(0), "{capability:?}");
        }
    }

    #[test]
    fn openai_chat_completions_accept_max_completion_tokens() {
        let body = json!({"model":"gpt-test","max_completion_tokens":100});
        for capability in [
            ProviderCapability::OpenaiChatCompletions,
            ProviderCapability::GeminiOpenaiChatCompletions,
        ] {
            let request = LimitRequest::from_value(Some(&body), "", Some(capability), &[], false);
            assert_eq!(request.max_tokens, Some(100), "{capability:?}");
        }
    }

    #[test]
    fn malformed_json_keeps_existing_default_without_local_regions() {
        let view = RequestBodyView::new(&Bytes::from_static(b"{"));
        let request = view.limit_request(
            "claude-test",
            Some(ProviderCapability::AnthropicMessages),
            &[],
            false,
        );
        assert_eq!(request.input_tokens_estimate, 4_000);
        assert_eq!(request.max_tokens, None);
        assert!(request.local_cache_input.is_none());
    }

    #[test]
    fn optional_model_operations_do_not_invent_model_ids() {
        for capability in [
            ProviderCapability::AnthropicModels,
            ProviderCapability::OpenaiModels,
            ProviderCapability::GeminiModels,
            ProviderCapability::GeminiOpenaiModels,
            ProviderCapability::OpenaiModerations,
            ProviderCapability::OpenaiCodexAuxiliary,
        ] {
            let request = LimitRequest::from_value(None, "", Some(capability), &[], false);
            assert_eq!(request.model, "", "{capability:?}");
            assert_eq!(request.input_tokens_estimate, 0);
            assert_eq!(request.max_tokens, Some(0));
        }
        let body = json!({"model":"omni-moderation-latest"});
        let explicit = LimitRequest::from_value(
            Some(&body),
            "",
            Some(ProviderCapability::OpenaiModerations),
            &[],
            false,
        );
        assert_eq!(explicit.model, "omni-moderation-latest");
        let missing = LimitRequest::from_value(
            None,
            "",
            Some(ProviderCapability::OpenaiResponses),
            &[],
            false,
        );
        assert_eq!(missing.model, "unknown");
        let empty = json!({"model":""});
        let empty_required = LimitRequest::from_value(
            Some(&empty),
            "",
            Some(ProviderCapability::OpenaiResponses),
            &[],
            false,
        );
        assert_eq!(empty_required.model, "unknown");
    }

    #[test]
    fn native_gemini_model_is_authoritative_but_openai_compat_uses_body() {
        let body = json!({"model":"body-model"});
        for capability in [
            ProviderCapability::GeminiGenerateContent,
            ProviderCapability::GeminiCountTokens,
            ProviderCapability::GeminiEmbedContent,
            ProviderCapability::GeminiPredict,
        ] {
            let request =
                LimitRequest::from_value(Some(&body), "path-model", Some(capability), &[], false);
            assert_eq!(request.model, "path-model");
        }
        for capability in [
            ProviderCapability::OpenaiChatCompletions,
            ProviderCapability::GeminiOpenaiChatCompletions,
        ] {
            let request =
                LimitRequest::from_value(Some(&body), "fallback", Some(capability), &[], false);
            assert_eq!(request.model, "body-model");
        }
        let cached_body = json!({"model":"models/gemini-2.5-flash"});
        let cached = LimitRequest::from_value(
            Some(&cached_body),
            "",
            Some(ProviderCapability::GeminiCachedContentsCreate),
            &[],
            false,
        );
        assert_eq!(cached.model, "gemini-2.5-flash");
    }
    #[test]
    fn chat_fanout_reservation_scales_output_not_input_estimates() {
        let single_body = json!({
            "model": "gpt-test",
            "n": 1,
            "max_completion_tokens": 10,
        });
        let fanout_body = json!({
            "model": "gpt-test",
            "n": 3,
            "max_completion_tokens": 10,
        });
        let single = LimitRequest::from_value(
            Some(&single_body),
            "",
            Some(ProviderCapability::OpenaiChatCompletions),
            &[],
            false,
        );
        let fanout = LimitRequest::from_value(
            Some(&fanout_body),
            "",
            Some(ProviderCapability::OpenaiChatCompletions),
            &[],
            false,
        );
        assert_eq!(single.max_tokens, Some(10));
        assert_eq!(fanout.max_tokens, Some(30));
        assert_eq!(fanout.input_tokens_estimate, single.input_tokens_estimate);
    }
}
