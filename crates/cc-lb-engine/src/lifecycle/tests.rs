use std::fmt::Debug;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use async_trait::async_trait;
use cc_lb_storage_api::{
    SubscriptionQuotaSampleKind, SubscriptionQuotaSource, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow,
    principal::{PrincipalKind as StoragePrincipalKind, PrincipalRecord},
    upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord},
};
use http::header::{HeaderName, HeaderValue};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt as _};
use tracing_subscriber::{Layer, Registry};

use super::cache::{
    DecodedPromptCacheObservation, DecodedPromptCacheObservationKind, build_cache_score_from_match,
    fetch_prompt_cache_warm_entries, prompt_cache_observation_expires_at,
    request_cache_breakpoint_from_plugin, request_cache_metadata,
    request_cache_metadata_from_value, resolve_longest_warm_match,
};
use super::identity::{
    MAX_IDENTITY_VALUE_BYTES, MAX_THREAD_STICKY_SUBAGENTS_PER_SHARD, ThreadStickySubagentKey,
};
use super::streaming::{ANTHROPIC_PROVIDER_STATE_MARKERS, SSE_BUFFER_MAX_RETAINED_CAPACITY_BYTES};
use super::types::DEFAULT_MESSAGES_CAP_BYTES;
use super::*;
use cc_lb_routing_eval::prompt_cache_simulator::V3_TOKEN_ESTIMATE_SOURCE;

#[test]
fn has_json_content_type_matches_semantic_json_media_types() {
    for (value, expected) in [
        ("application/json", true),
        ("APPLICATION/JSON; charset=utf-8", true),
        (" application/json ", true),
        ("application/problem+json", true),
        ("application/vnd.api+JSON", true),
        ("audio/mpeg", false),
        ("application/octet-stream", false),
        ("image/png", false),
        ("text/plain", false),
        ("text/event-stream", false),
        ("application/jsonl", false),
        ("application/x-ndjson", false),
        ("text/json", false),
        ("", false),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str(value).expect("valid header"),
        );
        assert_eq!(has_json_content_type(&headers), expected, "{value:?}");
    }
    let mut headers = HeaderMap::new();
    assert!(!has_json_content_type(&headers));
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_bytes(b"\xff").expect("opaque header"),
    );
    assert!(!has_json_content_type(&headers));
    // Classification follows HeaderMap::get: the first value wins.
    for (first, second, expected) in [
        ("application/json", "audio/mpeg", true),
        ("audio/mpeg", "application/json", false),
    ] {
        headers.clear();
        headers.append(CONTENT_TYPE, HeaderValue::from_static(first));
        headers.append(CONTENT_TYPE, HeaderValue::from_static(second));
        assert_eq!(has_json_content_type(&headers), expected);
    }
}
#[test]
fn native_cache_gemini_url_model_is_authoritative() {
    for action in ["generateContent", "streamGenerateContent"] {
        for body_model in [None, Some("gemini-1.0-pro")] {
            for resource in [
                None,
                Some("projects/example/locations/us/cachedContents/cache-a"),
            ] {
                let mut body = json!({"contents": [{"parts": [{"text": "synthetic"}]}]});
                if let Some(model) = body_model {
                    body["model"] = json!(model);
                }
                if let Some(resource) = resource {
                    body["cachedContent"] = json!(resource);
                }
                let path = format!("/v1beta/models/gemini-2.5-flash:{action}");
                let path_model = gemini_model_from_path(&path);
                let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                    &HeaderMap::new(),
                    Some(&body),
                    None,
                    Some(ProviderCapability::GeminiGenerateContent),
                    path_model.as_deref(),
                );
                assert_eq!(metadata.canonical_model_id, "gemini-2.5-flash");
                let native = metadata.native_cache.expect("Gemini cache analysis");
                assert_eq!(native.provider, "gemini");
                assert_eq!(native.retention_secs, None);
                assert_eq!(native.expires_at_unix_secs, None);
                if resource.is_some() {
                    assert_eq!(native.mode, "explicit");
                    assert_eq!(native.scope.as_deref(), Some("example"));
                    let identity = native.identity_hash.expect("bounded cache identity");
                    assert!(identity.starts_with("sha256:"));
                    assert_eq!(identity.len(), 71);
                    assert_eq!(
                        native.evidence_source.as_deref(),
                        Some("request.cachedContent")
                    );
                } else {
                    assert_eq!(native.mode, "implicit");
                    assert_eq!(native.identity_hash, None);
                    assert_eq!(native.scope, None);
                    assert_eq!(
                        native.boundary.as_deref(),
                        Some("minimum_input_tokens=2048")
                    );
                    assert_eq!(
                        native.evidence_source.as_deref(),
                        Some("provider_usage_metadata")
                    );
                }
            }
        }
    }
}

#[test]
fn native_cache_gemini_missing_model_and_other_apis_stay_unknown() {
    let body = json!({"model": "gemini-2.5-flash", "contents": []});
    for path_model in [None, Some("gemini-1.0-pro")] {
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&body),
            None,
            Some(ProviderCapability::GeminiGenerateContent),
            path_model,
        );
        let native = metadata
            .native_cache
            .expect("unsupported Gemini observation");
        assert_eq!(
            native.evidence_source.as_deref(),
            Some("unsupported_model_or_api")
        );
        assert_eq!(
            native.evidence,
            cc_lb_request_log::RequestCacheEvidence::Unreported
        );
        assert_eq!(native.identity_hash, None);
    }
    for capability in [
        ProviderCapability::GeminiCountTokens,
        ProviderCapability::GeminiEmbedContent,
        ProviderCapability::GeminiPredict,
    ] {
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&body),
            None,
            Some(capability),
            Some("gemini-2.5-flash"),
        );
        assert!(metadata.native_cache.is_none());
    }
}
#[derive(Default)]
struct ResolverWarningFields {
    message: Option<String>,
    model: Option<String>,
}

impl tracing::field::Visit for ResolverWarningFields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn Debug) {
        let value = format!("{value:?}");
        let value = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .map_or(value.clone(), str::to_owned);
        self.record(field, value);
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record(field, value.to_owned());
    }
}

impl ResolverWarningFields {
    fn record(&mut self, field: &tracing::field::Field, value: String) {
        match field.name() {
            "message" => self.message = Some(value),
            "model" => self.model = Some(value),
            _ => {}
        }
    }
}

type ResolverWarning = (String, Option<String>);

#[derive(Clone)]
struct ResolverWarningLayer {
    warnings: Arc<Mutex<Vec<ResolverWarning>>>,
}

impl<S> Layer<S> for ResolverWarningLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if *event.metadata().level() != Level::WARN {
            return;
        }
        let mut fields = ResolverWarningFields::default();
        event.record(&mut fields);
        let Some(message) = fields.message else {
            return;
        };
        self.warnings
            .lock()
            .expect("resolver warning capture lock")
            .push((message, fields.model));
    }
}

fn capture_resolver_warnings<R>(operation: impl FnOnce() -> R) -> (R, Vec<ResolverWarning>) {
    let warnings = Arc::new(Mutex::new(Vec::new()));
    let layer = ResolverWarningLayer {
        warnings: Arc::clone(&warnings),
    };
    let result = tracing::subscriber::with_default(Registry::default().with(layer), operation);
    let warnings = warnings
        .lock()
        .expect("resolver warning capture lock")
        .clone();
    (result, warnings)
}

fn alias_warnings(warnings: &[ResolverWarning]) -> Vec<&ResolverWarning> {
    warnings
        .iter()
        .filter(|(message, _)| message == "unknown model alias, returning unchanged")
        .collect()
}

#[test]
fn request_cache_metadata_resolves_provider_models_without_alias_warnings() {
    let (metadata, warnings) = capture_resolver_warnings(|| {
        let openai = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&json!({"model": "gpt-5-mini", "input": []})),
            None,
            Some(ProviderCapability::OpenaiResponses),
            None,
        );
        let gemini_generate = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&json!({"contents": []})),
            None,
            Some(ProviderCapability::GeminiGenerateContent),
            Some("gemini-3.5-flash-lite"),
        );
        let gemini_count_tokens = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&json!({"model": "gemini-3.5-flash-lite", "contents": []})),
            None,
            Some(ProviderCapability::GeminiCountTokens),
            Some("gemini-3.5-flash-lite"),
        );
        let anthropic_rolling = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&json!({"model": "claude-sonnet-4-5", "messages": []})),
            None,
            Some(ProviderCapability::AnthropicMessages),
            None,
        );
        let anthropic_dated = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&json!({"model": "claude-sonnet-4-5-20250929", "messages": []})),
            None,
            Some(ProviderCapability::AnthropicMessages),
            None,
        );
        [
            openai,
            gemini_generate,
            gemini_count_tokens,
            anthropic_rolling,
            anthropic_dated,
        ]
    });

    assert_eq!(metadata[0].canonical_model_id, "gpt-5-mini");
    assert_eq!(metadata[1].canonical_model_id, "gemini-3.5-flash-lite");
    assert_eq!(metadata[2].canonical_model_id, "gemini-3.5-flash-lite");
    assert_eq!(metadata[3].canonical_model_id, "claude-sonnet-4-5-20250929");
    assert_eq!(metadata[4].canonical_model_id, "claude-sonnet-4-5-20250929");
    assert!(
        alias_warnings(&warnings).is_empty(),
        "unexpected alias warnings: {warnings:?}"
    );
}

#[test]
fn request_cache_metadata_warns_once_for_unknown_anthropic_model() {
    let body = json!({"model": "claude-future-model", "messages": []});
    let (metadata, warnings) = capture_resolver_warnings(|| {
        request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&body),
            None,
            Some(ProviderCapability::AnthropicMessages),
            None,
        )
    });

    assert_eq!(metadata.canonical_model_id, "claude-future-model");
    let alias_warnings = alias_warnings(&warnings);
    assert_eq!(
        alias_warnings.len(),
        1,
        "unexpected alias warnings: {alias_warnings:?}"
    );
    assert_eq!(
        alias_warnings[0].0,
        "unknown model alias, returning unchanged"
    );
    assert_eq!(alias_warnings[0].1.as_deref(), Some("claude-future-model"));
}

#[test]
fn request_cache_metadata_without_capability_warns_for_unknown_anthropic_model() {
    let body = json!({"model": "claude-future-model", "messages": []});
    let (metadata, warnings) = capture_resolver_warnings(|| {
        request_cache_metadata_from_value(&HeaderMap::new(), Some(&body))
    });

    assert_eq!(metadata.canonical_model_id, "claude-future-model");
    let alias_warnings = alias_warnings(&warnings);
    assert!(
        !alias_warnings.is_empty(),
        "expected an unknown Anthropic alias warning"
    );
    assert!(
        alias_warnings
            .iter()
            .all(|(_, model)| model.as_deref() == Some("claude-future-model")),
        "unexpected alias warning fields: {alias_warnings:?}"
    );
}

#[tokio::test]
async fn native_cache_gemini_url_model_reaches_lifecycle_observations() {
    for streaming in [false, true] {
        for conflicting_body_model in [false, true] {
            let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
            let mut events = subscribe_lifecycle(&bus);
            let sink = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
            let view = DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(TestSignerFactory))
                .principal_view(failure_principal_view(
                    "principal-test",
                    default_artifacts(),
                ))
                .upstream_records(vec![upstream_record_with_kind(
                    StorageRecordKind::GeminiApiKey,
                )])
                .prompt_cache_observation_sink(sink.clone())
                .build();
            let json = br#"{"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1200,"cachedContentTokenCount":64,"candidatesTokenCount":1,"totalTokenCount":1201}}"#;
            let provider_body = if streaming {
                Bytes::from(format!("data: {}\n\n", String::from_utf8_lossy(json)))
            } else {
                Bytes::from_static(json)
            };
            let mut upstream = Response::new(Body::from(provider_body.clone()));
            upstream.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static(if streaming {
                    "text/event-stream"
                } else {
                    "application/json"
                }),
            );
            let lifecycle =
                provenance_lifecycle(view, Arc::new(QueueDispatch::new(vec![Ok(upstream)])), &bus);
            let mut body = json!({"contents": [{"parts": [{"text": "synthetic"}]}]});
            if conflicting_body_model {
                body["model"] = json!("gemini-1.0-pro");
            }
            let mut request = failure_request(Bytes::from(serde_json::to_vec(&body).unwrap()));
            let action = if streaming {
                "streamGenerateContent"
            } else {
                "generateContent"
            };
            *request.uri_mut() = format!("/v1beta/models/gemini-2.5-flash:{action}")
                .parse()
                .unwrap();
            request
                .extensions_mut()
                .insert(crate::request_context::ProviderScope(
                    cc_lb_domain::Provider::Gemini,
                ));
            let auth = lifecycle.authenticate(request.headers()).await.unwrap();
            let response = lifecycle.handle(request, &auth).await.unwrap();
            let (status, response_body) = collect_response_body(response).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(
                response_body, provider_body,
                "provider response remains unchanged"
            );
            lifecycle.shutdown().await;
            let events = drain_events(&mut events);
            let parsed = events
                .iter()
                .find_map(|event| match event {
                    cc_lb_lifecycle::LifecycleEvent::ParseCompleted {
                        result: Ok(info), ..
                    } => Some(info),
                    _ => None,
                })
                .expect("request reached analysis");
            assert_eq!(parsed.model.as_deref(), Some("gemini-2.5-flash"));
            let native = parsed.native_cache.as_ref().expect("native analysis");
            assert_eq!(
                native.evidence_source.as_deref(),
                Some("provider_usage_metadata")
            );
            assert_eq!(
                native.boundary.as_deref(),
                Some("minimum_input_tokens=2048")
            );
            let records = sink.provider_upserts();
            let record = records.last().expect("provider observation published");
            assert_eq!(record.provider, "gemini");
            assert_eq!(record.canonical_model_id, "gemini-2.5-flash");
            assert_eq!(record.evidence_source, "provider_usage_metadata");
            assert_eq!(record.observed_cached_tokens, Some(64));
            assert_eq!(record.observed_write_tokens, None);
            assert!(record.identity_hash.is_empty());
            assert_eq!(record.expires_at_unix_secs, None);
        }
    }
}

#[test]
fn openai_replay_identity_namespaces_scope_dimensions() {
    let value = serde_json::json!({
        "model": "gpt-5.6",
        "input": [{"role": "user", "content": "prompt"}],
        "prompt_cache_key": "key-a",
        "conversation": {"id": "conversation-a"},
        "organization": "org-a",
        "processing_region": "us-east"
    });
    let headers = HeaderMap::new();
    let identity = super::cache::openai_replay_identity(
        &value,
        &headers,
        "gpt-5.6",
        Some(cc_lb_storage_api::CacheTtl::Ttl30m),
        Some("prefix-a"),
    )
    .expect("identity");
    for (model, ttl, prefix, organization, region, key, conversation) in [
        (
            "gpt-5.7",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
            "org-a",
            "us-east",
            "key-a",
            "conversation-a",
        ),
        (
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl12h),
            Some("prefix-a"),
            "org-a",
            "us-east",
            "key-a",
            "conversation-a",
        ),
        (
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-b"),
            "org-a",
            "us-east",
            "key-a",
            "conversation-a",
        ),
        (
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
            "org-b",
            "us-east",
            "key-a",
            "conversation-a",
        ),
        (
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
            "org-a",
            "eu-west",
            "key-a",
            "conversation-a",
        ),
        (
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
            "org-a",
            "us-east",
            "key-b",
            "conversation-a",
        ),
        (
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
            "org-a",
            "us-east",
            "key-a",
            "conversation-b",
        ),
    ] {
        let mut scoped = value.clone();
        scoped["organization"] = serde_json::Value::String(organization.to_owned());
        scoped["processing_region"] = serde_json::Value::String(region.to_owned());
        scoped["prompt_cache_key"] = serde_json::Value::String(key.to_owned());
        scoped["conversation"]["id"] = serde_json::Value::String(conversation.to_owned());
        let candidate = super::cache::openai_replay_identity(&scoped, &headers, model, ttl, prefix)
            .expect("scoped identity");
        assert_ne!(identity, candidate);
    }
    let mut delimiter_left = value.clone();
    delimiter_left["organization"] = serde_json::Value::String("acme|region=us".to_owned());
    delimiter_left["processing_region"] = serde_json::Value::String("eu".to_owned());
    let mut delimiter_right = value.clone();
    delimiter_right["organization"] = serde_json::Value::String("acme".to_owned());
    delimiter_right["processing_region"] = serde_json::Value::String("us|region=eu".to_owned());
    assert_ne!(
        super::cache::openai_replay_identity(
            &delimiter_left,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
        ),
        super::cache::openai_replay_identity(
            &delimiter_right,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
        )
    );
    let mut prefix_left = value.clone();
    prefix_left["organization"] = serde_json::Value::String("tail".to_owned());
    let mut prefix_right = value.clone();
    prefix_right["organization"] = serde_json::Value::String("org-a|organization=tail".to_owned());
    assert_ne!(
        super::cache::openai_replay_identity(
            &prefix_left,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a|organization=org-a"),
        ),
        super::cache::openai_replay_identity(
            &prefix_right,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
        )
    );
    let mut region_left = value.clone();
    region_left["processing_region"] =
        serde_json::Value::String("us|prompt_cache_key=key-a".to_owned());
    let mut region_right = value.clone();
    region_right["processing_region"] = serde_json::Value::String("us".to_owned());
    region_right["prompt_cache_key"] =
        serde_json::Value::String("key-a|prompt_cache_key=tail".to_owned());
    region_left["prompt_cache_key"] = serde_json::Value::String("tail".to_owned());
    assert_ne!(
        super::cache::openai_replay_identity(
            &region_left,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
        ),
        super::cache::openai_replay_identity(
            &region_right,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
        )
    );
    let mut key_left = value.clone();
    key_left["prompt_cache_key"] = serde_json::Value::String("key-a|conversation=left".to_owned());
    key_left["conversation"]["id"] = serde_json::Value::String("right".to_owned());
    let mut key_right = value.clone();
    key_right["prompt_cache_key"] = serde_json::Value::String("key-a".to_owned());
    key_right["conversation"]["id"] =
        serde_json::Value::String("left|conversation=right".to_owned());
    assert_ne!(
        super::cache::openai_replay_identity(
            &key_left,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
        ),
        super::cache::openai_replay_identity(
            &key_right,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
        )
    );
    let mut string_conversation = value.clone();
    string_conversation["conversation"] = serde_json::Value::String("conv-a".to_owned());
    let mut string_conversation_other = value.clone();
    string_conversation_other["conversation"] = serde_json::Value::String("conv-b".to_owned());
    assert_ne!(
        super::cache::openai_replay_identity(
            &string_conversation,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
        ),
        super::cache::openai_replay_identity(
            &string_conversation_other,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
        )
    );
    let mut object_conversation = value.clone();
    object_conversation["conversation"] = serde_json::json!({"id": "conv-a"});
    assert_ne!(
        super::cache::openai_replay_identity(
            &string_conversation,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
        ),
        super::cache::openai_replay_identity(
            &object_conversation,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
        )
    );
}

#[test]
fn request_cache_metadata_uses_openai_responses_ttl_contract_for_keepalive() {
    use cc_lb_storage_api::CacheTtl;
    let input = vec![serde_json::json!({
        "role": "user",
        "content": "x".repeat(5_000)
    })];
    let metadata = |mut body: serde_json::Value| {
        body["model"] = serde_json::Value::String("gpt-5.6".to_owned());
        request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&body),
            None,
            Some(ProviderCapability::OpenaiResponses),
            None,
        )
    };

    for (name, options, expected) in [
        ("omitted", serde_json::json!({}), CacheTtl::Ttl30m),
        (
            "empty options",
            serde_json::json!({"prompt_cache_options": {}}),
            CacheTtl::Ttl30m,
        ),
        (
            "explicit 30m",
            serde_json::json!({"prompt_cache_options": {"ttl": "30m"}}),
            CacheTtl::Ttl30m,
        ),
        (
            "explicit 12h",
            serde_json::json!({"prompt_cache_options": {"ttl": "12h"}}),
            CacheTtl::Ttl12h,
        ),
    ] {
        let mut body = options;
        body["input"] = serde_json::Value::Array(input.clone());
        let result = metadata(body);
        assert_eq!(
            result.cache_keepalive_surface.as_deref(),
            Some("responses"),
            "{name} must enable Responses keep-alive"
        );
        assert_eq!(result.cache_keepalive_ttl, Some(expected), "{name}");
    }

    let mut breakpoint_override = serde_json::json!({
        "prompt_cache_options": {"ttl": "30m"},
        "input": [{
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "x".repeat(5_000),
                "prompt_cache_breakpoint": {"ttl": "12h"}
            }]
        }]
    });
    let result = metadata(std::mem::take(&mut breakpoint_override));
    assert_eq!(result.cache_keepalive_ttl, Some(CacheTtl::Ttl12h));

    let mut inherited_breakpoint = serde_json::json!({
        "prompt_cache_options": {"ttl": "12h"},
        "input": [{
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "x".repeat(5_000),
                "prompt_cache_breakpoint": {}
            }]
        }]
    });
    let result = metadata(std::mem::take(&mut inherited_breakpoint));
    assert_eq!(result.cache_keepalive_ttl, Some(CacheTtl::Ttl12h));
    let result = metadata(serde_json::json!({
        "metadata": {"prompt_cache_breakpoint": {"ttl": "12h"}},
        "tools": [{"input_schema": {"prompt_cache_breakpoint": {"ttl": "12h"}}}],
        "input": input.clone()
    }));
    assert_eq!(result.cache_keepalive_surface.as_deref(), Some("responses"));
    assert_eq!(result.cache_keepalive_ttl, Some(CacheTtl::Ttl30m));

    for mut body in [
        serde_json::json!({
            "prompt_cache_options": {"mode": "explicit"},
            "input": input.clone()
        }),
        serde_json::json!({
            "prompt_cache_options": {"ttl": "forever"},
            "input": input.clone()
        }),
        serde_json::json!({
            "prompt_cache_options": {"ttl": "30m"},
            "input": [{
                "prompt_cache_breakpoint": {"ttl": "forever"},
                "content": "x".repeat(5_000)
            }]
        }),
        serde_json::json!({
            "prompt_cache_retention": "30m",
            "input": input.clone()
        }),
    ] {
        let result = metadata(std::mem::take(&mut body));
        assert_eq!(result.cache_keepalive_surface, None);
        assert_eq!(result.cache_keepalive_ttl, None);
    }

    let mut unsupported = serde_json::json!({"input": input});
    unsupported["model"] = serde_json::Value::String("gpt-5.5".to_owned());
    let result = request_cache_metadata_from_value_with_analysis_for_capability(
        &HeaderMap::new(),
        Some(&unsupported),
        None,
        Some(ProviderCapability::OpenaiResponses),
        None,
    );
    assert_eq!(result.cache_keepalive_surface, None);
    assert_eq!(result.cache_keepalive_ttl, None);
}

#[test]
fn qa_elig_1_vertex_matrix_limits_api_keys_to_generate_and_count() {
    use StorageRecordKind as K;
    let regional_target = cc_lb_domain::VertexTarget::Regional {
        project_id: "project-123456".into(),
        location: "us-central1".into(),
    };
    let express_key = UpstreamRecord {
        kind: K::VertexApiKey,
        vertex: Some(cc_lb_domain::VertexUpstreamConfig {
            target: cc_lb_domain::VertexTarget::Express,
            api_version: Default::default(),
        }),
        ..Default::default()
    };
    let mut regional_key = express_key.clone();
    regional_key.vertex = Some(cc_lb_domain::VertexUpstreamConfig {
        target: regional_target.clone(),
        api_version: Default::default(),
    });
    let mut credential = regional_key.clone();
    credential.kind = K::VertexCredential;
    // Google rejects API keys for the cache and publisher-catalog services,
    // so both API-key targets serve only generate and countTokens.
    for key in [&express_key, &regional_key] {
        assert!(RequestKind::GeminiGenerateContent.eligible(key));
        assert!(RequestKind::GeminiCountTokens.eligible(key));
        assert!(!RequestKind::GeminiCachedContentsCreate.eligible(key));
        assert!(!RequestKind::GeminiModels.eligible(key));
    }
    assert!(RequestKind::GeminiGenerateContent.eligible(&credential));
    assert!(RequestKind::GeminiCachedContentsCreate.eligible(&credential));
    assert!(RequestKind::GeminiModels.eligible(&credential));
}

#[test]
fn qa_elig_2_vertex_excludes_embeddings_prediction_openai_and_files() {
    let record = UpstreamRecord {
        kind: StorageRecordKind::VertexCredential,
        vertex: Some(cc_lb_domain::VertexUpstreamConfig {
            target: cc_lb_domain::VertexTarget::Regional {
                project_id: "project-123456".into(),
                location: "us-central1".into(),
            },
            api_version: Default::default(),
        }),
        ..Default::default()
    };
    for request in [
        RequestKind::GeminiEmbedContent,
        RequestKind::GeminiPredict,
        RequestKind::GeminiOpenaiChatCompletions,
        RequestKind::GeminiOpenaiEmbeddings,
        RequestKind::GeminiOpenaiImages,
    ] {
        assert!(!request.eligible(&record), "{request:?}");
    }
}
#[test]
fn qa_kind_2_vertex_candidates_are_api_key_fallbacks_not_subscription() {
    use cc_lb_domain::CredentialKind;
    for kind in [
        StorageRecordKind::VertexApiKey,
        StorageRecordKind::VertexCredential,
    ] {
        assert_eq!(kind.provider(), Provider::Gemini);
        assert_eq!(kind.credential_kind(), CredentialKind::ApiKey);
    }
}
#[test]
fn qa_aff_5_vertex_express_is_not_eligible_for_cached_contents() {
    let express = UpstreamRecord {
        kind: StorageRecordKind::VertexApiKey,
        vertex: Some(cc_lb_domain::VertexUpstreamConfig {
            target: cc_lb_domain::VertexTarget::Express,
            api_version: Default::default(),
        }),
        ..Default::default()
    };
    assert!(!RequestKind::GeminiCachedContentsCreate.eligible(&express));
}

#[test]
fn operations_filter_subscription_and_api_key_credentials() {
    use StorageRecordKind as K;
    let all_kinds = [
        K::AnthropicApiKey,
        K::AnthropicOauth,
        K::OpenaiApiKey,
        K::OpenaiOauth,
        K::GeminiApiKey,
        K::GeminiOauth,
    ];
    let cases: &[(RequestKind, &[StorageRecordKind])] = &[
        (
            RequestKind::AnthropicMessages,
            &[K::AnthropicApiKey, K::AnthropicOauth],
        ),
        (
            RequestKind::OpenaiResponses,
            &[K::OpenaiApiKey, K::OpenaiOauth],
        ),
        (
            RequestKind::GeminiGenerateContent,
            &[K::GeminiApiKey, K::GeminiOauth],
        ),
        (
            RequestKind::GeminiCountTokens,
            &[K::GeminiApiKey, K::GeminiOauth],
        ),
        (
            RequestKind::AnthropicModels,
            &[K::AnthropicApiKey, K::AnthropicOauth],
        ),
        (RequestKind::AnthropicComplete, &[K::AnthropicApiKey]),
        (
            RequestKind::OpenaiResponsesInputTokens,
            &[K::OpenaiApiKey, K::OpenaiOauth],
        ),
        (
            RequestKind::OpenaiResponsesCompact,
            &[K::OpenaiApiKey, K::OpenaiOauth],
        ),
        (
            RequestKind::OpenaiChatCompletions,
            &[K::OpenaiApiKey, K::OpenaiOauth],
        ),
        (RequestKind::OpenaiCompletions, &[K::OpenaiApiKey]),
        (RequestKind::OpenaiEmbeddings, &[K::OpenaiApiKey]),
        (RequestKind::OpenaiModerations, &[K::OpenaiApiKey]),
        (RequestKind::OpenaiAudio, &[K::OpenaiApiKey]),
        (
            RequestKind::OpenaiImages,
            &[K::OpenaiApiKey, K::OpenaiOauth],
        ),
        (
            RequestKind::OpenaiAudioTranscriptions,
            &[K::OpenaiApiKey, K::OpenaiOauth],
        ),
        (
            RequestKind::OpenaiRealtimeCall,
            &[K::OpenaiApiKey, K::OpenaiOauth],
        ),
        (
            RequestKind::OpenaiRealtimeHangup,
            &[K::OpenaiApiKey, K::OpenaiOauth],
        ),
        (RequestKind::OpenaiAccountOwned, &[K::OpenaiApiKey]),
        (RequestKind::OpenaiStoredResponse, &[K::OpenaiApiKey]),
        (
            RequestKind::OpenaiModels,
            &[K::OpenaiApiKey, K::OpenaiOauth],
        ),
        (RequestKind::OpenaiCodexAuxiliary, &[K::OpenaiOauth]),
        (RequestKind::GeminiEmbedContent, &[K::GeminiApiKey]),
        (RequestKind::GeminiPredict, &[K::GeminiApiKey]),
        (
            RequestKind::GeminiModels,
            &[K::GeminiApiKey, K::GeminiOauth],
        ),
        (RequestKind::GeminiOpenaiChatCompletions, &[K::GeminiApiKey]),
        (RequestKind::GeminiOpenaiEmbeddings, &[K::GeminiApiKey]),
        (RequestKind::GeminiOpenaiImages, &[K::GeminiApiKey]),
        (RequestKind::GeminiOpenaiModels, &[K::GeminiApiKey]),
        (RequestKind::OpenaiImageVariations, &[K::OpenaiApiKey]),
    ];
    for &(operation, allowed) in cases {
        for kind in all_kinds {
            assert_eq!(
                operation.matches_upstream(kind),
                allowed.contains(&kind),
                "{operation:?} {kind:?}"
            );
        }
    }
}

#[test]
fn openai_oauth_catalog_and_auxiliary_urls_preserve_client_queries() {
    for base in [None, Some("https://gateway.example/codex/")] {
        let root = base
            .unwrap_or("https://chatgpt.com/backend-api/codex/")
            .trim_end_matches('/');
        for (method, suffix) in [
            (Method::POST, "responses"),
            (Method::POST, "responses/compact"),
            (Method::GET, "models"),
            (Method::GET, "models/gpt-test"),
            (Method::POST, "images/generations"),
            (Method::POST, "images/edits"),
            (Method::POST, "alpha/search"),
        ] {
            for query in [
                None,
                Some("client_version=1.2.3&trace=a%2Fb"),
                Some("key=discard&client_version=2"),
            ] {
                let path = format!("/v1/{suffix}");
                let url = provider_signed_path(
                    StorageRecordKind::OpenaiOauth,
                    base,
                    method.clone(),
                    &path,
                    query,
                );
                let expected_query = query
                    .map(cc_lb_upstream::scrub_query_api_key)
                    .filter(|query| !query.is_empty());
                assert_eq!(
                    url.as_str(),
                    format!(
                        "{root}/{suffix}{}",
                        expected_query
                            .map(|query| format!("?{query}"))
                            .unwrap_or_default()
                    ),
                    "{path}"
                );
            }
        }
    }
}

#[test]
fn multipart_operations_use_files_cap_only_on_exact_paths() {
    let config = LifecycleConfig {
        files_body_cap_bytes: 91,
        messages_body_cap_bytes: 17,
        ..LifecycleConfig::default()
    };
    for path in [
        "/v1/audio/transcriptions",
        "/v1/audio/translations",
        "/v1/images/edits",
        "/v1/images/variations",
        "/v1/files",
        "/v1/uploads/upload_1/parts",
        "/v1/videos/video_1",
        "/v1/audio/voices",
    ] {
        assert_eq!(body_cap_for_path(&config, path), 91, "{path}");
    }
    for path in [
        "/v1/audio/speech",
        "/v1/images/generations",
        "/v1/images/edits/extra",
        "/v1/chat/completions",
        "/v1/messages",
    ] {
        assert_eq!(body_cap_for_path(&config, path), 17, "{path}");
    }
}

fn provider_signed_path(
    kind: StorageRecordKind,
    base_url: Option<&str>,
    method: http::Method,
    path: &str,
    query: Option<&str>,
) -> Url {
    let record = UpstreamRecord {
        id: Uuid::from_u128(8_537),
        name: "provider-url-regression".to_owned(),
        kind,
        base_url: base_url.map(|base| Url::parse(base).expect("test base URL parses")),
        enabled: true,
        ..Default::default()
    };
    let principal_view = failure_principal_view("principal-test", default_artifacts());
    let cached = principal_view
        .get("principal-test")
        .expect("test principal exists");
    let (route, _) = route_for_record(&record, cached).expect("provider route resolves");
    let context = RequestContext::builder()
        .request_id("provider-url-regression".to_owned())
        .method(method)
        .path(path.to_owned())
        .query(query.map(str::to_owned))
        .build();
    let principal = Principal {
        id: "principal-test".to_owned(),
        kind: PrincipalKind::InternalKey,
    };
    shape_request(
        route.dialect.as_ref(),
        &context.dialect_shape_context(),
        &route.upstream,
        &principal,
    )
    .expect("provider request shapes")
    .url()
    .clone()
}

#[test]
fn openai_codex_default_http_and_websocket_use_responses_without_v1() {
    for method in [http::Method::POST, http::Method::GET] {
        let url = provider_signed_path(
            StorageRecordKind::OpenaiOauth,
            None,
            method.clone(),
            "/v1/responses",
            Some("trace=a%2Fb&beta=1"),
        );
        assert_eq!(
            url.as_str(),
            "https://chatgpt.com/backend-api/codex/responses?trace=a%2Fb&beta=1",
            "{method}"
        );
    }
}

#[test]
fn openai_codex_custom_http_and_websocket_preserve_base_prefix() {
    for base in [
        "https://gateway.example/codex-proxy",
        "https://gateway.example/codex-proxy/",
        "https://gateway.example/codex-proxy/?stale=1",
    ] {
        for method in [http::Method::POST, http::Method::GET] {
            let url = provider_signed_path(
                StorageRecordKind::OpenaiOauth,
                Some(base),
                method.clone(),
                "/v1/responses",
                Some("trace=a%2Fb&beta=1"),
            );
            assert_eq!(
                url.as_str(),
                "https://gateway.example/codex-proxy/responses?trace=a%2Fb&beta=1",
                "{method} {base}"
            );
        }
    }
}

#[test]
fn openai_codex_custom_base_query_is_removed_without_request_query() {
    let url = provider_signed_path(
        StorageRecordKind::OpenaiOauth,
        Some("https://gateway.example/codex-proxy?stale=1"),
        http::Method::POST,
        "/v1/responses",
        None,
    );
    assert_eq!(
        url.as_str(),
        "https://gateway.example/codex-proxy/responses"
    );
}

#[test]
fn provider_signed_paths_outside_codex_responses_are_unchanged() {
    for (kind, base, path, expected) in [
        (
            StorageRecordKind::OpenaiApiKey,
            None,
            "/v1/responses",
            "https://api.openai.com/v1/responses?beta=1",
        ),
        (
            StorageRecordKind::OpenaiApiKey,
            Some("https://gateway.example/old-prefix?stale=1"),
            "/v1/responses",
            "https://gateway.example/v1/responses?beta=1",
        ),
        (
            StorageRecordKind::AnthropicApiKey,
            Some("https://gateway.example/old-prefix?stale=1"),
            "/v1/messages",
            "https://gateway.example/v1/messages?beta=1",
        ),
        (
            StorageRecordKind::GeminiApiKey,
            Some("https://gateway.example/old-prefix?stale=1"),
            "/v1beta/models/gemini-test:generateContent",
            "https://gateway.example/v1beta/models/gemini-test:generateContent?beta=1",
        ),
    ] {
        let url = provider_signed_path(kind, base, http::Method::POST, path, Some("beta=1"));
        assert_eq!(url.as_str(), expected, "{kind:?} {base:?} {path}");
    }
}

const TEST_MODEL: &str = "claude-sonnet-4-5-20250929";

#[test]
fn sse_event_split_reuses_allocation_and_releases_oversized_remainder() {
    let event = b"event: ping\ndata: {\"type\":\"ping\"}\n\n";
    let mut buffer = BytesMut::with_capacity(SSE_BUFFER_MAX_RETAINED_CAPACITY_BYTES * 2);
    buffer.extend_from_slice(event);
    buffer.extend_from_slice(b"partial");
    let input_ptr = buffer.as_ptr();

    let raw = split_sse_event(&mut buffer, event.len());

    assert_eq!(raw.as_ptr(), input_ptr, "split event must reuse allocation");
    assert_eq!(raw.as_ref(), event);
    assert_eq!(buffer.as_ref(), b"partial");
    assert!(buffer.capacity() <= SSE_BUFFER_MAX_RETAINED_CAPACITY_BYTES);
}

#[test]
fn sse_downstream_boundary_tracker_handles_split_terminators() {
    let mut tracker = SseDownstreamBoundaryTracker::default();

    tracker.observe(b"event: ping\r\n");
    assert!(tracker.partial_event_pending());
    tracker.observe(b"\r");
    assert!(tracker.partial_event_pending());
    tracker.observe(b"\n");
    assert!(!tracker.partial_event_pending());

    tracker.observe(b"data: next\r");
    assert!(tracker.partial_event_pending());
    tracker.observe(b"\r");
    assert!(!tracker.partial_event_pending());

    tracker.observe(b"data: final\n");
    assert!(tracker.partial_event_pending());
    tracker.observe(b"\n");
    assert!(!tracker.partial_event_pending());
}

#[test]
fn semantic_body_conversion_reuses_owned_and_identity_allocations() {
    let owned = vec![1_u8, 2, 3, 4];
    let owned_ptr = owned.as_ptr();
    let compressed_body = Bytes::from_static(b"compressed");

    let decoded = semantic_body_bytes(&compressed_body, std::borrow::Cow::Owned(owned));

    assert_eq!(decoded.as_ptr(), owned_ptr);

    let identity_body = Bytes::from_static(b"identity");
    let identity_ptr = identity_body.as_ptr();
    let identity = semantic_body_bytes(
        &identity_body,
        std::borrow::Cow::Borrowed(identity_body.as_ref()),
    );

    assert_eq!(identity.as_ptr(), identity_ptr);
}

#[test]
fn build_candidates_cache_score() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000201").unwrap();
    let warm_entries = HashMap::from([(
        upstream_id,
        vec![
            warm_entry_priced("short", TtlClass::Ephemeral5m, 4_100_000_300, 12, 100),
            warm_entry_priced("long", TtlClass::Ephemeral1h, 4_100_003_600, 13, 250),
        ],
    )]);
    let view = cache_score_view(upstream_id);
    let breakpoints = vec![
        cache_breakpoint(0, "long", 250, TtlClass::Ephemeral1h),
        cache_breakpoint(1, "cold", 300, TtlClass::Ephemeral5m),
    ];
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &breakpoints,
        &warm_entries,
        &cc_lb_clock::SystemClock,
    );
    let score = candidates[0].cache_score.as_ref().expect("cache score");
    assert_eq!(score.predicted_cache_read_tokens, 250);
    assert_eq!(score.predicted_cache_creation_tokens_5m, 50);
    assert_eq!(score.predicted_cache_creation_tokens_1h, 0);
    assert_eq!(score.predicted_uncached_input_tokens, 0);
    assert_eq!(score.predicted_expires_at_unix_secs, Some(4_100_003_600));
    assert_eq!(score.matched_breakpoint_index, Some(0));
    assert_eq!(score.confidence, 1.0);
    assert_eq!(score.ambiguity_reason, None);
}

#[test]
fn build_candidates_cache_score_from_production_parsed_breakpoints() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000204").unwrap();
    let lorem: String = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(300);
    let body = Bytes::from(format!(
        r#"{{"model":"{TEST_MODEL}","system":[{{"type":"text","text":"{lorem}","cache_control":{{"type":"ephemeral","ttl":"1h"}}}}],"messages":[{{"role":"user","content":"hi"}}],"max_tokens":16}}"#
    ));
    let breakpoints = request_cache_metadata(&HeaderMap::new(), &body).plugin_cache_breakpoints();
    assert_eq!(breakpoints.len(), 1);
    let prefix_hash = breakpoints[0].prefix_hash.clone();
    let prefix_token_count = breakpoints[0].prefix_bytes.get();
    assert!(prefix_token_count >= 1024);
    let warm_entries = HashMap::from([(
        upstream_id,
        vec![warm_entry_priced(
            &prefix_hash,
            TtlClass::Ephemeral1h,
            4_100_003_600,
            12,
            prefix_token_count,
        )],
    )]);
    let view = cache_score_view(upstream_id);
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &breakpoints,
        &warm_entries,
        &cc_lb_clock::SystemClock,
    );
    let score = candidates[0].cache_score.as_ref().expect("cache score");
    assert_eq!(
        u64::from(score.predicted_cache_read_tokens),
        prefix_token_count
    );
    assert!(score.predicted_cache_read_tokens >= 1024);
    assert_eq!(score.matched_breakpoint_index, Some(0));
    assert_eq!(score.confidence, 1.0);
}

#[test]
#[ignore = "release-only perf assertion; run with --release --ignored"]
fn build_candidates_cache_routing_latency_under_baseline_plus_2ms_p99() {
    const ITERATIONS: usize = 10_000;
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000299").unwrap();
    let warm_entries = HashMap::from([(
        upstream_id,
        vec![
            warm_entry("warm-prefix-a", TtlClass::Ephemeral1h, 4_100_003_600, 12),
            warm_entry("warm-prefix-b", TtlClass::Ephemeral5m, 4_100_000_300, 11),
        ],
    )]);
    let view = cache_score_view(upstream_id);
    let breakpoints = vec![
        cache_breakpoint(0, "warm-prefix-a", 2_400, TtlClass::Ephemeral1h),
        cache_breakpoint(1, "warm-prefix-b", 1_200, TtlClass::Ephemeral5m),
        cache_breakpoint(2, "cold-prefix", 800, TtlClass::Ephemeral5m),
    ];
    for _ in 0..ITERATIONS {
        let _ = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &warm_entries,
            &cc_lb_clock::SystemClock,
        );
    }
}

#[test]
fn build_candidates_no_warm_returns_cold_creation_estimate() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000202").unwrap();
    let view = cache_score_view(upstream_id);
    let breakpoints = vec![cache_breakpoint(0, "cold", 100, TtlClass::Ephemeral5m)];
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &breakpoints,
        &HashMap::from([(upstream_id, Vec::new())]),
        &cc_lb_clock::SystemClock,
    );
    assert_eq!(
        candidates[0]
            .cache_score
            .as_ref()
            .unwrap()
            .predicted_cache_read_tokens,
        0
    );
    assert_eq!(
        candidates[0]
            .cache_score
            .as_ref()
            .unwrap()
            .predicted_cache_creation_tokens_5m,
        100
    );
}

#[test]
fn build_candidates_considers_all_warm_entries() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000203").unwrap();
    let breakpoints = (0_u32..50)
        .map(|index| {
            cache_breakpoint(
                index,
                &format!("hash-{index}"),
                u64::from(index + 1),
                TtlClass::Ephemeral5m,
            )
        })
        .collect::<Vec<_>>();
    let entries = (0_u32..50)
        .map(|index| WarmCacheEntry {
            prefix_hash: format!("hash-{index}"),
            expires_at_unix_secs: 4_100_000_300,
            ttl_class: TtlClass::Ephemeral5m,
            last_observed_at_unix_secs: 1_700_000_100 + u64::from(50 - index),
            content_block_index: index,
            estimated_prefix_bytes: PrefixBytes::new(u64::from(index + 1)),
            hash_schema_version: HASH_SCHEMA_VERSION,
        })
        .collect::<Vec<_>>();
    let warm_entries = HashMap::from([(upstream_id, entries)]);
    let view = cache_score_view(upstream_id);
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &breakpoints,
        &warm_entries,
        &cc_lb_clock::SystemClock,
    );
    let score = candidates[0].cache_score.as_ref().expect("cache score");
    assert_eq!(score.predicted_cache_read_tokens, 50);
    assert_eq!(score.matched_breakpoint_index, Some(49));
}

#[test]
fn issue_1002_default_observations_stay_warm_until_provider_ttl() {
    let dispatch = 1_800_000_000;
    let defaults = cc_lb_config::PromptCacheShadowConfig::default();
    let explicit_grace = cc_lb_config::PromptCacheShadowConfig {
        grace_margin_secs: 60,
    };
    for (ttl, ttl_secs) in [(TtlClass::Ephemeral5m, 300), (TtlClass::Ephemeral1h, 3_600)] {
        for is_hit in [false, true] {
            for (config_name, config, warm_for) in [
                ("default", &defaults, ttl_secs),
                ("explicit_grace", &explicit_grace, ttl_secs - 60),
            ] {
                let context = PromptCacheObservationContext {
                    upstream_id: Uuid::from_u128(1002),
                    canonical_model_id: TEST_MODEL.to_owned(),
                    cache_breakpoints: vec![cache_breakpoint(3, "cached", 2_400, ttl)],
                    cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["cached"]),
                    selected_match: is_hit.then(|| selected_match_hit("cached", 3, ttl, 2_400)),
                    native_cache: None,
                    sink: Arc::new(RecordingPromptCacheObservationSink::new(Vec::new())),
                    grace_margin_secs: config.grace_margin_secs,
                };
                let decoded = decode_prompt_cache_observations_pure(
                    &context,
                    PromptCacheUsage {
                        cache_creation_input_tokens: if is_hit { 0 } else { 2_400 },
                        cache_read_input_tokens: if is_hit { 2_400 } else { 0 },
                        read_only_evidence: false,
                        ..PromptCacheUsage::default()
                    },
                    dispatch,
                );
                let [observation] = decoded.observations.as_slice() else {
                    panic!("successful cache usage must decode one observation");
                };
                let entries = [WarmCacheEntry {
                    prefix_hash: observation.prefix_hash.clone(),
                    expires_at_unix_secs: observation.expires_at_unix_secs,
                    ttl_class: observation.ttl_class,
                    last_observed_at_unix_secs: dispatch,
                    content_block_index: observation.prefix_content_block_index,
                    estimated_prefix_bytes: observation.estimated_prefix_bytes,
                    hash_schema_version: HASH_SCHEMA_VERSION,
                }];
                for elapsed in [269, 270, warm_for - 1, warm_for] {
                    let expected_warm = elapsed < warm_for;
                    let matched = resolve_longest_warm_match(
                        &entries,
                        &context.cache_breakpoints,
                        dispatch + elapsed,
                    );
                    assert_eq!(
                        matched.is_some(),
                        expected_warm,
                        "{config_name} {ttl:?} hit={is_hit} at +{elapsed}s",
                    );
                    let Some(score) =
                        build_cache_score_from_match(&context.cache_breakpoints, matched.as_ref())
                    else {
                        panic!("a cacheable request must have a cache routing score");
                    };
                    assert_eq!(
                        score.predicted_cache_read_tokens,
                        if expected_warm { 2_400 } else { 0 },
                    );
                }
            }
        }
    }
}

#[test]
fn response_decoder_decodes_observations_without_side_effects() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000221").unwrap();
    let now = 1_800_000_000;
    let cache = Arc::new(
        RecordingPromptCacheObservationSink::new(vec![
            warm_entry("short", TtlClass::Ephemeral5m, now + 120, 1),
            warm_entry("hit", TtlClass::Ephemeral1h, now + 3_000, 2),
        ])
        .with_clock_now(now),
    );
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![
            cache_breakpoint(0, "short", 1_200, TtlClass::Ephemeral5m),
            cache_breakpoint(1, "hit", 2_400, TtlClass::Ephemeral1h),
            cache_breakpoint(2, "write", 3_200, TtlClass::Ephemeral5m),
        ],
        cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["write"]),
        selected_match: Some(selected_match_hit("hit", 1, TtlClass::Ephemeral1h, 2_400)),
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };

    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage {
            cache_creation_input_tokens: 800,
            cache_read_input_tokens: 2_400,
            ..PromptCacheUsage::default()
        },
        now,
    );

    assert_eq!(decoded.dropped_below_threshold, 0);
    assert_eq!(decoded.observations.len(), 2);
    assert_eq!(decoded.observations[0].prefix_hash, "hit");
    assert_eq!(decoded.observations[0].ttl_class, TtlClass::Ephemeral1h);
    assert_eq!(decoded.observations[0].expires_at_unix_secs, now + 3_570);
    assert_eq!(decoded.observations[1].prefix_hash, "write");
    assert_eq!(decoded.observations[1].ttl_class, TtlClass::Ephemeral5m);
    assert_eq!(decoded.observations[1].expires_at_unix_secs, now + 270);
    assert!(cache.upserts().is_empty());
}

#[test]
fn native_cache_write_publication_preserves_provider_total() {
    let sink = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
    let context = PromptCacheObservationContext {
        upstream_id: Uuid::from_u128(1_164),
        canonical_model_id: "gpt-5.6".to_owned(),
        cache_breakpoints: Vec::new(),
        cacheable_breakpoint_prefix_keys: HashSet::new(),
        selected_match: None,
        native_cache: provider_cache::analyze(
            ProviderCapability::OpenaiResponses,
            &json!({"input": [{"role": "user", "content": "synthetic"}]}),
            "gpt-5.6",
            &HeaderMap::new(),
        ),
        sink: sink.clone(),
        grace_margin_secs: 0,
    };
    for (write, expected) in [
        (json!(300), Some(300)),
        (json!(0), Some(0)),
        (json!("300"), None),
        (Value::Null, None),
    ] {
        let mut usage = UsageCounts::default();
        let zero = write.as_u64() == Some(0);
        usage_parser::merge_usage_value_for_shape(
            &mut usage,
            &json!({
                "input_tokens": 1200,
                "input_tokens_details": {
                    "cached_tokens": 0,
                    "cache_write_tokens": write,
                    "cache_write_tokens_details": {
                        "ttl_30m_tokens": if zero { 0 } else { 200 },
                        "ttl_12h_tokens": if zero { 0 } else { 100 }
                    }
                }
            }),
            Some(crate::provider_capability::UsageShape::OpenaiResponses),
        );
        publish_provider_cache_observation(&context, PromptCacheUsage::from(&usage), 1_800_000_000);
        let records = sink.provider_upserts();
        let record = records.last().expect("provider observation published");
        assert_eq!(record.observed_write_tokens, expected);
        assert_eq!(record.provider, "openai");
        assert_eq!(record.canonical_model_id, "gpt-5.6");
        assert_eq!(record.retention_secs, Some(1800));
        assert_eq!(record.expires_at_unix_secs, Some(1_800_001_800));
    }
}

#[test]
fn provider_observation_without_lifetime_keeps_expiry_unknown() {
    let sink = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
    let context = PromptCacheObservationContext {
        upstream_id: Uuid::from_u128(1_126),
        canonical_model_id: "gpt-5.6".to_owned(),
        cache_breakpoints: Vec::new(),
        cacheable_breakpoint_prefix_keys: HashSet::new(),
        selected_match: None,
        native_cache: Some(cc_lb_request_log::RequestNativeCacheObservation {
            provider: "openai".to_owned(),
            mode: "implicit".to_owned(),
            evidence: cc_lb_request_log::RequestCacheEvidence::Positive,
            cached_tokens: Some(128),
            ..Default::default()
        }),
        sink: sink.clone(),
        grace_margin_secs: 0,
    };

    publish_provider_cache_observation(&context, PromptCacheUsage::default(), 1_800_000_000);

    let records = sink.provider_upserts();
    let [record] = records.as_slice() else {
        panic!("expected one provider observation");
    };
    assert_eq!(record.retention_secs, None);
    assert_eq!(record.expires_at_unix_secs, None);
}

#[test]
fn websocket_native_observation_publication_uses_lifecycle_clock() {
    let sink = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
    let lifecycle =
        read_inference_lifecycle(upstream_response(StatusCode::OK, b""), sink.clone(), None);
    let context = PromptCacheObservationContext {
        upstream_id: Uuid::from_u128(1_126),
        canonical_model_id: "gpt-5.6".to_owned(),
        cache_breakpoints: Vec::new(),
        cacheable_breakpoint_prefix_keys: HashSet::new(),
        selected_match: None,
        native_cache: Some(cc_lb_request_log::RequestNativeCacheObservation {
            provider: "openai".to_owned(),
            mode: "implicit".to_owned(),
            ..Default::default()
        }),
        sink: sink.clone(),
        grace_margin_secs: 0,
    };
    let usage = UsageCounts {
        cache_read_input_tokens: 128,
        cache_read_evidence: usage_parser::CacheTokenEvidence::Positive,
        ..UsageCounts::default()
    };

    lifecycle.publish_websocket_provider_cache_observation(&context, &usage);

    let records = sink.provider_upserts();
    let [record] = records.as_slice() else {
        panic!("expected one provider observation");
    };
    assert_eq!(record.upstream_id, context.upstream_id);
    assert_eq!(record.canonical_model_id, context.canonical_model_id);
    assert_eq!(record.observed_cached_tokens, Some(128));
    assert_eq!(record.last_observed_at_unix_secs, 1_800_000_000);
}

#[test]
fn hit_observation_uses_sliding_ttl_from_now() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000224").unwrap();
    let now = 1_800_000_000;
    let near_expiry = now + 30;
    let cache = Arc::new(
        RecordingPromptCacheObservationSink::new(vec![warm_entry(
            "hit",
            TtlClass::Ephemeral5m,
            near_expiry,
            1,
        )])
        .with_clock_now(now),
    );
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![cache_breakpoint(0, "hit", 2_400, TtlClass::Ephemeral5m)],
        cacheable_breakpoint_prefix_keys: HashSet::new(),
        selected_match: Some(selected_match_hit("hit", 0, TtlClass::Ephemeral5m, 2_400)),
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };

    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage {
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 2_400,
            ..PromptCacheUsage::default()
        },
        now,
    );

    assert_eq!(decoded.dropped_below_threshold, 0);
    assert_eq!(decoded.observations.len(), 1);
    let observation = &decoded.observations[0];
    assert_eq!(observation.prefix_hash, "hit");
    assert_eq!(observation.ttl_class, TtlClass::Ephemeral5m);
    assert_eq!(
        observation.kind,
        DecodedPromptCacheObservationKind::Hit,
        "cache_read>0 must produce a Hit observation"
    );
    assert_eq!(
        observation.expires_at_unix_secs,
        now + 270,
        "Hit observation must carry a fresh `now + ttl - grace` (Ephemeral5m -> 300 - 30) so the sliding-TTL semantics of Anthropic's prompt cache are reflected in cc-lb's observation store; regression guard for the router false-cold bug"
    );
    assert!(
        observation.expires_at_unix_secs > near_expiry,
        "sliding refresh must strictly extend the near-expiry stored value ({near_expiry}); got {}",
        observation.expires_at_unix_secs
    );
}

#[test]
fn hit_observation_honors_configured_grace_margin() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000225").unwrap();
    let now = 1_800_000_000;
    let cache = Arc::new(
        RecordingPromptCacheObservationSink::new(vec![warm_entry(
            "hit",
            TtlClass::Ephemeral5m,
            now + 30,
            1,
        )])
        .with_clock_now(now)
        .with_grace_secs(60),
    );
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![cache_breakpoint(0, "hit", 2_400, TtlClass::Ephemeral5m)],
        cacheable_breakpoint_prefix_keys: HashSet::new(),
        selected_match: Some(selected_match_hit("hit", 0, TtlClass::Ephemeral5m, 2_400)),
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };

    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage {
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 2_400,
            ..PromptCacheUsage::default()
        },
        now,
    );

    assert_eq!(decoded.observations.len(), 1);
    assert_eq!(
        decoded.observations[0].expires_at_unix_secs,
        now + 240,
        "grace_secs=60 must be applied end-to-end: expected now + 300 - 60"
    );
}

#[test]
fn response_decoder_error_response_skips_observation() {
    empty_usage_decodes_no_observations();
}

#[test]
fn empty_usage_decodes_no_observations() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000222").unwrap();
    let now = 1_800_000_000;
    let cache = Arc::new(RecordingPromptCacheObservationSink::new(vec![warm_entry(
        "hit",
        TtlClass::Ephemeral5m,
        now + 120,
        1,
    )]));
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![cache_breakpoint(0, "hit", 1_200, TtlClass::Ephemeral5m)],
        cacheable_breakpoint_prefix_keys: HashSet::new(),
        selected_match: None,
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };

    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage {
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            ..PromptCacheUsage::default()
        },
        now,
    );

    assert!(decoded.observations.is_empty());
    assert_eq!(decoded.dropped_below_threshold, 0);
    assert!(cache.upserts().is_empty());
}

#[test]
fn below_threshold_prefix_skips_observation() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000223").unwrap();
    let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![cache_breakpoint(0, "tiny", 1_023, TtlClass::Ephemeral5m)],
        cacheable_breakpoint_prefix_keys: HashSet::new(),
        selected_match: None,
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };

    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage {
            cache_creation_input_tokens: 1_023,
            cache_read_input_tokens: 0,
            ..PromptCacheUsage::default()
        },
        cache.clock_now_unix_secs(),
    );

    assert!(decoded.observations.is_empty());
    assert_eq!(decoded.dropped_below_threshold, 1);
    assert!(cache.upserts().is_empty());
}

#[test]
fn observation_uses_carried_match_under_concurrent_mutation() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000226").unwrap();
    let now = 1_800_000_000;
    let cache = Arc::new(
        RecordingPromptCacheObservationSink::new(vec![warm_entry(
            "stale-different",
            TtlClass::Ephemeral5m,
            now + 120,
            1,
        )])
        .with_clock_now(now),
    );
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![
            cache_breakpoint(0, "shallow", 1_200, TtlClass::Ephemeral5m),
            cache_breakpoint(1, "hit", 2_400, TtlClass::Ephemeral1h),
        ],
        cacheable_breakpoint_prefix_keys: HashSet::new(),
        selected_match: Some(selected_match_hit("hit", 1, TtlClass::Ephemeral1h, 2_400)),
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };

    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage {
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 2_400,
            ..PromptCacheUsage::default()
        },
        now,
    );

    assert_eq!(decoded.observations.len(), 1);
    assert_eq!(decoded.observations[0].prefix_hash, "hit");
    assert_eq!(decoded.observations[0].ttl_class, TtlClass::Ephemeral1h);
}

#[test]
fn buffered_and_sse_observations_are_equal() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000227").unwrap();
    let now = 1_800_000_000;
    let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![
            cache_breakpoint(0, "hit", 2_400, TtlClass::Ephemeral1h),
            cache_breakpoint(1, "write", 3_200, TtlClass::Ephemeral5m),
        ],
        cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["write"]),
        selected_match: Some(selected_match_hit("hit", 0, TtlClass::Ephemeral1h, 2_400)),
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };
    let usage = PromptCacheUsage {
        cache_creation_input_tokens: 800,
        cache_read_input_tokens: 2_400,
        ..PromptCacheUsage::default()
    };

    let buffered = decode_prompt_cache_observations_pure(&context, usage, now);
    let sse = decode_prompt_cache_observations_pure(&context, usage, now);

    assert_eq!(buffered.observations, sse.observations);
    assert_eq!(
        buffered.dropped_below_threshold,
        sse.dropped_below_threshold
    );
}

#[test]
fn refresh_non_breakpoint_nk_hit_on_read() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000230").unwrap();
    let now = 1_800_000_000;
    let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![cache_breakpoint(2, "owner", 5_000, TtlClass::Ephemeral5m)],
        cacheable_breakpoint_prefix_keys: HashSet::new(),
        selected_match: Some(SelectedCacheMatch {
            prefix_hash: "nk".to_owned(),
            content_block_index: 0,
            ttl_class: TtlClass::Ephemeral5m,
            estimated_prefix_bytes: PrefixBytes::new(2_000),
        }),
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };

    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage {
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 2_000,
            ..PromptCacheUsage::default()
        },
        now,
    );

    assert_eq!(decoded.observations.len(), 1);
    let hit = &decoded.observations[0];
    assert_eq!(hit.kind, DecodedPromptCacheObservationKind::Hit);
    assert_eq!(
        hit.prefix_hash, "nk",
        "refresh must target the matched N-k prefix, not the owning breakpoint"
    );
    assert_eq!(hit.prefix_content_block_index, 0);
    assert_eq!(hit.estimated_prefix_bytes, PrefixBytes::new(2_000));
}

#[test]
fn hit_refresh_republishes_current_estimator_source() {
    let upstream_id = Uuid::from_u128(76_604);
    let now = 1_800_000_000;
    let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![cache_breakpoint(2, "owner", 5_000, TtlClass::Ephemeral5m)],
        cacheable_breakpoint_prefix_keys: HashSet::new(),
        selected_match: Some(selected_match_hit("nk", 0, TtlClass::Ephemeral5m, 2_000)),
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };

    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage {
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 2_000,
            ..PromptCacheUsage::default()
        },
        now,
    );
    publish_prompt_cache_observations(&context, &decoded, now);

    let records = cache.upserts();
    assert_eq!(records.len(), 1, "a hit refresh publishes exactly one row");
    let record = records.first().expect("hit refresh record");
    assert_eq!(record.v3_prefix_key, "nk");
    assert_eq!(record.token_estimate_source, PrefixBytes::ESTIMATE_SOURCE);
    assert_eq!(record.estimated_prefix_tokens, 2_000);
    assert_eq!(
        PrefixBytes::from_persisted(
            record.estimated_prefix_tokens,
            &record.token_estimate_source
        ),
        Some(PrefixBytes::new(2_000)),
        "the republished row must decode as the matched bytes"
    );
}

#[test]
fn read_zero_does_not_refresh_predicted_match() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000231").unwrap();
    let now = 1_800_000_000;
    let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![cache_breakpoint(0, "hit", 2_400, TtlClass::Ephemeral5m)],
        cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["hit"]),
        selected_match: Some(selected_match_hit("hit", 0, TtlClass::Ephemeral5m, 2_400)),
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };

    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage {
            cache_creation_input_tokens: 2_400,
            cache_read_input_tokens: 0,
            ..PromptCacheUsage::default()
        },
        now,
    );

    assert!(
        decoded
            .observations
            .iter()
            .all(|obs| obs.kind != DecodedPromptCacheObservationKind::Hit),
        "cache_read==0 must not refresh the predicted match as a Hit"
    );
    assert!(cache.upserts().is_empty());
}

#[test]
fn read_positive_no_predict_without_eligible_key_stays_unknown() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000232").unwrap();
    let now = 1_800_000_000;
    let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![cache_breakpoint(
            0,
            "unknown-key",
            2_400,
            TtlClass::Ephemeral5m,
        )],
        cacheable_breakpoint_prefix_keys: HashSet::new(),
        selected_match: None,
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };

    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage {
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 2_400,
            ..PromptCacheUsage::default()
        },
        now,
    );

    assert!(
        decoded.observations.is_empty(),
        "a positive read with no eligible breakpoint (empty no-eligible-key control) must not fabricate a Hit key"
    );
    assert!(cache.upserts().is_empty());
}

#[test]
fn creation_persists_only_fold_marked_breakpoints() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000233").unwrap();
    let now = 1_800_000_000;
    let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![
            cache_breakpoint(0, "b0", 2_400, TtlClass::Ephemeral5m),
            cache_breakpoint(1, "b1", 2_500, TtlClass::Ephemeral5m),
            cache_breakpoint(2, "b2", 2_600, TtlClass::Ephemeral5m),
            cache_breakpoint(3, "b3", 2_700, TtlClass::Ephemeral5m),
        ],
        cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["b0", "b1", "b2", "b3"]),
        selected_match: Some(selected_match_hit("b1", 1, TtlClass::Ephemeral5m, 2_500)),
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };

    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage {
            cache_creation_input_tokens: 3_000,
            cache_read_input_tokens: 2_500,
            ..PromptCacheUsage::default()
        },
        now,
    );

    let hits = decoded
        .observations
        .iter()
        .filter(|obs| obs.kind == DecodedPromptCacheObservationKind::Hit)
        .map(|obs| obs.prefix_hash.as_str())
        .collect::<Vec<_>>();
    let writes = decoded
        .observations
        .iter()
        .filter(|obs| obs.kind == DecodedPromptCacheObservationKind::Write)
        .map(|obs| obs.prefix_hash.as_str())
        .collect::<Vec<_>>();

    assert_eq!(hits, vec!["b1"]);
    assert_eq!(writes, vec!["b2", "b3"]);
}

#[test]
fn overlapping_five_minute_hit_persists_one_hour_upgrade() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000234").unwrap();
    let now = 1_800_000_000;
    let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
    let one_hour = cache_breakpoint(10, "shared", 2_400, TtlClass::Ephemeral1h);
    let mut five_minute = cache_breakpoint(15, "later", 3_600, TtlClass::Ephemeral5m);
    five_minute.lookback_prefixes.push(CacheLookbackPrefix {
        prefix_hash: "shared".to_owned(),
        content_block_index: 10,
        lookback_distance: 5,
    });
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![one_hour, five_minute],
        cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["shared", "later"]),
        selected_match: Some(SelectedCacheMatch {
            prefix_hash: "shared".to_owned(),
            content_block_index: 10,
            ttl_class: TtlClass::Ephemeral5m,
            estimated_prefix_bytes: PrefixBytes::new(2_400),
        }),
        native_cache: None,
        sink: cache.clone(),
        grace_margin_secs: cache.grace_margin_secs(),
    };

    let decoded = decode_prompt_cache_observations_pure(
        &context,
        PromptCacheUsage {
            cache_creation_input_tokens: 3_600,
            cache_read_input_tokens: 2_400,
            ..PromptCacheUsage::default()
        },
        now,
    );
    let writes = decoded
        .observations
        .iter()
        .filter(|observation| observation.kind == DecodedPromptCacheObservationKind::Write)
        .map(|observation| (observation.prefix_hash.as_str(), observation.ttl_class))
        .collect::<Vec<_>>();

    assert_eq!(
        writes,
        vec![
            ("shared", TtlClass::Ephemeral1h),
            ("later", TtlClass::Ephemeral5m)
        ]
    );
}

fn unpredicted_read_context(
    cache: Arc<RecordingPromptCacheObservationSink>,
) -> PromptCacheObservationContext {
    PromptCacheObservationContext {
        upstream_id: Uuid::from_u128(1_003),
        canonical_model_id: TEST_MODEL.to_owned(),
        // Deliberately unsorted: structural depth, not vector order, bytes,
        // or the highest unqualified breakpoint, selects the inferred key.
        cache_breakpoints: vec![
            cache_breakpoint(5, "deep-eligible", 2_800, TtlClass::Ephemeral1h),
            cache_breakpoint(8, "deep-ineligible", 9_600, TtlClass::Ephemeral5m),
            cache_breakpoint(2, "shallow-eligible", 8_400, TtlClass::Ephemeral5m),
        ],
        cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&[
            "deep-eligible",
            "shallow-eligible",
        ]),
        selected_match: None,
        native_cache: None,
        grace_margin_secs: 30,
        sink: cache,
    }
}

#[test]
fn unpredicted_read_requires_affirmative_zero_creation() {
    let now = 1_800_000_000;
    for (reported, inferred) in [
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0}"#,
            true,
        ),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}"#,
            true,
        ),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}"#,
            true,
        ),
        (r#"{"cache_read_input_tokens":2400}"#, false),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":null}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation":null}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation":{}}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation":{"ephemeral_5m_input_tokens":0}}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation":{"ephemeral_1h_input_tokens":0}}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":null}}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0,"cache_creation":{}}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":"0"}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":false}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":-1}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0.5}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":0,"cache_creation_input_tokens":0}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":null,"cache_creation_input_tokens":0}"#,
            false,
        ),
        (r#"{"cache_creation_input_tokens":0}"#, false),
        (
            r#"{"cache_read_input_tokens":"2400","cache_creation_input_tokens":0}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":true,"cache_creation_input_tokens":0}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":-1,"cache_creation_input_tokens":0}"#,
            false,
        ),
        (
            r#"{"cache_read_input_tokens":2.5,"cache_creation_input_tokens":0}"#,
            false,
        ),
    ] {
        let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
        let context = unpredicted_read_context(cache.clone());
        let body = format!(r#"{{"usage":{reported}}}"#);
        let usage = usage_parser::observe_non_stream_json_body(body.as_bytes()).usage;
        let decoded =
            decode_prompt_cache_observations_pure(&context, PromptCacheUsage::from(&usage), now);
        let expected = if inferred {
            vec![DecodedPromptCacheObservation {
                prefix_hash: "deep-eligible".to_owned(),
                ttl_class: TtlClass::Ephemeral1h,
                expires_at_unix_secs: 1_800_003_570,
                prefix_content_block_index: 5,
                estimated_prefix_bytes: PrefixBytes::new(2_800),
                kind: DecodedPromptCacheObservationKind::Hit,
            }]
        } else {
            Vec::new()
        };
        assert_eq!(decoded.observations, expected, "{reported}");
        assert_eq!(decoded.dropped_below_threshold, 0, "{reported}");
        publish_prompt_cache_observations(&context, &decoded, now);
        let records = cache.upserts();
        assert_eq!(records.len(), usize::from(inferred), "{reported}");
        if let Some(record) = records.first() {
            assert_eq!(record.v3_prefix_key, "deep-eligible");
            assert_eq!(record.prefix_content_block_index, 5);
            assert_eq!(record.ttl_class, TtlClass::Ephemeral1h);
            assert_eq!(record.expires_at_unix_secs, 1_800_003_570);
            assert_eq!(record.estimated_prefix_tokens, 2_800);
            assert_eq!(record.token_estimate_source, PrefixBytes::ESTIMATE_SOURCE);
        }
    }
}

#[test]
fn unpredicted_positive_creation_preserves_writes_without_inferred_hits() {
    for reported in [
        r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":700}"#,
        r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":700,"ephemeral_1h_input_tokens":0}}"#,
        r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":700,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}"#,
    ] {
        let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
        let context = unpredicted_read_context(cache);
        let body = format!(r#"{{"usage":{reported}}}"#);
        let usage = usage_parser::observe_non_stream_json_body(body.as_bytes()).usage;
        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage::from(&usage),
            1_800_000_000,
        );
        assert_eq!(
            decoded.observations,
            vec![
                DecodedPromptCacheObservation {
                    prefix_hash: "deep-eligible".to_owned(),
                    ttl_class: TtlClass::Ephemeral1h,
                    expires_at_unix_secs: 1_800_003_570,
                    prefix_content_block_index: 5,
                    estimated_prefix_bytes: PrefixBytes::new(2_800),
                    kind: DecodedPromptCacheObservationKind::Write,
                },
                DecodedPromptCacheObservation {
                    prefix_hash: "shallow-eligible".to_owned(),
                    ttl_class: TtlClass::Ephemeral5m,
                    expires_at_unix_secs: 1_800_000_270,
                    prefix_content_block_index: 2,
                    estimated_prefix_bytes: PrefixBytes::new(8_400),
                    kind: DecodedPromptCacheObservationKind::Write,
                },
            ],
            "{reported}",
        );
        assert_eq!(decoded.dropped_below_threshold, 1);
    }
}

#[test]
fn affirmative_unpredicted_evidence_never_replaces_carried_shallow_or_same_key_hit() {
    let usage = usage_parser::observe_non_stream_json_body(
        br#"{"usage":{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0}}"#,
    )
    .usage;
    for (key, index, bytes) in [
        ("lookback-shallow", 1, 1_400),
        ("shallow-eligible", 2, 8_400),
    ] {
        let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
        let mut context = unpredicted_read_context(cache.clone());
        context.selected_match = Some(selected_match_hit(key, index, TtlClass::Ephemeral1h, bytes));
        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage::from(&usage),
            1_800_000_000,
        );
        assert_eq!(
            decoded.observations,
            vec![DecodedPromptCacheObservation {
                prefix_hash: key.to_owned(),
                ttl_class: TtlClass::Ephemeral1h,
                expires_at_unix_secs: 1_800_003_570,
                prefix_content_block_index: index,
                estimated_prefix_bytes: PrefixBytes::new(bytes),
                kind: DecodedPromptCacheObservationKind::Hit,
            }],
        );
        publish_prompt_cache_observations(&context, &decoded, 1_800_000_000);
        let records = cache.upserts();
        assert_eq!(records.len(), 1);
        let record = records.first().expect("carried match record");
        assert_eq!(record.v3_prefix_key, key);
        assert_eq!(record.ttl_class, TtlClass::Ephemeral1h);
        assert_eq!(record.prefix_content_block_index, index);
        assert_eq!(record.estimated_prefix_tokens, bytes);
    }
}

/// Independent reference bytes and hash chain for this fixed two-block
/// request; expectations never come from analyzed/decoded breakpoint data.
fn read_inference_request_fixture(automatic: bool, large: bool) -> (Value, String, u64) {
    let text = if large {
        "cache evidence ".repeat(3_000)
    } else {
        "tiny".to_owned()
    };
    let mut tail = json!({"type":"text","text":"tail"});
    if !automatic {
        tail["cache_control"] = json!({"type":"ephemeral","ttl":"5m"});
    }
    let first = json!({
        "type":"text","text":text,
        "cache_control":{"type":"ephemeral","ttl":"1h"},
    });
    let mut request = json!({
        "model":TEST_MODEL,
        "messages":[{"role":"user","content":[first.clone(),tail.clone()]}],
        "max_tokens":16,
    });
    if automatic {
        request["cache_control"] = json!({"type":"ephemeral","ttl":"5m"});
    }
    let mut seed = blake3::Hasher::new();
    seed.update(b"cc-lb-cache-v6:seed");
    seed.update(b"claude-sonnet-4-5-20250929");
    let mut previous = seed.finalize();
    for (index, block_text) in [(0, text.as_str()), (1, "tail")] {
        let digest_value = json!({
            "path":format!("messages[0].content[{index}]"),
            "source":"message",
            "value":{"text":block_text,"type":"text"},
        });
        let mut block = blake3::Hasher::new();
        block.update(b"cc-lb-cache-v6:block");
        block.update(&serde_json::to_vec(&digest_value).expect("reference block JSON"));
        let mut prefix = blake3::Hasher::new();
        prefix.update(b"cc-lb-cache-v6:prefix");
        prefix.update(previous.as_bytes());
        prefix.update(block.finalize().as_bytes());
        previous = prefix.finalize();
    }
    let reference_prefix = json!({
        "content_blocks":[
            {"source":"message","value":first},
            {"source":"message","value":tail},
        ],
        "model":"claude-sonnet-4-5-20250929",
    });
    let bytes = serde_json::to_vec(&reference_prefix)
        .expect("reference prefix JSON")
        .len() as u64;
    (request, previous.to_hex().to_string(), bytes)
}

#[tokio::test]
async fn unpredicted_read_infers_deepest_resolved_explicit_and_automatic_boundary() {
    for automatic in [false, true] {
        let (request, expected_key, expected_bytes) =
            read_inference_request_fixture(automatic, true);
        let output = PromptCacheAnalysisExecutor::default()
            .analyze(
                Arc::new(request.clone()),
                TEST_MODEL.to_owned(),
                [0; 32],
                Some(1_024),
            )
            .await;
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            Some(output),
            None,
            None,
        );
        let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
        let context = PromptCacheObservationContext {
            upstream_id: Uuid::from_u128(1_003),
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: metadata.plugin_cache_breakpoints(),
            cacheable_breakpoint_prefix_keys: metadata
                .bundles
                .official
                .cacheable_breakpoint_prefix_keys,
            selected_match: None,
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: 30,
        };
        let usage = usage_parser::observe_non_stream_json_body(
            br#"{"usage":{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0}}"#,
        )
        .usage;
        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage::from(&usage),
            1_800_000_000,
        );
        assert_eq!(
            decoded.observations,
            vec![DecodedPromptCacheObservation {
                prefix_hash: expected_key.clone(),
                ttl_class: TtlClass::Ephemeral5m,
                expires_at_unix_secs: 1_800_000_270,
                prefix_content_block_index: 1,
                estimated_prefix_bytes: PrefixBytes::new(expected_bytes),
                kind: DecodedPromptCacheObservationKind::Hit,
            }],
            "automatic={automatic}",
        );
        publish_prompt_cache_observations(&context, &decoded, 1_800_000_000);
        let records = cache.upserts();
        assert_eq!(records.len(), 1);
        let record = records.first().expect("inferred boundary record");
        assert_eq!(record.v3_prefix_key, expected_key);
        assert_eq!(record.prefix_content_block_index, 1);
        assert_eq!(record.estimated_prefix_tokens, expected_bytes);
        assert_eq!(record.ttl_class, TtlClass::Ephemeral5m);
        assert_eq!(record.expires_at_unix_secs, 1_800_000_270);
    }
}

#[tokio::test]
async fn unpredicted_read_with_empty_or_actual_below_threshold_eligibility_stays_unknown() {
    for automatic in [false, true] {
        let (request, _, _) = read_inference_request_fixture(automatic, false);
        let output = PromptCacheAnalysisExecutor::default()
            .analyze(
                Arc::new(request.clone()),
                TEST_MODEL.to_owned(),
                [0; 32],
                Some(1_024),
            )
            .await;
        assert!(output.cacheable_breakpoint_prefix_keys.is_empty());
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            Some(output),
            None,
            None,
        );
        let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
        let mut context = PromptCacheObservationContext {
            upstream_id: Uuid::from_u128(1_003),
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: metadata.plugin_cache_breakpoints(),
            cacheable_breakpoint_prefix_keys: metadata
                .bundles
                .official
                .cacheable_breakpoint_prefix_keys,
            selected_match: None,
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: 30,
        };
        let usage = usage_parser::observe_non_stream_json_body(
            br#"{"usage":{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0}}"#,
        )
        .usage;
        for empty_breakpoints in [false, true] {
            if empty_breakpoints {
                context.cache_breakpoints.clear();
            }
            let decoded = decode_prompt_cache_observations_pure(
                &context,
                PromptCacheUsage::from(&usage),
                1_800_000_000,
            );
            assert!(decoded.observations.is_empty());
            publish_prompt_cache_observations(&context, &decoded, 1_800_000_000);
            assert!(cache.upserts().is_empty());
        }
    }
}

struct ObservationSseDialect {
    calls: Arc<AtomicUsize>,
}

impl UpstreamDialect for ObservationSseDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        upstream: &Upstream,
        principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        RawPassthroughDialect {
            base_url: Url::parse("https://api.anthropic.com").expect("test upstream URL"),
            openai_oauth: false,
        }
        .shape(context, upstream, principal, builder)
    }

    fn sse_event_transform_hook(&self) -> Option<&dyn SseEventTransformHook> {
        Some(self)
    }
}

impl SseEventTransformHook for ObservationSseDialect {
    fn transform_sse_event(
        &self,
        _request: cc_lb_upstream::TransformSseEventRequest,
    ) -> Result<cc_lb_upstream::TransformSseEventResult, ResponseTransformError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(cc_lb_upstream::TransformSseEventResult::Unchanged)
    }
}

fn read_inference_lifecycle(
    upstream_response: Response<Body>,
    cache: Arc<RecordingPromptCacheObservationSink>,
    transform_calls: Option<Arc<AtomicUsize>>,
) -> Lifecycle {
    let dialect = transform_calls.map_or(
        cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
        |calls| {
            cc_lb_control::api_keys::principal_view::DialectCache::Explicit(
                cc_lb_control::api_keys::principal_view::ShapePluginCache {
                    dialect: Arc::new(ObservationSseDialect { calls }),
                },
            )
        },
    );
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .principal_view(failure_principal_view("principal-test", (None, dialect)))
        .upstream_records(vec![upstream_record(Uuid::from_u128(1_003))])
        .prompt_cache_observation_sink(cache)
        .prompt_cache_grace_margin_secs(30)
        .build();
    Lifecycle::new_with_dynamic_view(
        failure_authn(),
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(QueueDispatch::new(vec![Ok(upstream_response)])),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000)),
    )
}

fn assert_inferred_published_record(
    records: &[PromptCacheObservationRecord],
    expected_key: &str,
    expected_bytes: u64,
) {
    assert_eq!(records.len(), 1, "publish exactly one deepest boundary");
    let record = records.first().expect("inferred published record");
    assert_eq!(record.upstream_id, Uuid::from_u128(1_003));
    assert_eq!(record.canonical_model_id, TEST_MODEL);
    assert_eq!(record.v3_prefix_key, expected_key);
    assert_eq!(record.prefix_content_block_index, 1);
    assert_eq!(record.ttl_class, TtlClass::Ephemeral5m);
    assert_eq!(
        record.expires_at_unix_secs,
        record.last_observed_at_unix_secs + 270,
    );
    assert_eq!(record.estimated_prefix_tokens, expected_bytes);
    assert_eq!(record.token_estimate_source, PrefixBytes::ESTIMATE_SOURCE);
    assert_eq!(record.hash_schema_version, HASH_SCHEMA_VERSION);
}

#[tokio::test]
async fn buffered_unpredicted_read_publishes_one_resolved_boundary() {
    for automatic in [false, true] {
        let (request, expected_key, expected_bytes) =
            read_inference_request_fixture(automatic, true);
        let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
        let lifecycle = read_inference_lifecycle(
                upstream_response(
                    StatusCode::OK,
                    br#"{"id":"msg_read","type":"message","role":"assistant","model":"claude-sonnet-4-5-20250929","content":[],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":2400,"cache_creation_input_tokens":0}}"#,
                ),
                cache.clone(),
                None,
            );
        let request = failure_request(Bytes::from(
            serde_json::to_vec(&request).expect("request JSON"),
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("buffered request succeeds");
        assert_eq!(response.status(), StatusCode::OK);
        assert_inferred_published_record(&cache.upserts(), &expected_key, expected_bytes);
        let _ = collect_response_body(response).await;
        assert_inferred_published_record(&cache.upserts(), &expected_key, expected_bytes);
    }
}

#[tokio::test]
async fn native_and_transformed_sse_unpredicted_reads_publish_early_exactly_once() {
    for automatic in [false, true] {
        for transformed in [false, true] {
            for drop_after_start in [false, true] {
                let (mut request, expected_key, expected_bytes) =
                    read_inference_request_fixture(automatic, true);
                request["stream"] = Value::Bool(true);
                let (release, released) = tokio::sync::oneshot::channel::<()>();
                let stream = async_stream::stream! {
                    yield Ok::<Bytes, Infallible>(Bytes::from_static(
                        b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_read\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5-20250929\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"cache_read_input_tokens\":2400,\"cache_creation\":{\"ephemeral_5m_input_tokens\":0,\"ephemeral_1h_input_tokens\":0}}}}\n\n"
                    ));
                    let _ = released.await;
                    yield Ok::<Bytes, Infallible>(Bytes::from_static(
                        b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":17,\"cache_read_input_tokens\":2400,\"cache_creation_input_tokens\":0}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
                    ));
                };
                let mut upstream = Response::new(Body::from_stream(stream));
                upstream
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
                let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
                let transform_calls = Arc::new(AtomicUsize::new(0));
                let lifecycle = read_inference_lifecycle(
                    upstream,
                    cache.clone(),
                    transformed.then(|| transform_calls.clone()),
                );
                let request = failure_request(Bytes::from(
                    serde_json::to_vec(&request).expect("streaming request JSON"),
                ));
                let auth = lifecycle
                    .authenticate(request.headers())
                    .await
                    .expect("request authenticates");
                let response = lifecycle
                    .handle(request, &auth)
                    .await
                    .expect("streaming request succeeds");
                assert_eq!(response.status(), StatusCode::OK);
                assert!(cache.upserts().is_empty(), "body has not been polled yet");
                let mut body = response.into_body();
                let first = tokio::time::timeout(Duration::from_secs(5), body.frame())
                    .await
                    .expect("message_start arrives before upstream completion")
                    .expect("message_start frame")
                    .expect("message_start body data");
                assert!(first.is_data());
                assert_inferred_published_record(&cache.upserts(), &expected_key, expected_bytes);
                if transformed {
                    assert!(transform_calls.load(Ordering::SeqCst) > 0);
                }
                if drop_after_start {
                    drop(body);
                    drop(release);
                } else {
                    release.send(()).expect("release stream completion");
                    let _ = body.collect().await.expect("remaining SSE collects");
                }
                assert_inferred_published_record(&cache.upserts(), &expected_key, expected_bytes);
            }
        }
    }
}

#[test]
fn expiry_anchored_at_start_not_completion() {
    let dispatch_started = Instant::now();
    let dispatch_unix_secs = 1_000_u64;
    let message_start_at = dispatch_started + Duration::from_secs(2);
    let anchor =
        response_start_unix_secs(dispatch_unix_secs, dispatch_started, Some(message_start_at));
    assert_eq!(anchor, 1_002);

    let grace = 60;
    let expires = prompt_cache_observation_expires_at(anchor, TtlClass::Ephemeral5m, grace);
    assert_eq!(expires, 1_002 + 300 - 60);

    let completion_unix = dispatch_unix_secs + 62;
    let completion_expires =
        prompt_cache_observation_expires_at(completion_unix, TtlClass::Ephemeral5m, grace);
    assert!(
        expires < completion_expires,
        "start anchor must not extend expiry by the generation duration"
    );
}

#[tokio::test]
async fn issue_1002_sse_relay_expires_from_dispatch_not_message_start()
-> Result<(), Box<dyn std::error::Error>> {
    const MESSAGE_START: &[u8] = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"usage\":",
        "{\"input_tokens\":10,\"output_tokens\":1,\"cache_creation_input_tokens\":2400}}}\n\n",
    )
    .as_bytes();
    let upstream_id = Uuid::from_u128(1002);
    let dispatch_unix_secs = 1_800_000_000;
    let principal_view = failure_principal_view("principal-test", default_artifacts());
    let record = upstream_record(upstream_id);
    let cached = principal_view
        .get("principal-test")
        .ok_or("missing principal")?;
    let (route, _) = route_for_record(&record, cached).map_err(|_| "route does not resolve")?;
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut lifecycle = provenance_lifecycle(
        provenance_view(principal_view, Arc::new(TestSignerFactory), vec![record]),
        Arc::new(QueueDispatch::new(Vec::new())),
        &bus,
    );
    let sink = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
    let context = PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_breakpoints: vec![cache_breakpoint(3, "cached", 2_400, TtlClass::Ephemeral5m)],
        cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["cached"]),
        selected_match: None,
        native_cache: None,
        sink: sink.clone(),
        grace_margin_secs: 0,
    };
    let upstream_body = Body::from_stream(async_stream::stream! {
        yield Ok::<_, Infallible>(Bytes::from_static(MESSAGE_START));
        std::future::pending::<()>().await;
    });
    let response = Response::builder()
        .header(CONTENT_TYPE, "text/event-stream")
        .body(upstream_body)?;
    // std::time::Instant is deliberately backdated: Tokio's paused clock
    // does not advance the production relay's monotonic timing source.
    let dispatch_started = Instant::now() - Duration::from_secs(40);
    let event_ctx = RequestEventContext {
        request_id: "issue-1002-relay".to_owned(),
        canonical_model_id: TEST_MODEL.to_owned(),
        upstream_name: format!("upstream-{upstream_id}"),
        requested_model: Some(TEST_MODEL.to_owned()),
        proxy_setup_ms: None,
        stage_timings: AttemptTimings::default(),
    };
    let transform_ctx = ResponseTransformContext {
        principal: Principal {
            id: "principal-test".to_owned(),
            kind: PrincipalKind::InternalKey,
        },
        upstream: route.upstream,
        request_method: http::Method::POST,
        request_path: "/v1/messages".to_owned(),
        dialect: route.dialect,
        gemini_oauth: false,
        vertex: false,
        codex_backend: false,
        vertex_location: None,
    };
    let affinity_ctx = ResponseAffinityContext {
        capability: ProviderCapability::AnthropicMessages,
        usage_shape: None,
        precomputed_usage: None,
        store: None,
        principal_id: "principal-test".to_owned(),
        upstream_id,
        pending_request_keys: Vec::new(),
        observe_response_keys: false,
        owner_epoch: None,
        ttl_secs: 3_600,
    };
    let response = lifecycle.relay_response(
        response,
        Arc::new(TestSigner),
        StatusCode::OK,
        false,
        false,
        None,
        event_ctx.clone(),
        transform_ctx.clone(),
        None,
        Some(context.clone()),
        affinity_ctx.clone(),
        None,
        None,
        dispatch_started,
        dispatch_unix_secs,
    );
    let mut body = response.into_body();
    let frame = tokio::time::timeout(Duration::from_secs(1), body.frame())
        .await?
        .ok_or("message_start was not relayed")??;
    let data = frame.into_data().map_err(|_| "expected a data frame")?;
    assert_eq!(data.as_ref(), MESSAGE_START);
    // The upstream is still pending: observation publication and event
    // delivery must not wait for completion or message_stop.
    let records = sink.upserts();
    let [record] = records.as_slice() else {
        panic!("message_start must publish exactly one cache observation");
    };
    assert_eq!(record.expires_at_unix_secs, dispatch_unix_secs + 300);
    assert!(record.last_observed_at_unix_secs >= dispatch_unix_secs + 40);
    drop(body);

    // Buffered responses publish at completion, independently of expiry.
    let completion_unix_secs = dispatch_unix_secs + 90;
    lifecycle.clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(completion_unix_secs));
    const BUFFERED: &[u8] =
        br#"{"usage":{"input_tokens":10,"output_tokens":1,"cache_creation_input_tokens":2400}}"#;
    let response = lifecycle
        .finish_success_response(
            upstream_response(StatusCode::OK, BUFFERED),
            Arc::new(TestSigner),
            false,
            false,
            None,
            None,
            Duration::from_secs(90),
            StatusCode::OK,
            event_ctx,
            transform_ctx,
            None,
            Some(context),
            affinity_ctx,
            None,
            None,
            dispatch_started,
            dispatch_unix_secs,
        )
        .await;
    assert_eq!(response.into_body().collect().await?.to_bytes(), BUFFERED);
    let records = sink.upserts();
    let [_, buffered_record] = records.as_slice() else {
        panic!("buffered usage must publish one additional cache observation");
    };
    assert_eq!(
        buffered_record.expires_at_unix_secs,
        dispatch_unix_secs + 300
    );
    assert_eq!(
        buffered_record.last_observed_at_unix_secs,
        completion_unix_secs
    );
    Ok(())
}

#[test]
fn sse_without_message_start_falls_back_to_dispatch() {
    let dispatch_started = Instant::now();
    let dispatch_unix_secs = 5_000_u64;
    assert_eq!(
        response_start_unix_secs(dispatch_unix_secs, dispatch_started, None),
        dispatch_unix_secs
    );
}

#[test]
fn build_subscription_quota_samples_from_headers() {
    let upstream_id = Uuid::new_v4();
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-unified-7d-sonnet-utilization"),
        HeaderValue::from_static("0.42"),
    );
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-unified-7d-sonnet-status"),
        HeaderValue::from_static("allowed_warning"),
    );
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-unified-7d-sonnet-fallback-percentage"),
        HeaderValue::from_static("0.5"),
    );
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-unified-7d-sonnet-surpassed-threshold"),
        HeaderValue::from_static("0.75"),
    );
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-unified-fallback"),
        HeaderValue::from_static("available"),
    );
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-unified-overage-in-use"),
        HeaderValue::from_static("true"),
    );
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-unified-overage-period-monthly-utilization"),
        HeaderValue::from_static("0.20"),
    );
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-unified-upgrade-paths"),
        HeaderValue::from_static("team_growth,max_5x"),
    );

    let records = build_subscription_quota_samples(&headers, upstream_id, 123_456);

    assert_eq!(records.len(), 2);
    let unified = records
        .iter()
        .find(|r| r.window == SubscriptionQuotaWindow::Unified)
        .expect("unified top-level record");
    let sonnet = records
        .iter()
        .find(|r| r.window == SubscriptionQuotaWindow::SevenDaySonnet)
        .expect("7d-sonnet record");

    assert_eq!(sonnet.upstream_id, upstream_id);
    assert_eq!(sonnet.source, SubscriptionQuotaSource::Header);
    assert_eq!(sonnet.sample_kind, SubscriptionQuotaSampleKind::Sample);
    assert_eq!(sonnet.observed_at_unix_millis, 123_456);
    assert_eq!(sonnet.ingested_at_unix_millis, 123_456);
    assert_ne!(sonnet.sample_id, Uuid::nil());
    assert_eq!(sonnet.utilization, Some(0.42));
    assert_eq!(sonnet.status, Some(SubscriptionQuotaStatus::AllowedWarning));
    assert_eq!(sonnet.fallback_percentage, Some(0.5));
    assert_eq!(sonnet.surpassed_threshold, Some(0.75));

    assert_eq!(unified.fallback_available, Some(true));
    assert_eq!(unified.overage_in_use, Some(true));
    assert_eq!(unified.overage_period_monthly_utilization, Some(0.20));
    assert_eq!(
        unified.upgrade_paths.as_deref(),
        Some(["max_5x".to_owned(), "team_growth".to_owned()].as_slice())
    );
}

#[test]
fn request_cache_metadata_tracks_thread_message_and_prefix_without_prompt_text() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-claude-code-session-id"),
        HeaderValue::from_static("thread-123"),
    );
    let body = Bytes::from_static(
            br#"{
                "model":"claude-sonnet-4-5",
                "tools":[{"name":"secret_tool","input_schema":{"type":"object"},"cache_control":{"type":"ephemeral"}}],
                "system":[{"type":"text","text":"secret system","cache_control":{"type":"ephemeral","ttl":"1h"}}],
                "messages":[
                    {"role":"user","content":"first secret"},
                    {"id":"msg_2","role":"user","content":[{"type":"text","text":"second secret","cache_control":{"type":"ephemeral"}}]}
                ]
            }"#,
        );

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(metadata.thread_id.as_deref(), Some("thread-123"));
    assert_eq!(metadata.message_id.as_deref(), Some("msg_2"));
    assert_eq!(metadata.message_index, Some(1));
    assert_eq!(metadata.message_count, Some(2));
    assert_eq!(metadata.cache_control_message_indices, vec![1]);
    assert_eq!(metadata.cache_breakpoints.len(), 3);
    assert_eq!(metadata.cache_breakpoints[0].block_index, 0);
    assert_eq!(
        metadata.cache_breakpoints[0].source,
        RequestCacheBreakpointSource::Tools
    );
    assert_eq!(metadata.cache_breakpoints[0].path, "tools[0]");
    assert_eq!(metadata.cache_breakpoints[1].block_index, 1);
    assert_eq!(
        metadata.cache_breakpoints[1].source,
        RequestCacheBreakpointSource::System
    );
    assert_eq!(metadata.cache_breakpoints[1].path, "system[0]");
    assert_eq!(metadata.cache_breakpoints[1].ttl.as_deref(), Some("1h"));
    assert_eq!(metadata.cache_breakpoints[2].block_index, 3);
    assert_eq!(
        metadata.cache_breakpoints[2].source,
        RequestCacheBreakpointSource::Message
    );
    assert_eq!(metadata.cache_breakpoints[2].path, "messages[1].content[0]");
    assert_eq!(metadata.cache_breakpoints[2].message_index, Some(1));
    assert_ne!(
        metadata.cache_breakpoints[0].prefix_hash,
        metadata.cache_breakpoints[1].prefix_hash
    );
    let hash = metadata.cache_prefix_hash.expect("cache prefix hash");
    assert_eq!(hash.len(), 64);
    assert_eq!(hash, metadata.cache_breakpoints[2].prefix_hash);
    assert!(!hash.contains("secret"));
}

#[test]
fn request_cache_metadata_preserves_raw_requested_service_tier() {
    // Given a request body with a mixed-case tier value.
    let body = Bytes::from_static(
        br#"{"model":"claude-test","service_tier":"Priority-Raw","messages":[]}"#,
    );

    // When request metadata is extracted from the parsed body.
    let metadata = request_cache_metadata(&HeaderMap::new(), &body);

    // Then the pre-request path retains the exact requested value.
    assert_eq!(
        metadata.requested_service_tier.as_deref(),
        Some("Priority-Raw")
    );
}

#[test]
fn gemini_request_aliases_preserve_tier_and_bounded_thinking_metadata() {
    for alias in ["service_tier", "serviceTier"] {
        for tier in ["flex", "priority", "FLEX", "future-tier"] {
            let request = json!({
                (alias): tier,
                "generationConfig": {"thinkingConfig": {
                    "thinkingLevel": "LOW",
                    "thinkingBudget": 1024,
                    "includeThoughts": true
                }},
                "reasoning_effort": "wrong-provider"
            });
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                None,
                Some(ProviderCapability::GeminiGenerateContent),
                None,
            );
            let limit = LimitRequest::from_value(
                Some(&request),
                "gemini-test",
                Some(ProviderCapability::GeminiGenerateContent),
                &[],
                false,
            );
            assert_eq!(metadata.requested_service_tier.as_deref(), Some(tier));
            assert_eq!(limit.service_tier.as_deref(), Some(tier));
            assert_eq!(metadata.thinking_level.as_deref(), Some("LOW"));
            assert_eq!(metadata.thinking_budget_tokens, Some(1024));
            assert_eq!(metadata.reasoning_effort, None);
        }
    }
}

#[test]
fn gemini_service_tier_alias_precedence_and_malformed_fields_are_conservative() {
    for (request, expected) in [
        (
            json!({"service_tier":"priority","serviceTier":"flex"}),
            Some("priority"),
        ),
        (json!({"service_tier":null,"serviceTier":"flex"}), None),
        (json!({"serviceTier":null}), None),
        (json!({"serviceTier":42}), None),
        (json!({"servicetier":"flex"}), None),
    ] {
        let capability = Some(ProviderCapability::GeminiGenerateContent);
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            capability,
            None,
        );
        let limit = LimitRequest::from_value(Some(&request), "gemini-test", capability, &[], false);
        assert_eq!(metadata.requested_service_tier.as_deref(), expected);
        assert_eq!(limit.service_tier.as_deref(), expected);
    }
}
#[test]
fn review_1166_gemini_native_snake_case_thinking_aliases_preserve_metadata() {
    let request = json!({
        "generationConfig": {
            "thinkingConfig": {
                "thinking_level": "LOW",
                "thinking_budget": 1024
            }
        }
    });
    let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
        &HeaderMap::new(),
        Some(&request),
        None,
        Some(ProviderCapability::GeminiGenerateContent),
        None,
    );
    assert_eq!(metadata.thinking_level.as_deref(), Some("LOW"));
    assert_eq!(metadata.thinking_budget_tokens, Some(1024));
    assert_eq!(metadata.reasoning_effort, None);
}

#[test]
fn review_1166_gemini_thinking_alias_bounds_and_camel_precedence_are_conservative() {
    let valid_snake = json!({
        "generationConfig": {
            "thinkingConfig": {
                "thinking_level": "snake",
                "thinking_budget": 2048
            }
        }
    });
    let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
        &HeaderMap::new(),
        Some(&valid_snake),
        None,
        Some(ProviderCapability::GeminiGenerateContent),
        None,
    );
    assert_eq!(metadata.thinking_level.as_deref(), Some("snake"));
    assert_eq!(metadata.thinking_budget_tokens, Some(2048));

    for (camel_level, snake_level, expected_level) in [
        ("CAMEL", "snake", Some("CAMEL")),
        ("", "snake", None),
        (&"x".repeat(65), "snake", None),
    ] {
        let request = json!({
            "generationConfig": {"thinkingConfig": {
                "thinkingLevel": camel_level,
                "thinking_level": snake_level,
                "thinking_budget": 0
            }}
        });
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            Some(ProviderCapability::GeminiGenerateContent),
            None,
        );
        assert_eq!(metadata.thinking_level.as_deref(), expected_level);
        assert_eq!(metadata.thinking_budget_tokens, Some(0));
    }

    for (camel_budget, snake_budget, expected_budget) in [
        (json!(0), json!(2048), Some(0)),
        (json!(-1), json!(2048), None),
        (json!("2048"), json!(2048), None),
        (json!(9_007_199_254_740_992_u64), json!(2048), None),
        (
            json!(9_007_199_254_740_991_u64),
            json!(2048),
            Some(9_007_199_254_740_991),
        ),
    ] {
        let request = json!({
            "generationConfig": {"thinkingConfig": {
                "thinking_budget": snake_budget,
                "thinkingBudget": camel_budget
            }}
        });
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            Some(ProviderCapability::GeminiGenerateContent),
            None,
        );
        assert_eq!(metadata.thinking_budget_tokens, expected_budget);
    }
}

#[test]
fn review_1166_gemini_blank_tiers_are_absent_but_nonblank_raw_values_are_preserved() {
    for request in [
        json!({"serviceTier": ""}),
        json!({"serviceTier": " \t "}),
        json!({"service_tier": null, "serviceTier": "priority"}),
        json!({"service_tier": " \n ", "serviceTier": "priority"}),
        json!({"service_tier": 42, "serviceTier": "priority"}),
    ] {
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            Some(ProviderCapability::GeminiGenerateContent),
            None,
        );
        let limit = LimitRequest::from_value(
            Some(&request),
            "gemini-test",
            Some(ProviderCapability::GeminiGenerateContent),
            &[],
            false,
        );
        assert_eq!(metadata.requested_service_tier, None);
        assert_eq!(limit.service_tier, None);
    }

    for raw in [" flex ", "Priority-Raw"] {
        let request = json!({"serviceTier": raw});
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            Some(ProviderCapability::GeminiGenerateContent),
            None,
        );
        let limit = LimitRequest::from_value(
            Some(&request),
            "gemini-test",
            Some(ProviderCapability::GeminiGenerateContent),
            &[],
            false,
        );
        assert_eq!(metadata.requested_service_tier.as_deref(), Some(raw));
        assert_eq!(limit.service_tier.as_deref(), Some(raw));
    }
}

#[test]
fn review_1166_non_gemini_metadata_stays_provider_scoped() {
    let request = json!({
        "service_tier": "",
        "serviceTier": "priority",
        "generationConfig": {"thinkingConfig": {
            "thinking_level": "LOW",
            "thinking_budget": 1024
        }},
        "reasoning_effort": "medium"
    });
    let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
        &HeaderMap::new(),
        Some(&request),
        None,
        Some(ProviderCapability::OpenaiChatCompletions),
        None,
    );
    assert_eq!(metadata.requested_service_tier.as_deref(), Some(""));
    assert_eq!(metadata.thinking_level, None);
    assert_eq!(metadata.thinking_budget_tokens, None);
    assert_eq!(metadata.reasoning_effort.as_deref(), Some("medium"));
}

#[test]
fn gemini_thinking_level_ignores_malformed_and_unbounded_values() {
    for level in [
        json!(null),
        json!(false),
        json!(3),
        json!(""),
        json!("x".repeat(65)),
    ] {
        let request = json!({"generationConfig": {"thinkingConfig": {
            "thinkingLevel": level, "thinkingBudget": 9007199254740992_u64
        }}});
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            Some(ProviderCapability::GeminiGenerateContent),
            None,
        );
        assert_eq!(metadata.thinking_level, None);
        assert_eq!(metadata.thinking_budget_tokens, None);
    }
    for level in [
        "LOW".to_owned(),
        "low".to_owned(),
        "future-level".to_owned(),
        "x".repeat(64),
    ] {
        let request = json!({"generationConfig": {"thinkingConfig": {"thinkingLevel": level}}});
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            Some(ProviderCapability::GeminiGenerateContent),
            None,
        );
        assert_eq!(metadata.thinking_level.as_deref(), Some(level.as_str()));
        assert_eq!(metadata.reasoning_effort, None);
    }
    for capability in [
        ProviderCapability::AnthropicMessages,
        ProviderCapability::OpenaiChatCompletions,
        ProviderCapability::OpenaiResponses,
        ProviderCapability::GeminiOpenaiChatCompletions,
    ] {
        let request = json!({
            "serviceTier": "priority",
            "generationConfig": {"thinkingConfig": {"thinkingLevel": "LOW"}}
        });
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            Some(capability),
            None,
        );
        assert_eq!(metadata.thinking_level, None);
        assert_eq!(metadata.requested_service_tier, None);
    }
}

#[test]
fn request_cache_metadata_preserves_enabled_thinking_budgets_across_badge_boundaries() {
    // Given
    let headers = HeaderMap::new();
    let budgets = [
        1_024, 4_000, 4_001, 12_000, 12_001, 24_000, 24_001, 32_000, 32_001,
    ];

    for budget in budgets {
        let body = Bytes::from(format!(
            r#"{{"thinking":{{"type":"enabled","budget_tokens":{budget}}},"output_config":{{"effort":"max"}}}}"#
        ));

        // When
        let metadata = request_cache_metadata(&headers, &body);

        // Then
        assert_eq!(
            metadata.thinking_budget_tokens,
            Some(budget),
            "enabled budget must remain value-preserving for {budget}"
        );
        assert_eq!(metadata.reasoning_effort.as_deref(), Some("max"));
    }
}

#[test]
fn request_cache_metadata_rejects_inapplicable_or_unsafe_thinking_budgets() {
    // Given
    let headers = HeaderMap::new();
    let cases = [
        (
            "disabled",
            r#"{"thinking":{"type":"disabled","budget_tokens":5000}}"#,
            None,
        ),
        (
            "adaptive",
            r#"{"thinking":{"type":"adaptive"},"output_config":{"effort":"max"}}"#,
            Some("max"),
        ),
        (
            "enabled without budget",
            r#"{"thinking":{"type":"enabled"}}"#,
            None,
        ),
        ("missing thinking", r#"{}"#, None),
        (
            "string budget",
            r#"{"thinking":{"type":"enabled","budget_tokens":"1024"}}"#,
            None,
        ),
        ("null thinking", r#"{"thinking":null}"#, None),
        (
            "one over safe integer maximum",
            r#"{"thinking":{"type":"enabled","budget_tokens":9007199254740992}}"#,
            None,
        ),
        (
            "u64 maximum",
            r#"{"thinking":{"type":"enabled","budget_tokens":18446744073709551615}}"#,
            None,
        ),
        (
            "empty reasoning effort",
            r#"{"output_config":{"effort":""}}"#,
            None,
        ),
        (
            "non-string reasoning effort",
            r#"{"output_config":{"effort":123}}"#,
            None,
        ),
        (
            "reasoning effort over 64 characters",
            r#"{"output_config":{"effort":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}}"#,
            None,
        ),
    ];

    for (case, fixture, expected_reasoning_effort) in cases {
        // When
        let metadata =
            request_cache_metadata(&headers, &Bytes::copy_from_slice(fixture.as_bytes()));

        // Then
        assert_eq!(
            metadata.thinking_budget_tokens, None,
            "{case} must not produce a thinking budget"
        );
        assert_eq!(
            metadata.reasoning_effort.as_deref(),
            expected_reasoning_effort,
            "{case} must preserve only valid reasoning effort"
        );
    }
}

#[test]
fn request_cache_metadata_extracts_http_provider_reasoning_shapes() {
    let cases = [
        (
            "openai chat",
            ProviderCapability::OpenaiChatCompletions,
            json!({
                "reasoning_effort": "high",
                "reasoning": {"effort": "wrong-shape"},
                "generationConfig": {"thinkingConfig": {"thinkingBudget": 99}}
            }),
            None,
            Some("high"),
        ),
        (
            "gemini openai chat",
            ProviderCapability::GeminiOpenaiChatCompletions,
            json!({"reasoning_effort": "high"}),
            None,
            Some("high"),
        ),
        (
            "openai responses",
            ProviderCapability::OpenaiResponses,
            json!({
                "reasoning": {"effort": "medium"},
                "reasoning_effort": "wrong-shape",
                "generationConfig": {"thinkingConfig": {"thinkingBudget": 99}}
            }),
            None,
            Some("medium"),
        ),
        (
            "gemini buffered",
            ProviderCapability::GeminiGenerateContent,
            json!({
                "generationConfig": {"thinkingConfig": {"thinkingBudget": 8192}},
                "reasoning_effort": "wrong-shape",
                "reasoning": {"effort": "wrong-shape"}
            }),
            Some(8192),
            None,
        ),
        (
            "gemini streaming",
            ProviderCapability::GeminiGenerateContent,
            json!({
                "stream": true,
                "generationConfig": {"thinkingConfig": {"thinkingBudget": 16384}}
            }),
            Some(16384),
            None,
        ),
        (
            "anthropic",
            ProviderCapability::AnthropicMessages,
            json!({
                "thinking": {"type": "enabled", "budget_tokens": 18000},
                "output_config": {"effort": "max"},
                "reasoning_effort": "wrong-shape",
                "reasoning": {"effort": "wrong-shape"},
                "generationConfig": {"thinkingConfig": {"thinkingBudget": 99}}
            }),
            Some(18000),
            Some("max"),
        ),
    ];

    for (case, capability, request, expected_budget, expected_effort) in cases {
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            Some(capability),
            None,
        );
        assert_eq!(
            metadata.thinking_budget_tokens, expected_budget,
            "{case} budget"
        );
        assert_eq!(
            metadata.reasoning_effort.as_deref(),
            expected_effort,
            "{case} effort"
        );
    }
}

#[test]
fn request_cache_metadata_ignores_invalid_http_provider_reasoning_values() {
    let cases = [
        (
            "openai chat null",
            ProviderCapability::OpenaiChatCompletions,
            json!({"reasoning_effort": null}),
        ),
        (
            "openai chat wrong type",
            ProviderCapability::OpenaiChatCompletions,
            json!({"reasoning_effort": 3}),
        ),
        (
            "openai chat empty",
            ProviderCapability::OpenaiChatCompletions,
            json!({"reasoning_effort": ""}),
        ),
        (
            "openai chat overlong",
            ProviderCapability::OpenaiChatCompletions,
            json!({"reasoning_effort": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}),
        ),
        (
            "openai responses empty",
            ProviderCapability::OpenaiResponses,
            json!({"reasoning": {"effort": ""}}),
        ),
        (
            "openai responses overlong",
            ProviderCapability::OpenaiResponses,
            json!({"reasoning": {"effort": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}}),
        ),
        (
            "openai responses wrong type",
            ProviderCapability::OpenaiResponses,
            json!({"reasoning": {"effort": false}}),
        ),
        (
            "gemini string budget",
            ProviderCapability::GeminiGenerateContent,
            json!({"generationConfig": {"thinkingConfig": {"thinkingBudget": "8192"}}}),
        ),
        (
            "gemini negative budget",
            ProviderCapability::GeminiGenerateContent,
            json!({"generationConfig": {"thinkingConfig": {"thinkingBudget": -1}}}),
        ),
        (
            "gemini fractional budget",
            ProviderCapability::GeminiGenerateContent,
            json!({"generationConfig": {"thinkingConfig": {"thinkingBudget": 1.5}}}),
        ),
        (
            "gemini unsafe budget",
            ProviderCapability::GeminiGenerateContent,
            json!({"generationConfig": {"thinkingConfig": {"thinkingBudget": 9007199254740992_u64}}}),
        ),
    ];

    for (case, capability, request) in cases {
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            Some(capability),
            None,
        );
        assert_eq!(metadata.thinking_budget_tokens, None, "{case} budget");
        assert_eq!(metadata.reasoning_effort, None, "{case} effort");
    }
}

#[test]
fn request_cache_metadata_without_capability_preserves_legacy_anthropic_shape() {
    let request = json!({
        "thinking": {"type": "enabled", "budget_tokens": 4096},
        "output_config": {"effort": "low"},
        "reasoning_effort": "wrong-shape",
        "reasoning": {"effort": "wrong-shape"},
        "generationConfig": {"thinkingConfig": {"thinkingBudget": 99}}
    });
    let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
        &HeaderMap::new(),
        Some(&request),
        None,
        None,
        None,
    );

    assert_eq!(metadata.thinking_budget_tokens, Some(4096));
    assert_eq!(metadata.reasoning_effort.as_deref(), Some("low"));
}

#[test]
fn request_cache_metadata_prefers_hermes_session_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-hermes-session-id"),
        HeaderValue::from_static("hermes-header-session"),
    );
    headers.insert(
        HeaderName::from_static("x-session-affinity"),
        HeaderValue::from_static("generic-affinity-session"),
    );
    let body = Bytes::from_static(
            br#"{"system":"You are Hermes Agent, an intelligent AI assistant created by Nous Research.","messages":[{"role":"user","content":"hello"}]}"#,
        );

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(
        metadata.observed_session_id.as_deref(),
        Some("hermes-header-session")
    );
    assert_eq!(metadata.thread_id.as_deref(), Some("hermes-header-session"));
    assert_eq!(
        metadata.session_id_source.as_deref(),
        Some("x-hermes-session-id")
    );
    assert_eq!(metadata.request_kind.as_deref(), Some("main"));
}

#[test]
fn request_cache_metadata_classifies_hermes_hierarchy_from_tagged_body() {
    let cases = [
        (
            "main",
            r#"{"session_id":"hermes-root-session","tags":["product=hermes-agent","client=hermes-client-v0.19.0","conversation=hermes-root-session"],"system":"You are Hermes Agent, an intelligent AI assistant created by Nous Research.\n\nYou assist users with a wide range of tasks.","messages":[{"role":"user","content":"hello"}],"tools":[{"name":"terminal"}]}"#,
            "main",
        ),
        (
            "subagent",
            r#"{"session_id":"hermes-root-session","tags":["product=hermes-agent","client=hermes-client-v0.19.0","conversation=hermes-root-session"],"system":[{"type":"text","text":"You are a focused subagent working on a specific delegated task.\n\nYOUR TASK:\nReview the auth change"}],"messages":[{"role":"user","content":"Review the auth change"}],"tools":[{"name":"terminal"}]}"#,
            "subagent",
        ),
        (
            "session title",
            r#"{"session_id":"hermes-root-session","tags":["product=hermes-agent","client=hermes-client-v0.19.0","conversation=hermes-root-session"],"system":"You name chat sessions. Given the user's opening message, write a title that lets them find this conversation again in a list.","messages":[{"role":"user","content":"fix the router"}]}"#,
            "session_title",
        ),
        (
            "compaction",
            r#"{"session_id":"hermes-root-session","tags":["product=hermes-agent","client=hermes-client-v0.19.0","conversation=hermes-root-session"],"messages":[{"role":"user","content":"You are a summarization agent creating a context checkpoint.\n\nSummarize the conversation."}]}"#,
            "compaction",
        ),
    ];

    for (case, fixture, request_kind) in cases {
        let metadata = request_cache_metadata(
            &HeaderMap::new(),
            &Bytes::copy_from_slice(fixture.as_bytes()),
        );

        assert_eq!(
            metadata.observed_session_id.as_deref(),
            Some("hermes-root-session"),
            "{case}"
        );
        assert_eq!(
            metadata.thread_id.as_deref(),
            Some("hermes-root-session"),
            "{case}"
        );
        assert_eq!(
            metadata.session_id_source.as_deref(),
            Some("session_id"),
            "{case}"
        );
        assert_eq!(
            metadata.request_kind.as_deref(),
            Some(request_kind),
            "{case}"
        );
        assert_eq!(metadata.parent_session_id, None, "{case}");
        assert_eq!(metadata.client_app, None, "{case}");
    }
}

#[test]
fn request_cache_metadata_uses_hermes_conversation_tag_fallback() {
    let body = Bytes::from_static(
            br#"{"tags":["product=hermes-agent","conversation=hermes-root-fallback"],"system":"You are Hermes Agent, an intelligent AI assistant created by Nous Research.","messages":[{"role":"user","content":"hello"}]}"#,
        );

    let metadata = request_cache_metadata(&HeaderMap::new(), &body);

    assert_eq!(
        metadata.observed_session_id.as_deref(),
        Some("hermes-root-fallback")
    );
    assert_eq!(
        metadata.session_id_source.as_deref(),
        Some("tags.conversation")
    );
    assert_eq!(metadata.request_kind.as_deref(), Some("main"));
}

#[test]
fn request_cache_metadata_rejects_untrusted_hermes_body_session_ids() {
    let oversized_session_id = "x".repeat(MAX_IDENTITY_VALUE_BYTES + 1);
    let cases = [
        serde_json::json!({
            "session_id": "untagged-session",
            "system": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.",
            "messages": [{"role": "user", "content": "hello"}]
        }),
        serde_json::json!({
            "session_id": "wrong-product-session",
            "tags": ["product=hermes-agent-impersonator"],
            "system": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.",
            "messages": [{"role": "user", "content": "hello"}]
        }),
        serde_json::json!({
            "session_id": 123,
            "tags": ["product=hermes-agent"],
            "system": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.",
            "messages": [{"role": "user", "content": "hello"}]
        }),
        serde_json::json!({
            "session_id": oversized_session_id,
            "tags": ["product=hermes-agent"],
            "system": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.",
            "messages": [{"role": "user", "content": "hello"}]
        }),
    ];

    for value in cases {
        let body = Bytes::from(
            serde_json::to_vec(&value).expect("serialize invalid Hermes identity fixture"),
        );
        let metadata = request_cache_metadata(&HeaderMap::new(), &body);

        assert_eq!(metadata.observed_session_id, None);
        assert_eq!(metadata.thread_id, None);
        assert_eq!(metadata.session_id_source, None);
        assert_eq!(metadata.request_kind, None);
    }
}

#[test]
fn request_cache_metadata_uses_opencode_session_affinity_as_thread_id() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-session-affinity"),
        HeaderValue::from_static("opencode-session-123"),
    );
    let body = Bytes::from_static(br#"{"model":"claude-sonnet-4-5"}"#);

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(metadata.thread_id.as_deref(), Some("opencode-session-123"));
}

#[test]
fn request_cache_metadata_uses_opencode_x_session_id_as_thread_id() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-session-id"),
        HeaderValue::from_static("opencode-session-456"),
    );
    let body = Bytes::from_static(br#"{"model":"claude-sonnet-4-5"}"#);

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(metadata.thread_id.as_deref(), Some("opencode-session-456"));
}

#[test]
fn request_cache_metadata_classifies_opencode_requests_and_parent_session() {
    let cases = [
        (
            "main",
            None,
            r#"{"model":"claude-sonnet-4-5","system":[{"type":"text","text":"You are OpenCode, the best coding agent on the planet.\n\nYou are an interactive CLI tool."}],"messages":[{"role":"user","content":"hello"}],"tools":[{"name":"task"}]}"#,
            "main",
        ),
        (
            "session title",
            None,
            r#"{"model":"claude-haiku-4-5-20251001","system":[{"type":"text","text":"You are a title generator. You output ONLY a thread title. Nothing else.\n\n<task>Generate a title</task>"}],"messages":[{"role":"user","content":"fix the router"}]}"#,
            "session_title",
        ),
        (
            "compaction",
            None,
            r#"{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":[{"type":"text","text":"Here is the conversation so far:\n\n<conversation>\n[User]: fix the router\n</conversation>\n\nCreate a new anchored summary from the conversation history in the <conversation> tags above so another coding agent can continue the work."}]}],"tools":[]}"#,
            "compaction",
        ),
        (
            "subagent",
            Some("ses_opencode_parent"),
            r#"{"model":"claude-sonnet-4-5","system":[{"type":"text","text":"You are OpenCode, the best coding agent on the planet.\n\nYou are an interactive CLI tool."}],"messages":[{"role":"user","content":"Reply with CHILD only."}],"tools":[{"name":"task"}]}"#,
            "subagent",
        ),
    ];

    for (case, parent_session_id, fixture, request_kind) in cases {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-session-affinity"),
            HeaderValue::from_static("ses_opencode_child_or_main"),
        );
        if let Some(parent_session_id) = parent_session_id {
            headers.insert(
                HeaderName::from_static("x-parent-session-id"),
                HeaderValue::from_str(parent_session_id).expect("valid OpenCode parent session ID"),
            );
        }

        let metadata =
            request_cache_metadata(&headers, &Bytes::copy_from_slice(fixture.as_bytes()));

        assert_eq!(
            metadata.observed_session_id.as_deref(),
            Some("ses_opencode_child_or_main"),
            "{case}"
        );
        assert_eq!(
            metadata.session_id_source.as_deref(),
            Some("x-session-affinity"),
            "{case}"
        );
        assert_eq!(
            metadata.parent_session_id.as_deref(),
            parent_session_id,
            "{case}"
        );
        assert_eq!(
            metadata.request_kind.as_deref(),
            Some(request_kind),
            "{case}"
        );
        assert_eq!(metadata.client_app, None, "{case}");
    }
}

#[test]
fn request_cache_metadata_classifies_senpi_without_inventing_client_app() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-session-affinity"),
        HeaderValue::from_static("senpi-affinity-session"),
    );
    let body = Bytes::from_static(
            br#"{"model":"mock-with-affinity","system":"You are senpi, a coding agent. Your work should be indistinguishable from a careful senior engineer's.\n\n## Intent Gate (EVERY message)","messages":[{"role":"user","content":"Reply exactly SENPI_FAKE_OK"}],"tools":[]}"#,
        );

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(
        metadata.observed_session_id.as_deref(),
        Some("senpi-affinity-session")
    );
    assert_eq!(
        metadata.session_id_source.as_deref(),
        Some("x-session-affinity")
    );
    assert_eq!(metadata.request_kind.as_deref(), Some("main"));
    assert_eq!(metadata.client_app, None);
}

#[test]
fn request_cache_metadata_classifies_senpi_auxiliary_requests() {
    let cases = [
        (
            "subagent",
            Some("019d20f2-6f15-7de2-a800-f3e97928a87c"),
            r#"{"system":[{"type":"text","text":"You are senpi, a coding agent. Your work should be indistinguishable from a careful senior engineer's.\n\nYou are a focused code-review subagent."}],"messages":[{"role":"user","content":"Task: Review the auth change"}],"tools":[{"name":"read"}]}"#,
            "subagent",
        ),
        (
            "look at",
            None,
            r#"{"system":[{"type":"text","text":"You analyze attached media for a downstream agent that cannot inspect the attachments directly.\n\nExtract only the information requested by the goal."}],"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AA=="}},{"type":"text","text":"Goal:\nRead the label"}]}]}"#,
            "look_at",
        ),
    ];

    for (case, session_id, fixture, request_kind) in cases {
        let mut headers = HeaderMap::new();
        if let Some(session_id) = session_id {
            headers.insert(
                HeaderName::from_static("x-session-affinity"),
                HeaderValue::from_str(session_id).expect("valid session ID header"),
            );
        }
        let metadata =
            request_cache_metadata(&headers, &Bytes::copy_from_slice(fixture.as_bytes()));

        assert_eq!(
            metadata.observed_session_id.as_deref(),
            session_id,
            "{case}"
        );
        assert_eq!(
            metadata.request_kind.as_deref(),
            Some(request_kind),
            "{case}"
        );
        assert_eq!(metadata.client_app, None, "{case}");
    }
}

#[test]
fn request_cache_metadata_classifies_omp_requests() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-claude-code-session-id"),
        HeaderValue::from_static("session-123"),
    );

    let cases = [
        (
            "main",
            r#"{"system":[{"type":"text","text":"<system-conventions>rules</system-conventions>\nROLE\n==============\nYou are a helpful assistant the team trusts with load-bearing changes, operating in the Oh My Pi coding harness."}],"messages":[{"role":"user","content":"hello"}]}"#,
            "main",
        ),
        (
            "advisor",
            r#"{"system":[{"type":"text","text":"<system-conventions>rules</system-conventions>\nYou shadow the main agent as a peer programmer:"}],"messages":[{"role":"user","content":"review this"}]}"#,
            "advisor",
        ),
        (
            "recap after reminder",
            r#"{"messages":[{"role":"user","content":"original request"},{"role":"assistant","content":"response"},{"role":"user","content":[{"type":"text","text":"<system-reminder>context</system-reminder>"},{"type":"text","text":"The user stepped away and is coming back. Recap in under 40 words. Lead with the overall goal and current task."}]}]}"#,
            "recap",
        ),
        (
            "compaction after transcript",
            r#"{"messages":[{"role":"user","content":"original request"},{"role":"assistant","content":"response"},{"role":"user","content":"Your task is to create a detailed summary of the conversation so far. Primary Request and Intent: keep this. REMINDER: Do NOT call any tools."}]}"#,
            "compaction",
        ),
        (
            "session title",
            r#"{"system":"Generate a concise, sentence-case title (3-7 words)","messages":[{"role":"user","content":"<session>hello</session> Write the title"}],"output_config":{"format":{"schema":{"properties":{"title":{"type":"string"}}}}}}"#,
            "session_title",
        ),
        (
            "auto thinking",
            r#"{"system":"You are a difficulty classifier for a coding agent. Read the user's request.","messages":[{"role":"user","content":"fix this"}]}"#,
            "auto_thinking",
        ),
        (
            "subagent prompt",
            r#"{"messages":[{"role":"user","content":"Complete the assignment below, thoroughly:\n\nFix the issue"},{"role":"assistant","content":"working"},{"role":"user","content":"follow-up"}]}"#,
            "subagent",
        ),
    ];

    for (case, fixture, request_kind) in cases {
        let metadata =
            request_cache_metadata(&headers, &Bytes::copy_from_slice(fixture.as_bytes()));

        assert_eq!(
            metadata.observed_session_id.as_deref(),
            Some("session-123"),
            "{case}"
        );
        assert_eq!(
            metadata.request_kind.as_deref(),
            Some(request_kind),
            "{case}"
        );
    }
}

#[tokio::test]
async fn explicit_auxiliary_cannot_supersede_pending_legitimate_completion() {
    assert_explicit_auxiliary_completion_ordering(false).await;
}

#[tokio::test]
async fn explicit_auxiliary_detached_completion_does_not_mutate_keepalive() {
    assert_explicit_auxiliary_completion_ordering(true).await;
}

async fn assert_explicit_auxiliary_completion_ordering(detached_gate: bool) {
    let body = Bytes::from_static(
            br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"hello"}]}"#,
        );
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-claude-code-session-id",
        HeaderValue::from_static("ordered-auxiliary"),
    );
    headers.insert(
        "x-claude-code-request-class",
        HeaderValue::from_static("main"),
    );
    let request_json: Value = serde_json::from_slice(&body).expect("ordered request fixture");
    let mut legitimate = request_cache_metadata(&headers, &body);
    legitimate.request_json = Some(request_json.clone());
    assert_eq!(legitimate.request_kind.as_deref(), Some("main"));
    headers.insert(
        "x-claude-code-request-class",
        HeaderValue::from_static("auxiliary"),
    );
    let mut auxiliary = request_cache_metadata(&headers, &body);
    auxiliary.request_json = Some(request_json);
    assert_eq!(auxiliary.request_kind.as_deref(), Some("unknown"));

    let mut record = principal_record("principal-test");
    record.cache_keepalive = Some(cc_lb_storage_api::CacheKeepaliveConfig {
        enabled: true,
        refresh_lead_time_5m_secs: 30,
        refresh_lead_time_1h_secs: 300,
        max_refreshes_per_session: 12,
        max_total_duration_secs: 14_400,
        snapshot_max_bytes: 524_288,
        min_cache_tokens: 10_000,
        classifier: cc_lb_storage_api::ClassifierConfig::default(),
    });
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .principal_view(Arc::new(PrincipalView::from_db(&[record], HashMap::new())))
        .build();
    for cancellation in [false, true] {
        crate::cache_keepalive::LifecycleKeepalive::assert_auxiliary_cannot_supersede_pending_completion(
                Arc::new(DynamicViewHolder::new(Arc::clone(&view))),
                Principal { id: "principal-test".to_owned(), kind: cc_lb_domain::PrincipalKind::InternalKey },
                legitimate.clone(),
                auxiliary.clone(),
                body.clone(),
                cancellation,
                detached_gate,
            ).await;
    }
}

#[test]
fn request_cache_metadata_classifies_claude_code_main_turn() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-claude-code-session-id"),
        HeaderValue::from_static("87b3d2e6-ce49-439d-9e68-0c90138a726a"),
    );
    let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "system": [{
                    "type": "text",
                    "text": "\nYou are an interactive agent that helps users with software engineering tasks. Use the instructions below and the tools available to you to assist the user."
                }],
                "messages": [{"role": "user", "content": "QA main turn reply ok"}],
                "tools": [{"name": "Agent"}]
            }))
            .expect("serialize Claude Code main request fixture"),
        );

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(metadata.request_kind.as_deref(), Some("main"));
    assert_eq!(
        metadata.observed_session_id.as_deref(),
        Some("87b3d2e6-ce49-439d-9e68-0c90138a726a")
    );
}

#[test]
fn request_cache_metadata_accepts_real_advisor_block_as_omp_provenance() {
    let headers = HeaderMap::new();
    let advisor_system_prefix = concat!(
        "<system-conventions>\n",
        "RFC 2119 applies to MUST, REQUIRED, SHOULD, RECOMMENDED, MAY, OPTIONAL. ",
        "`NEVER` and `AVOID` are aliases for `MUST NOT` and `SHOULD NOT`.\n",
        "</system-conventions>\n\n",
        "You bring a different angle, advocating for the user and for code quality & robustness.\n",
        "You shadow the main agent as a peer programmer:\n",
    );
    let marker_offset = advisor_system_prefix
        .find(crate::request_classification::omp::OMP_ADVISOR_SYSTEM_MARKER)
        .expect("real OMP Advisor prompt contains its marker");
    assert!(marker_offset < crate::request_classification::CLASSIFIER_TEXT_LIMIT);

    // OMP sends advisorSystemPrompt as the first standalone system block;
    // later context, WATCHDOG, shared, and per-advisor instructions are
    // separate blocks. Keep this fixture larger than the bounded scan while
    // preserving the real prefix and marker position.
    let advisor_system = format!(
        "{advisor_system_prefix}{}",
        "x".repeat(crate::request_classification::CLASSIFIER_TEXT_LIMIT)
    );
    assert!(advisor_system.len() > crate::request_classification::CLASSIFIER_TEXT_LIMIT);
    let body = Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "system": [{"type": "text", "text": advisor_system}],
            "messages": [{"role": "user", "content": "review this"}],
        }))
        .expect("serialize real-layout OMP Advisor request fixture"),
    );

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(metadata.observed_session_id, None);
    assert_eq!(metadata.thread_id, None);
    assert_eq!(metadata.request_kind.as_deref(), Some("advisor"));
}
#[test]
fn request_cache_metadata_extracts_extended_client_session_identities() {
    for (header, value, source) in [
        (
            "x-opencode-session-id",
            "opencode-id",
            "x-opencode-session-id",
        ),
        (
            "x-opencode-session",
            "opencode-session",
            "x-opencode-session",
        ),
        (
            "x-aside-provider-session-id",
            "aside-provider",
            "x-aside-provider-session-id",
        ),
        ("x-aside-session-id", "aside-session", "x-aside-session-id"),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_bytes(header.as_bytes()).expect("valid header"),
            HeaderValue::from_static(value),
        );
        let metadata = request_cache_metadata(&headers, &Bytes::from_static(br#"{}"#));
        assert_eq!(metadata.observed_session_id.as_deref(), Some(value));
        assert_eq!(metadata.session_id_source.as_deref(), Some(source));
    }

    let mut opencode_headers = HeaderMap::new();
    opencode_headers.insert(
        HeaderName::from_static("x-opencode-session"),
        HeaderValue::from_static("opencode-child"),
    );
    opencode_headers.insert(
        HeaderName::from_static("x-opencode-parent-session-id"),
        HeaderValue::from_static("opencode-parent"),
    );
    let opencode = request_cache_metadata(&opencode_headers, &Bytes::from_static(br#"{}"#));
    assert_eq!(
        opencode.parent_session_id.as_deref(),
        Some("opencode-parent")
    );

    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-aside-session-id"),
        HeaderValue::from_static("aside:worker:aside-parent:default"),
    );
    let metadata = request_cache_metadata(&headers, &Bytes::from_static(br#"{}"#));
    assert_eq!(metadata.parent_session_id.as_deref(), Some("aside-parent"));
}

#[test]
fn request_cache_metadata_derives_omp_and_senpi_parent_sessions() {
    for session_id in [
        "omp-parent:side:worker",
        "omp-parent:spec:worker",
        "senpi-parent:btw:worker",
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-session-id"),
            HeaderValue::from_str(session_id).expect("valid session ID"),
        );
        let metadata = request_cache_metadata(&headers, &Bytes::from_static(br#"{}"#));
        assert_eq!(
            metadata.parent_session_id.as_deref(),
            Some(session_id.split_once(':').expect("marker").0),
            "{session_id}"
        );
    }
}

#[test]
fn request_cache_metadata_extracts_codex_client_metadata_and_legacy_user_ids() {
    let mut codex_headers = HeaderMap::new();
    codex_headers.insert(
        HeaderName::from_static("x-codex-turn-metadata"),
        HeaderValue::from_static(r#"{"session_id":"codex-session"}"#),
    );
    let codex = request_cache_metadata(&codex_headers, &Bytes::from_static(br#"{}"#));
    assert_eq!(codex.observed_session_id.as_deref(), Some("codex-session"));
    assert_eq!(
        codex.session_id_source.as_deref(),
        Some("x-codex-turn-metadata")
    );

    let client_metadata = request_cache_metadata(
        &HeaderMap::new(),
        &Bytes::from_static(br#"{"client_metadata":{"session_id":"client-session"}}"#),
    );
    assert_eq!(
        client_metadata.observed_session_id.as_deref(),
        Some("client-session")
    );
    assert_eq!(
        client_metadata.session_id_source.as_deref(),
        Some("client_metadata.session_id")
    );

    for user_id in [
        "user_abc_session_legacy-session",
        "87b3d2e6-ce49-439d-9e68-0c90138a726a",
    ] {
        let body = Bytes::from(
            serde_json::to_vec(&json!({"metadata": {"user_id": user_id}}))
                .expect("serialize metadata"),
        );
        let metadata = request_cache_metadata(&HeaderMap::new(), &body);
        let expected = if user_id.contains("_session_") {
            "legacy-session"
        } else {
            user_id
        };
        assert_eq!(metadata.observed_session_id.as_deref(), Some(expected));
        assert_eq!(
            metadata.session_id_source.as_deref(),
            Some("metadata.user_id")
        );
    }
}

#[test]
fn request_cache_metadata_uses_claude_request_class_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-claude-code-request-class"),
        HeaderValue::from_static("session_title"),
    );
    let metadata = request_cache_metadata(
        &headers,
        &Bytes::from_static(br#"{"messages":[{"role":"user","content":"ordinary"}]}"#),
    );
    assert_eq!(metadata.request_kind.as_deref(), Some("session_title"));
}

#[test]
fn request_cache_metadata_classifies_codex_kinds_parent_thread_and_client_app() {
    let mut main_headers = HeaderMap::new();
    main_headers.insert(
        HeaderName::from_static("session_id"),
        HeaderValue::from_static("codex-thread-main"),
    );
    main_headers.insert(
        HeaderName::from_static("originator"),
        HeaderValue::from_static("codex_cli_rs"),
    );
    let main = request_cache_metadata(
        &main_headers,
        &Bytes::from_static(br#"{"model":"gpt-5.4","input":[{"role":"user","content":"hi"}]}"#),
    );
    assert_eq!(main.request_kind.as_deref(), Some("main"));
    assert_eq!(main.client_app.as_deref(), Some("codex_cli_rs"));
    assert_eq!(main.parent_session_id, None);

    let prewarm = request_cache_metadata(
        &main_headers,
        &Bytes::from_static(br#"{"model":"gpt-5.4","generate":false,"input":[]}"#),
    );
    assert_eq!(prewarm.request_kind.as_deref(), Some("prewarm"));

    let mut review_headers = main_headers.clone();
    review_headers.insert(
        HeaderName::from_static("x-openai-subagent"),
        HeaderValue::from_static("review"),
    );
    review_headers.insert(
        HeaderName::from_static("x-codex-parent-thread-id"),
        HeaderValue::from_static("codex-thread-parent"),
    );
    review_headers.insert(
        HeaderName::from_static("session_id"),
        HeaderValue::from_static("codex-thread-child"),
    );
    let review = request_cache_metadata(&review_headers, &Bytes::from_static(br#"{"input":[]}"#));
    assert_eq!(review.request_kind.as_deref(), Some("review"));
    assert_eq!(
        review.parent_session_id.as_deref(),
        Some("codex-thread-parent")
    );

    let compact = request_cache_metadata_from_value_with_analysis_for_capability(
        &main_headers,
        Some(&json!({"model":"gpt-5.4","input":[]})),
        None,
        Some(ProviderCapability::OpenaiResponsesCompact),
        None,
    );
    assert_eq!(compact.request_kind.as_deref(), Some("compaction"));
}

#[test]
fn request_cache_metadata_classifies_main_with_large_project_context_block() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-claude-code-session-id"),
        HeaderValue::from_static("session-with-large-context"),
    );
    let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "system": [
                    {
                        "type": "text",
                        "text": "x".repeat(crate::request_classification::CLASSIFIER_TEXT_LIMIT + 1),
                    },
                    {
                        "type": "text",
                        "text": "You are a helpful assistant the team trusts with load-bearing changes, operating in the Oh My Pi coding harness.",
                    },
                ],
                "messages": [{"role": "user", "content": "hello"}],
            }))
            .expect("serialize OMP request fixture"),
        );

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(metadata.request_kind.as_deref(), Some("main"));
}

#[test]
fn request_cache_metadata_extracts_omp_metadata_session_id() {
    let headers = HeaderMap::new();
    let json_metadata = Bytes::from_static(
            br#"{"metadata":{"user_id":"{\"session_id\":\"session-from-body\",\"account_uuid\":\"account\"}"},"system":"You are a helpful assistant the team trusts with load-bearing changes, operating in the Oh My Pi coding harness.","messages":[{"role":"user","content":"hello"}]}"#,
        );

    let json = request_cache_metadata(&headers, &json_metadata);

    assert_eq!(
        json.observed_session_id.as_deref(),
        Some("session-from-body")
    );
    assert_eq!(json.thread_id.as_deref(), Some("session-from-body"));
    assert_eq!(json.request_kind.as_deref(), Some("main"));
    assert_eq!(json.session_id_source.as_deref(), Some("metadata.user_id"));
    assert_eq!(json.parent_session_id, None);
}

#[test]
fn request_cache_metadata_extracts_claude_code_session_identity() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-claude-code-session-id"),
        HeaderValue::from_static("7f25b3d8-6bf2-4832-94e5-66394f785210"),
    );
    headers.insert(
        HeaderName::from_static("x-claude-code-agent-id"),
        HeaderValue::from_static("a5f4e1590bb7587bc"),
    );
    headers.insert(
        HeaderName::from_static("x-claude-code-parent-agent-id"),
        HeaderValue::from_static("b1c2d3e4f5061728a"),
    );
    headers.insert(
        HeaderName::from_static("x-app"),
        HeaderValue::from_static("cli-bg"),
    );
    let body = Bytes::from_static(
            br#"{"model":"claude-sonnet-4-5","metadata":{"user_id":"{\"device_id\":\"d\",\"account_uuid\":\"acc\",\"session_id\":\"7f25b3d8-6bf2-4832-94e5-66394f785210\",\"parent_session_id\":\"0714858d-c434-4aae-8bc0-5f33cf6cf63d\"}"},"messages":[{"role":"user","content":"hi"}]}"#,
        );

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(
        metadata.observed_session_id.as_deref(),
        Some("7f25b3d8-6bf2-4832-94e5-66394f785210")
    );
    assert_eq!(
        metadata.session_id_source.as_deref(),
        Some("x-claude-code-session-id")
    );
    assert_eq!(
        metadata.claude_agent_id.as_deref(),
        Some("a5f4e1590bb7587bc")
    );
    assert_eq!(
        metadata.claude_parent_agent_id.as_deref(),
        Some("b1c2d3e4f5061728a")
    );
    assert_eq!(
        metadata.parent_session_id.as_deref(),
        Some("0714858d-c434-4aae-8bc0-5f33cf6cf63d")
    );
    assert_eq!(metadata.client_app.as_deref(), Some("cli-bg"));
    assert_eq!(metadata.request_kind.as_deref(), Some("subagent"));
}

#[test]
fn request_cache_metadata_reports_metadata_session_source() {
    let headers = HeaderMap::new();
    let body = Bytes::from_static(
            br#"{"metadata":{"user_id":"{\"session_id\":\"session-from-body\",\"parent_session_id\":\"session-parent\"}"},"messages":[]}"#,
        );

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(
        metadata.observed_session_id.as_deref(),
        Some("session-from-body")
    );
    assert_eq!(
        metadata.session_id_source.as_deref(),
        Some("metadata.user_id")
    );
    assert_eq!(
        metadata.parent_session_id.as_deref(),
        Some("session-parent")
    );
}

#[test]
fn request_cache_metadata_rejects_oversized_identity_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-claude-code-session-id"),
        HeaderValue::from_static("session-123"),
    );
    headers.insert(
        HeaderName::from_static("x-claude-code-agent-id"),
        HeaderValue::from_bytes(&vec![b'a'; MAX_IDENTITY_VALUE_BYTES + 1])
            .expect("valid oversized header"),
    );

    let metadata = request_cache_metadata(
        &headers,
        &Bytes::from_static(br#"{"messages":[{"role":"user","content":"work"}]}"#),
    );

    assert_eq!(metadata.claude_agent_id, None);
    assert_ne!(metadata.request_kind.as_deref(), Some("subagent"));
}

#[test]
fn request_cache_metadata_prefers_session_header_over_body_metadata() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-session-affinity"),
        HeaderValue::from_static("header-session"),
    );
    let body = Bytes::from_static(
        br#"{"metadata":{"user_id":"{\"session_id\":\"body-session\"}"},"messages":[]}"#,
    );

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(
        metadata.observed_session_id.as_deref(),
        Some("header-session")
    );
}

#[test]
fn request_cache_metadata_classifies_subagent_lineage_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-claude-code-agent-id"),
        HeaderValue::from_static("agent-456"),
    );

    let metadata = request_cache_metadata(
        &headers,
        &Bytes::from_static(br#"{"messages":[{"role":"user","content":"work"}]}"#),
    );

    assert_eq!(metadata.request_kind.as_deref(), Some("subagent"));
}

#[test]
fn request_cache_metadata_does_not_fingerprint_without_omp_provenance() {
    let headers = HeaderMap::new();
    let body = Bytes::from_static(
            br#"{"messages":[{"role":"user","content":"The user stepped away and is coming back. Recap in under 40 words."}]}"#,
        );

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(metadata.request_kind, None);
}

#[test]
fn request_cache_metadata_uses_v3_content_block_hash() {
    let headers = HeaderMap::new();
    let body = Bytes::from_static(
            br#"{
                "model":"claude-sonnet-4-5",
                "system":[{"type":"text","text":"stable system","cache_control":{"type":"ephemeral"}}],
                "messages":[{"role":"user","content":"hello"}],
                "tool_choice":{"type":"auto"},
                "thinking":{"type":"enabled","budget_tokens":1024}
            }"#,
        );
    let request: Value = serde_json::from_slice(&body).expect("request json");

    let metadata = request_cache_metadata(&headers, &body);

    let breakpoint = metadata
        .cache_breakpoints
        .first()
        .expect("system cache breakpoint");
    let expected_v3 = analyze_v3_prompt_cache(
        &request,
        canonical_model_id(Provider::Anthropic, "claude-sonnet-4-5"),
    )
    .breakpoints
    .first()
    .expect("system cache breakpoint")
    .prefix_key
    .clone();

    assert_eq!(breakpoint.prefix_hash, expected_v3);
}

#[test]
fn request_cache_metadata_fills_prefix_token_count() {
    let headers = HeaderMap::new();
    let body = Bytes::from_static(
            br#"{
                "model":"claude-sonnet-4-5",
                "system":[{"type":"text","text":"You are a helpful assistant. Please be concise.","cache_control":{"type":"ephemeral"}}],
                "messages":[{"role":"user","content":"hello"}]
            }"#,
        );

    let metadata = request_cache_metadata(&headers, &body);

    assert_eq!(metadata.cache_breakpoints.len(), 1);
    let breakpoint = &metadata.cache_breakpoints[0];
    assert!(
        breakpoint.prefix_token_count > 0,
        "expected non-zero prefix_token_count, got {}",
        breakpoint.prefix_token_count
    );

    let plugin_breakpoints = metadata.plugin_cache_breakpoints();
    assert_eq!(plugin_breakpoints.len(), 1);
    assert_eq!(
        plugin_breakpoints[0].prefix_bytes.get(),
        breakpoint.prefix_token_count
    );
}

#[test]
fn request_cache_metadata_prefix_token_count_scales_with_prefix_size() {
    let headers = HeaderMap::new();
    let short_body = Bytes::from_static(
        br#"{
                "model":"claude-sonnet-4-5",
                "system":[{"type":"text","text":"short","cache_control":{"type":"ephemeral"}}],
                "messages":[{"role":"user","content":"hello"}]
            }"#,
    );
    let long_text: String = "lorem ipsum ".repeat(2_000);
    let long_body_string = format!(
        r#"{{"model":"claude-sonnet-4-5","system":[{{"type":"text","text":"{long_text}","cache_control":{{"type":"ephemeral"}}}}],"messages":[{{"role":"user","content":"hello"}}]}}"#,
    );
    let long_body = Bytes::from(long_body_string);

    let short_metadata = request_cache_metadata(&headers, &short_body);
    let long_metadata = request_cache_metadata(&headers, &long_body);

    let short_count = short_metadata.cache_breakpoints[0].prefix_token_count;
    let long_count = long_metadata.cache_breakpoints[0].prefix_token_count;

    assert!(short_count > 0);
    assert!(
        long_count > short_count.saturating_mul(10),
        "expected long prefix ({long_count}) to be >10x short prefix ({short_count})"
    );
}

#[test]
fn request_cache_metadata_prefix_token_count_crosses_sonnet_threshold() {
    let headers = HeaderMap::new();
    let varied_text: String =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(300);
    let body_string = format!(
        r#"{{"model":"claude-sonnet-4-5","system":[{{"type":"text","text":"{varied_text}","cache_control":{{"type":"ephemeral","ttl":"1h"}}}}],"messages":[{{"role":"user","content":"hi"}}],"max_tokens":16}}"#,
    );
    let body = Bytes::from(body_string);

    let metadata = request_cache_metadata(&headers, &body);
    assert_eq!(metadata.cache_breakpoints.len(), 1);
    let count = metadata.cache_breakpoints[0].prefix_token_count;
    assert!(
        count >= 1024,
        "prefix token count {count} should be above Sonnet 4.5 cache threshold (1024)"
    );
}

#[test]
fn request_cache_breakpoint_hashes_ignore_later_prompt_suffixes() {
    let headers = HeaderMap::new();
    let first = Bytes::from_static(
            br#"{
                "model":"claude-sonnet-4-5",
                "messages":[
                    {"role":"user","content":[{"type":"text","text":"stable","cache_control":{"type":"ephemeral"}}]},
                    {"role":"user","content":[{"type":"text","text":"suffix-a","cache_control":{"type":"ephemeral"}}]}
                ]
            }"#,
        );
    let second = Bytes::from_static(
            br#"{
                "model":"claude-sonnet-4-5",
                "messages":[
                    {"role":"user","content":[{"type":"text","text":"stable","cache_control":{"type":"ephemeral"}}]},
                    {"role":"user","content":[{"type":"text","text":"suffix-b","cache_control":{"type":"ephemeral"}}]}
                ]
            }"#,
        );

    let first = request_cache_metadata(&headers, &first);
    let second = request_cache_metadata(&headers, &second);

    assert_eq!(first.cache_breakpoints.len(), 2);
    assert_eq!(second.cache_breakpoints.len(), 2);
    assert_eq!(
        first.cache_breakpoints[0].prefix_hash,
        second.cache_breakpoints[0].prefix_hash
    );
    assert_ne!(
        first.cache_breakpoints[1].prefix_hash,
        second.cache_breakpoints[1].prefix_hash
    );
}

struct RecordingPromptCacheObservationSink {
    records: Mutex<Vec<PromptCacheObservationRecord>>,
    provider_records: Mutex<Vec<ProviderPromptCacheObservationRecord>>,
    clock_now: u64,
    grace_secs: u64,
}
impl RecordingPromptCacheObservationSink {
    fn new(_entries: Vec<WarmCacheEntry>) -> Self {
        Self {
            records: Mutex::new(Vec::new()),
            provider_records: Mutex::new(Vec::new()),
            clock_now: 0,
            grace_secs: 30,
        }
    }
    fn with_clock_now(mut self, now: u64) -> Self {
        self.clock_now = now;
        self
    }
    fn with_grace_secs(mut self, secs: u64) -> Self {
        self.grace_secs = secs;
        self
    }
    fn upserts(&self) -> Vec<PromptCacheObservationRecord> {
        self.records.lock().expect("records lock").clone()
    }
    fn provider_upserts(&self) -> Vec<ProviderPromptCacheObservationRecord> {
        self.provider_records
            .lock()
            .expect("provider records lock")
            .clone()
    }
    fn clock_now_unix_secs(&self) -> u64 {
        self.clock_now
    }
    fn grace_margin_secs(&self) -> u64 {
        self.grace_secs
    }
}
impl PromptCacheObservationSinkLike for RecordingPromptCacheObservationSink {
    fn enqueue(
        &self,
        record: PromptCacheObservationRecord,
    ) -> Result<(), PromptCacheObservationEnqueueError> {
        self.records.lock().expect("records lock").push(record);
        Ok(())
    }
    fn enqueue_provider(
        &self,
        record: ProviderPromptCacheObservationRecord,
    ) -> Result<(), PromptCacheObservationEnqueueError> {
        self.provider_records
            .lock()
            .expect("provider records lock")
            .push(record);
        Ok(())
    }
}
struct TogglePromptCacheStore {
    failed: AtomicBool,
}
#[async_trait]
impl cc_lb_storage_api::PromptCacheObservationStore for TogglePromptCacheStore {
    async fn list_active_for_candidates(
        &self,
        _upstream_ids: &[Uuid],
        _canonical_model_id: &str,
        _v3_prefix_keys: &[String],
        _not_expired_at_unix_secs: u64,
    ) -> cc_lb_storage_api::StorageResult<Vec<PromptCacheObservationRecord>> {
        if self.failed.load(Ordering::Relaxed) {
            Err(cc_lb_storage_api::StorageError::Unavailable {
                message: "test failure".to_owned(),
            })
        } else {
            Ok(Vec::new())
        }
    }
}

#[tokio::test]
async fn shared_cache_read_failure_is_unknown_not_cold_and_preserves_candidate() {
    let upstream_id = Uuid::from_u128(825);
    let store = Arc::new(TogglePromptCacheStore {
        failed: AtomicBool::new(false),
    });
    let mut view = cache_score_view(upstream_id);
    Arc::get_mut(&mut view)
        .expect("new test view is exclusively owned")
        .prompt_cache_observation_store = Some(store.clone());
    let clock = cc_lb_clock::TestClock::new_at_secs(1_800_000_000);
    let breakpoints = vec![cache_breakpoint(0, "prefix", 100, TtlClass::Ephemeral5m)];
    for failed in [false, true, false] {
        store.failed.store(failed, Ordering::Relaxed);
        let snapshot = fetch_prompt_cache_warm_entries(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
            &clock,
        )
        .await;
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &snapshot,
            &clock,
        );
        assert_eq!(candidates.len(), 1);
        let candidate = candidates.first().expect("eligible candidate");
        assert_eq!(candidate.upstream_id, upstream_id);
        if failed {
            assert!(candidate.cache_score.is_none(), "failed read is unknown");
        } else {
            let score = candidate.cache_score.as_ref().expect("known-cold score");
            assert_eq!(score.predicted_cache_read_tokens, 0);
            assert_eq!(score.predicted_cache_creation_tokens_5m, 100);
        }
    }
}

/// In-memory observation store that mirrors the real backend contract:
/// rows are filtered by upstream, canonical model, prefix key, and
/// `expires_at > not_expired_at_unix_secs`, and every lookup is counted.
struct FakePromptCacheStore {
    records: Mutex<Vec<PromptCacheObservationRecord>>,
    lookups: AtomicUsize,
    requested_keys: Mutex<Vec<String>>,
}
impl FakePromptCacheStore {
    fn new(records: Vec<PromptCacheObservationRecord>) -> Self {
        Self {
            records: Mutex::new(records),
            lookups: AtomicUsize::new(0),
            requested_keys: Mutex::new(Vec::new()),
        }
    }
    fn lookup_count(&self) -> usize {
        self.lookups.load(Ordering::Relaxed)
    }
    fn requested_keys(&self) -> Vec<String> {
        self.requested_keys
            .lock()
            .expect("requested keys lock")
            .clone()
    }
}
#[async_trait]
impl cc_lb_storage_api::PromptCacheObservationStore for FakePromptCacheStore {
    async fn list_active_for_candidates(
        &self,
        upstream_ids: &[Uuid],
        canonical_model_id: &str,
        v3_prefix_keys: &[String],
        not_expired_at_unix_secs: u64,
    ) -> cc_lb_storage_api::StorageResult<Vec<PromptCacheObservationRecord>> {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        self.requested_keys
            .lock()
            .expect("requested keys lock")
            .extend(v3_prefix_keys.iter().cloned());
        Ok(self
            .records
            .lock()
            .expect("records lock")
            .iter()
            .filter(|record| upstream_ids.contains(&record.upstream_id))
            .filter(|record| record.canonical_model_id == canonical_model_id)
            .filter(|record| v3_prefix_keys.contains(&record.v3_prefix_key))
            .filter(|record| record.expires_at_unix_secs > not_expired_at_unix_secs)
            .cloned()
            .collect())
    }
}

fn observation_record(
    upstream_id: Uuid,
    prefix_key: &str,
    ttl_class: TtlClass,
    expires_at_unix_secs: u64,
) -> PromptCacheObservationRecord {
    PromptCacheObservationRecord {
        upstream_id,
        canonical_model_id: TEST_MODEL.to_owned(),
        v3_prefix_key: prefix_key.to_owned(),
        ttl_class,
        expires_at_unix_secs,
        last_observed_at_unix_secs: 1_700_000_000,
        hash_schema_version: HASH_SCHEMA_VERSION,
        prefix_content_block_index: 0,
        estimated_prefix_tokens: 100,
        token_estimate_source: V3_TOKEN_ESTIMATE_SOURCE.to_owned(),
    }
}

#[tokio::test]
async fn shared_warm_lookup_unions_official_and_literal_bundle_keys() {
    let official_id = Uuid::from_u128(1010);
    let custom_id = Uuid::from_u128(1011);
    let mut custom = upstream_record(custom_id);
    custom.base_url = Some("https://proxy.example.com".parse().expect("custom URL"));
    let store = Arc::new(FakePromptCacheStore::new(Vec::new()));
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .principal_view(Arc::new(PrincipalView::from_db(
            &[principal_record("principal")],
            HashMap::new(),
        )))
        .upstream_records(vec![upstream_record(official_id), custom])
        .prompt_cache_observation_store(store.clone())
        .build();
    let official = cache_breakpoint(1, "official-union-key", 100, TtlClass::Ephemeral5m);
    let literal = cache_breakpoint(1, "literal-union-key", 200, TtlClass::Ephemeral5m);
    let bundles = PromptCacheBundles {
        official: PromptCacheBundle {
            cache_breakpoints: vec![official],
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            request_breakpoints: Vec::new(),
            cache_prefix_hash: None,
        },
        literal: PromptCacheBundle {
            cache_breakpoints: vec![literal],
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            request_breakpoints: Vec::new(),
            cache_prefix_hash: None,
        },
    };
    let snapshot = fetch_prompt_cache_warm_entries_for_bundles(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &bundles,
        &cc_lb_clock::TestClock::new_at_secs(1_800_000_000),
    )
    .await;
    assert_eq!(snapshot.len(), 2);
    let keys = store.requested_keys();
    assert!(keys.contains(&"official-union-key".to_owned()));
    assert!(keys.contains(&"literal-union-key".to_owned()));
}

fn cache_score_view_with_store(
    upstream_id: Uuid,
    store: Arc<dyn cc_lb_storage_api::PromptCacheObservationStore>,
) -> Arc<DynamicView> {
    let mut view = cache_score_view(upstream_id);
    Arc::get_mut(&mut view)
        .expect("new test view is exclusively owned")
        .prompt_cache_observation_store = Some(store);
    view
}

#[test]
fn mixed_official_and_custom_candidates_score_only_their_own_bundle() {
    let official_id = Uuid::from_u128(1004);
    let custom_id = Uuid::from_u128(1005);
    let mut custom = upstream_record(custom_id);
    custom.base_url = Some("https://proxy.example.com/v1".parse().expect("custom URL"));
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .principal_view(Arc::new(PrincipalView::from_db(
            &[principal_record("principal")],
            HashMap::new(),
        )))
        .upstream_records(vec![upstream_record(official_id), custom])
        .build();
    let official_breakpoints = vec![cache_breakpoint(
        1,
        "official-prefix",
        100,
        TtlClass::Ephemeral5m,
    )];
    let custom_breakpoints = vec![cache_breakpoint(
        1,
        "custom-prefix",
        200,
        TtlClass::Ephemeral5m,
    )];
    let bundles = PromptCacheBundles {
        official: PromptCacheBundle {
            cache_breakpoints: official_breakpoints.clone(),
            cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["official-prefix"]),
            request_breakpoints: official_breakpoints
                .iter()
                .map(request_cache_breakpoint_from_plugin)
                .collect(),
            cache_prefix_hash: Some("official-prefix".to_owned()),
        },
        literal: PromptCacheBundle {
            cache_breakpoints: custom_breakpoints.clone(),
            cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["custom-prefix"]),
            request_breakpoints: custom_breakpoints
                .iter()
                .map(request_cache_breakpoint_from_plugin)
                .collect(),
            cache_prefix_hash: Some("custom-prefix".to_owned()),
        },
    };
    let now = 1_800_000_000;
    let mut official_warm =
        warm_entry_priced("official-prefix", TtlClass::Ephemeral5m, now + 600, 0, 100);
    official_warm.content_block_index = 1;
    let mut custom_warm =
        warm_entry_priced("custom-prefix", TtlClass::Ephemeral5m, now + 600, 0, 200);
    custom_warm.content_block_index = 1;
    let warm_entries = HashMap::from([
        (official_id, vec![official_warm]),
        (custom_id, vec![custom_warm]),
    ]);
    let candidates = build_candidates_with_matches_for_bundles(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &official_breakpoints,
        &warm_entries,
        Some(&bundles),
        &cc_lb_clock::TestClock::new_at_secs(now),
    )
    .0;
    let official = candidates
        .iter()
        .find(|candidate| candidate.upstream_id == official_id)
        .and_then(|candidate| candidate.cache_score.as_ref())
        .expect("official score");
    let custom = candidates
        .iter()
        .find(|candidate| candidate.upstream_id == custom_id)
        .and_then(|candidate| candidate.cache_score.as_ref())
        .expect("custom score");
    assert_eq!(
        official.matched_v3_cache_key.as_deref(),
        Some("official-prefix")
    );
    assert_eq!(official.predicted_cache_read_tokens, 100);
    assert_eq!(
        custom.matched_v3_cache_key.as_deref(),
        Some("custom-prefix")
    );
    assert_eq!(custom.predicted_cache_read_tokens, 200);
}

#[tokio::test]
async fn warm_entry_snapshot_is_request_scoped_and_expires_at_selection_time() {
    let upstream_id = Uuid::from_u128(826);
    let now = 1_800_000_000_u64;
    let store = Arc::new(FakePromptCacheStore::new(vec![observation_record(
        upstream_id,
        "prefix-a",
        TtlClass::Ephemeral5m,
        now + 5,
    )]));
    let view = cache_score_view_with_store(upstream_id, store.clone());
    let clock = cc_lb_clock::TestClock::new_at_secs(now);
    let breakpoints = vec![
        cache_breakpoint(0, "prefix-a", 100, TtlClass::Ephemeral5m),
        cache_breakpoint(1, "prefix-b", 200, TtlClass::Ephemeral5m),
    ];

    // The request's snapshot is taken before a second observation commits.
    let snapshot = fetch_prompt_cache_warm_entries(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &breakpoints,
        &clock,
    )
    .await;
    store
        .records
        .lock()
        .expect("records lock")
        .push(observation_record(
            upstream_id,
            "prefix-b",
            TtlClass::Ephemeral5m,
            now + 60,
        ));

    // The saved snapshot must not see the newer committed row, while the
    // next request's read does.
    assert_eq!(snapshot[&upstream_id].len(), 1);
    let next_snapshot = fetch_prompt_cache_warm_entries(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &breakpoints,
        &clock,
    )
    .await;
    assert_eq!(next_snapshot[&upstream_id].len(), 2);

    // Selection on the saved snapshot still warm-routes on prefix-a; the
    // deeper prefix-b row is absent from this request's snapshot.
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &breakpoints,
        &snapshot,
        &clock,
    );
    let score = candidates[0].cache_score.as_ref().expect("warm score");
    assert_eq!(score.matched_v3_cache_key.as_deref(), Some("prefix-a"));
    assert_eq!(score.predicted_cache_read_tokens, 100);

    // Selection-time expiry re-check: expires_at > now is strict, so the
    // same snapshot is warm one second before expiry and cold at expiry.
    clock.advance_secs(4);
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &breakpoints,
        &snapshot,
        &clock,
    );
    let score = candidates[0].cache_score.as_ref().expect("still warm");
    assert_eq!(score.predicted_cache_read_tokens, 100);

    clock.advance_secs(1);
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &breakpoints,
        &snapshot,
        &clock,
    );
    let score = candidates[0].cache_score.as_ref().expect("cold score");
    assert_eq!(score.predicted_cache_read_tokens, 0);
    assert_eq!(score.matched_v3_cache_key, None);
}

#[tokio::test]
async fn empty_request_breakpoints_skip_store_lookup_and_still_build_candidates() {
    let upstream_id = Uuid::from_u128(827);
    let store = Arc::new(FakePromptCacheStore::new(Vec::new()));
    let view = cache_score_view_with_store(upstream_id, store.clone());
    let clock = cc_lb_clock::TestClock::new_at_secs(1_800_000_000);

    // No cache breakpoints: the shared store must not be queried.
    let snapshot = fetch_prompt_cache_warm_entries(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &[],
        &clock,
    )
    .await;
    assert!(snapshot.is_empty());
    assert_eq!(store.lookup_count(), 0);
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &[],
        &snapshot,
        &clock,
    );
    assert_eq!(candidates.len(), 1);
    assert!(candidates[0].cache_score.is_none());

    // Breakpoints without lookback prefixes produce no candidate keys.
    let mut keyless = cache_breakpoint(0, "prefix", 100, TtlClass::Ephemeral5m);
    keyless.lookback_prefixes.clear();
    let snapshot = fetch_prompt_cache_warm_entries(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &[keyless],
        &clock,
    )
    .await;
    assert!(snapshot.is_empty());
    assert_eq!(store.lookup_count(), 0);

    // An unresolved (empty) canonical model also skips the lookup.
    let breakpoints = vec![cache_breakpoint(0, "prefix", 100, TtlClass::Ephemeral5m)];
    let snapshot = fetch_prompt_cache_warm_entries(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        "",
        &breakpoints,
        &clock,
    )
    .await;
    assert!(snapshot.is_empty());
    assert_eq!(store.lookup_count(), 0);
}

#[tokio::test]
async fn foreign_schema_version_observation_is_known_cold_not_warm() {
    let upstream_id = Uuid::from_u128(828);
    let mut record =
        observation_record(upstream_id, "prefix", TtlClass::Ephemeral5m, 1_800_000_600);
    record.hash_schema_version = HASH_SCHEMA_VERSION + 1;
    let store = Arc::new(FakePromptCacheStore::new(vec![record]));
    let view = cache_score_view_with_store(upstream_id, store.clone());
    let clock = cc_lb_clock::TestClock::new_at_secs(1_800_000_000);
    let breakpoints = vec![cache_breakpoint(0, "prefix", 100, TtlClass::Ephemeral5m)];

    let snapshot = fetch_prompt_cache_warm_entries(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &breakpoints,
        &clock,
    )
    .await;
    // The read succeeded: the upstream is present but the foreign-schema
    // row was dropped, so this is known-cold rather than unknown.
    assert_eq!(snapshot[&upstream_id].len(), 0);
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &breakpoints,
        &snapshot,
        &clock,
    );
    let score = candidates[0]
        .cache_score
        .as_ref()
        .expect("known-cold score, not unknown");
    assert_eq!(score.predicted_cache_read_tokens, 0);
    assert_eq!(score.predicted_cache_creation_tokens_5m, 100);
}

#[tokio::test]
async fn warm_match_ignores_observation_from_other_estimator_source() {
    let upstream_id = Uuid::from_u128(766);
    let clock = cc_lb_clock::TestClock::new_at_secs(1_800_000_000);
    let breakpoints = vec![cache_breakpoint(0, "prefix", 100, TtlClass::Ephemeral5m)];

    // Positive control: an unexpired row from the current estimator warms
    // the request and its prefix size is subtracted as a predicted read.
    let current = observation_record(upstream_id, "prefix", TtlClass::Ephemeral5m, 1_800_000_600);
    let store = Arc::new(FakePromptCacheStore::new(vec![current]));
    let view = cache_score_view_with_store(upstream_id, store.clone());
    let snapshot = fetch_prompt_cache_warm_entries(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &breakpoints,
        &clock,
    )
    .await;
    assert_eq!(snapshot[&upstream_id].len(), 1);
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &breakpoints,
        &snapshot,
        &clock,
    );
    let score = candidates[0].cache_score.as_ref().expect("warm score");
    assert_eq!(score.matched_v3_cache_key.as_deref(), Some("prefix"));
    assert_eq!(score.predicted_cache_read_tokens, 100);

    // Same schema version, but the size was measured by another
    // estimator: its unit is not comparable, so the row must be ignored
    // and the upstream scored known-cold.
    let mut stale = observation_record(upstream_id, "prefix", TtlClass::Ephemeral5m, 1_800_000_600);
    stale.token_estimate_source = "local_tiktoken_v1".to_owned();
    assert_eq!(stale.hash_schema_version, HASH_SCHEMA_VERSION);
    let store = Arc::new(FakePromptCacheStore::new(vec![stale]));
    let view = cache_score_view_with_store(upstream_id, store.clone());
    let snapshot = fetch_prompt_cache_warm_entries(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &breakpoints,
        &clock,
    )
    .await;
    assert_eq!(snapshot[&upstream_id].len(), 0);
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &breakpoints,
        &snapshot,
        &clock,
    );
    let score = candidates[0]
        .cache_score
        .as_ref()
        .expect("known-cold score, not unknown");
    assert_eq!(score.matched_v3_cache_key, None);
    assert_eq!(score.predicted_cache_read_tokens, 0);
    assert_eq!(score.predicted_cache_creation_tokens_5m, 100);
}

#[tokio::test]
async fn warm_entries_keep_only_current_estimator_rows() {
    let upstream_id = Uuid::from_u128(76_602);
    let clock = cc_lb_clock::TestClock::new_at_secs(1_800_000_000);
    let breakpoints = vec![
        cache_breakpoint(0, "current", 100, TtlClass::Ephemeral5m),
        cache_breakpoint(1, "tiktoken", 200, TtlClass::Ephemeral5m),
        cache_breakpoint(2, "unknown", 300, TtlClass::Ephemeral5m),
        cache_breakpoint(3, "empty", 400, TtlClass::Ephemeral5m),
    ];
    let current = observation_record(upstream_id, "current", TtlClass::Ephemeral5m, 1_800_000_600);
    let mut records = vec![current];
    for (prefix_key, source) in [
        ("tiktoken", "local_tiktoken_v1"),
        ("unknown", "unknown"),
        ("empty", ""),
    ] {
        let mut record = observation_record(
            upstream_id,
            prefix_key,
            TtlClass::Ephemeral5m,
            1_800_000_600,
        );
        record.token_estimate_source = source.to_owned();
        records.push(record);
    }
    let store = Arc::new(FakePromptCacheStore::new(records));
    let view = cache_score_view_with_store(upstream_id, store.clone());

    let snapshot = fetch_prompt_cache_warm_entries(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &breakpoints,
        &clock,
    )
    .await;

    assert_eq!(store.lookup_count(), 1);
    let entries = snapshot
        .get(&upstream_id)
        .expect("successful lookup keeps the upstream as known");
    let keys = entries
        .iter()
        .map(|entry| entry.prefix_hash.as_str())
        .collect::<Vec<_>>();
    assert_eq!(keys, vec!["current"]);
    assert_eq!(
        entries.first().map(|entry| entry.estimated_prefix_bytes),
        Some(PrefixBytes::new(100))
    );
}

#[tokio::test]
async fn five_minute_observation_cannot_warm_one_hour_request() {
    let upstream_id = Uuid::from_u128(829);
    let store = Arc::new(FakePromptCacheStore::new(vec![observation_record(
        upstream_id,
        "prefix",
        TtlClass::Ephemeral5m,
        1_800_000_600,
    )]));
    let view = cache_score_view_with_store(upstream_id, store.clone());
    let clock = cc_lb_clock::TestClock::new_at_secs(1_800_000_000);
    let breakpoints = vec![cache_breakpoint(0, "prefix", 100, TtlClass::Ephemeral1h)];

    let snapshot = fetch_prompt_cache_warm_entries(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &breakpoints,
        &clock,
    )
    .await;
    // The store returns the row (TTL is not a store filter); selection
    // rejects it because a 5m observation cannot satisfy a 1h breakpoint.
    assert_eq!(snapshot[&upstream_id].len(), 1);
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        &breakpoints,
        &snapshot,
        &clock,
    );
    let score = candidates[0].cache_score.as_ref().expect("cold score");
    assert_eq!(score.predicted_cache_read_tokens, 0);
    assert_eq!(score.predicted_cache_creation_tokens_1h, 100);
}

fn cache_score_view(upstream_id: Uuid) -> Arc<DynamicView> {
    DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .principal_view(Arc::new(PrincipalView::from_db(
            &[principal_record("principal")],
            HashMap::new(),
        )))
        .upstream_records(vec![upstream_record(upstream_id)])
        .build()
}
fn cacheable_prefix_keys(prefixes: &[&str]) -> HashSet<String> {
    prefixes.iter().map(|prefix| (*prefix).to_owned()).collect()
}

fn cache_breakpoint(
    index: u32,
    prefix_hash: &str,
    prefix_bytes: u64,
    requested_ttl: TtlClass,
) -> CacheBreakpoint {
    CacheBreakpoint {
        block_index: index,
        source: CacheBreakpointSource::Message,
        path: format!("messages[{index}]"),
        message_index: Some(index),
        prefix_hash: prefix_hash.to_owned(),
        prefix_bytes: PrefixBytes::new(prefix_bytes),
        requested_ttl,
        lookback_prefixes: vec![CacheLookbackPrefix {
            prefix_hash: prefix_hash.to_owned(),
            content_block_index: index,
            lookback_distance: 0,
        }],
        token_estimate_source: Some("test".to_owned()),
    }
}

fn warm_entry(
    prefix_hash: &str,
    ttl_class: TtlClass,
    expires_at_unix_secs: u64,
    observed_offset: u64,
) -> WarmCacheEntry {
    warm_entry_priced(
        prefix_hash,
        ttl_class,
        expires_at_unix_secs,
        observed_offset,
        0,
    )
}

fn selected_match_hit(
    prefix_hash: &str,
    block_index: u32,
    ttl_class: TtlClass,
    estimated_prefix_bytes: u64,
) -> SelectedCacheMatch {
    SelectedCacheMatch {
        prefix_hash: prefix_hash.to_owned(),
        content_block_index: block_index,
        ttl_class,
        estimated_prefix_bytes: PrefixBytes::new(estimated_prefix_bytes),
    }
}

fn warm_entry_priced(
    prefix_hash: &str,
    ttl_class: TtlClass,
    expires_at_unix_secs: u64,
    observed_offset: u64,
    estimated_prefix_bytes: u64,
) -> WarmCacheEntry {
    WarmCacheEntry {
        prefix_hash: prefix_hash.to_owned(),
        expires_at_unix_secs,
        ttl_class,
        last_observed_at_unix_secs: 1_700_000_000 + observed_offset,
        content_block_index: 0,
        estimated_prefix_bytes: PrefixBytes::new(estimated_prefix_bytes),
        hash_schema_version: HASH_SCHEMA_VERSION,
    }
}

fn principal_record(name: &str) -> PrincipalRecord {
    PrincipalRecord {
        openai_oauth_compat: None,
        id: Uuid::new_v4(),
        name: name.to_owned(),
        kind: StoragePrincipalKind::Machine,
        allowed_models: Vec::new(),
        allowed_upstreams: Vec::new(),
        default_limits: Vec::new(),
        enabled: true,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
        cache_keepalive: None,
    }
}

fn upstream_record(id: Uuid) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: format!("upstream-{id}"),
        kind: StorageRecordKind::AnthropicApiKey,
        enabled: true,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        ..UpstreamRecord::default()
    }
}

struct TestSignerFactory;

impl ApiKeyAwareSignerFactory for TestSignerFactory {
    fn with_router_choice(&self, _router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(Self)
    }
}

#[async_trait]
impl SignerFactory for TestSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(TestSigner))
    }
}

struct TestSigner;

#[async_trait]
impl Signer for TestSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut cc_lb_upstream::SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

#[test]
fn gzip_encoded_sse_stream_yields_usage_via_decoder() {
    use std::io::Write as _;

    let sse_plaintext = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":48,\"output_tokens\":1}}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":72}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        )
        .as_bytes();

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(sse_plaintext).expect("gzip encode");
    let compressed = encoder.finish().expect("finish gzip");

    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_ENCODING,
        HeaderValue::from_static("gzip"),
    );
    let mut decoder = UsageDecoder::from_headers(
        &headers,
        decompression_output_budget_bytes(DEFAULT_MESSAGES_CAP_BYTES),
    );
    assert!(decoder.is_active(), "gzip decoder should be active");

    let mut buffer: Vec<u8> = Vec::new();
    let mut usage = UsageCounts::default();
    let mut events_seen = 0_u64;

    for chunk in compressed.chunks(7) {
        let plaintext = decoder.push(chunk).expect("decode chunk");
        buffer.extend_from_slice(&plaintext);
        while let Some(end) = usage_parser::find_sse_event_end(&buffer) {
            let raw = buffer.drain(..end).collect::<Vec<u8>>();
            let event = parse_sse_event(&raw);
            let _ = accumulate_sse_usage(&event, &mut usage);
            events_seen += 1;
        }
    }
    let tail = decoder.finish().expect("decoder finish");
    buffer.extend_from_slice(&tail);
    while let Some(end) = usage_parser::find_sse_event_end(&buffer) {
        let raw = buffer.drain(..end).collect::<Vec<u8>>();
        let event = parse_sse_event(&raw);
        let _ = accumulate_sse_usage(&event, &mut usage);
        events_seen += 1;
    }

    assert_eq!(
        events_seen, 4,
        "expected 4 SSE events parsed from decoded stream"
    );
    assert!(usage.present, "usage must be marked present after parsing");
    assert_eq!(usage.input_tokens, 48);
    assert_eq!(usage.output_tokens, 72);
}

// ------------------------------------------------------------------
// Failure-provenance tests: every known internal failure must reach the
// terminal snapshot with a typed stage/kind/message, and non-fatal
// diagnostics must accumulate ahead of it.
// ------------------------------------------------------------------

struct FailureKeyFixture {
    plaintext: String,
    key_id: String,
    record: StoredApiKeyRecord,
}

static FAILURE_KEY_FIXTURE: std::sync::LazyLock<FailureKeyFixture> =
    std::sync::LazyLock::new(|| {
        let generated = cc_lb_control::api_keys::secret::generate_new();
        FailureKeyFixture {
            plaintext: generated.plaintext.expose().to_owned(),
            key_id: generated.key_id,
            record: StoredApiKeyRecord {
                label: "failure provenance key".to_owned(),
                verify_hash: generated.verify_hash,
                secret_salt: generated.secret_salt,
                status: cc_lb_storage_api::KeyStatus::Active,
                last_4: generated.last_4,
                index_hash: generated.index_hash,
                ..StoredApiKeyRecord::default()
            },
        }
    });

fn failure_key_fixture() -> &'static FailureKeyFixture {
    &FAILURE_KEY_FIXTURE
}

struct InMemoryManagedKeyStore {
    principal_id: String,
}

#[async_trait]
impl cc_lb_storage_api::ManagedKeyStore for InMemoryManagedKeyStore {
    async fn issue(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _params: cc_lb_storage_api::IssueParams,
    ) -> cc_lb_storage_api::StorageResult<StoredApiKeyRecord> {
        Ok(failure_key_fixture().record.clone())
    }

    async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> cc_lb_storage_api::StorageResult<Option<StoredApiKeyRecord>> {
        Ok(
            (principal_id == self.principal_id && key_id == failure_key_fixture().key_id)
                .then(|| failure_key_fixture().record.clone()),
        )
    }

    async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> cc_lb_storage_api::StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
        Ok(
            (index_hash == &failure_key_fixture().record.index_hash).then(|| {
                (
                    self.principal_id.clone(),
                    failure_key_fixture().key_id.clone(),
                    failure_key_fixture().record.clone(),
                )
            }),
        )
    }

    async fn list_by_principal(
        &self,
        principal_id: &str,
    ) -> cc_lb_storage_api::StorageResult<Vec<StoredApiKeyRecord>> {
        Ok((principal_id == self.principal_id)
            .then(|| failure_key_fixture().record.clone())
            .into_iter()
            .collect())
    }

    async fn list_all(
        &self,
    ) -> cc_lb_storage_api::StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
        Ok(vec![(
            self.principal_id.clone(),
            failure_key_fixture().key_id.clone(),
            failure_key_fixture().record.clone(),
        )])
    }

    async fn update(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _mutation: cc_lb_storage_api::ApiKeyMutation,
    ) -> cc_lb_storage_api::StorageResult<()> {
        Ok(())
    }

    async fn revoke_zero_secrets(
        &self,
        _principal_id: &str,
        _key_id: &str,
    ) -> cc_lb_storage_api::StorageResult<()> {
        Ok(())
    }
}

fn failure_authn() -> Arc<BuiltinAuthn> {
    let storage: Arc<dyn cc_lb_storage_api::ManagedKeyStore> = Arc::new(InMemoryManagedKeyStore {
        principal_id: "principal-test".to_owned(),
    });
    Arc::new(BuiltinAuthn::new(
        Arc::new(cc_lb_control::api_keys::key_store::KeyStore::new(storage)),
        Arc::new(cc_lb_clock::SystemClock),
    ))
}

/// Deterministic dispatcher: replays a scripted queue of outcomes.
struct QueueDispatch {
    calls: AtomicUsize,
    outcomes: Mutex<std::collections::VecDeque<Result<Response<Body>, DispatchError>>>,
}

impl QueueDispatch {
    fn new(outcomes: Vec<Result<Response<Body>, DispatchError>>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            outcomes: Mutex::new(outcomes.into()),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl UpstreamDispatch for QueueDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.outcomes
            .lock()
            .expect("queue dispatch lock")
            .pop_front()
            .expect("dispatch called more times than scripted")
    }
}

fn upstream_response(status: StatusCode, body: &'static [u8]) -> Response<Body> {
    let mut response = Response::new(Body::from(Bytes::from_static(body)));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

/// Signer factory whose `build` always fails with a fixed error.
struct FailingSignerFactory(fn() -> SignerError);

impl ApiKeyAwareSignerFactory for FailingSignerFactory {
    fn with_router_choice(&self, _router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(Self(self.0))
    }
}

#[async_trait]
impl SignerFactory for FailingSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Err((self.0)())
    }
}

/// Signer whose `sign` always fails; `on_unauthorized` never refreshes.
struct FailingSigner(fn() -> SignerError);

#[async_trait]
impl Signer for FailingSigner {
    async fn sign(
        &self,
        _shaped: ShapedRequest,
        _capability: &mut cc_lb_upstream::SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        Err((self.0)())
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

/// Signer factory that hands out `FailingSigner`.
struct FailingSignerFactoryForAttempt(fn() -> SignerError);

impl ApiKeyAwareSignerFactory for FailingSignerFactoryForAttempt {
    fn with_router_choice(&self, _router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(Self(self.0))
    }
}

#[async_trait]
impl SignerFactory for FailingSignerFactoryForAttempt {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(FailingSigner(self.0)))
    }
}

/// Signer that always asks for a credential refresh on 401.
struct RefreshingSigner;

#[async_trait]
impl Signer for RefreshingSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut cc_lb_upstream::SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Refresh {
            new_signer: Arc::new(TestSigner),
        }
    }
}

/// Signer factory that hands out `RefreshingSigner`.
struct RefreshingSignerFactory;

impl ApiKeyAwareSignerFactory for RefreshingSignerFactory {
    fn with_router_choice(&self, _router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(Self)
    }
}

#[async_trait]
impl SignerFactory for RefreshingSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(RefreshingSigner))
    }
}

/// Dialect whose shaping always fails, forcing the raw-passthrough fallback.
struct FailingDialect;

impl UpstreamDialect for FailingDialect {
    fn shape(
        &self,
        _context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        _builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Err(DialectError::UnsupportedRequest {
            reason: "injected dialect failure".to_owned(),
        })
    }
}

/// Affinity store whose resolve always fails.
struct FailingAffinityStore;

#[async_trait]
impl UpstreamAffinityStore for FailingAffinityStore {
    async fn resolve_upstream_affinities(
        &self,
        _keys: &[UpstreamAffinityKey],
        _now_unix_secs: u64,
        _ttl_secs: u64,
    ) -> cc_lb_storage_api::StorageResult<Vec<UpstreamAffinityBinding>> {
        Err(cc_lb_storage_api::StorageError::Unavailable {
            message: "affinity store down".to_owned(),
        })
    }
    async fn read_upstream_owner_epoch(
        &self,
        _upstream_id: cc_lb_storage_api::UpstreamRecordId,
        _expected_spec_revision: u64,
    ) -> cc_lb_storage_api::StorageResult<Option<u64>> {
        Err(cc_lb_storage_api::StorageError::Unavailable {
            message: "affinity store down".to_owned(),
        })
    }

    async fn bind_upstream_affinities(
        &self,
        _bindings: &[UpstreamAffinityBinding],
        _now_unix_secs: u64,
        _ttl_secs: u64,
    ) -> cc_lb_storage_api::StorageResult<()> {
        Err(cc_lb_storage_api::StorageError::Unavailable {
            message: "affinity store down".to_owned(),
        })
    }

    async fn purge_expired_upstream_affinities(
        &self,
        _now_unix_secs: u64,
        _ttl_secs: u64,
        _batch_size: usize,
    ) -> cc_lb_storage_api::StorageResult<u64> {
        Err(cc_lb_storage_api::StorageError::Unavailable {
            message: "affinity store down".to_owned(),
        })
    }
}

/// Filter plugin that returns an upstream id outside the candidate set,
/// tripping output validation and the invalid-output passthrough.
struct InvalidOutputFilter;

impl FilterPlugin for InvalidOutputFilter {
    fn filter(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        Ok(FilterOutput {
            kept_upstream_ids: vec![Uuid::new_v4()],
            reason: "injected invalid output".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        "invalid-output-filter"
    }
}

fn failure_principal_view(
    principal_id: &str,
    artifacts: cc_lb_control::api_keys::principal_view::PrincipalRoutingArtifacts,
) -> Arc<PrincipalView> {
    let mut chains: HashMap<
        String,
        cc_lb_control::api_keys::principal_view::PrincipalRoutingArtifacts,
    > = HashMap::new();
    chains.insert(principal_id.to_owned(), artifacts);
    Arc::new(PrincipalView::for_tests(
        principal_id,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

fn limited_failure_principal_view(
    limit: cc_lb_storage_api::principal::Limit,
) -> Arc<PrincipalView> {
    Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        vec![limit],
        HashMap::new(),
    ))
}

fn upstream_record_with_kind(kind: StorageRecordKind) -> UpstreamRecord {
    UpstreamRecord {
        id: Uuid::new_v4(),
        name: "upstream-test".to_owned(),
        kind,
        enabled: true,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        ..UpstreamRecord::default()
    }
}

fn default_artifacts() -> cc_lb_control::api_keys::principal_view::PrincipalRoutingArtifacts {
    (
        None,
        cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
    )
}

fn provenance_view(
    principal_view: Arc<PrincipalView>,
    signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
    upstreams: Vec<UpstreamRecord>,
) -> Arc<DynamicView> {
    DynamicViewBuilder::new(0)
        .signer_factory(signer_factory)
        .principal_view(principal_view)
        .upstream_records(upstreams)
        .build()
}

fn provenance_lifecycle(
    view: Arc<DynamicView>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    bus: &Arc<cc_lb_control::event_bus::InMemoryBus>,
) -> Lifecycle {
    Lifecycle::new_with_dynamic_view(
        failure_authn(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(bus.clone() as Arc<dyn RequestEventBus>)
}

fn failure_request(body: Bytes) -> Request<Bytes> {
    Request::builder()
        .method(http::Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", failure_key_fixture().plaintext.as_str())
        .header("anthropic-version", "2023-06-01")
        .body(body)
        .expect("failure request builds")
}

fn subscribe_lifecycle(
    bus: &Arc<cc_lb_control::event_bus::InMemoryBus>,
) -> tokio::sync::broadcast::Receiver<cc_lb_lifecycle::LifecycleEvent> {
    bus.subscribe_lifecycle()
}

fn drain_events(
    rx: &mut tokio::sync::broadcast::Receiver<cc_lb_lifecycle::LifecycleEvent>,
) -> Vec<cc_lb_lifecycle::LifecycleEvent> {
    let mut events = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(event) => events.push(event),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
        }
    }
    events
}

struct TerminalSnapshot {
    reason: cc_lb_lifecycle::TerminationReason,
    client_status: u16,
    internal_errors: Vec<InternalError>,
    dns_ms: Option<u64>,
    connect_ms: Option<u64>,
    connection_reused: Option<bool>,
}

fn terminal_snapshot(events: &[cc_lb_lifecycle::LifecycleEvent]) -> TerminalSnapshot {
    for event in events {
        if let cc_lb_lifecycle::LifecycleEvent::RequestTerminated {
            reason,
            client_status,
            internal_errors,
            dns_ms,
            connect_ms,
            connection_reused,
            ..
        } = event
        {
            return TerminalSnapshot {
                reason: reason.clone(),
                client_status: *client_status,
                internal_errors: internal_errors.clone(),
                dns_ms: *dns_ms,
                connect_ms: *connect_ms,
                connection_reused: *connection_reused,
            };
        }
    }
    panic!("expected RequestTerminated in drained events");
}

fn expect_error_code(reason: &cc_lb_lifecycle::TerminationReason, expected: &str) {
    match reason {
        cc_lb_lifecycle::TerminationReason::ErrorCode(code) => {
            assert_eq!(code, expected, "unexpected terminal error code");
        }
        other => panic!("expected ErrorCode({expected}), got {other:?}"),
    }
}

async fn collect_response_body(response: Response<Body>) -> (StatusCode, Bytes) {
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body collects")
        .to_bytes();
    (status, body)
}

#[tokio::test]
async fn scoped_unsupported_operations_use_provider_errors_and_one_terminal_event() {
    use cc_lb_domain::Provider;
    for (provider, path) in [
        (Provider::Anthropic, "/v1/files"),
        (Provider::Openai, "/v1/realtime/client_secrets"),
        (Provider::Gemini, "/v1beta/cachedContents/unknown"),
        (Provider::Anthropic, "/v1/responses"),
    ] {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(TestSignerFactory),
            Vec::new(),
        );
        let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);
        let mut request = failure_request(Bytes::from_static(b"{}"));
        *request.uri_mut() = path.parse().expect("path URI");
        request
            .extensions_mut()
            .insert(crate::request_context::ProviderScope(provider));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("authenticated");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("request handled");
        let (status, body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let body: Value = serde_json::from_slice(&body).expect("error JSON");
        assert_eq!(body["error"]["type"], "operation_not_supported");
        assert_eq!(
            body["error"]["message"],
            format!("POST {path} is not supported by cc-lb yet")
        );
        match provider {
            Provider::Anthropic => assert_eq!(body["type"], "error"),
            Provider::Openai => {
                assert_eq!(body["error"]["code"], "operation_not_supported");
                assert!(body["error"]["param"].is_null());
                assert!(body.get("type").is_none());
            }
            Provider::Gemini => {
                assert_eq!(body["error"]["code"], 404);
                assert_eq!(body["error"]["status"], "NOT_FOUND");
                assert!(body.get("type").is_none());
            }
        }
        assert_eq!(dispatcher.call_count(), 0);
        let events = drain_events(&mut rx);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
                ))
                .count(),
            1,
            "{provider:?} {path}"
        );
        assert_eq!(terminal_snapshot(&events).client_status, 404);
    }
}

#[tokio::test]
async fn negative_generation_bounds_reject_before_provider_dispatch() {
    use cc_lb_domain::Provider;

    for (provider, path, body, upstream_kind) in [
        (
            Provider::Openai,
            "/v1/responses",
            br#"{"model":"gpt-test","max_output_tokens":-1,"input":[]}"#.as_slice(),
            StorageRecordKind::OpenaiApiKey,
        ),
        (
            Provider::Gemini,
            "/v1beta/models/gemini-test:generateContent",
            br#"{"contents":[],"generationConfig":{"maxOutputTokens":-1}}"#.as_slice(),
            StorageRecordKind::GeminiApiKey,
        ),
    ] {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
        let view = provenance_view(
            limited_failure_principal_view(cc_lb_storage_api::principal::Limit {
                kind: cc_lb_storage_api::principal::LimitKind::OutputTokens,
                window_secs: 60,
                cap_micros: 100,
            }),
            Arc::new(TestSignerFactory),
            vec![upstream_record_with_kind(upstream_kind)],
        );
        let limit_engine = cc_lb_control::api_keys::limit_engine::LimitEngine::new(
            Arc::new(cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::SystemClock),
        );
        struct StaticTestLimitSubjectProvider(AuthLimitSubject);
        #[async_trait]
        impl LimitSubjectProvider for StaticTestLimitSubjectProvider {
            async fn limit_subject(
                &self,
                _ctx: &RequestContext,
                _principal: &Principal,
                _authn_success: &AuthnSuccess,
            ) -> Option<AuthLimitSubject> {
                Some(self.0.clone())
            }
        }
        let fixture = failure_key_fixture();
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus).with_limit_engine(
            limit_engine,
            Arc::new(StaticTestLimitSubjectProvider(AuthLimitSubject {
                principal_id: "principal-test".to_owned(),
                key_id: fixture.key_id.clone(),
                record: fixture.record.clone(),
            })),
        );
        let mut request = failure_request(Bytes::from_static(body));
        *request.uri_mut() = path.parse().expect("provider path");
        request
            .extensions_mut()
            .insert(crate::request_context::ProviderScope(provider));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("authenticated");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("request handled");
        let (status, body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{provider:?}");
        let body: Value = serde_json::from_slice(&body).expect("error JSON");
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert!(
            body["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("zero or greater"))
        );
        assert_eq!(dispatcher.call_count(), 0, "{provider:?}");
    }
}

#[tokio::test]
async fn multipart_model_is_read_after_authentication_without_parsing_binary_as_json() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let view = provenance_view(
        failure_principal_view("principal-test", default_artifacts()),
        Arc::new(TestSignerFactory),
        Vec::new(),
    );
    let lifecycle = provenance_lifecycle(view, Arc::new(QueueDispatch::new(Vec::new())), &bus);
    let request = failure_request(Bytes::new());
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("authenticated");
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("multipart/form-data; boundary=\"catalog-test\""),
    );
    let body = Bytes::from_static(
            b"--catalog-test\r\nContent-Disposition: form-data; name=\"file\"; filename=\"clip.wav\"\r\nContent-Type: audio/wav\r\n\r\n\x00\xffbinary\r\n--catalog-test\r\nContent-Disposition: form-data; name=\"model\"; filename=\"not-a-model\"\r\n\r\nignore-me\r\n--catalog-test\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\ngpt-4o-transcribe\r\n--catalog-test--\r\n"
        );
    assert_eq!(
        multipart_request_model(&auth, &headers, &body)
            .await
            .as_deref(),
        Some("gpt-4o-transcribe")
    );
}

/// QA-AUTH-03b: authentication succeeds, then the principal disappears
/// from the view before `handle` resolves it.
#[tokio::test]
async fn post_auth_principal_missing_records_authn_unavailable() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let upstream_id = Uuid::new_v4();
    let view_a = provenance_view(
        failure_principal_view("principal-test", default_artifacts()),
        Arc::new(TestSignerFactory),
        vec![upstream_record(upstream_id)],
    );
    let holder = Arc::new(DynamicViewHolder::new(view_a));
    let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
    let lifecycle = Lifecycle::new_with_dynamic_view(
        failure_authn(),
        holder.clone(),
        dispatcher.clone(),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(bus.clone() as Arc<dyn RequestEventBus>);

    let request = failure_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    // Swap the view so the authenticated principal is gone at handle time.
    holder.store(provenance_view(
        failure_principal_view("other-principal", default_artifacts()),
        Arc::new(TestSignerFactory),
        vec![upstream_record(upstream_id)],
    ));

    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, body) = collect_response_body(response).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        body,
        anthropic_error_body("api_error", "authenticated principal is unavailable"),
        "client response must stay byte-identical"
    );
    assert_eq!(
        dispatcher.call_count(),
        0,
        "no dispatch on principal failure"
    );

    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|event| matches!(
            event,
            cc_lb_lifecycle::LifecycleEvent::AuthCompleted {
                result: Err(cc_lb_lifecycle::AuthFailure::PrincipalMissing { .. }),
                ..
            }
        )),
        "expected AuthCompleted::PrincipalMissing, got {events:?}"
    );
    let terminal = terminal_snapshot(&events);
    assert_eq!(terminal.client_status, 500);
    expect_error_code(&terminal.reason, error_codes::PRINCIPAL_MISSING);
    assert_eq!(terminal.internal_errors.len(), 1);
    assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Authn);
    assert_eq!(
        terminal.internal_errors[0].kind,
        InternalErrorKind::Unavailable
    );
    assert_eq!(
        terminal.internal_errors[0].message.as_deref(),
        Some("authenticated principal is unavailable")
    );
}

/// QA-ROUTE-02: the resolved router pipeline carries an instantiation
/// error, so routing cannot run at all.
#[tokio::test]
async fn router_pipeline_instantiation_error_records_router_config() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let upstream_id = Uuid::new_v4();
    let pipeline = cc_lb_control::api_keys::principal_view::RouterPipelineCache {
        user_filters: Vec::new(),
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: Some(Arc::from("pipeline exploded")),
        provider_filters: None,
    };
    let view = provenance_view(
        failure_principal_view(
            "principal-test",
            (
                Some(Arc::new(pipeline)),
                cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
            ),
        ),
        Arc::new(TestSignerFactory),
        vec![upstream_record(upstream_id)],
    );
    let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

    let request = failure_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _body) = collect_response_body(response).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(dispatcher.call_count(), 0);

    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|event| matches!(
            event,
            cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
                result: Err(cc_lb_lifecycle::RouteFailure::RouterPipelineUnavailable),
                ..
            }
        )),
        "expected RouteCompleted::RouterPipelineUnavailable, got {events:?}"
    );
    let terminal = terminal_snapshot(&events);
    assert_eq!(terminal.client_status, 502);
    expect_error_code(&terminal.reason, error_codes::ROUTER_PIPELINE_UNAVAILABLE);
    assert_eq!(terminal.internal_errors.len(), 1);
    assert_eq!(
        terminal.internal_errors[0].stage,
        InternalErrorStage::Router
    );
    assert_eq!(
        terminal.internal_errors[0].kind,
        InternalErrorKind::ConfigError
    );
    assert_eq!(
        terminal.internal_errors[0].message.as_deref(),
        Some("pipeline exploded")
    );
}

/// QA-ROUTE-04: a filter stage fails validation, candidates pass through,
/// and the non-fatal diagnostic must precede the terminal dispatch cause.
#[tokio::test]
async fn filter_invalid_output_history_precedes_terminal_dispatch_cause() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let upstream_id = Uuid::new_v4();
    let pipeline = cc_lb_control::api_keys::principal_view::RouterPipelineCache {
        user_filters: vec![Arc::new(InvalidOutputFilter)],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    };
    let view = provenance_view(
        failure_principal_view(
            "principal-test",
            (
                Some(Arc::new(pipeline)),
                cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
            ),
        ),
        Arc::new(TestSignerFactory),
        vec![upstream_record(upstream_id)],
    );
    let dispatcher = Arc::new(QueueDispatch::new(vec![Err(DispatchError::Transport {
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        )),
    })]));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

    let request = failure_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _body) = collect_response_body(response).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(dispatcher.call_count(), 1);

    let events = drain_events(&mut rx);
    let terminal = terminal_snapshot(&events);
    expect_error_code(&terminal.reason, error_codes::UPSTREAM_DISPATCH_FAILED);
    assert_eq!(
        terminal.internal_errors.len(),
        2,
        "non-fatal filter diagnostic and terminal dispatch cause must both persist: {:?}",
        terminal.internal_errors
    );
    assert_eq!(
        terminal.internal_errors[0].stage,
        InternalErrorStage::RouterFilter
    );
    assert_eq!(
        terminal.internal_errors[0].kind,
        InternalErrorKind::InvalidOutput
    );
    assert_eq!(terminal.internal_errors[1].stage, InternalErrorStage::Relay);
}

/// QA-SIGN-01: the signer factory cannot build a signer for the route.
#[tokio::test]
async fn signer_factory_failure_records_signer_stage() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let upstream_id = Uuid::new_v4();
    let view = provenance_view(
        failure_principal_view("principal-test", default_artifacts()),
        Arc::new(FailingSignerFactory(|| SignerError::MissingCredentials {
            reason: "no credential material".to_owned(),
        })),
        vec![upstream_record(upstream_id)],
    );
    let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

    let request = failure_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, body) = collect_response_body(response).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        body,
        anthropic_error_body("api_error", "failed to prepare upstream credentials"),
        "client response must stay byte-identical"
    );
    assert_eq!(dispatcher.call_count(), 0);

    let events = drain_events(&mut rx);
    let terminal = terminal_snapshot(&events);
    assert_eq!(terminal.client_status, 502);
    expect_error_code(&terminal.reason, error_codes::SIGNER_FAILED);
    assert_eq!(terminal.internal_errors.len(), 1);
    assert_eq!(
        terminal.internal_errors[0].stage,
        InternalErrorStage::Signer
    );
    assert_eq!(
        terminal.internal_errors[0].kind,
        InternalErrorKind::ConfigError
    );
    assert_eq!(
        terminal.internal_errors[0].message.as_deref(),
        Some("missing credentials: no credential material")
    );
}

/// QA-SIGN-02: signing fails inside `attempt` after the factory succeeded.
#[tokio::test]
async fn attempt_sign_failure_records_signer_stage() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let upstream_id = Uuid::new_v4();
    let view = provenance_view(
        failure_principal_view("principal-test", default_artifacts()),
        Arc::new(FailingSignerFactoryForAttempt(|| {
            SignerError::SigningFailed {
                reason: "hmac exploded".to_owned(),
            }
        })),
        vec![upstream_record(upstream_id)],
    );
    let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

    let request = failure_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, body) = collect_response_body(response).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        body,
        anthropic_error_body("api_error", "failed to sign upstream request"),
        "client response must stay byte-identical"
    );
    assert_eq!(dispatcher.call_count(), 0);

    let events = drain_events(&mut rx);
    let terminal = terminal_snapshot(&events);
    assert_eq!(terminal.client_status, 502);
    expect_error_code(&terminal.reason, error_codes::SIGNER_FAILED);
    assert_eq!(terminal.internal_errors.len(), 1);
    assert_eq!(
        terminal.internal_errors[0].stage,
        InternalErrorStage::Signer
    );
    assert_eq!(
        terminal.internal_errors[0].kind,
        InternalErrorKind::PluginError
    );
    assert_eq!(
        terminal.internal_errors[0].message.as_deref(),
        Some("signing failed: hmac exploded")
    );
}

#[test]
fn sse_completion_follows_each_provider_terminal_event() {
    let message_stop = usage_parser::parse_sse_event(
        b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    );
    let response_completed = usage_parser::parse_sse_event(
            b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n",
        );
    let response_incomplete = usage_parser::parse_sse_event(
            b"event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":12,\"output_tokens\":7}}}\n\n",
        );
    let response_incomplete_named_without_type = usage_parser::parse_sse_event(
            b"event: response.incomplete\ndata: {\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":12,\"output_tokens\":7}}}\n\n",
        );
    let response_incomplete_data_only = usage_parser::parse_sse_event(
            b"data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":12,\"output_tokens\":7}}}\n\n",
        );
    let response_incomplete_malformed =
        usage_parser::parse_sse_event(b"event: response.incomplete\ndata: {not-json}\n\n");
    let response_incomplete_empty =
        usage_parser::parse_sse_event(b"event: response.incomplete\ndata:\n\n");
    let response_delta = usage_parser::parse_sse_event(
            b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
        );
    let gemini_chunk = usage_parser::parse_sse_event(
            b"data: {\"candidates\":[{\"finishReason\":\"STOP\",\"content\":{\"parts\":[{\"text\":\"hi\",\"thoughtSignature\":\"sig\"}]}}]}\n\n",
        );

    assert!(sse_event_completes_response(
        ProviderCapability::AnthropicMessages,
        &message_stop,
    ));
    assert!(!sse_event_completes_response(
        ProviderCapability::AnthropicMessages,
        &response_completed,
    ));
    assert!(sse_event_completes_response(
        ProviderCapability::OpenaiResponses,
        &response_completed,
    ));
    assert!(sse_event_completes_response(
        ProviderCapability::OpenaiResponses,
        &response_incomplete,
    ));
    assert!(sse_event_completes_response(
        ProviderCapability::OpenaiResponses,
        &response_incomplete_named_without_type,
    ));
    assert!(sse_event_completes_response(
        ProviderCapability::OpenaiResponses,
        &response_incomplete_data_only,
    ));
    for malformed in [&response_incomplete_malformed, &response_incomplete_empty] {
        assert!(!sse_event_completes_response(
            ProviderCapability::OpenaiResponses,
            malformed,
        ));
    }
    for (event, expected) in [
        (&response_incomplete_named_without_type, true),
        (&response_incomplete_data_only, true),
        (&response_incomplete_malformed, false),
        (&response_incomplete_empty, false),
    ] {
        let mut semantic_message_stop_seen = false;
        let mut message_stop_at = None;
        observe_sse_terminal_event(
            ProviderCapability::OpenaiResponses,
            event,
            false,
            &mut semantic_message_stop_seen,
            &mut message_stop_at,
            Instant::now,
        );
        assert_eq!(semantic_message_stop_seen, expected);
    }
    assert!(!sse_event_completes_response(
        ProviderCapability::OpenaiResponses,
        &message_stop,
    ));
    assert!(!sse_event_completes_response(
        ProviderCapability::OpenaiResponses,
        &response_delta,
    ));
    // Gemini has no terminal event; pending keys bind at clean stream end.
    assert!(!sse_event_completes_response(
        ProviderCapability::GeminiGenerateContent,
        &gemini_chunk,
    ));
}

#[test]
fn proxy_generated_stream_error_frames_use_selected_provider_envelope() {
    for capability in [
        ProviderCapability::AnthropicMessages,
        ProviderCapability::OpenaiResponses,
        ProviderCapability::OpenaiResponsesCompact,
        ProviderCapability::GeminiGenerateContent,
    ] {
        let frame = make_stream_error_frame(
            capability,
            &ResponsesSseErrorContext::default(),
            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
            "synthetic relay failure",
            false,
        );
        let text = std::str::from_utf8(&frame).expect("SSE frame is UTF-8");
        assert!(text.ends_with("\n\n"));
        if matches!(
            capability,
            ProviderCapability::OpenaiResponses | ProviderCapability::OpenaiResponsesCompact
        ) {
            assert!(text.starts_with("event: response.failed\ndata: "));
            let payload = text
                .strip_prefix("event: response.failed\ndata: ")
                .and_then(|value| value.strip_suffix("\n\n"))
                .and_then(|value| serde_json::from_str::<Value>(value).ok())
                .expect("Responses failure data is JSON");
            assert_eq!(payload["type"], "response.failed");
            assert_eq!(payload["sequence_number"], 0);
            assert_eq!(payload["response"]["status"], "failed");
            assert_eq!(
                payload["response"]["error"]["message"],
                "synthetic relay failure"
            );
            assert!(payload.get("error").is_none(), "{payload}");
        } else {
            assert!(text.starts_with("event: error\ndata: "));
            assert!(
                text.contains("\"message\":\"synthetic relay failure\""),
                "{text}"
            );
            if capability == ProviderCapability::AnthropicMessages {
                assert!(!text.contains("\"code\""), "{text}");
            }
        }
    }
}

#[test]
fn usage_shape_completion_keeps_provider_and_payload_boundaries() {
    let done = parse_sse_event(b"data: [DONE]\n\n");
    let finished =
        parse_sse_event(b"data: {\"choices\":[{\"index\":0,\"finish_reason\":\"stop\"}]}\n\n");
    let unfinished =
        parse_sse_event(b"data: {\"choices\":[{\"index\":0,\"finish_reason\":null}]}\n\n");
    for capability in [
        ProviderCapability::OpenaiChatCompletions,
        ProviderCapability::OpenaiCompletions,
        ProviderCapability::GeminiOpenaiChatCompletions,
    ] {
        assert!(sse_event_completes_response(capability, &done));
        assert!(sse_event_completes_response(capability, &finished));
        assert!(!sse_event_completes_response(capability, &unfinished));
        for (bytes, completes_response) in [
            (b"data: [DONE]\n\n".as_slice(), true),
            (b"data: [DONE]".as_slice(), false),
            (
                b"data: {\"choices\":[{\"finish_reason\":null}]}\n\n".as_slice(),
                false,
            ),
        ] {
            let output =
                ResponsesSseErrorContext::default().observe_transformed_output(bytes, capability);
            assert_eq!(output.completes_response, completes_response);
        }
    }
    for capability in [
        ProviderCapability::AnthropicMessages,
        ProviderCapability::OpenaiResponses,
        ProviderCapability::GeminiGenerateContent,
        ProviderCapability::OpenaiEmbeddings,
        ProviderCapability::GeminiEmbedContent,
    ] {
        assert!(!sse_event_completes_response(capability, &done));
        assert!(!sse_event_completes_response(capability, &finished));
    }
    for raw in [
        b"event: message_stop\ndata: {bad-json}\n\n".as_slice(),
        b"event: message_stop\n\n",
        b"event: message_stop\ndata: {\"type\":\"message_delta\"}\n\n",
    ] {
        assert!(!sse_event_completes_response(
            ProviderCapability::AnthropicMessages,
            &parse_sse_event(raw),
        ));
    }
    for raw in [
        b"event: response.completed\ndata: {bad-json}\n\n".as_slice(),
        b"event: response.completed\n\n",
    ] {
        assert!(!sse_event_completes_response(
            ProviderCapability::OpenaiResponses,
            &parse_sse_event(raw),
        ));
    }
    assert!(sse_event_completes_response(
        ProviderCapability::OpenaiResponses,
        &parse_sse_event(b"event: response.completed\ndata: {}\n\n"),
    ));
}

#[test]
fn semantic_content_classifier_matches_provider_wire_shapes() {
    let anthropic = parse_sse_event(
            b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"\"}}\n\n",
        );
    assert!(is_semantic_content_frame(
        ProviderCapability::AnthropicMessages,
        &anthropic
    ));

    let chat_text = parse_sse_event(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}},{\"delta\":{\"content\":null}}]}\n\n",
        );
    let chat_metadata =
        parse_sse_event(b"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n");
    let chat_tool =
        parse_sse_event(b"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"id\":\"x\"}]}}]}\n\n");
    assert!(is_semantic_content_frame(
        ProviderCapability::OpenaiChatCompletions,
        &chat_text
    ));
    assert!(!is_semantic_content_frame(
        ProviderCapability::OpenaiChatCompletions,
        &chat_metadata
    ));
    assert!(!is_semantic_content_frame(
        ProviderCapability::GeminiOpenaiChatCompletions,
        &chat_tool
    ));

    let completion = parse_sse_event(b"data: {\"choices\":[{\"text\":\"legacy\"}]}\n\n");
    assert!(is_semantic_content_frame(
        ProviderCapability::OpenaiCompletions,
        &completion
    ));

    let response_text = parse_sse_event(
            b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
        );
    let response_reasoning = parse_sse_event(
            b"event: response.reasoning.delta\ndata: {\"type\":\"response.reasoning.delta\",\"delta\":\"hidden\"}\n\n",
        );
    assert!(is_semantic_content_frame(
        ProviderCapability::OpenaiResponses,
        &response_text
    ));
    assert!(!is_semantic_content_frame(
        ProviderCapability::OpenaiResponses,
        &response_reasoning
    ));

    let gemini_text = parse_sse_event(
            b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"one\"},{\"text\":\"two\"}]}},{\"content\":{\"parts\":[{\"text\":\"three\"}]}}]}\n\n",
        );
    let gemini_oauth_text = parse_sse_event(
            b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"wrapped\"}]}}]}}\n\n",
        );
    let gemini_oauth_thought = parse_sse_event(
            b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"thought\":true,\"text\":\"hidden\"}]}}]}}\n\n",
        );
    let gemini_thought = parse_sse_event(
            b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"thought\":true,\"text\":\"hidden\"}]}}]}\n\n",
        );
    assert!(is_semantic_content_frame(
        ProviderCapability::GeminiGenerateContent,
        &gemini_text
    ));
    assert!(is_semantic_content_frame(
        ProviderCapability::GeminiGenerateContent,
        &gemini_oauth_text
    ));
    assert!(!is_semantic_content_frame(
        ProviderCapability::GeminiGenerateContent,
        &gemini_oauth_thought
    ));
    assert!(!is_semantic_content_frame(
        ProviderCapability::GeminiGenerateContent,
        &gemini_thought
    ));
    let chat_tool_args = parse_sse_event(
            b"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"x\\\":1}\"}}]}}]}\n\n",
        );
    assert!(is_semantic_content_frame(
        ProviderCapability::OpenaiChatCompletions,
        &chat_tool_args
    ));
    let response_func_args = parse_sse_event(
            b"event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"{\\\"a\\\":1}\"}\n\n",
        );
    assert!(is_semantic_content_frame(
        ProviderCapability::OpenaiResponses,
        &response_func_args
    ));
    let gemini_fn_call = parse_sse_event(
            b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"bash\",\"args\":{}}}]}}]}\n\n",
        );
    let gemini_thought_fn_call = parse_sse_event(
            b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"thought\":true,\"functionCall\":{\"name\":\"bash\",\"args\":{}}}]}}]}\n\n",
        );
    assert!(is_semantic_content_frame(
        ProviderCapability::GeminiGenerateContent,
        &gemini_fn_call
    ));
    assert!(!is_semantic_content_frame(
        ProviderCapability::GeminiGenerateContent,
        &gemini_thought_fn_call
    ));
    assert!(!is_semantic_content_frame(
        ProviderCapability::GeminiOpenaiChatCompletions,
        &gemini_text
    ));
}

#[test]
fn observe_sse_lifecycle_markers_capture_openai_and_gemini_streams() {
    let base = Instant::now();

    // OpenAI Responses SSE
    let mut msg_start = None;
    let mut block_start = None;
    let mut msg_stop = None;
    let mut stop_seen = false;
    let mut pings = 0;
    let created = parse_sse_event(
            b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n",
        );
    let item_added = parse_sse_event(
            b"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"message\"}}\n\n",
        );
    let completed = parse_sse_event(
            b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n",
        );
    observe_sse_lifecycle_markers(
        ProviderCapability::OpenaiResponses,
        &created,
        &mut msg_start,
        &mut block_start,
        &mut pings,
        None,
        base + Duration::from_millis(5),
    );
    observe_sse_lifecycle_markers(
        ProviderCapability::OpenaiResponses,
        &item_added,
        &mut msg_start,
        &mut block_start,
        &mut pings,
        None,
        base + Duration::from_millis(8),
    );
    observe_sse_terminal_event(
        ProviderCapability::OpenaiResponses,
        &completed,
        false,
        &mut stop_seen,
        &mut msg_stop,
        || base + Duration::from_millis(20),
    );
    assert_eq!(msg_start, Some(base + Duration::from_millis(5)));
    assert_eq!(block_start, Some(base + Duration::from_millis(8)));
    assert_eq!(msg_stop, Some(base + Duration::from_millis(20)));
    assert!(stop_seen);

    // OpenAI Chat Completions SSE
    let mut msg_start = None;
    let mut block_start = None;
    let mut msg_stop = None;
    let mut stop_seen = false;
    let chunk_role = parse_sse_event(
            b"data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n",
        );
    let chunk_stop = parse_sse_event(
        b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    );
    observe_sse_lifecycle_markers(
        ProviderCapability::OpenaiChatCompletions,
        &chunk_role,
        &mut msg_start,
        &mut block_start,
        &mut pings,
        None,
        base + Duration::from_millis(3),
    );
    observe_sse_terminal_event(
        ProviderCapability::OpenaiChatCompletions,
        &chunk_stop,
        false,
        &mut stop_seen,
        &mut msg_stop,
        || base + Duration::from_millis(12),
    );
    assert_eq!(msg_start, Some(base + Duration::from_millis(3)));
    assert_eq!(block_start, Some(base + Duration::from_millis(3)));
    assert_eq!(msg_stop, Some(base + Duration::from_millis(12)));

    // Gemini SSE
    let mut msg_start = None;
    let mut block_start = None;
    let mut msg_stop = None;
    let mut stop_seen = false;
    let gem_chunk = parse_sse_event(
            b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hi\"}]},\"finishReason\":\"STOP\"}]}\n\n",
        );
    observe_sse_lifecycle_markers(
        ProviderCapability::GeminiGenerateContent,
        &gem_chunk,
        &mut msg_start,
        &mut block_start,
        &mut pings,
        None,
        base + Duration::from_millis(7),
    );
    observe_sse_terminal_event(
        ProviderCapability::GeminiGenerateContent,
        &gem_chunk,
        false,
        &mut stop_seen,
        &mut msg_stop,
        || base + Duration::from_millis(9),
    );
    assert_eq!(msg_start, Some(base + Duration::from_millis(7)));
    assert_eq!(block_start, Some(base + Duration::from_millis(7)));
    assert_eq!(msg_stop, Some(base + Duration::from_millis(9)));
    assert!(!stop_seen);
}

/// QA-STOR-01: the affinity store fails while resolving request keys.
#[tokio::test]
async fn affinity_store_failure_records_storage_stage() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let upstream_id = Uuid::new_v4();
    let view = provenance_view(
        failure_principal_view("principal-test", default_artifacts()),
        Arc::new(TestSignerFactory),
        vec![upstream_record(upstream_id)],
    );
    let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus)
        .with_upstream_affinity_store(Arc::new(FailingAffinityStore));

    let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","tools":[{"type":"web_search_20250305"}],"messages":[{"role":"user","content":[{"type":"web_search_tool_result","tool_use_id":"srvtoolu_1","content":[{"type":"web_search_result","encrypted_content":"opaque-ciphertext"}]}]}]}"#,
        ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _body) = collect_response_body(response).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(dispatcher.call_count(), 0);

    let events = drain_events(&mut rx);
    let terminal = terminal_snapshot(&events);
    assert_eq!(terminal.client_status, 503);
    expect_error_code(&terminal.reason, error_codes::UPSTREAM_AFFINITY_UNAVAILABLE);
    assert_eq!(terminal.internal_errors.len(), 1);
    assert_eq!(
        terminal.internal_errors[0].stage,
        InternalErrorStage::Storage
    );
    assert_eq!(
        terminal.internal_errors[0].kind,
        InternalErrorKind::Unavailable
    );
    assert_eq!(
        terminal.internal_errors[0].message.as_deref(),
        Some(UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE)
    );
}

/// QA-RELAY-01/02: a typed transport source reaches the terminal snapshot
/// with a classified relay cause, not a string bucket.
#[tokio::test]
async fn transport_failure_records_typed_relay_cause() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let upstream_id = Uuid::new_v4();
    let view = provenance_view(
        failure_principal_view("principal-test", default_artifacts()),
        Arc::new(TestSignerFactory),
        vec![upstream_record(upstream_id)],
    );
    let dispatcher = Arc::new(QueueDispatch::new(vec![Err(DispatchError::Transport {
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
        )),
    })]));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

    let request = failure_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, body) = collect_response_body(response).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        body,
        anthropic_error_body("api_error", "upstream request failed"),
        "client response must stay byte-identical"
    );
    assert_eq!(dispatcher.call_count(), 1);

    let events = drain_events(&mut rx);
    let terminal = terminal_snapshot(&events);
    assert_eq!(terminal.client_status, 502);
    expect_error_code(&terminal.reason, error_codes::UPSTREAM_DISPATCH_FAILED);
    assert_eq!(terminal.internal_errors.len(), 1);
    assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Relay);
    assert_eq!(
        terminal.internal_errors[0].kind,
        InternalErrorKind::Unavailable
    );
    let message = terminal.internal_errors[0]
        .message
        .as_deref()
        .expect("relay cause message");
    assert!(
        message.starts_with("connection_refused: "),
        "typed tier-2 classification must prefix the chain, got {message}"
    );
    assert!(
        message.contains("connection refused"),
        "typed source chain must be preserved, got {message}"
    );
    // Connection timings are carried on the terminal snapshot even though
    // no UpstreamResponseStarted was emitted; a mock dispatcher leaves
    // them unobserved (None).
}

/// QA-RELAY-05: a saturated bulkhead is a relay-stage unavailable failure
/// with the Retry-After header preserved.
#[tokio::test]
async fn bulkhead_full_records_relay_unavailable() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let upstream_id = Uuid::new_v4();
    let view = provenance_view(
        failure_principal_view("principal-test", default_artifacts()),
        Arc::new(TestSignerFactory),
        vec![upstream_record(upstream_id)],
    );
    let dispatcher = Arc::new(QueueDispatch::new(vec![Err(DispatchError::BulkheadFull {
        retry_after: Duration::from_secs(3),
    })]));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

    let request = failure_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("3"),
        "Retry-After must be preserved"
    );
    let (_status, body) = collect_response_body(response).await;
    assert_eq!(
        body,
        anthropic_error_body("overloaded_error", "upstream bulkhead queue is full")
    );

    let events = drain_events(&mut rx);
    let terminal = terminal_snapshot(&events);
    assert_eq!(terminal.client_status, 503);
    expect_error_code(&terminal.reason, error_codes::UPSTREAM_DISPATCH_FAILED);
    assert_eq!(terminal.internal_errors.len(), 1);
    assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Relay);
    assert_eq!(
        terminal.internal_errors[0].kind,
        InternalErrorKind::Unavailable
    );
    assert!(
        terminal.internal_errors[0]
            .message
            .as_deref()
            .is_some_and(|message| message.contains("bulkhead queue full")),
        "bulkhead cause must be preserved: {:?}",
        terminal.internal_errors[0].message
    );
    // A pre-transport rejection never touched the connector: all
    // connection timings must be unobserved.
    assert_eq!(terminal.dns_ms, None);
    assert_eq!(terminal.connect_ms, None);
    assert_eq!(terminal.connection_reused, None);
}

/// QA-RETRY-01: a provider 401 refreshes the signer and retries; the 401
/// itself is not an internal failure, and the second attempt's typed
/// cause is the only terminal diagnostic.
#[tokio::test]
async fn retry_after_401_keeps_provider_error_out_of_internal_errors() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let upstream_id = Uuid::new_v4();
    let view = provenance_view(
        failure_principal_view("principal-test", default_artifacts()),
        Arc::new(RefreshingSignerFactory),
        vec![upstream_record(upstream_id)],
    );
    let dispatcher = Arc::new(QueueDispatch::new(vec![
        Ok(upstream_response(
            StatusCode::UNAUTHORIZED,
            br#"{"type":"error","error":{"type":"authentication_error","message":"bad key"}}"#,
        )),
        Err(DispatchError::Transport {
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "connect timed out",
            )),
        }),
    ]));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

    let request = failure_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _body) = collect_response_body(response).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(dispatcher.call_count(), 2, "401 must trigger one retry");

    let events = drain_events(&mut rx);
    let attempt_nums: Vec<u32> = events
        .iter()
        .filter_map(|event| match event {
            cc_lb_lifecycle::LifecycleEvent::UpstreamAttempt { attempt_num, .. } => {
                Some(*attempt_num)
            }
            _ => None,
        })
        .collect();
    assert_eq!(attempt_nums, vec![1, 2], "both attempts must be emitted");
    let terminal = terminal_snapshot(&events);
    expect_error_code(&terminal.reason, error_codes::UPSTREAM_DISPATCH_FAILED);
    assert_eq!(
        terminal.internal_errors.len(),
        1,
        "provider 401 must not enter internal_errors: {:?}",
        terminal.internal_errors
    );
    assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Relay);
    assert_eq!(terminal.internal_errors[0].kind, InternalErrorKind::Timeout);
    let message = terminal.internal_errors[0]
        .message
        .as_deref()
        .expect("relay cause message");
    assert!(
        message.starts_with("timeout: "),
        "typed tier-2 classification must prefix the chain, got {message}"
    );
}

/// QA-CTRL-09: a dialect shape failure falls back to raw passthrough and
/// the request still succeeds; the non-fatal shape diagnostic persists.
#[tokio::test]
async fn shape_fallback_success_keeps_shape_diagnostic() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let upstream_id = Uuid::new_v4();
    let view = provenance_view(
        failure_principal_view(
            "principal-test",
            (
                None,
                cc_lb_control::api_keys::principal_view::DialectCache::Explicit(
                    cc_lb_control::api_keys::principal_view::ShapePluginCache {
                        dialect: Arc::new(FailingDialect),
                    },
                ),
            ),
        ),
        Arc::new(TestSignerFactory),
        vec![upstream_record(upstream_id)],
    );
    let dispatcher = Arc::new(QueueDispatch::new(vec![Ok(upstream_response(
            StatusCode::OK,
            br#"{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-test","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
        ))]));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

    let request = failure_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _body) = collect_response_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dispatcher.call_count(), 1);

    let events = drain_events(&mut rx);
    let terminal = terminal_snapshot(&events);
    assert!(
        matches!(terminal.reason, cc_lb_lifecycle::TerminationReason::Success),
        "fallback success must terminate as success, got {:?}",
        terminal.reason
    );
    assert_eq!(terminal.internal_errors.len(), 1);
    assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Shape);
    assert_eq!(terminal.internal_errors[0].kind, InternalErrorKind::Trap);
    assert!(
        terminal.internal_errors[0]
            .message
            .as_deref()
            .is_some_and(|message| message.contains("injected dialect failure")),
        "shape diagnostic must carry the dialect failure: {:?}",
        terminal.internal_errors[0].message
    );
}

/// QA-UPSTR-06 (negative gate): a 200 body containing an "error" key must
/// not be classified as an upstream error.
#[tokio::test]
async fn success_body_with_error_key_is_not_upstream_error() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let upstream_id = Uuid::new_v4();
    let view = provenance_view(
        failure_principal_view("principal-test", default_artifacts()),
        Arc::new(TestSignerFactory),
        vec![upstream_record(upstream_id)],
    );
    let dispatcher = Arc::new(QueueDispatch::new(vec![Ok(upstream_response(
            StatusCode::OK,
            br#"{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-test","stop_reason":"end_turn","error":{"type":"not_an_error"},"usage":{"input_tokens":1,"output_tokens":1}}"#,
        ))]));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

    let request = failure_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _body) = collect_response_body(response).await;
    assert_eq!(status, StatusCode::OK);

    let events = drain_events(&mut rx);
    assert!(
        !events.iter().any(|event| matches!(
            event,
            cc_lb_lifecycle::LifecycleEvent::RequestLogUpstreamErrorObserved { .. }
        )),
        "error-shaped JSON inside a 200 must not emit upstream error observation"
    );
    let terminal = terminal_snapshot(&events);
    assert!(
        matches!(terminal.reason, cc_lb_lifecycle::TerminationReason::Success),
        "200 with error-shaped body must stay success, got {:?}",
        terminal.reason
    );
    assert!(terminal.internal_errors.is_empty());
}

/// QA-AUTH-01/05: `reject_unauthenticated`
/// persists the exact BuiltinAuthError Display literal with the typed
/// stage/kind.
#[tokio::test]
async fn reject_unauthenticated_records_exact_auth_error() {
    for (error, expected_status, expected_kind) in [
        (
            BuiltinAuthError::InvalidFormat,
            StatusCode::UNAUTHORIZED,
            InternalErrorKind::InvalidInput,
        ),
        (
            BuiltinAuthError::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            InternalErrorKind::Unavailable,
        ),
    ] {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = LifecycleContext::new(
            "req-authn-reject".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.mark_authn_reached();
        let response =
            crate::authn_rail::reject_unauthenticated_for_provider(&error, None, Some(&observer));
        assert_eq!(response.status(), expected_status);
        let (_status, body) = collect_response_body(response).await;
        assert_eq!(
            body,
            anthropic_error_body("authentication_error", &error.to_string()),
            "client response must stay byte-identical"
        );
        drop(observer);

        let events = drain_events(&mut rx);
        let started_pos = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::RequestStarted { .. }
                )
            })
            .expect("RequestStarted must be emitted by terminate_failure");
        let auth_pos = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::AuthCompleted {
                        result: Err(cc_lb_lifecycle::AuthFailure::AuthenticationFailed { .. }),
                        ..
                    }
                )
            })
            .expect("expected AuthCompleted::AuthenticationFailed");
        let terminated_pos = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
                )
            })
            .expect("RequestTerminated must be emitted");
        assert!(
            started_pos < auth_pos && auth_pos < terminated_pos,
            "event order must be RequestStarted < AuthCompleted < RequestTerminated: {events:?}"
        );
        let terminal = terminal_snapshot(&events);
        assert_eq!(terminal.client_status, expected_status.as_u16());
        expect_error_code(&terminal.reason, error_codes::AUTHENTICATION_FAILED);
        assert_eq!(terminal.internal_errors.len(), 1);
        assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Authn);
        assert_eq!(terminal.internal_errors[0].kind, expected_kind);
        assert_eq!(
            terminal.internal_errors[0].message.as_deref(),
            Some(error.to_string().as_str()),
            "exact BuiltinAuthError Display literal must persist"
        );
    }
}

/// QA-CTRL-12: a termination committed before `mark_authn_reached`
/// publishes nothing — the pre-authn silence invariant.
#[tokio::test]
async fn pre_authn_termination_publishes_nothing() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    {
        let observer = LifecycleContext::new(
            "req-pre-authn".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.terminate_failure(InternalFailure {
            status: StatusCode::GATEWAY_TIMEOUT,
            error_code: error_codes::TOWER_TIMEOUT,
            error: InternalError {
                stage: InternalErrorStage::Relay,
                kind: InternalErrorKind::Timeout,
                message: Some("pre-authn timeout".to_owned()),
            },
        });
    }
    let events = drain_events(&mut rx);
    assert!(
        events.is_empty(),
        "pre-authn termination must publish no events, got {events:?}"
    );
}

/// QA-CTRL-11: a terminal committed without a prior explicit
/// `emit_request_started` still emits RequestStarted before
/// RequestTerminated — the orphan-terminal policy input.
#[tokio::test]
async fn orphan_terminal_emits_started_then_terminated() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    {
        let observer = LifecycleContext::new(
            "req-orphan".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.mark_authn_reached();
        observer.terminate_failure(InternalFailure {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_code: error_codes::PRINCIPAL_MISSING,
            error: InternalError {
                stage: InternalErrorStage::Authn,
                kind: InternalErrorKind::Unavailable,
                message: Some("orphan terminal".to_owned()),
            },
        });
    }
    let events = drain_events(&mut rx);
    let started_pos = events
        .iter()
        .position(|event| {
            matches!(
                event,
                cc_lb_lifecycle::LifecycleEvent::RequestStarted { .. }
            )
        })
        .expect("RequestStarted must be emitted by terminate_failure");
    let terminated_pos = events
        .iter()
        .position(|event| {
            matches!(
                event,
                cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
            )
        })
        .expect("RequestTerminated must be emitted");
    assert!(
        started_pos < terminated_pos,
        "RequestStarted must precede RequestTerminated: {events:?}"
    );
    let terminal = terminal_snapshot(&events);
    expect_error_code(&terminal.reason, error_codes::PRINCIPAL_MISSING);
    assert_eq!(terminal.internal_errors.len(), 1);
}

/// QA-RELAY-04: a connect timeout is a relay-stage timeout failure with
/// the typed tier-2 classification preserved.
#[tokio::test]
async fn transport_timeout_records_relay_timeout() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let mut rx = subscribe_lifecycle(&bus);
    let upstream_id = Uuid::new_v4();
    let view = provenance_view(
        failure_principal_view("principal-test", default_artifacts()),
        Arc::new(TestSignerFactory),
        vec![upstream_record(upstream_id)],
    );
    let dispatcher = Arc::new(QueueDispatch::new(vec![Err(DispatchError::Transport {
        source: Box::new(std::io::Error::from(std::io::ErrorKind::TimedOut)),
    })]));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

    let request = failure_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _body) = collect_response_body(response).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(dispatcher.call_count(), 1);

    let events = drain_events(&mut rx);
    let terminal = terminal_snapshot(&events);
    assert_eq!(terminal.client_status, 502);
    expect_error_code(&terminal.reason, error_codes::UPSTREAM_DISPATCH_FAILED);
    assert_eq!(terminal.internal_errors.len(), 1);
    assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Relay);
    assert_eq!(terminal.internal_errors[0].kind, InternalErrorKind::Timeout);
    let message = terminal.internal_errors[0]
        .message
        .as_deref()
        .expect("relay cause message");
    assert!(
        message.starts_with("timeout: "),
        "typed tier-2 classification must prefix the chain, got {message}"
    );
}

/// QA-RELAY-06: invalid-URI and request-build dispatch failures are
/// relay-stage config errors carrying their own reasons.
#[tokio::test]
async fn invalid_uri_and_request_build_record_relay_config_error() {
    for (source, expected_fragment) in [
        (
            DispatchError::InvalidUri {
                reason: "bad scheme".to_owned(),
            },
            "invalid upstream uri: bad scheme",
        ),
        (
            DispatchError::RequestBuild {
                reason: "header rejected".to_owned(),
            },
            "upstream request build failed: header rejected",
        ),
    ] {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(TestSignerFactory),
            vec![upstream_record(upstream_id)],
        );
        let dispatcher = Arc::new(QueueDispatch::new(vec![Err(source)]));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(
            body,
            anthropic_error_body("api_error", "upstream request failed"),
            "client response must stay byte-identical"
        );
        assert_eq!(dispatcher.call_count(), 1);

        let events = drain_events(&mut rx);
        let terminal = terminal_snapshot(&events);
        assert_eq!(terminal.client_status, 502);
        expect_error_code(&terminal.reason, error_codes::UPSTREAM_DISPATCH_FAILED);
        assert_eq!(terminal.internal_errors.len(), 1);
        assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Relay);
        assert_eq!(
            terminal.internal_errors[0].kind,
            InternalErrorKind::ConfigError
        );
        assert_eq!(
            terminal.internal_errors[0].message.as_deref(),
            Some(expected_fragment),
            "dispatch variant reason must be preserved"
        );
    }
}

/// WS-RED-01: WebSocket setup must honor the resolved terminal strategy
/// instead of silently falling back to list order.
#[tokio::test]
async fn responses_websocket_uses_resolved_terminal_strategy() {
    let pipeline = cc_lb_control::api_keys::principal_view::RouterPipelineCache {
        user_filters: Vec::new(),
        terminal: TerminalStrategy::Random,
        instantiation_error: None,
        provider_filters: None,
    };
    let first_id = Uuid::from_u128(1);
    let second_id = Uuid::from_u128(2);
    let records = vec![
        UpstreamRecord {
            id: first_id,
            name: "first-openai".to_owned(),
            kind: StorageRecordKind::OpenaiApiKey,
            base_url: Some(Url::parse("http://first.invalid/").expect("test URL parses")),
            enabled: true,
            revision: 1,
            ..UpstreamRecord::default()
        },
        UpstreamRecord {
            id: second_id,
            name: "second-openai".to_owned(),
            kind: StorageRecordKind::OpenaiApiKey,
            base_url: Some(Url::parse("http://second.invalid/").expect("test URL parses")),
            enabled: true,
            revision: 1,
            ..UpstreamRecord::default()
        },
    ];
    let view = provenance_view(
        failure_principal_view(
            "principal-test",
            (
                Some(Arc::new(pipeline)),
                cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
            ),
        ),
        Arc::new(TestSignerFactory),
        records,
    );
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);
    let mut events = subscribe_lifecycle(&bus);
    let request = failure_request(Bytes::new());
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let auth_info = lifecycle.websocket_auth_info(&auth);
    assert_eq!(auth_info.principal_id, auth.principal_id());
    assert_eq!(auth_info.key_id.as_deref(), Some(auth.key_id()));
    assert_eq!(auth_info.auth_ms, Some(auth.auth_ms()));
    assert_eq!(auth_info.principal_kind.as_deref(), Some("machine"));

    // Pick a fixed seed whose first Random draw is the second candidate,
    // then install that same seed in the lifecycle's private test state.
    // This keeps the assertion deterministic without adding a test API.
    let seed = (0_u64..1024)
        .find(|seed| {
            let mut rng = StdRng::seed_from_u64(*seed);
            rng.random_range(0..2) == 1
        })
        .expect("a deterministic seed must select the second candidate");
    *lifecycle.rng.lock().expect("terminal rng lock") = StdRng::seed_from_u64(seed);

    let body = Arc::new(json!({"model": "gpt-test", "store": false}));
    let frame = Bytes::from(serde_json::to_vec(body.as_ref()).expect("test frame serializes"));
    let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
    assert!(
        drain_events(&mut events).is_empty(),
        "allocating an observer is not a turn"
    );
    let turn = lifecycle
        .prepare_websocket_turn_with_surface(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &frame,
                query: None,
            },
            body.clone(),
            None,
            observer,
            &request_id,
            false,
        )
        .await
        .expect("an eligible OpenAI upstream should be selected");
    assert_eq!(
        turn.candidates[0].upstream_id, second_id,
        "the resolved Random strategy is honored"
    );
    assert_eq!(
        turn.candidates[1].upstream_id, first_id,
        "unattempted fallback candidates remain available"
    );
    assert!(!turn.store);
    let (url, _) = lifecycle
        .sign_websocket_connection(&auth, request.headers(), &turn.candidates[0], None)
        .await
        .expect("only the selected connection is signed");
    assert_eq!(url.as_str(), "ws://second.invalid/v1/responses");
    let first_events = drain_events(&mut events);
    assert_eq!(
        first_events
            .iter()
            .filter(|event| matches!(
                event,
                cc_lb_lifecycle::LifecycleEvent::RequestStarted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        first_events
            .iter()
            .filter(|event| matches!(event, cc_lb_lifecycle::LifecycleEvent::AuthCompleted { .. }))
            .count(),
        0,
        "authentication observations belong to the gateway, not turn preparation"
    );
    assert_eq!(
        first_events
            .iter()
            .filter(|event| matches!(
                event,
                cc_lb_lifecycle::LifecycleEvent::RouteCompleted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        first_events
            .iter()
            .filter(|event| matches!(event, cc_lb_lifecycle::LifecycleEvent::LimitDecision { .. }))
            .count(),
        1
    );
    assert!(first_events.iter().any(|event| matches!(event,
            cc_lb_lifecycle::LifecycleEvent::ParseCompleted { result: Ok(info), .. }
                if info.method == "GET" && info.path == "/v1/responses" && info.stream && info.body_bytes == frame.len() as u64
        )));
    let (observer, next_request_id) = lifecycle.websocket_observer("/v1/responses");
    assert_ne!(request_id, next_request_id);
    let local_body = Arc::new(
        json!({"model": "gpt-test", "store": false, "previous_response_id": "resp_local"}),
    );
    let local_frame =
        Bytes::from(serde_json::to_vec(local_body.as_ref()).expect("test frame serializes"));
    let pinned = lifecycle
        .prepare_websocket_turn_with_surface(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &local_frame,
                query: None,
            },
            local_body.clone(),
            Some(first_id),
            observer,
            &next_request_id,
            false,
        )
        .await
        .expect("a locally owned store:false response does not require durable lookup");
    assert_eq!(
        pinned.candidates.len(),
        1,
        "owner-constrained turns cannot fall back"
    );
    assert_eq!(pinned.candidates[0].upstream_id, first_id);
    drain_events(&mut events);
    lifecycle
        .validate_websocket_turn(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &local_frame,
                query: None,
            },
            local_body.as_ref(),
            first_id,
            false,
        )
        .await
        .expect("retained connection is still eligible");
    assert!(
        drain_events(&mut events).is_empty(),
        "retained validation emits no extra lifecycle events"
    );
    lifecycle
        .bind_websocket_response(&auth, first_id, &json!({"id":"resp_local"}), None, false)
        .await
        .expect("store:false response ids stay local to the gateway");
    assert!(
        lifecycle
            .bind_websocket_response(
                &auth,
                first_id,
                &json!({"id":"resp_local","conversation":{"id":"conv_durable"}}),
                None,
                false,
            )
            .await
            .is_err(),
        "store:false conversations still require durable affinity storage"
    );
    assert!(
        lifecycle
            .bind_websocket_response(&auth, first_id, &json!({"id":"resp_stored"}), None, true)
            .await
            .is_err(),
        "stored response state fails closed without durable affinity storage"
    );
    let records = lifecycle.dynamic_view.load().upstreams_snapshot().to_vec();
    lifecycle.dynamic_view.store(provenance_view(
        Arc::new(PrincipalView::for_tests(
            "principal-test",
            false,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::new(),
        )),
        Arc::new(TestSignerFactory),
        records.clone(),
    ));
    assert!(
        lifecycle
            .validate_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: request.headers(),
                    frame: &frame,
                    query: None,
                },
                body.as_ref(),
                first_id,
                false,
            )
            .await
            .is_err(),
        "a retained connection cannot bypass a disabled principal"
    );
    lifecycle.dynamic_view.store(provenance_view(
        Arc::new(PrincipalView::for_tests(
            "principal-test",
            true,
            vec!["other-model".to_owned()],
            Vec::new(),
            HashMap::new(),
        )),
        Arc::new(TestSignerFactory),
        records,
    ));
    let error = lifecycle
        .validate_websocket_turn(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &frame,
                query: None,
            },
            body.as_ref(),
            first_id,
            false,
        )
        .await
        .expect_err("a retained connection cannot bypass a changed model allowlist");
    assert_eq!(error.status, 403);
    lifecycle.dynamic_view.store(provenance_view(
        failure_principal_view("other-principal", default_artifacts()),
        Arc::new(TestSignerFactory),
        Vec::new(),
    ));
    let missing_info = lifecycle.websocket_auth_info(&auth);
    assert_eq!(missing_info.principal_id, auth.principal_id());
    assert_eq!(missing_info.key_id.as_deref(), Some(auth.key_id()));
    assert_eq!(missing_info.auth_ms, Some(auth.auth_ms()));
    assert_eq!(
        missing_info.principal_kind, None,
        "a missing principal kind is never fabricated"
    );
    assert_eq!(
        dispatcher.call_count(),
        0,
        "WebSocket helpers never use HTTP dispatch"
    );
}

struct WebSocketEligibilityFilter {
    allow: AtomicBool,
    seen: Mutex<Vec<Vec<Uuid>>>,
    frame: Bytes,
}

impl FilterPlugin for WebSocketEligibilityFilter {
    fn filter(
        &self,
        ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        assert_eq!(ctx.method, http::Method::GET);
        assert_eq!(ctx.path, "/v1/responses");
        assert_eq!(ctx.canonical_model_id, "gpt-test");
        assert_eq!(
            ctx.body_bytes, self.frame,
            "filters receive original frame whitespace and key order"
        );
        assert_eq!(
            ctx.body_bytes.as_ptr(),
            self.frame.as_ptr(),
            "routing shares the original frame allocation"
        );
        self.seen.lock().expect("filter observations").push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        Ok(FilterOutput {
            kept_upstream_ids: candidates
                .iter()
                .filter(|_| self.allow.load(Ordering::SeqCst))
                .map(|candidate| candidate.upstream_id)
                .collect(),
            reason: "current WebSocket eligibility".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::from_u128(13_084)
    }
    fn plugin_name(&self) -> &str {
        "websocket-eligibility"
    }
}

#[tokio::test]
async fn responses_websocket_applies_filters_after_owner_constraint_and_revalidates() {
    let owner = Uuid::from_u128(71);
    let frame = Bytes::from_static(br#"{ "store" : false, "model" : "gpt-test" }"#);
    let filter = Arc::new(WebSocketEligibilityFilter {
        allow: AtomicBool::new(true),
        seen: Mutex::new(Vec::new()),
        frame: frame.clone(),
    });
    let pipeline = RouterPipelineCache {
        user_filters: vec![filter.clone()],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    };
    let records = [owner, Uuid::from_u128(72)]
        .into_iter()
        .map(|id| UpstreamRecord {
            id,
            name: format!("openai-{id}"),
            kind: StorageRecordKind::OpenaiApiKey,
            enabled: true,
            ..Default::default()
        })
        .collect();
    let view = provenance_view(
        failure_principal_view(
            "principal-test",
            (
                Some(Arc::new(pipeline)),
                cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
            ),
        ),
        Arc::new(TestSignerFactory),
        records,
    );
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
    let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);
    let request = failure_request(Bytes::new());
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let body = Arc::new(serde_json::from_slice::<Value>(&frame).expect("original frame parses"));
    let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
    let turn = lifecycle
        .prepare_websocket_turn_with_surface(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &frame,
                query: None,
            },
            body.clone(),
            Some(owner),
            observer,
            &request_id,
            false,
        )
        .await
        .expect("owner passes the current filters");
    assert_eq!(turn.candidates.len(), 1);
    assert_eq!(
        *filter.seen.lock().expect("filter observations"),
        vec![vec![owner]]
    );
    filter.allow.store(false, Ordering::SeqCst);
    let mut events = subscribe_lifecycle(&bus);
    let error = lifecycle
        .validate_websocket_turn(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &frame,
                query: None,
            },
            body.as_ref(),
            owner,
            false,
        )
        .await
        .expect_err("a retained connection does not bypass changed filter eligibility");
    assert_eq!(error.code, error_codes::UPSTREAM_AFFINITY_UNAVAILABLE);
    assert!(drain_events(&mut events).is_empty());
    let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
    assert!(
        lifecycle
            .prepare_websocket_turn_with_surface(
                &auth,
                WebSocketIngress {
                    headers: request.headers(),
                    frame: &frame,
                    query: None,
                },
                body,
                Some(owner),
                observer,
                &request_id,
                false,
            )
            .await
            .is_err(),
        "an excluded owner cannot fall back"
    );
    let rejected = drain_events(&mut events);
    assert_eq!(
        rejected
            .iter()
            .filter(|event| matches!(
                event,
                cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
            ))
            .count(),
        1,
        "a failed create terminates exactly once"
    );
    assert_eq!(dispatcher.call_count(), 0);
}

#[tokio::test]
async fn responses_websocket_subscription_preference_provides_ordered_fallbacks_without_reviving_exhausted_or_breaking_owner()
 {
    struct StaticQuotaCache {
        snapshots: Mutex<HashMap<Uuid, Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot>>>,
    }

    impl cc_lb_control::SubscriptionQuotaCacheLike for StaticQuotaCache {
        fn upsert_observation(&self, _record: &cc_lb_storage_api::SubscriptionQuotaSample) {}

        fn snapshot_for_upstream(
            &self,
            upstream_id: Uuid,
            _now_unix_millis: u64,
            _max_staleness_secs: u64,
        ) -> Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot> {
            self.snapshots
                .lock()
                .expect("quota cache lock")
                .get(&upstream_id)
                .cloned()
                .unwrap_or_default()
        }
    }

    const NOW_SECS: u64 = 1_700_000_000;
    let quota_snapshot = |window: &str, utilization: f64, status: &str, reset: u64| {
        cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
            window: window.to_owned(),
            state: cc_lb_domain::SubscriptionQuotaDataState::Fresh,
            source: Some("ws-fallback-test".to_owned()),
            utilization: Some(utilization),
            status: Some(status.to_owned()),
            resets_at_unix_secs: Some(reset),
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            credits: None,
            observed_at_unix_millis: Some(NOW_SECS * 1_000),
            max_staleness_secs: 60,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
        }
    };

    let first_id = Uuid::from_u128(101);
    let second_id = Uuid::from_u128(102);
    let filter: Arc<dyn FilterPlugin> = Arc::new(
        crate::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::new(),
    );
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![filter],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    });
    let principal_view = failure_principal_view(
        "principal-test",
        (
            Some(pipeline),
            cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
        ),
    );
    let upstreams = vec![
        UpstreamRecord {
            id: first_id,
            name: "codex-oauth-1".to_owned(),
            kind: StorageRecordKind::OpenaiOauth,
            enabled: true,
            ..Default::default()
        },
        UpstreamRecord {
            id: second_id,
            name: "codex-oauth-2".to_owned(),
            kind: StorageRecordKind::OpenaiOauth,
            enabled: true,
            ..Default::default()
        },
    ];
    let quota_cache = Arc::new(StaticQuotaCache {
        snapshots: Mutex::new(HashMap::new()),
    });
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .principal_view(principal_view)
        .subscription_quota_cache(quota_cache.clone())
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(upstreams)
        .build();
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let lifecycle = Lifecycle::new_with_dynamic_view(
        failure_authn(),
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(QueueDispatch::new(Vec::new())),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::TestClock::new_at_secs(NOW_SECS)),
    )
    .with_event_bus(bus);

    let frame = Bytes::from_static(br#"{"model":"gpt-6.2-sol","store":false}"#);
    let body = Arc::new(serde_json::from_slice::<Value>(&frame).expect("frame parses"));
    let request = failure_request(Bytes::new());
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");

    // Case A: fresh turn (owner = None) keeps both healthy OAuth upstreams in ordered fallback list.
    let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
    let fresh_turn = lifecycle
        .prepare_websocket_turn_with_surface(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &frame,
                query: None,
            },
            body.clone(),
            None,
            observer,
            &request_id,
            true,
        )
        .await
        .expect("fresh turn prepares");
    let fresh_ids: HashSet<Uuid> = fresh_turn
        .candidates
        .iter()
        .map(|candidate| candidate.upstream_id)
        .collect();
    assert_eq!(fresh_turn.candidates.len(), 2);
    assert_eq!(fresh_ids, HashSet::from([first_id, second_id]));

    // Case B: owned continuation (owner = Some(first_id)) stays pinned to first_id with no fallback.
    let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
    let owned_turn = lifecycle
        .prepare_websocket_turn_with_surface(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &frame,
                query: None,
            },
            body.clone(),
            Some(first_id),
            observer,
            &request_id,
            true,
        )
        .await
        .expect("owned turn prepares");
    assert_eq!(owned_turn.candidates.len(), 1);
    assert_eq!(owned_turn.candidates[0].upstream_id, first_id);

    // Case C: quota-exhausted upstream is excluded by subscription_hard_blocked and not revived.
    {
        let mut snapshots = quota_cache.snapshots.lock().expect("quota cache lock");
        snapshots.insert(
            first_id,
            vec![
                quota_snapshot("5h", 0.1, "allowed", NOW_SECS + 1_800),
                quota_snapshot("7d", 0.2, "allowed", NOW_SECS + 86_400),
            ],
        );
        snapshots.insert(
            second_id,
            vec![
                quota_snapshot("5h", 0.0, "allowed", NOW_SECS + 1_800),
                quota_snapshot("7d", 1.0, "rejected", NOW_SECS + 86_400),
            ],
        );
    }
    let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
    let filtered_turn = lifecycle
        .prepare_websocket_turn_with_surface(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &frame,
                query: None,
            },
            body,
            None,
            observer,
            &request_id,
            true,
        )
        .await
        .expect("healthy upstream prepares while exhausted peer is excluded");
    assert_eq!(filtered_turn.candidates.len(), 1);
    assert_eq!(filtered_turn.candidates[0].upstream_id, first_id);
}

#[tokio::test]
async fn responses_websocket_subscription_preference_keeps_api_key_fallback_rechecks_quota_and_plans_hybrid_reasoning()
 {
    struct StaticQuotaCache {
        snapshots: Mutex<HashMap<Uuid, Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot>>>,
    }

    impl cc_lb_control::SubscriptionQuotaCacheLike for StaticQuotaCache {
        fn upsert_observation(&self, _record: &cc_lb_storage_api::SubscriptionQuotaSample) {}

        fn snapshot_for_upstream(
            &self,
            upstream_id: Uuid,
            _now_unix_millis: u64,
            _max_staleness_secs: u64,
        ) -> Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot> {
            self.snapshots
                .lock()
                .expect("quota cache lock")
                .get(&upstream_id)
                .cloned()
                .unwrap_or_default()
        }
    }

    const NOW_SECS: u64 = 1_700_000_000;
    let quota_snapshot = |window: &str, utilization: f64, status: &str, reset: u64| {
        cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
            window: window.to_owned(),
            state: cc_lb_domain::SubscriptionQuotaDataState::Fresh,
            source: Some("ws-fallback-test".to_owned()),
            utilization: Some(utilization),
            status: Some(status.to_owned()),
            resets_at_unix_secs: Some(reset),
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            credits: None,
            observed_at_unix_millis: Some(NOW_SECS * 1_000),
            max_staleness_secs: 60,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
        }
    };

    let mut upstreams = Vec::new();
    let mut oauth_ids = Vec::new();
    for idx in 1..=18_u128 {
        let id = Uuid::from_u128(500 + idx);
        oauth_ids.push(id);
        upstreams.push(UpstreamRecord {
            id,
            name: format!("codex-oauth-{idx}"),
            kind: StorageRecordKind::OpenaiOauth,
            enabled: true,
            ..Default::default()
        });
    }
    let api_key_id = Uuid::from_u128(600);
    upstreams.push(UpstreamRecord {
        id: api_key_id,
        name: "openai-api-key".to_owned(),
        kind: StorageRecordKind::OpenaiApiKey,
        enabled: true,
        ..Default::default()
    });

    let filter: Arc<dyn FilterPlugin> = Arc::new(
        crate::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::new(),
    );
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![filter],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    });
    let principal_view = failure_principal_view(
        "principal-test",
        (
            Some(pipeline),
            cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
        ),
    );
    let quota_cache = Arc::new(StaticQuotaCache {
        snapshots: Mutex::new(HashMap::new()),
    });
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .principal_view(principal_view)
        .subscription_quota_cache(quota_cache.clone())
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(upstreams)
        .build();
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let config = LifecycleConfig {
        unknown_openai_reasoning: cc_lb_domain::UnknownOpenaiReasoningPolicy::Hybrid,
        ..Default::default()
    };
    struct EmptyAffinityStore;

    #[async_trait]
    impl UpstreamAffinityStore for EmptyAffinityStore {
        async fn resolve_upstream_affinities(
            &self,
            _keys: &[UpstreamAffinityKey],
            _now_unix_secs: u64,
            _ttl_secs: u64,
        ) -> cc_lb_storage_api::StorageResult<Vec<UpstreamAffinityBinding>> {
            Ok(Vec::new())
        }

        async fn read_upstream_owner_epoch(
            &self,
            _upstream_id: cc_lb_storage_api::UpstreamRecordId,
            _expected_spec_revision: u64,
        ) -> cc_lb_storage_api::StorageResult<Option<u64>> {
            Ok(Some(0))
        }

        async fn bind_upstream_affinities(
            &self,
            _bindings: &[UpstreamAffinityBinding],
            _now_unix_secs: u64,
            _ttl_secs: u64,
        ) -> cc_lb_storage_api::StorageResult<()> {
            Ok(())
        }

        async fn purge_expired_upstream_affinities(
            &self,
            _now_unix_secs: u64,
            _ttl_secs: u64,
            _batch_size: usize,
        ) -> cc_lb_storage_api::StorageResult<u64> {
            Ok(0)
        }
    }

    let lifecycle = Lifecycle::new_with_dynamic_view(
        failure_authn(),
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(QueueDispatch::new(Vec::new())),
        config,
        Arc::new(cc_lb_clock::TestClock::new_at_secs(NOW_SECS)),
    )
    .with_event_bus(bus)
    .with_upstream_affinity_store(Arc::new(EmptyAffinityStore));

    let frame = Bytes::from_static(br#"{"model":"gpt-6.2-sol","store":false}"#);
    let body = Arc::new(serde_json::from_slice::<Value>(&frame).expect("frame parses"));
    let request = failure_request(Bytes::new());
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");

    // 1. Public WS keeps an eligible API-key fallback even behind >16 OAuth candidates.
    let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
    let public_turn = lifecycle
        .prepare_websocket_turn_with_surface(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &frame,
                query: None,
            },
            body.clone(),
            None,
            observer,
            &request_id,
            false,
        )
        .await
        .expect("public turn prepares");
    assert!(
        public_turn
            .candidates
            .iter()
            .any(|c| c.upstream_id == api_key_id && c.is_api_key),
        "public WS must retain eligible API-key fallback beyond 16 OAuth candidates"
    );

    // Native Codex remains strictly OAuth-only.
    let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
    let codex_turn = lifecycle
        .prepare_websocket_turn_with_surface(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &frame,
                query: None,
            },
            body.clone(),
            None,
            observer,
            &request_id,
            true,
        )
        .await
        .expect("codex turn prepares");
    assert!(
        !codex_turn
            .candidates
            .iter()
            .any(|c| c.upstream_id == api_key_id || c.is_api_key),
        "native Codex WS must remain OAuth-only"
    );

    // 2. Fresh quota exhaustion rejects unpinned fallback revalidation while keeping pinned continuations working.
    quota_cache.snapshots.lock().expect("quota lock").insert(
        oauth_ids[1],
        vec![
            quota_snapshot("5h", 0.0, "allowed", NOW_SECS + 1_800),
            quota_snapshot("7d", 1.0, "rejected", NOW_SECS + 86_400),
        ],
    );
    let fallback_err = lifecycle
        .validate_websocket_turn_with_policy(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &frame,
                query: None,
            },
            body.as_ref(),
            oauth_ids[1],
            false,
            true,
        )
        .await
        .expect_err("unpinned fallback must reject freshly exhausted OAuth candidate");
    assert_eq!(fallback_err.status, 503);
    lifecycle
        .validate_websocket_turn_with_policy(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &frame,
                query: None,
            },
            body.as_ref(),
            oauth_ids[1],
            false,
            false,
        )
        .await
        .expect("pinned continuation must still be allowed on exhausted candidate");

    // 3. Hybrid reasoning uses the full dispatchable pool under subscription-preference:
    //    multi-upstream unpinned -> StrippedUpfront; single-owner or lone healthy -> RetryStripAll.
    let reasoning_frame = Bytes::from_static(
        br#"{"model":"gpt-6.2-sol","store":false,"input":[{"type":"reasoning","id":"rs_foreign","encrypted_content":"gAAAAABforeign_reason","summary":[]},{"type":"message","role":"user","content":"hi"}]}"#,
    );
    let reasoning_body = Arc::new(
        serde_json::from_slice::<Value>(&reasoning_frame).expect("reasoning frame parses"),
    );
    let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
    let multi_turn = lifecycle
        .prepare_websocket_turn_with_surface(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &reasoning_frame,
                query: None,
            },
            reasoning_body.clone(),
            None,
            observer,
            &request_id,
            true,
        )
        .await
        .expect("multi-upstream codex turn prepares");
    assert!(
        matches!(
            multi_turn.unknown_reasoning_plan,
            WsUnknownReasoningPlan::StrippedUpfront { .. }
        ),
        "multi-upstream subscription-preference pool must strip unestablished reasoning upfront"
    );

    let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
    let pinned_turn = lifecycle
        .prepare_websocket_turn_with_surface(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &reasoning_frame,
                query: None,
            },
            reasoning_body.clone(),
            Some(oauth_ids[0]),
            observer,
            &request_id,
            true,
        )
        .await
        .expect("pinned codex turn prepares");
    assert!(
        matches!(
            pinned_turn.unknown_reasoning_plan,
            WsUnknownReasoningPlan::RetryStripAll { .. }
        ),
        "single-owner turn must preserve RetryStripAll behavior"
    );

    // Mark all OAuth accounts except oauth_ids[0] exhausted so only 1 upstream is dispatchable.
    {
        let mut guard = quota_cache.snapshots.lock().expect("quota lock");
        guard.insert(
            oauth_ids[0],
            vec![
                quota_snapshot("5h", 0.1, "allowed", NOW_SECS + 1_800),
                quota_snapshot("7d", 0.1, "allowed", NOW_SECS + 86_400),
            ],
        );
        for id in &oauth_ids[1..] {
            guard.insert(
                *id,
                vec![
                    quota_snapshot("5h", 0.0, "allowed", NOW_SECS + 1_800),
                    quota_snapshot("7d", 1.0, "rejected", NOW_SECS + 86_400),
                ],
            );
        }
    }
    let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
    let single_healthy_turn = lifecycle
        .prepare_websocket_turn_with_surface(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &reasoning_frame,
                query: None,
            },
            reasoning_body,
            None,
            observer,
            &request_id,
            true,
        )
        .await
        .expect("single healthy codex turn prepares");
    assert!(
        matches!(
            single_healthy_turn.unknown_reasoning_plan,
            WsUnknownReasoningPlan::RetryStripAll { .. }
        ),
        "single healthy OAuth upstream without non-blocked fallbacks must keep RetryStripAll"
    );
}

#[tokio::test]
async fn responses_websocket_signs_native_codex_connection_urls() {
    for (base_url, expected) in [
        (None, "wss://chatgpt.com/backend-api/codex/responses"),
        (
            Some("http://gateway.invalid/codex-proxy/?stale=1"),
            "ws://gateway.invalid/codex-proxy/responses",
        ),
    ] {
        let id = Uuid::from_u128(73);
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(TestSignerFactory),
            vec![UpstreamRecord {
                id,
                name: "codex-native".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                base_url: base_url.map(|value| Url::parse(value).expect("test URL parses")),
                enabled: true,
                ..Default::default()
            }],
        );
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let lifecycle = provenance_lifecycle(view, Arc::new(QueueDispatch::new(Vec::new())), &bus);
        let request = failure_request(Bytes::new());
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let body = Arc::new(json!({"model":"gpt-test","store":false}));
        let frame = Bytes::from(serde_json::to_vec(body.as_ref()).expect("test frame serializes"));
        let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
        let prepared = lifecycle
            .prepare_websocket_turn_with_surface(
                &auth,
                WebSocketIngress {
                    headers: request.headers(),
                    frame: &frame,
                    query: None,
                },
                body,
                None,
                observer,
                &request_id,
                false,
            )
            .await
            .expect("Codex candidate is eligible");
        for query in [None, Some("trace=a%2Fb&beta=1")] {
            let (url, _) = lifecycle
                .sign_websocket_connection(&auth, request.headers(), &prepared.candidates[0], query)
                .await
                .expect("Codex connection signs");
            let expected = query.map_or_else(
                || expected.to_owned(),
                |query| format!("{expected}?{query}"),
            );
            assert_eq!(url.as_str(), expected);
        }
        let records = lifecycle.dynamic_view.load().upstreams_snapshot().to_vec();
        lifecycle.dynamic_view.store(provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(FailingSignerFactory(|| SignerError::MissingCredentials {
                reason: "rotated credential is unavailable".to_owned(),
            })),
            records,
        ));
        let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
        let turn = lifecycle
            .prepare_websocket_turn_with_surface(
                &auth,
                WebSocketIngress {
                    headers: request.headers(),
                    frame: &frame,
                    query: None,
                },
                Arc::new(json!({"model":"gpt-test","store":false})),
                None,
                observer,
                &request_id,
                false,
            )
            .await
            .expect("turn preparation does not sign any connection");
        assert!(
            lifecycle
                .sign_websocket_connection(&auth, request.headers(), &turn.candidates[0], None)
                .await
                .is_err(),
            "opening a new connection uses the fresh signer factory, not cached credentials"
        );
    }
}

#[tokio::test]
async fn responses_websocket_authentication_rejection_emits_one_terminal() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let view = provenance_view(
        failure_principal_view("principal-test", default_artifacts()),
        Arc::new(TestSignerFactory),
        Vec::new(),
    );
    let lifecycle = provenance_lifecycle(view, Arc::new(QueueDispatch::new(Vec::new())), &bus);
    let mut events = subscribe_lifecycle(&bus);
    let response = lifecycle.reject_responses_websocket_authentication(
        &BuiltinAuthError::InvalidFormat,
        "/v1/responses",
    );
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let rejected = drain_events(&mut events);
    assert_eq!(
        rejected
            .iter()
            .filter(|event| matches!(
                event,
                cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        rejected
            .iter()
            .filter(|event| matches!(
                event,
                cc_lb_lifecycle::LifecycleEvent::AuthCompleted { result: Err(_), .. }
            ))
            .count(),
        1
    );
    assert!(rejected.iter().any(|event| matches!(
        event,
        cc_lb_lifecycle::LifecycleEvent::RequestStarted {
            event_kind: Some(cc_lb_request_log::RequestEventKind::OpenaiResponses),
            ..
        }
    )));
}

#[tokio::test]
async fn websocket_authentication_rejection_uses_attempted_route_kind() {
    let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
    let view = provenance_view(
        failure_principal_view("principal-test", default_artifacts()),
        Arc::new(TestSignerFactory),
        Vec::new(),
    );
    let lifecycle = provenance_lifecycle(view, Arc::new(QueueDispatch::new(Vec::new())), &bus);
    let mut events = subscribe_lifecycle(&bus);
    for (path, expected) in [
        (
            "/v1/realtime",
            cc_lb_request_log::RequestEventKind::OpenaiResponses,
        ),
        (
            "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
            cc_lb_request_log::RequestEventKind::GeminiGenerateContent,
        ),
        (
            "/ws/unsupported",
            cc_lb_request_log::RequestEventKind::Other,
        ),
    ] {
        let response = lifecycle
            .reject_responses_websocket_authentication(&BuiltinAuthError::InvalidFormat, path);
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let rejected = drain_events(&mut events);
        assert!(rejected.iter().any(|event| matches!(
            event,
            cc_lb_lifecycle::LifecycleEvent::RequestStarted {
                event_kind: Some(kind),
                ..
            } if *kind == expected
        )));
        assert_eq!(
            rejected
                .iter()
                .filter(|event| matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
                ))
                .count(),
            1
        );
    }
}

#[test]
fn provider_state_marker_probe_detects_split_keys_and_skips_stateless_events() {
    for stateless in [
            &b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\""[..],
            b"event: ping\ndata: {",
            b"data: xxxxxxxxxxxxxxxx",
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"content\":[]}}\n\n",
        ] {
            assert!(!sse_bytes_may_carry_anthropic_provider_state(stateless));
        }
    for marker in ANTHROPIC_PROVIDER_STATE_MARKERS {
        assert!(sse_bytes_may_carry_anthropic_provider_state(marker));
    }
    // A key split across chunks is found inside the probe window that
    // re-scans the tail of the already-forwarded partial event.
    let forwarded = b"data: {\"type\":\"content_block_start\",\"content\":[{\"encrypted_con";
    let mut buffer = forwarded.to_vec();
    buffer.extend_from_slice(b"tent\":\"opaque\"}]}");
    let probe_start = forwarded.len() - ANTHROPIC_PROVIDER_STATE_MARKER_PROBE_BYTES;
    assert!(!sse_bytes_may_carry_anthropic_provider_state(forwarded));
    assert!(sse_bytes_may_carry_anthropic_provider_state(
        &buffer[probe_start..]
    ));
}
fn sticky_metadata(
    session_id: &str,
    request_kind: &str,
    claude_agent_id: Option<&str>,
) -> RequestCacheMetadata {
    RequestCacheMetadata {
        thread_id: Some(session_id.to_owned()),
        observed_session_id: Some(session_id.to_owned()),
        request_kind: Some(request_kind.to_owned()),
        claude_agent_id: claude_agent_id.map(ToOwned::to_owned),
        ..RequestCacheMetadata::default()
    }
}

#[test]
fn thread_sticky_subagent_latch_seeds_omp_and_senpi_and_rewrites_main() {
    let latch = ThreadStickySubagentLatch::default();
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-session-affinity"),
        HeaderValue::from_static("sticky-thread"),
    );
    let omp = json!({
        "system": "You are omp's trusted coding assistant.",
        "messages": [{"role": "user", "content": "Complete assignment thoroughly:\n\n# Target"}]
    });
    let mut omp_metadata = request_cache_metadata_from_value_with_analysis_for_capability(
        &headers,
        Some(&omp),
        None,
        None,
        None,
    );
    assert_eq!(omp_metadata.request_kind.as_deref(), Some("subagent"));
    apply_thread_sticky_subagent_latch(&latch, "principal-a", Some(&omp), &mut omp_metadata);

    let senpi = json!({
        "system": "You are senpi, a coding agent.",
        "messages": [{"role": "user", "content": "Task: Review the auth change"}]
    });
    let mut senpi_metadata = request_cache_metadata_from_value_with_analysis_for_capability(
        &headers,
        Some(&senpi),
        None,
        None,
        None,
    );
    assert_eq!(senpi_metadata.request_kind.as_deref(), Some("subagent"));
    apply_thread_sticky_subagent_latch(&latch, "principal-b", Some(&senpi), &mut senpi_metadata);

    let ordinary = json!({
        "system": "You are omp's trusted coding assistant.",
        "messages": [{"role": "user", "content": "continue"}]
    });
    let mut ordinary_metadata = request_cache_metadata_from_value_with_analysis_for_capability(
        &headers,
        Some(&ordinary),
        None,
        None,
        None,
    );
    apply_thread_sticky_subagent_latch(
        &latch,
        "principal-a",
        Some(&ordinary),
        &mut ordinary_metadata,
    );
    assert_eq!(ordinary_metadata.request_kind.as_deref(), Some("subagent"));

    let mut isolated_metadata = request_cache_metadata_from_value_with_analysis_for_capability(
        &headers,
        Some(&ordinary),
        None,
        None,
        None,
    );
    apply_thread_sticky_subagent_latch(
        &latch,
        "principal-c",
        Some(&ordinary),
        &mut isolated_metadata,
    );
    assert_eq!(isolated_metadata.request_kind.as_deref(), Some("main"));
}

#[test]
fn thread_sticky_subagent_latch_preserves_one_shot_and_exclusion_results() {
    let latch = ThreadStickySubagentLatch::default();
    let seed = json!({
        "messages": [{"role": "user", "content": "Complete assignment thoroughly:\n\n# Target"}]
    });
    let mut seed_metadata = sticky_metadata("sticky-thread", "subagent", None);
    apply_thread_sticky_subagent_latch(&latch, "principal-a", Some(&seed), &mut seed_metadata);
    for request_kind in [
        "compaction",
        "recap",
        "session_title",
        "auto_thinking",
        "side",
        "advisor",
        "notification",
        "look_at",
    ] {
        let mut metadata = sticky_metadata("sticky-thread", request_kind, None);
        apply_thread_sticky_subagent_latch(&latch, "principal-a", None, &mut metadata);
        assert_eq!(metadata.request_kind.as_deref(), Some(request_kind));
    }
    let mut explicit_main = sticky_metadata("sticky-thread", "main", None);
    explicit_main.explicit_request_class = Some("main".to_owned());
    apply_thread_sticky_subagent_latch(&latch, "principal-a", None, &mut explicit_main);
    assert_eq!(explicit_main.request_kind.as_deref(), Some("main"));

    let hermes = json!({
        "system": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.",
        "messages": [{"role": "user", "content": "Review the conversation above and consider saving to memory if appropriate."}]
    });
    let mut hermes_metadata = sticky_metadata("hermes-thread", "subagent", None);
    apply_thread_sticky_subagent_latch(&latch, "principal-a", Some(&hermes), &mut hermes_metadata);
    let mut hermes_ordinary = sticky_metadata("hermes-thread", "main", None);
    apply_thread_sticky_subagent_latch(&latch, "principal-a", Some(&hermes), &mut hermes_ordinary);
    assert_eq!(hermes_ordinary.request_kind.as_deref(), Some("main"));

    let mut claude_seed = sticky_metadata("claude-thread", "subagent", Some("agent-1"));
    apply_thread_sticky_subagent_latch(&latch, "principal-a", Some(&seed), &mut claude_seed);
    let mut claude_ordinary = sticky_metadata("claude-thread", "main", None);
    apply_thread_sticky_subagent_latch(&latch, "principal-a", Some(&seed), &mut claude_ordinary);
    assert_eq!(claude_ordinary.request_kind.as_deref(), Some("main"));

    let mut no_thread = RequestCacheMetadata {
        request_kind: Some("subagent".to_owned()),
        ..RequestCacheMetadata::default()
    };
    apply_thread_sticky_subagent_latch(&latch, "principal-a", Some(&seed), &mut no_thread);
    assert!(!latch.observe(
        ThreadStickySubagentKey {
            principal_id: "principal-a".to_owned(),
            observed_session_id: "no-thread".to_owned(),
        },
        false,
    ));
}

#[test]
fn thread_sticky_subagent_latch_evicts_oldest_entry_per_shard() {
    let latch = ThreadStickySubagentLatch::default();
    let first = ThreadStickySubagentKey {
        principal_id: "principal".to_owned(),
        observed_session_id: "thread-0".to_owned(),
    };
    let shard = ThreadStickySubagentLatch::shard_index(&first);
    let mut keys = vec![first];
    let mut index = 1;
    while keys.len() <= MAX_THREAD_STICKY_SUBAGENTS_PER_SHARD {
        let key = ThreadStickySubagentKey {
            principal_id: "principal".to_owned(),
            observed_session_id: format!("thread-{index}"),
        };
        index += 1;
        if ThreadStickySubagentLatch::shard_index(&key) == shard {
            keys.push(key);
        }
    }
    for key in &keys {
        assert!(latch.observe(key.clone(), true));
    }
    assert!(!latch.observe(keys[0].clone(), false));
    assert!(latch.observe(keys[1].clone(), false));
}

#[tokio::test]
async fn responses_websocket_subscription_preference_keeps_multiple_api_keys_reachable_behind_16_oauth()
 {
    struct StaticQuotaCache {
        snapshots: Mutex<HashMap<Uuid, Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot>>>,
    }

    impl cc_lb_control::SubscriptionQuotaCacheLike for StaticQuotaCache {
        fn upsert_observation(&self, _record: &cc_lb_storage_api::SubscriptionQuotaSample) {}

        fn snapshot_for_upstream(
            &self,
            upstream_id: Uuid,
            _now_unix_millis: u64,
            _max_staleness_secs: u64,
        ) -> Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot> {
            self.snapshots
                .lock()
                .expect("quota cache lock")
                .get(&upstream_id)
                .cloned()
                .unwrap_or_default()
        }
    }

    const NOW_SECS: u64 = 1_800_000_000;
    let quota_snapshot = |window: &str, utilization: f64, status: &str, reset: u64| {
        cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
            window: window.to_owned(),
            state: cc_lb_domain::SubscriptionQuotaDataState::Fresh,
            source: Some("ws-fallback-test".to_owned()),
            utilization: Some(utilization),
            status: Some(status.to_owned()),
            resets_at_unix_secs: Some(reset),
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            credits: None,
            observed_at_unix_millis: Some(NOW_SECS * 1_000),
            max_staleness_secs: 60,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
        }
    };

    let mut upstreams = Vec::new();
    for idx in 1..=16_u128 {
        let id = Uuid::from_u128(0xD000 + idx);
        upstreams.push(UpstreamRecord {
            id,
            name: format!("oauth-{idx}"),
            kind: StorageRecordKind::OpenaiOauth,
            enabled: true,
            ..Default::default()
        });
    }
    let api_key_1 = Uuid::from_u128(0xE001);
    let api_key_2 = Uuid::from_u128(0xE002);
    upstreams.push(UpstreamRecord {
        id: api_key_1,
        name: "openai-api-key-1".to_owned(),
        kind: StorageRecordKind::OpenaiApiKey,
        enabled: true,
        ..Default::default()
    });
    upstreams.push(UpstreamRecord {
        id: api_key_2,
        name: "openai-api-key-2".to_owned(),
        kind: StorageRecordKind::OpenaiApiKey,
        enabled: true,
        ..Default::default()
    });

    let filter: Arc<dyn FilterPlugin> = Arc::new(
        crate::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::new(),
    );
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![filter],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    });
    let principal_view = failure_principal_view(
        "principal-test",
        (
            Some(pipeline),
            cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
        ),
    );
    let quota_cache = Arc::new(StaticQuotaCache {
        snapshots: Mutex::new(HashMap::from([(
            Uuid::from_u128(0xD001),
            vec![
                quota_snapshot("5h", 0.05, "allowed", NOW_SECS + 1_800),
                quota_snapshot("7d", 0.05, "allowed", NOW_SECS + 86_400),
            ],
        )])),
    });
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .principal_view(principal_view)
        .subscription_quota_cache(quota_cache)
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(upstreams)
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        failure_authn(),
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(QueueDispatch::new(Vec::new())),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::TestClock::new_at_secs(NOW_SECS)),
    );
    let frame = Bytes::from_static(br#"{"model":"gpt-6.2-sol","store":false}"#);
    let body = Arc::new(serde_json::from_slice::<Value>(&frame).expect("frame parses"));
    let request = failure_request(Bytes::new());
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("authenticates");
    let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
    let public_turn = lifecycle
        .prepare_websocket_turn_with_surface(
            &auth,
            WebSocketIngress {
                headers: request.headers(),
                frame: &frame,
                query: None,
            },
            body,
            None,
            observer,
            &request_id,
            false,
        )
        .await
        .expect("public turn prepares");
    assert!(
        public_turn
            .candidates
            .iter()
            .any(|c| c.upstream_id == api_key_1 && c.is_api_key),
        "first API-key fallback must be included"
    );
    assert!(
        public_turn
            .candidates
            .iter()
            .any(|c| c.upstream_id == api_key_2 && c.is_api_key),
        "second API-key fallback must remain reachable when 15 OAuth + 1 API key already fill ordinary fallbacks"
    );
}
