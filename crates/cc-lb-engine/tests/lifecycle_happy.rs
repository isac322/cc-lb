use crate::common;

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use base64::Engine as _;
use bytes::Bytes;
use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_control::api_keys::principal_view::{
    DialectCache, PrincipalRoutingArtifacts, PrincipalView, RouterPipelineCache, ShapePluginCache,
};
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder, RequestEventBus};
use cc_lb_domain::{Principal, TerminalStrategy, Upstream};
use cc_lb_engine::{
    DispatchError, Lifecycle, LifecycleConfig, LimitCostEstimator, UpstreamDispatch,
};
use cc_lb_lifecycle::{LifecycleEvent, LimitDecisionKind, RouteInfo};
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, PriceCatalog, Pricing, UsdPerMillion};
use cc_lb_storage_api::principal::{Limit, LimitKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{KeyStatus, MetaStore, Storage as StorageTrait, StoredApiKeyRecord};
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, SignedRequest,
    UpstreamDialect,
};
use http::StatusCode;
use http_body_util::BodyExt;
use tokio::time::{Duration, timeout};

use common::{
    DispatchMode, MockDispatch, PassthroughDialect, TestAuthn, TestLifecycleBus, TestState,
    collect_body, lifecycle_with, managed_api_key, messages_request,
};

#[derive(Default)]
struct RecordingLimitCostEstimator {
    service_tiers: Mutex<Vec<Option<String>>>,
}

impl LimitCostEstimator for RecordingLimitCostEstimator {
    fn estimate_max(
        &self,
        _model: &str,
        _max_input: u64,
        _max_output: u64,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.service_tiers
            .lock()
            .expect("service tier lock")
            .push(service_tier.map(ToOwned::to_owned));
        Some(1)
    }
    fn estimate_max_with_local_cache(
        &self,
        model: &str,
        input: cc_lb_pricing::LocalCacheInputEstimate,
        max_output: u64,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.estimate_max(
            model,
            input
                .uncached_input_tokens
                .saturating_add(input.cache_prefix_5m_tokens)
                .saturating_add(input.cache_prefix_1h_tokens),
            max_output,
            service_tier,
        )
    }
}

#[tokio::test]
async fn happy_sse_relays_incrementally() {
    let state = TestState::default();
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::StreamingOk,
        },
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"stream":true}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");

    assert_eq!(response.status(), http::StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("stream collects")
        .to_bytes();
    let data_lines = String::from_utf8_lossy(&body).matches("data:").count();
    println!("data_lines={data_lines}");
    assert!(
        data_lines >= 50,
        "expected at least 50 data lines, got {data_lines}"
    );
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn happy_non_streaming_relays_response() {
    let state = TestState::default();
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");

    assert_eq!(response.status(), http::StatusCode::OK);
    let _body = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();

    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn explicit_native_shape_removes_attribution_and_preserves_serving_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage_for_lifecycle_happy(&dir).await?);
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut lifecycle_events = test_bus.bus.attach_lifecycle_writer(128);
    let mut updates = test_bus.bus.subscribe();

    let cases = [
        (
            "official-api-key",
            StorageUpstreamKind::AnthropicApiKey,
            None,
        ),
        (
            "official-oauth",
            StorageUpstreamKind::AnthropicOauth,
            Some("https://api.anthropic.com:443/proxy?tenant=one#fragment"),
        ),
        (
            "custom-api-key",
            StorageUpstreamKind::AnthropicApiKey,
            Some("https://proxy.example.com/anthropic"),
        ),
        (
            "custom-oauth",
            StorageUpstreamKind::AnthropicOauth,
            Some("https://user:pass@api.anthropic.com"),
        ),
    ];

    for (index, (name, kind, record_base_url)) in cases.into_iter().enumerate() {
        let state = TestState::default();
        let dispatcher = CapturingSignedDispatch::default();
        let lifecycle = explicit_native_lifecycle(
            &state,
            kind,
            record_base_url.map(|url| url.parse()).transpose()?,
            Arc::new(dispatcher.clone()),
            test_bus.bus_arc(),
            index as u128 + 100,
            name,
        );

        let normal = shaped_prompt_request("normal");
        let normal_body = normal.body().clone();
        let response = handle(&lifecycle, normal).await;
        assert_eq!(response.status(), StatusCode::OK, "{name} normal response");
        response.into_body().collect().await?;
        let normal_route = next_route(&mut lifecycle_events).await;
        wait_for_final_update(&mut updates).await;

        let cron = shaped_prompt_request("normal cc_workload=cron;");
        let cron_body = cron.body().clone();
        let response = handle(&lifecycle, cron).await;
        assert_eq!(response.status(), StatusCode::OK, "{name} cron response");
        response.into_body().collect().await?;
        let cron_route = next_route(&mut lifecycle_events).await;
        wait_for_final_update(&mut updates).await;

        let dispatched = dispatcher
            .bodies
            .lock()
            .expect("captured bodies lock")
            .clone();
        assert_eq!(dispatched.len(), 2, "{name} dispatch count");
        let normal_dispatched = serde_json::from_slice::<serde_json::Value>(&dispatched[0])?;
        let cron_dispatched = serde_json::from_slice::<serde_json::Value>(&dispatched[1])?;
        let normal_original = serde_json::from_slice::<serde_json::Value>(&normal_body)?;
        let cron_original = serde_json::from_slice::<serde_json::Value>(&cron_body)?;
        assert_eq!(
            normal_original["model"], cron_original["model"],
            "{name} model"
        );
        assert_eq!(
            normal_original["system"][1], cron_original["system"][1],
            "{name} system tail"
        );
        assert_eq!(
            normal_original["tools"], cron_original["tools"],
            "{name} tools"
        );
        assert!(
            normal_original["system"][0]["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("x-anthropic-billing-header:")),
            "{name} normal request lacks attribution control"
        );
        assert!(
            cron_original["system"][0]["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("x-anthropic-billing-header:")),
            "{name} cron request lacks attribution control"
        );
        assert_ne!(
            normal_original["system"][0]["text"], cron_original["system"][0]["text"],
            "{name} attribution differs"
        );
        assert_eq!(
            normal_dispatched["model"], cron_dispatched["model"],
            "{name} dispatched model"
        );
        assert_eq!(
            normal_dispatched["system"]
                .as_array()
                .and_then(|system| system.last()),
            cron_dispatched["system"]
                .as_array()
                .and_then(|system| system.last()),
            "{name} dispatched system tail"
        );
        assert_eq!(
            normal_dispatched["tools"], cron_dispatched["tools"],
            "{name} dispatched tools"
        );

        assert_eq!(
            normal_dispatched, cron_dispatched,
            "{name} shaped normal/cron bodies"
        );
        assert_eq!(
            normal_dispatched["system"][0]["text"], "stable system tail",
            "{name} first billing block removed by actual shape"
        );
        assert!(
            !String::from_utf8_lossy(&dispatched[0]).contains("x-anthropic-billing-header:"),
            "{name} shaped body contains attribution header"
        );

        assert!(
            normal_route.cache_prefix_hash.is_some(),
            "{name} serving cache identity missing"
        );
        assert!(
            cron_route.cache_prefix_hash.is_some(),
            "{name} cron serving cache identity missing"
        );
        if name.starts_with("official-") {
            assert_eq!(
                normal_route.cache_prefix_hash, cron_route.cache_prefix_hash,
                "{name} lifecycle serving cache identity"
            );
        } else {
            assert_ne!(
                normal_route.cache_prefix_hash, cron_route.cache_prefix_hash,
                "{name} lifecycle literal cache identity"
            );
        }

        let diagnostics = test_bus.diagnostics_records();
        let records = diagnostics
            .iter()
            .filter(|record| record.upstream_name.as_deref() == Some(name))
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2, "{name} diagnostics count");
        assert_eq!(records[0].model.as_deref(), Some("claude-sonnet-4-5"));
        assert_eq!(records[1].model.as_deref(), Some("claude-sonnet-4-5"));
        assert_eq!(records[0].status, StatusCode::OK.as_u16());
        assert_eq!(records[1].status, StatusCode::OK.as_u16());
    }

    Ok(())
}

#[tokio::test]
async fn native_openai_opaque_cache_extensions_remain_neutral_and_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    let body = Bytes::from(serde_json::to_vec_pretty(&serde_json::json!({
        "model": "gpt-limit-test",
        "max_output_tokens": 1,
        "input": "opaque native payload",
        "cache_control": {"vendor": "opaque-extension"},
        "system": [
            {"type":"text","text":"x-anthropic-billing-header: normal cc_workload=cron;"},
            {
                "type":"text",
                "text":"cache-bearing vendor context",
                "cache_control":{"type":"ephemeral"}
            }
        ],
        "messages": [{"role":"user","content":[{
            "type":"text",
            "text":"cache-bearing vendor tail",
            "cache_control":{"type":"ephemeral"}
        }]}]
    }))?);
    // Positive control: a generic Anthropic analysis would produce cache
    // metadata from this payload. These extensions stay opaque on Responses.
    let hypothetical_anthropic =
        cc_lb_engine::parse_request_cache_breakpoints(&http::HeaderMap::new(), &body);
    assert!(!hypothetical_anthropic.is_empty());

    let state = TestState::default();
    let principal_view = Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        HashMap::new(),
    ));
    let authn = TestAuthn::with_principal_view(state, principal_view.clone());
    let dispatcher = CapturingSignedDispatch::default();
    let upstream_id = uuid::Uuid::from_u128(1_004_001);
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(principal_view)
        .upstream_records(vec![UpstreamRecord {
            id: upstream_id,
            name: "native-openai-neutral".to_owned(),
            kind: StorageUpstreamKind::OpenaiApiKey,
            base_url: Some("https://api.openai.com/".parse()?),
            enabled: true,
            api_key_ciphertext: Some(Vec::new()),
            ..UpstreamRecord::default()
        }])
        .build();
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.attach_lifecycle_writer(32);
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(dispatcher.clone()),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(test_bus.bus_arc());
    let request = http::Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("x-api-key", managed_api_key())
        .body(body.clone())?;
    let response = handle(&lifecycle, request).await;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;

    let route = next_route(&mut lifecycle_events).await;
    assert_eq!(route.upstream_id, upstream_id);
    assert_eq!(route.upstream_name, "native-openai-neutral");
    assert!(
        route.cache_breakpoints.as_ref().is_some_and(Vec::is_empty),
        "native capability must not inherit Anthropic cache metadata"
    );
    assert!(route.cache_prefix_hash.is_none());
    assert!(route.matched_v3_cache_key.is_none());
    let dispatched = dispatcher
        .bodies
        .lock()
        .expect("captured native bodies lock");
    assert_eq!(dispatched.as_slice(), &[body]);
    Ok(())
}

#[tokio::test]
async fn requested_service_tier_reaches_limit_cost_estimator_before_dispatch() {
    // Given a priority request with limit reservation enabled.
    let state = TestState::default();
    let estimator = Arc::new(RecordingLimitCostEstimator::default());
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(cc_lb_clock::SystemClock),
    );
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
    )
    .with_static_limit_subject(
        limit_engine,
        "principal-test".to_owned(),
        "key-test".to_owned(),
        StoredApiKeyRecord {
            key_hash_b64: "key-test".to_owned(),
            status: KeyStatus::Active,
            ..StoredApiKeyRecord::default()
        },
    )
    .with_limit_cost_estimator(estimator.clone());

    // When the request crosses the proxy lifecycle.
    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":16,"service_tier":"Priority-Raw","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");

    // Then reservation uses the exact raw requested tier.
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        *estimator.service_tiers.lock().expect("service tier lock"),
        vec![Some("Priority-Raw".to_owned())]
    );
}

#[tokio::test]
async fn oauth_db_record_drives_route_pricing_and_rejection_audit_identity() {
    let state = TestState::default();
    let principal_view = Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        vec![Limit {
            kind: LimitKind::OutputTokens,
            window_secs: 60,
            cap_micros: 1,
        }],
        std::collections::HashMap::new(),
    ));
    let authn = TestAuthn::with_principal_view(state.clone(), principal_view);
    let estimator = Arc::new(RecordingLimitCostEstimator::default());
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.attach_lifecycle_writer(32);
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![oauth_upstream_record()])
        .build();
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(cc_lb_clock::SystemClock),
    );
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(MockDispatch {
            state: state.clone(),
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        }),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_static_limit_subject(
        limit_engine,
        "principal-test".to_owned(),
        "key-test".to_owned(),
        StoredApiKeyRecord {
            key_hash_b64: "key-test".to_owned(),
            status: KeyStatus::Active,
            ..StoredApiKeyRecord::default()
        },
    )
    .with_limit_cost_estimator(estimator.clone())
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":16,"messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);

    let (route_kind, rejected_upstream_name) = timeout(Duration::from_secs(1), async {
        let mut route_kind = None;
        loop {
            match lifecycle_events
                .recv()
                .await
                .expect("lifecycle event channel remains open")
            {
                LifecycleEvent::RouteCompleted {
                    result: Ok(route), ..
                } => route_kind = route.upstream_kind,
                LifecycleEvent::LimitDecision {
                    decision: LimitDecisionKind::Rejected { route_summary, .. },
                    ..
                } => {
                    break (
                        route_kind.expect("successful route event precedes limit rejection"),
                        route_summary.upstream_name,
                    );
                }
                _ => {}
            }
        }
    })
    .await
    .expect("limit rejection lifecycle event arrives");

    assert_eq!(route_kind, "anthropic_oauth");
    assert_eq!(rejected_upstream_name, "oauth-db-primary");
}

#[tokio::test]
async fn invalid_key_is_rejected_at_the_authentication_boundary() {
    let state = TestState::default();
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
    );
    let mut request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    request.headers_mut().insert(
        "x-api-key",
        http::HeaderValue::from_static("invalid-managed-key"),
    );

    // Authentication now runs before `handle`: an invalid key is rejected at
    // the boundary and never reaches request work.
    let Err(error) = lifecycle.authenticate(request.headers()).await else {
        panic!("invalid key fails authentication");
    };
    let response = cc_lb_engine::reject_unauthenticated(&error, None);

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).expect("error body is JSON");
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "authentication_error");
}

fn oauth_upstream_record() -> UpstreamRecord {
    UpstreamRecord {
        id: uuid::Uuid::from_u128(1),
        name: "oauth-db-primary".to_owned(),
        kind: StorageUpstreamKind::AnthropicOauth,
        base_url: Some("http://upstream.local/".parse().expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        oauth_never_refresh: false,
        api_key_ciphertext: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
        workspace_id: None,
    }
}

// USD limit admission boundary.
//
// Most priced test models cost $1/M input and $10/M output, so with the
// lifecycle's fixed 4,000-token input estimate a request reserves
// `4_000 + 10 * max_output` micros. The image model below costs $5/M generic
// input and $8/M image input.
const PRICED_GEMINI_MODEL: &str = "gemini-limit-test";
const UNPRICED_GEMINI_MODEL: &str = "gemini-unpriced-test";
const PRICED_OPENAI_MODEL: &str = "gpt-limit-test";
const PRICED_OPENAI_IMAGE_MODEL: &str = "gpt-image-limit-test";

/// Rolling alias whose canonical dated form is deliberately left unpriced: a
/// limit lookup that canonicalizes the raw body model misses the price.
const PRICED_RAW_CLAUDE_ALIAS: &str = "claude-sonnet-4-5";
const COST_WITH_2000_OUTPUT_MICROS: i64 = 24_000;
const COST_WITHOUT_OUTPUT_MICROS: i64 = 4_000;

#[tokio::test]
async fn openai_image_multipart_admission_uses_image_rate_floor() {
    for path in ["/v1/images/edits", "/v1/images/variations"] {
        for (cap_micros, expected_status, expected_calls) in [
            (31_999, StatusCode::TOO_MANY_REQUESTS, 0),
            (32_000, StatusCode::OK, 1),
        ] {
            let state = TestState::default();
            let lifecycle =
                usd_limited_lifecycle(StorageUpstreamKind::OpenaiApiKey, cap_micros, &state);
            let body = Bytes::from(format!(
                "--image-boundary\r\n\
                 Content-Disposition: form-data; name=\"model\"\r\n\r\n\
                 {PRICED_OPENAI_IMAGE_MODEL}\r\n\
                 --image-boundary\r\n\
                 Content-Disposition: form-data; name=\"file\"; filename=\"input.png\"\r\n\
                 Content-Type: image/png\r\n\r\n\
                 image-bytes\r\n\
                 --image-boundary--\r\n"
            ));
            let mut request = http::Request::builder()
                .method("POST")
                .uri(path)
                .header("x-api-key", managed_api_key())
                .header(
                    http::header::CONTENT_TYPE,
                    "multipart/form-data; boundary=image-boundary",
                )
                .body(body)
                .expect("OpenAI Images multipart request builds");
            request
                .extensions_mut()
                .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Openai));
            let response = handle(&lifecycle, request).await;
            if expected_status == StatusCode::OK {
                let (status, _headers, body) = collect_body(response).await;
                assert_eq!(status, expected_status, "path {path}, body: {body:?}");
            } else {
                assert_cost_rate_limited(response).await;
            }
            assert_eq!(
                state.upstream_calls.load(Ordering::Relaxed),
                expected_calls,
                "path {path}, cap {cap_micros}"
            );
        }
    }
}

#[tokio::test]
async fn gemini_url_model_with_nested_output_cap_is_admitted_under_usd_limit() {
    // Given a USD cap that covers a 2,000-token Gemini output reservation.
    let state = TestState::default();
    let lifecycle = usd_limited_lifecycle(
        StorageUpstreamKind::GeminiApiKey,
        COST_WITH_2000_OUTPUT_MICROS,
        &state,
    );

    // When a native Gemini request names its model only in the URL.
    let response = handle(
        &lifecycle,
        gemini_request(
            PRICED_GEMINI_MODEL,
            br#"{"contents":[],"generationConfig":{"maxOutputTokens":2000}}"#,
        ),
    )
    .await;

    // Then the request is priced and reaches the upstream.
    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn gemini_nested_output_cap_counts_toward_usd_limit_rejection() {
    // Given a USD cap above the input-only estimate but below the estimate
    // that includes the nested 2,000-token output cap.
    let state = TestState::default();
    let lifecycle = usd_limited_lifecycle(
        StorageUpstreamKind::GeminiApiKey,
        COST_WITH_2000_OUTPUT_MICROS - 1,
        &state,
    );

    // When the Gemini request asks for 2,000 output tokens.
    let response = handle(
        &lifecycle,
        gemini_request(
            PRICED_GEMINI_MODEL,
            br#"{"contents":[],"generationConfig":{"maxOutputTokens":2000}}"#,
        ),
    )
    .await;

    // Then the cost limit rejects it before dispatch.
    assert_cost_rate_limited(response).await;
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn unpriced_gemini_url_model_fails_closed_naming_the_requested_model() {
    // Given a USD cap and a Gemini model absent from the price catalog.
    let state = TestState::default();
    let lifecycle = usd_limited_lifecycle(
        StorageUpstreamKind::GeminiApiKey,
        COST_WITH_2000_OUTPUT_MICROS,
        &state,
    );

    // When the unpriced model is requested through the Gemini URL.
    let response = handle(
        &lifecycle,
        gemini_request(
            UNPRICED_GEMINI_MODEL,
            br#"{"contents":[],"generationConfig":{"maxOutputTokens":2000}}"#,
        ),
    )
    .await;

    // Then admission fails closed with the real model name and no dispatch.
    let (status, headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!headers.contains_key("x-should-retry"));
    assert!(!headers.contains_key(http::header::RETRY_AFTER));
    let body: serde_json::Value = serde_json::from_slice(&body).expect("error body is JSON");
    assert_eq!(body["error"]["type"], "server_error");
    assert!(body["error"].get("retry_after_seconds").is_none());
    assert_eq!(
        body["error"]["message"],
        format!("cost limits unavailable for model {UNPRICED_GEMINI_MODEL}")
    );
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn gemini_native_output_cap_dominates_wrong_provider_fields_for_usd_limit() {
    // A Gemini request's native generationConfig bound must win over a
    // top-level OpenAI/Anthropic-shaped field.
    let state = TestState::default();
    let lifecycle = usd_limited_lifecycle(
        StorageUpstreamKind::GeminiApiKey,
        COST_WITH_2000_OUTPUT_MICROS - 1,
        &state,
    );

    let response = handle(
        &lifecycle,
        gemini_request(
            PRICED_GEMINI_MODEL,
            br#"{"contents":[],"max_tokens":1,"generationConfig":{"maxOutputTokens":2000}}"#,
        ),
    )
    .await;

    // The native 2,000-token bound is what the provider honors, so the USD
    // limit rejects the request before dispatch.
    assert_cost_rate_limited(response).await;
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn gemini_request_without_any_output_cap_reserves_zero_output() {
    // Given a USD cap that covers exactly the input-only estimate.
    let state = TestState::default();
    let lifecycle = usd_limited_lifecycle(
        StorageUpstreamKind::GeminiApiKey,
        COST_WITHOUT_OUTPUT_MICROS,
        &state,
    );

    // When the Gemini request carries no output cap anywhere.
    let response = handle(
        &lifecycle,
        gemini_request(PRICED_GEMINI_MODEL, br#"{"contents":[]}"#),
    )
    .await;

    // Then no output cost is reserved and the request is dispatched.
    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn anthropic_raw_alias_and_top_level_max_tokens_drive_usd_limit_admission() {
    // Given a USD cap that covers a 2,000-token reservation priced under the
    // raw alias only.
    let state = TestState::default();
    let lifecycle = usd_limited_lifecycle(
        StorageUpstreamKind::AnthropicApiKey,
        COST_WITH_2000_OUTPUT_MICROS,
        &state,
    );

    // When an Anthropic request sends the raw alias with top-level max_tokens.
    let response = handle(
        &lifecycle,
        messages_request(Bytes::from_static(
            br#"{"model":"claude-sonnet-4-5","max_tokens":2000,"messages":[]}"#,
        )),
    )
    .await;

    // Then the raw alias is priced and the request is dispatched.
    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn anthropic_max_tokens_wins_over_max_output_tokens_for_usd_limit_rejection() {
    // Given a USD cap below the 2,000-token reservation for the raw alias.
    let state = TestState::default();
    let lifecycle = usd_limited_lifecycle(
        StorageUpstreamKind::AnthropicApiKey,
        COST_WITH_2000_OUTPUT_MICROS - 1,
        &state,
    );

    // When max_tokens is large and max_output_tokens is tiny.
    let response = handle(
        &lifecycle,
        messages_request(Bytes::from_static(
            br#"{"model":"claude-sonnet-4-5","max_tokens":2000,"max_output_tokens":1,"messages":[]}"#,
        )),
    )
    .await;

    // Then max_tokens is reserved, so the cost cap rejects before dispatch.
    assert_cost_rate_limited(response).await;
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn openai_max_output_tokens_drives_usd_limit_admission_and_rejection() {
    for (cap_micros, expected_status, expected_calls) in [
        (COST_WITH_2000_OUTPUT_MICROS, StatusCode::OK, 1),
        (
            COST_WITH_2000_OUTPUT_MICROS - 1,
            StatusCode::TOO_MANY_REQUESTS,
            0,
        ),
    ] {
        // Given a USD cap at or just below the 2,000-token reservation.
        let state = TestState::default();
        let lifecycle =
            usd_limited_lifecycle(StorageUpstreamKind::OpenaiApiKey, cap_micros, &state);

        // When an OpenAI Responses request sets max_output_tokens.
        let request = http::Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("x-api-key", managed_api_key())
            .body(Bytes::from_static(
                br#"{"model":"gpt-limit-test","max_output_tokens":2000,"input":"hi"}"#,
            ))
            .expect("OpenAI request builds");
        let response = handle(&lifecycle, request).await;

        // Then max_output_tokens decides admission at the cap boundary.
        let (status, _headers, body) = collect_body(response).await;
        assert_eq!(status, expected_status, "cap {cap_micros}, body: {body:?}");
        assert_eq!(
            state.upstream_calls.load(Ordering::Relaxed),
            expected_calls,
            "cap {cap_micros}"
        );
    }
}

#[tokio::test]
async fn provider_output_cap_requires_a_provider_honored_generation_bound() {
    // QA-01/02: an output-limited key rejects generation requests whose
    // provider-specific output bound is absent, null, or malformed.
    let cases = [
        (
            "responses absent",
            StorageUpstreamKind::OpenaiApiKey,
            "/v1/responses",
            serde_json::json!({"model":"gpt-limit-test","input":"hi"}),
        ),
        (
            "responses null",
            StorageUpstreamKind::OpenaiApiKey,
            "/v1/responses",
            serde_json::json!({"model":"gpt-limit-test","max_output_tokens":null,"input":"hi"}),
        ),
        (
            "responses wrong type",
            StorageUpstreamKind::OpenaiApiKey,
            "/v1/responses",
            serde_json::json!({"model":"gpt-limit-test","max_output_tokens":"1","input":"hi"}),
        ),
        (
            "responses ignored max_tokens",
            StorageUpstreamKind::OpenaiApiKey,
            "/v1/responses",
            serde_json::json!({"model":"gpt-limit-test","max_tokens":1,"input":"hi"}),
        ),
        (
            "gemini absent",
            StorageUpstreamKind::GeminiApiKey,
            "/v1beta/models/gemini-limit-test:generateContent",
            serde_json::json!({"contents":[]}),
        ),
        (
            "gemini empty generation config",
            StorageUpstreamKind::GeminiApiKey,
            "/v1beta/models/gemini-limit-test:generateContent",
            serde_json::json!({"contents":[],"generationConfig":{}}),
        ),
        (
            "gemini null generation config",
            StorageUpstreamKind::GeminiApiKey,
            "/v1beta/models/gemini-limit-test:generateContent",
            serde_json::json!({"contents":[],"generationConfig":null}),
        ),
        (
            "gemini wrong generation config type",
            StorageUpstreamKind::GeminiApiKey,
            "/v1beta/models/gemini-limit-test:generateContent",
            serde_json::json!({"contents":[],"generationConfig":[]}),
        ),
        (
            "gemini stream absent",
            StorageUpstreamKind::GeminiApiKey,
            "/v1beta/models/gemini-limit-test:streamGenerateContent",
            serde_json::json!({"contents":[],"stream":true}),
        ),
        (
            "gemini ignored max_tokens",
            StorageUpstreamKind::GeminiApiKey,
            "/v1beta/models/gemini-limit-test:generateContent",
            serde_json::json!({"contents":[],"max_tokens":1}),
        ),
    ];

    for (name, kind, path, body) in cases {
        let state = TestState::default();
        let lifecycle = output_limited_lifecycle(kind, 1, &state);
        let response = handle(
            &lifecycle,
            provider_post_request(path, Bytes::from(body.to_string())),
        )
        .await;
        assert_output_cap_rejected(response).await;
        assert_eq!(
            state.upstream_calls.load(Ordering::Relaxed),
            0,
            "{name} dispatched upstream"
        );
    }
}

#[derive(Clone, Copy)]
enum ProviderOutputCase {
    Responses,
    Gemini,
    AnthropicMessages,
}

fn provider_output_request(
    case: ProviderOutputCase,
    max_tokens: i64,
) -> (StorageUpstreamKind, http::Request<Bytes>) {
    match case {
        ProviderOutputCase::Responses => (
            StorageUpstreamKind::OpenaiApiKey,
            provider_post_request(
                "/v1/responses",
                Bytes::from(
                    serde_json::json!({
                        "model": PRICED_OPENAI_MODEL,
                        "max_output_tokens": max_tokens,
                        "input": "hi"
                    })
                    .to_string(),
                ),
            ),
        ),
        ProviderOutputCase::Gemini => (
            StorageUpstreamKind::GeminiApiKey,
            provider_post_request(
                "/v1beta/models/gemini-limit-test:generateContent",
                Bytes::from(
                    serde_json::json!({
                        "contents": [],
                        "generationConfig": {"maxOutputTokens": max_tokens}
                    })
                    .to_string(),
                ),
            ),
        ),
        ProviderOutputCase::AnthropicMessages => (
            StorageUpstreamKind::AnthropicApiKey,
            provider_post_request(
                "/v1/messages",
                Bytes::from(
                    serde_json::json!({
                        "model": PRICED_RAW_CLAUDE_ALIAS,
                        "max_tokens": max_tokens,
                        "messages": []
                    })
                    .to_string(),
                ),
            ),
        ),
    }
}

#[tokio::test]
async fn provider_output_cap_accepts_exact_native_caps_and_rejects_larger_caps() {
    // QA-03: each operation uses the bound its provider actually honors.
    for (case, name) in [
        (ProviderOutputCase::Responses, "responses"),
        (ProviderOutputCase::Gemini, "gemini"),
        (ProviderOutputCase::AnthropicMessages, "anthropic messages"),
    ] {
        for (requested, expected_status, expected_calls) in [
            (1_i64, StatusCode::OK, 1_u64),
            (2_i64, StatusCode::BAD_REQUEST, 0_u64),
        ] {
            let state = TestState::default();
            let (kind, request) = provider_output_request(case, requested);
            let lifecycle = output_limited_lifecycle(kind, 1, &state);
            let response = handle(&lifecycle, request).await;
            if expected_status == StatusCode::BAD_REQUEST {
                assert_output_cap_rejected(response).await;
            } else {
                let (status, _headers, body) = collect_body(response).await;
                assert_eq!(status, expected_status, "{name}: {body:?}");
            }
            assert_eq!(
                state.upstream_calls.load(Ordering::Relaxed),
                expected_calls,
                "{name} requested={requested}"
            );
        }
    }
}

#[tokio::test]
async fn uncapped_output_total_token_admission_remains_input_only() {
    // QA-04: a key with no OutputTokens limit retains the existing input-only
    // behavior for a request that omits a provider output bound.
    let state = TestState::default();
    let lifecycle = limited_lifecycle(
        StorageUpstreamKind::OpenaiApiKey,
        vec![Limit {
            kind: LimitKind::TotalTokens,
            window_secs: 60,
            cap_micros: 4_000,
        }],
        &state,
        None,
    );
    let response = handle(
        &lifecycle,
        provider_post_request(
            "/v1/responses",
            Bytes::from_static(br#"{"model":"gpt-limit-test","input":"hi"}"#),
        ),
    )
    .await;
    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn provider_output_cap_does_not_charge_non_generating_operations() {
    // QA-05: token counting has no generated output and remains admissible
    // under an output cap of zero.
    let state = TestState::default();
    let lifecycle = output_limited_lifecycle(StorageUpstreamKind::GeminiApiKey, 0, &state);
    let response = handle(
        &lifecycle,
        provider_post_request(
            "/v1beta/models/gemini-limit-test:countTokens",
            Bytes::from_static(br#"{"contents":[]}"#),
        ),
    )
    .await;
    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn anthropic_count_tokens_does_not_charge_non_generating_operations() {
    let state = TestState::default();
    let lifecycle = output_limited_lifecycle(StorageUpstreamKind::AnthropicApiKey, 0, &state);
    let response = handle(
        &lifecycle,
        provider_post_request(
            "/v1/messages/count_tokens",
            Bytes::from_static(
                br#"{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}"#,
            ),
        ),
    )
    .await;
    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
}

fn provider_post_request(path: &str, body: Bytes) -> http::Request<Bytes> {
    http::Request::builder()
        .method("POST")
        .uri(path)
        .header("x-api-key", managed_api_key())
        .body(body)
        .expect("provider request builds")
}

#[tokio::test]
async fn rejected_provider_output_cap_does_not_consume_request_or_concurrent_reservations() {
    // QA-08: a missing provider cap is rejected before Requests/Concurrent
    // reservations, allowing the following valid request through.
    let state = TestState::default();
    let lifecycle = limited_lifecycle(
        StorageUpstreamKind::AnthropicApiKey,
        vec![
            Limit {
                kind: LimitKind::OutputTokens,
                window_secs: 60,
                cap_micros: 1,
            },
            Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 1,
            },
            Limit {
                kind: LimitKind::Concurrent,
                window_secs: 0,
                cap_micros: 1,
            },
        ],
        &state,
        None,
    );
    let rejected = handle(
        &lifecycle,
        provider_post_request(
            "/v1/messages",
            Bytes::from_static(br#"{"model":"claude-sonnet-4-5","messages":[]}"#),
        ),
    )
    .await;
    assert_output_cap_rejected(rejected).await;
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);

    let admitted = handle(
        &lifecycle,
        provider_post_request(
            "/v1/messages",
            Bytes::from_static(br#"{"model":"claude-sonnet-4-5","max_tokens":1,"messages":[]}"#),
        ),
    )
    .await;
    let (status, _headers, body) = collect_body(admitted).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
}
#[tokio::test]
async fn openai_chat_max_completion_tokens_respects_output_token_limit_boundary() {
    for (cap, expected_status, expected_calls) in [
        (2_000, StatusCode::OK, 1),
        (1_999, StatusCode::BAD_REQUEST, 0),
    ] {
        // Given an OpenAI Chat Completions request reserving 2,000 output
        // tokens and a limit at, or just below, that reservation.
        let state = TestState::default();
        let lifecycle = limited_lifecycle(
            StorageUpstreamKind::OpenaiApiKey,
            vec![Limit {
                kind: LimitKind::OutputTokens,
                window_secs: 60,
                cap_micros: cap,
            }],
            &state,
            None,
        );
        let mut request = http::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("x-api-key", managed_api_key())
            .body(Bytes::from_static(
                br#"{"model":"gpt-limit-test","max_completion_tokens":2000,"messages":[]}"#,
            ))
            .expect("OpenAI Chat Completions request builds");
        request
            .extensions_mut()
            .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Openai));
        let response = handle(&lifecycle, request).await;
        if expected_status == StatusCode::OK {
            let (status, _headers, body) = collect_body(response).await;
            assert_eq!(status, expected_status, "cap {cap}, body: {body:?}");
        } else {
            assert_local_limit_retryability(response, expected_status, None, None, None).await;
        }
        assert_eq!(
            state.upstream_calls.load(Ordering::Relaxed),
            expected_calls,
            "cap {cap}"
        );
    }
}

struct CatalogLimitCostEstimator {
    catalog: Arc<PriceCatalog>,
}

impl LimitCostEstimator for CatalogLimitCostEstimator {
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

fn priced_catalog() -> Arc<PriceCatalog> {
    let models = [
        PRICED_GEMINI_MODEL,
        PRICED_OPENAI_MODEL,
        PRICED_OPENAI_IMAGE_MODEL,
        PRICED_RAW_CLAUDE_ALIAS,
    ]
    .into_iter()
    .map(|model| {
        let image_rate =
            (model == PRICED_OPENAI_IMAGE_MODEL).then(|| UsdPerMillion::from_whole_usd(8));
        (
            model.to_owned(),
            Pricing {
                model: model.to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(if image_rate.is_some() {
                    5
                } else {
                    1
                }),
                output_per_million_usd: UsdPerMillion::from_whole_usd(10),
                input_image_per_million_usd: image_rate,
                output_image_per_million_usd: None,
                by_tier: BTreeMap::new(),
            },
        )
    })
    .collect();
    let mut above_tokens_rates = HashMap::new();
    above_tokens_rates.insert(
        PRICED_OPENAI_MODEL.to_owned(),
        BTreeMap::from([(
            200_000,
            cc_lb_pricing::ThresholdBand {
                default: cc_lb_pricing::ThresholdRate {
                    input_per_million_usd: Some(UsdPerMillion::from_whole_usd(10)),
                    ..cc_lb_pricing::ThresholdRate::default()
                },
                by_tier: BTreeMap::new(),
            },
        )]),
    );
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(CatalogSnapshot {
        models,
        above_tokens_rates,
        status: CatalogStatus::Ok,
        ..CatalogSnapshot::empty_cost_disabled()
    });
    catalog
}

fn usd_limited_lifecycle(
    kind: StorageUpstreamKind,
    cap_micros: i64,
    state: &TestState,
) -> Lifecycle {
    limited_lifecycle(
        kind,
        vec![Limit {
            kind: LimitKind::CostUsd,
            window_secs: 60,
            cap_micros,
        }],
        state,
        None,
    )
}

fn limited_lifecycle(
    kind: StorageUpstreamKind,
    limits: Vec<Limit>,
    state: &TestState,
    dispatch: Option<Arc<dyn UpstreamDispatch>>,
) -> Lifecycle {
    limited_lifecycle_at(
        kind,
        limits,
        state,
        dispatch,
        Some("http://upstream.local/".parse().expect("test URL parses")),
    )
}

fn output_limited_lifecycle(
    kind: StorageUpstreamKind,
    cap_tokens: i64,
    state: &TestState,
) -> Lifecycle {
    limited_lifecycle(
        kind,
        vec![Limit {
            kind: LimitKind::OutputTokens,
            window_secs: 60,
            cap_micros: cap_tokens,
        }],
        state,
        None,
    )
}

async fn assert_output_cap_rejected(response: http::Response<axum::body::Body>) {
    let (status, headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body:?}");
    assert!(!headers.contains_key("x-should-retry"));
    assert!(!headers.contains_key(http::header::RETRY_AFTER));
    let body: serde_json::Value = serde_json::from_slice(&body).expect("error body is JSON");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(body["error"]["limit_kind"], serde_json::Value::Null);
    assert!(body["error"].get("retry_after_seconds").is_none());
}

fn limited_lifecycle_at(
    kind: StorageUpstreamKind,
    limits: Vec<Limit>,
    state: &TestState,
    dispatch: Option<Arc<dyn UpstreamDispatch>>,
    record_base_url: Option<url::Url>,
) -> Lifecycle {
    let base_url = record_base_url.clone().unwrap_or_else(|| {
        "https://api.anthropic.com/"
            .parse()
            .expect("official URL parses")
    });
    let record = UpstreamRecord {
        id: uuid::Uuid::from_u128(2),
        name: "usd-limit-upstream".to_owned(),
        kind,
        base_url: record_base_url,
        api_key_ciphertext: Some(Vec::new()),
        ..oauth_upstream_record()
    };
    let mut chains: HashMap<String, PrincipalRoutingArtifacts> = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            Some(Arc::new(RouterPipelineCache::empty(
                TerminalStrategy::FirstPick,
            ))),
            DialectCache::Explicit(ShapePluginCache {
                dialect: Arc::new(PassthroughDialect { base_url }),
            }),
        ),
    );
    let principal = PrincipalRecord {
        id: uuid::Uuid::new_v4(),
        name: "principal-test".to_owned(),
        kind: cc_lb_storage_api::PrincipalKind::Machine,
        allowed_models: vec!["*".to_owned()],
        allowed_upstreams: vec![record.id],
        default_limits: limits,
        enabled: true,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
        cache_keepalive: None,
    };
    let authn = TestAuthn::with_principal_view(
        state.clone(),
        Arc::new(PrincipalView::from_db(&[principal], chains)),
    );
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![record])
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatch.unwrap_or_else(|| {
            Arc::new(MockDispatch {
                state: state.clone(),
                mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
            })
        }),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_static_limit_subject(
        LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::SystemClock),
        ),
        "principal-test".to_owned(),
        "key-test".to_owned(),
        StoredApiKeyRecord {
            key_hash_b64: "key-test".to_owned(),
            status: KeyStatus::Active,
            ..StoredApiKeyRecord::default()
        },
    )
    .with_limit_cost_estimator(Arc::new(CatalogLimitCostEstimator {
        catalog: priced_catalog(),
    }))
}

fn gemini_request(model: &str, body: &'static [u8]) -> http::Request<Bytes> {
    http::Request::builder()
        .method("POST")
        .uri(format!("/v1beta/models/{model}:generateContent"))
        .header("x-api-key", managed_api_key())
        .body(Bytes::from_static(body))
        .expect("Gemini request builds")
}

async fn handle(
    lifecycle: &Lifecycle,
    request: http::Request<Bytes>,
) -> http::Response<axum::body::Body> {
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request")
}

async fn assert_cost_rate_limited(response: http::Response<axum::body::Body>) {
    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "body: {body:?}");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("error body is JSON");
    assert_eq!(body["error"]["type"], "rate_limit_error");
    assert_eq!(body["error"]["limit_kind"], "cost_usd");
}

async fn assert_local_limit_retryability(
    response: http::Response<axum::body::Body>,
    status: StatusCode,
    kind: Option<&str>,
    should_retry: Option<&str>,
    delay: Option<u64>,
) -> serde_json::Value {
    let (actual_status, headers, body) = collect_body(response).await;
    assert_eq!(actual_status, status, "body: {body:?}");
    assert_eq!(
        headers
            .get("x-should-retry")
            .map(|value| value.to_str().expect("ASCII retry hint")),
        should_retry,
        "body: {body:?}",
    );
    assert_eq!(
        headers.get(http::header::RETRY_AFTER).map(|value| value
            .to_str()
            .expect("ASCII retry delay")
            .parse::<u64>()
            .expect("numeric retry delay")),
        delay,
    );
    let body: serde_json::Value = serde_json::from_slice(&body).expect("limit error JSON");
    let error = &body["error"];
    assert_eq!(
        error["type"],
        if status == StatusCode::BAD_REQUEST {
            "invalid_request_error"
        } else {
            "rate_limit_error"
        },
    );
    assert_eq!(error["limit_kind"].as_str(), kind);
    assert_eq!(error["retry_after_seconds"].as_u64(), delay);
    if delay.is_none() {
        assert!(error.get("retry_after_seconds").is_none());
    }
    if should_retry == Some("false") {
        let message = error["message"].as_str().expect("limit message");
        assert!(message.contains("cannot fit"), "{message}");
    }
    body
}

#[tokio::test]
async fn local_limit_retryability_unsatisfiable_cost_and_exact_cap_control() {
    // QA-01/02: the same authenticated request fits exactly, but cannot fit
    // an empty window whose configured cap is one micro smaller.
    for cap in [24_000, 23_999] {
        let state = TestState::default();
        let lifecycle = usd_limited_lifecycle(StorageUpstreamKind::AnthropicApiKey, cap, &state);
        let response = handle(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-sonnet-4-5","max_tokens":2000,"messages":[]}"#,
            )),
        )
        .await;
        if cap == 24_000 {
            let (status, _, body) = collect_body(response).await;
            assert_eq!(status, StatusCode::OK, "{body:?}");
            assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
        } else {
            assert_local_limit_retryability(
                response,
                StatusCode::TOO_MANY_REQUESTS,
                Some("cost_usd"),
                Some("false"),
                None,
            )
            .await;
            assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
        }
    }
}

#[tokio::test]
async fn local_limit_retryability_unsatisfiable_token_and_request_boundaries() {
    // QA-03/04/05: configured capacity, not current usage, makes these
    // requests impossible. Equality remains a healthy admission control.
    for (kind, name, requested) in [
        (LimitKind::InputTokens, "input_tokens", 4_000),
        (LimitKind::TotalTokens, "total_tokens", 6_000),
        (LimitKind::Requests, "requests", 1),
    ] {
        for cap in [requested, requested - 1] {
            let state = TestState::default();
            let lifecycle = limited_lifecycle(
                StorageUpstreamKind::AnthropicApiKey,
                vec![Limit {
                    kind,
                    window_secs: 60,
                    cap_micros: cap,
                }],
                &state,
                None,
            );
            let response = handle(
                &lifecycle,
                messages_request(Bytes::from_static(
                    br#"{"model":"claude-sonnet-4-5","max_tokens":2000,"messages":[]}"#,
                )),
            )
            .await;
            if cap == requested {
                let (status, _, body) = collect_body(response).await;
                assert_eq!(status, StatusCode::OK, "{name}: {body:?}");
                assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1, "{name}");
            } else {
                assert_local_limit_retryability(
                    response,
                    StatusCode::TOO_MANY_REQUESTS,
                    Some(name),
                    Some("false"),
                    None,
                )
                .await;
                assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0, "{name}");
            }
        }
    }
}

#[tokio::test]
async fn local_limit_retryability_held_windows_and_concurrent_slot_release() {
    // QA-06/07/14: hold a real admitted reservation before dispatch completes.
    // No sleep or settlement timing is involved in exhausting the next request.
    for (kind, name, cap, delay) in [
        (LimitKind::Requests, "requests", 1, 60),
        (LimitKind::InputTokens, "input_tokens", 4_000, 60),
        (LimitKind::OutputTokens, "output_tokens", 2_000, 60),
        (LimitKind::TotalTokens, "total_tokens", 6_000, 60),
        (LimitKind::CostUsd, "cost_usd", 24_000, 60),
        (LimitKind::Concurrent, "concurrent", 1, 1),
    ] {
        let state = TestState::default();
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let test_bus = TestLifecycleBus::new();
        let reconcile_rx = test_bus.bus.attach_lifecycle_limit_reconcile(128);
        let limit_engine = LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::SystemClock),
        );
        let lifecycle = limited_lifecycle(
            StorageUpstreamKind::AnthropicApiKey,
            vec![Limit {
                kind,
                window_secs: if kind == LimitKind::Concurrent { 0 } else { 60 },
                cap_micros: cap,
            }],
            &state,
            Some(Arc::new(GatedAdmissionDispatch {
                inner: MockDispatch {
                    state: state.clone(),
                    mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
                },
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
            })),
        )
        .with_static_limit_subject(
            limit_engine.clone(),
            "principal-test".to_owned(),
            "key-test".to_owned(),
            StoredApiKeyRecord {
                key_hash_b64: "key-test".to_owned(),
                status: KeyStatus::Active,
                ..StoredApiKeyRecord::default()
            },
        )
        .with_event_bus(test_bus.bus_arc());
        let bytes =
            Bytes::from_static(br#"{"model":"claude-sonnet-4-5","max_tokens":2000,"messages":[]}"#);
        let first = handle(&lifecycle, messages_request(bytes.clone()));
        tokio::pin!(first);
        tokio::select! {
            permit = entered.acquire() => permit.expect("first dispatch entered").forget(),
            response = &mut first => panic!("first request did not reach held dispatch: {}", response.status()),
        }
        let second = handle(&lifecycle, messages_request(bytes.clone()));
        tokio::pin!(second);
        let response = tokio::select! {
            permit = entered.acquire() => {
                permit.expect("unexpected second dispatch").forget();
                panic!("{name}: exhausted limit allowed another dispatch");
            },
            response = &mut second => response,
        };
        assert_local_limit_retryability(
            response,
            StatusCode::TOO_MANY_REQUESTS,
            Some(name),
            Some("true"),
            Some(delay),
        )
        .await;
        assert_eq!(
            state.upstream_calls.load(Ordering::Relaxed),
            0,
            "first dispatch is still held"
        );
        release.add_permits(1);
        let (status, _, body) = collect_body(first.await).await;
        assert_eq!(status, StatusCode::OK, "{name}: {body:?}");
        assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
        // Buffered completion emits RequestTerminated before returning. Drain
        // the real subscriber so its settlement, not a timer, releases the slot.
        cc_lb_engine::spawn_lifecycle_limit_reconcile_subscriber(
            reconcile_rx,
            limit_engine.clone(),
        )
        .shutdown()
        .await;
        if kind == LimitKind::Concurrent {
            let reconcile_rx = test_bus.bus.attach_lifecycle_limit_reconcile(128);
            release.add_permits(1);
            let (status, _, body) =
                collect_body(handle(&lifecycle, messages_request(bytes)).await).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "released concurrency slot: {body:?}"
            );
            assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 2);
            cc_lb_engine::spawn_lifecycle_limit_reconcile_subscriber(reconcile_rx, limit_engine)
                .shutdown()
                .await;
        }
    }
}

#[tokio::test]
async fn local_limit_retryability_output_shape_and_zero_concurrency() {
    // QA-08/12: neither existing output-shape nor concurrent-zero semantics
    // may become the new unsatisfiable-window response.
    for (kind, cap, status, name, retry, delay) in [
        (
            LimitKind::OutputTokens,
            1_999,
            StatusCode::BAD_REQUEST,
            None,
            None,
            None,
        ),
        (
            LimitKind::Concurrent,
            0,
            StatusCode::TOO_MANY_REQUESTS,
            Some("concurrent"),
            Some("true"),
            Some(1),
        ),
    ] {
        let state = TestState::default();
        let lifecycle = limited_lifecycle(
            StorageUpstreamKind::AnthropicApiKey,
            vec![Limit {
                kind,
                window_secs: if kind == LimitKind::Concurrent { 0 } else { 60 },
                cap_micros: cap,
            }],
            &state,
            None,
        );
        let response = handle(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-sonnet-4-5","max_tokens":2000,"messages":[]}"#,
            )),
        )
        .await;
        assert_local_limit_retryability(response, status, name, retry, delay).await;
        assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn local_limit_retryability_temporary_limits_cannot_mask_permanent_failures() {
    // QA-10/11: the first request fits both limits. Its held reservation
    // exhausts the temporary limit; the larger second request cannot ever fit
    // the other configured cap. Exercise both declaration orders.
    for temporary_kind in [LimitKind::Requests, LimitKind::Concurrent] {
        for hard_kind in [LimitKind::TotalTokens, LimitKind::OutputTokens] {
            for temporary_first in [true, false] {
                let state = TestState::default();
                let entered = Arc::new(tokio::sync::Semaphore::new(0));
                let release = Arc::new(tokio::sync::Semaphore::new(0));
                let temporary = Limit {
                    kind: temporary_kind,
                    window_secs: if temporary_kind == LimitKind::Concurrent {
                        0
                    } else {
                        60
                    },
                    cap_micros: 1,
                };
                let hard = Limit {
                    kind: hard_kind,
                    window_secs: 60,
                    cap_micros: if hard_kind == LimitKind::TotalTokens {
                        5_999
                    } else {
                        1_999
                    },
                };
                let limits = if temporary_first {
                    vec![temporary, hard]
                } else {
                    vec![hard, temporary]
                };
                let lifecycle = limited_lifecycle(
                    StorageUpstreamKind::AnthropicApiKey,
                    limits,
                    &state,
                    Some(Arc::new(GatedAdmissionDispatch {
                        inner: MockDispatch {
                            state: state.clone(),
                            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
                        },
                        entered: Arc::clone(&entered),
                        release: Arc::clone(&release),
                    })),
                );
                let first = handle(
                    &lifecycle,
                    messages_request(Bytes::from_static(
                        br#"{"model":"claude-sonnet-4-5","max_tokens":1,"messages":[]}"#,
                    )),
                );
                tokio::pin!(first);
                tokio::select! {
                    permit = entered.acquire() => permit.expect("first dispatch entered").forget(),
                    response = &mut first => panic!("first request did not reach held dispatch: {}", response.status()),
                }
                let second = handle(
                    &lifecycle,
                    messages_request(Bytes::from_static(
                        br#"{"model":"claude-sonnet-4-5","max_tokens":2000,"messages":[]}"#,
                    )),
                );
                tokio::pin!(second);
                let response = tokio::select! {
                    permit = entered.acquire() => {
                        permit.expect("unexpected second dispatch").forget();
                        panic!("impossible second request reached dispatch");
                    },
                    response = &mut second => response,
                };
                if hard_kind == LimitKind::OutputTokens {
                    assert_local_limit_retryability(
                        response,
                        StatusCode::BAD_REQUEST,
                        None,
                        None,
                        None,
                    )
                    .await;
                } else {
                    assert_local_limit_retryability(
                        response,
                        StatusCode::TOO_MANY_REQUESTS,
                        Some("total_tokens"),
                        Some("false"),
                        None,
                    )
                    .await;
                }
                assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
                release.add_permits(1);
                let (status, _, body) = collect_body(first.await).await;
                assert_eq!(status, StatusCode::OK, "{body:?}");
                assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
            }
        }
    }
}

#[tokio::test]
async fn local_limit_retryability_permanent_failures_preserve_declaration_order() {
    // QA-11: preflight must not make output-shape errors universally win, or
    // reorder two impossible reservations by kind rather than declaration.
    for (left, right, expected_kind) in [
        (
            LimitKind::TotalTokens,
            LimitKind::OutputTokens,
            Some("total_tokens"),
        ),
        (LimitKind::OutputTokens, LimitKind::TotalTokens, None),
        (
            LimitKind::InputTokens,
            LimitKind::CostUsd,
            Some("input_tokens"),
        ),
        (LimitKind::CostUsd, LimitKind::InputTokens, Some("cost_usd")),
    ] {
        let state = TestState::default();
        let lifecycle = limited_lifecycle(
            StorageUpstreamKind::AnthropicApiKey,
            [left, right]
                .into_iter()
                .map(|kind| Limit {
                    kind,
                    window_secs: 60,
                    cap_micros: match kind {
                        LimitKind::InputTokens => 3_999,
                        LimitKind::TotalTokens => 5_999,
                        LimitKind::OutputTokens => 1_999,
                        LimitKind::CostUsd => 23_999,
                        _ => unreachable!("only permanent failures in this table"),
                    },
                })
                .collect(),
            &state,
            None,
        );
        let response = handle(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-sonnet-4-5","max_tokens":2000,"messages":[]}"#,
            )),
        )
        .await;
        assert_local_limit_retryability(
            response,
            if expected_kind.is_some() {
                StatusCode::TOO_MANY_REQUESTS
            } else {
                StatusCode::BAD_REQUEST
            },
            expected_kind,
            expected_kind.map(|_| "false"),
            None,
        )
        .await;
        assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn cache_bearing_request_below_body_cliff_reserves_cliff_cost_and_cache_token_bound() {
    // Base input rate is $1/M (4,000 min tokens => 4,000 micros), while above-200k cliff input
    // rate is $10/M. A request with a 4,000-token body estimate and an opaque cache marker
    // reserves the policy worst-case bound (1,048,576 total tokens = 4,000 input + 1,044,576
    // cache tokens at $10/M => 10,485,760 micros), covering even a 1,000,000-token cached
    // prefix (10,040,000 micros). A 10,000,000-micro cap (well above the 2,000,010-micro
    // minimum cliff-crossing amount) is rejected, while an 11,000,000-micro cap is admitted.
    for (cap_micros, expected_status, expected_calls) in [
        (10_000_000, StatusCode::TOO_MANY_REQUESTS, 0),
        (11_000_000, StatusCode::OK, 1),
    ] {
        let state = TestState::default();
        let lifecycle =
            usd_limited_lifecycle(StorageUpstreamKind::OpenaiApiKey, cap_micros, &state);
        let request = http::Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("x-api-key", managed_api_key())
            .body(Bytes::from_static(
                br#"{"model":"gpt-limit-test","prompt_cache_key":"cache-ns-1","input":"hi"}"#,
            ))
            .expect("OpenAI request builds");
        let response = handle(&lifecycle, request).await;
        let (status, _headers, _body) = collect_body(response).await;
        assert_eq!(status, expected_status, "cap {cap_micros}");
        assert_eq!(
            state.upstream_calls.load(Ordering::Relaxed),
            expected_calls,
            "cap {cap_micros}"
        );
    }
}

// Real grayscale PNGs: filter-0 scanlines, zlib IDAT, and CRC-checked chunks.
// Compression changes encoded length, not the approved admission allowance.
fn synthetic_png_base64(side: u32, compression: flate2::Compression) -> String {
    fn chunk(png: &mut Vec<u8>, kind: &[u8; 4], data: &[u8]) {
        png.extend_from_slice(
            &u32::try_from(data.len())
                .expect("small PNG chunk")
                .to_be_bytes(),
        );
        png.extend_from_slice(kind);
        png.extend_from_slice(data);
        let mut crc = !0_u32;
        for byte in kind.iter().chain(data) {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
            }
        }
        png.extend_from_slice(&(!crc).to_be_bytes());
    }
    let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
    let mut ihdr = Vec::new();
    ihdr.extend_from_slice(&side.to_be_bytes());
    ihdr.extend_from_slice(&side.to_be_bytes());
    ihdr.extend_from_slice(&[8, 0, 0, 0, 0]);
    chunk(&mut png, b"IHDR", &ihdr);
    let row = vec![0_u8; usize::try_from(side).expect("small side") + 1];
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), compression);
    for _ in 0..side {
        encoder.write_all(&row).expect("PNG scanline compresses");
    }
    chunk(
        &mut png,
        b"IDAT",
        &encoder.finish().expect("PNG zlib stream finishes"),
    );
    chunk(&mut png, b"IEND", &[]);
    base64::engine::general_purpose::STANDARD.encode(png)
}

fn native_base64_image(data: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "image",
        "source": {"type": "base64", "media_type": "image/png", "data": data}
    })
}

fn anthropic_content_request(content: serde_json::Value, max_tokens: i64) -> serde_json::Value {
    serde_json::json!({
        "model": PRICED_RAW_CLAUDE_ALIAS,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": content}]
    })
}

async fn assert_anthropic_limit_boundary(
    label: &str,
    body: &serde_json::Value,
    kind: LimitKind,
    expected_reservation: i64,
) {
    for (cap, expected_status, expected_calls) in [
        (expected_reservation, StatusCode::OK, 1),
        (expected_reservation - 1, StatusCode::TOO_MANY_REQUESTS, 0),
    ] {
        let state = TestState::default();
        let lifecycle = limited_lifecycle(
            StorageUpstreamKind::AnthropicApiKey,
            vec![Limit {
                kind,
                window_secs: 60,
                cap_micros: cap,
            }],
            &state,
            None,
        );
        let response = handle(
            &lifecycle,
            messages_request(Bytes::from(serde_json::to_vec(body).expect("request JSON"))),
        )
        .await;
        let (status, _, response_body) = collect_body(response).await;
        assert_eq!(
            status, expected_status,
            "{label}, cap {cap}: {response_body:?}"
        );
        assert_eq!(
            state.upstream_calls.load(Ordering::Relaxed),
            expected_calls,
            "{label}, cap {cap}"
        );
        if status == StatusCode::TOO_MANY_REQUESTS {
            let error: serde_json::Value =
                serde_json::from_slice(&response_body).expect("rate limit JSON");
            assert_eq!(error["error"]["type"], "rate_limit_error", "{label}");
        }
    }
}

#[tokio::test]
async fn admission_large_valid_base64_image_observes_exact_cost_boundary() {
    let data = synthetic_png_base64(1024, flate2::Compression::none());
    assert!(
        data.len() > 1_000_000,
        "fixture reproduces the encoded-byte false 429"
    );
    let body = anthropic_content_request(serde_json::json!([native_base64_image(&data)]), 1);
    // user(4) + image(5) + base64(6) + image/png(9) = 24 retained bytes.
    // $1/M input and $10/M output: 4,784 + 24/4 + 10 = 4,800 micros.
    assert_anthropic_limit_boundary("valid large PNG", &body, LimitKind::CostUsd, 4_800).await;
}

#[tokio::test]
async fn admission_text_control_preserves_exact_cost_boundary() {
    let body = anthropic_content_request(serde_json::json!("x".repeat(16_004)), 1);
    // user(4) plus 16,004 text bytes => 4,002 input and 10 output micros.
    assert_anthropic_limit_boundary("text control", &body, LimitKind::CostUsd, 4_012).await;
}

#[tokio::test]
async fn admission_native_images_increment_input_and_total_tokens_at_exact_boundary() {
    let data = synthetic_png_base64(1, flate2::Compression::best());
    for count in [0_usize, 1, 2, 4] {
        let images: Vec<_> = (0..count).map(|_| native_base64_image(&data)).collect();
        let body = anthropic_content_request(serde_json::json!(images), 7);
        let count = i64::try_from(count).expect("small image count");
        let whole = (4_784 * count + (4 + 20 * count) / 4).max(4_000);
        assert_anthropic_limit_boundary("native image input", &body, LimitKind::InputTokens, whole)
            .await;
        assert_anthropic_limit_boundary(
            "native image total",
            &body,
            LimitKind::TotalTokens,
            whole + 7,
        )
        .await;
    }
}

#[tokio::test]
async fn admission_compressed_same_geometry_image_pair_cannot_escape_allowance() {
    let compressed = synthetic_png_base64(1568, flate2::Compression::best());
    let uncompressed = synthetic_png_base64(1568, flate2::Compression::none());
    assert!(
        compressed.len() * 100 < uncompressed.len(),
        "same pixels, different encoding size"
    );
    // Two visual allowances plus user(4) and two image metadata groups(20 each).
    for data in [&compressed, &uncompressed] {
        let body = anthropic_content_request(
            serde_json::json!([native_base64_image(data), native_base64_image(data)]),
            1,
        );
        assert_anthropic_limit_boundary("same-geometry pair", &body, LimitKind::InputTokens, 9_579)
            .await;
    }
}

#[tokio::test]
async fn admission_nested_tool_result_images_use_native_image_allowance() {
    let data = synthetic_png_base64(1024, flate2::Compression::none());
    let body = anthropic_content_request(
        serde_json::json!([{
            "type": "tool_result",
            "tool_use_id": "toolu_1",
            "content": [native_base64_image(&data), {"type": "text", "text": "done"}]
        }]),
        1,
    );
    // user(4), tool_result(11), toolu_1(7), image metadata(20), text(4), done(4).
    let whole = 4_784 + 50 / 4;
    assert_anthropic_limit_boundary("nested tool result", &body, LimitKind::InputTokens, whole)
        .await;
    assert_anthropic_limit_boundary(
        "nested tool result cost",
        &body,
        LimitKind::CostUsd,
        whole + 10,
    )
    .await;
}

#[tokio::test]
async fn admission_native_url_images_exclude_only_the_reference_value() {
    for reference in [
        "https://images.example/pixel.png".to_owned(),
        format!(
            "https://images.example/pixel.png?signature={}",
            "x".repeat(40_000)
        ),
    ] {
        let body = anthropic_content_request(
            serde_json::json!([{"type":"image", "source":{"type":"url", "url":reference}}]),
            1,
        );
        // user(4), image(5), url(3); the reference itself is not text input.
        assert_anthropic_limit_boundary("URL image", &body, LimitKind::InputTokens, 4_787).await;
    }
    let body = anthropic_content_request(
        serde_json::json!([{
            "type":"image",
            "source":{"type":"url", "url":"https://images.example/pixel.png", "note":"x".repeat(16_000)}
        }]),
        1,
    );
    assert_anthropic_limit_boundary(
        "retained source metadata",
        &body,
        LimitKind::InputTokens,
        8_787,
    )
    .await;
}

#[tokio::test]
async fn admission_document_tool_schema_and_text_images_remain_generic_strings() {
    let data = synthetic_png_base64(128, flate2::Compression::none());
    let image = native_base64_image(&data);
    let cases = [
        anthropic_content_request(
            serde_json::json!([{"type":"document", "source":{"type":"base64","media_type":"application/pdf","data":data}}]),
            1,
        ),
        anthropic_content_request(
            serde_json::json!([{"type":"tool_use","id":"toolu_1","name":"inspect","input":{"example":image}}]),
            1,
        ),
        anthropic_content_request(
            serde_json::json!([{"type":"text", "text":image.to_string()}]),
            1,
        ),
        serde_json::json!({
            "model":PRICED_RAW_CLAUDE_ALIAS, "max_tokens":1,
            "tools":[{"name":"inspect","input_schema":{"type":"object","example":image}}],
            "messages":[{"role":"user","content":"hello"}]
        }),
    ];
    for body in cases {
        // Generic-string oracle for negative controls only: none of these
        // image-looking values are native image content.
        let whole = i64::try_from(selected_string_bytes(&body) / 4)
            .expect("small generic estimate")
            .max(4_000);
        assert!(
            whole > 4_784,
            "fixture distinguishes generic data from one image"
        );
        assert_anthropic_limit_boundary("non-image control", &body, LimitKind::InputTokens, whole)
            .await;
    }
}

fn selected_string_bytes(body: &serde_json::Value) -> usize {
    fn strings(value: &serde_json::Value) -> usize {
        match value {
            serde_json::Value::String(text) => text.len(),
            serde_json::Value::Array(values) => values.iter().map(strings).sum(),
            serde_json::Value::Object(values) => values.values().map(strings).sum(),
            _ => 0,
        }
    }
    [
        "tools",
        "system",
        "messages",
        "instructions",
        "input",
        "systemInstruction",
        "contents",
    ]
    .iter()
    .filter_map(|key| body.get(key))
    .map(strings)
    .sum()
}

#[tokio::test]
async fn admission_non_anthropic_image_shaped_json_keeps_generic_estimate() {
    let data = synthetic_png_base64(128, flate2::Compression::none());
    for (kind, model, path, root) in [
        (
            StorageUpstreamKind::OpenaiApiKey,
            PRICED_OPENAI_MODEL,
            "/v1/responses",
            "input",
        ),
        (
            StorageUpstreamKind::GeminiApiKey,
            PRICED_GEMINI_MODEL,
            "/v1beta/models/gemini-limit-test:generateContent",
            "contents",
        ),
    ] {
        let mut body = serde_json::json!({"model":model, "max_tokens":1});
        body[root] = serde_json::json!([native_base64_image(&data)]);
        let whole = i64::try_from(selected_string_bytes(&body) / 4)
            .expect("small estimate")
            .max(4_000);
        assert!(whole > 4_784);
        for (cap, expected_status) in [
            (whole, StatusCode::OK),
            (whole - 1, StatusCode::TOO_MANY_REQUESTS),
        ] {
            let state = TestState::default();
            let lifecycle = limited_lifecycle(
                kind,
                vec![Limit {
                    kind: LimitKind::InputTokens,
                    window_secs: 60,
                    cap_micros: cap,
                }],
                &state,
                None,
            );
            let request = http::Request::builder()
                .method("POST")
                .uri(path)
                .header("x-api-key", managed_api_key())
                .body(Bytes::from(
                    serde_json::to_vec(&body).expect("request JSON"),
                ))
                .expect("provider request builds");
            let (status, _, response_body) = collect_body(handle(&lifecycle, request).await).await;
            assert_eq!(
                status, expected_status,
                "{model}, cap {cap}: {response_body:?}"
            );
            assert_eq!(
                state.upstream_calls.load(Ordering::Relaxed),
                u64::from(status == StatusCode::OK)
            );
        }
    }
}

#[tokio::test]
async fn admission_local_prefix_and_tail_do_not_overlap_at_exact_cost_boundary() {
    let body = serde_json::json!({
        "model":PRICED_RAW_CLAUDE_ALIAS, "max_tokens":1,
        "system":[{"type":"text","text":"p".repeat(16_000),"cache_control":{"type":"ephemeral","ttl":"5m"}}],
        "messages":[{"role":"user","content":[{"type":"text","text":"t".repeat(40_000)}]}]
    });
    // Prefix: text(4)+16,000+ephemeral(9)+5m(2) = 16,015 bytes
    // => 4,003 units, remainder 3. Tail: user(4)+text(4)+40,000
    // plus that remainder => 10,002 units. Gross 14,005, not serialized bytes.
    assert_anthropic_limit_boundary("disjoint local 5m", &body, LimitKind::CostUsd, 14_015).await;
    assert_anthropic_limit_boundary(
        "whole input includes cached prefix",
        &body,
        LimitKind::InputTokens,
        14_005,
    )
    .await;
    assert_anthropic_limit_boundary(
        "whole total includes cached prefix",
        &body,
        LimitKind::TotalTokens,
        14_006,
    )
    .await;
}

#[tokio::test]
async fn admission_local_one_hour_and_mixed_ttl_regions_are_disjoint() {
    let one_hour = serde_json::json!({
        "model":PRICED_RAW_CLAUDE_ALIAS, "max_tokens":1,
        "system":[{"type":"text","text":"p".repeat(16_000),"cache_control":{"type":"ephemeral","ttl":"1h"}}],
        "messages":[{"role":"user","content":[{"type":"text","text":"t".repeat(40_000)}]}]
    });
    // P1=4,003 at derived $2/M, tail=10,002 at $1/M, output=10.
    assert_anthropic_limit_boundary("derived 1h", &one_hour, LimitKind::CostUsd, 18_018).await;
    let mixed = serde_json::json!({
        "model":PRICED_RAW_CLAUDE_ALIAS, "max_tokens":1,
        "system":[
            {"type":"text","text":"a".repeat(16_000),"cache_control":{"type":"ephemeral","ttl":"1h"}},
            {"type":"text","text":"b".repeat(8_000),"cache_control":{"type":"ephemeral","ttl":"5m"}}
        ],
        "messages":[{"role":"user","content":[{"type":"text","text":"t".repeat(40_000)}]}]
    });
    // Shared remainder: P1=4,003 (3), P5=2,004 (2), tail=10,002.
    // Prefix snapshots must not be added: 2*4,003 + 2,004 + 10,002 + 10.
    assert_anthropic_limit_boundary("mixed TTL", &mixed, LimitKind::CostUsd, 20_022).await;
}

#[tokio::test]
async fn admission_automatic_and_explicit_local_markers_share_request_floor_once() {
    let explicit = serde_json::json!({
        "model":PRICED_RAW_CLAUDE_ALIAS, "max_tokens":1,
        "messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}]
    });
    let automatic = serde_json::json!({
        "model":PRICED_RAW_CLAUDE_ALIAS, "max_tokens":1,
        "cache_control":{"type":"ephemeral"},
        "messages":[{"role":"user","content":[{"type":"text","text":"hello"}]}]
    });
    for body in [explicit, automatic] {
        assert_anthropic_limit_boundary("single request floor", &body, LimitKind::CostUsd, 4_010)
            .await;
    }
}

#[tokio::test]
async fn admission_local_plus_opaque_marker_preserves_local_precedence() {
    let body = serde_json::json!({
        "model":PRICED_RAW_CLAUDE_ALIAS, "max_tokens":1,
        "prompt_cache_key":"opaque-context",
        "system":[{"type":"text","text":"p".repeat(16_000),"cache_control":{"type":"ephemeral","ttl":"5m"}}],
        "messages":[{"role":"user","content":[{"type":"text","text":"t".repeat(40_000)}]}]
    });
    assert_anthropic_limit_boundary(
        "resolved local suppresses opaque padding",
        &body,
        LimitKind::CostUsd,
        14_015,
    )
    .await;
}

fn serving_policy_admission_body() -> serde_json::Value {
    serde_json::json!({
        "model": PRICED_RAW_CLAUDE_ALIAS,
        "max_tokens": 1,
        "tools": [{
            "name": "inspect",
            "description": "p".repeat(16_000),
            "input_schema": {"type": "object"}
        }],
        "system": [{
            "type": "text",
            "text": "x-anthropic-billing-header: cron",
            "cache_control": {"type": "ephemeral", "ttl": "1h"}
        }],
        "messages": [{
            "role": "user",
            "content": "t".repeat(40_000)
        }]
    })
}

async fn assert_serving_policy_cost_boundary(
    label: &str,
    body: &serde_json::Value,
    kind: StorageUpstreamKind,
    base_url: Option<&str>,
    boundary_micros: i64,
) {
    let bytes = Bytes::from(serde_json::to_vec(body).expect("policy request serializes"));
    for (cap_micros, expected_status, expected_calls) in [
        (boundary_micros, StatusCode::OK, 1),
        (boundary_micros - 1, StatusCode::TOO_MANY_REQUESTS, 0),
    ] {
        let state = TestState::default();
        let lifecycle = limited_lifecycle_at(
            kind,
            vec![Limit {
                kind: LimitKind::CostUsd,
                window_secs: 60,
                cap_micros,
            }],
            &state,
            None,
            base_url.map(|value| value.parse().expect("policy URL parses")),
        );
        let response = handle(&lifecycle, messages_request(bytes.clone())).await;
        let (status, _, response_body) = collect_body(response).await;
        assert_eq!(
            status, expected_status,
            "{label}, {kind:?}, URL {base_url:?}, cap {cap_micros}: {response_body:?}"
        );
        assert_eq!(
            state.upstream_calls.load(Ordering::Relaxed),
            expected_calls,
            "{label}, {kind:?}, cap {cap_micros}"
        );
        if expected_status == StatusCode::TOO_MANY_REQUESTS {
            let error: serde_json::Value =
                serde_json::from_slice(&response_body).expect("limit rejection JSON");
            assert_eq!(error["error"]["type"], "rate_limit_error", "{label}");
            assert_eq!(error["error"]["limit_kind"], "cost_usd", "{label}");
        }
    }
}

#[tokio::test]
async fn admission_official_billing_only_breakpoint_preserves_opaque_cost_boundary() {
    let body = serving_policy_admission_body();
    // The official bundle has no effective breakpoint. The unchanged raw
    // marker therefore retains the existing opaque gross bound, not a local
    // cache partition: 1,048,576 input micros plus 10 output micros.
    for kind in [
        StorageUpstreamKind::AnthropicApiKey,
        StorageUpstreamKind::AnthropicOauth,
    ] {
        for base_url in [None, Some("https://api.anthropic.com:443/proxy")] {
            assert_serving_policy_cost_boundary(
                "official header-only fallback",
                &body,
                kind,
                base_url,
                1_048_586,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn admission_custom_billing_only_breakpoint_uses_literal_cost_boundary() {
    let body = serving_policy_admission_body();
    // Tools = 16,013 string bytes; billing block = 47. The literal
    // one-hour prefix is exactly 4,015 tokens. Tail = 10,001 tokens.
    // Real catalog pricing derives $2/M for 1h and charges $1/M tail,
    // $10/M output: 2 * 4,015 + 10,001 + 10 = 18,041 micros.
    // This is below the stale official bundle's 1,048,586-micro bound.
    for kind in [
        StorageUpstreamKind::AnthropicApiKey,
        StorageUpstreamKind::AnthropicOauth,
    ] {
        for base_url in [
            "https://proxy.example.com/anthropic",
            "https://user:pass@api.anthropic.com",
        ] {
            assert_serving_policy_cost_boundary(
                "custom header-only literal",
                &body,
                kind,
                Some(base_url),
                18_041,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn admission_non_billing_breakpoint_preserves_local_cost_across_policies() {
    let mut body = serving_policy_admission_body();
    // Same raw string length as the billing header, but not excluded by
    // either policy. Both serving bundles must keep the same local partition.
    body["system"][0]["text"] = serde_json::json!("v".repeat(32));
    for kind in [
        StorageUpstreamKind::AnthropicApiKey,
        StorageUpstreamKind::AnthropicOauth,
    ] {
        for base_url in [None, Some("https://proxy.example.com/anthropic")] {
            assert_serving_policy_cost_boundary(
                "ordinary system control",
                &body,
                kind,
                base_url,
                18_041,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn admission_custom_billing_one_hour_prefix_keeps_its_rate_before_five_minute_tail() {
    let mut body = serving_policy_admission_body();
    body["system"]
        .as_array_mut()
        .expect("system array")
        .push(serde_json::json!({
            "type": "text",
            "text": "b".repeat(8_000),
            "cache_control": {"type": "ephemeral", "ttl": "5m"}
        }));
    // Literal: 4,015 one-hour tokens, then 8,015 bytes => 2,003 five-minute
    // tokens; tail is 10,001. Cost = 20,044 micros.
    // Official excludes only the first breakpoint: the whole raw prefix
    // is 6,018 five-minute tokens, cost = 16,029. Reusing that bundle for
    // custom would admit the request at 20,043, under-reserving by 4,015.
    assert_serving_policy_cost_boundary(
        "custom mixed TTL",
        &body,
        StorageUpstreamKind::AnthropicApiKey,
        Some("https://proxy.example.com/anthropic"),
        20_044,
    )
    .await;
}

#[tokio::test]
async fn admission_official_mixed_ttl_preserves_effective_breakpoint_control() {
    let mut body = serving_policy_admission_body();
    body["system"]
        .as_array_mut()
        .expect("system array")
        .push(serde_json::json!({
            "type": "text",
            "text": "b".repeat(8_000),
            "cache_control": {"type": "ephemeral", "ttl": "5m"}
        }));
    assert_serving_policy_cost_boundary(
        "official mixed TTL control",
        &body,
        StorageUpstreamKind::AnthropicApiKey,
        None,
        16_029,
    )
    .await;
}

#[tokio::test]
async fn admission_five_literal_slots_do_not_inherit_four_official_slots() {
    let mut body = serving_policy_admission_body();
    for _ in 0..4 {
        body["system"]
            .as_array_mut()
            .expect("system array")
            .push(serde_json::json!({
                "type": "text",
                "text": "b".repeat(8_000),
                "cache_control": {"type": "ephemeral", "ttl": "5m"}
            }));
    }
    // Official has four valid markers: 48,120 prefix bytes / 4 = 12,030
    // five-minute tokens, plus 10,001 tail and 10 output => 22,041.
    // Literal has five markers and is invalid, so it must retain the
    // conservative opaque fallback, never borrow the official four slots.
    assert_serving_policy_cost_boundary(
        "four official slots",
        &body,
        StorageUpstreamKind::AnthropicApiKey,
        None,
        22_041,
    )
    .await;
    assert_serving_policy_cost_boundary(
        "five literal slots fallback",
        &body,
        StorageUpstreamKind::AnthropicApiKey,
        Some("https://proxy.example.com/anthropic"),
        1_048_586,
    )
    .await;
}

struct CapturingAdmissionDispatch {
    inner: MockDispatch,
    bodies: Arc<Mutex<Vec<Bytes>>>,
}

#[async_trait::async_trait]
impl UpstreamDispatch for CapturingAdmissionDispatch {
    async fn dispatch(
        &self,
        request: SignedRequest,
    ) -> Result<http::Response<axum::body::Body>, DispatchError> {
        self.bodies
            .lock()
            .expect("captured bodies lock")
            .push(request.body().clone());
        self.inner.dispatch(request).await
    }
}

struct AttributionRemovingDialect {
    base_url: url::Url,
}

impl UpstreamDialect for AttributionRemovingDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let body = remove_original_billing_block(&context.body_bytes)?;

        let mut url = self.base_url.clone();
        url.set_path(context.path.trim_start_matches('/'));
        url.set_query(context.query.as_deref());
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            body,
        ))
    }
}

fn remove_original_billing_block(body: &Bytes) -> Result<Bytes, DialectError> {
    let mut value = serde_json::from_slice::<serde_json::Value>(body).map_err(|error| {
        DialectError::UnsupportedRequest {
            reason: format!("test dialect received invalid JSON: {error}"),
        }
    })?;
    let Some(system) = value
        .get_mut("system")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return Ok(Bytes::from(serde_json::to_vec(&value).map_err(
            |error| DialectError::UnsupportedRequest {
                reason: format!("test dialect failed to serialize JSON: {error}"),
            },
        )?));
    };
    let remove_first = system.first().is_some_and(|block| {
        block.get("type").and_then(serde_json::Value::as_str) == Some("text")
            && block
                .get("text")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|text| text.starts_with("x-anthropic-billing-header:"))
    });
    if remove_first {
        system.remove(0);
    }
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|error| DialectError::UnsupportedRequest {
            reason: format!("test dialect failed to serialize JSON: {error}"),
        })
}

fn explicit_native_lifecycle(
    state: &TestState,
    kind: StorageUpstreamKind,
    record_base_url: Option<url::Url>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    event_bus: Arc<dyn cc_lb_control::RequestEventBus>,
    id_suffix: u128,
    name: &str,
) -> Lifecycle {
    let dialect_base_url = record_base_url.clone().unwrap_or_else(|| {
        "https://api.anthropic.com/"
            .parse()
            .expect("official URL parses")
    });
    let upstream_id = uuid::Uuid::from_u128(id_suffix);
    let principal = PrincipalRecord {
        id: uuid::Uuid::from_u128(id_suffix + 10_000),
        name: "principal-test".to_owned(),
        kind: cc_lb_storage_api::PrincipalKind::Machine,
        allowed_models: vec!["*".to_owned()],
        allowed_upstreams: vec![upstream_id],
        default_limits: Vec::new(),
        enabled: true,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
        cache_keepalive: None,
    };
    let record = UpstreamRecord {
        id: upstream_id,
        name: name.to_owned(),
        kind,
        base_url: record_base_url,
        enabled: true,
        api_key_ciphertext: matches!(kind, StorageUpstreamKind::AnthropicApiKey).then(Vec::new),
        ..UpstreamRecord::default()
    };
    let dialect = Arc::new(AttributionRemovingDialect {
        base_url: dialect_base_url,
    });
    let mut chains: HashMap<String, PrincipalRoutingArtifacts> = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            Some(Arc::new(RouterPipelineCache::empty(
                TerminalStrategy::FirstPick,
            ))),
            DialectCache::Explicit(ShapePluginCache { dialect }),
        ),
    );
    let authn = TestAuthn::with_principal_view(
        state.clone(),
        Arc::new(PrincipalView::from_db(&[principal], chains)),
    );
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![record])
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(event_bus)
}

fn shaped_prompt_request(attribution: &str) -> http::Request<Bytes> {
    let body = serde_json::to_vec(&serde_json::json!({
        "model": "claude-sonnet-4-5",
        "tools": [{
            "name": "lookup",
            "description": "stable tool",
            "input_schema": {"type": "object"}
        }],
        "system": [
            {"type": "text", "text": format!("x-anthropic-billing-header: {attribution}")},
            {
                "type": "text",
                "text": "stable system tail",
                "cache_control": {"type": "ephemeral"}
            }
        ],
        "messages": [{"role": "user", "content": "same user prompt"}]
    }))
    .expect("prompt body serializes");
    messages_request(Bytes::from(body))
}

async fn next_route(
    lifecycle_events: &mut tokio::sync::mpsc::Receiver<LifecycleEvent>,
) -> RouteInfo {
    timeout(Duration::from_secs(3), async {
        loop {
            let event = lifecycle_events
                .recv()
                .await
                .expect("lifecycle event stream remains open");
            if let LifecycleEvent::RouteCompleted {
                result: Ok(route), ..
            } = event
            {
                return route;
            }
        }
    })
    .await
    .expect("route event arrives")
}

async fn wait_for_final_update(
    updates: &mut tokio::sync::broadcast::Receiver<cc_lb_request_log::RequestEventUpdate>,
) {
    timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                updates
                    .recv()
                    .await
                    .expect("request update stream remains open"),
                cc_lb_request_log::RequestEventUpdate::Final(_)
            ) {
                return;
            }
        }
    })
    .await
    .expect("final request event arrives");
}

async fn sqlite_storage_for_lifecycle_happy(
    dir: &tempfile::TempDir,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!(
        "sqlite://{}",
        dir.path().join("lifecycle-happy.sqlite").display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await?;
    storage.initialize().await?;
    Ok(storage)
}

#[tokio::test]
async fn admission_image_fix_preserves_forwarded_bytes_and_routing_prefix_units() {
    let data = synthetic_png_base64(1024, flate2::Compression::none());
    let body = serde_json::json!({
        "model":PRICED_RAW_CLAUDE_ALIAS, "max_tokens":1,
        "messages":[{"role":"user","content":[{
            "type":"image", "source":{"type":"base64","media_type":"image/png","data":data},
            "cache_control":{"type":"ephemeral","ttl":"5m"}
        }]}]
    });
    let bytes = Bytes::from(serde_json::to_vec_pretty(&body).expect("pretty request JSON"));
    let before = cc_lb_engine::parse_request_cache_breakpoints(&http::HeaderMap::new(), &bytes);
    let prefix = before.first().expect("native image breakpoint");
    assert!(
        prefix.prefix_bytes.get() > 1_000_000,
        "routing retains serialized payload bytes"
    );
    assert_eq!(
        cc_lb_domain::PrefixBytes::ESTIMATE_SOURCE,
        "serialized_prefix_bytes_v1"
    );
    let state = TestState::default();
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let dispatch = Arc::new(CapturingAdmissionDispatch {
        inner: MockDispatch {
            state: state.clone(),
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
        bodies: Arc::clone(&bodies),
    });
    // Input = 4,784 + (user4+image5+base646+image/png9+ephemeral9+5m2)/4
    // = 4,792; output adds 10 micros. Prefix bytes must not set this cap.
    let lifecycle = limited_lifecycle(
        StorageUpstreamKind::AnthropicApiKey,
        vec![Limit {
            kind: LimitKind::CostUsd,
            window_secs: 60,
            cap_micros: 4_802,
        }],
        &state,
        Some(dispatch),
    );
    let response = handle(&lifecycle, messages_request(bytes.clone())).await;
    let (status, _, response_body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK, "{response_body:?}");
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
    let captured = bodies.lock().expect("captured body lock");
    let forwarded = captured.first().expect("one dispatched body");
    assert_eq!(forwarded, &bytes, "admission must not rewrite the payload");
    let after = cc_lb_engine::parse_request_cache_breakpoints(&http::HeaderMap::new(), forwarded);
    assert_eq!(
        serde_json::to_value(&after).expect("breakpoints serialize"),
        serde_json::to_value(&before).expect("breakpoints serialize")
    );
}

#[tokio::test]
async fn admission_tools_and_message_breakpoints_use_structural_regions() {
    let tool = serde_json::json!({
        "model":PRICED_RAW_CLAUDE_ALIAS, "max_tokens":1,
        "tools":[{
            "name":"inspect", "description":"p".repeat(16_000),
            "input_schema":{"type":"object"},
            "cache_control":{"type":"ephemeral","ttl":"5m"}
        }],
        "messages":[{"role":"user","content":"t".repeat(40_000)}]
    });
    // Tool strings: 7+16,000+6+9+2 = 16,024 => P5=4,006.
    // Tail: user(4)+40,000 => 10,001; output adds 10.
    assert_anthropic_limit_boundary("tool breakpoint", &tool, LimitKind::CostUsd, 14_017).await;
    let message = serde_json::json!({
        "model":PRICED_RAW_CLAUDE_ALIAS, "max_tokens":1,
        "messages":[
            {"role":"user","content":[{"type":"text","text":"p".repeat(16_000),"cache_control":{"type":"ephemeral","ttl":"5m"}}]},
            {"role":"user","content":[{"type":"text","text":"t".repeat(40_000)}]}
        ]
    });
    // P5=4,003 with carry 3. Both role strings stay in the uncached
    // accumulator: tail=(8+4+40,000+3)/4=10,003.
    assert_anthropic_limit_boundary("message breakpoint", &message, LimitKind::CostUsd, 14_016)
        .await;
}

#[tokio::test]
async fn admission_nested_tool_result_prefix_counts_its_image_once() {
    let data = synthetic_png_base64(1024, flate2::Compression::none());
    let body = serde_json::json!({
        "model":PRICED_RAW_CLAUDE_ALIAS, "max_tokens":1,
        "messages":[{"role":"user","content":[
            {
                "type":"tool_result","tool_use_id":"toolu_1",
                "content":[native_base64_image(&data)],
                "cache_control":{"type":"ephemeral","ttl":"5m"}
            },
            {"type":"text","text":"t".repeat(40_000)}
        ]}]
    });
    // Parent prefix: tool_result11+toolu_1(7)+image metadata20+ephemeral9+5m2
    // =49 bytes => 12 with carry 1, plus one 4,784 image. Tail=10,002.
    assert_anthropic_limit_boundary("nested cached image", &body, LimitKind::CostUsd, 14_808).await;
    assert_anthropic_limit_boundary(
        "nested cached image whole input",
        &body,
        LimitKind::InputTokens,
        14_798,
    )
    .await;
}

#[tokio::test]
async fn admission_unresolved_empty_marker_retains_opaque_fallback() {
    let body = serde_json::json!({
        "model":PRICED_RAW_CLAUDE_ALIAS, "max_tokens":1,
        "cache_control":{"type":"ephemeral"},
        "messages":[]
    });
    let bytes = Bytes::from(serde_json::to_vec(&body).expect("request JSON"));
    assert!(
        cc_lb_engine::parse_request_cache_breakpoints(&http::HeaderMap::new(), &bytes).is_empty()
    );
    // Empty automatic input cannot resolve a local block. Preserve the
    // existing 1,048,576-token opaque gross bound, plus 10 output micros.
    assert_anthropic_limit_boundary("unresolved marker", &body, LimitKind::CostUsd, 1_048_586)
        .await;
}

#[tokio::test]
async fn admission_file_image_preserves_existing_pre_admission_affinity_gate() {
    for reference in ["file_1".to_owned(), format!("file_{}", "x".repeat(40_000))] {
        let body = anthropic_content_request(
            serde_json::json!([{"type":"image","source":{"type":"file","file_id":reference}}]),
            1,
        );
        // File affinity is resolved before limits. Neither cap may bypass that
        // existing 503 gate; local file-image arithmetic belongs in T1 tests.
        for (cap, expected_status) in [
            (4_786, StatusCode::SERVICE_UNAVAILABLE),
            (4_787, StatusCode::SERVICE_UNAVAILABLE),
        ] {
            let state = TestState::default();
            let lifecycle = limited_lifecycle(
                StorageUpstreamKind::AnthropicApiKey,
                vec![Limit {
                    kind: LimitKind::InputTokens,
                    window_secs: 60,
                    cap_micros: cap,
                }],
                &state,
                None,
            );
            let response = handle(
                &lifecycle,
                messages_request(Bytes::from(
                    serde_json::to_vec(&body).expect("request JSON"),
                )),
            )
            .await;
            let (status, _, response_body) = collect_body(response).await;
            assert_eq!(
                status, expected_status,
                "file image cap {cap}: {response_body:?}"
            );
            assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
        }
    }
}

struct GatedAdmissionDispatch {
    inner: MockDispatch,
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

#[async_trait::async_trait]
impl UpstreamDispatch for GatedAdmissionDispatch {
    async fn dispatch(
        &self,
        request: SignedRequest,
    ) -> Result<http::Response<axum::body::Body>, DispatchError> {
        self.entered.add_permits(1);
        self.release
            .acquire()
            .await
            .expect("dispatch gate open")
            .forget();
        self.inner.dispatch(request).await
    }
}

#[tokio::test]
async fn admission_two_in_flight_images_use_the_sum_of_corrected_reservations() {
    let data = synthetic_png_base64(1, flate2::Compression::best());
    let body = anthropic_content_request(serde_json::json!([native_base64_image(&data)]), 1);
    let bytes = Bytes::from(serde_json::to_vec(&body).expect("request JSON"));
    // Each request reserves 4,790 input tokens. Hold the first dispatch open
    // so provider settlement cannot conceal an under-reserved second request.
    for (cap, expected_second_status, expected_calls) in [
        (9_580, StatusCode::OK, 2),
        (9_579, StatusCode::TOO_MANY_REQUESTS, 1),
    ] {
        let state = TestState::default();
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let dispatch = Arc::new(GatedAdmissionDispatch {
            inner: MockDispatch {
                state: state.clone(),
                mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
            },
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let lifecycle = limited_lifecycle(
            StorageUpstreamKind::AnthropicApiKey,
            vec![Limit {
                kind: LimitKind::InputTokens,
                window_secs: 60,
                cap_micros: cap,
            }],
            &state,
            Some(dispatch),
        );
        let first = handle(&lifecycle, messages_request(bytes.clone()));
        tokio::pin!(first);
        tokio::select! {
            permit = entered.acquire() => permit.expect("first dispatch entered").forget(),
            response = &mut first => panic!("first request returned before dispatch gate: {}", response.status()),
        }
        let second = handle(&lifecycle, messages_request(bytes.clone()));
        tokio::pin!(second);
        let second_response = tokio::select! {
            permit = entered.acquire() => {
                permit.expect("second dispatch entered").forget();
                release.add_permits(2);
                second.await
            },
            response = &mut second => {
                release.add_permits(1);
                response
            },
        };
        let (first_status, _, first_body) = collect_body(first.await).await;
        let (second_status, _, second_body) = collect_body(second_response).await;
        assert_eq!(first_status, StatusCode::OK, "cap {cap}: {first_body:?}");
        assert_eq!(
            second_status, expected_second_status,
            "cap {cap}: {second_body:?}"
        );
        assert_eq!(
            state.upstream_calls.load(Ordering::Relaxed),
            expected_calls,
            "cap {cap}"
        );
    }
}

#[tokio::test]
async fn admission_over_limit_and_unsupported_markers_keep_opaque_fallback() {
    let too_many: Vec<_> = (0..5)
        .map(|_| {
            serde_json::json!({
                "type":"text","text":"hello","cache_control":{"type":"ephemeral"}
            })
        })
        .collect();
    let bodies = [
        anthropic_content_request(serde_json::json!(too_many), 1),
        anthropic_content_request(
            serde_json::json!([{
                "type":"future_block", "payload":"hello",
                "cache_control":{"type":"ephemeral"}
            }]),
            1,
        ),
    ];
    for body in bodies {
        let bytes = Bytes::from(serde_json::to_vec(&body).expect("request JSON"));
        assert!(
            cc_lb_engine::parse_request_cache_breakpoints(&http::HeaderMap::new(), &bytes)
                .is_empty()
        );
        assert_anthropic_limit_boundary(
            "unresolved invalid marker",
            &body,
            LimitKind::CostUsd,
            1_048_586,
        )
        .await;
    }
}

#[tokio::test]
async fn admission_missing_and_unsupported_image_sources_keep_generic_estimate() {
    let data = synthetic_png_base64(128, flate2::Compression::none());
    for image in [
        serde_json::json!({"type":"image"}),
        serde_json::json!({"type":"image","source":null}),
        serde_json::json!({"type":"image","source":{"type":"unsupported","data":data}}),
    ] {
        let body = anthropic_content_request(serde_json::json!([image]), 1);
        let whole = i64::try_from(selected_string_bytes(&body) / 4)
            .expect("small generic estimate")
            .max(4_000);
        assert_anthropic_limit_boundary(
            "unrecognized source",
            &body,
            LimitKind::InputTokens,
            whole,
        )
        .await;
    }
}

#[derive(Clone, Default)]
struct CapturingSignedDispatch {
    bodies: Arc<Mutex<Vec<Bytes>>>,
}

#[async_trait]
impl UpstreamDispatch for CapturingSignedDispatch {
    async fn dispatch(
        &self,
        request: SignedRequest,
    ) -> Result<http::Response<Body>, DispatchError> {
        self.bodies
            .lock()
            .expect("captured bodies lock")
            .push(request.body().clone());
        Ok(http::Response::new(Body::from(Bytes::from_static(
            br#"{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}"#,
        ))))
    }
}
