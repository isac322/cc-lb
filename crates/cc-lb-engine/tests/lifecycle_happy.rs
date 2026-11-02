use crate::common;

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use bytes::Bytes;
use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_control::api_keys::principal_view::{
    DialectCache, PrincipalRoutingArtifacts, PrincipalView, RouterPipelineCache, ShapePluginCache,
};
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_domain::TerminalStrategy;
use cc_lb_engine::{
    DispatchError, Lifecycle, LifecycleConfig, LimitCostEstimator, UpstreamDispatch,
};
use cc_lb_lifecycle::{LifecycleEvent, LimitDecisionKind};
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, PriceCatalog, Pricing, UsdPerMillion};
use cc_lb_storage_api::principal::{Limit, LimitKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{KeyStatus, StoredApiKeyRecord};
use cc_lb_upstream::SignedRequest;
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
// Every priced test model costs $1/M input and $10/M output, so with the
// lifecycle's fixed 4,000-token input estimate a request reserves
// `4_000 + 10 * max_output` micros. A 2,000-token output cap therefore
// reserves 24,000 micros; no output cap reserves 4,000 micros.
const PRICED_GEMINI_MODEL: &str = "gemini-limit-test";
const UNPRICED_GEMINI_MODEL: &str = "gemini-unpriced-test";
const PRICED_OPENAI_MODEL: &str = "gpt-limit-test";
/// Rolling alias whose canonical dated form is deliberately left unpriced: a
/// limit lookup that canonicalizes the raw body model misses the price.
const PRICED_RAW_CLAUDE_ALIAS: &str = "claude-sonnet-4-5";
const COST_WITH_2000_OUTPUT_MICROS: i64 = 24_000;
const COST_WITHOUT_OUTPUT_MICROS: i64 = 4_000;

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
    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("error body is JSON");
    assert_eq!(body["error"]["type"], "server_error");
    assert_eq!(
        body["error"]["message"],
        format!("cost limits unavailable for model {UNPRICED_GEMINI_MODEL}")
    );
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn gemini_top_level_output_cap_takes_precedence_over_nested_cap() {
    // Given a USD cap that only admits the 1-token top-level output cap.
    let state = TestState::default();
    let lifecycle = usd_limited_lifecycle(
        StorageUpstreamKind::GeminiApiKey,
        COST_WITH_2000_OUTPUT_MICROS - 1,
        &state,
    );

    // When both a top-level and a nested output cap are present.
    let response = handle(
        &lifecycle,
        gemini_request(
            PRICED_GEMINI_MODEL,
            br#"{"contents":[],"max_tokens":1,"generationConfig":{"maxOutputTokens":2000}}"#,
        ),
    )
    .await;

    // Then the top-level cap is reserved and the request is dispatched.
    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
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
}

fn priced_catalog() -> Arc<PriceCatalog> {
    let models = [
        PRICED_GEMINI_MODEL,
        PRICED_OPENAI_MODEL,
        PRICED_RAW_CLAUDE_ALIAS,
    ]
    .into_iter()
    .map(|model| {
        (
            model.to_owned(),
            Pricing {
                model: model.to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(1),
                output_per_million_usd: UsdPerMillion::from_whole_usd(10),
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
    let base_url: url::Url = "http://upstream.local/".parse().expect("test URL parses");
    let record = UpstreamRecord {
        id: uuid::Uuid::from_u128(2),
        name: "usd-limit-upstream".to_owned(),
        kind,
        base_url: Some(base_url.clone()),
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
