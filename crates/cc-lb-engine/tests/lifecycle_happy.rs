use crate::common;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_control::api_keys::principal_view::{
    DialectCache, PrincipalRoutingArtifacts, PrincipalView, RouterPipelineCache, ShapePluginCache,
};
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_domain::TerminalStrategy;
use cc_lb_engine::{Lifecycle, LifecycleConfig, LimitCostEstimator};
use cc_lb_lifecycle::{LifecycleEvent, LimitDecisionKind};
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, PriceCatalog, Pricing, UsdPerMillion};
use cc_lb_storage_api::principal::{Limit, LimitKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{KeyStatus, StoredApiKeyRecord};
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
        default_limits: vec![Limit {
            kind: LimitKind::CostUsd,
            window_secs: 60,
            cap_micros,
        }],
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
        Arc::new(MockDispatch {
            state: state.clone(),
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
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
    // rate is $10/M. A cache-bearing request with a 4,000-token body estimate and a cache
    // directive reserves the worst-case cliff bound (200,001 total tokens = 4,000 input +
    // 196,001 cache tokens at $10/M => 2,000,010 micros), so even a 500,000-micro cap (well
    // above the 40,000-micro body-only cliff cost) is rejected while a 3,000,000-micro cap
    // is admitted.
    for (cap_micros, expected_status, expected_calls) in [
        (500_000, StatusCode::TOO_MANY_REQUESTS, 0),
        (3_000_000, StatusCode::OK, 1),
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
