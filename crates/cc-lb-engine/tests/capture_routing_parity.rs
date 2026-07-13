#![cfg(feature = "capture")]

mod common;
#[allow(dead_code)]
mod prompt_cache_routing_support;

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_capture::hook::CaptureHandle;
use cc_lb_domain::{RoutingTrace, TtlClass, WarmCacheEntry};
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_engine::builtin_filters::cache_affinity::CacheAffinityFilter;
use cc_lb_engine::{
    DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig, TestClock,
    parse_request_cache_breakpoints,
};
use cc_lb_lifecycle::LifecycleEvent;
use cc_lb_routing::FilterPlugin;
use http::{HeaderMap, HeaderValue, StatusCode};
use url::Url;
use uuid::Uuid;

use common::{
    DispatchMode, MockDispatch, TestAuthn, TestLifecycleBus, TestRouter, TestState, collect_body,
    messages_request,
};
use prompt_cache_routing_support::TestPromptCacheObservationCache;

const COLD_UPSTREAM_ID: Uuid = Uuid::from_u128(1);
const WARM_UPSTREAM_ID: Uuid = Uuid::from_u128(2);
const NOW_UNIX_SECS: u64 = 1_700_000_000;

#[tokio::test]
async fn enabling_capture_does_not_change_routing_decision() {
    // Given
    let capture_off = route_with_capture(false).await;

    // When
    let capture_on = route_with_capture(true).await;

    // Then
    assert_eq!(
        capture_on.0, capture_off.0,
        "capture state must not change the selected upstream"
    );
    assert_eq!(
        normalize_trace(capture_on.1),
        normalize_trace(capture_off.1),
        "capture state must not change routing decision fields"
    );
}

async fn route_with_capture(capture_enabled: bool) -> (Uuid, RoutingTrace) {
    let body = request_body();
    let breakpoints = parse_request_cache_breakpoints(&HeaderMap::new(), &body);
    let warm_prefix = breakpoints[0].lookback_prefixes[0].prefix_hash.clone();
    let prompt_cache = TestPromptCacheObservationCache::new(HashMap::from([(
        WARM_UPSTREAM_ID,
        vec![WarmCacheEntry {
            prefix_hash: warm_prefix,
            expires_at_unix_secs: NOW_UNIX_SECS + 300,
            ttl_class: TtlClass::Ephemeral5m,
            last_observed_at_unix_secs: NOW_UNIX_SECS,
            content_block_index: 0,
            estimated_prefix_tokens: breakpoints[0].prefix_token_count,
            token_estimate_source: "local_tiktoken_v1".to_owned(),
            hash_schema_version: 4,
        }],
    )]));
    let state = TestState::default();
    let principal_view = principal_view();
    let authn = TestAuthn::with_principal_view(state.clone(), Arc::clone(&principal_view));
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(TestRouter {
            base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
        }))
        .global_observability_hooks(Vec::new())
        .principal_view(principal_view)
        .upstream_records(vec![
            upstream_record(COLD_UPSTREAM_ID),
            upstream_record(WARM_UPSTREAM_ID),
        ])
        .prompt_cache_observation_cache(Arc::new(prompt_cache))
        .build();
    let event_bus = TestLifecycleBus::new();
    let mut events = event_bus.bus.attach_lifecycle_writer(128);
    let tempdir = tempfile::tempdir().expect("creates tempdir");
    let store = cc_lb_capture::store::open_capture_store(&tempdir.path().join("capture.sqlite"))
        .await
        .expect("opens capture store");
    let (sink, writer) = cc_lb_capture::sink::CaptureSink::new(store, 128);
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(MockDispatch {
            state,
            mode: DispatchMode::Statuses(Default::default()),
        }),
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(NOW_UNIX_SECS)),
    )
    .with_event_bus(event_bus.bus_arc());
    let lifecycle = if capture_enabled {
        lifecycle.with_capture_handle(CaptureHandle::from_sink(sink))
    } else {
        lifecycle
    };

    let mut request = messages_request(body);
    request
        .headers_mut()
        .insert("request-id", HeaderValue::from_static("capture-parity"));
    let response = lifecycle
        .handle(request)
        .await
        .expect("lifecycle handles request");
    let (status, _, _) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    writer.shutdown().await;

    while let Ok(event) = events.try_recv() {
        if let LifecycleEvent::RouteCompleted {
            result: Ok(route),
            routing_trace: Some(trace),
            ..
        } = event
        {
            return (route.upstream_id, trace);
        }
    }
    panic!("route-completed event must be published");
}

fn normalize_trace(mut trace: RoutingTrace) -> RoutingTrace {
    for stage in &mut trace.stages {
        stage.duration_us = 0;
    }
    trace
}

fn principal_view() -> Arc<PrincipalView> {
    let filters: Vec<Arc<dyn FilterPlugin>> = vec![Arc::new(CacheAffinityFilter::new())];
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: filters,
        terminal: cc_lb_domain::TerminalStrategy::FirstPick,
        instantiation_error: None,
    });
    Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        HashMap::from([(
            "principal-test".to_owned(),
            (
                Some(pipeline),
                ObservabilityHooksCache::Inherit,
                DialectCache::Inherit,
            ),
        )]),
    ))
}

fn upstream_record(id: Uuid) -> cc_lb_storage_api::upstream::UpstreamRecord {
    cc_lb_storage_api::upstream::UpstreamRecord {
        id,
        name: format!("upstream-{id}"),
        kind: cc_lb_storage_api::upstream::UpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        api_key_ciphertext: Some(Vec::new()),
        revision: 1,
        ..cc_lb_storage_api::upstream::UpstreamRecord::default()
    }
}

fn request_body() -> Bytes {
    Bytes::from_static(
        br#"{
            "model":"claude-sonnet-4-5",
            "system":[{"type":"text","text":"stable","cache_control":{"type":"ephemeral"}}],
            "messages":[{"role":"user","content":"hello"}],
            "max_tokens":16
        }"#,
    )
}
