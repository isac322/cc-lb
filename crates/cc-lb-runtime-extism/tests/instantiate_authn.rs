mod common;

use bytes::Bytes;
use cc_lb_plugin_api::{
    PluginRuntime, RateLimitKind, RateLimitObservation, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, Upstream, UpstreamCandidate, UpstreamKind, shape_request,
};
use cc_lb_runtime_extism::ExtismRuntime;
use uuid::Uuid;

#[test]
fn instantiate_router_and_dialect_wrappers() {
    let wat = common::module_with_functions(&[
        ("route", &common::route_response()),
        ("shape", &common::shape_response()),
    ]);
    let fixture = common::fixture("router", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let router = runtime
        .instantiate_router(&fixture.manifest)
        .expect("router instantiates");
    let route = router
        .route(&common::ctx(), &common::principal(), &[])
        .expect("route succeeds");
    assert_eq!(route.upstream_id, None);
    match route.upstream {
        Upstream::CustomAnthropicSpec { ref base_url } => {
            assert_eq!(base_url.as_str(), "http://upstream.test/");
        }
        _ => panic!("unexpected upstream"),
    }
    let shaped = shape_request(
        route.dialect.as_ref(),
        &common::ctx(),
        &route.upstream,
        &common::principal(),
    )
    .expect("route dialect shapes request");
    assert_eq!(shaped.url().as_str(), "http://upstream.test/v1/messages");
    assert_eq!(shaped.body(), &Bytes::from_static(br#"{"shaped":true}"#));
}

#[test]
fn route_returns_upstream_id_when_plugin_provides_one() {
    let upstream_id = Uuid::from_u128(0x11111111111111111111111111111111);
    let wat = common::module_with_functions(&[(
        "route",
        &common::route_response_with_upstream_id(&upstream_id.to_string()),
    )]);
    let fixture = common::fixture("router-upstream-id", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let router = runtime
        .instantiate_router(&fixture.manifest)
        .expect("router instantiates");

    let route = router
        .route(&common::ctx(), &common::principal(), &[])
        .expect("route succeeds");

    assert_eq!(route.upstream_id, Some(upstream_id));
}

#[test]
fn route_legacy_plugin_omits_upstream_id() {
    let wat = common::module_with_functions(&[("route", &common::route_response())]);
    let fixture = common::fixture("router-legacy", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let router = runtime
        .instantiate_router(&fixture.manifest)
        .expect("router instantiates");

    let route = router
        .route(&common::ctx(), &common::principal(), &[])
        .expect("route succeeds");

    assert_eq!(route.upstream_id, None);
}

#[test]
fn route_input_includes_candidates_as_sibling_field() {
    let success = common::route_response_with_base_url("http://candidate-input-ok.test/");
    let failure = common::route_response_with_base_url("http://candidate-input-missing.test/");
    let upstream_id = Uuid::from_u128(0x22222222222222222222222222222222);
    let markers: Vec<Vec<u8>> = vec![
        br#""request_id":"req-test""#.to_vec(),
        br#""method":"POST""#.to_vec(),
        br#""path":"/v1/messages""#.to_vec(),
        br#""principal":{"#.to_vec(),
        br#""candidates":[{"#.to_vec(),
        format!(r#""upstream_id":"{upstream_id}""#).into_bytes(),
        br#""name":"oauth-primary""#.to_vec(),
        br#""kind":"anthropic_oauth""#.to_vec(),
        br#""observed_rate_limits":[{"#.to_vec(),
        br#""kind":"input_tokens""#.to_vec(),
        br#""window":"minute""#.to_vec(),
        br#""limit":1000"#.to_vec(),
        br#""remaining":777"#.to_vec(),
        br#""reset":"2026-05-29T00:00:00Z""#.to_vec(),
        br#""subscription_quotas":[{"#.to_vec(),
        br#""window":"5h""#.to_vec(),
        br#""source":"merged""#.to_vec(),
        br#""data_state":"fresh""#.to_vec(),
        br#""utilization":0.42"#.to_vec(),
        br#""status":"allowed_warning""#.to_vec(),
        br#""resets_at_unix_secs":1800003600"#.to_vec(),
        br#""surpassed_threshold":true"#.to_vec(),
        br#""representative_claim":"org:claim""#.to_vec(),
        br#""disabled_reason":null"#.to_vec(),
        br#""observed_at_unix_millis":1800000000123"#.to_vec(),
        br#""age_secs":0"#.to_vec(),
        br#""observed_at_unix_secs":1800000000"#.to_vec(),
    ];
    let marker_refs: Vec<&[u8]> = markers.iter().map(Vec::as_slice).collect();
    let wat = common::route_module_requiring_input_markers(&marker_refs, &success, &failure);
    let fixture = common::fixture("router-candidates", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let router = runtime
        .instantiate_router(&fixture.manifest)
        .expect("router instantiates");
    let candidates = vec![UpstreamCandidate {
        upstream_id,
        name: "oauth-primary".to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        observed_rate_limits: vec![RateLimitObservation {
            kind: RateLimitKind::InputTokens,
            window: "minute".to_owned(),
            limit: Some(1000),
            remaining: Some(777),
            reset: Some("2026-05-29T00:00:00Z".to_owned()),
        }],
        subscription_quotas: vec![SubscriptionQuotaCandidateSnapshot {
            window: "5h".to_owned(),
            state: SubscriptionQuotaDataState::Fresh,
            source: Some("merged".to_owned()),
            utilization: Some(0.42),
            status: Some("allowed_warning".to_owned()),
            resets_at_unix_secs: Some(1_800_003_600),
            surpassed_threshold: Some(true),
            representative_claim: Some("org:claim".to_owned()),
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            observed_at_unix_millis: Some(1_800_000_000_123),
            max_staleness_secs: 300,
        }],
        observed_at_unix_secs: 1_800_000_000,
    }];

    let route = router
        .route(&common::ctx(), &common::principal(), &candidates)
        .expect("route succeeds");

    match route.upstream {
        Upstream::CustomAnthropicSpec { base_url } => {
            assert_eq!(base_url.as_str(), "http://candidate-input-ok.test/");
        }
        _ => panic!("unexpected upstream"),
    }
}

#[test]
fn instantiate_observability_batches_by_count() {
    let wat = common::module_with_functions(&[("observe", &common::observe_response())]);
    let fixture = common::fixture(
        "observe",
        &wat,
        common::metadata(&[("observe_batch_count", 2), ("observe_flush_ms", 10_000)]),
    );
    let runtime = ExtismRuntime::new();
    let hook = runtime
        .instantiate_observability(&fixture.manifest)
        .expect("observability hook instantiates");
    hook.observe(cc_lb_plugin_api::ObserveEvent::Chunk {
        batch_index: 0,
        event_count: 1,
        total_bytes: 10,
    })
    .expect("first event is buffered");
    hook.observe(cc_lb_plugin_api::ObserveEvent::Chunk {
        batch_index: 1,
        event_count: 1,
        total_bytes: 20,
    })
    .expect("second event flushes batch");
}
