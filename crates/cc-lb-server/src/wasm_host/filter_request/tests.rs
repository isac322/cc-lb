use super::*;

fn principal() -> Principal {
    Principal {
        id: "tenant-a".to_owned(),
        kind: PrincipalKind::ApiKey,
    }
}

fn context() -> RoutingContext {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    headers.insert(
        http::header::AUTHORIZATION,
        http::HeaderValue::from_static("Bearer secret"),
    );
    RoutingContext {
        routing_now_unix_secs: 1_800_000_000,
        request_id: "req-123".to_owned(),
        thread_id: None,
        requested_service_tier: Some("priority".to_owned()),
        downstream_headers: headers,
        method: http::Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: bytes::Bytes::from_static(b"{\"msg\":\"hi\"}"),
        canonical_model_id: "claude-fixture".to_owned(),
        cache_pricing: cc_lb_domain::CachePricingSummary::default(),
    }
}

#[test]
fn v1_encoding_includes_requested_service_tier() {
    let principal = principal();
    let context = context();
    FilterWireRequest {
        wire_version: WireVersion::V1,
        ctx: &context,
        principal: &principal,
        candidates: &[],
        cookie_redaction: false,
    }
    .encode(|bytes| {
        let archived =
            rkyv::access::<cc_lb_plugin_wire::v1::ArchivedFilterRequest, RkyvError>(bytes)
                .expect("archived V1");
        assert_eq!(archived.headers.len(), 1);
        let header_name: &str = &archived.headers[0].name;
        assert_eq!(header_name, "content-type");
        let tier: Option<&str> = archived.service_tier.as_ref().map(|value| &**value);
        assert_eq!(tier, Some("priority"));
    })
    .expect("encode V1");
}

fn snapshot(
    window: &str,
    state: SubscriptionQuotaDataState,
    utilization: Option<f64>,
    resets_at_unix_secs: Option<u64>,
    status: Option<&str>,
    disabled_reason: Option<&str>,
) -> cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
    cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
        window: window.to_owned(),
        state,
        source: Some("header".to_owned()),
        utilization,
        status: status.map(str::to_owned),
        resets_at_unix_secs,
        surpassed_threshold: None,
        representative_claim: None,
        disabled_reason: disabled_reason.map(str::to_owned),
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        observed_at_unix_millis: Some(1_762_996_400_000),
        max_staleness_secs: 300,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
    }
}

fn quota_candidate() -> UpstreamCandidate {
    UpstreamCandidate {
        upstream_id: uuid::Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
        name: "upstream-1".to_owned(),
        kind: cc_lb_domain::UpstreamKind::AnthropicApiKey,
        subscription_quotas: vec![
            snapshot(
                "5h",
                SubscriptionQuotaDataState::Fresh,
                Some(0.25),
                Some(1_763_003_600),
                Some("allowed"),
                None,
            ),
            snapshot(
                "7d",
                SubscriptionQuotaDataState::Stale,
                Some(0.9),
                Some(1_763_608_400),
                None,
                Some("overage_disabled"),
            ),
            snapshot(
                "monthly",
                SubscriptionQuotaDataState::Absent,
                None,
                None,
                None,
                None,
            ),
        ],
        observed_at_unix_secs: 0,
        cache_score: None,
        base_url: None,
        plan_capacity_ratio: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
    }
}

#[test]
fn v2_encoding_maps_subscription_quota_windows() {
    let principal = principal();
    let context = context();
    let candidates = [quota_candidate()];
    FilterWireRequest {
        wire_version: WireVersion::V2,
        ctx: &context,
        principal: &principal,
        candidates: &candidates,
        cookie_redaction: false,
    }
    .encode(|bytes| {
        let archived =
            rkyv::access::<cc_lb_plugin_wire::v2::ArchivedFilterRequest, RkyvError>(bytes)
                .expect("archived V2");
        assert_eq!(
            archived.routing_now_unix_secs,
            context.routing_now_unix_secs
        );
        assert_eq!(archived.candidates.len(), 1);
        let quotas = &archived.candidates[0].subscription_quotas;
        assert_eq!(quotas.len(), 3, "domain order preserved");

        let window: &str = &quotas[0].window;
        let state: &str = &quotas[0].state;
        let status: Option<&str> = quotas[0].status.as_ref().map(|value| &**value);
        let disabled: Option<&str> = quotas[0].disabled_reason.as_ref().map(|value| &**value);
        assert_eq!(window, "5h");
        assert_eq!(state, "fresh");
        assert_eq!(quotas[0].utilization, Some(0.25));
        assert_eq!(quotas[0].resets_at_unix_secs, Some(1_763_003_600));
        assert_eq!(quotas[0].observed_at_unix_millis, Some(1_762_996_400_000));
        assert_eq!(status, Some("allowed"));
        assert_eq!(disabled, None);
        assert_eq!(quotas[0].max_staleness_secs, 300);

        let window: &str = &quotas[1].window;
        let state: &str = &quotas[1].state;
        let disabled: Option<&str> = quotas[1].disabled_reason.as_ref().map(|value| &**value);
        assert_eq!(window, "7d");
        assert_eq!(state, "stale");
        assert_eq!(disabled, Some("overage_disabled"));

        let window: &str = &quotas[2].window;
        let state: &str = &quotas[2].state;
        assert_eq!(window, "monthly");
        assert_eq!(state, "absent");
    })
    .expect("encode V2");
}

#[test]
fn quota_data_state_maps_to_domain_serde_names() {
    assert_eq!(
        quota_data_state_to_wire(SubscriptionQuotaDataState::Fresh),
        "fresh"
    );
    assert_eq!(
        quota_data_state_to_wire(SubscriptionQuotaDataState::Stale),
        "stale"
    );
    assert_eq!(
        quota_data_state_to_wire(SubscriptionQuotaDataState::Absent),
        "absent"
    );
    assert_eq!(
        quota_data_state_to_wire(SubscriptionQuotaDataState::Unobserved),
        "unobserved"
    );
}

#[test]
fn v1_encoding_is_identical_with_or_without_domain_quota_data() {
    let principal = principal();
    let context = context();
    let populated = quota_candidate();
    let mut empty = populated.clone();
    empty.subscription_quotas.clear();
    let encode = |context: &RoutingContext, candidate| {
        FilterWireRequest {
            wire_version: WireVersion::V1,
            ctx: context,
            principal: &principal,
            candidates: std::slice::from_ref(candidate),
            cookie_redaction: false,
        }
        .encode(<[u8]>::to_vec)
        .expect("encode V1")
    };
    assert_eq!(encode(&context, &populated), encode(&context, &empty));
    let mut later = context.clone();
    later.routing_now_unix_secs += 99;
    assert_eq!(encode(&context, &populated), encode(&later, &populated));
}
