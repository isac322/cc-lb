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

#[test]
fn v2_encoding_includes_provider_kind_and_quota_sample() {
    let principal = principal();
    let context = context();
    let candidate = cc_lb_domain::UpstreamCandidate {
        upstream_id: uuid::Uuid::new_v4(),
        name: "gemini".to_owned(),
        kind: cc_lb_domain::UpstreamKind::GeminiOauth,
        subscription_quotas: vec![cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
            window: "gemini_5h".to_owned(),
            state: cc_lb_domain::SubscriptionQuotaDataState::Fresh,
            source: Some("api".to_owned()),
            utilization: Some(0.25),
            status: Some("allowed".to_owned()),
            resets_at_unix_secs: Some(123),
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            observed_at_unix_millis: Some(456),
            max_staleness_secs: 60,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
        }],
        observed_at_unix_secs: 10,
        cache_score: None,
        base_url: Some("https://generativelanguage.googleapis.com".to_owned()),
        plan_capacity_ratio: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
    };
    FilterWireRequest {
        wire_version: WireVersion::V2,
        ctx: &context,
        principal: &principal,
        candidates: &[candidate],
        cookie_redaction: false,
    }
    .encode(|bytes| {
        let archived =
            rkyv::access::<cc_lb_plugin_wire::v2::ArchivedFilterRequest, RkyvError>(bytes)
                .expect("archived V2");
        let candidate = &archived.candidates[0];
        assert_eq!(candidate.kind.as_ref(), "gemini_oauth");
        assert_eq!(
            candidate.base_url.as_ref().expect("base URL").as_ref(),
            "https://generativelanguage.googleapis.com"
        );
        assert_eq!(
            candidate.subscription_quotas[0].sample_kind.as_ref(),
            "sample"
        );
        assert_eq!(candidate.subscription_quotas[0].utilization, Some(0.25));
    })
    .expect("encode V2");
}
