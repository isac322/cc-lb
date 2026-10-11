use cc_lb_plugin_wire::v1::{
    CachePricingSummary, CachePricingSummaryRef, Claim, ClaimRef, Header, HeaderRef, Principal,
    PrincipalRef, QueryRef,
};
use cc_lb_plugin_wire::v2::{
    ArchivedFilterRequest, FilterRequest, FilterRequestRef, QuotaWindowSnapshot,
    QuotaWindowSnapshotRef, UpstreamCandidate, UpstreamCandidateRef,
};
use rkyv::rancor::Error;

fn principal() -> Principal {
    Principal {
        id: Box::from("tenant-a"),
        kind: Box::from("api_key"),
        claims: Box::new([Claim {
            key: Box::from("scope"),
            value: Box::from(&b"inference"[..]),
        }]),
    }
}

fn header(name: &str, value: &str) -> Header {
    Header {
        name: Box::from(name),
        value: Box::from(value.as_bytes()),
    }
}

#[test]
fn filter_request_round_trips() {
    let req = FilterRequest {
        routing_now_unix_secs: 1_800_000_000,
        request_id: Box::from("req-1"),
        thread_id: None,
        service_tier: Some(Box::from("priority")),
        canonical_model_id: Box::from("claude-test"),
        cache_pricing: CachePricingSummary {
            status: Box::from("known"),
            input_micros_per_million: Some(5_000_000),
            cache_creation_5m_micros_per_million: Some(6_250_000),
            cache_creation_1h_micros_per_million: Some(10_000_000),
            cache_read_micros_per_million: Some(500_000),
        },
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([header("content-type", "application/json")]),
        body: Box::from(&b"{\"k\":1}"[..]),
        principal: principal(),
        candidates: Box::new([UpstreamCandidate {
            upstream_id: Box::from("u-1"),
            name: Box::from("anthropic-direct"),
            kind: Box::from("anthropic_api_key"),
            observed_at_unix_secs: 42,
            predicted_cache_read_tokens: 256,
            predicted_cache_creation_tokens_5m: 512,
            predicted_cache_creation_tokens_1h: 1024,
            predicted_uncached_input_tokens: 32,
            plan_capacity_ratio: 1.0,
            organization_type: Box::from(""),
            rate_limit_tier: Box::from(""),
            seat_tier: Box::from(""),
            subscription_quotas: Box::new([
                QuotaWindowSnapshot {
                    window: Box::from("5h"),
                    state: Box::from("fresh"),
                    utilization: Some(0.25),
                    resets_at_unix_secs: Some(1_700_000_000),
                    observed_at_unix_millis: Some(1_699_990_000_000),
                    status: Some(Box::from("ok")),
                    disabled_reason: None,
                    max_staleness_secs: 300,
                },
                QuotaWindowSnapshot {
                    window: Box::from("7d"),
                    state: Box::from("stale"),
                    utilization: Some(0.9),
                    resets_at_unix_secs: Some(1_700_604_800),
                    observed_at_unix_millis: None,
                    status: None,
                    disabled_reason: Some(Box::from("overage_disabled")),
                    max_staleness_secs: 900,
                },
            ]),
        }]),
    };
    let bytes = rkyv::to_bytes::<Error>(&req).expect("encode");
    let archived = rkyv::access::<ArchivedFilterRequest, Error>(&bytes).expect("access");
    let owned: FilterRequest =
        rkyv::deserialize::<FilterRequest, Error>(archived).expect("deserialize");
    assert_eq!(owned.routing_now_unix_secs, 1_800_000_000);
    assert_eq!(&*owned.request_id, "req-1");
    assert_eq!(&*owned.canonical_model_id, "claude-test");
    assert_eq!(owned.candidates.len(), 1);
    assert_eq!(owned.candidates[0].predicted_cache_read_tokens, 256);
    assert_eq!(owned.candidates[0].predicted_cache_creation_tokens_5m, 512);
    assert_eq!(owned.candidates[0].subscription_quotas.len(), 2);
    let five_hour = &owned.candidates[0].subscription_quotas[0];
    assert_eq!(&*five_hour.window, "5h");
    assert_eq!(&*five_hour.state, "fresh");
    assert_eq!(five_hour.utilization, Some(0.25));
    assert_eq!(five_hour.resets_at_unix_secs, Some(1_700_000_000));
    assert_eq!(five_hour.observed_at_unix_millis, Some(1_699_990_000_000));
    assert_eq!(five_hour.status.as_deref(), Some("ok"));
    assert_eq!(five_hour.disabled_reason.as_deref(), None);
    assert_eq!(five_hour.max_staleness_secs, 300);
    let seven_day = &owned.candidates[0].subscription_quotas[1];
    assert_eq!(&*seven_day.window, "7d");
    assert_eq!(&*seven_day.state, "stale");
    assert_eq!(
        seven_day.disabled_reason.as_deref(),
        Some("overage_disabled")
    );
}

/// RFC-0001 #9: `FilterRequestRef<'_>` (borrowed) must produce bytes
/// the guest-side `ArchivedFilterRequest` view can parse cleanly.
#[test]
fn filter_request_ref_encodes_to_owned_wire() {
    let claim_refs = [ClaimRef {
        key: "scope",
        value: b"inference",
    }];
    let header_refs = [HeaderRef {
        name: "content-type",
        value: b"application/json",
    }];
    let quota_refs = [
        QuotaWindowSnapshotRef {
            window: "5h",
            state: "fresh",
            utilization: Some(0.25),
            resets_at_unix_secs: Some(1_700_000_000),
            observed_at_unix_millis: Some(1_699_990_000_000),
            status: Some(QueryRef { value: "ok" }),
            disabled_reason: None,
            max_staleness_secs: 300,
        },
        QuotaWindowSnapshotRef {
            window: "7d",
            state: "stale",
            utilization: Some(0.9),
            resets_at_unix_secs: Some(1_700_604_800),
            observed_at_unix_millis: None,
            status: None,
            disabled_reason: Some(QueryRef {
                value: "overage_disabled",
            }),
            max_staleness_secs: 900,
        },
    ];
    let candidate_refs = [UpstreamCandidateRef {
        upstream_id: "u-1",
        name: "anthropic-direct",
        kind: "anthropic_api_key",
        observed_at_unix_secs: 42,
        predicted_cache_read_tokens: 256,
        predicted_cache_creation_tokens_5m: 512,
        predicted_cache_creation_tokens_1h: 1024,
        predicted_uncached_input_tokens: 32,
        plan_capacity_ratio: 1.0,
        organization_type: "",
        rate_limit_tier: "",
        seat_tier: "",
        subscription_quotas: &quota_refs,
    }];
    let req_ref = FilterRequestRef {
        routing_now_unix_secs: 1_800_000_000,
        request_id: "req-1",
        thread_id: None,
        service_tier: Some(QueryRef { value: "priority" }),
        canonical_model_id: "claude-test",
        cache_pricing: CachePricingSummaryRef {
            status: "known",
            input_micros_per_million: Some(5_000_000),
            cache_creation_5m_micros_per_million: Some(6_250_000),
            cache_creation_1h_micros_per_million: Some(10_000_000),
            cache_read_micros_per_million: Some(500_000),
        },
        method: "POST",
        path: "/v1/messages",
        query: None,
        headers: &header_refs,
        body: b"{\"k\":1}",
        principal: PrincipalRef {
            id: "tenant-a",
            kind: "api_key",
            claims: &claim_refs,
        },
        candidates: &candidate_refs,
    };
    let bytes = rkyv::to_bytes::<Error>(&req_ref).expect("encode ref");
    let archived = rkyv::access::<ArchivedFilterRequest, Error>(&bytes).expect("access owned");
    assert_eq!(archived.routing_now_unix_secs, 1_800_000_000);
    let owned_body: &[u8] = &archived.body;
    assert_eq!(owned_body, b"{\"k\":1}");
    let owned_id: &str = &archived.request_id;
    assert_eq!(owned_id, "req-1");
    let service_tier: Option<&str> = archived.service_tier.as_ref().map(|value| &**value);
    assert_eq!(service_tier, Some("priority"));
    let model_id: &str = &archived.canonical_model_id;
    assert_eq!(model_id, "claude-test");
    let quotas = &archived.candidates[0].subscription_quotas;
    assert_eq!(quotas.len(), 2);
    let window: &str = &quotas[0].window;
    let state: &str = &quotas[0].state;
    assert_eq!(window, "5h");
    assert_eq!(state, "fresh");
    assert_eq!(quotas[0].utilization, Some(0.25));
    assert_eq!(quotas[0].resets_at_unix_secs, Some(1_700_000_000));
    assert_eq!(quotas[0].observed_at_unix_millis, Some(1_699_990_000_000));
    let status: Option<&str> = quotas[0].status.as_ref().map(|value| &**value);
    assert_eq!(status, Some("ok"));
    let disabled: Option<&str> = quotas[0].disabled_reason.as_ref().map(|value| &**value);
    assert_eq!(disabled, None);
    assert_eq!(quotas[0].max_staleness_secs, 300);
    let window: &str = &quotas[1].window;
    assert_eq!(window, "7d");
    let disabled: Option<&str> = quotas[1].disabled_reason.as_ref().map(|value| &**value);
    assert_eq!(disabled, Some("overage_disabled"));
}

/// The nested quota windows must archive identically in owned and
/// borrowed form: the host encodes `QuotaWindowSnapshotRef`, the guest
/// reads `ArchivedQuotaWindowSnapshot` through `ArchivedFilterRequest`.
#[test]
fn quota_window_owned_and_ref_layouts_agree() {
    let owned = QuotaWindowSnapshot {
        window: Box::from("5h"),
        state: Box::from("unobserved"),
        utilization: None,
        resets_at_unix_secs: None,
        observed_at_unix_millis: Some(1_699_990_000_000),
        status: None,
        disabled_reason: Some(Box::from("seat_not_provisioned")),
        max_staleness_secs: 45,
    };
    let borrowed = QuotaWindowSnapshotRef {
        window: "5h",
        state: "unobserved",
        utilization: None,
        resets_at_unix_secs: None,
        observed_at_unix_millis: Some(1_699_990_000_000),
        status: None,
        disabled_reason: Some(QueryRef {
            value: "seat_not_provisioned",
        }),
        max_staleness_secs: 45,
    };
    let owned_bytes = rkyv::to_bytes::<Error>(&owned).expect("encode owned");
    let borrowed_bytes = rkyv::to_bytes::<Error>(&borrowed).expect("encode borrowed");
    assert_eq!(
        owned_bytes.len(),
        borrowed_bytes.len(),
        "owned and borrowed quota snapshots must occupy the same archived size"
    );

    let archived: &cc_lb_plugin_wire::v2::ArchivedQuotaWindowSnapshot =
        rkyv::access::<cc_lb_plugin_wire::v2::ArchivedQuotaWindowSnapshot, Error>(&borrowed_bytes)
            .expect("access borrowed bytes as the owned archived type");
    let window: &str = &archived.window;
    let state: &str = &archived.state;
    let disabled: Option<&str> = archived.disabled_reason.as_ref().map(|value| &**value);
    assert_eq!(window, "5h");
    assert_eq!(state, "unobserved");
    let status: Option<&str> = archived.status.as_ref().map(|value| &**value);
    assert_eq!(archived.utilization, None::<f64>);
    assert_eq!(archived.resets_at_unix_secs, None::<u64>);
    assert_eq!(archived.observed_at_unix_millis, Some(1_699_990_000_000));
    assert_eq!(status, None);
    assert_eq!(disabled, Some("seat_not_provisioned"));
    assert_eq!(archived.max_staleness_secs, 45);

    let round_tripped: QuotaWindowSnapshot =
        rkyv::deserialize::<QuotaWindowSnapshot, Error>(archived).expect("deserialize");
    assert_eq!(&*round_tripped.window, "5h");
    assert_eq!(&*round_tripped.state, "unobserved");
    assert_eq!(round_tripped.utilization, None);
    assert_eq!(round_tripped.resets_at_unix_secs, None);
    assert_eq!(
        round_tripped.observed_at_unix_millis,
        Some(1_699_990_000_000)
    );
    assert_eq!(round_tripped.status, None);
    assert_eq!(
        round_tripped.disabled_reason.as_deref(),
        Some("seat_not_provisioned")
    );
    assert_eq!(round_tripped.max_staleness_secs, 45);
}
