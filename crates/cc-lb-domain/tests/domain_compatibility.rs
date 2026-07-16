use cc_lb_domain::{
    ANTHROPIC_IDENTITY_HEADERS, BUILTIN_CACHE_AFFINITY_ID, BUILTIN_CACHE_AFFINITY_NAME,
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BUILTIN_SUBSCRIPTION_PREFERENCE_NAME, BreakpointOrigin,
    CacheAffinityCandidate, CacheAffinityTrace, CacheBreakpoint, CacheBreakpointSource,
    CacheLookbackPrefix, CachePricingSummary, CacheScore, CandidateUrgency, CredentialStrategy,
    GLOBAL_PRINCIPAL, InternalError, InternalErrorKind, InternalErrorStage, MAX_ERROR_MESSAGE_LEN,
    MAX_ROUTING_TRACE_STAGES, MAX_STAGE_NAME_LEN, PlanInfo, Principal, PrincipalKind,
    PrincipalKindLite, RateLimitKind, RateLimitObservation, ReplicaIdentity, RoutingTrace,
    StageDecision, SubscriptionPreferenceTrace, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, SubscriptionTier, TerminalDecision, TerminalStrategy, TtlClass,
    Upstream, UpstreamCandidate, UpstreamKind, WarmCacheEntry,
};
use uuid::Uuid;

const REQUEST_EVENT_FIXTURE: &[u8] =
    include_bytes!("../../../tests/fixtures/reassembly/request_event.json");

macro_rules! assert_type_exists {
    ($($value_type:ty),+ $(,)?) => {
        $(let _ = std::mem::size_of::<$value_type>();)+
    };
}

fn fixture_object_after(key: &str) -> &[u8] {
    let marker = format!("\"{key}\":");
    let start = REQUEST_EVENT_FIXTURE
        .windows(marker.len())
        .position(|window| window == marker.as_bytes())
        .expect("fixture key exists")
        + marker.len();
    let bytes = &REQUEST_EVENT_FIXTURE[start..];
    let mut depth = 0_u32;
    let mut in_string = false;
    let mut escaped = false;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' if depth == 1 => return &bytes[..=index],
            b'}' => depth -= 1,
            _ => {}
        }
    }
    panic!("fixture object is terminated")
}

fn fixture_first_array_object_after(key: &str) -> &[u8] {
    let marker = format!("\"{key}\":[");
    let start = REQUEST_EVENT_FIXTURE
        .windows(marker.len())
        .position(|window| window == marker.as_bytes())
        .expect("fixture array key exists")
        + marker.len();
    let suffix = &REQUEST_EVENT_FIXTURE[start..];
    let object_start = suffix
        .iter()
        .position(|byte| *byte == b'{')
        .expect("fixture array contains an object");
    let object_bytes = &suffix[object_start..];
    let mut depth = 0_u32;
    for (index, byte) in object_bytes.iter().copied().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' if depth == 1 => return &object_bytes[..=index],
            b'}' => depth -= 1,
            _ => {}
        }
    }
    panic!("fixture array object is terminated")
}

#[test]
fn t1_public_value_api_exists_when_domain_is_built() {
    // Given: the authoritative T1 symbol-to-home set.
    // When: each expected public type is resolved from cc-lb-domain.
    assert_type_exists!(
        Principal,
        PrincipalKind,
        PrincipalKindLite,
        Upstream,
        UpstreamCandidate,
        UpstreamKind,
        CredentialStrategy,
        RateLimitKind,
        RateLimitObservation,
        SubscriptionQuotaDataState,
        SubscriptionQuotaCandidateSnapshot,
        SubscriptionTier,
        TtlClass,
        BreakpointOrigin,
        CacheBreakpointSource,
        CacheLookbackPrefix,
        CacheBreakpoint,
        WarmCacheEntry,
        CacheScore,
        CachePricingSummary,
        RoutingTrace,
        StageDecision,
        TerminalDecision,
        TerminalStrategy,
        SubscriptionPreferenceTrace,
        CandidateUrgency,
        CacheAffinityTrace,
        CacheAffinityCandidate,
        InternalError,
        InternalErrorKind,
        InternalErrorStage,
        PlanInfo,
        ReplicaIdentity,
    );

    // Then: the stable constants retain their established values.
    assert_eq!(GLOBAL_PRINCIPAL, "__global__");
    assert_eq!(BUILTIN_CACHE_AFFINITY_ID, Uuid::from_u128(1));
    assert_eq!(BUILTIN_CACHE_AFFINITY_NAME, "cache-affinity");
    assert_eq!(BUILTIN_SUBSCRIPTION_PREFERENCE_ID, Uuid::from_u128(2));
    assert_eq!(
        BUILTIN_SUBSCRIPTION_PREFERENCE_NAME,
        "subscription-preference"
    );
    assert_eq!(MAX_ROUTING_TRACE_STAGES, 100);
    assert_eq!(MAX_STAGE_NAME_LEN, 256);
    assert_eq!(MAX_ERROR_MESSAGE_LEN, 1024);
    assert!(ANTHROPIC_IDENTITY_HEADERS.contains(&"anthropic-organization-id"));
}

#[test]
fn routing_trace_and_internal_error_match_t0_fixture_bytes_when_reserialized() {
    // Given: exact committed T0 RequestEvent subobject bytes.
    let trace_bytes = fixture_object_after("routing_trace");
    let error_bytes = fixture_first_array_object_after("internal_errors");

    // When: domain values deserialize and reserialize those subobjects.
    let trace: RoutingTrace = serde_json::from_slice(trace_bytes).expect("deserialize trace");
    let error: InternalError = serde_json::from_slice(error_bytes).expect("deserialize error");

    // Then: serde emits byte-identical JSON in the original field order.
    assert_eq!(
        serde_json::to_vec(&trace).expect("serialize trace"),
        trace_bytes
    );
    assert_eq!(
        serde_json::to_vec(&error).expect("serialize error"),
        error_bytes
    );
}

#[test]
fn upstream_candidate_retains_current_serde_bytes_when_populated_from_t0_identity() {
    // Given: a candidate populated with the T0 fixture's upstream identity.
    let candidate = UpstreamCandidate {
        upstream_id: Uuid::from_u128(0x018f_2a3b_4c5d_7e6f_8123_4567_89ab_cdef),
        name: "subscription-primary".to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        observed_rate_limits: Vec::new(),
        subscription_quotas: Vec::new(),
        observed_at_unix_secs: 1_720_000_000,
        cache_score: None,
        base_url: None,
        plan_capacity_ratio: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
    };

    // When: the moved domain value is serialized.
    let bytes = serde_json::to_vec(&candidate).expect("serialize candidate");

    // Then: its established baseline byte shape is unchanged.
    assert_eq!(
        bytes,
        br#"{"upstream_id":"018f2a3b-4c5d-7e6f-8123-456789abcdef","name":"subscription-primary","kind":"anthropic_oauth","observed_rate_limits":[],"subscription_quotas":[],"observed_at_unix_secs":1720000000}"#
    );
}

#[test]
fn ttl_class_keeps_canonical_snake_case_strings() {
    // Given: both canonical prompt-cache TTL classes.
    let values = [TtlClass::Ephemeral5m, TtlClass::Ephemeral1h];

    // When: the domain values are serialized.
    let bytes = serde_json::to_vec(&values).expect("serialize ttl classes");

    // Then: the historical storage/plugin strings remain exact.
    assert_eq!(bytes, br#"["ephemeral5m","ephemeral1h"]"#);
}

#[test]
fn candidate_urgency_equality_uses_total_float_ordering() {
    // Given: a value containing NaN in every floating-point equality seam.
    let value = CandidateUrgency {
        upstream_id: Uuid::nil(),
        tier: SubscriptionTier::KnownBase,
        quota_urgency: f64::NAN,
        quota_urgency_5h: Some(f64::NAN),
        quota_urgency_7d: Some(f64::NAN),
        quota_urgency_combined: Some(f64::NAN),
        predicted_cache_read_tokens: 0,
        predicted_cache_creation_tokens_5m: 0,
        predicted_cache_creation_tokens_1h: 0,
        predicted_uncached_input_tokens: 0,
        cache_ratio: f64::NAN,
        warning_multiplier: f64::NAN,
        cache_savings_ratio: f64::NAN,
        estimated_input_cost_micros: 0,
        cache_value_micros: None,
        matched_v3_cache_key: None,
        matched_content_block_index: None,
        breakpoint_content_block_index: None,
        lookback_distance: None,
        token_estimate_source: None,
    };

    // When: custom equality compares identical total-order float bit patterns.
    let equal = value == value.clone();

    // Then: the established Eq behavior remains reflexive even for NaN.
    assert!(equal);
}
