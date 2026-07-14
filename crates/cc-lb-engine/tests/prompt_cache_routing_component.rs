use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_domain::{TtlClass, UpstreamCandidate, WarmCacheEntry, WrhKeySource};
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_engine::{
    DynamicView, DynamicViewBuilder, RequestKind, build_candidates, parse_request_cache_breakpoints,
};
use cc_lb_routing::{FilterPlugin, RoutingContext};
use http::{HeaderMap, Method};
use url::Url;
use uuid::Uuid;

use crate::common;
use crate::prompt_cache_routing_support;

use common::{TestAuthn, TestRouter, TestState};
use prompt_cache_routing_support::{
    TEST_MODEL, TEST_QUOTA_NOW_SECS, TestPromptCacheObservationCache, TestSubscriptionQuotaCache,
    known_base_quota_snapshots, plugin_principal, principal_record, seeded_uuid,
    test_cache_pricing, upstream_record,
};

#[test]
fn built_candidate_cache_score_drives_subscription_preference_component_route() {
    let owner_id = seeded_uuid(1);
    let quota_peer_id = seeded_uuid(2);
    let thread_id = "ses-warm-owner";
    let body = Bytes::from_static(
        br#"{
            "model":"claude-sonnet-4-5",
            "system":[{"type":"text","text":"stable system","cache_control":{"type":"ephemeral"}}],
            "messages":[{"role":"user","content":"hi"}],
            "max_tokens":16
        }"#,
    );
    let breakpoints = parse_request_cache_breakpoints(&HeaderMap::new(), &body);
    assert_eq!(breakpoints.len(), 1);
    let owner_prefix = breakpoints[0].lookback_prefixes[0].prefix_hash.clone();
    let prompt_cache = TestPromptCacheObservationCache::new(HashMap::from([(
        owner_id,
        vec![WarmCacheEntry {
            prefix_hash: owner_prefix.clone(),
            expires_at_unix_secs: TEST_QUOTA_NOW_SECS + 300,
            ttl_class: TtlClass::Ephemeral5m,
            last_observed_at_unix_secs: TEST_QUOTA_NOW_SECS,
            content_block_index: 0,
            estimated_prefix_tokens: breakpoints[0].prefix_token_count,
            token_estimate_source: "local_tiktoken_v1".to_owned(),
            hash_schema_version: 4,
        }],
    )]));
    let view = test_view(prompt_cache, owner_id, quota_peer_id);

    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &breakpoints,
        Some(thread_id),
        &cc_lb_engine::SystemClock,
    );
    let owner = candidate(&candidates, owner_id, "owner");
    let peer = candidate(&candidates, quota_peer_id, "quota peer");
    let owner_score = owner.cache_score.as_ref().expect("owner cache score");
    assert_eq!(
        owner_score.matched_v3_cache_key.as_deref(),
        Some(owner_prefix.as_str())
    );
    assert_eq!(owner_score.ambiguity_reason, None);
    assert_eq!(
        peer.cache_score
            .as_ref()
            .expect("peer creation-only score")
            .predicted_cache_read_tokens,
        0
    );

    let output = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx(body, breakpoints, thread_id),
            &plugin_principal(),
            &candidates,
        )
        .expect("subscription preference filter succeeds");
    let trace = output
        .subscription_preference
        .expect("subscription preference trace present");
    let owner_weight = trace_row_weight(&trace.candidates, owner_id, "owner");
    let peer_weight = trace_row_weight(&trace.candidates, quota_peer_id, "quota peer");

    assert_eq!(trace.wrh_key_source, WrhKeySource::CacheHash);
    assert_eq!(
        trace.bucket_v3_cache_affinity_key.as_deref(),
        Some(owner_prefix.as_str())
    );
    assert!(
        owner_weight > peer_weight * 1000.0,
        "generated thread cache score must dominate moderate quota advantage: owner={owner_weight}, peer={peer_weight}"
    );
    assert_eq!(output.kept_upstream_ids, vec![owner_id]);
}

// Regression: moving cache_control to an appended tail must not drop the warm
// deep prefix, so routing stays on cache_hash instead of scattering on request_id.
#[test]
fn moved_cache_control_keeps_warm_upstream_routed_by_cache_hash() {
    let owner_id = seeded_uuid(1);
    let quota_peer_id = seeded_uuid(2);
    let thread_id = "ses-warm-owner";
    let deep_text = "lorem ipsum dolor sit amet ".repeat(400);

    // Turn T: deep user block carries cache_control and is warmed on the owner.
    let turn_one = Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "model": "claude-sonnet-4-5",
            "system": [{"type":"text","text":"stable system","cache_control":{"type":"ephemeral"}}],
            "messages": [{"role":"user","content":[
                {"type":"text","text": deep_text, "cache_control":{"type":"ephemeral"}}
            ]}],
            "max_tokens": 16
        }))
        .expect("turn one body serializes"),
    );
    let turn_one_breakpoints = parse_request_cache_breakpoints(&HeaderMap::new(), &turn_one);
    assert_eq!(turn_one_breakpoints.len(), 2);
    let deep_prefix = turn_one_breakpoints[1].lookback_prefixes[0]
        .prefix_hash
        .clone();

    // Turn T+1: a new message is appended and cache_control moves to it; the deep
    // block loses cache_control. Only the deep prefix is warm on the owner.
    let turn_two = Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "model": "claude-sonnet-4-5",
            "system": [{"type":"text","text":"stable system","cache_control":{"type":"ephemeral"}}],
            "messages": [
                {"role":"user","content":[{"type":"text","text": deep_text}]},
                {"role":"user","content":[
                    {"type":"text","text":"short next turn","cache_control":{"type":"ephemeral"}}
                ]}
            ],
            "max_tokens": 16
        }))
        .expect("turn two body serializes"),
    );
    let turn_two_breakpoints = parse_request_cache_breakpoints(&HeaderMap::new(), &turn_two);

    let prompt_cache = TestPromptCacheObservationCache::new(HashMap::from([(
        owner_id,
        vec![WarmCacheEntry {
            prefix_hash: deep_prefix.clone(),
            expires_at_unix_secs: TEST_QUOTA_NOW_SECS + 300,
            ttl_class: TtlClass::Ephemeral5m,
            last_observed_at_unix_secs: TEST_QUOTA_NOW_SECS,
            content_block_index: 0,
            estimated_prefix_tokens: turn_one_breakpoints[1].prefix_token_count,
            token_estimate_source: "local_tiktoken_v1".to_owned(),
            hash_schema_version: 4,
        }],
    )]));
    let view = test_view(prompt_cache, owner_id, quota_peer_id);

    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &turn_two_breakpoints,
        Some(thread_id),
        &cc_lb_engine::SystemClock,
    );
    let owner = candidate(&candidates, owner_id, "owner");
    let owner_score = owner.cache_score.as_ref().expect("owner cache score");
    assert_eq!(
        owner_score.matched_v3_cache_key.as_deref(),
        Some(deep_prefix.as_str()),
        "moved cache_control must still match the previous deep prefix as warm"
    );
    assert!(
        owner_score.predicted_cache_read_tokens > 0,
        "deep warm prefix must drive a non-zero predicted cache read"
    );

    let output = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx(turn_two, turn_two_breakpoints, thread_id),
            &plugin_principal(),
            &candidates,
        )
        .expect("subscription preference filter succeeds");
    let trace = output
        .subscription_preference
        .expect("subscription preference trace present");

    assert_eq!(trace.wrh_key_source, WrhKeySource::CacheHash);
    assert_eq!(
        trace.bucket_v3_cache_affinity_key.as_deref(),
        Some(deep_prefix.as_str())
    );
    assert_eq!(output.kept_upstream_ids, vec![owner_id]);
}

fn ctx(
    body_bytes: Bytes,
    _cache_breakpoints: Vec<cc_lb_domain::CacheBreakpoint>,
    thread_id: &str,
) -> RoutingContext {
    RoutingContext {
        request_id: "req-1".to_owned(),
        thread_id: Some(thread_id.to_owned()),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes,
        canonical_model_id: TEST_MODEL.to_owned(),
        cache_pricing: test_cache_pricing(),
    }
}

fn test_view(
    prompt_cache: TestPromptCacheObservationCache,
    owner_id: Uuid,
    quota_peer_id: Uuid,
) -> Arc<DynamicView> {
    let quota_cache = TestSubscriptionQuotaCache::new(HashMap::from([
        (owner_id, known_base_quota_snapshots(0.22)),
        (quota_peer_id, known_base_quota_snapshots(0.10)),
    ]));
    let authn = TestAuthn::with_principal_view(
        TestState::default(),
        Arc::new(PrincipalView::from_db(
            &[principal_record("principal")],
            HashMap::new(),
        )),
    );
    DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(TestRouter {
            base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
        }))
        .global_observability_hooks(Vec::new())
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![
            upstream_record(owner_id),
            upstream_record(quota_peer_id),
        ])
        .prompt_cache_observation_cache(Arc::new(prompt_cache))
        .subscription_quota_cache(Arc::new(quota_cache))
        .subscription_quota_routing_max_staleness_secs(60)
        .build()
}

fn candidate<'a>(
    candidates: &'a [UpstreamCandidate],
    upstream_id: Uuid,
    label: &str,
) -> &'a UpstreamCandidate {
    candidates
        .iter()
        .find(|candidate| candidate.upstream_id == upstream_id)
        .unwrap_or_else(|| panic!("{label} candidate present"))
}

fn trace_row_weight(
    rows: &[cc_lb_domain::CandidateUrgency],
    upstream_id: Uuid,
    label: &str,
) -> f64 {
    rows.iter()
        .find(|row| row.upstream_id == upstream_id)
        .unwrap_or_else(|| panic!("{label} urgency present"))
        .effective_weight
}
