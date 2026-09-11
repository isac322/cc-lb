use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_domain::{
    CacheBreakpoint, CacheBreakpointSource, TtlClass, UpstreamCandidate, WarmCacheEntry,
};
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_engine::{
    DynamicView, DynamicViewBuilder, RequestKind, build_candidates, lifecycle::HASH_SCHEMA_VERSION,
    parse_request_cache_breakpoints,
};
use cc_lb_routing::{FilterPlugin, RoutingContext};
use http::{HeaderMap, Method};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;

use crate::common;
mod prompt_cache_routing_support;

use common::{TestAuthn, TestRouter, TestState};
use prompt_cache_routing_support::{
    TEST_MODEL, TEST_QUOTA_NOW_SECS, TestPromptCacheObservationCache, TestSubscriptionQuotaCache,
    known_base_quota_snapshots, plugin_principal, principal_record, seeded_uuid,
    test_cache_pricing, upstream_record,
};

#[test]
fn t2__built_candidate_cache_score_drives_subscription_preference_component_route() {
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
            hash_schema_version: HASH_SCHEMA_VERSION,
        }],
    )]));
    let view = test_view(prompt_cache, owner_id, quota_peer_id);
    let clock = cc_lb_engine::TestClock::new_at_secs(TEST_QUOTA_NOW_SECS);

    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &breakpoints,
        Some(thread_id),
        &clock,
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
    let owner_row = trace_row(&trace.candidates, owner_id, "owner");
    let peer_row = trace_row(&trace.candidates, quota_peer_id, "quota peer");

    assert_eq!(
        trace.bucket_v3_cache_key.as_deref(),
        Some(owner_prefix.as_str())
    );
    assert_eq!(
        owner_row.matched_v3_cache_key.as_deref(),
        Some(owner_prefix.as_str())
    );
    assert!(owner_row.predicted_cache_read_tokens > peer_row.predicted_cache_read_tokens);
    assert_eq!(
        trace.formula_winner_upstream_id,
        Some(owner_id),
        "generated thread cache score must dominate moderate quota advantage"
    );
    assert_eq!(output.kept_upstream_ids, vec![owner_id]);
}

// Regression: moving cache_control to an appended tail must not drop the warm
// deep prefix, so routing stays on cache_hash instead of scattering on request_id.
#[test]
fn t2__moved_cache_control_keeps_warm_upstream_routed_by_cache_hash() {
    let owner_id = seeded_uuid(1);
    let quota_peer_id = seeded_uuid(2);
    let thread_id = "ses-warm-owner";
    let deep_text = "lorem ipsum dolor sit amet ".repeat(400);

    // Turn T: deep user block carries cache_control and is warmed on the owner.
    let turn_one = Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "model": "claude-sonnet-4-5",
            "system": [{"type":"text","text":"stable system","cache_control":{"type":"ephemeral","ttl":"1h"}}],
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
            "system": [{"type":"text","text":"stable system","cache_control":{"type":"ephemeral","ttl":"1h"}}],
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
    assert_eq!(turn_two_breakpoints.len(), 2);

    let prompt_cache = TestPromptCacheObservationCache::new(HashMap::from([(
        owner_id,
        vec![WarmCacheEntry {
            prefix_hash: deep_prefix.clone(),
            expires_at_unix_secs: TEST_QUOTA_NOW_SECS + 300,
            ttl_class: TtlClass::Ephemeral5m,
            last_observed_at_unix_secs: TEST_QUOTA_NOW_SECS,
            content_block_index: turn_one_breakpoints[1].block_index,
            estimated_prefix_tokens: turn_one_breakpoints[1].prefix_token_count,
            token_estimate_source: "local_tiktoken_v1".to_owned(),
            hash_schema_version: HASH_SCHEMA_VERSION,
        }],
    )]));
    let view = test_view(prompt_cache, owner_id, quota_peer_id);
    let clock = cc_lb_engine::TestClock::new_at_secs(TEST_QUOTA_NOW_SECS);

    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &turn_two_breakpoints,
        Some(thread_id),
        &clock,
    );
    let owner = candidate(&candidates, owner_id, "owner");
    let owner_score = owner.cache_score.as_ref().expect("owner cache score");
    assert_eq!(
        owner_score.matched_v3_cache_key.as_deref(),
        Some(deep_prefix.as_str()),
        "moved cache_control must still match the previous deep prefix as warm"
    );
    assert_eq!(
        u64::from(owner_score.predicted_cache_read_tokens),
        turn_one_breakpoints[1].prefix_token_count
    );
    assert_eq!(
        u64::from(owner_score.predicted_cache_creation_tokens_5m),
        turn_two_breakpoints[1]
            .prefix_token_count
            .saturating_sub(turn_one_breakpoints[1].prefix_token_count)
    );
    assert_eq!(owner_score.predicted_cache_creation_tokens_1h, 0);
    assert_eq!(owner_score.matched_breakpoint_index, Some(2));
    assert_eq!(owner_score.matched_content_block_index, Some(1));
    assert_eq!(owner_score.lookback_distance, Some(1));
    assert_eq!(
        owner_score.token_estimate_source.as_deref(),
        Some("local_tiktoken_v1")
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

    assert_eq!(
        trace.bucket_v3_cache_key.as_deref(),
        Some(deep_prefix.as_str())
    );
    assert_eq!(output.kept_upstream_ids, vec![owner_id]);
}

#[test]
fn t2__automatic_cache_control_creates_one_breakpoint_and_routes_to_warm_owner() {
    let owner_id = seeded_uuid(1);
    let quota_peer_id = seeded_uuid(2);
    let thread_id = "ses-automatic-cache";
    let request = json!({
        "model": TEST_MODEL,
        "max_tokens": 64,
        "cache_control": {"type": "ephemeral"},
        "system": [{"type": "text", "text": "automatic caching stable prefix ".repeat(900)}],
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": "automatic caching tail ".repeat(160)}
        ]}]
    });
    let body = request_body(&request);
    let breakpoints = parse_request_cache_breakpoints(&HeaderMap::new(), &body);
    let repeated = parse_request_cache_breakpoints(&HeaderMap::new(), &body);

    assert_eq!(breakpoints.len(), 1);
    assert_eq!(repeated[0].prefix_hash, breakpoints[0].prefix_hash);

    let warm_prefix = breakpoints[0].prefix_hash.clone();
    let prompt_cache = TestPromptCacheObservationCache::new(HashMap::from([(
        owner_id,
        vec![warm_entry_for(&breakpoints[0])],
    )]));
    let view = test_view(prompt_cache, owner_id, quota_peer_id);
    let clock = cc_lb_engine::TestClock::new_at_secs(TEST_QUOTA_NOW_SECS);
    let candidates = build_candidates(
        &view,
        "principal",
        RequestKind::AnthropicMessages,
        TEST_MODEL,
        &breakpoints,
        Some(thread_id),
        &clock,
    );
    let owner = candidate(&candidates, owner_id, "owner");
    let score = owner.cache_score.as_ref().expect("owner cache score");

    assert_eq!(
        score.matched_v3_cache_key.as_deref(),
        Some(warm_prefix.as_str())
    );
    assert_eq!(
        u64::from(score.predicted_cache_read_tokens),
        breakpoints[0].prefix_token_count
    );

    let output = SubscriptionPreferenceFilter::new()
        .filter(
            &ctx(body, breakpoints, thread_id),
            &plugin_principal(),
            &candidates,
        )
        .expect("subscription preference filter succeeds");
    assert_eq!(output.kept_upstream_ids, vec![owner_id]);
}

#[test]
fn t2__request_level_invalidators_match_stable_tools_prefix_not_invalidated_message() {
    type Mutator = fn(&mut Value);
    let cases: [(&str, Mutator); 3] = [
        ("effort", |request| {
            request["output_config"] = json!({"effort": "low"});
        }),
        ("thinking", |request| {
            request["thinking"] = json!({"type": "disabled"});
        }),
        ("tool_choice", |request| {
            request["tool_choice"] = json!({"type": "any"});
        }),
    ];

    for (case, mutate) in cases {
        let owner_id = seeded_uuid(1);
        let quota_peer_id = seeded_uuid(2);
        let thread_id = format!("ses-invalidator-{case}");
        let base = scoped_invalidator_request(case);
        let base_body = request_body(&base);
        let base_breakpoints = parse_request_cache_breakpoints(&HeaderMap::new(), &base_body);
        let base_tools = breakpoint_for_source(&base_breakpoints, CacheBreakpointSource::Tools);
        let base_message = breakpoint_for_source(&base_breakpoints, CacheBreakpointSource::Message);

        let mut variant = base.clone();
        mutate(&mut variant);
        assert_ne!(
            variant, base,
            "case={case}: mutator must change the request"
        );
        let variant_body = request_body(&variant);
        let variant_breakpoints = parse_request_cache_breakpoints(&HeaderMap::new(), &variant_body);

        let prompt_cache = TestPromptCacheObservationCache::new(HashMap::from([(
            owner_id,
            vec![warm_entry_for(base_tools), warm_entry_for(base_message)],
        )]));
        let view = test_view(prompt_cache, owner_id, quota_peer_id);
        let clock = cc_lb_engine::TestClock::new_at_secs(TEST_QUOTA_NOW_SECS);
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &variant_breakpoints,
            Some(&thread_id),
            &clock,
        );
        let owner = candidate(&candidates, owner_id, "owner");
        let score = owner.cache_score.as_ref().expect("owner cache score");

        assert_eq!(
            score.matched_v3_cache_key.as_deref(),
            Some(base_tools.prefix_hash.as_str()),
            "case={case}: stable tools prefix remains reusable"
        );
        assert_ne!(
            score.matched_v3_cache_key.as_deref(),
            Some(base_message.prefix_hash.as_str()),
            "case={case}: invalidated message prefix must not be reused"
        );
        assert_eq!(
            score.matched_content_block_index,
            Some(base_tools.block_index),
            "case={case}"
        );
        assert_eq!(
            u64::from(score.predicted_cache_read_tokens),
            base_tools.prefix_token_count,
            "case={case}"
        );

        let output = SubscriptionPreferenceFilter::new()
            .filter(
                &ctx(variant_body, variant_breakpoints, &thread_id),
                &plugin_principal(),
                &candidates,
            )
            .expect("subscription preference filter succeeds");
        assert_eq!(
            output.kept_upstream_ids,
            vec![owner_id],
            "case={case}: stable tools-tier affinity must keep the warm owner"
        );
    }
}

fn request_body(request: &Value) -> Bytes {
    Bytes::from(serde_json::to_vec(request).expect("prompt-cache request serializes"))
}

fn scoped_invalidator_request(marker: &str) -> Value {
    json!({
        "model": TEST_MODEL,
        "max_tokens": 64,
        "tools": [{
            "name": "lookup",
            "description": format!("{marker} stable oversized tool description ").repeat(400),
            "input_schema": {"type": "object"},
            "cache_control": {"type": "ephemeral"}
        }],
        "messages": [{"role": "user", "content": [{
            "type": "text",
            "text": format!("{marker} stable message prefix ").repeat(900),
            "cache_control": {"type": "ephemeral"}
        }]}]
    })
}

fn breakpoint_for_source(
    breakpoints: &[CacheBreakpoint],
    source: CacheBreakpointSource,
) -> &CacheBreakpoint {
    breakpoints
        .iter()
        .find(|breakpoint| breakpoint.source == source)
        .unwrap_or_else(|| panic!("prompt-cache fixture contains a {source:?} breakpoint"))
}

fn warm_entry_for(breakpoint: &CacheBreakpoint) -> WarmCacheEntry {
    WarmCacheEntry {
        prefix_hash: breakpoint.prefix_hash.clone(),
        expires_at_unix_secs: TEST_QUOTA_NOW_SECS + 300,
        ttl_class: breakpoint.requested_ttl,
        last_observed_at_unix_secs: TEST_QUOTA_NOW_SECS,
        content_block_index: breakpoint.block_index,
        estimated_prefix_tokens: breakpoint.prefix_token_count,
        token_estimate_source: breakpoint
            .token_estimate_source
            .clone()
            .expect("parsed breakpoint carries token estimate source"),
        hash_schema_version: HASH_SCHEMA_VERSION,
    }
}

fn ctx(
    body_bytes: Bytes,
    _cache_breakpoints: Vec<cc_lb_domain::CacheBreakpoint>,
    thread_id: &str,
) -> RoutingContext {
    RoutingContext {
        request_id: "req-1".to_owned(),
        thread_id: Some(thread_id.to_owned()),
        requested_service_tier: None,
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

fn trace_row<'a>(
    rows: &'a [cc_lb_domain::CandidateUrgency],
    upstream_id: Uuid,
    label: &str,
) -> &'a cc_lb_domain::CandidateUrgency {
    rows.iter()
        .find(|row| row.upstream_id == upstream_id)
        .unwrap_or_else(|| panic!("{label} urgency present"))
}
