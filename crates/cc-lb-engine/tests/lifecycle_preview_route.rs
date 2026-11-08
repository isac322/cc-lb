use crate::common;
use crate::prompt_cache_routing_support;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_clock::TestClock;
use cc_lb_control::api_keys::principal_view::{DialectCache, PrincipalView, RouterPipelineCache};
use cc_lb_control::{
    ApplyStatus, DynamicViewBuilder, DynamicViewHolder, UpstreamStatusEntry, UpstreamStatusSnapshot,
};
use cc_lb_domain::{Principal, TerminalStrategy, TtlClass, UpstreamCandidate, WarmCacheEntry};
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_engine::lifecycle::{PreviewRouteError, PreviewRouteInput};
use cc_lb_engine::prompt_cache_simulator::{V3PromptCacheBreakpoint, analyze_v3_prompt_cache};
use cc_lb_engine::{Lifecycle, LifecycleConfig};
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use http::HeaderMap;
use prompt_cache_routing_support::{
    TestPromptCacheObservationCache, TestSubscriptionQuotaCache, known_base_quota_snapshots,
};
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use common::{DispatchMode, MockDispatch, TestAuthn, TestState};

const PRINCIPAL: &str = "principal-test";

#[tokio::test]
async fn happy_path_returns_trace_with_stages_and_winner() {
    let (lifecycle, upstream_ids) = build_lifecycle(vec![kept_filter("keep-first", vec![0])]);

    let outcome = lifecycle
        .preview_route(input(PRINCIPAL, None))
        .await
        .expect("preview succeeds");

    assert_eq!(outcome.trace.stages.len(), 1);
    assert_eq!(outcome.trace.stages[0].stage_name, "keep-first");
    assert_eq!(
        outcome
            .trace
            .terminal_decision
            .as_ref()
            .unwrap()
            .upstream_id,
        Some(upstream_ids[0])
    );
    assert_eq!(outcome.winner_upstream_id, Some(upstream_ids[0]));
    assert_eq!(outcome.winner_upstream_name.as_deref(), Some("upstream-0"));
}

#[tokio::test]
async fn preview_route_matches_dispatch_capability_and_gemini_path_model() {
    let cases = [
        (
            cc_lb_domain::Provider::Anthropic,
            "/v1/messages",
            br#"{"model":"claude-test","messages":[]}"# as &'static [u8],
            StorageUpstreamKind::AnthropicApiKey,
            "anthropic-preview",
        ),
        (
            cc_lb_domain::Provider::Openai,
            "/v1/responses",
            br#"{"model":"gpt-5.2","input":[]}"# as &'static [u8],
            StorageUpstreamKind::OpenaiApiKey,
            "openai-preview",
        ),
        (
            cc_lb_domain::Provider::Gemini,
            "/v1beta/models/gemini-3.1-pro-preview:streamGenerateContent",
            br#"{"contents":[]}"# as &'static [u8],
            StorageUpstreamKind::GeminiApiKey,
            "gemini-preview",
        ),
    ];

    for (provider, path, body, upstream_kind, filter_name) in cases {
        let filter = kept_filter(filter_name, vec![0]);
        let mut upstream = upstream_record(0);
        upstream.kind = upstream_kind;
        let lifecycle = build_lifecycle_with_status_snapshot(
            principal_view_with_pipeline(vec![filter]),
            vec![upstream.clone()],
            UpstreamStatusSnapshot::default(),
        );
        let outcome = lifecycle
            .preview_route(PreviewRouteInput {
                principal_id: PRINCIPAL.to_owned(),
                request_id: Some(format!("preview-{provider:?}")),
                headers: HeaderMap::new(),
                body_bytes: Bytes::from_static(body),
                provider: Some(provider),
                path: path.to_owned(),
            })
            .await
            .expect("provider preview succeeds");

        assert_eq!(
            outcome.winner_upstream_id,
            Some(upstream.id),
            "{provider:?}"
        );
        assert_eq!(
            outcome.trace.stages[0].stage_name, filter_name,
            "{provider:?} filter pipeline selected"
        );
    }
}

#[tokio::test]
async fn preview_route_rejects_unclassified_operation_instead_of_anthropic_fallback() {
    let (lifecycle, _) = build_lifecycle(vec![kept_filter("keep-all", vec![0])]);
    let error = lifecycle
        .preview_route(PreviewRouteInput {
            principal_id: PRINCIPAL.to_owned(),
            request_id: Some("unsupported-preview".to_owned()),
            headers: HeaderMap::new(),
            body_bytes: Bytes::from_static(br#"{"model":"gpt-5.2","input":[]}"#),
            provider: Some(cc_lb_domain::Provider::Openai),
            path: "/v1/files".to_owned(),
        })
        .await
        .expect_err("unsupported operation must fail closed");

    assert!(matches!(
        error,
        PreviewRouteError::UnsupportedOperation { method, path }
            if method == "POST" && path == "/v1/files"
    ));
}

#[tokio::test]
async fn preview_route_uses_path_model_as_canonical_gemini_model() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let filter = Arc::new(ContextRecordingFilter {
        calls: calls.clone(),
    });
    let mut upstream = upstream_record(0);
    upstream.kind = StorageUpstreamKind::GeminiApiKey;
    let lifecycle = build_lifecycle_with_status_snapshot(
        principal_view_with_pipeline(vec![filter]),
        vec![upstream],
        UpstreamStatusSnapshot::default(),
    );

    lifecycle
        .preview_route(PreviewRouteInput {
            principal_id: PRINCIPAL.to_owned(),
            request_id: Some("gemini-path-model".to_owned()),
            headers: HeaderMap::new(),
            body_bytes: Bytes::from_static(br#"{"contents":[]}"#),
            provider: Some(cc_lb_domain::Provider::Gemini),
            path: "/v1beta/models/gemini-3.1-pro-preview:generateContent".to_owned(),
        })
        .await
        .expect("Gemini preview succeeds");

    assert_eq!(
        calls.lock().unwrap().as_slice(),
        &[(
            "/v1beta/models/gemini-3.1-pro-preview:generateContent".to_owned(),
            "gemini-3.1-pro-preview".to_owned(),
        )]
    );
}
#[tokio::test]
async fn unknown_principal_returns_error() {
    let (lifecycle, _) = build_lifecycle(vec![kept_filter("keep-first", vec![0])]);

    let err = lifecycle
        .preview_route(input("nonexistent-principal", None))
        .await
        .expect_err("must fail");

    match err {
        PreviewRouteError::PrincipalNotFound(id) => assert_eq!(id, "nonexistent-principal"),
        other => panic!("expected PrincipalNotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn same_request_id_produces_deterministic_winner() {
    let (lifecycle, _) = build_lifecycle(vec![kept_filter("keep-all", vec![0, 1, 2])]);

    let first = lifecycle
        .preview_route(input(PRINCIPAL, Some("fixed-req-id")))
        .await
        .expect("first preview succeeds");
    let second = lifecycle
        .preview_route(input(PRINCIPAL, Some("fixed-req-id")))
        .await
        .expect("second preview succeeds");

    assert_eq!(first.winner_upstream_id, second.winner_upstream_id);
    assert_eq!(first.winner_upstream_name, second.winner_upstream_name);
}

#[tokio::test]
async fn pipeline_instantiation_error_returns_error() {
    let upstream_records = default_upstream_records();
    let principal_view = principal_view_with_broken_pipeline();
    let lifecycle = build_lifecycle_with_status_snapshot(
        principal_view,
        upstream_records,
        UpstreamStatusSnapshot::default(),
    );

    let err = lifecycle
        .preview_route(input(PRINCIPAL, None))
        .await
        .expect_err("must fail");

    match err {
        PreviewRouteError::PipelineInstantiationError(detail) => {
            assert!(
                detail.contains("broken pipeline fixture"),
                "detail was: {detail}"
            );
        }
        other => panic!("expected PipelineInstantiationError, got {other:?}"),
    }
}

#[tokio::test]
async fn empty_candidate_pool_returns_null_winner() {
    let (lifecycle, _) = build_lifecycle(vec![kept_filter("drop-all", vec![])]);

    let outcome = lifecycle
        .preview_route(input(PRINCIPAL, None))
        .await
        .expect("preview succeeds");

    assert!(outcome.winner_upstream_id.is_none());
    assert!(outcome.winner_upstream_name.is_none());
    assert_eq!(
        outcome
            .trace
            .terminal_decision
            .as_ref()
            .unwrap()
            .upstream_id,
        None
    );
}

// #851: an upstream whose last apply errored cannot sign, so preview routing
// must skip it for a healthy sibling; when every upstream errored there is
// no winner.
#[tokio::test]
async fn preview_route_skips_apply_error_upstreams() {
    let upstream_records = default_upstream_records();
    let errored = upstream_records[0].id;
    let healthy = upstream_records[1].id;
    let third = upstream_records[2].id;

    // The errored upstream sorts first by id yet must be skipped.
    let filter = Arc::new(IndexKeepingFilter {
        name: "keep-all",
        kept_indices: vec![0, 1, 2],
        calls: Arc::new(Mutex::new(Vec::new())),
    });
    let mut snapshot = status_snapshot([(errored, ApplyStatus::Error)]);
    snapshot.entries.insert(
        upstream_records[1].name.clone(),
        UpstreamStatusEntry {
            status: ApplyStatus::Active,
            last_apply_error: None,
            last_apply_at_unix_secs: 1,
        },
    );
    let lifecycle = build_lifecycle_with_status_snapshot(
        principal_view_with_pipeline(vec![filter.clone()]),
        upstream_records.clone(),
        snapshot,
    );

    let outcome = lifecycle
        .preview_route(input(PRINCIPAL, Some("apply-error-skip")))
        .await
        .expect("preview succeeds");

    assert_eq!(outcome.winner_upstream_id, Some(healthy));
    assert_eq!(outcome.winner_upstream_name.as_deref(), Some("upstream-1"));
    let seen = filter.calls.lock().unwrap().clone();
    assert_eq!(seen.as_slice(), &[vec![healthy, third]]);
    assert!(
        outcome
            .trace
            .stages
            .iter()
            .all(|stage| stage.upstream_id != Some(errored)),
        "trace stages must not reference the errored upstream: {:?}",
        outcome.trace.stages
    );

    // All errored: no upstream can sign, so the preview has no winner.
    let lifecycle = build_lifecycle_with_status_snapshot(
        principal_view_with_pipeline(vec![kept_filter("keep-all", vec![0, 1, 2])]),
        upstream_records.clone(),
        status_snapshot([
            (errored, ApplyStatus::Error),
            (healthy, ApplyStatus::Error),
            (third, ApplyStatus::Error),
        ]),
    );

    let outcome = lifecycle
        .preview_route(input(PRINCIPAL, Some("apply-error-empty")))
        .await
        .expect("preview succeeds");

    assert!(outcome.winner_upstream_id.is_none());
    assert!(outcome.winner_upstream_name.is_none());
    assert_eq!(
        outcome
            .trace
            .terminal_decision
            .as_ref()
            .unwrap()
            .upstream_id,
        None
    );
}

#[tokio::test]
async fn preview_route_uses_endpoint_specific_billing_first_cache_keys() {
    const NOW_SECS: u64 = 1_700_000_000;
    let official_id = Uuid::from_u128(1);
    let custom_id = Uuid::from_u128(2);
    let normal_body = billing_first_body("normal");
    let cron_body = billing_first_body("cron");
    let normal_keys = payload_cache_keys(&normal_body);
    let cron_keys = payload_cache_keys(&cron_body);

    // The official endpoint ignores only the billing attribution block, while
    // a literal endpoint includes it in the prefix identity.
    assert_eq!(normal_keys.official, cron_keys.official);
    assert_ne!(normal_keys.literal, cron_keys.literal);

    let warm_entries = HashMap::from([
        (
            official_id,
            vec![warm_entry(&normal_keys.official_breakpoint, NOW_SECS)],
        ),
        (
            custom_id,
            vec![
                warm_entry(&normal_keys.literal_breakpoint, NOW_SECS),
                // Negative control: a custom endpoint must not accept the
                // official key even when that observation exists for it.
                warm_entry(&normal_keys.official_breakpoint, NOW_SECS),
            ],
        ),
    ]);
    let upstream_records = vec![
        preview_upstream(
            official_id,
            "official",
            Some(parse_url("https://api.anthropic.com")),
        ),
        preview_upstream(
            custom_id,
            "custom",
            Some(parse_url("https://proxy.example.com")),
        ),
    ];
    let lifecycle = build_prompt_cache_lifecycle(
        upstream_records.clone(),
        warm_entries,
        official_id,
        custom_id,
    );

    let normal = lifecycle
        .preview_route(preview_input(normal_body.clone(), "preview-billing-normal"))
        .await
        .expect("normal preview succeeds");
    let normal_trace = subscription_trace(&normal);
    let normal_official = trace_candidate(normal_trace, official_id);
    let normal_custom = trace_candidate(normal_trace, custom_id);
    assert_eq!(normal.winner_upstream_id, Some(official_id));
    assert_eq!(terminal_upstream(&normal), Some(official_id));
    assert_eq!(
        normal_official.matched_v3_cache_key.as_deref(),
        Some(normal_keys.official.as_str())
    );
    assert_eq!(
        normal_custom.matched_v3_cache_key.as_deref(),
        Some(normal_keys.literal.as_str())
    );
    assert!(normal_official.predicted_cache_read_tokens > 0);
    assert!(normal_custom.predicted_cache_read_tokens > 0);
    assert_eq!(normal_trace.formula_winner_upstream_id, Some(official_id));

    // Mutating only the billing attribution must preserve the official warm
    // owner, but the custom endpoint must miss rather than inherit that key.
    let cron = lifecycle
        .preview_route(preview_input(cron_body, "preview-billing-cron"))
        .await
        .expect("cron preview succeeds");
    let cron_trace = subscription_trace(&cron);
    let cron_official = trace_candidate(cron_trace, official_id);
    let cron_custom = trace_candidate(cron_trace, custom_id);
    assert_eq!(cron.winner_upstream_id, Some(official_id));
    assert_eq!(terminal_upstream(&cron), Some(official_id));
    assert_eq!(
        cron_official.matched_v3_cache_key.as_deref(),
        Some(cron_keys.official.as_str())
    );
    assert_eq!(cron_keys.official, normal_keys.official);
    assert_eq!(cron_custom.matched_v3_cache_key, None);
    assert_eq!(cron_custom.predicted_cache_read_tokens, 0);
    assert_eq!(cron_trace.formula_winner_upstream_id, Some(official_id));
    assert_eq!(
        cron_trace.bucket_v3_cache_key.as_deref(),
        Some(cron_keys.official.as_str())
    );

    // The explicit official authority follows the same policy as no base URL.
    let no_base_url = build_prompt_cache_lifecycle(
        upstream_records
            .into_iter()
            .map(|mut record| {
                if record.id == official_id {
                    record.base_url = None;
                }
                record
            })
            .collect(),
        HashMap::from([
            (
                official_id,
                vec![warm_entry(&normal_keys.official_breakpoint, NOW_SECS)],
            ),
            (
                custom_id,
                vec![
                    warm_entry(&normal_keys.literal_breakpoint, NOW_SECS),
                    warm_entry(&normal_keys.official_breakpoint, NOW_SECS),
                ],
            ),
        ]),
        official_id,
        custom_id,
    );
    let without_base_url = no_base_url
        .preview_route(preview_input(normal_body, "preview-billing-no-base-url"))
        .await
        .expect("no-base-url preview succeeds");
    let without_base_url_trace = subscription_trace(&without_base_url);
    assert_eq!(
        without_base_url.winner_upstream_id,
        normal.winner_upstream_id
    );
    assert_eq!(
        trace_candidate(without_base_url_trace, official_id)
            .matched_v3_cache_key
            .as_deref(),
        Some(normal_keys.official.as_str())
    );
}

fn input(principal_id: &str, request_id: Option<&str>) -> PreviewRouteInput {
    PreviewRouteInput {
        principal_id: principal_id.to_owned(),
        request_id: request_id.map(str::to_owned),
        headers: HeaderMap::new(),
        body_bytes: Bytes::from_static(br#"{"model":"claude-test","messages":[]}"#),
        provider: Some(cc_lb_domain::Provider::Anthropic),
        path: "/v1/messages".to_owned(),
    }
}

struct PayloadCacheKeys {
    official: String,
    literal: String,
    official_breakpoint: V3PromptCacheBreakpoint,
    literal_breakpoint: V3PromptCacheBreakpoint,
}

fn billing_first_body(attribution: &str) -> Bytes {
    let body = serde_json::json!({
        "model": "claude-sonnet-4-5",
        "system": [
            {
                "type": "text",
                "text": format!("x-anthropic-billing-header: {attribution}")
            },
            {
                "type": "text",
                "text": "stable system context ".repeat(400),
                "cache_control": {"type": "ephemeral"}
            }
        ],
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 16
    });
    let bytes = match serde_json::to_vec(&body) {
        Ok(bytes) => bytes,
        Err(error) => panic!("billing-first body serializes: {error}"),
    };
    Bytes::from(bytes)
}

fn payload_cache_keys(body: &Bytes) -> PayloadCacheKeys {
    let value: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(error) => panic!("billing-first body parses: {error}"),
    };
    let analysis = analyze_v3_prompt_cache(
        &value,
        cc_lb_engine::model_resolution::canonical_model_id(
            value.get("model").and_then(Value::as_str).unwrap_or(""),
        ),
    );
    let Some(official_breakpoint) = analysis.breakpoints.first().cloned() else {
        panic!("official payload breakpoint present");
    };
    let Some(literal_breakpoint) = analysis.literal_breakpoints.first().cloned() else {
        panic!("literal payload breakpoint present");
    };
    let Some(official_prefix) = official_breakpoint.lookback_prefixes.first() else {
        panic!("official lookback key present");
    };
    let Some(literal_prefix) = literal_breakpoint.lookback_prefixes.first() else {
        panic!("literal lookback key present");
    };
    PayloadCacheKeys {
        official: official_prefix.prefix_key.clone(),
        literal: literal_prefix.prefix_key.clone(),
        official_breakpoint,
        literal_breakpoint,
    }
}

fn warm_entry(breakpoint: &V3PromptCacheBreakpoint, now_secs: u64) -> WarmCacheEntry {
    let Some(prefix) = breakpoint.lookback_prefixes.first() else {
        panic!("warm breakpoint has a lookback key");
    };
    let ttl_class = match breakpoint.ttl.as_deref() {
        Some("1h") => TtlClass::Ephemeral1h,
        _ => TtlClass::Ephemeral5m,
    };
    WarmCacheEntry {
        prefix_hash: prefix.prefix_key.clone(),
        expires_at_unix_secs: now_secs + 300,
        ttl_class,
        last_observed_at_unix_secs: now_secs,
        content_block_index: prefix.content_block_index as u32,
        estimated_prefix_bytes: breakpoint.prefix_bytes,
        hash_schema_version: cc_lb_engine::lifecycle::HASH_SCHEMA_VERSION,
    }
}

fn preview_input(body_bytes: Bytes, request_id: &str) -> PreviewRouteInput {
    PreviewRouteInput {
        principal_id: PRINCIPAL.to_owned(),
        request_id: Some(request_id.to_owned()),
        headers: HeaderMap::new(),
        body_bytes,
        provider: Some(cc_lb_domain::Provider::Anthropic),
        path: "/v1/messages".to_owned(),
    }
}

fn preview_upstream(id: Uuid, name: &str, base_url: Option<Url>) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: name.to_owned(),
        kind: StorageUpstreamKind::AnthropicOauth,
        base_url,
        enabled: true,
        revision: 1,
        ..UpstreamRecord::default()
    }
}

fn parse_url(value: &str) -> Url {
    match Url::parse(value) {
        Ok(url) => url,
        Err(error) => panic!("test URL parses: {error}"),
    }
}

fn build_prompt_cache_lifecycle(
    upstream_records: Vec<UpstreamRecord>,
    warm_entries: HashMap<Uuid, Vec<WarmCacheEntry>>,
    official_id: Uuid,
    custom_id: Uuid,
) -> Lifecycle {
    let principal_view =
        principal_view_with_pipeline(vec![Arc::new(SubscriptionPreferenceFilter::new())]);
    let state = TestState::default();
    let authn = TestAuthn::with_principal_view(state.clone(), principal_view.clone());
    let dispatcher: Arc<dyn cc_lb_engine::UpstreamDispatch> = Arc::new(MockDispatch {
        state,
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![http::StatusCode::OK].into()))),
    });
    let prompt_cache = TestPromptCacheObservationCache::new(warm_entries);
    let quota_cache = TestSubscriptionQuotaCache::new(HashMap::from([
        (official_id, known_base_quota_snapshots(0.25)),
        (custom_id, known_base_quota_snapshots(0.0)),
    ]));
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(principal_view)
        .upstream_records(upstream_records)
        .prompt_cache_observation_store(Arc::new(prompt_cache))
        .subscription_quota_cache(Arc::new(quota_cache))
        .subscription_quota_routing_max_staleness_secs(60)
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(1_700_000_000)),
    )
}

fn subscription_trace(
    outcome: &cc_lb_engine::lifecycle::PreviewRouteOutcome,
) -> &cc_lb_domain::SubscriptionPreferenceTrace {
    outcome
        .trace
        .stages
        .iter()
        .find_map(|stage| stage.subscription_preference.as_ref())
        .unwrap_or_else(|| panic!("subscription preference trace present"))
}

fn trace_candidate(
    trace: &cc_lb_domain::SubscriptionPreferenceTrace,
    upstream_id: Uuid,
) -> &cc_lb_domain::CandidateUrgency {
    trace
        .candidates
        .iter()
        .find(|candidate| candidate.upstream_id == upstream_id)
        .unwrap_or_else(|| panic!("candidate {upstream_id} appears in trace"))
}

fn terminal_upstream(outcome: &cc_lb_engine::lifecycle::PreviewRouteOutcome) -> Option<Uuid> {
    outcome
        .trace
        .terminal_decision
        .as_ref()
        .and_then(|decision| decision.upstream_id)
}

fn kept_filter(name: &'static str, kept_indices: Vec<usize>) -> Arc<dyn FilterPlugin> {
    Arc::new(IndexKeepingFilter {
        name,
        kept_indices,
        calls: Arc::new(Mutex::new(Vec::new())),
    })
}

fn build_lifecycle(filters: Vec<Arc<dyn FilterPlugin>>) -> (Lifecycle, Vec<Uuid>) {
    let upstream_records = default_upstream_records();
    let upstream_ids: Vec<Uuid> = upstream_records.iter().map(|u| u.id).collect();
    let principal_view = principal_view_with_pipeline(filters);
    let lifecycle = build_lifecycle_with_status_snapshot(
        principal_view,
        upstream_records,
        UpstreamStatusSnapshot::default(),
    );
    (lifecycle, upstream_ids)
}

fn build_lifecycle_with_status_snapshot(
    principal_view: Arc<PrincipalView>,
    upstream_records: Vec<UpstreamRecord>,
    upstream_status_snapshot: UpstreamStatusSnapshot,
) -> Lifecycle {
    let state = TestState::default();
    let authn = TestAuthn::with_principal_view(state.clone(), principal_view.clone());
    let dispatcher: Arc<dyn cc_lb_engine::UpstreamDispatch> = Arc::new(MockDispatch {
        state,
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![http::StatusCode::OK].into()))),
    });
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(principal_view)
        .upstream_records(upstream_records)
        .upstream_status_snapshot(Arc::new(upstream_status_snapshot))
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
}

fn status_snapshot(
    statuses: impl IntoIterator<Item = (Uuid, ApplyStatus)>,
) -> UpstreamStatusSnapshot {
    let upstream_names: HashMap<Uuid, String> = default_upstream_records()
        .into_iter()
        .map(|record| (record.id, record.name))
        .collect();
    let entries = statuses
        .into_iter()
        .map(|(id, status)| {
            (
                upstream_names[&id].clone(),
                UpstreamStatusEntry {
                    status,
                    last_apply_error: (status == ApplyStatus::Error)
                        .then(|| "apply failed".to_owned()),
                    last_apply_at_unix_secs: 1,
                },
            )
        })
        .collect();
    UpstreamStatusSnapshot {
        entries,
        applied_at_unix_secs: 1,
        revision_hash: 1,
    }
}

fn principal_view_with_pipeline(filters: Vec<Arc<dyn FilterPlugin>>) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: filters,
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    });
    let mut chains = HashMap::new();
    chains.insert(
        PRINCIPAL.to_owned(),
        (Some(pipeline), DialectCache::Inherit),
    );
    Arc::new(PrincipalView::for_tests(
        PRINCIPAL,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

fn principal_view_with_broken_pipeline() -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: Vec::new(),
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: Some(Arc::from("broken pipeline fixture")),
        provider_filters: None,
    });
    let mut chains = HashMap::new();
    chains.insert(
        PRINCIPAL.to_owned(),
        (Some(pipeline), DialectCache::Inherit),
    );
    Arc::new(PrincipalView::for_tests(
        PRINCIPAL,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

struct ContextRecordingFilter {
    calls: Arc<Mutex<Vec<(String, String)>>>,
}

impl FilterPlugin for ContextRecordingFilter {
    fn filter(
        &self,
        ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.calls
            .lock()
            .unwrap()
            .push((ctx.path.clone(), ctx.canonical_model_id.clone()));
        Ok(FilterOutput {
            kept_upstream_ids: candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
            reason: "recorded".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::from_u128(2)
    }

    fn plugin_name(&self) -> &str {
        "context-recording"
    }
}

fn default_upstream_records() -> Vec<UpstreamRecord> {
    (0..3).map(upstream_record).collect()
}

fn upstream_record(index: u8) -> UpstreamRecord {
    let mut bytes = [0u8; 16];
    bytes[15] = index + 1;
    UpstreamRecord {
        id: Uuid::from_bytes(bytes),
        name: format!("upstream-{index}"),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        oauth_never_refresh: false,
        api_key_ciphertext: Some(Vec::new()),
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

struct IndexKeepingFilter {
    name: &'static str,
    kept_indices: Vec<usize>,
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
}

impl FilterPlugin for IndexKeepingFilter {
    fn filter(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.calls.lock().unwrap().push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        let kept_upstream_ids = self
            .kept_indices
            .iter()
            .filter_map(|index| {
                candidates
                    .get(*index)
                    .map(|candidate| candidate.upstream_id)
            })
            .collect::<Vec<_>>();
        let reason = format!("{} kept {}", self.name, kept_upstream_ids.len());
        Ok(FilterOutput {
            kept_upstream_ids,
            reason,
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        self.name
    }
}
