use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_plugin_api::{
    FilterError, FilterPlugin, Principal, PrincipalKind, RequestContext, UpstreamCandidate,
    UpstreamKind,
};
use http::{Method, StatusCode};
use uuid::Uuid;

use crate::common::{collect_body, messages_request};
use crate::router_lifecycle_support::{
    DropAllFilter, InvalidOutputFilter, InvalidOutputKind, KeepAllFilter, KeepFirstNFilter,
    RouterLifecycleState, RuntimeErrorFilter, TrappingFilter, api_key_record,
    invalid_unknown_upstream_id, lifecycle_with_records,
};

#[test]
fn keep_first_n_filter_keeps_candidate_prefix() {
    let candidates = candidates(5);
    let output = KeepFirstNFilter { n: 3 }
        .filter(&ctx(), &principal(), &candidates)
        .expect("keep-first-n succeeds");

    assert_eq!(
        output.kept_upstream_ids,
        vec![upstream_id(1), upstream_id(2), upstream_id(3)]
    );
    assert_eq!(output.per_candidate_reasons, Vec::new());
    assert!(output.reason.contains("kept first 3 of 5 candidates"));
}

#[test]
fn drop_all_and_keep_all_filters_return_expected_candidate_sets() {
    let candidates = candidates(3);
    let drop_all = DropAllFilter
        .filter(&ctx(), &principal(), &candidates)
        .expect("drop-all succeeds");
    let keep_all = KeepAllFilter
        .filter(&ctx(), &principal(), &candidates)
        .expect("keep-all succeeds");

    assert!(drop_all.kept_upstream_ids.is_empty());
    assert_eq!(
        keep_all.kept_upstream_ids,
        vec![upstream_id(1), upstream_id(2), upstream_id(3)]
    );
}

#[test]
fn trapping_and_runtime_filters_return_distinct_error_variants() {
    let candidates = candidates(1);
    let trap = TrappingFilter
        .filter(&ctx(), &principal(), &candidates)
        .expect_err("trapping filter returns trap error");
    let runtime = RuntimeErrorFilter
        .filter(&ctx(), &principal(), &candidates)
        .expect_err("runtime filter returns runtime error");

    assert!(matches!(trap, FilterError::Trap { .. }));
    assert!(matches!(runtime, FilterError::Runtime { .. }));
}

#[test]
fn invalid_output_filter_builds_unknown_duplicate_and_superset_outputs() {
    let candidates = candidates(2);
    let unknown = InvalidOutputFilter {
        kind: InvalidOutputKind::Unknown,
    }
    .filter(&ctx(), &principal(), &candidates)
    .expect("unknown invalid output succeeds");
    let duplicate = InvalidOutputFilter {
        kind: InvalidOutputKind::Duplicate,
    }
    .filter(&ctx(), &principal(), &candidates)
    .expect("duplicate invalid output succeeds");
    let superset = InvalidOutputFilter {
        kind: InvalidOutputKind::Superset,
    }
    .filter(&ctx(), &principal(), &candidates)
    .expect("superset invalid output succeeds");

    assert_eq!(
        unknown.kept_upstream_ids,
        vec![invalid_unknown_upstream_id()]
    );
    assert_eq!(
        duplicate.kept_upstream_ids,
        vec![upstream_id(1), upstream_id(1)]
    );
    assert_eq!(
        superset.kept_upstream_ids,
        vec![
            upstream_id(1),
            upstream_id(2),
            invalid_unknown_upstream_id()
        ]
    );
}

#[test]
fn mock_filter_plugin_metadata_is_unique() {
    let plugins: Vec<Box<dyn FilterPlugin>> = vec![
        Box::new(KeepFirstNFilter { n: 1 }),
        Box::new(DropAllFilter),
        Box::new(KeepAllFilter),
        Box::new(TrappingFilter),
        Box::new(RuntimeErrorFilter),
        Box::new(InvalidOutputFilter {
            kind: InvalidOutputKind::Unknown,
        }),
        Box::new(InvalidOutputFilter {
            kind: InvalidOutputKind::Duplicate,
        }),
        Box::new(InvalidOutputFilter {
            kind: InvalidOutputKind::Superset,
        }),
    ];
    let ids = plugins
        .iter()
        .map(|plugin| plugin.plugin_id())
        .collect::<HashSet<_>>();
    let names = plugins
        .iter()
        .map(|plugin| plugin.plugin_name().to_owned())
        .collect::<HashSet<_>>();

    assert_eq!(ids.len(), plugins.len());
    assert_eq!(names.len(), plugins.len());
}

#[tokio::test]
async fn lifecycle_helper_runs_filter_pipeline_and_dispatches_first_survivor() {
    let first = upstream_id(1);
    let second = upstream_id(2);
    let state = RouterLifecycleState::default();
    let lifecycle = lifecycle_with_records(
        vec![
            api_key_record(first, "first", "http://first.local/"),
            api_key_record(second, "second", "http://second.local/"),
        ],
        vec![Arc::new(KeepFirstNFilter { n: 1 })],
        state.clone(),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        state
            .router_candidates
            .lock()
            .expect("router candidates lock")
            .as_slice(),
        &[vec![first]]
    );
    assert_eq!(
        state
            .router_choice_names
            .lock()
            .expect("router choice names lock")
            .as_slice(),
        &["first".to_owned()]
    );
    assert_eq!(
        state
            .dispatched_urls
            .lock()
            .expect("dispatched URLs lock")
            .as_slice(),
        &["https://api.anthropic.com/v1/messages".to_owned()]
    );
}

fn ctx() -> RequestContext {
    RequestContext {
        request_id: "req-router-lifecycle-support".to_owned(),
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: "claude-test".to_owned(),
    }
}

fn principal() -> Principal {
    Principal {
        id: "principal-test".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    }
}

fn candidates(count: u128) -> Vec<UpstreamCandidate> {
    (1..=count)
        .map(|index| UpstreamCandidate {
            upstream_id: upstream_id(index),
            name: format!("upstream-{index}"),
            kind: UpstreamKind::AnthropicApiKey,
            observed_rate_limits: Vec::new(),
            subscription_quotas: Vec::new(),
            observed_at_unix_secs: 0,
            cache_score: None,
            base_url: None,
        })
        .collect()
}

fn upstream_id(index: u128) -> Uuid {
    Uuid::from_u128(index)
}
