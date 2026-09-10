use std::{future::Future, sync::Arc};

use anyhow::Result;
use cc_lb_storage_api::{
    RequestEventListItem, RequestEventListQuery, RequestEventStore, RequestEventStreamFilters,
    StatusClass,
    types::{RequestEvent, RequestEventUpstream},
};
use serde_json::json;
use uuid::Uuid;

use crate::harness::{ConformanceBackend, ConformanceFixture};

pub async fn request_event_list_projects_rows_and_preserves_detail<B: ConformanceBackend>(
    backend: Arc<B>,
) -> Result<()> {
    with_fixture(backend, |storage| async move {
        let upstream_id = Uuid::from_u128(7);
        let other_upstream_id = Uuid::from_u128(8);

        let mut matching =
            request_event("event-b", "req-b", 2_000, 200, "principal-a", upstream_id);
        matching.iterations = Some(json!({"not_in_list_projection": "x".repeat(1_000_000)}));

        let legacy = RequestEvent {
            ts: 2,
            request_id: "req-a".to_owned(),
            event_id: Some("event-a".to_owned()),
            principal_id: Some("principal-a".to_owned()),
            upstream: Some(RequestEventUpstream::AnthropicDirect),
            upstream_id: Some(upstream_id),
            upstream_name: Some("upstream-a".to_owned()),
            thread_id: Some("thread-a".to_owned()),
            model: Some("claude-sonnet-4-5".to_owned()),
            status: 204,
            ..Default::default()
        };

        let mut renewal = request_event("event-c", "req-c", 3_000, 202, "principal-a", upstream_id);
        renewal.source_kind = Some("renewal".to_owned());

        let mut wrong_principal = decoy(&matching, "decoy-principal", 9_600);
        wrong_principal.principal_id = Some("principal-b".to_owned());
        let mut wrong_model = decoy(&matching, "decoy-model", 9_500);
        wrong_model.model = Some("claude-opus-4-1".to_owned());
        let mut wrong_upstream_id = decoy(&matching, "decoy-upstream-id", 9_400);
        wrong_upstream_id.upstream_id = Some(other_upstream_id);
        let mut wrong_thread = decoy(&matching, "decoy-thread", 9_300);
        wrong_thread.thread_id = Some("thread-b".to_owned());
        let mut wrong_upstream = decoy(&matching, "decoy-upstream", 9_200);
        wrong_upstream.upstream = None;
        let mut wrong_status = decoy(&matching, "decoy-status", 9_100);
        wrong_status.status = 500;

        for event in [
            &legacy,
            &matching,
            &renewal,
            &wrong_principal,
            &wrong_model,
            &wrong_upstream_id,
            &wrong_thread,
            &wrong_upstream,
            &wrong_status,
        ] {
            storage.append_request_event(event).await?;
        }

        let filters = RequestEventStreamFilters {
            principal_id: Some("principal-a".to_owned()),
            thread_id: Some("thread-a".to_owned()),
            model: Some("claude-sonnet-4-5".to_owned()),
            upstream: Some(RequestEventUpstream::AnthropicDirect),
            upstream_id: Some(upstream_id),
            status_class: Some(StatusClass::TwoXx),
        };

        let default_page = storage
            .list_request_events(&list_query(filters.clone(), None, 10, None, None))
            .await?;
        assert_event_ids(&default_page, &["event-b", "event-a"]);
        assert_list_matches_details(storage.as_ref(), &default_page).await?;

        let first_page = storage
            .list_request_events(&list_query(filters.clone(), Some("all"), 2, None, None))
            .await?;
        assert_event_ids(&first_page, &["event-c", "event-b"]);
        assert_list_matches_details(storage.as_ref(), &first_page).await?;

        let cursor = first_page.last().expect("first page cursor row");
        let second_page = storage
            .list_request_events(&list_query(
                filters.clone(),
                Some("all"),
                2,
                Some(
                    cursor
                        .ts_ms
                        .unwrap_or_else(|| cursor.ts.saturating_mul(1_000)),
                ),
                Some(
                    cursor
                        .event_id
                        .clone()
                        .unwrap_or_else(|| cursor.request_id.clone()),
                ),
            ))
            .await?;
        assert_event_ids(&second_page, &["event-a"]);
        assert_list_matches_details(storage.as_ref(), &second_page).await?;

        let renewal_page = storage
            .list_request_events(&list_query(
                filters.clone(),
                Some("renewal"),
                10,
                None,
                None,
            ))
            .await?;
        assert_event_ids(&renewal_page, &["event-c"]);
        assert_list_matches_details(storage.as_ref(), &renewal_page).await?;

        let proxy_page = storage
            .list_request_events(&list_query(filters, Some("proxy"), 10, None, None))
            .await?;
        assert_event_ids(&proxy_page, &["event-b"]);
        assert_list_matches_details(storage.as_ref(), &proxy_page).await?;

        let detail = storage
            .get_request_event(matching.event_id.as_deref().unwrap_or_default())
            .await?
            .expect("stored request event detail");
        assert_eq!(serde_json::to_vec(&detail)?, serde_json::to_vec(&matching)?);

        Ok(())
    })
    .await
}

pub async fn request_event_list_model_filter_matches_case_insensitive_prefix<
    B: ConformanceBackend,
>(
    backend: Arc<B>,
) -> Result<()> {
    with_fixture(backend, |storage| async move {
        let upstream_id = Uuid::from_u128(7);
        let base = request_event(
            "event-base",
            "req-base",
            5_000,
            200,
            "principal-a",
            upstream_id,
        );

        let mut dated = decoy(&base, "event-dated", 4_000);
        dated.model = Some("claude-sonnet-4-5-20250929".to_owned());
        let mut upper = decoy(&base, "event-upper", 3_000);
        upper.model = Some("CLAUDE-SONNET-4-5-PREVIEW".to_owned());
        let mut other_family = decoy(&base, "event-opus", 2_000);
        other_family.model = Some("claude-opus-4-1".to_owned());
        let mut wildcard = decoy(&base, "event-wildcard", 1_000);
        wildcard.model = Some("claude%sonnet".to_owned());
        let mut model_less = decoy(&base, "event-model-less", 500);
        model_less.model = None;

        for event in [&base, &dated, &upper, &other_family, &wildcard, &model_less] {
            storage.append_request_event(event).await?;
        }

        let by_model = |model: &str| RequestEventStreamFilters {
            model: Some(model.to_owned()),
            ..RequestEventStreamFilters::default()
        };

        // A shortened prefix keeps the exact match and picks up the dated and
        // mixed-case variants; other families and model-less rows stay out.
        let prefix_page = storage
            .list_request_events(&list_query(by_model("claude-sonnet"), None, 10, None, None))
            .await?;
        assert_event_ids(&prefix_page, &["event-base", "event-dated", "event-upper"]);

        let upper_needle_page = storage
            .list_request_events(&list_query(by_model("Claude-Sonnet"), None, 10, None, None))
            .await?;
        assert_event_ids(
            &upper_needle_page,
            &["event-base", "event-dated", "event-upper"],
        );

        let exact_page = storage
            .list_request_events(&list_query(
                by_model("claude-sonnet-4-5-20250929"),
                None,
                10,
                None,
                None,
            ))
            .await?;
        assert_event_ids(&exact_page, &["event-dated"]);

        // `%` is a literal in the needle, not a wildcard.
        let wildcard_page = storage
            .list_request_events(&list_query(by_model("claude%"), None, 10, None, None))
            .await?;
        assert_event_ids(&wildcard_page, &["event-wildcard"]);

        let miss_page = storage
            .list_request_events(&list_query(by_model("gpt"), None, 10, None, None))
            .await?;
        assert_event_ids(&miss_page, &[]);

        Ok(())
    })
    .await
}

async fn with_fixture<B, F, Fut>(backend: Arc<B>, scenario: F) -> Result<()>
where
    B: ConformanceBackend,
    F: FnOnce(Arc<B::Storage>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result = scenario(fixture.storage()).await;
    let teardown_result = fixture.teardown().await;

    result?;
    teardown_result
}

fn request_event(
    event_id: &str,
    request_id: &str,
    ts_ms: u64,
    status: u16,
    principal_id: &str,
    upstream_id: Uuid,
) -> RequestEvent {
    RequestEvent {
        ts: ts_ms / 1_000,
        ts_ms: Some(ts_ms),
        request_id: request_id.to_owned(),
        event_id: Some(event_id.to_owned()),
        source_kind: Some("proxy".to_owned()),
        principal_id: Some(principal_id.to_owned()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        upstream_id: Some(upstream_id),
        upstream_name: Some("upstream-a".to_owned()),
        thread_id: Some("thread-a".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        observed_session_id: Some("session-a".to_owned()),
        request_kind: Some("subagent".to_owned()),
        claude_agent_id: Some("agent-a".to_owned()),
        claude_parent_agent_id: Some("agent-parent".to_owned()),
        parent_session_id: Some("session-parent".to_owned()),
        client_app: Some("cli-bg".to_owned()),
        session_id_source: Some("x-claude-code-session-id".to_owned()),
        reasoning_effort: Some("high".to_owned()),
        thinking_budget_tokens: Some(101),
        thinking_tokens: Some(102),
        service_tier: Some("standard".to_owned()),
        status,
        error_code: Some("provider_error".to_owned()),
        upstream_error_type: Some("rate_limit_error".to_owned()),
        upstream_error_message: Some("bounded provider message".to_owned()),
        duration_ms: 40,
        auth_ms: Some(5),
        route_ms: Some(6),
        limit_reserve_ms: Some(7),
        json_parse_ms: Some(0.125),
        cache_structure_ms: Some(0.25),
        cache_token_key_ms: Some(0.375),
        cache_count_lookup_ms: Some(0.5),
        cache_tokenizer_queue_ms: Some(0.625),
        cache_serialize_ms: Some(0.75),
        cache_tokenize_ms: Some(0.0),
        prepare_signer_ms: Some(1.25),
        bulkhead_wait_ms: Some(8),
        dns_ms: Some(9),
        connect_ms: Some(10),
        connection_reused: Some(true),
        limit_reconcile_ms: Some(11),
        observability_post_ms: Some(12),
        proxy_setup_ms: Some(13),
        shape_ms: Some(14),
        sign_ms: Some(15),
        upstream_ttfb_ms: Some(16),
        upstream_body_ms: Some(17),
        stream_first_content_delta_ms: Some(18),
        stream_last_content_delta_ms: Some(19),
        inter_token_avg_ms: Some(20),
        input_tokens: Some(21),
        output_tokens: Some(22),
        cache_creation_input_tokens: Some(23),
        cache_creation_input_tokens_5m: Some(24),
        cache_creation_input_tokens_1h: Some(25),
        cache_read_input_tokens: Some(26),
        cost_usd_micros: Some(27),
        cost_input_micros: Some(28),
        cost_output_micros: Some(29),
        cost_cache_creation_5m_micros: Some(30),
        cost_cache_creation_1h_micros: Some(31),
        cost_cache_read_micros: Some(32),
        ..Default::default()
    }
}

fn list_query(
    filters: RequestEventStreamFilters,
    source_kind: Option<&str>,
    limit: usize,
    until_ts_ms: Option<u64>,
    until_event_id: Option<String>,
) -> RequestEventListQuery {
    RequestEventListQuery {
        since_unix_secs: 0,
        until_unix_secs: u64::MAX,
        until_ts_ms,
        until_event_id,
        limit,
        filters,
        source_kind: source_kind.map(str::to_owned),
    }
}

fn assert_event_ids(items: &[RequestEventListItem], expected: &[&str]) {
    let actual = items
        .iter()
        .map(|item| item.event_id.as_deref().unwrap_or_default())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

async fn assert_list_matches_details<S>(storage: &S, items: &[RequestEventListItem]) -> Result<()>
where
    S: RequestEventStore + ?Sized,
{
    for item in items {
        let event_id = item.event_id.as_deref().expect("list event ID");
        let detail = storage
            .get_request_event(event_id)
            .await?
            .expect("stored request event detail");
        assert_eq!(item, &list_projection(&detail));
    }
    Ok(())
}

fn list_projection(event: &RequestEvent) -> RequestEventListItem {
    RequestEventListItem {
        ts: event.ts,
        ts_ms: Some(
            event
                .ts_ms
                .unwrap_or_else(|| event.ts.saturating_mul(1_000)),
        ),
        request_id: event.request_id.clone(),
        event_id: event.event_id.clone(),
        source_kind: event.source_kind.clone(),
        principal_id: event.principal_id.clone(),
        upstream: event.upstream,
        upstream_id: event.upstream_id,
        upstream_name: event.upstream_name.clone(),
        thread_id: event.thread_id.clone(),
        observed_session_id: event.observed_session_id.clone(),
        request_kind: event.request_kind.clone(),
        claude_agent_id: event.claude_agent_id.clone(),
        claude_parent_agent_id: event.claude_parent_agent_id.clone(),
        parent_session_id: event.parent_session_id.clone(),
        client_app: event.client_app.clone(),
        session_id_source: event.session_id_source.clone(),
        model: event.model.clone(),
        reasoning_effort: event.reasoning_effort.clone(),
        thinking_budget_tokens: event.thinking_budget_tokens,
        thinking_tokens: event.thinking_tokens,
        service_tier: event.service_tier.clone(),
        status: event.status,
        error_code: event.error_code.clone(),
        upstream_error_type: event.upstream_error_type.clone(),
        upstream_error_message: event.upstream_error_message.clone(),
        duration_ms: event.duration_ms,
        request_body_read_ms: event.request_body_read_ms,
        request_body_bytes: event.request_body_bytes,
        auth_ms: event.auth_ms,
        route_ms: event.route_ms,
        limit_reserve_ms: event.limit_reserve_ms,
        json_parse_ms: event.json_parse_ms,
        cache_structure_ms: event.cache_structure_ms,
        cache_token_key_ms: event.cache_token_key_ms,
        cache_count_lookup_ms: event.cache_count_lookup_ms,
        cache_tokenizer_queue_ms: event.cache_tokenizer_queue_ms,
        cache_serialize_ms: event.cache_serialize_ms,
        cache_tokenize_ms: event.cache_tokenize_ms,
        prepare_signer_ms: event.prepare_signer_ms,
        bulkhead_wait_ms: event.bulkhead_wait_ms,
        dns_ms: event.dns_ms,
        connect_ms: event.connect_ms,
        connection_reused: event.connection_reused,
        limit_reconcile_ms: event.limit_reconcile_ms,
        observability_post_ms: event.observability_post_ms,
        proxy_setup_ms: event.proxy_setup_ms,
        shape_ms: event.shape_ms,
        sign_ms: event.sign_ms,
        upstream_ttfb_ms: event.upstream_ttfb_ms,
        upstream_body_ms: event.upstream_body_ms,
        finalize_ms: event.finalize_ms,
        stream_first_content_delta_ms: event.stream_first_content_delta_ms,
        stream_last_content_delta_ms: event.stream_last_content_delta_ms,
        inter_token_avg_ms: event.inter_token_avg_ms,
        input_tokens: event.input_tokens,
        output_tokens: event.output_tokens,
        cache_creation_input_tokens: event.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: event.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: event.cache_creation_input_tokens_1h,
        cache_read_input_tokens: event.cache_read_input_tokens,
        cost_usd_micros: event.cost_usd_micros,
        cost_input_micros: event.cost_input_micros,
        cost_output_micros: event.cost_output_micros,
        cost_cache_creation_5m_micros: event.cost_cache_creation_5m_micros,
        cost_cache_creation_1h_micros: event.cost_cache_creation_1h_micros,
        cost_cache_read_micros: event.cost_cache_read_micros,
    }
}

fn decoy(event: &RequestEvent, event_id: &str, ts_ms: u64) -> RequestEvent {
    let mut event = event.clone();
    event.ts = ts_ms / 1_000;
    event.ts_ms = Some(ts_ms);
    event.request_id = format!("{event_id}-request");
    event.event_id = Some(event_id.to_owned());
    event.iterations = None;
    event
}
