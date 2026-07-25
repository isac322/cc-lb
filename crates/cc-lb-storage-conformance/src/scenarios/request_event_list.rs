use std::{future::Future, sync::Arc};

use anyhow::Result;
use cc_lb_storage_api::{
    RequestEventListQuery, RequestEventStore as _, RequestEventStreamFilters,
    types::{RequestEvent, RequestEventUpstream},
};
use serde_json::json;

use crate::harness::{ConformanceBackend, ConformanceFixture};

pub async fn request_event_list_projects_rows_and_preserves_detail<B: ConformanceBackend>(
    backend: Arc<B>,
) -> Result<()> {
    with_fixture(backend, |storage| async move {
        let matching = request_event("event-b", "req-b", 2_000, 200, "principal-a");
        let older_matching = request_event("event-a", "req-a", 2_000, 200, "principal-a");
        let non_matching = request_event("event-c", "req-c", 3_000, 500, "principal-b");

        storage.append_request_event(&older_matching).await?;
        storage.append_request_event(&matching).await?;
        storage.append_request_event(&non_matching).await?;

        let page = storage
            .list_request_events(&RequestEventListQuery {
                since_unix_secs: 0,
                until_unix_secs: u64::MAX,
                until_ts_ms: None,
                until_event_id: None,
                limit: 1,
                filters: RequestEventStreamFilters {
                    principal_id: Some("principal-a".to_owned()),
                    ..Default::default()
                },
                source_kind: None,
            })
            .await?;

        assert_eq!(page.len(), 1);
        assert_eq!(page[0].request_id, matching.request_id);
        assert_eq!(page[0].event_id.as_deref(), matching.event_id.as_deref());
        assert_eq!(page[0].ts_ms, matching.ts_ms);
        assert_eq!(page[0].status, matching.status);
        assert_eq!(page[0].cost_usd_micros, matching.cost_usd_micros);
        assert_eq!(
            page[0].upstream_error_type.as_deref(),
            matching.upstream_error_type.as_deref()
        );
        assert_eq!(
            page[0].upstream_error_message.as_deref(),
            matching.upstream_error_message.as_deref()
        );
        assert_eq!(page[0].auth_ms, matching.auth_ms);
        assert_eq!(page[0].connect_ms, matching.connect_ms);
        assert_eq!(page[0].connection_reused, matching.connection_reused);
        assert_eq!(
            page[0].observed_session_id.as_deref(),
            matching.observed_session_id.as_deref()
        );
        assert_eq!(
            page[0].request_kind.as_deref(),
            matching.request_kind.as_deref()
        );

        let detail = storage
            .get_request_event(matching.event_id.as_deref().unwrap_or_default())
            .await?
            .expect("stored request event detail");
        assert_eq!(serde_json::to_vec(&detail)?, serde_json::to_vec(&matching)?);

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
) -> RequestEvent {
    RequestEvent {
        ts: ts_ms / 1_000,
        ts_ms: Some(ts_ms),
        request_id: request_id.to_owned(),
        event_id: Some(event_id.to_owned()),
        principal_id: Some(principal_id.to_owned()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        model: Some("claude-sonnet-4-5".to_owned()),
        observed_session_id: Some("session-a".to_owned()),
        request_kind: Some("subagent".to_owned()),
        status,
        input_tokens: Some(10),
        output_tokens: Some(20),
        cost_usd_micros: Some(30),
        duration_ms: 40,
        upstream_error_type: Some("rate_limit_error".to_owned()),
        upstream_error_message: Some("bounded provider message".to_owned()),
        auth_ms: Some(5),
        connect_ms: Some(7),
        connection_reused: Some(true),
        iterations: Some(json!({"not_in_list_projection": "x".repeat(1_000_000)})),
        ..Default::default()
    }
}
