use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{
    AuditEntry, AuditQueryScope, AuditStore as _, ConfigDraftState, ConfigStore as _, HistoryEntry,
    RequestEvent, RequestEventKind, RequestEventStore as _, RequestEventStreamFilters,
};
use serde_json::json;

use crate::harness::{ConformanceBackend, ConformanceFixture, stored_request_events};

macro_rules! assert_byte_identical_vec {
    ($actual:expr, $expected:expr, $label:expr) => {{
        ensure!(
            $actual.len() == $expected.len(),
            "{} length mismatch: actual {}, expected {}",
            $label,
            $actual.len(),
            $expected.len()
        );
        for (index, (actual, expected)) in $actual.iter().zip($expected.iter()).enumerate() {
            assert_byte_identical!(actual, expected, &format!("{}[{index}]", $label))?;
        }

        Ok::<(), anyhow::Error>(())
    }};
}

macro_rules! assert_byte_identical {
    ($actual:expr, $expected:expr, $label:expr) => {{
        let actual = serde_json::to_vec(&$actual)?;
        let expected = serde_json::to_vec(&$expected)?;
        ensure!(actual == expected, "{} JSON bytes differ", $label);
        Ok::<(), anyhow::Error>(())
    }};
}

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    audit_store_roundtrip(Arc::clone(&backend)).await?;
    request_event_store_roundtrip(Arc::clone(&backend)).await?;
    request_event_store_event_id_idempotency(Arc::clone(&backend)).await?;
    config_store_roundtrip(Arc::clone(&backend)).await?;

    Ok(())
}

pub async fn audit_store_roundtrip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let entries = audit_entries();

        for entry in &entries {
            storage.append_audit(entry).await?;
        }

        let mut read_back = storage
            .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 10, false)
            .await?;
        // query_recent_audit returns newest first; the fixture is oldest first.
        read_back.reverse();
        assert_byte_identical_vec!(read_back, entries, "AuditStore readback")?;

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn request_event_store_roundtrip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let events = request_events();

        for event in &events {
            storage.append_request_event(event).await?;
        }

        let read_back = stored_request_events(storage.as_ref()).await?;
        assert_byte_identical_vec!(read_back, events, "RequestEventStore readback")?;

        backend
            .wait_for_events_visible_for_rollup(storage.as_ref())
            .await?;
        let cursor = storage.current_request_event_cursor().await?;
        let cursor_events = storage
            .query_request_events_between_cursors(
                0,
                cursor,
                events.len() + 1,
                &RequestEventStreamFilters::default(),
            )
            .await?
            .into_iter()
            .map(|(_, event)| event)
            .collect::<Vec<_>>();
        assert_byte_identical_vec!(cursor_events, events, "RequestEventStore cursor readback")?;

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

/// Guards the `event_id` partial UNIQUE INDEX UPSERT match: writing the
/// same `event_id` twice must succeed silently (ON CONFLICT DO NOTHING) and
/// keep exactly one row. Catches partial-index WHERE-clause mismatches that
/// would otherwise return `ON CONFLICT clause does not match any PRIMARY KEY
/// or UNIQUE constraint` at runtime.
pub async fn request_event_store_event_id_idempotency<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();

        let event_id = "0193a7b8-1234-7e2f-9012-deadbeefcafe".to_owned();
        let first = RequestEvent {
            ts: 1_900_400_100,
            ts_ms: Some(1_900_400_100_000),
            request_id: "idempotency-event-001".to_owned(),
            event_id: Some(event_id.clone()),
            principal_id: Some("idempotency-principal".to_owned()),
            principal_kind: Some("api_key".to_owned()),
            status: 200,
            duration_ms: 50,
            ..Default::default()
        };
        let second = RequestEvent {
            ts: 1_900_400_200,
            ts_ms: Some(1_900_400_200_000),
            request_id: "idempotency-event-001".to_owned(),
            event_id: Some(event_id.clone()),
            principal_id: Some("idempotency-principal".to_owned()),
            principal_kind: Some("api_key".to_owned()),
            status: 500,
            duration_ms: 999,
            error_code: Some("retry_after_first_write".to_owned()),
            ..Default::default()
        };

        let first_cursor = storage
            .append_request_event(&first)
            .await
            .context("first write must succeed (no partial-index match error)")?;
        let second_cursor = storage
            .append_request_event(&second)
            .await
            .context("second write with same event_id must succeed (ON CONFLICT DO NOTHING)")?;
        ensure!(
            first_cursor == second_cursor,
            "duplicate event_id should return existing cursor: first {}, second {}",
            first_cursor,
            second_cursor
        );

        let read_back = stored_request_events(storage.as_ref()).await?;
        ensure!(
            read_back.len() == 1,
            "exactly one row expected after duplicate event_id write, got {}",
            read_back.len()
        );
        let stored = &read_back[0];
        ensure!(
            stored.event_id.as_deref() == Some(event_id.as_str()),
            "stored event_id should match first write"
        );
        ensure!(
            stored.status == 200,
            "first write should win (ON CONFLICT DO NOTHING semantics), got status {}",
            stored.status
        );
        ensure!(
            stored.error_code.is_none(),
            "first write had no error_code; duplicate must not overwrite"
        );

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn config_store_roundtrip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let draft = ConfigDraftState {
            draft: Some(json!({
                "timeouts": { "upstream_total_secs": 30 },
                "body": { "messages_cap_bytes": 1048576 }
            })),
            saved_at_unix_secs: Some(1_800_400_000),
            ..ConfigDraftState::default()
        };

        let revision = storage.put_config_draft(draft.clone(), 0).await?;
        ensure!(
            revision == 1,
            "first config draft write should produce revision 1"
        );

        let expected_draft = ConfigDraftState {
            revision,
            last_validated_revision: None,
            last_validation: None,
            ..draft
        };
        let read_back = storage.get_config_draft().await?;
        assert_byte_identical!(read_back, expected_draft, "ConfigStore draft readback")?;

        let history = HistoryEntry {
            revision,
            applied_at_unix_secs: 1_800_400_010,
        };
        storage
            .append_config_history(history.revision, history.applied_at_unix_secs)
            .await?;

        let listed = storage.list_config_history(10).await?;
        assert_byte_identical_vec!(
            listed,
            std::slice::from_ref(&history),
            "ConfigStore history list readback"
        )?;

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

fn audit_entries() -> Vec<AuditEntry> {
    vec![
        AuditEntry {
            ts: 1_800_300_000,
            request_id: "roundtrip-audit-001".to_owned(),
            principal_id: "roundtrip-principal-a".to_owned(),
            route: "messages".to_owned(),
            upstream: "anthropic_direct".to_owned(),
            model: Some("claude-sonnet-4-5".to_owned()),
            status: 200,
            input_tokens: Some(123),
            output_tokens: Some(456),
            duration_ms: 789,
            agent_label: Some("roundtrip-agent".to_owned()),
            ..Default::default()
        },
        AuditEntry {
            ts: 1_800_300_001,
            request_id: "roundtrip-audit-002".to_owned(),
            principal_id: "roundtrip-principal-b".to_owned(),
            route: "admin".to_owned(),
            upstream: "control_plane".to_owned(),
            model: None,
            status: 403,
            input_tokens: Some(0),
            output_tokens: Some(0),
            duration_ms: 12,
            agent_label: None,
            ..Default::default()
        },
    ]
}

fn request_events() -> Vec<RequestEvent> {
    vec![
        RequestEvent {
            ts: 1_800_300_100,
            request_id: "roundtrip-event-001".to_owned(),
            event_id: Some("0193a7b8-9c5d-7e2f-9012-aabbccddeeff".to_owned()),
            source_kind: Some("proxy".to_owned()),
            source_ref_id: Some("roundtrip-source-ref-a".to_owned()),
            event_kind: Some(RequestEventKind::Messages),
            ts_ms: Some(1_800_300_100_123),
            principal_id: Some("roundtrip-principal-a".to_owned()),
            key_id: Some("key-a".to_owned()),
            principal_kind: Some("api_key".to_owned()),
            upstream_id: Some(uuid::Uuid::from_u128(17)),
            upstream_name: Some("roundtrip-upstream-a".to_owned()),
            model: Some("claude-sonnet-4-5".to_owned()),
            status: 502,
            input_tokens: Some(12),
            output_tokens: Some(34),
            cache_creation_input_tokens: Some(5),
            cache_creation_input_tokens_5m: Some(3),
            cache_creation_input_tokens_1h: Some(2),
            cache_read_input_tokens: Some(6),
            thread_id: Some("thread-roundtrip-a".to_owned()),
            thinking_budget_tokens: Some(18000),
            reasoning_effort: Some("max".to_owned()),
            observed_session_id: Some("session-roundtrip-a".to_owned()),
            request_kind: Some("subagent".to_owned()),
            claude_agent_id: Some("agent-roundtrip-a".to_owned()),
            claude_parent_agent_id: Some("parent-agent-roundtrip-a".to_owned()),
            parent_session_id: Some("parent-session-roundtrip-a".to_owned()),
            client_app: Some("cli-roundtrip".to_owned()),
            session_id_source: Some("x-claude-code-session-id".to_owned()),
            token_estimate_source: Some("local_tiktoken_v1".to_owned()),
            cost_usd_micros: Some(7),
            cost_input_micros: Some(1),
            cost_output_micros: Some(2),
            cost_cache_creation_5m_micros: Some(3),
            cost_cache_creation_1h_micros: Some(4),
            cost_cache_read_micros: Some(5),
            auth_ms: Some(1),
            route_ms: Some(2),
            limit_reserve_ms: Some(3),
            json_parse_ms: Some(0.125),
            cache_structure_ms: Some(0.25),
            cache_token_key_ms: Some(0.375),
            cache_count_lookup_ms: Some(0.5),
            cache_tokenizer_queue_ms: Some(0.625),
            cache_serialize_ms: Some(0.75),
            cache_tokenize_ms: Some(0.875),
            prepare_signer_ms: Some(1.125),
            bulkhead_wait_ms: Some(4),
            dns_ms: Some(5),
            connect_ms: Some(6),
            connection_reused: Some(false),
            request_body_read_ms: Some(7),
            request_body_first_chunk_ms: Some(1.25),
            request_body_receive_ms: Some(1.375),
            request_body_wait_ms: Some(1.5),
            request_body_process_ms: Some(1.625),
            request_body_chunk_count: Some(8),
            request_body_bytes: Some(4096),
            duration_ms: 89,
            proxy_setup_ms: Some(9),
            shape_ms: Some(10),
            sign_ms: Some(11),
            upstream_ttfb_ms: Some(12),
            upstream_body_ms: Some(13),
            response_body_wait_ms: Some(1.75),
            response_body_process_ms: Some(1.875),
            response_body_downstream_poll_gap_ms: Some(2.125),
            retry_overhead_ms: Some(2.25),
            finalize_ms: Some(14),
            body_chunk_count: Some(15),
            body_bytes: Some(8192),
            stream_message_start_ms: Some(16),
            stream_content_block_start_ms: Some(17),
            stream_first_content_delta_ms: Some(18),
            stream_last_content_delta_ms: Some(19),
            stream_message_stop_ms: Some(20),
            stream_last_chunk_ms: Some(21),
            stream_total_ms: Some(22),
            sse_event_count: Some(23),
            content_delta_count: Some(24),
            ping_count: Some(25),
            inter_token_avg_ms: Some(26),
            error_code: Some("upstream_body_error".to_owned()),
            thinking_tokens: Some(27),
            service_tier: Some("standard".to_owned()),
            upstream_error_type: Some("transport_error".to_owned()),
            upstream_error_message: Some("upstream connection reset".to_owned()),
            upstream_http_version: Some("HTTP/2.0".to_owned()),
            upstream_request_id: Some("req-upstream-roundtrip-a".to_owned()),
            upstream_content_encoding: Some("gzip".to_owned()),
            upstream_content_length: Some(16384),
            upstream_body_bytes: Some(8191),
            upstream_body_end: Some("transport_error".to_owned()),
            upstream_body_error_cause: Some("upstream_body_error".to_owned()),
            upstream_body_error_io_kind: Some("connection_reset".to_owned()),
            upstream_body_error_h2_reason: Some("INTERNAL_ERROR".to_owned()),
            upstream_stream_warning_type: Some("upstream_response_body_error".to_owned()),
            upstream_stream_warning_message: Some(
                "upstream body ended after message_stop".to_owned(),
            ),
            ..Default::default()
        },
        RequestEvent {
            ts: 1_800_300_101,
            ts_ms: Some(1_800_300_101_000),
            request_id: "roundtrip-event-002".to_owned(),
            event_id: Some("0193a7b8-9c5d-7e2f-9012-bbccddeeff00".to_owned()),
            principal_id: Some("roundtrip-principal-b".to_owned()),
            principal_kind: Some("oauth".to_owned()),
            model: Some("claude-opus-4-1".to_owned()),
            status: 429,
            duration_ms: 144,
            error_code: Some("rate_limit".to_owned()),
            ..Default::default()
        },
    ]
}
