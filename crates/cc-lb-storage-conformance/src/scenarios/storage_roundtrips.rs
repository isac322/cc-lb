use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{
    AuditStore as _, ConfigDraftState, ConfigStore as _, RequestEventStore as _,
    types::{
        AuditEntry, HistoryEntry, RequestCacheBreakpoint, RequestCacheBreakpointSource,
        RequestCacheState, RequestEvent, RequestEventUpstream,
    },
};
use serde_json::json;

use crate::harness::{ConformanceBackend, ConformanceFixture};

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

        let read_back = storage.query_audit(None, 0, u64::MAX, 10).await?;
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
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let events = request_events();

        for event in &events {
            storage.append_request_event(event).await?;
        }

        let read_back = storage.query_request_events(0, u64::MAX, 10).await?;
        assert_byte_identical_vec!(read_back, events, "RequestEventStore readback")?;

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
            request_id: "idempotency-event-001".to_owned(),
            event_id: Some(event_id.clone()),
            principal_id: Some("idempotency-principal".to_owned()),
            principal_kind: Some("api_key".to_owned()),
            upstream: Some(RequestEventUpstream::AnthropicDirect),
            status: 200,
            duration_ms: 50,
            ..Default::default()
        };
        let second = RequestEvent {
            ts: 1_900_400_200,
            request_id: "idempotency-event-001".to_owned(),
            event_id: Some(event_id.clone()),
            principal_id: Some("idempotency-principal".to_owned()),
            principal_kind: Some("api_key".to_owned()),
            upstream: Some(RequestEventUpstream::AnthropicDirect),
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

        let read_back = storage.query_request_events(0, u64::MAX, 10).await?;
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
            kind: Some("request".to_owned()),
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
            kind: Some("admin".to_owned()),
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
            ts_ms: Some(1_800_300_100_123),
            principal_id: Some("roundtrip-principal-a".to_owned()),
            key_id: Some("key-a".to_owned()),
            principal_kind: Some("api_key".to_owned()),
            upstream: Some(RequestEventUpstream::AnthropicDirect),
            model: Some("claude-sonnet-4-5".to_owned()),
            status: 200,
            input_tokens: Some(12),
            output_tokens: Some(34),
            cache_creation_input_tokens: Some(5),
            cache_read_input_tokens: Some(6),
            cache_state: Some(RequestCacheState::Refresh),
            thread_id: Some("thread-roundtrip-a".to_owned()),
            message_id: Some("msg-roundtrip-a".to_owned()),
            message_index: Some(2),
            message_count: Some(3),
            cache_control_block_count: Some(2),
            thinking_budget_tokens: Some(18000),
            reasoning_effort: Some("max".to_owned()),
            cache_control_message_indices: vec![0, 2],
            cache_breakpoints: vec![
                RequestCacheBreakpoint {
                    block_index: 0,
                    source: RequestCacheBreakpointSource::System,
                    path: "system[0]".to_owned(),
                    message_index: None,
                    ttl: Some("1h".to_owned()),
                    prefix_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                    prefix_token_count: 1536,
                    lookback_prefixes: Vec::new(),
                    token_estimate_source: Some("local_tiktoken_v1".to_owned()),
                },
                RequestCacheBreakpoint {
                    block_index: 1,
                    source: RequestCacheBreakpointSource::Message,
                    path: "messages[2].content[0]".to_owned(),
                    message_index: Some(2),
                    ttl: Some("5m".to_owned()),
                    prefix_hash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
                    prefix_token_count: 4096,
                    lookback_prefixes: Vec::new(),
                    token_estimate_source: Some("local_tiktoken_v1".to_owned()),
                },
            ],
            cache_prefix_hash: Some(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
            ),
            quota_urgency_5h: Some(0.125),
            quota_urgency_7d: Some(0.75),
            quota_urgency_combined: Some(0.8),
            quota_warning_multiplier: Some(0.2),
            cost_usd_micros: Some(7),
            duration_ms: 89,
            error_code: None,
            ..Default::default()
        },
        RequestEvent {
            ts: 1_800_300_101,
            request_id: "roundtrip-event-002".to_owned(),
            event_id: Some("0193a7b8-9c5d-7e2f-9012-bbccddeeff00".to_owned()),
            principal_id: Some("roundtrip-principal-b".to_owned()),
            principal_kind: Some("oauth".to_owned()),
            upstream: Some(RequestEventUpstream::AnthropicDirect),
            model: Some("claude-opus-4-1".to_owned()),
            status: 429,
            duration_ms: 144,
            error_code: Some("rate_limit".to_owned()),
            ..Default::default()
        },
    ]
}
