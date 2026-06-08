use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{
    AuditStore as _, BackendKind, ConfigDraftState, ConfigStore as _, LimitStateStore as _,
    OAuthCredentialStore as _, RequestEventStore as _, StorageError,
    types::{
        AuditEntry, HistoryEntry, HistorySummary, PrincipalLimitIdentityKind, PrincipalLimitKind,
        PrincipalLimitState, RequestCacheBreakpoint, RequestCacheBreakpointSource,
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
    limit_state_store_roundtrip(Arc::clone(&backend)).await?;
    config_store_roundtrip(Arc::clone(&backend)).await?;
    oauth_credential_store_roundtrip(backend).await?;

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

pub async fn limit_state_store_roundtrip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let states = principal_limit_states();

        if fixture.backend_kind() == BackendKind::Redb {
            let error = storage
                .put_principal_limit_state(&states[0])
                .await
                .expect_err("redb limit-state storage is intentionally removed");
            ensure!(
                matches!(error, StorageError::Fatal { message } if message == "legacy redb limit state store removed; use limit engine"),
                "redb must report the documented limit-state removal"
            );
            return Ok(());
        }

        for state in &states {
            storage.put_principal_limit_state(state).await?;
        }

        let read_back = storage
            .get_principal_limit_state(
                "roundtrip-limit-principal",
                PrincipalLimitIdentityKind::Credential,
                Some("credential-a"),
                "weekly",
                PrincipalLimitKind::Tokens,
            )
            .await?
            .context("credential limit state should round-trip")?;
        assert_byte_identical!(read_back, states[0], "LimitStateStore point read")?;

        let listed = storage
            .list_principal_limit_states("roundtrip-limit-principal")
            .await?;
        assert_byte_identical_vec!(listed, states, "LimitStateStore list readback")?;

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
                "principals": [{"id": "local", "label": "Local"}],
                "upstreams": {"primary": {"kind": "anthropic_direct"}}
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
            last_validation_error: None,
            ..draft
        };
        let read_back = storage.get_config_draft().await?;
        assert_byte_identical!(read_back, expected_draft, "ConfigStore draft readback")?;

        let history = HistoryEntry {
            revision,
            config_toml: "[oauth.anthropic]\nclient_id = \"test-client\"\n".to_owned(),
            applied_at_unix_secs: 1_800_400_010,
            summary: HistorySummary {
                upstreams: 1,
                principals: 1,
                plugin_count: 0,
                tls_enabled: true,
            },
        };
        storage
            .append_config_history(
                history.revision,
                history.config_toml.clone(),
                history.applied_at_unix_secs,
                history.summary.clone(),
            )
            .await?;

        let read_back = storage
            .get_config_history(revision)
            .await?
            .context("config history entry should round-trip")?;
        assert_byte_identical!(read_back, history, "ConfigStore history point read")?;

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

pub async fn oauth_credential_store_roundtrip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let oauth_ciphertext = b"\x00oauth\xffciphertext\x10bytes";
        storage
            .put_oauth_ciphertext(
                "roundtrip-oauth-principal",
                "anthropic_oauth",
                oauth_ciphertext,
            )
            .await?;
        let read_back = storage
            .get_oauth_ciphertext("roundtrip-oauth-principal", "anthropic_oauth")
            .await?
            .context("OAuth ciphertext should round-trip")?;
        ensure!(
            read_back == oauth_ciphertext,
            "OAuthCredentialStore must preserve OAuth ciphertext bytes exactly"
        );

        let api_key_ciphertext = b"\x01anthropic\xfeapi-key\x20ciphertext";
        storage
            .put_anthropic_api_key_ciphertext(
                "anthropic:roundtrip-oauth-principal:default",
                api_key_ciphertext,
            )
            .await?;
        let read_back = storage
            .get_anthropic_api_key_ciphertext("anthropic:roundtrip-oauth-principal:default")
            .await?
            .context("Anthropic API-key ciphertext should round-trip")?;
        ensure!(
            read_back == api_key_ciphertext,
            "OAuthCredentialStore must preserve Anthropic API-key ciphertext bytes exactly"
        );

        ensure!(
            storage
                .delete_oauth("roundtrip-oauth-principal", "anthropic_oauth")
                .await?,
            "delete_oauth should report true for an existing row"
        );
        ensure!(
            storage
                .get_oauth_ciphertext("roundtrip-oauth-principal", "anthropic_oauth")
                .await?
                .is_none(),
            "deleted OAuth ciphertext should be absent"
        );

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
                },
            ],
            cache_prefix_hash: Some(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
            ),
            cost_usd_micros: Some(7),
            duration_ms: 89,
            error_code: None,
            ..Default::default()
        },
        RequestEvent {
            ts: 1_800_300_101,
            request_id: "roundtrip-event-002".to_owned(),
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

fn principal_limit_states() -> Vec<PrincipalLimitState> {
    vec![
        PrincipalLimitState {
            principal_id: "roundtrip-limit-principal".to_owned(),
            identity_kind: PrincipalLimitIdentityKind::Credential,
            identity_value: Some("credential-a".to_owned()),
            account_observed: false,
            window: "weekly".to_owned(),
            kind: PrincipalLimitKind::Tokens,
            limit: Some(100_000),
            remaining: Some(99_000),
            reset: Some("2026-05-29T00:00:00Z".to_owned()),
            observed_at_unix_secs: 1_800_300_200,
            stored_at_unix_secs: 1_800_300_201,
        },
        PrincipalLimitState {
            principal_id: "roundtrip-limit-principal".to_owned(),
            identity_kind: PrincipalLimitIdentityKind::Unobserved,
            identity_value: None,
            account_observed: false,
            window: "minute".to_owned(),
            kind: PrincipalLimitKind::Requests,
            limit: None,
            remaining: None,
            reset: None,
            observed_at_unix_secs: 1_800_300_202,
            stored_at_unix_secs: 1_800_300_203,
        },
    ]
}
