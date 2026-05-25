use cc_lb_storage_api::{
    AuditEntry, AuditStore, RequestEvent, RequestEventStore, RequestEventUpstream,
};
use cc_lb_storage_redb::RedbStorage;
use serde_json::json;

#[tokio::test]
async fn audit_store_trait_path_appends_queries_and_prunes()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-audit.redb");
    let storage = RedbStorage::open(&path)?;

    AuditStore::append_audit(&storage, &audit_entry("req-0001", "alice", 1_700_000_000)).await?;
    AuditStore::append_audit(&storage, &audit_entry("req-0002", "alice", 1_700_000_000)).await?;
    AuditStore::append_audit(&storage, &audit_entry("req-0003", "bob", 1_700_000_001)).await?;

    let entries = AuditStore::query_audit(&storage, None, 0, u64::MAX, 10).await?;
    assert_eq!(request_ids(&entries), ["req-0001", "req-0002", "req-0003"]);

    let alice_entries = AuditStore::query_audit(&storage, Some("alice"), 0, u64::MAX, 10).await?;
    assert_eq!(request_ids(&alice_entries), ["req-0001", "req-0002"]);

    let pruned = AuditStore::prune_audit(&storage, 1_700_000_001).await?;
    assert_eq!(pruned, 2);

    let remaining = AuditStore::query_audit(&storage, None, 0, u64::MAX, 10).await?;
    assert_eq!(request_ids(&remaining), ["req-0003"]);

    Ok(())
}

#[tokio::test]
async fn request_event_store_trait_path_appends_and_queries()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-events.redb");
    let storage = RedbStorage::open(&path)?;

    RequestEventStore::append_request_event(&storage, &request_event("req-0001", 1_800_000_000))
        .await?;
    RequestEventStore::append_request_event(&storage, &request_event("req-0002", 1_800_000_000))
        .await?;

    let events =
        RequestEventStore::query_request_events(&storage, 1_800_000_000, u64::MAX, 10).await?;

    assert_eq!(event_ids(&events), ["req-0001", "req-0002"]);
    assert_eq!(events[0].principal_id.as_deref(), Some("principal-a"));
    assert_eq!(
        events[0].upstream,
        Some(RequestEventUpstream::AnthropicDirect)
    );

    Ok(())
}

fn audit_entry(request_id: &str, principal_id: &str, ts: u64) -> AuditEntry {
    AuditEntry {
        ts,
        request_id: request_id.to_owned(),
        principal_id: principal_id.to_owned(),
        route: "messages".to_owned(),
        upstream: "anthropic_direct".to_owned(),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: 13,
        output_tokens: 21,
        duration_ms: 34,
        agent_label: Some("adapter-test".to_owned()),
        kind: Some("request".to_owned()),
        payload: Some(json!({"source":"adapter-trait"})),
    }
}

fn request_event(request_id: &str, ts: u64) -> RequestEvent {
    RequestEvent {
        ts,
        request_id: request_id.to_owned(),
        principal_id: Some("principal-a".to_owned()),
        principal_kind: Some("api_key".to_owned()),
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(13),
        output_tokens: Some(21),
        duration_ms: 34,
        error_code: None,
    }
}

fn request_ids(entries: &[AuditEntry]) -> Vec<&str> {
    entries
        .iter()
        .map(|entry| entry.request_id.as_str())
        .collect()
}

fn event_ids(events: &[RequestEvent]) -> Vec<&str> {
    events
        .iter()
        .map(|event| event.request_id.as_str())
        .collect()
}
