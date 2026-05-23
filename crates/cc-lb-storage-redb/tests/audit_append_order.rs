use cc_lb_storage_redb::{AuditEntry, Storage};

#[test]
fn audit_query_returns_append_order_for_monotonic_keys() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let storage = Storage::open(&path, [15; 32])?;

    for index in 0..1_000 {
        storage.append_audit(&audit_entry(index))?;
    }

    let entries = storage.query_audit(None, 0, u64::MAX, 1_000)?;
    assert_eq!(entries.len(), 1_000);
    for (index, entry) in entries.iter().enumerate() {
        assert_eq!(entry.request_id, format!("req-{index:04}"));
    }

    let limited = storage.query_audit(Some("alice"), 1_700_000_050, u64::MAX, 7)?;
    assert_eq!(limited.len(), 7);
    assert!(limited.iter().all(|entry| entry.principal_id == "alice"));
    assert!(limited.iter().all(|entry| entry.ts >= 1_700_000_050));

    let pruned = storage.prune_audit(1_700_000_050)?;
    assert_eq!(pruned, 500);
    let remaining = storage.query_audit(None, 0, u64::MAX, 1_000)?;
    assert_eq!(remaining.len(), 500);
    assert_eq!(remaining[0].request_id, "req-0500");

    Ok(())
}

fn audit_entry(index: usize) -> AuditEntry {
    AuditEntry {
        ts: 1_700_000_000 + (index / 10) as u64,
        request_id: format!("req-{index:04}"),
        principal_id: if index.is_multiple_of(2) { "alice" } else { "bob" }.to_owned(),
        route: "messages".to_owned(),
        upstream: "anthropic_direct".to_owned(),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: index as u64,
        output_tokens: (index * 2) as u64,
        duration_ms: 25,
        agent_label: Some("test-agent".to_owned()),
        kind: None,
        payload: None,
    }
}
