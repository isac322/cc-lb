//! Append ordering conformance scenarios.

use std::{future::Future, sync::Arc};

use anyhow::Result;
use cc_lb_storage_api::{
    AuditEntry, AuditQueryScope, AuditStore as _, RequestEvent, RequestEventStore as _,
    RequestEventStreamFilters,
};

use crate::harness::{ConformanceBackend, ConformanceFixture};

const AUDIT_BASE_TS: u64 = 1_700_000_000;
const REQUEST_EVENT_BASE_TS: u64 = 1_800_000_000;

pub async fn audit_append_order<B: ConformanceBackend>(backend: Arc<B>) -> Result<()> {
    with_fixture(backend, |storage| async move {
        for index in 0..1_000 {
            storage.append_audit(&audit_entry(index)).await?;
        }

        let entries = storage
            .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 1_000, false)
            .await?;
        assert_eq!(entries.len(), 1_000);
        // Newest first; timestamp ties fall back to reverse append order.
        for (position, entry) in entries.iter().enumerate() {
            let index = 999 - position;
            assert_eq!(entry.request_id, format!("req-{index:04}"));
        }

        Ok(())
    })
    .await
}

pub async fn audit_principal_filter<B: ConformanceBackend>(backend: Arc<B>) -> Result<()> {
    with_fixture(backend, |storage| async move {
        for index in 0..100 {
            storage.append_audit(&audit_entry(index)).await?;
        }

        let entries = storage
            .query_recent_audit(
                AuditQueryScope::Principal("alice"),
                AUDIT_BASE_TS + 5,
                AUDIT_BASE_TS + 6,
                7,
                false,
            )
            .await?;
        let request_ids = entries
            .iter()
            .map(|entry| entry.request_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            request_ids,
            vec![
                "req-0068", "req-0066", "req-0064", "req-0062", "req-0060", "req-0058", "req-0056",
            ]
        );
        assert!(entries.iter().all(|entry| entry.principal_id == "alice"));
        assert!(
            entries
                .iter()
                .all(|entry| { entry.ts >= AUDIT_BASE_TS + 5 && entry.ts <= AUDIT_BASE_TS + 6 })
        );

        Ok(())
    })
    .await
}

pub async fn audit_prune<B: ConformanceBackend>(backend: Arc<B>) -> Result<()> {
    with_fixture(backend, |storage| async move {
        for index in 0..1_000 {
            storage.append_audit(&audit_entry(index)).await?;
        }

        let pruned = storage
            .prune_audit_before((AUDIT_BASE_TS + 50) * 1_000_000, 1_000)
            .await?;
        let remaining = storage
            .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 1_000, false)
            .await?;

        assert_eq!(pruned, 500);
        assert_eq!(remaining.len(), 500);
        assert_eq!(remaining[0].request_id, "req-0999");
        assert_eq!(remaining[remaining.len() - 1].request_id, "req-0500");
        assert!(remaining.iter().all(|entry| entry.ts >= AUDIT_BASE_TS + 50));

        Ok(())
    })
    .await
}

pub async fn request_event_append_order<B: ConformanceBackend>(backend: Arc<B>) -> Result<()> {
    with_fixture(backend, |storage| async move {
        let mut cursors = Vec::with_capacity(100);
        for index in 0..100 {
            cursors.push(storage.append_request_event(&request_event(index)).await?);
        }
        assert!(cursors.windows(2).all(|w| w[0] < w[1]));

        let current = storage.current_request_event_cursor().await?;
        assert_eq!(current, cursors[99]);

        let rows = storage
            .query_request_events_between_cursors(
                cursors[19],
                current,
                5,
                &RequestEventStreamFilters::default(),
            )
            .await?;

        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].1.request_id, "req-0020");
        assert_eq!(rows[4].1.request_id, "req-0024");
        assert_eq!(
            rows.iter().map(|(cursor, _)| *cursor).collect::<Vec<_>>(),
            cursors[20..25].to_vec()
        );
        assert!(
            rows.iter()
                .all(|(_, event)| event.ts >= REQUEST_EVENT_BASE_TS + 2)
        );

        Ok(())
    })
    .await
}

pub async fn run_all<B: ConformanceBackend>(backend: Arc<B>) -> Result<()> {
    audit_append_order(Arc::clone(&backend)).await?;
    audit_principal_filter(Arc::clone(&backend)).await?;
    audit_prune(Arc::clone(&backend)).await?;
    request_event_append_order(backend).await?;

    Ok(())
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
    teardown_result?;

    Ok(())
}

fn audit_entry(index: usize) -> AuditEntry {
    AuditEntry {
        ts: AUDIT_BASE_TS + (index / 10) as u64,
        request_id: format!("req-{index:04}"),
        principal_id: if index.is_multiple_of(2) {
            "alice"
        } else {
            "bob"
        }
        .to_owned(),
        route: "messages".to_owned(),
        upstream: "anthropic_direct".to_owned(),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(index as u64),
        output_tokens: Some((index * 2) as u64),
        duration_ms: 25,
        agent_label: Some("test-agent".to_owned()),
        ..Default::default()
    }
}

fn request_event(index: usize) -> RequestEvent {
    let ts = REQUEST_EVENT_BASE_TS + (index / 10) as u64;
    RequestEvent {
        ts,
        ts_ms: Some(ts * 1_000),
        request_id: format!("req-{index:04}"),
        event_id: Some(format!("event-{index:04}")),
        principal_id: Some("principal-a".to_owned()),
        principal_kind: Some("api_key".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(index as u64),
        output_tokens: Some((index * 2) as u64),
        duration_ms: 25,
        error_code: None,
        ..Default::default()
    }
}
