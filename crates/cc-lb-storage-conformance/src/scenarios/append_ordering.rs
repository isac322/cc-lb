//! Append ordering conformance scenarios.

use std::{future::Future, sync::Arc};

use anyhow::Result;
use cc_lb_storage_api::{AuditEntry, AuditStore as _, RequestEvent, RequestEventStore as _};

use crate::harness::{ConformanceBackend, ConformanceFixture};

const AUDIT_BASE_TS: u64 = 1_700_000_000;
const REQUEST_EVENT_BASE_TS: u64 = 1_800_000_000;

pub async fn audit_append_order<B: ConformanceBackend>(backend: Arc<B>) -> Result<()> {
    with_fixture(backend, |storage| async move {
        for index in 0..1_000 {
            storage.append_audit(&audit_entry(index)).await?;
        }

        let entries = storage.query_audit(None, 0, u64::MAX, 1_000).await?;
        assert_eq!(entries.len(), 1_000);
        for (index, entry) in entries.iter().enumerate() {
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
            .query_audit(Some("alice"), AUDIT_BASE_TS + 5, u64::MAX, 7)
            .await?;
        let request_ids = entries
            .iter()
            .map(|entry| entry.request_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            request_ids,
            vec![
                "req-0050", "req-0052", "req-0054", "req-0056", "req-0058", "req-0060", "req-0062",
            ]
        );
        assert!(entries.iter().all(|entry| entry.principal_id == "alice"));
        assert!(entries.iter().all(|entry| entry.ts >= AUDIT_BASE_TS + 5));

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
        let remaining = storage.query_audit(None, 0, u64::MAX, 1_000).await?;

        assert_eq!(pruned, 500);
        assert_eq!(remaining.len(), 500);
        assert_eq!(remaining[0].request_id, "req-0500");
        assert!(remaining.iter().all(|entry| entry.ts >= AUDIT_BASE_TS + 50));

        Ok(())
    })
    .await
}

pub async fn request_event_append_order<B: ConformanceBackend>(backend: Arc<B>) -> Result<()> {
    with_fixture(backend, |storage| async move {
        for index in 0..100 {
            storage.append_request_event(&request_event(index)).await?;
        }

        let events = storage
            .query_request_events(REQUEST_EVENT_BASE_TS + 2, u64::MAX, 5)
            .await?;

        assert_eq!(events.len(), 5);
        assert_eq!(events[0].request_id, "req-0020");
        assert_eq!(events[4].request_id, "req-0024");
        assert!(
            events
                .iter()
                .all(|event| event.ts >= REQUEST_EVENT_BASE_TS + 2)
        );

        Ok(())
    })
    .await
}

pub async fn request_event_recent_descending<B: ConformanceBackend>(backend: Arc<B>) -> Result<()> {
    with_fixture(backend, |storage| async move {
        for index in 0..100 {
            storage.append_request_event(&request_event(index)).await?;
        }

        let events = storage.query_recent_request_events(0, u64::MAX, 5).await?;

        assert_eq!(events.len(), 5);
        assert_eq!(events[0].request_id, "req-0099");
        assert_eq!(events[4].request_id, "req-0095");
        assert!(events.windows(2).all(|w| w[0].ts >= w[1].ts));

        let in_range = storage
            .query_recent_request_events(REQUEST_EVENT_BASE_TS + 5, REQUEST_EVENT_BASE_TS + 6, 100)
            .await?;
        assert_eq!(in_range.len(), 20);
        assert_eq!(in_range[0].request_id, "req-0069");
        assert_eq!(in_range[19].request_id, "req-0050");

        let empty_range = storage
            .query_recent_request_events(REQUEST_EVENT_BASE_TS + 4, REQUEST_EVENT_BASE_TS + 3, 100)
            .await?;
        assert!(empty_range.is_empty());

        let zero_limit = storage.query_recent_request_events(0, u64::MAX, 0).await?;
        assert!(zero_limit.is_empty());

        Ok(())
    })
    .await
}

pub async fn request_event_time_range<B: ConformanceBackend>(backend: Arc<B>) -> Result<()> {
    with_fixture(backend, |storage| async move {
        for index in 0..50 {
            storage.append_request_event(&request_event(index)).await?;
        }

        let events = storage
            .query_request_events(REQUEST_EVENT_BASE_TS + 2, REQUEST_EVENT_BASE_TS + 3, 100)
            .await?;
        assert_eq!(events.len(), 20);
        assert_eq!(events[0].request_id, "req-0020");
        assert_eq!(events[19].request_id, "req-0039");
        assert!(events.iter().all(|event| {
            (REQUEST_EVENT_BASE_TS + 2..=REQUEST_EVENT_BASE_TS + 3).contains(&event.ts)
        }));

        let empty_range = storage
            .query_request_events(REQUEST_EVENT_BASE_TS + 4, REQUEST_EVENT_BASE_TS + 3, 100)
            .await?;
        assert!(empty_range.is_empty());

        let empty_limit = storage
            .query_request_events(REQUEST_EVENT_BASE_TS, REQUEST_EVENT_BASE_TS + 4, 0)
            .await?;
        assert!(empty_limit.is_empty());

        Ok(())
    })
    .await
}

pub async fn run_all<B: ConformanceBackend>(backend: Arc<B>) -> Result<()> {
    audit_append_order(Arc::clone(&backend)).await?;
    audit_principal_filter(Arc::clone(&backend)).await?;
    audit_prune(Arc::clone(&backend)).await?;
    request_event_append_order(Arc::clone(&backend)).await?;
    request_event_time_range(Arc::clone(&backend)).await?;
    request_event_recent_descending(backend).await?;

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
