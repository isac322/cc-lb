use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{RequestEvent, RequestEventStore};

use crate::harness::{ConformanceBackend, with_conformance_fixture};

const KEY_SEQUENCE_SCALE: u64 = 1_000_000;
const BASE_MS: u64 = 1_800_720_000_000;

pub async fn strict_millisecond_boundary_and_batch_limit<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let events = [
            event("prune-before-second", BASE_MS),
            event("prune-before-subsecond", BASE_MS + 499),
            event("prune-at-cutoff", BASE_MS + 500),
            event("prune-after-cutoff", BASE_MS + 501),
        ];
        for event in &events {
            storage.append_request_event(event).await?;
        }

        let cutoff = (BASE_MS + 500).saturating_mul(KEY_SEQUENCE_SCALE);
        ensure!(
            storage.prune_request_events_before(cutoff, 0).await? == 0,
            "a zero-sized request-event prune batch must be a no-op"
        );
        ensure!(
            storage.prune_request_events_before(cutoff, 1).await? == 1,
            "the first request-event prune batch must remove one oldest row"
        );
        ensure!(
            storage.prune_request_events_before(cutoff, 10).await? == 1,
            "the second request-event prune batch must remove the remaining row below the cutoff"
        );

        let remaining = storage.query_request_events(0, u64::MAX, 10).await?;
        ensure!(
            remaining == vec![events[2].clone(), events[3].clone()],
            "request-event pruning must retain the row exactly at the millisecond cutoff and newer rows"
        );
        ensure!(
            storage.prune_request_events_before(cutoff, 10).await? == 0,
            "request-event pruning must report zero when no older rows remain"
        );
        Ok(())
    })
    .await
}

fn event(event_id: &str, ts_ms: u64) -> RequestEvent {
    RequestEvent {
        ts: ts_ms / 1_000,
        ts_ms: Some(ts_ms),
        request_id: format!("request-{event_id}"),
        event_id: Some(event_id.to_owned()),
        principal_id: Some("prune-principal".to_owned()),
        key_id: Some("prune-key".to_owned()),
        status: 200,
        duration_ms: 1,
        ..Default::default()
    }
}
