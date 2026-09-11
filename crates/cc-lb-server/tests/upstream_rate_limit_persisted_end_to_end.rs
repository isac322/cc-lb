use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cc_lb_engine::{UpstreamRateLimitSink, observe_rate_limits, start_upstream_rate_limit_writer};
use cc_lb_observability::NoopMetricsHook;
use cc_lb_storage_api::{
    RateLimitKind, StorageResult, UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore,
};
use cc_lb_testkit::fixed_clock;
use http::{HeaderMap, HeaderValue};
use uuid::Uuid;

const NOW_UNIX_SECS: u64 = 1_800_000_000;
const UPSTREAM_ID: Uuid = Uuid::from_u128(1);

#[tokio::test]
async fn t2__upstream_rate_limit_observations_are_persisted_end_to_end() {
    let clock = fixed_clock(NOW_UNIX_SECS);
    let store = Arc::new(RecordingRateLimitStore::default());
    let (sink, receiver) = UpstreamRateLimitSink::with_capacity(4);
    let writer =
        start_upstream_rate_limit_writer(store.clone(), receiver, Arc::new(NoopMetricsHook));

    let records = observe_rate_limits(
        &fixture_headers(),
        UPSTREAM_ID,
        clock
            .now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("fixed clock after epoch")
            .as_secs(),
    );
    assert_eq!(records.len(), 2, "expected request and token observations");
    for record in records {
        sink.enqueue(record)
            .expect("rate-limit observation enqueues");
    }
    drop(sink);
    writer.await.expect("rate-limit writer exits cleanly");

    let records = store
        .list_for_upstream_ids(&[UPSTREAM_ID])
        .await
        .expect("recorded rate-limit observations list");
    assert!(
        records.iter().any(|record| {
            record.upstream_id == UPSTREAM_ID
                && record.kind == RateLimitKind::Requests
                && record.window == "default"
                && record.remaining == Some(999)
                && record.observed_at_unix_secs == NOW_UNIX_SECS
        }),
        "missing persisted requests rate-limit observation: {records:?}"
    );
    assert!(
        records.iter().any(|record| {
            record.upstream_id == UPSTREAM_ID
                && record.kind == RateLimitKind::Tokens
                && record.window == "default"
                && record.remaining == Some(999_000)
                && record.observed_at_unix_secs == NOW_UNIX_SECS
        }),
        "missing persisted tokens rate-limit observation: {records:?}"
    );
}

fn fixture_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "anthropic-ratelimit-requests-remaining",
        HeaderValue::from_static("999"),
    );
    headers.insert(
        "anthropic-ratelimit-tokens-remaining",
        HeaderValue::from_static("999000"),
    );
    headers
}

#[derive(Default)]
struct RecordingRateLimitStore {
    records: Mutex<Vec<UpstreamRateLimitObservationRecord>>,
}

#[async_trait]
impl UpstreamRateLimitStateStore for RecordingRateLimitStore {
    async fn put_observation(
        &self,
        record: &UpstreamRateLimitObservationRecord,
    ) -> StorageResult<()> {
        self.records
            .lock()
            .expect("rate-limit records lock")
            .push(record.clone());
        Ok(())
    }

    async fn list_for_upstream_ids(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>> {
        Ok(self
            .records
            .lock()
            .expect("rate-limit records lock")
            .iter()
            .filter(|record| upstream_ids.contains(&record.upstream_id))
            .cloned()
            .collect())
    }
}
