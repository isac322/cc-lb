use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{RequestEvent, RequestEventKeyUsageQuery, RequestEventStore};

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn materialized_key_usage_preserves_bucket_contract<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    with_conformance_fixture(backend, |storage| async move {
        const START_MS: u64 = 1_900_600_000_000;
        const STEP_MS: u64 = 60_000;
        const PRINCIPAL: &str = "key-usage-principal";
        const KEY: &str = "key-usage-key";

        let events = [
            key_usage_event(START_MS, "key-usage-start", PRINCIPAL, KEY, Some(11), 1),
            key_usage_event(
                START_MS + STEP_MS - 1,
                "key-usage-null-cost",
                PRINCIPAL,
                KEY,
                None,
                2,
            ),
            key_usage_event(
                START_MS + STEP_MS,
                "key-usage-step-boundary",
                PRINCIPAL,
                KEY,
                Some(5),
                3,
            ),
            key_usage_event(
                START_MS + 2 * STEP_MS,
                "key-usage-inclusive-end",
                PRINCIPAL,
                KEY,
                Some(7),
                4,
            ),
            key_usage_event(
                START_MS + 2 * STEP_MS + 1,
                "key-usage-after-end",
                PRINCIPAL,
                KEY,
                Some(100),
                5,
            ),
            key_usage_event(
                START_MS + 1,
                "key-usage-other-key",
                PRINCIPAL,
                "other-key",
                Some(100),
                6,
            ),
            key_usage_event(
                START_MS + 1,
                "key-usage-other-principal",
                "other-principal",
                KEY,
                Some(100),
                7,
            ),
        ];
        for event in &events {
            storage.append_request_event(event).await?;
        }

        let buckets = storage
            .request_event_key_usage(&RequestEventKeyUsageQuery {
                principal_id: PRINCIPAL.to_owned(),
                key_id: KEY.to_owned(),
                range_start_ms: START_MS,
                range_end_ms: START_MS + 2 * STEP_MS,
                step_ms: STEP_MS,
                bucket_count: 3,
            })
            .await?;

        ensure!(buckets.len() == 3, "expected exactly three key-usage buckets");
        ensure!(
            buckets[0].bucket_start_unix_secs == START_MS / 1_000
                && buckets[0].request_count == 2
                && buckets[0].input_tokens == 9
                && buckets[0].output_tokens == 6
                && buckets[0].cost_usd_micros == 11,
            "first key-usage bucket did not preserve pre-boundary rows and NULL cost semantics: {:?}",
            buckets[0]
        );
        ensure!(
            buckets[1].bucket_start_unix_secs == (START_MS + STEP_MS) / 1_000
                && buckets[1].request_count == 1
                && buckets[1].input_tokens == 9
                && buckets[1].output_tokens == 6
                && buckets[1].cost_usd_micros == 5,
            "step-boundary event was not assigned to the next key-usage bucket: {:?}",
            buckets[1]
        );
        ensure!(
            buckets[2].bucket_start_unix_secs == (START_MS + 2 * STEP_MS) / 1_000
                && buckets[2].request_count == 1
                && buckets[2].input_tokens == 12
                && buckets[2].output_tokens == 8
                && buckets[2].cost_usd_micros == 7,
            "range_end_ms event was not included in the final key-usage bucket: {:?}",
            buckets[2]
        );
        Ok(())
    })
    .await
}

fn key_usage_event(
    ts_ms: u64,
    event_id: &str,
    principal_id: &str,
    key_id: &str,
    cost_usd_micros: Option<i64>,
    token_seed: u64,
) -> RequestEvent {
    RequestEvent {
        ts: ts_ms / 1_000,
        ts_ms: Some(ts_ms),
        request_id: format!("request-{event_id}"),
        event_id: Some(event_id.to_owned()),
        principal_id: Some(principal_id.to_owned()),
        key_id: Some(key_id.to_owned()),
        input_tokens: Some(token_seed),
        cache_creation_input_tokens: Some(token_seed),
        cache_read_input_tokens: Some(token_seed),
        output_tokens: Some(token_seed * 2),
        cost_usd_micros,
        status: 200,
        ..Default::default()
    }
}
