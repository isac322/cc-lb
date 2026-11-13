use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    RequestEvent, RequestEventKind, RequestEventStore, UsageRollupResolution, UsageRollupStore,
};

use crate::harness::{ConformanceBackend, ConformanceFixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    recreated_bucket_backfill(Arc::clone(&backend)).await?;
    let raw = Arc::clone(&backend);
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let start = 1_800_000_000;
        let mut events = Vec::new();
        for (name, cost) in [("positive", 5), ("zero", 0), ("negative", -7)] {
            let mut event = event(name, start);
            event.input_tokens = Some(1);
            event.cost_usd_micros = Some(cost);
            events.push((event, (1, 0, 0), cost.max(0) as u64));
        }
        for index in 0..7 {
            let mut event = event(&format!("unpriced-{index}"), start);
            match index {
                0 => event.input_tokens = Some(1),
                1 => event.output_tokens = Some(1),
                2 => event.cache_creation_input_tokens = Some(1),
                3 => event.cache_read_input_tokens = Some(1),
                4 => event.cache_write_input_tokens = Some(1),
                5 => event.cache_write_input_tokens_30m = Some(1),
                _ => event.cache_write_input_tokens_12h = Some(1),
            }
            if index >= 4 {
                event.source_kind = Some("cache_keepalive".to_owned());
                event.event_kind = Some(RequestEventKind::Renewal);
            }
            events.push((event, (0, 1, 0), 0));
        }
        for (index, kind, status) in [
            (0, RequestEventKind::CountTokens, 200),
            (1, RequestEventKind::Models, 200),
            (2, RequestEventKind::Messages, 401),
            (3, RequestEventKind::Messages, 403),
            (4, RequestEventKind::Messages, 404),
            (5, RequestEventKind::Messages, 429),
        ] {
            let mut event = event(&format!("unmetered-{index}"), start);
            event.event_kind = Some(kind);
            event.status = status;
            event.input_tokens = Some(0);
            events.push((event, (0, 0, 1), 0));
        }
        let mut explicit_zero = event("explicit-zero-no-tokens", start);
        explicit_zero.cost_usd_micros = Some(0);
        events.push((explicit_zero, (1, 0, 0), 0));
        for (event, _, _) in &events {
            storage.append_request_event(event).await?;
        }
        raw.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
        storage.rollup_usage_once().await?;
        let high_water = storage.usage_rollup_checkpoint().await?;
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let rows = storage.query_usage_rollups_in_range(resolution, start, start + 3600).await?;
            ensure!(rows.len() == events.len(), "missing {resolution:?} series: {rows:?}");
            for (event, counts, cost) in &events {
                let row = rows.iter().find(|row| row.model == event.model.as_deref().unwrap()).unwrap();
                ensure!((row.priced_event_count, row.unpriced_event_count, row.unmetered_event_count) == *counts, "classification mismatch: {row:?}");
                ensure!(row.virtual_cost_micros == *cost, "negative cost must clamp without losing priced classification");
                ensure!(row.priced_event_count + row.unpriced_event_count + row.unmetered_event_count <= row.request_count);
            }
        }
        // Historical backfill has its own revision and leaves request/cost totals unchanged.
        raw.execute_raw_sql(storage.as_ref(), "UPDATE usage_rollups_v2 SET priced_event_count=0, unpriced_event_count=0, unmetered_event_count=0").await?;
        raw.execute_raw_sql(storage.as_ref(), "DELETE FROM usage_rollup_checkpoints_v1 WHERE id IN ('pricing_completeness_backfill_upper','pricing_completeness_backfill_cursor')").await?;
        let before = storage.usage_rollup_revision().await?;
        let run = storage.rollup_usage_once().await?;
        ensure!(run.processed_events == 0);
        ensure!(storage.usage_rollup_checkpoint().await? == high_water);
        ensure!(storage.usage_rollup_revision().await? != before, "backfill-only work must invalidate ETags");
        let revision = storage.usage_rollup_revision().await?;
        storage.rollup_usage_once().await?;
        ensure!(storage.usage_rollup_revision().await? == revision, "completed backfill must be idempotent");
        // Freeze the upper cursor then append live work: no event belongs to both ranges.
        raw.execute_raw_sql(storage.as_ref(), "UPDATE usage_rollups_v2 SET priced_event_count=0, unpriced_event_count=0, unmetered_event_count=0").await?;
        raw.execute_raw_sql(storage.as_ref(), "DELETE FROM usage_rollup_checkpoints_v1 WHERE id IN ('pricing_completeness_backfill_upper','pricing_completeness_backfill_cursor')").await?;
        let mut live = event("positive-live", start);
        live.model = Some("positive".to_owned());
        live.cost_usd_micros = Some(11);
        storage.append_request_event(&live).await?;
        raw.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
        storage.rollup_usage_once().await?;
        storage.rollup_usage_once().await?;
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let rows = storage.query_usage_rollups_in_range(resolution, start, start + 3600).await?;
            for (event, counts, cost) in &events {
                let row = rows.iter().find(|row| row.model == event.model.as_deref().unwrap()).unwrap();
                let extra = u64::from(row.model == "positive");
                ensure!(row.request_count == 1 + extra);
                ensure!((row.priced_event_count, row.unpriced_event_count, row.unmetered_event_count) == (counts.0 + extra, counts.1, counts.2));
                ensure!(row.virtual_cost_micros == cost + extra * 11);
            }
        }
        // Retained events cannot resurrect rollups removed by retention.
        raw.execute_raw_sql(storage.as_ref(), "DELETE FROM usage_rollup_checkpoints_v1 WHERE id IN ('pricing_completeness_backfill_upper','pricing_completeness_backfill_cursor')").await?;
        while storage.prune_usage_rollups_before(start + 3600, start + 3600, 100).await? > 0 {}
        storage.rollup_usage_once().await?;
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            ensure!(storage.query_usage_rollups_in_range(resolution, start, start + 3600).await?.is_empty());
        }
        Ok(())
    }.await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

async fn recreated_bucket_backfill<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    for backfill_after_recreation in [false, true] {
        let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
        let result: Result<()> = async {
            let storage = fixture.storage();
            let start = 1_800_000_000;
            let mut historical = event("historical", start);
            historical.model = Some("recreated".to_owned());
            historical.input_tokens = Some(1);
            storage.append_request_event(&historical).await?;
            backend
                .wait_for_events_visible_for_rollup(storage.as_ref())
                .await?;
            storage.rollup_usage_once().await?;

            let initial = storage
                .query_usage_rollups_in_range(UsageRollupResolution::Minute, start, start + 60)
                .await?;
            ensure!(initial.len() == 1, "historical minute must exist");
            ensure!(
                (initial[0].request_count, initial[0].unpriced_event_count) == (1, 1),
                "historical event must begin unpriced: {initial:?}"
            );
            ensure!(
                storage.prune_usage_rollups_before(start + 60, start, 100).await? == 1,
                "prune only the minute while retaining the raw event and hour"
            );

            if !backfill_after_recreation {
                // Simulate an upgrade with a stalled historical backfill before live work.
                backend.execute_raw_sql(storage.as_ref(), "UPDATE usage_rollups_v2 SET priced_event_count=0, unpriced_event_count=0, unmetered_event_count=0").await?;
                backend.execute_raw_sql(storage.as_ref(), "DELETE FROM usage_rollup_checkpoints_v1 WHERE id IN ('pricing_completeness_backfill_upper','pricing_completeness_backfill_cursor')").await?;
            }
            let mut live = event("live", start);
            live.model = historical.model.clone();
            live.input_tokens = Some(1);
            live.cost_usd_micros = Some(5);
            storage.append_request_event(&live).await?;
            backend
                .wait_for_events_visible_for_rollup(storage.as_ref())
                .await?;
            let run = storage.rollup_usage_once().await?;
            ensure!(run.processed_events == 1, "only the live event is new");

            if backfill_after_recreation {
                let recreated = storage
                    .query_usage_rollups_in_range(UsageRollupResolution::Minute, start, start + 60)
                    .await?;
                ensure!(recreated.len() == 1);
                ensure!(
                    (recreated[0].request_count, recreated[0].priced_event_count) == (1, 1),
                    "live work must recreate a fully classified minute: {recreated:?}"
                );
                // Leave the recreated minute classified, but expose legacy remainder in the hour.
                backend.execute_raw_sql(storage.as_ref(), "UPDATE usage_rollups_v2 SET priced_event_count=0, unpriced_event_count=0, unmetered_event_count=0 WHERE resolution='hour'").await?;
                backend.execute_raw_sql(storage.as_ref(), "DELETE FROM usage_rollup_checkpoints_v1 WHERE id IN ('pricing_completeness_backfill_upper','pricing_completeness_backfill_cursor')").await?;
                let run = storage.rollup_usage_once().await?;
                ensure!(run.processed_events == 0, "later work must be backfill-only");
                ensure!(run.updated_rollups > 0, "historical backfill must run");
            }

            for (resolution, expected) in [
                (UsageRollupResolution::Minute, (1, 1, 0, 0)),
                (UsageRollupResolution::Hour, (2, 1, 1, 0)),
            ] {
                let rows = storage
                    .query_usage_rollups_in_range(resolution, start, start + 3600)
                    .await?;
                ensure!(rows.len() == 1, "missing {resolution:?} bucket: {rows:?}");
                let row = &rows[0];
                ensure!(
                    (
                        row.request_count,
                        row.priced_event_count,
                        row.unpriced_event_count,
                        row.unmetered_event_count,
                    ) == expected,
                    "backfill_after_recreation={backfill_after_recreation}, {resolution:?}: {row:?}"
                );
            }
            Ok(())
        }
        .await;
        let teardown = fixture.teardown().await;
        result?;
        teardown?;
    }
    Ok(())
}

fn event(name: &str, ts: u64) -> RequestEvent {
    RequestEvent {
        ts,
        ts_ms: Some(ts * 1000),
        request_id: format!("pricing-{name}"),
        event_id: Some(format!("pricing-{name}")),
        model: Some(name.to_owned()),
        status: 200,
        ..RequestEvent::default()
    }
}
