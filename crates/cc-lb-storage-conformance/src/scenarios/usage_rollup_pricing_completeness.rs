use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    BackendKind, PricingCompleteness, PricingCompletenessInput, RequestEvent, RequestEventKind,
    RequestEventStore, UsageRollupResolution, UsageRollupStore, derive_pricing_completeness,
};

use crate::harness::{ConformanceBackend, ConformanceFixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    recreated_bucket_backfill(Arc::clone(&backend)).await?;
    pre_upgrade_recreated_bucket_backfill(Arc::clone(&backend)).await?;
    pre_upgrade_interior_hole_backfill(Arc::clone(&backend)).await?;
    frozen_upper_mixed_worker_backfill(Arc::clone(&backend)).await?;
    nested_catchup_mixed_worker_backfill(Arc::clone(&backend)).await?;
    upstream_rename_recreated_bucket_backfill(Arc::clone(&backend)).await?;
    partial_retention_interrupted_prune_backfill(Arc::clone(&backend)).await?;
    mixed_old_and_new_writer_effective_cache_writes(Arc::clone(&backend)).await?;
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

async fn pre_upgrade_recreated_bucket_backfill<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    for cross_batch in [false, true] {
        let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
        let result: Result<()> = async {
            let storage = fixture.storage();
            let start = 1_800_000_000;
            let mut historical = event("pre-prune", start);
            historical.model = Some("pre-upgrade-recreated".to_owned());
            historical.input_tokens = Some(10);
            historical.cost_usd_micros = Some(0);
            storage.append_request_event(&historical).await?;
            backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
            storage.rollup_usage_once().await?;
            ensure!(
                storage.prune_usage_rollups_before(start + 60, start, 100).await? == 1,
                "only the historical minute must be pruned"
            );
            if cross_batch {
                let sql = match backend.kind() {
                    BackendKind::Sqlite => {
                        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10000) \
                         INSERT INTO request_events_v1 \
                         (request_id,event_id,ts,list_ts_ms,list_status,list_duration_ms,model) \
                         SELECT 'suffix-filler-'||i,'suffix-filler-'||i,1800003600,1800003600000,200,0,'suffix-filler' FROM n"
                    }
                    BackendKind::Postgres => {
                        "INSERT INTO request_events_v1 \
                         (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,model) \
                         SELECT 'suffix-filler-'||i,'suffix-filler-'||i,to_timestamp(1800003600),1800003600000,200,0,'suffix-filler' \
                         FROM generate_series(1,10000) AS i"
                    }
                };
                ensure!(backend.execute_raw_sql(storage.as_ref(), sql).await? == 10_000);
            }
            let mut recreated = event("post-prune", start);
            recreated.model = historical.model.clone();
            recreated.input_tokens = Some(10);
            recreated.cost_usd_micros = None;
            storage.append_request_event(&recreated).await?;
            backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
            let passes = if cross_batch { 2 } else { 1 };
            let mut processed = 0;
            for _ in 0..passes {
                processed += storage.rollup_usage_once().await?.processed_events;
            }
            ensure!(processed == if cross_batch { 10_001 } else { 1 });
            let high_water = storage.usage_rollup_checkpoint().await?;
            backend.execute_raw_sql(storage.as_ref(), "UPDATE usage_rollups_v2 SET priced_event_count=0, unpriced_event_count=0, unmetered_event_count=0").await?;
            backend.execute_raw_sql(storage.as_ref(), "DELETE FROM usage_rollup_checkpoints_v1 WHERE id IN ('pricing_completeness_backfill_upper','pricing_completeness_backfill_cursor')").await?;
            for batch in 0..passes {
                ensure!(storage.rollup_usage_once().await?.processed_events == 0);
                ensure!(storage.usage_rollup_checkpoint().await? == high_water);
                let rows = storage
                    .query_usage_rollups_in_range(UsageRollupResolution::Minute, start, start + 60)
                    .await?;
                ensure!(rows.len() == 1);
                let row = &rows[0];
                let (expected_minute, expected_state) = if cross_batch
                    && batch + 1 < passes
                    && backend.kind() == BackendKind::Postgres
                {
                    ((1, 0, 0, 0), PricingCompleteness::Unknown)
                } else {
                    ((1, 0, 1, 0), PricingCompleteness::Unpriced)
                };
                ensure!(
                    (row.request_count, row.priced_event_count, row.unpriced_event_count, row.unmetered_event_count) == expected_minute,
                    "cross_batch={cross_batch}, batch={batch}: recreated minute must retain only the post-prune suffix: {row:?}"
                );
                ensure!(
                    derive_pricing_completeness(PricingCompletenessInput {
                        request_count: row.request_count,
                        priced_event_count: row.priced_event_count,
                        unpriced_event_count: row.unpriced_event_count,
                        unmetered_event_count: row.unmetered_event_count,
                        total_tokens: row.input_tokens,
                        virtual_cost_micros: row.virtual_cost_micros,
                    }) == Some(expected_state),
                    "recreated minute must never become known-zero"
                );
            }
            let hours = storage
                .query_usage_rollups_in_range(UsageRollupResolution::Hour, start, start + 3600)
                .await?;
            ensure!(hours.len() == 1);
            let hour = &hours[0];
            ensure!(
                (hour.request_count, hour.priced_event_count, hour.unpriced_event_count, hour.unmetered_event_count) == (2, 1, 1, 0),
                "the surviving hour represents both populations: {hour:?}"
            );
            ensure!(
                derive_pricing_completeness(PricingCompletenessInput {
                    request_count: hour.request_count,
                    priced_event_count: hour.priced_event_count,
                    unpriced_event_count: hour.unpriced_event_count,
                    unmetered_event_count: hour.unmetered_event_count,
                    total_tokens: hour.input_tokens,
                    virtual_cost_micros: hour.virtual_cost_micros,
                }) == Some(PricingCompleteness::Partial)
            );
            Ok(())
        }
        .await;
        let teardown = fixture.teardown().await;
        result?;
        teardown?;
    }
    Ok(())
}
async fn frozen_upper_mixed_worker_backfill<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let start = 1_800_000_000;
        let mut historical = event("mixed-worker-A", start);
        historical.model = Some("mixed-worker-recreated".to_owned());
        historical.cost_usd_micros = Some(0);
        historical.input_tokens = Some(10);
        ensure!(storage.append_request_event(&historical).await? == 1);
        backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
        let historical_fillers = match backend.kind() {
            BackendKind::Sqlite => {
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10000) \
                 INSERT INTO request_events_v1 \
                 (request_id,event_id,ts,list_ts_ms,list_status,list_duration_ms,model) \
                 SELECT 'hist-filler-'||i,'hist-filler-'||i,1800003600,1800003600000,200,0,'hist-filler' FROM n"
            }
            BackendKind::Postgres => {
                "INSERT INTO request_events_v1 \
                 (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,model) \
                 SELECT 'hist-filler-'||i,'hist-filler-'||i,to_timestamp(1800003600),1800003600000,200,0,'hist-filler' \
                 FROM generate_series(1,10000) AS i"
            }
        };
        ensure!(backend.execute_raw_sql(storage.as_ref(), historical_fillers).await? == 10_000);
        backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
        ensure!(storage.rollup_usage_once().await?.processed_events == 10_000);
        ensure!(storage.rollup_usage_once().await?.processed_events == 1);
        ensure!(
            storage.prune_usage_rollups_before(start + 60, start, 100).await? == 1,
            "only A's minute must be pruned"
        );
        backend.execute_raw_sql(
            storage.as_ref(),
            "UPDATE usage_rollups_v2 SET priced_event_count=0,unpriced_event_count=0,unmetered_event_count=0",
        ).await?;
        backend.execute_raw_sql(
            storage.as_ref(),
            "DELETE FROM usage_rollup_checkpoints_v1 WHERE id LIKE 'pricing_completeness_%'",
        ).await?;
        ensure!(storage.rollup_usage_once().await?.processed_events == 0);

        // The frozen historical upper is 10001; A remains below its first 10000-row batch.
        let live_fillers = match backend.kind() {
            BackendKind::Sqlite => {
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10000) \
                 INSERT INTO request_events_v1 \
                 (request_id,event_id,ts,list_ts_ms,list_status,list_duration_ms,model) \
                 SELECT 'live-filler-'||i,'live-filler-'||i,1800007200,1800007200000,200,0,'live-filler' FROM n"
            }
            BackendKind::Postgres => {
                "INSERT INTO request_events_v1 \
                 (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,model) \
                 SELECT 'live-filler-'||i,'live-filler-'||i,to_timestamp(1800007200),1800007200000,200,0,'live-filler' \
                 FROM generate_series(1,10000) AS i"
            }
        };
        ensure!(backend.execute_raw_sql(storage.as_ref(), live_fillers).await? == 10_000);
        let mut recreated = event("mixed-worker-B", start);
        recreated.model = historical.model.clone();
        recreated.cost_usd_micros = None;
        recreated.input_tokens = Some(10);
        ensure!(storage.append_request_event(&recreated).await? == 20_002);
        backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;

        // An old worker accounts for B, but cannot write the new pricing checkpoints.
        let recreate_minute = match backend.kind() {
            BackendKind::Sqlite => {
                "INSERT INTO usage_rollups_v2 \
                 (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,input_tokens,priced_event_count,unpriced_event_count,unmetered_event_count,updated_at) \
                 SELECT 'minute',1800000000,principal_id,upstream_id,upstream_name,model,1,10,0,0,0,unixepoch() \
                 FROM usage_rollups_v2 WHERE resolution='hour' AND bucket_start_unix_secs=1800000000 AND model='mixed-worker-recreated'"
            }
            BackendKind::Postgres => {
                "INSERT INTO usage_rollups_v2 \
                 (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,input_tokens,priced_event_count,unpriced_event_count,unmetered_event_count,updated_at) \
                 SELECT 'minute',1800000000,principal_id,upstream_id,upstream_name,model,1,10,0,0,0,NOW() \
                 FROM usage_rollups_v2 WHERE resolution='hour' AND bucket_start_unix_secs=1800000000 AND model='mixed-worker-recreated'"
            }
        };
        ensure!(backend.execute_raw_sql(storage.as_ref(), recreate_minute).await? == 1);
        ensure!(backend.execute_raw_sql(
            storage.as_ref(),
            "UPDATE usage_rollups_v2 SET request_count=request_count+1,input_tokens=input_tokens+10 \
             WHERE resolution='hour' AND bucket_start_unix_secs=1800000000 AND model='mixed-worker-recreated'",
        ).await? == 1);
        let advance_high_water = match backend.kind() {
            BackendKind::Sqlite => {
                "UPDATE usage_rollup_checkpoints_v1 SET value=(SELECT MAX(id) FROM request_events_v1) WHERE id='high_water'"
            }
            BackendKind::Postgres => {
                "UPDATE usage_rollup_checkpoints_v1 SET (value,value_xid)= \
                 (SELECT seq,tx_id FROM request_events_v1 ORDER BY tx_id DESC,seq DESC LIMIT 1) WHERE id='high_water'"
            }
        };
        ensure!(backend.execute_raw_sql(storage.as_ref(), advance_high_water).await? == 1);
        let high_water = storage.usage_rollup_checkpoint().await?;
        let before = storage.usage_rollup_revision().await?;
        for pass in 0..4 {
            ensure!(storage.rollup_usage_once().await?.processed_events == 0);
            ensure!(storage.usage_rollup_checkpoint().await? == high_water);
            if pass == 0 {
                ensure!(
                    storage.usage_rollup_revision().await? != before,
                    "catchup-only progress must invalidate dashboard ETags"
                );
            }
        }
        for (resolution, expected, completeness) in [
            (
                UsageRollupResolution::Minute,
                (1, 0, 1, 0),
                PricingCompleteness::Unpriced,
            ),
            (
                UsageRollupResolution::Hour,
                (2, 1, 1, 0),
                PricingCompleteness::Partial,
            ),
        ] {
            let rows = storage
                .query_usage_rollups_in_range(resolution, start, start + 3600)
                .await?;
            let row = rows
                .iter()
                .find(|row| row.model == "mixed-worker-recreated")
                .ok_or_else(|| anyhow::anyhow!("missing mixed-worker {resolution:?} bucket"))?;
            ensure!(
                (
                    row.request_count,
                    row.priced_event_count,
                    row.unpriced_event_count,
                    row.unmetered_event_count,
                ) == expected,
                "old-worker catchup must classify B before the older historical A: {row:?}"
            );
            ensure!(
                derive_pricing_completeness(PricingCompletenessInput {
                    request_count: row.request_count,
                    priced_event_count: row.priced_event_count,
                    unpriced_event_count: row.unpriced_event_count,
                    unmetered_event_count: row.unmetered_event_count,
                    total_tokens: row.input_tokens,
                    virtual_cost_micros: row.virtual_cost_micros,
                }) == Some(completeness)
            );
        }
        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

async fn nested_catchup_mixed_worker_backfill<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let start = 1_800_000_000;
        storage.append_request_event(&event("nested-seed", start + 7200)).await?;
        backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
        storage.rollup_usage_once().await?;
        backend.execute_raw_sql(storage.as_ref(),
            "DELETE FROM usage_rollup_checkpoints_v1 WHERE id LIKE 'pricing_completeness_%'").await?;
        storage.rollup_usage_once().await?;

        let mut b1 = event("nested-B1", start);
        b1.model = Some("nested-catchup".to_owned());
        b1.input_tokens = Some(10);
        b1.cost_usd_micros = Some(0);
        ensure!(storage.append_request_event(&b1).await? == 2);
        let fillers = match backend.kind() {
            BackendKind::Sqlite => {
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10000) \
                 INSERT INTO request_events_v1 \
                 (request_id,event_id,ts,list_ts_ms,list_status,list_duration_ms,model,list_cost_usd_micros) \
                 SELECT 'nested-filler-'||i,'nested-filler-'||i,1800003600,1800003600000,200,0,'nested-filler',0 FROM n"
            }
            BackendKind::Postgres => {
                "INSERT INTO request_events_v1 \
                 (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,model,list_cost_usd_micros) \
                 SELECT 'nested-filler-'||i,'nested-filler-'||i,to_timestamp(1800003600),1800003600000,200,0,'nested-filler',0 \
                 FROM generate_series(1,10000) AS i"
            }
        };
        ensure!(backend.execute_raw_sql(storage.as_ref(), fillers).await? == 10_000);
        backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
        ensure!(storage.rollup_usage_once().await?.processed_events == 10_000);
        ensure!(storage.rollup_usage_once().await?.processed_events == 1);
        // Retain the old worker's aggregates/high-water but remove its unavailable
        // pricing writes, leaving the frozen historical upper at U0.
        backend.execute_raw_sql(storage.as_ref(),
            "UPDATE usage_rollups_v2 SET priced_event_count=0,unpriced_event_count=0,unmetered_event_count=0 \
             WHERE model IN ('nested-catchup','nested-filler')").await?;
        let rewind_watermark = match backend.kind() {
            BackendKind::Sqlite =>
                "UPDATE usage_rollup_checkpoints_v1 SET value= \
                 (SELECT value FROM usage_rollup_checkpoints_v1 WHERE id='pricing_completeness_backfill_upper') \
                 WHERE id='pricing_completeness_live_watermark'",
            BackendKind::Postgres =>
                "UPDATE usage_rollup_checkpoints_v1 SET (value,value_xid)= \
                 (SELECT value,value_xid FROM usage_rollup_checkpoints_v1 WHERE id='pricing_completeness_backfill_upper') \
                 WHERE id='pricing_completeness_live_watermark'",
        };
        ensure!(backend.execute_raw_sql(storage.as_ref(), rewind_watermark).await? == 1);
        ensure!(storage.rollup_usage_once().await?.processed_events == 0);
        let pending_b1 = storage.query_usage_rollups_in_range(
            UsageRollupResolution::Minute, start, start + 60).await?;
        ensure!(pending_b1.len() == 1 && pending_b1[0].priced_event_count == 0,
            "the first catchup batch must leave B1 pending");
        ensure!(storage.prune_usage_rollups_before(start + 60, start, 100).await? == 1);

        let mut b2 = event("nested-B2", start);
        b2.model = b1.model.clone();
        b2.input_tokens = Some(10);
        ensure!(storage.append_request_event(&b2).await? == 10_003);
        backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
        let recreate = match backend.kind() {
            BackendKind::Sqlite =>
                "INSERT INTO usage_rollups_v2 \
                 (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,input_tokens,updated_at) \
                 SELECT 'minute',1800000000,principal_id,upstream_id,upstream_name,model,1,10,unixepoch() \
                 FROM usage_rollups_v2 WHERE resolution='hour' AND model='nested-catchup'",
            BackendKind::Postgres =>
                "INSERT INTO usage_rollups_v2 \
                 (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,input_tokens,updated_at) \
                 SELECT 'minute',1800000000,principal_id,upstream_id,upstream_name,model,1,10,NOW() \
                 FROM usage_rollups_v2 WHERE resolution='hour' AND model='nested-catchup'",
        };
        ensure!(backend.execute_raw_sql(storage.as_ref(), recreate).await? == 1);
        ensure!(backend.execute_raw_sql(storage.as_ref(),
            "UPDATE usage_rollups_v2 SET request_count=request_count+1,input_tokens=input_tokens+10 \
             WHERE resolution='hour' AND model='nested-catchup'").await? == 1);
        let advance = match backend.kind() {
            BackendKind::Sqlite =>
                "UPDATE usage_rollup_checkpoints_v1 SET value=(SELECT MAX(id) FROM request_events_v1) WHERE id='high_water'",
            BackendKind::Postgres =>
                "UPDATE usage_rollup_checkpoints_v1 SET (value,value_xid)= \
                 (SELECT seq,tx_id FROM request_events_v1 ORDER BY tx_id DESC,seq DESC LIMIT 1) WHERE id='high_water'",
        };
        ensure!(backend.execute_raw_sql(storage.as_ref(), advance).await? == 1);
        let high_water = storage.usage_rollup_checkpoint().await?;
        let mut drained = false;
        for pass in 0..8 {
            let before = storage.usage_rollup_revision().await?;
            ensure!(storage.rollup_usage_once().await?.processed_events == 0);
            ensure!(storage.usage_rollup_checkpoint().await? == high_water);
            if pass == 0 {
                let rows = storage.query_usage_rollups_in_range(
                    UsageRollupResolution::Minute, start, start + 60).await?;
                ensure!(rows.len() == 1 && rows[0].unpriced_event_count == 1
                    && rows[0].priced_event_count == 0,
                    "nested B2 slot must run before pending B1: {rows:?}");
            }
            if storage.usage_rollup_revision().await? == before {
                drained = true;
                break;
            }
        }
        ensure!(drained, "nested catchup stack must drain");
        for (resolution, expected, completeness) in [
            (UsageRollupResolution::Minute, (1, 0, 1, 0), PricingCompleteness::Unpriced),
            (UsageRollupResolution::Hour, (2, 1, 1, 0), PricingCompleteness::Partial),
        ] {
            let rows = storage.query_usage_rollups_in_range(resolution, start, start + 7200).await?;
            let row = rows.iter().find(|row| row.model == "nested-catchup").unwrap();
            ensure!((row.request_count, row.priced_event_count, row.unpriced_event_count,
                row.unmetered_event_count) == expected, "nested catchup: {row:?}");
            ensure!(derive_pricing_completeness(PricingCompletenessInput {
                request_count: row.request_count,
                priced_event_count: row.priced_event_count,
                unpriced_event_count: row.unpriced_event_count,
                unmetered_event_count: row.unmetered_event_count,
                total_tokens: row.input_tokens,
                virtual_cost_micros: row.virtual_cost_micros,
            }) == Some(completeness));
            let filler = rows.iter().find(|row| row.model == "nested-filler").unwrap();
            ensure!((filler.request_count, filler.priced_event_count, filler.unpriced_event_count,
                filler.unmetered_event_count) == (10_000, 10_000, 0, 0),
                "completed filler batches must not be reapplied: {filler:?}");
        }
        Ok(())
    }.await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

async fn pre_upgrade_interior_hole_backfill<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    for mode in [
        "pre_upgrade_hole",
        "intact_control",
        "durable_marker_control",
    ] {
        let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
        let result: Result<()> = async {
            let storage = fixture.storage();
            let start = 1_800_003_600;
            let filler_sql = match backend.kind() {
                BackendKind::Sqlite => {
                    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<999) \
                     INSERT INTO request_events_v1 \
                     (request_id,event_id,ts,list_ts_ms,list_status,list_duration_ms,model,input_tokens,list_cost_usd_micros) \
                     SELECT 'interior-filler-'||i,'interior-filler-'||i,1800000000,1800000000000,200,0,'filler',1,1 FROM n"
                }
                BackendKind::Postgres => {
                    "INSERT INTO request_events_v1 \
                     (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,model,input_tokens,list_cost_usd_micros) \
                     SELECT 'interior-filler-'||i,'interior-filler-'||i,to_timestamp(1800000000),1800000000000,200,0,'filler',1,1 \
                     FROM generate_series(1,999) AS i"
                }
            };
            ensure!(backend.execute_raw_sql(storage.as_ref(), filler_sql).await? == 999);

            let mut older = event("interior-A", start + 350);
            older.model = Some("interior-target".to_owned());
            older.cost_usd_micros = Some(0);
            older.input_tokens = Some(10);
            ensure!(storage.append_request_event(&older).await? == 1000);
            backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
            storage.rollup_usage_once().await?;
            ensure!(
                storage
                    .query_usage_rollups_in_range(
                        UsageRollupResolution::Hour,
                        start,
                        start + 3600,
                    )
                    .await?
                    .iter()
                    .any(|row| row.model == "interior-target")
            );

            storage
                .prune_usage_rollups_before(start + 360, start + 330, 2000)
                .await?;
            ensure!(
                storage
                    .query_usage_rollups_in_range(
                        UsageRollupResolution::Hour,
                        start,
                        start + 3600,
                    )
                    .await?
                    .iter()
                    .all(|row| row.model != "interior-target")
            );
            ensure!(
                storage
                    .query_usage_rollups_in_range(
                        UsageRollupResolution::Minute,
                        start + 300,
                        start + 360,
                    )
                    .await?
                    .iter()
                    .all(|row| row.model != "interior-target")
            );
            ensure!(
                storage.current_request_event_cursor().await? == 1000,
                "the pre-upgrade raw-prune failure must retain cursors 1..=1000"
            );

            let mut recreated = event("interior-B", start + 340);
            recreated.model = Some("interior-target".to_owned());
            recreated.input_tokens = Some(10);
            ensure!(storage.append_request_event(&recreated).await? == 1001);
            let mut control = event("interior-C", start - 1);
            control.model = Some("interior-other".to_owned());
            control.cost_usd_micros = Some(5);
            control.input_tokens = Some(10);
            ensure!(storage.append_request_event(&control).await? == 1002);
            backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
            storage.rollup_usage_once().await?;

            if mode != "intact_control" {
                ensure!(
                    storage
                        .prune_request_events_before(
                            (start + 345) * 1000 * 1_000_000,
                            1000,
                        )
                        .await?
                        == 1000,
                    "{mode}: raw prune must delete the 999 fillers and B only"
                );
            }
            backend
                .execute_raw_sql(
                    storage.as_ref(),
                    "UPDATE usage_rollups_v2 SET priced_event_count=0, unpriced_event_count=0, unmetered_event_count=0",
                )
                .await?;
            backend
                .execute_raw_sql(
                    storage.as_ref(),
                    "DELETE FROM usage_rollup_checkpoints_v1 WHERE id LIKE 'pricing_completeness_backfill_%'",
                )
                .await?;
            if mode == "pre_upgrade_hole" {
                backend
                    .execute_raw_sql(
                        storage.as_ref(),
                        "DELETE FROM usage_rollup_checkpoints_v1 WHERE id='request_events_prune_cutoff_unix_secs'",
                    )
                    .await?;
            }
            storage.rollup_usage_once().await?;

            if mode != "durable_marker_control" {
                let control_rows = storage
                    .query_usage_rollups_in_range(
                        UsageRollupResolution::Minute,
                        start - 60,
                        start,
                    )
                    .await?;
                let control_row = control_rows
                    .iter()
                    .find(|row| row.model == "interior-other")
                    .ok_or_else(|| anyhow::anyhow!("missing interior-other control row"))?;
                let expected_control = if mode == "intact_control"
                    || backend.kind() == BackendKind::Sqlite
                {
                    (1, 1, 0, 0)
                } else {
                    (1, 0, 0, 0)
                };
                ensure!(
                    (
                        control_row.request_count,
                        control_row.priced_event_count,
                        control_row.unpriced_event_count,
                        control_row.unmetered_event_count,
                    ) == expected_control,
                    "{mode} ({:?}): unexpected interior-other row: {control_row:?}",
                    backend.kind()
                );
            }

            for (resolution, bucket_start) in [
                (UsageRollupResolution::Hour, start),
                (UsageRollupResolution::Minute, start + 300),
            ] {
                let rows = storage
                    .query_usage_rollups_in_range(
                        resolution,
                        bucket_start,
                        bucket_start + if resolution == UsageRollupResolution::Hour {
                            3600
                        } else {
                            60
                        },
                    )
                    .await?;
                let row = rows
                    .iter()
                    .find(|row| row.model == "interior-target")
                    .ok_or_else(|| anyhow::anyhow!("missing interior-target {resolution:?} row"))?;
                let expected = if mode == "intact_control" {
                    (1, 0, 1, 0)
                } else {
                    (1, 0, 0, 0)
                };
                ensure!(
                    (
                        row.request_count,
                        row.priced_event_count,
                        row.unpriced_event_count,
                        row.unmetered_event_count,
                    ) == expected,
                    "{mode}: interior hole classification is unsafe: {row:?}"
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

async fn upstream_rename_recreated_bucket_backfill<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    for (old_name, new_name) in [("a", "z"), ("z", "a")] {
        let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
        let result: Result<()> = async {
            let storage = fixture.storage();
            let start = 1_800_000_000;
            // No upstream spec is inserted: event names must drive identity resolution.
            let upstream_id = uuid::Uuid::new_v4();
            let mut older = event("rename-older", start);
            older.model = Some("renamed".to_owned());
            older.upstream_id = Some(upstream_id);
            older.upstream_name = Some(old_name.to_owned());
            older.input_tokens = Some(10);
            older.cost_usd_micros = Some(0);
            storage.append_request_event(&older).await?;
            backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
            storage.rollup_usage_once().await?;
            ensure!(storage.prune_usage_rollups_before(start + 60, start, 100).await? == 1);

            let mut newer = event("rename-newer", start);
            newer.model = older.model.clone();
            newer.upstream_id = Some(upstream_id);
            newer.upstream_name = Some(new_name.to_owned());
            newer.input_tokens = Some(10);
            storage.append_request_event(&newer).await?;
            backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
            storage.rollup_usage_once().await?;
            backend.execute_raw_sql(storage.as_ref(), "UPDATE usage_rollups_v2 SET priced_event_count=0, unpriced_event_count=0, unmetered_event_count=0").await?;
            backend.execute_raw_sql(storage.as_ref(), "DELETE FROM usage_rollup_checkpoints_v1 WHERE id LIKE 'pricing_completeness_backfill_%'").await?;
            storage.rollup_usage_once().await?;
            for (resolution, expected, completeness) in [
                (UsageRollupResolution::Minute, (1, 0, 1, 0), PricingCompleteness::Unpriced),
                (UsageRollupResolution::Hour, (2, 1, 1, 0), PricingCompleteness::Partial),
            ] {
                let rows = storage.query_usage_rollups_in_range(resolution, start, start + 3600).await?;
                ensure!(rows.len() == 1, "{old_name}->{new_name}: missing {resolution:?}: {rows:?}");
                let row = &rows[0];
                ensure!(
                    (row.request_count, row.priced_event_count, row.unpriced_event_count, row.unmetered_event_count) == expected,
                    "{old_name}->{new_name}: wrong {resolution:?} population: {row:?}"
                );
                ensure!(
                    derive_pricing_completeness(PricingCompletenessInput {
                        request_count: row.request_count,
                        priced_event_count: row.priced_event_count,
                        unpriced_event_count: row.unpriced_event_count,
                        unmetered_event_count: row.unmetered_event_count,
                        total_tokens: row.input_tokens,
                        virtual_cost_micros: row.virtual_cost_micros,
                    }) == Some(completeness)
                );
            }
            Ok(())
        }.await;
        let teardown = fixture.teardown().await;
        result?;
        teardown?;
    }
    Ok(())
}

async fn partial_retention_interrupted_prune_backfill<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    for mode in ["checkpoint", "pre-upgrade", "intact", "fully-pruned"] {
        let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
        let result: Result<()> = async {
            let storage = fixture.storage();
            let start = 1_800_000_000;
            let mut older = event("retained-older", start + 3000);
            older.model = Some("interrupted-retention".to_owned());
            older.input_tokens = Some(10);
            older.cost_usd_micros = Some(0);
            storage.append_request_event(&older).await?;
            backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
            storage.rollup_usage_once().await?;
            ensure!(storage.prune_request_events_before((start + 300) * 1000 * 1_000_000, 100).await? == 0);
            ensure!(storage.prune_usage_rollups_before(start + 300, start + 300, 100).await? == 1);
            let mut newer = event("pruned-newer", start + 600);
            newer.model = older.model.clone();
            newer.input_tokens = Some(10);
            storage.append_request_event(&newer).await?;
            backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
            storage.rollup_usage_once().await?;
            if mode != "intact" {
                ensure!(
                    storage.prune_request_events_before((start + 900) * 1000 * 1_000_000, 100).await? == 1,
                    "{mode}: only the newer-cursor B must be deleted"
                );
                if mode == "fully-pruned" {
                    ensure!(storage.prune_usage_rollups_before(start + 900, start + 900, 100).await? == 2);
                }
            }
            backend.execute_raw_sql(storage.as_ref(), "UPDATE usage_rollups_v2 SET priced_event_count=0, unpriced_event_count=0, unmetered_event_count=0").await?;
            if mode == "pre-upgrade" {
                backend.execute_raw_sql(storage.as_ref(), "DELETE FROM usage_rollup_checkpoints_v1 WHERE id != 'high_water'").await?;
                backend.execute_raw_sql(storage.as_ref(), "DELETE FROM usage_rollups_v2 WHERE resolution = 'minute'").await?;
            } else {
                backend.execute_raw_sql(storage.as_ref(), "DELETE FROM usage_rollup_checkpoints_v1 WHERE id LIKE 'pricing_completeness_backfill_%'").await?;
            }
            storage.rollup_usage_once().await?;
            let hours = storage.query_usage_rollups_in_range(UsageRollupResolution::Hour, start, start + 3600).await?;
            if mode == "fully-pruned" {
                ensure!(hours.is_empty(), "fully-pruned hour must not be resurrected: {hours:?}");
            } else {
                ensure!(hours.len() == 1, "{mode}: recreated hour missing: {hours:?}");
                let row = &hours[0];
                let (expected, completeness) = if mode == "intact" {
                    ((1, 0, 1, 0), PricingCompleteness::Unpriced)
                } else {
                    ((1, 0, 0, 0), PricingCompleteness::Unknown)
                };
                ensure!(
                    (row.request_count, row.priced_event_count, row.unpriced_event_count, row.unmetered_event_count) == expected,
                    "{mode}: an unproven retained A must never replace B: {row:?}"
                );
                ensure!(
                    derive_pricing_completeness(PricingCompletenessInput {
                        request_count: row.request_count,
                        priced_event_count: row.priced_event_count,
                        unpriced_event_count: row.unpriced_event_count,
                        unmetered_event_count: row.unmetered_event_count,
                        total_tokens: row.input_tokens,
                        virtual_cost_micros: row.virtual_cost_micros,
                    }) == Some(completeness)
                );
            }
            Ok(())
        }.await;
        let teardown = fixture.teardown().await;
        result?;
        teardown?;
    }
    Ok(())
}

async fn mixed_old_and_new_writer_effective_cache_writes<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let start = 1_800_000_000;
        let mut auxiliary = event("mixed-writers", start);
        auxiliary.input_tokens = Some(0);
        storage.append_request_event(&auxiliary).await?;
        backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
        storage.rollup_usage_once().await?;
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let rows = storage.query_usage_rollups_in_range(resolution, start, start + 3600).await?;
            ensure!(rows.len() == 1);
            ensure!(
                (rows[0].request_count, rows[0].unmetered_event_count, rows[0].effective_cache_write_input_tokens) == (1, 1, Some(0))
            );
        }
        backend.execute_raw_sql(
            storage.as_ref(),
            "UPDATE usage_rollups_v2 SET request_count = request_count + 1, cache_write_input_tokens_30m = 5",
        ).await?;
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let rows = storage.query_usage_rollups_in_range(resolution, start, start + 3600).await?;
            ensure!(rows.len() == 1);
            let row = &rows[0];
            ensure!(row.effective_cache_write_input_tokens == Some(0));
            ensure!(row.effective_cache_write_tokens() == 5, "raw evidence must override stale Some(0): {row:?}");
            ensure!(
                derive_pricing_completeness(PricingCompletenessInput {
                    request_count: row.request_count,
                    priced_event_count: row.priced_event_count,
                    unpriced_event_count: row.unpriced_event_count,
                    unmetered_event_count: row.unmetered_event_count,
                    total_tokens: row.effective_cache_write_tokens(),
                    virtual_cost_micros: row.virtual_cost_micros,
                }) == Some(PricingCompleteness::Unknown)
            );
        }
        let mut live = event("mixed-writers-live", start);
        live.model = auxiliary.model.clone();
        live.cache_write_input_tokens_12h = Some(3);
        storage.append_request_event(&live).await?;
        backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
        storage.rollup_usage_once().await?;
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let rows = storage.query_usage_rollups_in_range(resolution, start, start + 3600).await?;
            ensure!(rows.len() == 1);
            let row = &rows[0];
            ensure!(row.effective_cache_write_input_tokens == Some(8), "new writer must self-heal stale effective total: {row:?}");
            ensure!(row.effective_cache_write_tokens() == 8);
        }
        Ok(())
    }.await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
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
