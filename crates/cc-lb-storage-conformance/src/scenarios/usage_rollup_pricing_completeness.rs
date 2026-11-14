use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    PricingCompleteness, PricingCompletenessInput, RequestEvent, RequestEventKind,
    RequestEventStore, UsageRollup, UsageRollupResolution, UsageRollupStore,
    derive_pricing_completeness,
};

use crate::harness::{ConformanceBackend, ConformanceFixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    untouched_legacy_history(Arc::clone(&backend)).await?;
    old_and_new_worker_gaps(Arc::clone(&backend)).await?;
    recreated_bucket_is_forward_only(Arc::clone(&backend)).await?;
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
        raw.wait_for_events_visible_for_rollup(storage.as_ref())
            .await?;
        let (first, concurrent) =
            tokio::join!(storage.rollup_usage_once(), storage.rollup_usage_once(),);
        ensure!(first?.processed_events + concurrent?.processed_events == events.len() as u64);
        let high_water = storage.usage_rollup_checkpoint().await?;
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let rows = storage
                .query_usage_rollups_in_range(resolution, start, start + 3600)
                .await?;
            ensure!(
                rows.len() == events.len(),
                "missing {resolution:?} series: {rows:?}"
            );
            for (event, counts, cost) in &events {
                let row = rows
                    .iter()
                    .find(|row| row.model == event.model.as_deref().unwrap())
                    .unwrap();
                ensure!(
                    (
                        row.priced_event_count,
                        row.unpriced_event_count,
                        row.unmetered_event_count
                    ) == *counts,
                    "classification mismatch: {row:?}"
                );
                ensure!(
                    row.virtual_cost_micros == *cost,
                    "negative cost must clamp without losing priced classification"
                );
                ensure!(
                    row.priced_event_count + row.unpriced_event_count + row.unmetered_event_count
                        <= row.request_count
                );
            }
        }
        // Re-appending an already committed event must not add totals or evidence.
        for (event, _, _) in &events {
            storage.append_request_event(event).await?;
        }
        raw.wait_for_events_visible_for_rollup(storage.as_ref())
            .await?;
        ensure!(storage.rollup_usage_once().await?.processed_events == 0);
        ensure!(storage.usage_rollup_checkpoint().await? == high_water);
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let rows = storage
                .query_usage_rollups_in_range(resolution, start, start + 3600)
                .await?;
            for (event, counts, cost) in &events {
                let row = rows
                    .iter()
                    .find(|row| row.model == event.model.as_deref().unwrap())
                    .unwrap();
                ensure!(row.request_count == 1);
                ensure!(
                    (
                        row.priced_event_count,
                        row.unpriced_event_count,
                        row.unmetered_event_count
                    ) == *counts
                );
                ensure!(row.virtual_cost_micros == *cost);
            }
        }
        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

async fn untouched_legacy_history<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    for retention in ["retained", "interior-hole", "pruned"] {
        let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
        let result: Result<()> = async {
            let storage = fixture.storage();
            let start = 1_800_000_000;
            for (index, name) in ["zero", "positive", "native", "tokens"].into_iter().enumerate() {
                let mut historical = event(name, start + index as u64 * 60);
                match name {
                    "zero" => historical.cost_usd_micros = Some(0),
                    "positive" => historical.cost_usd_micros = Some(7),
                    "native" => historical.cache_write_input_tokens_12h = Some(5),
                    _ => historical.input_tokens = Some(3),
                }
                storage.append_request_event(&historical).await?;
            }
            backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
            ensure!(storage.rollup_usage_once().await?.processed_events == 4);
            backend.execute_raw_sql(storage.as_ref(),
                "UPDATE usage_rollups_v2 SET priced_event_count=0, unpriced_event_count=0, unmetered_event_count=0").await?;
            // Old schemas did not retain native dimensions. Retained raw evidence
            // is deliberately stronger than the aggregate and must not repair it.
            backend.execute_raw_sql(storage.as_ref(),
                "UPDATE usage_rollups_v2 SET cache_write_input_tokens=NULL, \
                 cache_write_input_tokens_30m=NULL, cache_write_input_tokens_12h=NULL, \
                 effective_cache_write_input_tokens=NULL WHERE model='native'").await?;
            if retention == "interior-hole" {
                ensure!(backend.execute_raw_sql(storage.as_ref(),
                    "DELETE FROM request_events_v1 WHERE model='positive'").await? == 1);
            } else if retention == "pruned" {
                ensure!(storage.prune_request_events_before((start + 3600) * 1_000_000_000, 100).await? == 4);
            }
            let checkpoint = storage.usage_rollup_checkpoint().await?;
            let before_minutes = storage.query_usage_rollups_in_range(
                UsageRollupResolution::Minute, start, start + 3600).await?;
            let before_hours = storage.query_usage_rollups_in_range(
                UsageRollupResolution::Hour, start, start + 3600).await?;
            for _ in 0..3 {
                ensure!(storage.rollup_usage_once().await?.processed_events == 0);
                ensure!(storage.usage_rollup_checkpoint().await? == checkpoint);
                for (resolution, before) in [
                    (UsageRollupResolution::Minute, &before_minutes),
                    (UsageRollupResolution::Hour, &before_hours),
                ] {
                    let rows = storage.query_usage_rollups_in_range(resolution, start, start + 3600).await?;
                    ensure!(rows.len() == before.len());
                    for row in &rows {
                        let old = before.iter().find(|old| old.model == row.model).unwrap();
                        ensure!(evidence(row) == evidence(old), "{retention}: idle tick changed history: {row:?}");
                        let expected = if row.model == "positive" {
                            PricingCompleteness::Partial
                        } else {
                            PricingCompleteness::Unknown
                        };
                        ensure!(completeness(row) == Some(expected), "{retention}: {row:?}");
                        if row.model == "native" {
                            ensure!(row.effective_cache_write_input_tokens.is_none());
                            ensure!(row.effective_cache_write_tokens() == 0);
                        }
                    }
                }
            }
            Ok(())
        }.await;
        let teardown = fixture.teardown().await;
        result?;
        teardown?;
    }
    Ok(())
}

async fn old_and_new_worker_gaps<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let start = 1_800_000_000;
        for name in ["priced-zero", "unpriced", "unmetered", "positive-subtotal"] {
            let mut old = event(&format!("old-{name}"), start);
            old.model = Some(name.to_owned());
            old.input_tokens = Some(0);
            storage.append_request_event(&old).await?;
        }
        backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
        ensure!(storage.rollup_usage_once().await?.processed_events == 4);
        backend.execute_raw_sql(storage.as_ref(),
            "UPDATE usage_rollups_v2 SET priced_event_count=0, unpriced_event_count=0, unmetered_event_count=0").await?;
        for name in ["priced-zero", "unpriced", "unmetered", "positive-subtotal"] {
            let mut live = event(&format!("new-{name}"), start);
            live.model = Some(name.to_owned());
            match name {
                "priced-zero" => live.cost_usd_micros = Some(0),
                "unpriced" => live.cache_write_input_tokens_30m = Some(5),
                "positive-subtotal" => live.cost_usd_micros = Some(7),
                _ => live.input_tokens = Some(0),
            }
            storage.append_request_event(&live).await?;
        }
        backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
        ensure!(storage.rollup_usage_once().await?.processed_events == 4);
        // A later old worker adds ordinary totals without classification evidence.
        backend.execute_raw_sql(storage.as_ref(),
            "UPDATE usage_rollups_v2 SET request_count=request_count+1").await?;
        for _ in 0..3 {
            ensure!(storage.rollup_usage_once().await?.processed_events == 0);
        }
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let rows = storage.query_usage_rollups_in_range(resolution, start, start + 3600).await?;
            ensure!(rows.len() == 4);
            for row in &rows {
                let (counts, expected, cost) = match row.model.as_str() {
                    "priced-zero" => ((1, 0, 0), PricingCompleteness::Partial, 0),
                    "unpriced" => ((0, 1, 0), PricingCompleteness::Unknown, 0),
                    "unmetered" => ((0, 0, 1), PricingCompleteness::Unknown, 0),
                    _ => ((1, 0, 0), PricingCompleteness::Partial, 7),
                };
                ensure!(row.request_count == 3, "{row:?}");
                ensure!((row.priced_event_count, row.unpriced_event_count, row.unmetered_event_count) == counts, "{row:?}");
                ensure!(row.virtual_cost_micros == cost);
                ensure!(completeness(row) == Some(expected), "{row:?}");
            }
        }
        Ok(())
    }.await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

async fn recreated_bucket_is_forward_only<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    for (old_name, new_name) in [("a", "z"), ("z", "a")] {
        let mut fixture = ConformanceFixture::new(Arc::clone(&backend)).await?;
        let result: Result<()> = async {
            let storage = fixture.storage();
            let start = 1_800_000_000;
            let mut historical = event("before-prune", start);
            historical.model = Some("recreated".to_owned());
            historical.upstream_id = Some(uuid::Uuid::new_v4());
            historical.upstream_name = Some(old_name.to_owned());
            historical.cost_usd_micros = Some(7);
            storage.append_request_event(&historical).await?;
            backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
            storage.rollup_usage_once().await?;
            backend.execute_raw_sql(storage.as_ref(),
                "UPDATE usage_rollups_v2 SET priced_event_count=0, unpriced_event_count=0, unmetered_event_count=0").await?;
            ensure!(storage.prune_usage_rollups_before(start + 60, start, 100).await? == 1);
            let mut live = event("after-prune", start);
            live.model = historical.model.clone();
            live.upstream_id = historical.upstream_id;
            live.upstream_name = Some(new_name.to_owned());
            live.cache_write_input_tokens_12h = Some(5);
            storage.append_request_event(&live).await?;
            backend.wait_for_events_visible_for_rollup(storage.as_ref()).await?;
            ensure!(storage.rollup_usage_once().await?.processed_events == 1);
            for _ in 0..3 {
                ensure!(storage.rollup_usage_once().await?.processed_events == 0);
            }
            for (resolution, requests, cost, expected) in [
                (UsageRollupResolution::Minute, 1, 0, PricingCompleteness::Unpriced),
                (UsageRollupResolution::Hour, 2, 7, PricingCompleteness::Partial),
            ] {
                let rows = storage.query_usage_rollups_in_range(resolution, start, start + 3600).await?;
                ensure!(rows.len() == 1, "{old_name}->{new_name}: {rows:?}");
                let row = &rows[0];
                ensure!(row.request_count == requests && row.virtual_cost_micros == cost, "{row:?}");
                ensure!((row.priced_event_count, row.unpriced_event_count, row.unmetered_event_count) == (0, 1, 0), "{row:?}");
                ensure!(completeness(row) == Some(expected), "{row:?}");
            }
            while storage.prune_usage_rollups_before(start + 3600, start + 3600, 100).await? > 0 {}
            ensure!(storage.rollup_usage_once().await?.processed_events == 0);
            for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
                ensure!(storage.query_usage_rollups_in_range(resolution, start, start + 3600).await?.is_empty(),
                    "retained raw events must not resurrect pruned totals");
            }
            Ok(())
        }.await;
        let teardown = fixture.teardown().await;
        result?;
        teardown?;
    }
    Ok(())
}

fn evidence(row: &UsageRollup) -> ([u64; 9], [Option<u64>; 4]) {
    (
        [
            row.request_count,
            row.priced_event_count,
            row.unpriced_event_count,
            row.unmetered_event_count,
            row.input_tokens,
            row.output_tokens,
            row.cache_creation_input_tokens,
            row.cache_read_input_tokens,
            row.virtual_cost_micros,
        ],
        [
            row.cache_write_input_tokens,
            row.cache_write_input_tokens_30m,
            row.cache_write_input_tokens_12h,
            row.effective_cache_write_input_tokens,
        ],
    )
}

fn completeness(row: &UsageRollup) -> Option<PricingCompleteness> {
    derive_pricing_completeness(PricingCompletenessInput {
        request_count: row.request_count,
        priced_event_count: row.priced_event_count,
        unpriced_event_count: row.unpriced_event_count,
        unmetered_event_count: row.unmetered_event_count,
        virtual_cost_micros: row.virtual_cost_micros,
    })
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
