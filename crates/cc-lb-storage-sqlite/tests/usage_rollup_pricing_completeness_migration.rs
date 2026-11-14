use std::{str::FromStr, sync::Arc};

use cc_lb_storage_api::{
    MetaStore, RequestEvent, RequestEventStore, UsageRollupResolution, UsageRollupStore,
};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};

#[tokio::test]
async fn upgrade_122_retained_events_remain_unclassified_after_idle_restart_and_raw_pruning() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("upgrade.sqlite").display());
    let options = SqliteConnectOptions::from_str(&url)
        .unwrap()
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let migrator = sqlx::migrate!("./migrations");
    migrator
        .run_direct(Some(122), &mut connection, false)
        .await
        .unwrap();
    sqlx::raw_sql(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10001)
         INSERT INTO request_events_v1 (request_id,event_id,ts,list_ts_ms,list_status,list_duration_ms,model,input_tokens,list_cost_usd_micros)
         SELECT 'pricing-'||i,'pricing-'||i,1800000000,1800000000000,200,0,'pricing',
                CASE WHEN i%3=1 THEN 1 ELSE 0 END, CASE WHEN i%3=0 THEN 0 ELSE NULL END FROM n;
         INSERT INTO usage_rollups_v2 (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,input_tokens,virtual_cost_micros,cache_write_input_tokens_30m,updated_at)
         VALUES ('minute',1800000000,'unknown','00000000-0000-0000-0000-000000000000','unknown','pricing',10001,3334,17,13,0),
                ('hour',1800000000,'unknown','00000000-0000-0000-0000-000000000000','unknown','pricing',10001,3334,17,13,0),
                ('minute',1800000060,'legacy','00000000-0000-0000-0000-000000000000','unknown','legacy',1,0,0,NULL,0);
         INSERT INTO usage_rollup_checkpoints_v1 (id,value) VALUES ('high_water',10001),('cost_components_backfill_upper',10001),('cost_components_backfill_cursor',10001);
         UPDATE usage_rollup_reconciliations_v1 SET status='complete';",
    ).execute(&mut connection).await.unwrap();
    migrator
        .run_direct(Some(124), &mut connection, false)
        .await
        .unwrap();
    connection.close().await.unwrap();
    let storage =
        cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap();
    let before = storage.usage_rollup_revision().await.unwrap();
    let snapshot = rows_snapshot(&storage).await;
    assert_eq!(totals(&storage).await, (0, 0, 0));
    let (left, right) = tokio::join!(storage.rollup_usage_once(), storage.rollup_usage_once());
    for run in [left.unwrap(), right.unwrap()] {
        assert_eq!((run.processed_events, run.updated_rollups), (0, 0));
        assert_eq!(run.checkpoint, Some(10001));
    }
    assert_eq!(rows_snapshot(&storage).await, snapshot);
    assert_eq!(storage.usage_rollup_revision().await.unwrap(), before);
    assert_no_pricing_recovery_metadata(&storage).await;
    storage.pool().close().await;
    let storage =
        cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap();
    storage.rollup_usage_once().await.unwrap();
    assert_eq!(rows_snapshot(&storage).await, snapshot);
    // Neither sequence holes nor missing retained raw rows change historical evidence.
    sqlx::query("DELETE FROM request_events_v1 WHERE id IN (1,5000,10001)")
        .execute(storage.pool())
        .await
        .unwrap();
    storage.rollup_usage_once().await.unwrap();
    let cutoff = 1_800_000_060_u64 * 1_000 * 1_000_000;
    assert_eq!(
        storage
            .prune_request_events_before(cutoff, 0)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        storage
            .prune_request_events_before(cutoff, 100)
            .await
            .unwrap(),
        100
    );
    assert_eq!(
        storage
            .prune_request_events_before(cutoff, 10_000)
            .await
            .unwrap(),
        9898
    );
    assert_eq!(
        storage
            .prune_request_events_before(cutoff, 10_000)
            .await
            .unwrap(),
        0
    );
    storage.rollup_usage_once().await.unwrap();
    assert_eq!(rows_snapshot(&storage).await, snapshot);
    assert_eq!(storage.usage_rollup_revision().await.unwrap(), before);
    for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
        let rows = storage
            .query_usage_rollups_in_range(resolution, 1800000000, 1800003600)
            .await
            .unwrap();
        let row = rows.iter().find(|r| r.model == "pricing").unwrap();
        assert_eq!(
            (
                row.request_count,
                row.priced_event_count,
                row.unpriced_event_count,
                row.unmetered_event_count
            ),
            (10001, 0, 0, 0),
        );
        assert_eq!(row.virtual_cost_micros, 17);
        assert_eq!(row.effective_cache_write_input_tokens, None);
    }
    // Fast initialization and incremental migrations share the same additive schema.
    let fresh_url = format!("sqlite://{}", dir.path().join("fresh.sqlite").display());
    let fresh = cc_lb_storage_sqlite::open_sqlite(&fresh_url, Arc::new(cc_lb_clock::SystemClock))
        .await
        .unwrap();
    fresh.initialize().await.unwrap();
    assert_eq!(shape(storage.pool()).await, shape(fresh.pool()).await);
    assert_no_pricing_recovery_metadata(&fresh).await;
    sqlx::query("DROP TABLE usage_rollups_v2")
        .execute(storage.pool())
        .await
        .unwrap();
    assert_eq!(storage.usage_rollup_revision().await.unwrap(), before);
}

#[tokio::test]
async fn concurrent_live_rollups_classify_only_their_own_deltas_and_recreated_minutes() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("live.sqlite").display());
    let storage = fresh_storage(&url).await;
    let old = event("old-worker");
    storage.append_request_event(&old).await.unwrap();
    // An old worker advances totals and high_water but has no classification columns.
    sqlx::raw_sql(
        "INSERT INTO usage_rollups_v2
         (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,
          request_count,virtual_cost_micros,cache_write_input_tokens_30m,effective_cache_write_input_tokens,updated_at)
         VALUES ('minute',1800000000,'unknown','00000000-0000-0000-0000-000000000000','old-name','pricing',1,17,13,5,0),
                ('hour',1800000000,'unknown','00000000-0000-0000-0000-000000000000','old-name','pricing',1,17,13,5,0);
         INSERT INTO usage_rollup_checkpoints_v1 (id,value)
         VALUES ('high_water',1),('cost_components_backfill_upper',1),('cost_components_backfill_cursor',1);",
    )
    .execute(storage.pool())
    .await
    .unwrap();
    let priced = RequestEvent {
        cost_usd_micros: Some(0),
        ..event("priced-zero")
    };
    let unpriced = RequestEvent {
        cache_write_input_tokens_12h: Some(7),
        ..event("unpriced-native")
    };
    let unmetered = event("unmetered");
    for event in [&priced, &unpriced, &unmetered] {
        storage.append_request_event(event).await.unwrap();
    }
    // A duplicate append must not produce another classified delta.
    storage.append_request_event(&priced).await.unwrap();
    let (left, right) = tokio::join!(storage.rollup_usage_once(), storage.rollup_usage_once());
    assert_eq!(
        left.unwrap().processed_events + right.unwrap().processed_events,
        3
    );
    assert_eq!(storage.usage_rollup_checkpoint().await.unwrap(), Some(4));
    for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
        let rows = storage
            .query_usage_rollups_in_range(resolution, 1800000000, 1800003600)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(
            (
                row.request_count,
                row.priced_event_count,
                row.unpriced_event_count,
                row.unmetered_event_count
            ),
            (4, 1, 1, 1)
        );
        assert_eq!(row.virtual_cost_micros, 17);
        assert_eq!(row.effective_cache_write_input_tokens, Some(20));
        assert_eq!(row.cache_write_input_tokens, None);
        assert_eq!(row.cache_write_input_tokens_30m, Some(13));
        assert_eq!(row.cache_write_input_tokens_12h, Some(7));
        assert_eq!(row.upstream_name, "unknown");
    }
    // An old worker can run again after a new writer; its residual remains unclassified.
    storage
        .append_request_event(&event("old-worker-after-new"))
        .await
        .unwrap();
    sqlx::raw_sql(
        "UPDATE usage_rollups_v2 SET request_count=request_count+1;
         UPDATE usage_rollup_checkpoints_v1
         SET value=(SELECT MAX(id) FROM request_events_v1) WHERE id='high_water';",
    )
    .execute(storage.pool())
    .await
    .unwrap();
    let old_worker_snapshot = rows_snapshot(&storage).await;
    let run = storage.rollup_usage_once().await.unwrap();
    assert_eq!((run.processed_events, run.updated_rollups), (0, 0));
    assert_eq!(rows_snapshot(&storage).await, old_worker_snapshot);
    assert_eq!(totals(&storage).await, (2, 2, 2));
    // Retention removes the minute only; surviving hour evidence cannot leak into its recreation.
    sqlx::query("DELETE FROM usage_rollups_v2 WHERE resolution='minute'")
        .execute(storage.pool())
        .await
        .unwrap();
    let replacement = RequestEvent {
        cost_usd_micros: Some(0),
        ..event("replacement")
    };
    storage.append_request_event(&replacement).await.unwrap();
    let run = storage.rollup_usage_once().await.unwrap();
    assert_eq!(run.processed_events, 1);
    let minute = storage
        .query_usage_rollups_in_range(UsageRollupResolution::Minute, 1800000000, 1800000060)
        .await
        .unwrap();
    let hour = storage
        .query_usage_rollups_in_range(UsageRollupResolution::Hour, 1800000000, 1800003600)
        .await
        .unwrap();
    assert_eq!(
        (
            minute[0].request_count,
            minute[0].priced_event_count,
            minute[0].unpriced_event_count,
            minute[0].unmetered_event_count
        ),
        (1, 1, 0, 0)
    );
    assert_eq!(minute[0].virtual_cost_micros, 0);
    assert_eq!(minute[0].effective_cache_write_input_tokens, Some(0));
    assert_eq!(
        (
            hour[0].request_count,
            hour[0].priced_event_count,
            hour[0].unpriced_event_count,
            hour[0].unmetered_event_count
        ),
        (6, 2, 1, 1)
    );
    let snapshot = rows_snapshot(&storage).await;
    let revision = storage.usage_rollup_revision().await.unwrap();
    storage.rollup_usage_once().await.unwrap();
    assert_eq!(rows_snapshot(&storage).await, snapshot);
    assert_eq!(storage.usage_rollup_revision().await.unwrap(), revision);
    assert_no_pricing_recovery_metadata(&storage).await;
}

#[tokio::test]
async fn failed_live_batch_rolls_back_cost_backfill_and_checkpoint_then_restarts_forward_only() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("rollback.sqlite").display());
    let storage = fresh_storage(&url).await;
    let historical = RequestEvent {
        cost_usd_micros: Some(6),
        cost_input_micros: Some(6),
        ..event("historical")
    };
    storage.append_request_event(&historical).await.unwrap();
    sqlx::raw_sql(
        "INSERT INTO usage_rollups_v2
         (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,virtual_cost_micros,updated_at)
         VALUES ('minute',1800000000,'unknown','00000000-0000-0000-0000-000000000000','unknown','pricing',1,6,0),
                ('hour',1800000000,'unknown','00000000-0000-0000-0000-000000000000','unknown','pricing',1,6,0);
         INSERT INTO usage_rollup_checkpoints_v1 (id,value) VALUES ('high_water',1);",
    )
    .execute(storage.pool())
    .await
    .unwrap();
    // Each event fits SQLite's scalar range, but the batch aggregate does not.
    for id in ["overflow-one", "overflow-two"] {
        storage
            .append_request_event(&RequestEvent {
                input_tokens: Some(i64::MAX as u64),
                ..event(id)
            })
            .await
            .unwrap();
    }
    let snapshot = rows_snapshot(&storage).await;
    assert!(storage.rollup_usage_once().await.is_err());
    assert_eq!(storage.usage_rollup_checkpoint().await.unwrap(), Some(1));
    assert_eq!(rows_snapshot(&storage).await, snapshot);
    let category_totals: (i64, i64) = sqlx::query_as(
        "SELECT SUM(cost_input_micros),SUM(cost_components_event_count) FROM usage_rollups_v2",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(category_totals, (0, 0));
    let cost_checkpoints: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM usage_rollup_checkpoints_v1 WHERE id LIKE 'cost_components_backfill_%'",
    ).fetch_one(storage.pool()).await.unwrap();
    assert_eq!(cost_checkpoints, 0);
    storage.pool().close().await;
    let storage =
        cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap();
    sqlx::query("DELETE FROM request_events_v1 WHERE id > 1")
        .execute(storage.pool())
        .await
        .unwrap();
    storage
        .append_request_event(&RequestEvent {
            cost_usd_micros: Some(0),
            ..event("valid-after-gap")
        })
        .await
        .unwrap();
    let run = storage.rollup_usage_once().await.unwrap();
    assert_eq!(run.processed_events, 1);
    assert_eq!(storage.usage_rollup_checkpoint().await.unwrap(), Some(4));
    for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
        let rows = storage
            .query_usage_rollups_in_range(resolution, 1800000000, 1800003600)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(
            (
                row.request_count,
                row.priced_event_count,
                row.unpriced_event_count,
                row.unmetered_event_count
            ),
            (2, 1, 0, 0)
        );
        assert_eq!(row.virtual_cost_micros, 6);
        assert_eq!(row.cost_input_micros, 6);
        assert_eq!(row.cost_components_event_count, 1);
    }
    let revision = storage.usage_rollup_revision().await.unwrap();
    storage.rollup_usage_once().await.unwrap();
    assert_eq!(storage.usage_rollup_revision().await.unwrap(), revision);
    assert_eq!(totals(&storage).await, (2, 0, 0));
    assert_no_pricing_recovery_metadata(&storage).await;
}

#[tokio::test]
async fn idle_rollup_does_not_wait_for_an_unrelated_sqlite_writer() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("idle-lock.sqlite").display());
    let storage = fresh_storage(&url).await;
    // Initialize the independent category-cost cursors before testing a truly idle tick.
    storage.rollup_usage_once().await.unwrap();
    let revision = storage.usage_rollup_revision().await.unwrap();
    let mut writer = SqliteConnection::connect(&url).await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut writer)
        .await
        .unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        storage.rollup_usage_once(),
    )
    .await;
    sqlx::query("ROLLBACK").execute(&mut writer).await.unwrap();
    let run = result
        .expect("idle rollup must remain read-only while another connection holds the write lock")
        .unwrap();
    assert_eq!((run.processed_events, run.updated_rollups), (0, 0));
    assert_eq!(run.checkpoint, Some(0));
    assert_eq!(storage.usage_rollup_revision().await.unwrap(), revision);
}

fn event(id: &str) -> RequestEvent {
    RequestEvent {
        ts: 1_800_000_000,
        ts_ms: Some(1_800_000_000_000),
        request_id: id.to_owned(),
        event_id: Some(id.to_owned()),
        model: Some("pricing".to_owned()),
        status: 200,
        ..RequestEvent::default()
    }
}

async fn fresh_storage(url: &str) -> cc_lb_storage_sqlite::SqliteStorage {
    let storage = cc_lb_storage_sqlite::open_sqlite(url, Arc::new(cc_lb_clock::SystemClock))
        .await
        .unwrap();
    storage.initialize().await.unwrap();
    sqlx::query("UPDATE usage_rollup_reconciliations_v1 SET status='complete'")
        .execute(storage.pool())
        .await
        .unwrap();
    storage
}

async fn assert_no_pricing_recovery_metadata(storage: &cc_lb_storage_sqlite::SqliteStorage) {
    let checkpoints: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM usage_rollup_checkpoints_v1 WHERE id LIKE 'pricing_completeness_%'",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(checkpoints, 0);
    let stage_tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'usage_rollup_pricing_%'",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(stage_tables, 0);
}

async fn rows_snapshot(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
) -> Vec<(String, i64, i64, i64, i64, i64, i64, i64, Option<i64>, i64)> {
    sqlx::query_as(
        "SELECT resolution,bucket_start_unix_secs,request_count,input_tokens,virtual_cost_micros,
         priced_event_count,unpriced_event_count,unmetered_event_count,effective_cache_write_input_tokens,updated_at
         FROM usage_rollups_v2 ORDER BY resolution,bucket_start_unix_secs,principal_id,upstream_id,model",
    ).fetch_all(storage.pool()).await.unwrap()
}

async fn totals(storage: &cc_lb_storage_sqlite::SqliteStorage) -> (i64, i64, i64) {
    sqlx::query_as("SELECT SUM(priced_event_count),SUM(unpriced_event_count),SUM(unmetered_event_count) FROM usage_rollups_v2")
        .fetch_one(storage.pool()).await.unwrap()
}

async fn shape(pool: &sqlx::SqlitePool) -> Vec<(String, String, i64, Option<String>, i64)> {
    sqlx::query_as("SELECT name,type,\"notnull\",dflt_value,pk FROM pragma_table_info('usage_rollups_v2') ORDER BY cid")
        .fetch_all(pool).await.unwrap()
}
