use std::{str::FromStr, sync::Arc};

use cc_lb_storage_api::{MetaStore, UsageRollupResolution, UsageRollupStore};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};

#[tokio::test]
async fn upgrade_122_retained_events_backfills_idle_database_resumably_and_cas_safe() {
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
         INSERT INTO usage_rollups_v2 (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,updated_at)
         VALUES ('minute',1800000000,'unknown','00000000-0000-0000-0000-000000000000','unknown','pricing',10001,0),
                ('hour',1800000000,'unknown','00000000-0000-0000-0000-000000000000','unknown','pricing',10001,0),
                ('minute',1800000060,'legacy','00000000-0000-0000-0000-000000000000','unknown','legacy',1,0);
         INSERT INTO usage_rollup_checkpoints_v1 (id,value) VALUES ('high_water',10001),('cost_components_backfill_upper',10001),('cost_components_backfill_cursor',10001);
         UPDATE usage_rollup_reconciliations_v1 SET status='complete';"
    ).execute(&mut connection).await.unwrap();
    // The migration itself is metadata-only: idle historical rows remain unclassified until a worker pass.
    migrator
        .run_direct(Some(123), &mut connection, false)
        .await
        .unwrap();
    connection.close().await.unwrap();
    let storage =
        cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap();
    let before = storage.usage_rollup_revision().await.unwrap();
    assert_eq!(totals(&storage).await, (0, 0, 0));
    let (left, right) = tokio::join!(storage.rollup_usage_once(), storage.rollup_usage_once());
    assert_eq!(left.unwrap().processed_events, 0);
    assert_eq!(right.unwrap().processed_events, 0);
    // Depending on the CAS race, the second worker either loses its snapshot or takes the next batch.
    let cursor: i64 = sqlx::query_scalar("SELECT value FROM usage_rollup_checkpoints_v1 WHERE id='pricing_completeness_backfill_cursor'")
        .fetch_one(storage.pool()).await.unwrap();
    assert!(
        cursor == 2 || cursor == 0,
        "unexpected descending cursor: {cursor}"
    );
    storage.pool().close().await;
    let storage =
        cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap();
    storage.rollup_usage_once().await.unwrap();
    assert_eq!(totals(&storage).await, (6666, 6668, 6668));
    assert_ne!(storage.usage_rollup_revision().await.unwrap(), before);
    assert_eq!(
        storage.usage_rollup_checkpoint().await.unwrap(),
        Some(10001)
    );
    for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
        let rows = storage
            .query_usage_rollups_in_range(resolution, 1800000000, 1800003600)
            .await
            .unwrap();
        let row = rows.iter().find(|r| r.model == "pricing").unwrap();
        assert_eq!(
            (
                row.priced_event_count,
                row.unpriced_event_count,
                row.unmetered_event_count
            ),
            (3333, 3334, 3334)
        );
        assert_eq!(row.request_count, 10001);
        if let Some(legacy) = rows.iter().find(|r| r.model == "legacy") {
            assert_eq!(
                (
                    legacy.priced_event_count,
                    legacy.unpriced_event_count,
                    legacy.unmetered_event_count
                ),
                (0, 0, 0)
            );
        }
    }
    let revision = storage.usage_rollup_revision().await.unwrap();
    storage.rollup_usage_once().await.unwrap();
    assert_eq!(storage.usage_rollup_revision().await.unwrap(), revision);
    assert_eq!(totals(&storage).await, (6666, 6668, 6668));
    // Current initialize fast path and the actual incremental runner produce identical schemas.
    let fresh_url = format!("sqlite://{}", dir.path().join("fresh.sqlite").display());
    let fresh = cc_lb_storage_sqlite::open_sqlite(&fresh_url, Arc::new(cc_lb_clock::SystemClock))
        .await
        .unwrap();
    fresh.initialize().await.unwrap();
    assert_eq!(shape(storage.pool()).await, shape(fresh.pool()).await);
    // Revision lookup must survive a missing rollup table (conditional dashboard responses).
    sqlx::query("DROP TABLE usage_rollups_v2")
        .execute(storage.pool())
        .await
        .unwrap();
    assert_eq!(storage.usage_rollup_revision().await.unwrap(), revision);
}

async fn totals(storage: &cc_lb_storage_sqlite::SqliteStorage) -> (i64, i64, i64) {
    sqlx::query_as("SELECT SUM(priced_event_count),SUM(unpriced_event_count),SUM(unmetered_event_count) FROM usage_rollups_v2")
        .fetch_one(storage.pool()).await.unwrap()
}

async fn shape(pool: &sqlx::SqlitePool) -> Vec<(String, String, i64, Option<String>, i64)> {
    sqlx::query_as("SELECT name,type,\"notnull\",dflt_value,pk FROM pragma_table_info('usage_rollups_v2') ORDER BY cid")
        .fetch_all(pool).await.unwrap()
}
