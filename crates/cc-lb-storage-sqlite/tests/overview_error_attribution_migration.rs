use std::{str::FromStr, sync::Arc, time::Instant};

use cc_lb_storage_api::{
    MetaStore, RequestEvent, RequestEventStore, UpstreamCreate, UpstreamKind, UpstreamStore,
    UsageRollupStore,
};
use cc_lb_storage_sqlite::SqliteStorage;
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use uuid::Uuid;

const AT: u64 = 1_800_000_000;
const UPSTREAM: &str = "00000000-0000-0000-0000-000000001300";

type Row = (String, i64, String, i64);

#[tokio::test]
async fn startup_attribution_resumes_committed_batches_without_double_counting() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}",
        dir.path().join("attribution.sqlite").display()
    );
    let options = SqliteConnectOptions::from_str(&url)
        .unwrap()
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let migrator = sqlx::migrate!("./migrations");
    migrator
        .run_direct(Some(125), &mut connection, false)
        .await
        .unwrap();
    sqlx::raw_sql(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10001)
         INSERT INTO request_events_v1
             (request_id,event_id,ts,list_ts_ms,list_status,list_duration_ms,upstream_id,upstream_name)
         SELECT 'excluded-'||i,'excluded-'||i,1800000000,1800000000000,403,0,
                '00000000-0000-0000-0000-000000001300','captured' FROM n;
         INSERT INTO overview_excluded_error_rollups_v1
             (resolution,bucket_start_unix_secs,error_count,updated_at)
         VALUES ('minute',1800000000,10001,0),('hour',1800000000,10001,0);
         INSERT INTO usage_rollup_checkpoints_v1 (id,value)
         VALUES ('high_water',10001),('cost_components_backfill_upper',10001),
                ('cost_components_backfill_cursor',10001);
         UPDATE usage_rollup_reconciliations_v1 SET status='complete';",
    ).execute(&mut connection).await.unwrap();
    // Install the fault after real SQLx schema migration, before the startup hook.
    migrator
        .run_direct(None, &mut connection, false)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TRIGGER interrupt_attribution BEFORE UPDATE OF error_count
         ON overview_excluded_error_rollups_v1
         WHEN NEW.upstream_id <> '' AND NEW.error_count > 1000
         BEGIN SELECT RAISE(ABORT, 'controlled attribution interruption'); END;",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();
    let storage =
        cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap();
    let error = storage
        .initialize()
        .await
        .expect_err("second attribution batch must fail");
    assert!(
        error
            .to_string()
            .contains("controlled attribution interruption"),
        "{error}"
    );
    for resolution in ["minute", "hour"] {
        let attributed: i64 = sqlx::query_scalar(
            "SELECT error_count FROM overview_excluded_error_rollups_v1
             WHERE resolution=? AND upstream_id=?",
        )
        .bind(resolution)
        .bind(UPSTREAM)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(
            attributed, 1000,
            "first batch must have committed: {resolution}"
        );
        let total: i64 = sqlx::query_scalar(
            "SELECT SUM(error_count) FROM overview_excluded_error_rollups_v1 WHERE resolution=?",
        )
        .bind(resolution)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(total, 10001, "interruption must preserve global totals");
    }
    sqlx::query("DROP TRIGGER interrupt_attribution")
        .execute(storage.pool())
        .await
        .unwrap();
    storage.pool().close().await;
    let storage =
        cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap();
    storage.initialize().await.unwrap();
    storage.verify_schema_current().await.unwrap();
    let expected = vec![
        ("hour".to_owned(), AT as i64, UPSTREAM.to_owned(), 10001),
        ("minute".to_owned(), AT as i64, UPSTREAM.to_owned(), 10001),
    ];
    assert_eq!(snapshot(&storage).await, expected);
    assert_eq!(
        storage.usage_rollup_checkpoint().await.unwrap(),
        Some(10001)
    );
    let before = storage.usage_rollup_revision().await.unwrap();
    let started = Instant::now();
    storage.initialize().await.unwrap();
    assert!(
        started.elapsed().as_secs_f64() < 5.0,
        "completed initialization must be cheap"
    );
    assert_eq!(snapshot(&storage).await, expected);
    storage.rollup_usage_once().await.unwrap();
    assert_eq!(snapshot(&storage).await, expected);
    assert_eq!(storage.usage_rollup_revision().await.unwrap(), before);
    storage
        .append_request_event(&RequestEvent {
            ts: AT,
            ts_ms: Some(AT * 1000),
            event_id: Some("post-upgrade".to_owned()),
            request_id: "post-upgrade".to_owned(),
            upstream_id: Some(Uuid::from_u128(0x1301)),
            status: 403,
            ..RequestEvent::default()
        })
        .await
        .unwrap();
    assert_eq!(
        storage.rollup_usage_once().await.unwrap().processed_events,
        1
    );
    assert_eq!(
        storage.rollup_usage_once().await.unwrap().processed_events,
        0
    );
    for resolution in ["minute", "hour"] {
        let total: i64 = sqlx::query_scalar(
            "SELECT SUM(error_count) FROM overview_excluded_error_rollups_v1 WHERE resolution=?",
        )
        .bind(resolution)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(total, 10002);
    }
    let fresh_url = format!("sqlite://{}", dir.path().join("fresh.sqlite").display());
    let fresh = cc_lb_storage_sqlite::open_sqlite(&fresh_url, Arc::new(cc_lb_clock::SystemClock))
        .await
        .unwrap();
    fresh.initialize().await.unwrap();
    fresh.verify_schema_current().await.unwrap();
    assert_eq!(shape(&storage).await, shape(&fresh).await);
    assert!(snapshot(&fresh).await.is_empty());
}

#[tokio::test]
async fn retained_and_api_pruned_history_keep_exact_upstream_identity_and_checkpoint() {
    for prune_history in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}",
            dir.path().join("identities.sqlite").display()
        );
        let options = SqliteConnectOptions::from_str(&url)
            .unwrap()
            .create_if_missing(true);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::migrate!("./migrations")
            .run_direct(Some(125), &mut connection, false)
            .await
            .unwrap();
        connection.close().await.unwrap();
        let storage =
            cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
                .await
                .unwrap();
        for (id, name, kind) in [
            (0x1300, "named", UpstreamKind::OpenaiOauth),
            (0x1301, "other", UpstreamKind::GeminiOauth),
        ] {
            storage
                .create(UpstreamCreate {
                    id: Uuid::from_u128(id),
                    name: name.to_owned(),
                    kind,
                    base_url: None,
                    api_key_ciphertext: None,
                    oauth_tokens: None,
                    warmup_enabled: false,
                    warmup_dialect_plugin: None,
                    workspace_id: None,
                    api_key_access: None,
                })
                .await
                .unwrap();
        }
        for (index, upstream_id, name, status) in [
            (0, Some(Uuid::from_u128(0x1300)), Some("named"), 401),
            (1, Some(Uuid::from_u128(0x1301)), Some("other"), 403),
            (2, None, None, 404),
            (3, None, Some("named"), 403),
        ] {
            let ts = AT + if index == 0 { 1 } else { 10 };
            storage
                .append_request_event(&RequestEvent {
                    ts,
                    ts_ms: Some(ts * 1000),
                    request_id: format!("identity-{index}"),
                    event_id: Some(format!("identity-{index}")),
                    upstream_id,
                    upstream_name: name.map(str::to_owned),
                    status,
                    ..RequestEvent::default()
                })
                .await
                .unwrap();
        }
        sqlx::raw_sql(
            "INSERT INTO overview_excluded_error_rollups_v1
             (resolution,bucket_start_unix_secs,error_count,updated_at)
             VALUES ('minute',1800000000,4,0),('hour',1800000000,4,0);
             INSERT INTO usage_rollup_checkpoints_v1 (id,value)
             VALUES ('high_water',4),('cost_components_backfill_upper',4),
                    ('cost_components_backfill_cursor',4);
             UPDATE usage_rollup_reconciliations_v1 SET status='complete';",
        )
        .execute(storage.pool())
        .await
        .unwrap();
        if prune_history {
            assert_eq!(
                storage
                    .prune_request_events_before((AT + 5) * 1_000_000_000, 100)
                    .await
                    .unwrap(),
                1,
            );
        }
        storage
            .append_request_event(&RequestEvent {
                ts: AT + 10,
                ts_ms: Some((AT + 10) * 1000),
                request_id: "after-checkpoint".to_owned(),
                event_id: Some("after-checkpoint".to_owned()),
                upstream_id: Some(Uuid::from_u128(0x1301)),
                status: 404,
                ..RequestEvent::default()
            })
            .await
            .unwrap();
        storage.initialize().await.unwrap();
        let mut expected = Vec::new();
        for resolution in ["hour", "minute"] {
            if prune_history {
                expected.push((resolution.to_owned(), AT as i64, String::new(), 1));
            }
            expected.push((resolution.to_owned(), AT as i64, Uuid::nil().to_string(), 1));
            expected.push((
                resolution.to_owned(),
                AT as i64,
                UPSTREAM.to_owned(),
                if prune_history { 1 } else { 2 },
            ));
            expected.push((
                resolution.to_owned(),
                AT as i64,
                Uuid::from_u128(0x1301).to_string(),
                1,
            ));
        }
        assert_eq!(snapshot(&storage).await, expected);
        for resolution in [
            cc_lb_storage_api::UsageRollupResolution::Minute,
            cc_lb_storage_api::UsageRollupResolution::Hour,
        ] {
            let buckets = storage
                .query_overview_excluded_error_buckets_in_range(resolution, AT, AT + 60)
                .await
                .unwrap();
            let unknown = buckets
                .iter()
                .find(|bucket| bucket.upstream_id == Some(Uuid::nil()))
                .expect("actual unknown identity is Some(nil), not global residual");
            assert_eq!(unknown.error_count, 1);
            assert_eq!(
                buckets
                    .iter()
                    .find(|bucket| bucket.upstream_id.is_none())
                    .map(|bucket| bucket.error_count),
                if prune_history { Some(1) } else { None },
            );
        }
        assert_eq!(storage.usage_rollup_checkpoint().await.unwrap(), Some(4));
        storage.initialize().await.unwrap();
        assert_eq!(snapshot(&storage).await, expected);
        assert_eq!(
            storage.rollup_usage_once().await.unwrap().processed_events,
            1
        );
        for row in &mut expected {
            if row.2 == Uuid::from_u128(0x1301).to_string() {
                row.3 = 2;
            }
        }
        assert_eq!(snapshot(&storage).await, expected);
        assert_eq!(
            storage.rollup_usage_once().await.unwrap().processed_events,
            0
        );
        assert_eq!(snapshot(&storage).await, expected);
        for status in [400, 429, 500] {
            storage
                .append_request_event(&RequestEvent {
                    ts: AT + 10,
                    ts_ms: Some((AT + 10) * 1000),
                    request_id: format!("included-{status}"),
                    event_id: Some(format!("included-{status}")),
                    upstream_id: Some(Uuid::from_u128(0x1301)),
                    status,
                    ..RequestEvent::default()
                })
                .await
                .unwrap();
        }
        assert_eq!(
            storage.rollup_usage_once().await.unwrap().processed_events,
            3
        );
        assert_eq!(
            snapshot(&storage).await,
            expected,
            "400/429/500 must not be excluded"
        );
        for resolution in [
            cc_lb_storage_api::UsageRollupResolution::Minute,
            cc_lb_storage_api::UsageRollupResolution::Hour,
        ] {
            let usage = storage
                .query_usage_rollups_in_range(resolution, AT, AT + 60)
                .await
                .unwrap();
            assert_eq!(usage.iter().map(|row| row.request_count).sum::<u64>(), 4);
            assert_eq!(usage.iter().map(|row| row.error_count).sum::<u64>(), 4);
        }
    }
}

async fn snapshot(storage: &SqliteStorage) -> Vec<Row> {
    sqlx::query_as(
        "SELECT resolution,bucket_start_unix_secs,upstream_id,error_count
         FROM overview_excluded_error_rollups_v1 ORDER BY resolution,bucket_start_unix_secs,upstream_id",
    ).fetch_all(storage.pool()).await.unwrap()
}

async fn shape(storage: &SqliteStorage) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT name,sql FROM sqlite_master WHERE type='table'
         AND name LIKE 'overview_excluded_error%' ORDER BY name",
    )
    .fetch_all(storage.pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn startup_attribution_rejects_insufficient_and_exhausted_legacy_totals() {
    for legacy_count in [1_i64, 1000] {
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}",
            dir.path().join("undercount.sqlite").display()
        );
        let options = SqliteConnectOptions::from_str(&url)
            .unwrap()
            .create_if_missing(true);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::migrate!("./migrations")
            .run_direct(Some(125), &mut connection, false)
            .await
            .unwrap();
        sqlx::query(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?)
             INSERT INTO request_events_v1
             (request_id,event_id,ts,list_ts_ms,list_status,list_duration_ms,upstream_id)
             SELECT 'undercount-'||i,'undercount-'||i,1800000000,1800000000000,403,0,
                    '00000000-0000-0000-0000-000000001300' FROM n",
        )
        .bind(legacy_count + 1)
        .execute(&mut connection)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO overview_excluded_error_rollups_v1
             (resolution,bucket_start_unix_secs,error_count,updated_at)
             VALUES ('minute',1800000000,?,0),('hour',1800000000,?,0)",
        )
        .bind(legacy_count)
        .bind(legacy_count)
        .execute(&mut connection)
        .await
        .unwrap();
        sqlx::query("INSERT INTO usage_rollup_checkpoints_v1 (id,value) VALUES ('high_water',?)")
            .bind(legacy_count + 1)
            .execute(&mut connection)
            .await
            .unwrap();
        connection.close().await.unwrap();
        let storage =
            cc_lb_storage_sqlite::open_sqlite_existing(&url, Arc::new(cc_lb_clock::SystemClock))
                .await
                .unwrap();
        let error = storage
            .initialize()
            .await
            .expect_err("retained events exceed legacy exclusion total");
        match error {
            cc_lb_storage_api::StorageError::Corrupted { message } => {
                assert!(
                    message.contains("overview excluded error attribution undercount"),
                    "{message}"
                );
            }
            other => panic!("expected explicit data inconsistency, not SQL/setup failure: {other}"),
        }
        let expected_id = if legacy_count == 1 { "" } else { UPSTREAM };
        assert_eq!(
            snapshot(&storage).await,
            vec![
                (
                    "hour".to_owned(),
                    AT as i64,
                    expected_id.to_owned(),
                    legacy_count
                ),
                (
                    "minute".to_owned(),
                    AT as i64,
                    expected_id.to_owned(),
                    legacy_count
                ),
            ]
        );
        let cursor: i64 = sqlx::query_scalar(
            "SELECT cursor FROM overview_excluded_error_attribution_v1 WHERE id=1",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(cursor, if legacy_count == 1 { 0 } else { 1000 });
    }
}
