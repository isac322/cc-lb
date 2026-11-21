use std::{str::FromStr, sync::Arc, time::Instant};

use cc_lb_storage_api::{MetaStore, RequestEvent, RequestEventStore, UsageRollupStore};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, Connection, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Row = (String, i64, String, i64);
const UPSTREAM: &str = "00000000-0000-0000-0000-000000001300";

#[tokio::test]
async fn startup_attribution_resumes_committed_batches_without_double_counting() -> TestResult {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skipped: CI_POSTGRES_URL unset; PostgreSQL attribution assertions did not run");
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let result: TestResult = async {
        let storage = &fixture.storage;
        let pool = storage.pool();
        let mut connection = sqlx::PgConnection::connect_with(&fixture.options).await?;
        let migrator = sqlx::migrate!("./migrations");
        migrator.run_direct(Some(169), &mut connection, false).await?;
        // Separate transactions exercise the xid/seq historical checkpoint ordering.
        for (start, end) in [(1_i64, 6000_i64), (6001, 10001)] {
            sqlx::query(
                "INSERT INTO request_events_v1
                 (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,upstream_id,upstream_name)
                 SELECT 'excluded-'||i,'excluded-'||i,to_timestamp(1800000000),1800000000000,
                        403,0,'00000000-0000-0000-0000-000000001300'::uuid,'captured'
                 FROM generate_series($1,$2) AS i",
            ).bind(start).bind(end).execute(pool).await?;
        }
        sqlx::raw_sql(
            "INSERT INTO overview_excluded_error_rollups_v1
             (resolution,bucket_start_unix_secs,error_count,updated_at)
             VALUES ('minute',1800000000,10001,NOW()),('hour',1800000000,10001,NOW());
             INSERT INTO usage_rollup_checkpoints_v1 (id,value,value_xid)
             SELECT id,seq,tx_id
             FROM (VALUES ('high_water'),('cost_components_backfill_upper'),
                          ('cost_components_backfill_cursor')) AS ids(id)
             CROSS JOIN LATERAL
                 (SELECT seq,tx_id FROM request_events_v1 ORDER BY tx_id DESC,seq DESC LIMIT 1) AS last_event;
             UPDATE usage_rollup_reconciliations_v1 SET status='complete';",
        ).execute(pool).await?;
        let checkpoint = storage.usage_rollup_checkpoint().await?;
        // Schema still goes through the actual SQLx runner; initialization executes its hook.
        migrator.run_direct(None, &mut connection, false).await?;
        sqlx::raw_sql(
            "CREATE FUNCTION interrupt_attribution() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.upstream_id <> '' AND NEW.error_count > 10000 THEN
                     RAISE EXCEPTION 'controlled attribution interruption';
                 END IF;
                 RETURN NEW;
             END $$;
             CREATE TRIGGER interrupt_attribution BEFORE UPDATE OF error_count
             ON overview_excluded_error_rollups_v1
             FOR EACH ROW EXECUTE FUNCTION interrupt_attribution();",
        ).execute(pool).await?;
        connection.close().await?;
        let error = storage.initialize().await.expect_err("second attribution batch must fail");
        assert!(error.to_string().contains("controlled attribution interruption"), "{error}");
        for resolution in ["minute", "hour"] {
            let attributed: i64 = sqlx::query_scalar(
                "SELECT error_count FROM overview_excluded_error_rollups_v1
                 WHERE resolution=$1 AND upstream_id=$2",
            ).bind(resolution).bind(UPSTREAM).fetch_one(pool).await?;
            assert_eq!(attributed, 10000, "first batch must have committed: {resolution}");
            let total: i64 = sqlx::query_scalar(
                "SELECT SUM(error_count)::bigint FROM overview_excluded_error_rollups_v1 WHERE resolution=$1",
            ).bind(resolution).fetch_one(pool).await?;
            assert_eq!(total, 10001);
        }
        sqlx::raw_sql(
            "DROP TRIGGER interrupt_attribution ON overview_excluded_error_rollups_v1;
             DROP FUNCTION interrupt_attribution();",
        ).execute(pool).await?;
        let resumed = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
        resumed.initialize().await?;
        resumed.verify_schema_current().await?;
        let expected = vec![
            ("hour".to_owned(), 1800000000, UPSTREAM.to_owned(), 10001),
            ("minute".to_owned(), 1800000000, UPSTREAM.to_owned(), 10001),
        ];
        assert_eq!(snapshot(&resumed).await?, expected);
        assert_eq!(resumed.usage_rollup_checkpoint().await?, checkpoint);
        let before = resumed.usage_rollup_revision().await?;
        let started = Instant::now();
        resumed.initialize().await?;
        assert!(started.elapsed().as_secs_f64() < 5.0, "completed initialization must be cheap");
        resumed.rollup_usage_once().await?;
        assert_eq!(snapshot(&resumed).await?, expected);
        assert_eq!(resumed.usage_rollup_revision().await?, before);
        resumed.append_request_event(&RequestEvent {
            ts: 1_800_000_000,
            ts_ms: Some(1_800_000_000_000),
            event_id: Some("post-upgrade".to_owned()),
            request_id: "post-upgrade".to_owned(),
            upstream_id: Some(Uuid::from_u128(0x1301)),
            status: 403,
            ..RequestEvent::default()
        }).await?;
        resumed.request_event_stable_seq_ceiling(std::time::Duration::from_secs(30)).await?;
        assert_eq!(resumed.rollup_usage_once().await?.processed_events, 1);
        assert_eq!(resumed.rollup_usage_once().await?.processed_events, 0);
        for resolution in ["minute", "hour"] {
            let total: i64 = sqlx::query_scalar(
                "SELECT SUM(error_count)::bigint FROM overview_excluded_error_rollups_v1 WHERE resolution=$1",
            ).bind(resolution).fetch_one(pool).await?;
            assert_eq!(total, 10002);
        }
        let fresh = Fixture::create_templated(&url).await?;
        let parity: TestResult = async {
            fresh.storage.initialize().await?;
            fresh.storage.verify_schema_current().await?;
            assert_eq!(shape(&resumed).await?, shape(&fresh.storage).await?);
            assert!(snapshot(&fresh.storage).await?.is_empty());
            Ok(())
        }.await;
        let teardown = fresh.drop_database().await;
        parity?;
        teardown?;
        Ok(())
    }.await;
    let teardown = fixture.drop_database().await;
    result?;
    teardown
}

#[tokio::test]
async fn retained_and_api_pruned_history_keep_exact_upstream_identity_and_checkpoint() -> TestResult
{
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skipped: CI_POSTGRES_URL unset; PostgreSQL identity assertions did not run");
        return Ok(());
    };
    for prune_history in [false, true] {
        let fixture = Fixture::create(&url).await?;
        let result: TestResult = async {
            let storage = &fixture.storage;
            let mut connection = sqlx::PgConnection::connect_with(&fixture.options).await?;
            sqlx::migrate!("./migrations")
                .run_direct(Some(169), &mut connection, false).await?;
            for (id, name, kind) in [
                (0x1300_u128, "named", "openai_oauth"),
                (0x1301_u128, "other", "gemini_oauth"),
            ] {
                sqlx::query(
                    "INSERT INTO upstream_spec_v1
                     (id,name,kind,base_url,enabled,warmup_enabled,warmup_dialect_plugin,
                      spec_revision,created_at,updated_at,deleted_at,workspace_id,owner_epoch)
                     VALUES ($1,$2,$3,NULL,TRUE,FALSE,NULL,1,NOW(),NOW(),NULL,NULL,0)",
                )
                .bind(Uuid::from_u128(id))
                .bind(name)
                .bind(kind)
                .execute(storage.pool())
                .await?;
            }
            sqlx::query(
                "INSERT INTO overview_excluded_error_rollups_v1
                 (resolution,bucket_start_unix_secs,error_count,updated_at)
                 VALUES ('minute',1800000000,4,NOW()),('hour',1800000000,4,NOW())",
            )
            .execute(storage.pool())
            .await?;
            for (index, upstream_id, name, status) in [
                (0, Some(Uuid::from_u128(0x1300)), Some("named"), 401),
                (1, Some(Uuid::from_u128(0x1301)), Some("other"), 403),
                (2, None, None, 404),
                (3, None, Some("named"), 403),
            ] {
                let request_id = format!("identity-{index}");
                sqlx::query(
                    "INSERT INTO request_events_v1
                     (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,upstream_id,upstream_name)
                     VALUES ($1,$1,to_timestamp($2),$3,$4,0,$5,$6)",
                )
                .bind(&request_id)
                .bind(1_800_000_000_i64 + if index == 0 { 1 } else { 10 })
                .bind((1_800_000_000_i64 + if index == 0 { 1 } else { 10 }) * 1000)
                .bind(status)
                .bind(upstream_id)
                .bind(name)
                .execute(storage.pool())
                .await?;
            }
            sqlx::raw_sql(
                "INSERT INTO usage_rollup_checkpoints_v1 (id,value,value_xid)
                 SELECT id,seq,tx_id
                 FROM (VALUES ('high_water'),('cost_components_backfill_upper'),
                              ('cost_components_backfill_cursor')) AS ids(id)
                 CROSS JOIN LATERAL
                     (SELECT seq,tx_id FROM request_events_v1 ORDER BY tx_id DESC,seq DESC LIMIT 1) AS last_event;
                 UPDATE usage_rollup_reconciliations_v1 SET status='complete';",
            )
            .execute(storage.pool())
            .await?;
            sqlx::migrate!("./migrations")
                .run_direct(None, &mut connection, false)
                .await?;
            connection.close().await?;
            for id in [Uuid::from_u128(0x1300), Uuid::from_u128(0x1301)] {
                let record = cc_lb_storage_api::UpstreamStore::get_by_id(storage, id)
                    .await?
                    .expect("legacy record");
                assert!(
                    record.vertex.is_none(),
                    "previous-release records must keep vertex=None"
                );
            }
            let checkpoint = storage.usage_rollup_checkpoint().await?;
            if prune_history {
                assert_eq!(
                    storage.prune_request_events_before(1_800_000_005_000_000_000, 100).await?,
                    1,
                );
            }
            storage.append_request_event(&RequestEvent {
                ts: 1_800_000_010,
                ts_ms: Some(1_800_000_010_000),
                request_id: "after-checkpoint".to_owned(),
                event_id: Some("after-checkpoint".to_owned()),
                upstream_id: Some(Uuid::from_u128(0x1301)),
                status: 404,
                ..RequestEvent::default()
            }).await?;
            storage.initialize().await?;
            let mut expected = Vec::new();
            for resolution in ["hour", "minute"] {
                if prune_history {
                    expected.push((resolution.to_owned(), 1800000000, String::new(), 1));
                }
                // PostgreSQL usage identity intentionally does not resolve a name-only event.
                expected.push((resolution.to_owned(), 1800000000, Uuid::nil().to_string(), 2));
                if !prune_history {
                    expected.push((resolution.to_owned(), 1800000000, UPSTREAM.to_owned(), 1));
                }
                expected.push((resolution.to_owned(), 1800000000, Uuid::from_u128(0x1301).to_string(), 1));
            }
            assert_eq!(snapshot(storage).await?, expected);
            for resolution in [
                cc_lb_storage_api::UsageRollupResolution::Minute,
                cc_lb_storage_api::UsageRollupResolution::Hour,
            ] {
                let buckets = storage.query_overview_excluded_error_buckets_in_range(
                    resolution, 1_800_000_000, 1_800_000_060,
                ).await?;
                let unknown = buckets.iter().find(|bucket| bucket.upstream_id == Some(Uuid::nil()))
                    .expect("actual unknown identity is Some(nil), not global residual");
                assert_eq!(unknown.error_count, 2);
                assert_eq!(
                    buckets.iter().find(|bucket| bucket.upstream_id.is_none()).map(|bucket| bucket.error_count),
                    if prune_history { Some(1) } else { None },
                );
            }
            assert_eq!(storage.usage_rollup_checkpoint().await?, checkpoint);
            storage.initialize().await?;
            assert_eq!(snapshot(storage).await?, expected);
            storage.request_event_stable_seq_ceiling(std::time::Duration::from_secs(30)).await?;
            assert_eq!(storage.rollup_usage_once().await?.processed_events, 1);
            for row in &mut expected {
                if row.2 == Uuid::from_u128(0x1301).to_string() {
                    row.3 = 2;
                }
            }
            assert_eq!(snapshot(storage).await?, expected);
            assert_eq!(storage.rollup_usage_once().await?.processed_events, 0);
            assert_eq!(snapshot(storage).await?, expected);
            for status in [400, 429, 500] {
                storage.append_request_event(&RequestEvent {
                    ts: 1_800_000_010,
                    ts_ms: Some(1_800_000_010_000),
                    request_id: format!("included-{status}"),
                    event_id: Some(format!("included-{status}")),
                    upstream_id: Some(Uuid::from_u128(0x1301)),
                    status,
                    ..RequestEvent::default()
                }).await?;
            }
            storage.request_event_stable_seq_ceiling(std::time::Duration::from_secs(30)).await?;
            assert_eq!(storage.rollup_usage_once().await?.processed_events, 3);
            assert_eq!(snapshot(storage).await?, expected, "400/429/500 must not be excluded");
            for resolution in [
                cc_lb_storage_api::UsageRollupResolution::Minute,
                cc_lb_storage_api::UsageRollupResolution::Hour,
            ] {
                let usage = storage.query_usage_rollups_in_range(
                    resolution, 1_800_000_000, 1_800_000_060,
                ).await?;
                assert_eq!(usage.iter().map(|row| row.request_count).sum::<u64>(), 4);
                assert_eq!(usage.iter().map(|row| row.error_count).sum::<u64>(), 4);
            }
            Ok(())
        }.await;
        let teardown = fixture.drop_database().await;
        result?;
        teardown?;
    }
    Ok(())
}

#[tokio::test]
async fn startup_attribution_production_volume_rehearsal() -> TestResult {
    let Ok(count) = std::env::var("CC_LB_ATTRIBUTION_REHEARSAL_EVENTS") else {
        eprintln!(
            "skipped: set CC_LB_ATTRIBUTION_REHEARSAL_EVENTS for production-volume rehearsal"
        );
        return Ok(());
    };
    let count: i64 = count.parse()?;
    assert!(
        count >= 100_000,
        "production rehearsal requires at least 100k rows"
    );
    let url = std::env::var("CI_POSTGRES_URL")?;
    let fixture = Fixture::create(&url).await?;
    let result: TestResult = async {
        let storage = &fixture.storage;
        let mut connection = sqlx::PgConnection::connect_with(&fixture.options).await?;
        sqlx::migrate!("./migrations")
            .run_direct(Some(169), &mut connection, false).await?;
        connection.close().await?;
        sqlx::query(
            "INSERT INTO request_events_v1
             (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,upstream_id,upstream_name)
             SELECT 'rehearsal-'||i,'rehearsal-'||i,to_timestamp(1800000000),1800000000000,403,0,
                    CASE WHEN i%2=0 THEN '00000000-0000-0000-0000-000000001300'::uuid
                         ELSE '00000000-0000-0000-0000-000000001301'::uuid END,
                    'rehearsal-upstream'
             FROM generate_series(1,$1::bigint) AS i",
        ).bind(count).execute(storage.pool()).await?;
        sqlx::query(
            "INSERT INTO overview_excluded_error_rollups_v1
             (resolution,bucket_start_unix_secs,error_count,updated_at)
             VALUES ('minute',1800000000,$1,NOW()),('hour',1800000000,$1,NOW())",
        ).bind(count).execute(storage.pool()).await?;
        sqlx::raw_sql(
            "INSERT INTO usage_rollup_checkpoints_v1 (id,value,value_xid)
             SELECT id,seq,tx_id FROM (VALUES ('high_water'),('cost_components_backfill_upper'),
                      ('cost_components_backfill_cursor')) AS ids(id)
             CROSS JOIN LATERAL
                 (SELECT seq,tx_id FROM request_events_v1 ORDER BY tx_id DESC,seq DESC LIMIT 1) AS last_event;
             UPDATE usage_rollup_reconciliations_v1 SET status='complete';",
        ).execute(storage.pool()).await?;
        let bytes: i64 = sqlx::query_scalar(
            "SELECT pg_total_relation_size('request_events_v1')",
        ).fetch_one(storage.pool()).await?;
        let started = Instant::now();
        storage.initialize().await?;
        let elapsed = started.elapsed();
        eprintln!("attribution rehearsal: events={count} raw_relation_bytes={bytes} initialize={elapsed:?}");
        let mut expected = Vec::new();
        for resolution in ["hour", "minute"] {
            expected.push((resolution.to_owned(), 1800000000, UPSTREAM.to_owned(), count / 2));
            expected.push((resolution.to_owned(), 1800000000, Uuid::from_u128(0x1301).to_string(), count - count / 2));
        }
        assert_eq!(snapshot(storage).await?, expected);
        assert!(elapsed.as_secs_f64() <= 15.0, "startup exceeded 15-second lock-hold target: {elapsed:?}");
        let started = Instant::now();
        storage.initialize().await?;
        eprintln!("attribution completed no-op: {:?}", started.elapsed());
        assert_eq!(snapshot(storage).await?, expected);
        Ok(())
    }.await;
    let teardown = fixture.drop_database().await;
    result?;
    teardown
}

async fn snapshot(storage: &PostgresStorage) -> TestResult<Vec<Row>> {
    Ok(sqlx::query_as(
        "SELECT resolution,bucket_start_unix_secs,upstream_id,error_count
         FROM overview_excluded_error_rollups_v1 ORDER BY resolution,bucket_start_unix_secs,upstream_id",
    ).fetch_all(storage.pool()).await?)
}

async fn shape(
    storage: &PostgresStorage,
) -> TestResult<Vec<(String, String, String, Option<String>)>> {
    Ok(sqlx::query_as(
        "SELECT table_name,column_name,data_type,column_default
         FROM information_schema.columns WHERE table_schema=current_schema()
         AND table_name LIKE 'overview_excluded_error%' ORDER BY table_name,ordinal_position",
    )
    .fetch_all(storage.pool())
    .await?)
}

struct Fixture {
    database: String,
    options: PgConnectOptions,
    admin: Option<PgPool>,
    isolated: Option<crate::pg_test_support::IsolatedPg>,
    storage: PostgresStorage,
}

impl Fixture {
    async fn create_templated(url: &str) -> TestResult<Self> {
        let isolated = crate::pg_test_support::IsolatedPg::create_templated(url).await?;
        let options = isolated.connect_options();
        let pool = match isolated.pool(4).await {
            Ok(pool) => pool,
            Err(error) => {
                let _ = isolated.drop().await;
                return Err(error.into());
            }
        };
        Ok(Self {
            database: String::new(),
            options,
            admin: None,
            isolated: Some(isolated),
            storage: PostgresStorage::new(pool, Arc::new(cc_lb_clock::SystemClock)),
        })
    }

    async fn create(url: &str) -> TestResult<Self> {
        let database = format!("cc_lb_attribution_{}", Uuid::new_v4().simple());
        let base = PgConnectOptions::from_str(url)?;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base.clone().database("postgres"))
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE DATABASE {database}")))
            .execute(&admin)
            .await?;
        let options = base.database(&database);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options.clone())
            .await?;
        Ok(Self {
            database,
            options,
            admin: Some(admin),
            isolated: None,
            storage: PostgresStorage::new(pool, Arc::new(cc_lb_clock::SystemClock)),
        })
    }

    async fn drop_database(self) -> TestResult {
        self.storage.pool().close().await;
        if let Some(isolated) = self.isolated {
            if let Some(admin) = self.admin {
                admin.close().await;
            }
            isolated.drop().await?;
            return Ok(());
        }
        if let Some(admin) = self.admin {
            sqlx::query(AssertSqlSafe(format!(
                "DROP DATABASE {} WITH (FORCE)",
                self.database
            )))
            .execute(&admin)
            .await?;
            admin.close().await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn attribution_cursor_preserves_numeric_xid_order_across_digit_boundary() -> TestResult {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skipped: CI_POSTGRES_URL unset; xid ordering assertions did not run");
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let result: TestResult = async {
        let storage = &fixture.storage;
        let mut connection = sqlx::PgConnection::connect_with(&fixture.options).await?;
        sqlx::migrate!("./migrations").run_direct(Some(169), &mut connection, false).await?;
        connection.close().await?;
        // Historical xid8 values are controlled independently of current cluster transactions.
        sqlx::raw_sql(
            "INSERT INTO request_events_v1
             (request_id,event_id,tx_id,ts,list_ts_ms,list_status,duration_ms,upstream_id)
             SELECT 'xid-'||i,'xid-'||i,
                    (CASE WHEN i<=10000 THEN '8' WHEN i<=20000 THEN '9' ELSE '10' END)::xid8,
                    to_timestamp(1800000000),1800000000000,403,0,
                    '00000000-0000-0000-0000-000000001300'::uuid
             FROM generate_series(1,20001) AS i;
             INSERT INTO overview_excluded_error_rollups_v1
             (resolution,bucket_start_unix_secs,error_count,updated_at)
             VALUES ('minute',1800000000,20001,NOW()),('hour',1800000000,20001,NOW());
             INSERT INTO usage_rollup_checkpoints_v1 (id,value,value_xid)
             VALUES ('high_water',20001,'10'::xid8);",
        ).execute(storage.pool()).await?;
        storage.initialize().await?;
        assert_eq!(snapshot(storage).await?, vec![
            ("hour".to_owned(),1800000000,UPSTREAM.to_owned(),20001),
            ("minute".to_owned(),1800000000,UPSTREAM.to_owned(),20001),
        ]);
        let cursor: (String,i64) = sqlx::query_as(
            "SELECT cursor_xid::text,cursor_seq FROM overview_excluded_error_attribution_v1 WHERE id=1",
        ).fetch_one(storage.pool()).await?;
        assert_eq!(cursor, ("10".to_owned(),20001));
        Ok(())
    }.await;
    let teardown = fixture.drop_database().await;
    result?;
    teardown
}

#[tokio::test]
async fn startup_attribution_rejects_insufficient_and_exhausted_legacy_totals() -> TestResult {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skipped: CI_POSTGRES_URL unset; undercount assertions did not run");
        return Ok(());
    };
    for legacy_count in [1_i64, 10000] {
        let fixture = Fixture::create(&url).await?;
        let result: TestResult = async {
            let storage = &fixture.storage;
            let mut connection = sqlx::PgConnection::connect_with(&fixture.options).await?;
            sqlx::migrate!("./migrations")
                .run_direct(Some(169), &mut connection, false)
                .await?;
            connection.close().await?;
            sqlx::query(
                "INSERT INTO request_events_v1
                 (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,upstream_id)
                 SELECT 'undercount-'||i,'undercount-'||i,to_timestamp(1800000000),
                        1800000000000,403,0,'00000000-0000-0000-0000-000000001300'::uuid
                 FROM generate_series(1,$1::bigint) AS i",
            )
            .bind(legacy_count + 1)
            .execute(storage.pool())
            .await?;
            sqlx::query(
                "INSERT INTO overview_excluded_error_rollups_v1
                 (resolution,bucket_start_unix_secs,error_count,updated_at)
                 VALUES ('minute',1800000000,$1,NOW()),('hour',1800000000,$1,NOW())",
            )
            .bind(legacy_count)
            .execute(storage.pool())
            .await?;
            sqlx::raw_sql(
                "INSERT INTO usage_rollup_checkpoints_v1 (id,value,value_xid)
                 SELECT 'high_water',r.seq,r.tx_id FROM request_events_v1 r
                 ORDER BY r.tx_id DESC,r.seq DESC LIMIT 1;",
            )
            .execute(storage.pool())
            .await?;
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
                other => {
                    panic!("expected explicit data inconsistency, not SQL/setup failure: {other}")
                }
            }
            let expected_id = if legacy_count == 1 { "" } else { UPSTREAM };
            assert_eq!(
                snapshot(storage).await?,
                vec![
                    (
                        "hour".to_owned(),
                        1800000000,
                        expected_id.to_owned(),
                        legacy_count
                    ),
                    (
                        "minute".to_owned(),
                        1800000000,
                        expected_id.to_owned(),
                        legacy_count
                    ),
                ]
            );
            let cursor: i64 = sqlx::query_scalar(
                "SELECT cursor_seq FROM overview_excluded_error_attribution_v1 WHERE id=1",
            )
            .fetch_one(storage.pool())
            .await?;
            assert_eq!(cursor, if legacy_count == 1 { 0 } else { 10000 });
            Ok(())
        }
        .await;
        let teardown = fixture.drop_database().await;
        result?;
        teardown?;
    }
    Ok(())
}

#[tokio::test]
async fn empty_actual_runner_marks_complete_before_any_backfill_batch() -> TestResult {
    let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
        eprintln!("skipped: CI_POSTGRES_URL unset; empty startup assertions did not run");
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let result: TestResult = async {
        let storage = &fixture.storage;
        let mut connection = sqlx::PgConnection::connect_with(&fixture.options).await?;
        let migrator = sqlx::migrate!("./migrations");
        migrator
            .run_direct(Some(169), &mut connection, false)
            .await?;
        migrator.run_direct(None, &mut connection, false).await?;
        connection.close().await?;
        let phase: String = sqlx::query_scalar(
            "SELECT phase FROM overview_excluded_error_attribution_v1 WHERE id=1",
        )
        .fetch_one(storage.pool())
        .await?;
        assert_eq!(
            phase, "complete",
            "empty source must finish within schema migration"
        );
        let legacy_exists: bool = sqlx::query_scalar(
            "SELECT to_regclass('overview_excluded_error_legacy_v1') IS NOT NULL",
        )
        .fetch_one(storage.pool())
        .await?;
        assert!(!legacy_exists);
        let mut blocker = storage.pool().begin().await?;
        sqlx::query("SELECT id FROM overview_excluded_error_attribution_v1 WHERE id=1 FOR UPDATE")
            .fetch_one(&mut *blocker)
            .await?;
        let initialized =
            tokio::time::timeout(std::time::Duration::from_secs(2), storage.initialize()).await;
        blocker.rollback().await?;
        initialized.expect("completed hook must not open a batch and lock progress state")?;
        storage.initialize().await?;
        assert!(snapshot(storage).await?.is_empty());
        Ok(())
    }
    .await;
    let teardown = fixture.drop_database().await;
    result?;
    teardown
}
