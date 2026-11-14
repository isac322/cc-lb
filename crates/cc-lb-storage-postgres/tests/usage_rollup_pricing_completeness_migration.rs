use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use cc_lb_storage_api::{
    MetaStore, PricingCompleteness, PricingCompletenessInput, RequestEventStore,
    UsageRollupResolution, UsageRollupStore, derive_pricing_completeness,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, Connection as _, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn migration_166_to_168_is_metadata_only_and_never_repairs_history() -> TestResult {
    let Some(fixture) = Fixture::create(166).await? else {
        return Ok(());
    };
    let result: TestResult = async {
        let pool = fixture.storage.pool();
        // Two committed transactions ensure the historical cursor must preserve both tx_id and seq.
        for (start,end) in [(1_i64,6000_i64),(6001,10001)] {
            sqlx::query(
                "INSERT INTO request_events_v1
                 (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,model,input_tokens,list_cost_usd_micros)
                 SELECT 'pricing-'||i,'pricing-'||i,to_timestamp(1800000000),1800000000000,200,0,'pricing',
                        CASE WHEN i%3=1 THEN 1 ELSE 0 END,CASE WHEN i%3=0 THEN 0 ELSE NULL END
                 FROM generate_series($1,$2) AS i",
            ).bind(start).bind(end).execute(pool).await?;
        }
        sqlx::raw_sql(
            "INSERT INTO usage_rollups_v2 (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,updated_at)
             VALUES ('minute',1800000000,'unknown','00000000-0000-0000-0000-000000000000','unknown','pricing',10001,NOW()),
                    ('hour',1800000000,'unknown','00000000-0000-0000-0000-000000000000','unknown','pricing',10001,NOW()),
                    ('minute',1800000060,'legacy','00000000-0000-0000-0000-000000000000','unknown','legacy',1,NOW());
             INSERT INTO usage_rollup_checkpoints_v1 (id,value,value_xid)
             SELECT id,seq,tx_id FROM (VALUES ('high_water'),('cost_components_backfill_upper'),('cost_components_backfill_cursor')) AS ids(id)
             CROSS JOIN LATERAL (SELECT seq,tx_id FROM request_events_v1 ORDER BY tx_id DESC,seq DESC LIMIT 1) AS last_event;
             UPDATE usage_rollup_reconciliations_v1 SET status='complete';"
        ).execute(pool).await?;
        let migrator = sqlx::migrate!("./migrations");
        let mut blocker = pool.begin().await?;
        sqlx::query("LOCK TABLE usage_rollups_v2 IN ACCESS SHARE MODE").execute(&mut *blocker).await?;
        let mut connection = fixture.connect_standalone().await?;
        let started = Instant::now();
        let blocked = migrator.run_direct(Some(168), &mut connection, false).await;
        assert!(blocked.is_err(),"migration must honor its one-second lock_timeout");
        assert!(started.elapsed() < Duration::from_secs(5));
        connection.close().await?;
        blocker.rollback().await?;
        let mut connection = fixture.connect_standalone().await?;
        let started = Instant::now();
        migrator.run_direct(Some(168), &mut connection, false).await?;
        assert!(started.elapsed() < Duration::from_secs(15),"metadata migration exceeded startup lock-hold target");
        let started = Instant::now();
        migrator.run_direct(Some(168), &mut connection, false).await?;
        assert!(started.elapsed() < Duration::from_secs(2),"already-applied migration should be a fast no-op");
        connection.close().await?;
        assert_eq!(totals(pool).await?, (0, 0, 0));
        let high_water = fixture.storage.usage_rollup_checkpoint().await?;
        let revision = fixture.storage.usage_rollup_revision().await?;
        let resumed = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
        for _ in 0..4 {
            let run = resumed.rollup_usage_once().await?;
            assert_eq!(run.processed_events, 0);
            assert_eq!(run.updated_rollups, 0);
            assert_eq!(totals(pool).await?, (0, 0, 0));
        }
        assert_eq!(resumed.usage_rollup_checkpoint().await?, high_water);
        assert_eq!(resumed.usage_rollup_revision().await?, revision);
        let legacy: Vec<(i64, i64, i64, i64, Option<i64>)> = sqlx::query_as(
            "SELECT request_count,priced_event_count,unpriced_event_count,unmetered_event_count, \
             effective_cache_write_input_tokens FROM usage_rollups_v2 ORDER BY resolution,model",
        ).fetch_all(pool).await?;
        assert_eq!(legacy.len(), 3);
        assert!(legacy.iter().all(|(_, p, u, m, effective)| (*p, *u, *m, *effective) == (0, 0, 0, None)));
        assert_eq!(legacy.iter().map(|row| row.0).sum::<i64>(), 20003);
        let lifecycle_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM usage_rollup_checkpoints_v1 WHERE id LIKE 'pricing_completeness_%'",
        ).fetch_one(pool).await?;
        assert_eq!(lifecycle_count, 0);
        let stage_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
             WHERE table_schema=current_schema() AND table_name='usage_rollup_pricing_backfill_stage_v1')",
        ).fetch_one(pool).await?;
        assert!(!stage_exists);
        let fresh = Fixture::create(168).await?.expect("same PostgreSQL DSN");
        let parity = shape(pool).await? == shape(fresh.storage.pool()).await?;
        fresh.storage.verify_schema_current().await?;
        fresh.drop_schema().await?;
        assert!(parity, "fast/current initialize schema must match incremental migration schema");
        resumed.initialize().await?;
        resumed.verify_schema_current().await?;
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn gemini_normal_and_nested_merges_preserve_pricing_classification() -> TestResult {
    let Some(fixture) = Fixture::create(168).await? else {
        return Ok(());
    };
    let result: TestResult = async {
        let pool = fixture.storage.pool();
        let id = Uuid::from_u128(1280);
        sqlx::query("INSERT INTO upstream_spec_v1 (id,name,kind,enabled,warmup_enabled,spec_revision,created_at,updated_at) VALUES ($1,'pricing-gemini','gemini_api_key',TRUE,FALSE,1,NOW(),NOW())")
            .bind(id).execute(pool).await?;
        for nested in [false,true] {
            sqlx::query("DELETE FROM usage_rollups_v2").execute(pool).await?;
            sqlx::query("DELETE FROM usage_rollup_reconciliation_markers_v1").execute(pool).await?;
            let target = if nested { "models_gemini" } else { "gemini" };
            let source = if nested { "models_models_gemini" } else { "models_gemini" };
            for model in [target,source] {
                sqlx::query("INSERT INTO usage_rollups_v2 (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model,request_count,priced_event_count,unpriced_event_count,unmetered_event_count,updated_at) VALUES ('minute',1800000000,'pricing',$1,'pricing-gemini',$2,6,1,2,3,NOW())")
                    .bind(id).bind(model).execute(pool).await?;
            }
            if nested {
                sqlx::query("INSERT INTO usage_rollup_reconciliation_markers_v1 (resolution,bucket_start_unix_secs,principal_id,upstream_id,marker_model) VALUES ('minute',1800000000,'pricing',$1,'models_gemini')")
                    .bind(id).execute(pool).await?;
            }
            sqlx::query("UPDATE usage_rollup_reconciliations_v1 SET status=$1,cursor=0,event_upper_bound=0,rollup_upper_bound=(SELECT FLOOR(EXTRACT(EPOCH FROM MAX(updated_at))*1000)::BIGINT FROM usage_rollups_v2)")
                .bind(if nested { "rewriting_nested" } else { "rewriting" }).execute(pool).await?;
            for _ in 0..4 { fixture.storage.rollup_usage_once().await?; }
            let rows = fixture.storage.query_usage_rollups_in_range(UsageRollupResolution::Minute,1800000000,1800000060).await?;
            assert_eq!(rows.len(),1);
            assert_eq!(rows[0].model,target);
            assert_eq!(rows[0].request_count,12);
            assert_eq!((rows[0].priced_event_count,rows[0].unpriced_event_count,rows[0].unmetered_event_count),(2,4,6));
            assert_eq!(rows[0].effective_cache_write_input_tokens, None);
        }
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn historical_cost_components_recover_without_classifying_pricing() -> TestResult {
    let Some(fixture) = Fixture::create(166).await? else {
        return Ok(());
    };
    let result: TestResult = async {
        let pool = fixture.storage.pool();
        sqlx::raw_sql(
            "INSERT INTO request_events_v1 \
             (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,model,input_tokens, \
              list_cost_usd_micros,list_cost_input_micros) \
             VALUES ('category-history','category-history',to_timestamp(1800000000), \
                     1800000000000,200,0,'category-history',5,50,50); \
             INSERT INTO usage_rollups_v2 \
             (resolution,bucket_start_unix_secs,principal_id,upstream_id,upstream_name,model, \
              request_count,input_tokens,virtual_cost_micros,updated_at) \
             SELECT resolution,1800000000,'unknown','00000000-0000-0000-0000-000000000000', \
                    'unknown','category-history',1,5,50,NOW() \
             FROM (VALUES ('minute'),('hour')) AS resolutions(resolution); \
             INSERT INTO usage_rollup_checkpoints_v1 (id,value,value_xid) \
             SELECT 'high_water',seq,tx_id FROM request_events_v1; \
             UPDATE usage_rollup_reconciliations_v1 SET status='complete';",
        )
        .execute(pool)
        .await?;
        let mut connection = fixture.connect_standalone().await?;
        sqlx::migrate!("./migrations")
            .run_direct(Some(168), &mut connection, false)
            .await?;
        connection.close().await?;
        let high_water = fixture.storage.usage_rollup_checkpoint().await?;
        let revision = fixture.storage.usage_rollup_revision().await?;
        assert_eq!(
            fixture.storage.rollup_usage_once().await?.processed_events,
            0
        );
        assert_eq!(fixture.storage.usage_rollup_checkpoint().await?, high_water);
        assert_ne!(fixture.storage.usage_rollup_revision().await?, revision);
        assert_eq!(totals(pool).await?, (0, 0, 0));
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let rows = fixture
                .storage
                .query_usage_rollups_in_range(resolution, 1_800_000_000, 1_800_003_600)
                .await?;
            assert_eq!(rows.len(), 1);
            let row = &rows[0];
            assert_eq!(
                (
                    row.request_count,
                    row.input_tokens,
                    row.virtual_cost_micros,
                    row.cost_input_micros,
                    row.cost_components_event_count
                ),
                (1, 5, 50, 50, 1)
            );
            assert_eq!(row.effective_cache_write_input_tokens, None);
            assert_eq!(
                derive_pricing_completeness(PricingCompletenessInput {
                    request_count: row.request_count,
                    priced_event_count: row.priced_event_count,
                    unpriced_event_count: row.unpriced_event_count,
                    unmetered_event_count: row.unmetered_event_count,
                    virtual_cost_micros: row.virtual_cost_micros,
                }),
                Some(PricingCompleteness::Partial)
            );
        }
        let revision = fixture.storage.usage_rollup_revision().await?;
        for _ in 0..3 {
            assert_eq!(
                fixture.storage.rollup_usage_once().await?.processed_events,
                0
            );
            assert_eq!(fixture.storage.usage_rollup_revision().await?, revision);
        }
        let components: i64 =
            sqlx::query_scalar("SELECT SUM(cost_input_micros)::BIGINT FROM usage_rollups_v2")
                .fetch_one(pool)
                .await?;
        assert_eq!(
            components, 100,
            "independent cost backfill must remain exactly-once"
        );
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn xid_seq_inversion_prune_and_bucket_recreation_never_repair_history() -> TestResult {
    for intact in [true, false] {
        let Some(fixture) = Fixture::create(168).await? else {
            return Ok(());
        };
        let result: TestResult = async {
            const H: u64 = 1_800_003_600;
            let pool = fixture.storage.pool();
            sqlx::query(
                "INSERT INTO request_events_v1 \
                 (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,model,input_tokens,list_cost_usd_micros) \
                 SELECT 'inv-filler-'||i,'inv-filler-'||i,TO_TIMESTAMP(1800003600 - 7200),1799996400000,200,0,'inv-filler',1,1 \
                 FROM GENERATE_SERIES(1,999) AS i",
            )
            .execute(pool)
            .await?;
            sqlx::query("UPDATE usage_rollup_reconciliations_v1 SET status='complete'")
                .execute(pool)
                .await?;
            let mut conn_a = fixture.connect_standalone().await?;
            let mut conn_b = fixture.connect_standalone().await?;
            let mut conn_c = fixture.connect_standalone().await?;
            sqlx::query("BEGIN").execute(&mut conn_a).await?;
            let x1: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
                .fetch_one(&mut conn_a)
                .await?;
            sqlx::query("BEGIN").execute(&mut conn_b).await?;
            let x2: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
                .fetch_one(&mut conn_b)
                .await?;
            sqlx::query("BEGIN").execute(&mut conn_c).await?;
            let x3: String = sqlx::query_scalar("SELECT pg_current_xact_id()::text")
                .fetch_one(&mut conn_c)
                .await?;
            assert!(x1.parse::<u64>()? < x2.parse::<u64>()?);
            assert!(x2.parse::<u64>()? < x3.parse::<u64>()?);
            // Assign sequence numbers in the opposite order from transaction IDs.
            let insert = "INSERT INTO request_events_v1 \
                (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,model,input_tokens,list_cost_usd_micros) \
                VALUES ($1,$1,TO_TIMESTAMP($2::double precision),$3,200,0,$4,10,$5) RETURNING seq";
            let c_seq: i64 = sqlx::query_scalar(insert)
                .bind("inv-C")
                .bind((H + 400) as f64)
                .bind(((H + 400) * 1000) as i64)
                .bind("inv-other")
                .bind(Some(5_i64))
                .fetch_one(&mut conn_c)
                .await?;
            let a_seq: i64 = sqlx::query_scalar(insert)
                .bind("inv-A")
                .bind((H + 350) as f64)
                .bind(((H + 350) * 1000) as i64)
                .bind("inv-target")
                .bind(Some(0_i64))
                .fetch_one(&mut conn_a)
                .await?;
            let b_seq: i64 = sqlx::query_scalar(insert)
                .bind("inv-B")
                .bind((H + 340) as f64)
                .bind(((H + 340) * 1000) as i64)
                .bind("inv-target")
                .bind(None::<i64>)
                .fetch_one(&mut conn_b)
                .await?;
            assert_eq!((c_seq, a_seq, b_seq), (1000, 1001, 1002));
            sqlx::query("COMMIT").execute(&mut conn_a).await?;
            wait_until_xid_below_xmin(pool, &x1).await?;
            fixture.storage.rollup_usage_once().await?;
            fixture
                .storage
                .prune_usage_rollups_before(H + 360, H + 330, 2000)
                .await?;
            sqlx::query("COMMIT").execute(&mut conn_b).await?;
            sqlx::query("COMMIT").execute(&mut conn_c).await?;
            conn_a.close().await?;
            conn_b.close().await?;
            conn_c.close().await?;
            wait_until_xid_below_xmin(pool, &x3).await?;
            fixture.storage.rollup_usage_once().await?;
            let high_water: (i64, String) = sqlx::query_as(
                "SELECT value,value_xid::text FROM usage_rollup_checkpoints_v1 WHERE id='high_water'",
            )
            .fetch_one(pool)
            .await?;
            assert_eq!(high_water, (1000, x3));
            if !intact {
                assert_eq!(
                    fixture
                        .storage
                        .prune_request_events_before((H + 345) * 1000 * 1_000_000, 1000)
                        .await?,
                    1000,
                );
            }
            sqlx::query(
                "UPDATE usage_rollups_v2 SET priced_event_count=0,unpriced_event_count=0,unmetered_event_count=0",
            )
            .execute(pool)
            .await?;
            for _ in 0..4 {
                fixture.storage.rollup_usage_once().await?;
            }
            for (resolution, bucket_start) in [
                (UsageRollupResolution::Hour, H),
                (UsageRollupResolution::Minute, H + 300),
            ] {
                let rows = fixture
                    .storage
                    .query_usage_rollups_in_range(resolution, bucket_start, bucket_start + 3600)
                    .await?;
                let row = rows
                    .iter()
                    .find(|row| row.model == "inv-target")
                    .expect("recreated target bucket");
                assert_eq!(
                    (
                        row.request_count,
                        row.priced_event_count,
                        row.unpriced_event_count,
                        row.unmetered_event_count,
                    ),
                    (1, 0, 0, 0),
                    "intact={intact}, resolution={resolution:?}",
                );
                assert_eq!(
                    derive_pricing_completeness(PricingCompletenessInput {
                        request_count: row.request_count,
                        priced_event_count: row.priced_event_count,
                        unpriced_event_count: row.unpriced_event_count,
                        unmetered_event_count: row.unmetered_event_count,
                        virtual_cost_micros: row.virtual_cost_micros,
                    }),
                    Some(PricingCompleteness::Unknown),
                );
            }
            Ok(())
        }
        .await;
        let teardown = fixture.drop_schema().await;
        result?;
        teardown?;
    }
    Ok(())
}

#[tokio::test]
async fn old_worker_gaps_and_new_live_classification_remain_additive() -> TestResult {
    let Some(fixture) = Fixture::create(168).await? else {
        return Ok(());
    };
    let result: TestResult = async {
        let pool = fixture.storage.pool();
        sqlx::query("UPDATE usage_rollup_reconciliations_v1 SET status='complete'")
            .execute(pool).await?;
        let event = cc_lb_storage_api::RequestEvent {
            request_id: "live-pricing".to_owned(),
            event_id: Some("live-pricing".to_owned()),
            ts: 1_800_000_001,
            ts_ms: Some(1_800_000_001_000),
            model: Some("forward-only".to_owned()),
            status: 200,
            input_tokens: Some(5),
            cost_usd_micros: Some(0),
            ..Default::default()
        };
        fixture.storage.append_request_event(&event).await?;
        let xid: String = sqlx::query_scalar("SELECT tx_id::text FROM request_events_v1 LIMIT 1")
            .fetch_one(pool).await?;
        wait_until_xid_below_xmin(pool, &xid).await?;
        assert_eq!(fixture.storage.rollup_usage_once().await?.processed_events, 1);
        // An old worker advances the SAME ordinary checkpoint and totals without
        // supplying classification counters. Retained raw evidence is not repair authority.
        sqlx::query(
            "INSERT INTO request_events_v1 \
             (request_id,event_id,ts,list_ts_ms,list_status,duration_ms,model,input_tokens,list_cost_usd_micros) \
             VALUES ('old-worker','old-worker',to_timestamp(1800000001),1800000001000,200,0,'forward-only',7,9)",
        ).execute(pool).await?;
        sqlx::raw_sql(
            "UPDATE usage_rollups_v2 SET request_count=request_count+1,input_tokens=input_tokens+7, \
                 virtual_cost_micros=virtual_cost_micros+9; \
             UPDATE usage_rollup_checkpoints_v1 SET (value,value_xid)= \
                 (SELECT seq,tx_id FROM request_events_v1 ORDER BY tx_id DESC,seq DESC LIMIT 1) \
                 WHERE id='high_water';",
        ).execute(pool).await?;
        let old_checkpoint = fixture.storage.usage_rollup_checkpoint().await?;
        for _ in 0..3 {
            assert_eq!(fixture.storage.rollup_usage_once().await?.processed_events, 0);
            assert_eq!(totals(pool).await?, (2, 0, 0));
        }
        let next = cc_lb_storage_api::RequestEvent {
            request_id: "live-unpriced".to_owned(),
            event_id: Some("live-unpriced".to_owned()),
            cost_usd_micros: None,
            input_tokens: None,
            cache_write_input_tokens_30m: Some(11),
            ..event.clone()
        };
        fixture.storage.append_request_event(&next).await?;
        let unmetered = cc_lb_storage_api::RequestEvent {
            request_id: "live-unmetered".to_owned(),
            event_id: Some("live-unmetered".to_owned()),
            input_tokens: None,
            cost_usd_micros: None,
            ..event.clone()
        };
        fixture.storage.append_request_event(&unmetered).await?;
        let xid: String = sqlx::query_scalar(
            "SELECT tx_id::text FROM request_events_v1 ORDER BY tx_id DESC,seq DESC LIMIT 1",
        ).fetch_one(pool).await?;
        wait_until_xid_below_xmin(pool, &xid).await?;
        // Both workers contend on the ordinary advisory transaction lock.
        let (left, right) = tokio::join!(
            fixture.storage.rollup_usage_once(),
            fixture.storage.rollup_usage_once(),
        );
        assert_eq!(left?.processed_events + right?.processed_events, 2);
        assert_ne!(fixture.storage.usage_rollup_checkpoint().await?, old_checkpoint);
        let revision = fixture.storage.usage_rollup_revision().await?;
        fixture.storage.append_request_event(&next).await?;
        assert_eq!(fixture.storage.rollup_usage_once().await?.processed_events, 0);
        assert_eq!(fixture.storage.usage_rollup_revision().await?, revision);
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let rows = fixture.storage.query_usage_rollups_in_range(
                resolution, 1_800_000_000, 1_800_003_600,
            ).await?;
            assert_eq!(rows.len(), 1);
            let row = &rows[0];
            assert_eq!((row.request_count, row.priced_event_count, row.unpriced_event_count,
                row.unmetered_event_count, row.input_tokens, row.virtual_cost_micros),
                (4, 1, 1, 1, 12, 9));
            assert_eq!(row.effective_cache_write_input_tokens, Some(11));
            assert_eq!(derive_pricing_completeness(PricingCompletenessInput {
                request_count: row.request_count,
                priced_event_count: row.priced_event_count,
                unpriced_event_count: row.unpriced_event_count,
                unmetered_event_count: row.unmetered_event_count,
                virtual_cost_micros: row.virtual_cost_micros,
            }), Some(PricingCompleteness::Partial));
        }
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

async fn wait_until_xid_below_xmin(pool: &PgPool, xid: &str) -> TestResult {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let visible: bool =
                sqlx::query_scalar("SELECT pg_snapshot_xmin(pg_current_snapshot()) > $1::xid8")
                    .bind(xid)
                    .fetch_one(pool)
                    .await?;
            if visible {
                return Ok::<(), sqlx::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

async fn totals(pool: &PgPool) -> TestResult<(i64, i64, i64)> {
    Ok(sqlx::query_as("SELECT SUM(priced_event_count)::BIGINT,SUM(unpriced_event_count)::BIGINT,SUM(unmetered_event_count)::BIGINT FROM usage_rollups_v2")
        .fetch_one(pool).await?)
}

async fn shape(pool: &PgPool) -> TestResult<Vec<(String, String, String, Option<String>)>> {
    Ok(sqlx::query_as("SELECT column_name,data_type,is_nullable,column_default FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='usage_rollups_v2' ORDER BY ordinal_position")
        .fetch_all(pool).await?)
}

struct Fixture {
    database: String,
    options: PgConnectOptions,
    admin: PgPool,
    storage: PostgresStorage,
}

impl Fixture {
    async fn create(version: i64) -> TestResult<Option<Self>> {
        let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
            eprintln!(
                "skipped: CI_POSTGRES_URL unset; PostgreSQL pricing migration assertions did not run"
            );
            return Ok(None);
        };
        // Advisory locks and MVCC xmin horizons are database-scoped in PostgreSQL.
        let database = format!("cc_lb_pricing_{}", Uuid::new_v4().simple());
        let schema = format!("pricing_completeness_{}", Uuid::new_v4().simple());
        let base_options = PgConnectOptions::from_str(&url)?;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE DATABASE {database}")))
            .execute(&admin)
            .await?;
        let setup_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database(&database))
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&setup_pool)
            .await?;
        setup_pool.close().await;
        let search_path = format!("{schema}, public");
        let options = base_options
            .database(&database)
            .options([("search_path", search_path.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options.clone())
            .await?;
        let storage = PostgresStorage::new(pool, Arc::new(cc_lb_clock::SystemClock));
        let fixture = Self {
            database,
            options,
            admin,
            storage,
        };
        let setup: TestResult = async {
            if version == 168 {
                fixture.storage.initialize().await?;
            } else {
                let mut connection = fixture.connect_standalone().await?;
                let res = sqlx::migrate!("./migrations")
                    .run_direct(Some(version), &mut connection, false)
                    .await;
                connection.close().await?;
                res?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = setup {
            fixture.drop_schema().await?;
            return Err(error);
        }
        Ok(Some(fixture))
    }

    async fn connect_standalone(&self) -> TestResult<sqlx::PgConnection> {
        Ok(sqlx::PgConnection::connect_with(&self.options).await?)
    }

    async fn drop_schema(self) -> TestResult {
        self.storage.pool().close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP DATABASE {} WITH (FORCE)",
            self.database
        )))
        .execute(&self.admin)
        .await?;
        self.admin.close().await;
        Ok(())
    }
}
