use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use cc_lb_storage_api::{MetaStore, UsageRollupResolution, UsageRollupStore};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, Connection as _, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn migration_166_to_167_is_metadata_only_bounded_and_backfill_resumes() -> TestResult {
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
        let blocked = migrator.run_direct(Some(167), &mut connection, false).await;
        assert!(blocked.is_err(),"migration must honor its one-second lock_timeout");
        assert!(started.elapsed() < Duration::from_secs(5));
        connection.close().await?;
        blocker.rollback().await?;
        let mut connection = fixture.connect_standalone().await?;
        let started = Instant::now();
        migrator.run_direct(Some(167), &mut connection, false).await?;
        assert!(started.elapsed() < Duration::from_secs(15),"metadata migration exceeded startup lock-hold target");
        let started = Instant::now();
        migrator.run_direct(Some(167), &mut connection, false).await?;
        assert!(started.elapsed() < Duration::from_secs(2),"already-applied migration should be a fast no-op");
        connection.close().await?;
        assert_eq!(totals(pool).await?,(0,0,0));
        let high_water = fixture.storage.usage_rollup_checkpoint().await?;
        let before = fixture.storage.usage_rollup_revision().await?;
        let first = fixture.storage.rollup_usage_once().await?;
        assert_eq!(first.processed_events,0);
        let cursor: (i64,String) = sqlx::query_as("SELECT value,value_xid::text FROM usage_rollup_checkpoints_v1 WHERE id='pricing_completeness_backfill_cursor'")
            .fetch_one(pool).await?;
        assert_eq!(cursor.0,10000);
        assert_ne!(cursor.1,"0");
        assert_eq!(totals(pool).await?,(6666,6668,6666));
        // A new storage handle resumes from durable checkpoint state instead of repeating the first batch.
        let resumed = PostgresStorage::new(pool.clone(),Arc::new(cc_lb_clock::SystemClock));
        resumed.rollup_usage_once().await?;
        assert_eq!(totals(pool).await?,(6666,6668,6668));
        assert_eq!(resumed.usage_rollup_checkpoint().await?,high_water);
        assert_ne!(resumed.usage_rollup_revision().await?,before);
        let revision = resumed.usage_rollup_revision().await?;
        resumed.rollup_usage_once().await?;
        assert_eq!(resumed.usage_rollup_revision().await?,revision);
        for resolution in [UsageRollupResolution::Minute,UsageRollupResolution::Hour] {
            let rows = resumed.query_usage_rollups_in_range(resolution,1800000000,1800003600).await?;
            let row = rows.iter().find(|r| r.model=="pricing").unwrap();
            assert_eq!((row.priced_event_count,row.unpriced_event_count,row.unmetered_event_count),(3333,3334,3334));
            if let Some(row) = rows.iter().find(|r| r.model=="legacy") {
                assert_eq!((row.priced_event_count,row.unpriced_event_count,row.unmetered_event_count),(0,0,0));
            }
        }
        let fresh = Fixture::create(167).await?.expect("same PostgreSQL DSN");
        let parity = shape(pool).await? == shape(fresh.storage.pool()).await?;
        fresh.drop_schema().await?;
        assert!(parity,"fast/current initialize schema must match incremental migration schema");
        sqlx::query("DROP TABLE usage_rollups_v2").execute(pool).await?;
        assert_eq!(resumed.usage_rollup_revision().await?,revision);
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn gemini_normal_and_nested_merges_preserve_pricing_classification() -> TestResult {
    let Some(fixture) = Fixture::create(167).await? else {
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
        }
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
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
    url: String,
    schema: String,
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
        let schema = format!("pricing_completeness_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await?;
        let search_path = format!("{schema}, public");
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                PgConnectOptions::from_str(&url)?.options([("search_path", search_path.as_str())]),
            )
            .await?;
        let storage = PostgresStorage::new(pool, Arc::new(cc_lb_clock::SystemClock));
        let fixture = Self {
            url,
            schema,
            admin,
            storage,
        };
        let setup: TestResult = async {
            if version == 167 {
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
        let search_path = format!("{}, public", self.schema);
        let options =
            PgConnectOptions::from_str(&self.url)?.options([("search_path", search_path.as_str())]);
        Ok(sqlx::PgConnection::connect_with(&options).await?)
    }

    async fn drop_schema(self) -> TestResult {
        self.storage.pool().close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await?;
        self.admin.close().await;
        Ok(())
    }
}
