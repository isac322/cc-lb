use std::{sync::Mutex, time::Duration};

use cc_lb_storage_api::{
    MetaStore, RequestEvent, RequestEventStore, UsageRollup, UsageRollupResolution,
    UsageRollupStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgPoolOptions};
use tokio::{runtime::Runtime, time};
use url::Url;
use uuid::Uuid;

const TIMEOUT_ITERATIONS: usize = 50;
const CONNECTION_DROP_ITERATIONS: usize = 50;
const EVENTS_PER_ITERATION: u64 = 5;
static CRASH_RECOVERY_SCENARIO_LOCK: Mutex<()> = Mutex::new(());
fn get_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

#[test]
fn client_timeout_mid_commit() {
    let _guard = CRASH_RECOVERY_SCENARIO_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let url = match get_postgres_url() {
        Some(url) => url,
        None => {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return;
        }
    };

    Runtime::new()
        .expect("tokio runtime")
        .block_on(async move {
            let schema = schema_name("timeout", 0);
            let fixture = CrashFixture::create(&url, &schema, "timeout", 0.05).await?;
            let storage = fixture.storage.clone();

            for iter in 0..TIMEOUT_ITERATIONS {
                if iter > 0 {
                    fixture.reset_iteration().await?;
                }

                let result =
                    time::timeout(Duration::from_millis(1), storage.rollup_usage_once()).await;

                let consistency = fixture.consistency().await?;
                println!(
                    "iter {iter} result={} timeout_elapsed={} rollup_result={}",
                    consistency.as_str(),
                    result.is_err(),
                    rollup_result_label(&result),
                );
                assert!(
                    !consistency.is_partial(),
                    "iter {iter}: partial commit detected: {}",
                    consistency.details()
                );
            }

            fixture.drop_database().await?;

            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .expect("client timeout crash recovery test");
}

#[test]
fn connection_drop_mid_commit() {
    let _guard = CRASH_RECOVERY_SCENARIO_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let url = match get_postgres_url() {
        Some(url) => url,
        None => {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return;
        }
    };

    Runtime::new()
        .expect("tokio runtime")
        .block_on(async move {
            let schema = schema_name("drop", 0);
            let mut fixture = CrashFixture::create(&url, &schema, "drop", 2.0).await?;

            for iter in 0..CONNECTION_DROP_ITERATIONS {
                if iter > 0 {
                    fixture.reset_iteration().await?;
                }

                let storage = fixture.storage.clone();
                let pool = fixture.pool.clone();

                fixture.wait_for_events_eligible().await?;
                let handle = tokio::spawn(async move { storage.rollup_usage_once().await });
                let pid = fixture.wait_for_checkpoint_backend().await?;
                let terminated = sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                    .bind(pid)
                    .fetch_one(&fixture.admin_pool)
                    .await?;
                assert!(
                    terminated,
                    "iter {iter}: pg_terminate_backend returned false"
                );
                pool.close().await;

                let rollup_result = handle.await?;
                let verifier = fixture.reconnect().await?;
                let consistency = verifier.consistency().await?;
                println!(
                    "iter {iter} result={} backend_pid={} terminated={} rollup_result={}",
                    consistency.as_str(),
                    pid,
                    terminated,
                    storage_result_label(&rollup_result),
                );
                assert!(
                    !consistency.is_partial(),
                    "iter {iter}: partial commit detected: {}",
                    consistency.details()
                );

                fixture = verifier;
            }

            fixture.drop_database().await?;

            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .expect("connection drop crash recovery test");
}

#[derive(Clone)]
struct CrashFixture {
    url: String,
    database: String,
    schema: String,
    app_name: String,
    checkpoint_lock_key: i32,
    admin_pool: PgPool,
    pool: PgPool,
    storage: PostgresStorage,
}

impl CrashFixture {
    async fn create(
        url: &str,
        schema: &str,
        scenario: &str,
        checkpoint_sleep_secs: f64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Advisory locks and xmin horizons are database-scoped; isolate this
        // crash injection from other tests running in the same PostgreSQL service.
        let database = format!("cc_lb_crash_{scenario}_{}", Uuid::new_v4().simple());
        let mut admin_url = Url::parse(url)?;
        admin_url.set_path("/postgres");
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(admin_url.as_str())
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE DATABASE {}",
            quote_ident(&database)
        )))
        .execute(&admin_pool)
        .await?;

        let mut database_url = Url::parse(url)?;
        database_url.set_path(&format!("/{database}"));
        let setup_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(database_url.as_str())
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(schema)
        )))
        .execute(&setup_pool)
        .await?;
        setup_pool.close().await;

        let app_name = format!("cc_lb_crash_{scenario}_{}", Uuid::new_v4().simple());
        let checkpoint_lock_key = (u32::from_be_bytes(Uuid::new_v4().as_bytes()[..4].try_into()?)
            & 0x7fff_ffff)
            .max(1) as i32;
        let pool = schema_pool(database_url.as_str(), schema, &app_name, 1).await?;
        let storage =
            PostgresStorage::new(pool.clone(), std::sync::Arc::new(cc_lb_clock::SystemClock));
        storage.initialize().await?;
        // A fresh schema may spend its first rollup pass completing the
        // pending Gemini model-reconciliation migration. Finish that pass
        // before installing the checkpoint fault trigger so the crash tests
        // exercise the intended checkpoint transaction.
        storage.rollup_usage_once().await?;
        install_checkpoint_sleep_trigger(&pool, checkpoint_sleep_secs, checkpoint_lock_key).await?;
        seed_request_events(&storage).await?;

        Ok(Self {
            url: database_url.to_string(),
            database,
            schema: schema.to_owned(),
            app_name,
            checkpoint_lock_key,
            admin_pool,
            pool,
            storage,
        })
    }

    async fn reconnect(&self) -> Result<Self, Box<dyn std::error::Error>> {
        let pool = schema_pool(&self.url, &self.schema, &self.app_name, 1).await?;
        let storage =
            PostgresStorage::new(pool.clone(), std::sync::Arc::new(cc_lb_clock::SystemClock));
        Ok(Self {
            url: self.url.clone(),
            database: self.database.clone(),
            schema: self.schema.clone(),
            app_name: self.app_name.clone(),
            checkpoint_lock_key: self.checkpoint_lock_key,
            admin_pool: self.admin_pool.clone(),
            pool,
            storage,
        })
    }

    async fn reset_iteration(&self) -> Result<(), Box<dyn std::error::Error>> {
        sqlx::query(
            "TRUNCATE TABLE request_events_v1, usage_rollups_v2, usage_rollup_checkpoints_v1 RESTART IDENTITY CASCADE",
        )
        .execute(&self.pool)
        .await?;
        seed_request_events(&self.storage).await?;
        Ok(())
    }

    async fn consistency(&self) -> Result<Consistency, Box<dyn std::error::Error>> {
        let checkpoint = self.storage.usage_rollup_checkpoint().await?;
        let mut rollups = Vec::new();
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            rollups.extend(
                self.storage
                    .query_usage_rollups_in_range(resolution, 0, i64::MAX.unsigned_abs())
                    .await?,
            );
        }

        if checkpoint == Some(EVENTS_PER_ITERATION) && expected_rollups_visible(&rollups) {
            return Ok(Consistency::AllVisible);
        }
        if checkpoint.is_none() && rollups.is_empty() {
            return Ok(Consistency::AllGone);
        }

        Ok(Consistency::Partial {
            checkpoint,
            rollups,
        })
    }

    async fn wait_for_checkpoint_backend(&self) -> Result<i32, Box<dyn std::error::Error>> {
        for _ in 0..1000 {
            let pid = sqlx::query_scalar::<_, i32>(
                "SELECT pid FROM pg_locks \
                 WHERE locktype = 'advisory' \
                   AND classid = 0 \
                   AND objid = $1::oid \
                   AND granted \
                 LIMIT 1",
            )
            .bind(self.checkpoint_lock_key)
            .fetch_optional(&self.admin_pool)
            .await?;

            if let Some(pid) = pid {
                return Ok(pid);
            }

            time::sleep(Duration::from_millis(2)).await;
        }

        Err(format!(
            "timed out waiting for checkpoint advisory lock {}",
            self.checkpoint_lock_key
        )
        .into())
    }

    // The rollup only consumes events whose tx_id has fallen below the snapshot
    // xmin horizon, and a concurrent transaction elsewhere in the cluster can
    // hold that horizon back. Wait until every seeded event is eligible so the
    // rollup deterministically reaches its checkpoint write instead of
    // committing an empty no-op (which would starve the crash injection below).
    async fn wait_for_events_eligible(&self) -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..7500 {
            let eligible = sqlx::query_scalar::<_, bool>(
                "SELECT NOT EXISTS (                      SELECT 1 FROM request_events_v1                      WHERE tx_id >= pg_snapshot_xmin(pg_current_snapshot())                  )",
            )
            .fetch_one(&self.pool)
            .await?;

            if eligible {
                return Ok(());
            }

            time::sleep(Duration::from_millis(2)).await;
        }

        Err(format!(
            "timed out waiting for seeded events to fall below the snapshot xmin horizon for {}",
            self.app_name
        )
        .into())
    }

    async fn drop_database(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP DATABASE {}",
            quote_ident(&self.database)
        )))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

#[derive(Debug)]
enum Consistency {
    AllVisible,
    AllGone,
    Partial {
        checkpoint: Option<u64>,
        rollups: Vec<UsageRollup>,
    },
}

impl Consistency {
    fn as_str(&self) -> &'static str {
        match self {
            Self::AllVisible => "all_visible",
            Self::AllGone => "all_gone",
            Self::Partial { .. } => "partial",
        }
    }

    fn is_partial(&self) -> bool {
        matches!(self, Self::Partial { .. })
    }

    fn details(&self) -> String {
        match self {
            Self::AllVisible => "checkpoint and rollups are all visible".to_owned(),
            Self::AllGone => "checkpoint and rollups are all gone".to_owned(),
            Self::Partial {
                checkpoint,
                rollups,
            } => {
                format!("checkpoint={checkpoint:?} rollups={rollups:?}")
            }
        }
    }
}

async fn schema_pool(
    url: &str,
    schema: &str,
    app_name: &str,
    max_connections: u32,
) -> Result<PgPool, sqlx::Error> {
    let search_path = format!("{}, public", quote_ident(schema));
    let app_name = app_name.to_owned();

    PgPoolOptions::new()
        .max_connections(max_connections)
        .after_connect(move |connection, _metadata| {
            let search_path = search_path.clone();
            let app_name = app_name.clone();
            Box::pin(async move {
                sqlx::query("SELECT set_config('search_path', $1, false)")
                    .bind(&search_path)
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SELECT set_config('application_name', $1, false)")
                    .bind(&app_name)
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect(url)
        .await
}

async fn install_checkpoint_sleep_trigger(
    pool: &PgPool,
    sleep_secs: f64,
    checkpoint_lock_key: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(AssertSqlSafe(format!(
        "CREATE OR REPLACE FUNCTION cc_lb_sleep_before_checkpoint() \
         RETURNS trigger \
         LANGUAGE plpgsql \
         AS $$ \
         BEGIN \
             PERFORM pg_advisory_lock({checkpoint_lock_key}); \
             PERFORM pg_sleep({sleep_secs}); \
             RETURN NEW; \
         END; \
         $$"
    )))
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TRIGGER cc_lb_sleep_before_checkpoint_trigger \
         BEFORE INSERT OR UPDATE ON usage_rollup_checkpoints_v1 \
         FOR EACH ROW \
         WHEN (NEW.id = 'high_water') \
         EXECUTE FUNCTION cc_lb_sleep_before_checkpoint()",
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn seed_request_events(storage: &PostgresStorage) -> Result<(), Box<dyn std::error::Error>> {
    for index in 0..EVENTS_PER_ITERATION {
        storage
            .append_request_event(&RequestEvent {
                ts: 1_800_000_000,
                ts_ms: Some(1_800_000_000_000),
                request_id: format!("crash-recovery-{index}"),
                event_id: Some(format!("crash-recovery-event-{index}")),
                principal_id: Some("crash-recovery-principal".to_owned()),
                principal_kind: Some("account".to_owned()),
                upstream_id: Some(Uuid::nil()),
                upstream_name: Some("anthropic_direct".to_owned()),
                model: Some("claude-sonnet-4-5".to_owned()),
                status: 200,
                input_tokens: Some(10 + index),
                output_tokens: Some(20 + index),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                cost_usd_micros: None,
                duration_ms: 30 + index,
                error_code: None,
                ..Default::default()
            })
            .await?;
    }

    Ok(())
}

fn expected_rollups_visible(rollups: &[UsageRollup]) -> bool {
    if rollups.len() != 2 {
        return false;
    }

    let expected_input: u64 = (0..EVENTS_PER_ITERATION).map(|index| 10 + index).sum();
    let expected_output: u64 = (0..EVENTS_PER_ITERATION).map(|index| 20 + index).sum();
    let expected_latency_sum: u64 = (0..EVENTS_PER_ITERATION).map(|index| 30 + index).sum();
    let expected_latency_min = 30;
    let expected_latency_max = 30 + EVENTS_PER_ITERATION - 1;

    [UsageRollupResolution::Minute, UsageRollupResolution::Hour]
        .into_iter()
        .all(|resolution| {
            rollups.iter().any(|rollup| {
                rollup.resolution == resolution
                    && rollup.principal == "crash-recovery-principal"
                    && rollup.upstream_id == Uuid::nil()
                    && rollup.upstream_name == "anthropic_direct"
                    && rollup.model == "claude-sonnet-4-5"
                    && rollup.request_count == EVENTS_PER_ITERATION
                    && rollup.priced_event_count == 0
                    && rollup.unpriced_event_count == EVENTS_PER_ITERATION
                    && rollup.unmetered_event_count == 0
                    && rollup.input_tokens == expected_input
                    && rollup.output_tokens == expected_output
                    && rollup.error_count == 0
                    && rollup.latency_count == EVENTS_PER_ITERATION
                    && rollup.latency_ms_sum == expected_latency_sum
                    && rollup.latency_ms_min == Some(expected_latency_min)
                    && rollup.latency_ms_max == Some(expected_latency_max)
            })
        })
}

fn schema_name(scenario: &str, iter: usize) -> String {
    format!("cc_lb_crash_{scenario}_{iter}_{}", Uuid::new_v4().simple())
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}

fn rollup_result_label<T>(result: &Result<T, time::error::Elapsed>) -> &'static str {
    match result {
        Ok(_) => "completed",
        Err(_) => "elapsed",
    }
}

fn storage_result_label<T, E>(result: &Result<T, E>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(_) => "err",
    }
}

#[test]
fn pricing_backfill_checkpoint_failure_rolls_back_counts_and_resumes() {
    let Some(url) = get_postgres_url() else {
        eprintln!("skipped: CI_POSTGRES_URL unset; pricing crash recovery did not run");
        return;
    };
    let _guard = CRASH_RECOVERY_SCENARIO_LOCK.lock().unwrap();
    Runtime::new().unwrap().block_on(async {
        let fixture = CrashFixture::create(&url,&schema_name("pricing",0),"pricing",0.0).await.unwrap();
        fixture.wait_for_events_eligible().await.unwrap();
        fixture.storage.rollup_usage_once().await.unwrap();
        let checkpoint = fixture.storage.usage_rollup_checkpoint().await.unwrap();
        sqlx::raw_sql(
            "UPDATE usage_rollups_v2 SET priced_event_count=0,unpriced_event_count=0,unmetered_event_count=0;
             DELETE FROM usage_rollup_checkpoints_v1 WHERE id IN ('pricing_completeness_backfill_upper','pricing_completeness_backfill_cursor');
             CREATE FUNCTION fail_pricing_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'injected crash after pricing UPDATE before checkpoint commit'; END $$;
             CREATE TRIGGER fail_pricing_checkpoint BEFORE INSERT OR UPDATE ON usage_rollup_checkpoints_v1
             FOR EACH ROW WHEN (NEW.id='pricing_completeness_backfill_cursor' AND NEW.value>0)
             EXECUTE FUNCTION fail_pricing_checkpoint();"
        ).execute(&fixture.pool).await.unwrap();
        let revision = fixture.storage.usage_rollup_revision().await.unwrap();
        assert!(fixture.storage.rollup_usage_once().await.is_err());
        assert_eq!(fixture.storage.usage_rollup_checkpoint().await.unwrap(),checkpoint);
        assert_eq!(fixture.storage.usage_rollup_revision().await.unwrap(),revision);
        let counts: (i64,i64) = sqlx::query_as(
            "SELECT SUM(unpriced_event_count)::BIGINT,
             (SELECT COUNT(*) FROM usage_rollup_checkpoints_v1 WHERE id LIKE 'pricing_completeness_backfill_%')
             FROM usage_rollups_v2"
        ).fetch_one(&fixture.pool).await.unwrap();
        assert_eq!(counts,(0,0),"rollup UPDATE and both pricing cursors must roll back atomically");
        sqlx::query("DROP TRIGGER fail_pricing_checkpoint ON usage_rollup_checkpoints_v1")
            .execute(&fixture.pool).await.unwrap();
        let resumed = fixture.reconnect().await.unwrap();
        resumed.storage.rollup_usage_once().await.unwrap();
        assert!(matches!(resumed.consistency().await.unwrap(),Consistency::AllVisible));
        let revision = resumed.storage.usage_rollup_revision().await.unwrap();
        resumed.storage.rollup_usage_once().await.unwrap();
        assert_eq!(resumed.storage.usage_rollup_revision().await.unwrap(),revision);
        resumed.pool.close().await;
        fixture.drop_database().await.unwrap();
    });
}
