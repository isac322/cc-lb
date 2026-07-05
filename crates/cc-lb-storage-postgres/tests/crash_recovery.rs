use std::time::Duration;

use cc_lb_storage_api::{
    BackendKind, MetaStore, RequestEvent, RequestEventStore, RequestEventUpstream, UsageRollup,
    UsageRollupResolution, UsageRollupStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgPoolOptions};
use tokio::{runtime::Runtime, time};
use uuid::Uuid;

const TIMEOUT_ITERATIONS: usize = 50;
const CONNECTION_DROP_ITERATIONS: usize = 50;
const EVENTS_PER_ITERATION: u64 = 5;
fn get_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

#[test]
fn client_timeout_mid_commit() {
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
            for iter in 0..TIMEOUT_ITERATIONS {
                let schema = schema_name("timeout", iter);
                let fixture = CrashFixture::create(&url, &schema, "timeout", 0.05).await?;
                let storage = fixture.storage.clone();

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

                fixture.drop_schema().await?;
            }

            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .expect("client timeout crash recovery test");
}

#[test]
fn connection_drop_mid_commit() {
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
            for iter in 0..CONNECTION_DROP_ITERATIONS {
                let schema = schema_name("drop", iter);
                let fixture = CrashFixture::create(&url, &schema, "drop", 2.0).await?;
                let app_name = fixture.app_name.clone();
                let storage = fixture.storage.clone();
                let pool = fixture.pool.clone();

                let handle = tokio::spawn(async move { storage.rollup_usage_once().await });
                let pid = fixture.wait_for_checkpoint_backend(&app_name).await?;
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

                verifier.drop_schema().await?;
            }

            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .expect("connection drop crash recovery test");
}

#[derive(Clone)]
struct CrashFixture {
    url: String,
    schema: String,
    app_name: String,
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
        let admin_pool = PgPoolOptions::new().max_connections(2).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(schema)
        )))
        .execute(&admin_pool)
        .await?;

        let app_name = format!("cc_lb_crash_{scenario}_{}", Uuid::new_v4().simple());
        let pool = schema_pool(url, schema, &app_name, 1).await?;
        let storage =
            PostgresStorage::new(pool.clone(), std::sync::Arc::new(cc_lb_clock::SystemClock));
        storage.initialize(BackendKind::Postgres).await?;
        install_checkpoint_sleep_trigger(&pool, checkpoint_sleep_secs).await?;
        seed_request_events(&storage).await?;

        Ok(Self {
            url: url.to_owned(),
            schema: schema.to_owned(),
            app_name,
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
            schema: self.schema.clone(),
            app_name: self.app_name.clone(),
            admin_pool: self.admin_pool.clone(),
            pool,
            storage,
        })
    }

    async fn consistency(&self) -> Result<Consistency, Box<dyn std::error::Error>> {
        let checkpoint = self.storage.usage_rollup_checkpoint().await?;
        let rollups = self.storage.query_usage_rollups().await?;

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

    async fn wait_for_checkpoint_backend(
        &self,
        app_name: &str,
    ) -> Result<i32, Box<dyn std::error::Error>> {
        for _ in 0..200 {
            let pid = sqlx::query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity                  WHERE application_name = $1                    AND state = 'active'                    AND query LIKE '%usage_rollup_checkpoints_v1%'                  ORDER BY query_start ASC                  LIMIT 1",
            )
            .bind(app_name)
            .fetch_optional(&self.admin_pool)
            .await?;

            if let Some(pid) = pid {
                return Ok(pid);
            }

            time::sleep(Duration::from_millis(10)).await;
        }

        Err(format!("timed out waiting for active checkpoint query for {app_name}").into())
    }

    async fn drop_schema(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&self.schema)
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
) -> Result<(), sqlx::Error> {
    sqlx::query(AssertSqlSafe(format!(
        "CREATE OR REPLACE FUNCTION cc_lb_sleep_before_checkpoint()          RETURNS trigger          LANGUAGE plpgsql          AS $$          BEGIN              PERFORM pg_sleep({sleep_secs});              RETURN NEW;          END;          $$"
    )))
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TRIGGER cc_lb_sleep_before_checkpoint_trigger          BEFORE INSERT OR UPDATE ON usage_rollup_checkpoints_v1          FOR EACH ROW          WHEN (NEW.id = 'high_water')          EXECUTE FUNCTION cc_lb_sleep_before_checkpoint()",
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
                request_id: format!("crash-recovery-{index}"),
                principal_id: Some("crash-recovery-principal".to_owned()),
                principal_kind: Some("account".to_owned()),
                upstream: Some(RequestEventUpstream::AnthropicDirect),
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
