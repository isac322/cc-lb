use std::sync::Arc;

use anyhow::Result;
use cc_lb_storage_api::{MetaStore, PlanTierStore, TierResolutionSource, UpstreamPlanTierRecord};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::PgPool;
use tokio::{runtime::Runtime, sync::Barrier};
use uuid::Uuid;

#[test]
fn plan_tier_first_insert_concurrent_upserts_are_idempotent() {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL or PG_URL not set");
        return;
    };

    Runtime::new()
        .expect("tokio runtime")
        .block_on(async move { run_test(&url).await })
        .expect("plan tier concurrent first insert test");
}

async fn run_test(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let result = concurrent_upstream_first_insert_is_idempotent(&fixture).await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

async fn concurrent_upstream_first_insert_is_idempotent(fixture: &Fixture) -> Result<()> {
    let upstream_id = Uuid::new_v4();
    let record = UpstreamPlanTierRecord {
        upstream_id,
        organization_uuid: Some("org-concurrent".to_owned()),
        organization_type: Some("claude_max".to_owned()),
        rate_limit_tier: Some("default_claude_max_5x".to_owned()),
        seat_tier: None,
        tier_key: Some("max_5x".to_owned()),
        resolution_source: TierResolutionSource::Builtin,
        resolved_ratio_snapshot: Some(5.0),
        observed_at_unix_millis: 1_900_000_000,
        effective_from_unix_millis: 1_900_000_000,
        effective_to_unix_millis: None,
        provenance: "plan-tier-concurrency-test".to_owned(),
        created_at_unix_millis: 1_900_000_000,
    };

    let first_storage = fixture.storage().await?;
    let second_storage = fixture.storage().await?;
    let barrier = Arc::new(Barrier::new(2));
    let first = tokio::spawn(append_upstream_after_barrier(
        first_storage,
        Arc::clone(&barrier),
        record.clone(),
    ));
    let second = tokio::spawn(append_upstream_after_barrier(
        second_storage,
        barrier,
        record,
    ));
    first.await??;
    second.await??;

    let open_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM upstream_plan_tier_history_v1 \
         WHERE upstream_id = $1 AND effective_to_unix_millis IS NULL",
    )
    .bind(upstream_id)
    .fetch_one(&fixture.pool)
    .await?;
    anyhow::ensure!(open_rows == 1, "expected one open upstream plan-tier row");
    Ok(())
}

async fn append_upstream_after_barrier(
    storage: PostgresStorage,
    barrier: Arc<Barrier>,
    record: UpstreamPlanTierRecord,
) -> Result<()> {
    barrier.wait().await;
    storage.append_upstream_plan_tier(&record).await?;
    storage.pool().close().await;
    Ok(())
}

struct Fixture {
    isolated: crate::pg_test_support::IsolatedPg,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let isolated = crate::pg_test_support::IsolatedPg::create_templated(url).await?;
        let pool = match isolated.pool(4).await {
            Ok(pool) => pool,
            Err(error) => {
                let _ = isolated.drop().await;
                return Err(error);
            }
        };
        PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock))
            .initialize()
            .await?;

        Ok(Self { isolated, pool })
    }

    async fn storage(&self) -> Result<PostgresStorage> {
        let pool = self.isolated.pool(1).await?;
        Ok(PostgresStorage::new(
            pool,
            Arc::new(cc_lb_clock::SystemClock),
        ))
    }

    async fn drop_schema(self) -> Result<()> {
        self.pool.close().await;
        self.isolated.drop().await
    }
}

fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
}
