use std::str::FromStr as _;
use std::sync::Arc;

use cc_lb_clock::SystemClock;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::jobs::watchdog::{WatchdogEntityKind, run_entity_watchdog};
use cc_lb_scheduler::worker::{
    ADAPTIVE_QUEUE, AdaptiveJob, SchedulerBackend, SchedulerPushTask, SqliteSchedulerStorage,
};
use tempfile::TempDir;
use uuid::Uuid;

const TICK_UNIX_SECS: u64 = 1_782_000_000;
const RUN_AT_UNIX_SECS: u64 = 1_782_000_030;
const SEVEN_DAY_RESET: u64 = 1_782_414_000;

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn watchdog_keeps_future_cycle_keyed_warmup_from_being_pulled_forward() -> TestResult<()> {
    for scenario in active_cycle_key_scenarios() {
        let fixture = Fixture::new().await?;
        let upstream_id = Uuid::new_v4();
        fixture
            .insert_warmup_cycle(upstream_id, scenario.into_existing_cycle())
            .await?;

        let stats = fixture.run_warmup_watchdog(upstream_id).await?;

        assert_eq!(stats.seeded, 0, "{} should remain active", scenario.status);
        assert_eq!(fixture.warmup_task_count(upstream_id).await?, 1);
        assert_eq!(
            fixture.cycle_run_at(upstream_id).await?,
            Some(SEVEN_DAY_RESET)
        );
    }
    Ok(())
}

#[tokio::test]
async fn watchdog_bootstraps_warmup_when_only_cycle_keyed_dlq_remains() -> TestResult<()> {
    let fixture = Fixture::new().await?;
    let upstream_id = Uuid::new_v4();
    fixture
        .insert_warmup_cycle(
            upstream_id,
            ExistingWarmupCycle {
                status: "Failed",
                attempts: 5,
                max_attempts: 5,
            },
        )
        .await?;

    let stats = fixture.run_warmup_watchdog(upstream_id).await?;

    assert_eq!(stats.seeded, 1);
    assert_eq!(fixture.warmup_task_count(upstream_id).await?, 2);
    assert_eq!(
        fixture.bootstrap_run_at(upstream_id).await?,
        Some(RUN_AT_UNIX_SECS)
    );
    Ok(())
}

#[tokio::test]
async fn watchdog_bootstrap_for_idle_warmup_runs_at_current_tick() -> TestResult<()> {
    let fixture = Fixture::new().await?;
    let upstream_id = Uuid::new_v4();

    let stats = fixture.run_warmup_watchdog(upstream_id).await?;

    assert_eq!(stats.seeded, 1);
    assert_eq!(
        fixture.bootstrap_run_at(upstream_id).await?,
        Some(RUN_AT_UNIX_SECS)
    );
    Ok(())
}

#[derive(Clone, Copy)]
struct CycleKeyScenario {
    status: &'static str,
    attempts: i32,
}

impl CycleKeyScenario {
    const fn into_existing_cycle(self) -> ExistingWarmupCycle {
        ExistingWarmupCycle {
            status: self.status,
            attempts: self.attempts,
            max_attempts: 5,
        }
    }
}

#[derive(Clone, Copy)]
struct ExistingWarmupCycle {
    status: &'static str,
    attempts: i32,
    max_attempts: i32,
}

fn active_cycle_key_scenarios() -> [CycleKeyScenario; 2] {
    [
        CycleKeyScenario {
            status: "Pending",
            attempts: 0,
        },
        CycleKeyScenario {
            status: "Failed",
            attempts: 4,
        },
    ]
}

struct Fixture {
    _dir: TempDir,
    pool: sqlx::SqlitePool,
    backend: SchedulerBackend,
}

impl Fixture {
    async fn new() -> TestResult<Self> {
        let dir = tempfile::tempdir()?;
        let url = format!("sqlite://{}", dir.path().join("scheduler.sqlite").display());
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(&url)?.create_if_missing(true);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        apalis_sqlite::SqliteStorage::setup(&pool).await?;
        cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
        let storage = apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_with_config(
            &pool,
            &apalis_sqlite::Config::new(ADAPTIVE_QUEUE),
        );
        let backend = SchedulerBackend::Sqlite(SqliteSchedulerStorage {
            pool: pool.clone(),
            storage,
            clock: Arc::new(SystemClock),
        });
        Ok(Self {
            _dir: dir,
            pool,
            backend,
        })
    }

    async fn insert_warmup_cycle(
        &self,
        upstream_id: Uuid,
        cycle: ExistingWarmupCycle,
    ) -> TestResult<()> {
        let job = UpstreamWarmupJob::new(upstream_id, SEVEN_DAY_RESET);
        let key = job.idempotency_key(SEVEN_DAY_RESET);
        self.backend
            .push_adaptive_task(SchedulerPushTask {
                args: AdaptiveJob::Warmup(job),
                idempotency_key: Some(key.clone()),
                run_at_unix_secs: Some(SEVEN_DAY_RESET),
                max_attempts: None,
            })
            .await?;
        sqlx::query(
            "UPDATE Jobs SET status = ?1, attempts = ?2, max_attempts = ?3 WHERE idempotency_key = ?4",
        )
        .bind(cycle.status)
        .bind(cycle.attempts)
        .bind(cycle.max_attempts)
        .bind(key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn run_warmup_watchdog(
        &self,
        upstream_id: Uuid,
    ) -> TestResult<cc_lb_scheduler::jobs::watchdog::WatchdogSeedStats> {
        Ok(run_entity_watchdog(
            &self.backend,
            WatchdogEntityKind::Warmup,
            &[upstream_id],
            TICK_UNIX_SECS,
            RUN_AT_UNIX_SECS,
        )
        .await?)
    }

    async fn warmup_task_count(&self, upstream_id: Uuid) -> TestResult<i64> {
        let pattern = format!("adaptive:warmup:{upstream_id}:%");
        Ok(
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE idempotency_key LIKE ?1")
                .bind(pattern)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn cycle_run_at(&self, upstream_id: Uuid) -> TestResult<Option<u64>> {
        let key =
            UpstreamWarmupJob::new(upstream_id, SEVEN_DAY_RESET).idempotency_key(SEVEN_DAY_RESET);
        self.run_at_for_key(key).await
    }

    async fn bootstrap_run_at(&self, upstream_id: Uuid) -> TestResult<Option<u64>> {
        self.run_at_for_key(WatchdogEntityKind::Warmup.bootstrap_key(upstream_id, TICK_UNIX_SECS))
            .await
    }

    async fn run_at_for_key(&self, key: String) -> TestResult<Option<u64>> {
        let run_at = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT run_at FROM Jobs WHERE idempotency_key = ?1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?
        .flatten();
        Ok(run_at.and_then(|value| u64::try_from(value).ok()))
    }
}
