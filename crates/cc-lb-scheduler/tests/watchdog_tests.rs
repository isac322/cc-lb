#![cfg(all(feature = "sqlite", not(feature = "postgres")))]

use std::str::FromStr as _;
use std::sync::Arc;

use cc_lb_clock::SystemClock;
use cc_lb_scheduler::jobs::watchdog::{WatchdogEntityKind, run_entity_watchdog};
use cc_lb_scheduler::worker::AdaptiveJob;
use cc_lb_scheduler::worker::{SchedulerBackend, SchedulerPushTask, SqliteSchedulerBackend};
use tempfile::TempDir;
use uuid::Uuid;

const TICK_UNIX_SECS: u64 = 1_800_000_000;
const RUN_AT_UNIX_SECS: u64 = 1_800_000_001;

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn watchdog_seeds_only_when_primary_is_inactive() -> TestResult<()> {
    for kind in [WatchdogEntityKind::Warmup, WatchdogEntityKind::OAuthRefresh] {
        for scenario in scenarios() {
            let fixture = Fixture::new().await?;
            let upstream_id = Uuid::new_v4();
            if let Some(existing) = scenario.existing {
                fixture
                    .insert_existing(
                        kind,
                        upstream_id,
                        existing,
                        scenario.attempts,
                        scenario.max_attempts,
                    )
                    .await?;
            }

            let stats = run_entity_watchdog(
                &fixture.backend,
                kind,
                &[upstream_id],
                TICK_UNIX_SECS,
                RUN_AT_UNIX_SECS,
            )
            .await?;

            let key = kind.bootstrap_key(upstream_id, TICK_UNIX_SECS);
            assert_eq!(
                stats.seeded, scenario.expected_seeded,
                "{kind:?} {:?}",
                scenario.existing
            );
            assert_eq!(
                fixture.count_key(&key).await?,
                i64::try_from(scenario.expected_seeded)?,
                "{kind:?} {:?}",
                scenario.existing
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn watchdog_concurrent_same_tick_uses_one_bootstrap_key() -> TestResult<()> {
    for kind in [WatchdogEntityKind::Warmup, WatchdogEntityKind::OAuthRefresh] {
        let fixture = Fixture::new().await?;
        let upstream_id = Uuid::new_v4();
        let upstream_ids = [upstream_id];
        let first = run_entity_watchdog(
            &fixture.backend,
            kind,
            &upstream_ids,
            TICK_UNIX_SECS,
            RUN_AT_UNIX_SECS,
        );
        let second = run_entity_watchdog(
            &fixture.backend,
            kind,
            &upstream_ids,
            TICK_UNIX_SECS,
            RUN_AT_UNIX_SECS,
        );

        let (first, second) = tokio::join!(first, second);

        let total_seeded = first?.seeded + second?.seeded;
        let key = kind.bootstrap_key(upstream_id, TICK_UNIX_SECS);
        assert_eq!(total_seeded, 1, "{kind:?} concurrent seeded count");
        assert_eq!(fixture.count_key(&key).await?, 1, "{kind:?} bootstrap rows");
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct Scenario {
    existing: Option<&'static str>,
    attempts: i32,
    max_attempts: i32,
    expected_seeded: usize,
}

fn scenarios() -> [Scenario; 8] {
    [
        Scenario {
            existing: Some("Pending"),
            attempts: 0,
            max_attempts: 5,
            expected_seeded: 0,
        },
        Scenario {
            existing: Some("Queued"),
            attempts: 0,
            max_attempts: 5,
            expected_seeded: 0,
        },
        Scenario {
            existing: Some("Running"),
            attempts: 0,
            max_attempts: 5,
            expected_seeded: 0,
        },
        Scenario {
            existing: Some("Done"),
            attempts: 1,
            max_attempts: 5,
            expected_seeded: 1,
        },
        Scenario {
            existing: Some("Failed"),
            attempts: 2,
            max_attempts: 5,
            expected_seeded: 0,
        },
        Scenario {
            existing: Some("Failed"),
            attempts: 5,
            max_attempts: 5,
            expected_seeded: 1,
        },
        Scenario {
            existing: Some("Killed"),
            attempts: 1,
            max_attempts: 5,
            expected_seeded: 1,
        },
        Scenario {
            existing: None,
            attempts: 0,
            max_attempts: 5,
            expected_seeded: 1,
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
        let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
            pool.clone(),
            Arc::new(SystemClock),
        ));
        Ok(Self {
            _dir: dir,
            pool,
            backend,
        })
    }

    async fn insert_existing(
        &self,
        kind: WatchdogEntityKind,
        upstream_id: Uuid,
        status: &str,
        attempts: i32,
        max_attempts: i32,
    ) -> TestResult<()> {
        let key = kind.bootstrap_key(upstream_id, 1_700_000_000);
        let task = SchedulerPushTask {
            args: entity_job(kind, upstream_id),
            idempotency_key: Some(key.clone()),
            run_at_unix_secs: Some(RUN_AT_UNIX_SECS),
            max_attempts: None,
        };
        self.backend.push_adaptive_task(task).await?;
        sqlx::query(
            "UPDATE Jobs SET status = ?1, attempts = ?2, max_attempts = ?3 WHERE idempotency_key = ?4",
        )
        .bind(status)
        .bind(attempts)
        .bind(max_attempts)
        .bind(key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn count_key(&self, key: &str) -> TestResult<i64> {
        Ok(
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?1")
                .bind(key)
                .fetch_one(&self.pool)
                .await?,
        )
    }
}

fn entity_job(kind: WatchdogEntityKind, upstream_id: Uuid) -> AdaptiveJob {
    match kind {
        WatchdogEntityKind::Warmup => AdaptiveJob::Warmup(
            cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob::new(upstream_id, TICK_UNIX_SECS),
        ),
        WatchdogEntityKind::OAuthRefresh => AdaptiveJob::OAuthRefresh(
            cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::new(upstream_id),
        ),
    }
}
